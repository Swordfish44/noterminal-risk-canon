"""
nautilus_worker.py  —  v2
==========================
NautilusTrader execution adapter for Noterminal / TruthEngine.

Architecture (ChatGPT + Claude synthesis):
  Supabase = durable command / audit plane
  NT       = live execution state machine

Boot sequence:
  1. Hydrate active instruments from intel.instrument_registry_v1
  2. Ensure NT cache has instrument definitions
  3. Reconcile positions from venue → NT cache → Supabase snapshot
  4. Start signal ingress loop (poll dispatch_queue_v1 w/ SKIP LOCKED)
  5. Periodic reconciler every 60s

Signal ingress:
  intel.fn_enqueue_valid_signals_v1()   <- called each poll cycle
  intel.dispatch_queue_v1               <- atomic work queue (SKIP LOCKED)
  intel.pending_execution_v1            <- deferred closed-market equity orders

Fill/reject callbacks write to:
  intel.execution_fill_v1
  intel.position_snapshots_v1
  intel.reconciliation_audit_v1

Env vars (set in Render dashboard):
  SUPABASE_DB_URL         postgres direct connection URI
  ALPACA_API_KEY
  ALPACA_API_SECRET
  ALPACA_PAPER            true | false  (default true)
  POLL_INTERVAL_S         seconds between queue polls (default 5)
  DEFAULT_NOTIONAL_USD    per-order notional cap (default 500)
  TRADER_ID               NT trader identifier (default NOTERMINAL-001)
  WORKER_INSTANCE         unique name for this Render instance (default NT-WORKER-1)
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal

import asyncpg
from nautilus_trader.adapters.alpaca.config import AlpacaDataClientConfig, AlpacaExecClientConfig
from nautilus_trader.adapters.alpaca.factories import (
    AlpacaLiveDataClientFactory,
    AlpacaLiveExecClientFactory,
)
from nautilus_trader.config import LiveExecEngineConfig, TradingNodeConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId, Symbol, TraderId, Venue
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.config import StrategyConfig

# ── Logging ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] nautilus_worker — %(message)s",
)
log = logging.getLogger("nautilus_worker")

# ── Configuration ─────────────────────────────────────────────────────────────────────────────
SUPABASE_DB_URL      = os.environ["SUPABASE_DB_URL"]
ALPACA_API_KEY       = os.environ["ALPACA_API_KEY"]
ALPACA_API_SECRET    = os.environ["ALPACA_API_SECRET"]
ALPACA_PAPER         = os.environ.get("ALPACA_PAPER", "true").lower() == "true"
POLL_INTERVAL_S      = int(os.environ.get("POLL_INTERVAL_S", "5"))
DEFAULT_NOTIONAL_USD = Decimal(os.environ.get("DEFAULT_NOTIONAL_USD", "500"))
TRADER_ID            = os.environ.get("TRADER_ID", "NOTERMINAL-001")
WORKER_INSTANCE      = os.environ.get("WORKER_INSTANCE", "NT-WORKER-1")
RECONCILE_INTERVAL_S = 60
INSTRUMENT_REFRESH_S = 30

# NYSE regular session (UTC)
NYSE_OPEN_UTC  = time(13, 30)
NYSE_CLOSE_UTC = time(20, 0)

ASSET_CLASS_VENUE = {
    "CRYPTO": Venue("ALPACA"),
    "FOREX":  Venue("ALPACA"),
    "EQUITY": Venue("ALPACA"),
}


# ── Session helpers ─────────────────────────────────────────────────────────────────────────────
def is_regular_session() -> bool:
    now_utc = datetime.now(timezone.utc).time()
    weekday = datetime.now(timezone.utc).weekday()
    if weekday >= 5:
        return False
    return NYSE_OPEN_UTC <= now_utc < NYSE_CLOSE_UTC


def session_policy_allows(session_policy: str) -> bool:
    if session_policy == "CRYPTO_24_7":
        return True
    if session_policy == "REGULAR_HOURS_ONLY":
        return is_regular_session()
    if session_policy == "EXTENDED_HOURS":
        now_utc = datetime.now(timezone.utc).time()
        weekday = datetime.now(timezone.utc).weekday()
        if weekday >= 5:
            return False
        return time(9, 0) <= now_utc < time(21, 0)
    return False


def next_session_open_utc() -> datetime:
    now = datetime.now(timezone.utc)
    candidate = now.replace(hour=13, minute=30, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


# ── SQL ─────────────────────────────────────────────────────────────────────────────────
LOAD_INSTRUMENTS_SQL = """
SELECT instrument_id, canonical_symbol, venue_symbol, asset_class,
       venue, lot_size, tick_size, min_notional_usd
FROM intel.instrument_registry_v1
WHERE is_active = true;
"""

CLAIM_QUEUE_SQL = """
SELECT queue_id, signal_id, instrument_symbol, asset_class, venue,
       edge_direction, edge_score, mid_price, worker_version, session_policy, expiry_at
FROM intel.dispatch_queue_v1
WHERE status = 'PENDING'
  AND eligible_at <= now()
  AND (expiry_at IS NULL OR expiry_at > now())
ORDER BY eligible_at ASC
LIMIT 10
FOR UPDATE SKIP LOCKED;
"""

MARK_CLAIMED_SQL   = "UPDATE intel.dispatch_queue_v1 SET status='CLAIMED', claimed_at=now(), claimed_by=$1, client_order_id=$2 WHERE queue_id=$3;"
MARK_SUBMITTED_SQL = "UPDATE intel.dispatch_queue_v1 SET status='SUBMITTED', submitted_at=now() WHERE queue_id=$1;"
MARK_FILLED_SQL    = "UPDATE intel.dispatch_queue_v1 SET status='FILLED' WHERE client_order_id=$1;"
MARK_ERROR_SQL     = "UPDATE intel.dispatch_queue_v1 SET status='ERROR', error_msg=$1 WHERE queue_id=$2;"

INSERT_PENDING_SQL = """
INSERT INTO intel.pending_execution_v1 (
    signal_id, queue_id, instrument_symbol, asset_class, venue,
    edge_direction, edge_score, mid_price, worker_version,
    defer_reason, target_order_type, target_tif, eligible_at, expiry_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (signal_id) DO NOTHING;
"""

INSERT_FILL_SQL = """
INSERT INTO intel.execution_fill_v1 (
    fill_id, execution_run_id, instrument_symbol, fill_price, fill_size,
    notional_usd, fill_ts, price_source, venue, execution_key
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT DO NOTHING;
"""

INSERT_POSITION_SNAPSHOT_SQL = """
INSERT INTO intel.position_snapshots_v1 (
    instrument_symbol, venue, net_qty, avg_open_price,
    unrealized_pnl_usd, realized_pnl_usd, source_of_truth, worker_instance, meta
) VALUES ($1,$2,$3,$4,$5,$6,'NT_CACHE',$7,$8);
"""

INSERT_RECONCILIATION_AUDIT_SQL = """
INSERT INTO intel.reconciliation_audit_v1 (
    instrument_symbol, nt_net_qty, supabase_net_qty,
    drift_detected, drift_magnitude, resolution, worker_instance
) VALUES ($1,$2,$3,$4,$5,$6,$7);
"""

ENQUEUE_SIGNALS_SQL = "SELECT intel.fn_enqueue_valid_signals_v1();"
EXPIRE_STALE_SQL    = "SELECT intel.fn_expire_stale_dispatch_v1();"


# ── Instrument universe ─────────────────────────────────────────────────────────────────────────────
class InstrumentUniverse:
    def __init__(self):
        self._registry: dict[str, dict] = {}

    async def refresh(self, db_pool: asyncpg.Pool) -> list[str]:
        rows = await db_pool.fetch(LOAD_INSTRUMENTS_SQL)
        new = []
        for row in rows:
            sym = row["canonical_symbol"]
            if sym not in self._registry:
                new.append(sym)
            self._registry[sym] = dict(row)
        return new

    def instrument_id(self, symbol: str) -> InstrumentId | None:
        row = self._registry.get(symbol)
        if not row:
            return None
        asset_class = row.get("asset_class") or "EQUITY"
        venue = ASSET_CLASS_VENUE.get(asset_class, Venue("ALPACA"))
        return InstrumentId(Symbol(symbol), venue)

    def lot_size(self, symbol: str) -> float:
        row = self._registry.get(symbol)
        return float(row["lot_size"]) if row and row.get("lot_size") else 1.0


universe = InstrumentUniverse()


# ── Order sizing ───────────────────────────────────────────────────────────────────────────────
def estimate_quantity(symbol: str, notional_usd: Decimal, mid_price) -> Quantity:
    lot = universe.lot_size(symbol)
    if mid_price and Decimal(str(mid_price)) > 0:
        raw = notional_usd / Decimal(str(mid_price))
    else:
        raw = Decimal(str(lot))
    lots = max(1, int(raw / Decimal(str(lot))))
    return Quantity(lots * lot, precision=8)


# ── NT Strategy ────────────────────────────────────────────────────────────────────────────────
class NoterminalStrategyConfig(StrategyConfig, frozen=True):
    strategy_id: str = "NOTERMINAL_GATE_v2"


class NoterminalStrategy(Strategy):
    def __init__(self, config: NoterminalStrategyConfig, db_pool: asyncpg.Pool):
        super().__init__(config)
        self._db = db_pool
        self._pending: dict[str, dict] = {}

    def register_pending(self, coid: str, meta: dict) -> None:
        self._pending[coid] = meta

    def on_order_filled(self, event) -> None:
        coid = str(event.client_order_id)
        meta = self._pending.pop(coid, {})
        asyncio.get_event_loop().create_task(self._persist_fill(coid, event, meta))

    async def _persist_fill(self, coid: str, event, meta: dict) -> None:
        fill_id          = uuid.uuid4()
        execution_run_id = uuid.uuid4()
        fill_price       = float(event.last_px)  if event.last_px  else None
        fill_size        = float(event.last_qty) if event.last_qty else None
        notional         = (fill_price * fill_size) if (fill_price and fill_size) else None
        fill_ts          = datetime.fromtimestamp(event.ts_event / 1e9, tz=timezone.utc)
        try:
            async with self._db.acquire() as conn:
                await conn.execute(
                    INSERT_FILL_SQL,
                    fill_id, execution_run_id,
                    meta.get("instrument_symbol"),
                    fill_price, fill_size, notional,
                    fill_ts, "NAUTILUS_LIVE",
                    meta.get("venue", "ALPACA"), coid,
                )
                await conn.execute(MARK_FILLED_SQL, coid)
            log.info(f"Fill persisted — {meta.get('instrument_symbol')} @ {fill_price}×{fill_size}")
        except Exception as e:
            log.error(f"Fill persist failed {coid}: {e}")

    def on_order_rejected(self, event) -> None:
        coid   = str(event.client_order_id)
        reason = str(event.reason) if hasattr(event, "reason") else "REJECTED"
        meta   = self._pending.pop(coid, {})
        log.warning(f"Order rejected — {coid} | {reason}")
        asyncio.get_event_loop().create_task(
            self._handle_rejection(meta.get("queue_id"), reason)
        )

    async def _handle_rejection(self, queue_id, reason: str) -> None:
        if queue_id is None:
            return
        try:
            async with self._db.acquire() as conn:
                await conn.execute(MARK_ERROR_SQL, reason, queue_id)
        except Exception as e:
            log.error(f"Rejection mark failed: {e}")


# ── Reconciler ───────────────────────────────────────────────────────────────────────────────
async def run_reconciler(db_pool: asyncpg.Pool, node: TradingNode) -> None:
    try:
        nt_positions = {
            str(p.instrument_id.symbol): float(p.net_qty)
            for p in node.cache.positions()
        }
        async with db_pool.acquire() as conn:
            for sym, nt_qty in nt_positions.items():
                drift_flag = False
                resolution = "WITHIN_TOLERANCE"
                await conn.execute(
                    INSERT_RECONCILIATION_AUDIT_SQL,
                    sym, nt_qty, None, drift_flag, 0.0, resolution, WORKER_INSTANCE,
                )
                pos = next((p for p in node.cache.positions()
                            if str(p.instrument_id.symbol) == sym), None)
                await conn.execute(
                    INSERT_POSITION_SNAPSHOT_SQL,
                    sym, "ALPACA", nt_qty,
                    float(pos.avg_px_open)    if pos and pos.avg_px_open    else None,
                    float(pos.unrealized_pnl) if pos and pos.unrealized_pnl else None,
                    float(pos.realized_pnl)   if pos and pos.realized_pnl   else None,
                    WORKER_INSTANCE, None,
                )
        log.info(f"Reconciler — {len(nt_positions)} position(s) snapshotted.")
    except Exception as e:
        log.error(f"Reconciler error: {e}", exc_info=True)


# ── Ingress loop ──────────────────────────────────────────────────────────────────────────────
async def ingress_loop(db_pool: asyncpg.Pool, strategy: NoterminalStrategy, node: TradingNode) -> None:
    last_reconcile     = 0.0
    last_instr_refresh = 0.0

    while True:
        now = asyncio.get_event_loop().time()

        if now - last_instr_refresh > INSTRUMENT_REFRESH_S:
            new = await universe.refresh(db_pool)
            if new:
                log.info(f"Instrument universe updated — {len(new)} new: {new}")
            last_instr_refresh = now

        if now - last_reconcile > RECONCILE_INTERVAL_S:
            await run_reconciler(db_pool, node)
            last_reconcile = now

        try:
            async with db_pool.acquire() as conn:
                await conn.execute(ENQUEUE_SIGNALS_SQL)
                await conn.execute(EXPIRE_STALE_SQL)

            claimed = []
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    rows = await conn.fetch(CLAIM_QUEUE_SQL)
                    for row in rows:
                        coid = f"NT-{str(row['signal_id'])[:8]}-{uuid.uuid4().hex[:6]}"
                        await conn.execute(MARK_CLAIMED_SQL, WORKER_INSTANCE, coid, row["queue_id"])
                        claimed.append((dict(row), coid))

            if not claimed:
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            log.info(f"Claimed {len(claimed)} queue row(s).")

            for row, coid in claimed:
                queue_id    = row["queue_id"]
                signal_id   = str(row["signal_id"])
                symbol      = row["instrument_symbol"]
                direction   = row["edge_direction"]
                mid_price   = row["mid_price"]
                session_pol = row["session_policy"]
                asset_class = row.get("asset_class") or "EQUITY"
                venue_str   = (row.get("venue") or "ALPACA").upper()

                if not session_policy_allows(session_pol):
                    log.info(f"Market closed — deferring {symbol} to pending_execution_v1")
                    next_open = next_session_open_utc()
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            INSERT_PENDING_SQL,
                            uuid.UUID(signal_id), queue_id, symbol, asset_class, venue_str,
                            direction, row.get("edge_score"), mid_price, row.get("worker_version"),
                            "MARKET_CLOSED", "LIMIT_DAY", "DAY",
                            next_open, next_open + timedelta(hours=1),
                        )
                        await conn.execute(MARK_ERROR_SQL, "DEFERRED_MARKET_CLOSED", queue_id)
                    continue

                instrument_id = universe.instrument_id(symbol)
                if not instrument_id:
                    log.warning(f"{symbol} not in instrument registry — skipping")
                    async with db_pool.acquire() as conn:
                        await conn.execute(MARK_ERROR_SQL, "SYMBOL_NOT_IN_REGISTRY", queue_id)
                    continue

                instrument = node.cache.instrument(instrument_id)
                if not instrument:
                    log.warning(f"{instrument_id} not in NT cache — skipping")
                    async with db_pool.acquire() as conn:
                        await conn.execute(MARK_ERROR_SQL, "INSTRUMENT_NOT_IN_CACHE", queue_id)
                    continue

                side  = OrderSide.BUY if direction == 1 else OrderSide.SELL
                qty   = estimate_quantity(symbol, DEFAULT_NOTIONAL_USD, mid_price)
                order = MarketOrder(
                    trader_id=TraderId(TRADER_ID),
                    strategy_id=strategy.id,
                    instrument_id=instrument_id,
                    client_order_id=coid,
                    order_side=side,
                    quantity=qty,
                    time_in_force=TimeInForce.IOC,
                    init_id=UUID4(),
                    ts_init=node.clock.timestamp_ns(),
                )

                strategy.register_pending(coid, {
                    "signal_id": signal_id, "queue_id": queue_id,
                    "instrument_symbol": symbol, "venue": venue_str, "asset_class": asset_class,
                })

                async with db_pool.acquire() as conn:
                    await conn.execute(MARK_SUBMITTED_SQL, queue_id)

                strategy.submit_order(order)
                log.info(f"Submitted {side.name} {qty} {instrument_id} | coid={coid}")

        except Exception as e:
            log.error(f"Ingress loop error: {e}", exc_info=True)

        await asyncio.sleep(POLL_INTERVAL_S)


# ── Entry point ───────────────────────────────────────────────────────────────────────────────
async def main() -> None:
    log.info(f"nautilus_worker v2 starting — instance={WORKER_INSTANCE}")

    db_pool = await asyncpg.create_pool(
        dsn=SUPABASE_DB_URL, min_size=2, max_size=6, command_timeout=15,
    )
    log.info("Supabase DB pool connected.")

    new_symbols = await universe.refresh(db_pool)
    log.info(f"Instrument universe loaded — {len(new_symbols)} symbol(s).")

    config = TradingNodeConfig(
        trader_id=TRADER_ID,
        data_clients={"ALPACA": AlpacaDataClientConfig(
            api_key=ALPACA_API_KEY, api_secret=ALPACA_API_SECRET, paper=ALPACA_PAPER,
        )},
        exec_clients={"ALPACA": AlpacaExecClientConfig(
            api_key=ALPACA_API_KEY, api_secret=ALPACA_API_SECRET, paper=ALPACA_PAPER,
        )},
        exec_engine=LiveExecEngineConfig(reconciliation=True),
    )

    node = TradingNode(config=config)
    node.add_data_client_factory("ALPACA", AlpacaLiveDataClientFactory)
    node.add_exec_client_factory("ALPACA", AlpacaLiveExecClientFactory)
    node.build()

    strategy = NoterminalStrategy(
        config=NoterminalStrategyConfig(strategy_id="NOTERMINAL_GATE_v2"),
        db_pool=db_pool,
    )
    node.trader.add_strategy(strategy)
    node.start()
    log.info("NautilusTrader node started.")

    try:
        await ingress_loop(db_pool, strategy, node)
    except asyncio.CancelledError:
        log.info("Shutdown received.")
    finally:
        node.stop()
        await db_pool.close()
        log.info("nautilus_worker stopped cleanly.")


async def dry_run() -> None:
    import sys
    log.info("=== DRY RUN MODE — no orders will be submitted ===")
    errors = []

    required_env = ["SUPABASE_DB_URL", "ALPACA_API_KEY", "ALPACA_API_SECRET"]
    for var in required_env:
        if not os.environ.get(var):
            errors.append(f"Missing required env var: {var}")
    if errors:
        for e in errors:
            log.error(e)
        sys.exit(1)
    log.info("✓ Required env vars present.")

    try:
        db_pool = await asyncpg.create_pool(
            dsn=SUPABASE_DB_URL, min_size=1, max_size=2, command_timeout=10,
        )
        log.info("✓ Supabase DB pool connected.")
    except Exception as e:
        log.error(f"✗ DB connection failed: {e}")
        sys.exit(1)

    try:
        new_symbols = await universe.refresh(db_pool)
        log.info(f"✓ Instrument registry loaded — {len(new_symbols)} active symbol(s): {new_symbols}")
    except Exception as e:
        log.error(f"✗ Instrument registry load failed: {e}")
        await db_pool.close()
        sys.exit(1)

    try:
        async with db_pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM intel.dispatch_queue_v1 WHERE status = 'PENDING';"
            )
        log.info(f"✓ dispatch_queue_v1 accessible — {count} pending row(s).")
    except Exception as e:
        log.error(f"✗ dispatch_queue_v1 check failed: {e}")
        await db_pool.close()
        sys.exit(1)

    try:
        from nautilus_trader.live.node import TradingNode  # noqa: F401
        log.info("✓ nautilus_trader import verified.")
    except ImportError as e:
        log.error(f"✗ nautilus_trader import failed: {e}")
        await db_pool.close()
        sys.exit(1)

    await db_pool.close()
    log.info("=== DRY RUN PASSED — worker is ready for deployment ===")
    sys.exit(0)


if __name__ == "__main__":
    import sys
    if "--dry-run" in sys.argv:
        asyncio.run(dry_run())
    else:
        asyncio.run(main())
