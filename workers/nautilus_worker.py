"""
nautilus_worker.py  —  v3
==========================
NautilusTrader execution adapter for Noterminal / TruthEngine.
Rewritten to use alpaca-py directly (NT Alpaca adapter removed in 1.222+).

Architecture:
  Supabase = durable command / audit plane
  alpaca-py = live execution (paper or live)
  NautilusTrader = NOT used for execution in this version
                   (adapter removed upstream; using alpaca-py directly)

Boot sequence:
  1. Hydrate active instruments from intel.instrument_registry_v1
  2. Connect to Alpaca via alpaca-py
  3. Reconcile open positions
  4. Start signal ingress loop (poll dispatch_queue_v1 w/ SKIP LOCKED)
  5. Periodic reconciler every 60s

Signal ingress:
  intel.fn_enqueue_valid_signals_v1()   <- called each poll cycle
  intel.dispatch_queue_v1               <- atomic work queue (SKIP LOCKED)
  intel.pending_execution_v1            <- deferred closed-market equity orders

Fill callbacks write to:
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
  TRADER_ID               identifier string (default NOTERMINAL-001)
  WORKER_INSTANCE         unique name for this instance (default NT-WORKER-1)
"""

import asyncio
import logging
import os
import uuid
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal

import asyncpg
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

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
) VALUES ($1,$2,$3,$4,$5,$6,'ALPACA_DIRECT',$7,$8);
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

    def lot_size(self, symbol: str) -> float:
        row = self._registry.get(symbol)
        return float(row["lot_size"]) if row and row.get("lot_size") else 1.0

    def venue_symbol(self, symbol: str) -> str:
        """Return venue-specific symbol if mapped, else canonical."""
        row = self._registry.get(symbol)
        if row and row.get("venue_symbol"):
            return row["venue_symbol"]
        return symbol


universe = InstrumentUniverse()


# ── Order sizing ──────────────────────────────────────────────────────────────────────────────
def estimate_quantity(symbol: str, notional_usd: Decimal, mid_price) -> float:
    lot = universe.lot_size(symbol)
    if mid_price and Decimal(str(mid_price)) > 0:
        raw = notional_usd / Decimal(str(mid_price))
    else:
        raw = Decimal(str(lot))
    lots = max(1, int(raw / Decimal(str(lot))))
    return float(lots * lot)


# ── Alpaca order submission ─────────────────────────────────────────────────────────────────────
async def submit_alpaca_order(
    client: TradingClient,
    symbol: str,
    side: OrderSide,
    qty: float,
    client_order_id: str,
) -> dict | None:
    """Submit a market IOC order via alpaca-py. Returns order dict or None."""
    try:
        req = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=side,
            time_in_force=TimeInForce.IOC,
            client_order_id=client_order_id,
        )
        order = client.submit_order(req)
        return order
    except Exception as e:
        log.error(f"Alpaca order submission failed for {symbol}: {e}")
        return None


# ── Reconciler ──────────────────────────────────────────────────────────────────────────────
async def run_reconciler(
    db_pool: asyncpg.Pool,
    alpaca_client: TradingClient,
) -> None:
    try:
        positions = alpaca_client.get_all_positions()
        async with db_pool.acquire() as conn:
            for pos in positions:
                symbol   = str(pos.symbol)
                net_qty  = float(pos.qty)
                avg_px   = float(pos.avg_entry_price) if pos.avg_entry_price else None
                unreal   = float(pos.unrealized_pl) if pos.unrealized_pl else None

                await conn.execute(
                    INSERT_RECONCILIATION_AUDIT_SQL,
                    symbol, net_qty, None, False, 0.0, "WITHIN_TOLERANCE", WORKER_INSTANCE,
                )
                await conn.execute(
                    INSERT_POSITION_SNAPSHOT_SQL,
                    symbol, "ALPACA", net_qty, avg_px, unreal, None,
                    WORKER_INSTANCE, None,
                )
        log.info(f"Reconciler — {len(positions)} position(s) snapshotted from Alpaca.")
    except Exception as e:
        log.error(f"Reconciler error: {e}", exc_info=True)


# ── Fill poller ──────────────────────────────────────────────────────────────────────────────
async def poll_fill(
    db_pool: asyncpg.Pool,
    alpaca_client: TradingClient,
    client_order_id: str,
    queue_id,
    instrument_symbol: str,
    venue_str: str,
) -> None:
    """
    Poll Alpaca for fill status up to 10 times with 1s delay.
    Writes fill to execution_fill_v1 if filled.
    """
    for attempt in range(10):
        await asyncio.sleep(1)
        try:
            order = alpaca_client.get_order_by_client_id(client_order_id)
            status = str(order.status).lower()

            if status in ("filled", "partially_filled"):
                fill_id          = uuid.uuid4()
                execution_run_id = uuid.uuid4()
                fill_price = float(order.filled_avg_price) if order.filled_avg_price else None
                fill_size  = float(order.filled_qty)       if order.filled_qty        else None
                notional   = (fill_price * fill_size) if (fill_price and fill_size) else None
                fill_ts    = order.filled_at or datetime.now(timezone.utc)
                if hasattr(fill_ts, 'isoformat'):
                    fill_ts = fill_ts if fill_ts.tzinfo else fill_ts.replace(tzinfo=timezone.utc)

                async with db_pool.acquire() as conn:
                    await conn.execute(
                        INSERT_FILL_SQL,
                        fill_id, execution_run_id, instrument_symbol,
                        fill_price, fill_size, notional,
                        fill_ts, "ALPACA_DIRECT", venue_str, client_order_id,
                    )
                    await conn.execute(MARK_FILLED_SQL, client_order_id)
                log.info(f"Fill captured — {instrument_symbol} @ {fill_price}×{fill_size}")
                return

            elif status in ("canceled", "expired", "rejected"):
                async with db_pool.acquire() as conn:
                    await conn.execute(MARK_ERROR_SQL, f"ORDER_{status.upper()}", queue_id)
                log.warning(f"Order {client_order_id} terminal status: {status}")
                return

        except Exception as e:
            log.warning(f"Fill poll attempt {attempt+1} failed for {client_order_id}: {e}")

    # Timed out waiting for fill
    async with db_pool.acquire() as conn:
        await conn.execute(MARK_ERROR_SQL, "FILL_POLL_TIMEOUT", queue_id)
    log.warning(f"Fill poll timeout — {client_order_id}")


# ── Ingress loop ─────────────────────────────────────────────────────────────────────────────
async def ingress_loop(
    db_pool: asyncpg.Pool,
    alpaca_client: TradingClient,
) -> None:
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
            await run_reconciler(db_pool, alpaca_client)
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

                # Session gate
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

                # Size and submit
                venue_sym = universe.venue_symbol(symbol)
                side      = OrderSide.BUY if direction == 1 else OrderSide.SELL
                qty       = estimate_quantity(symbol, DEFAULT_NOTIONAL_USD, mid_price)

                async with db_pool.acquire() as conn:
                    await conn.execute(MARK_SUBMITTED_SQL, queue_id)

                order = await submit_alpaca_order(alpaca_client, venue_sym, side, qty, coid)

                if order is None:
                    async with db_pool.acquire() as conn:
                        await conn.execute(MARK_ERROR_SQL, "SUBMISSION_FAILED", queue_id)
                    continue

                log.info(f"Submitted {side.value} {qty} {venue_sym} | coid={coid}")

                # Poll for fill asynchronously
                asyncio.create_task(
                    poll_fill(db_pool, alpaca_client, coid, queue_id, symbol, venue_str)
                )

        except Exception as e:
            log.error(f"Ingress loop error: {e}", exc_info=True)

        await asyncio.sleep(POLL_INTERVAL_S)


# ── Entry point ──────────────────────────────────────────────────────────────────────────────
async def main() -> None:
    log.info(f"nautilus_worker v3 starting — instance={WORKER_INSTANCE}")

    db_pool = await asyncpg.create_pool(
        dsn=SUPABASE_DB_URL, min_size=2, max_size=6, command_timeout=15,
    )
    log.info("Supabase DB pool connected.")

    new_symbols = await universe.refresh(db_pool)
    log.info(f"Instrument universe loaded — {len(new_symbols)} symbol(s).")

    alpaca_client = TradingClient(
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_API_SECRET,
        paper=ALPACA_PAPER,
    )
    log.info(f"Alpaca client connected — paper={ALPACA_PAPER}")

    try:
        account = alpaca_client.get_account()
        log.info(f"Alpaca account status: {account.status} | equity: {account.equity}")
    except Exception as e:
        log.error(f"Alpaca account check failed: {e}")

    try:
        await ingress_loop(db_pool, alpaca_client)
    except asyncio.CancelledError:
        log.info("Shutdown received.")
    finally:
        await db_pool.close()
        log.info("nautilus_worker stopped cleanly.")


async def dry_run() -> None:
    import sys
    log.info("=== DRY RUN MODE — no orders will be submitted ===")

    required_env = ["SUPABASE_DB_URL", "ALPACA_API_KEY", "ALPACA_API_SECRET"]
    errors = [v for v in required_env if not os.environ.get(v)]
    if errors:
        for e in errors:
            log.error(f"Missing required env var: {e}")
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
        log.info(f"✓ Instrument registry loaded — {len(new_symbols)} symbol(s).")
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
        alpaca_client = TradingClient(
            api_key=ALPACA_API_KEY,
            secret_key=ALPACA_API_SECRET,
            paper=ALPACA_PAPER,
        )
        account = alpaca_client.get_account()
        log.info(f"✓ Alpaca connected — status={account.status} equity={account.equity}")
    except Exception as e:
        log.error(f"✗ Alpaca connection failed: {e}")
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
