import os
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID
print("WORKER VERSION: side/is_maker ACTIVE", flush=True)
import asyncpg
import websockets
from dotenv import load_dotenv

# ============================================================
# LOAD .env FROM PROJECT ROOT
# ============================================================
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

FUND_ID = os.getenv("FUND_ID")
PG_CONN = os.getenv("PG_CONN")

print("RUNNING FILE:", __file__)
print("FUND_ID AT RUNTIME:", FUND_ID)

if not FUND_ID:
    raise RuntimeError("FUND_ID missing from .env")
if not PG_CONN:
    raise RuntimeError("PG_CONN missing from .env")

# ============================================================
# CONFIG
# ============================================================
KRAKEN_WS_URL = "wss://ws.kraken.com/v2"

# Kraken symbol → stable sentinel UUID written to market.ticks_v1
# These UUIDs are the Kraken feed namespace (not market.symbols_v1 UUIDs).
# portfolio_sync.py maintains TICK_TO_SYMBOL to bridge to the canonical namespace.
KRAKEN_TO_SYMBOL_ID: dict[str, UUID] = {
    "BTC/USD": UUID("22222222-2222-2222-2222-222222222222"),
    "ETH/USD": UUID("33333333-3333-3333-3333-333333333333"),
    "SOL/USD": UUID("44444444-4444-4444-4444-444444444444"),
}

# Kraken symbol → canonical market.symbols_v1 UUID (same mapping as portfolio_sync TICK_TO_SYMBOL).
# Used when writing individual trades directly to market.ticks_raw_v1 with side info.
KRAKEN_TO_CANONICAL: dict[str, UUID] = {
    "BTC/USD": UUID("d85b4396-20a5-4f47-91fa-d83b802734b5"),
    "ETH/USD": UUID("60f3954d-6fbf-427f-8670-e666c873b2e5"),
    "SOL/USD": UUID("37c9a4dc-438e-4366-8e73-35460f21bec8"),
}

FLUSH_INTERVAL    = 5   # seconds between DB flushes
STALE_TIMEOUT     = 30  # seconds without a price update before forcing reconnect
WATCHDOG_INTERVAL = 5   # seconds between staleness checks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ============================================================
# WORKER
# ============================================================
class OpsWorker:
    def __init__(self):
        self.pool = None
        # {kraken_symbol: (last_price, last_size, event_ts)}
        self.prices: dict[str, tuple] = {}
        # Individual trade events pending flush → ticks_raw_v1
        # Each entry: (canonical_symbol_id, event_ts, price, size, side)
        self.raw_trades: list[tuple] = []
        self._ws_reconnect = asyncio.Event()

    # ---------------- DB ----------------
    async def connect_db(self):
        logging.info("Connecting to DB...")
        self.pool = await asyncpg.create_pool(
            dsn=PG_CONN,
            min_size=1,
            max_size=3,
            statement_cache_size=0,
        )
        logging.info("DB ready.")

    # ---------------- WRITE ----------------
    async def write_ticks(self):
        if not self.prices:
            return
        for kraken_sym, (price, size, ts) in list(self.prices.items()):
            symbol_id = KRAKEN_TO_SYMBOL_ID[kraken_sym]
            try:
                await self.pool.execute(
                    """
                    INSERT INTO market.ticks_v1
                        (symbol_id, event_ts, last_price, last_size, created_at)
                    VALUES ($1, $2, $3, $4, now())
                    ON CONFLICT (symbol_id) DO UPDATE
                    SET last_price = EXCLUDED.last_price,
                        last_size  = EXCLUDED.last_size,
                        event_ts   = EXCLUDED.event_ts,
                        created_at = now()
                    WHERE EXCLUDED.event_ts > market.ticks_v1.event_ts
                    """,
                    str(symbol_id),
                    ts,
                    price,
                    size,
                )
            except Exception as e:
                logging.error("DB WRITE FAILED [%s]: %s", kraken_sym, e)

    async def write_raw_trades(self):
        if not self.raw_trades:
            return
        batch, self.raw_trades = self.raw_trades, []
        try:
            await self.pool.executemany(
                """
                INSERT INTO market.ticks_raw_v1
                    (symbol_id, event_ts, last_price, last_size, side, created_at)
                VALUES ($1, $2, $3, $4, $5, now())
                ON CONFLICT (symbol_id, event_ts) DO NOTHING
                """,
                [(str(cid), ts, price, size, side) for cid, ts, price, size, side in batch],
            )
            logging.debug("Raw trades flushed: %d rows", len(batch))
        except Exception as e:
            logging.error("RAW TRADE WRITE FAILED: %s", e)
            # Re-queue so data isn't lost on a transient error
            self.raw_trades = batch + self.raw_trades

    async def flusher(self):
        while True:
            await asyncio.sleep(FLUSH_INTERVAL)
            await self.write_raw_trades()
            await self.write_ticks()

    # ---------------- MARKET DATA ----------------
    async def _subscribe(self, ws):
        symbols = list(KRAKEN_TO_SYMBOL_ID.keys())
        for channel in ("ticker", "trade"):
            await ws.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": channel, "symbol": symbols},
            }))
        logging.info("Subscribed ticker+trade: %s", symbols)

    def _handle_message(self, data: dict):
        channel = data.get("channel")
        if channel not in ("ticker", "trade"):
            return
        if data.get("type") not in ("snapshot", "update"):
            return
        for item in data.get("data", []):
            sym = item.get("symbol")
            if sym not in KRAKEN_TO_SYMBOL_ID:
                continue
            ts_raw = item.get("timestamp")
            event_ts = (
                datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                if ts_raw
                else datetime.now(timezone.utc)
            )
            if channel == "trade":
                price = item.get("price")
                size  = item.get("qty")
                side  = item.get("side")  # "buy" or "sell" from Kraken v2
                if price is None:
                    continue
                dec_price = Decimal(str(price))
                dec_size  = Decimal(str(size)) if size is not None else Decimal("0")
                self.prices[sym] = (dec_price, dec_size, event_ts)
                canonical = KRAKEN_TO_CANONICAL.get(sym)
                if canonical is not None:
                    self.raw_trades.append((canonical, event_ts, dec_price, dec_size, side, side == "sell"))
                logging.info("TRADE %-9s  %s  qty=%s  side=%s", sym, price, size, side)
            else:  # ticker — fallback / fill-in
                price = item.get("last")
                if price is None:
                    continue
                # Only update if we have no trade data yet, or ticker is newer.
                # Preserve existing last_size — ticker messages carry no trade qty.
                existing = self.prices.get(sym)
                if existing is None or event_ts > existing[2]:
                    existing_size = existing[1] if existing is not None else Decimal("0")
                    self.prices[sym] = (Decimal(str(price)), existing_size, event_ts)
                    logging.info("TICK  %-9s  %s", sym, price)

    async def market_loop(self):
        logging.info("Connecting to Kraken...")
        while True:
            self._ws_reconnect.clear()
            try:
                async with websockets.connect(
                    KRAKEN_WS_URL,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    await self._subscribe(ws)
                    logging.info("Kraken LIVE")

                    async for msg in ws:
                        if self._ws_reconnect.is_set():
                            logging.warning("Reconnect signaled — cycling connection")
                            break
                        self._handle_message(json.loads(msg))

            except Exception as e:
                logging.error("WS ERROR → reconnecting in 3s: %s", e)
            await asyncio.sleep(3)

    # ---------------- STALENESS WATCHDOG ----------------
    async def staleness_watchdog(self):
        """
        Runs every WATCHDOG_INTERVAL seconds after an initial grace period.
        Sets _ws_reconnect if any subscribed symbol has not received a price
        update within STALE_TIMEOUT seconds.
        """
        await asyncio.sleep(STALE_TIMEOUT)  # allow time for initial snapshot
        while True:
            now = datetime.now(timezone.utc)
            for sym in KRAKEN_TO_SYMBOL_ID:
                state = self.prices.get(sym)
                if state is None:
                    age = float("inf")
                else:
                    _, _, event_ts = state
                    age = (now - event_ts).total_seconds()
                if age > STALE_TIMEOUT:
                    logging.warning(
                        "STALE  %-9s  %.0fs — signaling reconnect", sym, age
                    )
                    self._ws_reconnect.set()
                    break
            await asyncio.sleep(WATCHDOG_INTERVAL)

    # ---------------- RUNNER ----------------
    async def run(self):
        await self.connect_db()
        logging.info("OPS WORKER STARTED")
        await asyncio.gather(
            self.market_loop(),
            self.flusher(),
            self.staleness_watchdog(),
        )

# ============================================================
# MAIN
# ============================================================
async def main():
    worker = OpsWorker()
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
