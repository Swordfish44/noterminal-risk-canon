import os
import asyncio
import logging
import socket
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID

import asyncpg
from dotenv import load_dotenv

# ============================================================
# LOAD .env FROM PROJECT ROOT
# ============================================================
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

PG_CONN = os.getenv("PG_CONN")

print("RUNNING FILE:", __file__)

if not PG_CONN:
    raise RuntimeError("PG_CONN missing from .env")

_DB_PASSWORD = (
    urlparse(PG_CONN).password
    or os.environ.get("DB_PASSWORD")
    or os.environ.get("PG_PASSWORD")
)

# ============================================================
# CONFIG
# ============================================================
SYNC_INTERVAL = 10  # seconds between sync cycles

# Maps market.ticks_v1.symbol_id → market.symbols_v1.id (= market.ticks_raw_v1.symbol_id)
#
# ticks_v1 is keyed by CoinCap sentinel UUIDs (written by ops_worker).
# ticks_raw_v1 is keyed by market.symbols_v1.id — the canonical namespace.
# The view chain reads from ticks_raw_v1:
#   ticks_raw_v1 → last_price_v1 (DISTINCT ON symbol_id ORDER BY event_ts DESC)
#                → position_market_value_v1 (positions JOIN last_price_v1)
TICK_TO_SYMBOL: dict[UUID, UUID] = {
    UUID("22222222-2222-2222-2222-222222222222"): UUID("d85b4396-20a5-4f47-91fa-d83b802734b5"),  # BTC/USD
    UUID("33333333-3333-3333-3333-333333333333"): UUID("60f3954d-6fbf-427f-8670-e666c873b2e5"),  # ETH/USD
    UUID("44444444-4444-4444-4444-444444444444"): UUID("37c9a4dc-438e-4366-8e73-35460f21bec8"),  # SOL/USD
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ============================================================
# WORKER
# ============================================================
class PortfolioSyncWorker:
    def __init__(self):
        self.pool = None

    # ---------------- DB ----------------
    async def connect_db(self):
        log.info("Connecting to DB...")
        _ipv4 = socket.getaddrinfo(
            "aws-0-us-east-2.pooler.supabase.com", 6543, socket.AF_INET
        )[0][4][0]
        self.pool = await asyncpg.create_pool(
            host=_ipv4,
            port=6543,
            user=urlparse(os.environ.get("PG_CONN", "")).username or "postgres.dtcmofpvixbkpwqvecid",
            password=_DB_PASSWORD,
            database="postgres",
            ssl="require",
            min_size=1,
            max_size=5,
            statement_cache_size=0,
        )
        log.info("DB ready.")

    # ---------------- READ ----------------
    async def load_ticks(self) -> list:
        """Read latest tick per symbol from market.ticks_v1 (ops_worker feed)."""
        return await self.pool.fetch(
            "SELECT symbol_id, event_ts, last_price, last_size FROM market.ticks_v1"
        )

    # ---------------- WRITE ----------------
async def forward_to_raw(self, ticks: list) -> int:
    inserted = 0
    for tick in ticks:
        canonical = TICK_TO_SYMBOL.get(tick["symbol_id"])
        if canonical is None:
            log.warning("No symbol mapping for tick symbol_id=%s", tick["symbol_id"])
            continue

        result = await self.pool.execute(
            """
            INSERT INTO market.ticks_raw_v1
                (symbol_id, event_ts, last_price, last_size,
                 side, is_maker, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, now())
            ON CONFLICT (symbol_id, event_ts) DO UPDATE
            SET
                side     = EXCLUDED.side,
                is_maker = EXCLUDED.is_maker
            """,
            canonical,
            tick["event_ts"],
            tick["last_price"],
            tick["last_size"],
            tick.get("side"),      # None for now — ops_worker doesn't pass it yet
            tick.get("is_maker"),  # None for now
        )
        n = int(result.split()[-1])
        inserted += n

    return inserted

    # ---------------- LOG PORTFOLIO STATE ----------------
    async def log_portfolio(self):
        """Read position_market_value_v1 view and emit one log line per position."""
        rows = await self.pool.fetch("SELECT * FROM portfolio.position_market_value_v1")
        if not rows:
            log.info("portfolio.position_market_value_v1 — no rows")
            return
        for r in rows:
            log.info(
                "fund=%.8s sym=%.8s | qty=%s price=%s mv=%s upnl=%s total_pnl=%s",
                r["fund_id"],
                r["symbol_id"],
                r["qty"],
                r["last_price"],
                round(r["market_value"], 2),
                round(r["unrealized_pnl"], 2),
                round(r["total_pnl"], 2),
            )

    # ---------------- CYCLE ----------------
    async def run_cycle(self):
        ticks = await self.load_ticks()
        if not ticks:
            log.info("No ticks available.")
            return

        inserted = await self.forward_to_raw(ticks)
        log.info("Forwarded %d tick(s) → ticks_raw_v1 (%d new row(s))", len(ticks), inserted)
        await self.log_portfolio()

    # ---------------- RUNNER ----------------
    async def sync_loop(self):
        log.info("Sync loop started (interval=%ss)", SYNC_INTERVAL)
        while True:
            try:
                await self.run_cycle()
            except Exception as e:
                log.error("Cycle error: %s", e)
            await asyncio.sleep(SYNC_INTERVAL)

    async def run(self):
        await self.connect_db()
        log.info("PORTFOLIO SYNC WORKER STARTED")
        await self.sync_loop()

# ============================================================
# MAIN
# ============================================================
async def main():
    worker = PortfolioSyncWorker()
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
