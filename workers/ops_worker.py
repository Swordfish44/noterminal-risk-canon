import os
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal

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
COINCAP_WS_URL = "wss://ws.coincap.io/prices?assets=bitcoin,ethereum,solana"
BTCUSDT_ASSET_ID = "22222222-2222-2222-2222-222222222222"

STARTING_CASH  = 100000
FLUSH_INTERVAL = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============================================================
# WORKER
# ============================================================
class OpsWorker:
    def __init__(self):
        self.pool       = None
        self.last_price = None
        self.last_size  = None
        self.last_ts    = None
        self.cash       = STARTING_CASH

    # ---------------- DB ----------------
    async def connect_db(self):
        logging.info("Connecting to DB...")
        self.pool = await asyncpg.create_pool(
            dsn=PG_CONN,
            min_size=1,
            max_size=3,
            statement_cache_size=0
        )
        logging.info("DB ready.")

    async def write_tick(self):
        if self.last_price is None:
            return

        try:
            await self.pool.execute(
                """
                INSERT INTO market.ticks_v1
                (symbol_id, event_ts, last_price, last_size, created_at)
                VALUES ($1, $2, $3, $4, now())
                """,
                BTCUSDT_ASSET_ID,
                self.last_ts,
                self.last_price,
                self.last_size,
            )

        except Exception:
            # UPSERT FALLBACK
            await self.pool.execute(
                """
                UPDATE market.ticks_v1
                SET last_price = $2,
                    last_size  = $3,
                    event_ts   = $4,
                    created_at = now()
                WHERE symbol_id = $1
                  AND $4 > event_ts
                """,
                BTCUSDT_ASSET_ID,
                self.last_price,
                self.last_size,
                self.last_ts,
            )

    async def flusher(self):
        while True:
            await asyncio.sleep(FLUSH_INTERVAL)
            try:
                await self.write_tick()
            except Exception as e:
                logging.error(f"DB WRITE FAILED: {e}")

    # ---------------- MARKET DATA ----------------
    async def market_loop(self):
        logging.info("Connecting to CoinCap...")

        while True:
            try:
                async with websockets.connect(
                    COINCAP_WS_URL,
                    ping_interval=20,
                    ping_timeout=20
                ) as ws:

                    logging.info("CoinCap LIVE")

                    async for msg in ws:
                        data = json.loads(msg)

                        if "bitcoin" in data:
                            self.last_price = Decimal(data["bitcoin"])
                            self.last_size  = Decimal("0")
                            self.last_ts    = datetime.now(timezone.utc)

            except Exception as e:
                logging.error(f"WS ERROR → reconnecting in 3s: {e}")
                await asyncio.sleep(3)

    # ---------------- RUNNER ----------------
    async def run(self):
        await self.connect_db()
        logging.info("OPS WORKER STARTED")

        await asyncio.gather(
            self.market_loop(),
            self.flusher(),
        )

# ============================================================
# MAIN
# ============================================================
async def main():
    worker = OpsWorker()
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
