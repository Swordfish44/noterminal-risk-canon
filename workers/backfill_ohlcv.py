import os
import time
import asyncio
import logging
import aiohttp
import asyncpg
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

PG_CONN = os.getenv("PG_CONN")
if not PG_CONN:
    raise RuntimeError("PG_CONN missing from .env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================
BASE_URL = "https://api.binance.us/api/v3/klines"
INTERVAL = "1m"
LIMIT    = 1000  # max per request

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "DOGEUSDT",
    "DOTUSDT",
    "MATICUSDT",
]

# ============================================================
# FETCH
# ============================================================
async def fetch_klines(session, symbol, start_ts):
    params = {
        "symbol":    symbol,
        "interval":  INTERVAL,
        "startTime": start_ts,
        "limit":     LIMIT,
    }
    async with session.get(BASE_URL, params=params) as resp:
        resp.raise_for_status()
        return await resp.json()

# ============================================================
# WRITE
# ============================================================
async def write_klines(pool, symbol, klines):
    rows = [
        (
            symbol,
            datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
            float(k[1]),  # open
            float(k[2]),  # high
            float(k[3]),  # low
            float(k[4]),  # close
            float(k[5]),  # volume
        )
        for k in klines
    ]
    await pool.executemany(
        """
        INSERT INTO market.ohlcv_v1 (symbol, open_ts, open, high, low, close, volume)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (symbol, open_ts) DO NOTHING
        """,
        rows,
    )
    return len(rows)

# ============================================================
# BACKFILL ONE SYMBOL
# ============================================================
async def backfill_symbol(session, pool, symbol):
    log.info(f"Starting backfill for {symbol}")

    # Start from 3 years ago
    start_ts = int(datetime(2022, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    now_ts   = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    total    = 0

    while start_ts < now_ts:
        try:
            klines = await fetch_klines(session, symbol, start_ts)
            if not klines:
                break

            written = await write_klines(pool, symbol, klines)
            total  += written

            last_ts  = klines[-1][0]
            start_ts = last_ts + 60000  # next minute

            log.info(f"{symbol} — wrote {written} candles | total: {total} | up to {datetime.fromtimestamp(last_ts/1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')}")

            # Be nice to the API
            await asyncio.sleep(0.2)

        except Exception as e:
            log.error(f"{symbol} error: {e} — retrying in 5s")
            await asyncio.sleep(5)

    log.info(f"{symbol} DONE — total candles: {total}")

# ============================================================
# MAIN
# ============================================================
async def main():
    log.info("Connecting to DB...")
    pool = await asyncpg.create_pool(
        dsn=PG_CONN,
        min_size=1,
        max_size=5,
        statement_cache_size=0,
    )
    log.info("DB ready. Starting backfill...")

    async with aiohttp.ClientSession() as session:
        for symbol in SYMBOLS:
            await backfill_symbol(session, pool, symbol)

    await pool.close()
    log.info("Backfill complete!")

if __name__ == "__main__":
    asyncio.run(main())