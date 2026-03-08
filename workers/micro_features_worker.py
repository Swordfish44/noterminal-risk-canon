"""
micro_features_worker.py

Computes 1-second micro-structure features from market.ticks_raw_v1
and upserts into market.micro_features_1s_v1.

Features per (bucket_ts, symbol_id):
  bucket_ts       : DATE_TRUNC('second', event_ts)
  symbol_id       : from ticks_raw_v1
  trade_count     : COUNT(*)
  trade_intensity : trade_count per second (= trade_count for 1s buckets)
  micro_vol       : STDDEV(last_price) within the bucket
  ofi             : order flow imbalance — tick-rule approximation:
                    SUM(last_size * SIGN(price - prev_price))
  avg_trade_size  : AVG(last_size)
  mid             : AVG(last_price)  ← was missing; now fixed

Only processes buckets in the trailing LOOKBACK_S window to bound the query.
Completed seconds older than LOOKBACK_S are never re-touched (idempotent).

Run:
  python workers/micro_features_worker.py

Env (required):
  PG_CONN=postgresql://...

Env (optional, with defaults):
  MICRO_LOOKBACK_S=120     lookback window for feature computation (seconds)
  MICRO_LOOP_INTERVAL_S=2  cadence between compute cycles
"""

import asyncio
import logging
import os
import time
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

PG_CONN = os.getenv("PG_CONN")
if not PG_CONN:
    raise RuntimeError("PG_CONN is not set")

LOOKBACK_S       = int(os.getenv("MICRO_LOOKBACK_S",       "120"))
LOOP_INTERVAL_S  = float(os.getenv("MICRO_LOOP_INTERVAL_S", "2"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("micro_features_worker")


# ─────────────────────────── SQL ─────────────────────────────────────────────

# Compute 1s bucket aggregates from ticks_raw_v1.
#
# OFI via tick rule: each trade is classified as buyer-initiated (+size)
# if the price is above the previous trade price, seller-initiated (-size)
# if below, and zero if unchanged.  SIGN(0) = 0, so ties are neutral.
#
# mid = AVG(last_price) — the average trade price across all trades that
# occurred within the 1-second bucket, per symbol.
COMPUTE_SQL = """
WITH ranked AS (
    SELECT
        symbol_id,
        event_ts,
        last_price,
        last_size,
        DATE_TRUNC('second', event_ts)                                  AS bucket_ts,
        last_price
            - LAG(last_price) OVER (
                PARTITION BY symbol_id
                ORDER BY event_ts
              )                                                          AS price_diff
    FROM market.ticks_raw_v1
    WHERE event_ts >= NOW() - ($1::int * INTERVAL '1 second')
      AND event_ts <  DATE_TRUNC('second', NOW())   -- exclude the live, incomplete second
)
SELECT
    bucket_ts,
    symbol_id,
    COUNT(*)::int                                                        AS trade_count,
    COUNT(*)::float8                                                     AS trade_intensity,
    STDDEV(last_price)::float8                                           AS micro_vol,
    SUM(last_size * SIGN(COALESCE(price_diff, 0)))::float8               AS ofi,
    AVG(last_size)::float8                                               AS avg_trade_size,
    AVG(last_price)                                                      AS mid,
    SUM(CASE WHEN side = 'buy'  THEN last_size ELSE 0 END)::float8      AS buy_vol,
    SUM(CASE WHEN side = 'sell' THEN last_size ELSE 0 END)::float8      AS sell_vol
FROM ranked
GROUP BY bucket_ts, symbol_id
ORDER BY bucket_ts, symbol_id
"""

# UPSERT — assumes PK on (bucket_ts, symbol_id).
# All columns are refreshed so a re-run over the same window is idempotent.
UPSERT_SQL = """
INSERT INTO market.micro_features_1s_v1 (
    bucket_ts,
    symbol_id,
    trade_count,
    trade_intensity,
    micro_vol,
    ofi,
    avg_trade_size,
    mid,
    buy_vol,
    sell_vol
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (bucket_ts, symbol_id) DO UPDATE SET
    trade_count     = EXCLUDED.trade_count,
    trade_intensity = EXCLUDED.trade_intensity,
    micro_vol       = EXCLUDED.micro_vol,
    ofi             = EXCLUDED.ofi,
    avg_trade_size  = EXCLUDED.avg_trade_size,
    mid             = EXCLUDED.mid,
    buy_vol         = EXCLUDED.buy_vol,
    sell_vol        = EXCLUDED.sell_vol
"""


# ─────────────────────────── Core loop ───────────────────────────────────────

async def run_cycle(pool: asyncpg.Pool) -> int:
    """Compute features and upsert. Returns number of rows written."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(COMPUTE_SQL, LOOKBACK_S)
        if not rows:
            return 0

        records = [
            (
                r["bucket_ts"],
                r["symbol_id"],
                r["trade_count"],
                r["trade_intensity"],
                r["micro_vol"],
                r["ofi"],
                r["avg_trade_size"],
                r["mid"],
                r["buy_vol"],
                r["sell_vol"],
            )
            for r in rows
        ]

        await conn.executemany(UPSERT_SQL, records)
        return len(records)


async def main() -> None:
    pool = await asyncpg.create_pool(
        PG_CONN,
        min_size=1,
        max_size=3,
        statement_cache_size=0,  # required for Supabase
        command_timeout=30,
    )
    log.info(
        "Micro-features worker online. lookback_s=%d interval_s=%.1f",
        LOOKBACK_S, LOOP_INTERVAL_S,
    )

    try:
        while True:
            t0 = time.perf_counter()
            try:
                n = await run_cycle(pool)
                elapsed_ms = (time.perf_counter() - t0) * 1000
                if n:
                    log.info("Upserted %d feature rows (%.0f ms)", n, elapsed_ms)
                else:
                    log.debug("No ticks in window, nothing to write (%.0f ms)", elapsed_ms)
            except Exception:
                log.exception("Cycle error — continuing")

            elapsed = time.perf_counter() - t0
            await asyncio.sleep(max(0.0, LOOP_INTERVAL_S - elapsed))

    finally:
        await pool.close()
        log.info("Pool closed. Worker stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
