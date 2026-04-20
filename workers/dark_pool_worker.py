"""
dark_pool_worker.py

Scans market.ticks_raw_v1 for large block trades whose size is statistically
anomalous relative to the trailing rolling window and flags them into
market.dark_pool_trades_v1.

A trade is flagged when BOTH conditions are met:
  1. size z-score >= DP_ZSCORE_THRESHOLD  (against trailing LOOKBACK_S window)
  2. notional_usd >= DP_MIN_NOTIONAL_USD

Run:
  python workers/dark_pool_worker.py

Env (required):
  PG_CONN=postgresql://...

Env (optional, with defaults):
  DP_LOOKBACK_S=1800        rolling window for stats in seconds (default 30 min)
  DP_ZSCORE_THRESHOLD=2.5   z-score cutoff
  DP_MIN_NOTIONAL_USD=1000  minimum notional value in USD to flag
  DP_LOOP_INTERVAL_S=5      polling cadence in seconds
"""

import asyncio
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

PG_CONN = os.getenv("PG_CONN") or os.getenv("DATABASE_URL")
if not PG_CONN:
    raise RuntimeError("Neither PG_CONN nor DATABASE_URL is set")

LOOKBACK_S       = int(os.getenv("DP_LOOKBACK_S",        "1800"))
ZSCORE_THRESHOLD = float(os.getenv("DP_ZSCORE_THRESHOLD", "2.5"))
MIN_NOTIONAL_USD = float(os.getenv("DP_MIN_NOTIONAL_USD", "1000"))
LOOP_INTERVAL_S  = float(os.getenv("DP_LOOP_INTERVAL_S",  "5"))

# Canonical market.symbols_v1 UUIDs → human-readable symbol name
CANONICAL_SYMBOLS: dict[UUID, str] = {
    UUID("d85b4396-20a5-4f47-91fa-d83b802734b5"): "BTC/USD",
    UUID("60f3954d-6fbf-427f-8670-e666c873b2e5"): "ETH/USD",
    UUID("37c9a4dc-438e-4366-8e73-35460f21bec8"): "SOL/USD",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("dark_pool_worker")


# ─────────────────────────── SQL ─────────────────────────────────────────────

FETCH_SQL = """
SELECT symbol_id, event_ts, last_price, last_size, side
FROM market.ticks_raw_v1
WHERE symbol_id = ANY($1::uuid[])
  AND event_ts >= $2
ORDER BY symbol_id, event_ts
"""

INSERT_SQL = """
INSERT INTO market.dark_pool_trades_v1
    (dark_pool_trade_id, trade_ts, symbol, price, size,
     notional_usd, side, vwap, meta, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, now())
ON CONFLICT (symbol, trade_ts, price, size) DO NOTHING
"""


# ─────────────────────────── Stats helpers ───────────────────────────────────

def _rolling_stats(sizes: list[float], prices: list[float]) -> dict:
    """Mean, population stddev, VWAP, and count for a list of trades."""
    n = len(sizes)
    if n == 0:
        return {"mean": 0.0, "std": 0.0, "vwap": None, "count": 0}
    mean = sum(sizes) / n
    std = math.sqrt(sum((s - mean) ** 2 for s in sizes) / n) if n > 1 else 0.0
    total_vol = sum(sizes)
    vwap = (
        sum(p * s for p, s in zip(prices, sizes)) / total_vol
        if total_vol > 0
        else None
    )
    return {"mean": mean, "std": std, "vwap": vwap, "count": n}


# ─────────────────────────── Core loop ───────────────────────────────────────

async def run_cycle(
    pool: asyncpg.Pool,
    watermarks: dict[UUID, datetime],
) -> tuple[int, dict[UUID, datetime]]:
    """
    Fetch trades since the watermark, compute per-trade rolling stats,
    and insert block-trade candidates.  Returns (flagged_count, new_watermarks).
    """
    symbol_ids = list(CANONICAL_SYMBOLS.keys())
    lookback_start = datetime.now(timezone.utc) - timedelta(seconds=LOOKBACK_S)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            FETCH_SQL,
            [str(sid) for sid in symbol_ids],
            lookback_start,
        )

    if not rows:
        return 0, watermarks

    # Group rows by symbol_id
    by_symbol: dict[UUID, list] = {sid: [] for sid in symbol_ids}
    for r in rows:
        sid = r["symbol_id"]  # asyncpg returns uuid.UUID natively
        if sid in by_symbol:
            by_symbol[sid].append(r)

    flagged: list[tuple] = []
    new_watermarks = dict(watermarks)

    for sid, trades in by_symbol.items():
        if not trades:
            continue

        sym_name = CANONICAL_SYMBOLS[sid]
        wm = watermarks.get(sid, lookback_start)

        new_trades = [t for t in trades if t["event_ts"] > wm]
        if not new_trades:
            continue

        new_watermarks[sid] = new_trades[-1]["event_ts"]

        for trade in new_trades:
            trade_ts: datetime = trade["event_ts"]
            price = float(trade["last_price"])
            size = float(trade["last_size"])
            side = trade["side"]
            notional = price * size

            if notional < MIN_NOTIONAL_USD:
                continue

            # Trailing window: all trades *before* this trade within LOOKBACK_S
            window_start = trade_ts - timedelta(seconds=LOOKBACK_S)
            window = [
                t for t in trades
                if window_start <= t["event_ts"] < trade_ts
            ]

            w_sizes  = [float(t["last_size"])  for t in window]
            w_prices = [float(t["last_price"]) for t in window]
            stats = _rolling_stats(w_sizes, w_prices)

            z_score = (
                (size - stats["mean"]) / stats["std"]
                if stats["std"] > 0
                else 0.0
            )

            if z_score < ZSCORE_THRESHOLD:
                continue

            vwap = stats["vwap"]
            meta = {
                "z_score":             round(z_score, 4),
                "rolling_mean_size":   round(stats["mean"], 8),
                "rolling_std_size":    round(stats["std"],  8),
                "rolling_trade_count": stats["count"],
            }

            flagged.append((
                uuid4(),
                trade_ts,
                sym_name,
                Decimal(str(price)),
                Decimal(str(size)),
                Decimal(str(round(notional, 4))),
                side,
                Decimal(str(round(vwap, 8))) if vwap is not None else None,
                json.dumps(meta),
            ))

            log.info(
                "BLOCK TRADE  %-9s  price=%.2f  size=%.6f  notional=$%.0f  z=%.2f  side=%s",
                sym_name, price, size, notional, z_score, side,
            )

    if flagged:
        async with pool.acquire() as conn:
            await conn.executemany(INSERT_SQL, flagged)

    return len(flagged), new_watermarks


async def main() -> None:
    pool = await asyncpg.create_pool(
        PG_CONN,
        min_size=1,
        max_size=3,
        statement_cache_size=0,  # required for Supabase
        command_timeout=30,
    )
    log.info(
        "Dark pool worker online — lookback_s=%d  zscore>=%.1f  min_notional=$%.0f  interval_s=%.1f",
        LOOKBACK_S, ZSCORE_THRESHOLD, MIN_NOTIONAL_USD, LOOP_INTERVAL_S,
    )

    watermarks: dict[UUID, datetime] = {}

    try:
        while True:
            t0 = time.perf_counter()
            try:
                n, watermarks = await run_cycle(pool, watermarks)
                elapsed_ms = (time.perf_counter() - t0) * 1000
                if n:
                    log.info("Flagged %d block trade(s) (%.0f ms)", n, elapsed_ms)
                else:
                    log.debug("No block trades flagged (%.0f ms)", elapsed_ms)
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
