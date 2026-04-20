"""
deploy_regime_pipeline.py

Creates / replaces the regime pipeline functions and schedules the cron job.

  python sql/deploy_regime_pipeline.py

Env (required):
  PG_CONN=postgresql://...
"""

import asyncio
import logging
import os
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")

PG_CONN = os.getenv("PG_CONN")
if not PG_CONN:
    raise RuntimeError("PG_CONN is not set")

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).parent


async def main() -> None:
    conn = await asyncpg.connect(PG_CONN, statement_cache_size=0)

    # ── 1. Create functions ────────────────────────────────────────────────
    for fname in [
        "fn_compute_regime_pressure_fixed.sql",  # fixes text[]||literal bug
        "fn_detect_market_shock_fixed.sql",       # fixes text[]||literal bug
        "fn_build_inference_from_features.sql",
        "fn_run_opportunity_cycle.sql",
    ]:
        sql = (SQL_DIR / fname).read_text()
        await conn.execute(sql)
        log.info("Created/replaced regime.%s", fname.removesuffix(".sql"))

    # ── 2. Schedule cron (idempotent: unschedule first if exists) ─────────
    existing = await conn.fetchval(
        "SELECT jobid FROM cron.job WHERE jobname = 'regime_opportunity_cycle'"
    )
    if existing:
        log.info("Removing existing cron job (jobid=%s)", existing)
        await conn.execute("SELECT cron.unschedule('regime_opportunity_cycle')")

    job_id = await conn.fetchval(
        "SELECT cron.schedule($1, $2, $3)",
        "regime_opportunity_cycle",
        "*/5 * * * *",
        "SELECT regime.fn_run_opportunity_cycle()",
    )
    log.info("Cron job scheduled: jobid=%s  every 5 minutes", job_id)

    # ── 3. Smoke test ──────────────────────────────────────────────────────
    log.info("Running smoke test...")
    inference_id = await conn.fetchval("SELECT regime.fn_run_opportunity_cycle()")

    if inference_id is None:
        log.warning("Smoke test: fn_run_opportunity_cycle returned NULL (insufficient data?)")
    else:
        inf = await conn.fetchrow(
            "SELECT * FROM regime.inference_run_v1 WHERE inference_run_id = $1",
            inference_id,
        )
        log.info("Inference row written: %s", dict(inf))

        sigs = await conn.fetch(
            """
            SELECT os.opportunity_type, os.direction, os.signal_score, os.thesis
            FROM regime.opportunity_signal_v1 os
            JOIN regime.opportunity_run_v1 r
              ON r.opportunity_run_id = os.opportunity_run_id
            WHERE r.inference_run_id = $1
            """,
            inference_id,
        )
        log.info("Opportunity signals (%d):", len(sigs))
        for s in sigs:
            log.info(
                "  %s %s  score=%.3f  %s",
                s["opportunity_type"],
                s["direction"],
                float(s["signal_score"]),
                s["thesis"],
            )

    await conn.close()
    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())
