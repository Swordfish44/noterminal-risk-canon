"""
earnings_accuracy_worker.py
─────────────────────────────────────────────────────────────────────────────
Noterminal — Earnings Accuracy Scoring Worker

Runs daily. Checks for:
  1. New entries in market.earnings_actuals_v1
  2. Active predictions for that symbol+quarter → marks them RESOLVED
  3. All ACTIVE/SUPERSEDED predictions for that quarter → scores each one
  4. Writes to market.earnings_model_accuracy_v1
  5. Updates v_earnings_model_scorecard_v1 (view auto-refreshes)

This is the self-improvement loop. Every scored prediction teaches the
model where it was wrong — direction, magnitude, timing. The scorecard
view surfaces MAPE and direction accuracy in real time.

Canon compliance:
  - Restart loop in start.sh (non-critical worker)
  - asyncpg → Transaction Pooler port 6543
  - Idempotent on (prediction_id, actual_id)
  - Provenance via ingest_run_log_v1
"""

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [earnings_accuracy_worker] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

DB_URL             = os.environ["DATABASE_URL"]
WORKER_VERSION     = "earnings_accuracy_worker_v1"
POLL_INTERVAL_SECS = 86400     # daily

# Beat/miss classification thresholds (vs consensus)
BEAT_THRESHOLD      =  2.0     # >+2% vs consensus = BEAT
STRONG_BEAT_THRESHOLD = 5.0    # >+5% = STRONG_BEAT
MISS_THRESHOLD      = -2.0     # <-2% = MISS
STRONG_MISS_THRESHOLD = -5.0   # <-5% = STRONG_MISS

# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class ActualRow:
    actual_id:              uuid.UUID
    symbol:                 str
    fiscal_quarter:         str
    report_date:            date
    actual_revenue_usd:     float
    actual_eps:             Optional[float]

@dataclass
class PredictionRow:
    prediction_id:          uuid.UUID
    nowcast_id:             uuid.UUID
    symbol:                 str
    fiscal_quarter:         str
    prediction:             str
    predicted_revenue_usd:  Optional[float]
    surprise_pct:           Optional[float]
    confidence_score:       Optional[float]
    status:                 str
    created_at:             date

# ─── Helpers ──────────────────────────────────────────────────────────────────

def classify_direction(vs_consensus_pct: float) -> str:
    if vs_consensus_pct >= STRONG_BEAT_THRESHOLD:
        return "STRONG_BEAT"
    if vs_consensus_pct >= BEAT_THRESHOLD:
        return "BEAT"
    if vs_consensus_pct <= STRONG_MISS_THRESHOLD:
        return "STRONG_MISS"
    if vs_consensus_pct <= MISS_THRESHOLD:
        return "MISS"
    return "IN_LINE"

def mape(predicted: float, actual: float) -> float:
    if actual == 0:
        return 0.0
    return round(abs((actual - predicted) / actual) * 100, 4)

# ─── DB reads ─────────────────────────────────────────────────────────────────

async def fetch_unscored_actuals(conn) -> list[ActualRow]:
    """
    Actuals that have at least one prediction not yet scored.
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT
            a.actual_id, a.symbol, a.fiscal_quarter,
            a.report_date, a.actual_revenue_usd, a.actual_eps
        FROM market.earnings_actuals_v1 a
        JOIN market.earnings_predictions_v1 p
            ON p.symbol = a.symbol AND p.fiscal_quarter = a.fiscal_quarter
        WHERE NOT EXISTS (
            SELECT 1 FROM market.earnings_model_accuracy_v1 acc
            WHERE acc.prediction_id = p.prediction_id
              AND acc.actual_id     = a.actual_id
        )
        ORDER BY a.report_date
        """
    )
    return [
        ActualRow(
            actual_id=r["actual_id"],
            symbol=r["symbol"],
            fiscal_quarter=r["fiscal_quarter"],
            report_date=r["report_date"],
            actual_revenue_usd=float(r["actual_revenue_usd"]),
            actual_eps=float(r["actual_eps"]) if r["actual_eps"] else None,
        )
        for r in rows
    ]

async def fetch_predictions_for_quarter(conn, symbol: str,
                                         fiscal_quarter: str) -> list[PredictionRow]:
    """All predictions (any status) for a symbol+quarter."""
    rows = await conn.fetch(
        """
        SELECT
            prediction_id, nowcast_id, symbol, fiscal_quarter,
            prediction, predicted_revenue_usd, surprise_pct,
            confidence_score, status, created_at::date AS created_at
        FROM market.earnings_predictions_v1
        WHERE symbol         = $1
          AND fiscal_quarter = $2
        ORDER BY created_at
        """,
        symbol, fiscal_quarter,
    )
    return [
        PredictionRow(
            prediction_id=r["prediction_id"],
            nowcast_id=r["nowcast_id"],
            symbol=r["symbol"],
            fiscal_quarter=r["fiscal_quarter"],
            prediction=r["prediction"],
            predicted_revenue_usd=float(r["predicted_revenue_usd"])
                if r["predicted_revenue_usd"] else None,
            surprise_pct=float(r["surprise_pct"]) if r["surprise_pct"] else None,
            confidence_score=float(r["confidence_score"])
                if r["confidence_score"] else None,
            status=r["status"],
            created_at=r["created_at"],
        )
        for r in rows
    ]

async def fetch_consensus_revenue(conn, symbol: str,
                                   fiscal_quarter: str) -> Optional[float]:
    row = await conn.fetchrow(
        """
        SELECT consensus_revenue_usd
        FROM market.earnings_consensus_v1
        WHERE symbol = $1 AND fiscal_quarter = $2
        ORDER BY as_of_date DESC LIMIT 1
        """,
        symbol, fiscal_quarter,
    )
    return float(row["consensus_revenue_usd"]) if row and row["consensus_revenue_usd"] else None

async def fetch_nowcast_panel_meta(conn, nowcast_id: uuid.UUID) -> dict:
    row = await conn.fetchrow(
        """
        SELECT panel_weeks_used, panel_coverage_score
        FROM market.earnings_nowcast_runs_v1
        WHERE nowcast_id = $1
        """,
        nowcast_id,
    )
    if not row:
        return {"panel_weeks_used": None, "panel_coverage_score": None}
    return {
        "panel_weeks_used": row["panel_weeks_used"],
        "panel_coverage_score": float(row["panel_coverage_score"])
            if row["panel_coverage_score"] else None,
    }

# ─── DB writes ────────────────────────────────────────────────────────────────

async def resolve_active_predictions(conn, symbol: str,
                                      fiscal_quarter: str) -> None:
    """Mark all ACTIVE predictions for this quarter as RESOLVED."""
    await conn.execute(
        """
        UPDATE market.earnings_predictions_v1
        SET status      = 'RESOLVED',
            resolved_at = now()
        WHERE symbol         = $1
          AND fiscal_quarter = $2
          AND status         = 'ACTIVE'
        """,
        symbol, fiscal_quarter,
    )

async def update_actual_beat_miss(conn, actual: ActualRow,
                                   consensus_rev: Optional[float],
                                   actual_growth_pct: Optional[float]) -> None:
    """Populate vs_consensus_revenue_pct and beat_miss_label on the actual row."""
    if not consensus_rev or consensus_rev == 0:
        return

    vs_consensus = ((actual.actual_revenue_usd - consensus_rev) / consensus_rev) * 100
    beat_miss = classify_direction(vs_consensus)

    await conn.execute(
        """
        UPDATE market.earnings_actuals_v1
        SET vs_consensus_revenue_pct = $1,
            beat_miss_label          = $2,
            actual_revenue_growth_pct = $3
        WHERE actual_id = $4
        """,
        round(vs_consensus, 4), beat_miss, actual_growth_pct, actual.actual_id,
    )

async def score_prediction(conn, pred: PredictionRow, actual: ActualRow,
                            consensus_rev: Optional[float]) -> None:
    """Score one prediction against actuals. Idempotent."""
    accuracy_id = uuid.uuid4()

    # vs consensus actual surprise
    actual_surprise_pct = None
    actual_direction    = "UNKNOWN"
    if consensus_rev and consensus_rev > 0:
        actual_surprise_pct = round(
            ((actual.actual_revenue_usd - consensus_rev) / consensus_rev) * 100, 4
        )
        actual_direction = classify_direction(actual_surprise_pct)

    # Revenue error
    rev_error_usd = None
    rev_error_pct = None
    rev_mape      = None
    if pred.predicted_revenue_usd:
        rev_error_usd = round(actual.actual_revenue_usd - pred.predicted_revenue_usd, 2)
        rev_error_pct = round(
            (rev_error_usd / actual.actual_revenue_usd) * 100
            if actual.actual_revenue_usd else 0.0, 4
        )
        rev_mape = mape(pred.predicted_revenue_usd, actual.actual_revenue_usd)

    # Direction correct?
    direction_correct = (pred.prediction == actual_direction)

    # Surprise error
    surprise_error = None
    if pred.surprise_pct is not None and actual_surprise_pct is not None:
        surprise_error = round(pred.surprise_pct - actual_surprise_pct, 4)

    # Days before earnings at time of prediction
    days_before = (actual.report_date - pred.created_at).days \
        if pred.created_at else None

    # Panel metadata
    panel_meta = await fetch_nowcast_panel_meta(conn, pred.nowcast_id)

    await conn.execute(
        """
        INSERT INTO market.earnings_model_accuracy_v1 (
            accuracy_id, prediction_id, actual_id,
            symbol, fiscal_quarter, model_version,
            predicted_revenue_usd, actual_revenue_usd,
            revenue_error_usd, revenue_error_pct, revenue_mape,
            predicted_direction, actual_direction, direction_correct,
            predicted_surprise_pct, actual_surprise_pct, surprise_error_pct,
            panel_weeks_used, panel_coverage_score, days_before_earnings,
            meta
        ) VALUES (
            $1,$2,$3,$4,$5,$6,
            $7,$8,$9,$10,$11,
            $12,$13,$14,
            $15,$16,$17,
            $18,$19,$20,$21
        )
        ON CONFLICT (prediction_id, actual_id) DO NOTHING
        """,
        accuracy_id, pred.prediction_id, actual.actual_id,
        pred.symbol, pred.fiscal_quarter, WORKER_VERSION,
        pred.predicted_revenue_usd, actual.actual_revenue_usd,
        rev_error_usd, rev_error_pct, rev_mape,
        pred.prediction, actual_direction, direction_correct,
        pred.surprise_pct, actual_surprise_pct, surprise_error,
        panel_meta["panel_weeks_used"], panel_meta["panel_coverage_score"],
        days_before,
        json.dumps({
            "worker": WORKER_VERSION,
            "status_at_score": pred.status,
        }),
    )

async def log_run(conn, run_id: uuid.UUID, status: str, meta: dict) -> None:
    await conn.execute(
        """
        INSERT INTO market.ingest_run_log_v1
            (ingest_run_id, source_name, run_status, meta, completed_at)
        VALUES ($1,$2,$3,$4,now())
        ON CONFLICT DO NOTHING
        """,
        run_id, WORKER_VERSION, status, json.dumps(meta, default=str),
    )

# ─── Main loop ────────────────────────────────────────────────────────────────

async def run_once(pool: asyncpg.Pool) -> None:
    run_id = uuid.uuid4()
    log.info("⚡ earnings_accuracy_worker — starting run")
    actuals_processed = 0
    predictions_scored = 0

    async with pool.acquire() as conn:
        actuals = await fetch_unscored_actuals(conn)
        log.info("🎯 %d actuals with unscored predictions", len(actuals))

        for actual in actuals:
            try:
                log.info("  📋 Scoring %s %s (reported %s)",
                         actual.symbol, actual.fiscal_quarter, actual.report_date)

                consensus_rev = await fetch_consensus_revenue(
                    conn, actual.symbol, actual.fiscal_quarter
                )

                # Compute actual growth for bookkeeping
                actual_growth = None
                if consensus_rev and consensus_rev > 0:
                    prior_implied = None  # would need prior year actual — skipped for now
                    pass

                # Update actual row with beat/miss label
                await update_actual_beat_miss(conn, actual, consensus_rev, actual_growth)

                # Resolve active predictions
                await resolve_active_predictions(
                    conn, actual.symbol, actual.fiscal_quarter
                )

                # Score every prediction for this quarter
                predictions = await fetch_predictions_for_quarter(
                    conn, actual.symbol, actual.fiscal_quarter
                )

                for pred in predictions:
                    await score_prediction(conn, pred, actual, consensus_rev)
                    predictions_scored += 1
                    log.info(
                        "    ✅ Scored prediction %s → direction=%s, mape=%.2f%%",
                        str(pred.prediction_id)[:8],
                        pred.prediction,
                        mape(pred.predicted_revenue_usd or 0,
                             actual.actual_revenue_usd),
                    )

                actuals_processed += 1

            except Exception as e:
                log.error("  ❌ %s %s — scoring failed: %s",
                          actual.symbol, actual.fiscal_quarter, e, exc_info=True)

        await log_run(conn, run_id, "COMPLETED", {
            "actuals_processed": actuals_processed,
            "predictions_scored": predictions_scored,
        })

    log.info("✅ Run complete — %d actuals, %d predictions scored",
             actuals_processed, predictions_scored)

async def main() -> None:
    log.info("🚀 earnings_accuracy_worker starting (poll=%ds)", POLL_INTERVAL_SECS)
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=3)
    try:
        while True:
            try:
                await run_once(pool)
            except Exception as e:
                log.error("Run failed: %s — retrying next cycle", e, exc_info=True)
            log.info("💤 Sleeping %ds", POLL_INTERVAL_SECS)
            await asyncio.sleep(POLL_INTERVAL_SECS)
    finally:
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
