"""
earnings_nowcast_worker.py
─────────────────────────────────────────────────────────────────────────────
Noterminal — Earnings Nowcast Worker

Runs weekly (Sundays). For every symbol with an upcoming earnings date:
  1. Pulls the latest panel data (merchant_spend_panel_v1)
  2. Pulls Wall St consensus (earnings_consensus_v1)
  3. Runs the nowcast model — extrapolates QTD spend to full-quarter revenue
  4. Computes surprise vs consensus
  5. Makes the prediction call (STRONG_BEAT / BEAT / IN_LINE / MISS / STRONG_MISS)
  6. Supersedes prior active prediction for same symbol+quarter
  7. Writes nowcast_run + prediction + fires signal into truth.signals_v1

Extrapolation methods:
  LINEAR_QTD   — run-rate current QTD spend to full quarter (default)
  SEASONAL_ADJ — apply historical seasonality factor from prior years
  (REGRESSION added in v2 when we have enough historical accuracy data)

Canon compliance:
  - Restart loop in start.sh (non-critical worker)
  - asyncpg → Transaction Pooler port 6543
  - inputs_hash / outputs_hash on every nowcast row
  - Vol regime gate before signal write
  - Provenance via ingest_run_log_v1
"""

import asyncio
import hashlib
import json
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [earnings_nowcast_worker] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

DB_URL              = os.environ["DATABASE_URL"]
WORKER_VERSION      = "earnings_nowcast_worker_v1"
POLL_INTERVAL_SECS  = 604800        # weekly (7 days)
MIN_PANEL_WEEKS     = 2             # minimum weeks of panel data to run model
MIN_COVERAGE_SCORE  = 0.15          # minimum panel quality to produce prediction
MIN_CONFIDENCE      = 0.55          # minimum confidence to write signal
MAX_DAYS_TO_EARNINGS = 90           # don't run model if earnings > 90 days out

# Surprise band → prediction label + confidence
PREDICTION_BANDS = [
    ( 5.0, "STRONG_BEAT", 0.90),
    ( 2.0, "BEAT",        0.75),
    (-2.0, "IN_LINE",     0.60),
    (-5.0, "MISS",        0.75),
    (None, "STRONG_MISS", 0.90),
]

# ─── Data classes ─────────────────────────────────────────────────────────────

@dataclass
class ConsensusRow:
    consensus_id:               uuid.UUID
    symbol:                     str
    fiscal_quarter:             str
    fiscal_quarter_end:         date
    earnings_date:              Optional[date]
    consensus_revenue_usd:      float
    consensus_revenue_growth_pct: Optional[float]
    analyst_count:              Optional[int]

@dataclass
class PanelSummary:
    symbol:                 str
    fiscal_quarter:         str
    weeks_used:             int
    qtd_spend_usd:          float
    yoy_growth_pct:         Optional[float]
    user_count:             int
    coverage_score:         float
    latest_week_start:      date

@dataclass
class NowcastResult:
    nowcast_revenue_usd:    float
    nowcast_growth_pct:     float
    nowcast_surprise_pct:   float
    nowcast_revenue_low:    float
    nowcast_revenue_high:   float
    quarter_completion_pct: float
    weeks_remaining:        int
    method:                 str
    prediction:             str
    confidence_score:       float
    confidence_label:       str

# ─── DB reads ─────────────────────────────────────────────────────────────────

async def fetch_upcoming_consensus(conn) -> list[ConsensusRow]:
    """All symbols with earnings within MAX_DAYS_TO_EARNINGS."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (symbol, fiscal_quarter)
            consensus_id, symbol, fiscal_quarter, fiscal_quarter_end,
            earnings_date, consensus_revenue_usd,
            consensus_revenue_growth_pct, analyst_count
        FROM market.earnings_consensus_v1
        WHERE earnings_date IS NOT NULL
          AND earnings_date BETWEEN CURRENT_DATE
                                AND CURRENT_DATE + $1
          AND consensus_revenue_usd IS NOT NULL
        ORDER BY symbol, fiscal_quarter, as_of_date DESC
        """,
        MAX_DAYS_TO_EARNINGS,
    )
    return [
        ConsensusRow(
            consensus_id=r["consensus_id"],
            symbol=r["symbol"],
            fiscal_quarter=r["fiscal_quarter"],
            fiscal_quarter_end=r["fiscal_quarter_end"],
            earnings_date=r["earnings_date"],
            consensus_revenue_usd=float(r["consensus_revenue_usd"]),
            consensus_revenue_growth_pct=float(r["consensus_revenue_growth_pct"])
                if r["consensus_revenue_growth_pct"] else None,
            analyst_count=r["analyst_count"],
        )
        for r in rows
    ]

async def fetch_panel_summary(conn, symbol: str,
                               fiscal_quarter: str) -> Optional[PanelSummary]:
    """Latest QTD panel state for symbol+quarter."""
    row = await conn.fetchrow(
        """
        SELECT
            COUNT(*)                    AS weeks_used,
            MAX(qtd_spend_usd)          AS qtd_spend_usd,
            AVG(yoy_growth_pct)         AS yoy_growth_pct,
            MAX(qtd_user_count)         AS user_count,
            AVG(panel_coverage_score)   AS coverage_score,
            MAX(week_start)             AS latest_week_start
        FROM market.merchant_spend_panel_v1
        WHERE canonical_symbol = $1
          AND fiscal_quarter    = $2
          AND min_user_threshold_met = true
        """,
        symbol, fiscal_quarter,
    )

    if not row or not row["weeks_used"] or row["weeks_used"] == 0:
        return None
    if not row["qtd_spend_usd"]:
        return None

    return PanelSummary(
        symbol=symbol,
        fiscal_quarter=fiscal_quarter,
        weeks_used=int(row["weeks_used"]),
        qtd_spend_usd=float(row["qtd_spend_usd"]),
        yoy_growth_pct=float(row["yoy_growth_pct"]) if row["yoy_growth_pct"] else None,
        user_count=int(row["user_count"]) if row["user_count"] else 0,
        coverage_score=float(row["coverage_score"]) if row["coverage_score"] else 0.0,
        latest_week_start=row["latest_week_start"],
    )

async def get_vol_regime(conn) -> str:
    row = await conn.fetchrow(
        "SELECT regime_label FROM intel.vol_regime_log_v1 ORDER BY evaluated_at DESC LIMIT 1"
    )
    return row["regime_label"] if row else "UNKNOWN"

# ─── Nowcast model ─────────────────────────────────────────────────────────────

def quarter_date_range(fiscal_quarter: str, fq_end: date) -> tuple[date, date]:
    """Return (quarter_start, quarter_end) from fiscal_quarter string."""
    q, yr = fiscal_quarter.split("-")
    yr = int(yr)
    q_start_month = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}[q]
    q_start = date(yr, q_start_month, 1)
    return q_start, fq_end

def compute_quarter_completion(fq_start: date, fq_end: date) -> tuple[float, int]:
    """How far through the quarter are we today."""
    today = date.today()
    total_days = (fq_end - fq_start).days
    elapsed = min((today - fq_start).days, total_days)
    completion_pct = round((elapsed / total_days) * 100, 2)
    # Remaining weeks
    remaining_days = max((fq_end - today).days, 0)
    weeks_remaining = remaining_days // 7
    return completion_pct, weeks_remaining

def linear_qtd_extrapolation(panel: PanelSummary,
                              completion_pct: float) -> tuple[float, float, float]:
    """
    Project QTD spend to full-quarter revenue via linear run-rate.
    Returns (nowcast_revenue, low, high).
    Uncertainty band widens early in the quarter, tightens as it completes.
    """
    if completion_pct <= 0:
        completion_pct = 1.0

    # Run-rate: annualize QTD spend based on quarter completion
    nowcast = panel.qtd_spend_usd / (completion_pct / 100)

    # Uncertainty: ±15% early, narrows to ±3% at quarter end
    uncertainty = max(0.03, 0.15 * (1 - completion_pct / 100))
    low  = nowcast * (1 - uncertainty)
    high = nowcast * (1 + uncertainty)

    return round(nowcast, 2), round(low, 2), round(high, 2)

def seasonal_adj_extrapolation(panel: PanelSummary, completion_pct: float,
                                consensus: ConsensusRow) -> tuple[float, float, float]:
    """
    Blend panel YoY growth with consensus growth using panel coverage as weight.
    Higher coverage = more weight on panel signal.
    """
    panel_growth = panel.yoy_growth_pct or 0.0
    consensus_growth = consensus.consensus_revenue_growth_pct or 0.0
    prior_rev = consensus.consensus_revenue_usd / (1 + consensus_growth / 100) \
        if consensus_growth != -100 else consensus.consensus_revenue_usd

    # Blend: panel_weight driven by coverage score and quarter completion
    panel_weight = min(panel.coverage_score * (completion_pct / 100), 0.85)
    blended_growth = (panel_growth * panel_weight) + (consensus_growth * (1 - panel_weight))
    nowcast = prior_rev * (1 + blended_growth / 100)

    uncertainty = max(0.02, 0.10 * (1 - panel.coverage_score))
    low  = nowcast * (1 - uncertainty)
    high = nowcast * (1 + uncertainty)

    return round(nowcast, 2), round(low, 2), round(high, 2)

def determine_prediction(surprise_pct: float,
                          coverage_score: float,
                          completion_pct: float) -> tuple[str, float, str]:
    """Map surprise_pct → prediction label + confidence score."""
    # Base confidence from surprise bands
    for threshold, label, base_conf in PREDICTION_BANDS:
        if threshold is None or surprise_pct <= threshold:
            # Discount confidence based on panel quality and quarter completeness
            quality_factor = min(coverage_score * 1.5, 1.0)
            completion_factor = min(completion_pct / 100, 1.0)
            confidence = base_conf * (0.5 + 0.3 * quality_factor + 0.2 * completion_factor)
            confidence = round(min(confidence, 0.95), 4)
            conf_label = (
                "HIGH"     if confidence >= 0.80 else
                "MODERATE" if confidence >= 0.65 else
                "LOW"
            )
            return label, confidence, conf_label

    return "IN_LINE", 0.50, "LOW"

def sha256_of(obj: dict) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode()
    ).hexdigest()

# ─── Model runner ─────────────────────────────────────────────────────────────

def run_nowcast_model(panel: PanelSummary,
                      consensus: ConsensusRow,
                      fq_start: date) -> NowcastResult:
    completion_pct, weeks_remaining = compute_quarter_completion(
        fq_start, consensus.fiscal_quarter_end
    )

    # Choose method: seasonal if we have YoY data, linear otherwise
    if panel.yoy_growth_pct is not None and panel.coverage_score >= 0.3:
        method = "SEASONAL_ADJ"
        nowcast, low, high = seasonal_adj_extrapolation(
            panel, completion_pct, consensus
        )
    else:
        method = "LINEAR_QTD"
        nowcast, low, high = linear_qtd_extrapolation(panel, completion_pct)

    # Growth implied by our nowcast vs prior year
    consensus_growth = consensus.consensus_revenue_growth_pct or 0.0
    prior_rev = consensus.consensus_revenue_usd / (1 + consensus_growth / 100) \
        if consensus_growth != -100 else consensus.consensus_revenue_usd
    nowcast_growth = round(
        ((nowcast - prior_rev) / prior_rev * 100) if prior_rev else 0.0, 4
    )

    # Surprise = our growth estimate minus consensus growth estimate
    nowcast_surprise = round(nowcast_growth - consensus_growth, 4)

    prediction, confidence, conf_label = determine_prediction(
        nowcast_surprise, panel.coverage_score, completion_pct
    )

    return NowcastResult(
        nowcast_revenue_usd=nowcast,
        nowcast_growth_pct=nowcast_growth,
        nowcast_surprise_pct=nowcast_surprise,
        nowcast_revenue_low=low,
        nowcast_revenue_high=high,
        quarter_completion_pct=completion_pct,
        weeks_remaining=weeks_remaining,
        method=method,
        prediction=prediction,
        confidence_score=confidence,
        confidence_label=conf_label,
    )

# ─── DB writes ────────────────────────────────────────────────────────────────

async def write_nowcast_and_prediction(conn, panel: PanelSummary,
                                        consensus: ConsensusRow,
                                        result: NowcastResult) -> uuid.UUID:
    """Atomic write: nowcast run + prediction. Supersedes prior active prediction."""
    nowcast_id   = uuid.uuid4()
    prediction_id = uuid.uuid4()
    days_to_earnings = (
        (consensus.earnings_date - date.today()).days
        if consensus.earnings_date else None
    )

    inputs = {
        "symbol": panel.symbol,
        "fiscal_quarter": panel.fiscal_quarter,
        "panel_weeks_used": panel.weeks_used,
        "panel_qtd_spend_usd": panel.qtd_spend_usd,
        "panel_yoy_growth_pct": panel.yoy_growth_pct,
        "panel_coverage_score": panel.coverage_score,
        "consensus_revenue_usd": consensus.consensus_revenue_usd,
        "consensus_growth_pct": consensus.consensus_revenue_growth_pct,
        "method": result.method,
    }
    outputs = {
        "nowcast_id": str(nowcast_id),
        "nowcast_revenue_usd": result.nowcast_revenue_usd,
        "nowcast_surprise_pct": result.nowcast_surprise_pct,
        "prediction": result.prediction,
        "confidence_score": result.confidence_score,
    }

    async with conn.transaction():
        # 1. Write nowcast run
        await conn.execute(
            """
            INSERT INTO market.earnings_nowcast_runs_v1 (
                nowcast_id, symbol, fiscal_quarter, run_ts, model_version,
                panel_weeks_used, panel_qtd_spend_usd, panel_yoy_growth_pct,
                panel_user_count, panel_coverage_score,
                consensus_id, consensus_revenue_usd, consensus_growth_pct,
                nowcast_revenue_usd, nowcast_growth_pct, nowcast_surprise_pct,
                nowcast_revenue_low, nowcast_revenue_high,
                extrapolation_method, quarter_completion_pct, weeks_remaining,
                inputs_hash, outputs_hash, raw_inputs
            ) VALUES (
                $1,$2,$3,now(),$4,
                $5,$6,$7,$8,$9,
                $10,$11,$12,
                $13,$14,$15,$16,$17,
                $18,$19,$20,
                $21,$22,$23
            )
            """,
            nowcast_id, panel.symbol, panel.fiscal_quarter, WORKER_VERSION,
            panel.weeks_used, panel.qtd_spend_usd, panel.yoy_growth_pct,
            panel.user_count, panel.coverage_score,
            consensus.consensus_id, consensus.consensus_revenue_usd,
            consensus.consensus_revenue_growth_pct,
            result.nowcast_revenue_usd, result.nowcast_growth_pct,
            result.nowcast_surprise_pct, result.nowcast_revenue_low,
            result.nowcast_revenue_high,
            result.method, result.quarter_completion_pct, result.weeks_remaining,
            sha256_of(inputs), sha256_of(outputs),
            json.dumps(inputs, default=str),
        )

        # 2. Supersede prior active prediction
        await conn.execute(
            """
            UPDATE market.earnings_predictions_v1
            SET status       = 'SUPERSEDED',
                superseded_by = $1,
                resolved_at  = now()
            WHERE symbol         = $2
              AND fiscal_quarter = $3
              AND status         = 'ACTIVE'
            """,
            prediction_id, panel.symbol, panel.fiscal_quarter,
        )

        # 3. Write new prediction
        await conn.execute(
            """
            INSERT INTO market.earnings_predictions_v1 (
                prediction_id, nowcast_id, symbol, fiscal_quarter,
                earnings_date, prediction, predicted_revenue_usd,
                surprise_pct, confidence_score, confidence_label,
                status, days_to_earnings, meta
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'ACTIVE',$11,$12)
            """,
            prediction_id, nowcast_id, panel.symbol, panel.fiscal_quarter,
            consensus.earnings_date, result.prediction, result.nowcast_revenue_usd,
            result.nowcast_surprise_pct, result.confidence_score,
            result.confidence_label, days_to_earnings,
            json.dumps({"model_version": WORKER_VERSION,
                        "method": result.method}, default=str),
        )

    return prediction_id

async def write_signal(conn, panel: PanelSummary, consensus: ConsensusRow,
                        result: NowcastResult, prediction_id: uuid.UUID) -> Optional[uuid.UUID]:
    """Fire a signal into truth.signals_v1 for BEAT/STRONG_BEAT/MISS/STRONG_MISS."""
    if result.prediction == "IN_LINE":
        return None
    if result.confidence_score < MIN_CONFIDENCE:
        log.info("    ⏭  %s confidence %.2f below threshold — no signal",
                 panel.symbol, result.confidence_score)
        return None

    side = "LONG" if result.prediction in ("BEAT", "STRONG_BEAT") else "SHORT"
    signal_id = uuid.uuid4()

    # Stub entry price — replace with live Alpaca quote
    entry_price = 100.0
    atr_proxy   = 0.015
    stop   = round(entry_price * (1 - atr_proxy) if side == "LONG"
                   else entry_price * (1 + atr_proxy), 4)
    target = round(entry_price * (1 + atr_proxy * 2) if side == "LONG"
                   else entry_price * (1 - atr_proxy * 2), 4)

    async with conn.transaction():
        await conn.execute(
            """
            INSERT INTO truth.signals_v1 (
                signal_id, strategy_name, symbol, session_date, signal_ts,
                side, signal_type, entry_trigger_price, stop_price,
                target_price, confidence_score, meta
            ) VALUES ($1,$2,$3,CURRENT_DATE,now(),$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (signal_id) DO NOTHING
            """,
            signal_id,
            f"EARNINGS_NOWCAST_{result.prediction}_v1",
            panel.symbol,
            side,
            "FUNDAMENTAL_REVENUE",
            entry_price, stop, target,
            result.confidence_score,
            json.dumps({
                "prediction": result.prediction,
                "surprise_pct": result.nowcast_surprise_pct,
                "earnings_date": str(consensus.earnings_date),
                "prediction_id": str(prediction_id),
                "worker": WORKER_VERSION,
            }),
        )

        # Link signal back to prediction
        await conn.execute(
            """
            UPDATE market.earnings_predictions_v1
            SET signal_id = $1
            WHERE prediction_id = $2
            """,
            signal_id, prediction_id,
        )

    return signal_id

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
    log.info("⚡ earnings_nowcast_worker — starting run")
    predictions_written = 0
    signals_fired = 0

    async with pool.acquire() as conn:
        consensus_rows = await fetch_upcoming_consensus(conn)
        log.info("📅 %d symbols with upcoming earnings", len(consensus_rows))

        regime = await get_vol_regime(conn)
        if regime == "HIGH":
            log.warning("🚫 Vol regime HIGH — suppressing all signals this run")

        for consensus in consensus_rows:
            try:
                panel = await fetch_panel_summary(
                    conn, consensus.symbol, consensus.fiscal_quarter
                )
                if not panel:
                    log.info("  ⏭  %s — no panel data yet", consensus.symbol)
                    continue
                if panel.weeks_used < MIN_PANEL_WEEKS:
                    log.info("  ⏭  %s — only %d weeks of panel (need %d)",
                             consensus.symbol, panel.weeks_used, MIN_PANEL_WEEKS)
                    continue
                if panel.coverage_score < MIN_COVERAGE_SCORE:
                    log.info("  ⏭  %s — coverage %.2f below threshold",
                             consensus.symbol, panel.coverage_score)
                    continue

                fq_start, _ = quarter_date_range(
                    consensus.fiscal_quarter, consensus.fiscal_quarter_end
                )
                result = run_nowcast_model(panel, consensus, fq_start)

                prediction_id = await write_nowcast_and_prediction(
                    conn, panel, consensus, result
                )
                predictions_written += 1

                log.info(
                    "  📈 %s %s → %s (surprise %.2f%%, conf %.2f, method=%s)",
                    consensus.symbol, consensus.fiscal_quarter,
                    result.prediction, result.nowcast_surprise_pct,
                    result.confidence_score, result.method,
                )

                if regime != "HIGH":
                    sig_id = await write_signal(
                        conn, panel, consensus, result, prediction_id
                    )
                    if sig_id:
                        signals_fired += 1
                        log.info("    🔔 Signal fired → %s", sig_id)

            except Exception as e:
                log.error("  ❌ %s — failed: %s",
                          consensus.symbol, e, exc_info=True)

        await log_run(conn, run_id, "COMPLETED", {
            "symbols_evaluated": len(consensus_rows),
            "predictions_written": predictions_written,
            "signals_fired": signals_fired,
            "vol_regime": regime,
        })

    log.info("✅ Run complete — %d predictions, %d signals",
             predictions_written, signals_fired)

async def main() -> None:
    log.info("🚀 earnings_nowcast_worker starting (poll=%ds)", POLL_INTERVAL_SECS)
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
