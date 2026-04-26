"""
panel_aggregation_worker.py
─────────────────────────────────────────────────────────────────────────────
Noterminal — Panel Aggregation Worker

Runs nightly. Reads market.plaid_merchant_observations_v1, aggregates by
symbol + week, computes YoY growth, QTD cumulative, ticket stats, and
panel quality score. Writes to market.merchant_spend_panel_v1.

This table is the proprietary moat — the aggregated, PII-free spend panel
that powers the earnings nowcast model.

Canon compliance:
  - Restart loop in start.sh (non-critical worker)
  - asyncpg → Transaction Pooler port 6543
  - Idempotent upsert on (canonical_symbol, week_start)
  - Skips symbols with < MIN_USERS_THRESHOLD distinct users
  - Full provenance via ingest_run_log_v1
"""

import asyncio
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
    format="%(asctime)s [panel_aggregation_worker] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

DB_URL              = os.environ["DATABASE_URL"]
WORKER_VERSION      = "panel_aggregation_worker_v1"
POLL_INTERVAL_SECS  = 86400          # nightly
MIN_USERS_THRESHOLD = 10             # suppress panel row if fewer users (raise as panel grows)
LOOKBACK_WEEKS      = 56             # 52 weeks current + 4 prior-year buffer

# Fiscal quarter mapping — extend each year
FISCAL_QUARTERS = {
    (1, 2, 3):  "Q1",
    (4, 5, 6):  "Q2",
    (7, 8, 9):  "Q3",
    (10, 11, 12): "Q4",
}

def fiscal_quarter(d: date) -> str:
    for months, q in FISCAL_QUARTERS.items():
        if d.month in months:
            return f"{q}-{d.year}"
    return f"Q?-{d.year}"

def week_start(d: date) -> date:
    """Return Monday of the week containing d."""
    return d - timedelta(days=d.weekday())

# ─── Panel computation ────────────────────────────────────────────────────────

@dataclass
class PanelRow:
    canonical_symbol:       str
    week_start_date:        date
    fq:                     str
    user_count:             int
    transaction_count:      int
    total_spend_usd:        float
    avg_ticket_usd:         Optional[float]
    prior_year_spend_usd:   Optional[float]
    yoy_growth_pct:         Optional[float]
    qtd_spend_usd:          Optional[float]
    qtd_user_count:         Optional[int]
    qtd_transaction_count:  Optional[int]
    panel_coverage_score:   float
    min_user_threshold_met: bool

def compute_coverage_score(user_count: int, tx_count: int,
                            prior_year_spend: Optional[float]) -> float:
    """
    0-1 score representing panel quality.
    Factors: user count depth, transaction density, YoY comparability.
    Grows as panel scales — honest about limitations early on.
    """
    user_score = min(user_count / 500, 1.0)          # saturates at 500 users
    tx_score   = min(tx_count / (user_count * 4), 1.0) if user_count else 0.0
    yoy_score  = 1.0 if prior_year_spend and prior_year_spend > 0 else 0.5
    return round((user_score * 0.5) + (tx_score * 0.3) + (yoy_score * 0.2), 4)

async def fetch_symbols(conn) -> list[str]:
    """All symbols that have observations."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT canonical_symbol
        FROM market.plaid_merchant_observations_v1
        WHERE canonical_symbol IS NOT NULL
        ORDER BY canonical_symbol
        """
    )
    return [r["canonical_symbol"] for r in rows]

async def fetch_weekly_aggregates(conn, symbol: str,
                                   cutoff: date) -> list[asyncpg.Record]:
    """Weekly spend aggregates for a symbol from cutoff date."""
    return await conn.fetch(
        """
        SELECT
            date_trunc('week', transaction_date)::date   AS week_start,
            COUNT(DISTINCT user_token)                   AS user_count,
            COUNT(*)                                     AS transaction_count,
            SUM(amount_usd)                              AS total_spend_usd,
            AVG(amount_usd)                              AS avg_ticket_usd
        FROM market.plaid_merchant_observations_v1
        WHERE canonical_symbol = $1
          AND transaction_date >= $2
          AND amount_usd > 0
        GROUP BY date_trunc('week', transaction_date)::date
        ORDER BY week_start
        """,
        symbol, cutoff,
    )

async def fetch_prior_year_spend(conn, symbol: str,
                                  ws: date) -> Optional[float]:
    """Total spend in the same week one year ago (±3 days tolerance)."""
    prior = ws - timedelta(weeks=52)
    row = await conn.fetchrow(
        """
        SELECT SUM(amount_usd) AS spend
        FROM market.plaid_merchant_observations_v1
        WHERE canonical_symbol = $1
          AND transaction_date BETWEEN $2 AND $3
          AND amount_usd > 0
        """,
        symbol,
        prior - timedelta(days=3),
        prior + timedelta(days=10),
    )
    return float(row["spend"]) if row and row["spend"] else None

async def fetch_qtd_aggregates(conn, symbol: str,
                                fq: str, week_end: date) -> asyncpg.Record:
    """QTD cumulative spend from start of fiscal quarter to week_end."""
    # Derive quarter start from fq string e.g. "Q2-2026"
    q, yr = fq.split("-")
    yr = int(yr)
    q_start_month = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}[q]
    q_start = date(yr, q_start_month, 1)

    return await conn.fetchrow(
        """
        SELECT
            SUM(amount_usd)            AS qtd_spend_usd,
            COUNT(DISTINCT user_token) AS qtd_user_count,
            COUNT(*)                   AS qtd_transaction_count
        FROM market.plaid_merchant_observations_v1
        WHERE canonical_symbol = $1
          AND transaction_date BETWEEN $2 AND $3
          AND amount_usd > 0
        """,
        symbol, q_start, week_end,
    )

async def build_panel_rows(conn, symbol: str) -> list[PanelRow]:
    cutoff = date.today() - timedelta(weeks=LOOKBACK_WEEKS)
    weekly = await fetch_weekly_aggregates(conn, symbol, cutoff)
    rows = []

    for rec in weekly:
        ws          = rec["week_start"]
        user_count  = rec["user_count"]
        tx_count    = rec["transaction_count"]
        total_spend = float(rec["total_spend_usd"] or 0)
        avg_ticket  = float(rec["avg_ticket_usd"]) if rec["avg_ticket_usd"] else None
        fq          = fiscal_quarter(ws)

        prior_spend = await fetch_prior_year_spend(conn, symbol, ws)
        yoy = None
        if prior_spend and prior_spend > 0:
            yoy = round(((total_spend - prior_spend) / prior_spend) * 100, 4)

        week_end = ws + timedelta(days=6)
        qtd_rec  = await fetch_qtd_aggregates(conn, symbol, fq, week_end)
        qtd_spend = float(qtd_rec["qtd_spend_usd"]) if qtd_rec["qtd_spend_usd"] else None
        qtd_users = int(qtd_rec["qtd_user_count"]) if qtd_rec["qtd_user_count"] else None
        qtd_txs   = int(qtd_rec["qtd_transaction_count"]) if qtd_rec["qtd_transaction_count"] else None

        coverage = compute_coverage_score(user_count, tx_count, prior_spend)
        threshold_met = user_count >= MIN_USERS_THRESHOLD

        rows.append(PanelRow(
            canonical_symbol=symbol,
            week_start_date=ws,
            fq=fq,
            user_count=user_count,
            transaction_count=tx_count,
            total_spend_usd=total_spend,
            avg_ticket_usd=avg_ticket,
            prior_year_spend_usd=prior_spend,
            yoy_growth_pct=yoy,
            qtd_spend_usd=qtd_spend,
            qtd_user_count=qtd_users,
            qtd_transaction_count=qtd_txs,
            panel_coverage_score=coverage,
            min_user_threshold_met=threshold_met,
        ))

    return rows

async def upsert_panel_rows(conn, rows: list[PanelRow]) -> int:
    written = 0
    for row in rows:
        await conn.execute(
            """
            INSERT INTO market.merchant_spend_panel_v1 (
                canonical_symbol, week_start, fiscal_quarter,
                user_count, transaction_count, total_spend_usd,
                avg_ticket_usd, prior_year_spend_usd, yoy_growth_pct,
                qtd_spend_usd, qtd_user_count, qtd_transaction_count,
                panel_coverage_score, min_user_threshold_met, computed_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,now())
            ON CONFLICT (canonical_symbol, week_start) DO UPDATE SET
                user_count              = EXCLUDED.user_count,
                transaction_count       = EXCLUDED.transaction_count,
                total_spend_usd         = EXCLUDED.total_spend_usd,
                avg_ticket_usd          = EXCLUDED.avg_ticket_usd,
                prior_year_spend_usd    = EXCLUDED.prior_year_spend_usd,
                yoy_growth_pct          = EXCLUDED.yoy_growth_pct,
                qtd_spend_usd           = EXCLUDED.qtd_spend_usd,
                qtd_user_count          = EXCLUDED.qtd_user_count,
                qtd_transaction_count   = EXCLUDED.qtd_transaction_count,
                panel_coverage_score    = EXCLUDED.panel_coverage_score,
                min_user_threshold_met  = EXCLUDED.min_user_threshold_met,
                computed_at             = now()
            """,
            row.canonical_symbol, row.week_start_date, row.fq,
            row.user_count, row.transaction_count, row.total_spend_usd,
            row.avg_ticket_usd, row.prior_year_spend_usd, row.yoy_growth_pct,
            row.qtd_spend_usd, row.qtd_user_count, row.qtd_transaction_count,
            row.panel_coverage_score, row.min_user_threshold_met,
        )
        written += 1
    return written

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
    log.info("⚡ panel_aggregation_worker — starting run")
    total_written = 0

    async with pool.acquire() as conn:
        symbols = await fetch_symbols(conn)
        log.info("📊 Found %d symbols with observations", len(symbols))

        for symbol in symbols:
            try:
                rows = await build_panel_rows(conn, symbol)
                written = await upsert_panel_rows(conn, rows)
                total_written += written
                log.info("  ✅ %s — %d panel rows upserted", symbol, written)
            except Exception as e:
                log.error("  ❌ %s — failed: %s", symbol, e, exc_info=True)

        await log_run(conn, run_id, "COMPLETED", {
            "symbols_processed": len(symbols),
            "panel_rows_written": total_written,
        })

    log.info("✅ Run complete — %d total panel rows written", total_written)

async def main() -> None:
    log.info("🚀 panel_aggregation_worker starting (poll=%ds)", POLL_INTERVAL_SECS)
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
