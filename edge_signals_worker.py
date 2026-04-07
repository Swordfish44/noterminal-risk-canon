"""
edge_signals_worker.py  (repo root)

Polls market.v_aelc_features_1s_v1 every POLL_INTERVAL_S seconds.
Rows with edge_score >= EDGE_THRESHOLD are upserted into market.edge_signals_v1.

Env (required):
  SUPABASE_DB_URL  postgresql://...

Env (optional, with defaults):
  EDGE_THRESHOLD=2
  POLL_INTERVAL_S=5
"""

import logging
import os
import signal
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

DB_URL = (
    os.environ.get("SUPABASE_DB_URL")
    or os.environ.get("PG_CONN")
    or os.environ.get("DATABASE_URL")
)
if not DB_URL:
    raise RuntimeError("No database URL found. Set SUPABASE_DB_URL, PG_CONN, or DATABASE_URL.")

EDGE_THRESHOLD  = int(os.getenv("EDGE_THRESHOLD", "2"))
POLL_INTERVAL_S = float(os.getenv("POLL_INTERVAL_S", "5"))

BACKOFF_BASE_S = 3.0
BACKOFF_MAX_S  = 60.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("edge_signals_worker")

# ── SQL ───────────────────────────────────────────────────────────────────────

POLL_SQL = """
SELECT
    symbol_id, instrument_symbol, bucket_ts,
    edge_label, edge_score, edge_direction,
    is_flow_dominant, is_exhausted, is_toxic_entry, is_high_confidence,
    net_flow, net_flow_z, ofi_normalised, ofi_z, toxicity_ratio,
    flow_momentum, momentum_direction, persistence_seconds,
    flow_direction, trade_count, total_vol, vwap, mid_price
FROM market.v_aelc_features_1s_v1
WHERE edge_score >= %s
"""

UPSERT_SQL = """
INSERT INTO market.edge_signals_v1 (
    symbol_id, instrument_symbol, bucket_ts,
    edge_label, edge_score, edge_direction,
    is_flow_dominant, is_exhausted, is_toxic_entry, is_high_confidence,
    net_flow, net_flow_z, ofi_normalised, ofi_z, toxicity_ratio,
    flow_momentum, momentum_direction, persistence_seconds,
    flow_direction, trade_count, total_vol, vwap, mid_price
)
VALUES %s
ON CONFLICT (symbol_id, bucket_ts) DO UPDATE SET
    instrument_symbol  = EXCLUDED.instrument_symbol,
    edge_label         = EXCLUDED.edge_label,
    edge_score         = EXCLUDED.edge_score,
    edge_direction     = EXCLUDED.edge_direction,
    is_flow_dominant   = EXCLUDED.is_flow_dominant,
    is_exhausted       = EXCLUDED.is_exhausted,
    is_toxic_entry     = EXCLUDED.is_toxic_entry,
    is_high_confidence = EXCLUDED.is_high_confidence,
    net_flow           = EXCLUDED.net_flow,
    net_flow_z         = EXCLUDED.net_flow_z,
    ofi_normalised     = EXCLUDED.ofi_normalised,
    ofi_z              = EXCLUDED.ofi_z,
    toxicity_ratio     = EXCLUDED.toxicity_ratio,
    flow_momentum      = EXCLUDED.flow_momentum,
    momentum_direction = EXCLUDED.momentum_direction,
    persistence_seconds = EXCLUDED.persistence_seconds,
    flow_direction     = EXCLUDED.flow_direction,
    trade_count        = EXCLUDED.trade_count,
    total_vol          = EXCLUDED.total_vol,
    vwap               = EXCLUDED.vwap,
    mid_price          = EXCLUDED.mid_price
"""

# ── Lifecycle ─────────────────────────────────────────────────────────────────

_shutdown = False


def _handle_sigterm(signum, frame):
    global _shutdown
    log.info("SIGTERM received — shutting down")
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_sigterm)


def connect(backoff: float) -> "psycopg2.extensions.connection":
    while not _shutdown:
        try:
            conn = psycopg2.connect(DB_URL)
            conn.autocommit = False
            log.info("Connected to DB")
            return conn
        except psycopg2.OperationalError as e:
            log.error("DB connect failed: %s — retrying in %.0fs", e, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_S)
    raise SystemExit(0)


# ── Core cycle ────────────────────────────────────────────────────────────────

def run_cycle(conn) -> int:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(POLL_SQL, (EDGE_THRESHOLD,))
        rows = cur.fetchall()
    if not rows:
        return 0
    values = [
        (
            r["symbol_id"],
            r["instrument_symbol"] or str(r["symbol_id"]),
            r["bucket_ts"],
            r["edge_label"],
            r["edge_score"],
            r["edge_direction"],
            r["is_flow_dominant"],
            r["is_exhausted"],
            r["is_toxic_entry"],
            r["is_high_confidence"],
            r["net_flow"],
            r["net_flow_z"],
            r["ofi_normalised"],
            r["ofi_z"],
            r["toxicity_ratio"],
            r["flow_momentum"],
            r["momentum_direction"],
            r["persistence_seconds"],
            r["flow_direction"],
            r["trade_count"],
            r["total_vol"],
            r["vwap"],
            r["mid_price"],
        )
        for r in rows
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, values)
    conn.commit()
    return len(values)


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    log.info(
        "Edge signal worker online. threshold=%d interval=%.1fs",
        EDGE_THRESHOLD, POLL_INTERVAL_S,
    )
    backoff = BACKOFF_BASE_S
    conn = None

    while not _shutdown:
        try:
            if conn is None or conn.closed:
                conn = connect(backoff)
                backoff = BACKOFF_BASE_S  # reset after successful connect

            t0 = time.perf_counter()
            n = run_cycle(conn)
            elapsed_ms = (time.perf_counter() - t0) * 1000

            if n:
                log.info("Upserted %d edge signal rows (%.0f ms)", n, elapsed_ms)
            else:
                log.debug("No signals above threshold (%.0f ms)", elapsed_ms)

            remaining = POLL_INTERVAL_S - (time.perf_counter() - t0)
            if remaining > 0:
                time.sleep(remaining)

        except psycopg2.OperationalError as e:
            log.error("DB error: %s — reconnecting", e)
            if conn and not conn.closed:
                conn.close()
            conn = None
            time.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_S)

        except Exception:
            log.exception("Unexpected error in cycle — continuing")
            if conn and not conn.closed:
                conn.rollback()
            time.sleep(POLL_INTERVAL_S)

    if conn and not conn.closed:
        conn.close()
    log.info("Worker stopped.")


if __name__ == "__main__":
    main()
