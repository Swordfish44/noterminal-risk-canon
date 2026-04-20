import os
from pathlib import Path
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT_DIR / ".env"

load_dotenv(ENV_FILE)

PG_CONN = os.getenv("PG_CONN")

if not PG_CONN:
    raise RuntimeError("PG_CONN is not set")

"""
Truth Engine — Lead/Lag Arbitrage Detection Worker (async, v2)

Computes:
- Rolling cross-correlation over candidate lags (ms)
- Rolling OLS regression of lagger returns on shifted leader returns
- Model health: residual autocorrelation, innovation mean/vol, z-score
- Regime gates: spread, volatility, liquidity, xcorr_min, beta_change_pct
- Persists edge stats to arb.leadlag_edge_v1 (UPSERT)
  including all model health columns required by v7 schema
- Emits signals to arb.leadlag_signal_v1 with edge_asof_ts set
  (enables partition-pruned lookup in publish function)
- Publishes signals into Partner Intelligence Layer via:
    arb.fn_publish_leadlag_signal_to_intel_v1

Fixes from v1 review:
  1. Edge upsert now includes all v7 schema columns:
       pair_id, resid_vol_bps, resid_mean_bps, resid_autocorr,
       z_score_at_compute, z_entry_threshold, beta_prev,
       beta_change_pct, xcorr_min, beta_change_pct_max, stale_after_ms
  2. Signal insert now includes edge_asof_ts (partition-pruned publish),
       pair_id, resid_vol_bps, z_score, z_entry_threshold, compute_version
  3. on conflict do nothing replaced with explicit conflict logging;
       dedupe handled by checking signal_id is None and logging clearly
  4. Publish function call corrected to 5-param signature:
       (signal_id, partner_id, delivery_method, strategy_id, route_hint)
       p_strategy_id passes None, not a text string
  5. UUID validation at startup; asyncpg return types handled correctly
  6. Connection-per-task via pool: each upsert acquires its own connection
       so asyncio.gather provides real concurrency across pairs
  7. Variable shadowing fixed in build_symbol_series
  8. Pool teardown in try/finally in main()
  9. Loop cadence drift fixed: sleep(max(0, interval - elapsed))
 10. stale_after_ms read from pair registry and passed in edge upsert

Assumptions / integration points you must map to your DB:
- You have 1s (or near-1s) features per symbol including:
    spread_bps, vol_bps, liq_score, and mid/return series.
- Replace FETCH_SERIES_SQL to match your canonical feature tables.
- Replace FETCH_PAIRS_SQL to match your pair registry query.
- The rest of the worker is stable.

Run:
  python workers/leadlag_worker_v2.py

Env (required):
  PG_CONN=postgresql://...

Env (optional, with defaults):
  LEADLAG_LOOKBACK_S=300
  LEADLAG_BUCKET_S=1
  LEADLAG_MAX_LAG_MS=5000
  LEADLAG_LAG_STEP_MS=100
  LEADLAG_EDGE_THRESHOLD=2.5
  LEADLAG_CONFIDENCE_MIN=0.60
  LEADLAG_SIGNAL_TTL_MS=30000
  LEADLAG_LOOP_INTERVAL_S=1.0
  LEADLAG_TASK_CHUNK=50
  LEADLAG_SPREAD_BPS_MAX=8
  LEADLAG_VOL_BPS_MIN=2
  LEADLAG_VOL_BPS_MAX=250
  LEADLAG_LIQ_SCORE_MIN=0.50
  LEADLAG_XCORR_MIN=0.30
  LEADLAG_BETA_CHANGE_PCT_MAX=40.0
  LEADLAG_STALE_AFTER_MS=60000
  LEADLAG_PARTNER_ID=<uuid>       (optional; if set, auto-publish to intel)
  LEADLAG_STRATEGY_ID=<uuid>      (optional; attached to published opportunities)
"""

import os
import uuid
import json
import time
import math
import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import asyncpg
import numpy as np


# ─────────────────────────── Logging ────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("leadlag_worker")


# ─────────────────────────── Config ─────────────────────────────────────────

def _require_env(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"Required env var {key!r} is not set")
    return v


def _parse_uuid(key: str) -> Optional[uuid.UUID]:
    """Parse optional UUID env var. Raises clearly at startup if malformed."""
    raw = os.getenv(key)
    if not raw:
        return None
    try:
        return uuid.UUID(raw)
    except ValueError:
        raise RuntimeError(
            f"Env var {key!r} = {raw!r} is not a valid UUID. "
            f"Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        )


PG_CONN           = _require_env("PG_CONN")

LOOKBACK_S        = int(os.getenv("LEADLAG_LOOKBACK_S",        "300"))
BUCKET_S          = int(os.getenv("LEADLAG_BUCKET_S",          "1"))
MAX_LAG_MS        = int(os.getenv("LEADLAG_MAX_LAG_MS",        "5000"))
LAG_STEP_MS       = int(os.getenv("LEADLAG_LAG_STEP_MS",       "100"))

EDGE_THRESHOLD    = float(os.getenv("LEADLAG_EDGE_THRESHOLD",  "2.5"))
CONFIDENCE_MIN    = float(os.getenv("LEADLAG_CONFIDENCE_MIN",  "0.60"))
SIGNAL_TTL_MS     = int(os.getenv("LEADLAG_SIGNAL_TTL_MS",     "30000"))
LOOP_INTERVAL_S   = float(os.getenv("LEADLAG_LOOP_INTERVAL_S", "1.0"))
TASK_CHUNK        = int(os.getenv("LEADLAG_TASK_CHUNK",        "50"))

# Regime gate defaults — should match arb schema defaults
SPREAD_BPS_MAX      = float(os.getenv("LEADLAG_SPREAD_BPS_MAX",      "8"))
VOL_BPS_MIN         = float(os.getenv("LEADLAG_VOL_BPS_MIN",         "2"))
VOL_BPS_MAX         = float(os.getenv("LEADLAG_VOL_BPS_MAX",         "250"))
LIQ_SCORE_MIN       = float(os.getenv("LEADLAG_LIQ_SCORE_MIN",       "0.50"))
XCORR_MIN           = float(os.getenv("LEADLAG_XCORR_MIN",           "0.30"))
BETA_CHANGE_PCT_MAX = float(os.getenv("LEADLAG_BETA_CHANGE_PCT_MAX",  "40.0"))
STALE_AFTER_MS      = int(os.getenv("LEADLAG_STALE_AFTER_MS",        "60000"))

# Optional: auto-publish to Partner Intelligence Layer
PARTNER_ID    = _parse_uuid("LEADLAG_PARTNER_ID")
STRATEGY_ID   = _parse_uuid("LEADLAG_STRATEGY_ID")


# ─────────────────────────── SQL ─────────────────────────────────────────────

# Adapt this query to your canonical feature tables.
# Required output columns per row:
#   bucket_ts (timestamptz), symbol_id (uuid),
#   mid (float8, nullable), ret (float8, nullable),
#   spread_bps (float8), vol_bps (float8), liq_score (float8)
# Provide mid OR ret; if both present, ret takes precedence.
FETCH_SERIES_SQL = """
with base as (
  select
    bucket_ts,
    symbol_id,
    mid::float8       as mid,
    null::float8      as ret,
    spread_bps::float8 as spread_bps,
    vol_bps::float8   as vol_bps,
    liq_score::float8 as liq_score
  from market.micro_features_1s_v1
  where bucket_ts >= (now() - ($1::int * interval '1 second'))
    and symbol_id  = any($2::uuid[])
  order by bucket_ts asc
)
select * from base;
"""

# Fetch active pairs from pair registry.
# Returns: pair_id, leader_symbol_id, lagger_symbol_id, stale_after_ms
# If you don't have the pair registry populated yet, replace with a
# hardcoded list or a cross-join of your symbol universe.
FETCH_PAIRS_SQL = """
select
  pair_id,
  leader_symbol_id,
  lagger_symbol_id,
  stale_after_ms
from arb.leadlag_pair_v1
where status = 'ACTIVE'
order by last_active_ts desc nulls last;
"""

# FIX 1: All v7 schema columns included — model health, beta drift,
# xcorr/beta gates, stale_after_ms. stale_after_ts is maintained by
# trigger trg_leadlag_edge_set_stale_after_ts on insert/update.
UPSERT_EDGE_SQL = """
insert into arb.leadlag_edge_v1 (
  asof_ts,
  pair_id,
  leader_symbol_id,
  lagger_symbol_id,
  model,
  lookback_s,
  horizon_ms,
  xcorr_peak,
  xcorr_lag_ms,
  beta,
  alpha,
  r2,
  tstat_beta,
  n_obs,
  resid_vol_bps,
  resid_mean_bps,
  resid_autocorr,
  z_score_at_compute,
  z_entry_threshold,
  beta_prev,
  beta_change_pct,
  edge_score,
  leader_spread_bps,
  lagger_spread_bps,
  leader_vol_bps,
  lagger_vol_bps,
  leader_liq_score,
  lagger_liq_score,
  spread_bps_max,
  vol_bps_min,
  vol_bps_max,
  liq_score_min,
  xcorr_min,
  beta_change_pct_max,
  gates_passed,
  stale_after_ms,
  compute_ms,
  compute_version,
  notes
)
values (
  $1,  $2,  $3,  $4,
  $5::arb.leadlag_model_v1,
  $6,  $7,  $8,  $9,
  $10, $11, $12, $13,
  $14, $15, $16, $17,
  $18, $19, $20, $21,
  $22, $23, $24, $25,
  $26, $27, $28, $29,
  $30, $31, $32, $33,
  $34, $35, $36, $37,
  $38
)
on conflict (asof_ts, leader_symbol_id, lagger_symbol_id, model, lookback_s, horizon_ms)
do update set
  xcorr_peak          = excluded.xcorr_peak,
  xcorr_lag_ms        = excluded.xcorr_lag_ms,
  beta                = excluded.beta,
  alpha               = excluded.alpha,
  r2                  = excluded.r2,
  tstat_beta          = excluded.tstat_beta,
  n_obs               = excluded.n_obs,
  resid_vol_bps       = excluded.resid_vol_bps,
  resid_mean_bps      = excluded.resid_mean_bps,
  resid_autocorr      = excluded.resid_autocorr,
  z_score_at_compute  = excluded.z_score_at_compute,
  z_entry_threshold   = excluded.z_entry_threshold,
  beta_prev           = excluded.beta_prev,
  beta_change_pct     = excluded.beta_change_pct,
  edge_score          = excluded.edge_score,
  leader_spread_bps   = excluded.leader_spread_bps,
  lagger_spread_bps   = excluded.lagger_spread_bps,
  leader_vol_bps      = excluded.leader_vol_bps,
  lagger_vol_bps      = excluded.lagger_vol_bps,
  leader_liq_score    = excluded.leader_liq_score,
  lagger_liq_score    = excluded.lagger_liq_score,
  spread_bps_max      = excluded.spread_bps_max,
  vol_bps_min         = excluded.vol_bps_min,
  vol_bps_max         = excluded.vol_bps_max,
  liq_score_min       = excluded.liq_score_min,
  xcorr_min           = excluded.xcorr_min,
  beta_change_pct_max = excluded.beta_change_pct_max,
  gates_passed        = excluded.gates_passed,
  stale_after_ms      = excluded.stale_after_ms,
  compute_ms          = excluded.compute_ms,
  compute_version     = excluded.compute_version,
  notes               = excluded.notes
returning edge_id, asof_ts;
"""

# FIX 2: Signal insert includes edge_asof_ts (partition-pruned publish),
# pair_id, resid_vol_bps, z_score, z_entry_threshold, compute_version.
INSERT_SIGNAL_SQL = """
insert into arb.leadlag_signal_v1 (
  valid_from_ts,
  valid_to_ts,
  edge_id,
  edge_asof_ts,
  pair_id,
  leader_symbol_id,
  lagger_symbol_id,
  side,
  horizon_ms,
  edge_score,
  confidence,
  expected_move_bps,
  stop_bps,
  take_profit_bps,
  max_hold_ms,
  gates_passed,
  leader_spread_bps,
  lagger_spread_bps,
  leader_vol_bps,
  lagger_vol_bps,
  leader_liq_score,
  lagger_liq_score,
  resid_vol_bps,
  z_score,
  z_entry_threshold,
  compute_version,
  payload
)
values (
  $1,  $2,  $3,  $4,  $5,
  $6,  $7,
  $8::arb.leadlag_signal_side_v1,
  $9,  $10, $11, $12,
  $13, $14, $15,
  $16, $17, $18, $19,
  $20, $21, $22,
  $23, $24, $25,
  $26, $27::jsonb
)
returning signal_id;
"""

# FIX 3: Dedupe checked explicitly before insert; conflict logged.

# FIX 4: Correct 5-parameter publish function signature.
# p_strategy_id (4th) is uuid or null; p_route_hint (5th) is text or null.
PUBLISH_SIGNAL_SQL = """
select opportunity_id, event_ts, delivery_id
from arb.fn_publish_leadlag_signal_to_intel_v1(
  $1::uuid,   -- p_signal_id
  $2::uuid,   -- p_partner_id
  $3::text,   -- p_delivery_method
  $4::uuid,   -- p_strategy_id  (null ok)
  $5::text    -- p_route_hint   (null ok)
);
"""

# Check for existing signal on same edge+side+window to avoid silent drops.
CHECK_SIGNAL_EXISTS_SQL = """
select signal_id
from arb.leadlag_signal_v1
where edge_id       = $1
  and side          = $2::arb.leadlag_signal_side_v1
  and valid_from_ts = $3
  and valid_to_ts   = $4
limit 1;
"""

# Update pair performance summary after each cycle.
UPDATE_PAIR_ACTIVE_SQL = """
update arb.leadlag_pair_v1
set last_active_ts  = $1,
    best_horizon_ms = case
      when best_xcorr_peak is null or $3 > best_xcorr_peak
      then $2 else best_horizon_ms end,
    best_xcorr_peak = case
      when best_xcorr_peak is null or $3 > best_xcorr_peak
      then $3 else best_xcorr_peak end
where pair_id = $4;
"""


# ─────────────────────────── Data structures ─────────────────────────────────

@dataclass
class PairConfig:
    pair_id: uuid.UUID
    leader_id: str
    lagger_id: str
    stale_after_ms: int


@dataclass
class EdgeStats:
    """All computed statistics for one (leader, lagger, horizon) edge."""
    asof_ts: datetime
    peak_corr: Optional[float]
    peak_lag_ms: int
    beta: Optional[float]
    beta_prev: Optional[float]          # from prior cycle via DB lookup (None on first run)
    beta_change_pct: Optional[float]
    alpha: Optional[float]
    r2: Optional[float]
    t_beta: Optional[float]
    n_obs: int
    resid_vol_bps: Optional[float]      # σ_e
    resid_mean_bps: Optional[float]     # μ_e
    resid_autocorr: Optional[float]     # AR(1) of e(t)
    z_score: Optional[float]            # z(t) at compute time
    z_entry_threshold: Optional[float]  # cost-adjusted entry threshold
    edge_score: float
    # regime
    leader_spread_bps: Optional[float]
    lagger_spread_bps: Optional[float]
    leader_vol_bps: Optional[float]
    lagger_vol_bps: Optional[float]
    leader_liq_score: Optional[float]
    lagger_liq_score: Optional[float]
    gates_passed: bool
    compute_ms: int


@dataclass
class RegimeSnapshot:
    leader_spread_bps: Optional[float]
    lagger_spread_bps: Optional[float]
    leader_vol_bps: Optional[float]
    lagger_vol_bps: Optional[float]
    leader_liq_score: Optional[float]
    lagger_liq_score: Optional[float]

    def gates_passed(self) -> bool:
        vals = [
            self.leader_spread_bps, self.lagger_spread_bps,
            self.leader_vol_bps,    self.lagger_vol_bps,
            self.leader_liq_score,  self.lagger_liq_score,
        ]
        if any(v is None or math.isnan(v) for v in vals):
            return False
        if self.leader_spread_bps > SPREAD_BPS_MAX or self.lagger_spread_bps > SPREAD_BPS_MAX:
            return False
        if self.leader_vol_bps < VOL_BPS_MIN or self.lagger_vol_bps < VOL_BPS_MIN:
            return False
        if self.leader_vol_bps > VOL_BPS_MAX or self.lagger_vol_bps > VOL_BPS_MAX:
            return False
        if self.leader_liq_score < LIQ_SCORE_MIN or self.lagger_liq_score < LIQ_SCORE_MIN:
            return False
        return True


# ─────────────────────────── Math helpers ────────────────────────────────────

def _nanf(v: float) -> Optional[float]:
    """Return None if v is nan/inf, else float. Safe for DB params."""
    if v is None or not math.isfinite(v):
        return None
    return float(v)


def safe_confidence_from_tstat(t: Optional[float]) -> float:
    """Map t-stat magnitude to bounded [0,1) confidence proxy."""
    if t is None or not math.isfinite(t):
        return 0.0
    return float(1.0 - math.exp(-abs(t) / 3.0))


def rolling_log_returns(mid: np.ndarray) -> np.ndarray:
    mid = np.asarray(mid, dtype=np.float64)
    mid = np.where(mid <= 0, np.nan, mid)
    return np.diff(np.log(mid))


def align_for_lag(
    x: np.ndarray, y: np.ndarray, lag_steps: int
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Positive lag_steps: x[t] leads y[t + lag_steps].
    Returns aligned (x_shifted, y_shifted) slices.
    """
    if lag_steps <= 0:
        return x, y
    if len(x) <= lag_steps or len(y) <= lag_steps:
        return np.empty(0, dtype=np.float64), np.empty(0, dtype=np.float64)
    return x[:-lag_steps], y[lag_steps:]


def find_xcorr_peak(
    x: np.ndarray, y: np.ndarray, lag_steps_list: List[int]
) -> Tuple[float, int]:
    """
    Scan lags and return (peak_corr, peak_lag_steps).
    Minimum 30 aligned observations required per lag.
    """
    best_corr: float = 0.0
    best_lag: int = 0

    x0 = x - np.nanmean(x)
    y0 = y - np.nanmean(y)

    for lag in lag_steps_list:
        xa, ya = align_for_lag(x0, y0, lag)
        if xa.size < 30:
            continue
        sx = np.nanstd(xa)
        sy = np.nanstd(ya)
        denom = sx * sy
        if denom <= 0 or not math.isfinite(denom):
            continue
        c = float(np.nanmean(xa * ya) / denom)
        if abs(c) > abs(best_corr):
            best_corr = c
            best_lag = lag

    return float(best_corr), int(best_lag)


def ols_stats(
    x: np.ndarray, y: np.ndarray
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    OLS: y = alpha + beta * x
    Returns (alpha, beta, r2, tstat_beta). All None if insufficient data.
    Minimum 50 finite observations required.
    """
    x = np.asarray(x, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    mask = np.isfinite(x) & np.isfinite(y)
    x, y = x[mask], y[mask]
    n = x.size
    if n < 50:
        return None, None, None, None

    X = np.column_stack([np.ones(n), x])
    XtX = X.T @ X
    try:
        XtX_inv = np.linalg.inv(XtX)
    except np.linalg.LinAlgError:
        return None, None, None, None

    b = XtX_inv @ (X.T @ y)
    alpha = float(b[0])
    beta  = float(b[1])

    y_hat  = X @ b
    resid  = y - y_hat
    ss_res = float(resid @ resid)
    ss_tot = float(((y - y.mean()) ** 2).sum())
    r2     = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else None

    dof = n - 2
    if dof <= 0 or XtX_inv[1, 1] <= 0:
        return alpha, beta, r2, None

    sigma2  = ss_res / dof
    se_beta = math.sqrt(float(sigma2 * XtX_inv[1, 1]))
    t_beta  = float(beta / se_beta) if se_beta > 0 else None

    return alpha, beta, r2, t_beta


def compute_residual_stats(
    x: np.ndarray, y: np.ndarray, alpha: float, beta: float
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Compute innovation process stats on aligned (x, y).
    Returns (resid_vol_bps, resid_mean_bps, resid_autocorr).
    resid_vol_bps = σ_e = std of e(t) = y - (alpha + beta*x), in bps.
    resid_mean_bps = μ_e = mean of e(t).
    resid_autocorr = AR(1) coefficient of e(t). Should be ~0 if model is
                     correctly specified. Serial correlation signals β or
                     τ* has drifted and is a regime gate candidate.
    """
    mask = np.isfinite(x) & np.isfinite(y)
    x, y = x[mask], y[mask]
    if x.size < 10:
        return None, None, None

    e = y - (alpha + beta * x)
    e_bps = e * 10000.0   # convert log-return residuals to bps

    resid_vol_bps  = float(np.std(e_bps))  if e_bps.size > 1  else None
    resid_mean_bps = float(np.mean(e_bps)) if e_bps.size > 0  else None

    # AR(1): regress e(t) on e(t-1)
    resid_autocorr: Optional[float] = None
    if e_bps.size >= 10:
        e_lag  = e_bps[:-1]
        e_curr = e_bps[1:]
        mask2  = np.isfinite(e_lag) & np.isfinite(e_curr)
        if mask2.sum() >= 10:
            cov  = np.cov(e_lag[mask2], e_curr[mask2])
            denom = cov[0, 0]
            resid_autocorr = float(cov[0, 1] / denom) if denom > 0 else None

    return resid_vol_bps, resid_mean_bps, resid_autocorr


def compute_z_score_and_threshold(
    e_t: float,
    resid_mean_bps: Optional[float],
    resid_vol_bps: Optional[float],
    leader_spread_bps: Optional[float],
    lagger_spread_bps: Optional[float],
) -> Tuple[Optional[float], Optional[float]]:
    """
    z(t) = (e(t) - μ_e) / σ_e
    z_entry_threshold = (spread_cost_bps) / σ_e  (cost-adjusted entry bar)
    Returns (z_score, z_entry_threshold).
    """
    if resid_vol_bps is None or resid_vol_bps <= 0:
        return None, None

    mean = resid_mean_bps or 0.0
    z = float((e_t - mean) / resid_vol_bps)

    # Entry threshold: must exceed round-trip spread cost
    if leader_spread_bps is not None and lagger_spread_bps is not None:
        spread_cost = (leader_spread_bps + lagger_spread_bps) / 2.0
        z_threshold = spread_cost / resid_vol_bps if resid_vol_bps > 0 else None
    else:
        z_threshold = None

    return z, z_threshold


def compute_edge_score(
    xcorr: Optional[float],
    r2: Optional[float],
    tstat: Optional[float],
    n_obs: int,
) -> float:
    """
    Composite edge score. Higher = stronger edge.
    Weights: t-stat (2x), correlation (10x), r2 (5x).
    Replace with AELC-style scoring when available.
    """
    if n_obs <= 0:
        return 0.0
    x  = abs(xcorr)        if xcorr is not None and math.isfinite(xcorr) else 0.0
    rr = max(0.0, min(1.0, r2)) if r2 is not None and math.isfinite(r2) else 0.0
    t  = min(20.0, abs(tstat))  if tstat is not None and math.isfinite(tstat) else 0.0
    return float(2.0 * t + 10.0 * x + 5.0 * rr)


# ─────────────────────────── Series helpers ──────────────────────────────────

def build_symbol_series(
    rows: List[asyncpg.Record],
) -> Dict[str, Dict[str, np.ndarray]]:
    """
    Returns dict: symbol_id -> {ts, ret, spread_bps, vol_bps, liq_score}.
    FIX 7: variable shadowing eliminated — loop vars renamed to avoid
    collision with outer scope and comprehension vars.
    """
    by_sym: Dict[str, List[asyncpg.Record]] = {}
    for rec in rows:
        sid = str(rec["symbol_id"])
        by_sym.setdefault(sid, []).append(rec)

    out: Dict[str, Dict[str, np.ndarray]] = {}
    for sid, sym_rows in by_sym.items():
        sym_rows = sorted(sym_rows, key=lambda r: r["bucket_ts"])

        ts_arr = np.array(
            [r["bucket_ts"].timestamp() for r in sym_rows], dtype=np.float64
        )

        raw_mid = [r["mid"] for r in sym_rows]
        raw_ret = [r["ret"] for r in sym_rows]

        has_ret = raw_ret[0] is not None
        has_mid = raw_mid[0] is not None

        if has_ret and not np.all(np.isnan(np.array(raw_ret, dtype=np.float64))):
            ret_arr    = np.array(raw_ret,                       dtype=np.float64)
            spread_arr = np.array([r["spread_bps"] for r in sym_rows], dtype=np.float64)
            vol_arr    = np.array([r["vol_bps"]    for r in sym_rows], dtype=np.float64)
            liq_arr    = np.array([r["liq_score"]  for r in sym_rows], dtype=np.float64)
        elif has_mid:
            mid_arr    = np.array(raw_mid, dtype=np.float64)
            log_rets   = rolling_log_returns(mid_arr)
            # log_rets length is n-1; trim ts and regime arrays to match
            ts_arr     = ts_arr[1:]
            ret_arr    = log_rets
            spread_arr = np.array([r["spread_bps"] for r in sym_rows], dtype=np.float64)[1:]
            vol_arr    = np.array([r["vol_bps"]    for r in sym_rows], dtype=np.float64)[1:]
            liq_arr    = np.array([r["liq_score"]  for r in sym_rows], dtype=np.float64)[1:]
        else:
            log.debug("Symbol %s: no usable mid or ret series, skipping", sid)
            continue

        out[sid] = {
            "ts":         ts_arr,
            "ret":        ret_arr,
            "spread_bps": spread_arr,
            "vol_bps":    vol_arr,
            "liq_score":  liq_arr,
        }
    return out


def latest_regime(series: Dict[str, np.ndarray]) -> Tuple[float, float, float]:
    """Return last finite spread/vol/liq values, or nan if none exist."""
    def last_finite(arr: np.ndarray) -> float:
        idx = np.where(np.isfinite(arr))[0]
        return float(arr[idx[-1]]) if idx.size > 0 else float("nan")

    return (
        last_finite(series["spread_bps"]),
        last_finite(series["vol_bps"]),
        last_finite(series["liq_score"]),
    )


# ─────────────────────────── Beta drift ──────────────────────────────────────

async def fetch_prev_beta(
    conn: asyncpg.Connection,
    leader_id: str,
    lagger_id: str,
) -> Optional[float]:
    """
    Fetch the most recent beta for this pair from the edge table.
    Used to compute beta_change_pct for drift gating.
    Returns None if no prior edge exists.
    """
    row = await conn.fetchrow(
        """
        select beta
        from arb.leadlag_edge_v1
        where leader_symbol_id = $1
          and lagger_symbol_id = $2
          and beta is not null
        order by asof_ts desc
        limit 1
        """,
        uuid.UUID(leader_id),
        uuid.UUID(lagger_id),
    )
    if row and row["beta"] is not None:
        return float(row["beta"])
    return None


# ─────────────────────────── Core compute ────────────────────────────────────

async def compute_and_persist_pair(
    pool: asyncpg.Pool,
    pair: PairConfig,
    asof_ts: datetime,
    leader_series: Dict[str, np.ndarray],
    lagger_series: Dict[str, np.ndarray],
) -> None:
    """
    FIX 6: acquires its own connection from the pool so asyncio.gather
    across pairs provides real concurrency — no shared connection.
    """
    t0 = time.perf_counter()

    # Convert ms config to sample steps
    max_steps  = max(1, int((MAX_LAG_MS   / 1000.0) / BUCKET_S))
    step_steps = max(1, int((LAG_STEP_MS  / 1000.0) / BUCKET_S))
    lag_steps_list = list(range(0, max_steps + 1, step_steps))

    x = leader_series["ret"]
    y = lagger_series["ret"]

    n = min(len(x), len(y))
    if n < 120:
        log.debug(
            "Pair %s→%s: insufficient obs (%d < 120), skipping",
            pair.leader_id, pair.lagger_id, n,
        )
        return

    x = x[-n:]
    y = y[-n:]

    # ── Step 1: τ* discovery via cross-correlation scan ──────────────────
    peak_corr, peak_lag_steps = find_xcorr_peak(x, y, lag_steps_list)
    peak_lag_ms = int(peak_lag_steps * BUCKET_S * 1000)

    # ── Step 2: OLS at peak lag ───────────────────────────────────────────
    xa, ya = align_for_lag(x, y, peak_lag_steps)
    alpha, beta, r2, t_beta = ols_stats(xa, ya)
    n_obs = int(xa.size)

    # ── Step 3: Model health — residual process stats ─────────────────────
    resid_vol_bps = resid_mean_bps = resid_autocorr = None
    z_score = z_entry_threshold = None

    if alpha is not None and beta is not None and xa.size >= 10:
        resid_vol_bps, resid_mean_bps, resid_autocorr = compute_residual_stats(
            xa, ya, alpha, beta
        )
        # z(t) at the last aligned observation
        if ya.size > 0 and xa.size > 0:
            last_e_bps = float((ya[-1] - (alpha + beta * xa[-1])) * 10000.0)
            l_spread, _, _ = latest_regime(leader_series)
            g_spread, _, _ = latest_regime(lagger_series)
            z_score, z_entry_threshold = compute_z_score_and_threshold(
                last_e_bps, resid_mean_bps, resid_vol_bps, l_spread, g_spread
            )

    # ── Step 4: Beta drift ────────────────────────────────────────────────
    # Fetch prior beta async before acquiring write connection
    async with pool.acquire() as read_conn:
        beta_prev = await fetch_prev_beta(
            read_conn, pair.leader_id, pair.lagger_id
        )

    beta_change_pct: Optional[float] = None
    if beta is not None and beta_prev is not None and abs(beta_prev) > 0:
        beta_change_pct = float((beta - beta_prev) / abs(beta_prev) * 100.0)

    # ── Step 5: Regime snapshot ───────────────────────────────────────────
    l_spread, l_vol, l_liq = latest_regime(leader_series)
    g_spread, g_vol, g_liq = latest_regime(lagger_series)

    regime = RegimeSnapshot(
        leader_spread_bps = _nanf(l_spread),
        lagger_spread_bps = _nanf(g_spread),
        leader_vol_bps    = _nanf(l_vol),
        lagger_vol_bps    = _nanf(g_vol),
        leader_liq_score  = _nanf(l_liq),
        lagger_liq_score  = _nanf(g_liq),
    )

    # Additional gates: xcorr and beta drift
    xcorr_gate_ok  = (
        peak_corr is not None
        and math.isfinite(peak_corr)
        and abs(peak_corr) >= XCORR_MIN
    )
    beta_drift_ok  = (
        beta_change_pct is None
        or abs(beta_change_pct) <= BETA_CHANGE_PCT_MAX
    )
    gates_ok = regime.gates_passed() and xcorr_gate_ok and beta_drift_ok

    score = compute_edge_score(peak_corr, r2, t_beta, n_obs)

    compute_ms = int((time.perf_counter() - t0) * 1000)

    # ── Step 6: Persist edge (own connection) ─────────────────────────────
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            UPSERT_EDGE_SQL,
            # $1–$4: identifiers
            asof_ts,
            pair.pair_id,
            uuid.UUID(pair.leader_id),
            uuid.UUID(pair.lagger_id),
            # $5–$9: model config + xcorr
            "BOTH",
            LOOKBACK_S,
            peak_lag_ms,
            _nanf(peak_corr),
            peak_lag_ms,          # xcorr_lag_ms = peak_lag_ms in single-scan mode
            # $10–$14: OLS
            _nanf(beta)   if beta   is not None else None,
            _nanf(alpha)  if alpha  is not None else None,
            _nanf(r2)     if r2     is not None else None,
            _nanf(t_beta) if t_beta is not None else None,
            n_obs,
            # $15–$19: model health
            _nanf(resid_vol_bps),
            _nanf(resid_mean_bps),
            _nanf(resid_autocorr),
            _nanf(z_score),
            _nanf(z_entry_threshold),
            # $20–$21: beta drift
            _nanf(beta_prev),
            _nanf(beta_change_pct),
            # $22: edge score
            float(score),
            # $23–$28: regime snapshot
            _nanf(l_spread), _nanf(g_spread),
            _nanf(l_vol),    _nanf(g_vol),
            _nanf(l_liq),    _nanf(g_liq),
            # $29–$34: gate thresholds
            SPREAD_BPS_MAX,
            VOL_BPS_MIN,
            VOL_BPS_MAX,
            LIQ_SCORE_MIN,
            XCORR_MIN,
            BETA_CHANGE_PCT_MAX,
            # $35–$38: control
            gates_ok,
            pair.stale_after_ms,
            compute_ms,
            "leadlag_v2",
            None,  # notes
        )

        if row is None:
            log.warning(
                "Edge upsert returned no row for pair %s→%s",
                pair.leader_id, pair.lagger_id,
            )
            return

        edge_id  = row["edge_id"]
        edge_asof_ts = row["asof_ts"]

        # Update pair registry best config if improved
        if peak_corr is not None and math.isfinite(peak_corr):
            await conn.execute(
                UPDATE_PAIR_ACTIVE_SQL,
                asof_ts,
                peak_lag_ms,
                abs(peak_corr),
                pair.pair_id,
            )

        # ── Step 7: Signal logic ──────────────────────────────────────────
        if not gates_ok:
            return
        if score < EDGE_THRESHOLD:
            return
        if beta is None or peak_corr is None:
            return

        side = (
            "LONG_LAGGER_SHORT_LEADER"
            if beta > 0
            else "SHORT_LAGGER_LONG_LEADER"
        )

        confidence = float(max(0.0, min(1.0,
            0.25 * safe_confidence_from_tstat(t_beta)
            + 0.25 * min(1.0, abs(peak_corr))
            + 0.50 * min(1.0, score / (EDGE_THRESHOLD * 2.0))
        )))
        if confidence < CONFIDENCE_MIN:
            return

        std_x = float(np.nanstd(xa)) if xa.size > 10 else 0.0
        expected_move_bps = float(abs(beta) * std_x * 10000.0)

        now_ts   = datetime.now(timezone.utc)
        valid_to = now_ts + timedelta(milliseconds=SIGNAL_TTL_MS)

        # FIX 3: check for existing signal explicitly before insert
        existing = await conn.fetchval(
            CHECK_SIGNAL_EXISTS_SQL,
            edge_id, side, now_ts, valid_to,
        )
        if existing is not None:
            log.debug(
                "Signal already exists for edge %s side=%s, skipping duplicate",
                edge_id, side,
            )
            return

        payload = {
            "engine":      "leadlag_v2",
            "asof_ts":     asof_ts.isoformat(),
            "lookback_s":  LOOKBACK_S,
            "peak_corr":   _nanf(peak_corr),
            "peak_lag_ms": peak_lag_ms,
            "alpha":       _nanf(alpha),
            "beta":        _nanf(beta),
            "beta_prev":   _nanf(beta_prev),
            "r2":          _nanf(r2),
            "t_beta":      _nanf(t_beta),
            "n_obs":       n_obs,
            "resid_vol_bps":   _nanf(resid_vol_bps),
            "resid_mean_bps":  _nanf(resid_mean_bps),
            "resid_autocorr":  _nanf(resid_autocorr),
            "z_score":         _nanf(z_score),
            "z_entry_threshold": _nanf(z_entry_threshold),
            "regime": {
                "spread_bps_max":      SPREAD_BPS_MAX,
                "vol_bps_min":         VOL_BPS_MIN,
                "vol_bps_max":         VOL_BPS_MAX,
                "liq_score_min":       LIQ_SCORE_MIN,
                "xcorr_min":           XCORR_MIN,
                "beta_change_pct_max": BETA_CHANGE_PCT_MAX,
                "leader": {
                    "spread_bps": _nanf(l_spread),
                    "vol_bps":    _nanf(l_vol),
                    "liq_score":  _nanf(l_liq),
                },
                "lagger": {
                    "spread_bps": _nanf(g_spread),
                    "vol_bps":    _nanf(g_vol),
                    "liq_score":  _nanf(g_liq),
                },
            },
        }

        # FIX 2: include edge_asof_ts, pair_id, model health columns
        signal_id = await conn.fetchval(
            INSERT_SIGNAL_SQL,
            now_ts,          # $1  valid_from_ts
            valid_to,        # $2  valid_to_ts
            edge_id,         # $3  edge_id  (soft ref)
            edge_asof_ts,    # $4  edge_asof_ts — enables partition-pruned publish
            pair.pair_id,    # $5  pair_id
            uuid.UUID(pair.leader_id),   # $6
            uuid.UUID(pair.lagger_id),   # $7
            side,            # $8  side enum
            peak_lag_ms,     # $9  horizon_ms
            float(score),    # $10 edge_score
            confidence,      # $11 confidence
            expected_move_bps, # $12
            None,            # $13 stop_bps
            None,            # $14 take_profit_bps
            SIGNAL_TTL_MS,   # $15 max_hold_ms
            True,            # $16 gates_passed
            _nanf(l_spread), # $17
            _nanf(g_spread), # $18
            _nanf(l_vol),    # $19
            _nanf(g_vol),    # $20
            _nanf(l_liq),    # $21
            _nanf(g_liq),    # $22
            _nanf(resid_vol_bps),      # $23
            _nanf(z_score),            # $24
            _nanf(z_entry_threshold),  # $25
            "leadlag_v2",              # $26 compute_version
            json.dumps(payload),       # $27 payload
        )

        if signal_id is None:
            log.warning(
                "Signal insert returned None for edge %s side=%s "
                "(unexpected after explicit dedupe check)",
                edge_id, side,
            )
            return

        log.info(
            "Signal %s | score=%.2f conf=%.2f lag=%dms side=%s "
            "leader=%s lagger=%s z=%.2f",
            signal_id, score, confidence, peak_lag_ms, side,
            pair.leader_id, pair.lagger_id,
            z_score if z_score is not None else float("nan"),
        )

        # ── Step 8: Publish to intel (optional) ──────────────────────────
        if PARTNER_ID is not None:
            try:
                # FIX 4: correct 5-param signature
                # p_strategy_id ($4) is uuid|None, p_route_hint ($5) is text|None
                pub_row = await conn.fetchrow(
                    PUBLISH_SIGNAL_SQL,
                    signal_id,            # $1 p_signal_id
                    PARTNER_ID,           # $2 p_partner_id (uuid object)
                    "API",                # $3 p_delivery_method
                    STRATEGY_ID,          # $4 p_strategy_id (uuid|None)
                    None,                 # $5 p_route_hint
                )
                if pub_row:
                    log.info(
                        "Published signal %s → opportunity %s partner %s",
                        signal_id, pub_row["opportunity_id"], PARTNER_ID,
                    )
            except Exception as exc:
                log.warning(
                    "Signal %s created but publish failed: %s",
                    signal_id, exc,
                )


# ─────────────────────────── Pair loader ─────────────────────────────────────

async def load_pairs(conn: asyncpg.Connection) -> List[PairConfig]:
    """
    Load active pairs from pair registry.
    Falls back to arb.leadlag_watchlist_v1 for backwards compatibility.
    """
    try:
        rows = await conn.fetch(FETCH_PAIRS_SQL)
        if not rows:
            raise RuntimeError(
                "arb.leadlag_pair_v1 has no ACTIVE pairs. "
                "Insert rows with status = 'ACTIVE' or promote candidates."
            )
        pairs = [
            PairConfig(
                pair_id        = row["pair_id"],
                leader_id      = str(row["leader_symbol_id"]),
                lagger_id      = str(row["lagger_symbol_id"]),
                stale_after_ms = int(row["stale_after_ms"]),
            )
            for row in rows
        ]
        log.info("Loaded %d active pairs from pair registry", len(pairs))
        return pairs

    except asyncpg.UndefinedTableError:
        raise RuntimeError(
            "arb.leadlag_pair_v1 not found. "
            "Deploy arb schema v7 first, then populate the pair registry."
        )


# ─────────────────────────── Main loop ───────────────────────────────────────

async def main() -> None:
    pool = await asyncpg.create_pool(
        PG_CONN,
        min_size=2,
        max_size=max(10, TASK_CHUNK // 5),
        command_timeout=60,
    )
    log.info(
        "LeadLag worker v2 online. "
        "lookback_s=%d max_lag_ms=%d step_ms=%d interval_s=%.1f",
        LOOKBACK_S, MAX_LAG_MS, LAG_STEP_MS, LOOP_INTERVAL_S,
    )
    if PARTNER_ID:
        log.info("Auto-publish enabled → partner %s", PARTNER_ID)
    if STRATEGY_ID:
        log.info("Strategy tag → %s", STRATEGY_ID)

    # FIX 8: pool teardown in try/finally
    try:
        async with pool.acquire() as conn:
            pairs = await load_pairs(conn)

        all_symbol_ids = list({
            sid
            for p in pairs
            for sid in (p.leader_id, p.lagger_id)
        })

        while True:
            # FIX 9: cadence drift fix — track loop start, sleep remainder
            loop_start = time.perf_counter()

            try:
                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        FETCH_SERIES_SQL,
                        LOOKBACK_S,
                        [uuid.UUID(s) for s in all_symbol_ids],
                    )

                series_map = build_symbol_series(rows)

                live_pair_ids = {
                    sid for sid in all_symbol_ids if sid in series_map
                }
                live_pairs = [
                    p for p in pairs
                    if p.leader_id in live_pair_ids
                    and p.lagger_id in live_pair_ids
                ]

                if len(live_pairs) == 0:
                    log.warning("No live pairs with data this cycle")
                else:
                    asof_ts = datetime.now(timezone.utc)

                    # FIX 6: each task acquires its own connection from pool
                    tasks = [
                        compute_and_persist_pair(
                            pool, pair, asof_ts,
                            series_map[pair.leader_id],
                            series_map[pair.lagger_id],
                        )
                        for pair in live_pairs
                    ]

                    # Chunked gather — limits concurrent pool acquisitions
                    for k in range(0, len(tasks), TASK_CHUNK):
                        await asyncio.gather(*tasks[k : k + TASK_CHUNK])

                    log.debug(
                        "Cycle complete: %d pairs, %.0fms",
                        len(live_pairs),
                        (time.perf_counter() - loop_start) * 1000,
                    )

            except Exception:
                log.exception("Loop error — continuing")

            # FIX 9: sleep only the remaining interval budget
            elapsed = time.perf_counter() - loop_start
            sleep_s = max(0.0, LOOP_INTERVAL_S - elapsed)
            await asyncio.sleep(sleep_s)

    finally:
        await pool.close()
        log.info("Pool closed. Worker stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass