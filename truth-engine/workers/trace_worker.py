"""
Noterminal — FINRA TRACE Bond Flow Worker v2.0.0
Source: FINRA TRACE public EOD API — no auth required
Target: credit.trace_bond_signals, credit.trace_rolling_baseline
Emits: market.edge_signals_v1 (BOND_FLOW_PRESSURE)
Schedule: Nightly, runs at 08:00 ET (after FINRA publishes prior day)
Canon: restart-loop managed by start.sh
"""

import os
import time
import logging
import requests
import json
from datetime import datetime, timezone, date, timedelta
from math import sqrt

from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRACE] %(levelname)s %(message)s"
)
log = logging.getLogger("trace_worker")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

WORKER_VERSION = "2.0.0"
WINDOW_DAYS    = 20

# BFP composite weights
W_VOL   = 0.40
W_COUNT = 0.30
W_YIELD = 0.20
W_PRICE = 0.10

BFP_THRESHOLD   = 0.5
MIN_CUSIP_COUNT = 2

# FINRA TRACE public API
FINRA_BASE = "https://api.finra.org/data/group/otcMarket/name/tradesAgg"


def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ── Math helpers ─────────────────────────────────────────────────────────

def mean(arr: list[float]) -> float:
    return sum(arr) / len(arr) if arr else 0.0

def stddev(arr: list[float], m: float = None) -> float:
    if len(arr) < 2:
        return 0.0
    mu = m if m is not None else mean(arr)
    return sqrt(sum((v - mu) ** 2 for v in arr) / (len(arr) - 1))

def safe_z(value: float, mu: float, sd: float) -> float | None:
    if not sd:
        return None
    return (value - mu) / sd


# ── BFP score ────────────────────────────────────────────────────────────

def compute_bfp(par_vol_z, count_z, yield_z, price_z):
    components = []
    if par_vol_z is not None: components.append((W_VOL,   par_vol_z))
    if count_z   is not None: components.append((W_COUNT,  count_z))
    if yield_z   is not None: components.append((W_YIELD, -yield_z))  # yield compression = buying
    if price_z   is not None: components.append((W_PRICE,  price_z))
    if not components:
        return None, 0, "LOW"
    total_w = sum(w for w, _ in components)
    score = sum((w / total_w) * v for w, v in components)
    direction = 1 if score >= BFP_THRESHOLD else (-1 if score <= -BFP_THRESHOLD else 0)
    confidence = "HIGH" if len(components) == 4 else ("MED" if len(components) >= 2 else "LOW")
    return score, direction, confidence


# ── FINRA fetch ──────────────────────────────────────────────────────────

def fetch_trace_agg(cusip_prefix: str, trade_date: str, session: requests.Session) -> list[dict]:
    """
    Fetch FINRA TRACE aggregated bond trades for a CUSIP prefix on a given date.
    Uses FINRA's public REST API — no auth needed.
    """
    compare_filter = json.dumps([{
        "compareType": "STARTSWITH",
        "fieldName": "cusip",
        "fieldValue": cusip_prefix
    }])
    date_filter = json.dumps([{
        "startDate": trade_date,
        "endDate": trade_date,
        "fieldName": "tradeDate"
    }])

    params = {
        "limit": "50",
        "offset": "0",
        "fields": "cusip,tradeDate,totalParAmt,tradeCount,avgPrice,highPrice,lowPrice,avgYield",
        "compareFilters": compare_filter,
        "dateRangeFilters": date_filter,
    }

    try:
        resp = session.get(FINRA_BASE, params=params, timeout=30,
                           headers={"Accept": "application/json"})
        if resp.status_code == 404:
            return []  # No trades on this date (weekend/holiday)
        if not resp.ok:
            log.warning(f"FINRA fetch failed {cusip_prefix}/{trade_date}: {resp.status}")
            return []
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        log.warning(f"FINRA fetch error {cusip_prefix}: {e}")
        return []


# ── Historical baseline ──────────────────────────────────────────────────

def fetch_historical(supabase: Client, equity_symbol: str, before_date: str) -> dict:
    result = supabase.schema("credit").table("trace_bond_signals").select(
        "par_volume_usd, meta"
    ).eq("equity_symbol", equity_symbol).lt(
        "signal_date", before_date
    ).order("signal_date", desc=True).limit(WINDOW_DAYS).execute()

    par_vols, trade_counts, yields = [], [], []
    for row in (result.data or []):
        if row.get("par_volume_usd"):
            par_vols.append(float(row["par_volume_usd"]))
        meta = row.get("meta") or {}
        if meta.get("trade_count"):
            trade_counts.append(float(meta["trade_count"]))
        if meta.get("weighted_yield"):
            yields.append(float(meta["weighted_yield"]))

    return {"par_vols": par_vols, "trade_counts": trade_counts, "yields": yields}


# ── Emit to market.edge_signals_v1 ───────────────────────────────────────

def emit_edge_signal(supabase: Client, equity_symbol: str, trade_date: str,
                     score: float, direction: int, confidence: str):
    if direction == 0:
        return
    import uuid
    strength = min(abs(score), 1.0) if score else 0.0
    try:
        supabase.schema("market").table("edge_signals_v1").insert({
            "signal_id":         str(uuid.uuid4()),
            "instrument_symbol": equity_symbol,
            "bucket_ts":         f"{trade_date}T08:00:00+00:00",
            "emitted_at":        datetime.now(timezone.utc).isoformat(),
            "edge_label":        "BOND_FLOW_PRESSURE",
            "edge_score":        int(strength * 100),
            "edge_direction":    direction,
            "net_flow":          None,
            "net_flow_z":        score,
            "ofi_normalised":    None,
            "ofi_z":             None,
            "toxicity_ratio":    None,
            "flow_momentum":     strength,
            "momentum_direction": direction,
            "persistence_seconds": 86400,
            "flow_direction":    direction,
            "is_flow_dominant":  strength > 0.6,
            "is_toxic_entry":    False,
            "is_exhausted":      False,
            "is_high_confidence": confidence == "HIGH",
            "trade_count":       None,
            "total_vol":         None,
            "vwap":              None,
            "mid_price":         None,
            "worker_version":    WORKER_VERSION,
            "run_id":            str(uuid.uuid4()),
            "meta":              {"confidence": confidence, "source": "FINRA_TRACE"},
        }).execute()
        log.info(f"BOND_FLOW_PRESSURE emitted: {equity_symbol} dir={direction:+d} score={score:.3f} [{confidence}]")
    except Exception as e:
        log.warning(f"Edge signal emit failed {equity_symbol}: {e}")


# ── Main cycle ───────────────────────────────────────────────────────────

def run_once(supabase: Client, session: requests.Session, trade_date: str):
    log.info(f"TRACE cycle for trade_date={trade_date}")

    # Load CUSIP map
    cusip_map = supabase.schema("credit").table("trace_cusip_map").select(
        "cusip_prefix, equity_symbol, issuer_name"
    ).eq("active", True).execute()

    if not cusip_map.data:
        log.error("CUSIP map empty — cannot proceed")
        return

    log.info(f"Loaded {len(cusip_map.data)} CUSIP prefixes")

    # Aggregate by symbol
    aggs = {}
    for row in cusip_map.data:
        prefix   = row["cusip_prefix"]
        symbol   = row["equity_symbol"]
        trades   = fetch_trace_agg(prefix, trade_date, session)

        if not trades:
            continue

        if symbol not in aggs:
            aggs[symbol] = {
                "cusip_count": 0, "par_volume_usd": 0.0,
                "trade_count": 0, "weighted_yield": 0.0,
                "weighted_price": 0.0, "yield_par": 0.0,
            }
        agg = aggs[symbol]

        for t in trades:
            par = float(t.get("totalParAmt") or 0)
            agg["par_volume_usd"]  += par
            agg["trade_count"]     += int(t.get("tradeCount") or 0)
            agg["weighted_price"]  += float(t.get("avgPrice") or 0) * par
            if t.get("avgYield") is not None:
                agg["weighted_yield"] += float(t["avgYield"]) * par
                agg["yield_par"]      += par
            agg["cusip_count"] += 1

    if not aggs:
        log.warning(f"No TRACE data for any symbol on {trade_date} — possible market holiday")
        return

    log.info(f"Aggregated data for {len(aggs)} symbols: {list(aggs.keys())}")

    # Score each symbol
    signal_rows = []
    baseline_rows = []

    for symbol, agg in aggs.items():
        weighted_yield = (agg["weighted_yield"] / agg["yield_par"]) if agg["yield_par"] > 0 else None
        vwap_price     = (agg["weighted_price"] / agg["par_volume_usd"]) if agg["par_volume_usd"] > 0 else None

        # Historical baseline
        hist = fetch_historical(supabase, symbol, trade_date)

        pv_mean = mean(hist["par_vols"])
        pv_std  = stddev(hist["par_vols"], pv_mean)
        tc_mean = mean(hist["trade_counts"])
        tc_std  = stddev(hist["trade_counts"], tc_mean)
        y_mean  = mean(hist["yields"]) if hist["yields"] else None
        y_std   = stddev(hist["yields"], y_mean) if len(hist["yields"]) >= 2 else None

        par_vol_z = safe_z(agg["par_volume_usd"], pv_mean, pv_std)
        count_z   = safe_z(agg["trade_count"],    tc_mean, tc_std)
        yield_z   = safe_z(weighted_yield, y_mean, y_std) if (weighted_yield and y_mean and y_std) else None

        score, direction, confidence = compute_bfp(par_vol_z, count_z, yield_z, None)

        baseline_rows.append({
            "equity_symbol":      symbol,
            "stat_date":          trade_date,
            "window_days":        WINDOW_DAYS,
            "par_vol_mean":       pv_mean,
            "par_vol_stddev":     pv_std,
            "trade_count_mean":   tc_mean,
            "trade_count_stddev": tc_std,
            "yield_mean":         y_mean,
            "yield_stddev":       y_std,
            "sample_n":           len(hist["par_vols"]),
            "updated_at":         datetime.now(timezone.utc).isoformat(),
        })

        signal_rows.append({
            "signal_date":         trade_date,
            "equity_symbol":       symbol,
            "cusip_count":         agg["cusip_count"],
            "par_volume_usd":      agg["par_volume_usd"],
            "par_volume_z":        par_vol_z,
            "trade_count_z":       count_z,
            "yield_compression_z": yield_z,
            "price_drift_z":       None,
            "bfp_score":           score,
            "bfp_direction":       direction,
            "confidence":          confidence,
            "emitted_to_edge":     direction != 0,
            "meta": {
                "trade_count":    agg["trade_count"],
                "weighted_yield": weighted_yield,
                "vwap_price":     vwap_price,
                "window_sample":  len(hist["par_vols"]),
                "worker_version": WORKER_VERSION,
                "ingest_ts":      datetime.now(timezone.utc).isoformat(),
            },
        })

        if direction != 0:
            emit_edge_signal(supabase, symbol, trade_date, score, direction, confidence)

    # Upsert baselines
    if baseline_rows:
        supabase.schema("credit").table("trace_rolling_baseline").upsert(
            baseline_rows, on_conflict="equity_symbol,window_days"
        ).execute()

    # Upsert signals
    if signal_rows:
        supabase.schema("credit").table("trace_bond_signals").upsert(
            signal_rows, on_conflict="signal_date,equity_symbol"
        ).execute()
        log.info(f"TRACE upserted {len(signal_rows)} signal rows for {trade_date}")


def last_business_day() -> str:
    """Return yesterday, skipping weekends."""
    d = date.today() - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d.isoformat()


def already_ran(supabase: Client, trade_date: str) -> bool:
    result = supabase.schema("credit").table("trace_bond_signals").select(
        "signal_date", count="exact"
    ).eq("signal_date", trade_date).execute()
    return (result.count or 0) > 0


def main():
    log.info(f"TRACE Worker v{WORKER_VERSION} starting")
    supabase = get_supabase()
    session  = requests.Session()
    session.headers["User-Agent"] = "Noterminal-TRACE/2.0"

    while True:
        try:
            trade_date = last_business_day()
            if not already_ran(supabase, trade_date):
                run_once(supabase, session, trade_date)
            else:
                log.info(f"TRACE already ingested for {trade_date} — skipping")
        except Exception as e:
            log.error(f"TRACE cycle error: {e}")

        # Sleep until next morning check (6 hours)
        log.info("Sleeping 6h")
        time.sleep(6 * 3600)


if __name__ == "__main__":
    main()
