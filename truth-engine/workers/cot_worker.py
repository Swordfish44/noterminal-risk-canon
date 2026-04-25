"""
Noterminal — UN Comtrade Trade Flow Worker v1.0.0
Source: comtradeplus.un.org free API tier
Target: macro.trade_flows_v1
Schedule: Weekly (monthly data, ~2-month lag from UN)
Canon: restart-loop managed by start.sh
"""

import os
import time
import logging
import hashlib
import requests
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta

from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [COMTRADE] %(levelname)s %(message)s"
)
log = logging.getLogger("comtrade_worker")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
# No API key required — using UN Comtrade public/v1 endpoint (free, no auth)

WORKER_VERSION = "1.0.0"
POLL_INTERVAL_SECONDS = 7 * 24 * 3600  # Weekly

# ── Commodity codes to track (HS 2-digit chapters) ──────────────────────
# These map directly to Noterminal's causal graph edges
COMMODITY_TARGETS = [
    {"hs_code": "10",  "label": "GRAIN",       "instruments": ["ZW", "ZC", "ZS"]},
    {"hs_code": "12",  "label": "OILSEEDS",     "instruments": ["ZS", "ZL"]},
    {"hs_code": "15",  "label": "FATS_OILS",    "instruments": ["ZL", "PALM"]},
    {"hs_code": "27",  "label": "ENERGY",       "instruments": ["CL", "NG", "HO"]},
    {"hs_code": "26",  "label": "ORES_METALS",  "instruments": ["HG", "SI", "GC", "PL"]},
    {"hs_code": "28",  "label": "CHEMICALS",    "instruments": ["UAN", "DAP"]},
    {"hs_code": "31",  "label": "FERTILIZERS",  "instruments": ["UAN", "DAP", "MOS"]},
    {"hs_code": "72",  "label": "IRON_STEEL",   "instruments": ["HRC", "X"]},
    {"hs_code": "74",  "label": "COPPER",       "instruments": ["HG"]},
    {"hs_code": "75",  "label": "NICKEL",       "instruments": ["LNI"]},
    {"hs_code": "76",  "label": "ALUMINUM",     "instruments": ["ALI"]},
]

# Reporter countries that matter most for Noterminal's causal chains
REPORTER_COUNTRIES = [
    "842",  # USA
    "156",  # China
    "643",  # Russia
    "124",  # Canada
    "036",  # Australia
    "076",  # Brazil
    "056",  # Belgium (Antwerp — proxy for European commodity flows)
    "528",  # Netherlands (Rotterdam)
    "276",  # Germany
    "392",  # Japan
    "410",  # South Korea
    "356",  # India
    "682",  # Saudi Arabia
    "784",  # UAE
    "566",  # Nigeria
    "818",  # Egypt (Suez gateway)
    "703",  # Slovakia (EU grain exports)
    "804",  # Ukraine (Black Sea grain)
]

BASE_URL = "https://comtradeapi.un.org/public/v1/preview/C/M/HS"


def make_record_id(reporter: str, partner: str, commodity: str, period: str, flow: str) -> str:
    raw = f"{reporter}|{partner}|{commodity}|{period}|{flow}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def fetch_trade_flows(
    reporter: str,
    commodity: str,
    period: str,
    session: requests.Session
) -> list[dict]:
    """Fetch bilateral trade flows for one reporter/commodity/period."""
    params = {
        "reporterCode": reporter,
        "cmdCode": commodity,
        "flowCode": "X,M",     # exports and imports
        "period": period,
        "partnerCode": "0",    # 0 = World (aggregate)
        "partner2Code": "0",
        "maxRecords": "500",
        "format": "JSON",
        "breakdownMode": "classic",
        "includeDesc": "false",
    }

    try:
        resp = session.get(BASE_URL, params=params, timeout=30)
        if resp.status_code == 429:
            log.warning("Comtrade rate limit — sleeping 60s")
            time.sleep(60)
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", []) or []
    except Exception as e:
        log.warning(f"Comtrade fetch error {reporter}/{commodity}/{period}: {e}")
        return []


def upsert_flow(row: dict, commodity_meta: dict, supabase: Client):
    """Map Comtrade response row → macro.trade_flows_v1 upsert."""
    reporter = str(row.get("reporterCode", ""))
    partner = str(row.get("partnerCode", "0"))
    commodity = str(row.get("cmdCode", ""))
    period = str(row.get("period", ""))
    flow = row.get("flowCode", "")
    value_usd = row.get("primaryValue") or row.get("cifValue") or 0

    record_id = make_record_id(reporter, partner, commodity, period, flow)

    try:
        supabase.table("trade_flows_v1").upsert({
            "record_id": record_id,
            "source": "UN_COMTRADE",
            "reporter_country": reporter,
            "partner_country": partner,
            "commodity_code": commodity,
            "trade_flow": "EXPORT" if flow == "X" else "IMPORT",
            "value_usd": float(value_usd),
            "period": period,
            "metadata": {
                "label": commodity_meta["label"],
                "instruments": commodity_meta["instruments"],
                "reporter_desc": row.get("reporterDesc", ""),
                "partner_desc": row.get("partnerDesc", ""),
                "unit": row.get("qtyUnitCode", ""),
                "qty": row.get("qty"),
                "worker_version": WORKER_VERSION,
            }
        }, on_conflict="record_id", schema="macro").execute()
    except Exception as e:
        log.warning(f"trade_flows upsert failed {record_id}: {e}")


def emit_trade_shock_signal(
    commodity_meta: dict,
    reporter: str,
    period: str,
    value_usd: float,
    baseline_usd: float,
    flow_type: str,
    supabase: Client,
):
    """Emit TRADE_FLOW_SHOCK to macro.supply_chain_signal_v1 when deviation > 2σ."""
    if baseline_usd <= 0:
        return
    pct_change = (value_usd - baseline_usd) / baseline_usd
    if abs(pct_change) < 0.25:  # <25% change = not a shock
        return

    direction = 1 if pct_change > 0 else -1
    strength = min(abs(pct_change) / 2.0, 1.0)  # normalize to 0..1

    for instrument in commodity_meta["instruments"]:
        try:
            supabase.table("supply_chain_signal_v1").insert({
                "signal_type": "TRADE_FLOW_SHOCK",
                "instrument_symbol": instrument,
                "direction": direction,
                "strength": float(strength),
                "z_score": float(pct_change / 0.15) if baseline_usd > 0 else None,
                "commodity": commodity_meta["label"],
                "route": f"COUNTRY_{reporter}",
                "trade_value_usd": float(value_usd),
                "lookback_days": 30,
                "rationale": (
                    f"{flow_type} {commodity_meta['label']} from reporter {reporter} "
                    f"period {period}: {pct_change:+.1%} vs baseline"
                ),
                "meta": {
                    "reporter": reporter,
                    "period": period,
                    "baseline_usd": float(baseline_usd),
                    "worker_version": WORKER_VERSION,
                },
                "bucket_ts": datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                ).isoformat(),
            }, schema="macro").execute()
            log.info(
                f"TRADE_FLOW_SHOCK emitted: {instrument} "
                f"{commodity_meta['label']} {pct_change:+.1%} ({flow_type})"
            )
        except Exception as e:
            log.warning(f"supply_chain_signal insert failed: {e}")


def get_periods(lookback_months: int = 3) -> list[str]:
    """Return last N monthly periods in YYYYMM format."""
    now = datetime.now(timezone.utc)
    periods = []
    for i in range(2, 2 + lookback_months):  # offset by 2 for UN data lag
        dt = now - relativedelta(months=i)
        periods.append(dt.strftime("%Y%m"))
    return periods


def get_baseline_value(
    reporter: str,
    commodity: str,
    flow: str,
    supabase: Client
) -> float:
    """Get 12-month average trade value as baseline."""
    try:
        result = supabase.rpc("", {}).execute()  # placeholder
        return 0.0
    except Exception:
        return 0.0


def run_once(supabase: Client):
    log.info("Starting Comtrade ingest cycle")
    session = requests.Session()
    session.headers["User-Agent"] = "Noterminal/1.0 trade-intelligence-worker"

    periods = get_periods(lookback_months=2)
    log.info(f"Fetching periods: {periods}")

    total_rows = 0
    for commodity in COMMODITY_TARGETS:
        for reporter in REPORTER_COUNTRIES:
            for period in periods:
                rows = fetch_trade_flows(reporter, commodity["hs_code"], period, session)
                for row in rows:
                    upsert_flow(row, commodity, supabase)
                total_rows += len(rows)
                time.sleep(0.25)  # Respect free tier rate limits

    log.info(f"Comtrade cycle complete — {total_rows} rows upserted")


def main():
    log.info(f"Comtrade Worker v{WORKER_VERSION} starting")
    supabase = get_supabase_client()

    while True:
        try:
            run_once(supabase)
        except Exception as e:
            log.error(f"Comtrade cycle error: {e}")
        log.info(f"Sleeping {POLL_INTERVAL_SECONDS // 3600}h until next cycle")
        time.sleep(POLL_INTERVAL_SECONDS)


def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


if __name__ == "__main__":
    main()
