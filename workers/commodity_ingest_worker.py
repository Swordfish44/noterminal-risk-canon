"""
Noterminal Commodity Causality Graph — Ingestion Workers
Sources: USDA AMS, EIA, World Bank, UN Comtrade, MPOB, omkar.cloud
Schema target: macro.commodity_prices_v1, macro.trade_flows_v1
Canon: fail-closed, idempotent upserts, append-only signal events
"""

import os
import time
import hashlib
import logging
import requests
from datetime import datetime, timezone, timedelta
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("commodity_ingest")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
EIA_API_KEY = os.environ["EIA_API_KEY"]
OMKAR_API_KEY = os.environ["OMKAR_API_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

INGEST_INTERVAL_SECONDS = 3600  # 1 hour default poll


# ─────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────

def make_record_id(source: str, commodity: str, as_of: str) -> str:
    """Deterministic record ID — idempotent upsert key."""
    raw = f"{source}:{commodity}:{as_of}"
    return hashlib.sha256(raw.encode()).hexdigest()


def upsert_price(source: str, commodity: str, price: float, unit: str, as_of: str, metadata: dict = None):
    record_id = make_record_id(source, commodity, as_of)
    row = {
        "record_id": record_id,
        "source": source,
        "commodity": commodity,
        "price": price,
        "unit": unit,
        "as_of": as_of,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata or {},
    }
    try:
        supabase.table("macro.commodity_prices_v1").upsert(row, on_conflict="record_id").execute()
        log.info(f"Upserted [{source}] {commodity} @ {price} {unit} as_of={as_of}")
    except Exception as e:
        log.error(f"Upsert failed [{source}] {commodity}: {e}")


def upsert_trade_flow(source: str, reporter: str, partner: str, commodity_code: str,
                      flow: str, value_usd: float, period: str, metadata: dict = None):
    record_id = make_record_id(f"{source}:{reporter}:{partner}:{flow}", commodity_code, period)
    row = {
        "record_id": record_id,
        "source": source,
        "reporter_country": reporter,
        "partner_country": partner,
        "commodity_code": commodity_code,
        "trade_flow": flow,
        "value_usd": value_usd,
        "period": period,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata or {},
    }
    try:
        supabase.table("macro.trade_flows_v1").upsert(row, on_conflict="record_id").execute()
        log.info(f"Upserted trade flow [{source}] {reporter}->{partner} {commodity_code} {period}")
    except Exception as e:
        log.error(f"Trade flow upsert failed: {e}")


# ─────────────────────────────────────────────
# WORKER 1: USDA AMS
# ─────────────────────────────────────────────

USDA_COMMODITIES = [
    {"slug": "corn",     "report": "2685"},
    {"slug": "wheat",    "report": "2690"},
    {"slug": "soybeans", "report": "2684"},
    {"slug": "cattle",   "report": "2498"},
    {"slug": "hogs",     "report": "2466"},
]

def run_usda_ams():
    log.info("USDA AMS: starting ingest")
    base = "https://mymarketnews.ams.usda.gov/public_data/report"
    for c in USDA_COMMODITIES:
        try:
            url = f"{base}/{c['report']}?q=wtavgprice&format=json&limit=1"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if not results:
                log.warning(f"USDA AMS: no results for {c['slug']}")
                continue
            row = results[0]
            price = float(row.get("wtavgprice") or row.get("price") or 0)
            unit = row.get("unit", "USD/bu")
            as_of = row.get("report_date", datetime.now(timezone.utc).date().isoformat())
            if price <= 0:
                log.warning(f"USDA AMS: zero price for {c['slug']}, skipping")
                continue
            upsert_price("USDA_AMS", c["slug"], price, unit, as_of, metadata={"report_id": c["report"]})
        except Exception as e:
            log.error(f"USDA AMS [{c['slug']}] failed: {e}")
    log.info("USDA AMS: ingest complete")


# ─────────────────────────────────────────────
# WORKER 2: EIA
# ─────────────────────────────────────────────

EIA_SERIES = [
    {"id": "PET.RWTC.W",                      "commodity": "WTI_crude_oil",         "unit": "USD/bbl"},
    {"id": "PET.RBRTE.W",                      "commodity": "brent_crude_oil",       "unit": "USD/bbl"},
    {"id": "NG.RNGWHHD.W",                     "commodity": "natural_gas_henry_hub", "unit": "USD/MMBtu"},
    {"id": "PET.EMM_EPM0_PTE_NUS_DPG.W",       "commodity": "retail_gasoline",       "unit": "USD/gal"},
    {"id": "PET.EER_EPD2F_PF4_RGC_DPG.W",      "commodity": "heating_oil",           "unit": "USD/gal"},
]

def run_eia():
    log.info("EIA: starting ingest")
    for s in EIA_SERIES:
        try:
            url = (
                f"https://api.eia.gov/v2/seriesid/{s['id']}"
                f"?api_key={EIA_API_KEY}&data[0]=value&sort[0][column]=period"
                f"&sort[0][direction]=desc&length=1"
            )
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            rows = data.get("response", {}).get("data", [])
            if not rows:
                log.warning(f"EIA: no data for {s['commodity']}")
                continue
            row = rows[0]
            price = float(row.get("value") or 0)
            as_of = row.get("period", datetime.now(timezone.utc).date().isoformat())
            if price <= 0:
                log.warning(f"EIA: zero price for {s['commodity']}, skipping")
                continue
            upsert_price("EIA", s["commodity"], price, s["unit"], as_of, metadata={"series_id": s["id"]})
        except Exception as e:
            log.error(f"EIA [{s['commodity']}] failed: {e}")
    log.info("EIA: ingest complete")


# ─────────────────────────────────────────────
# WORKER 3: WORLD BANK
# ─────────────────────────────────────────────

WORLD_BANK_COMMODITIES = [
    "ALUMINUM", "COPPER", "IRON_ORE", "LEAD", "NICKEL",
    "ZINC", "GOLD", "SILVER", "PLATINUM",
    "COAL_AUS", "CRUDE_OIL_BRENT",
    "WHEAT_US_HRW", "MAIZE", "RICE_05", "SOYBEANS",
    "PALM_OIL", "SOYBEAN_OIL", "SUGAR_WLD", "COTTON_A_INDX",
    "LOGS_CMR", "SAWNWD_CMR", "PLYWOOD",
    "RUBBER1_MYSG", "PHOSROCK", "UREA_EE_BULK", "DAP",
]

def run_world_bank():
    log.info("World Bank: starting ingest")
    try:
        url = "https://api.worldbank.org/v2/country/all/indicator/PCOMM?format=json&mrv=1&per_page=1000"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rows = data[1] if len(data) > 1 else []
        for row in rows:
            commodity = row.get("indicator", {}).get("id", "").replace("PCOMM.", "")
            if commodity not in WORLD_BANK_COMMODITIES:
                continue
            value = row.get("value")
            if value is None:
                continue
            as_of = row.get("date", "")
            upsert_price("WORLD_BANK", commodity, float(value), "USD", as_of,
                         metadata={"country": row.get("country", {}).get("id", "")})
    except Exception as e:
        log.error(f"World Bank ingest failed: {e}")
    log.info("World Bank: ingest complete")


# ─────────────────────────────────────────────
# WORKER 4: UN COMTRADE
# ─────────────────────────────────────────────

COMTRADE_HS_CODES = [
    {"hs": "1001", "name": "wheat"},
    {"hs": "1005", "name": "corn"},
    {"hs": "1201", "name": "soybeans"},
    {"hs": "1511", "name": "palm_oil"},
    {"hs": "2601", "name": "iron_ore"},
    {"hs": "2602", "name": "manganese_ore"},
    {"hs": "2603", "name": "copper_ore"},
    {"hs": "2609", "name": "tin_ore"},
    {"hs": "3102", "name": "nitrogen_fertilizers"},
    {"hs": "3104", "name": "potash"},
    {"hs": "3105", "name": "mixed_fertilizers"},
    {"hs": "4403", "name": "logs_rough_wood"},
    {"hs": "4407", "name": "sawnwood"},
    {"hs": "7204", "name": "ferrous_scrap"},
]

COMTRADE_REPORTER_COUNTRIES = ["USA", "CAN", "BRA", "AUS", "CHN", "DEU", "MYS", "IDN"]

def run_un_comtrade():
    log.info("UN Comtrade: starting ingest")
    period = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y%m")
    for hs in COMTRADE_HS_CODES[:5]:
        for reporter in COMTRADE_REPORTER_COUNTRIES[:4]:
            try:
                url = (
                    f"https://comtradeapi.un.org/public/v1/preview/C/A/HS"
                    f"?reporterCode={reporter}&period={period}"
                    f"&cmdCode={hs['hs']}&flowCode=X&includeDesc=true"
                )
                resp = requests.get(url, timeout=20)
                if resp.status_code == 429:
                    log.warning("UN Comtrade rate limited — backing off")
                    time.sleep(60)
                    continue
                resp.raise_for_status()
                data = resp.json()
                for row in data.get("data", []):
                    value_usd = row.get("primaryValue", 0)
                    partner = row.get("partnerCode", "WLD")
                    if value_usd and float(value_usd) > 0:
                        upsert_trade_flow(
                            source="UN_COMTRADE",
                            reporter=reporter,
                            partner=str(partner),
                            commodity_code=hs["hs"],
                            flow="EXPORT",
                            value_usd=float(value_usd),
                            period=period,
                            metadata={"commodity_name": hs["name"]}
                        )
                time.sleep(1)
            except Exception as e:
                log.error(f"UN Comtrade [{reporter}:{hs['hs']}] failed: {e}")
    log.info("UN Comtrade: ingest complete")


# ─────────────────────────────────────────────
# WORKER 5: MPOB
# ─────────────────────────────────────────────

def run_mpob():
    log.info("MPOB: starting ingest")
    try:
        url = "https://bepi.mpob.gov.my/index.php/en/price/price-of-palm-oil-products2"
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Noterminal/1.0"})
        if resp.status_code != 200:
            log.warning(f"MPOB returned {resp.status_code} — skipping")
            return
        today = datetime.now(timezone.utc).date().isoformat()
        upsert_price("MPOB_CHECK", "palm_oil_cpo", 0.0, "MYR/tonne", today,
                     metadata={"status": "page_accessible", "note": "full_parse_pending"})
    except Exception as e:
        log.error(f"MPOB ingest failed: {e}")
    log.info("MPOB: ingest complete")


# ─────────────────────────────────────────────
# WORKER 6: OMKAR.CLOUD FUTURES
# ─────────────────────────────────────────────

OMKAR_COMMODITIES = [
    "gold", "silver", "platinum", "palladium",
    "crude_oil", "brent_crude_oil", "natural_gas",
    "wheat", "corn", "soybean", "soybean_oil", "soybean_meal",
    "lumber", "coffee", "cocoa", "sugar", "cotton",
    "live_cattle", "lean_hogs", "copper", "aluminum",
]

def run_omkar_futures():
    log.info("omkar.cloud futures: starting ingest")
    base = "https://commodity-price-api.omkar.cloud/commodity-price"
    headers = {"API-Key": OMKAR_API_KEY}
    for commodity in OMKAR_COMMODITIES:
        try:
            resp = requests.get(base, params={"name": commodity}, headers=headers, timeout=10)
            if resp.status_code == 429:
                log.warning("omkar.cloud rate limited — sleeping 30s")
                time.sleep(30)
                continue
            resp.raise_for_status()
            data = resp.json()
            price = float(data.get("price_usd") or 0)
            exchange = data.get("exchange", "CME")
            updated_at = data.get("updated_at", datetime.now(timezone.utc).isoformat())
            as_of = updated_at[:10]
            if price <= 0:
                log.warning(f"omkar.cloud: zero price for {commodity}, skipping")
                continue
            upsert_price(f"OMKAR_{exchange}", commodity, price, "USD", as_of,
                         metadata={"exchange": exchange, "updated_at": updated_at})
            time.sleep(0.5)
        except Exception as e:
            log.error(f"omkar.cloud [{commodity}] failed: {e}")
    log.info("omkar.cloud futures: ingest complete")


# ─────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────

def run_all():
    log.info("=== Commodity Ingest Cycle Starting ===")
    run_usda_ams()
    run_eia()
    run_world_bank()
    run_omkar_futures()
    run_un_comtrade()
    run_mpob()
    log.info("=== Commodity Ingest Cycle Complete ===")


if __name__ == "__main__":
    while True:
        try:
            run_all()
        except Exception as e:
            log.error(f"Top-level ingest failure: {e}")
        log.info(f"Sleeping {INGEST_INTERVAL_SECONDS}s until next cycle")
        time.sleep(INGEST_INTERVAL_SECONDS)