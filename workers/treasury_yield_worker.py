"""
Noterminal — US Treasury Yield Curve Ingest Worker
Source: US Treasury Daily Yield Curve (free, no key required)
Schema target: public.yield_curve
Canon: fail-closed, idempotent upserts
"""

import os
import time
import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("treasury_yield_ingest")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

INGEST_INTERVAL_SECONDS = 3600

TREASURY_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml"


def fetch_yield_curve() -> list:
    try:
        params = {
            "data": "daily_treasury_yield_curve",
            "field_tdr_date_value_month": datetime.now(timezone.utc).strftime("%Y%m"),
        }
        resp = requests.get(TREASURY_URL, params=params, timeout=20)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)

        def get_val(props, tag):
            el = props.find(f"{{http://schemas.microsoft.com/ado/2007/08/dataservices}}{tag}")
            if el is not None and el.text:
                try:
                    return float(el.text)
                except Exception:
                    return None
            return None

        def get_date(props, tag):
            el = props.find(f"{{http://schemas.microsoft.com/ado/2007/08/dataservices}}{tag}")
            if el is not None and el.text:
                return el.text[:10]
            return None

        records = []
        for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
            props = entry.find(".//{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}properties")
            if props is None:
                continue

            record_date = get_date(props, "NEW_DATE")
            if not record_date:
                continue

            records.append({
                "record_date": record_date,
                "1mo":  get_val(props, "BC_1MONTH"),
                "2mo":  get_val(props, "BC_2MONTH"),
                "3mo":  get_val(props, "BC_3MONTH"),
                "6mo":  get_val(props, "BC_6MONTH"),
                "1yr":  get_val(props, "BC_1YEAR"),
                "2yr":  get_val(props, "BC_2YEARS"),
                "3yr":  get_val(props, "BC_3YEARS"),
                "5yr":  get_val(props, "BC_5YEARS"),
                "7yr":  get_val(props, "BC_7YEARS"),
                "10yr": get_val(props, "BC_10YEARS"),
                "20yr": get_val(props, "BC_20YEARS"),
                "30yr": get_val(props, "BC_30YEARS"),
            })

        return records

    except Exception as e:
        log.error(f"Treasury yield fetch failed: {e}")
        return []


def upsert_yield_curve(record: dict):
    try:
        supabase.table("yield_curve").upsert(
            record, on_conflict="record_date"
        ).execute()
        two_yr = record.get("2yr") or 0
        ten_yr = record.get("10yr") or 0
        log.info(f"Upserted {record['record_date']} 2yr={two_yr} 10yr={ten_yr} spread={round(ten_yr - two_yr, 3)}")
    except Exception as e:
        log.error(f"Yield curve upsert failed [{record.get('record_date')}]: {e}")


def run_treasury_ingest():
    log.info("Treasury yield curve: starting ingest")
    records = fetch_yield_curve()
    if not records:
        log.warning("Treasury yield curve: no data returned — fail closed")
        return
    for record in records:
        if not record.get("record_date"):
            continue
        if not record.get("10yr") and not record.get("2yr"):
            log.warning(f"Treasury: missing key tenors for {record.get('record_date')}, skipping")
            continue
        upsert_yield_curve(record)
    log.info(f"Treasury yield curve: {len(records)} records upserted")


if __name__ == "__main__":
    while True:
        try:
            run_treasury_ingest()
        except Exception as e:
            log.error(f"Treasury top-level failure: {e}")
        log.info(f"Sleeping {INGEST_INTERVAL_SECONDS}s until next cycle")
        time.sleep(INGEST_INTERVAL_SECONDS)