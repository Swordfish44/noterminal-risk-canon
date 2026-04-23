"""
Noterminal — GDELT Event Ingest Worker
Monitors global news for commodity causal chain trigger events
Sources: GDELT 2.0 DOC API (no key required, free)
Schema target: macro.gdelt_events_v1
Canon: fail-closed, idempotent upserts, append-only
Updates every 15 minutes — matches GDELT refresh cycle
"""

import os
import time
import hashlib
import logging
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gdelt_ingest")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"
INGEST_INTERVAL_SECONDS = 900  # 15 min — matches GDELT update cycle

GDELT_QUERIES = [
    {
        "query": "Saskatchewan flood OR drought OR crop failure OR canola",
        "causal_trigger": "canola_supply_shock",
        "source_node": "canola",
        "region": "CAN",
        "description": "Canadian crop disruption — triggers canola->palm_oil and canola->potash chains",
    },
    {
        "query": "iron ore mine disruption OR BHP OR Vale OR Rio Tinto derail OR flood OR strike",
        "causal_trigger": "iron_ore_supply_shock",
        "source_node": "iron_ore",
        "region": "GLOBAL",
        "description": "Iron ore supply disruption — triggers iron_ore->steel chain",
    },
    {
        "query": "semiconductor factory fire OR DRAM shortage OR SK Hynix OR Samsung fab OR TSMC disruption",
        "causal_trigger": "semiconductor_supply_shock",
        "source_node": "semiconductor_fab_event",
        "region": "GLOBAL",
        "description": "Semiconductor fab disruption — triggers DRAM supply shock chain",
    },
    {
        "query": "natural gas pipeline disruption OR LNG shortage OR Gazprom OR European gas supply",
        "causal_trigger": "natural_gas_supply_shock",
        "source_node": "natural_gas",
        "region": "EUR",
        "description": "Natural gas supply shock — triggers nat_gas->nitrogen_fertilizer chain",
    },
    {
        "query": "palm oil Malaysia OR Indonesia flood OR drought OR plantation fire OR CPO export ban",
        "causal_trigger": "palm_oil_supply_shock",
        "source_node": "palm_oil",
        "region": "MYS",
        "description": "Palm oil supply disruption — triggers substitution chains",
    },
    {
        "query": "lumber mill fire OR Canadian lumber OR BC wildfire timber OR sawmill closure",
        "causal_trigger": "lumber_supply_shock",
        "source_node": "lumber",
        "region": "USA",
        "description": "Lumber supply disruption — triggers lumber->housing_starts chain",
    },
    {
        "query": "copper mine strike OR Chile copper OR Peru copper mine OR Codelco disruption",
        "causal_trigger": "copper_supply_shock",
        "source_node": "copper",
        "region": "GLOBAL",
        "description": "Copper supply disruption — triggers copper->industrial_production chain",
    },
    {
        "query": "port strike OR shipping disruption OR Suez Canal OR Panama Canal OR container shortage",
        "causal_trigger": "shipping_disruption",
        "source_node": "global_shipping",
        "region": "GLOBAL",
        "description": "Global shipping disruption — upstream trigger for multiple commodity chains",
    },
    {
        "query": "potash shortage OR fertilizer shortage OR Nutrien OR Mosaic OR Belarus potash sanctions",
        "causal_trigger": "fertilizer_supply_shock",
        "source_node": "potash",
        "region": "GLOBAL",
        "description": "Fertilizer supply shock — direct trigger for agricultural cost chains",
    },
    {
        "query": "Brazil soybean drought OR Argentina soybean crop OR soy harvest disruption",
        "causal_trigger": "soybean_supply_shock",
        "source_node": "soybean",
        "region": "GLOBAL",
        "description": "Soybean supply disruption — triggers soybean->soybean_oil->palm_oil chain",
    },
]


def make_record_id(url: str, causal_trigger: str) -> str:
    raw = f"GDELT:{causal_trigger}:{url}"
    return hashlib.sha256(raw.encode()).hexdigest()


def upsert_event(record_id, causal_trigger, source_node, region,
                 title, url, domain, language, source_country,
                 seen_date, description):
    row = {
        "record_id": record_id,
        "source": "GDELT",
        "causal_trigger": causal_trigger,
        "source_node": source_node,
        "region": region,
        "title": title,
        "url": url,
        "domain": domain,
        "language": language,
        "source_country": source_country,
        "seen_date": seen_date,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "description": description,
    }
    try:
        supabase.table("macro.gdelt_events_v1").upsert(
            row, on_conflict="record_id"
        ).execute()
        log.info(f"Upserted [{causal_trigger}] {title[:60]}")
    except Exception as e:
        log.error(f"Upsert failed [{causal_trigger}]: {e}")


def fetch_gdelt_articles(query: str, timespan: str = "15min", max_records: int = 10) -> list:
    params = {
        "query": query,
        "mode": "artlist",
        "timespan": timespan,
        "maxrecords": max_records,
        "format": "json",
    }
    try:
        resp = requests.get(GDELT_DOC_API, params=params, timeout=20)
        if resp.status_code == 429:
            log.warning("GDELT rate limited — backing off 60s")
            time.sleep(60)
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("articles", []) or []
    except Exception as e:
        log.error(f"GDELT fetch failed [{query[:40]}]: {e}")
        return []


def run_gdelt_ingest():
    log.info("GDELT: starting ingest cycle")

    for q in GDELT_QUERIES:
        articles = fetch_gdelt_articles(
            query=q["query"],
            timespan="15min",
            max_records=10
        )

        if not articles:
            log.info(f"GDELT: no new articles for [{q['causal_trigger']}]")
            time.sleep(1)
            continue

        for article in articles:
            url = article.get("url", "")
            title = article.get("title", "")
            domain = article.get("domain", "")
            language = article.get("language", "")
            source_country = article.get("sourcecountry", "")
            seen_date = article.get("seendate", "")

            if not url or not title:
                continue

            record_id = make_record_id(url, q["causal_trigger"])

            upsert_event(
                record_id=record_id,
                causal_trigger=q["causal_trigger"],
                source_node=q["source_node"],
                region=q["region"],
                title=title,
                url=url,
                domain=domain,
                language=language,
                source_country=source_country,
                seen_date=seen_date,
                description=q["description"],
            )

        log.info(f"GDELT [{q['causal_trigger']}]: {len(articles)} articles ingested")
        time.sleep(2)

    log.info("GDELT: ingest cycle complete")


if __name__ == "__main__":
    while True:
        try:
            run_gdelt_ingest()
        except Exception as e:
            log.error(f"GDELT top-level failure: {e}")
        log.info(f"Sleeping {INGEST_INTERVAL_SECONDS}s until next cycle")
        time.sleep(INGEST_INTERVAL_SECONDS)