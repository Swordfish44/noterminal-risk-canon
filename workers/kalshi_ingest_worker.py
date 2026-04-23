"""
Noterminal — Kalshi Prediction Market Ingest Worker
Pulls macro event probabilities from Kalshi public API (no key required)
Schema target: macro.prediction_market_signals_v1
Canon: fail-closed, idempotent upserts, append-only
"""

import os
import time
import hashlib
import logging
import requests
from datetime import datetime, timezone
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("kalshi_ingest")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
INGEST_INTERVAL_SECONDS = 900  # 15 min poll

KALSHI_SERIES = [
    {"series": "KXFED",       "signal_category": "FED_RATE",     "description": "Fed funds rate decision"},
    {"series": "KXFOMC",      "signal_category": "FED_RATE",     "description": "FOMC meeting outcome"},
    {"series": "KXCPI",       "signal_category": "INFLATION",    "description": "CPI monthly outcome"},
    {"series": "KXPCE",       "signal_category": "INFLATION",    "description": "PCE inflation outcome"},
    {"series": "KXGDP",       "signal_category": "GDP",          "description": "GDP growth outcome"},
    {"series": "KXRECSSNBER", "signal_category": "RECESSION",    "description": "US recession probability NBER definition"},
    {"series": "KXUR",        "signal_category": "UNEMPLOYMENT", "description": "Unemployment rate outcome"},
    {"series": "KXSPY",       "signal_category": "EQUITY_LEVEL", "description": "SPY price level outcome"},
    {"series": "KXBTC",       "signal_category": "CRYPTO",       "description": "Bitcoin price outcome"},
]


def make_record_id(ticker: str, as_of: str) -> str:
    raw = f"KALSHI:{ticker}:{as_of}"
    return hashlib.sha256(raw.encode()).hexdigest()


def upsert_signal(ticker, series, signal_category, yes_price, no_price,
                  volume, open_interest, close_time, title, as_of, metadata=None):
    record_id = make_record_id(ticker, as_of)
    row = {
        "record_id": record_id,
        "source": "KALSHI",
        "ticker": ticker,
        "series": series,
        "signal_category": signal_category,
        "title": title,
        "yes_price": yes_price,
        "no_price": no_price,
        "volume": volume,
        "open_interest": open_interest,
        "close_time": close_time,
        "as_of": as_of,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "metadata": metadata or {},
    }
    try:
        supabase.table("macro.prediction_market_signals_v1").upsert(
            row, on_conflict="record_id"
        ).execute()
        log.info(f"Upserted [{signal_category}] {ticker} yes={yes_price:.2f} vol={volume}")
    except Exception as e:
        log.error(f"Upsert failed [{ticker}]: {e}")


def fetch_open_markets(series_ticker: str) -> list:
    try:
        url = f"{KALSHI_BASE}/markets"
        params = {"series_ticker": series_ticker, "status": "open", "limit": 20}
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 404:
            log.warning(f"Kalshi series not found: {series_ticker}")
            return []
        resp.raise_for_status()
        return resp.json().get("markets", [])
    except Exception as e:
        log.error(f"Kalshi fetch failed [{series_ticker}]: {e}")
        return []


def parse_price(val) -> float:
    try:
        return round(float(val or 0), 4)
    except Exception:
        return 0.0


def run_kalshi_ingest():
    log.info("Kalshi: starting ingest cycle")
    as_of = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:00")

    for s in KALSHI_SERIES:
        markets = fetch_open_markets(s["series"])
        if not markets:
            log.warning(f"Kalshi: no open markets for {s['series']}")
            time.sleep(0.5)
            continue

        for market in markets:
            ticker = market.get("ticker", "")
            if not ticker:
                continue

            yes_price = parse_price(market.get("yes_bid_dollars") or market.get("last_price_dollars"))
            no_price = parse_price(market.get("no_bid_dollars"))
            volume = float(market.get("volume_fp") or 0)
            open_interest = float(market.get("open_interest_fp") or 0)
            close_time = market.get("close_time", "")
            title = market.get("title", market.get("yes_sub_title", ""))

            if yes_price <= 0:
                log.warning(f"Kalshi: zero yes_price for {ticker}, skipping")
                continue

            upsert_signal(
                ticker=ticker,
                series=s["series"],
                signal_category=s["signal_category"],
                yes_price=yes_price,
                no_price=no_price,
                volume=volume,
                open_interest=open_interest,
                close_time=close_time,
                title=title,
                as_of=as_of,
                metadata={
                    "description": s["description"],
                    "event_ticker": market.get("event_ticker", ""),
                    "status": market.get("status", ""),
                }
            )

        time.sleep(0.5)

    log.info("Kalshi: ingest cycle complete")


if __name__ == "__main__":
    while True:
        try:
            run_kalshi_ingest()
        except Exception as e:
            log.error(f"Kalshi top-level failure: {e}")
        log.info(f"Sleeping {INGEST_INTERVAL_SECONDS}s until next cycle")
        time.sleep(INGEST_INTERVAL_SECONDS)