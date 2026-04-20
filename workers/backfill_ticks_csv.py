"""
backfill_ticks_csv.py

Reads historical crypto price CSVs directly from a .7z archive and streams
them into market.ticks_v1.

NOTE: ticks_v1 stores exactly ONE row per symbol (last-price semantics via
UPSERT ON CONFLICT (symbol_id)).  Each symbol ends up with its most-recent
historical close price.  To load a full time-series, use market.ohlcv_v1.

Usage:
    python workers/backfill_ticks_csv.py
    python workers/backfill_ticks_csv.py /path/to/archive.7z.001
"""

import os
import io
import csv
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone
from uuid import UUID, uuid5, NAMESPACE_URL

import asyncpg
import py7zr
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

PG_CONN = os.getenv("PG_CONN")
if not PG_CONN:
    raise RuntimeError("PG_CONN missing from .env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ============================================================
# CONFIG — edit to match your CSV column names
# ============================================================
DEFAULT_ARCHIVE = Path("C:/Users/User/Desktop/Historical Crypto Prices_2.7z.001")

DATE_COL   = "date"    # timestamp column (also tries: timestamp, time, Date)
SYMBOL_COL = "symbol"  # symbol column — set to None if one file per symbol
PRICE_COL  = "close"   # column to use as last_price (also tries: Close, price)
SIZE_COL   = "volume"  # column to use as last_size  (None → 0)

BATCH_SIZE = 500       # rows per executemany call

# Sentinel UUIDs — Kraken feed namespace (matches ops_worker.py)
KNOWN_UUIDS: dict[str, UUID] = {
    "BTC":  UUID("22222222-2222-2222-2222-222222222222"),
    "ETH":  UUID("33333333-3333-3333-3333-333333333333"),
    "SOL":  UUID("44444444-4444-4444-4444-444444444444"),
}

# ============================================================
# HELPERS
# ============================================================
def symbol_uuid(symbol: str) -> UUID:
    """Canonical UUID for a symbol; deterministic fallback for unknown symbols."""
    key = symbol.upper().strip().replace("USDT", "").replace("USD", "")
    return KNOWN_UUIDS.get(key) or uuid5(NAMESPACE_URL, f"market.symbol:{key}")


DATE_FORMATS = (
    "%Y-%m-%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%b %d %Y",
)


def parse_ts(raw: str) -> datetime:
    raw = raw.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    # Try unix timestamp
    try:
        return datetime.fromtimestamp(float(raw), tz=timezone.utc)
    except ValueError:
        pass
    raise ValueError(f"Unrecognised date format: {raw!r}")


def resolve_col(candidates: list[str | None], headers: list[str]) -> str | None:
    """Return the first candidate that exists in headers (case-insensitive)."""
    for c in candidates:
        if c and c.lower() in headers:
            return c.lower()
    return None


# ============================================================
# DB
# ============================================================
UPSERT_SQL = """
    INSERT INTO market.ticks_v1
        (symbol_id, event_ts, last_price, last_size, created_at)
    VALUES ($1, $2, $3, $4, now())
    ON CONFLICT (symbol_id) DO UPDATE
    SET last_price = EXCLUDED.last_price,
        last_size  = EXCLUDED.last_size,
        event_ts   = EXCLUDED.event_ts,
        created_at = now()
    WHERE EXCLUDED.event_ts > market.ticks_v1.event_ts
"""


async def flush(pool, batch: list[tuple]) -> None:
    await pool.executemany(UPSERT_SQL, batch)


# ============================================================
# PROCESS ONE CSV
# ============================================================
async def process_csv(
    pool,
    filename: str,
    content: bytes,
    default_symbol: str | None = None,
) -> int:
    text   = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))

    if not reader.fieldnames:
        log.warning("%s — no headers, skipping", filename)
        return 0

    headers = [h.lower().strip() for h in reader.fieldnames]
    log.info("%s — headers: %s", filename, headers)

    date_col = resolve_col([DATE_COL, "timestamp", "time", "Date", "Timestamp"], headers)
    sym_col  = resolve_col([SYMBOL_COL, "coin", "currency", "asset"], headers)
    price_col = resolve_col([PRICE_COL, "price", "close_price", "Close"], headers)
    size_col  = resolve_col([SIZE_COL, "vol", "Volume"], headers)

    if not date_col:
        log.error("%s — no date column found in %s, skipping", filename, headers)
        return 0
    if not price_col:
        log.error("%s — no price column found in %s, skipping", filename, headers)
        return 0

    batch: list[tuple] = []
    total = 0
    skipped = 0

    for raw_row in reader:
        row = {k.lower().strip(): v for k, v in raw_row.items()}

        sym = (row.get(sym_col) if sym_col else None) or default_symbol
        if not sym:
            skipped += 1
            continue

        price_raw = row.get(price_col, "").replace(",", "").strip()
        if not price_raw or price_raw in ("-", "N/A", "null", ""):
            skipped += 1
            continue

        try:
            event_ts  = parse_ts(row[date_col])
            last_price = float(price_raw)
        except (ValueError, KeyError) as e:
            log.debug("Row parse error (%s): %s", e, row)
            skipped += 1
            continue

        if size_col:
            size_raw = row.get(size_col, "0").replace(",", "").strip() or "0"
            try:
                last_size = float(size_raw)
            except ValueError:
                last_size = 0.0
        else:
            last_size = 0.0

        batch.append((str(symbol_uuid(sym)), event_ts, last_price, last_size))

        if len(batch) >= BATCH_SIZE:
            await flush(pool, batch)
            total += len(batch)
            batch.clear()

    if batch:
        await flush(pool, batch)
        total += len(batch)

    log.info(
        "%s DONE — inserted/updated: %d  skipped: %d",
        filename, total, skipped,
    )
    return total


# ============================================================
# MAIN
# ============================================================
async def main() -> None:
    archive = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ARCHIVE

    if not archive.exists():
        log.error("Archive not found: %s", archive)
        sys.exit(1)

    log.info("Connecting to DB...")
    pool = await asyncpg.create_pool(
        dsn=PG_CONN,
        min_size=1,
        max_size=3,
        statement_cache_size=0,
    )
    log.info("DB ready.")

    log.info("Opening archive: %s", archive)
    with py7zr.SevenZipFile(str(archive), mode="r") as z:
        all_names = z.getnames()
        csv_names = [n for n in all_names if n.lower().endswith(".csv")]
        log.info("Found %d CSV file(s) in archive", len(csv_names))

        grand_total = 0
        for name in csv_names:
            z.reset()
            data    = z.read([name])
            content = data[name].read()

            # Use the filename stem as default symbol (e.g. "BTC.csv" → "BTC")
            stem = Path(name).stem.upper()
            default_sym = stem if stem.isalpha() or stem.replace("/", "").isalpha() else None

            grand_total += await process_csv(pool, name, content, default_symbol=default_sym)

    await pool.close()
    log.info("All done — grand total rows upserted: %d", grand_total)


if __name__ == "__main__":
    asyncio.run(main())
