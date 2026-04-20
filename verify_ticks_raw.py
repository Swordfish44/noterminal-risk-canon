import os
import asyncio
from pathlib import Path

import asyncpg
from dotenv import load_dotenv


# Load .env from project root
load_dotenv(Path(__file__).resolve().parent / ".env")

PG_CONN = os.getenv("PG_CONN") or os.getenv("DATABASE_URL")

if not PG_CONN:
    raise RuntimeError("Neither PG_CONN nor DATABASE_URL is set")


async def main():
    conn = await asyncpg.connect(PG_CONN, statement_cache_size=0)
    try:
        rows = await conn.fetch(
            """
            select
                symbol_id,
                event_ts,
                last_price,
                last_size,
                side,
                is_maker
            from market.ticks_raw_v1
            order by event_ts desc
            limit 10
            """
        )
    finally:
        await conn.close()

    print("\n=== VERIFY TICKS_RAW_V1 ===\n")

    if not rows:
        print("No rows found.")
        return

    for i, r in enumerate(rows, 1):
        print(
            f"{i}. symbol={r['symbol_id']} "
            f"price={r['last_price']} size={r['last_size']} "
            f"side={r['side']} is_maker={r['is_maker']}"
        )

    null_count = sum(
        1 for r in rows if r["side"] is None or r["is_maker"] is None
    )

    print("\n--- STATUS ---")

    if null_count == 0:
        print("SUCCESS: side + is_maker fully populated")
    elif null_count < len(rows):
        print("PARTIAL: some rows still missing values")
    else:
        print("FAIL: side + is_maker not being written")


if __name__ == "__main__":
    asyncio.run(main())
