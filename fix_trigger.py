"""
One-shot repair: fix truth.capture_nav_snapshot_on_tick()

The trigger was written before fund_id / positions_value were added as NOT NULL
columns to truth.nav_snapshots_v1, causing every INSERT to market.ticks_raw_v1
to fail and roll back.

Run once, then discard.
"""
import asyncio
import os
from pathlib import Path
from dotenv import load_dotenv
import asyncpg

load_dotenv(Path(__file__).resolve().parent / ".env")

PG_CONN = os.getenv("PG_CONN")
if not PG_CONN:
    raise RuntimeError("PG_CONN missing from .env")


async def main():
    pool = await asyncpg.create_pool(dsn=PG_CONN, min_size=1, max_size=2, statement_cache_size=0)

    print("Applying fix to truth.capture_nav_snapshot_on_tick() ...")

    await pool.execute("""
        CREATE OR REPLACE FUNCTION truth.capture_nav_snapshot_on_tick()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            INSERT INTO truth.nav_snapshots_v1
                (nav_ts, gross_nav, net_nav, cash_nav, fund_id, positions_value)
            SELECT
                now(),
                100000 + SUM(p.qty * lp.last_price),
                100000 + SUM(p.qty * lp.last_price),
                100000,
                p.fund_id,
                SUM(p.qty * lp.last_price)
            FROM portfolio.positions_v1 p
            LEFT JOIN market.last_price_v1 lp ON lp.symbol_id = p.symbol_id
            WHERE p.qty <> 0
            GROUP BY p.fund_id;
            RETURN NEW;
        END;
        $$;
    """)

    print("Done. Verifying trigger function body ...")

    rows = await pool.fetch("""
        SELECT pg_get_functiondef(oid) AS body
        FROM pg_proc
        WHERE proname = 'capture_nav_snapshot_on_tick'
          AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'truth')
    """)
    for r in rows:
        print(r["body"])

    await pool.close()
    print("Fix complete.")


if __name__ == "__main__":
    asyncio.run(main())
