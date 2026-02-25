import psycopg2
from psycopg2.extras import RealDictCursor
import os
import time

DB_URL = os.getenv("DATABASE_URL")


def get_connection():
    return psycopg2.connect(DB_URL, cursor_factory=RealDictCursor)


def fetch_latest_nav(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                fund_id,
                nav_total AS nav_value,
                nav_asof  AS nav_date
            FROM capital.nav_snapshots_v1
            ORDER BY nav_asof DESC
            LIMIT 1;
        """)
        return cur.fetchone()


def run_nav_sensor():
    print("NAV SENSOR LOOP STARTED")

    conn = get_connection()

    while True:
        try:
            row = fetch_latest_nav(conn)

            if row:
                print(
                    f"NAV → fund={row['fund_id']} | "
                    f"value={row['nav_value']} | "
                    f"date={row['nav_date']}"
                )
            else:
                print("NAV → no rows found")

            time.sleep(5)

        except Exception as e:
            print("NAV SENSOR ERROR:", e)
            time.sleep(5)


if __name__ == "__main__":
    print("Truth Engine booting...")
    run_nav_sensor()