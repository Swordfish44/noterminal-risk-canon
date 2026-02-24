from dotenv import load_dotenv
import os
import psycopg2
from pprint import pprint

load_dotenv()

conn = psycopg2.connect(os.getenv("PG_CONN"))
cur = conn.cursor()

cur.execute("""
SELECT account_type,
       account_ref,
       COALESCE(sum(amount),0)::numeric AS balance
FROM truth.cash_ledger_v1
GROUP BY account_type, account_ref
ORDER BY account_type, account_ref;
""")

rows = cur.fetchall()

print("\n=== CASH LEDGER STATE ===")
pprint(rows)

cur.close()
conn.close()