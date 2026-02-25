from dotenv import load_dotenv
import os
import time
import psycopg2

print("🚀 Truth Engine booting...")

load_dotenv()

pg_conn = os.getenv("PG_CONN")

if not pg_conn:
    print("❌ NO PG_CONN FOUND")
    exit()

print("✅ ENV LOADED")

try:
    conn = psycopg2.connect(pg_conn)
    print("✅ DATABASE CONNECTED")
except Exception as e:
    print("❌ DB CONNECTION FAILED:", e)
    exit()

while True:
    print("⚙️ ENGINE RUNNING")
    time.sleep(5)