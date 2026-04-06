import os
import json
import asyncio
import logging
from collections import defaultdict, deque
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID
print("WORKER VERSION: side/is_maker ACTIVE", flush=True)
import asyncpg
import websockets
from dotenv import load_dotenv

# ============================================================
# LOAD .env FROM PROJECT ROOT
# ============================================================
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

FUND_ID = os.getenv("FUND_ID")
PG_CONN = os.getenv("PG_CONN")

print("RUNNING FILE:", __file__)
print("FUND_ID AT RUNTIME:", FUND_ID)

if not FUND_ID:
    raise RuntimeError("FUND_ID missing from .env")
if not PG_CONN:
    raise RuntimeError("PG_CONN missing from .env")


# ============================================================
# CONFIG
# ============================================================
BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://stream.binance.us:9443/ws")
WS_RECONNECT_BASE_S = float(os.getenv("WS_RECONNECT_BASE_S", "3"))
WS_RECONNECT_MAX_S = float(os.getenv("WS_RECONNECT_MAX_S", "30"))

BINANCE_TO_SYMBOL_ID: dict[str, UUID] = {
    "BTCUSDT": UUID("22222222-2222-2222-2222-222222222222"),
    "ETHUSDT": UUID("33333333-3333-3333-3333-333333333333"),
    "SOLUSDT": UUID("44444444-4444-4444-4444-444444444444"),
}

BINANCE_TO_CANONICAL: dict[str, UUID] = {
    "BTCUSDT": UUID("d85b4396-20a5-4f47-91fa-d83b802734b5"),
    "ETHUSDT": UUID("60f3954d-6fbf-427f-8670-e666c873b2e5"),
    "SOLUSDT": UUID("37c9a4dc-438e-4366-8e73-35460f21bec8"),
}

BINANCE_DISPLAY_SYMBOL: dict[str, str] = {
    "BTCUSDT": "BTC/USD",
    "ETHUSDT": "ETH/USD",
    "SOLUSDT": "SOL/USD",
}

FLUSH_INTERVAL = 5
STALE_TIMEOUT = 30
WATCHDOG_INTERVAL = 5
MAX_SEEN_TRADE_IDS = int(os.getenv("MAX_SEEN_TRADE_IDS", "20000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ============================================================
# WORKER
# ============================================================
class OpsWorker:
    def __init__(self):
        self.pool = None
        self.prices: dict[str, tuple] = {}
        self.raw_trades: list[tuple] = []
        self._ws_reconnect = asyncio.Event()
        self._has_raw_side = False
        self._has_raw_is_maker = False
        self._raw_side_is_numeric = False
        self._seen_trade_ids: dict[str, deque[int]] = defaultdict(deque)
        self._seen_trade_id_sets: dict[str, set[int]] = defaultdict(set)

    async def connect_db(self):
        logging.info("Connecting to DB...")
        self.pool = await asyncpg.create_pool(
            dsn=PG_CONN,
            min_size=1,
            max_size=3,
            statement_cache_size=0,
        )
        await self._load_ticks_raw_schema()
        logging.info("DB ready.")

    async def _load_ticks_raw_schema(self):
        rows = await self.pool.fetch("""
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'market'
              AND table_name = 'ticks_raw_v1'
        """)
        columns = {row["column_name"]: row for row in rows}
        side_meta = columns.get("side")

        self._has_raw_side = side_meta is not None
        self._has_raw_is_maker = "is_maker" in columns
        self._raw_side_is_numeric = bool(side_meta) and side_meta["udt_name"] in {
            "int2", "int4", "int8", "float4", "float8", "numeric",
        }

        logging.info(
            "ticks_raw_v1 schema: side=%s is_maker=%s side_numeric=%s",
            self._has_raw_side,
            self._has_raw_is_maker,
            self._raw_side_is_numeric,
        )
        print("SCHEMA FLAGS:", self._has_raw_side, self._has_raw_is_maker)

    async def write_ticks(self):
        if not self.prices:
            return
        for venue_sym, (price, size, ts, side, is_maker) in list(self.prices.items()):
            symbol_id = BINANCE_TO_SYMBOL_ID[venue_sym]
            try:
                await self.pool.execute("""
                    INSERT INTO market.ticks_v1
                        (symbol_id, event_ts, last_price, last_size, created_at)
                    VALUES ($1, $2, $3, $4, now())
                    ON CONFLICT (symbol_id) DO UPDATE
                    SET last_price = EXCLUDED.last_price,
                        last_size  = EXCLUDED.last_size,
                        event_ts   = EXCLUDED.event_ts,
                        created_at = now()
                    WHERE EXCLUDED.event_ts > market.ticks_v1.event_ts
                """,
                str(symbol_id),
                ts,
                price,
                size)
            except Exception as e:
                logging.error("DB WRITE FAILED [%s]: %s", venue_sym, e)

    async def write_raw_trades(self):
        if not self.raw_trades:
            return

        batch, self.raw_trades = self.raw_trades, []
        try:
            sql, params = self._build_ticks_raw_insert(batch)
            await self.pool.executemany(sql, params)
            logging.debug("Raw trades flushed: %d rows", len(batch))
        except Exception as e:
            logging.error("RAW TRADE WRITE FAILED: %s", e)
            self.raw_trades = batch + self.raw_trades

    def _build_ticks_raw_insert(self, batch):

        if self._has_raw_side and self._has_raw_is_maker:
            sql = """
                INSERT INTO market.ticks_raw_v1
                    (symbol_id, event_ts, last_price, last_size, side, is_maker, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, now())
                ON CONFLICT (symbol_id, event_ts) DO UPDATE
                SET
                    side = EXCLUDED.side,
                    is_maker = EXCLUDED.is_maker
            """
            params = [
                (
                    str(cid),
                    ts,
                    price,
                    size,
                    side if self._raw_side_is_numeric else ("buy" if side > 0 else "sell"),
                    is_maker,
                )
                for cid, ts, price, size, side, is_maker in batch
            ]
            return sql, params

        # fallback modes (unchanged)
        sql = """
            INSERT INTO market.ticks_raw_v1
                (symbol_id, event_ts, last_price, last_size, created_at)
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (symbol_id, event_ts) DO NOTHING
        """
        params = [(str(cid), ts, price, size) for cid, ts, price, size, _, _ in batch]
        return sql, params

    async def flusher(self):
        while True:
            await asyncio.sleep(FLUSH_INTERVAL)
            await self.write_raw_trades()
            await self.write_ticks()

    async def _subscribe(self, ws):
        params = [f"{symbol.lower()}@trade" for symbol in BINANCE_TO_SYMBOL_ID]
        await ws.send(json.dumps({
            "method": "SUBSCRIBE",
            "params": params,
            "id": 1,
        }))

    def _remember_trade_id(self, symbol, trade_id):
        seen = self._seen_trade_id_sets[symbol]
        if trade_id in seen:
            return False
        self._seen_trade_ids[symbol].append(trade_id)
        seen.add(trade_id)
        return True

    def _handle_message(self, payload):

        data = payload.get("data", payload)
        if data.get("e") != "trade":
            return

        sym = data.get("s")
        if sym not in BINANCE_TO_SYMBOL_ID:
            return

        trade_ts = data.get("T")
        event_ts = datetime.fromtimestamp(trade_ts / 1000, tz=timezone.utc)

        price = Decimal(str(data["p"]))
        size = Decimal(str(data["q"]))

        is_maker = bool(data.get("m", False))
        side = -1 if is_maker else 1
        side_str = "sell" if is_maker else "buy"

        self.prices[sym] = (price, size, event_ts, side_str, is_maker)

        canonical = BINANCE_TO_CANONICAL.get(sym)
        if canonical:
            self.raw_trades.append((canonical, event_ts, price, size, side, is_maker))

    async def market_loop(self):
        while True:
            try:
                async with websockets.connect(BINANCE_WS_URL) as ws:
                    await self._subscribe(ws)
                    async for msg in ws:
                        self._handle_message(json.loads(msg))
            except Exception as e:
                logging.error("WS ERROR: %s", e)
                await asyncio.sleep(5)

    async def run(self):
        await self.connect_db()
        await asyncio.gather(
            self.market_loop(),
            self.flusher(),
        )


async def main():
    worker = OpsWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
