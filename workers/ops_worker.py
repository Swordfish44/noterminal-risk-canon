import os
import json
import asyncio
import logging
from collections import defaultdict, deque
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

print("WORKER VERSION: Kraken v2 WebSocket", flush=True)

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
KRAKEN_WS_URL = "wss://ws.kraken.com/v2"
WS_RECONNECT_BASE_S = float(os.getenv("WS_RECONNECT_BASE_S", "3"))
WS_RECONNECT_MAX_S  = float(os.getenv("WS_RECONNECT_MAX_S",  "30"))

# Symbols to subscribe to on Kraken
KRAKEN_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD"]

# Kraken symbol → market.ticks_v1 sentinel UUID
KRAKEN_TO_SYMBOL_ID: dict[str, UUID] = {
    "BTC/USD": UUID("22222222-2222-2222-2222-222222222222"),
    "ETH/USD": UUID("33333333-3333-3333-3333-333333333333"),
    "SOL/USD": UUID("44444444-4444-4444-4444-444444444444"),
}

# Kraken symbol → market.symbols_v1 canonical UUID (used for ticks_raw_v1)
KRAKEN_TO_CANONICAL: dict[str, UUID] = {
    "BTC/USD": UUID("d85b4396-20a5-4f47-91fa-d83b802734b5"),
    "ETH/USD": UUID("60f3954d-6fbf-427f-8670-e666c873b2e5"),
    "SOL/USD": UUID("37c9a4dc-438e-4366-8e73-35460f21bec8"),
}

FLUSH_INTERVAL    = 5   # seconds between DB flushes
STALE_TIMEOUT     = 30  # seconds without a trade before forcing reconnect
WATCHDOG_INTERVAL = 5   # seconds between staleness checks
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
        self.prices: dict[str, tuple] = {}       # venue_sym → (price, size, ts, side_str, is_maker)
        self.raw_trades: list[tuple] = []
        self._ws_reconnect = asyncio.Event()
        self._last_trade_ts: float = 0.0          # monotonic clock — for watchdog
        self._has_raw_side = False
        self._has_raw_is_maker = False
        self._raw_side_is_numeric = False
        self._seen_trade_ids: dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_SEEN_TRADE_IDS))
        self._seen_trade_id_sets: dict[str, set] = defaultdict(set)

    # --------------------------------------------------------
    # DB
    # --------------------------------------------------------
    async def connect_db(self):
        logging.info("Connecting to DB...")
        self.pool = await asyncpg.create_pool(
            dsn=PG_CONN,
            min_size=1,
            max_size=3,
            statement_cache_size=0,
        )
        async with self.pool.acquire() as conn:
            await conn.execute("DEALLOCATE ALL")
        await self._load_ticks_raw_schema()
        logging.info("DB ready.")

    async def _load_ticks_raw_schema(self):
        rows = await self.pool.fetch("""
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'market'
              AND table_name   = 'ticks_raw_v1'
        """)
        columns = {row["column_name"]: row for row in rows}
        side_meta = columns.get("side")

        self._has_raw_side      = side_meta is not None
        self._has_raw_is_maker  = "is_maker" in columns
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

    # --------------------------------------------------------
    # DB WRITES
    # --------------------------------------------------------
    async def write_ticks(self):
        if not self.prices:
            return
        for venue_sym, (price, size, ts, side_str, is_maker) in list(self.prices.items()):
            symbol_id = KRAKEN_TO_SYMBOL_ID[venue_sym]
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
                str(symbol_id), ts, price, size)
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
            self.raw_trades = batch + self.raw_trades  # requeue on failure

    def _build_ticks_raw_insert(self, batch):
        if self._has_raw_side and self._has_raw_is_maker:
            sql = """
                INSERT INTO market.ticks_raw_v1
                    (symbol_id, event_ts, last_price, last_size, side, is_maker, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, now())
                ON CONFLICT (symbol_id, event_ts) DO UPDATE
                SET side     = EXCLUDED.side,
                    is_maker = EXCLUDED.is_maker
            """
            params = [
                (
                    str(cid),
                    ts,
                    price,
                    size,
                    side_int if self._raw_side_is_numeric else ("buy" if side_int > 0 else "sell"),
                    is_maker,
                )
                for cid, ts, price, size, side_int, is_maker in batch
            ]
            return sql, params

        # Fallback — schema without side/is_maker columns
        sql = """
            INSERT INTO market.ticks_raw_v1
                (symbol_id, event_ts, last_price, last_size, created_at)
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (symbol_id, event_ts) DO NOTHING
        """
        params = [(str(cid), ts, price, size) for cid, ts, price, size, _, _ in batch]
        return sql, params

    # --------------------------------------------------------
    # FLUSHER
    # --------------------------------------------------------
    async def flusher(self):
        while True:
            await asyncio.sleep(FLUSH_INTERVAL)
            await self.write_raw_trades()
            await self.write_ticks()

    # --------------------------------------------------------
    # WATCHDOG — force reconnect if no trades for STALE_TIMEOUT seconds
    # --------------------------------------------------------
    async def watchdog(self):
        await asyncio.sleep(STALE_TIMEOUT)  # allow initial connect time
        while True:
            await asyncio.sleep(WATCHDOG_INTERVAL)
            idle = asyncio.get_event_loop().time() - self._last_trade_ts
            if idle > STALE_TIMEOUT:
                logging.warning("Watchdog: no trades for %.0fs — forcing reconnect", idle)
                self._ws_reconnect.set()

    # --------------------------------------------------------
    # KRAKEN v2 — SUBSCRIBE
    # --------------------------------------------------------
    async def _subscribe(self, ws):
        msg = json.dumps({
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": KRAKEN_SYMBOLS,
            },
        })
        await ws.send(msg)
        logging.info("Subscribed to Kraken trade channel: %s", KRAKEN_SYMBOLS)

    # --------------------------------------------------------
    # DEDUP
    # --------------------------------------------------------
    def _remember_trade_id(self, symbol: str, trade_id: int) -> bool:
        seen_set = self._seen_trade_id_sets[symbol]
        if trade_id in seen_set:
            return False
        q = self._seen_trade_ids[symbol]
        if len(q) >= MAX_SEEN_TRADE_IDS:
            evicted = q.popleft()
            seen_set.discard(evicted)
        q.append(trade_id)
        seen_set.add(trade_id)
        return True

    # --------------------------------------------------------
    # MESSAGE HANDLER
    # Kraken v2 trade message shape:
    # {
    #   "channel": "trade",
    #   "type": "update" | "snapshot",
    #   "data": [
    #     {
    #       "symbol": "BTC/USD",
    #       "side": "buy" | "sell",   <- taker side
    #       "qty": 0.001,
    #       "price": 45000.0,
    #       "trade_id": 4621818,
    #       "timestamp": "2024-01-01T00:00:00.123456Z"
    #     }, ...
    #   ]
    # }
    # --------------------------------------------------------
    def _handle_message(self, payload: dict):
        channel = payload.get("channel")

        if channel == "heartbeat":
            return

        if channel != "trade":
            return

        data_list = payload.get("data")
        if not data_list:
            return

        for trade in data_list:
            sym = trade.get("symbol")
            if sym not in KRAKEN_TO_SYMBOL_ID:
                continue

            trade_id = trade.get("trade_id")
            if trade_id is not None and not self._remember_trade_id(sym, trade_id):
                continue

            ts_str = trade.get("timestamp")
            event_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))

            price = Decimal(str(trade["price"]))
            size  = Decimal(str(trade["qty"]))

            # Kraken side = taker side.
            # "buy"  -> buyer was aggressor  -> side=+1, is_maker=False
            # "sell" -> seller was aggressor -> side=-1, is_maker=True
            side_str = trade.get("side", "buy")
            side_int = 1 if side_str == "buy" else -1
            is_maker = side_str == "sell"

            self.prices[sym] = (price, size, event_ts, side_str, is_maker)
            self._last_trade_ts = asyncio.get_event_loop().time()

            canonical = KRAKEN_TO_CANONICAL.get(sym)
            if canonical:
                self.raw_trades.append((canonical, event_ts, price, size, side_int, is_maker))

    # --------------------------------------------------------
    # MARKET LOOP — exponential backoff reconnect
    # --------------------------------------------------------
    async def market_loop(self):
        delay = WS_RECONNECT_BASE_S
        while True:
            self._ws_reconnect.clear()
            try:
                async with websockets.connect(
                    KRAKEN_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    await self._subscribe(ws)
                    self._last_trade_ts = asyncio.get_event_loop().time()
                    delay = WS_RECONNECT_BASE_S  # reset backoff on successful connect

                    async def recv_loop():
                        async for raw in ws:
                            self._handle_message(json.loads(raw))

                    recv_task      = asyncio.create_task(recv_loop())
                    reconnect_task = asyncio.create_task(self._ws_reconnect.wait())

                    done, pending = await asyncio.wait(
                        [recv_task, reconnect_task],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for t in pending:
                        t.cancel()

                    if reconnect_task in done:
                        logging.info("Reconnect signalled by watchdog — reconnecting...")

            except Exception as e:
                logging.error("WS ERROR: %s — retrying in %.0fs", e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, WS_RECONNECT_MAX_S)

    # --------------------------------------------------------
    # RUN
    # --------------------------------------------------------
    async def run(self):
        await self.connect_db()
        await asyncio.gather(
            self.market_loop(),
            self.flusher(),
            self.watchdog(),
        )


async def main():
    worker = OpsWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
