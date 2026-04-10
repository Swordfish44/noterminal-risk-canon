import os
import asyncio
import uuid
from datetime import datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg
from dotenv import load_dotenv

# ============================================================
# LOAD .env FROM PROJECT ROOT
# ============================================================
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

PG_CONN = os.getenv("PG_CONN")

print("RUNNING FILE:", __file__, flush=True)

if not PG_CONN:
    raise RuntimeError("PG_CONN missing from .env")

# ============================================================
# CONFIG
# ============================================================
CYCLE_INTERVAL = 60  # seconds

ET = ZoneInfo("America/New_York")
MARKET_OPEN  = time(9, 25)
MARKET_CLOSE = time(16, 5)


# ============================================================
# HELPERS
# ============================================================
def is_nyse_hours() -> bool:
    now = datetime.now(ET)
    if now.weekday() >= 5:          # Sat=5, Sun=6
        return False
    return MARKET_OPEN <= now.time() <= MARKET_CLOSE


# ============================================================
# WORKER
# ============================================================
class ControlPlaneWorker:

    def __init__(self):
        self.pool = None

    # ------------------------------------------------------------------
    # DB
    # ------------------------------------------------------------------
    async def connect_db(self):
        print("Connecting to DB...", flush=True)
        self.pool = await asyncpg.create_pool(
            dsn=PG_CONN,
            min_size=1,
            max_size=3,
            statement_cache_size=0,
        )
        print("DB ready.", flush=True)

    # ------------------------------------------------------------------
    # STARTUP ASSERTION
    # ------------------------------------------------------------------
    async def assert_canon_integrity(self):
        """Block until intel.fn_assert_canon_integrity('v1') succeeds."""
        attempt = 0
        while True:
            try:
                await self.pool.fetchval("SELECT intel.fn_assert_canon_integrity('v1')")
                print("Canon integrity OK.", flush=True)
                return
            except Exception as e:
                attempt += 1
                print(f"Canon integrity check failed (attempt {attempt}): {e}", flush=True)
                await asyncio.sleep(10)

    # ------------------------------------------------------------------
    # CYCLE
    # ------------------------------------------------------------------
    async def run_cycle(self):
        run_id = uuid.uuid4()
        print(f"[run={run_id}] cycle start", flush=True)

        # 1. Open run record
        await self.pool.execute(
            """
            INSERT INTO intel.control_plane_run_v1
                (run_id, started_at, status, fail_closed)
            VALUES ($1, now(), 'running', false)
            """,
            run_id,
        )

        try:
            # 2. Fetch active edges with kill + amp inputs
            edges = await self.pool.fetch(
                """
                SELECT edge_id, kill_input, amp_input
                FROM intel.edge_registry_v1
                WHERE is_active = true
                """
            )
            print(f"[run={run_id}] {len(edges)} active edge(s)", flush=True)

            # 3. Resolve control action per edge
            actions = []
            for edge in edges:
                row = await self.pool.fetchrow(
                    "SELECT * FROM intel.fn_resolve_control_action_v1($1, $2, $3, $4)",
                    run_id,
                    edge["edge_id"],
                    edge["kill_input"],
                    edge["amp_input"],
                )
                if row is not None:
                    actions.append(dict(row))

            # 4. Retire old rows + insert new rows (single transaction)
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        """
                        UPDATE intel.execution_control_plane_v1
                        SET retired_at = now(), is_retired = true
                        WHERE is_retired = false
                        """
                    )
                    for action in actions:
                        await conn.execute(
                            """
                            INSERT INTO intel.execution_control_plane_v1
                                (run_id, edge_id, action, is_retired, created_at)
                            VALUES ($1, $2, $3, false, now())
                            """,
                            run_id,
                            action["edge_id"],
                            action["action"],
                        )

            # 5. Write input snapshot
            await self.pool.execute(
                """
                INSERT INTO intel.control_plane_input_snapshot_v1
                    (run_id, edge_count, snapshot_ts)
                VALUES ($1, $2, now())
                """,
                run_id,
                len(edges),
            )

            # 6. Shadow cycle
            await self.pool.fetchval(
                "SELECT intel.fn_run_shadow_cycle($1)",
                run_id,
            )

            # 7. Mark run complete
            await self.pool.execute(
                """
                UPDATE intel.control_plane_run_v1
                SET
                    completed_at = now(),
                    status       = 'complete',
                    edge_count   = $2,
                    action_count = $3
                WHERE run_id = $1
                """,
                run_id,
                len(edges),
                len(actions),
            )
            print(
                f"[run={run_id}] complete — edges={len(edges)} actions={len(actions)}",
                flush=True,
            )

        except Exception as e:
            print(f"[run={run_id}] ERROR: {e}", flush=True)
            try:
                await self.pool.execute(
                    """
                    UPDATE intel.control_plane_run_v1
                    SET
                        completed_at = now(),
                        status       = 'error',
                        fail_closed  = true
                    WHERE run_id = $1
                    """,
                    run_id,
                )
            except Exception as mark_err:
                print(f"[run={run_id}] Failed to mark error: {mark_err}", flush=True)

    # ------------------------------------------------------------------
    # LOOP
    # ------------------------------------------------------------------
    async def control_loop(self):
        print("Control plane loop started.", flush=True)
        while True:
            if is_nyse_hours():
                await self.run_cycle()
            else:
                now = datetime.now(ET)
                print(
                    f"Outside NYSE hours ({now.strftime('%a %H:%M ET')}) — sleeping.",
                    flush=True,
                )
            await asyncio.sleep(CYCLE_INTERVAL)

    async def run(self):
        await self.connect_db()
        await self.assert_canon_integrity()
        await self.control_loop()


# ============================================================
# MAIN
# ============================================================
async def main():
    worker = ControlPlaneWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
