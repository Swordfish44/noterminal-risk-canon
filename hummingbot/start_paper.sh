#!/usr/bin/env bash
# start_paper.sh — launches Hummingbot paper trading (headless, no terminal)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# ── Load env from project root ────────────────────────────────────────────────
ENV_FILE="$SCRIPT_DIR/../.env"
if [ -f "$ENV_FILE" ]; then
  set -a
  # shellcheck source=/dev/null
  source "$ENV_FILE"
  set +a
else
  echo "[start_paper] WARNING: .env not found at $ENV_FILE"
fi

# ── Validate required vars ────────────────────────────────────────────────────
if [ -z "${DATABASE_URL:-}" ]; then
  echo "[start_paper] ERROR: DATABASE_URL is not set in .env"
  exit 1
fi

export HUMMINGBOT_PASSWORD="${HUMMINGBOT_PASSWORD:-password}"

# ── Tear down any stale container ─────────────────────────────────────────────
if docker ps -a --format '{{.Names}}' | grep -q '^hummingbot$'; then
  echo "[start_paper] Removing stale container..."
  docker rm -f hummingbot >/dev/null
fi

# ── Start ─────────────────────────────────────────────────────────────────────
echo "[start_paper] Launching Hummingbot (headless, paper trading)..."
docker-compose up -d

# ── Wait for healthy startup ───────────────────────────────────────────────────
echo "[start_paper] Waiting for strategy to start..."
MAX_WAIT=90
ELAPSED=0
INTERVAL=3

while [ "$ELAPSED" -lt "$MAX_WAIT" ]; do
  LOG=$(docker logs hummingbot 2>&1 || true)

  if echo "$LOG" | grep -qiE "Starting V2 script strategy|Paper trade balance"; then
    echo "[start_paper] Strategy running."
    break
  fi

  if echo "$LOG" | grep -qiE "Invalid password|Failed to load|Error loading|Exiting"; then
    echo "[start_paper] ERROR: Hummingbot failed to start. Last logs:"
    docker logs --tail 30 hummingbot 2>&1
    exit 1
  fi

  sleep "$INTERVAL"
  ELAPSED=$((ELAPSED + INTERVAL))
done

if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
  echo "[start_paper] WARNING: strategy start not confirmed after ${MAX_WAIT}s — showing last 20 log lines:"
  docker logs --tail 20 hummingbot 2>&1
fi

# ── Tail logs ─────────────────────────────────────────────────────────────────
echo ""
echo "[start_paper] Tailing logs (Ctrl+C to stop following — container keeps running)..."
echo "  To stop:   docker stop hummingbot"
echo "  To check:  docker logs --tail 50 hummingbot"
echo ""
docker logs -f hummingbot 2>&1
