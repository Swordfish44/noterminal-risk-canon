#!/usr/bin/env bash
# Launches all three workers as sibling processes.
# Exits (with the crashed process's exit code) as soon as any worker dies,
# so Render restarts the service rather than running with a silent failure.
set -euo pipefail

python workers/ops_worker.py &
python workers/portfolio_sync.py &
python workers/micro_features_worker.py &

wait -n
