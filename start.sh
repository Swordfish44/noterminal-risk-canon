#!/usr/bin/env bash
# ops_worker is the critical process — if it dies, exit so Render restarts
# the whole service. The other three workers restart themselves on failure.
set -euo pipefail

# Non-critical workers: restart individually on crash without killing the service.
(while true; do python workers/portfolio_sync.py;      echo "portfolio_sync exited $? — restarting"; sleep 5; done) &
(while true; do python workers/micro_features_worker.py; echo "micro_features exited $? — restarting"; sleep 5; done) &
(while true; do python edge_signals_worker.py;         echo "edge_signals exited $? — restarting";   sleep 5; done) &

# Critical worker — exit triggers Render restart of the whole service.
python workers/ops_worker.py
