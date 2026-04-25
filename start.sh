#!/usr/bin/env bash
# ops_worker is the critical process — if it dies, exit so Render restarts
# the whole service. All other workers restart themselves on failure.
set -euo pipefail

# ── Non-critical workers: restart individually on crash ───────────────────

(while true; do python workers/portfolio_sync.py;            echo "portfolio_sync exited $? — restarting";            sleep 5;  done) &
(while true; do python workers/micro_features_worker.py;     echo "micro_features exited $? — restarting";            sleep 5;  done) &
(while true; do python workers/edge_signals_worker.py;       echo "edge_signals exited $? — restarting";              sleep 5;  done) &
# nautilus_worker.py — SUSPENDED pending bandwidth audit
# (while true; do python workers/nautilus_worker.py; echo "nautilus_worker exited $? — restarting"; sleep 5; done) &
(while true; do python workers/control_plane_worker.py;      echo "control_plane_worker exited $? — restarting";      sleep 2;  done) &
(while true; do python workers/retention_worker.py;          echo "retention_worker exited $? — restarting";          sleep 5;  done) &
(while true; do python workers/commodity_ingest_worker.py;   echo "commodity_ingest_worker exited $? — restarting";   sleep 5;  done) &
(while true; do python workers/kalshi_ingest_worker.py;      echo "kalshi_ingest_worker exited $? — restarting";      sleep 5;  done) &
(while true; do python workers/gdelt_ingest_worker.py;       echo "gdelt_ingest_worker exited $? — restarting";       sleep 5;  done) &
(while true; do python workers/treasury_yield_worker.py;     echo "treasury_yield_worker exited $? — restarting";     sleep 5;  done) &

# ── Weekly / slow workers ─────────────────────────────────────────────────

(while true; do python workers/cot_worker.py;                echo "cot_worker exited $? — restarting in 60s";         sleep 60; done) &
(while true; do python workers/trace_worker.py;              echo "trace_worker exited $? — restarting in 60s";       sleep 60; done) &
(while true; do python workers/comtrade_worker.py;           echo "comtrade_worker exited $? — restarting in 60s";    sleep 60; done) &
(while true; do python workers/manifest_worker.py;           echo "manifest_worker exited $? — restarting in 60s";    sleep 60; done) &

# ── Daily workers ─────────────────────────────────────────────────────────

(while true; do python workers/ipo_worker.py;                echo "ipo_worker exited $? — restarting in 60s";         sleep 60; done) &
(while true; do python workers/options_flow_worker.py;       echo "options_flow_worker exited $? — restarting in 60s"; sleep 60; done) &

# ── Continuous / streaming workers ───────────────────────────────────────

(while true; do python workers/ais_stream_worker.py;         echo "ais_stream_worker exited $? — restarting in 10s";  sleep 10; done) &
(while true; do python workers/supply_chain_signal_worker.py; echo "supply_chain_signal_worker exited $? — restarting in 30s"; sleep 30; done) &
(while true; do python workers/news_nlp_worker.py;           echo "news_nlp_worker exited $? — restarting in 30s";    sleep 30; done) &
(while true; do python workers/regime_cartographer_worker.py; echo "regime_cartographer_worker exited $? — restarting in 60s"; sleep 60; done) &

# ── Critical worker — exit triggers Render restart of the whole service ──
python workers/ops_worker.py