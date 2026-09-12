#!/bin/bash
# Wait for the DB update's delta worker to exit before building the 1H state.
# build_state peaks near 7.9 GB and the worker holds ~3.5 GB on a 16 GB machine with swap
# already 8 GB deep — the profile that took this machine down twice.
set -e
cd /Users/sachoki/Desktop/sachoki-desktop/backend
while pgrep -f "_delta_worker" >/dev/null 2>&1; do sleep 60; done
echo "=== delta worker exited, memory free ==="
nice -n 10 .venv/bin/python t5_cross_completion.py
