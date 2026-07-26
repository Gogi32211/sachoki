#!/bin/bash
# paper_loop.sh — the autonomous PAPER trading loop, fired at US market open (launchd).
# NO real money / NO broker — it only drives the brain's paper book via the backend API:
#   1. auto-CLOSE positions that hit the validated exit (structural stop / 25% trail / 60-bar)
#   2. auto-TAKE today's allocated BUYs into the paper book
# Both run INSIDE the backend (Full Disk Access) via curl, so this shell never touches the
# TCC-protected project files itself. Closed trades feed the autopsy → calibrate learning loop.
set -uo pipefail
PORT="${BACKEND_PORT:-8080}"
BASE="http://127.0.0.1:$PORT/api/brain"
LOG="$HOME/Library/Logs/sachoki_paperloop.log"
ts() { date '+%Y-%m-%d %H:%M:%S %Z'; }

echo "[$(ts)] paper_loop start" >> "$LOG"
# backend up?
if ! curl -sf --max-time 5 "$BASE/regime" >/dev/null 2>&1; then
  echo "[$(ts)] backend :$PORT down — skip" >> "$LOG"; exit 0
fi
# 1) exits first (free slots) — up to 200s (frame may need warming)
echo "[$(ts)] auto-close:" >> "$LOG"
curl -s --max-time 260 "$BASE/auto-close?apply=true" >> "$LOG" 2>&1; echo "" >> "$LOG"
# 2) then take today's allocated BUYs
echo "[$(ts)] auto-take:" >> "$LOG"
curl -s --max-time 260 "$BASE/auto-take?apply=true" >> "$LOG" 2>&1; echo "" >> "$LOG"
echo "[$(ts)] paper_loop done" >> "$LOG"
