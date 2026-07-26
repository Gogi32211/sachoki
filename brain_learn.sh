#!/bin/bash
# brain_learn.sh — the brain's daily self-learning pass (launchd, 11:00 Tbilisi), driven via the
# BACKEND API (curl). The backend has Full Disk Access, so it does all the brain/*.json reads and
# writes; this shell never touches the TCC-protected project files itself (running python directly
# here EPERM'd once when macOS re-evaluated Desktop-folder access). Fully isolated to brain/ files.
#   calibrate (own closed trades) + revalidate (data) — DAILY
#   miner (self-discovery, HEAVY) — SATURDAYS only; restart backend if it promoted combos.
set -uo pipefail
PORT="${BACKEND_PORT:-8080}"
BASE="http://127.0.0.1:$PORT/api/brain"
LOG="$HOME/Library/Logs/sachoki_brainlearn.log"
ts() { date '+%Y-%m-%d %H:%M:%S %Z'; }

echo "[$(ts)] brain_learn start" >> "$LOG"
if ! curl -sf --max-time 5 "$BASE/regime" >/dev/null 2>&1; then
  echo "[$(ts)] backend :$PORT down — skip" >> "$LOG"; exit 0
fi

# 1) outcome-learning (own closed trades) — cheap
echo "[$(ts)] calibrate:" >> "$LOG"
curl -s --max-time 120 "$BASE/learn?apply=true" >> "$LOG" 2>&1; echo "" >> "$LOG"

# 2) data-learning (re-path-sim every live edge) — warms the frame, ~2min
echo "[$(ts)] revalidate:" >> "$LOG"
curl -s --max-time 400 "$BASE/revalidate?apply=true" >> "$LOG" 2>&1; echo "" >> "$LOG"

# 3) self-discovery — Saturdays only (date +%u: 6 = Saturday). max-time outlasts the ~30-45min run.
PROMOTED=0
if [ "$(date +%u)" = "6" ]; then
  echo "[$(ts)] miner (weekly):" >> "$LOG"
  MINE="$(curl -s --max-time 3600 "$BASE/mine?apply=true" 2>&1)"
  echo "$MINE" >> "$LOG"; echo "" >> "$LOG"
  PROMOTED="$(printf '%s' "$MINE" | /usr/bin/python3 -c 'import sys,json
try: print(len(json.load(sys.stdin).get("promoted",[])))
except Exception: print(0)' 2>/dev/null || echo 0)"
else
  echo "[$(ts)] miner skipped (weekly on Sat)" >> "$LOG"
fi

# 4) if the miner promoted combos, restart backend so they build + fire live (11:00 = safe window)
if [ "${PROMOTED:-0}" -gt 0 ] 2>/dev/null; then
  echo "[$(ts)] promoted $PROMOTED combos → restarting backend" >> "$LOG"
  launchctl kickstart -k "gui/$(id -u)/com.sachoki.backend" >> "$LOG" 2>&1 || true
fi
echo "[$(ts)] brain_learn done" >> "$LOG"
