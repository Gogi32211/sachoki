#!/bin/bash
# Recompute sig_cisd_* on every timeframe after the bar-0 seeding fix.
# Smallest DB first: the machinery proves itself on 1.1M rows before it reaches 90M.
# The guards live in the module (nightly window, disk headroom, unharmed fingerprint)
# and are re-checked per timeframe, not once at the start — the window can close mid-run.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
LOG=/tmp/cisd_backfill.log
: > "$LOG"
say() { echo "[$(TZ=Asia/Tbilisi date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

for DB in studio_1w.duckdb studio_analytics.duckdb studio_4h.duckdb studio_1h.duckdb studio_15m.duckdb; do
  say "=== $DB ==="
  .venv/bin/python -W ignore -c "
import sys, json; sys.path.insert(0,'.')
from studio.cisd_backfill import backfill_tf
r = backfill_tf('$DB', verbose=True)
print('RESULT ' + json.dumps({k: r[k] for k in
      ('db','groups','rows_written','before','after','n_errors','unharmed','minutes')}))
" 2>&1 | tee -a "$LOG"
  say "--- $DB done ---"
done
say "ALL TIMEFRAMES DONE"
