#!/bin/bash
# Wait for every sanctioned writer to finish, then fill the physics columns on 15m.
#
# `bars` has two sanctioned writers and this backfill is a third, so it does not compete for the
# lock — it waits for one. The wait is deliberately over-specified: the nightly chain is not one
# process but several in sequence (delta worker → intraday updater → derive_intraday → the 15m
# enriched top-up), and finishing the first one is not finishing the chain. A waiter that watched
# only launchd would start in the gap between two steps and collide with the next.
#
# The last gate is the honest one: try to TAKE the write lock. Process names are a proxy for
# "is someone writing"; the lock is the fact.

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1
LOG=/tmp/physics_15m.log
: > "$LOG"

say() { echo "[$(TZ=Asia/Tbilisi date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

say "waiting for the write lock on studio_15m.duckdb"

WAITED=0
while true; do
  BUSY=""
  pgrep -f "update_all.sh"         >/dev/null 2>&1 && BUSY="$BUSY update_all"
  pgrep -f "studio\._delta_worker" >/dev/null 2>&1 && BUSY="$BUSY delta_worker"
  pgrep -f "derive_intraday"       >/dev/null 2>&1 && BUSY="$BUSY derive_intraday"
  pgrep -f "update_intraday_db"    >/dev/null 2>&1 && BUSY="$BUSY intraday_update"
  pgrep -f "build_15m_base"        >/dev/null 2>&1 && BUSY="$BUSY build_15m"
  if [ "$(launchctl print gui/501/com.sachoki.dbupdate 2>/dev/null | grep -c 'state = running')" -gt 0 ]; then
    BUSY="$BUSY launchd"
  fi

  if [ -z "$BUSY" ]; then
    if .venv/bin/python - >/dev/null 2>&1 <<'PY'
import sys, duckdb
sys.path.insert(0, '.')
from studio.paths import db_path
c = duckdb.connect(db_path("studio_15m.duckdb"), read_only=False)
c.close()
PY
    then
      say "lock free after ${WAITED}s — starting"
      break
    fi
    BUSY=" duckdb-lock"
  fi

  if [ $((WAITED % 600)) -eq 0 ]; then say "still busy:${BUSY}"; fi
  sleep 30
  WAITED=$((WAITED + 30))
done

say "backfilling 15m (90.2M rows) — the guard re-checks disk and the nightly window itself"
.venv/bin/python -W ignore -c "
import sys, json; sys.path.insert(0,'.')
from studio.physics_backfill import backfill_tf
r = backfill_tf('studio_15m.duckdb', verbose=True)
print('RESULT ' + json.dumps({k: r[k] for k in
      ('db','groups','rows_written','n_errors','unharmed','minutes','disk')}))
" 2>&1 | tee -a "$LOG"

say "done"
