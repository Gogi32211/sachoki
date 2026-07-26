#!/usr/bin/env bash
# update_all_dbs.sh — ganaxlebs yvela DB-s: 1D + 1H + 4H + 1W
#
# gamoyeneba:
#   ./update_all_dbs.sh           # yvela
#   ./update_all_dbs.sh --no-1d   # mxolod intraday/weekly (1D-is gareSe)
#   ./update_all_dbs.sh --only 1h # mxolod erTi tf
#   WORKERS=6 ./update_all_dbs.sh
set -uo pipefail

BACKEND_DIR="$(cd "$(dirname "$0")/backend" && pwd)"
PY="$BACKEND_DIR/.venv/bin/python"
WORKERS="${WORKERS:-$(python3 -c 'import os; print(max(2, (os.cpu_count() or 4) - 1))')}"

SKIP_1D=0
ONLY_TF=""

for arg in "$@"; do
  case "$arg" in
    --no-1d)    SKIP_1D=1 ;;
    --only)     ;;
    1h|4h|1w)   ONLY_TF="$arg" ;;
  esac
done

echo "════════════════════════════════════════════════════════"
echo "  DB Update  $(date '+%Y-%m-%d %H:%M')"
echo "════════════════════════════════════════════════════════"

# ── 1) 1D Studio DB (incremental via backend endpoint) ───────────────────────
if [ "$SKIP_1D" = "0" ] && [ -z "$ONLY_TF" ]; then
  echo ""
  echo "── [1/4] 1D Studio DB ──────────────────────────────────"
  cd "$(dirname "$0")" && bash update_db.sh
fi

# ── helper: run intraday/weekly update ───────────────────────────────────────
run_tf() {
  local tf="$1"
  echo ""
  echo "── [$2/4] ${tf} DB ─────────────────────────────────────"
  cd "$BACKEND_DIR"
  "$PY" update_intraday_db.py --tf "$tf" --workers "$WORKERS"
  # verify last date after update
  DB="$HOME/Downloads/studio_${tf}.duckdb"
  "$PY" - "$DB" <<'PY'
import sys, duckdb
c = duckdb.connect(sys.argv[1], read_only=True)
r = c.execute("SELECT max(date) FROM bars").fetchone()
n = c.execute("SELECT count(distinct ticker) FROM bars").fetchone()
print(f"  ✓ last={r[0]}  tickers={n[0]}")
c.close()
PY
}

# ── 2-4) Intraday / Weekly ────────────────────────────────────────────────────
if [ -z "$ONLY_TF" ]; then
  run_tf "1h" 2
  run_tf "4h" 3
  run_tf "1w" 4
else
  case "$ONLY_TF" in
    1h) run_tf "1h" 1 ;;
    4h) run_tf "4h" 1 ;;
    1w) run_tf "1w" 1 ;;
  esac
fi

echo ""
echo "════════════════════════════════════════════════════════"
echo "  ✅ yvela DB ganaxlebuli  $(date '+%H:%M:%S')"
echo "════════════════════════════════════════════════════════"
