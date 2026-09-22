#!/bin/bash
# What update_all.sh must do when the 1H/4H trust gate fails.
#
# On 2026-09-22 the dual writer returned NOT PASS and exited 2, and eight downstream stages then
# built a derived layer on a store the gate had just refused to certify — because every stage was
# written as `command || echo "⚠ failed"`, which turns an exit code into a sentence. The opposite
# reflex is just as bad: killing the run would have cost the options snapshot, which has no history
# API and cannot be recaptured.
#
# So the contract is dependency-aware: skip what READS the 1H/4H canonical, run everything else,
# and never gate GEX. These tests stub every python call, so nothing touches a store or the vendor.
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PASS=0; FAIL=0
ok(){ if [ "$1" = "1" ]; then echo "  ✓ $2"; PASS=$((PASS+1)); else echo "  ✗ $2"; FAIL=$((FAIL+1)); fi; }
ran(){ grep -q "^$1\$" "$T/calls" && echo 1 || echo 0; }

setup(){
  T="$(mktemp -d)"; export T
  mkdir -p "$T/backend/.venv/bin" "$T/Library/Logs"
  cp "$ROOT/update_all.sh" "$T/update_all.sh"
  printf '#!/bin/bash\nexit 0\n' > "$T/update_db.sh"; chmod +x "$T/update_db.sh"
  # stub interpreter: records the script name, and fails the one named in FAIL_SCRIPT
  cat > "$T/backend/.venv/bin/python" <<'PY'
#!/bin/bash
s=""
for a in "$@"; do case "$a" in *.py) s="$(basename "$a")"; break;; esac; done
[ -n "$s" ] && echo "$s" >> "$T/calls"
if [ "$s" = "${FAIL_SCRIPT:-}" ]; then exit "${FAIL_CODE:-2}"; fi
exit 0
PY
  chmod +x "$T/backend/.venv/bin/python"
  : > "$T/calls"
}
run(){ ( export HOME="$T"; cd "$T" && BACKEND_PORT="${BACKEND_PORT:-8080}" bash ./update_all.sh ); echo $? > "$T/rc"; }

echo "── 1 · trust gate PASSES: everything runs, status PASS ──"
setup; FAIL_SCRIPT="" run
ok "$(ran update_intraday_db.py)" "dual writer ran"
ok "$(ran backfill_intraday_fwd.py)" "1H/4H forward labels ran"
ok "$(ran lbal_build.py)" "LBAL ran"
ok "$(ran anatomy_build.py)" "anatomy ran"
ok "$(ran shape_ctx_build.py)" "SHAPE_CTX ran"
ok "$(ran gex_edge_logger.py)" "GEX ran"
ok "$(grep -qc 'STATUS: PASS' "$T/Library/Logs/sachoki_update.log" && echo 1 || echo 0)" "status line says PASS"
ok "$([ "$(cat "$T/rc")" = "0" ] && echo 1 || echo 0)" "exit 0"

echo "── 2 · trust gate FAILS: dependents skipped, independents still run ──"
setup; FAIL_SCRIPT="update_intraday_db.py" FAIL_CODE=2 run
ok "$([ "$(ran backfill_intraday_fwd.py)" = "0" ] && echo 1 || echo 0)" "forward labels SKIPPED (read 1H/4H)"
ok "$([ "$(ran lbal_build.py)" = "0" ] && echo 1 || echo 0)" "LBAL SKIPPED (ATTACHes studio_1h)"
ok "$([ "$(ran anatomy_build.py)" = "0" ] && echo 1 || echo 0)" "anatomy SKIPPED (opens studio_1h)"
ok "$(ran build_15m_base.py)" "15m base STILL RAN (independent)"
ok "$(ran derive_intraday.py)" "15m enriched STILL RAN (independent)"
ok "$(ran shape_ctx_build.py)" "SHAPE_CTX STILL RAN (1D only)"
ok "$(ran gex_edge_logger.py)" "GEX STILL RAN — never gated, cannot be recaptured"
ok "$(grep -qc 'STATUS: DEGRADED' "$T/Library/Logs/sachoki_update.log" && echo 1 || echo 0)" "status line says DEGRADED"
ok "$([ "$(cat "$T/rc")" = "3" ] && echo 1 || echo 0)" "exit 3"
ok "$(grep -qc 'TRUST GATE FAILED' "$T/Library/Logs/sachoki_update.log" && echo 1 || echo 0)" "the gate failure is stated, not implied"

echo "── 3 · GEX fails: the whole night is FAILED, not merely degraded ──"
setup; FAIL_SCRIPT="gex_edge_logger.py" FAIL_CODE=1 run
ok "$(grep -qc 'STATUS: FAILED' "$T/Library/Logs/sachoki_update.log" && echo 1 || echo 0)" "status line says FAILED"
ok "$([ "$(cat "$T/rc")" = "1" ] && echo 1 || echo 0)" "exit 1"
ok "$(ran lbal_build.py)" "the 1H branch was unaffected by an unrelated GEX failure"

echo
echo "passed $PASS · failed $FAIL"
[ "$FAIL" = "0" ]
