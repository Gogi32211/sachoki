#!/bin/bash
# Chain: FULL must finish and pass ALL its gates before the scope decision is frozen.
# Each step exits non-zero on failure and the chain stops there — no step is reached on the
# strength of an unverified predecessor.
set -e
cd /Users/sachoki/Desktop/sachoki-desktop/backend
PY=.venv/bin/python

while kill -0 33245 2>/dev/null; do sleep 20; done
echo "=== FULL run exited ==="

# The enumerator raises before writing its artifact if any gate fails, so the file's existence
# already implies all-PASS. Re-assert it anyway: the scope decision's provenance depends on it.
$PY - <<'EOF'
import json,sys
d=json.load(open("T5_15M_X_ONLY_ENUMERATION.json"))
g=d["gates"]
print("FULL gates:", {k:bool(v) for k,v in g.items()})
if not all(g.values()): sys.exit("FULL gates not all PASS — chain stops")
print(f"FULL k_final {d['k_15m_final']:,} · digest {d['class_digest']}")
EOF

echo "=== freeze scope ==="
$PY t5_15m_scope_v2.py

echo "=== TWO_BAR enumeration ==="
T5_15M_SCOPE=TWO_BAR $PY t5_15m_enumerate.py

echo "=== subset equivalence gate ==="
$PY t5_15m_subset_gate.py
echo "=== CHAIN COMPLETE ==="
