"""The checkpoint holds: the same five fixtures still produce the same drawer structure.

Structure, not text. A golden made of rendered strings fails the first time a column widens,
and today that exact confusion cost one test failure and a wrong diagnosis before it.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import studio_foundation as F                                       # noqa: E402

GOLD = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                   "STUDIO_FOUNDATION.json")))
fx = F.fixtures()
bad = []
for name, want in GOLD["goldens"].items():
    got = F.drawer_structure(fx[name])
    if got != want:
        for k in want:
            if got.get(k) != want[k]:
                bad.append(f"{name}.{k}")
print("=" * 92)
print(f"  FOUNDATION CHECKPOINT · {GOLD['checkpoint']}")
print("=" * 92)
print(f"  fixtures      {', '.join(GOLD['fixtures'])}")
print(f"  blocked pair  G1 vs G2 → {GOLD['blocked_pair']['result']}")
for f_, h in GOLD["modules"].items():
    now = __import__("subprocess").check_output(
        ["git", "hash-object", f_],
        cwd=os.path.dirname(os.path.abspath(__file__))).decode().strip()
    print(f"  {f_:<24s} {'MATCH' if now == h else 'MOVED'}  {h[:16]}")
print("=" * 92)
print(f"  goldens: {'ALL MATCH' if not bad else 'DRIFT in ' + ', '.join(bad)}")
sys.exit(1 if bad else 0)
