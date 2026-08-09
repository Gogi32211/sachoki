"""B1b — reconstruct the historical trial count from the study scripts on disk.

The ledger only sees studies run from now on. But roughly fifty studies already happened, and
their `n_trials` were declared by hand inside each script. Those files are committed, so the
count is partially recoverable — and a partially recoverable floor is worth far more than
pretending the history started today.

WHAT THIS PRODUCES, AND WHAT IT DOES NOT

  N_low   the sum of what the scripts THEMSELVES declared. Solid: it is written in the file.
  N_mid   the number of scoring calls actually made — usually higher than the declaration,
          because studies grow while they run. Counted from the source, still solid.
  N_high  N_mid inflated for the exploratory runs that were never saved. This one is a
          GUESS, and it is labelled as such wherever it appears.

Deliberately NOT claimed: that any of these is the true search burden. Runs made in a shell,
abandoned notebooks and variants edited in place before committing are all invisible. Every
figure here is a FLOOR.
"""
from __future__ import annotations

import ast
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ledger import family_of, log_trial          # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
UNSAVED_MULT = 2.5        # stated guess for exploratory runs never committed

# scripts that are studies, not engine/runtime code
PATTERNS = ("validate_", "study_", "verify_", "_scan", "_study", "h1_step", "hb_",
            "macro_", "tz_", "portfolio_sim", "seq_", "check_legacy", "edge_echo",
            "adx_regime", "coil_score", "wavetrend", "short_", "car_", "cost_wmt",
            "d1_segments", "h4_w1", "m15_seg", "register_seq", "splice_", "ca_impact",
            "survivorship", "truncation", "path_fix", "demo_old", "absorb_base")
SKIP = {"ledger.py", "ledger_backfill.py", "research_kit.py", "edge_replay.py",
        "overfit_stats.py", "analysis_kit.py"}

# a "scoring call" = one cell measured. These are the shapes our studies actually use.
CALL_RE = re.compile(r"\b(?:run|score|sc|cell|_score|row)\s*\(")


def scan(path: str) -> dict | None:
    try:
        src = open(path, encoding="utf-8", errors="ignore").read()
    except OSError:
        return None
    if "_pathsim" not in src and "EdgeStudy" not in src and "Study(" not in src:
        return None
    declared = 0
    m = re.search(r"^N_TRIALS\s*=\s*(\d+)", src, re.M)
    if m:
        declared = int(m.group(1))
    else:
        m2 = re.search(r"n_trials\s*=\s*(\d+)", src)
        declared = int(m2.group(1)) if m2 else 0
    calls = len(CALL_RE.findall(src))
    # loops multiply cells; count the obvious sweeps
    loops = len(re.findall(r"^\s*for\s+\w+.*\bin\b.*[\[\(]", src, re.M))
    return dict(script=os.path.basename(path), declared=declared, calls=calls, loops=loops)


rows = []
for fn in sorted(os.listdir(HERE)):
    if not fn.endswith(".py") or fn in SKIP:
        continue
    if not any(p in fn for p in PATTERNS):
        continue
    r = scan(os.path.join(HERE, fn))
    if r:
        rows.append(r)

print(f"study scripts found on disk: {len(rows)}\n", flush=True)
n_low = sum(r["declared"] for r in rows)
n_mid = sum(max(r["declared"], r["calls"]) for r in rows)
n_high = int(n_mid * UNSAVED_MULT)
declared_missing = sum(1 for r in rows if r["declared"] == 0)

print(f"  {'script':38s} {'declared':>9s} {'calls':>6s} {'loops':>6s}", flush=True)
for r in sorted(rows, key=lambda x: -max(x["declared"], x["calls"]))[:22]:
    print(f"  {r['script']:38s} {r['declared']:>9d} {r['calls']:>6d} {r['loops']:>6d}",
          flush=True)

print(f"\n{'='*84}", flush=True)
print(f"  N_low   {n_low:>6,}   sum of the n_trials each script declared", flush=True)
print(f"  N_mid   {n_mid:>6,}   scoring calls actually present in the source", flush=True)
print(f"  N_high  {n_high:>6,}   N_mid × {UNSAVED_MULT} for runs never committed  ⚠ GUESS",
      flush=True)
print(f"\n  scripts with NO declared n_trials: {declared_missing} of {len(rows)} "
      f"— these ran with no multiplicity control at all", flush=True)
print(f"\n  ⚠ ALL THREE ARE FLOORS. Shell one-liners, abandoned notebooks and variants", flush=True)
print(f"    edited in place before committing are invisible to this scan.", flush=True)
print("=" * 84, flush=True)

if "--write" in sys.argv:
    for r in rows:
        log_trial(f"[backfill] {r['script']}", family=r["script"],
                  n_cells=max(r["declared"], r["calls"]),
                  verdict="", params={"declared": r["declared"], "calls": r["calls"],
                                      "source": "backfill"},
                  script=r["script"])
    print(f"\n  wrote {len(rows)} backfill rows to the ledger", flush=True)
else:
    print("\n  (dry run — pass --write to append these to the ledger)", flush=True)
