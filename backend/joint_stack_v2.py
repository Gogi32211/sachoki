"""JOINT_STACK_V2 — the same registry, on a universe where a low count means QUIET, not MISSING.

WHY. V1 passed both ladders (LIT span +8.19 MINE / +2.92 VERIFY, BULL +3.13 / +0.97, all p < 0.005)
and the post-outcome diagnostic then showed why that pass cannot be believed as it stands:

    rung        anatomy present    lbal present    dollar-vol median
    1-2 (low)        73.4 %            77.7 %          $36.7M
    7-9 (high)       99.7 %           100.0 %          $74.6M

Four of the nine layers are derived from intraday data, so a bar whose 1h/15m history is missing
scores low for a reason that has nothing to do with the market — and the names it happens to
(CRMT, CDLX, TTGT, WKHS, SEAT …) are small and troubled, which is exactly the population with bad
forward returns. The bottom rung's −7.93 is therefore at least partly a coverage artifact. This is
the SAME class of defect as the anatomy `key` bug found earlier the same day, one level further out:
I fixed "no analytics row" and did not see that "no intraday row" sat behind it.

WHAT CHANGES. Only the universe: a session survives only if every COVERAGE source has a row for it
(anatomy · lbal · vol7 · shapectx · edge_votes). OVD (38.1 %) and L-VX (32.4 %) are EVENT sources —
their absence is a real market state, not a gap — so they are not required. The ladders, the vote
tables, the statistic, the stop rule, the seeds and the shuffle count are the SAME objects, executed
from the same module.

WHAT DOES NOT CHANGE. k. This is the same question with a corrected instrument, not a new one, so
the arc's k stays at PAIR_CONFLUENCE 3 + L34_GREEN 1 + JOINT_STACK 2 = 6. V1's artifacts are kept
and marked SUPERSEDED; they are the record of what the uncorrected instrument said.

STILL NOT CONTROLLED, and stated before running: liquidity. V1's dollar-volume median doubled across
the ladder ($36.7M → $74.6M). Full coverage will shrink that gap but is not guaranteed to close it.
The per-rung liquidity profile is printed in the census; if it still slopes, a liquidity-stratified
V3 is the next step and this V2 is not a BUILD either.

RUN  backend/.venv/bin/python backend/joint_stack_v2.py
"""
from __future__ import annotations
import hashlib
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import joint_stack_family as JS                                          # noqa: E402

FAMILY = "JOINT_STACK_V2"
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/JOINT_STACK_V2"
V1_DIR = "/Users/sachoki/MASSIVE_DATA/JOINT_STACK_V1"


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def main():
    v1 = json.load(open(os.path.join(V1_DIR, "SEAL.json")))
    os.makedirs(FAMILY_DIR, exist_ok=True)
    json.dump(dict(
        supersedes="JOINT_STACK_V1", v1_sealed_at=v1.get("sealed_at"),
        v1_registry_sha256_16=v1.get("registry_sha256_16"), k_unchanged=v1.get("k"),
        change="universe only — every COVERAGE source must have a row for the session "
               f"({', '.join(JS.COVERAGE_SOURCES)})",
        reason="V1's count was confounded with data coverage: anatomy present on 73.4% of the bottom "
               "rung vs 99.7% of the top, lbal 77.7% vs 100%. Four of nine layers are intraday-"
               "derived, so a missing 1h/15m history forces a low count for non-market reasons.",
        not_controlled="liquidity — V1's dollar-volume median doubled across the ladder "
                       "($36.7M -> $74.6M). The per-rung profile is printed in the census.",
        started_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        open(os.path.join(FAMILY_DIR, "SUPERSEDES.json"), "w"), indent=1)

    JS.FAMILY, JS.FAMILY_DIR = FAMILY, FAMILY_DIR
    JS.REQUIRE_FULL_COVERAGE = True
    print(f"{FAMILY}: same registry source joint_stack_family.py {_dig(JS.__file__)}; "
          f"only the universe changes\n")
    JS.build()
    s = JS.seal()
    print(f"  sealed {s['registry_sha256_16']} · k {s['k']}\n")
    JS.outcome()


if __name__ == "__main__":
    main()
