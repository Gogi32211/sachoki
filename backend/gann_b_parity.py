"""GANN_B_PARITY_V1 — settle the legacy Pine integer semantics for finalBarsPerStep.

B = max(20, reduce(barsDiff / 6)). The four plausible reductions disagree on exactly the
values the review named, and B is the SLOPE of every line in the lattice, so a wrong
choice tilts the whole grid:

    barsDiff   floor   round   trunc   exact        B(floor)  B(round)  B(exact)
       119       19      20      19    19.8333          20        20     20.0000
       120       20      20      20    20.0000          20        20     20.0000
       121       20      20      20    20.1667          20        20     20.1667
       125       20      21      20    20.8333          20        21     20.8333
       131       21      22      21    21.8333          21        22     21.8333

This module does not guess. It takes the Pine-side outputs for the fixtures, finds which
rule (if any) reproduces ALL of them, and seals that rule with the evidence. If no single
rule matches every fixture, it seals FAIL and the grid stays unbuilt.

    usage:  python gann_b_parity.py '{"119": 20, "125": 21, ...}'
            (the mapping is barsDiff -> the B Pine actually used)

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import json, math, os, sys, time                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import gann_grid as GG                                               # noqa: E402

OUT = "GANN_B_PARITY_V1.json"
FIXTURES = (119, 120, 121, 125, 131, 137, 143, 20, 19, 6, 1)


def main():
    if len(sys.argv) < 2:
        print("fixtures needing a Pine-side answer (barsDiff -> B):")
        for d in FIXTURES:
            cands = {k: max(GG.MIN_BARS_PER_STEP, f(d)) for k, f in GG.B_RULES.items()}
            spread = len(set(map(float, cands.values())))
            print(f"  barsDiff {d:>4} -> " +
                  " · ".join(f"{k} {v}" for k, v in cands.items())
                  + ("   <-- DISCRIMINATING" if spread > 1 else ""))
        print("\nrun again with the Pine outputs, e.g.:")
        print("  python gann_b_parity.py '{\"125\": 21, \"131\": 22}'")
        return
    observed = {int(k): float(v) for k, v in json.loads(sys.argv[1]).items()}
    verdict = {}
    for name, fn in GG.B_RULES.items():
        ok = all(abs(max(GG.MIN_BARS_PER_STEP, fn(d)) - b) < 1e-9
                 for d, b in observed.items())
        verdict[name] = ok
    winners = [k for k, v in verdict.items() if v]
    result = "PASS" if len(winners) == 1 else ("AMBIGUOUS" if winners else "FAIL")
    body = dict(
        spec_id="GANN_B_PARITY_V1", status="FROZEN" if result == "PASS" else "OPEN",
        result=result,
        question="which integer reduction of barsDiff/6 does the legacy Pine use?",
        pine_observed={str(k): v for k, v in observed.items()},
        candidate_rules={k: {str(d): max(GG.MIN_BARS_PER_STEP, f(d)) for d in FIXTURES}
                         for k, f in GG.B_RULES.items()},
        rules_consistent_with_pine=winners,
        sealed_rule=winners[0] if result == "PASS" else None,
        formula="B = max(20, <rule>(barsDiff / 6))",
        why_it_matters="B is the slope of every ascending and descending line; floor and "
                       "round differ at barsDiff 125 and 131, which tilts the entire "
                       "lattice and moves every touch",
        binding="gann_grid.bars_per_step refuses to run until GANN_B_RULE names the "
                "sealed rule; no grid may be built on a guessed reduction",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(body, OUT, required=("spec_id", "result", "pine_observed"),
                 supersede=os.path.exists(OUT))
    print(f"rules consistent with Pine: {winners or 'NONE'} · {result}")
    print(f"GANN_B_PARITY_V1 · {d}")


if __name__ == "__main__":
    main()
