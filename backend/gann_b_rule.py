"""GANN_B_RULE_V1 — the bars-per-step rule, frozen before X construction and before any Y.

CLASSIFICATION: LEGACY_INTENT_OPERATIONALIZATION — not runtime parity.

The legacy Pine source declares the bars-per-step state as integer throughout:

    int barsDiff = math.abs(finalHighBar - finalLowBar)
    int bpsA     = math.max(minBarsPerStep, barsDiff / autoLevels)
    var int lockedBarsPerStep = na
    var int finalBarsPerStep  = na

The author's intent is therefore unambiguous: an integer number of bars per step. What is
NOT unambiguous is what the v5 runtime did with `barsDiff / autoLevels`, because Pine's
qualifier-dependent integer division can retain a fractional remainder when an operand is
series/input — and confirming that would need a compiler fixture, not a reading. So this
artifact freezes the INTENT and says so; it does not claim byte parity with whatever the
old chart executed.

    AUTO_LEVELS       = 6
    MIN_BARS_PER_STEP = 20
    B = max(20, floor(barsDiff / 6))          barsDiff >= 0, so floor == truncation

Freezing one rule before Y is the whole point: running floor / round / exact and keeping
whichever performs best would manufacture four grid hypotheses and the multiplicity to go
with them. No alternative B rule may be searched in V1.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import math, os, sys, time                                           # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402

OUT = "GANN_B_RULE_V1.json"
AUTO_LEVELS = 6
MIN_BARS_PER_STEP = 20
FIXTURES = {119: 20, 120: 20, 121: 20, 125: 20, 131: 21, 137: 22, 143: 23}


def B(bars_diff: int) -> int:
    return max(MIN_BARS_PER_STEP, math.floor(bars_diff / AUTO_LEVELS))


def main():
    got = {d: B(d) for d in FIXTURES}
    ok = got == FIXTURES
    # the discriminating alternatives, recorded so the choice is visible, not hidden
    alts = {}
    for name, fn in (("floor", lambda d: math.floor(d / AUTO_LEVELS)),
                     ("round", lambda d: int(round(d / AUTO_LEVELS))),
                     ("trunc", lambda d: int(d / AUTO_LEVELS)),
                     ("exact", lambda d: d / AUTO_LEVELS)):
        alts[name] = {str(d): max(MIN_BARS_PER_STEP, fn(d)) for d in FIXTURES}
    body = dict(
        spec_id="GANN_B_RULE_V1", status="FROZEN",
        classification="LEGACY_INTENT_OPERATIONALIZATION",
        not_claimed="exact Pine runtime parity — no compiler/runtime fixture was executed",
        statement="The legacy source consistently declares the bars-per-step state as "
                  "integer; V1 freezes the corresponding truncating integer "
                  "operationalization before X construction and before any Y access.",
        legacy_source_declarations=[
            "int barsDiff = math.abs(finalHighBar - finalLowBar)",
            "int bpsA     = math.max(minBarsPerStep, barsDiff / autoLevels)",
            "var int lockedBarsPerStep = na",
            "var int finalBarsPerStep  = na"],
        runtime_ambiguity="Pine v5 integer division is qualifier-dependent: with a series "
                          "or input operand a fractional remainder can survive, so the "
                          "literal source does not by itself fix the runtime semantics",
        rule=dict(auto_levels=AUTO_LEVELS, min_bars_per_step=MIN_BARS_PER_STEP,
                  formula="B = max(20, floor(barsDiff / 6))",
                  note="barsDiff >= 0, so floor == truncation toward zero"),
        fixtures={str(k): v for k, v in FIXTURES.items()},
        fixtures_reproduced=ok,
        alternatives_considered=alts,
        alternatives_forbidden="no alternative B rule (round / exact / other lookback or "
                               "level counts) may be searched in V1 — running several and "
                               "keeping the best would create four grid hypotheses and "
                               "their multiplicity",
        binding="gann_grid.bars_per_step consumes GANN_B_RULE='floor' bound to this "
                "artifact; the module refuses to build a grid on any unsealed rule",
        outcome_exposure="NOT_EXPOSED — frozen before X construction and before any Y",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(body, OUT, required=("spec_id", "classification", "rule", "fixtures"),
                 supersede=os.path.exists(OUT))
    for k, v in FIXTURES.items():
        print(f"  barsDiff {k:>4} -> B {got[k]:>3} · expected {v:>3} · "
              f"{'ok' if got[k] == v else 'MISMATCH'}")
    print(f"\nGANN_B_RULE_V1 · {d} · fixtures reproduced {ok}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
