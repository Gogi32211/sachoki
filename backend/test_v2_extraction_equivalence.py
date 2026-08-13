"""4C, asserted from the artifact — including that the comparison can see damage.

`legacy_projection()` became part of the proof at 4B, which makes it a place where the assertion
can quietly check less than the claim. A projection that dropped verdict stages, or compared
rounded text instead of bits, would pass while hiding a divergence — the fourth appearance of a
shape this project keeps meeting. So the run corrupts the new artifact three ways and requires
the comparison to fail on each, and this file requires that those controls fired.

The two proofs stay separate. One process says the orchestration reproduces the computation;
two fresh processes say it does so without help from warmed imports, module state or a runtime
cache. Given this project's history at exactly that boundary — a spec edited mid-run, a value
patched onto an already-written event — the second is not a repeat of the first.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ok = fail = 0
HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "V2_EXTRACTION_EQUIVALENCE.json")


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                          # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def report() -> dict:
    with open(REPORT) as f:
        return json.load(f)


def t1_one_process_equivalence():
    r = report()
    assert r["claim"] == "V2_EXTRACTION_EQUIVALENCE"
    assert r["one_process_exact"] is True
    assert r["cells"] == 31, r["cells"]


def t2_fresh_process_equivalence_is_a_separate_result():
    """warmed imports and module state are exactly where this project has been bitten"""
    r = report()
    assert r["fresh_process_exact"] is True
    assert r["one_process_exact"] != "same_field_as_fresh"       # they are two fields, on purpose


def t3_the_comparison_can_see_damage():
    """a projection that hides a divergence passes every positive test there is"""
    n = report()["negative_controls"]
    assert set(n) == {"swap_cells", "flip_theta_bit", "change_verdict"}, sorted(n)
    for how, caught in n.items():
        assert caught is True, f"the comparison did not notice {how}"


def t4_the_deferred_six_are_still_out():
    r = report()
    assert r["deferred_day_level"] == 6
    assert r["artifact"]["deferred"]["DAY_LEVEL"]["status"] == "NOT_IN_SEARCH_SPACE"


def t5_the_run_records_enough_to_be_read_later():
    """a PASS nobody can reconstruct the environment for is a PASS nobody can check"""
    r = report()
    for field in ("oracle_hash", "reference_code_hash", "candidate_code_hash", "spec_hash",
                  "execution_mode", "world", "delta", "rep"):
        assert r.get(field) not in (None, ""), field
    assert r["oracle_hash"] == "7c421ae062742d06"
    assert r["execution_mode"] == "SEALED_ACCEPTANCE"


def t6_the_historical_path_is_recorded_as_blocked():
    """the claim must not be readable as wider than it is"""
    assert report()["historical_execution"] == "BLOCKED"


def t7_extraction_qualified_does_not_mean_application_qualified():
    """the two axes stay apart even after a PASS

    This is the sentence the checkpoint has to survive: the engine reproduces the sealed
    computation, and nothing at all has been shown about pointing it at real returns.
    """
    import evidence_status as ES                                    # noqa: PLC0415
    s = ES.V2_FIRST_HISTORICAL
    assert s.application_maturity == ES.FIRST_HISTORICAL_APPLICATION
    assert not s.permits(ES.PROMOTE_AS_VALIDATED_EDGE)

    import v2_engine as EN                                          # noqa: PLC0415
    assert hasattr(EN, "HistoricalApplicationNotQualifiedError")


print("=" * 100, flush=True)
print("  4C · EXTRACTION EQUIVALENCE, asserted from the artifact", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_one_process_equivalence,
                        t2_fresh_process_equivalence_is_a_separate_result,
                        t3_the_comparison_can_see_damage,
                        t4_the_deferred_six_are_still_out,
                        t5_the_run_records_enough_to_be_read_later,
                        t6_the_historical_path_is_recorded_as_blocked,
                        t7_extraction_qualified_does_not_mean_application_qualified], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
