"""Five fixtures for one drawer. It is ready when all five read correctly and none read alike.

    1  G1  0.065        INFERENTIAL · VALID
    2  G2  0.685        INFERENTIAL · VALID, generator limitation must surface
    3  N1  θ = 0        DETERMINISTIC — no interval, and NOT APPLICABLE says why
    4  N2  +0.0038pp    INFERENTIAL — uncertainty mandatory
    5      INVALID+BUILD  the conclusion is shown and is not a result

These are the most dangerous semantic distinctions the system contains. A drawer that survives
them survives most of the rest.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import semantic_inspector as INS                                    # noqa: E402
from sampling_target import structured_permutation_null, synthetic_dgp  # noqa: E402
from semantic_metric import (BUILD, DETERMINISTIC, INFERENTIAL, INVALID, VALID,  # noqa: E402
                             ConditioningSpec, Known, NotApplicable, Provenance,
                             SemanticMetric, Unknown, can_compare)

ok = fail = 0


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                          # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


COND = (ConditioningSpec("Date", "IS", "trading date"),
        ConditioningSpec("BaseSetup", "IS", "family"))
P_N0 = Provenance("N0-2026-08-11", "3c6ccda05dde7eb0", "n0_run@2591906", "bars@2026-08-10")
P_V2 = Provenance("N1N2P1-2026-08-11", "38f2665d653beb64", "combolab_v2@2e4257a",
                  "bars@2026-08-10")

G1 = SemanticMetric(0.065, INFERENTIAL, "family_wise_false_promotion_rate",
                    structured_permutation_null("within_stratum_outcome_v1"), COND, P_N0,
                    uncertainty=Known("200 null worlds · nominal family level 0.05"),
                    population=Known("31 OPPORTUNITY_LEVEL claim classes"),
                    label="Structured-null FWER")
G2 = SemanticMetric(0.685, INFERENTIAL, "family_wise_false_promotion_rate",
                    structured_permutation_null("date_level_label_circular_v1"), COND, P_N0,
                    uncertainty=Known("200 null worlds · nominal family level 0.05"),
                    population=Known("6 DAY_LEVEL claim classes"),
                    label="Structured-null FWER")
N1 = SemanticMetric(0.0, DETERMINISTIC, "incremental_effect_in_composition_only_world",
                    synthetic_dgp("incremental_composition_generator_v1"), COND, P_V2,
                    units="pp",
                    uncertainty=NotApplicable("algebraically determined by the construction: "
                                              "within a stratum every row shares one outcome"),
                    population=Known("31 classes, deterministic world"),
                    label="θ in a composition-only world")
N2 = SemanticMetric(0.0038, INFERENTIAL, "incremental_effect_estimate",
                    synthetic_dgp("incremental_composition_generator_v1"), COND, P_V2,
                    units="pp",
                    uncertainty=Known("mean over 40 worlds · RMSE 0.124pp"),
                    population=Known("31 classes"), label="θ̂ under stochastic composition")
BAD = SemanticMetric(1.2, INFERENTIAL, "incremental_return_pp",
                     synthetic_dgp("incremental_composition_generator_v1"), COND,
                     Provenance("HYPOTHETICAL", "spec@x", "code@y", "bars@z"),
                     integrity_status=INVALID, conclusion_status=BUILD, units="pp",
                     uncertainty=Known("95% clustered bootstrap"),
                     population=Unknown("manifest hash did not match at run time"),
                     label="incremental return")


def t1_g1():
    """G1 renders as an inferential rate with its null model named"""
    d = INS.build(G1)
    t = INS.render(d)
    assert d.badge == "INFERENTIAL · VALID" and not d.banner
    assert "Y ⊥ Cell | Date, BaseSetup" in t, "the null model is missing"
    assert "31 OPPORTUNITY_LEVEL" in t


def t2_g2_limitation_surfaces():
    """G2 must carry what its generator does NOT preserve"""
    t = INS.render(INS.build(G2))
    assert "Y ⊥ DayProperty" in t
    assert "does not preserve" in t and "calendar-year prevalence" in t, \
        "the generator's limitation did not reach the drawer"
    assert "6 DAY_LEVEL" in t


def t3_deterministic_has_no_interval():
    """N1 shows NOT APPLICABLE with a reason, never an empty dash"""
    t = INS.render(INS.build(N1))
    assert "DETERMINISTIC" in t
    assert "NOT APPLICABLE" in t and "algebraically determined" in t
    assert "UNKNOWN" not in t, "a determined quantity was reported as unknown"


def t4_unknown_and_na_differ():
    """the two kinds of absence must not render alike"""
    a = INS.render(INS.build(N1))          # NotApplicable
    b = INS.render(INS.build(BAD))         # Unknown population
    assert "NOT APPLICABLE" in a and "UNKNOWN" not in a
    assert "UNKNOWN" in b and "manifest hash did not match" in b


def t5_invalid_breaks_the_layout():
    """the conclusion is visible, is not a result, and provenance survives for investigation"""
    d = INS.build(BAD)
    t = INS.render(d)
    assert d.headline == BUILD, "the recorded conclusion was hidden"
    assert "NOT INTERPRETABLE" in d.banner
    assert "recorded conclusion, not a result" in d.subhead
    assert "HYPOTHETICAL" in t, "provenance must remain reachable for investigation"


def t6_comparison_block_is_carried_not_computed():
    """the drawer displays the guard's verdict and does not reach one of its own"""
    r = can_compare(G1, G2)
    d = INS.build(G1, comparison=r, against="G2")
    t = INS.render(d)
    assert "BLOCKED" in t and "SAMPLING_TARGET_MISMATCH" in t
    sec = [s for s in d.sections if s.title == "COMPARISON"][0]
    assert sec.emphasis == "block"
    d2 = INS.build(G1, comparison=can_compare(G1, G1), against="itself")
    assert "ALLOWED" in INS.render(d2)


def t7_types_do_not_share_a_layout():
    """a determined zero and an estimated one must not produce the same sections"""
    s1 = {s.title for s in INS.build(N1).sections}
    s2 = {s.title for s in INS.build(N2).sections}
    assert "BASIS" in s1 and "BASIS" not in s2
    assert "MEANING" in s2 and "MEANING" not in s1


def t8_tolerance_is_visible_as_data():
    """a WITHIN condition shows its width and its hash, not a phrase

    Asserted against the drawer STRUCTURE, not the rendered text. The first version checked a
    substring of the rendered string and failed because word-wrapping split the phrase across
    two lines — a brittle test of presentation standing in for a test of content. The note is
    data; the wrap is layout.
    """
    m = SemanticMetric(1.0, INFERENTIAL, "e", synthetic_dgp("g"),
                       (ConditioningSpec("RSI_14", "WITHIN", 45, 5, " pts"),), P_V2,
                       uncertainty=Known("CI"))
    d = INS.build(m)
    sec = [x for x in d.sections if x.title == "CONDITIONING"][0]
    rows = {r[0]: r for r in sec.rows}
    assert rows["RSI_14"][1] == "45 ±5 pts", rows["RSI_14"]
    assert rows["RSI_14"][2].startswith("hash "), "the condition hash is not shown"
    assert "different conditioning object" in rows["condition set"][2]
    assert "45 ±5 pts" in INS.render(d)


print("=" * 104, flush=True)
print("  SEMANTICS INSPECTOR — five fixtures, none may read alike", flush=True)
print("=" * 104, flush=True)
for fn in (t1_g1, t2_g2_limitation_surfaces, t3_deterministic_has_no_interval,
           t4_unknown_and_na_differ, t5_invalid_breaks_the_layout,
           t6_comparison_block_is_carried_not_computed, t7_types_do_not_share_a_layout,
           t8_tolerance_is_visible_as_data):
    check((fn.__doc__ or fn.__name__).splitlines()[0], fn)
print("=" * 104, flush=True)
print(f"  {ok} passed · {fail} failed\n", flush=True)
if not fail:
    for nm, m, cmp_, ag in (("G1", G1, can_compare(G1, G2), "G2"), ("G2", G2, None, ""),
                            ("N1", N1, None, ""), ("INVALID+BUILD", BAD, None, "")):
        print("─" * 78, flush=True)
        print(f"FIXTURE {nm}", flush=True)
        print("─" * 78, flush=True)
        print(INS.render(INS.build(m, comparison=cmp_, against=ag)), flush=True)
        print(flush=True)
sys.exit(1 if fail else 0)
