"""The first acceptance test of Analytic Studio, on the hardest real pair we have.

N0 produced two numbers that a screen would happily average:

    G1  within_stratum_outcome_v1          FWER 0.065   31 claims
    G2  date_level_label_circular_v1       FWER 0.685    6 claims

They are not one metric with two values. Different null models, different denominators,
different hypotheses. A layout that shows `average FWER 0.375` or `G2 is 10.5× worse` destroys
in one line what the null-family work established.

Five conditions. If any fails, the schema is not ready and Combo Lab is premature.

    1  neither number can exist without its semantic core
    2  each opens a full inspector
    3  compare(G1, G2) is refused, with a reason code
    4  co-display is still allowed, carrying an explicit boundary
    5  no arithmetic path produces a combined figure
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sampling_target import structured_permutation_null, synthetic_dgp   # noqa: E402
from semantic_metric import (BUILD, DETERMINISTIC, INFERENTIAL, INVALID,  # noqa: E402
                             NULL_, VALID, ComparisonSemanticsError, ConditioningSpec,
                             Known, NotApplicable, Provenance, SemanticContractError,
                             SemanticMetric, Unknown, assert_comparable, can_compare,
                             co_display, difference, ratio)

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
PROV = Provenance("N0-2026-08-11", "3c6ccda05dde7eb0", "n0_run@2591906", "bars@2026-08-10")

G1 = SemanticMetric(
    value=0.065, semantic_type=INFERENTIAL, estimand="family_wise_false_promotion_rate",
    sampling_target=structured_permutation_null("within_stratum_outcome_v1"),
    conditioning=COND, provenance=PROV, integrity_status=VALID,
    uncertainty=Known("200 worlds, binomial se 1.7pp"),
    population=Known("31 OPPORTUNITY_LEVEL claims"), label="G1 FWER_search")

G2 = SemanticMetric(
    value=0.685, semantic_type=INFERENTIAL, estimand="family_wise_false_promotion_rate",
    sampling_target=structured_permutation_null("date_level_label_circular_v1"),
    conditioning=COND, provenance=PROV, integrity_status=VALID,
    uncertainty=Known("200 worlds, binomial se 3.3pp"),
    population=Known("6 DAY_LEVEL claims"), label="G2 FWER_band")


# 1 ─ the core cannot be skipped
def t1_core_is_mandatory():
    for kw, why in (
            (dict(semantic_type="SOMETHING"), "semantic_type"),
            (dict(estimand=""), "estimand"),
            (dict(sampling_target="within_stratum_outcome_v1"), "sampling_target as a string"),
            (dict(provenance=None), "provenance"),
            (dict(integrity_status="MAYBE"), "integrity_status"),
    ):
        base = dict(value=0.065, semantic_type=INFERENTIAL, estimand="x",
                    sampling_target=structured_permutation_null("g"), conditioning=COND,
                    provenance=PROV, uncertainty=Known("x"))
        base.update(kw)
        try:
            SemanticMetric(**base)
        except SemanticContractError:
            continue
        raise AssertionError(f"a metric was built without {why}")


def t1b_type_discipline():
    """a deterministic quantity may not carry an interval; an estimate may not omit one"""
    try:
        SemanticMetric(value=0.0, semantic_type=DETERMINISTIC, estimand="theta_exact",
                       sampling_target=synthetic_dgp("gen"), conditioning=COND, provenance=PROV,
                       uncertainty=Known("95% CI"))
    except SemanticContractError:
        pass
    else:
        raise AssertionError("a DETERMINISTIC value accepted an interval")
    try:
        SemanticMetric(value=0.0038, semantic_type=INFERENTIAL, estimand="theta_hat",
                       sampling_target=synthetic_dgp("gen"), conditioning=COND, provenance=PROV)
    except SemanticContractError:
        return
    raise AssertionError("an INFERENTIAL value was accepted with no uncertainty at all")


def t1c_absence_is_a_state():
    """None is refused; Unknown and NotApplicable carry their reason"""
    base = dict(value=1.0, semantic_type=INFERENTIAL, estimand="x",
                sampling_target=synthetic_dgp("g"), conditioning=COND, provenance=PROV)
    try:
        SemanticMetric(**base, uncertainty=None)
    except SemanticContractError:
        pass
    else:
        raise AssertionError("None was accepted where a state was required")
    m = SemanticMetric(**base, uncertainty=Unknown("bootstrap not yet run"),
                       population=NotApplicable("deterministic count"))
    assert "unknown — bootstrap not yet run" in m.inspect()["uncertainty"]
    assert "n/a — deterministic count" in m.inspect()["population"]


# 2 ─ the inspector
def t2_inspector_is_complete():
    d = G1.inspect()
    for k in ("value", "semantic_type", "estimand", "sampling_target", "conditioning",
              "uncertainty", "population", "integrity", "conclusion", "provenance"):
        assert k in d and d[k] not in ("", None), f"inspector missing {k}"
    assert "within_stratum_outcome_v1" in d["sampling_target"]
    assert "date_level_label_circular_v1" in G2.inspect()["sampling_target"]
    assert d["provenance"]["spec"] and d["provenance"]["code"]


# 3 ─ the refusal, with a reason
def t3_comparison_refused():
    r = can_compare(G1, G2)
    assert not r.comparable
    assert r.reason_code == "SAMPLING_TARGET_MISMATCH", r.reason_code
    assert "within_stratum_outcome_v1" in r.left and "date_level_label_circular_v1" in r.right
    try:
        assert_comparable(G1, G2)
    except ComparisonSemanticsError as e:
        assert "SAMPLING_TARGET_MISMATCH" in str(e)
        return
    raise AssertionError("assert_comparable let the pair through")


# 4 ─ co-display allowed, boundary attached
def t4_co_display_allowed_with_boundary():
    c = co_display(G1, G2)
    assert c["comparable"] is False
    assert c["boundary"] and "do not combine" in c["boundary"]
    assert c["left"]["value"] == 0.065 and c["right"]["value"] == 0.685


# 5 ─ no arithmetic path exists
def t5_no_arithmetic_path():
    for fn, nm in ((difference, "difference"), (ratio, "ratio")):
        try:
            fn(G1, G2)
        except ComparisonSemanticsError:
            continue
        raise AssertionError(f"{nm}() produced a combined figure from two null models")


# 6 ─ integrity outranks capability, in the type
def t6_integrity_outranks_capability():
    m = SemanticMetric(value=1.2, semantic_type=INFERENTIAL, estimand="incremental_return_pp",
                       sampling_target=synthetic_dgp("gen"), conditioning=COND, provenance=PROV,
                       integrity_status=INVALID, conclusion_status=BUILD,
                       uncertainty=Known("CI"), units="pp")
    assert "NOT INTERPRETABLE" in m.renderable_conclusion, m.renderable_conclusion
    assert m.renderable_conclusion != BUILD
    r = can_compare(m, G1)
    assert not r.comparable and r.reason_code == "INTEGRITY_INVALID"


# 7 ─ tolerance is a research decision, not a caption
def t7_tolerance_is_data():
    try:
        ConditioningSpec("RSI_14", "WITHIN", 45)
    except SemanticContractError:
        pass
    else:
        raise AssertionError("WITHIN accepted with no tolerance")
    a = ConditioningSpec("RSI_14", "WITHIN", 45, 1, " pts")
    b = ConditioningSpec("RSI_14", "WITHIN", 45, 5, " pts")
    assert a.hash != b.hash, "±1 and ±5 hashed the same"
    mk = lambda cs: SemanticMetric(value=1.0, semantic_type=INFERENTIAL, estimand="e",
                                   sampling_target=synthetic_dgp("g"), conditioning=(cs,),
                                   provenance=PROV, uncertainty=Known("CI"))
    r = can_compare(mk(a), mk(b))
    assert not r.comparable and r.reason_code == "CONDITIONING_MISMATCH"


# 8 ─ the legitimate case still works
def t8_same_experiment_compares():
    g1b = SemanticMetric(value=0.065, semantic_type=INFERENTIAL,
                         estimand="family_wise_false_promotion_rate",
                         sampling_target=structured_permutation_null("within_stratum_outcome_v1"),
                         conditioning=COND, provenance=PROV, uncertainty=Known("se 1.7pp"),
                         population=Known("31 claims"))
    assert can_compare(G1, g1b).comparable
    assert abs(difference(G1, g1b)) < 1e-12


print("=" * 104, flush=True)
print("  SEMANTIC CORE — first acceptance, fixtures are the real N0/G1 and N0/G2", flush=True)
print("=" * 104, flush=True)
for fn in (t1_core_is_mandatory, t1b_type_discipline, t1c_absence_is_a_state,
           t2_inspector_is_complete, t3_comparison_refused,
           t4_co_display_allowed_with_boundary, t5_no_arithmetic_path,
           t6_integrity_outranks_capability, t7_tolerance_is_data, t8_same_experiment_compares):
    check((fn.__doc__ or fn.__name__).splitlines()[0], fn)
print("=" * 104, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
if not fail:
    print("\n  Inspector for G1, as the drawer will render it:", flush=True)
    for k, v in G1.inspect().items():
        print(f"    {k:<20s} {v}", flush=True)
    print("\n  And the boundary the pair carries:", flush=True)
    print(f"    {co_display(G1, G2)['boundary']}", flush=True)
sys.exit(1 if fail else 0)
