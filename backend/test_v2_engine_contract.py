"""Structural tests, written before the extraction so the refactor cannot drift the ontology.

None of these compute anything statistical. They check that the objects cannot express the
wrong thing — that a mode cannot see the other mode's outcomes, that a spec hash moves for
statistical changes and not for execution ones, that a rerun cannot become a source of fresh
noise, and that arm computability is a fact about geometry.

`t10` is the one that would otherwise reappear as a feature request. Same question, same data,
new session, new run id — and the interval must be identical. If any of those entered the RNG,
`revisit` would stop being free in the only way that matters: a researcher could re-open a
specification until the Monte-Carlo noise fell somewhere pleasant.
"""
from __future__ import annotations

import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import v2_engine_contract as EC                                      # noqa: E402

ok = fail = 0


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                           # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def spec(**over) -> EC.V2RunSpec:
    base = dict(estimand_version="stratified_within_setup_median_difference_pp",
                search_space_manifest_hash="3600ae3dd52a25e6",
                support_policy_hash="6f825ca4763fea76", null_family="OPPORTUNITY_LEVEL",
                decision_policy_hash="verdict_v2", bootstrap_policy_hash="boot_v1",
                outcome_definition="ret*100 within frozen strata")
    base.update(over)
    return EC.V2RunSpec(**base)


def outcome(kind=EC.HISTORICAL_OBSERVED, n=6) -> EC.OutcomeVector:
    return EC.OutcomeVector(values=np.arange(n, dtype=float), outcome_id=f"oc-{kind}",
                            outcome_semantics="what these numbers are",
                            source_kind=kind, source_snapshot_id="snap-1", units="pp",
                            row_alignment_hash="align-A", construction_hash="constr-1")


def sealed_material():
    return EC.RNGMaterial(namespace="sealed_v2", provenance="freeze_commit:abc123",
                          frozen_seeds=(11, 22, 33))


# ── identity of the specification ───────────────────────────────────────────
def t1_the_same_question_hashes_the_same():
    assert spec().spec_hash == spec().spec_hash


def t2_execution_details_do_not_touch_the_spec_hash():
    """RNG, snapshot, session and mode are not part of WHAT is computed"""
    s = spec()
    for mode, mat in ((EC.SEALED_ACCEPTANCE, sealed_material()),
                      (EC.HISTORICAL_RESEARCH, EC.research_rng_material(s, "snap-1"))):
        policy = EC.SEALED_RNG if mode == EC.SEALED_ACCEPTANCE else EC.RESEARCH_RNG
        EC.ExecutionContext(mode, "snap-1", "code-1", policy, mat, f"x-{mode}")
    assert spec().spec_hash == s.spec_hash, "building a context changed the specification"


def t3_a_statistical_change_moves_the_spec_hash():
    base = spec().spec_hash
    for field, value in (("support_policy_hash", "different"),
                         ("decision_policy_hash", "verdict_v3"),
                         ("null_family", "DAY_LEVEL"),
                         ("bootstrap_policy_hash", "boot_v2"),
                         ("estimand_version", "something_else")):
        assert spec(**{field: value}).spec_hash != base, field


# ── the two modes cannot see each other's data ──────────────────────────────
def t4_a_sealed_run_may_not_compute_on_real_returns():
    try:
        EC.assert_outcome_allowed(EC.SEALED_ACCEPTANCE, outcome(EC.HISTORICAL_OBSERVED))
    except EC.OutcomeSemanticsError as e:
        assert "spends the seal" in str(e)
        return
    raise AssertionError("the sealed acceptance was pointed at real returns")


def t5_a_historical_run_may_not_compute_on_a_composition_world():
    for kind in EC.SYNTHETIC_KINDS:
        try:
            EC.assert_outcome_allowed(EC.HISTORICAL_RESEARCH, outcome(kind))
        except EC.OutcomeSemanticsError as e:
            assert "describing nothing but noise" in str(e)
            continue
        raise AssertionError(f"a HISTORICAL_RESEARCH run accepted a {kind} outcome")
    EC.assert_outcome_allowed(EC.HISTORICAL_RESEARCH, outcome(EC.HISTORICAL_OBSERVED))


def t5b_REPRODUCTION_a_bare_ndarray_carries_no_such_refusal():
    """the guard shown its defect: values with the meaning left behind"""
    synthetic = outcome(EC.SYNTHETIC_COMPOSITION_WORLD).values
    historical = outcome(EC.HISTORICAL_OBSERVED).values
    assert np.array_equal(synthetic, historical), (
        "the reproduction failed: the two arrays were supposed to be indistinguishable, which "
        "is exactly why the meaning has to travel with them")
    assert (outcome(EC.SYNTHETIC_COMPOSITION_WORLD).outcome_hash
            != outcome(EC.HISTORICAL_OBSERVED).outcome_hash)


def t6_same_length_is_not_the_same_rows():
    o = outcome()
    EC.assert_row_alignment(o, "align-A", 6)
    try:
        EC.assert_row_alignment(o, "align-B", 6)
    except EC.OutcomeSemanticsError as e:
        assert "index different rows" in str(e)
        return
    raise AssertionError("a misaligned outcome was accepted because the length matched")


# ── search-space identity ───────────────────────────────────────────────────
def t7_search_space_identity_is_not_a_count():
    kw = dict(engine_version="v2", execution_mode=EC.HISTORICAL_RESEARCH, spec_hash="s",
              input_outcome_hash="o", data_snapshot_id="snap-1",
              registered_search_space_hash="AAA", executed_search_space_hash="BBB",
              executed_cell_order_hash="ORDER",
              estimand_version="e", support_policy_hash="sp", null_family="OPPORTUNITY_LEVEL",
              decision_policy_hash="dp", bootstrap_policy_hash="bp", rng_policy_id="r",
              rng_provenance_hash="rp")
    try:
        EC.EngineResultArtifact(**kw)
    except EC.SpecIdentityError as e:
        assert "31 == 31 says nothing about which 31" in str(e)
        EC.EngineResultArtifact(**{**kw, "executed_search_space_hash": "AAA"})
        return
    raise AssertionError("an artifact was built on a space it did not execute")


# ── RNG lineage, not integer equality ───────────────────────────────────────
def t8_sealed_material_is_refused_outside_the_sealed_run():
    try:
        EC.ExecutionContext(EC.HISTORICAL_RESEARCH, "snap-1", "code-1", EC.RESEARCH_RNG,
                            sealed_material(), "x1")
    except EC.SealedRNGReuseError as e:
        assert "already reproducible" in str(e)
    else:
        raise AssertionError("sealed randomness reached an interactive run")

    # and the reverse: an acceptance whose seeds were not sealed is not an acceptance
    try:
        EC.ExecutionContext(EC.SEALED_ACCEPTANCE, "snap-1", "code-1", EC.SEALED_RNG,
                            EC.research_rng_material(spec(), "snap-1"), "x2")
    except EC.SealedRNGReuseError as e:
        assert "could not have been chosen after looking" in str(e)
        return
    raise AssertionError("a sealed acceptance ran on freely chosen randomness")


def t8b_lineage_is_read_rather_than_the_numbers():
    """a coincidence is not a breach; a transformed copy of sealed material is"""
    coincidence = EC.RNGMaterial(namespace="combolab_v2_research",
                                 provenance="derived:spec+snapshot", frozen_seeds=(11, 22, 33))
    assert not coincidence.is_sealed_lineage, (
        "identical integers were treated as a breach; the check must read lineage")
    laundered = EC.RNGMaterial(namespace="combolab_v2_research",
                               provenance="freeze_commit:abc123 transformed +1",
                               frozen_seeds=(12, 23, 34))
    assert laundered.is_sealed_lineage, (
        "sealed material survived an arithmetic transform and stopped being recognised")


def t9_historical_material_is_determined_by_the_question_and_the_data():
    s = spec()
    a = EC.research_rng_material(s, "snap-1")
    b = EC.research_rng_material(s, "snap-1")
    assert a.material_hash == b.material_hash
    assert EC.research_rng_material(s, "snap-2").material_hash != a.material_hash
    assert EC.research_rng_material(spec(null_family="DAY_LEVEL"),
                                    "snap-1").material_hash != a.material_hash


def t10_a_rerun_in_a_new_session_is_not_a_source_of_fresh_noise():
    """otherwise `revisit` becomes a way to shop for Monte-Carlo noise, and it is free"""
    s = spec()
    first = EC.ExecutionContext(EC.HISTORICAL_RESEARCH, "snap-1", "code-1", EC.RESEARCH_RNG,
                                EC.research_rng_material(s, "snap-1"), "run-001")
    later = EC.ExecutionContext(EC.HISTORICAL_RESEARCH, "snap-1", "code-1", EC.RESEARCH_RNG,
                                EC.research_rng_material(s, "snap-1"), "run-999-other-session")
    assert first.rng_provenance_hash == later.rng_provenance_hash, (
        "the run id perturbed the randomness; the same question on the same data would return a "
        "different interval every time somebody reopened it")


# ── arm computability is geometry ───────────────────────────────────────────
def t11_arm_computability_cannot_depend_on_the_outcome():
    """the signature is the enforcement: there is no `y` to pass"""
    import inspect
    params = list(inspect.signature(EC.arm_computability_mask).parameters)
    assert "y" not in params and "outcome" not in params, params

    dates = np.array([0, 0, 1, 1, 2, 2])
    treated = np.array([1, 1, 1, 1, 0, 0], dtype=bool)
    comparator = ~treated
    assert EC.arm_computability_mask(treated, comparator, dates, np.array([0, 1])) is False
    assert EC.arm_computability_mask(treated, comparator, dates, np.array([0, 2])) is True


def t11b_the_diagnostic_invariant_over_two_different_outcomes():
    """if this ever fails, the finding is outcome-dependent eligibility, not awkward returns"""
    rng = np.random.default_rng(3)
    dates = np.repeat(np.arange(8), 4)
    treated = rng.random(dates.size) < 0.5
    comparator = ~treated
    y_synth = rng.normal(size=dates.size)
    y_real = rng.normal(size=dates.size) * 17 + 3       # a completely different outcome vector

    for _ in range(50):
        drawn = rng.choice(np.unique(dates), size=8, replace=True)
        a = EC.arm_computability_mask(treated, comparator, dates, drawn)
        b = EC.arm_computability_mask(treated, comparator, dates, drawn)
        assert a == b
        # the outcomes exist and are different; the mask does not and cannot see them
        assert not np.array_equal(y_synth, y_real)


print("=" * 104, flush=True)
print("  V2 ENGINE CONTRACT — written before run_v2() exists", flush=True)
print("=" * 104, flush=True)
for i, fn in enumerate([t1_the_same_question_hashes_the_same,
                        t2_execution_details_do_not_touch_the_spec_hash,
                        t3_a_statistical_change_moves_the_spec_hash,
                        t4_a_sealed_run_may_not_compute_on_real_returns,
                        t5_a_historical_run_may_not_compute_on_a_composition_world,
                        t5b_REPRODUCTION_a_bare_ndarray_carries_no_such_refusal,
                        t6_same_length_is_not_the_same_rows,
                        t7_search_space_identity_is_not_a_count,
                        t8_sealed_material_is_refused_outside_the_sealed_run,
                        t8b_lineage_is_read_rather_than_the_numbers,
                        t9_historical_material_is_determined_by_the_question_and_the_data,
                        t10_a_rerun_in_a_new_session_is_not_a_source_of_fresh_noise,
                        t11_arm_computability_cannot_depend_on_the_outcome,
                        t11b_the_diagnostic_invariant_over_two_different_outcomes], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 104, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
