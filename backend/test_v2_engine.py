"""`run_v2()` is orchestration, and these tests are mostly about what it refuses.

Two of them are the ones the user asked for by name, and both defend against the same habit —
checking a count and concluding identity:

    t7  thirty-one results for thirty-one cells, misfiled       CellResultIdentityMismatchError
    t8  thirty of thirty-one completed                          PartialExecutionError

The third is structural rather than behavioural: no display parameter may appear in the engine's
signature. The first `displayed_top_k` accepted here would put presentation policy inside the
estimator, and it would arrive looking like a convenience.
"""
from __future__ import annotations

import inspect
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import v2_engine as EN                                              # noqa: E402
import v2_engine_contract as C                                      # noqa: E402

ok = fail = 0
ALIGN, SNAP, SPACE = "align-A", "snap-1", "space-AAA"


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                          # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def spec() -> C.V2RunSpec:
    return C.V2RunSpec(estimand_version="stratified_within_setup_median_difference_pp",
                       search_space_manifest_hash=SPACE, support_policy_hash="sp",
                       null_family="OPPORTUNITY_LEVEL", decision_policy_hash="verdict_v2",
                       bootstrap_policy_hash="boot_v1", outcome_definition="composition world")


def sealed_context() -> C.ExecutionContext:
    return C.ExecutionContext(
        C.SEALED_ACCEPTANCE, SNAP, "code-1", C.SEALED_RNG,
        C.RNGMaterial(namespace="sealed_v2", provenance="freeze_commit:abc", frozen_seeds=(1,)),
        "exec-1")


def historical_context() -> C.ExecutionContext:
    s = spec()
    return C.ExecutionContext(C.HISTORICAL_RESEARCH, SNAP, "code-1", C.RESEARCH_RNG,
                              C.research_rng_material(s, SNAP), "exec-2")


def outcome(kind=C.SYNTHETIC_COMPOSITION_WORLD, n=8, snap=SNAP, align=ALIGN) -> C.OutcomeVector:
    return C.OutcomeVector(values=np.arange(n, dtype=float), outcome_id="oc-1",
                           outcome_semantics="synthetic composition world", source_kind=kind,
                           source_snapshot_id=snap, units="pp", row_alignment_hash=align,
                           construction_hash="constr-1")


# ── the gate that keeps a mode from being a licence ─────────────────────────
def t1_the_historical_mode_exists_and_is_refused():
    """an enum member is not permission to run"""
    try:
        EN.assert_compatible(spec(), historical_context(), outcome(C.HISTORICAL_OBSERVED),
                             registered_space_hash=SPACE, expected_alignment=ALIGN, n_rows=8)
    except EN.HistoricalApplicationNotQualifiedError as e:
        assert "does not open the historical client" in str(e)
        return
    raise AssertionError("the historical mode ran because its type existed")


def t2_sealed_mode_refuses_real_returns():
    try:
        EN.assert_compatible(spec(), sealed_context(), outcome(C.HISTORICAL_OBSERVED),
                             registered_space_hash=SPACE, expected_alignment=ALIGN, n_rows=8)
    except C.OutcomeSemanticsError as e:
        assert "spends the seal" in str(e)
        return
    raise AssertionError("a sealed run accepted real returns")


def t3_the_snapshot_must_agree_with_the_outcome():
    try:
        EN.assert_compatible(spec(), sealed_context(), outcome(snap="snap-OTHER"),
                             registered_space_hash=SPACE, expected_alignment=ALIGN, n_rows=8)
    except C.OutcomeSemanticsError as e:
        assert "describing a different dataset" in str(e)
        return
    raise AssertionError("an outcome from another snapshot was accepted")


def t4_alignment_is_checked_before_anything_is_computed():
    try:
        EN.assert_compatible(spec(), sealed_context(), outcome(align="align-B"),
                             registered_space_hash=SPACE, expected_alignment=ALIGN, n_rows=8)
    except C.OutcomeSemanticsError as e:
        assert "index different rows" in str(e)
        return
    raise AssertionError("a misaligned outcome reached the engine")


def t5_the_registered_space_is_compared_by_hash():
    try:
        EN.assert_compatible(spec(), sealed_context(), outcome(),
                             registered_space_hash="space-DIFFERENT", expected_alignment=ALIGN,
                             n_rows=8)
    except C.SpecIdentityError as e:
        assert "Equal counts would not have caught this" in str(e)
        return
    raise AssertionError("the engine ran against a space it did not declare")


def t6_a_compatible_sealed_run_passes_the_gate():
    """the guard must not be a wall: the legitimate case still goes through"""
    EN.assert_compatible(spec(), sealed_context(), outcome(),
                         registered_space_hash=SPACE, expected_alignment=ALIGN, n_rows=8)


# ── count is not identity ───────────────────────────────────────────────────
def _result(cell) -> C.CellResult:
    return C.CellResult(cell_identity=cell, evidence_claim_hash="e", decision_spec_hash="d",
                        computation=C.CellComputation(theta_hex="0x0p+0", interval_hex=("a", "b"),
                                                      bootstrap_summary={}, support_identity="s",
                                                      eligibility={}),
                        decision=C.CellDecision(stages=("NULL",), final_verdict="NULL",
                                                decision_policy_hash="verdict_v2"))


def t7_thirty_one_results_can_still_be_the_wrong_thirty_one():
    """the test the user asked for by name: same count, swapped identities"""
    order = [f"c{i}" for i in range(31)]
    good = [_result(c) for c in order]
    EN.assert_results_match_cells(order, good)

    swapped = list(good)
    swapped[3], swapped[17] = swapped[17], swapped[3]
    assert len(swapped) == len(order) == 31
    try:
        EN.assert_results_match_cells(order, swapped)
    except EN.CellResultIdentityMismatchError as e:
        assert "counts matched" in str(e)
        return
    raise AssertionError("results filed against the wrong cells passed because the count did")


def t8_a_partial_execution_is_a_failed_one():
    """thirty of thirty-one is a different search, and the missing one is not random"""
    order = [f"c{i}" for i in range(31)]
    results = [_result(c) for c in order[:30]]
    try:
        EN.assert_complete(order, results)
    except EN.PartialExecutionError as e:
        assert "nobody registered" in str(e)
        return
    raise AssertionError("a partial execution produced an artifact")


# ── order is recorded, not chosen ───────────────────────────────────────────
def t9_the_same_set_in_a_different_order_is_a_different_execution():
    a = ["x", "y", "z"]
    assert EN.cell_order_hash(a) != EN.cell_order_hash(list(reversed(a)))
    assert EN.cell_order_hash(a) == EN.cell_order_hash(list(a))


# ── no presentation policy inside the estimator ─────────────────────────────
def t10_the_engine_signature_carries_no_display_parameter():
    """the first `displayed_top_k` accepted here would arrive looking like a convenience"""
    params = set(inspect.signature(EN.run_v2).parameters)
    for forbidden in ("displayed_top_k", "top_k", "rows_inspectable", "presentation_sort",
                      "sort_key", "displayed", "surface", "rank", "ranking_policy"):
        assert forbidden not in params, forbidden

    # Scanned as IDENTIFIERS, not as text. The first version searched the raw source and flagged
    # the module docstring — the paragraph that exists to forbid these names contains them, so
    # the guard reported a violation by reading its own prohibition. Same trap as testing a
    # rendered string instead of a structure, arriving in a new shape.
    import ast                                                       # noqa: PLC0415
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "v2_engine.py")).read()
    tree = ast.parse(src)
    identifiers = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    identifiers |= {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    identifiers |= {a.arg for n in ast.walk(tree)
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                    for a in list(n.args.args) + list(n.args.kwonlyargs)}
    for forbidden in ("displayed_top_k", "SearchRunView", "SemanticMetricView",
                      "rank_and_authorise", "rows_inspectable", "presentation_sort"):
        assert forbidden not in identifiers, (
            f"{forbidden} is used as an identifier in the engine, not merely named in prose")


def t11_the_deferred_six_are_declared_rather_than_filtered():
    d = EN.default_deferred()
    assert d["DAY_LEVEL"]["count"] == 6
    assert d["DAY_LEVEL"]["status"] == "NOT_IN_SEARCH_SPACE"
    assert "v2.1" in d["DAY_LEVEL"]["reason"]


def t12_the_artifact_separates_computation_from_decision():
    r = _result("c0")
    assert hasattr(r.computation, "theta_hex") and hasattr(r.decision, "final_verdict")
    assert not hasattr(r.computation, "final_verdict"), "a verdict leaked into the measurement"
    assert not hasattr(r.decision, "theta_hex"), "an estimate leaked into the decision"


print("=" * 100, flush=True)
print("  run_v2 — orchestration, mostly by refusing", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_the_historical_mode_exists_and_is_refused,
                        t2_sealed_mode_refuses_real_returns,
                        t3_the_snapshot_must_agree_with_the_outcome,
                        t4_alignment_is_checked_before_anything_is_computed,
                        t5_the_registered_space_is_compared_by_hash,
                        t6_a_compatible_sealed_run_passes_the_gate,
                        t7_thirty_one_results_can_still_be_the_wrong_thirty_one,
                        t8_a_partial_execution_is_a_failed_one,
                        t9_the_same_set_in_a_different_order_is_a_different_execution,
                        t10_the_engine_signature_carries_no_display_parameter,
                        t11_the_deferred_six_are_declared_rather_than_filtered,
                        t12_the_artifact_separates_computation_from_decision], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
