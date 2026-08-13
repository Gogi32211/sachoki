"""4B · the wrapper became the only place capability coordinates can be confused.

Before the inversion, `world`, `delta` and `rep` were closure reads inside `boot`, so a mismatch
was impossible by construction — there was nothing to mismatch them WITH. Now the kernel never
sees them and the wrapper assembles them, which means the wrapper is exactly where an adversarial
fixture belongs. This is a general shape worth naming: removing a coupling does not remove the
error, it moves it to whoever now does the assembling.

The dangerous case is the quiet one. A unit run with the right world, the right δ and the WRONG
rep produces a perfectly well-formed artifact: the spec hash matches, the search-space hash
matches, the snapshot matches, every gate in `run_v2` passes. Only the numbers are from a
different replication, and nothing downstream can tell.

So the outcome carries the coordinates it was built for, and the wrapper checks them. That check
did not exist before this file — writing the adversarial test is what revealed there was nothing
to catch it.
"""
from __future__ import annotations

import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import v2_engine_contract as C                                      # noqa: E402
import v2_sealed_run as SR                                          # noqa: E402

ok = fail = 0
SNAP, ALIGN = "snap-1", "align-A"


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                          # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def y(n=8):
    return np.arange(n, dtype=float)


def t1_the_outcome_is_stamped_with_the_coordinates_it_was_built_for():
    o = SR.sealed_outcome(y(), world=4242, delta=0.0, snapshot_id=SNAP, alignment=ALIGN)
    SR.assert_outcome_matches_coordinates(o, 4242, 0.0)
    assert o.source_kind == C.SYNTHETIC_COMPOSITION_WORLD
    inj = SR.sealed_outcome(y(), world=4242, delta=1.5, snapshot_id=SNAP, alignment=ALIGN)
    assert inj.source_kind == C.SYNTHETIC_INJECTED_WORLD
    assert inj.construction_hash != o.construction_hash


def t2_an_outcome_from_an_adjacent_delta_is_refused():
    """the mismatch nothing else in the system can see"""
    o = SR.sealed_outcome(y(), world=4242, delta=1.5, snapshot_id=SNAP, alignment=ALIGN)
    try:
        SR.assert_outcome_matches_coordinates(o, 4242, 0.60)
    except SR.SealedCoordinateMismatchError as e:
        assert "nothing else in the system is able to see" in str(e)
        return
    raise AssertionError("an outcome built at one δ ran under another")


def t3_an_outcome_from_an_adjacent_world_is_refused():
    o = SR.sealed_outcome(y(), world=4242, delta=0.0, snapshot_id=SNAP, alignment=ALIGN)
    try:
        SR.assert_outcome_matches_coordinates(o, 9999, 0.0)
    except SR.SealedCoordinateMismatchError:
        return
    raise AssertionError("an outcome built in one world ran in another")


def t4_the_wrong_rep_produces_a_different_rng_stream():
    """right world, right δ, wrong replication — every hash still matches

    This is the case the coordinate check cannot reach, because `rep` does not enter the
    outcome: two replications share one world and one δ. What separates them is the RNG stream,
    so that is where it has to be visible.
    """
    right = SR.SealedBootstrapRNGProvider(4242, 0.0, 0)
    wrong = SR.SealedBootstrapRNGProvider(4242, 0.0, 1)
    assert right.semantic_key("cellA") != wrong.semantic_key("cellA")
    a = right.open_stream("cellA").multinomial(10, np.full(10, 0.1))
    b = wrong.open_stream("cellA").multinomial(10, np.full(10, 0.1))
    assert not np.array_equal(a, b), (
        "two replications produced the same first draw; the rep coordinate is not reaching the "
        "stream and a wrong-rep unit would be undetectable")
    again = SR.SealedBootstrapRNGProvider(4242, 0.0, 0).open_stream("cellA").multinomial(
        10, np.full(10, 0.1))
    assert np.array_equal(a, again), "the same coordinates did not reproduce the same draw"


def t5_the_capability_loop_stays_in_the_harness():
    """`run_v2` takes one computational unit; world, δ and rep are not its parameters"""
    import inspect                                                   # noqa: PLC0415
    import v2_engine as EN                                           # noqa: PLC0415
    params = set(inspect.signature(EN.run_v2).parameters)
    for coordinate in ("world", "delta", "rep", "replicate", "delta_grid"):
        assert coordinate not in params, (
            f"{coordinate} reached the generic engine; the capability experiment's loop belongs "
            f"to the sealed harness")
    # the provider is where they live, and it is constructed outside the engine
    assert set(inspect.signature(SR.SealedBootstrapRNGProvider.__init__).parameters) == {
        "self", "world", "delta", "rep"}


def t6_the_legacy_projection_is_the_old_schema_and_nothing_more():
    """the artifact grew; its projection onto the proven semantics did not"""
    art = C.EngineResultArtifact(
        engine_version="v2", execution_mode=C.SEALED_ACCEPTANCE, spec_hash="s",
        input_outcome_hash="o", data_snapshot_id=SNAP,
        registered_search_space_hash="A", executed_search_space_hash="A",
        executed_cell_order_hash="ORD", estimand_version="e", support_policy_hash="sp",
        null_family="OPPORTUNITY_LEVEL", decision_policy_hash="dp", bootstrap_policy_hash="bp",
        rng_policy_id="r", rng_provenance_hash="rp",
        cell_results=(C.CellResult(
            cell_identity="cellA", evidence_claim_hash="ev", decision_spec_hash="ds",
            computation=C.CellComputation(theta_hex="0x1p+0", interval_hex=("0x0p+0", "0x1p+1"),
                                          bootstrap_summary={"n_boot": 200},
                                          support_identity="sup-1", eligibility={}),
            decision=C.CellDecision(stages=("BUILD",), final_verdict="BUILD",
                                    decision_policy_hash="dp")),))
    proj = art.legacy_projection()
    assert set(proj) == {"cellA"}
    assert set(proj["cellA"]) == {"theta_hex", "ci_low_hex", "ci_high_hex", "verdict",
                                  "support_hash"}
    assert proj["cellA"]["theta_hex"] == "0x1p+0"
    assert proj["cellA"]["verdict"] == "BUILD"


def t7_the_wrapper_rewiring_did_not_open_the_gate():
    """4B changes the wrapper, not the gate — asserted as a mechanism, not as a state

    Third test in this project to assert "X is blocked" and be falsified by X being legitimately
    unblocked later. The state was true when written and Gate 2 is a real transition. What
    survives a legitimate change is the MECHANISM: the path is shut when no record exists, and
    rewiring the wrapper is not what opens it.
    """
    import historical_application_gate as GATE                      # noqa: PLC0415
    stashed = GATE.RECORD + ".stashed-by-test"
    had = os.path.exists(GATE.RECORD)
    if had:
        os.rename(GATE.RECORD, stashed)
    try:
        _assert_historical_blocked()
    finally:
        if had:
            os.rename(stashed, GATE.RECORD)


def _assert_historical_blocked():
    import v2_engine as EN                                           # noqa: PLC0415
    spec = C.V2RunSpec("e", "A", "sp", "OPPORTUNITY_LEVEL", "dp", "bp", "def")
    ctx = C.ExecutionContext(C.HISTORICAL_RESEARCH, SNAP, "code",
                             C.RESEARCH_RNG, C.research_rng_material(spec, SNAP), "x")
    o = C.OutcomeVector(values=y(), outcome_id="real", outcome_semantics="ret*100",
                        source_kind=C.HISTORICAL_OBSERVED, source_snapshot_id=SNAP, units="pp",
                        row_alignment_hash=ALIGN, construction_hash="c")
    try:
        EN.assert_compatible(spec, ctx, o, registered_space_hash="A",
                             expected_alignment=ALIGN, n_rows=8)
    except EN.HistoricalApplicationNotQualifiedError:
        return
    raise AssertionError("the historical path was open with no governance record")


print("=" * 100, flush=True)
print("  4B · SEALED WRAPPER — the coordinates now live here, so the fixture does too", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_the_outcome_is_stamped_with_the_coordinates_it_was_built_for,
                        t2_an_outcome_from_an_adjacent_delta_is_refused,
                        t3_an_outcome_from_an_adjacent_world_is_refused,
                        t4_the_wrong_rep_produces_a_different_rng_stream,
                        t5_the_capability_loop_stays_in_the_harness,
                        t6_the_legacy_projection_is_the_old_schema_and_nothing_more,
                        t7_the_wrapper_rewiring_did_not_open_the_gate], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
