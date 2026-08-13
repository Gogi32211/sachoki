"""Gate 1, asserted from the artifact — including that it did not read the answer.

The criteria were registered at 601f359bf5f47184 before this ran, and the sharpest assertion
here is `t6`: none of them mentions whether the results are good. A qualification that could
fail because the numbers were disappointing would be an acceptance test for the data.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import real_y_qualification_spec as SPEC                            # noqa: E402

ok = fail = 0
HERE = os.path.dirname(os.path.abspath(__file__))


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
    with open(os.path.join(HERE, "V2_REAL_Y_QUALIFICATION.json")) as f:
        return json.load(f)


def t1_it_ran_against_the_registered_spec():
    r = report()
    assert r["spec_hash_registered"] == SPEC.spec_hash() == "601f359bf5f47184"
    assert r["reps_requested"] == SPEC.BOOTSTRAP_REPS_REQUESTED == 2000
    assert r["valid_floor"] == SPEC.BOOTSTRAP_VALID_FLOOR == 1.0


def t2_every_registered_criterion_held():
    r = report()
    assert r["failures"] == [], r["failures"]
    assert r["status"] == "PASSED"
    assert len(r["cells"]) == r["cells_requested"] == 31
    assert r["nonfinite_y"] == 0


def t3_the_accounting_reconciles_per_cell():
    """valid + rejected == requested, or replicates went somewhere unrecorded"""
    for c in report()["cells"]:
        assert c["reps_valid"] + c["reps_rejected"] == c["reps_requested"] == 2000, c["cell"]
    total = sum(c["reps_valid"] for c in report()["cells"])
    assert total == 31 * 2000, total


def t4_no_replicate_was_dropped_without_a_reason():
    r = report()
    for c in r["cells"]:
        if c["reps_rejected"]:
            assert c["rejection_reasons"], c["cell"]
            for reason in c["rejection_reasons"]:
                assert reason in SPEC.REASON_CODES, reason
        if not c["publishable"]:
            assert c["uncomputable_reason"] in SPEC.REASON_CODES, c["cell"]


def t5_the_rerun_is_bit_identical_and_the_rng_is_not_sealed():
    r = report()
    assert r["deterministic_rerun"] is True
    assert r["rng_policy_id"] == "historical_research_rng_v1"
    assert r["rng_is_sealed_lineage"] is False, "sealed material reached a historical run"


def t6_the_gate_never_read_the_answer():
    """the assertion that keeps this a qualification rather than a result

    Nothing in the registered criteria mentions effect direction, significance or how many cells
    reached BUILD. A gate that could fail on disappointing numbers is an acceptance test for the
    data, not for the instrument.
    """
    joined = " ".join(SPEC.CRITERIA.values()).lower()
    for forbidden in ("positive", "significant", "build", "excludes zero", "interesting"):
        assert forbidden not in joined, forbidden
    not_criteria = " ".join(SPEC.NOT_CRITERIA).lower()
    for named in ("build", "positive", "excludes zero", "interesting"):
        assert named in not_criteria, named


def t7_the_numerical_gate_did_not_open_the_engine_path_by_itself():
    """the qualification run recorded that it left the path shut; Gate 2 opened it later

    This test asserted a blocked path when it was written, and Gate 2 is a real transition that
    happened afterwards. Re-asserting the block would be asserting that a governance decision
    did not occur. What must still hold is that the numerical gate is not what opened it — so
    the Gate 2 record is hidden and the path must close again.
    """
    assert "remains blocked" in report()["execution_mode"], (
        "the qualification run must record that it did not open anything")

    import historical_application_gate as GATE                      # noqa: PLC0415
    stashed = GATE.RECORD + ".stashed-by-test"
    had_record = os.path.exists(GATE.RECORD)
    if had_record:
        os.rename(GATE.RECORD, stashed)
    try:
        import v2_engine as EN                                      # noqa: PLC0415
        import v2_engine_contract as C                              # noqa: PLC0415
        spec = C.V2RunSpec("e", "A", "sp", "OPPORTUNITY_LEVEL", "dp", "bp", "d")
        ctx = C.ExecutionContext(C.HISTORICAL_RESEARCH, "s", "c", C.RESEARCH_RNG,
                                 C.research_rng_material(spec, "s"), "x")
        o = C.OutcomeVector(values=__import__("numpy").zeros(3), outcome_id="r",
                            outcome_semantics="ret", source_kind=C.HISTORICAL_OBSERVED,
                            source_snapshot_id="s", units="pp", row_alignment_hash="a",
                            construction_hash="c")
        try:
            EN.assert_compatible(spec, ctx, o, registered_space_hash="A",
                                 expected_alignment="a", n_rows=3)
        except EN.HistoricalApplicationNotQualifiedError:
            pass
        else:
            raise AssertionError("the numerical gate opened the historical engine path by itself")
    finally:
        if had_record:
            os.rename(stashed, GATE.RECORD)


def t8_the_exposure_was_recorded_and_is_immutable():
    with open(os.path.join(HERE, "EVIDENCE_EXPOSURE_LOG.json")) as f:
        log = json.load(f)
    rec = [e for e in log["exposures"] if e["exposure_id"].startswith("v2-real-y")]
    assert len(rec) == 1, len(rec)
    e = rec[0]
    assert e["immutable"] is True
    assert e["evidence_status"]["result_role"] == "ENGINE_QUALIFICATION_EVIDENCE"
    assert e["evidence_status"]["application_maturity"] == "FIRST_HISTORICAL_APPLICATION"
    assert len(e["cells"]) == 31


print("=" * 100, flush=True)
print("  GATE 1 · REAL-Y NUMERICAL QUALIFICATION", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_it_ran_against_the_registered_spec,
                        t2_every_registered_criterion_held,
                        t3_the_accounting_reconciles_per_cell,
                        t4_no_replicate_was_dropped_without_a_reason,
                        t5_the_rerun_is_bit_identical_and_the_rng_is_not_sealed,
                        t6_the_gate_never_read_the_answer,
                        t7_the_numerical_gate_did_not_open_the_engine_path_by_itself,
                        t8_the_exposure_was_recorded_and_is_immutable], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
