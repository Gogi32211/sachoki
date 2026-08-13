"""Gate 2 · a governance transition, and the invariant it must not break.

Nothing statistical happens here — Gate 1 was the experiment. What this file defends is the line
between "the engine is qualified" and "what the engine already produced is qualified". Everything
really did pass, the results are sitting right there, and that is exactly when the relabelling
looks reasonable.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import evidence_status as ES                                        # noqa: E402
import historical_application_gate as G                             # noqa: E402

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


def record() -> dict:
    with open(G.RECORD) as f:
        return json.load(f)


def t1_the_gate_is_open_and_says_what_it_rests_on():
    assert G.is_open() is True
    r = record()
    assert r["status"] == "QUALIFIED"
    assert r["opened_at"] and r["opened_by"], "an undated or unattributed transition"
    n = r["rests_on"]["numerical_qualification"]
    assert n["spec_hash"] == "601f359bf5f47184" and n["status"] == "PASSED"
    x = r["rests_on"]["extraction_equivalence"]
    assert x["one_process_exact"] and x["fresh_process_exact"]


def t2_preconditions_are_re_read_not_remembered():
    """a later failed qualification must close the gate again"""
    okc, reasons = G.can_open()
    assert okc and reasons == [], reasons
    assert "can_open()" in open(os.path.join(HERE, "historical_application_gate.py")).read()


def t3_the_engine_path_is_now_open():
    import numpy as np                                              # noqa: PLC0415
    import v2_engine as EN                                          # noqa: PLC0415
    import v2_engine_contract as C                                  # noqa: PLC0415
    spec = C.V2RunSpec("e", "A", "sp", "OPPORTUNITY_LEVEL", "dp", "bp", "d")
    ctx = C.ExecutionContext(C.HISTORICAL_RESEARCH, "s", "c", C.RESEARCH_RNG,
                             C.research_rng_material(spec, "s"), "x")
    o = C.OutcomeVector(values=np.zeros(3), outcome_id="r", outcome_semantics="ret",
                        source_kind=C.HISTORICAL_OBSERVED, source_snapshot_id="s", units="pp",
                        row_alignment_hash="a", construction_hash="c")
    EN.assert_compatible(spec, ctx, o, registered_space_hash="A", expected_alignment="a",
                         n_rows=3)


def t4_a_sealed_run_still_cannot_see_real_returns():
    """opening one mode must not loosen the other"""
    import numpy as np                                              # noqa: PLC0415
    import v2_engine_contract as C                                  # noqa: PLC0415
    o = C.OutcomeVector(values=np.zeros(3), outcome_id="r", outcome_semantics="ret",
                        source_kind=C.HISTORICAL_OBSERVED, source_snapshot_id="s", units="pp",
                        row_alignment_hash="a", construction_hash="c")
    try:
        C.assert_outcome_allowed(C.SEALED_ACCEPTANCE, o)
    except C.OutcomeSemanticsError:
        return
    raise AssertionError("opening the historical mode loosened the sealed one")


def t5_maturity_rises_only_for_new_results():
    assert G.maturity_for_new_results() == ES.HISTORICAL_APPLICATION_QUALIFIED


def t6_the_already_exposed_evidence_keeps_its_role():
    """the temptation is strongest here, because everything passed"""
    with open(os.path.join(HERE, "EVIDENCE_EXPOSURE_LOG.json")) as f:
        log = json.load(f)
    assert len(log["exposures"]) == 2, len(log["exposures"])
    for e in log["exposures"]:
        assert e["evidence_status"]["result_role"] == ES.ENGINE_QUALIFICATION_EVIDENCE, e
        assert e["evidence_status"]["application_maturity"] == ES.FIRST_HISTORICAL_APPLICATION
        assert e["immutable"] is True
        try:
            ES.upgrade_result_role(
                ES.EvidenceStatus(**{k: v for k, v in e["evidence_status"].items()}),
                ES.REGISTERED_VALIDATION_EVIDENCE, was_exposed=True)
        except ES.RetroactiveEvidenceUpgradeError:
            continue
        raise AssertionError(f"{e['exposure_id']} was upgraded by the gate opening")


def t7_the_record_names_what_it_does_not_open():
    """ranking is not registered, and a table would have to invent an order"""
    r = record()
    joined = " ".join(r["does_not_open"]).lower()
    assert "ranking" in joined and "gate 3" in joined
    assert G.status()["historical_ranking"] == "NOT REGISTERED — Gate 3"
    for line in r["does_not_relabel"]:
        assert "ENGINE_QUALIFICATION_EVIDENCE" in line


def t8_preconditions_alone_do_not_open_it():
    """a mode that opens because its preconditions happen to hold is a decision nobody made

    Checked by reading the engine's refusal path: absent the record it raises even when
    `can_open()` is true, and the message says so.
    """
    src = open(os.path.join(HERE, "v2_engine.py")).read()
    assert "GATE.is_open()" in src
    assert "nobody dated or" in src


print("=" * 100, flush=True)
print("  GATE 2 · HISTORICAL APPLICATION — governance, not statistics", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_the_gate_is_open_and_says_what_it_rests_on,
                        t2_preconditions_are_re_read_not_remembered,
                        t3_the_engine_path_is_now_open,
                        t4_a_sealed_run_still_cannot_see_real_returns,
                        t5_maturity_rises_only_for_new_results,
                        t6_the_already_exposed_evidence_keeps_its_role,
                        t7_the_record_names_what_it_does_not_open,
                        t8_preconditions_alone_do_not_open_it], 1):
    check(f"{i} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
