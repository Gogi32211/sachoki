"""Gate 2 · opening HISTORICAL_RESEARCH, as a recorded fact rather than a flag in code.

Nothing statistical happens here. Gate 1 was the experiment; this is the governance transition
that follows from it, and the difference matters: a boolean flipped in a source file is a
decision nobody can date, attribute or check. So the gate reads a record that must name what it
rests on, and refuses when the record is absent, incomplete, or points at a qualification that
did not pass.

    ENGINE_EXTRACTION           QUALIFIED   analytic-studio-combolab-engine-extraction-v1
    HISTORICAL_NUMERICAL_USE    QUALIFIED   real_y_qualification_v1 · 601f359bf5f47184 PASSED
    HISTORICAL_APPLICATION      QUALIFIED   this record

THE INVARIANT THIS FILE EXISTS TO PRESERVE. Opening the mode raises maturity for results produced
FROM NOW ON. It does not reach back:

    the 31 θ pinned in V2_CORE_ORACLE.json          ENGINE_QUALIFICATION_EVIDENCE, forever
    the 31 intervals from the qualification run     ENGINE_QUALIFICATION_EVIDENCE, forever

Both were exposed while establishing that the engine works, and a later decision that it does
work is not new evidence about them. `RetroactiveEvidenceUpgradeError` already refuses the
relabelling; this file is where the temptation actually arises, because everything really did
pass and the results are sitting right there.

WHAT THIS GATE DOES NOT OPEN. Ranking. ComboLab v2 has no registered production ranking for
historical cells — the sealed acceptance ranked planted needles inside a capability experiment —
so a results table would still have to invent an order. Choosing one after seeing which cells it
puts on top is the search degree of freedom this whole system exists to refuse. That is Gate 3
and it needs its own preregistration.
"""
from __future__ import annotations

import hashlib
import json
import os

import evidence_status as ES

HERE = os.path.dirname(os.path.abspath(__file__))
RECORD = os.path.join(HERE, "HISTORICAL_APPLICATION_QUALIFICATION.json")
QUALIFICATION_REPORT = os.path.join(HERE, "V2_REAL_Y_QUALIFICATION.json")
EXTRACTION_REPORT = os.path.join(HERE, "V2_EXTRACTION_EQUIVALENCE.json")

REQUIRED_SPEC_HASH = "601f359bf5f47184"


class HistoricalApplicationGateError(RuntimeError):
    """The gate cannot be opened, or cannot be shown to have been opened properly."""


def _read(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def evidence_of_qualification() -> dict:
    """What the transition must rest on, re-read rather than remembered."""
    q = _read(QUALIFICATION_REPORT)
    x = _read(EXTRACTION_REPORT)
    return {
        "numerical_qualification": {
            "spec_hash": q["spec_hash_registered"], "status": q["status"],
            "cells": q["cells_requested"], "publishable": q["cells_publishable"],
            "deterministic_rerun": q["deterministic_rerun"],
            "failures": q["failures"],
        },
        "extraction_equivalence": {
            "one_process_exact": x["one_process_exact"],
            "fresh_process_exact": x["fresh_process_exact"],
            "oracle_hash": x["oracle_hash"],
        },
    }


def can_open() -> tuple:
    """(bool, reasons). Every precondition, checked from the artifacts and not from memory."""
    reasons = []
    try:
        ev = evidence_of_qualification()
    except FileNotFoundError as e:
        return False, [f"missing evidence: {e.filename}"]

    n, x = ev["numerical_qualification"], ev["extraction_equivalence"]
    if n["spec_hash"] != REQUIRED_SPEC_HASH:
        reasons.append(f"qualification ran against {n['spec_hash']}, not {REQUIRED_SPEC_HASH}")
    if n["status"] != "PASSED":
        reasons.append(f"numerical qualification status is {n['status']}")
    if n["failures"]:
        reasons.append(f"{len(n['failures'])} registered criteria did not hold")
    if n["publishable"] != n["cells"]:
        reasons.append(f"{n['publishable']} of {n['cells']} cells publishable")
    if not n["deterministic_rerun"]:
        reasons.append("the qualification did not reproduce bit-identically")
    if not (x["one_process_exact"] and x["fresh_process_exact"]):
        reasons.append("extraction equivalence is not established in both processes")
    return (not reasons), reasons


def open_gate(*, opened_at: str, opened_by: str, note: str = "") -> dict:
    """Write the record. Refuses unless every precondition holds, read from the artifacts."""
    ok, reasons = can_open()
    if not ok:
        raise HistoricalApplicationGateError(
            "the historical application gate cannot be opened: " + "; ".join(reasons))
    rec = {
        "gate": "HISTORICAL_APPLICATION",
        "status": "QUALIFIED",
        "opened_at": opened_at,
        "opened_by": opened_by,
        "note": note,
        "rests_on": evidence_of_qualification(),
        "raises_maturity_for": "results produced after this record",
        "does_not_relabel": [
            "V2_CORE_ORACLE.json · 31 real-y theta · ENGINE_QUALIFICATION_EVIDENCE",
            "V2_REAL_Y_QUALIFICATION.json · 31 intervals · ENGINE_QUALIFICATION_EVIDENCE",
        ],
        "does_not_open": [
            "historical ranking — ComboLab v2 has no registered production ranking for "
            "historical cells; choosing one after seeing the results is Gate 3",
            "promotion of any already-exposed qualification evidence",
        ],
    }
    rec["record_hash"] = hashlib.sha256(
        json.dumps(rec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]
    with open(RECORD, "w") as f:
        json.dump(rec, f, indent=1, sort_keys=True)
    return rec


def is_open() -> bool:
    """Fail-closed: no record, a damaged record, or one whose evidence no longer holds."""
    if not os.path.exists(RECORD):
        return False
    try:
        rec = _read(RECORD)
    except Exception:                                                # noqa: BLE001
        return False
    if rec.get("gate") != "HISTORICAL_APPLICATION" or rec.get("status") != "QUALIFIED":
        return False
    ok, _ = can_open()          # re-checked, so a later failed qualification closes it again
    return ok


def maturity_for_new_results() -> str:
    return (ES.HISTORICAL_APPLICATION_QUALIFIED if is_open()
            else ES.FIRST_HISTORICAL_APPLICATION)


def maturity_for(evidence_verdict: dict | None = None) -> str:
    """The same answer, except for evidence that was already exposed before this gate opened.

    NEW RESULTS is doing real work in the function above, and nothing enforced it. A rerun of the
    31 already-exposed cells looks like a new result to every field except its identity — same
    data, same claims, same estimand, new timestamp — and would have collected
    HISTORICAL_APPLICATION_QUALIFIED for numbers exposed before this gate existed. That is the
    laundering route the gate itself opened, and it closes here rather than in the estimator,
    because re-executing is allowed; profiting from having re-executed is not.
    """
    if evidence_verdict is None:
        return maturity_for_new_results()
    import evidence_fingerprint as FP                                 # noqa: PLC0415
    proposed = {"application_maturity": maturity_for_new_results()}
    FP.assert_no_replay_laundering(evidence_verdict, proposed)
    return proposed["application_maturity"]


def status() -> dict:
    ok, reasons = can_open()
    return {"engine_extraction": "QUALIFIED",
            "historical_numerical_use": "QUALIFIED" if ok else "NOT QUALIFIED",
            "historical_application": "QUALIFIED" if is_open() else "NOT QUALIFIED",
            "historical_ranking": "NOT REGISTERED — Gate 3",
            "blocking_reasons": reasons,
            "maturity_for_new_results": maturity_for_new_results()}
