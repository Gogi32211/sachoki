"""The last laundering path: a new signboard over data that has already been read.

The fork rule closed the counter reset. This closes the one above it — the researcher who looks
at A, B and C, likes C, opens a fresh session, calls it independent research, preregisters C and
evaluates it on the same history. Every session-level rule in the system passes that sequence.
It is still the answer to a question whose answer was already seen.

THE ACCEPTANCE STATEMENT, and `t1` is exactly it:

    explore a winner → open a new session → register it → evaluate on the SAME already-exposed
    history → NOT confirmatory. Evaluate it on future or untouched data → confirmatory eligible.

DEFECT REPRODUCTION DISCIPLINE. Twice now a guard in this project passed on broken code: a test
that scanned rendered strings instead of structure, and an OpenAPI guard that could not run when
OpenAPI was the thing that was broken. So the central guards here are each shown a reproduced
defect first and required to fail on it. A guard that has only ever seen correct code is a claim
about a failure it has never detected.

    DefectReproduction  →  must fail  →  Guard  →  Fix  →  must pass
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import research_store as RS                                          # noqa: E402
from evidence_boundary import (CLEAN, CONTAMINATED, FORWARD, UNKNOWN,  # noqa: E402
                               DataWindow, EvidenceBoundary, EvidenceBoundaryError,
                               ExposureRegistry, confirmatory_verdict)
from research_family import ResearchFamily                           # noqa: E402

ok = fail = 0
TMP = tempfile.mkdtemp(prefix="evidence_")

HISTORY = DataWindow("bars_1d", "2021-01-01", "2026-08-01")
DEV = DataWindow("bars_1d", "2021-01-01", "2023-12-31")
OOS = DataWindow("bars_1d", "2024-01-01", "2026-08-01")
FUTURE = DataWindow("bars_1d", "2026-09-01", "2026-12-31")
TODAY = "2026-08-12"


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                           # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def ledger(name: str) -> RS.DurableLedger:
    return RS.DurableLedger(os.path.join(TMP, f"{name}.jsonl"))


def explore(L, session, family, claims, window):
    """A session that looks at several specifications on a window of data."""
    L.append(session, family, "SESSION_CREATED", event_id=0, prior_state_hash="",
             new_state_hash=f"{session}0")
    h, eid = f"{session}0", 1
    for c in claims:
        L.append(session, family, "RESULT_EXPOSED", event_id=eid, prior_state_hash=h,
                 new_state_hash=f"{session}{eid}", claim_hash=c,
                 payload={"window": window.as_dict()})
        h, eid = f"{session}{eid}", eid + 1
    return h, eid


def register(L, session, family, claim):
    L.append(session, family, "SESSION_CREATED", event_id=0, prior_state_hash="",
             new_state_hash=f"{session}0")
    L.append(session, family, "SESSION_FROZEN", event_id=1, prior_state_hash=f"{session}0",
             new_state_hash=f"{session}1", claim_hash=claim,
             payload={"space_id": "combolab_v2", "size": 31, "hash": "3600ae3dd52a25e6"})


def boundary(validation: DataWindow, available: str = TODAY) -> EvidenceBoundary:
    return EvidenceBoundary(development=DEV, validation=validation,
                            claim_registered_at=f"{TODAY}T12:00:00",
                            data_available_at_registration=available)


# ── THE ACCEPTANCE STATEMENT ────────────────────────────────────────────────
def t1_a_new_session_does_not_make_seen_evidence_fresh():
    """explore A B C → new session → register B → same history → NOT confirmatory"""
    L = ledger("main")
    explore(L, "s1", "F1", ["A", "B", "C"], HISTORY)      # the whole history was read
    register(L, "s2", "F1", "B")                          # a clean, brand-new session

    fam = ResearchFamily("F1", L.read_all())
    v = fam.confirmatory(boundary(OOS))
    assert v["eligible"] is False, f"already-exposed history became confirmatory evidence: {v}"
    assert v["status"] == CONTAMINATED, v
    assert "has been seen" in v["why"], v

    # and the same registered claim IS eligible against observations that did not exist yet
    fwd = fam.confirmatory(boundary(FUTURE))
    assert fwd["eligible"] is True, f"forward evidence was refused: {fwd}"
    assert fwd["status"] == FORWARD, fwd


def t2_renaming_the_family_launders_nothing():
    """'start independent research' is a button, and it does not unsee 2024-2026"""
    L = ledger("rename")
    explore(L, "s1", "F1", ["A", "B", "C"], HISTORY)
    register(L, "s2", "F2", "B")                          # a DIFFERENT family entirely

    v = ResearchFamily("F2", L.read_all()).confirmatory(boundary(OOS))
    assert v["eligible"] is False, f"a new family_id cleared the contamination: {v}"
    assert v["status"] == CONTAMINATED, v
    assert v["sessions_in_family"] == 1, "the new family really is separate, and it does not help"


def t2b_REPRODUCTION_a_family_scoped_registry_would_pass_this():
    """the guard shown its defect: scope contamination to the family and t2 goes green"""
    L = ledger("rename2")
    explore(L, "s1", "F1", ["A", "B", "C"], HISTORY)
    register(L, "s2", "F2", "B")
    all_events = L.read_all()

    broken = ExposureRegistry.from_events([e for e in all_events if e.family_id == "F2"])
    wrong = confirmatory_verdict(registered=True, boundary=boundary(OOS), registry=broken)
    assert wrong["eligible"] is True, (
        "the reproduction failed to reproduce: a family-scoped registry was supposed to wrongly "
        "approve this, and if it does not, t2 is not testing what it claims to test")

    correct = ExposureRegistry.from_events(all_events)
    right = confirmatory_verdict(registered=True, boundary=boundary(OOS), registry=correct)
    assert right["eligible"] is False, "the global registry did not catch what the broken one let through"


# ── the other verdicts ──────────────────────────────────────────────────────
def t3_untouched_historical_oos_is_clean():
    """development-only exposure leaves a genuine holdout, and it counts"""
    L = ledger("clean")
    explore(L, "s1", "F3", ["A", "B", "C"], DEV)          # only 2021-2023 was ever read
    register(L, "s2", "F3", "B")
    v = ResearchFamily("F3", L.read_all()).confirmatory(boundary(OOS))
    assert v["status"] == CLEAN and v["eligible"] is True, v
    assert "rests on the ledger being complete" in v["why"], v


def t4_an_exposure_with_no_footprint_is_treated_as_contamination():
    """UNKNOWN must not read as clean: the weakest bookkeeping cannot license the strongest claim"""
    L = ledger("unknown")
    L.append("s1", "F4", "SESSION_CREATED", event_id=0, prior_state_hash="", new_state_hash="x0")
    L.append("s1", "F4", "RESULT_EXPOSED", event_id=1, prior_state_hash="x0",
             new_state_hash="x1", claim_hash="A")          # no window recorded
    register(L, "s2", "F4", "A")
    v = ResearchFamily("F4", L.read_all()).confirmatory(boundary(OOS))
    assert v["status"] == UNKNOWN and v["eligible"] is False, v


def t4b_REPRODUCTION_unknown_ranked_below_clean_would_pass():
    """if UNKNOWN were checked after CLEAN, an unrecorded exposure would be approved"""
    # a registry holding exactly one exposure whose footprint is unknown
    from evidence_boundary import ExposureRecord
    reg = ExposureRegistry([ExposureRecord("s1", "F4", "A", None)])
    status, _ = reg.status_for(boundary(OOS))
    assert status == UNKNOWN, (
        f"an unrecorded exposure resolved to {status}. If this ever returns CLEAN, every session "
        f"that forgets to record its data footprint becomes eligible for confirmatory standing.")


def t5_registration_alone_is_not_confirmatory_standing():
    L = ledger("noboundary")
    register(L, "s1", "F5", "A")
    v = ResearchFamily("F5", L.read_all()).confirmatory(None)
    assert v["eligible"] is False and v["status"] == "NO_BOUNDARY", v
    assert "not confirmatory standing" in v["why"], v


def t6_an_unregistered_family_has_nothing_to_confirm():
    L = ledger("unreg")
    explore(L, "s1", "F6", ["A"], DEV)
    v = ResearchFamily("F6", L.read_all()).confirmatory(boundary(OOS))
    assert v["eligible"] is False and v["status"] == "NOT_REGISTERED", v


def t7_forward_beats_an_incomplete_ledger():
    """FORWARD does not depend on the ledger being complete, and CLEAN does"""
    L = ledger("forward")
    L.append("s1", "F7", "SESSION_CREATED", event_id=0, prior_state_hash="", new_state_hash="y0")
    L.append("s1", "F7", "RESULT_EXPOSED", event_id=1, prior_state_hash="y0",
             new_state_hash="y1", claim_hash="A")          # footprint unknown → poisons CLEAN
    register(L, "s2", "F7", "A")
    fam = ResearchFamily("F7", L.read_all())
    assert fam.confirmatory(boundary(OOS))["status"] == UNKNOWN
    fwd = fam.confirmatory(boundary(FUTURE))
    assert fwd["status"] == FORWARD and fwd["eligible"] is True, \
        f"forward validation was blocked by bookkeeping it does not depend on: {fwd}"


def t8_a_boundary_that_contains_itself_is_refused():
    try:
        EvidenceBoundary(development=HISTORY, validation=OOS,
                         claim_registered_at=f"{TODAY}T12:00:00",
                         data_available_at_registration=TODAY)
    except EvidenceBoundaryError as e:
        assert "not a boundary" in str(e)
        return
    raise AssertionError("development and validation were allowed to overlap")


def t9_forward_is_decided_by_data_availability_not_by_the_clock():
    """registering today against a window that already exists is not forward validation"""
    already = boundary(OOS, available=TODAY)
    assert already.is_forward is False
    truly = boundary(FUTURE, available=TODAY)
    assert truly.is_forward is True
    # backdating what was available cannot manufacture forwardness in the other direction either
    fake = boundary(OOS, available="2023-12-31")
    assert fake.is_forward is True, (
        "is_forward reads data_available_at_registration, so that field is a commitment. It is "
        "carried in boundary_hash and belongs in the ledger, not in a form field nobody checks.")
    assert fake.boundary_hash != already.boundary_hash


def t10_the_multiplicity_a_verdict_must_survive_is_the_family():
    """three sessions, one selection history — k is the union, not the last session"""
    L = ledger("mult")
    explore(L, "s1", "F8", ["A", "B", "C"], DEV)
    explore(L, "s2", "F8", ["C", "D"], DEV)               # C reopened, D new
    register(L, "s3", "F8", "D")
    acc = ResearchFamily("F8", L.read_all()).accounting()
    assert acc.k_family_exposed == 4, f"expected |{{A,B,C,D}}| = 4, got {acc.k_family_exposed}"
    assert len(acc.session_ids) == 3, acc
    v = ResearchFamily("F8", L.read_all()).confirmatory(boundary(FUTURE))
    assert v["k_family_exposed"] == 4, "the verdict reported the session's k, not the family's"


print("=" * 104, flush=True)
print("  EVIDENCE BOUNDARY — a new session is not new evidence", flush=True)
print("=" * 104, flush=True)
for i, fn in enumerate([t1_a_new_session_does_not_make_seen_evidence_fresh,
                        t2_renaming_the_family_launders_nothing,
                        t2b_REPRODUCTION_a_family_scoped_registry_would_pass_this,
                        t3_untouched_historical_oos_is_clean,
                        t4_an_exposure_with_no_footprint_is_treated_as_contamination,
                        t4b_REPRODUCTION_unknown_ranked_below_clean_would_pass,
                        t5_registration_alone_is_not_confirmatory_standing,
                        t6_an_unregistered_family_has_nothing_to_confirm,
                        t7_forward_beats_an_incomplete_ledger,
                        t8_a_boundary_that_contains_itself_is_refused,
                        t9_forward_is_decided_by_data_availability_not_by_the_clock,
                        t10_the_multiplicity_a_verdict_must_survive_is_the_family], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 104, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
shutil.rmtree(TMP, ignore_errors=True)
sys.exit(1 if fail else 0)
