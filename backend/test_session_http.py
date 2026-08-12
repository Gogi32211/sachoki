"""Acceptance over the real ASGI routing layer, because that is the layer that broke.

Every test this project had for the session API called a Python function. `change_and_run(...)`
returned the right dictionary, `to_view(...)` produced the right strings, and `tsc` was happy —
and every POST answered 422, because FastAPI bound the request model as a query parameter. The
defect lived precisely in the gap the tests jumped over.

    a test that imports the function proves the function
    a test that issues a request proves the endpoint

So this file never imports a handler. It builds the app, mounts the routers exactly as
`main.py` does, and goes through `TestClient` — real routing, real body parsing, real validation,
real status codes.

Two kinds of test live here:

  BINDING     structural claims about the wiring itself, and the regression guards for
              2026-08-12. The cause was narrower than the first diagnosis said. A locally
              scoped pydantic model is harmless on its own — reproduced, and it works. It
              becomes fatal only together with `from __future__ import annotations`, which this
              module has: PEP 563 turns every annotation into a string, FastAPI resolves it
              against the module globals, a class defined inside a function is not there, and
              the parameter degrades to `Annotated[ForwardRef('ChangeBody'), Query(...)]`.

              That has two symptoms, and only one of them was noticed. The request answers 422
              with `loc ["query", "b"]`. The schema cannot be built at all, so `/openapi.json`
              — and with it the whole application's `/docs` — answers 500. `t1` checks the
              schema generates; `t2` checks no write endpoint carries the malformation. Neither
              looks for the string "ChangeBody"; both look for the shape.

  OPERATIONAL the four browser actions from the vertical slice, replayed over HTTP as a golden
              fixture. `k_selectable` is 31 at every step and 5 at none of them.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI                                          # noqa: E402
from fastapi.testclient import TestClient                            # noqa: E402

import studio_semantics_api as SEM                                   # noqa: E402
import studio_session_api as SESS                                    # noqa: E402

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


def _app() -> FastAPI:
    """Mounted the way main.py mounts it. If that changes, this must change with it."""
    app = FastAPI()
    app.include_router(SESS.build_router())
    app.include_router(SEM.build_router())
    return app


APP = _app()
C = TestClient(APP, raise_server_exceptions=False)
_SPEC_RESPONSE = C.get("/openapi.json")


def _new_session() -> str:
    r = C.post("/api/studio/session/create")
    assert r.status_code == 200, r.text
    return r.json()["session"]["session_id"]


def _acc(sid: str) -> dict:
    r = C.get(f"/api/studio/session/{sid}")
    assert r.status_code == 200, r.text
    return r.json()["session"]


# ── BINDING ─────────────────────────────────────────────────────────────────
def t1_the_schema_can_be_built_at_all():
    """the unnoticed half of 2026-08-12: an unresolvable annotation breaks /docs, not one route"""
    assert _SPEC_RESPONSE.status_code == 200, (
        f"/openapi.json answered {_SPEC_RESPONSE.status_code}. A handler annotation cannot be "
        f"resolved, so no schema exists for any route in the application — the failure is "
        f"app-wide even when a single endpoint caused it. {_SPEC_RESPONSE.text[:300]}")


def t2_no_post_binds_a_model_as_a_query_parameter():
    """the same defect seen structurally, for the variants where a schema still builds"""
    SPEC = _SPEC_RESPONSE.json()
    offenders = []
    for path, ops in SPEC["paths"].items():
        for verb, op in ops.items():
            if verb.lower() not in ("post", "put", "patch"):
                continue
            nonpath = [p for p in op.get("parameters", []) if p.get("in") != "path"]
            if nonpath and "requestBody" not in op:
                offenders.append((verb.upper(), path, [p["name"] for p in nonpath]))
    assert not offenders, (
        f"a write endpoint takes non-path parameters and declares no body: {offenders}. That is "
        f"what an unresolved body annotation looks like from outside: FastAPI could not find the "
        f"model and fell back to Query.")


def t3_the_body_actually_binds():
    """the failing call itself: a JSON body reaches the handler as a body"""
    sid = _new_session()
    r = C.post(f"/api/studio/session/{sid}/change",
               json={"parameter_id": "conditioning_tolerance", "horizon": "20",
                     "tolerance": "5", "new_value": "1"})
    assert r.status_code == 200, f"HTTP {r.status_code}: {r.text}"
    d = r.json()
    assert d["tolerance"] == "1" and d["horizon"] == "20", d
    assert d["change_type"] == "CLAIM_CHANGE", d


def t4_every_transported_field_is_a_string_in_the_json_itself():
    """not asserted on the dataclass — on what the wire carries after serialisation"""
    s = _acc(_new_session())
    bad = {k: type(v).__name__ for k, v in s.items() if not isinstance(v, str)}
    assert not bad, f"a non-string reached the browser: {bad}"


def t5_a_missing_body_is_still_refused():
    """the guard must not have been relaxed into accepting anything"""
    sid = _new_session()
    r = C.post(f"/api/studio/session/{sid}/change", json={})
    assert r.status_code == 422, f"an incomplete claim change was accepted: {r.status_code}"


def t6_unknown_session_is_404_not_500():
    assert C.get("/api/studio/session/nope").status_code == 404
    r = C.post("/api/studio/session/nope/revisit", json={"horizon": "20", "tolerance": "5"})
    assert r.status_code == 404, r.status_code


# ── OPERATIONAL · the golden four-action fixture ────────────────────────────
GOLDEN = [
    ("START",                  "0", "0",  "0", "0"),
    ("tolerance +-5 -> +-1",   "1", "31", "0", "1"),
    ("reopen this result",     "1", "31", "1", "1"),
    ("horizon 20 -> 40",       "2", "31", "1", "2"),
]


def t7_four_actions_over_http():
    """the browser sequence, replayed through routing; the numbers are the acceptance"""
    sid = _new_session()
    seen = [("START", _acc(sid))]

    r = C.post(f"/api/studio/session/{sid}/change",
               json={"parameter_id": "conditioning_tolerance", "horizon": "20",
                     "tolerance": "5", "new_value": "1"})
    assert r.status_code == 200, r.text
    seen.append(("tolerance +-5 -> +-1", r.json()["session"]))

    r = C.post(f"/api/studio/session/{sid}/revisit", json={"horizon": "20", "tolerance": "1"})
    assert r.status_code == 200, r.text
    seen.append(("reopen this result", r.json()["session"]))

    r = C.post(f"/api/studio/session/{sid}/change",
               json={"parameter_id": "horizon", "horizon": "20", "tolerance": "1",
                     "new_value": "40"})
    assert r.status_code == 200, r.text
    seen.append(("horizon 20 -> 40", r.json()["session"]))

    for (label, exp, sel, rev, chg), (got_label, s) in zip(GOLDEN, seen):
        assert got_label == label
        actual = (s["k_exposed"], s["k_selectable"], s["revisits"], s["changes_claim"])
        assert actual == (exp, sel, rev, chg), f"{label}: expected {(exp, sel, rev, chg)}, got {actual}"


def t8_multiplicity_never_equals_the_screen():
    """the fatal UI accounting defect, checked at every step and not only at the end"""
    sid = _new_session()
    states = [_acc(sid)]
    for tol in ("1", "2"):
        r = C.post(f"/api/studio/session/{sid}/change",
                   json={"parameter_id": "conditioning_tolerance", "horizon": "20",
                         "tolerance": "5", "new_value": tol, "space_size": 31, "displayed": 5})
        states.append(r.json()["session"])
    for s in states[1:]:
        assert s["k_selectable"] == "31", s
        assert s["displayed_at_most"] == "5", s
        assert s["k_selectable"] != s["displayed_at_most"], \
            "multiplicity followed the screen through the transport layer"


def t9_revisiting_is_free_and_recorded_as_free():
    sid = _new_session()
    C.post(f"/api/studio/session/{sid}/change",
           json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5", "new_value": "40"})
    before = _acc(sid)
    for _ in range(3):
        C.post(f"/api/studio/session/{sid}/revisit", json={"horizon": "40", "tolerance": "5"})
    after = _acc(sid)
    assert after["k_exposed"] == before["k_exposed"], \
        f"reopening the same specification cost multiplicity: {before} -> {after}"
    assert int(after["revisits"]) == int(before["revisits"]) + 3, after


def t10_preview_costs_nothing():
    """the UI asks what a change means BEFORE it happens, and asking is not doing"""
    sid = _new_session()
    before = _acc(sid)
    r = C.post(f"/api/studio/session/{sid}/preview",
               json={"parameter_id": "conditioning_tolerance", "horizon": "20",
                     "tolerance": "5", "new_value": "1"})
    assert r.status_code == 200, r.text
    p = r.json()
    assert p["change_type"] == "CLAIM_CHANGE" and p["multiplicity_effect"] == "NEW_SELECTABLE_CLAIM"
    after = _acc(sid)
    assert after["state_hash"] == before["state_hash"], \
        "a preview mutated the ledger; the question became the answer"


def t11_exploration_is_never_confirmatory_over_the_wire():
    sid = _new_session()
    C.post(f"/api/studio/session/{sid}/change",
           json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5", "new_value": "40"})
    assert _acc(sid)["confirmatory_eligible"] == "NO"


# ── REGISTERED and the fork, over HTTP ──────────────────────────────────────
def _register(sid: str, start: str = "2026-09-01", end: str = "2026-12-31", **kw):
    """Preregistration names the validation window; the clock and cutoff are the server's."""
    body = {"validation_start": start, "validation_end": end}
    body.update(kw)
    return C.post(f"/api/studio/session/{sid}/register", json=body)


def t14_register_then_freeze():
    sid = _new_session()
    r = _register(sid)
    assert r.status_code == 200, r.text
    s = r.json()["session"]
    assert s["mode"] == "REGISTERED" and s["confirmatory_eligible"] == "YES", s
    assert s["k_declared"] == "31", s


def t15_register_after_seeing_is_refused_with_a_sentence():
    """the refusal must be readable, not a disabled button with no explanation"""
    sid = _new_session()
    C.post(f"/api/studio/session/{sid}/change",
           json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5", "new_value": "40"})
    r = _register(sid)
    assert r.status_code == 409, r.status_code
    d = r.json()["detail"]
    assert d["error"] == "CannotRegisterAfterExposureError", d
    assert "exploratory forever" in d["detail"], d
    assert d["remedy"], "a refusal arrived with no remedy for the UI to show"
    assert d["next_action"] == "NEW_SESSION", \
        f"the refusal pointed at a move that cannot help: {d['next_action']}"


def t16_a_refused_registration_leaves_no_trace():
    """the bug found while writing this: declaring the space before the check"""
    sid = _new_session()
    C.post(f"/api/studio/session/{sid}/change",
           json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5", "new_value": "40"})
    before = _acc(sid)
    assert _register(sid).status_code == 409
    after = _acc(sid)
    assert after["state_hash"] == before["state_hash"], \
        f"a refused registration moved the ledger: {before['state_hash']} -> {after['state_hash']}"
    assert after["k_declared"] == "0", \
        f"a search space was declared by a registration that never happened: {after}"


def t17_a_frozen_study_refuses_mutation_and_offers_the_fork():
    sid = _new_session()
    _register(sid)
    before = _acc(sid)
    r = C.post(f"/api/studio/session/{sid}/change",
               json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5",
                     "new_value": "40"})
    assert r.status_code == 409, r.status_code
    d = r.json()["detail"]
    assert d["offers_fork"] == "YES" and d["next_action"] == "FORK", d
    assert "Fork it into a new exploratory session" in d["remedy"], d
    assert _acc(sid)["state_hash"] == before["state_hash"], "the refusal itself mutated the study"


def t18_the_fork_is_a_new_session_carrying_the_lineage():
    sid = _new_session()
    _register(sid)
    C.post(f"/api/studio/session/{sid}/revisit", json={"horizon": "20", "tolerance": "5"})
    r = C.post(f"/api/studio/session/{sid}/fork",
               json={"reason": "horizon 20 no longer plausible", "horizon": "20",
                     "tolerance": "5"})
    assert r.status_code == 200, r.text
    d = r.json()
    child, parent = d["session"], d["parent"]
    assert child["session_id"] != sid and child["parent_session_id"] == sid, child
    assert child["mode"] == "EXPLORE" and child["confirmatory_eligible"] == "NO", child
    assert child["k_exposed"] == "0", "the child claims to have run something"
    assert child["k_exposed_lineage"] == "1", f"the upstream exposure vanished: {child}"
    assert child["k_declared"] == "0", "a preregistration was inherited"
    assert d["inherited"] == {"horizon": "20", "tolerance": "5"}, d["inherited"]
    assert parent["mode"] == "ACTIVE_REGISTERED", parent


def t19_a_fork_cannot_launder_the_counter_over_http():
    """the whole point of the fork contract, exercised through routing"""
    sid = _new_session()
    for v in ("40", "60"):
        C.post(f"/api/studio/session/{sid}/change",
               json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5",
                     "new_value": v})
    r = C.post(f"/api/studio/session/{sid}/fork",
               json={"reason": "start again", "horizon": "60", "tolerance": "5"})
    child = r.json()["session"]
    assert child["k_exposed"] == "0" and child["k_exposed_lineage"] == "2", child
    rr = _register(child["session_id"])
    assert rr.status_code == 409, "a fork reset the counter and became registerable"
    assert "upstream" in rr.json()["detail"]["detail"], rr.json()


def t20b_a_fork_of_a_clean_parent_still_cannot_preregister():
    """no result was ever exposed in this lineage, and the fork is still exploratory"""
    sid = _new_session()
    _register(sid)
    r = C.post(f"/api/studio/session/{sid}/fork",
               json={"reason": "different horizon", "horizon": "20", "tolerance": "5"})
    child = r.json()["session"]
    assert child["k_exposed_lineage"] == "0", child
    rr = _register(child["session_id"])
    assert rr.status_code == 409, "a fork of a clean parent entered the confirmatory track"
    d = rr.json()["detail"]
    assert "no parent" in d["detail"], d
    assert d["next_action"] == "NEW_SESSION" and d["offers_fork"] == "NO", \
        f"a fork was offered to a session that forking cannot help: {d}"
    assert "forking will not get you there" in d["remedy"], d


def t20_an_anonymous_fork_is_refused():
    sid = _new_session()
    _register(sid)
    r = C.post(f"/api/studio/session/{sid}/fork",
               json={"reason": "   ", "horizon": "20", "tolerance": "5"})
    assert r.status_code == 409, r.status_code
    assert "must say why" in r.json()["detail"]["detail"], r.json()


# ── durability and idempotency, over the same routing layer ─────────────────
def t21_a_registration_survives_a_process_restart():
    """the reason the ledger went to disk: a promise a restart can forget is a note"""
    sid = _new_session()
    _register(sid)
    before = _acc(sid)
    # a new app object over the SAME durable store is what a restart looks like from here
    fresh = TestClient(_app(), raise_server_exceptions=False)
    after = fresh.get(f"/api/studio/session/{sid}").json()["session"]
    assert after == before, f"state changed across a restart:\n{before}\n{after}"
    assert after["mode"] == "REGISTERED" and after["k_declared"] == "31", after


def t22_a_retried_change_is_not_a_second_claim():
    """the response was lost and the browser sent the same action again"""
    sid = _new_session()
    body = {"parameter_id": "conditioning_tolerance", "horizon": "20", "tolerance": "5",
            "new_value": "1", "idempotency_key": f"key-{sid}-1"}
    first = C.post(f"/api/studio/session/{sid}/change", json=body)
    assert first.status_code == 200, first.text
    again = C.post(f"/api/studio/session/{sid}/change", json=body)
    assert again.status_code == 200, again.text
    assert again.json() == first.json(), "the retry produced a different answer"
    s = _acc(sid)
    assert s["k_exposed"] == "1" and s["changes_claim"] == "1", \
        f"a lost response inflated the accounting: {s}"


def t23_the_same_action_without_a_key_is_a_second_claim():
    """the contract is explicit: no key means not retryable, and that is honest"""
    sid = _new_session()
    body = {"parameter_id": "horizon", "horizon": "20", "tolerance": "5", "new_value": "40"}
    C.post(f"/api/studio/session/{sid}/change", json=body)
    C.post(f"/api/studio/session/{sid}/change", json=dict(body, horizon="40", new_value="60"))
    assert _acc(sid)["changes_claim"] == "2", _acc(sid)


def t24_a_key_reused_for_different_arguments_is_refused():
    sid = _new_session()
    k = f"key-{sid}-x"
    C.post(f"/api/studio/session/{sid}/change",
           json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5",
                 "new_value": "40", "idempotency_key": k})
    r = C.post(f"/api/studio/session/{sid}/change",
               json={"parameter_id": "horizon", "horizon": "40", "tolerance": "5",
                     "new_value": "60", "idempotency_key": k})
    assert r.status_code == 409, r.status_code
    assert r.json()["detail"]["error"] == "IdempotencyConflictError", r.json()


# ── confirmatory standing, the acceptance statement over HTTP ───────────────
def _validate(sid, boundary_hash=""):
    """Takes a session id. The window it is judged on came from the freeze, not from here."""
    return C.post(f"/api/studio/session/{sid}/validate",
                  json={"boundary_hash": boundary_hash}).json()


def _session_reading(start, end, family_id=""):
    """A session that declares which slice of data it reads, so its exposures have a footprint."""
    body = {"window_start": start, "window_end": end}
    if family_id:
        body["family_id"] = family_id
    return C.post("/api/studio/session/create", json=body).json()["session"]["session_id"]


def t25_seen_history_cannot_become_fresh_confirmatory_evidence():
    """explore a winner on 2024-2026, open a NEW session, register it, evaluate on 2024-2026

    The first version of this test asserted `status in (CONTAMINATED, INVALID_BOUNDARY)` and
    passed on INVALID_BOUNDARY, because the validation window it chose overlapped the declared
    development window. It was green and it proved nothing. The window here is disjoint from
    development, so CONTAMINATED is the only way to pass and the guard has to earn it.
    """
    explorer = _session_reading("2024-01-01", "2026-08-01")
    for v in ("40", "60"):
        C.post(f"/api/studio/session/{explorer}/change",
               json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5",
                     "new_value": v})
    fam = C.get(f"/api/studio/session/{explorer}/family").json()["family"]["family_id"]

    clean_looking = C.post("/api/studio/session/create",
                           json={"family_id": fam}).json()["session"]["session_id"]
    # the winner is preregistered against the very window that was already read
    assert _register(clean_looking, "2024-01-01", "2026-08-01").status_code == 200
    v = _validate(clean_looking)
    assert v["status"] == "CONTAMINATED", f"already-read data was not flagged: {v}"
    assert v["eligible"] is False, v
    assert "has been seen" in v["why"], v


def t26_a_new_family_over_the_same_data_is_still_contaminated():
    """'independent research' is a declaration, and it does not unsee anything"""
    explorer = _session_reading("2024-01-01", "2026-08-01")
    C.post(f"/api/studio/session/{explorer}/change",
           json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5",
                 "new_value": "40"})
    independent = _new_session()                       # its own family entirely
    _register(independent, "2024-01-01", "2026-08-01")
    v = _validate(independent)
    assert v["status"] == "CONTAMINATED", f"a new family_id laundered the exposure: {v}"
    assert v["eligible"] is False, v


def t27_forward_evidence_is_eligible():
    sid = _new_session()
    r = _register(sid)
    assert r.status_code == 200, r.text
    b = r.json()["boundary"]
    assert b["data_cutoff_at_registration"] and b["registered_at"], \
        "the two server-derived fields are empty; forwardness would be an assertion again"
    v = _validate(sid)
    assert v["status"] == "FORWARD" and v["eligible"] is True, v
    assert v["boundary_hash"] == b["boundary_hash"], "validated against a different boundary"


def t28_validation_uses_the_frozen_boundary_and_nothing_else():
    """the hole this milestone closed: the window can no longer be chosen after the answer"""
    sid = _new_session()
    _register(sid)
    r = C.post(f"/api/studio/session/{sid}/validate",
               json={"boundary_hash": "deadbeefdeadbeef"})
    assert r.status_code == 409, r.status_code
    d = r.json()["detail"]
    assert d["error"] == "EvidenceBoundaryDriftError", d
    assert "different study" in d["detail"], d


def t28b_registering_without_a_validation_window_is_refused():
    sid = _new_session()
    r = C.post(f"/api/studio/session/{sid}/register", json={})
    assert r.status_code == 422, f"a study froze with no declared evidence: {r.status_code}"


def t28c_a_declared_window_narrower_than_the_read_still_contaminates():
    """the access layer governs: what was touched, not what was announced"""
    over = _session_reading("2024-01-01", "2025-12-31")
    C.post(f"/api/studio/session/{over}/change",
           json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5",
                 "new_value": "40", "overreach_start": "2026-01-01",
                 "overreach_end": "2026-03-01"})
    later = _new_session()
    _register(later, "2026-01-01", "2026-06-30")
    v = _validate(later)
    assert v["status"] == "CONTAMINATED", f"the declaration was trusted over the footprint: {v}"
    assert v["overreaching_reads"] >= 1, v


def t28d_the_boundary_and_the_footprint_survive_a_restart():
    """a frozen boundary a restart can forget is not a boundary"""
    sid = _session_reading("2024-01-01", "2026-08-01")
    C.post(f"/api/studio/session/{sid}/change",
           json={"parameter_id": "horizon", "horizon": "20", "tolerance": "5",
                 "new_value": "40"})
    reg = _register(sid, "2026-09-01", "2026-12-31")
    assert reg.status_code == 409, "the explorer had already exposed a result"

    fresh_sid = _new_session()
    frozen = _register(fresh_sid).json()["boundary"]
    before = _validate(fresh_sid)

    restarted = TestClient(_app(), raise_server_exceptions=False)
    after = restarted.post(f"/api/studio/session/{fresh_sid}/validate", json={}).json()
    assert after["boundary_hash"] == frozen["boundary_hash"] == before["boundary_hash"], \
        f"the boundary changed across a restart: {before} -> {after}"
    assert after["status"] == before["status"], after

    # and the footprint the access layer recorded is still what contamination reads
    contaminated = _new_session()
    _register(contaminated, "2024-01-01", "2026-08-01")
    v = restarted.post(f"/api/studio/session/{contaminated}/validate", json={}).json()
    assert v["status"] == "CONTAMINATED", f"the footprint did not survive the restart: {v}"


def t28e_a_retried_registration_does_not_freeze_twice():
    """one action, one freeze, however many times the request is delivered"""
    sid = _new_session()
    body = {"validation_start": "2026-09-01", "validation_end": "2026-12-31",
            "idempotency_key": f"freeze-{sid}"}
    first = C.post(f"/api/studio/session/{sid}/register", json=body)
    assert first.status_code == 200, first.text
    again = C.post(f"/api/studio/session/{sid}/register", json=body)
    assert again.status_code == 200, again.text
    assert again.json() == first.json(), "the retry produced a different freeze"
    s = _acc(sid)
    assert s["mode"] == "REGISTERED", s
    assert s["events"] == first.json()["session"]["events"], \
        f"the retry appended events: {s['events']} vs {first.json()['session']['events']}"


def t28f_a_second_genuine_registration_is_refused_not_duplicated():
    """without a key, the state machine is what stops it — and it must, not the response log"""
    sid = _new_session()
    assert _register(sid).status_code == 200
    r = _register(sid, "2027-01-01", "2027-06-30")
    assert r.status_code == 409, f"a session froze twice: {r.status_code}"


# ── the parameter surface ───────────────────────────────────────────────────
def _param(sid, pid, val, **kw):
    body = {"parameter_id": pid, "new_value": val}
    body.update(kw)
    return C.post(f"/api/studio/session/{sid}/parameter", json=body)


def _param_preview(sid, pid, val):
    return C.post(f"/api/studio/session/{sid}/parameter/preview",
                  json={"parameter_id": pid, "new_value": val}).json()["plan"]


def t29_a_literal_route_is_not_swallowed_by_a_path_parameter():
    """GET /parameters answered 'no session parameters' until it was declared first

    FastAPI matches in declaration order, so `/{sid}` registered earlier consumes every literal
    that follows it. Nothing in the type system or the tests below would have noticed; the route
    simply resolved to the wrong handler and returned a plausible 404.
    """
    r = C.get("/api/studio/session/parameters")
    assert r.status_code == 200, f"{r.status_code}: {r.text[:120]}"
    d = r.json()
    assert len(d["parameters"]) == 22, len(d["parameters"])
    assert set(d["roles"]) == {"PRESENTATION_ONLY", "CLAIM_CHANGE", "DESIGN_CHANGE",
                               "SEARCH_SPACE_CHANGE", "POLICY_CHANGE"}, d["roles"]


def t30_every_parameter_of_a_role_costs_the_same_over_http():
    """the acceptance statement, exercised through routing rather than in-process"""
    spec = C.get("/api/studio/session/parameters").json()
    by_role = {}
    for p in spec["parameters"]:
        by_role.setdefault(p["semantic_role"], []).append(
            (p["multiplicity_effect"], p["registered_effect"], p["mutable_in_registered"]))
    for role, shapes in by_role.items():
        assert len(set(shapes)) == 1, f"{role} members disagree over the wire: {shapes}"


def t31_preview_and_commit_agree_over_the_wire():
    sid = _new_session()
    for pid, val in (("horizon", "40"), ("layout", "list"), ("selection_top_k", "37"),
                     ("equivalence_margin", "1.0"), ("support_cutoff", "250")):
        plan = _param_preview(sid, pid, val)
        committed = _param(sid, pid, val, plan_hash=plan["plan_hash"])
        assert committed.status_code == 200, committed.text
        c = committed.json()["classification"]
        for a, b in (("semantic_role", "role"), ("new_claim_hash", "new_claim_hash"),
                     ("multiplicity_effect", "multiplicity_effect"),
                     ("new_search_space_hash", "new_search_space_hash"),
                     ("registered_effect", "registered_effect")):
            assert plan[a] == c[b], (pid, a, plan[a], c[b])


def t32_a_cosmetic_knob_reaches_no_ledger_even_ten_times():
    sid = _new_session()
    before = _acc(sid)
    for i in range(10):
        r = _param(sid, "sort_by_displayed_column", f"col{i}")
        assert r.json()["recorded"] == "NO", r.json()
    after = _acc(sid)
    assert after["events"] == before["events"], \
        f"a view reached the ledger: {before['events']} -> {after['events']}"
    assert after["k_exposed"] == before["k_exposed"]


def t33_a_frozen_study_refuses_by_role_and_offers_the_fork():
    sid = _new_session()
    assert _register(sid).status_code == 200
    r = _param(sid, "horizon", "60")
    assert r.status_code == 409, r.status_code
    d = r.json()["detail"]
    assert d["error"] == "ParameterSurfaceError" and d["next_action"] == "FORK", d
    # and the cosmetic set stays live, because its role says it cannot change the answer
    ok = _param(sid, "layout", "grid")
    assert ok.status_code == 200 and ok.json()["recorded"] == "NO", ok.text


def t34_the_settings_survive_a_restart():
    """a knob whose value lives only in a variable is a knob a restart resets silently"""
    sid = _new_session()
    _param(sid, "horizon", "40")
    _param(sid, "selection_top_k", "37")
    before = _param_preview(sid, "horizon", "40")
    assert before["no_op"] == "YES", before

    SESS._SURFACES.clear()                       # the process restarts; the cache is gone
    after = _param_preview(sid, "horizon", "40")
    assert after["no_op"] == "YES", (
        f"the settings were replayed wrong after a restart, so turning the knob to the value it "
        f"already holds looked like a new claim: {after}")
    assert after["old_value"] == "40", after


# ── the plan between preview and commit ─────────────────────────────────────
def t35_a_preview_returns_a_plan_pinned_to_the_state_it_saw():
    sid = _new_session()
    plan = _param_preview(sid, "horizon", "40")
    assert plan["plan_hash"] and plan["prior_state_hash"], plan
    assert plan["parameter_registry_hash"], plan
    assert plan["prior_state_hash"] == _acc(sid)["state_hash"], plan


def t36_a_plan_approved_at_one_state_cannot_commit_at_another():
    """TOCTOU: the classifier is the same and the transition is not

    Nothing here is a race in the request; the gap is human. A person reads what a change will
    cost, thinks about it, and clicks — and in between, the session moved. Recomputing silently
    would apply a change nobody approved, so it is refused.
    """
    sid = _new_session()
    plan = _param_preview(sid, "horizon", "40")
    _param(sid, "conditioning_tolerance", "1")          # the session moves underneath
    r = _param(sid, "horizon", "40", plan_hash=plan["plan_hash"])
    assert r.status_code == 409, r.status_code
    d = r.json()["detail"]
    assert d["error"] == "StaleChangePlanError", d
    assert d["next_action"] == "REPREVIEW", d
    assert "not what would happen" in d["detail"], d
    # and nothing was applied
    assert _param_preview(sid, "horizon", "40")["old_value"] == "20", "the refusal still applied it"


def t37_a_plan_is_single_use():
    sid = _new_session()
    plan = _param_preview(sid, "selection_top_k", "37")
    assert _param(sid, "selection_top_k", "37", plan_hash=plan["plan_hash"]).status_code == 200
    again = _param(sid, "selection_top_k", "37", plan_hash=plan["plan_hash"])
    assert again.status_code == 409, "a plan committed twice"


def t38_a_plan_cannot_be_pointed_at_a_different_knob():
    sid = _new_session()
    plan = _param_preview(sid, "horizon", "40")
    r = _param(sid, "universe", "sp500", plan_hash=plan["plan_hash"])
    assert r.status_code == 409, "a plan approved for one parameter committed another"


def t39_the_session_parameter_list_carries_current_values_and_roles():
    """everything the UI needs to render 22 controls, and nothing it needs to classify them"""
    sid = _new_session()
    _param(sid, "horizon", "40")
    d = C.get(f"/api/studio/session/{sid}/parameters").json()
    assert len(d["parameters"]) == 22, len(d["parameters"])
    assert d["parameter_registry_hash"], d
    by_id = {p["parameter_id"]: p for p in d["parameters"]}
    assert by_id["horizon"]["current_value"] == "40", by_id["horizon"]
    for p in d["parameters"]:
        assert p["ui_kind"] in ("NUMBER", "ENUM", "MULTI", "BOOLEAN", "TEXT"), p
        assert p["group"] in d["groups"], p
        assert p["semantic_role"], p
        assert p["label"], p


# ── the neighbouring surface, same routing layer ────────────────────────────
def t12_semantics_screen_serves_and_carries_no_operand():
    r = C.get("/api/studio/semantics/n0")
    assert r.status_code == 200, r.text
    d = r.json()

    def nums(o, p=""):
        out = []
        if isinstance(o, dict):
            for k, v in o.items():
                out += nums(v, f"{p}.{k}")
        elif isinstance(o, list):
            for i, v in enumerate(o):
                out += nums(v, f"{p}[{i}]")
        elif isinstance(o, bool):
            pass
        elif isinstance(o, (int, float)):
            out.append((p, o))
        return out

    assert not nums(d["metrics"]), f"numeric leaves crossed the wire: {nums(d['metrics'])}"


def t13_the_blocked_comparison_is_blocked_at_the_http_layer_too():
    r = C.get("/api/studio/semantics/compare/n0.g1.fwer_search/n0.g2.fwer_search")
    assert r.status_code == 409, f"a cross-null delta was served with HTTP {r.status_code}"


print("=" * 100, flush=True)
print("  SESSION API — over the real ASGI routing layer", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_the_schema_can_be_built_at_all,
                        t2_no_post_binds_a_model_as_a_query_parameter,
                        t3_the_body_actually_binds,
                        t4_every_transported_field_is_a_string_in_the_json_itself,
                        t5_a_missing_body_is_still_refused,
                        t6_unknown_session_is_404_not_500,
                        t7_four_actions_over_http,
                        t8_multiplicity_never_equals_the_screen,
                        t9_revisiting_is_free_and_recorded_as_free,
                        t10_preview_costs_nothing,
                        t11_exploration_is_never_confirmatory_over_the_wire,
                        t14_register_then_freeze,
                        t15_register_after_seeing_is_refused_with_a_sentence,
                        t16_a_refused_registration_leaves_no_trace,
                        t17_a_frozen_study_refuses_mutation_and_offers_the_fork,
                        t18_the_fork_is_a_new_session_carrying_the_lineage,
                        t19_a_fork_cannot_launder_the_counter_over_http,
                        t20_an_anonymous_fork_is_refused,
                        t20b_a_fork_of_a_clean_parent_still_cannot_preregister,
                        t21_a_registration_survives_a_process_restart,
                        t22_a_retried_change_is_not_a_second_claim,
                        t23_the_same_action_without_a_key_is_a_second_claim,
                        t24_a_key_reused_for_different_arguments_is_refused,
                        t25_seen_history_cannot_become_fresh_confirmatory_evidence,
                        t26_a_new_family_over_the_same_data_is_still_contaminated,
                        t27_forward_evidence_is_eligible,
                        t28_validation_uses_the_frozen_boundary_and_nothing_else,
                        t28b_registering_without_a_validation_window_is_refused,
                        t28c_a_declared_window_narrower_than_the_read_still_contaminates,
                        t28d_the_boundary_and_the_footprint_survive_a_restart,
                        t28e_a_retried_registration_does_not_freeze_twice,
                        t28f_a_second_genuine_registration_is_refused_not_duplicated,
                        t29_a_literal_route_is_not_swallowed_by_a_path_parameter,
                        t30_every_parameter_of_a_role_costs_the_same_over_http,
                        t31_preview_and_commit_agree_over_the_wire,
                        t32_a_cosmetic_knob_reaches_no_ledger_even_ten_times,
                        t33_a_frozen_study_refuses_by_role_and_offers_the_fork,
                        t34_the_settings_survive_a_restart,
                        t35_a_preview_returns_a_plan_pinned_to_the_state_it_saw,
                        t36_a_plan_approved_at_one_state_cannot_commit_at_another,
                        t37_a_plan_is_single_use,
                        t38_a_plan_cannot_be_pointed_at_a_different_knob,
                        t39_the_session_parameter_list_carries_current_values_and_roles,
                        t12_semantics_screen_serves_and_carries_no_operand,
                        t13_the_blocked_comparison_is_blocked_at_the_http_layer_too], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
