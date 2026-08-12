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
                        t12_semantics_screen_serves_and_carries_no_operand,
                        t13_the_blocked_comparison_is_blocked_at_the_http_layer_too], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
