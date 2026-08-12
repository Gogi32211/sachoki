"""The four-action acceptance fixture, run against the transport, not the domain.

If the screen shows k_selectable = 5 because it received five cards, that is a fatal UI
accounting defect. The transport is where it must already be impossible.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import studio_session_api as S                                       # noqa: E402

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


def numeric(o, p=""):
    if isinstance(o, dict):
        return [x for k, v in o.items() for x in numeric(v, f"{p}.{k}")]
    if isinstance(o, list):
        return [x for i, v in enumerate(o) for x in numeric(v, f"{p}[{i}]")]
    if isinstance(o, bool):
        return []
    if isinstance(o, (int, float)):
        return [f"{p}={o}"]
    return []


sid = S.create()["session"]["session_id"]
STATE = {"horizon": "20", "tolerance": "5"}


def t1_start():
    """a fresh EXPLORE session accounts for nothing yet"""
    v = S.accounting(sid)["session"]
    assert v["mode"] == "EXPLORE" and v["k_exposed"] == "0"
    assert v["confirmatory_eligible"] == "NO"
    assert not numeric(v), f"numeric leaves in the session view: {numeric(v)}"


def t2_preview_precedes_the_result():
    """the backend classifies before anything is run"""
    p = S.preview(sid, "conditioning_tolerance", STATE["horizon"], STATE["tolerance"], "1")
    assert p["change_type"] == "CLAIM_CHANGE"
    assert p["multiplicity_effect"] == "NEW_SELECTABLE_CLAIM"
    assert p["old_claim_hash"] != p["new_claim_hash"]


def t3_action1_tolerance():
    """ACTION 1 · ±5 → ±1 is a new claim and k_exposed rises"""
    r = S.change_and_run(sid, "conditioning_tolerance", STATE["horizon"], STATE["tolerance"], "1")
    STATE["tolerance"] = r["tolerance"]
    v = r["session"]
    assert r["change_type"] == "CLAIM_CHANGE"
    assert v["k_exposed"] == "1" and v["changes_claim"] == "1", v


def t4_action2_revisit_is_free():
    """ACTION 2 · reopening the same result costs a revisit, not a claim"""
    r = S.revisit(sid, STATE["horizon"], STATE["tolerance"])
    v = r["session"]
    assert v["k_exposed"] == "1", f"a revisit was charged as a new claim: {v}"
    assert v["revisits"] == "1", v


def t5_action3_search_space_not_screen():
    """ACTION 3 · ranked 31, rendered 5 → the view says 31"""
    v = S.accounting(sid)["session"]
    assert v["k_selectable"] == "31", f"multiplicity followed the screen: {v}"
    assert v["displayed_at_most"] == "5", v


def t6_action4_horizon():
    """ACTION 4 · horizon 20 → 40 moves the accounting again"""
    r = S.change_and_run(sid, "horizon", STATE["horizon"], STATE["tolerance"], "40")
    STATE["horizon"] = r["horizon"]
    v = r["session"]
    assert v["k_exposed"] == "2" and v["changes_claim"] == "2", v


def t7_no_ledger_crosses():
    """the browser never receives the event stream"""
    v = S.accounting(sid)["session"]
    for forbidden in ("events_list", "ledger", "event", "claims", "history"):
        assert forbidden not in v, f"{forbidden} crossed the wire"
    assert isinstance(v["events"], str)


def t8_every_field_is_a_string():
    v = S.accounting(sid)["session"]
    bad = {k: type(x).__name__ for k, x in v.items() if not isinstance(x, str)}
    assert not bad, bad


def t9_state_hash_moves():
    """the session view carries a hash that changes when the session does"""
    a = S.accounting(sid)["session"]["state_hash"]
    S.revisit(sid, STATE["horizon"], STATE["tolerance"])
    b = S.accounting(sid)["session"]["state_hash"]
    assert a != b


print("=" * 100, flush=True)
print("  COMBO LAB VERTICAL SLICE — four actions through the transport", flush=True)
print("=" * 100, flush=True)
for fn in (t1_start, t2_preview_precedes_the_result, t3_action1_tolerance,
           t4_action2_revisit_is_free, t5_action3_search_space_not_screen,
           t6_action4_horizon, t7_no_ledger_crosses, t8_every_field_is_a_string,
           t9_state_hash_moves):
    check((fn.__doc__ or fn.__name__).splitlines()[0], fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
if not fail:
    print("\n  session view as the browser receives it:", flush=True)
    for k, v in S.accounting(sid)["session"].items():
        print(f"    {k:<24s} {v!r}", flush=True)
sys.exit(1 if fail else 0)
