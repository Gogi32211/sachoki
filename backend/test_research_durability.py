"""Durability tested through damage, not through the happy path.

A store that works when nothing goes wrong proves nothing about a promise. Every test here
breaks something on purpose: kills the process between events, tears a write in half, replays a
lost request, corrupts the middle of the file. The acceptance statement:

    A registered claim survives a restart intact, a retried request does not become a second
    claim, and a history that cannot be verified makes the session INVALID rather than smaller.

The last clause is the one worth defending. Reconstructing "as much as the file still supports"
would silently drop exposures, and dropping exposures always moves `k` down — the direction that
makes a finding look better than it is. Damage must fail loudly upward, never quietly downward.
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import research_store as RS                                          # noqa: E402
from research_family import ResearchFamily                           # noqa: E402
from data_access import CATALOG, DataAccessLayer, DataAccessSpec      # noqa: E402

CATALOG.register("bars_1d", lambda: ("snap-durability", "2026-08-11"))

ok = fail = 0
TMP = tempfile.mkdtemp(prefix="research_store_")


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


_SPEC = DataAccessSpec(source_id="bars_1d", universe="russell", start="2021-01-01",
                       end="2023-12-31", temporal_resolution="1d")
_L = DataAccessLayer(_SPEC, CATALOG)
_L.record(_SPEC.start, _SPEC.end, dates=1)
WIN = _L.footprint().as_dict()


def build(L, session="s1", family="F1", n=3, start_id=0, prior=""):
    """A small session: created, then n exposures, chained properly."""
    h = prior
    eid = start_id
    if start_id == 0:
        L.append(session, family, "SESSION_CREATED", event_id=0, prior_state_hash="",
                 new_state_hash="h0")
        h, eid = "h0", 1
    for i in range(n):
        L.append(session, family, "RESULT_EXPOSED", event_id=eid, prior_state_hash=h,
                 new_state_hash=f"h{eid}", claim_hash=f"c{i}", payload={"window": WIN})
        h, eid = f"h{eid}", eid + 1
    return h, eid


# ── restart ─────────────────────────────────────────────────────────────────
def t1_a_registration_survives_a_restart():
    """the reason the store exists: REGISTERED is a promise, not a note in RAM"""
    L = ledger("restart")
    h, eid = build(L, n=2)
    L.append("s1", "F1", "SESSION_FROZEN", event_id=eid, prior_state_hash=h,
             new_state_hash="frozen", payload={"space_id": "combolab_v2", "size": 31,
                                               "hash": "3600ae3dd52a25e6"})
    before = [(e.event_type, e.new_state_hash) for e in L.read_session("s1")]

    del L                                        # the process dies here
    L2 = RS.DurableLedger(os.path.join(TMP, "restart.jsonl"))
    after = [(e.event_type, e.new_state_hash) for e in L2.read_session("s1")]
    assert before == after, f"state changed across a restart:\n{before}\n{after}"
    assert any(t == "SESSION_FROZEN" for t, _ in after), "the registration was forgotten"


def t2_the_exposure_count_survives_a_restart():
    L = ledger("restart2")
    build(L, n=5)
    L2 = RS.DurableLedger(os.path.join(TMP, "restart2.jsonl"))
    fam = ResearchFamily("F1", L2.read_all()).accounting()
    assert fam.k_family_exposed == 5, fam


# ── the lost response ───────────────────────────────────────────────────────
def t3_a_retried_request_is_not_a_second_claim():
    """server wrote the event, the response never arrived, the browser sent it again"""
    L = ledger("retry")
    build(L, n=1)
    key, rh = "client-uuid-1", RS.request_hash("s1", "change", {"tolerance": "1"})
    a = L.append("s1", "F1", "CONDITION_CHANGED", event_id=2, prior_state_hash="h1",
                 new_state_hash="h2", idempotency_key=key, request_hash=rh)
    n_after_first = len(L.read_all())
    b = L.append("s1", "F1", "CONDITION_CHANGED", event_id=2, prior_state_hash="h1",
                 new_state_hash="h2", idempotency_key=key, request_hash=rh)
    assert b.seq == a.seq, "the retry produced a different event"
    assert len(L.read_all()) == n_after_first, "the retry was appended"


def t4_a_reused_key_for_a_different_request_is_refused():
    """the opposite failure: a stale key hiding a real change behind an old one"""
    L = ledger("retry2")
    build(L, n=1)
    key = "client-uuid-2"
    L.append("s1", "F1", "CONDITION_CHANGED", event_id=2, prior_state_hash="h1",
             new_state_hash="h2", idempotency_key=key,
             request_hash=RS.request_hash("s1", "change", {"tolerance": "1"}))
    try:
        L.append("s1", "F1", "CONDITION_CHANGED", event_id=3, prior_state_hash="h2",
                 new_state_hash="h3", idempotency_key=key,
                 request_hash=RS.request_hash("s1", "change", {"tolerance": "2"}))
    except RS.IdempotencyConflictError as e:
        assert "hide a real change" in str(e)
        return
    raise AssertionError("one key authorised two different requests")


def t5_deliberately_repeating_an_action_is_still_recorded():
    """a payload-derived key would have erased this: same values, new intention"""
    L = ledger("retry3")
    build(L, n=1)
    same = RS.request_hash("s1", "revisit", {"tolerance": "5"})
    L.append("s1", "F1", "CLAIM_REVISITED", event_id=2, prior_state_hash="h1",
             new_state_hash="h2", idempotency_key="click-1", request_hash=same)
    L.append("s1", "F1", "CLAIM_REVISITED", event_id=3, prior_state_hash="h2",
             new_state_hash="h3", idempotency_key="click-2", request_hash=same)
    revisits = [e for e in L.read_session("s1") if e.event_type == "CLAIM_REVISITED"]
    assert len(revisits) == 2, f"the second deliberate revisit vanished: {len(revisits)}"


# ── the interrupted append ──────────────────────────────────────────────────
def t6_a_torn_last_line_is_the_state_before_it():
    """crash mid-write: either the old state or the complete new one, never half"""
    path = os.path.join(TMP, "torn.jsonl")
    L = RS.DurableLedger(path)
    build(L, n=3)
    good = len(L.read_all())

    with open(path, "r") as f:
        blob = f.read()
    with open(path, "a") as f:                   # a write that stopped in the middle
        f.write(blob.splitlines()[-1][:37])

    L2 = RS.DurableLedger(path)
    st, _ = L2.status()
    assert st == RS.TORN_TAIL, st
    assert len(L2.read_all()) == good, "a half-written event was counted"


def t7_damage_in_the_middle_is_not_survivable():
    """the same corruption, one line earlier, must NOT be repaired into a smaller history"""
    path = os.path.join(TMP, "mid.jsonl")
    L = RS.DurableLedger(path)
    build(L, n=4)
    lines = open(path).read().splitlines()
    lines[2] = lines[2][:40]                     # break an event that is not the last
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")

    L2 = RS.DurableLedger(path)
    st, _ = L2.status()
    assert st == RS.CORRUPT, st
    try:
        L2.read_all()
    except RS.LedgerCorruptError as e:
        assert "INVALID" in str(e) and "undercount" in str(e)
        return
    raise AssertionError("a corrupt ledger was read as a shorter but valid history")


def t8_a_rewritten_event_breaks_the_chain():
    """append-only means the past cannot be edited, and editing it must be visible"""
    path = os.path.join(TMP, "edit.jsonl")
    L = RS.DurableLedger(path)
    build(L, n=4)
    lines = open(path).read().splitlines()
    del lines[2]                                 # quietly remove one exposure
    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")

    st, _ = RS.DurableLedger(path).status()
    assert st == RS.CORRUPT, f"an exposure was deleted and the ledger still verified: {st}"


def t9_an_append_onto_a_stale_head_is_refused():
    """two writers, or a lost event — either way the chain must not silently branch"""
    L = ledger("stale")
    h, eid = build(L, n=2)
    try:
        L.append("s1", "F1", "RESULT_EXPOSED", event_id=eid, prior_state_hash="h0",
                 new_state_hash="other", claim_hash="x")
    except RS.ChainBreakError as e:
        assert "persisted head" in str(e)
        return
    raise AssertionError("an event was appended onto a state that is no longer the head")


def t10_event_ids_are_dense_and_ordered():
    L = ledger("ids")
    h, eid = build(L, n=3)
    try:
        L.append("s1", "F1", "RESULT_EXPOSED", event_id=eid + 5, prior_state_hash=h,
                 new_state_hash="z")
    except RS.ChainBreakError:
        return
    raise AssertionError("a gap was accepted in the event sequence")


# ── the family, over a durable history ──────────────────────────────────────
def t11_family_multiplicity_is_a_union_not_a_sum():
    """the same specification opened in two sessions is one claim, not two"""
    L = ledger("family")
    L.append("s1", "F9", "SESSION_CREATED", event_id=0, prior_state_hash="", new_state_hash="a0")
    L.append("s1", "F9", "RESULT_EXPOSED", event_id=1, prior_state_hash="a0",
             new_state_hash="a1", claim_hash="SHARED", payload={"window": WIN})
    L.append("s2", "F9", "SESSION_CREATED", event_id=0, prior_state_hash="", new_state_hash="b0")
    L.append("s2", "F9", "RESULT_EXPOSED", event_id=1, prior_state_hash="b0",
             new_state_hash="b1", claim_hash="SHARED", payload={"window": WIN})
    L.append("s2", "F9", "RESULT_EXPOSED", event_id=2, prior_state_hash="b1",
             new_state_hash="b2", claim_hash="OTHER", payload={"window": WIN})

    acc = ResearchFamily("F9", L.read_all()).accounting()
    assert acc.k_family_exposed == 2, f"summed instead of unioned: {acc}"
    assert set(acc.session_ids) == {"s1", "s2"}, acc


def t12_one_space_searched_twice_is_counted_once():
    L = ledger("family2")
    for i, sid in enumerate(("s1", "s2")):
        L.append(sid, "F8", "SESSION_CREATED", event_id=0, prior_state_hash="",
                 new_state_hash=f"{sid}0")
        L.append(sid, "F8", "SEARCH_RUN", event_id=1, prior_state_hash=f"{sid}0",
                 new_state_hash=f"{sid}1",
                 payload={"space_id": "combolab_v2", "space_hash": "3600ae3dd52a25e6",
                          "space_size": 31, "displayed": 5, "window": WIN})
    acc = ResearchFamily("F8", L.read_all()).accounting()
    assert acc.k_family_selectable == 31, f"the same 31 claims were counted twice: {acc}"
    assert acc.k_family_selectable_is_bound is False, acc


def t13_two_different_spaces_are_reported_as_a_bound():
    """summing distinct spaces can overstate and never understate — and it says which it is"""
    L = ledger("family3")
    L.append("s1", "F7", "SESSION_CREATED", event_id=0, prior_state_hash="", new_state_hash="q0")
    L.append("s1", "F7", "SEARCH_RUN", event_id=1, prior_state_hash="q0", new_state_hash="q1",
             payload={"space_id": "combolab_v2", "space_hash": "AAA", "space_size": 31,
                      "displayed": 5, "window": WIN})
    L.append("s1", "F7", "SEARCH_RUN", event_id=2, prior_state_hash="q1", new_state_hash="q2",
             payload={"space_id": "combolab_v3", "space_hash": "BBB", "space_size": 12,
                      "displayed": 5, "window": WIN})
    acc = ResearchFamily("F7", L.read_all()).accounting()
    assert acc.k_family_selectable == 43, acc
    assert acc.k_family_selectable_is_bound is True, \
        "an upper bound was presented as an exact count"


print("=" * 100, flush=True)
print("  DURABLE LEDGER — tested through damage", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_a_registration_survives_a_restart,
                        t2_the_exposure_count_survives_a_restart,
                        t3_a_retried_request_is_not_a_second_claim,
                        t4_a_reused_key_for_a_different_request_is_refused,
                        t5_deliberately_repeating_an_action_is_still_recorded,
                        t6_a_torn_last_line_is_the_state_before_it,
                        t7_damage_in_the_middle_is_not_survivable,
                        t8_a_rewritten_event_breaks_the_chain,
                        t9_an_append_onto_a_stale_head_is_refused,
                        t10_event_ids_are_dense_and_ordered,
                        t11_family_multiplicity_is_a_union_not_a_sum,
                        t12_one_space_searched_twice_is_counted_once,
                        t13_two_different_spaces_are_reported_as_a_bound], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
shutil.rmtree(TMP, ignore_errors=True)
sys.exit(1 if fail else 0)
