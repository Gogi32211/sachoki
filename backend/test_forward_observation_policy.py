"""ForwardObservationPolicy v1 · the last freedom, closed.

Every statistical object above this one is frozen, and all of them together are undone by running
3C nightly and stopping when it looks good. These tests are about the looking.

The look ledger is real and append-only, so the tests get their own — a test run must not consume
the one registered look that exists.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import forward_evaluation as FE                                       # noqa: E402
import forward_observation_policy as OP                               # noqa: E402
import forward_v2_adapter as AD                                       # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OP.LOOK_LEDGER = os.path.join(HERE, ".test_look_ledger.json")
if os.path.exists(OP.LOOK_LEDGER):
    os.remove(OP.LOOK_LEDGER)

ok = fail = 0
CUTOFF = FE.record()["data_cutoff_at_registration"]
NEED = OP.FIRST_LOOK_TRADING_DAYS


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                            # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1
    finally:
        if os.path.exists(OP.LOOK_LEDGER):
            os.remove(OP.LOOK_LEDGER)


def day(offset: int) -> str:
    return str(np.datetime64(CUTOFF) + np.timedelta64(offset, "D"))


def novel_world(n_days, per_day=24, seed=7, effect=1.5):
    """Novel rows only, in the same six-family shape the adapter fixtures use."""
    rng = np.random.default_rng(seed)
    rows = []
    for k in range(1, n_days + 1):
        for j in range(per_day):
            member = (j // 6) % 2 == 0
            rows.append({"date": day(k), "family": f"fam{j % 6}", "in_cell": member,
                         "y": float(rng.normal(effect if member else 0.0, 3.0))})
    df = pd.DataFrame(rows)
    return (df[["family"]].copy(), df["date"].to_numpy(), df["y"].to_numpy(float),
            {"cellA": df["in_cell"].to_numpy(bool),
             "cellB": (~df["in_cell"].to_numpy(bool)) | (np.arange(len(df)) % 7 == 0)})


# ── 1 ───────────────────────────────────────────────────────────────────────
def t1_the_policy_is_frozen_and_bound_to_every_other_frozen_object():
    p = OP.assert_bound()
    assert p["policy_version"] == "forward_observation_policy_v1"
    b = p["binds_to"]
    assert b["forward_evaluation_spec_hash"] == FE.record()["spec_hash"]
    assert b["forward_adapter_hash"] == AD.record()["adapter_hash"]
    assert b["ranking_policy_hash"] == "2aef967dc92786ce"
    assert p["repeated_looks"] == "FORBIDDEN_IN_V1"
    assert p["first_look"]["trigger_is_outcome_independent"] is True


# ── 2 ───────────────────────────────────────────────────────────────────────
def t2_a_look_before_the_trigger_is_refused():
    """the nightly run is the whole problem"""
    _, dates, _, _ = novel_world(NEED - 1)
    try:
        OP.assert_look_permitted(dates)
    except OP.PrematureLookError as e:
        assert f"{NEED - 1} of {NEED}" in str(e), str(e)
        return
    raise AssertionError("a look was permitted before its registered trigger")


# ── 3 ───────────────────────────────────────────────────────────────────────
def t3_the_trigger_reads_no_outcome():
    """a trigger that reads theta is optional stopping with a schedule"""
    p = OP.record()
    for forbidden in ("theta", "interval", "verdict", "build", "attractive"):
        assert any(forbidden in t.lower() for t in p["first_look"]["forbidden_triggers"]), \
            forbidden
    # the same number of days with wildly different outcomes gives the same permission
    for effect in (-50.0, 0.0, 50.0):
        _, dates, _, _ = novel_world(NEED, effect=effect)
        g = OP.assert_look_permitted(dates)
        assert g["novel_trading_days"] == NEED, g
    import inspect                                                    # noqa: PLC0415
    src = inspect.getsource(OP.assert_look_permitted) + inspect.getsource(OP.novel_trading_days)
    for operand in ("theta", "ci_", "verdict", "interval", " y ", "outcome_value"):
        assert operand not in src, f"the look gate reads {operand!r}"


# ── 4 · the one that removes the incentive ──────────────────────────────────
def t4_waiting_longer_adds_no_rows():
    """delaying the look must gain nothing, or discipline is required at its most expensive"""
    _, d30, _, _ = novel_world(NEED)
    _, d90, _, _ = novel_world(90)
    assert OP.look_window(d30) == OP.look_window(d90)[:NEED]
    assert len(OP.look_window(d90)) == NEED, len(OP.look_window(d90))
    assert OP.look_population(d90).size == NEED * 24, OP.look_population(d90).size

    O, dates, y, masks = novel_world(90)
    art = AD.evaluate(O=O, dates=dates, y=y, masks=masks, cutoff=CUTOFF,
                      purpose=AD.FIRST_PROSPECTIVE_EVALUATION,
                      forward_spec_hash=FE.record()["spec_hash"],
                      adapter_hash=AD.record()["adapter_hash"])
    assert art["forward_rows"] == NEED * 24, art["forward_rows"]
    assert art["forward_window"][1] == day(NEED), art["forward_window"]


# ── 5 ───────────────────────────────────────────────────────────────────────
def t5_the_late_look_and_the_punctual_one_produce_the_same_artifact():
    """which is what makes the result checkable by someone who was not there"""
    def take(n_days):
        O, dates, y, masks = novel_world(n_days)
        return AD.evaluate(O=O, dates=dates, y=y, masks=masks, cutoff=CUTOFF,
                           purpose=AD.FIRST_PROSPECTIVE_EVALUATION,
                           forward_spec_hash=FE.record()["spec_hash"],
                           adapter_hash=AD.record()["adapter_hash"])
    punctual, late = take(NEED), take(120)
    assert punctual["artifact_hash"] == late["artifact_hash"], (
        "running the look 90 days late produced a different artifact, so delay is a lever")


# ── 6 ───────────────────────────────────────────────────────────────────────
def t6_a_second_look_is_refused():
    O, dates, y, masks = novel_world(NEED)
    art = OP.run_first_prospective_look(O=O, dates=dates, y=y, masks=masks,
                                        taken_at="2026-09-20T00:00:00Z")
    assert art["look_index"] == 1
    assert len(OP.looks_taken()) == 1
    try:
        OP.run_first_prospective_look(O=O, dates=dates, y=y, masks=masks,
                                      taken_at="2026-09-21T00:00:00Z")
    except OP.RepeatedLookError as e:
        assert "registered one" in str(e)
        return
    raise AssertionError("a second look was taken under a one-look policy")


# ── 7 ───────────────────────────────────────────────────────────────────────
def t7_the_operational_status_can_count_days_and_nothing_else():
    """counting the days is not peeking; seeing a theta move is"""
    _, dates, _, _ = novel_world(7)
    s = OP.operational_status(dates)
    assert s["state"] == OP.WAITING, s["state"]
    assert s["novel_trading_days"] == 7
    assert s["novel_trading_days_remaining"] == NEED - 7
    OP.assert_no_outcome_fields(s)
    for forbidden in OP.FORBIDDEN_IN_OPERATIONAL_STATUS:
        assert forbidden not in s, forbidden

    # and the guard is not decorative
    try:
        OP.assert_no_outcome_fields(dict(s, theta=1.23))
    except OP.OutcomeLeakError as e:
        assert "whatever the screen calls it" in str(e)
        return
    raise AssertionError("an estimate passed the operational-status guard")


# ── 8 ───────────────────────────────────────────────────────────────────────
def t8_the_state_machine_is_waiting_then_ready_then_taken():
    _, few, _, _ = novel_world(3)
    _, enough, _, _ = novel_world(NEED)
    assert OP.operational_status(few)["state"] == OP.WAITING
    assert OP.operational_status(enough)["state"] == OP.READY
    O, dates, y, masks = novel_world(NEED)
    OP.run_first_prospective_look(O=O, dates=dates, y=y, masks=masks, taken_at="x")
    assert OP.operational_status(enough)["state"] == OP.CONSUMED
    assert OP.operational_status(few)["state"] == OP.CONSUMED, (
        "a taken look must stay taken even when the day count is read differently")


# ── 9 ───────────────────────────────────────────────────────────────────────
def t9_the_policy_cannot_be_edited_after_freezing():
    p = OP.record()
    edited = dict(p)
    edited["first_look"] = dict(p["first_look"], n_novel_trading_days=5)
    edited["policy_hash"] = OP._h({k: v for k, v in edited.items() if k != "policy_hash"})
    try:
        OP.freeze(edited)
    except OP.ForwardObservationPolicyError as e:
        assert "last freedom" in str(e)
        return
    raise AssertionError("the frozen observation policy accepted a new trigger")


# ── 10 ──────────────────────────────────────────────────────────────────────
def t10_no_look_has_been_taken_on_real_data():
    """the whole point of this commit is to be waiting, correctly"""
    real = os.path.join(HERE, "FORWARD_LOOK_LEDGER.json")
    assert not os.path.exists(real), (
        "a forward look has been recorded; the first prospective evaluation happened before this "
        "policy was meant to be waiting")
    assert OP.operational_status()["state"] == OP.WAITING


TESTS = [t1_the_policy_is_frozen_and_bound_to_every_other_frozen_object,
         t2_a_look_before_the_trigger_is_refused,
         t3_the_trigger_reads_no_outcome,
         t4_waiting_longer_adds_no_rows,
         t5_the_late_look_and_the_punctual_one_produce_the_same_artifact,
         t6_a_second_look_is_refused,
         t7_the_operational_status_can_count_days_and_nothing_else,
         t8_the_state_machine_is_waiting_then_ready_then_taken,
         t9_the_policy_cannot_be_edited_after_freezing,
         t10_no_look_has_been_taken_on_real_data]

print("=" * 100, flush=True)
print("  FORWARD OBSERVATION POLICY · everything above is frozen; this is the looking", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate(TESTS, 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
