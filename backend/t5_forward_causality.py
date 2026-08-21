"""G6 — decision-time causality, split into what history CAN prove and what forward MUST enforce.

STATUS: DRAFT. This decides whether T5_FORWARD_SESSION_LENGTH_V1 is freezable at all. It is a
precondition for writing that artifact, not a section inside it.

WHY THE OBVIOUS ASSERTION IS TOO WEAK

    decision_deadline_ts >= last_bar_ts        proves only that the SESSION CLOSED

It does not prove the session LENGTH was known. Under the state machine the length becomes
known at SESSION_LENGTH_COMMITTED — SOURCE_FINAL plus settle window plus digest stability
later. A session can close at 16:00 and still have no committed length the next morning if
ingest ran late.

THE SPLIT, BECAUSE ONE WORD 'CAUSALITY' WOULD OVERCLAIM

History cannot test vendor ingest latency: the timestamps at which data ARRIVED were never
recorded. Only market time is recoverable. So the claim is split and neither half borrows the
other's strength:

    G6A  HISTORICAL_CAUSAL_HEADROOM      market-time feasibility, counterfactual commit
         counterfactual_commit_ts = last_bar_ts + SETTLE_HOURS
         ASSERT counterfactual_commit_ts < decision_deadline_ts

    G6B  ACTUAL_FORWARD_CAUSALITY        enforced at runtime, real commit timestamps
         ASSERT last_bar_ts <= actual_commit_ts
         ASSERT actual_commit_ts < decision_deadline_ts

G6A answers "was the market-time gap ever sufficient?". It does NOT answer "did the vendor
deliver in time?", because that question has no historical data. G6B answers it forward, on
observed timestamps, or the episode is held.

ENTRY IS NOT DECISION

    entry_ts             = next_session_open
    decision_deadline_ts = next_session_open, as an EXCLUSIVE causal upper bound

The strict inequality is the point. Saying the decision happens AT the open would let the
opening print inform it. Saying commit must land strictly BEFORE the open says only this: every
input must already be in hand when the next session opens. This adds no arbitrary hour — no
08:00, no 20:00 — to a specification that never contained one; it reads the existing entry
semantics as a bound. Other cutoffs are printed as DIAGNOSTICS so that none of them can be
promoted to the binding gate later.

OPENING-PRINT EXCLUSION IS CHECKED, NOT PROMISED

'The opening print may not inform the decision' is operationalised: every session any rule
reads must be strictly earlier than the entry session. Asserted per episode.

SETTLE_HOURS IS AN OPERATIONAL CONSTANT

It parameterises a finality PROXY, not the rule, and is draft-frozen at 6h BEFORE the headroom
distribution was computed. Headroom is diagnostics, never a tuning target: if these numbers
came out tight, the correct response is to say so, not to shrink the window.
"""
from __future__ import annotations
import json, os, sys                                                   # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_dna as D                                                     # noqa: E402

SETTLE_HOURS = 6                     # draft-frozen BEFORE any headroom number was seen
DIAG_CUTOFFS = {"08_00_ET_entry_day": ("ENTRY", 8), "20_00_ET_t5_day": ("T5", 20)}


class NotFreezable(RuntimeError):
    pass


class CausalityViolation(RuntimeError):
    pass


# ══════════════════════════════════════════════════════════════════════════
# G6B — the forward runtime guard. Frozen here, with tests, before any forward X.
# ══════════════════════════════════════════════════════════════════════════
def assert_forward_causal(last_bar_ts, actual_commit_ts, decision_deadline_ts, episode_id=""):
    """Called per episode at ingest, on OBSERVED timestamps. Raises, never warns.

    Strict '<' against the deadline: equality means the length landed exactly as the session
    opened, which is not 'in hand before the open'."""
    if actual_commit_ts < last_bar_ts:
        raise CausalityViolation(
            f"{episode_id}: commit {actual_commit_ts} precedes the last required bar "
            f"{last_bar_ts} — a negative settle window, which is a clock or pipeline fault.")
    if not (actual_commit_ts < decision_deadline_ts):
        raise CausalityViolation(
            f"{episode_id}: SESSION_LENGTH_LATE_FOR_DECISION — commit {actual_commit_ts} is not "
            f"strictly before the deadline {decision_deadline_ts}. The episode is HELD: not "
            f"evaluated, not counted toward the 20,000, reported in the DQ ledger. It is NOT "
            f"backfilled, because evaluating it with a length that arrived after the decision "
            f"is the retrospection this protocol exists to prevent.")
    return True


def test_forward_guard():
    """G6B is frozen with tests rather than with intentions."""
    t = pd.Timestamp("2026-08-21 16:00")
    ok = assert_forward_causal(t, t + pd.Timedelta(hours=6), t + pd.Timedelta(hours=17.5))
    checks = {"passes_when_early": ok}
    for name, commit, dead in (
            ("rejects_late", t + pd.Timedelta(hours=18), t + pd.Timedelta(hours=17.5)),
            ("rejects_equal", t + pd.Timedelta(hours=17.5), t + pd.Timedelta(hours=17.5)),
            ("rejects_negative_settle", t - pd.Timedelta(hours=1),
             t + pd.Timedelta(hours=17.5))):
        try:
            assert_forward_causal(t, commit, dead)
            checks[name] = False
        except CausalityViolation:
            checks[name] = True
    return checks


# ══════════════════════════════════════════════════════════════════════════
def to_et(M):
    """ts_et is stored as a naive 'YYYY-MM-DD HH:MM' string in exchange local time. Parsed once
    here: every timestamp in this gate lives in that single convention, so no cross-zone
    arithmetic is performed and none is implied."""
    M = M.copy()
    M["ts_et"] = pd.to_datetime(M["ts_et"])
    return M


def session_opens(M):
    """Observed first bar per session. Data-derived rather than hardcoded to 09:30, so a late
    open is handled by observation."""
    return M.groupby("session_date")["ts_et"].min()


def decision_deadlines(E, opens, cal):
    """decision_deadline_ts := the observed open of the first trading session strictly after
    t5_date, as an EXCLUSIVE bound. Deterministic, calendar-derived, outcome-blind, and it
    introduces no timestamp the specification did not already contain."""
    idx = {d: i for i, d in enumerate(cal)}
    nxt = {d: cal[idx[d] + 1] for d in cal if idx[d] + 1 < len(cal)}
    ent = E.t5_date.map(nxt)
    return pd.Series(ent.map(opens.to_dict()).to_numpy(), index=E.episode_id), \
        pd.Series(ent.to_numpy(), index=E.episode_id)


def gate_opening_print(M, entry_session):
    """Every session a rule reads must be strictly earlier than the entry session."""
    latest = M.groupby("episode_id")["session_date"].max().rename("latest_read")
    J = pd.DataFrame({"latest_read": latest}).join(
        entry_session.rename("entry_session"), how="inner").dropna()
    bad = J[J.latest_read >= J.entry_session]
    return dict(episodes_checked=int(len(J)), violations=int(len(bad)),
                first=[str(x) for x in bad.index[:5]],
                rule="no rule may read the entry session, so the opening print cannot inform "
                     "the decision")


# ══════════════════════════════════════════════════════════════════════════
def gate_6a(M, E, cal, settle_hours=SETTLE_HOURS):
    opens = session_opens(M)
    last_bar = M.groupby("episode_id")["ts_et"].max().rename("last_bar_ts")
    dead, entry_session = decision_deadlines(E, opens, cal)

    J = pd.DataFrame({"last_bar_ts": last_bar}).join(
        dead.rename("decision_deadline_ts"), how="inner").dropna(
        subset=["decision_deadline_ts"])
    if not len(J):
        raise NotFreezable(
            "no episode has a reconstructible decision_deadline_ts. CAUSALITY_GATE = "
            "NOT_TESTABLE, therefore SESSION_LENGTH_SPEC = NOT_FREEZABLE. Obtain the field "
            "before the first forward membership evaluation; do not record a structural "
            "argument and proceed.")

    J["counterfactual_commit_ts"] = J.last_bar_ts + pd.Timedelta(hours=settle_hours)
    J["headroom_h"] = (J.decision_deadline_ts
                       - J.counterfactual_commit_ts).dt.total_seconds() / 3600.0
    J["gap_h"] = (J.decision_deadline_ts - J.last_bar_ts).dt.total_seconds() / 3600.0

    v_order = int((J.counterfactual_commit_ts < J.last_bar_ts).sum())
    v_late = int((J.counterfactual_commit_ts >= J.decision_deadline_ts).sum())   # strict '<'

    q = lambda s: dict(min=float(s.min()), p01=float(s.quantile(0.01)),
                       p05=float(s.quantile(0.05)), median=float(s.median()))

    # DIAGNOSTIC ONLY. Printed so that no alternative cutoff can be promoted to binding later.
    diag = {}
    t5d = pd.to_datetime(pd.Series(J.index.map(
        E.set_index("episode_id").t5_date.to_dict()), index=J.index))
    ent_d = pd.to_datetime(pd.Series(J.index.map(entry_session.to_dict()), index=J.index))
    for name, (anchor, hour) in DIAG_CUTOFFS.items():
        base = ent_d if anchor == "ENTRY" else t5d
        alt = base.dt.normalize() + pd.Timedelta(hours=hour)
        h = (alt - J.counterfactual_commit_ts).dt.total_seconds() / 3600.0
        diag[f"headroom_to_{name}"] = dict(binding=False, **q(h),
                                           would_fail=int((h <= 0).sum()))

    return dict(
        gate="G6A_HISTORICAL_CAUSAL_HEADROOM",
        proves="market-time feasibility only",
        does_not_prove="vendor ingest latency — the timestamps at which historical data "
                       "ARRIVED were never recorded, so no historical test can speak to it. "
                       "That question is answered forward by G6B, on observed timestamps.",
        commit_ts_basis="COUNTERFACTUAL: last_bar_ts + settle window. Historical sessions have "
                        "no observed commit time; the bulk-load time would fail every episode "
                        "and mean nothing. This tests the RULE against historical timing.",
        entry_ts="next_session_open",
        decision_deadline_ts="next_session_open, as an EXCLUSIVE causal upper bound",
        binding_inequality="session_length_commit_ts < decision_deadline_ts",
        why_exclusive="equality would let the opening print inform the decision. The strict "
                      "bound says only that every input is in hand before the next session "
                      "opens, and adds no arbitrary hour to a specification that never "
                      "contained one.",
        episodes_checked=int(len(J)),
        violations_order=v_order,
        violations_late_for_decision=v_late,
        first_late=[str(x) for x in J.index[
            J.counterfactual_commit_ts >= J.decision_deadline_ts][:5]],
        headroom_hours_binding=q(J.headroom_h),
        market_gap_hours=q(J.gap_h),
        settle_hours=settle_hours,
        max_tolerable_settle_hours=float(J.gap_h.min()),
        settle_window_role="OPERATIONAL FINALITY PROXY, not a feature parameter. Draft-frozen "
                           "at 6h BEFORE these numbers were computed; headroom is diagnostics "
                           "and must never become a tuning target.",
        diagnostics_non_binding=diag,
        why_diagnostics="publishing alternative cutoffs makes the binding choice visible and "
                        "prevents a later 'use the permissive one, the strict one failed'")


# ══════════════════════════════════════════════════════════════════════════
def main():
    print("G6 · decision-time causality · DRAFT", flush=True)
    g6b = test_forward_guard()
    for k, v in g6b.items():
        print(f"    {'PASS' if v else 'FAIL'} G6B.{k}")
    if not all(g6b.values()):
        raise NotFreezable("G6B runtime guard failed its own tests")

    M = to_et(pd.read_parquet(D.OUT_MS, columns=["episode_id", "session_date", "ts_et"]))
    E = pd.read_parquet(D.OUT_EP, columns=["episode_id", "t5_date"])
    print(f"  decision_ts materialised in episode table: {'decision_ts' in E.columns}")
    cal = sorted(pd.unique(M.session_date))

    op = gate_opening_print(M, decision_deadlines(E, session_opens(M), cal)[1])
    print(f"  opening-print exclusion · checked {op['episodes_checked']:,} · "
          f"violations {op['violations']}")

    r = gate_6a(M, E, cal)
    for k in ("episodes_checked", "violations_order", "violations_late_for_decision",
              "headroom_hours_binding", "market_gap_hours", "max_tolerable_settle_hours"):
        print(f"  {k:<32} {r[k]}")
    for k, v in r["diagnostics_non_binding"].items():
        print(f"  [diag] {k:<26} min {v['min']:.1f}h · would_fail {v['would_fail']:,}")

    ok = (r["violations_order"] == 0 and r["violations_late_for_decision"] == 0
          and op["violations"] == 0)
    print(f"\n  G6A {'PASS' if ok else 'FAIL'} · G6B guard frozen with tests")
    if not ok:
        raise NotFreezable("G6A failed — SESSION_LENGTH_SPEC = NOT_FREEZABLE")
    r["opening_print_exclusion"] = op
    r["g6b_guard_tests"] = g6b
    json.dump(r, open("/tmp/g6_causality.json", "w"), indent=2, default=str)


if __name__ == "__main__":
    main()
