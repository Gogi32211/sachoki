"""Gate 3A · a ranking frozen after the evidence is exploratory, and says so permanently.

The sharpest test is `t5`: same evidence, same θ, a different decision policy, and the order must
not move. If it does, the ranking was a decision ranking wearing an evidence ranking's name, and
the evidence_claim / decision_spec separation established earlier would be undone by the table
that displays it.

The second is `t3`. "Register the policy, then apply it to the existing 31, then call it
preregistered" is the exact phrasing this project's own exposure definition forbids, and it is
the same mistake as upgrading exposed evidence — one level along.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import historical_ranking_policy as RP                              # noqa: E402

ok = fail = 0


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                          # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def cells(**over):
    base = {"adx0": {"theta": 0.42}, "adx1": {"theta": 1.90}, "adx2": {"theta": -0.31},
            "rsi<35": {"theta": 1.90}, "rs": {"theta": 0.05}}
    base.update(over)
    return base


def t1_the_policy_is_registered_and_frozen():
    assert RP.is_registered()
    r = RP.record()
    assert r["policy_hash"] == RP.policy_hash()
    assert r["registered_at"] and r["registered_by"]
    p = r["policy"]
    assert p["direction"] == "descending"
    assert p["verdict_affects_rank"] is False
    assert p["uncertainty_affects_rank"] is False
    assert p["support_affects_rank"] == "eligibility_only"


def t2_the_current_evidence_is_recorded_as_predating_it():
    r = RP.record()
    assert r["registered_after_current_historical_exposure"] is True
    assert r["current_snapshot_usage"] == RP.POST_EXPOSURE_EXPLORATORY_ONLY
    assert r["future_usage"] == RP.PROSPECTIVELY_REGISTERED
    prior = r["evidence_that_predates_this_policy"]
    assert len(prior) == 2, prior
    for e in prior:
        assert e["result_role"] == "ENGINE_QUALIFICATION_EVIDENCE", e


def t3_an_already_seen_universe_cannot_be_called_preregistered():
    """the phrasing the exposure definition forbids, refused as an error"""
    usage = RP.usage_for(snapshot_exposed_before_policy=True)
    assert usage == RP.POST_EXPOSURE_EXPLORATORY_ONLY
    RP.assert_not_claimed_preregistered(usage, RP.POST_EXPOSURE_EXPLORATORY_ONLY)
    try:
        RP.assert_not_claimed_preregistered(usage, RP.PROSPECTIVELY_REGISTERED)
    except RP.RetroactiveRankingRegistrationError as e:
        assert "does not turn an already-seen universe" in str(e)
        assert "does not exist yet" in str(e), "the refusal must name the legitimate path"
        return
    raise AssertionError("an order over exposed evidence was labelled preregistered")


def t4_untouched_evidence_gets_the_prospective_standing():
    """the guard must not be a wall: the legitimate case is exactly what the policy is for"""
    usage = RP.usage_for(snapshot_exposed_before_policy=False)
    assert usage == RP.PROSPECTIVELY_REGISTERED
    RP.assert_not_claimed_preregistered(usage, RP.PROSPECTIVELY_REGISTERED)


def t5_the_order_does_not_move_when_the_decision_policy_does():
    """same evidence, same θ, different decision rule — the ranking is unchanged

    This is what makes it an evidence ranking. The function is not even given a verdict, which
    is the contract rather than an oversight.
    """
    c = cells()
    a = RP.rank(c)
    # the same cells under a decision policy that would relabel every verdict
    b = RP.rank({k: dict(v, verdict="BUILD", ci_low=-9.0, ci_high=9.0) for k, v in c.items()})
    assert [x["cell_identity"] for x in a] == [x["cell_identity"] for x in b]
    assert RP.ranking_hash(a) == RP.ranking_hash(b)


def t6_verdicts_do_not_float_to_the_top():
    """rank 1 with UNRESOLVED above rank 2 with BUILD is legitimate and must stay possible"""
    ordered = RP.rank({"weak_but_big": {"theta": 2.1}, "solid": {"theta": 1.8}})
    assert ordered[0]["cell_identity"] == "weak_but_big"
    assert RP.POLICY["verdict_affects_rank"] is False


def t7_ties_break_canonically_and_deterministically():
    a = RP.rank(cells())
    b = RP.rank(dict(reversed(list(cells().items()))))
    assert [x["cell_identity"] for x in a] == [x["cell_identity"] for x in b]
    tied = [x["cell_identity"] for x in a if x["theta"] == 1.90]
    assert tied == sorted(tied), "ties did not break on canonical cell identity"


def t8_the_estimand_is_theta_and_nothing_else_is_read():
    try:
        RP.rank({"a": {"verdict": "BUILD", "ci_low": 1.0}})
    except RP.RankingPolicyError as e:
        assert "ranking estimand is theta" in str(e)
        return
    raise AssertionError("a cell without theta was ranked on something else")


def t9_top_k_is_not_part_of_this_policy():
    """ordering 31 and authorising 5 are different policies with different consequences"""
    assert "exposure policy" in RP.POLICY["top_k"]
    assert "top_k" not in {r["cell_identity"] for r in RP.rank(cells())}
    import inspect                                                   # noqa: PLC0415
    assert "top_k" not in inspect.signature(RP.rank).parameters


def t10_what_rank_one_means_is_written_down():
    """because a column that does not say it will be read as 'the best edge'"""
    m = RP.POLICY["what_rank_one_means"].lower()
    assert "largest estimated incremental" in m
    assert "not the most certain" in m


print("=" * 100, flush=True)
print("  GATE 3A · HISTORICAL RANKING POLICY v1 — registered prospectively", flush=True)
print("=" * 100, flush=True)
for i, fn in enumerate([t1_the_policy_is_registered_and_frozen,
                        t2_the_current_evidence_is_recorded_as_predating_it,
                        t3_an_already_seen_universe_cannot_be_called_preregistered,
                        t4_untouched_evidence_gets_the_prospective_standing,
                        t5_the_order_does_not_move_when_the_decision_policy_does,
                        t6_verdicts_do_not_float_to_the_top,
                        t7_ties_break_canonically_and_deterministically,
                        t8_the_estimand_is_theta_and_nothing_else_is_read,
                        t9_top_k_is_not_part_of_this_policy,
                        t10_what_rank_one_means_is_written_down], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 100, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
sys.exit(1 if fail else 0)
