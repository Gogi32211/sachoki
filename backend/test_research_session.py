"""Adversarial tests for multiplicity accounting, on synthetic sessions.

The acceptance statement, and everything below serves it:

    A user cannot change a statistically meaningful degree of freedom, see a new result, and
    leave no machine-classified event in the ledger.

Sessions A–D are the four cases that separate a correct accountant from a click counter. D is
the one that matters: the screen showed five, the algorithm chose among thirty-one, and the
multiplicity is thirty-one.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from research_session import (CLAIM_CHANGE, PRESENTATION_ONLY, SEARCH_SPACE_CHANGE,  # noqa: E402
                             CannotRegisterAfterExposureError, ClaimIdentity,
                             ResearchSession, SearchSpaceDriftError, SessionStateError,
                             UnregisteredSelectionError, classify_change,
                             preview_design_change)

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


def claim(tol: str = "5", horizon: str = "20", outcome: str = "median_return") -> ClaimIdentity:
    return ClaimIdentity(
        estimand="incremental_return_pp", outcome=outcome, horizon=horizon,
        population="price_21_89", conditioning_hash=f"rsi45pm{tol}",
        feature_rule_hash="rsi_14", support_policy_hash="6f825ca4763fea76",
        null_family="OPPORTUNITY_LEVEL", decision_policy_version="verdict_v2")


# ── the four sessions ────────────────────────────────────────────────────────
def tA_reopening_the_same_claim_costs_nothing():
    """A · one specification opened three times is one claim"""
    s = ResearchSession("A").start_exploration()
    c = claim()
    for _ in range(3):
        s.execute(c).expose(c)
    a = s.accounting()
    assert a["distinct_claims_executed"] == 1, a
    assert a["k_exposed"] == 1, a
    assert a["revisits"] == 2, a


def tB_tolerance_changes_are_new_claims():
    """B · ±5, ±1, ±2 are three claims, not one view of one"""
    s = ResearchSession("B").start_exploration()
    for tol in ("5", "1", "2"):
        c = claim(tol)
        s.change_parameter("conditioning_tolerance", "prev", tol)
        s.execute(c).expose(c)
    a = s.accounting()
    assert a["k_exposed"] == 3, a
    assert a["changes_by_role"][CLAIM_CHANGE] == 3, a


def tC_technical_reruns_cost_nothing():
    """C · five technical re-runs of one hash remain one claim"""
    s = ResearchSession("C").start_exploration()
    c = claim()
    for _ in range(5):
        s.execute(c)
    s.expose(c)
    a = s.accounting()
    assert a["distinct_claims_executed"] == 1 and a["revisits"] == 4, a
    assert a["k_exposed"] == 1, a


def tD_search_multiplicity_is_the_space_not_the_screen():
    """D · ranked 31, displayed 5 → k_selectable = 31"""
    s = ResearchSession("D").start_exploration()
    s.search_run(space_id="combolab_v2", space_size=31, space_hash="3600ae3dd52a25e6",
                 displayed=5)
    for i in range(5):
        c = claim(tol=str(i))
        s.expose(c)
    a = s.accounting()
    assert a["k_selectable"] == 31, f"multiplicity followed the screen, not the algorithm: {a}"
    assert a["displayed_at_most"] == 5, a
    assert a["k_exposed"] == 5, a


# ── the irreversible edge ────────────────────────────────────────────────────
def tE_cannot_register_after_seeing():
    """looking first and registering afterwards is refused, not warned about"""
    s = ResearchSession("E").start_exploration()
    s.declare_search_space("combolab_v2", 31, "3600ae3dd52a25e6")
    c = claim()
    s.execute(c).expose(c)
    try:
        s.register()
    except CannotRegisterAfterExposureError as e:
        assert "NEW session" in str(e)
        assert s.accounting()["confirmatory_eligible"] is False
        return
    raise AssertionError("a session became registered after exposing a result")


def tF_registration_needs_a_declared_space():
    s = ResearchSession("F").start_exploration()
    try:
        s.register()
    except SessionStateError:
        return
    raise AssertionError("registered without declaring what may be searched")


def tG_registered_sessions_do_not_mutate():
    """a claim-changing knob is refused once frozen; a cosmetic one is not"""
    s = ResearchSession("G").start_exploration()
    s.declare_search_space("combolab_v2", 31, "3600ae3dd52a25e6").register()
    s.change_parameter("layout", "grid", "list")          # cosmetic: allowed
    for p in ("conditioning_tolerance", "horizon", "top_k", "equivalence_margin"):
        try:
            s.change_parameter(p, "a", "b")
        except SessionStateError:
            continue
        raise AssertionError(f"{p} mutated a registered study")


# ── the two fatal contracts ──────────────────────────────────────────────────
def tH_unregistered_selection_is_fatal():
    s = ResearchSession("H").start_exploration()
    s.declare_search_space("combolab_v2", 31, "3600ae3dd52a25e6").register()
    inside = claim("5")
    outside = claim("1")
    s.request_promotion(inside, registered_claims={inside.claim_hash})
    try:
        s.request_promotion(outside, registered_claims={inside.claim_hash})
    except UnregisteredSelectionError as e:
        assert "integrity failure" in str(e)
        return
    raise AssertionError("a confirmatory verdict was allowed on an unregistered claim")


def tI_search_space_drift_is_fatal():
    s = ResearchSession("I").start_exploration()
    s.declare_search_space("combolab_v2", 31, "3600ae3dd52a25e6").register()
    s.search_run("combolab_v2", 31, "3600ae3dd52a25e6", 5)         # the declared space
    try:
        s.search_run("combolab_v2", 37, "deadbeefdeadbeef", 5)      # something else
    except SearchSpaceDriftError as e:
        assert "not the multiplicity that was paid" in str(e)
        return
    raise AssertionError("a registered session searched a space it never declared")


# ── the parameter registry ───────────────────────────────────────────────────
def tJ_undeclared_knob_is_refused():
    """a knob with no declared role is a hidden degree of freedom"""
    try:
        classify_change("mystery_slider")
    except KeyError as e:
        assert "hidden degree of freedom" in str(e)
        return
    raise AssertionError("an undeclared parameter was accepted")


def tK_sorting_is_not_uniformly_harmless():
    """re-ranking by a NEW outcome metric is a selection path, not a view"""
    assert classify_change("sort_by_displayed_column").semantic_role == PRESENTATION_ONLY
    d = classify_change("sort_by_new_outcome_metric")
    assert d.semantic_role == SEARCH_SPACE_CHANGE and d.affects_search_space


def tL_preview_classifies_before_the_result_exists():
    """the UI asks first; the answer cannot be relabelled once the number is attractive"""
    p = preview_design_change("conditioning_tolerance", claim("5"), claim("1"))
    assert p["change_type"] == CLAIM_CHANGE
    assert p["multiplicity_effect"] == "NEW_SELECTABLE_CLAIM"
    assert p["old_claim_hash"] != p["new_claim_hash"]
    q = preview_design_change("layout", claim("5"), claim("5"))
    assert q["multiplicity_effect"] == "NONE"


def tM_identity_refuses_holes():
    """an identity with an empty field cannot tell two claims apart"""
    try:
        ClaimIdentity("e", "o", "h", "p", "", "f", "s", "n", "v")
    except ValueError as e:
        assert "conditioning_hash" in str(e)
        return
    raise AssertionError("an identity was built with a hole in it")


def tN_every_meaningful_change_leaves_a_classified_event():
    """the acceptance statement itself"""
    s = ResearchSession("N").start_exploration()
    before = claim("5")
    s.execute(before).expose(before)
    s.change_parameter("horizon", "20", "40")
    after = claim("5", horizon="40")
    s.execute(after).expose(after)
    ev = [e for e in s.events if e.event_type == "CONDITION_CHANGED"]
    assert len(ev) == 1 and ev[0].payload["role"] == CLAIM_CHANGE
    a = s.accounting()
    assert a["k_exposed"] == 2, a
    # and the ledger is a chain, not a bag
    for i, e in enumerate(s.events):
        assert e.event_id == i and e.prior_state_hash and e.new_state_hash


def tO_exploration_can_never_become_confirmatory():
    s = ResearchSession("O").start_exploration()
    c = claim()
    s.execute(c).expose(c)
    s.close()
    assert s.state == "CLOSED_EXPLORATORY"
    assert s.accounting()["confirmatory_eligible"] is False


print("=" * 104, flush=True)
print("  RESEARCH SESSION — multiplicity accounting, adversarial", flush=True)
print("=" * 104, flush=True)
for fn in (tA_reopening_the_same_claim_costs_nothing, tB_tolerance_changes_are_new_claims,
           tC_technical_reruns_cost_nothing, tD_search_multiplicity_is_the_space_not_the_screen,
           tE_cannot_register_after_seeing, tF_registration_needs_a_declared_space,
           tG_registered_sessions_do_not_mutate, tH_unregistered_selection_is_fatal,
           tI_search_space_drift_is_fatal, tJ_undeclared_knob_is_refused,
           tK_sorting_is_not_uniformly_harmless, tL_preview_classifies_before_the_result_exists,
           tM_identity_refuses_holes, tN_every_meaningful_change_leaves_a_classified_event,
           tO_exploration_can_never_become_confirmatory):
    check((fn.__doc__ or fn.__name__).splitlines()[0], fn)
print("=" * 104, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
if not fail:
    s = ResearchSession("DEMO").start_exploration()
    s.search_run("combolab_v2", 31, "3600ae3dd52a25e6", 5)
    for tol in ("5", "1"):
        s.change_parameter("conditioning_tolerance", "prev", tol)
        c = claim(tol)
        s.execute(c).expose(c)
    s.execute(claim("5"))
    print("\n  a live exploration session, as the ledger sees it:", flush=True)
    for k, v in s.accounting().items():
        print(f"    {k:<26s} {v}", flush=True)
sys.exit(1 if fail else 0)
