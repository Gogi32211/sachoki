"""Twenty-two knobs, and the test that matters is that they are not twenty-two special cases.

Wiring parameters one at a time is how a single ledger acquires twenty different ways around
itself. Each control grows its own path — this one updates the hash, that one forgot, the third
previews one thing and commits another — and every path is a place where the accounting can
differ from what the screen did.

So the central test is `t1`: within a semantic role, every parameter behaves identically, with
no per-parameter code anywhere. If `horizon` and `universe` need different handling to both
count as CLAIM_CHANGE, then the role is decoration and the real behaviour lives in whichever
branch was written last.

THE OTHER ACCEPTANCE TEST IS THE ROUND TRIP. Explore the space, come back to where you started,
and the screen is identical to how it began — while the ledger is not:

    current state = initial state   ⇏   research history = initial history

Every laundering path found so far has the same shape: something that looks clean because a
counter was reset, a session renamed, a family re-declared, or a read unobserved. Returning the
sliders to their original position is the cheapest version of it, and it needs no new mechanism
at all — just the assumption that state and history are the same thing.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import parameter_surface as PS                                       # noqa: E402
from research_session import (CLAIM_CHANGE, DESIGN_CHANGE, POLICY_CHANGE,  # noqa: E402
                              PRESENTATION_ONLY, SEARCH_SPACE_CHANGE, SELECTION_PATH_CHANGE,
                              ClaimIdentity, ResearchSession)

ok = fail = 0

ROLES = (PRESENTATION_ONLY, CLAIM_CHANGE, DESIGN_CHANGE, SEARCH_SPACE_CHANGE, POLICY_CHANGE)


def check(name, fn):
    global ok, fail
    try:
        fn()
        print(f"  PASS  {name}", flush=True)
        ok += 1
    except Exception as e:                                           # noqa: BLE001
        print(f"  FAIL  {name}: {e}", flush=True)
        fail += 1


def surface() -> PS.ParameterSurface:
    return PS.ParameterSurface.initial(
        horizon="20", conditioning_tolerance="5", universe="russell",
        selection_top_k="31", displayed_top_k="5", support_cutoff="100",
        equivalence_margin="0.5", null_family="opportunity_level", direction="long",
        date_range="2021-2023", outcome_metric="median_return", weighting="equal",
        setup_subset="a,b,c", rank_metric="ic", base_setup_conditioning="none",
        conditioning_feature="rsi_14", top_k="31", layout="grid", theme="dark",
        column_order="a,b", sort_by_displayed_column="score",
        sort_by_new_outcome_metric="sharpe")


# ── THE CENTRAL TEST ────────────────────────────────────────────────────────
def t1_two_knobs_of_one_role_behave_identically():
    """the acceptance statement: role decides, not the parameter

    Every member of a role is driven through the same call and its answer compared field by
    field. A parameter that needed its own branch to behave correctly would show up here as a
    role whose members disagree.
    """
    s = surface()
    for role in ROLES:
        members = PS.by_role(role)
        assert members, f"{role} has no parameters; the registry lost a group"
        shapes = {}
        for pid in members:
            c = PS.classify(s, pid, "SOMETHING-NEW")
            shapes[pid] = (c["role"], c["hashes_moved"], c["multiplicity_effect"],
                           c["registered_effect"])
        distinct = set(shapes.values())
        assert len(distinct) == 1, (
            f"{role} members disagree, so behaviour is per-parameter and not per-role: {shapes}")


def t1b_REPRODUCTION_one_special_case_and_t1_fails():
    """the guard shown its defect: give a single parameter its own branch

    `t1` compares members of a role field by field. That only means something if a divergence
    would actually surface, so here is the divergence — one parameter handled specially, exactly
    the way a hand-wired control would end up — and the same comparison run over it.
    """
    s = surface()

    def classify_with_a_special_case(surface_, pid, value):
        c = dict(PS.classify(surface_, pid, value))
        if pid == "universe":                    # the hand-wired one
            c["multiplicity_effect"] = "NONE"    # "it's just a filter, surely"
            c["hashes_moved"] = ()
        return c

    shapes = {}
    for pid in PS.by_role(CLAIM_CHANGE):
        c = classify_with_a_special_case(s, pid, "SOMETHING-NEW")
        shapes[pid] = (c["role"], c["hashes_moved"], c["multiplicity_effect"],
                       c["registered_effect"])
    assert len(set(shapes.values())) > 1, (
        "the reproduction failed to reproduce: a parameter given its own branch was supposed to "
        "break the role's uniformity, and if it does not, t1 cannot detect the thing it exists "
        "to detect")

    # and the real classifier has no such branch
    real = {pid: PS.classify(s, pid, "SOMETHING-NEW")["hashes_moved"]
            for pid in PS.by_role(CLAIM_CHANGE)}
    assert len(set(real.values())) == 1, real


def t2_the_registry_cannot_contradict_itself():
    """a role that says one thing and a flag that says another"""
    problems = PS.declared_effects_are_consistent()
    assert not problems, problems
    assert len(PS.REGISTRY) == 22, f"{len(PS.REGISTRY)} parameters declared"
    for pid, r in PS.REGISTRY.items():
        assert r.semantic_role in ROLES, (pid, r.semantic_role)
        assert callable(r.canonicalizer), pid
    assert PS.registry_has_one_home(), (
        "the surface declared a parameter the ledger does not know. It would be turnable on "
        "screen and refused by change_parameter — two sources of truth about one knob.")


# ── one fixture per role ────────────────────────────────────────────────────
def t3_PRESENTATION_ONLY_is_free_ten_times_over():
    """sorting the same list ten ways changes nothing that a verdict depends on"""
    s = surface()
    before = s.hashes
    for i in range(10):
        s, c = PS.apply(s, "sort_by_displayed_column", f"column_{i}")
        assert c["multiplicity_effect"] == "NONE", c
        assert c["hashes_moved"] == (), c
    assert s.hashes == before, f"a view changed the research object: {before} -> {s.hashes}"


def t4_CLAIM_CHANGE_moves_the_claim_and_nothing_else():
    s = surface()
    after, c = PS.apply(s, "horizon", "40")
    assert c["role"] == CLAIM_CHANGE and c["multiplicity_effect"] == "NEW_SELECTABLE_CLAIM"
    assert c["hashes_moved"] == ("claim_hash",), c["hashes_moved"]
    assert after.search_space_hash == s.search_space_hash, \
        "a claim change moved the search space as a side effect"
    assert after.decision_policy_hash == s.decision_policy_hash


def t5_DESIGN_CHANGE_is_a_new_statistical_object():
    s = surface()
    _, c = PS.apply(s, "support_cutoff", "250")
    assert c["role"] == DESIGN_CHANGE, c
    assert c["multiplicity_effect"] == "NEW_STATISTICAL_OBJECT", c
    assert "not comparable" in c["note"], c


def t6_SEARCH_SPACE_CHANGE_moves_the_space_not_the_claim():
    s = surface()
    after, c = PS.apply(s, "selection_top_k", "37")
    assert c["hashes_moved"] == ("search_space_hash",), c["hashes_moved"]
    assert after.claim_hash == s.claim_hash, "the question changed when only the menu did"


def t7_POLICY_CHANGE_moves_only_the_decision_rule():
    s = surface()
    after, c = PS.apply(s, "equivalence_margin", "1.0")
    assert c["hashes_moved"] == ("decision_policy_hash",), c["hashes_moved"]
    assert after.claim_hash == s.claim_hash


# ── the UI number that carries no role ──────────────────────────────────────
def t8_a_number_on_screen_says_nothing_about_what_it_costs():
    """display 5 → 10 is a view; selection 31 → 37 is multiplicity, and both are 'a number'"""
    s = surface()
    shown = PS.classify(s, "displayed_top_k", "10")
    chosen = PS.classify(s, "selection_top_k", "37")
    assert shown["role"] == PRESENTATION_ONLY and shown["hashes_moved"] == ()
    assert chosen["role"] == SEARCH_SPACE_CHANGE
    assert chosen["hashes_moved"] == ("search_space_hash",)
    assert shown["multiplicity_effect"] != chosen["multiplicity_effect"], (
        "two controls that both read 'a number went up' produced the same statistical verdict; "
        "the registry is what separates them and it did not")


# ── identity ────────────────────────────────────────────────────────────────
def t9_canonicalisation_is_part_of_identity():
    """'20', ' 20 ' and 20 are one horizon, or k drifts upward on whitespace"""
    base = surface()
    a = base.with_value("horizon", "20")
    for variant in (" 20 ", 20, "20"):
        assert base.with_value("horizon", variant).claim_hash == a.claim_hash, variant
    assert base.with_value("horizon", "40").claim_hash != a.claim_hash
    # and an unordered selection is unordered
    x = base.with_value("setup_subset", "b,a,c")
    y = base.with_value("setup_subset", "c,b,a")
    assert x.claim_hash == y.claim_hash or x.search_space_hash == y.search_space_hash


def t10_reselecting_the_current_value_costs_nothing():
    """otherwise k grows by clicking the setting that is already selected"""
    s = surface()
    c = PS.classify(s, "horizon", " 20 ")
    assert c["no_op"] is True and c["multiplicity_effect"] == "NONE", c
    after, _ = PS.apply(s, "horizon", "20")
    assert after.hashes == s.hashes


# ── the freeze ──────────────────────────────────────────────────────────────
def t11_a_registered_study_permits_exactly_the_cosmetic_set():
    s = surface()
    for role in ROLES:
        for pid in PS.by_role(role):
            c = PS.classify(s, pid, "ANY", state="ACTIVE_REGISTERED")
            expected = "ALLOW" if role == PRESENTATION_ONLY else "REJECT"
            assert c["registered_effect"] == expected, (pid, role, c["registered_effect"])
            if expected == "REJECT":
                try:
                    PS.apply(s, pid, "ANY", state="ACTIVE_REGISTERED")
                except PS.ParameterSurfaceError as e:
                    assert "fork it" in str(e).lower()
                    continue
                raise AssertionError(f"{pid} mutated a frozen study")


def t12_preview_and_commit_cannot_disagree():
    """they are the same call; a preview the ledger does not honour is worse than none"""
    s = surface()
    for pid in ("horizon", "selection_top_k", "layout", "equivalence_margin", "support_cutoff"):
        previewed = PS.classify(s, pid, "NEWVALUE")
        _, committed = PS.apply(s, pid, "NEWVALUE")
        assert previewed == committed, (pid, previewed, committed)


def t13_an_undeclared_knob_is_still_refused():
    try:
        PS.record("mystery_slider")
    except KeyError as e:
        assert "hidden degree of freedom" in str(e)
        return
    raise AssertionError("an undeclared parameter got a record")


# ── THE ROUND TRIP ──────────────────────────────────────────────────────────
def _claim(surface_: PS.ParameterSurface) -> ClaimIdentity:
    v = surface_.values
    return ClaimIdentity(
        estimand="incremental_return_pp", outcome=v["outcome_metric"], horizon=v["horizon"],
        population="price_21_89", conditioning_hash=f"rsi45pm{v['conditioning_tolerance']}",
        feature_rule_hash=v["conditioning_feature"], support_policy_hash=v["support_cutoff"],
        null_family=v["null_family"], decision_policy_version="verdict_v2")


def t14_returning_to_the_starting_point_does_not_return_the_history():
    """the cheapest laundering path, and it needs no new mechanism — only a wrong assumption

        tolerance ±5 → ±1 · horizon 20 → 40 · sort A → B · horizon 40 → 20 · ±1 → ±5

    The screen ends where it began. `k` does not rewind, because the specifications in between
    were looked at and looking cannot be undone.
    """
    s = surface()
    session = ResearchSession("RT").start_exploration()
    start_hashes = s.hashes
    start_claim = _claim(s).claim_hash

    route = [("conditioning_tolerance", "1"), ("horizon", "40"),
             ("sort_by_displayed_column", "pf"),          # free, in the middle
             ("horizon", "20"), ("conditioning_tolerance", "5")]
    for pid, val in route:
        before = s
        s, c = PS.apply(s, pid, val)
        if c["multiplicity_effect"] == "NONE":
            continue
        session.change_parameter(pid, str(before.claim_hash), str(s.claim_hash))
        claim = _claim(s)
        session.execute(claim)
        session.expose(claim)

    assert s.hashes == start_hashes, "the round trip did not actually return to the start"
    assert _claim(s).claim_hash == start_claim

    a = session.accounting()
    assert a["k_exposed"] >= 3, (
        f"the ledger rewound with the sliders: k_exposed={a['k_exposed']}. Three distinct "
        f"specifications were looked at on the way round.")
    assert a["changes_by_role"][CLAIM_CHANGE] == 4, a["changes_by_role"]
    assert a["changes_by_role"][PRESENTATION_ONLY] == 0, \
        "a free action was charged for on the way past"
    assert a["confirmatory_eligible"] is False


def t15_the_free_action_in_the_middle_really_was_free():
    """the round trip is only meaningful if the presentation step cost nothing"""
    s = surface()
    session = ResearchSession("RT2").start_exploration()
    before = session.accounting()
    for i in range(5):
        s, c = PS.apply(s, "layout", f"grid-{i}")
        assert c["multiplicity_effect"] == "NONE"
    after = session.accounting()
    assert after["k_exposed"] == before["k_exposed"] == 0, (before, after)
    assert after["events"] == before["events"], "a cosmetic change reached the ledger"


# ── evidence vs decision ────────────────────────────────────────────────────
def _ci(tol="5", nf="opportunity_level", horizon="20"):
    return ClaimIdentity("inc", "median_return", horizon, "russell", f"rsi45pm{tol}",
                         "rsi_14", "100", nf, "verdict_v2")


def t16_k_can_say_what_multiplied_it():
    """seven different effects, or one effect under seven decision rules — not the same search"""
    same_evidence_new_rule = (_ci(), _ci(nf="day_level"))
    a, b = same_evidence_new_rule
    assert a.evidence_claim_hash == b.evidence_claim_hash, "a decision rule moved the estimand"
    assert a.decision_spec_hash != b.decision_spec_hash
    assert a.claim_hash != b.claim_hash, "the pair must still differ; that is the conservative bit"

    c = _ci(tol="1")
    assert c.evidence_claim_hash != a.evidence_claim_hash
    assert c.decision_spec_hash == a.decision_spec_hash, "a conditioning change moved the rule"

    s = ResearchSession("EV").start_exploration()
    for claim in (a, b, c):
        s.execute(claim).expose(claim)
    acc = s.accounting()
    assert acc["k_exposed"] == 3, acc
    assert acc["distinct_evidence_claims_exposed"] == 2, acc
    assert acc["distinct_decision_specs_exposed"] == 2, acc
    assert acc["accounting_policy_version"].endswith("EVIDENCE_AND_DECISION"), acc


def t16b_REPRODUCTION_one_hash_cannot_answer_the_question():
    """the guard shown its defect: with only claim_hash, k=3 explains nothing"""
    a, b, c = _ci(), _ci(nf="day_level"), _ci(tol="1")
    pairs = {x.claim_hash for x in (a, b, c)}
    assert len(pairs) == 3
    # the two different searches that produce the same k
    one_effect_two_rules = {_ci().claim_hash, _ci(nf="day_level").claim_hash}
    two_effects_one_rule = {_ci().claim_hash, _ci(tol="1").claim_hash}
    assert len(one_effect_two_rules) == len(two_effects_one_rule) == 2, (
        "the reproduction failed: these two searches were supposed to be indistinguishable by "
        "claim_hash alone, which is why the components had to be recorded separately")
    assert ({_ci().evidence_claim_hash, _ci(nf="day_level").evidence_claim_hash} !=
            {_ci().evidence_claim_hash, _ci(tol="1").evidence_claim_hash}), \
        "the components do not separate the two searches either"


# ── when a display sort is free ─────────────────────────────────────────────
def t17_display_sort_is_free_only_while_it_reaches_nothing():
    """PRESENTATION_ONLY iff it cannot change eligibility, promotion order or inspectability"""
    s = surface()
    for key in ("ticker", "support", "n"):
        for caps in (PS.CONTROL_SURFACE, PS.RESULTS_SURFACE):
            c = PS.classify(s, "sort_by_displayed_column", key, caps=caps)
            assert c["role"] == PRESENTATION_ONLY, (key, caps, c["role"])
            assert c["hashes_moved"] == (), c

    for key in ("effect", "pf", "dsr"):
        free = PS.classify(s, "sort_by_displayed_column", key, caps=PS.CONTROL_SURFACE)
        assert free["role"] == PRESENTATION_ONLY, (
            f"sorting by {key} on a surface with nothing to click reaches no decision")
        costed = PS.classify(s, "sort_by_displayed_column", key, caps=PS.RESULTS_SURFACE)
        assert costed["role"] == SELECTION_PATH_CHANGE, (key, costed["role"])
        assert costed["multiplicity_effect"] == "SELECTION_PATH_CHANGED", costed
        assert costed["hashes_moved"] == ("search_space_hash",), costed


def t17b_an_unknown_sort_key_fails_closed():
    """guessing wrong the other way would hand out a free selection path"""
    s = surface()
    c = PS.classify(s, "sort_by_displayed_column", "some_new_column", caps=PS.RESULTS_SURFACE)
    assert c["role"] == SELECTION_PATH_CHANGE, c["role"]


def t17c_the_reclassification_is_caused_by_one_declared_flag():
    """adding a row affordance changes the role, with no new classification code"""
    s = surface()
    before = PS.classify(s, "sort_by_displayed_column", "effect",
                         caps=PS.SurfaceCapabilities())
    after = PS.classify(s, "sort_by_displayed_column", "effect",
                        caps=PS.SurfaceCapabilities(rows_inspectable=True))
    assert before["role"] == PRESENTATION_ONLY and after["role"] == SELECTION_PATH_CHANGE
    assert before["surface_capabilities"] != after["surface_capabilities"]
    assert before["role_is_conditional"] is True, "the plan must say the role was derived"


def t17d_a_frozen_study_refuses_a_selection_path():
    s = surface()
    c = PS.classify(s, "sort_by_displayed_column", "effect", state="ACTIVE_REGISTERED",
                    caps=PS.RESULTS_SURFACE)
    assert c["registered_effect"] == "REJECT", c
    ok_ = PS.classify(s, "sort_by_displayed_column", "ticker", state="ACTIVE_REGISTERED",
                      caps=PS.RESULTS_SURFACE)
    assert ok_["registered_effect"] == "ALLOW", ok_


print("=" * 104, flush=True)
print("  PARAMETER SURFACE — twenty-two knobs, five behaviours", flush=True)
print("=" * 104, flush=True)
for i, fn in enumerate([t1_two_knobs_of_one_role_behave_identically,
                        t1b_REPRODUCTION_one_special_case_and_t1_fails,
                        t2_the_registry_cannot_contradict_itself,
                        t3_PRESENTATION_ONLY_is_free_ten_times_over,
                        t4_CLAIM_CHANGE_moves_the_claim_and_nothing_else,
                        t5_DESIGN_CHANGE_is_a_new_statistical_object,
                        t6_SEARCH_SPACE_CHANGE_moves_the_space_not_the_claim,
                        t7_POLICY_CHANGE_moves_only_the_decision_rule,
                        t8_a_number_on_screen_says_nothing_about_what_it_costs,
                        t9_canonicalisation_is_part_of_identity,
                        t10_reselecting_the_current_value_costs_nothing,
                        t11_a_registered_study_permits_exactly_the_cosmetic_set,
                        t12_preview_and_commit_cannot_disagree,
                        t13_an_undeclared_knob_is_still_refused,
                        t14_returning_to_the_starting_point_does_not_return_the_history,
                        t15_the_free_action_in_the_middle_really_was_free,
                        t16_k_can_say_what_multiplied_it,
                        t16b_REPRODUCTION_one_hash_cannot_answer_the_question,
                        t17_display_sort_is_free_only_while_it_reaches_nothing,
                        t17b_an_unknown_sort_key_fails_closed,
                        t17c_the_reclassification_is_caused_by_one_declared_flag,
                        t17d_a_frozen_study_refuses_a_selection_path], 1):
    check(f"{i:>2d} · {(fn.__doc__ or fn.__name__).splitlines()[0]}", fn)
print("=" * 104, flush=True)
print(f"  {ok} passed · {fail} failed", flush=True)
if not fail:
    for role in ROLES:
        print(f"    {role:<21s} {len(PS.by_role(role))}  {', '.join(PS.by_role(role))}",
              flush=True)
sys.exit(1 if fail else 0)
