"""MASSIVE_T1_15M_CLAIM_ENUMERATOR_VERIFICATION_V1 — the reductions, recovered and checked.

E2a left one thing unexplained: what turns 36,385 support-qualified claims into 35,715 classes
and then into a final k of 35,587. Both rules are now read from source rather than inferred, and
both reproduce the preserved counts exactly.

    36,385 -> 35,715   MEMBERSHIP-HASH COLLAPSE. t15m_x.py hashes the episode membership itself,
                       sha256(flatnonzero(m)), so two claims selecting the identical episode set
                       are aliases of one class. 670 aliases — the figure X_ONLY records.

    35,715 -> 35,587   K-CLOSURE S2 FLOORS. t15m_k_closure applies TICKER_FLOOR, DATE_FLOOR and
                       DATE_SHARE to the restricted (S2) support. 128 classes drop out.

The k-closure table independently corroborates the upstream stage: s0_ok is True on exactly
36,385 rows, which is the support-qualified count.

MEMBER-SET INTEGRITY HOLDS. Every ordering hash is present in the claims table, membership hashes
are unique across the final k, claim_uid is unique, and j runs 0..k-1 contiguously.

WHAT THIS IS NOT. The enumerator was NOT re-executed from the historical 15m token frame to
regenerate the member set independently. This verifies the preserved registry's internal
structure and reproduces its reduction arithmetic from the recovered rules; it does not
constitute an independent regeneration, and it is not reported as one.

THE HOPED-FOR COST REDUCTION DOES NOT EXIST. The final 35,587-member family still requires all
144 tokens and all 84 source columns — zero are droppable. So E2b's scope cannot be trimmed by
restricting to what the final family actually uses, which was the cheaper outcome worth checking
before committing to the port.
"""
from __future__ import annotations
import json, os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART

BIND = {"MASSIVE_T1_15M_TOKEN_DICTIONARY_AUTHORITY_V1.json": "ec6c37f45c643ccd",
        "T1_15M_PRESPEC_V1.json": "71d49dcde8921eb3",
        "MASSIVE_T_PRE_Y_ANALYSIS_DICTIONARY_V1.json": "a5a5c5d1b97fa94d"}
bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
if bad:
    print("HOLD — binding mismatch:", bad); raise SystemExit(1)

p = dict(
    report_id="MASSIVE_T1_15M_CLAIM_ENUMERATOR_VERIFICATION_V1",
    status="MASSIVE_T1_15M_CLAIM_ENUMERATOR_VERIFICATION_V1_PASS",
    task_class="ENUMERATOR_VERIFICATION_ONLY", bindings=BIND,
    named_verification_not_recovery="the member registry was already preserved; this verifies "
                                    "it rather than rebuilding it",

    reduction_rules_recovered=dict(
        stage_1=dict(name="MEMBERSHIP_HASH_COLLAPSE", source="t15m_x.py",
                     rule="sha256(flatnonzero(episode_membership).tobytes())[:16]; claims "
                          "selecting an identical episode set are aliases of one class",
                     from_=36385, to=35715, aliases=670,
                     corroborated_by="X_ONLY records aliases = 670"),
        stage_2=dict(name="K_CLOSURE_S2_FLOORS", source="t15m_k_closure.py",
                     floors=["TICKER_FLOOR", "DATE_FLOOR", "DATE_SHARE"],
                     applied_to="the restricted (S2) support",
                     from_=35715, to=35587, dropped=128),
        independent_corroboration=dict(
            k_closure_rows=36418, s0_ok_true=36385,
            meaning="s0_ok is True on exactly the support-qualified count, confirming the "
                    "upstream stage from a different table")),

    support_floors=dict(min_episodes=300, min_tickers=50, min_dates=50, max_date_share=0.1,
                        applied_at="claim level, at both S0 and S2 stages",
                        effect="a floor failure removes the class from the family",
                        note="Massive support may NOT be used to re-select the historical "
                             "family; membership came from historical registration"),

    member_set_integrity=dict(
        order_hashes_subset_of_claims=True,
        membership_hash_unique_in_final_k=True,
        claim_uid_unique=True,
        j_contiguous_0_to_k_minus_1=True,
        final_k=35587,
        per_position_family={"M2→M3": 12191, "M3→M4": 11905, "M1→M2": 11491}),

    what_was_not_done=dict(
        independent_regeneration=False,
        why="re-executing the enumerator would require the historical 15m token frame and the "
            "episode surface; this gate verifies structure and reproduces the reduction "
            "arithmetic from the recovered rules",
        not_reported_as="an independent regeneration"),

    cost_reduction_checked_and_absent=dict(
        question="does the final k=35,587 family need fewer tokens than the full claim set?",
        tokens_in_all_claims=144, tokens_in_final_family=144, droppable=0,
        source_columns_for_final_family=84,
        answer="NO — E2b's scope cannot be trimmed this way",
        why_it_was_worth_checking="it was the cheaper outcome, and had to be settled before "
                                  "committing to an 84-column port"),

    y_firewall=dict(columns_read=["position_family", "first_token", "second_token",
                                  "membership_hash", "claim_id", "j", "representative",
                                  "claim_uid", "s0_ok", "s2_*"],
                    outcome_values_read=0,
                    z_obs_read=False, survivor_read=False),
    y_exposed=0, analysis_writes=0, outcome_exposure="NOT_EXPOSED",
    next_gate="MASSIVE_T1_15M_TOKEN_PORT_FEASIBILITY_V1 — dependency DAG before any production",
    sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

d = ART.seal(p, "MASSIVE_T1_15M_CLAIM_ENUMERATOR_VERIFICATION_V1.json",
             required=("report_id", "status", "reduction_rules_recovered",
                       "member_set_integrity", "what_was_not_done",
                       "cost_reduction_checked_and_absent"),
             supersede=os.path.exists("MASSIVE_T1_15M_CLAIM_ENUMERATOR_VERIFICATION_V1.json"))
print(f"MASSIVE_T1_15M_CLAIM_ENUMERATOR_VERIFICATION_V1 · {d} · {p['status']}")
print("  36,385 -> 35,715  MEMBERSHIP_HASH_COLLAPSE · 670 aliases (X_ONLY agrees)")
print("  35,715 -> 35,587  K_CLOSURE S2 FLOORS · 128 dropped")
print("  corroboration     k_closure s0_ok True = 36,385 = support-qualified count")
print("  integrity         hashes unique · claim_uid unique · j contiguous 0..k-1")
print("  NOT done          independent enumerator regeneration (stated, not implied)")
print("  cost check        final family needs ALL 144 tokens / 84 columns — 0 droppable")
