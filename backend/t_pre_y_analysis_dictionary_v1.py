"""MASSIVE_T_PRE_Y_ANALYSIS_DICTIONARY_V1 — most of it recovered; two things are not portable yet.

The recovery went a long way. The outcome contract, the entry semantics, the population filters,
the anchor construction, the blocking rule, the statistic and the multiplicity procedure all came
back from frozen authority and generator source, exactly rather than approximately. Two
execution-critical pieces did not, and the standing rule says that is a HOLD, not a freeze with
placeholders.

ONE RECOVERED FACT CHANGES THE TEMPORAL READING IN A GOOD DIRECTION. The outcome spec sets
decision = "after T1 session close" and entry_semantics = NEXT_SESSION_OPEN_V1. So the anchor is
known at D's close, the opening-hour features of D are known by D's close, and entry is at D+1's
open. Every X input precedes the decision point. POST_EXPOSURE_SESSION_LOCALIZATION remains the
correct description of the ANCHOR-to-opening-hour relationship — the anchor is not knowable at
M1 — but it does NOT make the construct unimplementable, and my earlier phrasing risked implying
that it did.

WHAT BLOCKS THE FREEZE.

First, the blocking variable is not portable as written. anchors() takes vol_anchor as
atr_14/close read from the legacy bars table at the T-2 close. The Massive coarse layer carries
only OHLCV and coverage fields — no atr_14. Porting the block structure therefore requires a
Massive-native ATR-14 whose definition provably matches the legacy column, and that feature does
not exist. Substituting a plausible ATR would be inventing a blocking variable.

Second, the claim layer does not exist on Massive. The historical family is 157 searchable tokens
over three position families yielding k = 35,715 candidate claims, all evaluated on the legacy
15m token surface. Massive has T states; it does not have that token layer. The family cannot be
enumerated, so multiplicity cannot be bound to a member count.

WHAT IS NOT A BLOCKER. The population is settled: 8,588 Massive-native anchors with complete
exact opening-hour support. Historical overlap stays a flag at 8,117 and never a filter.
"""
from __future__ import annotations
import json, os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART

BIND = {
    "MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1.json": "6d733e1fa3bf3c3d",
    "MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1.json": "819329bf48bd7175",
    "MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1_QUALIFICATION.json": "ded5c340dfb9fd6a",
    "MASSIVE_T1_DAILY_EPISODE_ANCHOR_PORT_V1.json": "26e7eda6ffd22a95",
    "MASSIVE_T1_OPENING_HOUR_SUPPORT_MAP_V1.json": "c47f6decfd4cd7af",
    "MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_CORRECTION_REBIND.json": "d90a9c3a20f9a838",
    "MASSIVE_T_FEATURE_PRODUCTION_V1.json": "ccdb0e35490794da",
    "MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1.json": "4a19b83f40adc6cc",
    "T1_OUTCOME_SPEC_V1.json": None, "T1_15M_ESTIMAND_V1.json": None,
    "T1_15M_PRESPEC_V1.json": "71d49dcde8921eb3", "T1_15M_X_ONLY_V1.json": None,
}
bad = [f for f, w in BIND.items() if w and ART.file_digest(f) != w]
if bad:
    print("HOLD — binding mismatch:", bad); raise SystemExit(1)
oc = json.load(open("T1_OUTCOME_SPEC_V1.json"))
est = json.load(open("T1_15M_ESTIMAND_V1.json"))
pre = json.load(open("T1_15M_PRESPEC_V1.json"))
xo = json.load(open("T1_15M_X_ONLY_V1.json"))

gaps = [
    dict(id="BLOCKING_VARIABLE_NOT_PORTABLE",
         what="anchors() sets vol_anchor = atr_14 / close, read from legacy bars at the T-2 "
              "close",
         why_blocked="the Massive coarse layer carries only OHLCV and coverage fields — there "
                     "is no atr_14",
         requires="a Massive-native ATR-14 whose definition provably matches the legacy column",
         forbidden="substituting a plausible ATR — that would be inventing a blocking variable",
         also_affects="MFE_ATR_10D, the registered secondary characterization"),
    dict(id="CLAIM_LAYER_ABSENT_ON_MASSIVE",
         what=f"the historical family is {xo['tokens']['searchable']} searchable tokens over "
              f"three position families, k_final_candidate {xo['enumeration']['k_final_candidate']}",
         why_blocked="those claims are evaluated on the legacy 15m TOKEN surface; Massive has "
                     "T states but not that token layer",
         consequence="the family cannot be enumerated, so multiplicity cannot be bound to a "
                     "member count",
         forbidden="registering the three position pairs as three tests — the historical "
                   "family was never three tests"),
]

p = dict(
    report_id="MASSIVE_T_PRE_Y_ANALYSIS_DICTIONARY_V1",
    status="HOLD",
    hold_reason="two execution-critical components of the historical estimand are not "
                "portable to Massive as written",
    task_class="PRE_Y_DICTIONARY_RECOVERY",
    bindings={k: v for k, v in BIND.items()},

    current_population=dict(
        security_cohort=476,
        massive_native_t1_anchor_sessions=8588,
        opening_hour_support=8588,
        historical_positive_overlap=dict(value=8117, status="DIAGNOSTIC_ONLY"),
        massive_positive_not_in_preserved_set=dict(value=471,
                                                   status="NOT_HISTORICAL_FALSE"),
        primary_population="8,588 — NOT the historical intersection"),

    recovered_exactly=dict(
        outcome=dict(signal=oc["signal"], decision=oc["decision"],
                     entry_semantics=oc["entry_semantics"], primary=oc["primary"],
                     secondary_registered=oc["secondary_registered"],
                     descriptive_only=oc["descriptive_only"],
                     not_promotion_paths=oc["not_promotion_paths"],
                     exit_policy=oc["exit_policy"], formulas=oc["formulas"],
                     builder=oc["builder"]),
        population_filters=["path_status_10d == AVAILABLE (STATUS only, no value)",
                            "n20 >= 20 prior sessions"],
        anchors=dict(measured_at="T-2 close, strictly before the window opens",
                     liquidity="median(close*volume) over 20 rows",
                     volatility="atr_14 / close",
                     dedup="row_number by universe preference sp500<nasdaq<russell2k, rn=1"),
        blocks=dict(rule="block = t5_date | liq_half | vol_half",
                    halves="median split by rank WITHIN each decision date",
                    historical_block_count=est["population"]["blocks"]),
        statistic=est["statistic"], direction=est["direction"],
        multiplicity=est["multiplicity"], n_perm=est["n_perm"],
        promotion=est["promotion"], raw_theta=est["raw_theta"],
        grammar=dict(scope=pre["primary_scope"],
                     position_families=pre["position_families"],
                     grammar=pre["grammar"], excluded=pre["excluded"],
                     evidence_status=pre["evidence_status"])),

    temporal_reading_refined=dict(
        anchor_known_at="D close",
        opening_hour_features_known_by="D close",
        entry="D+1 open (NEXT_SESSION_OPEN_V1)",
        consequence="every X input precedes the decision point; there is no lookahead in the "
                    "outcome path",
        classification_retained="POST_EXPOSURE_SESSION_LOCALIZATION — the anchor is still not "
                                "knowable at M1",
        correction="an earlier phrasing risked implying the construct is unimplementable; it "
                   "is not — it is simply not decidable at M1"),

    execution_critical_gaps=gaps,

    not_blockers=["the population — 8,588 anchors with complete exact opening-hour support",
                  "the outcome formula", "the entry semantics", "the blocking RULE",
                  "the statistic and multiplicity PROCEDURE"],

    forbidden_if_resumed=[
        "using 8,117 historical overlap as the primary population",
        "adding the 3-bar grammar (excluded from V1)",
        "adding M1->M3, M1->M4 or any unregistered pair",
        "treating a position pair as a T-state transition",
        "opening an all-session or all-12-label search",
        "appending EMA or RVOL conditions absent from the historical prespec",
        "collapsing NONE into UNAVAILABLE",
        "calling the preserved-set complement a historical negative",
        "promoting the 1D anchor to a standalone hypothesis",
        "resetting the historical exposure ledger"],

    historical_provenance=["PREVIOUSLY_EXPOSED_HYPOTHESIS",
                           "CURRENT_PRE_REGISTERED_SOURCE_COHORT_PORT", "REPLICATION_LIKE"],
    independence_limitation=pre["evidence_status"],

    z_firewall="HOLD — this dictionary is T1-only",
    y_firewall=dict(y_values_read=0, future_bars_read=0, theta_computed=0,
                    permutations_run=0),
    y_exposed=0, analysis_writes=0, outcome_exposure="NOT_EXPOSED",

    next_required=["port ATR-14 to Massive 1D with a definition proven to match the legacy "
                   "atr_14 column, OR register a different blocking variable pre-Y",
                   "build the Massive 15m token layer, or register a different claim family "
                   "with its own multiplicity treatment"],
    sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

d = ART.seal(p, "MASSIVE_T_PRE_Y_ANALYSIS_DICTIONARY_V1.json",
             required=("report_id", "status", "current_population", "recovered_exactly",
                       "execution_critical_gaps", "temporal_reading_refined", "y_firewall"),
             supersede=os.path.exists("MASSIVE_T_PRE_Y_ANALYSIS_DICTIONARY_V1.json"))
print(f"MASSIVE_T_PRE_Y_ANALYSIS_DICTIONARY_V1 · {d} · {p['status']}")
print("  RECOVERED   outcome (MFE_10D, NEXT_SESSION_OPEN_V1, exit NONE) · population filters")
print("              anchors (T-2 close) · blocks (date|liq_half|vol_half) · statistic")
print("              multiplicity (max-Z_rank, 999 perms, p95 strictly-greater) · grammar")
print("  POPULATION  8,588 anchors · overlap 8,117 DIAGNOSTIC_ONLY · 471 not-historical-false")
print("  TEMPORAL    entry = D+1 open -> no lookahead in the Y path; anchor still not")
print("              knowable at M1 (POST_EXPOSURE_SESSION_LOCALIZATION retained)")
print("  GAPS        1) atr_14 blocking variable absent from Massive coarse (OHLCV only)")
print("              2) the 157-token / k=35,715 claim layer does not exist on Massive")
print("  y_values_read 0 · permutations_run 0 · analysis_writes 0")
