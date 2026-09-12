"""MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V2 — the limitation, corrected.

AMENDMENT_V1 froze 15M_LEGACY_VS_MASSIVE_FEED_DIVERGENCE = NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_
DATA. The premise under it was that `bars` is structurally daily, which is true and remains
proven. The conclusion drawn from it was not, because this comparison never required a legacy T
column at all — it runs the canonical engine on both sides — and a legacy 15m OHLC store existed
the whole time. Two different claims were collapsed into one, and the search stopped too early.
V1 is not edited; its limitation is superseded here.

THE SPLIT IS PERMANENT AND IS THE POINT OF THIS AMENDMENT:
    legacy 15m MATERIALIZED T population        -> DOES NOT EXIST      (unchanged, still true)
    legacy-15m population EQUIVALENCE           -> NOT ESTIMABLE       (follows from the above)
    same-engine cross-source 15m DIVERGENCE     -> ESTIMATED, 0.945539 agreement

AND THE MEASUREMENT MATTERS RATHER THAN MERELY EXISTING. Direct 15m divergence is 5.4461% against
2.4% at 1D, so carrying the 1D figure across would have HALVED the real effect. The rule against
imputation was not bookkeeping; it was load-bearing. The 1D diagnostic is demoted to supporting
evidence — it is no longer the only cross-source evidence available.

POSITION DECOMPOSITION IS RECORDED BECAUSE THE HISTORICAL FAMILY IS OPENING-HOUR ANCHORED. The
registered historical study is built on M1->M2 / M2->M3 / M3->M4, and agreement is markedly
uneven across exactly those positions (M1 0.9941 vs M2 0.9278). It is recorded as an X-side fact
with NO outcome interpretation attached.

ATTRIBUTION IS DELIBERATELY UNDER-CLAIMED. The ~1.1% of bars differing by more than a cent look
like corporate-action vintage and cluster the same way they did at 1D, but no split/spin-off or
symbol-history authority has been joined at row level, so the class is PLAUSIBLE /
STRONGLY_SUGGESTED and explicitly NOT CONFIRMED.

A DEFECT OF MINE IS RECORDED HERE RATHER THAN TIDIED AWAY. The first run of the diagnostic
matched zero bars — legacy timestamps are datetime64[us] and were divided as if nanoseconds,
yielding seconds against the store's milliseconds — and it SEALED ANYWAY on zero support. That
artifact is retained as INVALID_ZERO_SUPPORT, and zero matched support is now a HOLD.
"""
from __future__ import annotations
import json, os, sys, time                                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

V1, V1_D = ("MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1.json",
            "c6358576c5c9411b")
DIAG, DIAG_D = "MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1.json", "53c92f9df3b40d26"
ZERO_D = "4f546fe5d8a89675"
SMOKE, SMOKE_D = "MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1.json", "49eb9658eda72773"
PROD, PROD_D = "MASSIVE_T_FEATURE_PRODUCTION_V1.json", "ccdb0e35490794da"


def main():
    for f, w in ((V1, V1_D), (DIAG, DIAG_D), (SMOKE, SMOKE_D), (PROD, PROD_D)):
        if ART.file_digest(f) != w:
            print(f"HOLD — digest mismatch {f}: {ART.file_digest(f)}"); return 1
    zpath = f"{DIAG}.superseded.{ZERO_D}.json"
    if not os.path.exists(zpath):
        print("HOLD — the zero-support run must be retained as defect history"); return 1
    d = json.load(open(DIAG))
    z = json.load(open(zpath))
    if z["support"]["bars_compared"] != 0:
        print("HOLD — the retained artifact is not the zero-support run"); return 1

    p = dict(
        amendment_id="MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V2",
        status="MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V2_FROZEN",
        task_class="LIMITATION_CORRECTION_ONLY",
        amends=dict(artifact="MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1",
                    digest=V1_D, disposition="FROZEN — NOT edited",
                    scope_of_supersession="its 15m feed-divergence LIMITATION only; its "
                                          "research/diagnostic timeframe registry is "
                                          "unchanged"),

        superseded_limitation=dict(
            old="15M_LEGACY_VS_MASSIVE_FEED_DIVERGENCE = "
                "NOT_ESTIMABLE_FROM_AVAILABLE_MATCHED_DATA",
            why_the_premise_failed="the premise (bars is structurally daily) is TRUE and still "
                                   "proven; the inference from it was wrong, because this "
                                   "comparison runs the canonical engine on BOTH sides and "
                                   "never needed a legacy T column — and a legacy 15m OHLC "
                                   "store existed all along",
            error_class="TWO_CLAIMS_COLLAPSED_INTO_ONE + SEARCH_STOPPED_TOO_EARLY"),

        new_frozen_limitation=dict(
            LEGACY_15M_MATERIALIZED_T_POPULATION="DOES_NOT_EXIST",
            LEGACY_15M_MATERIALIZED_POPULATION_EQUIVALENCE="NOT_ESTIMABLE",
            SAME_CANONICAL_ENGINE_CROSS_SOURCE_15M_DIVERGENCE="ESTIMATED",
            authority=dict(artifact="MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1",
                           digest=DIAG_D),
            matched_bars=d["support"]["bars_compared"],
            matched_securities=d["support"]["securities_compared"],
            t_agreement=d["agreement"]["t_label_agreement"],
            t_divergence=round(1 - d["agreement"]["t_label_agreement"], 6),
            predecessor_also_matched_agreement=d["agreement"][
                "predecessor_also_matched"]["agreement"],
            support_loss_by_reason=d["support"]["loss_by_reason"]),

        does_not_establish=["same historical materialized T population",
                            "legacy 15m label reproduction",
                            "feed equivalence",
                            "independent replication"],

        one_d_diagnostic_demoted=dict(
            was="the only cross-source evidence available",
            now="SECONDARY_SUPPORTING_DIAGNOSTIC",
            one_d_t_agreement=0.975711, fifteen_m_t_agreement=d["agreement"][
                "t_label_agreement"],
            why_it_matters="direct 15m divergence is 5.4461% against 2.4% at 1D, so carrying "
                           "the 1D figure across would have HALVED the real effect; the "
                           "no-imputation rule was load-bearing, not bookkeeping"),

        position_decomposition=dict(
            values={k: v["agreement"] for k, v in d["by_session_position"].items()},
            why_recorded="the registered historical study is opening-hour anchored on "
                         "M1->M2 / M2->M3 / M3->M4, and agreement is markedly uneven across "
                         "exactly those positions",
            interpretation="X_SIDE_FACT_ONLY — no outcome interpretation is attached",
            outcome_interpretation_forbidden=True),
        body_size_decomposition={k: v["agreement"] for k, v in d["by_body_size"].items()},

        attribution=dict(
            INPUT_VINTAGE_DIFFERENCE="PLAUSIBLE / STRONGLY_SUGGESTED",
            confirmed=False,
            what_would_confirm="joining a split / spin-off / symbol-history authority to the "
                               "differing rows at row level",
            observation="~1.1% of matched bars differ by more than one cent, clustering the "
                        "same way they did at 1D",
            UNRESOLVED_remains_valid=True,
            forbidden="calling it 'rounding' or 'noise'"),

        my_defect_recorded=dict(
            defect="the first run of the 15m diagnostic matched ZERO bars and sealed anyway",
            cause="legacy timestamps are datetime64[us] and were divided as if nanoseconds, "
                  "yielding SECONDS against the store's MILLISECONDS — a 1000x unit mismatch",
            severity="FAIL_OPEN — a diagnostic that matches nothing is a harness defect, not "
                     "a measurement",
            retained_artifact=zpath,
            retained_status="INVALID_ZERO_SUPPORT / SUPERSEDED — not deleted, not overwritten",
            fix="explicit conversion through datetime64[ms] so the unit cannot depend on the "
                "source resolution",
            new_negative_fixture=dict(id="N_ZERO_MATCHED_SUPPORT",
                                      rule="matched_support == 0 => HOLD, sealing forbidden",
                                      implemented=True)),

        unchanged_from_v1=["research port timeframe 15m ONLY",
                           "1D remains DIAGNOSTIC_ONLY_NOT_HYPOTHESIS_REGISTRATION",
                           "the six forbidden consequences",
                           "research_family_enlarged = False",
                           "SESSION_CLOSE_BOUNDARY_BAR stays neutral"],
        research_family_enlarged=False,
        port_hash_unchanged="0ca86eeb1f8ee8fe",
        production_rebuild_required=False,
        why_no_rebuild="the diagnostic changed no engine, no port hash, no config and no "
                       "Massive input semantics; T production remains byte-valid",
        downstream_authorization="HOLD until the acceptability rebind",
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    dg = ART.seal(p, "MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V2.json",
                  required=("amendment_id", "status", "superseded_limitation",
                            "new_frozen_limitation", "does_not_establish",
                            "position_decomposition", "my_defect_recorded"),
                  supersede=os.path.exists(
                      "MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V2.json"))
    n = p["new_frozen_limitation"]
    print(f"MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V2 · {dg} · {p['status']}")
    print(f"  supersedes  the 15m LIMITATION of {V1_D} only (V1 not edited)")
    print(f"  materialized-population equivalence : NOT_ESTIMABLE (unchanged)")
    print(f"  cross-source 15m divergence         : ESTIMATED {n['t_divergence']} "
          f"on {n['matched_bars']:,} bars / {n['matched_securities']} securities")
    print(f"  1D demoted  2.4% -> SECONDARY (15m is 5.4461%, so 1D would have HALVED it)")
    print(f"  position    {p['position_decomposition']['values']}")
    print(f"  attribution INPUT_VINTAGE_DIFFERENCE = PLAUSIBLE, NOT CONFIRMED")
    print(f"  defect      zero-support fail-open recorded · artifact retained")
    print(f"  rebuild     production rebuild required = False")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
