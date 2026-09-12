"""MASSIVE_T_LEGACY_15M_AUTHORITY_CORRECTION_V1 — one correction, on a closed evidence surface.

Three sealed absence claims in this programme were wrong, all in one shape: a bounded search
returned nothing and the nothing was published as non-existence. Rather than a fourth incremental
amendment that a fifth discovery could invalidate, this is written only after the search space
was enumerated and closed — MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1 reports
enumeration_complete with zero permission failures, zero unreadable databases in scope and zero
schema failures, which is what makes any statement here about what exists or does not exist
admissible at all.

WHAT THE CLOSED SURFACE SHOWS. The legacy materialized signal family exists at FIVE timeframes —
1D, 1H, 4H, 15m, 1W — each carrying t_sig, z_sig, l_sig, full_suffix and vol_bucket, with
studio_15m.duckdb holding 90,948,070 bar-grain rows whose t_sig varies within the session. So a
legacy materialized 15m T population exists, and legacy-15m population equivalence is estimable
on matched support. The claims that it did not exist, and that the divergence was not estimable,
are FALSE — not merely outdated, and the distinction matters: they were not superseded by new
facts, they were wrong when written.

DISCOVERY IS NOT REGISTRATION. Finding materialized T at five timeframes does not grant any of
them research authorization. Every discovered surface is classified LEGACY_DATA_SURFACE_DISCOVERED
and nothing more, so that an inventory cannot quietly become a search-space expansion.

WHAT SURVIVES UNTOUCHED. Layer 1 exactness, the port hash, the canonical config, T production and
its independent verification rest on Massive inputs and same-input semantics; none of them
depended on the false premise. The 15m source-divergence measurement stands as a measurement —
only the sentence framing it as impossible-until-now was wrong.
"""
from __future__ import annotations
import json, os, sys, time                                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

INV, INV_D = "MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json", "5ebcad54f619f97d"
PRES, PRES_D = ("MASSIVE_T1_HISTORICAL_EPISODE_POPULATION_PRESERVATION_V1.json",
                "4a19b83f40adc6cc")


def main():
    for f, w in ((INV, INV_D), (PRES, PRES_D)):
        if ART.file_digest(f) != w:
            print(f"HOLD — binding mismatch {f}"); return 1
    inv = json.load(open(INV))
    g = inv["completeness_guard"]
    if not g["enumeration_complete"] or not g["negative_existence_claims_permitted"]:
        print("HOLD — the evidence surface is not closed; no correction may be written on it")
        return 1

    tf = {}
    for s in inv["signal_bearing_tables"]:
        if "t_sig" in s["signal_columns"] and "/data/studio_" in s["store"]:
            tf[s["store"].split("/")[-1]] = dict(rows=s["rows"], time_range=s["time_range"])

    affected = [
        dict(artifact="MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V1",
             digest="c6358576c5c9411b",
             claim="15M_LEGACY_VS_MASSIVE_FEED_DIVERGENCE = NOT_ESTIMABLE_FROM_AVAILABLE_"
                   "MATCHED_DATA",
             classification="FALSE",
             why="a legacy 15m OHLC store and a legacy materialized 15m t_sig both existed "
                 "when it was written"),
        dict(artifact="MASSIVE_T_PORT_CONFORMANCE_DIAGNOSTIC_SCOPE_AMENDMENT_V2",
             digest="1582a8d21042c6a8",
             claim="LEGACY_15M_MATERIALIZED_T_POPULATION = DOES_NOT_EXIST",
             classification="FALSE",
             why="studio_15m.duckdb.bars.t_sig is bar-grain, varies within the session and "
                 "carries all twelve T labels",
             note="this amendment corrected half of the original error and re-asserted the "
                  "other half"),
        dict(artifact="MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1", digest="4c9b15eb8cfed786",
             claim="the 15m limitation carried in its decision text",
             classification="REQUIRES_REBIND",
             why="its ACCEPT rested on invariants that still hold, but one recorded premise "
                 "was false"),
        dict(artifact="MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_1",
             digest="cdd94787f424b5cb", claim="carried the same limitation forward",
             classification="REQUIRES_REBIND"),
        dict(artifact="MASSIVE_T_PORT_ACCEPTABILITY_DECISION_V1_AMENDMENT_2",
             digest="e8908bdf5d5d8a23",
             claim="new_limitation asserting population equivalence is unavailable",
             classification="REQUIRES_REBIND",
             why="the measured divergence it records is sound; the limitation framing is not"),
        dict(artifact="MASSIVE_T_PORT_SMOKE_FEED_CONFORMANCE_V1", digest="49eb9658eda72773",
             claim="Layer 2 was run at 1D as LEGACY_1D_MATERIALIZER_STORAGE_CONFORMANCE",
             classification="SUPERSEDED",
             why="Layer 2 belongs at 15m, the research timeframe, and was placed at 1D only "
                 "because 15m was believed impossible",
             unaffected_parts=["Layer 1 exactness", "all fixtures", "the 15m capability "
                               "census", "config and timeframe guards"]),
        dict(artifact="MASSIVE_T_15M_SOURCE_DIVERGENCE_DIAGNOSTIC_V1",
             digest="53c92f9df3b40d26",
             claim="'a legacy 15m MATERIALIZED T population genuinely does not exist'",
             classification="FALSE_FRAMING_VALID_MEASUREMENT",
             why="the engine-vs-engine measurement (0.945539 on 10,458,803 bars) stands; only "
                 "the sentence declaring the materialized comparison impossible was wrong"),
        dict(artifact="MASSIVE_T_15M_EPISODE_DEFINITION_RECOVERY_V1",
             digest="73375ec0d8ab5344",
             claim="POPULATION_NOT_REPRODUCIBLE_FROM_CURRENT_STORE",
             classification="TRUE_BUT_INCOMPLETE",
             completed_by=PRES_D,
             why="true of every mutable store, but the sealed population was never lost — it "
                 "is preserved as a materialized artifact in two independent copies"),
        dict(artifact="MASSIVE_T_FEATURE_PRODUCTION_V1", digest="ccdb0e35490794da",
             classification="UNAFFECTED",
             why="it rests on Massive inputs and same-input semantics; no false premise "
                 "entered it"),
        dict(artifact="MASSIVE_T_FEATURE_PRODUCTION_VERIFY_V1", digest="2bc48ac417c2cbc9",
             classification="UNAFFECTED"),
        dict(artifact="MASSIVE_T1_EPISODE_TEMPORAL_SEMANTICS_V1", digest="819329bf48bd7175",
             classification="UNAFFECTED",
             why="every claim in it is positive evidence read from generator SQL"),
    ]

    p = dict(
        report_id="MASSIVE_T_LEGACY_15M_AUTHORITY_CORRECTION_V1",
        status="LEGACY_15M_AUTHORITY_CORRECTED",
        task_class="CONSOLIDATED_CORRECTION_ONLY",
        written_against=dict(
            inventory=INV_D, enumeration_complete=True,
            negative_existence_claims_permitted=True,
            preservation=PRES_D,
            why_it_waited="writing this on a partially open surface would have repeated the "
                          "exact defect class the inventory exists to end"),

        error_class=dict(
            name="NEGATIVE_SEARCH_WITHOUT_ENUMERATION",
            instances=[
                "`timeout 60 grep …` never ran on macOS; empty output was read as 'no matches' "
                "and LOCAL_PYTHON was sealed as REFUTED",
                "`bars` was proven daily and generalised into 'no legacy 15m store exists'",
                "studio_15m_base.duckdb was found (OHLCV-only), half the claim was corrected, "
                "and the other half re-asserted — while `data/` was never enumerated"],
            common_shape="a bounded search returned nothing and the nothing was published as "
                         "non-existence",
            structural_fix="MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1 makes permission to "
                           "claim absence depend on a measured property of the search",
            not_fixed_by="being more careful — care is what failed three times"),

        new_canonical_facts=dict(
            LEGACY_SIGNAL_STORE_FAMILY="EXHAUSTIVELY_INVENTORIED",
            LEGACY_15M_MATERIALIZED_T_POPULATION="EXISTS",
            authority_surface="data/studio_15m.duckdb :: bars.t_sig",
            legacy_15m_t_sig=dict(grain="bar-grain", varies_intraday=True,
                                  labels="the twelve canonical T labels",
                                  rows=tf.get("studio_15m.duckdb", {}).get("rows")),
            LEGACY_15M_MATERIALIZED_POPULATION_EQUIVALENCE="ESTIMABLE_ON_MATCHED_SUPPORT",
            HISTORICAL_T1_EPISODE_POPULATION="PRESERVED_EXACTLY",
            CURRENT_STORE_REGENERATION="NOT_AUTHORITATIVE",
            timeframes_discovered=tf),

        discovery_is_not_registration=dict(
            status_granted="LEGACY_DATA_SURFACE_DISCOVERED",
            status_NOT_granted="REGISTERED_RESEARCH_TIMEFRAME",
            applies_to=sorted(tf),
            why="otherwise a data inventory silently becomes a search-space expansion",
            research_state_timeframe_remains="15m",
            episode_selector_timeframe_remains="1D (anchor role only)"),

        affected_sealed_claims=affected,
        no_artifact_edited=True,
        classification_legend=["FALSE", "FALSE_FRAMING_VALID_MEASUREMENT", "SUPERSEDED",
                               "TRUE_BUT_INCOMPLETE", "REQUIRES_REBIND", "UNAFFECTED"],

        what_becomes_possible=dict(
            A="legacy materialized 15m t_sig",
            B="canonical engine on legacy 15m OHLC",
            C="canonical engine on Massive 15m OHLC",
            A_vs_B="LEGACY_15M_MATERIALIZER_STORAGE_CONFORMANCE — the true Layer 2, at the "
                   "research timeframe",
            B_vs_C="source-induced same-engine divergence (already measured: 0.945539)",
            A_vs_C="observed historical-vs-Massive population divergence — the comparison the "
                   "chain declared impossible",
            decomposition_note="A↔C is NOT source divergence alone; it carries the "
                               "materializer/storage component and any input-vintage "
                               "component as well",
            support_requirement="report CURRENT_BAR_MATCHED and "
                                "CURRENT_AND_REQUIRED_PREDECESSOR_MATCHED separately; the "
                                "primary semantic comparison belongs on the second",
            position_decomposition="M1 / M2 / M3 / M4 / SESSION_FINAL / OTHER, because the "
                                   "registered historical claims are opening-hour "
                                   "position-pairs",
            forbidden=["nearest-timestamp matching", "forward fill",
                       "session-reset predecessors", "correcting Massive labels toward legacy",
                       "weighting observations by agreement",
                       "reading divergence as a Y edge"]),

        gates=dict(a_b_c_diagnostic="AUTHORIZED_NEXT", acceptability_rebind="REQUIRED_AFTER",
                   c3_anchor_port="HOLD", pre_y_dictionary="HOLD", z="HOLD", y="HOLD"),
        production_disposition=dict(artifact="MASSIVE_T_FEATURE_PRODUCTION_V1",
                                    digest="ccdb0e35490794da",
                                    status="BYTE_VALID / SEMANTICALLY_VERIFIED",
                                    downstream_authorization="HOLD_PENDING_CORRECTION_REBIND",
                                    rebuild_required=False),
        y_exposed=0, outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_LEGACY_15M_AUTHORITY_CORRECTION_V1.json",
                 required=("report_id", "status", "written_against", "error_class",
                           "new_canonical_facts", "affected_sealed_claims",
                           "discovery_is_not_registration", "what_becomes_possible"),
                 supersede=os.path.exists("MASSIVE_T_LEGACY_15M_AUTHORITY_CORRECTION_V1.json"))
    print(f"MASSIVE_T_LEGACY_15M_AUTHORITY_CORRECTION_V1 · {d} · {p['status']}")
    print(f"  surface     enumeration COMPLETE ({INV_D}) — absence claims admissible")
    print(f"  legacy T at {len(tf)} timeframes: "
          f"{', '.join(k.replace('studio_','').replace('.duckdb','') for k in sorted(tf))}")
    print(f"  15m t_sig   {tf.get('studio_15m.duckdb', {}).get('rows', 0):,} bar-grain rows")
    for a in affected:
        print(f"    {a['classification']:32s} {a['artifact']}")
    print(f"  discovery   LEGACY_DATA_SURFACE_DISCOVERED — NOT research registration")
    print(f"  next        A/B/C 15m decomposition, then computed acceptability rebind")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
