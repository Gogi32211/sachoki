"""MASSIVE_T_MATERIALIZER_AUTHORITY_RESOLUTION_V1 — three surfaces searched, producer not found.

You asked me to settle the authority by tracing what actually produced canonical `t_sig`,
explicitly NOT by which implementation agrees better with the column. Three candidate surfaces
have now been searched to exhaustion. The downstream half of the chain is proven; the upstream
half is not, and no further local searching will change that.

    [ PRODUCER — NOT IDENTIFIED ]
        -> {sp500,nasdaq,russell2k}_signals_5y.csv     2026-05-29
        -> studio/importer.py:41   "T" -> t_sig
        -> bars.t_sig              CANONICAL
        -> importer.py:372         sig_t1..sig_t12 derived FROM t_sig

WHAT WAS REFUTED, AND HOW.

Local Python. No module in backend/ emits any of the CSV's uppercase column names as a literal,
and nothing anywhere references the filename `signals_5y`. Neither signal_logic.py nor
signal_engine.py wrote the column.

Local Pine. Every on-disk TZ Pine file has ZERO plot/plotchar/plotshape calls — output is
label.new() and alertcondition() only. A TradingView chart-data export emits PLOTTED SERIES;
labels are not exportable columns, so these files cannot produce a CSV column at all.
Independently: the importer maps 74 CSV columns and only 9 of them appear anywhere in
260523_TZ_F_WLNBB_CMB_pattern.pine. And every on-disk TZ Pine has an mtime AFTER the import.

TradingView cloud. 932 saved scripts, all 932 sources fetched and scanned, zero fetch errors.
A TradingView export header equals the plot titles, so a producer must contain the CSV column
names as quoted plot titles. The UNION across all 932 scripts covers 27 of 74. Forty-seven
columns appear in no cloud script whatsoever — whole families: TURBO_SCORE*, RTB_*, WYC_*,
GOG_TIER/SCORE, BETA_*, PREBREAK_*, PB_*, PRICE_GT/LT_*, RSI_GE/LE_*, FINAL_BULL_SCORE,
FINAL_REGIME, ALL_SIGNALS, ULT. No script is named EXPORT or CSV. So the composite CSV was not
produced by any single cloud script, and not by any combination of them either.

I ALSO REFUTED MY OWN PRIOR DRAFT. An unsealed draft of this artifact named Pine 260523 as
CANONICAL_LEGACY_MATERIALIZER. The plot-count and vocabulary evidence above refutes it. It was
never sealed, so nothing was rewritten; the hypothesis is recorded here as tested and refuted.

WHAT SURVIVES AS A CANDIDATE, WITHOUT BEING ELEVATED. Twenty-one cloud scripts both plot "T" as
a titled series and carry the T-family logic (bullCode / T*_raw / tBase), all of them using
minBodyRatio, syminfo.mintick and isDoji. The broadest is `260501 GOG Priority Engine —
A/SM + N/MX + SQB/BCT/LD` (2026-05-01, 47 plots, 14 of the 74 columns including T and Z). That
is a candidate for the T COLUMN specifically. It is not the producer of the 74-column file, and
nothing ties it to the 2026-05-29 export. Per your rule, LIKELY is not promoted to AUTHORITY.

THE ONE THING THIS ARTIFACT MUST NOT BLUR: bars.t_sig remains canonical. Its status as the
canonical historical materialized observation does not depend on our being able to name its
producer. What is missing is its executable semantics recovered BY PROVENANCE — which is
exactly the claim the Massive port is therefore not allowed to make.

NO T4 AMENDMENT IS ISSUED HERE, by your instruction: if the producer were later identified, an
amendment written now would need its own amendment. Deferred, with the defect recorded.
"""
from __future__ import annotations
import json, os, re, sys, time                                            # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

MQ, MQ_D = "MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1.json", "0cad620d6a5fb7bc"
AM1_D = "de60bdc75fc61e3c"
T4, T4_D = "T4_ENGULF_CONFIG_V1.json", "713f09144a245408"
PORT_D, REC_D = "545d293b4f1e4653", "7d00e3908feb26d5"
PINE = "../analysis/260523_TZ_F_WLNBB_CMB_pattern.pine"

IMMUTABLE = {"MASSIVE_T_FEATURE_PORT_SPEC_V1.json": PORT_D,
             "MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1.json": AM1_D,
             "MASSIVE_T_PINE_SEMANTIC_RECOVERY_V1.json": REC_D,
             T4: T4_D, MQ: MQ_D}

# The 47 CSV columns that exist in NO TradingView cloud script.
ABSENT_EVERYWHERE = [
    "AD_FRESH", "ALL_SIGNALS", "ALREADY_EXTENDED_FLAG", "BETA_SCORE", "BETA_ZONE", "BE_UP",
    "BO_UP", "BX_UP", "FINAL_BULL_SCORE", "FINAL_REGIME", "GOG_SCORE", "GOG_TIER", "HILO_BUY",
    "PB_LVBO", "PB_MACRO_PENALTY", "PB_STOP_CAUSE", "PB_WVF_CONFIRM", "PREBREAK_PRIME",
    "PREBREAK_READY", "PREBREAK_WATCH", "PRICE_GT_20", "PRICE_GT_200", "PRICE_GT_50",
    "PRICE_GT_89", "PRICE_LT_20", "PRICE_LT_200", "PRICE_LT_50", "PRICE_LT_89", "RSI_GE_70",
    "RSI_LE_35", "RTB_PHASE", "RTB_TOTAL", "SIG_260308", "SWING_TYPE", "THREE_G",
    "TURBO_SCORE", "TURBO_SCORE_N10", "TURBO_SCORE_N3", "TURBO_SCORE_N5", "ULT", "VBO_UP",
    "VOL_BUCKET", "WYC_IN_TR", "WYC_PHASE", "WYC_SOS", "WYC_SOW", "WYC_SPRING"]


def verify_immutable():
    out, broken = {}, []
    for f, want in IMMUTABLE.items():
        got = ART.file_digest(f) if os.path.exists(f) else None
        out[f] = dict(cited=want, actual=got, unchanged=(got == want))
        if got != want:
            broken.append(f)
    return out, broken


def main():
    imm, broken = verify_immutable()
    if broken:
        print("HOLD — a frozen artifact changed on disk:", broken); return 1
    t4 = json.load(open(T4))

    # Re-verify the downstream half from source rather than restating it.
    ev = {}
    for path, pat, label in (
            ("studio/db.py", r"t_sig\s+VARCHAR,\s*--\s*from 'T' column", "schema comment"),
            ("studio/importer.py", r'"T":\s*"t_sig"', "column mapping")):
        ev[path] = dict(found=bool(os.path.exists(path)
                                   and re.search(pat, open(path).read())), evidence=label)
    pine_plots = None
    if os.path.exists(PINE):
        pine_plots = len(re.findall(r"\bplot[a-z]*\s*\(", open(PINE).read()))

    p = dict(
        report_id="MASSIVE_T_MATERIALIZER_AUTHORITY_RESOLUTION_V1",
        status="UNRESOLVED",
        resolution_class="CANDIDATE_FOUND_PRODUCER_IDENTITY_NOT_PROVEN",
        task_class="PROVENANCE_RESOLUTION_ONLY",
        producer_identity_not_proven=True,

        csv_import_chain=dict(
            proven_segment=["{sp500,nasdaq,russell2k}_signals_5y.csv",
                            "studio/importer.py:41  \"T\" -> t_sig",
                            "bars.t_sig  (CANONICAL)",
                            "importer.py:372  sig_t1..sig_t12 derived FROM t_sig"],
            unproven_segment="whatever produced the CSV column \"T\"",
            evidence=dict(
                import_log=dict(rows=[4291639, 3049375, 741314],
                                universes=["russell2k", "nasdaq", "sp500"],
                                imported_at="2026-05-29",
                                meaning="bars was populated by IMPORT, not by executing any "
                                        "module in this repository"),
                schema_comment="studio/db.py:229  t_sig VARCHAR, -- from 'T' column",
                column_mapping="studio/importer.py:41  \"T\": \"t_sig\"",
                source_checks=ev),
            csv_files_no_longer_on_disk=True,
            consequence_of_that="the export header fingerprint is unrecoverable"),

        upstream_producer_search=dict(
            method="a TradingView CSV export header equals the PLOT TITLES, so any producer "
                   "must contain the CSV column names as quoted plot titles; and a script "
                   "with no plot calls cannot export a column at all",
            csv_columns_mapped_by_importer=74,
            surfaces=[
                dict(surface="LOCAL_PYTHON", verdict="REFUTED",
                     evidence=["no backend/*.py emits any CSV column name as a literal",
                               "no backend/*.py references the filename signals_5y"]),
                dict(surface="LOCAL_PINE", verdict="REFUTED",
                     evidence=[f"260523_TZ_F_WLNBB_CMB_pattern.pine plot-call count = "
                               f"{pine_plots} (output is label.new/alertcondition only)",
                               "all on-disk TZ Pine files have plot-call count 0",
                               "only 9 of the 74 CSV columns appear in the Pine at all",
                               "every on-disk TZ Pine mtime POSTDATES the 2026-05-29 import"]),
                dict(surface="TRADINGVIEW_CLOUD", verdict="REFUTED_AS_PRODUCER",
                     scripts_listed=932, sources_fetched=932, fetch_errors=0,
                     coverage="100%",
                     evidence=["union of quoted plot titles across ALL 932 scripts covers "
                               "27 of 74 CSV columns",
                               "47 of 74 columns appear in NO cloud script whatsoever",
                               "no script is named EXPORT or CSV",
                               "no single script exceeds 14 of the 74 columns"],
                     columns_absent_from_every_cloud_script=ABSENT_EVERYWHERE,
                     also_refutes="the multi-indicator export hypothesis — TradingView "
                                  "exports all visible indicators at once, but even the "
                                  "UNION of the entire corpus falls 47 columns short")],
            conclusion="the 74-column composite CSV was not produced by any surface "
                       "available to this investigation"),

        best_candidate=dict(
            scope="THE T COLUMN ONLY — not the 74-column file",
            family=dict(
                count=21,
                description="cloud scripts that BOTH plot \"T\" as a titled series AND carry "
                            "the T-family logic (bullCode / T*_raw / tBase)",
                all_use=["minBodyRatio", "syminfo.mintick", "isDoji"]),
            broadest=dict(name="260501 GOG Priority Engine — A/SM + N/MX + SQB/BCT/LD",
                          modified="2026-05-01", plot_calls=47,
                          csv_columns_present=14,
                          columns=["B", "F", "G1C", "G1L", "G1P", "G2C", "G2L", "G2P", "G3C",
                                   "G3P", "L", "SVS", "T", "Z"]),
            status="NOT_PROMOTED_TO_AUTHORITY",
            why_not=["nothing ties any of these to the 2026-05-29 export",
                     "none produces the 74-column composite",
                     "selection by naming similarity or by output agreement is forbidden"]),

        roles_fixed=dict(
            bars_t_sig="CANONICAL_HISTORICAL_MATERIALIZED_OBSERVATION",
            its_producer="CURRENTLY_UNKNOWN",
            signal_logic_py="NOT_PROVEN_PRODUCER",
            signal_engine_py="NOT_PROVEN_PRODUCER",
            pine_260523="NOT_PROVEN_PRODUCER",
            studio_db_py="STORAGE_AND_IMPORT_MAPPING_AUTHORITY_ONLY — not T semantic "
                         "authority",
            canonical_status_note="bars.t_sig does not lose canonical status because its "
                                  "producer could not be named; what is missing is its "
                                  "executable semantics recovered BY PROVENANCE"),

        hypotheses_tested_and_refuted=[
            dict(hypothesis="signal_logic.py is the canonical materializer",
                 origin="provisional direction, prior session", verdict="REFUTED",
                 why="it never wrote the column; its 99.14% agreement is not provenance"),
            dict(hypothesis="Pine 260523 is the canonical materializer",
                 origin="my own unsealed draft of THIS artifact", verdict="REFUTED",
                 why="zero plot calls, 9/74 columns, mtime postdates the import",
                 artifact_was_never_sealed=True, nothing_rewritten=True)],

        retractions=[
            dict(artifact="MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1", digest=MQ_D,
                 claim="1e-10 is the frozen authoritative value from the implementation that "
                       "produced the column",
                 status="RETRACTED",
                 why="no implementation is proven to have produced the column; the claim "
                     "presupposed a producer that was never established",
                 artifact_not_edited=True),
            dict(artifact="MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1", digest=MQ_D,
                 claim="TradingView is not the authority for this value, so no tick metadata "
                       "is needed",
                 status="WITHDRAWN_AS_UNSUPPORTED",
                 why="its basis (a proven Python producer) is refuted; this does NOT license "
                     "the opposite claim either, since the producer is unknown",
                 explicitly_not="an assertion that TradingView IS the authority",
                 artifact_not_edited=True)],
        superseded_authority_wise_not_historically=[MQ_D],

        mintick_status=dict(
            reopened=True, current_status="UNRESOLVED",
            amendment_1_vindicated=dict(artifact="MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1",
                                        digest=AM1_D, original_verdict="HOLD",
                                        assessment="correct; the later qualification of it "
                                                   "rested on a provenance error"),
            exposure_still_real=dict(
                measured="prevBody < $0.01 on 6.01% of sampled Massive 15m bars",
                gates="bodyRatioOk -> fullyEngulfs -> T4/T6, priority ranks 1 and 2"),
            observation_not_conclusion="every T-capable cloud script evaluates "
                                       "syminfo.mintick rather than an epsilon; this "
                                       "describes the Pine branch and cannot be attributed "
                                       "to an unidentified producer"),

        t4_config_defect=dict(
            artifact="T4_ENGULF_CONFIG_V1", digest=T4_D, status="FROZEN, not edited",
            semantic_claims=dict(use_wick=t4["use_wick"],
                                 min_body_ratio=t4["min_body_ratio"],
                                 doji=t4["doji_semantics"],
                                 assessment="UNCHANGED — not re-adjudicated here"),
            citation_defect=dict(
                cited=t4["source_identity"],
                problem="cites local reimplementations as though they were the "
                        "materialization's source",
                severity="MATERIAL"),
            mintick_claim_defect=dict(
                cited=t4["mintick_semantics"],
                problem="reports a reimplementation's epsilon as the materialization's "
                        "semantics",
                severity="MATERIAL"),
            compounding_hazard=dict(
                detail="on isDoji, signal_engine.py actively CONTRADICTS the claim it is "
                       "cited for (threshold 0.05 vs exact equality)",
                why_it_matters="isDoji feeds prev1IsBear, which gates T1, T3, T5, T9, T1G"),
            amendment_issued_here=False,
            amendment_deferred_until="producer identification succeeds or is abandoned",
            why_deferred="if the producer were later identified, an amendment written now "
                         "would itself need an amendment"),

        claim_language=dict(
            forbidden="the same canonical legacy materializer semantics",
            reason="no producer is proven, so no executable semantics can be claimed as "
                   "recovered by provenance",
            permitted_if_port_proceeds="a PRE-REGISTERED T-family definition ported from "
                                       "Pine 260523 as a SECONDARY reference, evaluated on "
                                       "Massive data, explicitly NOT claimed to reproduce "
                                       "canonical t_sig"),

        gates=dict(t_port_smoke="HOLD", t_production="HOLD", z="HOLD", y="HOLD",
                   t4_amendment="DEFERRED"),
        immutability_check=imm,
        y_exposed=0, t_production_writes=0, smoke_started=False,
        outcome_exposure="NOT_EXPOSED",
        search_exhausted=["local repository Python", "local Pine corpus",
                          "TradingView cloud script inventory (932/932)"],
        remaining_evidence_paths=[
            "recovery of the original CSV files or their header from backup",
            "a producer outside all three searched surfaces (another machine, a deleted "
            "script, or a non-TradingView tool)"],
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_MATERIALIZER_AUTHORITY_RESOLUTION_V1.json",
                 required=("report_id", "status", "csv_import_chain",
                           "upstream_producer_search", "best_candidate", "roles_fixed",
                           "retractions", "mintick_status", "t4_config_defect"),
                 supersede=os.path.exists(
                     "MASSIVE_T_MATERIALIZER_AUTHORITY_RESOLUTION_V1.json"))
    print(f"MASSIVE_T_MATERIALIZER_AUTHORITY_RESOLUTION_V1 · {d} · {p['status']}")
    print(f"  class       {p['resolution_class']}")
    print(f"  refuted     LOCAL_PYTHON · LOCAL_PINE · TRADINGVIEW_CLOUD (932/932, 0 errors)")
    print(f"  coverage    union of all 932 cloud scripts = 27/74 columns; "
          f"{len(ABSENT_EVERYWHERE)} columns absent everywhere")
    print(f"  candidate   21 T-plotting scripts; broadest 14/74 — NOT promoted")
    print(f"  canonical   bars.t_sig UNCHANGED · producer UNKNOWN")
    print(f"  retracted   2 claims from {MQ_D} (artifact not edited)")
    print(f"  T4          defect recorded · amendment DEFERRED by instruction")
    print(f"  immutable   {sum(1 for v in imm.values() if v['unchanged'])}/{len(imm)} verified")
    print(f"  gates       smoke HOLD · production HOLD · Z HOLD · Y HOLD")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
