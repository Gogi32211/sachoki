"""MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1 — the producer is found, and I had it wrong.

Your Superchart hypothesis is correct, and it is your variant A: the application computes T,
and the Superchart exporter only serializes it. The end-to-end chain now runs unbroken from the
export action back to the first line where the field "T" obtains its value.

    signal_engine.py :: compute_signals(df)                  <-- T IS COMPUTED HERE
        SIG_NAMES {1:"T1G", 2:"T1", ... 12:"T12"}
        priority  T4 > T6 > T1G > T2G > T1 > T2 > T9 > T10 > T3 > T11 > T5 > T12
        isDoji = (c == o) | mintick = 1e-10 | min_body_ratio = 1.0 | use_wick = False
      -> main.py:34 import; main.py:4060 compute_signals(df)      [DEFAULT arguments]
      -> main.py:4835  tz = sig_df.iloc[i]["sig_name"]
      -> main.py:5253  "tz": tz                     -> /api/bar-signals payload
      -> SuperchartPanel.jsx:1051  fetch /api/bar-signals
      -> SuperchartPanel.jsx:1195  exportCsv()
      -> :1238 headers [... 'Z','T','L','F','FLY','G','B','Combo','ULT' ...]
      -> :1405 b.tz?.startsWith('T') ? b.tz : ''    <-- the exported T cell
      -> :1610 csv = [headers, ...rows]             -> *_signals_5y.csv
      -> studio/importer.py:41 "T" -> t_sig ; :470-472 seeds those exact filenames
      -> bars.t_sig  CANONICAL

THIS SUPERSEDES MASSIVE_T_MATERIALIZER_AUTHORITY_RESOLUTION_V1 (1e465831f96bc855) AUTHORITY-
WISE. That artifact stays on disk and is not edited. Almost all of its conclusions were wrong,
and the reasons are mine, not the evidence's.

HOW I GOT IT WRONG — TWO COMPOUNDING ERRORS.

First, a tooling error I failed to notice. I ran the decisive searches as `timeout 60 grep ...`.
`timeout` does not exist on macOS, so the shell returned "command not found" and printed
nothing. I read empty output as "no matches" and sealed LOCAL_PYTHON as REFUTED on the strength
of a command that never ran. Re-run properly, those same greps return eleven files, including
bulk_export.py, and importer.py:470-472 naming the CSVs outright. This is precisely the failure
mode the data-contract rule exists for: every guard I applied passed, because the guard was
reading an empty result rather than a real one.

Second, a stale note carried forward. I recorded that signal_engine.py computes a threshold
doji, `(bdy/rng) <= doji_thresh`. It does not. Line 99 is `isDoji = (c == o)`, exact equality.
`doji_thresh` exists as a declared parameter at line 69 and never enters isDoji.

WHAT THOSE ERRORS COST, AND WHAT IS NOW RESTORED.

MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1 (0cad6a...) was RIGHT and my retraction of it was
wrong. 1e-10 really is the value used by the implementation that produced the column, because
that implementation is signal_engine.py. TradingView really is not the authority here. Both
claims are REINSTATED, mintick is CLOSED rather than reopened, and Amendment 1's HOLD is
RESOLVED rather than vindicated.

T4_ENGULF_CONFIG_V1 (713f09...) is CORRECT IN FULL, citation included. It cites
signal_engine.py:67-69,97-122 — exactly where use_wick, min_body_ratio, doji_thresh, mintick,
isDoji and engOk live in the proven producer. Its doji_semantics sentence, that doji_thresh
exists but does not participate in isDoji, is a verbatim description of the producer. There is
no citation defect and NO AMENDMENT IS NEEDED. The defect was in my reading.

signal_logic.py remains a NON-PRODUCER. Its 99.14% agreement was never the argument and is not
the argument now; it simply is not in the call path.

THE TRADINGVIEW INVENTORY WAS NOT WASTED. It correctly excluded the TradingView-direct-export
hypothesis on real evidence — 932/932 sources, union 27 of 74. That exclusion is what made the
application surface the only place left to look, and it is preserved here as valid.

RUNTIME CONFIGURATION IS NO LONGER UNKNOWN for the Python producer. main.py:4060 calls
compute_signals(df) with no arguments, so the export ran on the declared defaults:
use_wick=False, min_body_ratio=1.0, doji_thresh=0.05.

VINTAGE. The only commit touching signal_engine.py after the 2026-05-29 export is deb9ba0
(2026-06-08), a one-line Z1 condition fix that does not touch T. All three parameter markers are
byte-identical at the export vintage (bc037e1, 2026-04-16) and at HEAD, so the current file
carries the export-vintage T semantics.
"""
from __future__ import annotations
import json, os, re, subprocess, sys, time                                # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

PRIOR, PRIOR_D = "MASSIVE_T_MATERIALIZER_AUTHORITY_RESOLUTION_V1.json", "1e465831f96bc855"
MQ, MQ_D = "MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1.json", "0cad6a"
T4, T4_D = "T4_ENGULF_CONFIG_V1.json", "713f09144a245408"
AM1_D, PORT_D, REC_D = "de60bdc75fc61e3c", "545d293b4f1e4653", "7d00e3908feb26d5"
SP = "../frontend/src/components/SuperchartPanel.jsx"

# Each link is re-verified from source here rather than restated from prose.
LINKS = [
    ("signal_engine.py", r'def compute_signals', "producer entry point"),
    ("signal_engine.py", r'1:\s*"T1G",\s*2:\s*"T1"', "SIG_NAMES maps ids to T-labels"),
    ("signal_engine.py", r'isDoji\s*=\s*\(c\s*==\s*o\)', "isDoji is EXACT equality"),
    ("signal_engine.py", r'mintick\s*=\s*1e-10', "mintick constant"),
    ("signal_engine.py", r'min_body_ratio:\s*float\s*=\s*1\.0', "min_body_ratio default"),
    ("signal_engine.py", r'use_wick:\s*bool\s*=\s*False', "use_wick default"),
    ("main.py", r'from signal_engine import compute_signals', "import of the producer"),
    ("main.py", r'lambda:\s*compute_signals\(df\)', "called with DEFAULT arguments"),
    ("main.py", r'tz\s*=\s*str\(sig_df\.iloc\[i\]\.get\("sig_name"', "tz <- sig_name"),
    (SP, r'const exportCsv', "the Superchart Export CSV action"),
    (SP, r"b\.tz\?\.startsWith\('T'\)", "the exported T cell"),
    ("studio/importer.py", r'"T":\s*"t_sig"', "CSV T -> t_sig"),
    ("studio/importer.py", r'_seed\("sp500_signals_5y\.csv"\)', "the imported filenames"),
]


def main():
    if ART.file_digest(PRIOR) != PRIOR_D:
        print("HOLD — prior resolution digest mismatch"); return 1
    if ART.file_digest(T4) != T4_D:
        print("HOLD — T4 config digest mismatch"); return 1
    t4 = json.load(open(T4))

    chain, missing = [], []
    for path, pat, label in LINKS:
        ok = bool(os.path.exists(path) and re.search(pat, open(path).read()))
        chain.append(dict(file=path, evidence=label, verified=ok))
        if not ok:
            missing.append(f"{path}:{label}")
    if missing:
        print("HOLD — a chain link did not verify:", missing); return 1

    try:
        after = subprocess.run(
            ["git", "log", "--format=%h %ad %s", "--date=short", "--since=2026-05-29",
             "--", "backend/signal_engine.py"], cwd="..", capture_output=True, text=True,
            timeout=60).stdout.strip().splitlines()
    except Exception:
        after = ["<git unavailable>"]

    p = dict(
        report_id="MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1",
        status="MATERIALIZER_AUTHORITY_RESOLVED",
        resolution_class="PRODUCER_POSITIVELY_IDENTIFIED",
        task_class="PROVENANCE_RESOLUTION_ONLY",
        supersedes_authority_wise=dict(
            artifact="MASSIVE_T_MATERIALIZER_AUTHORITY_RESOLUTION_V1", digest=PRIOR_D,
            disposition="LEFT ON DISK, NOT EDITED",
            note="its UNRESOLVED verdict, its two retractions and its T4 citation-defect "
                 "finding are all withdrawn as products of the errors recorded below"),

        end_to_end_chain=dict(
            export_action="SuperchartPanel.jsx:1195  exportCsv()",
            superchart_variant="A — the application computes T; the exporter only serializes",
            steps=[
                "signal_engine.py :: compute_signals(df)  — T IS COMPUTED HERE",
                "main.py:34  from signal_engine import compute_signals",
                "main.py:4060  compute_signals(df)  [DEFAULT arguments]",
                "main.py:4835  tz = sig_df.iloc[i][\"sig_name\"]",
                "main.py:5253  \"tz\": tz  -> /api/bar-signals payload",
                "SuperchartPanel.jsx:1051  fetch /api/bar-signals",
                "SuperchartPanel.jsx:1238  headers [... 'Z','T','L','F' ...]",
                "SuperchartPanel.jsx:1405  b.tz?.startsWith('T') ? b.tz : ''",
                "SuperchartPanel.jsx:1610  csv = [headers, ...rows]",
                "*_signals_5y.csv",
                "studio/importer.py:41  \"T\" -> t_sig",
                "bars.t_sig  CANONICAL"],
            first_location_where_T_obtains_its_value=dict(
                module="signal_engine.py", function="compute_signals",
                mechanism="sig_id -> SIG_NAMES -> sig_name; bullish priority chain "
                          "T4 > T6 > T1G > T2G > T1 > T2 > T9 > T10 > T3 > T11 > T5 > T12",
                not_a_pass_through=True),
            link_verification=chain,
            corroboration=dict(
                superchart_panel_header_coverage="71 of the 74 importer-mapped columns",
                bulk_export_header_coverage="71 of 74 (a later batch replica of the button)",
                compare_tradingview_cloud="27 of 74 across all 932 scripts",
                compare_local_pine="9 of 74",
                unmatched_three=["TURBO_SCORE_N3", "TURBO_SCORE_N5", "TURBO_SCORE_N10"],
                unmatched_note="header-vintage difference; not attributed"),
            explains="why one CSV mixes T/Z, TURBO, RTB, WYC, GOG, BETA and PREBREAK — "
                     "api_bar_signals() assembles every engine into a single row"),

        authority_hierarchy=dict(
            PRIMARY=dict(implementation="backend/signal_engine.py :: compute_signals",
                         role="CANONICAL_LEGACY_MATERIALIZER",
                         basis="proven call path, not output agreement"),
            SECONDARY=dict(implementation="Pine 260523_TZ_F_WLNBB_CMB_pattern.pine",
                           role="REFERENCE_ONLY",
                           note="same priority chain, but it is not in the call path"),
            NON_PRODUCER=[
                dict(path="analyzers/tz_wlnbb/signal_logic.py",
                     role="LOCAL_REIMPLEMENTATION",
                     note="99.14% agreement was never the argument; it is not in the call "
                          "path"),
                dict(path="frontend SuperchartPanel.jsx",
                     role="SERIALIZER_ONLY — carries no T semantics"),
                dict(path="backend/bulk_export.py",
                     role="LATER_BATCH_REPLICA — first committed 2026-07-26, after the "
                          "2026-05-29 export")],
            studio_db_py="STORAGE_AND_IMPORT_MAPPING_AUTHORITY_ONLY"),

        my_errors=[
            dict(error="ran the decisive searches as `timeout 60 grep ...`; `timeout` does "
                       "not exist on macOS, so the command never ran and printed nothing",
                 consequence="empty output was read as 'no matches' and LOCAL_PYTHON was "
                             "sealed as REFUTED",
                 corrected_by="re-running the identical greps without `timeout`, which "
                              "return 11 files including bulk_export.py and importer.py",
                 error_class="TOOLING_FAILURE_READ_AS_EVIDENCE"),
            dict(error="carried forward a note that signal_engine.py used a threshold doji "
                       "`(bdy/rng) <= doji_thresh`",
                 consequence="a non-existent contradiction was diagnosed between "
                             "T4_ENGULF_CONFIG_V1 and its own cited source",
                 corrected_by="signal_engine.py:99 is `isDoji = (c == o)`; doji_thresh is "
                              "declared at :69 and never enters isDoji",
                 error_class="STALE_NOTE_NOT_RE_VERIFIED")],

        reinstatements=[
            dict(artifact="MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1", digest_prefix=MQ_D,
                 claim="1e-10 is the value used by the implementation that produced the "
                       "column",
                 status="REINSTATED",
                 why="the producer is signal_engine.py, whose line 97 is mintick = 1e-10"),
            dict(artifact="MASSIVE_T_MINTICK_SOURCE_QUALIFICATION_V1", digest_prefix=MQ_D,
                 claim="TradingView is not the authority for this value",
                 status="REINSTATED",
                 why="the producer is a local Python module, not a TradingView script")],

        mintick_status=dict(
            current_status="CLOSED",
            value=1e-10, source="signal_engine.py:97 — the proven producer",
            amendment_1=dict(artifact="MASSIVE_T_FEATURE_PORT_SPEC_V1_AMENDMENT_1",
                             digest=AM1_D, original_verdict="HOLD", now="RESOLVED",
                             note="its HOLD was the right call under the provenance then "
                                  "available; the provenance is now established"),
            no_tradingview_metadata_required=True,
            syminfo_mintick_relevance="applies to the Pine SECONDARY reference only"),

        t4_config_status=dict(
            artifact="T4_ENGULF_CONFIG_V1", digest=T4_D,
            verdict="CORRECT_IN_FULL_INCLUDING_CITATION",
            amendment_required=False,
            amendment_issued=False,
            checked=dict(use_wick=t4["use_wick"], min_body_ratio=t4["min_body_ratio"],
                         doji=t4["doji_semantics"], mintick=t4["mintick_semantics"],
                         cited=t4["source_identity"]),
            why="signal_engine.py:67-69,97-122 is exactly where use_wick, min_body_ratio, "
                "doji_thresh, mintick, isDoji and engOk live in the proven producer; the "
                "doji_semantics sentence describes that code verbatim",
            previously_alleged_defect="WITHDRAWN — it was my misreading, not a defect"),

        runtime_configuration=dict(
            previously="LEGACY_RUNTIME_CONFIGURATION_UNKNOWN",
            now="RESOLVED_FOR_THE_PYTHON_PRODUCER",
            evidence="main.py:4060 calls compute_signals(df) with no arguments",
            values=dict(use_wick=False, min_body_ratio=1.0, doji_thresh=0.05,
                        mintick=1e-10)),

        vintage=dict(
            export_date="2026-05-29",
            commits_touching_producer_since=after,
            assessment="the only post-export commit is a one-line Z1 condition fix; it does "
                       "not touch T",
            parameter_markers_identical_at="bc037e1 (2026-04-16) and HEAD",
            conclusion="the current signal_engine.py carries the export-vintage T semantics"),

        tradingview_inventory_disposition=dict(
            artifact_finding_preserved=True,
            scripts=932, sources_fetched=932, errors=0, union_coverage="27 of 74",
            status="VALID — correctly excluded the TradingView-direct-export hypothesis",
            not_wasted="that exclusion is what moved the hunt to the application surface"),

        claim_language=dict(
            now_supported="the same CANONICAL LEGACY T MATERIALIZER semantics — "
                          "signal_engine.py :: compute_signals at its export vintage, on its "
                          "declared defaults — re-executed on Massive 15m source data",
            still_required="the port must be verified against the producer, not against "
                           "Pine, and not by agreement with t_sig"),

        gates=dict(t_port_smoke="UNBLOCKED_PENDING_YOUR_APPROVAL", t_production="HOLD",
                   z="HOLD", y="HOLD", t4_amendment="NOT_REQUIRED"),
        y_exposed=0, t_production_writes=0, smoke_started=False,
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1.json",
                 required=("report_id", "status", "end_to_end_chain", "authority_hierarchy",
                           "my_errors", "reinstatements", "mintick_status",
                           "t4_config_status", "vintage"),
                 supersede=os.path.exists("MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1.json"))
    print(f"MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1 · {d} · {p['status']}")
    print(f"  PRODUCER    signal_engine.py :: compute_signals  (Superchart variant A)")
    print(f"  chain       {sum(1 for c in chain if c['verified'])}/{len(chain)} links "
          f"verified from source")
    print(f"  supersedes  {PRIOR_D} authority-wise (left on disk, not edited)")
    print(f"  my errors   2 recorded — `timeout` never ran; stale isDoji note")
    print(f"  reinstated  both mintick claims · mintick CLOSED at 1e-10")
    print(f"  T4 config   CORRECT IN FULL incl. citation · NO amendment required")
    print(f"  runtime     use_wick=False min_body_ratio=1.0 doji_thresh=0.05 (defaults)")
    print(f"  vintage     only post-export commit is a 1-line Z1 fix — T unchanged")
    print(f"  gates       smoke UNBLOCKED(pending approval) · production HOLD · Z/Y HOLD")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
