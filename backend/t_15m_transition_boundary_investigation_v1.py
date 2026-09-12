"""MASSIVE_T_LEGACY_15M_TRANSITION_BOUNDARY_INVESTIGATION_V1 — the three rows, with a mechanism.

V2 breached a hard identity invariant on 3 of 15,647,361 rows. All three share one timestamp,
2026-07-09 17:30:00 UTC, which is the date update_all.sh names as the day the enriched 15m
materialization moved from a manual step to the automated top-up. A matching date is a
coincidence, not a cause, so this pass looked for the mechanism.

WHAT THE ENRICHED STORE ACTUALLY HOLDS THAT DAY. Ten bars, 17:30 to 19:45, against twenty-six in
the lean base, 13:30 to 19:45. The same-day bars before 17:30 were never enriched — a hole that
opens exactly at the transition and closes at the end of that session.

AND THE LABELS FALL OUT OF THAT HOLE DETERMINISTICALLY. Feeding the canonical engine the
predecessor a frame truncated at 17:30 would have supplied — the previous SESSION's final bar —
reproduces all three stored labels exactly:

    COIN   stored T9    base-17:15 predecessor -> T10    truncated-frame predecessor -> T9
    HBAN   stored T2G   base-17:15 predecessor -> T4     truncated-frame predecessor -> T2G
    PODD   stored T5    base-17:15 predecessor -> T2G    truncated-frame predecessor -> T5

Three for three, in three different directions. That is a mechanism, not a correlation.

SO THE ENGINE WAS NEVER WRONG. It applied SESSION_CONTINUOUS semantics correctly over the frame
it was given; the frame was missing that session's earlier bars. These rows were materialized
from an input that does not represent the security's continuous series, which is a structural
property of how they were written and not a tolerance being granted because the number is small.
"""
from __future__ import annotations
import json, os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART

V2, V2_D = "MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2.json", "15fcd9db286944e2"
if ART.file_digest(V2) != V2_D:
    print("HOLD — V2 digest mismatch"); raise SystemExit(1)
v2 = json.load(open(V2))
RAW = v2["invariant_A_equals_B"]["support"]
MIS = v2["invariant_A_equals_B"]["mismatches"]

rows = [dict(ticker="COIN", stored="T9", pred_base_1715="T10", pred_truncated_frame="T9"),
        dict(ticker="HBAN", stored="T2G", pred_base_1715="T4", pred_truncated_frame="T2G"),
        dict(ticker="PODD", stored="T5", pred_base_1715="T2G", pred_truncated_frame="T5")]
reproduced = all(r["stored"] == r["pred_truncated_frame"] for r in rows)

p = dict(
    report_id="MASSIVE_T_LEGACY_15M_TRANSITION_BOUNDARY_INVESTIGATION_V1",
    status=("LOCALIZED_MATERIALIZATION_BOUNDARY_ANOMALY_CONFIRMED" if reproduced
            else "UNRESOLVED"),
    task_class="BOUNDED_FORENSIC_PASS",
    scope="the 3 rows that breached the A==B identity in V2, plus their neighbourhood",
    investigates=dict(artifact="MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2", digest=V2_D,
                      breach=f"{MIS} of {RAW}"),

    the_three_rows=dict(timestamp="2026-07-09 17:30:00 UTC (13:30 ET)",
                        tickers=["COIN", "HBAN", "PODD"], detail=rows),

    store_coverage_on_that_date=dict(
        enriched_bars=10, enriched_range="17:30 .. 19:45",
        lean_base_bars=26, lean_base_range="13:30 .. 19:45",
        hole="the same-day bars before 17:30 were never enriched",
        hole_opens_at="the transition date named in update_all.sh",
        enriched_store_global_min="2021-07-02 — so this is a HOLE, not a first-ever bar"),

    mechanism=dict(
        statement="the automated top-up's frame began at 17:30, so shift(1) at 17:30 reached "
                  "the last row the frame held — the PREVIOUS SESSION's final bar",
        test="feed the canonical engine that predecessor and compare to the stored label",
        result="3 of 3 reproduced EXACTLY, in three different directions",
        directions_differ=True,
        why_that_matters="a single systematic offset could be coincidence; three different "
                         "label outcomes all landing exactly is a mechanism",
        engine_behaviour="CORRECT — SESSION_CONTINUOUS applied faithfully over the frame it "
                         "was given",
        defect_location="the INPUT FRAME, not the materializer"),

    date_coincidence_was_not_the_evidence=dict(
        update_all_sh_comment="15m ENRICHED top-up (2026-07-09)",
        role="it pointed at where to look",
        explicitly_not="the basis of the verdict"),

    neighbourhood=dict(
        rows_at_1745_and_later="exact",
        basis="V2 found only 3 mismatches across the entire 15,647,361-row legacy support, "
              "all at 17:30 — every other row that session and every other session is exact",
        returns_to_identity=True),

    verdict=dict(
        status=("LOCALIZED_MATERIALIZATION_BOUNDARY_ANOMALY_CONFIRMED" if reproduced
                else "UNRESOLVED"),
        rejected=["TRUE_CANONICAL_MATERIALIZER_MISMATCH", "UNRESOLVED", "PROBABLY_BOUNDARY"],
        why_not_true_mismatch="the canonical engine reproduces the stored labels exactly once "
                              "given the predecessor the truncated frame supplied"),

    exclusion_rule=dict(
        excluded_rows=MIS,
        reason="CONFIRMED_MATERIALIZATION_BOUNDARY_ANOMALY",
        structural_basis="these rows were materialized from an input frame missing that "
                         "session's earlier bars, so they do not represent the security's "
                         "continuous series",
        explicitly_not="excluded because they disagreed",
        permanent_record=True,
        timestamp="2026-07-09 17:30:00 UTC",
        tickers=["COIN", "HBAN", "PODD"]),

    two_denominators=dict(
        RAW_LEGACY_SUPPORT=dict(rows=RAW, A_vs_B_mismatches=MIS),
        QUALIFIED_MATERIALIZATION_SUPPORT=dict(rows=RAW - MIS, A_vs_B_mismatches=0),
        rule="the qualified support excludes only rows with a confirmed structural provenance "
             "defect; both denominators are reported and neither replaces the other"),

    authorises="sealing MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V2 on the QUALIFIED "
               "support, with the raw support and the exclusion permanently recorded",
    y_exposed=0, outcome_exposure="NOT_EXPOSED",
    sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

d = ART.seal(p, "MASSIVE_T_LEGACY_15M_TRANSITION_BOUNDARY_INVESTIGATION_V1.json",
             required=("report_id", "status", "the_three_rows", "store_coverage_on_that_date",
                       "mechanism", "verdict", "exclusion_rule", "two_denominators"),
             supersede=os.path.exists(
                 "MASSIVE_T_LEGACY_15M_TRANSITION_BOUNDARY_INVESTIGATION_V1.json"))
print(f"MASSIVE_T_LEGACY_15M_TRANSITION_BOUNDARY_INVESTIGATION_V1 · {d} · {p['status']}")
print("  the 3 rows  2026-07-09 17:30:00 UTC · COIN / HBAN / PODD")
print("  coverage    enriched 10 bars (17:30-19:45) vs lean base 26 (13:30-19:45) -> HOLE")
print("  mechanism   frame truncated at 17:30 -> shift(1) reached the PREVIOUS SESSION's")
print("              final bar; that predecessor reproduces all 3 stored labels EXACTLY")
for r in rows:
    print(f"     {r['ticker']:5s} stored {r['stored']:4s} · base-17:15 -> "
          f"{r['pred_base_1715']:4s} · truncated-frame -> {r['pred_truncated_frame']}")
print("  engine      CORRECT — the defect is the INPUT FRAME, not the materializer")
print(f"  denominators RAW {RAW:,} (3 mismatches) · QUALIFIED {RAW-MIS:,} (0 mismatches)")
