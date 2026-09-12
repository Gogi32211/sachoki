"""MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1 — writer resolved, input surface is not.

The 15m lesson said: never infer that the OHLC sitting beside a stored label produced it. Applied
to the daily anchor, that lesson changes the answer.

WRITER — RESOLVED, and already sealed. bars.t_sig was written by the CSV import of a Superchart
export whose cells came from api_bar_signals -> compute_all_signals -> signal_engine.
compute_signals (a8e1f3725ddfe7a1). Same canonical producer as 15m.

TRUE INPUT SURFACE — NOT PRESERVED. api_bar_signals computed T on the full-precision frame
returned by fetch_ohlcv(1d) at export time. The exporter then ROUNDED to two decimals before
writing the CSV (bulk_export.py:186-189, and the Superchart exporter likewise), and that rounded
rendering is what bars now holds: across 780,261 sp500 rows, ZERO values carry more than two
decimals. So the frame that produced the labels was never stored.

WHICH MAKES EXACT REPRODUCTION STRUCTURALLY IMPOSSIBLE, not merely unachieved. Engine on the
stored 2dp OHLC reproduces the stored label at 0.999171 — close, and not exact.

TWO EXPLANATIONS WERE TESTED AND BOTH FAILED. Full precision does not do better: Massive 1D at
full precision scores 0.916174, far worse, which is consistent with it being a different feed
(its close is the last regular-session minute, not the official close). And the residual is not
a write-path artefact: it is scattered across 2021-2024 with at most two per date, and the
CSV-seed era rate (0.000736) is HIGHER than the nightly era (0.000142), so the vintage boundary
explains nothing.

WHAT REMAINS IS CONSISTENT WITH TWO-DECIMAL ROUNDING of the true input — daily comparisons that
sit within a cent flip when rounded — but that is an inference, not a demonstration, and it
cannot be demonstrated from data that no longer exists. It is recorded as UNPROVEN.

WHY THIS MAY NOT BLOCK C3, WHICH IS THE USER'S CALL AND NOT MINE. C3 builds a MASSIVE-NATIVE
daily anchor; it does not need to reproduce the legacy stored label. The unreproducibility bites
the HISTORICAL DIAGNOSTIC comparison, not the anchor construction.
"""
from __future__ import annotations
import json, os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART

BIND = {"MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1.json": "a8e1f3725ddfe7a1",
        "MASSIVE_T1_EPISODE_RULE_AUTHORITY_DECISION_V1.json": "6d733e1fa3bf3c3d",
        "MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1.json": "5bb9cdc3167e2ff7"}
bad = [f for f, w in BIND.items() if ART.file_digest(f) != w]
if bad:
    print("HOLD — binding mismatch:", bad); raise SystemExit(1)

p = dict(
    report_id="MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1",
    status="WRITER_RESOLVED__TRUE_INPUT_SURFACE_NOT_PRESERVED",
    task_class="PROVENANCE_RESOLUTION_ONLY",
    question="which writer, input OHLC surface and producer created the daily t_sig == 'T1' "
             "that t1_dna.py uses as the historical episode selector?",

    writer=dict(status="RESOLVED", authority="MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1",
                digest=BIND["MASSIVE_T_SUPERCHART_EXPORT_PROVENANCE_V1.json"],
                chain=["SuperchartPanel.jsx exportCsv()", "/api/bar-signals",
                       "main.api_bar_signals", "compute_all_signals",
                       "signal_engine.compute_signals", "*_signals_5y.csv",
                       "studio/importer.py", "bars.t_sig"],
                producer="the same canonical producer confirmed at 15m"),

    true_input_surface=dict(
        status="NOT_PRESERVED",
        what_it_was="the full-precision frame returned by fetch_ohlcv(interval='1d') at export "
                    "time",
        what_is_stored="a two-decimal rendering of it",
        evidence=dict(rows_checked=780261, values_with_more_than_2dp=0,
                      exporter_rounding="bulk_export.py:186-189 round(..., 2); the Superchart "
                                        "exporter rounds likewise"),
        consequence="exact reproduction from stored data is STRUCTURALLY IMPOSSIBLE, not "
                    "merely unachieved"),

    reproduction=dict(
        engine_on_stored_2dp_ohlc=0.999171,
        engine_on_massive_1d_full_precision=0.916174,
        sample_bars=27760,
        exact=False),

    hypotheses_tested_and_refuted=[
        dict(hypothesis="full precision reproduces the label better",
             verdict="REFUTED", evidence="Massive 1D full precision scores 0.916174, far "
                                         "worse — consistent with a different feed whose "
                                         "close is the last regular-session minute"),
        dict(hypothesis="the residual is a write-path/vintage artefact at the CSV-seed "
                        "boundary",
             verdict="REFUTED",
             evidence="scattered across 2021-2024, at most 2 per date; CSV-seed era rate "
                      "0.000736 vs nightly era 0.000142 — the boundary explains nothing")],

    remaining_explanation=dict(
        statement="consistent with two-decimal rounding of the true input — daily comparisons "
                  "sitting within a cent flip when rounded",
        status="UNPROVEN",
        why_unprovable="it would require the unrounded frame, which was never stored",
        residual_rate=round(1 - 0.999171, 6)),

    consequence_for_c3=dict(
        blocks_anchor_construction=False,
        why="C3 builds a MASSIVE-NATIVE daily anchor and does not need to reproduce the "
            "legacy stored label",
        what_it_does_limit="the HISTORICAL DIAGNOSTIC comparison against the preserved "
                           "historical anchor positives",
        required_limitation_if_c3_proceeds=(
            "historical daily anchor labels cannot be exactly regenerated from stored data; "
            "any historical-vs-Massive anchor comparison inherits a ~0.08% legacy "
            "irreproducibility floor that is NOT attributable to the Massive port"),
        decision_owner="user — the standing rule is 'exact, or HOLD', and this is not exact"),

    y_exposed=0, outcome_exposure="NOT_EXPOSED",
    sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

d = ART.seal(p, "MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1.json",
             required=("report_id", "status", "writer", "true_input_surface", "reproduction",
                       "hypotheses_tested_and_refuted", "consequence_for_c3"),
             supersede=os.path.exists("MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1.json"))
print(f"MASSIVE_T1_DAILY_ANCHOR_MATERIALIZER_PROVENANCE_V1 · {d} · {p['status']}")
print("  writer      RESOLVED — same canonical producer (a8e1f3725ddfe7a1)")
print("  input       NOT PRESERVED — stored bars is a 2dp rendering (0/780,261 rows >2dp)")
print("  reproduce   stored-2dp 0.999171 · Massive-1D-full-precision 0.916174 · EXACT False")
print("  refuted     full-precision-is-better · CSV-seed-vintage-boundary")
print("  remaining   consistent with 2dp rounding — UNPROVEN, and unprovable from stored data")
print("  C3          anchor construction NOT blocked; historical diagnostic inherits a")
print("              ~0.08% legacy irreproducibility floor — decision is the user's")
