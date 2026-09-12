"""MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1 — resolved, and it was my error.

The ~4-5% A<->B residual is gone, and it was never a provenance mystery. It was the wrong input
surface on my side.

THE WRITER CHAIN, READ FROM SOURCE:
    update_all.sh  ->  derive_intraday.py --tf 15m
    derive_intraday reads the LEAN base studio_15m_base.duckdb and, for 15m, resamples by
    IDENTITY ("base IS 15m")
    -> build_intraday_db._process
    -> main.api_bar_signals(tk, tf, bars, universe, _df=df)
    -> compute_all_signals -> signal_engine.compute_signals
    -> studio_15m.duckdb :: bars.t_sig

So the 15m materializer is the SAME canonical producer already frozen as authority.

THE MEASUREMENT THAT SETTLES IT. Running the canonical engine on the LEAN BASE OHLC — the frame
actually handed to api_bar_signals — reproduces the stored t_sig EXACTLY: 1.000000 over 1,893,031
bars across 60 declared securities. Not 99-point-something. Exactly.

WHERE MY 4% CAME FROM. I fed B the ENRICHED store's own OHLC column, assuming it was the input
that produced its own t_sig. It is not: the enriched OHLC matches the lean base on only 34.53% of
bars. The residual I was preparing to carry into the acceptability rebind as an unexplained
limitation was an artefact of comparing a label against a feed that never produced it.

CONSEQUENCE FOR THE EARLIER GATES. The three-way gate's A<->B and B<->C both used the wrong
legacy input surface and must be recomputed against the lean base; its A<->C figures are between
two correctly-identified objects but their interpretation changes, because A is now provably
engine(lean-base OHLC) rather than an independent materialization. Those artifacts are not
edited; they are corrected by recomputation.
"""
from __future__ import annotations
import json, os, sys, time
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART

p = dict(
    report_id="MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1",
    status="SAME_MATERIALIZER_CONFIRMED",
    task_class="PROVENANCE_RESOLUTION_ONLY",
    target="studio_15m.duckdb :: bars.t_sig",
    writer_chain=[
        "update_all.sh -> derive_intraday.py --tf 15m",
        "derive_intraday reads studio_15m_base.duckdb (LEAN base); for 15m the resample is "
        "IDENTITY ('base IS 15m')",
        "build_intraday_db._process",
        "main.api_bar_signals(tk, tf=tf, bars=len(df), universe=u, _df=df)",
        "compute_all_signals -> signal_engine.compute_signals",
        "studio_15m.duckdb :: bars.t_sig"],
    upstream_ohlc_surface="studio_15m_base.duckdb :: bars (OHLCV only)",
    preprocessing="identity resample for 15m; no transform before compute_signals",
    runtime_config="producer defaults (api_bar_signals calls compute_signals(df) with no "
                   "overrides)",
    decisive_measurement=dict(
        sample_securities=60, common_bars=1893031, deterministic_sample=True,
        A_vs_engine_on_LEAN_BASE=1.000000,
        A_vs_engine_on_ENRICHED_OWN_OHLC=0.960849,
        enriched_ohlc_equals_base_ohlc=0.345301,
        reading="the canonical engine on the actual input surface reproduces the stored "
                "t_sig EXACTLY"),
    classification="SAME_MATERIALIZER_CONFIRMED",
    classification_rejected=["DIFFERENT_MATERIALIZER_CONFIRMED",
                             "HIDDEN_PREPROCESS_TRANSFORM_CONFIRMED",
                             "SAME_MATERIALIZER_DIFFERENT_INPUT_VINTAGE_CONFIRMED",
                             "PROVENANCE_UNRESOLVED"],
    my_error=dict(
        what="I gave B the ENRICHED store's own OHLC column as the presumed input to its own "
             "t_sig",
        actual_input="the LEAN base store",
        cost="a 4-5% residual I was about to carry into the acceptability rebind as an "
             "unexplained provenance limitation",
        class_="WRONG_INPUT_SURFACE_ASSUMED_FROM_COLOCATION",
        lesson="co-location of a label and an OHLC column in one table does not mean that "
               "column produced that label"),
    corrects=[
        dict(artifact="MASSIVE_T_LEGACY_15M_THREE_WAY_CONFORMANCE_V1",
             digest="715765f2be4b8711",
             what="A<->B and B<->C used the wrong legacy input surface",
             disposition="RECOMPUTATION_REQUIRED — not edited"),
        dict(artifact="MASSIVE_T_LEGACY_15M_TOKEN_POSITION_PORTABILITY_V1",
             digest="f93999b9d10e0b0d",
             what="A<->C objects are correct; interpretation changes because A is now "
                  "provably engine(lean-base OHLC)",
             disposition="INTERPRETATION_AMENDED — measurements stand"),
        dict(artifact="MASSIVE_T_LEGACY_15M_OHLC_VINTAGE_DIAGNOSTIC_V1",
             digest="e9b2e267b1dd6e75",
             what="its UNRESOLVED verdict was correct; the residual it could not explain has "
                  "a different cause entirely",
             disposition="SUPERSEDED_BY_RESOLUTION")],
    consequence_for_authority=dict(
        canonical_15m_materializer="signal_engine.py :: compute_signals via api_bar_signals",
        strength="the strongest provenance evidence in the programme — exact reproduction, "
                 "not statistical agreement",
        note="the 15m materializer is the same producer already frozen for the daily surface"),
    stop_rule=dict(bounded_pass=True, closed=True,
                   further_archaeology_required=False),
    y_exposed=0, outcome_exposure="NOT_EXPOSED",
    sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

d = ART.seal(p, "MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1.json",
             required=("report_id", "status", "writer_chain", "decisive_measurement",
                       "classification", "my_error", "corrects"),
             supersede=os.path.exists(
                 "MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1.json"))
print(f"MASSIVE_T_LEGACY_15M_MATERIALIZER_PROVENANCE_RESOLUTION_V1 · {d} · {p['status']}")
print("  writer      derive_intraday --tf 15m -> build_intraday_db._process")
print("              -> api_bar_signals -> signal_engine.compute_signals")
print("  input       studio_15m_base.duckdb (LEAN base), identity resample, no transform")
print("  EXACT       engine(lean base) == stored t_sig : 1.000000 on 1,893,031 bars")
print("  my error    fed B the enriched store's own OHLC (matches base only 34.53%)")
print("  corrects    three-way A<->B / B<->C require recomputation")
