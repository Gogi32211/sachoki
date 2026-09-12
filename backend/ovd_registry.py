"""OPENING_VOLUME_DYNAMICS_V1 — search registry + multiplicity universe (pre-outcome).

Enumerates EVERY claim cell the outcome phase is allowed to score, from the bins frozen in
FEATURE_SPEC_V1.draft.md. k is the size of this list. Survivors never shrink k; anything
not in this list is POST_EXPOSURE_HYPOTHESIS. No data is read here; this is bookkeeping.

Family A = pre-breakout regime (sample: BASE days with no breakout yet: base AND NOT breakout)
Family B = breakout quality       (sample: BREAKOUT days)
"""
from __future__ import annotations
import os, sys, json, hashlib, time                                     # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"

# feature -> (column, bins/states, families)   bins are right-open unless stated
FEATURES = {
    "OPEN1H_RVOL_LEVEL":        ("open1h_rvol",        ["<0.75", "0.75-1.0", "1.0-1.5", "1.5-2.5", ">2.5"],            "AB"),
    "OPEN1H_RAMP_3D":           ("ramp_3d",            ["STRICT_RISING", "STRICT_FALLING", "MIXED"],                     "AB"),
    "OPEN1H_RAMP_MAG":          ("ramp_mag",           ["<1.0", "1.0-1.25", "1.25-1.5", "1.5-2.0", ">2.0"],             "AB"),
    "OPEN1H_RAMP_5D":           ("ramp_5d_slope",      ["<=0", "0-0.1", "0.1-0.25", ">0.25"],                          "AB"),
    # pre-seal issue 6: neutral naming — the prior event is a high-participation session in a
    # declining context (RVOL_OPEN1H>=2 AND close(s-1)<close(s-6)); it is NOT asserted to have
    # "stopped" anything, and its own-day response is a separate feature, not a selector.
    "PRIOR_HV_VOLUME_RECLAIM":  ("prior_hv_reclaim",   ["<0.5", "0.5-0.75", "0.75-1.0", "1.0-1.25", ">1.25"],          "B"),
    "BASE_DRYUP":               ("base_dryup",         ["<0.6", "0.6-0.8", "0.8-1.0", ">1.0"],                         "AB"),
    "DRYUP_TO_EXPANSION":       ("dryup_to_expansion", ["<1.0", "1.0-1.5", "1.5-2.5", "2.5-4.0", ">4.0"],              "B"),
    "OPEN15_BREADTH":           ("open15_breadth",     ["0", "1", "2", "3", "4"],                                       "AB"),
    "OPEN15_CONCENTRATION":     ("open15_concentration", ["<0.30", "0.30-0.45", "0.45-0.60", ">0.60"],                  "AB"),
    "OPEN15_PERSISTENCE":       ("open15_persistence", ["<0.5", "0.5-0.8", "0.8-1.2", ">1.2"],                          "AB"),
    "OPEN15_SHAPE":             ("open15_shape",       ["FRONT_LOADED", "SUSTAINED_HIGH", "BUILDING", "OPEN_AND_LATE", "OTHER"], "AB"),
    "EFFORT_VS_RESULT":         ("effort_result",      ["ABSORB_LIKE", "NOT"],                                          "A"),
    "PRIOR_HV_DAY_RESPONSE":    ("prior_hv_response",  ["HV_WIDE_DOWN", "HV_NARROW_RESULT", "HV_STRONG_RECOVERY", "HV_OTHER"], "A"),
    "INTRADAY_BREADTH_SHIFT":   ("breadth_shift",      ["QUIET_TO_BROAD", "BROAD_TO_BROAD", "BROAD_TO_COLLAPSE", "OTHER"], "AB"),
    "VOLUME_TRANSFER":          ("volume_transfer",    ["<=-0.10", "(-0.10,0.10)", ">=0.10"],                            "A"),
    "IDIO_OPEN1H":              ("idio_open1h",        ["<0.75", "0.75-1.0", "1.0-1.5", "1.5-2.5", ">2.5"],            "AB"),
}
# the complete interaction set (pre-registered; nothing added after exposure)
INTERACTIONS = [
    ("OPEN1H_RAMP_3D", "BASE_DRYUP",              "AB"),
    ("OPEN1H_RAMP_3D", "PRIOR_HV_VOLUME_RECLAIM", "B"),
    ("PRIOR_HV_VOLUME_RECLAIM", "OPEN15_BREADTH", "B"),
    ("BASE_DRYUP", "OPEN15_PERSISTENCE",          "AB"),
    ("IDIO_OPEN1H", "OPEN15_BREADTH",             "AB"),
    ("GAP_BUCKET", "OPEN15_PERSISTENCE",          "AB"),
]
GAP_BINS = ["DOWN", "FLAT", "UP_MOD", "UP_LARGE"]
STRATA = {"year": "calendar year", "price_bucket": ["$5-8", "$8-21", "$21-89", "$89-200", "$200+"],
          "dv_bucket": ["3-10M", "10-50M", "50-250M", "250M+"], "gap_bucket": GAP_BINS,
          "mkt_regime": "MKT_OPEN1H terciles frozen on the X table at seal"}

# ── PRE-SEAL GATE (2026-09-04, after CHECKPOINT 1D; spec A9–A11). None of this touches FEATURES /
#    INTERACTIONS / cells, so k is unchanged. ovd_seal measures the window on X and REFUSES on mismatch.
OBSERVATION_WINDOW_AUTHORITY = dict(
    amendment="PRE_OUTCOME_SAMPLE_WINDOW_AMENDMENT",
    canonical_1d_available_range=["2021-09-07", "2026-09-03"],
    eligible_sessions=1254,
    rule="the observation window IS the canonical 1D authority's available range (vendor history floor .. last complete session); "
         "the builder's lookback build(months=72) -> max_session - 2292 days is a non-binding superset bound echoed as build_report.months, "
         "and ovd_canonical_1d.roster(months=72) selected WHICH tickers (frozen 5,193 roster), not the window; neither carries eligibility meaning",
    retraction="the old '72 months' wording is RETRACTED",
    forbidden=["no synthetic extension",
               "no older Studio-only rows may be appended to recover the RETRACTED window",
               "no mixed-basis history"],
    per_year_note="2021 is partial (Sep-Dec) and 2026 runs to 09-03; year strata are descriptive only")
REPORT_ONLY_DIAGNOSTICS = {
    "POST_ENTRY_DISTRIBUTION_EXPOSURE_GE5": dict(
        status="REPORT-ONLY POST-OUTCOME DESCRIPTIVE DIAGNOSTIC — not X, not eligibility, not a claim, not in k",
        definition="TRUE iff an authoritative cash-distribution ex-date with cash / canonical close on the last session BEFORE the "
                   "ex-date >= 0.05 occurs strictly AFTER the _pathsim entry session (entry = D+1 open) AND ON OR BEFORE the ACTUAL "
                   "simulated exit session; FALSE otherwise (a distribution after the actual exit is FALSE even if inside maxh); "
                   "NULL when the outcome row has no exit",
        authority="canonical_1d/dividend_reference/dividends_20260904T062743Z.parquet (sha256_16 a49c5b80bcff6933) + canonical closes",
        materiality_rule="exact-duplicate reference rows removed; cash SUMMED per (ticker, ex_date) (regular + special on one ex-date = ONE event); "
                         "reference price = canonical close on the last session strictly before the ex-date; exposure set materialised PRE-SEAL "
                         "(GE5_EXPOSURE_TABLE_A10_<canonical_run>.parquet + MATERIALITY_CENSUS_A10.json, digests bound in SEAL.json)",
        code="backend/ovd_diagnostics.py::post_entry_distribution_exposure_ge5 (never imported by the X builder)",
        computed="only AFTER _pathsim has produced entry/exit sessions",
        must_not=["enter eligibility", "enter registry claims", "enter k", "alter _pathsim", "alter entry/exit",
                  "promote, veto or rescue a candidate", "remove rows from the primary result"],
        reporting=dict(PRIMARY="all registered observations",
                       DESCRIPTIVE_SENSITIVITY=["rows with POST_ENTRY_DISTRIBUTION_EXPOSURE_GE5",
                                                "rows without POST_ENTRY_DISTRIBUTION_EXPOSURE_GE5"]),
        statement="Ex-distribution stratification is post-entry descriptive diagnostics, not causal predictor evidence.",
        coverage="authoritative CASH distributions only; NOT a complete non-split corporate-action flag (spin-offs / "
                 "reorganisations / other events may affect price paths without being captured)")}
LIMITATIONS = dict(
    price_basis="Massive /v2/aggs adjusted=true => split-adjusted as of the canonical fetch (one basis_asof)",
    dividends="DIVIDENDS_NOT_ADJUSTED",
    outcome_semantics="_pathsim outcomes are PRICE-RETURN paths, not total shareholder return; no finding may be called an economic total-return edge",
    seam_claim="'0 unresolved reference-split seams' (arbiter /v3/reference/splits) — never '0 corporate-action seams'",
    diagnostic_coverage="the dividend diagnostic does not cover every non-split corporate action")


def enumerate_cells() -> list[dict]:
    cells = []
    for fam in "AB":
        for name, (col, bins, fams) in FEATURES.items():
            if fam not in fams:
                continue
            for b in bins:
                cells.append(dict(kind="single", family=fam, feature=name, column=col, bin=b))
        for f1, f2, fams in INTERACTIONS:
            if fam not in fams:
                continue
            b1 = GAP_BINS if f1 == "GAP_BUCKET" else FEATURES[f1][1]
            b2 = FEATURES[f2][1]
            for x in b1:
                for y in b2:
                    cells.append(dict(kind="interaction", family=fam, feature=f"{f1}x{f2}", bin=f"{x}|{y}"))
    return cells


def main():
    cells = enumerate_cells()
    k_single = sum(1 for c in cells if c["kind"] == "single")
    k_inter = sum(1 for c in cells if c["kind"] == "interaction")
    reg = dict(
        registry_id="OPENING_VOLUME_DYNAMICS_SEARCH_REGISTRY_V1", status="DRAFT_PRE_SEAL",
        spec="FEATURE_SPEC_V1.draft.md", generated_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        samples=dict(A="X.base AND NOT X.breakout (pre-breakout base days)", B="X.breakout (breakout days)"),
        entry="mask on session D -> _pathsim entry at D+1 open (feature_available_at 10:30 NY)",
        exit="book default: trail, atr_k=12, maxh=60 (+ horizon sweep 3/5/10/20/60 as diagnostic, not extra claims)",
        # ── PRE-SEAL ISSUE 7: statistical identity frozen here, before any outcome ──
        primary_statistic="day-clustered median edge vs same-day control at the registered _pathsim config; trade-level reported alongside",
        multiplicity=dict(option="OPTION_1_GLOBAL", k_rule="one confirmatory search family; k = every registered cell across A and B; "
                          "findings may be headlined across A and B, therefore DSR is bound to the GLOBAL k; survivors never shrink k"),
        same_day_control=dict(
            control_universe="all OTHER eligible observations of the SAME scientific family (A: base AND NOT breakout; B: breakout) "
                             "with a non-null _pathsim outcome on the same entry day",
            matching="family + entry day only; NO price/liquidity/gap matching in the primary (strata are descriptive)",
            self_excluded=True, min_control_count=20,
            no_control="edge_i = UNAVAILABLE (row excluded from the cell's day-edge statistic, counted in an 'unmatched' column)",
            algorithm=["control_d = median(outcome_j : j != i, same family, entry day d)",
                       "edge_i = outcome_i - control_d",
                       "day_edge_d = median(edge_i : entries on day d)",
                       "cell statistic = median over days of day_edge_d; day_win = share of days with day_edge_d > 0; n_days = #days"],
            coverage_guard="controls are drawn from the same family, so a cell cannot gain an edge merely by having lower-TF coverage "
                           "that the control lacks; rows with UNAVAILABLE features are NEVER used as implicit controls of a feature cell"),
        horizon_sweep=dict(status="DESCRIPTIVE / SENSITIVITY ONLY", horizons=[3, 5, 10, 20, 60],
                           rule="cannot promote, rescue or reclassify any cell; the primary horizon is the registered _pathsim config; "
                                "the five horizons are NOT in k"),
        breakout_predicate=dict(BASE="in_tr_1 AND close_1 <= ceiling AND (hi25-lo25)/lo25 <= 0.35",
                                BREAKOUT="BASE AND close > ceiling",
                                ceiling="wt_resistance at D-1 (REUSED: wyckoff_trig_engine via studio/enricher)",
                                window_25_and_0_35="REUSED from edge_replay.py E_coilfloor (25-bar prior range <= 0.35)",
                                above_ar_band="validate_above_ar's (ceiling, ceiling*1.05] band is a DESCRIPTIVE column (breakout_q) only — "
                                              "NOT eligibility, NOT a registered feature",
                                prior_hv_event="RVOL_OPEN1H(s) >= 2.0 AND close(s-1) < close(s-6), s in D-30..D-6 — NEW_RESEARCH_SPEC"),
        observation_window_authority=OBSERVATION_WINDOW_AUTHORITY,
        report_only_diagnostics=REPORT_ONLY_DIAGNOSTICS,
        limitations=LIMITATIONS,
        strata=STRATA, features=FEATURES, interactions=INTERACTIONS,
        k=dict(single=k_single, interaction=k_inter, total=len(cells)),
        cells=cells)
    body = json.dumps(reg, sort_keys=True, default=str).encode()
    reg["registry_sha256_16"] = hashlib.sha256(body).hexdigest()[:16]
    os.makedirs(FAMILY_DIR, exist_ok=True)
    with open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V1.draft.json"), "w") as f:
        json.dump(reg, f, indent=1, default=str)
    mult = dict(multiplicity_id="OPENING_VOLUME_DYNAMICS_MULTIPLICITY_UNIVERSE_V1", status="DRAFT_PRE_SEAL",
                registry_sha256_16=reg["registry_sha256_16"], k_total=len(cells), k_single=k_single,
                k_interaction=k_inter, k_by_family={f: sum(1 for c in cells if c["family"] == f) for f in "AB"},
                correction="DSR (Bailey & López de Prado) bound to k_total per family; PBO/CSCV only if the arc's overfit_stats is reused unchanged; strata are NOT extra claims (reported descriptively)",
                rule="k never shrinks; survivors are counted against the full k; cells added after exposure are POST_EXPOSURE_HYPOTHESIS and excluded from confirmatory claims")
    with open(os.path.join(FAMILY_DIR, "MULTIPLICITY_UNIVERSE_V1.draft.json"), "w") as f:
        json.dump(mult, f, indent=1)
    print(json.dumps(dict(k=reg["k"], by_family=mult["k_by_family"], registry_sha=reg["registry_sha256_16"]), indent=1))


if __name__ == "__main__":
    main()
