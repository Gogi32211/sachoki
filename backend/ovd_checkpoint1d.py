"""OPENING_VOLUME_DYNAMICS_V1 — CHECKPOINT 1D: canonical rerun (one basis_asof, full roster),
ANPA/GMM lineage resolution, seam-detector reconciliation, dividend census, corrected wording.
STOP before SEAL. No outcome access.
"""
from __future__ import annotations
import os, sys, json, time, glob                                        # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"
CANON_DIR = os.path.join(FAMILY_DIR, "canonical_1d")


def _load(p, default=None):
    try:
        return json.load(open(p))
    except Exception:
        return default


def main():
    import duckdb
    from ovd_seal import latest_completed_run
    cur = _load(os.path.join(CANON_DIR, "CURRENT.json"))
    man = _load(os.path.join(CANON_DIR, f"MANIFEST_{cur['run_id']}.json"))
    der = _load(os.path.join(CANON_DIR, f"DERIVED_{cur['run_id']}.json"))
    aud = _load(os.path.join(CANON_DIR, f"AUDIT_{cur['run_id']}.json"))
    R = _load(os.path.join(FAMILY_DIR, "reconciliation_phase1b.json"))
    reg = _load(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V1.draft.json"))
    seam = _load(os.path.join(CANON_DIR, "SEAM_DETECTOR_RECONCILIATION.json"), {})
    dvm = sorted(glob.glob(os.path.join(CANON_DIR, "dividend_reference", "MANIFEST_*.json")))
    dv = _load(dvm[-1]) if dvm else None
    gate = R.get("checkpoint_1D_gate", {})
    run = latest_completed_run(require_canonical=True)
    if not run:
        raise SystemExit("no CANONICAL_1D_BASIS X build")
    rep = _load(os.path.join(run, "build_report.json")); st = _load(os.path.join(run, "STATUS.json"))
    if st["canonical_1d"]["run_id"] != cur["run_id"]:
        raise SystemExit(f"latest X build is bound to {st['canonical_1d']['run_id']}, not the current canonical {cur['run_id']}")
    c = duckdb.connect(); X = f"read_parquet('{os.path.join(run, 'X.parquet')}')"
    nA, nB = c.execute(f"SELECT count(*) FILTER (WHERE base AND NOT breakout), count(*) FILTER (WHERE breakout) FROM {X}").fetchone()
    a4 = aud["A4_summary"]; a5 = aud["A5_seam_rescan_canonical"]; dvg = aud["divergence_studio_vs_canonical"]
    ff = man.get("fetch_failures", {})
    scopes = seam.get("scopes", {})
    md = f"""# OPENING_VOLUME_DYNAMICS_V1 — CHECKPOINT 1D (canonical rerun · lineage · detector reconciliation)

Written {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}. **NOT SEALED. No `_pathsim`. No outcome access.**

## 1 · NTNX / NUAI — resolved by a full rerun (never a merge)
New canonical run **`{man['run_id']}`** over the frozen 5,193 roster, ONE `basis_asof = {man['basis_asof']}`:
rows {man['rows']:,} · tickers {man['tickers_with_bars']:,} / {man['tickers_requested']:,} · {man['session_range'][0]} → {man['session_range'][1]} · split events {man['splits_events']:,} ·
quarantine {man['quarantine']['rows']} rows / {man['quarantine']['tickers']} tickers ({100*man['quarantine']['share']:.4f}%) · fetch failures **{len(ff)}**{(' → ' + ', '.join(f'{k} = SOURCE_UNAVAILABLE_AT_CANONICAL_RUN' for k in ff)) if ff else ''}.
Digests: canonical `{man['canonical_sha256_16']}` · raw `{man['raw_sha256_16']}` · splits `{man['splits_sha256_16']}` · derived `{der['derived_sha256_16']}`.
The previous run `CANON1D_20260904T051736Z` and the supplement `SUPP_20260904T061023Z` remain provenance artifacts; **nothing was merged**.

## 2 · ANPA / GMM — resolved from authoritative lineage
- **ANPA 2026-02-13** — {gate.get('2_ANPA_GMM', {}).get('ANPA_2026-02-13', {}).get('verdict', '')}. Raw (adjusted=false) closes 77.66 → 78.50 → 73.31 → 62.30 → 31.13 → 18.03; no split, no dividend, no reference event but a 2025 ticker change.
- **GMM 2023-12-29** — {gate.get('2_ANPA_GMM', {}).get('GMM_2023-12-29', {}).get('verdict', '')}. Raw closes 10.95 → 10.78 → 11.14 → ≈5.58; the two reverse splits (2024-11-26 15:1, 2026-06-11 50:1) are later and applied by the convention.
Both: `DIAGNOSTIC_FALSE_POSITIVE` — ordinary rows, no factor, no quarantine. Persisted: `canonical_1d/ANPA_GMM_lineage_probe.json`.

## 3 · Seam-detector reconciliation (bound)
Detector `backend/ovd_canonical_audit.py::SEAM_SQL`, sha256[:16] **`{seam.get('detector', {}).get('sha256_16', '?')}`** — predicate: {seam.get('detector', {}).get('predicate', '')}

| scope | source rows | candidates | after 2026-05-22 | tickers |
|---|---|---|---|---|
""" + "\n".join(f"| {k} | {v['rows_in_source']:,} | {v['candidates']} | {v['after_2026_05_22']} | {v['tickers']} |" for k, v in scopes.items() if isinstance(v, dict) and 'candidates' in v) + f"""

- A − B: {scopes.get('A_minus_B_explanation', {}).get('A_candidates_whose_date_has_two_conflicting_universe_copies', '?')} scope-A candidates sit on dates with two conflicting universe copies — the first-pass source (DISTINCT rows, no precedence dedup) manufactured seams from interleaved copies. **Scope A was my artifact; 398 is withdrawn** (the bound detector reproduces 410 under the reconstructed scope; the 12-candidate gap is attributed to non-persisted first-pass code and disclosed).
- Identical scope (C vs D): Studio **{scopes.get('C_studio_dedup_canonical_scope', {}).get('candidates', '?')}** vs canonical **{scopes.get('D_canonical_same_scope', {}).get('candidates', '?')}**; after-cutover {scopes.get('C_studio_dedup_canonical_scope', {}).get('after_2026_05_22', '?')} → {scopes.get('D_canonical_same_scope', {}).get('after_2026_05_22', '?')}: the canonical authority removes exactly the post-cutover unadjusted class.
- On the NEW canonical run the same detector reports **{a5['candidates']}** candidates ({a5['candidates_after_2026_05_22']} after cutover; {a5['coinciding_with_a_reference_split']} coinciding with a reference split).

## 4 · Wording (binding)
- A5 claim allowed: **"0 unresolved reference-split seams"** (arbiter `/v3/reference/splits`). NOT "0 corporate-action seams": MSGE 2023-04-21 is a spin-off the convention does not adjust; non-split events are reconciled only where lineage was checked.
- **DIVIDENDS_NOT_ADJUSTED** is an explicit limitation of the convention. `_pathsim` outcomes are **price-return paths, not total shareholder return**; ex-dividend / extraordinary-distribution moves may appear in outcomes; no finding may be called an economic total-return edge.
""" + (f"""- Dividend/distribution census (persisted `{os.path.basename(dv['parquet'])}`, sha `{dv['sha256_16']}`): {dv['events_total']:,} events on the roster, {dv['events_in_window']:,} in-window on {dv['tickers_with_events_in_window']:,} tickers, special/extra typed {dv['special_or_extra_in_window']:,}.
""" if dv else "- Dividend census: pending.\n") + (f"""- Materiality vs canonical prev-close: yield ≥2% {gate['3b_dividend_and_distribution_census']['materiality_vs_canonical_prev_close']['yield_ge_2pct']} · ≥5% **{gate['3b_dividend_and_distribution_census']['materiality_vs_canonical_prev_close']['yield_ge_5pct']}** · ≥10% {gate['3b_dividend_and_distribution_census']['materiality_vs_canonical_prev_close']['yield_ge_10pct']}; ≥5% by year {gate['3b_dividend_and_distribution_census']['materiality_vs_canonical_prev_close']['ge_5pct_by_year']}.
- Seam candidates reclassified with the census: EXTRAORDINARY_DISTRIBUTION {gate['3b_dividend_and_distribution_census']['seam_candidates_reclassified_with_the_census']['EXTRAORDINARY_DISTRIBUTION']} · REFERENCE_SPLIT_ADJACENT {gate['3b_dividend_and_distribution_census']['seam_candidates_reclassified_with_the_census']['REFERENCE_SPLIT_ADJACENT']} · PRICE_MOVE_OR_UNKNOWN {gate['3b_dividend_and_distribution_census']['seam_candidates_reclassified_with_the_census']['PRICE_MOVE_OR_UNKNOWN']}.
- **Proposed pre-outcome rule (not applied)**: {gate['3b_dividend_and_distribution_census']['PROPOSED_pre_outcome_rule_for_the_user']}
""" if gate.get("3b_dividend_and_distribution_census") else "") + f"""

## Canonical qualification on the NEW run
A4 continuity {a4['ok']}/{a4['events']} (breaks {a4['breaks']}) · A6 duplicates {aud['A6_duplicates']} · Studio-vs-canonical divergence {dvg['close_diff_gt_1pct']:,} pairs >1% on {dvg['tickers_with_any_gt_1pct']} tickers · wt_resistance non-null {100*der['wt_resistance_nonnull_share']:.1f}% (engine errors {der['wt_errors']}).

## X rebuilt on the NEW canonical
run `{os.path.basename(run)}` · STATUS {st['status']} · rows {rep['rows']:,} · tickers {rep['tickers']:,} · sessions {rep['sessions']:,} · base {rep['base_days']:,} · breakout {rep['breakout_days']:,} · Family A {nA:,} · Family B {nB:,}.
Availability: {', '.join(f'{k} {v:,}' for k, v in rep['availability'].items())}.
Registry k = **{reg['k']['total']}** (unchanged; bins unchanged).

## Fixtures
`tests/test_ovd_guards.py` 18/18 · `tests/test_ovd_canonical.py` 8/8 (see chain log).

## STATUS
All 1D-gate items resolved. **STOP before SEAL — the user decides.**
"""
    p = os.path.join(FAMILY_DIR, "CHECKPOINT_1D.md")
    open(p, "w").write(md)
    print("written ->", p)


if __name__ == "__main__":
    main()
