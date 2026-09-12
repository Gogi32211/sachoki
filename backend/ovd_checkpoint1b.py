"""OPENING_VOLUME_DYNAMICS_V1 — PRE-SEAL RECONCILIATION checkpoint (1B) writer.

Reads reconciliation_phase1b.json, the latest COMPLETED X build and the draft registry;
computes per-family evaluable rows over X.parquet (X-only); writes
CHECKPOINT_1B_RECONCILIATION.md. No outcome access. Does not seal.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"
FEATS = ["open1h_rvol", "ramp_3d", "ramp_5d_slope", "base_dryup", "dryup_to_expansion", "effort_result",
         "breadth_shift", "prior_hv_reclaim", "prior_hv_response", "idio_open1h", "open15_breadth",
         "open15_shape", "open15_persistence", "open15_concentration", "volume_transfer"]


def main():
    import duckdb
    from ovd_seal import latest_completed_run
    R = json.load(open(os.path.join(FAMILY_DIR, "reconciliation_phase1b.json")))
    reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V1.draft.json")))
    run = latest_completed_run()
    if not run:
        raise SystemExit("no COMPLETED X build")
    rep = json.load(open(os.path.join(run, "build_report.json")))
    c = duckdb.connect(); X = f"read_parquet('{os.path.join(run, 'X.parquet')}')"
    nA, nB = c.execute(f"SELECT count(*) FILTER (WHERE base AND NOT breakout), count(*) FILTER (WHERE breakout) FROM {X}").fetchone()
    tA, tB = c.execute(f"SELECT count(DISTINCT ticker) FILTER (WHERE base AND NOT breakout), count(DISTINCT ticker) FILTER (WHERE breakout) FROM {X}").fetchone()
    ev = {f: c.execute(f"SELECT count({f}) FILTER (WHERE base AND NOT breakout), count({f}) FILTER (WHERE breakout) FROM {X}").fetchone() for f in FEATS}
    recon = c.execute(f"SELECT count(*) FILTER (WHERE open1h_vol IS NOT NULL AND open1h_vol_1hstore IS NOT NULL), "
                      f"count(*) FILTER (WHERE open1h_vol IS NOT NULL AND open1h_vol_1hstore IS NOT NULL AND abs(open1h_vol-open1h_vol_1hstore) <= 0.001*open1h_vol_1hstore) FROM {X}").fetchone()
    i1, i2, i3, i4, i5, i6, i7 = (R["issue_1_adjustment_authority"], R["issue_2_duplicate_authority"], R["issue_3_universe_taxonomy"],
                                  R["issue_4_open1h_source"], R["issue_5_family_B_and_intraday_engine"], R["issue_6_prior_hv_event"], R["issue_7_statistical_identity"])
    ks = i1["known_split_audit_1D"]; seam = i1["seam_scan_1D_whole_store"]; el = seam["inside_THIS_family_eligibility (prev close>=5, dv>=3M)"]
    md = f"""# OPENING_VOLUME_DYNAMICS_V1 — PRE-SEAL RECONCILIATION (checkpoint 1B)

Written {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}. **NOT SEALED. No `_pathsim` call. No outcome access.**
Machine-readable record: `reconciliation_phase1b.json`. X build used below: `{os.path.basename(run)}` (COMPLETED).

## 1 · Adjustment / corporate-action authority — **HARD STOP**

- Lineage: 1D bulk = CSV import (files gone, exporter unknown → status established empirically only); 1D daily delta =
  `api_bar_signals` per ticker (adjusted-as-of-fetch, **history not re-adjusted**); 1H/15m = Polygon `adjusted=true`;
  **no corporate-action table, no split utility** anywhere in the repo.
- Known-split audit (close ratio across the split date): {', '.join(f'{k.split()[0]} {v}' for k, v in ks.items() if k != 'reading')} → bulk era and 2024 are adjusted; 1H/15m adjusted.
- Whole-store seam scan: {seam['price_jumps_at_split_ratios']:,} split-ratio jumps, **{seam['with_inverse_volume_jump (unadjusted-split signature)']} with the unadjusted signature**,
  {seam['of_which_after_2026-05-22']} after the 2026-05-22 cutover. Inside this family's eligibility: **{el['tickers']} tickers / {el['events']} events** ({seam['share_of_eligible_1D_rows_on_seam_tickers_after_their_first_seam']} of eligible rows),
  e.g. {', '.join(seam['examples_after_cutover'][:4])}.
- Cross-timeframe inconsistency proven: {seam['cross_timeframe_inconsistency']}
- **Verdict**: {i1['verdict']}
- Remediation options (user's decision): {' · '.join(i1['remediation_options_for_the_user'])}

## 2 · Duplicate-row authority — RESOLVED

{i2['duplicated_keys']:,} duplicated keys ({i2['rows_involved']:,} rows); OHLCV-identical {i2['OHLCV_identical_keys']:,}; conflicting {i2['OHLCV_conflicting_keys']:,}
(field conflicts: {', '.join(f'{k} {v:,}' for k, v in i2['field_conflicts'].items())}). Authority: {i2['authority_found']}.
Defect fixed: {i2['defect_in_first_checkpoint']}

## 3 · Universe taxonomy — CORRECTED

1D-eligible tickers {i3['1D_eligible_tickers_72m']:,} · any 1H {i3['with_any_1H']:,} · any 15m {i3['with_any_15m']:,} · both {i3['with_both']:,} (stores hold {i3['lower_TF_store_tickers']:,}).
X.parquet ({os.path.basename(run)}): rows {rep['rows']:,}, tickers {rep['tickers']:,}, sessions {rep['sessions']:,}.
Family A (base AND NOT breakout): {nA:,} rows / {tA:,} tickers · Family B (breakout): {nB:,} rows / {tB:,} tickers.
{i3['wording_fix']}

| feature | evaluable rows A | evaluable rows B |
|---|---|---|
""" + "\n".join(f"| {f} | {a:,} | {b:,} |" for f, (a, b) in ev.items()) + f"""

Control guard: {i3['control_guard']}

## 4 · OPEN1H primary source — FROZEN

Reconciliation (15m M1+M2+M3+M4 vs stored 1H 09:30): compared {i4['reconciliation']['compared']:,} · exact {i4['reconciliation']['exact_match']:,} · within 0.1% {i4['reconciliation']['within_0_1pct']:,} · >1% apart {i4['reconciliation']['mismatch_gt_1pct']:,} · 15m-only {i4['reconciliation']['m15_only']:,} · 1H-only {i4['reconciliation']['h1_only']} · partial 15m hours {i4['reconciliation']['m15_partial_hours']:,}.
In this X build: {recon[0]:,} rows carry both sources, {recon[1]:,} agree within 0.1%.
**Decision**: {i4['frozen_decision']}
Consequence: {i4['consequence']}

## 5 · Family A / B causal claims — RENAMED

Family A = PREBREAKOUT_BASE_VOLUME_STATE → path outcome from D+1 open. Family B = {i5['family_B_claim_renamed']}
Same-day question: {i5['same_day_question']}.

## 6 · Intraday path-engine discovery

{i5['intraday_engine_discovery']}

## 7 · Prior high-volume event — REPLACED (neutral)

Old: {i6['old']}
New: **{i6['new']}**. Response feature: {i6['response_feature']}. Reclaim: {i6['reclaim_feature']}. Constants: {i6['constants']}. {i6['language']}.

## 8 · Multiplicity — FROZEN

{i7['multiplicity']}

## 9 · Same-day control — FROZEN

{i7['same_day_control']}

## 10 · Horizon sweep — FROZEN

{i7['horizon_sweep']}

## 11 · BREAKOUT predicate — ONE form

{i7['breakout_predicate']}

## 12 · Registry

k = **{reg['k']['total']}** (single {reg['k']['single']} + interaction {reg['k']['interaction']}); draft sha `{reg['registry_sha256_16']}`; change since checkpoint 1: {R['registry']['change_from_checkpoint_1']}.

## 13 · Fixtures

{R['fixtures']}.

## STATUS

Issues 2–7 resolved and frozen. **Issue 1 remains a HARD STOP: SEAL.json will not be written and `_pathsim` will not be
called until the user selects remediation A, B or C.**
"""
    p = os.path.join(FAMILY_DIR, "CHECKPOINT_1B_RECONCILIATION.md")
    open(p, "w").write(md)
    print("written ->", p)


if __name__ == "__main__":
    main()
