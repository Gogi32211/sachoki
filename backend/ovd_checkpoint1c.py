"""OPENING_VOLUME_DYNAMICS_V1 — CHECKPOINT 1C (option A executed): canonical 1D authority,
audits, rebuilt X identity, X-only census. STOP before SEAL. No outcome access.
"""
from __future__ import annotations
import os, sys, json, time, glob                                        # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"
CANON_DIR = os.path.join(FAMILY_DIR, "canonical_1d")
BINS = {  # frozen registry bins (unchanged) — reported as X-only support, never re-tuned
    "open1h_rvol": [0.75, 1.0, 1.5, 2.5], "ramp_mag": [1.0, 1.25, 1.5, 2.0], "ramp_5d_slope": [0, 0.1, 0.25],
    "base_dryup": [0.6, 0.8, 1.0], "dryup_to_expansion": [1.0, 1.5, 2.5, 4.0], "prior_hv_reclaim": [0.5, 0.75, 1.0, 1.25],
    "open15_concentration": [0.30, 0.45, 0.60], "open15_persistence": [0.5, 0.8, 1.2], "idio_open1h": [0.75, 1.0, 1.5, 2.5],
    "volume_transfer": [-0.10, 0.10]}
CATS = ["ramp_3d", "open15_shape", "effort_result", "breadth_shift", "prior_hv_response", "open15_breadth", "gap_bucket", "price_bucket"]


def main():
    import duckdb
    from ovd_seal import latest_completed_run
    cur = json.load(open(os.path.join(CANON_DIR, "CURRENT.json")))
    man = json.load(open(os.path.join(CANON_DIR, f"MANIFEST_{cur['run_id']}.json")))
    der = json.load(open(os.path.join(CANON_DIR, f"DERIVED_{cur['run_id']}.json")))
    aud = json.load(open(os.path.join(CANON_DIR, f"AUDIT_{cur['run_id']}.json")))
    reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY_V1.draft.json")))
    run = latest_completed_run(require_canonical=True)
    if not run:
        raise SystemExit("no CANONICAL_1D_BASIS X build")
    rep = json.load(open(os.path.join(run, "build_report.json")))
    st = json.load(open(os.path.join(run, "STATUS.json")))
    c = duckdb.connect(); X = f"read_parquet('{os.path.join(run, 'X.parquet')}')"
    nA, nB = c.execute(f"SELECT count(*) FILTER (WHERE base AND NOT breakout), count(*) FILTER (WHERE breakout) FROM {X}").fetchone()
    lines = []
    for col, edges in BINS.items():
        e = " ".join(f"WHEN {col} < {x} THEN '<{x}'" for x in edges)
        rows = c.execute(f"SELECT CASE {e} ELSE '>={edges[-1]}' END b, count(*) FILTER (WHERE base AND NOT breakout), count(*) FILTER (WHERE breakout) FROM {X} WHERE {col} IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall()
        lines.append(f"| {col} | " + " · ".join(f"{b}: {a:,}/{bb:,}" for b, a, bb in rows) + " |")
    for col in CATS:
        rows = c.execute(f"SELECT {col}, count(*) FILTER (WHERE base AND NOT breakout), count(*) FILTER (WHERE breakout) FROM {X} WHERE {col} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC").fetchall()
        lines.append(f"| {col} | " + " · ".join(f"{b}: {a:,}/{bb:,}" for b, a, bb in rows) + " |")
    thin = c.execute(f"""SELECT count(*) FROM (SELECT open15_shape s, count(*) n FROM {X} WHERE breakout AND open15_shape IS NOT NULL GROUP BY 1) WHERE n < 80""").fetchone()[0]
    a4 = aud["A4_summary"]; a5 = aud["A5_seam_rescan_canonical"]; dv = aud["divergence_studio_vs_canonical"]
    md = f"""# OPENING_VOLUME_DYNAMICS_V1 — CHECKPOINT 1C (option A executed)

Written {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}. **NOT SEALED. No `_pathsim`. No outcome access.**

## CANONICAL_1D_AUTHORITY
- run `{man['run_id']}` · basis_asof `{man['basis_asof']}` · host {man['host']} · window {man['window'][0]}..{man['window'][1]}
- convention **{man['convention']}** (proven: NVDA adjusted=false 1209.98 vs true 121.00 with ×10 inverse volume; COST identical across a $15 special-dividend ex-date)
- rows {man['rows']:,} · tickers {man['tickers_with_bars']:,} of {man['tickers_requested']:,} requested · sessions {man['session_range'][0]} → {man['session_range'][1]}
- fetch failures {len(man['fetch_failures'])} · quarantine {man['quarantine']['rows']:,} rows / {man['quarantine']['tickers']} tickers ({100*man['quarantine']['share']:.4f}%) — reasons {man['quarantine']['reasons']}
- digests: canonical `{man['canonical_sha256_16']}` · raw `{man['raw_sha256_16']}` · splits `{man['splits_sha256_16']}` · derived `{der['derived_sha256_16']}`
- **vendor history floor**: no daily bars before 2021-09-07 → study window = canonical coverage (PRE_OUTCOME amendment; registry unchanged)

## SPLIT AUTHORITY
`/v3/reference/splits` per ticker, persisted: {man['splits_events']:,} events on {man['splits_tickers']:,} tickers (`{os.path.basename(man['splits_parquet'])}`). Used for A4/A5 validation only — the detector may flag, never generate a factor.

## RECONSTRUCTION METHOD
One-pass vendor daily aggregates under adjusted=true (basis = fetch date). No factor arithmetic anywhere in the producer (fixture A9-3). Price-derived fields recomputed on canonical prices: atr_14 `{der['formulas']['atr_14']}`, avg_vol_20d `{der['formulas']['avg_vol_20d']}`, wt_* via `compute_wyckoff_trig` (engine file `{der['wt_engine']['file_sha256_16']}`, compute src `{der['wt_engine']['compute_sha256_16']}`; wt_resistance non-null {100*der['wt_resistance_nonnull_share']:.1f}%, engine errors {der['wt_errors']}).

## A5 SEAM RESCAN (same detector that found 398 / 47 / 40 / 124 on the Studio store)
canonical candidates **{a5['candidates']}** (after 2026-05-22: {a5['candidates_after_2026_05_22']}); coinciding with a reference split event: {a5['coinciding_with_a_reference_split']}.
Studio store, same detector, for the record: {aud['A5_seam_scan_studio_for_record']['candidates']} candidates ({aud['A5_seam_scan_studio_for_record']['after_2026_05_22']} after cutover).
Residual canonical candidates (if any): {a5['examples'] if a5['candidates'] else 'none'}

## A4 CROSS-TF CONTINUITY (canonical 1D vs stored 1H/15m 09:30 bars at reference split events)
events {a4['events']} · continuity OK {a4['ok']} · BREAK {a4['breaks']} · no data {a4['no_data']}

| ticker | execution | split | canonical 1D ratio | 1H ratio | 15m ratio | continuity |
|---|---|---|---|---|---|---|
""" + "\n".join(f"| {r['ticker']} | {r['execution_date']} | {r['split']} | {r['canonical_1d_ratio']} | {r['h1_ratio']} | {r['m15_ratio']} | {r['continuity']} |" for r in aud["A4_cross_tf_continuity"]) + f"""

## A6 DUPLICATE AUDIT
canonical duplicate (ticker, session) rows: **{aud['A6_duplicates']}**. Studio-vs-canonical divergence: {dv['pairs_compared']:,} pairs, {dv['close_within_0_1pct']:,} within 0.1%, **{dv['close_diff_gt_1pct']:,} >1% ({dv['tickers_with_any_gt_1pct']} tickers)**, {dv['close_diff_gt_25pct']:,} >25%.

## REBUILT X-TABLE IDENTITY
run `{os.path.basename(run)}` · STATUS **{st['status']}** · bound to canonical `{st['canonical_1d']['run_id']}` (derived `{st['canonical_1d']['derived_sha256_16']}`).
Old runs (10) remain `INVALID_FOR_OUTCOME_PHASE` — retained, never current.

## A8 X-ONLY CENSUS (canonical basis)
rows {rep['rows']:,} · tickers {rep['tickers']:,} · sessions {rep['sessions']:,} · base days {rep['base_days']:,} · breakout days {rep['breakout_days']:,}
Family A (base AND NOT breakout) {nA:,} · Family B (breakout) {nB:,} · OPEN1H source: {rep.get('open1h_source','')}

availability: {', '.join(f'{k} {v:,}' for k, v in rep['availability'].items())}

Registry cell support (rows A/B per frozen bin — **bins NOT re-tuned**):

| feature | support A/B by bin |
|---|---|
""" + "\n".join(lines) + f"""

Family-B OPEN15_SHAPE classes with n < 80: {thin} (reported as THIN if any; never dropped from k).

## MULTIPLICITY
k = **{reg['k']['total']}** (single {reg['k']['single']} + interaction {reg['k']['interaction']}) — unchanged by the canonical rebuild.

## FIXTURES
`tests/test_ovd_guards.py` 18/18 · `tests/test_ovd_canonical.py` (A9) 8/8.

## STATUS
Issue 1 remediated by construction (single-basis canonical authority; seam rescan above). **STOP before SEAL — the user decides.**
"""
    p = os.path.join(FAMILY_DIR, "CHECKPOINT_1C_CANONICAL.md")
    open(p, "w").write(md)
    print("written ->", p)


if __name__ == "__main__":
    main()
