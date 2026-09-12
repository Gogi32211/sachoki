"""Render CHECKPOINT_3_FIRST_OUTCOME.md from the COMPLETED first outcome run (read-only; no _pathsim)."""
from __future__ import annotations
import os, sys, json, glob, hashlib, time
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import RunSpace                                          # noqa: E402
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def current_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OUT_*")), reverse=True):
        if RunSpace.is_current(r, "results_300.parquet"):
            return r
    raise SystemExit("no CURRENT outcome run")


def f(x, nd=2):
    return "—" if x is None else f"{x:+.{nd}f}" if isinstance(x, (int, float)) and not isinstance(x, bool) else str(x)


def row(r):
    yrs = r.get("per_year") or {}
    return (f"| {r['claim_id']} | {r['family']} | {r['n_obs']:,} | {r['n_days']:,} | {f(r['median_edge'])} | {f(r['day_win'],1)} | {f(r['raw_median'])} | "
            f"{f(r['win_rate'],1)} | {f(r['profit_factor'])} | {r['positive_years']}/{r['years_counted']} | {f(r['worst_year'])} | "
            f"{f(r['dsr'],3)} | {f(r['top2_share'],1)} | {r['classification']}{' (THIN)' if r['thin'] else ''} |")


HDR = ("| claim | fam | n_obs | n_days | median edge (pp) | day-win % | raw median % | win % | PF | +yrs | worst yr | DSR | top2 % | class |\n"
       "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")


def main():
    run = current_run(); rid = os.path.basename(run)
    S = json.load(open(os.path.join(run, "summary.json"))); R = json.load(open(os.path.join(run, "results_300.json")))
    gov_p = os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION_V1.json"); G = json.load(open(gov_p))
    led = json.load(open(os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")))
    by = {r["claim_id"]: r for r in R}
    cls = S["classification_census"]
    seven = [r for r in R if r["kind"] == "state"]
    builds = sorted([r for r in R if r["classification"] == "BUILD_CANDIDATE"], key=lambda r: -(r["median_edge"] or 0))
    vetos = sorted([r for r in R if r["classification"] == "VETO_CANDIDATE"], key=lambda r: (r["median_edge"] or 0))
    nonthin = [r for r in R if not r["thin"] and r["median_edge"] is not None]
    top_pos = sorted(nonthin, key=lambda r: -(r["median_edge"] or 0))[:12]
    top_neg = sorted(nonthin, key=lambda r: (r["median_edge"] or 0))[:12]
    dsrs = np.array([r["dsr"] for r in R if r["dsr"] is not None], float)
    meds = np.array([r["median_edge"] for r in nonthin], float)
    # three-resolution grouping per logic (never a ranking)
    res_tbl = {}
    for r in R:
        key = (r["logic"], r["resolution"])
        res_tbl.setdefault(key, []).append(r)
    res_lines = []
    for (lg, res), rs in sorted(res_tbl.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        m = [x["median_edge"] for x in rs if x["median_edge"] is not None and not x["thin"]]
        res_lines.append(f"| Logic {lg} | {res} | {len(rs)} | {len(m)} | {f(float(np.median(m))) if m else '—'} | "
                         f"{sum(1 for x in rs if x['classification']=='BUILD_CANDIDATE')} / {sum(1 for x in rs if x['classification']=='VETO_CANDIDATE')} |")
    # horizon sweep for the seven + top cells (descriptive)
    def sweep(r):
        return " · ".join(f"h{h}: {f(r.get(f'median_edge_h{h}' if h != 60 else 'median_edge'))}" for h in (3, 5, 10, 20, 60))
    # distribution diagnostic aggregate
    dd = [(r["claim_id"], r["distribution_diagnostic"]) for r in R if r.get("distribution_diagnostic")]
    exp_share = S.get("dist_exposed_rows")
    yearly = {}
    for r in nonthin:
        for y, v in (r.get("per_year") or {}).items():
            yearly.setdefault(int(y), []).append(v)
    rc_p = os.path.join(FAMILY_DIR, f"ROW_CONSERVATION_{rid}.json"); rc = json.load(open(rc_p)); cons_sha = _dig(rc_p)
    cens = " · ".join(f"h{h} {v['RIGHT_CENSORED_inside_defined']:,}" for h, v in sorted(rc["horizons"].items(), key=lambda kv: int(kv[0])))
    cons_txt = (f"X_v2 rows {rc['x_rows']:,} = submitted {rc['submitted_to_pathsim']:,} → defined {rc['horizons']['60']['pathsim_defined']:,} "
                f"+ NO_NEXT_SESSION {rc['drops']['NO_NEXT_SESSION']:,} + NO_ENTRY_OPEN {rc['drops']['NO_ENTRY_OPEN']} "
                f"+ COOLDOWN_SKIPPED {rc['drops']['COOLDOWN_SKIPPED_by_construction']} (exact on all five horizons). " + rc["explanation"]
                + " ATR fallback rows: 0. RIGHT_CENSORED inside the defined set: " + cens
                + ". The earlier phrase 'eligible rows minus each ticker's last session' is RETRACTED (addendum record bound to the governance record).")
    md = f"""# OPENING_VOLUME_DYNAMICS — CHECKPOINT 3 · FIRST HISTORICAL OUTCOME EXECUTION (SEALED RECORD)

Written {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}.
```
execution authority   SEAL_V2_1  {S['seal_v2_1_sha256']}
run                   {rid}   results_300.parquet {G['results_sha256_16']}   summary {G['summary_sha256_16']}
governance record     FIRST_OUTCOME_EXECUTION_V1.json  {_dig(gov_p)}
first outcome access  {S['first_outcome_access_at']}    outcome_access_count = {led['outcome_access_count']}   (ledger never resets)
_pathsim              edge_replay.py::_pathsim  src {S['pathsim_src_sha256_16']}  params {S['params_sha256_16']}  ({S['params']['mode']}, atr_k {S['params']['atr_k']}, maxh {S['params']['maxh']}, slip {S['params']['slip']})
registry              {S['registry_sha256_16']}   k = {S['k']}   tested {S['reconciliation']['tested']} · missing {S['reconciliation']['missing']} · extra {S['reconciliation']['extra']} · duplicate {S['reconciliation']['duplicate']}
input census          {S['input_census_sha256_16']}  (the seven V2.1 cells reconcile exactly to the sealed pre-outcome census)
fixtures              {S['fixtures']['passed']}/{S['fixtures']['tests']}
row outcomes          {' · '.join(f"h{h}: {v['n']:,} (censored {v['censored']:,})" for h, v in S['row_outcomes'].items())}
```
**The family is now SEARCH-EXPOSED.** BUILD_CANDIDATE ≠ confirmed edge; VETO_CANDIDATE ≠ production veto; every hypothesis born from these
numbers is POST_EXPOSURE_HYPOTHESIS. Outcomes are PRICE RETURN (dividends not adjusted), not total shareholder return.

## Row-outcome conservation (engineering; locked before any scientific reading) — `ROW_CONSERVATION_{rid}.json` {cons_sha}
{cons_txt}

## Execution-design decisions (recorded before any outcome existed)
{chr(10).join('- ' + d for d in G['execution_design_decisions'])}
- Classification gates: {json.dumps(S['gates']['build'])} · veto = mirror · THIN = {json.dumps(S['gates']['thin'])} — {S['gates']['source']}.
- Primary statistic: leave-one-out same-day control within the family on rows where the cell's feature is evaluable (≥ {S['min_control']} others), edge = ret − control,
  day median → cell median over days. Multiplicity: overfit_stats.dsr with the cell's day-edge series, trial family = the 300 cells' day-edge Sharpes, n_trials = 300.

## Classification census (all 300 registered cells; thin cells stay in k)
{' · '.join(f'{k} {v}' for k, v in sorted(cls.items()))}
DSR (k=300) over cells: n {len(dsrs)} · median {np.median(dsrs):.3f} · ≥ 0.95: {int((dsrs >= 0.95).sum())} · ≥ 0.90: {int((dsrs >= 0.90).sum())}.
Median day-edge over non-thin cells: median {np.median(meds):+.3f} pp · IQR [{np.percentile(meds,25):+.3f}, {np.percentile(meds,75):+.3f}] · share > 0: {(meds>0).mean()*100:.0f}%.

## The seven V2.1 cells (Logic 1 / 2 / 3 / 4 — reported individually; A and B never collapsed)
{HDR}
{chr(10).join(row(r) for r in seven)}
Horizon sweep (DESCRIPTIVE / SENSITIVITY ONLY — median day-edge, pp):
{chr(10).join(f"- {r['claim_id']}: {sweep(r)}" for r in seven)}
Direct cell-mask reconciliation (the engine's 5-bar cooldown applied, descriptive): {json.dumps(S['direct_cell_mask_reconciliation'])}
Logic 4 rows enter at D+2 open; never same-day D+1 reversal evidence. Logic 3: CLOSE60/OPEN60 ≥ 1.0 was known to be broad — not retrofitted.

## Three-resolution view (structure, not a ranking — no "best timeframe")
| logic | resolution | cells | non-thin | median of cell medians (pp) | BUILD / VETO |
|---|---|---|---|---|---|
{chr(10).join(res_lines)}

## Strongest registered BUILD candidates ({len(builds)})
{HDR}
{chr(10).join(row(r) for r in builds[:15]) if builds else '| — none passed every registered gate — | | | | | | | | | | | | | |'}

## Strongest registered VETO candidates ({len(vetos)})
{HDR}
{chr(10).join(row(r) for r in vetos[:15]) if vetos else '| — none passed every registered veto gate — | | | | | | | | | | | | | |'}

## Largest positive / negative cell medians among non-thin cells (descriptive; gates decide, not rank)
{HDR}
{chr(10).join(row(r) for r in top_pos)}

{HDR}
{chr(10).join(row(r) for r in top_neg)}

## Yearly robustness (median over non-thin cells of the per-year median day-edge, pp; 2021 = Sep–Dec, 2026 = to Sep-03)
{' · '.join(f'{y}: {np.median(v):+.2f} (n={len(v)})' for y, v in sorted(yearly.items()))}

## A10 distribution diagnostic (POST_ENTRY_DISTRIBUTION_EXPOSURE_GE5; descriptive, never promotes / vetoes / removes rows)
Exposed row-outcomes: {exp_share:,} of {S['row_outcomes']['60']['n']:,}. For the seven cells and BUILD candidates (median day-edge, all / exposed / non-exposed):
{chr(10).join(f"- {cid}: all {f(d['all']['median_edge'])} (days {d['all']['n_days']}) · exposed {f(d['exposed']['median_edge'])} (obs {d['exposed']['n_obs']}) · non-exposed {f(d['non_exposed']['median_edge'])}" for cid, d in dd if by[cid]['kind']=='state' or by[cid]['classification']=='BUILD_CANDIDATE')}
Ex-distribution stratification is post-entry descriptive diagnostics, not causal predictor evidence; it does not cover every non-split corporate action.

## Disclosed defects / limitations
{chr(10).join('- ' + d for d in G['disclosed_defects']) if G['disclosed_defects'] else '- none recorded during execution'}
- Right-censoring: entries in the last {S['params']['maxh']} sessions before 2026-09-03 close at the last available bar (counts above).
- The runner (`ovd_outcome_v1.py`, sha {G['runner_sha256_16']}) is the single authorised `_pathsim` call site; the pre-outcome guard `assert_no_outcome_access` now trips by design (family exposed).

## STOP
Complete registered historical outcome phase executed once. No V3 created. No thresholds added. No second search started.
"""
    p = os.path.join(FAMILY_DIR, "CHECKPOINT_3_FIRST_OUTCOME.md")
    open(p, "w").write(md)
    print("written", p)


if __name__ == "__main__":
    main()
