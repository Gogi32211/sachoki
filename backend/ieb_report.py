"""Render CHECKPOINT_FIRST_OUTCOME.md for INTRADAY_EFFORT_BALANCE_V1 (read-only)."""
from __future__ import annotations
import os, sys, json, glob, hashlib, time
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import RunSpace                                          # noqa: E402
F = "/Users/sachoki/MASSIVE_DATA/INTRADAY_EFFORT_BALANCE_V1"


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def f(x, nd=2):
    return "—" if x is None else (f"{x:+.{nd}f}" if isinstance(x, (int, float)) and not isinstance(x, bool) else str(x))


def main():
    run = [r for r in sorted(glob.glob(os.path.join(F, "runs", "OUT_*")), reverse=True) if RunSpace.is_current(r, "results.json")][0]
    S = json.load(open(os.path.join(run, "summary.json"))); R = json.load(open(os.path.join(run, "results.json")))
    seal = json.load(open(os.path.join(F, "SEAL.json"))); gov = json.load(open(os.path.join(F, "FIRST_OUTCOME_EXECUTION.json")))
    xrep = json.load(open(os.path.join(F, "runs", seal["x_run"], "build_report.json")))
    pre = seal["precheck"]
    hdr = ("| cell | signal | selected | cooldown | MINE n_days | MINE median | MINE day-win | MINE +yrs | MINE worst | DSR(k16) | VERIFY n_days | VERIFY median | VERIFY day-win | VERIFY worst | class |\n"
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    def row(r):
        m, v = r["mine"], r["verify"]
        return (f"| {r['claim_id']} | {r['signal_true_n']:,} | {r['direct_selected_n']:,} | {100*r['cooldown_suppressed_n']/max(1,r['signal_true_n']):.0f}% | "
                f"{m['n_days']:,} | {f(m['median_edge'])} | {f(m['day_win'],1)} | {m['positive_years']}/{m['years_counted']} | {f(m['worst_year'])} | {f(r['dsr'],3)} | "
                f"{v['n_days']:,} | {f(v['median_edge'])} | {f(v['day_win'],1)} | {f(v['worst_year'])} | {r['classification']} |")
    ink = [r for r in R if r["in_k"]]; rep = [r for r in R if not r["in_k"]]
    md = f"""# INTRADAY_EFFORT_BALANCE_V1 — CHECKPOINT · FIRST (AND ONLY) OUTCOME EXECUTION

Written {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}.
```
seal               {seal['sealed_at']} · registry {seal['registry_sha256_16']} · k = {seal['k']} · X {seal['x_sha256_16']} (run {seal['x_run']})
outcome run        {S['run_id']} · results {gov['results_sha256_16']} · outcome_access_count = {S['outcome_access_count']}
_pathsim           direct sacred call per registered mask (cooldown included) · src {S['pathsim_src_sha256_16']} · trail atr_k 12 · maxh 60 · slip 0.0015
OOS                MINE {S['oos']['MINE'][0]}..{S['oos']['MINE'][1]} decides · VERIFY {S['oos']['VERIFY'][0]}..{S['oos']['VERIFY'][1]} replicates (mine survivors only)
classification     {' · '.join(f'{k} {v}' for k, v in sorted(S['classification_census'].items()))}
```
**The family is now SEARCH-EXPOSED.** Outcomes are PRICE RETURN (canonical 1D, dividends not adjusted).

## Pre-outcome census (X-only) and the free pre-check
Eligible rows {xrep['census']['rows']:,} · balance available {xrep['census']['available']['bal']:,} (V {xrep['census']['available']['bal_v']:,}; 1H {xrep['census']['available']['bal_1h']:,}) ·
median {xrep['census']['effort_bars_per_day_median']:.0f} effort bars of {xrep['census']['bars_per_day_median']:.0f} 15m bars per day · balance quantiles 5/25/50/75/95 {xrep['census']['bal_quantiles']}.
Balance vs the bar's own return: Spearman {pre['robust']['spearman']}, winsorized Pearson {pre['robust']['pearson_winsor_pm20']} (R² ≈ {pre['robust']['r2_winsor']}); B+ share RED {pre['robust']['share_Bplus']['RED']} vs GREEN {pre['robust']['share_Bplus']['GREEN']}.
→ the balance carries the candle but ~80 % of its variance is not the candle. Raw Pearson (−0.000) was destroyed by {pre['robust']['outliers']['eligible_rows_abs_ret_gt_500pct']} canonical ticker-lineage jumps
({pre['robust']['outliers']['placeholder_lineage_prev_close_lt_1']} placeholder relistings, rest genuine spikes) — disclosed, eligibility unchanged, medians unaffected.

## The 16 registered cells (MINE decides, VERIFY replicates)
{hdr}
{chr(10).join(row(r) for r in ink)}

## 1H replication of the class-A cells (descriptive echo test, not in k)
{hdr}
{chr(10).join(row(r) for r in rep)}

## Reading
- Conditional hypotheses: C1 = RED ∧ B+ ∧ RSI<40 (absorption under a red candle, oversold) → {next((r['classification'] for r in ink if r['claim_id']=='A|RED|B+|RSI<40'), '—')} (A), {next((r['classification'] for r in ink if r['claim_id']=='B|RED|B+|RSI<40'), '—')} (V);
  C2 = GREEN ∧ B- ∧ RSI≥60 (distribution under a green candle, overbought) → {next((r['classification'] for r in ink if r['claim_id']=='A|GREEN|B-|RSI>=60'), '—')} (A), {next((r['classification'] for r in ink if r['claim_id']=='B|GREEN|B-|RSI>=60'), '—')} (V).
- Sum-based fields (mean, PF) are tail-sensitive in this universe; the day-clustered median vs same-day control is the primary statistic.
- Stop rule applies: no threshold / L-combination / timeframe search follows; anything born from these numbers is POST_EXPOSURE_HYPOTHESIS.
"""
    p = os.path.join(F, "CHECKPOINT_FIRST_OUTCOME.md"); open(p, "w").write(md); print("written", p, _dig(p))


if __name__ == "__main__":
    main()
