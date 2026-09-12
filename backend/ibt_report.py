"""Render CHECKPOINT_FIRST_OUTCOME.md for INTRADAY_BREADTH_TRANSITION_V1 (read-only)."""
from __future__ import annotations
import os, sys, json, glob, hashlib, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import RunSpace                                          # noqa: E402
F = "/Users/sachoki/MASSIVE_DATA/INTRADAY_BREADTH_TRANSITION_V1"


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def f(x, nd=2):
    return "—" if x is None else (f"{x:+.{nd}f}" if isinstance(x, (int, float)) and not isinstance(x, bool) else str(x))


def main():
    run = [r for r in sorted(glob.glob(os.path.join(F, "runs", "OUT_*")), reverse=True) if RunSpace.is_current(r, "results.json")][0]
    S = json.load(open(os.path.join(run, "summary.json"))); R = json.load(open(os.path.join(run, "results.json")))
    seal = json.load(open(os.path.join(F, "SEAL.json"))); gov = json.load(open(os.path.join(F, "FIRST_OUTCOME_EXECUTION.json")))
    xrep = json.load(open(os.path.join(F, "runs", seal["x_run"], "build_report.json")))
    pre = seal["precheck"]; cen = xrep["census"]
    hdr = ("| cell | signal | selected | cooldown | MINE n_days | MINE raw med | MINE edge med | MINE day-win | MINE +yrs | MINE worst | DSR(k12) | VERIFY n_days | VERIFY raw med | VERIFY edge med | VERIFY day-win | VERIFY worst | class |\n"
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    def row(r):
        m, v = r["mine"], r["verify"]
        return (f"| {r['claim_id']} | {r['signal_true_n']:,} | {r['direct_selected_n']:,} | {100*r['cooldown_suppressed_n']/max(1,r['signal_true_n']):.0f}% | "
                f"{m['n_days']:,} | {f(m['raw_median'])} | {f(m['median_edge'])} | {f(m['day_win'],1)} | {m['positive_years']}/{m['years_counted']} | {f(m['worst_year'])} | {f(r['dsr'],3)} | "
                f"{v['n_days']:,} | {f(v['raw_median'])} | {f(v['median_edge'])} | {f(v['day_win'],1)} | {f(v['worst_year'])} | {r['classification']} |")
    md = f"""# INTRADAY_BREADTH_TRANSITION_V1 — CHECKPOINT · FIRST (AND ONLY) OUTCOME EXECUTION

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
Eligible rows {cen['rows']:,} · breadth available {cen['breadth_available']:,} · breadth quantiles 10/25/50/75/90 {cen['breadth_quantiles']} ·
event days T {cen['event_days']['T']:,} · F {cen['event_days']['F']:,} · both {cen['event_days']['T_and_F']:,}.
Breadth vs the day's OWN move: Spearman with |return| {pre['spearman_breadth_vs_abs_same_day_return']}, with the signed return {pre['spearman_breadth_vs_signed_same_day_return']},
with daily RVOL {pre['spearman_breadth_vs_rvol']} → breadth is a magnitude feature, not a sign feature; the cells therefore split by location and RVOL.

## The 12 registered cells (MINE decides, VERIFY replicates)
raw med = median of the cell's own trades (pp) · edge med = day-clustered median of (trade − same-day event-class control)
{hdr}
{chr(10).join(row(r) for r in R)}

## Reading
- Sum-based fields (mean, PF) are tail-sensitive in this universe; the day-clustered edge median vs the same-day control is the primary statistic; the raw median is shown so the level of the class itself is visible.
- Stop rule applies: no threshold / window / timeframe search follows; anything born from these numbers is POST_EXPOSURE_HYPOTHESIS.
"""
    p = os.path.join(F, "CHECKPOINT_FIRST_OUTCOME.md"); open(p, "w").write(md); print("written", p, _dig(p))


if __name__ == "__main__":
    main()
