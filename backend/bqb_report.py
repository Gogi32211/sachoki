"""Render CHECKPOINT_FIRST_OUTCOME.md for BQB_V1 (read-only)."""
from __future__ import annotations
import os, sys, json, glob, hashlib, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import RunSpace                                          # noqa: E402
F = "/Users/sachoki/MASSIVE_DATA/BQB_V1"


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def f(x, nd=2):
    return "—" if x is None else (f"{x:+.{nd}f}" if isinstance(x, (int, float)) and not isinstance(x, bool) else str(x))


def main():
    run = [r for r in sorted(glob.glob(os.path.join(F, "runs", "OUT_*")), reverse=True) if RunSpace.is_current(r, "results.json")][0]
    S = json.load(open(os.path.join(run, "summary.json"))); R = json.load(open(os.path.join(run, "results.json")))
    seal = json.load(open(os.path.join(F, "SEAL.json"))); gov = json.load(open(os.path.join(F, "FIRST_OUTCOME_EXECUTION.json")))
    xrep = json.load(open(os.path.join(F, "runs", seal["x_run"], "build_report.json")))
    cen = xrep["census"]
    hdr = ("| cell | signal | selected | cooldown | MINE n_days | MINE raw med | MINE edge med | MINE day-win | MINE +yrs | MINE worst | DSR(k6) | VERIFY n_days | VERIFY raw med | VERIFY edge med | VERIFY day-win | VERIFY worst | class |\n"
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    def row(r):
        m, v = r["mine"], r["verify"]
        return (f"| {r['claim_id']} | {r['signal_true_n']:,} | {r['direct_selected_n']:,} | {100*r['cooldown_suppressed_n']/max(1,r['signal_true_n']):.0f}% | "
                f"{m['n_days']:,} | {f(m['raw_median'])} | {f(m['median_edge'])} | {f(m['day_win'],1)} | {m['positive_years']}/{m['years_counted']} | {f(m['worst_year'])} | {f(r['dsr'],3)} | "
                f"{v['n_days']:,} | {f(v['raw_median'])} | {f(v['median_edge'])} | {f(v['day_win'],1)} | {f(v['worst_year'])} | {r['classification']} |")
    md = f"""# BQB_V1 — CHECKPOINT · FIRST (AND ONLY) OUTCOME EXECUTION

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
Pattern {xrep['params']} · eligible event days {cen['eligible_event_days']} · Q median {cen['median_age_Q']:.0f} bars after B1, B2 median {cen['median_age_B2']:.0f}.
Universe census (X-only, before the seal): after a burst a broad supply day follows in 53 %, a quiet pullback in 10.8 %, the full B1→Q→B2 sequence in 1.3 %.

## The 6 registered cells (MINE decides, VERIFY replicates) — B1 rows are the REFERENCE (buy the burst), the control class
raw med = median of the cell's own trades (pp) · edge med = day-clustered median of (trade − same-day B1-only control)
{hdr}
{chr(10).join(row(r) for r in R)}

## Reading
- The edge is RELATIVE to buying the burst, which itself loses (B1 raw medians −2..−4 pp). A positive edge with a negative raw median means 'less bad than chasing the burst', not 'profitable'.
- Sum-based fields (mean, PF) are tail-sensitive in this universe; the day-clustered edge median vs the same-day control is the primary statistic.
- Stop rule applies: no threshold / window / timeframe search follows; anything born from these numbers is POST_EXPOSURE_HYPOTHESIS.
"""
    p = os.path.join(F, "CHECKPOINT_FIRST_OUTCOME.md"); open(p, "w").write(md); print("written", p, _dig(p))


if __name__ == "__main__":
    main()
