"""Render CHECKPOINT_3B_DIRECT_CORRECTION.md from the COMPLETED direct-pathsim correction run (read-only)."""
from __future__ import annotations
import os, sys, json, glob, hashlib, time
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import RunSpace                                          # noqa: E402
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def f(x, nd=2):
    return "—" if x is None else (f"{x:+.{nd}f}" if isinstance(x, (int, float)) and not isinstance(x, bool) else str(x))


def main():
    runs = [r for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OUTDIRECT_*")), reverse=True) if RunSpace.is_current(r, "results_300_direct.parquet")]
    if not runs:
        raise SystemExit("no CURRENT direct run")
    run = runs[0]; rid = os.path.basename(run)
    S = json.load(open(os.path.join(run, "summary.json"))); R = json.load(open(os.path.join(run, "results_300_direct.json")))
    A = json.load(open(os.path.join(FAMILY_DIR, "PATHSIM_SEMANTICS_AUTHORITY.json")))
    G = json.load(open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION_V1_CORRECTION_DIRECT_PATHSIM.json")))
    C1 = json.load(open(os.path.join(FAMILY_DIR, "runs", S["first_run"], "EXECUTION_CLASSIFICATION.json")))
    led = json.load(open(os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")))
    M5 = {r["claim_id"]: r for r in json.load(open(os.path.join(FAMILY_DIR, "runs", S["first_run"], "results_300.json")))}
    seven = [r for r in R if r["kind"] == "state"]
    nonthin = [r for r in R if not r["thin"] and r["median_edge"] is not None]
    dsrs = np.array([r["dsr"] for r in R if r["dsr"] is not None], float)
    meds = np.array([r["median_edge"] for r in nonthin], float)
    hdr = ("| claim | signal TRUE | direct selected | cooldown-suppressed | no-next | n_days | median edge (pp) | day-win % | raw median % | +yrs | worst | DSR | class | mod-5 median → direct |\n"
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    def row(r):
        m = M5[r["claim_id"]]
        return (f"| {r['claim_id']} | {r['signal_true_n']:,} | {r['direct_selected_n']:,} | {r['cooldown_suppressed_n']:,} ({100*r['cooldown_suppressed_n']/max(1,r['signal_true_n']):.0f}%) | {r['NO_NEXT_SESSION']} | "
                f"{r['n_days']:,} | {f(r['median_edge'])} | {f(r['day_win'],1)} | {f(r.get('raw_median'))} | {r['positive_years']}/{r['years_counted']} | {f(r['worst_year'])} | "
                f"{f(r['dsr'],3)} | {r['classification']}{' (THIN)' if r['thin'] else ''} | {f(m['median_edge'])} → {f(r['median_edge'])} |")
    gates = S["gates"]["build"]
    def near(r):
        yrs = {y: v for y, v in r["per_year"].items() if r["days_per_year"].get(y, 0) >= gates["min_days_per_year"]}
        return (not r["thin"] and (r["median_edge"] or 0) > 0 and (r["day_win"] or 0) > 50 and sum(1 for v in yrs.values() if v > 0) >= 4
                and yrs and min(yrs.values()) >= -2.0 and (r["dsr"] or 0) < 0.95)
    nears = sorted([r for r in R if near(r)], key=lambda r: -(r["median_edge"] or 0))
    top = sorted(nonthin, key=lambda r: -(r["median_edge"] or 0))[:10]; bot = sorted(nonthin, key=lambda r: (r["median_edge"] or 0))[:10]
    yearly = {}
    for r in nonthin:
        for y, v in r["per_year"].items():
            yearly.setdefault(int(y), []).append(v)
    dd = [(r["claim_id"], r["distribution_diagnostic"]) for r in seven]
    md = f"""# OPENING_VOLUME_DYNAMICS — CHECKPOINT 3B · POST-EXPOSURE PATHSIM SEMANTICS RECONCILIATION (direct sacred `_pathsim`)

Written {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}.
```
label                  POST_EXPOSURE_EXECUTION_CORRECTION — not independent confirmation
run                    {rid}   results_300_direct {G['results_sha256_16']}   summary {G['summary_sha256_16']}   selection conservation {G['selection_conservation_sha256_16']}
correction record      FIRST_OUTCOME_EXECUTION_V1_CORRECTION_DIRECT_PATHSIM.json  {_dig(os.path.join(FAMILY_DIR, 'FIRST_OUTCOME_EXECUTION_V1_CORRECTION_DIRECT_PATHSIM.json'))}
authority              {A['verdict']}   (PATHSIM_SEMANTICS_AUTHORITY.json {G['authority_record_sha256_16']}; explicit neutralisation hits: {len(A['explicit_neutralisation_hits'])})
first run              {S['first_run']} = {C1['status']}   (preserved, immutable; results PROVISIONAL)
outcome access count   {led['outcome_access_count']}   (first access {led['events'][0]['at']}; this correction {led['events'][-1]['at']}; never reset)
_pathsim               src {S['pathsim_src_sha256_16']} · {S['pathsim_calls']} direct calls (300 cell masks + 7 FALSE-side control-only masks) · same trail/maxh/slip/entry
registry               {S['registry_sha256_16']}   k = {S['k']}   tested {S['reconciliation']['tested']} · missing 0 · extra 0 · duplicate 0
```

## 1 · Pre-outcome authority verdict
{A['rule']}
Binding (V1 registry {A['v1_registry_binding']['registry_sha256_16']}): entry = "{A['v1_registry_binding']['entry']}"; control universe = "{A['v1_registry_binding']['control_universe']}".
The first run's mod-5 construction was an execution-design decision disclosed before results but never registered pre-outcome → **EXECUTION_SEMANTICS_DEVIATION**; nothing deleted.

## 2 · Direct vs mod-5 — execution-semantics comparison (no optimisation, no selection)
Classification census — direct: {' · '.join(f'{k} {v}' for k, v in sorted(S['classification_census_direct'].items()))}   (mod-5 was: NULL 283 · DESCRIPTIVE_ONLY 17)
Classification changes: **{len(S['classification_changes'])}** {('→ ' + '; '.join(f"{c['claim_id']}: {c['mod5']} → {c['direct']}" for c in S['classification_changes'])) if S['classification_changes'] else ''}
Median-edge difference (direct − mod-5) over {S['median_edge_diff_direct_minus_mod5']['n']} cells: median {S['median_edge_diff_direct_minus_mod5']['median']:+.3f} pp, 5–95 % [{S['median_edge_diff_direct_minus_mod5']['p05']:+.3f}, {S['median_edge_diff_direct_minus_mod5']['p95']:+.3f}].
Cooldown suppression share (suppressed / signal TRUE): median {100*S['cooldown_suppression_share']['median']:.0f} %, max {100*S['cooldown_suppression_share']['max']:.0f} %.
DSR (k=300, direct): median {np.median(dsrs):.3f} · ≥ 0.95: {int((dsrs >= 0.95).sum())} · ≥ 0.90: {int((dsrs >= 0.90).sum())}. Non-thin cell medians: median {np.median(meds):+.3f} pp, IQR [{np.percentile(meds,25):+.3f}, {np.percentile(meds,75):+.3f}], share > 0 {(meds>0).mean()*100:.0f} %.

## 3 · The seven V2.1 cells — direct sacred `_pathsim` (cooldown included)
{hdr}
{chr(10).join(row(r) for r in seven)}
Logic 4 rows enter at D+2 open — never same-day D+1 reversal evidence. CLOSE60/OPEN60 ≥ 1.0 unchanged. A and B never collapsed; their outcome separation is visible descriptively.

## 4 · Largest positive / negative direct cell medians (non-thin; descriptive — gates decide)
{hdr}
{chr(10).join(row(r) for r in top)}

{hdr}
{chr(10).join(row(r) for r in bot)}

## 5 · Near misses under direct semantics (all registered gates except DSR ≥ 0.95): {len(nears)}
{hdr}
{chr(10).join(row(r) for r in nears[:12]) if nears else '| — | | | | | | | | | | | | | |'}

## 6 · Yearly robustness (median over non-thin cells of per-year median day-edge, pp)
{' · '.join(f'{y}: {np.median(v):+.2f} (n={len(v)})' for y, v in sorted(yearly.items()))}

## 7 · A10 distribution diagnostic (descriptive; never promotes / vetoes / removes rows)
{chr(10).join(f"- {cid}: all {f(d['all']['median_edge'])} (days {d['all']['n_days']}) · exposed {f(d['exposed']['median_edge'])} (obs {d['exposed']['n_obs']}) · non-exposed {f(d['non_exposed']['median_edge'])}" for cid, d in dd)}
Primary aggregate medians appear materially unchanged after stratifying out ≥5 % cash-distribution-exposed paths. Outcomes are PRICE RETURN, not total shareholder return; the flag does not cover every non-split corporate action.

## 8 · Provenance status
V1 / V2 / V2.1 seals immutable · first exposed run preserved with EXECUTION_CLASSIFICATION.json · this run = POST_EXPOSURE_EXECUTION_CORRECTION · no V3 · no thresholds added · no new search.
Any hypothesis generated from these outcomes is POST_EXPOSURE_HYPOTHESIS and requires fresh evidence.
"""
    p = os.path.join(FAMILY_DIR, "CHECKPOINT_3B_DIRECT_CORRECTION.md")
    open(p, "w").write(md); print("written", p, _dig(p))


if __name__ == "__main__":
    main()
