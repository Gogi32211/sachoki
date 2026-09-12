"""SCORE_AUDIT_V1 — render the sealed outcome run (results.json + descriptives.json) as a checkpoint report.
Read-only over the run directory; writes CHECKPOINT_FIRST_OUTCOME.md next to the family's seal. No statistics are
computed here beyond formatting — every number comes from the outcome run."""
from __future__ import annotations
import os, sys, json, glob

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/SCORE_AUDIT_V1"


def _f(v, d=2, w=6):
    if v is None:
        return "—".rjust(w)
    try:
        return f"{float(v):{w}.{d}f}"
    except Exception:
        return str(v).rjust(w)


def latest_out(run_id: str | None = None):
    runs = sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OUT_*")), reverse=True)
    for r in runs:
        if run_id and os.path.basename(r) != run_id:
            continue
        if os.path.exists(os.path.join(r, "COMPLETED")) and os.path.exists(os.path.join(r, "results.json")):
            return r
    return None


def main(write: bool = True, run_id: str | None = None, out_name: str = "CHECKPOINT_FIRST_OUTCOME.md") -> str:
    run = latest_out(run_id)
    if not run:
        raise SystemExit("no completed outcome run")
    res = json.load(open(os.path.join(run, "results.json"))); desc = json.load(open(os.path.join(run, "descriptives.json")))
    summ = json.load(open(os.path.join(run, "summary.json"))); cons = json.load(open(os.path.join(run, "selection_conservation.json")))
    seal = json.load(open(os.path.join(FAMILY_DIR, "SEAL.json"))); reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")))
    L = []
    L.append(f"# SCORE_AUDIT_V1 — first outcome ({summ['run_id']})\n")
    L.append(f"Seal registry `{seal['registry_sha256_16']}` · X run `{seal['x_run']}` · k = {summ['k']} · path-sim `{summ['pathsim_src_sha256_16']}` · "
             f"outcome access {summ['outcome_access_count']} · trades {summ['trades']:,} on {summ['days']:,} entry days.\n")
    L.append(f"Selection conservation: {cons}\n")
    L.append(f"Cell centre statistic: **{summ.get('demean_stat', 'mean')}**" + (f" · amendment **{summ['amendment']}** (post-exposure correction)" if summ.get("amendment") else "") + "\n")
    if summ.get("centring"):
        L.append(f"Centring check, all rows (day-median of the demeaned edge · % days > 0): {summ['centring']}\n")
    L.append(f"Role census (in-k cells): {summ['role_census']}\n")
    L.append("## Deciding table — cell-demeaned day series (median per day over HIGH rows)\n")
    L.append("| cell | HIGH rows | MINE med | win% | yrs+ | worst | SR/day | SR* | DSR | P(SR>0) | VERIFY med | win% | worst | class | role |")
    L.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|")
    for r in res:
        m, v = r["mine"], r["verify"]
        L.append(f"| {r['claim_id']} | {r['high_rows']:,} | {_f(m['median_edge'])} | {_f(m['day_win'],1)} | {m['positive_years']}/{m['years_counted']} | {_f(m['worst_year'])} | "
                 f"{_f(r.get('sr'),3)} | {_f(r.get('sr_star'),3)} | {_f(r.get('dsr'))} | {_f(r.get('psr0'))} | "
                 f"{_f(v['median_edge'])} | {_f(v['day_win'],1)} | {_f(v['worst_year'])} | {r['classification']} | {r['role']} |")
    L.append("\nRaw (same-day-median) day edge of the same HIGH rows, for reading beside the demeaned series:\n")
    L.append("| cell | MINE raw med | win% | VERIFY raw med | win% |")
    L.append("|---|---:|---:|---:|---:|")
    for r in res:
        a, b = r["raw"]["mine"], r["raw"]["verify"]
        L.append(f"| {r['claim_id']} | {_f(a['median_day_edge_raw'])} | {_f(a['day_win_raw'],1)} | {_f(b['median_day_edge_raw'])} | {_f(b['day_win_raw'],1)} |")
    L.append("\n## Descriptive — within-day quintile ladders (median raw edge, MINE / VERIFY)\n")
    L.append("| score | Q1 | Q2 | Q3 | Q4 | Q5 | · | Q1 | Q2 | Q3 | Q4 | Q5 |")
    L.append("|---|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|")
    for col, lad in desc["ladders"].items():
        mm = " | ".join(_f(x["median"]) for x in lad["mine"]); vv = " | ".join(_f(x["median"]) for x in lad["verify"])
        L.append(f"| {col} | {mm} | · | {vv} |")
    L.append("\n## Descriptive — daily rank-IC (mean over days · t over days · % days > 0)\n")
    L.append("| score | raw MINE | raw VERIFY | conditional MINE | conditional VERIFY |")
    L.append("|---|---|---|---|---|")
    for col, ic in desc["ic"].items():
        def _c(x):
            return f"{_f(x['mean_ic'],4,7)} · t {_f(x['t_stat'],1,5)} · {_f(x['share_pos'],0,3)}%"
        L.append(f"| {col} | {_c(ic['raw']['mine'])} | {_c(ic['raw']['verify'])} | {_c(ic['conditional']['mine'])} | {_c(ic['conditional']['verify'])} |")
    L.append("\n## Descriptive — 🎲 ladders (median raw edge · n rows), live axes-UV3 vs core-UV3\n")
    L.append("| hits | live MINE | live VERIFY | core MINE | core VERIFY |")
    L.append("|---|---|---|---|---|")
    hl, hc = desc["hits"]["live"], desc["hits"]["core"]
    for i in range(len(hl["mine"])):
        def _h(x):
            return f"{_f(x['median'])} (n {x['n']:,})"
        L.append(f"| {hl['mine'][i]['bin']} | {_h(hl['mine'][i])} | {_h(hl['verify'][i])} | {_h(hc['mine'][i])} | {_h(hc['verify'][i])} |")
    L.append("\n## Descriptive — UV3 > 25 zone, live vs core\n")
    for w, z in desc["uv3_zone"].items():
        L.append(f"- {w}: live>25 share {z['live_gt25_share']:.3f} (median edge {_f(z['live_gt25_median'])}) · core>25 share {z['core_gt25_share']:.3f} "
                 f"(median {_f(z['core_gt25_median'])}) · live-only rows median {_f(z['live_only_median'])} · axes defined {z['axes_defined_share']:.3f}")
    L.append("\nGates: " + json.dumps(reg["gates"]) + "\n")
    txt = "\n".join(L)
    if write:
        open(os.path.join(FAMILY_DIR, out_name), "w").write(txt)
    return txt


if __name__ == "__main__":
    a = sys.argv[1:]
    rid = a[a.index("--run") + 1] if "--run" in a else None
    name = a[a.index("--out") + 1] if "--out" in a else "CHECKPOINT_FIRST_OUTCOME.md"
    print(main(write="--dry" not in a, run_id=rid, out_name=name))
