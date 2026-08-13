"""Market Physics · stage 1 — the collapse test. Descriptive, no outcome, no k.

One question: how many INDEPENDENT dimensions are actually there. If seven named quantities
carry two dimensions of information, the vocabulary is decorative and everything downstream
would be seven names for one thing.

Three tests, and the second and third can both prove me wrong:

    A  EFFECTIVE DIMENSIONALITY   participation ratio and PCs-to-90% for each parameterisation.
                                  I claimed the declared seven collapse to about four.

    B  THE PAIRS I PREDICTED      C↔K, R↔M, H↔S. Named in advance so they cannot be found
                                  after the fact and presented as insight.

    C  IS MY SET NEW INFORMATION  every proposed axis regressed on ALL declared axes. High R²
                                  means I renamed rather than replaced — the harshest test of
                                  my own §3, and the reason both sets are computed at all.

SPEARMAN, NOT PEARSON. Several of these are heavy-tailed by construction (a ratio with a small
denominator, a barrier height in log units). Pearson on raw values would report correlations
driven by a handful of bars; rank correlation asks the question that was meant.

ONE ROW PER (ticker, date), and the sample is seeded. PCA does not need 774k rows, and saying
the sample size out loud is cheaper than pretending precision that a diagnostic does not need.
"""
from __future__ import annotations

import json
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import market_physics as MP                                           # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "MARKET_PHYSICS_STAGE1.json")

SAMPLE = 100_000
SEED = 0

# named BEFORE the numbers exist, so they cannot become post-hoc insight
PREDICTED_PAIRS = [
    ("decl_C", "decl_K", "landscape and stretch measure the same object twice"),
    ("decl_R", "decl_M", "mass ∝ 1/impact couples them"),
    ("decl_H", "decl_S", "disorder in time ≈ disagreement across scales"),
    ("dis_perm_entropy", "decl_H", "my entropy vs the declared one — both may be saturated"),
    ("dis_hurst", "decl_S", "DFA vs EMA-slope agreement: is mine independent?"),
    ("land_dist_mode_atr", "decl_K", "volume-mode distance vs EMA distance"),
]


def _spearman(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    return df[cols].corr(method="spearman")


def _effective_dim(corr: pd.DataFrame) -> dict:
    """Participation ratio and PCs-to-90%. Both on the correlation matrix, so scale-free."""
    ev = np.linalg.eigvalsh(corr.to_numpy())[::-1]
    ev = np.clip(ev, 0, None)
    tot = ev.sum()
    pr = (tot ** 2) / (ev ** 2).sum()          # participation ratio: "how many really count"
    cum = np.cumsum(ev) / tot
    return {"n_axes": len(ev), "participation_ratio": round(float(pr), 2),
            "pcs_to_90pct": int(np.searchsorted(cum, 0.90) + 1),
            "pc1_share": round(float(ev[0] / tot), 3),
            "eigenvalues": [round(float(x), 3) for x in ev]}


def _explained_by(df: pd.DataFrame, target: str, predictors: list) -> float:
    """R² of target on predictors, on rank scores. 'Did I rename or replace?'"""
    y = df[target].rank(pct=True).to_numpy()
    X = df[predictors].rank(pct=True).to_numpy()
    X = np.c_[np.ones(len(X)), X]
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    return float(1 - resid.var() / y.var())


def run(universe: str = "sp500") -> dict:
    t0 = time.time()
    print(f"  computing axes · {universe}", flush=True)
    phys = MP.load_and_compute(universe=universe, verbose=True)
    cols = MP.AXES_PROPOSED + MP.AXES_DECLARED
    d = phys[["ticker", "date"] + cols].replace([np.inf, -np.inf], np.nan).dropna()
    print(f"  {len(d):,} complete rows of {len(phys):,} computed "
          f"({time.time() - t0:.0f}s)", flush=True)
    if len(d) > SAMPLE:
        d = d.sample(SAMPLE, random_state=SEED)

    corr_all = _spearman(d, cols)
    out = {
        "stage": "1 — COLLAPSE TEST. Descriptive: no outcome column was read, no k charged.",
        "universe": universe, "rows_complete": int(len(phys)), "rows_used": int(len(d)),
        "sample_seed": SEED, "spec": MP.spec(),
        "effective_dim": {
            "declared_7": _effective_dim(_spearman(d, MP.AXES_DECLARED)),
            "proposed": _effective_dim(_spearman(d, MP.AXES_PROPOSED)),
            "combined": _effective_dim(corr_all),
        },
        "predicted_pairs": [
            {"a": a, "b": b, "why_predicted": why,
             "spearman": round(float(corr_all.loc[a, b]), 3)}
            for a, b, why in PREDICTED_PAIRS if a in corr_all and b in corr_all],
        "proposed_explained_by_declared": {
            c: round(_explained_by(d, c, MP.AXES_DECLARED), 3) for c in MP.AXES_PROPOSED},
        "declared_explained_by_proposed": {
            c: round(_explained_by(d, c, MP.AXES_PROPOSED), 3) for c in MP.AXES_DECLARED},
        "dispersion": {c: {"std_of_rank_free_value": round(float(d[c].std()), 4),
                           "iqr": round(float(d[c].quantile(.75) - d[c].quantile(.25)), 4)}
                       for c in cols},
        "correlation_matrix": {a: {b: round(float(corr_all.loc[a, b]), 3) for b in cols}
                               for a in cols},
        "seconds": round(time.time() - t0, 1),
    }
    return out


def _top_pairs(out: dict, n: int = 12) -> list:
    m = out["correlation_matrix"]
    cols = list(m)
    seen = []
    for i, a in enumerate(cols):
        for b in cols[i + 1:]:
            seen.append((abs(m[a][b]), a, b, m[a][b]))
    seen.sort(reverse=True)
    return seen[:n]


if __name__ == "__main__":
    uni = sys.argv[1] if len(sys.argv) > 1 else "sp500"
    r = run(uni)
    with open(REPORT, "w") as f:
        json.dump(r, f, indent=1, sort_keys=True)

    print("\n" + "=" * 96, flush=True)
    print("  STAGE 1 · COLLAPSE TEST", flush=True)
    print("=" * 96, flush=True)
    print(f"  {r['rows_used']:,} rows (seeded sample of {r['rows_complete']:,} computed) · "
          f"{r['seconds']}s", flush=True)

    print(f"\n  EFFECTIVE DIMENSIONALITY", flush=True)
    print(f"    {'set':<14}{'axes':>6}{'participation':>15}{'PCs→90%':>10}{'PC1 share':>12}",
          flush=True)
    for k, v in r["effective_dim"].items():
        print(f"    {k:<14}{v['n_axes']:>6}{v['participation_ratio']:>15}"
              f"{v['pcs_to_90pct']:>10}{v['pc1_share']:>12}", flush=True)

    print(f"\n  PAIRS I PREDICTED (named before the numbers existed)", flush=True)
    for p in r["predicted_pairs"]:
        verdict = ("CONFIRMED" if abs(p["spearman"]) >= 0.6 else
                   "partial" if abs(p["spearman"]) >= 0.35 else "WRONG")
        print(f"    {p['a']:<22}{p['b']:<22}{p['spearman']:>+7.3f}  {verdict:<10}"
              f"{p['why_predicted']}", flush=True)

    print(f"\n  DID I RENAME OR REPLACE? (R² of each proposed axis on ALL seven declared)",
          flush=True)
    for c, v in sorted(r["proposed_explained_by_declared"].items(), key=lambda kv: -kv[1]):
        tag = "RENAMED" if v >= 0.7 else ("overlaps" if v >= 0.4 else "new information")
        print(f"    {c:<26}{v:>7.3f}  {tag}", flush=True)

    print(f"\n  AND THE REVERSE (R² of each declared axis on the proposed set)", flush=True)
    for c, v in sorted(r["declared_explained_by_proposed"].items(), key=lambda kv: -kv[1]):
        print(f"    {c:<26}{v:>7.3f}", flush=True)

    print(f"\n  STRONGEST PAIRS OVERALL", flush=True)
    for _, a, b, v in _top_pairs(r):
        print(f"    {a:<26}{b:<26}{v:>+7.3f}", flush=True)
    print(f"\n  written to {os.path.basename(REPORT)}", flush=True)
