"""A4 — does excluding unadjusted corporate actions change the ORDER of the book?

A3 established the shape of the problem: 336 unadjusted corporate actions reach the frame
(264 of 282 affected tickers clear the $3M floor), they touch 0.140% of bars, and they move
MEDIANS by 0.007pp — nothing. But they leave trades returning up to +1528%, and those move
every statistic built on the mean: Sharpe, profit factor, and therefore DSR — the layer the
whole retro-deflation of 119 setups is about to rest on.

So the question is not "do the numbers change" (they will, slightly) but:

    ⟹ does the RANKING of setups change?

If the order is stable, the contamination is cosmetic and the deflation can proceed on the
existing family. If the order moves, part of the book is ranked by tail artefacts and the
family has to be rebuilt clean before anything is deflated.

Exposure is defined exactly, not by a date window: a trade is exposed when a corporate action
falls inside its own holding period, date_in ≤ ca_date ≤ date_out.

Measures only. Nothing in edge_replay is modified — if the answer says the mask is needed,
that becomes a separate, deliberate change to the engine.
"""
import os
import sys

import numpy as np
import pandas as pd
from scipy import stats as sps

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er            # noqa: E402
import overfit_stats as ofs         # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
CA = os.path.join(HERE, "corporate_actions.csv")
pd.set_option("display.width", 210)

S = pd.read_csv(CA)
S["date"] = pd.to_datetime(S["date"])
by_tk: dict[str, list] = {}
for _, r in S.iterrows():
    by_tk.setdefault(r.ticker, []).append(r.date)
print(f"corporate actions on file: {len(S):,} · {len(by_tk):,} tickers\n", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp):,} tickers\n", flush=True)


def exposed(tr: pd.DataFrame) -> np.ndarray:
    """A trade is exposed when a corporate action falls INSIDE its holding period."""
    di = pd.to_datetime(tr["date_in"]).to_numpy()
    do = pd.to_datetime(tr["date_out"]).to_numpy()
    out = np.zeros(len(tr), bool)
    for i, tk in enumerate(tr["ticker"].to_numpy()):
        for ca in by_tk.get(tk, ()):
            if di[i] <= ca.to_datetime64() <= do[i]:
                out[i] = True
                break
    return out


def block(r: np.ndarray) -> dict:
    w = r > 0
    den = -r[~w].sum()
    return dict(n=len(r), mean=float(r.mean() * 100), med=float(np.median(r) * 100),
                sharpe=float(ofs.sharpe(r)),
                pf=float(r[w].sum() / den) if den > 0 else float("inf"))


rows = []
print("### per-setup, dirty vs clean\n", flush=True)
print(f"  {'setup':30s} {'n':>7s} {'exp':>4s} {'mean':>8s} {'mean_c':>8s} "
      f"{'sharpe':>7s} {'shrp_c':>7s} {'pf':>6s} {'pf_c':>6s} {'maxret':>8s}", flush=True)
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 100:
        continue
    e = exposed(tr)
    r_all = tr["ret"].to_numpy(float)
    r_cln = tr.loc[~e, "ret"].to_numpy(float)
    if len(r_cln) < 50:
        continue
    a, c = block(r_all), block(r_cln)
    rows.append(dict(setup=name, n=a["n"], exp=int(e.sum()),
                     mean=a["mean"], mean_c=c["mean"], med=a["med"], med_c=c["med"],
                     sharpe=a["sharpe"], sharpe_c=c["sharpe"], pf=a["pf"], pf_c=c["pf"],
                     maxret=float(r_all.max() * 100)))
    if e.sum():
        print(f"  {name:30s} {a['n']:>7,} {int(e.sum()):>4d} {a['mean']:>+8.2f} "
              f"{c['mean']:>+8.2f} {a['sharpe']:>7.3f} {c['sharpe']:>7.3f} "
              f"{a['pf']:>6.2f} {c['pf']:>6.2f} {r_all.max()*100:>+8.1f}", flush=True)

R = pd.DataFrame(rows)
print(f"\nmeasured {len(R)} setups · {int((R.exp > 0).sum())} have ≥1 exposed trade",
      flush=True)

# ── the decisive test: does the ORDER move? ──────────────────────────────────
print("\n### the decisive test — does the ranking change?\n", flush=True)
for key in ("sharpe", "mean", "pf"):
    a = R[key].rank(ascending=False)
    b = R[f"{key}_c"].rank(ascending=False)
    rho = sps.spearmanr(a, b).statistic
    kend = sps.kendalltau(a, b).statistic
    moved = int((a - b).abs().ge(3).sum())
    print(f"  rank by {key:7s}: spearman {rho:.5f} · kendall {kend:.5f} · "
          f"{moved} setups move ≥3 places", flush=True)

R["d_sharpe_rank"] = R["sharpe"].rank(ascending=False) - R["sharpe_c"].rank(ascending=False)
print("\n  biggest rank movers (by Sharpe):", flush=True)
print(R.reindex(R.d_sharpe_rank.abs().sort_values(ascending=False).index)
      .head(10)[["setup", "n", "exp", "sharpe", "sharpe_c", "d_sharpe_rank", "maxret"]]
      .to_string(index=False, float_format=lambda x: f"{x:.3f}"), flush=True)

# ── what it does to DSR, the layer that matters ──────────────────────────────
print("\n### DSR impact — the family is rebuilt clean and a few setups re-deflated\n",
      flush=True)
fam_dirty = [x for x in R["sharpe"] if np.isfinite(x)]
fam_clean = [x for x in R["sharpe_c"] if np.isfinite(x)]
print(f"  family max Sharpe: dirty {max(fam_dirty):.3f} → clean {max(fam_clean):.3f}",
      flush=True)
print(f"  family mean Sharpe: dirty {np.mean(fam_dirty):.4f} → clean {np.mean(fam_clean):.4f}",
      flush=True)
probe = ["🧊Coil-Floor", "QZ-Capit-Rev", "Washout", "L43-TRIPLE", "🎯Confluence≥3",
         "Atomic", "Zone-Retest", "RTB-Base"]
print(f"\n  {'setup':22s} {'DSR dirty':>10s} {'DSR clean':>10s} {'Δ':>8s}", flush=True)
for name, col in er.SETUPS:
    if name not in probe:
        continue
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 100:
        continue
    e = exposed(tr)
    d1 = ofs.dsr(tr["ret"].to_numpy(float), fam_dirty, n_trials=len(R))["dsr"]
    d2 = ofs.dsr(tr.loc[~e, "ret"].to_numpy(float), fam_clean, n_trials=len(R))["dsr"]
    print(f"  {name:22s} {d1:>10.3f} {d2:>10.3f} {d2-d1:>+8.3f}", flush=True)

R.to_csv(os.path.join(HERE, "ca_impact.csv"), index=False)
print(f"\n  → ca_impact.csv", flush=True)
print("\nDONE", flush=True)
