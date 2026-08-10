"""F, corrected: leave-one-date-out shifts are not additive, so stop adding them.

diag3 reported "top-5 dates move the median by 0.178pp" and divided that by the bootstrap
half-width to get 0.665. The numerator was a SUM OF SEPARATE one-at-a-time shifts. Those are not
additive — removing five dates jointly is not removing each and adding up — and a CI half-width
is not a variance decomposition, so the quotient does not mean "top-5 explain 66.5% of the
interval". The qualitative reading (a small number of calendar clusters carries the statistic)
survives; the number does not.

What replaces it is a straight stress test that needs no additivity assumption:

    θ(all)  −  θ(all minus the top-k jointly)          k = 1, 5, 10, 25

and, because "top by influence" is selected ON the influence itself, a null band for the same
operation on RANDOM date sets of the same size. Without that band the top-k shift is guaranteed
to look large — it was chosen for being large. The two are printed side by side.

Also here: the share of total rows the top-k dates carry, which is the concentration that makes
row-iid a fantasy in the first place.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sources as srcs                                          # noqa: E402

HOR, DELTA, N_NULL = 10, 0.60, 400
KS = [1, 5, 10, 25]
pd.set_option("display.width", 205)

print("loading", flush=True)
df = srcs.bars("1d", columns=("t_sig", "z_sig"), min_price=5.0, min_dollar_vol=3_000_000,
               verbose=False)
tk = df["ticker"].to_numpy()
c, o = df["close"].to_numpy(float), df["open"].to_numpy(float)
ent = np.where(np.r_[tk[1:] == tk[:-1], False], np.r_[o[1:], np.nan], np.nan)
okh = np.r_[tk[HOR:] == tk[:-HOR], np.zeros(HOR, bool)]
ex = np.where(okh, np.r_[c[HOR:], np.full(HOR, np.nan)], np.nan)
base = (ex / ent - 1) * 100
T = df["t_sig"].fillna("").astype(str).to_numpy()
Z = df["z_sig"].fillna("").astype(str).to_numpy()
tok = np.where((T != "") & (T != "nan"), T, Z)
keep = np.isfinite(base)
RET, DATE = base[keep], df["date"].astype(str).str[:10].to_numpy()[keep]
TRT = np.where(tok[keep] == "T6")[0]
A, DA = RET[TRT] + DELTA, DATE[TRT]
theta = float(np.median(A))
dser = pd.Series(DA)
groups = dser.groupby(dser).groups
n_per = dser.value_counts()
bar = "=" * 118

print(f"  treated {len(A):,} rows · {len(groups):,} dates · median {theta:+.5f}pp\n", flush=True)

# one-at-a-time influence, used ONLY to rank
infl = {d0: theta - float(np.median(np.delete(A, np.asarray(idx))))
        for d0, idx in groups.items()}
order = pd.Series(infl).abs().sort_values(ascending=False).index.to_numpy()

print(bar, flush=True)
print("  JOINT REMOVAL vs a null band for random date sets of the same size", flush=True)
print(bar, flush=True)
print(f"  {'k':>4s} {'rows removed':>13s} {'% of rows':>10s} {'θ without top-k':>16s} "
      f"{'shift':>9s} | {'random |shift|':>15s} {'p95':>8s} {'ratio':>7s}", flush=True)
rng = np.random.default_rng(7)
all_dates = np.asarray(list(groups.keys()))
rows = []
for k in KS:
    drop = set(order[:k])
    m = ~np.isin(DA, list(drop))
    th_k = float(np.median(A[m]))
    shift = theta - th_k
    nrow = int((~m).sum())

    null = np.empty(N_NULL)
    for j in range(N_NULL):
        rd = rng.choice(all_dates, size=k, replace=False)
        null[j] = abs(theta - float(np.median(A[~np.isin(DA, rd)])))
    rows.append(dict(k=k, n_rows=nrow, shift=shift, null_mean=null.mean(),
                     null_p95=float(np.percentile(null, 95))))
    print(f"  {k:>4d} {nrow:>13,d} {100*nrow/len(A):>9.2f}% {th_k:>+16.5f} {shift:>+9.5f} | "
          f"{null.mean():>15.5f} {np.percentile(null,95):>8.5f} "
          f"{abs(shift)/max(null.mean(),1e-12):>7.2f}", flush=True)

R = pd.DataFrame(rows)
HALF = 0.2679          # bootstrap CI half-width for this arm, from diag3
print("\n" + bar, flush=True)
print("  STRESS MAGNITUDE RATIO  |Δ(joint removal)| / CI half-width", flush=True)
print(bar, flush=True)
print(f"    CI half-width {HALF:.4f}pp (diag3, same arm)", flush=True)
for _, r in R.iterrows():
    print(f"    k={int(r['k']):>3d}  shift {r['shift']:>+9.5f}pp  → stress ratio "
          f"{abs(r['shift'])/HALF:>5.2f}   ({100*r['n_rows']/len(A):>5.2f}% of rows)", flush=True)
k5 = R[R.k == 5].iloc[0]
k1 = R[R.k == 1].iloc[0]
print("\n" + bar, flush=True)
print("  READING", flush=True)
print(bar, flush=True)
print(f"    Joint removal is NOT monotone in k: one date moves the median {k1['shift']:+.5f}, "
      f"five move it\n    {k5['shift']:+.5f}. Ranking by |influence| mixes dates that push in "
      f"OPPOSITE directions, so they\n    partly cancel when removed together. That is exactly "
      f"why summing one-at-a-time shifts was\n    wrong, and it happens to cut the other way "
      f"from what I assumed.", flush=True)
print(f"\n    The earlier claim — 'top-5 dates carry two thirds of the interval', 0.665 — is "
      f"WITHDRAWN.\n    The measured joint stress ratio at k=5 is "
      f"{abs(k5['shift'])/HALF:.2f}, roughly five times smaller.", flush=True)
print(f"\n    Against the null band, the selected set is still real: "
      f"{abs(k5['shift'])/max(k5['null_mean'],1e-12):.1f}× the random-5\n    baseline "
      f"({k5['null_mean']:.5f}pp). But it was SELECTED for being extreme, so this is an upper "
      f"bound\n    on date leverage, not a measure of it.", flush=True)
print(f"\n    What survives without any selection or additivity assumption: "
      f"{k5['n_rows']:,.0f} rows "
      f"({100*k5['n_rows']/len(A):.2f}% of the\n    sample) sit on 5 of {len(groups):,} dates. "
      f"That concentration is a fact about the data, and it\n    is why a row-iid interval is "
      f"a fantasy regardless of how the median responds to deletion.", flush=True)
R.to_csv("inference_diag5.csv", index=False)
print("\nDONE", flush=True)
