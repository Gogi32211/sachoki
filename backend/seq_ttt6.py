"""anyT → anyT → T6: does the run-up add anything, and how deep can it go in five bars?

Two questions, and the second one is the one that decides position size: what growth follows
three consecutive T bars ending in T6, and how far against you it can travel first.

Everything learned today is wired in:

  · T6 fires before a gap DOWN of about 0.63%, and entering at the printed open collects a
    "recovery" that is largely the spread — so every number is reported at BOTH entries,
    open[i+1] (what the book pays) and close[i+1] (one bar later, no shared print). An effect
    that only exists in the first column is an artifact.
  · the control is matched on price × liquidity × year AND on gap depth, because the cell is
    selected by a token that predicts the gap.
  · the prefix has to beat T6 alone. "T6 after two T bars" is only interesting if two T bars
    change what T6 means; otherwise it is T6 with a smaller sample.

Drawdown is the true path minimum, min(low[i+1 … i+5])/entry − 1, not a close-to-close
figure — what a stop would actually have seen.

Three consecutive bars, no gaps. Since T6 is itself a T, this is a run of three T bars whose
last one is T6.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from naked_study import NakedStudy

st = NakedStudy("anyT → anyT → T6 — growth, and how deep it can go in 5 bars",
                n_trials=10, columns=("t_sig", "z_sig"), horizons=(1, 3, 5, 10, 20),
                min_price=5.0, min_dollar_vol=3_000_000)
d = st.df
T = d["t_sig"].fillna("").astype(str).to_numpy()
isT = (T != "") & (T != "nan")
t6 = T == "T6"
tk = d["ticker"].to_numpy()

prev1 = np.r_[False, isT[:-1]] & np.r_[False, tk[:-1] == tk[1:]]
prev2 = np.r_[False, False, isT[:-2]] & np.r_[False, False, tk[:-2] == tk[2:]]
c_t6 = t6
c_tt6 = t6 & prev1
c_ttt6 = t6 & prev1 & prev2

nxo = d.groupby("ticker", sort=False)["open"].shift(-1).to_numpy()
gap = np.nan_to_num((nxo / d["close"].to_numpy() - 1) * 100)
print(f"\n  T6 {c_t6.sum():,} · T→T6 {c_tt6.sum():,} · T→T→T6 {c_ttt6.sum():,} "
      f"({c_ttt6.sum() / max(c_t6.sum(), 1):.1%} of all T6) · "
      f"median gap on T→T→T6 {np.median(gap[c_ttt6]):+.3f}%", flush=True)

st.population(n_boot=300)
R = {}
for lbl, m in (("T6 alone", c_t6), ("T → T6", c_tt6), ("★ T → T → T6", c_ttt6)):
    R[lbl] = st.signal(lbl, m, n_boot=400, on=gap)

# ── the same cells priced from close[i+1] ────────────────────────────────────
c = d["close"].to_numpy(float)
c1 = np.r_[c[1:], np.nan]
c1 = np.where(np.r_[tk[:-1] == tk[1:], False], c1, np.nan)
lo = d["low"].to_numpy(float)


def fwd_from_c1(m, N):
    """close[i+1] → close[i+1+N], and the path low over the same span."""
    idx = np.where(m)[0]
    idx = idx[idx < len(c) - (N + 2)]
    good = np.isfinite(c1[idx]) & (c1[idx] > 0) & (tk[idx] == tk[idx + N + 1])
    idx = idx[good]
    ent = c1[idx]
    ret = c[idx + N + 1] / ent - 1
    mae = np.array([lo[i + 2:i + N + 2].min() for i in idx]) / ent - 1
    return ret * 100, mae * 100


print("\n" + "=" * 122, flush=True)
print("  THE TWO ENTRIES — an effect present only on the left is the opening print",
      flush=True)
print("=" * 122, flush=True)
print(f"  {'cell':16s} {'n':>8s} | " + " ".join(f"{f'open {h}b':>10s}" for h in (1, 5, 10))
      + " | " + " ".join(f"{f'close {h}b':>11s}" for h in (5, 10)), flush=True)
for lbl, m in (("T6 alone", c_t6), ("T → T6", c_tt6), ("★ T → T → T6", c_ttt6)):
    o = [R[lbl][h].med for h in (1, 5, 10)]
    c5, _ = fwd_from_c1(m, 5)
    c10, _ = fwd_from_c1(m, 10)
    print(f"  {lbl:16s} {R[lbl][10].n:>8,} | "
          + " ".join(f"{x:>+10.3f}" for x in o) + " | "
          + f"{np.median(c5):>+11.3f} {np.median(c10):>+11.3f}", flush=True)

# ── does the prefix add anything ────────────────────────────────────────────
print("\n" + "=" * 122, flush=True)
print("  DOES THE RUN-UP ADD ANYTHING?  (each cell minus its own gap-matched control)",
      flush=True)
print("=" * 122, flush=True)
print(f"  {'cell':16s} " + " ".join(f"{f'{h}b':>9s}" for h in st.hor)
      + f" {'win 5b':>8s} {'n_eff':>9s}", flush=True)
for lbl in R:
    lift = [R[lbl][h].med - st.ctl_all[lbl][h].med for h in st.hor]
    print(f"  {lbl:16s} " + " ".join(f"{x:>+9.3f}" for x in lift)
          + f" {R[lbl][5].up * 100:>8.2f} {R[lbl][5].n_eff:>9,}", flush=True)
print("\n  T→T→T6 minus T6 alone (the prefix's own contribution):", flush=True)
print(f"  {'':16s} " + " ".join(
    f"{R['★ T → T → T6'][h].med - R['T6 alone'][h].med:>+9.3f}" for h in st.hor), flush=True)

# ── the drawdown question ───────────────────────────────────────────────────
print("\n" + "=" * 122, flush=True)
print("  HOW DEEP CAN IT GO IN 5 BARS — true path low from entry, not close-to-close",
      flush=True)
print("=" * 122, flush=True)
a5 = d["a5"].to_numpy() * 100
print(f"  {'cell':16s} {'entry':>7s} {'n':>8s} {'median':>8s} {'p25':>8s} {'p10':>8s} "
      f"{'p1':>8s} {'worst':>9s} | {'>5%':>6s} {'>10%':>6s} {'>15%':>6s} {'>25%':>6s}",
      flush=True)
for lbl, m in (("T6 alone", c_t6), ("T → T6", c_tt6), ("★ T → T → T6", c_ttt6)):
    for ent, x in (("open", a5[m][np.isfinite(a5[m])]), ("close+1", fwd_from_c1(m, 5)[1])):
        print(f"  {lbl if ent == 'open' else '':16s} {ent:>7s} {len(x):>8,} "
              f"{np.median(x):>+8.2f} {np.percentile(x, 25):>+8.2f} "
              f"{np.percentile(x, 10):>+8.2f} {np.percentile(x, 1):>+8.2f} "
              f"{x.min():>+9.2f} | " + " ".join(f"{(x < -k).mean():>6.1%}"
                                                for k in (5, 10, 15, 25)), flush=True)
    print(flush=True)

# ── payoff shape ────────────────────────────────────────────────────────────
print("=" * 122, flush=True)
print("  PAYOFF SHAPE at 5 bars — reward against the risk it takes to get it", flush=True)
print("=" * 122, flush=True)
r5 = d["r5"].to_numpy() * 100
f5 = d["f5"].to_numpy() * 100
for lbl, m in (("T6 alone", c_t6), ("T → T6", c_tt6), ("★ T → T → T6", c_ttt6)):
    v = np.isfinite(r5) & m
    med, mfe, mae = np.median(r5[v]), np.median(f5[v]), np.median(a5[v])
    print(f"  {lbl:16s} median ret {med:>+6.2f}  ·  MFE {mfe:>5.2f}  ·  MAE {mae:>6.2f}"
          f"  ·  MFE/|MAE| {mfe / abs(mae):>4.2f}  ·  captured {med / mfe:>5.1%} of MFE",
          flush=True)
print("\nDONE", flush=True)
