"""Every concrete pair that can precede T6: tok1 → tok2 → T6, one row per combination.

The family verdict is already in: three T bars ending in T6 produce no growth once the
opening print is removed (−0.013% at five bars), and the run-up's only contribution is a
slightly shallower drawdown. This script asks the next question — whether any PARTICULAR
pair behaves differently from the family it belongs to.

Fourteen T tokens in each of two slots is 196 cells, which is a search, so it is run as one:

  · every number is priced from close[i+1], never the printed open — the open manufactured
    the whole apparent edge in this family earlier today
  · the ranking is accompanied by what pure chance produces at the SAME cell sizes; a winner
    inside that band is a lucky cell, not a combination
  · and the honest test on top: rank on 2021-05 → 2023-12, then read those same cells on
    2024-01 → 2026-07 without touching them. A real pair keeps its place; a mined one does not

Drawdown is the true path low over the five bars after entry, since that is the number that
decides whether any of these is sizeable at all.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from naked_study import NakedStudy

MIN_N = 100
SPLIT = "2024-01-01"

st = NakedStudy("tok1 → tok2 → T6 — which concrete pair, and how deep",
                n_trials=2, columns=("t_sig", "z_sig"), horizons=(5,),
                min_price=5.0, min_dollar_vol=3_000_000)
d = st.df
T = d["t_sig"].fillna("").astype(str).to_numpy()
tk = d["ticker"].to_numpy()
c = d["close"].to_numpy(float)
lo = d["low"].to_numpy(float)
n = len(c)

# entry = close[i+1]; outcome over the five bars after it. Nothing here touches open[i+1].
c1 = np.r_[c[1:], np.nan]
lowroll = pd.Series(lo).rolling(5).min().shift(-5).to_numpy()   # at j: min(low[j+1..j+5])
c6 = np.r_[c[6:], np.full(6, np.nan)]
same1 = np.r_[tk[:-1] == tk[1:], False]
same6 = np.r_[tk[:-6] == tk[6:], np.zeros(6, bool)]
ent = np.where(same1, c1, np.nan)
low5 = np.where(same6, np.r_[lowroll[1:], np.nan], np.nan)
ret5 = np.where(same6, c6 / ent - 1, np.nan) * 100
mae5 = (low5 / ent - 1) * 100

isT = (T != "") & (T != "nan")
t6 = T == "T6"
p1 = np.r_[["" ], T[:-1]]
p2 = np.r_[["", ""], T[:-2]]
adj1 = np.r_[False, tk[:-1] == tk[1:]]
adj2 = np.r_[False, False, tk[:-2] == tk[2:]]
base = t6 & adj1 & adj2 & np.isfinite(ret5) & np.isfinite(mae5)
base &= np.r_[False, isT[:-1]] & np.r_[False, False, isT[:-2]]

yr = d["_dt"].to_numpy()
mined = base & (yr < np.datetime64(SPLIT))
oos = base & (yr >= np.datetime64(SPLIT))
print(f"\n  T→T→T6 usable: {base.sum():,}   mined {mined.sum():,} · OOS {oos.sum():,}",
      flush=True)
fam_r, fam_m = np.median(ret5[base]), np.median(mae5[base])
print(f"  family: median ret5 {fam_r:+.3f}% · median MAE5 {fam_m:+.3f}% "
      f"· win {(ret5[base] > 0).mean():.2%}", flush=True)

TOKS = sorted({t for t in np.unique(T[isT])})
rows = []
for a in TOKS:
    for b in TOKS:
        m = base & (p2 == a) & (p1 == b)
        k = int(m.sum())
        if k < MIN_N:
            continue
        r, mae = ret5[m], mae5[m]
        rows.append(dict(pair=f"{a} → {b} → T6", a=a, b=b, n=k,
                         ret=np.median(r), win=(r > 0).mean() * 100,
                         mae=np.median(mae), p10=np.percentile(mae, 10),
                         d5=(mae < -5).mean() * 100, d10=(mae < -10).mean() * 100,
                         n_mined=int((mined & (p2 == a) & (p1 == b)).sum()),
                         r_mined=np.median(ret5[mined & (p2 == a) & (p1 == b)])
                         if (mined & (p2 == a) & (p1 == b)).sum() >= 40 else np.nan,
                         r_oos=np.median(ret5[oos & (p2 == a) & (p1 == b)])
                         if (oos & (p2 == a) & (p1 == b)).sum() >= 40 else np.nan))
D = pd.DataFrame(rows).sort_values("ret", ascending=False)
print(f"  combinations with n ≥ {MIN_N}: {len(D)} of {len(TOKS)**2} possible "
      f"(they cover {D.n.sum() / base.sum():.1%} of the family)\n", flush=True)

print("=" * 126, flush=True)
print(f"  ALL COMBINATIONS — 5 bars, entry at close[i+1].  Ranked by median; the top row "
      f"is a SELECTION.", flush=True)
print("=" * 126, flush=True)
print(f"  {'pair':22s} {'n':>7s} {'ret5':>8s} {'Δfam':>7s} {'win':>7s} | "
      f"{'MAE med':>8s} {'MAE p10':>8s} {'>5%':>7s} {'>10%':>7s}", flush=True)
for _, r in D.iterrows():
    print(f"  {r.pair:22s} {r.n:>7,} {r.ret:>+8.3f} {r.ret - fam_r:>+7.3f} {r.win:>6.2f}% | "
          f"{r.mae:>+8.2f} {r.p10:>+8.2f} {r.d5:>6.1f}% {r.d10:>6.1f}%", flush=True)

# ── what chance produces at these sizes ─────────────────────────────────────
rng = np.random.default_rng(0)
pool = ret5[base]
sizes = D.n.to_numpy()
spreads, tops = [], []
for _ in range(500):
    meds = np.array([np.median(rng.choice(pool, s, replace=False)) for s in sizes])
    spreads.append(meds.max() - meds.min())
    tops.append(meds.max() - fam_r)
obs, obs_top = D.ret.max() - D.ret.min(), D.ret.max() - fam_r
print("\n" + "=" * 126, flush=True)
print("  IS THE RANKING REAL?", flush=True)
print("=" * 126, flush=True)
print(f"    observed best−worst spread   {obs:+.3f}pp", flush=True)
print(f"    chance, same cell sizes      median {np.median(spreads):.3f} · "
      f"p95 {np.percentile(spreads, 95):.3f}", flush=True)
print(f"    observed best cell vs family {obs_top:+.3f}pp   ·  chance p95 "
      f"{np.percentile(tops, 95):+.3f}pp", flush=True)
inside = obs <= np.percentile(spreads, 95)
print(f"    → the winner is {'INSIDE' if inside else 'OUTSIDE'} what chance produces across "
      f"{len(D)} cells"
      f"{'  — it is a lucky cell, not a combination' if inside else ''}", flush=True)

# ── the honest test: rank on the past, read on the future ───────────────────
E = D.dropna(subset=["r_mined", "r_oos"]).copy()
print("\n" + "=" * 126, flush=True)
print(f"  MINED 2021-05→2023-12  →  FROZEN OOS 2024-01→2026-07   ({len(E)} cells with "
      f"n≥40 in both halves)", flush=True)
print("=" * 126, flush=True)
E = E.sort_values("r_mined", ascending=False)
print(f"  {'pair':22s} {'n mined':>8s} {'mined':>8s} {'OOS':>8s} {'kept':>8s}", flush=True)
for _, r in E.head(10).iterrows():
    print(f"  {r.pair:22s} {r.n_mined:>8,} {r.r_mined:>+8.3f} {r.r_oos:>+8.3f} "
          f"{(r.r_oos / r.r_mined if r.r_mined else np.nan):>7.0%}", flush=True)
print(f"  {'…':22s}", flush=True)
for _, r in E.tail(3).iterrows():
    print(f"  {r.pair:22s} {r.n_mined:>8,} {r.r_mined:>+8.3f} {r.r_oos:>+8.3f} "
          f"{(r.r_oos / r.r_mined if r.r_mined else np.nan):>7.0%}", flush=True)
top = E.head(max(3, len(E) // 4))
print(f"\n    top quartile on the mined half: median {top.r_mined.median():+.3f} "
      f"→ OOS {top.r_oos.median():+.3f}", flush=True)
print(f"    rank correlation mined ↔ OOS: "
      f"{E.r_mined.corr(E.r_oos, method='spearman'):+.3f}   "
      f"(a real ranking is positive; ≈0 means the order was noise)", flush=True)
print(f"    sign held out of sample: {int((np.sign(top.r_mined) == np.sign(top.r_oos)).sum())}"
      f"/{len(top)} of the mined winners", flush=True)

# ── the drawdown answer, which is stable regardless of the ranking ──────────
print("\n" + "=" * 126, flush=True)
print("  DRAWDOWN — the part that does not depend on any ranking surviving", flush=True)
print("=" * 126, flush=True)
q = D.sort_values("mae", ascending=False)
print(f"    shallowest: " + " · ".join(f"{r.pair.replace(' → T6','')} {r.mae:+.2f}"
                                       for _, r in q.head(4).iterrows()), flush=True)
print(f"    deepest   : " + " · ".join(f"{r.pair.replace(' → T6','')} {r.mae:+.2f}"
                                       for _, r in q.tail(4).iterrows()), flush=True)
print(f"    spread across combinations {q.mae.max() - q.mae.min():.2f}pp · "
      f"family median {fam_m:+.2f}", flush=True)
D.to_csv("seq_ttt6_pairs.csv", index=False)
print("\n  written: seq_ttt6_pairs.csv", flush=True)
print("\nDONE", flush=True)
