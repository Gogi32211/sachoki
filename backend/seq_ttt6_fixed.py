"""anyT → anyT → T6, redone on deduplicated bars with genuine calendar adjacency.

Everything printed here replaces the earlier run, which was wrong twice over: the frame
carried one row per index membership so 39.6% of bars were duplicated (and `shift(-1)`
returned the same bar), and the sequence was assembled from consecutive ROWS rather than
consecutive DAYS, so for filtered names the "previous two bars" could be months away. That
is how JLHL was labelled T2 → T2 → T6 when its chart plainly reads Z10 → T5 → T6.

Both are now structural: NakedStudy dedupes at load and asserts the data contract, and the
sequence is built with sequence_mask(), which ANDs the token pattern with the calendar
adjacency of every step. A run of three can no longer be stitched out of bars a filter left
apart.

The first thing this script does is read a sample of its own hits back from the source, so
the labels can be checked against a chart before any statistic is believed.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from data_contract import sequence_mask, verify_sample
from naked_study import NakedStudy

st = NakedStudy("anyT → anyT → T6 (deduplicated, calendar-adjacent)",
                n_trials=8, columns=("t_sig", "z_sig"), horizons=(1, 3, 5, 10, 20),
                min_price=5.0, min_dollar_vol=3_000_000)
d = st.df
T = d["t_sig"].fillna("").astype(str)
Z = d["z_sig"].fillna("").astype(str)
d["tok"] = np.where(T.ne("") & T.ne("nan"), T, Z)
TT = sorted({t for t in d["tok"].unique() if str(t).startswith("T")})
print(f"\n  T tokens: {TT}", flush=True)

anyT = list(TT)
m_ttt6 = sequence_mask(d, [anyT, anyT, "T6"])
m_tt6 = sequence_mask(d, [anyT, "T6"])
m_t6 = (d["tok"] == "T6").to_numpy()

# the same masks WITHOUT the adjacency requirement — the size of the old mistake
tokarr = d["tok"].to_numpy()
tkarr = d["ticker"].to_numpy()
p1 = np.r_[[""], tokarr[:-1]]
p2 = np.r_[["", ""], tokarr[:-2]]
sameT = np.r_[False, tkarr[:-1] == tkarr[1:]] & np.r_[False, False, tkarr[:-2] == tkarr[2:]]
m_rows = m_t6 & sameT & np.isin(p1, anyT) & np.isin(p2, anyT)
print(f"\n  T6 bars {m_t6.sum():,}", flush=True)
print(f"  T→T→T6 by ROW adjacency (the old, wrong way): {m_rows.sum():,}", flush=True)
print(f"  T→T→T6 by CALENDAR adjacency (correct):       {m_ttt6.sum():,}"
      f"   → {m_rows.sum() - m_ttt6.sum():,} of the old hits were never a sequence "
      f"({1 - m_ttt6.sum() / max(m_rows.sum(), 1):.1%})", flush=True)

verify_sample(d, m_ttt6, n=6, label="T→T→T6 (fixed)")

st.population(n_boot=300)
nxo = d.groupby("ticker", sort=False)["open"].shift(-1).to_numpy()
gap = np.nan_to_num((nxo / d["close"].to_numpy() - 1) * 100)
R = {}
for lbl, m in (("T6 alone", m_t6), ("T → T6", m_tt6), ("★ T → T → T6", m_ttt6)):
    R[lbl] = st.signal(lbl, m, n_boot=400, on=gap)

# ── priced from close[i+1] as well, since T6 fires before a gap ─────────────
c = d["close"].to_numpy(float)
lo = d["low"].to_numpy(float)
tk = d["ticker"].to_numpy()
c1 = np.r_[c[1:], np.nan]
lowroll = pd.Series(lo).rolling(5).min().shift(-5).to_numpy()
c6 = np.r_[c[6:], np.full(6, np.nan)]
s1 = np.r_[tk[:-1] == tk[1:], False]
s6 = np.r_[tk[:-6] == tk[6:], np.zeros(6, bool)]
ent = np.where(s1, c1, np.nan)
ret5c = np.where(s6, c6 / ent - 1, np.nan) * 100
mae5c = (np.where(s6, np.r_[lowroll[1:], np.nan], np.nan) / ent - 1) * 100

print("\n" + "=" * 122, flush=True)
print("  RESULT — both entries, deduplicated, calendar-adjacent", flush=True)
print("=" * 122, flush=True)
print(f"  {'cell':16s} {'n':>8s} | " + " ".join(f"{f'open {h}b':>10s}" for h in (1, 5, 10))
      + f" | {'close+1 5b':>11s} {'MAE5 open':>10s} {'MAE5 c+1':>10s}", flush=True)
a5 = d["a5"].to_numpy() * 100
for lbl, m in (("T6 alone", m_t6), ("T → T6", m_tt6), ("★ T → T → T6", m_ttt6)):
    o = [R[lbl][h].med for h in (1, 5, 10)]
    v = m & np.isfinite(ret5c)
    print(f"  {lbl:16s} {int(m.sum()):>8,} | " + " ".join(f"{x:>+10.3f}" for x in o)
          + f" | {np.median(ret5c[v]):>+11.3f} "
          f"{np.median(a5[m & np.isfinite(a5)]):>+10.2f} "
          f"{np.median(mae5c[v]):>+10.2f}", flush=True)

print("\n  lift over each cell's own gap-matched control:", flush=True)
print(f"  {'cell':16s} " + " ".join(f"{f'{h}b':>9s}" for h in st.hor), flush=True)
for lbl in R:
    print(f"  {lbl:16s} " + " ".join(
        f"{R[lbl][h].med - st.ctl_all[lbl][h].med:>+9.3f}" for h in st.hor), flush=True)

print("\n" + "=" * 122, flush=True)
print("  DRAWDOWN IN 5 BARS — five genuine trading days now", flush=True)
print("=" * 122, flush=True)
print(f"  {'cell':16s} {'entry':>8s} {'n':>8s} {'median':>8s} {'p25':>8s} {'p10':>8s} "
      f"{'p1':>8s} | {'>5%':>7s} {'>10%':>7s} {'>15%':>7s}", flush=True)
for lbl, m in (("T6 alone", m_t6), ("T → T6", m_tt6), ("★ T → T → T6", m_ttt6)):
    for ent_lbl, x in (("open", a5[m & np.isfinite(a5)]),
                       ("close+1", mae5c[m & np.isfinite(mae5c)])):
        print(f"  {lbl if ent_lbl == 'open' else '':16s} {ent_lbl:>8s} {len(x):>8,} "
              f"{np.median(x):>+8.2f} {np.percentile(x, 25):>+8.2f} "
              f"{np.percentile(x, 10):>+8.2f} {np.percentile(x, 1):>+8.2f} | "
              + " ".join(f"{(x < -k).mean():>7.1%}" for k in (5, 10, 15)), flush=True)
    print(flush=True)

out = pd.DataFrame(dict(ticker=d["ticker"][m_ttt6], date=d["date"][m_ttt6].astype(str),
                        prev1=p1[m_ttt6], prev2=p2[m_ttt6],
                        close=d["close"][m_ttt6].round(2),
                        ret5_c1=np.round(ret5c[m_ttt6], 2),
                        mae5_c1=np.round(mae5c[m_ttt6], 2)))
out.sort_values("mae5_c1").to_csv("seq_ttt6_fixed.csv", index=False)
print(f"  written: seq_ttt6_fixed.csv ({len(out):,} rows, worst drawdown first)", flush=True)
print("\nDONE", flush=True)
