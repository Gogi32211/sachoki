"""Four-bar T-token sequences ending in T1G — mined with the OOS window reserved first.

The question: among 4-bar sequences whose final bar is T1G, which prefixes carry an edge?

WHY THE DESIGN LOOKS LIKE THIS. The full descriptor space — T × Z × L over four bars — is 2.1e14
cells. Ranking the top 10 of anything that large produces ten winners whether or not an effect
exists, and this project already has the receipt: the _TZ package's 66,989-rule sequence database
yielded 8 survivors where chance alone predicts 11.

So the space is cut to something with a countable, declared k:

    prefix = (t_sig[t-3], t_sig[t-2], t_sig[t-1])   over 13 tokens incl. empty
    anchor = t_sig[t] == "T1G"                       the decision bar
    k      = 13³ = 2,197                             declared, not discovered

Yesterday's measurement is the reason for the chance band rather than a p-value: searching 46
cells already needed ~3pp of true effect to surface one, against 0.6pp when the cell was named
in advance. At 2,197 cells the bar is higher still, and the max-statistic permutation band is
what prices it.

    IS   2021-01 … 2024-12     mining
    OOS  2025-01 … 2026-08     reserved BEFORE looking, untouched until the end

Outcome is path-sim, not an MFE proxy — that substitution once turned a real −2.4 into a
reported +3.4 in this project. Exit is the built ATR law: trail = clip(12·ATR%, 15%, 60%),
60-bar cap, 5-bar cooldown per ticker.

Calendar adjacency is enforced: a "previous bar" must be the previous TRADING bar, not merely
the previous row. Row adjacency after liquidity filtering is not calendar adjacency, and that
distinction cost this project three invalidated analyses.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as ER                                            # noqa: E402
import sources as srcs                                              # noqa: E402

pd.set_option("display.width", 200)
BAR = "=" * 124
IS_END, OOS_START = "2024-12-31", "2025-01-01"
PRICE_LO, PRICE_HI = 21.0, 89.0
MIN_N, N_PERM = 40, 300


print(BAR, flush=True)
print("  4-BAR T-SEQUENCES ENDING IN T1G — OOS reserved before mining", flush=True)
print(BAR, flush=True)
df = srcs.bars("1d", columns=("t_sig", "l_sig", "full_suffix", "atr_14"),
               min_price=5.0, min_dollar_vol=3_000_000)
df = df.sort_values(["ticker", "date"], ignore_index=True)
tk = df["ticker"].to_numpy()
d = pd.to_datetime(df["date"])
T = df["t_sig"].fillna("").astype(str).to_numpy()

# calendar adjacency — the previous ROW is only the previous BAR if the gap is small and the
# ticker is the same. Three analyses died on this distinction.
gap = d.diff().dt.days.to_numpy()
same = np.r_[False, tk[1:] == tk[:-1]]
ok1 = same & (gap <= 4)
ok = ok1.copy()
prefix = []
for k in (1, 2, 3):
    shifted = np.r_[[""] * k, T[:-k]]
    prefix.append(shifted)
    if k > 1:
        ok = ok & np.r_[[False] * (k - 1), ok1[k - 1:]]
P1, P2, P3 = prefix[0], prefix[1], prefix[2]          # t-1, t-2, t-3

close = df["close"].to_numpy(float)
entry_ok = (T == "T1G") & ok & (close >= PRICE_LO) & (close <= PRICE_HI)
df["_ent"] = entry_ok
print(f"  T1G bars in band, calendar-clean: {int(entry_ok.sum()):,}", flush=True)

grp = {t: g for t, g in df.groupby("ticker", sort=False)}
tr = ER._pathsim(grp, "_ent", mode="trail", stop=0.0, target=0.0,
                 trail=0.25, maxh=60, atr_k=12.0)
print(f"  path-sim trades (5-bar cooldown applied): {len(tr):,}", flush=True)

# map each trade back to its cell via the signal bar
sig = df.loc[entry_ok, ["ticker", "date"]].copy()
sig["seq"] = [f"{a}>{b}>{c}>T1G" for a, b, c in
              zip(P3[entry_ok], P2[entry_ok], P1[entry_ok])]
sig["l_sig"] = df.loc[entry_ok, "l_sig"].fillna("").astype(str).to_numpy()
sig["suffix"] = df.loc[entry_ok, "full_suffix"].fillna("").astype(str).to_numpy()
sig["date"] = pd.to_datetime(sig["date"]).astype(str).str[:10]
tr["date_in"] = pd.to_datetime(tr["date_in"]).astype(str).str[:10]

# _pathsim records date_in as the ENTRY bar, which is the bar AFTER the signal. Matching the
# trade to its cell on date_in would attach every trade to the wrong sequence — off by exactly
# one bar, and silently, because most of the join would still succeed.
ds = pd.to_datetime(df["date"]).astype(str).str[:10].to_numpy()
nxt = {}
for t_, g_ in df.groupby("ticker", sort=False):
    idx = g_.index.to_numpy()
    dd_ = ds[idx]
    for a, b in zip(dd_[1:], dd_[:-1]):
        nxt[(t_, a)] = b                      # entry-bar date → signal-bar date
tr["date_sig"] = [nxt.get((t_, d_), None) for t_, d_ in zip(tr["ticker"], tr["date_in"])]
tr = tr.merge(sig, left_on=["ticker", "date_sig"], right_on=["ticker", "date"], how="left")
tr = tr.dropna(subset=["seq"])
tr["ret"] = tr["ret"].astype(float) * 100
tr["yr"] = pd.to_datetime(tr["date_in"]).dt.year
IS = tr[tr["date_in"] <= IS_END]
OOS = tr[tr["date_in"] >= OOS_START]
print(f"  matched trades {len(tr):,} · IS {len(IS):,} · OOS {len(OOS):,}", flush=True)
print(f"  distinct sequences present: {tr['seq'].nunique():,} of 2,197 possible\n", flush=True)

base = float(np.median(tr["ret"]))
print(f"  baseline median over ALL T1G trades: {base:+.3f}%\n", flush=True)

# ── chance band: the best of k under within-date label permutation ────────────
print(BAR, flush=True)
print(f"  CHANCE BAND — best-of-k under within-date permutation, {N_PERM} draws", flush=True)
print(BAR, flush=True)
v = IS["ret"].to_numpy(float)
dd = IS["date_in"].to_numpy()
cc = IS["seq"].to_numpy()
sizes = pd.Series(cc).value_counts()
keep = sizes[sizes >= MIN_N].index
print(f"  cells with n ≥ {MIN_N}: {len(keep)} (these are the ones that can be ranked)",
      flush=True)
order = np.argsort(dd, kind="stable")
vs, cs, ds = v[order], cc[order], dd[order]
starts = np.r_[0, np.flatnonzero(ds[1:] != ds[:-1]) + 1, len(ds)]
rng = np.random.default_rng(7)
best = np.empty(N_PERM)
for p in range(N_PERM):
    lab = cs.copy()
    for a, b in zip(starts[:-1], starts[1:]):
        if b - a > 1:
            lab[a:b] = rng.permutation(lab[a:b])
    m = pd.Series(vs).groupby(lab).median()
    n = pd.Series(vs).groupby(lab).size()
    m = m[n[n >= MIN_N].index.intersection(m.index)]
    best[p] = m.max() if len(m) else np.nan
band = float(np.nanpercentile(best, 95))
print(f"  best-of-k p95 under the null: {band:+.3f}%   (baseline {base:+.3f}%)", flush=True)
print(f"  → a cell must beat {band:+.3f}% in-sample just to be distinguishable from luck\n",
      flush=True)

# ── in-sample ranking ────────────────────────────────────────────────────────
g = IS.groupby("seq")["ret"]
R = pd.DataFrame({"n_is": g.size(), "med_is": g.median(),
                  "win_is": IS.groupby("seq")["ret"].apply(lambda s: (s > 0).mean())})
R = R[R["n_is"] >= MIN_N].sort_values("med_is", ascending=False)
R["beats_band"] = R["med_is"] > band
go = OOS.groupby("seq")["ret"]
R["n_oos"] = go.size().reindex(R.index).fillna(0).astype(int)
R["med_oos"] = go.median().reindex(R.index)
R["win_oos"] = OOS.groupby("seq")["ret"].apply(lambda s: (s > 0).mean()).reindex(R.index)

print(BAR, flush=True)
print("  IN-SAMPLE TOP 12 — shown so the shape is visible, NOT as a result", flush=True)
print(BAR, flush=True)
print(R.head(12).to_string(float_format=lambda x: f"{x:.3f}"), flush=True)

surv = R[R["beats_band"] & (R["n_oos"] >= 20) & (R["med_oos"] > 0)]
print("\n" + BAR, flush=True)
print("  SURVIVORS — beat the chance band in-sample AND stayed positive out of sample",
      flush=True)
print(BAR, flush=True)
if len(surv):
    print(surv.to_string(float_format=lambda x: f"{x:.3f}"), flush=True)
else:
    print("  NONE.", flush=True)
n_beat = int(R["beats_band"].sum())
print(f"\n  cells beating the band in-sample: {n_beat} of {len(R)} rankable", flush=True)
print(f"  of those, positive in OOS with n ≥ 20: {len(surv)}", flush=True)
exp = 0.05 * len(R)
print(f"  expected by chance at the 95th percentile: ~{exp:.0f}", flush=True)

# ── full-descriptor slice for whatever survived ──────────────────────────────
if len(surv):
    print("\n" + BAR, flush=True)
    print("  FULL DESCRIPTOR — never judge a TZ sequence by t/z alone", flush=True)
    print(BAR, flush=True)
    for s in surv.index[:5]:
        sub = tr[tr["seq"] == s]
        print(f"\n  {s}   n={len(sub):,}  median {np.median(sub['ret']):+.3f}%", flush=True)
        for col, nm in (("l_sig", "L-line"), ("suffix", "suffix")):
            q = sub.groupby(col)["ret"].agg(["size", "median"])
            q = q[q["size"] >= 15].sort_values("median", ascending=False)
            if len(q) >= 2:
                top, bot = q.iloc[0], q.iloc[-1]
                print(f"      {nm:<8s} rescuer {q.index[0]:<10s} n={int(top['size']):>4d} "
                      f"{top['median']:+.2f}%   suppressor {q.index[-1]:<10s} "
                      f"n={int(bot['size']):>4d} {bot['median']:+.2f}%", flush=True)
R.to_csv("t1g_seq_study.csv")
print("\nDONE", flush=True)
