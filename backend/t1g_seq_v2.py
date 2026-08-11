"""T1G sequences, v2 — a SIZE-FAIR band, because the first design let n=40 set the bar.

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
MIN_N, N_PERM = 200, 400


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


# ── size-fair band: every cell judged against ITS OWN null ───────────────────
# The first run used one max-statistic band over cells ranging from n=40 to n=997. The median of
# a 40-trade cell swings far more than that of a 997-trade cell, so the maximum was set by the
# smallest cells and the large ones paid for their noise. The band came out at +6.118% and
# nothing could clear it — including cells whose own sampling error was a fraction of that.
#
# The repair is not a looser threshold. It is a statistic that is comparable across sizes:
#
#     z_c = (med_c − null_mean_c) / null_sd_c
#
# where null_mean_c and null_sd_c come from the SAME permutations, per cell. A big cell and a
# small one are then on one scale, and the max-of-z band still prices having looked k times.
#
# n ≥ 200 on top of that, declared here, because a path-sim median on fewer trades is not
# something this project has any business ranking.
print(BAR, flush=True)
print(f"  SIZE-FAIR CHANCE BAND — per-cell null, {N_PERM} draws, n ≥ {MIN_N}", flush=True)
print(BAR, flush=True)
v = IS["ret"].to_numpy(float)
dd = IS["date_in"].to_numpy()
cc = IS["seq"].to_numpy()
sizes = pd.Series(cc).value_counts()
keep = list(sizes[sizes >= MIN_N].index)
K = len(keep)
print(f"  k = {K} cells with n >= {MIN_N}   (derived from the data, not typed)", flush=True)
order = np.argsort(dd, kind="stable")
vs, cs, ds = v[order], cc[order], dd[order]
starts = np.r_[0, np.flatnonzero(ds[1:] != ds[:-1]) + 1, len(ds)]
rng = np.random.default_rng(11)
null = {c: np.empty(N_PERM) for c in keep}
for p in range(N_PERM):
    lab = cs.copy()
    for a, b in zip(starts[:-1], starts[1:]):
        if b - a > 1:
            lab[a:b] = rng.permutation(lab[a:b])
    m = pd.Series(vs).groupby(lab).median()
    for c in keep:
        null[c][p] = m.get(c, np.nan)
mu = {c: float(np.nanmean(null[c])) for c in keep}
sd = {c: float(np.nanstd(null[c], ddof=1)) for c in keep}
Z = np.empty((N_PERM, K))
for j, c in enumerate(keep):
    Z[:, j] = (null[c] - mu[c]) / sd[c]
zband = float(np.nanpercentile(np.nanmax(Z, axis=1), 95))
print(f"  max-of-z p95 under the null: {zband:+.3f} sigma", flush=True)
print(f"  per-cell null sd ranges {min(sd.values()):.3f}% .. {max(sd.values()):.3f}% "
      f"— that spread is exactly what the first design ignored\n", flush=True)

g = IS.groupby("seq")["ret"]
R = pd.DataFrame({"n_is": g.size(), "med_is": g.median()}).loc[keep]
R["null_mu"] = [mu[c] for c in R.index]
R["null_sd"] = [sd[c] for c in R.index]
R["z"] = (R.med_is - R.null_mu) / R.null_sd
R = R.sort_values("z", ascending=False)
R["beats"] = R["z"] > zband
go = OOS.groupby("seq")["ret"]
R["n_oos"] = go.size().reindex(R.index).fillna(0).astype(int)
R["med_oos"] = go.median().reindex(R.index)
B26 = OOS[OOS["date_in"] >= "2026-01-01"]
R["n_26"] = B26.groupby("seq")["ret"].size().reindex(R.index).fillna(0).astype(int)
R["med_26"] = B26.groupby("seq")["ret"].median().reindex(R.index)
SEEN = ["T5>T2>>T1G","T1>T10>>T1G","T3>T10>>T1G","T2G>T12>>T1G","T4>T2>>T1G","T6>>>T1G",
        "T2>T10>>T1G","T1>T2>>T1G","T9>>>T1G","T2G>T2G>>T1G","T5>T2G>>T1G",">T1>>T1G"]
R["oos_seen"] = [c in SEEN for c in R.index]

print(BAR, flush=True)
print("  RANKED BY z — the whole table, k is small enough to show it all", flush=True)
print(BAR, flush=True)
print(R.to_string(float_format=lambda x: f"{x:.3f}"), flush=True)

surv = R[R["beats"]]
print("\n" + BAR, flush=True)
print("  VERDICT", flush=True)
print(BAR, flush=True)
if len(surv):
    print(f"  {len(surv)} cell(s) beat the size-fair band:", flush=True)
    print(surv.to_string(float_format=lambda x: f"{x:.3f}"), flush=True)
    for c in surv.index:
        tag = "OOS ALREADY SEEN in the previous run — evidence weakened" if c in SEEN \
            else "OOS blind"
        print(f"    {c:<18s} {tag}", flush=True)
else:
    print("  NONE beat the size-fair band either.", flush=True)
    print(f"  best z = {R.z.max():+.3f} against a band of {zband:+.3f}", flush=True)
print(f"\n  Under a max-of-k band at p95, P(any cell exceeds) = 5% by construction.", flush=True)
print(f"  So 0 survivors is the modal outcome under the null, not a surprise.", flush=True)
R.to_csv("t1g_seq_study_v2.csv")
print("\nDONE", flush=True)
