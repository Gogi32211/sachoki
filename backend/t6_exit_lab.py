"""T6 exits: the position sees +3.17% and keeps +0.10%. Can any exit rule change that?

The entry side is settled — T6, T→T6 and T→T→T6 all overlap their controls at every horizon,
so nothing here is trying to manufacture an edge from a signal that has none. The question is
narrower and worth answering on its own: given that these trades happen, what does the exit
rule do to the distribution, and is any of it stable out of sample?

One thing in the drawdown work says a stop should help more than usual here. Of the 127 trades
that gained more than 25%, only 7 (5.5%) first fell past −10% — the winners go up more or less
straight. A stop that cuts the left tail should therefore cost very little of the right one.
This lab tests that instead of assuming it.

The simulation is a real path, not a proxy: entry at the open after the T6 bar, then bar by
bar, checking the STOP FIRST when a bar could have hit both (the pessimistic assumption), a
trailing stop measured from the running high, and a hard cap in bars. Costs are a flat 0.15%
round trip, plus another 0.05% on stop and trail exits, which fill worse than a close.

It is a search — 4 phases, ~60 configurations — so it is run as one: everything is fitted on
2021-05 → 2023-12 and the survivors are read once on 2024-01 → 2026-07, untouched.

⚡ATR×12, the book's own exit law, is included as the benchmark to beat.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from naked_study import NakedStudy

MAXH = 60
COST, SLIP = 0.15, 0.05
SPLIT = np.datetime64("2024-01-01")

st = NakedStudy("T6 exit lab", n_trials=2, columns=("t_sig", "z_sig", "atr_14"),
                horizons=(5,), min_price=5.0, min_dollar_vol=3_000_000)
d = st.df
T = d["t_sig"].fillna("").astype(str).to_numpy()
t6 = (T == "T6")
tk = d["ticker"].to_numpy()
o = d["open"].to_numpy(float)
h = d["high"].to_numpy(float)
lo = d["low"].to_numpy(float)
c = d["close"].to_numpy(float)
atr = d["atr_14"].to_numpy(float)

idx = np.where(t6)[0]
idx = idx[(idx + MAXH + 1 < len(d))]
idx = idx[tk[idx] == tk[idx + MAXH + 1]]          # whole path inside one ticker
ent = o[idx + 1]
good = np.isfinite(ent) & (ent > 0)
idx, ent = idx[good], ent[good]
print(f"\n  T6 trades with a full {MAXH}-bar path: {len(idx):,}", flush=True)

# path matrices, built once: rows = trades, columns = bars held
off = np.arange(1, MAXH + 1)
H = h[idx[:, None] + off]
L = lo[idx[:, None] + off]
C = c[idx[:, None] + off]
E = ent[:, None]
atr_pct = np.clip(atr[idx] / c[idx], 0.005, 0.25)
yr = d["_dt"].to_numpy()[idx]
mined, oos = yr < SPLIT, yr >= SPLIT
print(f"  mined {mined.sum():,} · OOS {oos.sum():,}", flush=True)


def simulate(stop=None, target=None, trail=None, maxh=MAXH, atr_k=None):
    """Bar-by-bar exit. Stop is checked before target when a bar could have hit both."""
    n = len(idx)
    hh = H[:, :maxh]
    ll = L[:, :maxh]
    cc = C[:, :maxh]
    runmax = np.maximum.accumulate(np.maximum(hh, E), axis=1)
    trig = np.zeros_like(hh, bool)
    price = np.full(hh.shape, np.nan)

    if stop is not None:
        sp = E * (1 - stop)
        hit = ll <= sp
        trig |= hit
        price = np.where(hit & np.isnan(price), sp, price)
    tr = trail if trail is not None else (
        np.clip(atr_k * atr_pct, 0.15, 0.60)[:, None] if atr_k else None)
    if tr is not None:
        tp = runmax * (1 - tr)
        # only after the bar that set the high, so compare against the PREVIOUS running max
        prev = np.concatenate([E, runmax[:, :-1]], axis=1) * (1 - tr)
        hit = ll <= prev
        trig |= hit
        price = np.where(hit & np.isnan(price), prev, price)
    if target is not None:
        tg = E * (1 + target)
        hit = hh >= tg
        # a stop in the same bar wins the tie
        already = np.isfinite(price)
        trig |= hit
        price = np.where(hit & ~already, tg, price)

    first = np.where(trig.any(axis=1), trig.argmax(axis=1), maxh - 1)
    rows = np.arange(n)
    exited = trig.any(axis=1)
    px = np.where(exited, price[rows, first], cc[rows, maxh - 1])
    px = np.where(np.isfinite(px), px, cc[rows, maxh - 1])
    ret = (px / ent - 1) * 100 - COST
    ret -= np.where(exited & (stop is not None or tr is not None), SLIP, 0.0)
    return ret, first + 1, exited


def stats(r, m):
    x = r[m]
    win = x > 0
    pf = x[win].sum() / abs(x[~win].sum()) if (~win).any() and x[~win].sum() != 0 else np.nan
    return dict(n=len(x), mean=x.mean(), med=np.median(x), win=win.mean() * 100, pf=pf)


CONFIGS = []
for b in (1, 2, 3, 5, 10, 20, 40, 60):
    CONFIGS.append((f"hold {b}b", dict(maxh=b)))
for s in (0.03, 0.05, 0.08, 0.12, 0.20):
    CONFIGS.append((f"stop {s:.0%} · 20b", dict(stop=s, maxh=20)))
for t in (0.03, 0.05, 0.08, 0.12, 0.20):
    CONFIGS.append((f"target {t:.0%} · 20b", dict(target=t, maxh=20)))
for t in (0.05, 0.08, 0.12, 0.20, 0.30):
    CONFIGS.append((f"trail {t:.0%} · 60b", dict(trail=t, maxh=60)))
for k in (6, 9, 12, 16):
    CONFIGS.append((f"⚡ATR×{k} · 60b", dict(atr_k=float(k), maxh=60)))
for s in (0.05, 0.08, 0.12):
    for t in (0.05, 0.08, 0.12, 0.20):
        CONFIGS.append((f"stop {s:.0%} + target {t:.0%} · 20b",
                        dict(stop=s, target=t, maxh=20)))
for s in (0.05, 0.08, 0.12):
    for t in (0.08, 0.12, 0.20):
        CONFIGS.append((f"stop {s:.0%} + trail {t:.0%} · 60b",
                        dict(stop=s, trail=t, maxh=60)))

print(f"  configurations: {len(CONFIGS)} — this is a SEARCH, so it is fitted on the mined "
      f"half and read once on the frozen half\n", flush=True)

rows = []
for name, kw in CONFIGS:
    r, held, ex = simulate(**kw)
    a, b = stats(r, mined), stats(r, oos)
    rows.append(dict(rule=name, n=len(r), mean_all=r.mean(), med_all=np.median(r),
                     win_all=(r > 0).mean() * 100, hold=held.mean(),
                     exit_rate=ex.mean() * 100,
                     mean_mined=a["mean"], mean_oos=b["mean"],
                     win_mined=a["win"], win_oos=b["win"],
                     pf_mined=a["pf"], pf_oos=b["pf"]))
R = pd.DataFrame(rows).sort_values("mean_mined", ascending=False)

print("=" * 128, flush=True)
print("  ALL RULES — fitted on 2021-05→2023-12, read on 2024-01→2026-07", flush=True)
print("=" * 128, flush=True)
print(f"  {'rule':30s} {'hold':>6s} {'exit%':>6s} | {'MINED mean':>11s} {'win':>6s} "
      f"{'PF':>5s} | {'OOS mean':>9s} {'win':>6s} {'PF':>5s} | {'kept':>6s}", flush=True)
for _, r in R.iterrows():
    kept = r.mean_oos / r.mean_mined if r.mean_mined > 0 else np.nan
    print(f"  {r.rule:30s} {r.hold:>6.1f} {r.exit_rate:>5.1f}% | {r.mean_mined:>+11.3f} "
          f"{r.win_mined:>5.1f}% {r.pf_mined:>5.2f} | {r.mean_oos:>+9.3f} {r.win_oos:>5.1f}% "
          f"{r.pf_oos:>5.2f} | {kept:>6.0%}", flush=True)

print("\n" + "=" * 128, flush=True)
print("  DOES THE RANKING TRANSFER?", flush=True)
print("=" * 128, flush=True)
rho = R.mean_mined.corr(R.mean_oos, method="spearman")
top = R.head(5)
print(f"    rank correlation mined ↔ OOS: {rho:+.3f}", flush=True)
print(f"    top-5 on the mined half: mean {top.mean_mined.mean():+.3f} → "
      f"OOS {top.mean_oos.mean():+.3f}", flush=True)
print(f"    best OOS rule overall: {R.loc[R.mean_oos.idxmax(), 'rule']} "
      f"({R.mean_oos.max():+.3f}) — and it ranked "
      f"{int(R.reset_index().index[R.reset_index()['mean_oos'].idxmax()]) + 1} of "
      f"{len(R)} on the mined half", flush=True)
pos = R[(R.mean_mined > 0) & (R.mean_oos > 0)]
print(f"    rules positive in BOTH halves: {len(pos)} of {len(R)}", flush=True)
bench = R[R.rule.str.contains("ATR×12")]
if len(bench):
    b = bench.iloc[0]
    print(f"\n    the book's own law, ⚡ATR×12: mined {b.mean_mined:+.3f} · "
          f"OOS {b.mean_oos:+.3f} · hold {b.hold:.1f} bars", flush=True)
    better = R[(R.mean_oos > b.mean_oos) & (R.mean_mined > b.mean_mined)]
    print(f"    rules beating it in BOTH halves: {len(better)}"
          + (f" → {list(better.rule)[:5]}" if len(better) else "  — none"), flush=True)
R.to_csv("t6_exit_lab.csv", index=False)
print("\n  written: t6_exit_lab.csv", flush=True)
print("\nDONE", flush=True)
