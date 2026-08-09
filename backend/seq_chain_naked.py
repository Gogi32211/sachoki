"""The chain measured NAKED — no stop, no trail, no target, no RS/RSI/ATR anywhere.

The two earlier runs both answered a question with our machinery attached to it:
seq_chain_v2 measured "chain + the book's ⚡ATR×12 trail + 60-bar cap", and its second
study additionally sat inside the 🏆RS & RSI<45 gates. Useful questions, but neither one
tells you what the CHAIN itself does.

Here nothing is attached. Entry is the open of the bar after the T1G that completes the
chain (the chain completes on that bar, so this looks forward at nothing). Then simply:

    ret_N  = close[i+N] / open[i+1] − 1          for N = 5, 10, 20, 60
    mfe_N  = max(high[i+1 .. i+N]) / open[i+1] − 1
    mae_N  = min(low [i+1 .. i+N]) / open[i+1] − 1

No position is ever closed early, so no exit rule can flatter or spoil the answer. There is
also no 5-bar spacing rule — every completed chain is counted, which is what "how many were
there" actually means.

The comparison is the identical measurement on EVERY bar of the same universe. If the chain
carries information, its forward distribution has to differ from a bar picked at random.
Point estimates use all bars; the clustered intervals use a seeded 150k subsample of the
baseline, since bootstrapping 2.9M rows a thousand times buys no precision worth the wait.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er                             # noqa: E402
from analysis_kit import bootstrap_ci_clustered     # noqa: E402

pd.set_option("display.width", 235)

W = 14
HEADS = ("T10", "T11", "Z11")
HOR = (5, 10, 20, 60)
SEED = 0


def token_arr(g):
    t = g["t"].astype(str).to_numpy()
    z = g["z"].astype(str).to_numpy()
    out = np.where((t != "") & (t != "nan") & (t != "None"), t, z)
    return np.where((out == "nan") | (out == "None"), "", out)


grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp):,} tickers\n", flush=True)

recs = []          # chain completions
base = []          # every bar, same measurement
for tk, g in grp.items():
    n = len(g)
    if n < 80:
        continue
    o = g["open"].to_numpy(float)
    h = g["high"].to_numpy(float)
    lo_ = g["low"].to_numpy(float)
    c = g["close"].to_numpy(float)
    d = pd.to_datetime(g["date"]).dt.strftime("%Y-%m-%d").to_numpy()
    tok = token_arr(g)
    is_t1g, is_mid, is_head = tok == "T1G", tok == "Z2G", np.isin(tok, HEADS)

    # rolling max/min of high/low starting at i+1, for each horizon
    fwd = {}
    for N in HOR:
        hi = pd.Series(h).rolling(N).max().shift(-N).to_numpy()
        ll = pd.Series(lo_).rolling(N).min().shift(-N).to_numpy()
        cc = pd.Series(c).shift(-N).to_numpy()
        fwd[N] = (cc, hi, ll)

    ent = np.r_[o[1:], np.nan]                       # entry = next bar's open
    valid = np.isfinite(ent) & (ent > 0)
    idx = np.where(valid)[0]
    idx = idx[idx < n - max(HOR) - 1]
    if not len(idx):
        continue
    row = {"tk": tk, "date": d[idx], "yr": pd.to_datetime(d[idx]).year}
    for N in HOR:
        cc, hi, ll = fwd[N]
        row[f"r{N}"] = cc[idx] / ent[idx] - 1
        row[f"f{N}"] = hi[idx] / ent[idx] - 1
        row[f"a{N}"] = ll[idx] / ent[idx] - 1
    base.append(pd.DataFrame(row))

    # chain geometry on the T1G bars
    keep, hd, g1, g2 = [], [], [], []
    for i in np.where(is_t1g)[0]:
        if not valid[i] or i >= n - max(HOR) - 1:
            continue
        s = max(0, i - (W - 1))
        mids = np.where(is_mid[s:i])[0] + s
        for j in mids[::-1]:
            hs = np.where(is_head[s:j])[0] + s
            if len(hs):
                k = hs[-1]
                keep.append(i); hd.append(tok[k]); g1.append(j - k); g2.append(i - j)
                break
    if keep:
        ii = np.array(keep)
        r = {"tk": tk, "date": d[ii], "yr": pd.to_datetime(d[ii]).year,
             "hd": hd, "g1": g1, "g2": g2}
        for N in HOR:
            cc, hi, ll = fwd[N]
            r[f"r{N}"] = cc[ii] / ent[ii] - 1
            r[f"f{N}"] = hi[ii] / ent[ii] - 1
            r[f"a{N}"] = ll[ii] / ent[ii] - 1
        recs.append(pd.DataFrame(r))

CH = pd.concat(recs, ignore_index=True)
BA = pd.concat(base, ignore_index=True)
CH = CH[np.isfinite(CH[f"r{max(HOR)}"])]
BA = BA[np.isfinite(BA[f"r{max(HOR)}"])]
print("=" * 120, flush=True)
print(f"  RAW COUNT — every completed chain, no spacing rule, no filter", flush=True)
print(f"    chain(14) completions   {len(CH):>10,}   on {CH.tk.nunique():,} tickers", flush=True)
print(f"    all bars (the baseline) {len(BA):>10,}   on {BA.tk.nunique():,} tickers", flush=True)
print(f"    → the chain is {len(CH)/len(BA):.2%} of all bars "
      f"(≈{len(CH)/CH.date.nunique():.1f} per trading day across the whole market)", flush=True)
print("=" * 120 + "\n", flush=True)

rng = np.random.default_rng(SEED)
sub = BA.sample(min(150_000, len(BA)), random_state=SEED)


def blk(df, N, sub_for_ci=None):
    r, f, a = df[f"r{N}"] * 100, df[f"f{N}"] * 100, df[f"a{N}"] * 100
    src = sub_for_ci if sub_for_ci is not None else df
    lo, hi = bootstrap_ci_clustered(src[f"r{N}"] * 100, src["date"], stat="median",
                                    n_boot=600, seed=SEED)
    return dict(n=len(r), up=(r > 0).mean(), med=r.median(), mean=r.mean(), lo=lo, hi=hi,
                fmed=f.median(), f90=f.quantile(.90), amed=a.median(),
                p5=(f >= 5).mean(), p10=(f >= 10).mean(), p20=(f >= 20).mean())


print("─" * 120, flush=True)
print("  NAKED FORWARD RETURNS — entry = next open · nothing closes the trade early",
      flush=True)
print("─" * 120, flush=True)
hdr = (f"  {'':16s} {'N':>4s} {'n':>9s} {'↑':>7s} {'med':>7s} {'CI(days)':>16s} "
       f"{'mean':>7s} | {'MFEmed':>7s} {'MFEp90':>7s} {'MAEmed':>7s} | "
       f"{'≥5%':>6s} {'≥10%':>6s} {'≥20%':>6s}")
print(hdr, flush=True)
summary = {}
for lbl, df, s in (("chain(14)", CH, None), ("ALL BARS", BA, sub)):
    for N in HOR:
        b = blk(df, N, s)
        summary[(lbl, N)] = b
        print(f"  {lbl if N == HOR[0] else '':16s} {N:>4d} {b['n']:>9,} {b['up']:>7.2%} "
              f"{b['med']:>+7.2f} [{b['lo']:>+6.2f},{b['hi']:>+6.2f}] {b['mean']:>+7.2f} | "
              f"{b['fmed']:>7.2f} {b['f90']:>7.1f} {b['amed']:>7.2f} | "
              f"{b['p5']:>6.1%} {b['p10']:>6.1%} {b['p20']:>6.1%}", flush=True)
    print(flush=True)

print("  Δ chain − all bars (this is the only number that matters):", flush=True)
for N in HOR:
    a, b = summary[("chain(14)", N)], summary[("ALL BARS", N)]
    ovl = not (a["lo"] > b["hi"] or a["hi"] < b["lo"])
    print(f"    {N:>3d} bars   Δmed {a['med'] - b['med']:>+6.2f}pp   "
          f"Δ↑ {(a['up'] - b['up']) * 100:>+5.2f}pp   Δmean {a['mean'] - b['mean']:>+6.2f}pp   "
          f"ΔMFE {a['fmed'] - b['fmed']:>+6.2f}pp   ΔMAE {a['amed'] - b['amed']:>+6.2f}pp   "
          f"{'intervals OVERLAP → not distinguishable' if ovl else 'intervals SEPARATE'}",
          flush=True)

# ── where the chain lives, without any gate ──────────────────────────────────
BEST = 20
print("\n" + "─" * 120, flush=True)
print(f"  BREAKDOWN at {BEST} bars — naked", flush=True)
print("─" * 120, flush=True)


def band(v):
    return np.where(v <= 2, "≤2", np.where(v <= 5, "3-5", "≥6"))


CH = CH.assign(b1=band(CH.g1.to_numpy()), b2=band(CH.g2.to_numpy()))
allb = summary[("ALL BARS", BEST)]


def row(lbl, m):
    d = CH[m]
    if len(d) < 40:
        print(f"    {lbl:20s} n={len(d):>6,}  (thin)", flush=True); return
    r, f = d[f"r{BEST}"] * 100, d[f"f{BEST}"] * 100
    ym = r.groupby(d.yr).median()
    print(f"    {lbl:20s} n={len(d):>6,} ↑{(r > 0).mean():>6.2%} med {r.median():>+6.2f} "
          f"(Δ {r.median() - allb['med']:>+5.2f}) MFEmed {f.median():>5.2f} "
          f"p90 {f.quantile(.90):>5.1f} ≥10% {(f >= 10).mean():>5.1%} "
          f"{int((ym > 0).sum())}/{len(ym)}yr worst {ym.min():>+6.2f}", flush=True)


row("ALL chain", pd.Series(True, index=CH.index))
print(flush=True)
for h in HEADS:
    row(f"head {h}", CH.hd == h)
print(flush=True)
for b in ("≤2", "3-5", "≥6"):
    row(f"g1 (head→Z2G) {b}", CH.b1 == b)
print(flush=True)
for b in ("≤2", "3-5", "≥6"):
    row(f"g2 (Z2G→T1G) {b}", CH.b2 == b)
print(flush=True)
for y in sorted(CH.yr.unique()):
    row(str(y), CH.yr == y)

print("\n" + "─" * 120, flush=True)
print(f"  27 CELLS (head × g1 × g2) at {BEST} bars — ranked; the top row is a SELECTION",
      flush=True)
print("─" * 120, flush=True)
rows = []
for h in HEADS:
    for b1 in ("≤2", "3-5", "≥6"):
        for b2 in ("≤2", "3-5", "≥6"):
            m = (CH.hd == h) & (CH.b1 == b1) & (CH.b2 == b2)
            d = CH[m]
            if len(d) < 40:
                rows.append(dict(cell=f"{h} g1{b1} g2{b2}", n=len(d), thin=True)); continue
            r, f = d[f"r{BEST}"] * 100, d[f"f{BEST}"] * 100
            ym = r.groupby(d.yr).median()
            rows.append(dict(cell=f"{h} g1{b1} g2{b2}", n=len(d), thin=False,
                             up=(r > 0).mean(), med=r.median(), mfe=f.median(),
                             p90=f.quantile(.90), yrs=int((ym > 0).sum()), nyr=len(ym),
                             worst=ym.min()))
E = pd.DataFrame(rows)
ok = E[~E.thin].sort_values("med", ascending=False)
print(f"    {'cell':22s} {'n':>7s} {'↑':>7s} {'med':>7s} {'Δall':>6s} {'MFEmed':>7s} "
      f"{'MFEp90':>7s} {'yrs':>5s} {'worst':>7s}", flush=True)
for _, r in ok.iterrows():
    print(f"    {r.cell:22s} {r.n:>7,} {r.up:>7.2%} {r.med:>+7.2f} "
          f"{r.med - allb['med']:>+6.2f} {r.mfe:>7.2f} {r.p90:>7.1f} "
          f"{r.yrs}/{r.nyr:<3d} {r.worst:>+7.2f}", flush=True)
print(f"    ({int(E.thin.sum())} of 27 thin)", flush=True)

# how wide a spread does pure noise give? shuffle the labels, keep the sizes.
sizes = ok.n.to_numpy()
spreads = []
pool = CH[f"r{BEST}"].to_numpy() * 100
for _ in range(400):
    meds = [np.median(rng.choice(pool, s, replace=False)) for s in sizes]
    spreads.append(max(meds) - min(meds))
obs = ok.med.max() - ok.med.min()
print(f"\n    observed best−worst spread {obs:+.2f}pp", flush=True)
print(f"    same spread from RANDOM cells of the same sizes: median "
      f"{np.median(spreads):.2f} · p95 {np.percentile(spreads, 95):.2f}", flush=True)
print(f"    → the winner is {'INSIDE' if obs <= np.percentile(spreads, 95) else 'OUTSIDE'}"
      f" what pure noise produces across 27 cells", flush=True)
CH.to_csv("seq_chain_naked.csv", index=False)
print("\nDONE", flush=True)
