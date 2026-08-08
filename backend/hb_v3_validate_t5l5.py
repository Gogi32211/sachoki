"""v3 — trading validation of v2's one replicated finding: T5|L5 in a weak state.

Why this candidate and no other: it is the only cell that repeated across TWO
pre-specified states (RSI<35, below EMA200) AND both universes (🏦 and full), on BOTH
metrics (median and first-touch asymmetry), with n in the thousands:
    🏦  RSI<35        asym 37.5% (Δ+7.1) med +1.19  5/6  worst -0.63  n=1710
    🏦  below EMA200  asym 36.2% (Δ+5.7) med +1.17  6/6  worst +0.80  n=5776
    FULL RSI<35       asym 38.1% (Δ+6.5) med +1.34  6/6  worst +0.03  n=2814
Replication across independent slices is the one thing multiplicity cannot manufacture.

Tests (real engine): path-sim on the book default ⚡ATR×12 + trail25 + 2x-slip stress,
per-year 2021-26, price buckets, RSI-threshold plateau (30/35/40/45), DSR against the
114 PRE-SPECIFIED v2 trials (the honest denominator this time), overlap with every
existing board setup, and the L-line specificity control (is it L5, or would any L do?).
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

BASE = os.path.dirname(os.path.abspath(__file__))
SEG = pd.read_csv(os.path.join(BASE, "seg_frozen_2123.csv"), index_col=0)["seg_is2123"]
BANK = set(SEG[SEG == "🏦"].index)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}", flush=True)

for tk, g in grp.items():
    t = g["t"].fillna("").to_numpy()
    l = g["l"].fillna("").to_numpy()
    cl = g["close"].to_numpy(float)
    ema200 = pd.Series(cl).ewm(span=200, adjust=False).mean().to_numpy()
    isT5 = (t == "T5")
    g["_e200"] = cl < ema200
    g["V_t5l5_ovs"] = isT5 & (l == "L5") & (g["rsi_14"].to_numpy() < 35)
    g["V_t5l5_e200"] = isT5 & (l == "L5") & (cl < ema200)
    g["V_t5l5_both"] = g["V_t5l5_ovs"] & (cl < ema200)
    g["V_t5_ovs"] = isT5 & (g["rsi_14"].to_numpy() < 35)          # L-specificity control
    g["V_t5l46_ovs"] = isT5 & (l == "L46") & (g["rsi_14"].to_numpy() < 35)
    g["V_t5l25_ovs"] = isT5 & (l == "L25") & (g["rsi_14"].to_numpy() < 35)
    g["V_t5l12_ovs"] = isT5 & (l == "L12") & (g["rsi_14"].to_numpy() < 35)
    for th in (30, 40, 45):
        g[f"V_t5l5_r{th}"] = isT5 & (l == "L5") & (g["rsi_14"].to_numpy() < th)
    g["_bank"] = tk in BANK
print("masks built", flush=True)


def st(col, label, atr_k=12.0, slip=None, bank=False, lo=None, hi=None, show=True):
    if bank or lo is not None:
        for tk, g in grp.items():
            m = g[col].copy()
            if bank:
                m = m & g["_bank"]
            if lo is not None:
                m = m & g["close"].between(lo, hi)
            g["_TMP"] = m
        use = "_TMP"
    else:
        use = col
    tr = er._pathsim(grp, use, "trail", 0.10, 0.25, 0.25, 60, slip=slip, atr_k=atr_k)
    if len(tr) < 25:
        if show: print(f"  {label:34s} n={len(tr)} thin", flush=True)
        return None, tr
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in range(2021, 2027))
    d = dict(n=len(tr), med=tr["ret"].median() * 100, win=w.mean() * 100, pf=pf,
             pos=int((ym > 0).sum()), ny=len(ym), worst=float(ym.min()),
             sr=ofs.sharpe(tr["ret"].to_numpy()))
    if show:
        print(f"  {label:34s} n={d['n']:>5d} med{d['med']:>+7.2f} win{d['win']:>5.1f} "
              f"pf{d['pf']:>5.2f} |{ys} | {d['pos']}/{d['ny']} worst{d['worst']:>+6.2f}", flush=True)
    return d, tr


print("\n===== 1. the candidate under three exits =====", flush=True)
keep = {}
for lab, kw in [("⚡ATR×12", dict(atr_k=12.0)), ("trail25", dict(atr_k=None)),
                ("⚡ATR×12 2×slip", dict(atr_k=12.0, slip=0.003))]:
    print(f"\n— {lab} —", flush=True)
    for col, nm in [("V_t5l5_ovs", "T5|L5 · RSI<35"),
                    ("V_t5l5_e200", "T5|L5 · below EMA200"),
                    ("V_t5l5_both", "T5|L5 · RSI<35 AND <EMA200")]:
        d, tr = st(col, nm, **kw)
        if lab == "⚡ATR×12" and d:
            keep[col] = tr
        st(col, "   ...🏦 only", bank=True, **kw)

print("\n===== 2. L-line specificity (RSI<35, ATR exit) =====", flush=True)
for col, nm in [("V_t5_ovs", "T5 · any L"), ("V_t5l5_ovs", "T5|L5"),
                ("V_t5l46_ovs", "T5|L46"), ("V_t5l25_ovs", "T5|L25"),
                ("V_t5l12_ovs", "T5|L12")]:
    st(col, nm)

print("\n===== 3. RSI-threshold plateau =====", flush=True)
for th, col in [(30, "V_t5l5_r30"), (35, "V_t5l5_ovs"), (40, "V_t5l5_r40"), (45, "V_t5l5_r45")]:
    st(col, f"T5|L5 · RSI<{th}")

print("\n===== 4. price buckets (RSI<35, ATR exit) =====", flush=True)
for lo, hi in [(5, 21), (21, 89), (89, 377)]:
    st("V_t5l5_ovs", f"T5|L5 · ${lo}-{hi}", lo=lo, hi=hi)

print("\n===== 5. DSR vs the 114 PRE-SPECIFIED v2 trials =====", flush=True)
fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
for col, nm in [("V_t5l5_ovs", "T5|L5·RSI<35"), ("V_t5l5_e200", "T5|L5·<EMA200")]:
    if col not in keep:
        continue
    r = keep[col]["ret"].to_numpy()
    d = ofs.dsr(r, fam, n_trials=114)
    print(f"  {nm:16s} SR {d['sr']:.4f} · sr* {d['sr_star']:.4f} · DSR {d['dsr']:.3f} "
          f"(114 trials)", flush=True)

print("\n===== 6. overlap with existing board setups =====", flush=True)
for col, nm in [("V_t5l5_ovs", "T5|L5·RSI<35"), ("V_t5l5_e200", "T5|L5·<EMA200")]:
    tot = sum(int(g[col].sum()) for g in grp.values())
    hits = []
    for name, ecol in er.SETUPS:
        inter = sum(int((g[col] & g[ecol].fillna(False)).sum())
                    for g in grp.values() if ecol in g)
        if inter and tot:
            hits.append((100.0 * inter / tot, name))
    hits.sort(reverse=True)
    print(f"  {nm:16s} fires {tot:>5d} · " +
          (" · ".join(f"{n} {p:.0f}%" for p, n in hits[:6]) or "no overlap"), flush=True)

print("\nDONE", flush=True)
