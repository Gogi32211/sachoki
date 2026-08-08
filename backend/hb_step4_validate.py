"""Step 4 — final validation of the ONE survivor of the 🏦 1D sweep.

Candidate, two levels:
  CORE  Z1G → Z2G → T5           (TZ codes only, n=1127 on fwd10)
  L46   Z1G|L46 → Z2G|L46 → T5|L46  (all three bars on the L46 volume line, n=188)

Everything here is on the real trading engine, not fwd-returns:
  - path-sim with the book's current default exit (⚡ATR×12, clip 15-60%) AND the legacy
    trail25, both with 15bps slippage; plus a 2x-slip stress
  - per-year 2021-26, worst-year, pf, MAE, hold
  - price buckets ($5-21 / $21-89 / $89-377)
  - DSR against the sweep's HONEST trial count (5,389 cells were examined)
  - overlap with every existing board setup (is this new, or a relabel?)
  - 🏦-only vs the full universe (does the segment restriction matter?)
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
print(f"frame as_of {as_of} · 🏦 tickers in frame: "
      f"{sum(1 for t in grp if t in BANK)}", flush=True)

# ── build the masks on the replay frame ─────────────────────────────────────────
for tk, g in grp.items():
    code = np.where(g["t"].to_numpy() != "", g["t"].to_numpy(), g["z"].to_numpy())
    l = g["l"].fillna("").to_numpy()
    c1 = np.roll(code, 1); c2 = np.roll(code, 2)
    l1 = np.roll(l, 1); l2 = np.roll(l, 2)
    c1[:1] = ""; c2[:2] = ""; l1[:1] = ""; l2[:2] = ""
    core = (c2 == "Z1G") & (c1 == "Z2G") & (code == "T5")
    g["S_core"] = core
    g["S_l46"] = core & (l2 == "L46") & (l1 == "L46") & (l == "L46")
    g["S_core_bank"] = g["S_core"] & (tk in BANK)
    g["S_l46_bank"] = g["S_l46"] & (tk in BANK)
print("masks built", flush=True)


def stats(tr, label, show=True):
    if len(tr) < 20:
        if show:
            print(f"  {label:34s} n={len(tr)} — thin", flush=True)
        return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in range(2021, 2027))
    d = dict(n=len(tr), med=tr["ret"].median() * 100, win=w.mean() * 100, pf=pf,
             pos=int((ym > 0).sum()), ny=len(ym), worst=float(ym.min()),
             mae=tr["mae"].median() * 100, hold=tr["hold"].mean(),
             sr=ofs.sharpe(tr["ret"].to_numpy()))
    if show:
        print(f"  {label:34s} n={d['n']:>5d} med{d['med']:>+7.2f} win{d['win']:>5.1f} "
              f"pf{d['pf']:>5.2f} |{ys} | {d['pos']}/{d['ny']} worst{d['worst']:>+6.2f} "
              f"MAE{d['mae']:>6.2f} hold{d['hold']:>5.1f}", flush=True)
    return d


EXITS = [("⚡ATR×12 (book default)", dict(atr_k=12.0, slip=None)),
         ("trail25 (legacy)",        dict(atr_k=None, slip=None)),
         ("⚡ATR×12 · 2× slip",       dict(atr_k=12.0, slip=0.003))]

print("\n===== 1. the two levels under three exits =====", flush=True)
keep = {}
for ename, kw in EXITS:
    print(f"\n— {ename} —", flush=True)
    for col, lab in [("S_core", "CORE  Z1G→Z2G→T5"),
                     ("S_core_bank", "  ...🏦 only"),
                     ("S_l46", "L46   all-three-L46"),
                     ("S_l46_bank", "  ...🏦 only")]:
        tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60,
                         slip=kw["slip"], atr_k=kw["atr_k"])
        d = stats(tr, lab)
        if ename.startswith("⚡ATR×12 (") and d:
            keep[col] = tr

print("\n===== 2. price buckets (ATR exit) =====", flush=True)
for col, lab in [("S_core", "CORE"), ("S_l46", "L46")]:
    for lo, hi in [(5, 21), (21, 89), (89, 377)]:
        for tk, g in grp.items():
            g["_B"] = g[col] & g["close"].between(lo, hi)
        stats(er._pathsim(grp, "_B", "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0),
              f"{lab} ${lo}-{hi}")

print("\n===== 3. DSR vs the sweep's honest trial count (5,389 cells) =====", flush=True)
fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
for col, lab in [("S_core", "CORE"), ("S_l46", "L46")]:
    if col not in keep:
        continue
    r = keep[col]["ret"].to_numpy()
    d = ofs.dsr(r, fam, n_trials=5389)
    print(f"  {lab:6s} SR {d['sr']:.4f} · sr* {d['sr_star']:.4f} · DSR {d['dsr']:.3f} "
          f"(trials 5389)", flush=True)
    d2 = ofs.dsr(r, fam)
    print(f"         vs board family only ({len(fam)} trials): DSR {d2['dsr']:.3f}", flush=True)

print("\n===== 4. overlap with existing board setups =====", flush=True)
for col, lab in [("S_core", "CORE"), ("S_l46", "L46")]:
    tot = sum(int(g[col].sum()) for g in grp.values())
    hits = []
    for name, ecol in er.SETUPS:
        inter = sum(int((g[col] & g[ecol].fillna(False)).sum())
                    for g in grp.values() if ecol in g)
        if inter and tot:
            hits.append((100.0 * inter / tot, name))
    hits.sort(reverse=True)
    top = " · ".join(f"{n} {p:.0f}%" for p, n in hits[:5]) or "none"
    print(f"  {lab:6s} fires {tot:>5d} · max overlap: {top}", flush=True)

print("\nDONE", flush=True)
