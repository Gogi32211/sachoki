"""Deciding L3 test for 🪨 T1G-NB + L34-in-prefix after DSR failed at the family level.

Two questions the first pass left open:
 1. PLATEAU — does the lift survive varying the prefix window (L34 within 1/2/3/4/5/6
    bars)? A real structural effect degrades smoothly; a lucky cell only works at the
    exact width that was searched. This is the honest discriminator.
 2. NARROW DSR — the 87-cell family mixes parents whose medians differ 5x (CROWN +17%
    vs SAND +1.8%), so sr* is inflated by CROWN's scale, not by T1G-NB's search. The
    family actually searched FOR THIS PARENT is its own ~7 descriptor cells. Both
    numbers are reported; the wide one stays the headline.
 3. CONTROL — is it L34 specifically, or would ANY L-line in the prefix do? If every
    L-line lifts equally, the finding is "prefix has any L", not "L34".
"""
import numpy as np
import pandas as pd
import edge_replay as er
import overfit_stats as ofs

grp, as_of = er._frame(60, 3_000_000)
print(f"frame {len(grp)} as_of {as_of}\n", flush=True)

LINES = ["L34", "L46", "L12", "L3", "L25", "L5"]

for tk, g in grp.items():
    l = g["l"].fillna("")
    sh = [l.shift(k).eq("L34") for k in range(1, 7)]
    for w in range(1, 7):                       # L34 within the last w bars
        acc = sh[0]
        for k in range(1, w):
            acc = acc | sh[k]
        g[f"S_w{w}"] = g["E_t1gnb_rs"] & acc
    for ln in LINES:                            # control: any other line, 3-bar window
        a = l.shift(1).eq(ln) | l.shift(2).eq(ln) | l.shift(3).eq(ln)
        g[f"S_{ln}"] = g["E_t1gnb_rs"] & a


def row(lab, col):
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60)
    if len(tr) == 0:
        print(f"  {lab:24s}  n=0", flush=True); return None
    yr = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    ys = "".join(f"{yr.get(y, float('nan')):>7.1f}" for y in range(2022, 2027))
    print(f"  {lab:24s} {len(tr):>5d} {tr['ret'].median()*100:>+7.2f} {w.mean()*100:>5.1f}"
          f" {pf:>6.2f} {ys}  {int((yr>0).sum())}/{len(yr)} {yr.min():>+6.1f}", flush=True)
    return tr


HDR = (f"  {'cell':24s} {'n':>5s} {'med':>7s} {'win':>5s} {'pf':>6s} "
       f"{'2022':>7s}{'2023':>7s}{'2024':>7s}{'2025':>7s}{'2026':>7s}  pos  worst")

print("===== 1. PLATEAU — L34 within the last w bars =====\n" + HDR, flush=True)
plateau = {}
for w in range(1, 7):
    plateau[w] = row(f"w={w} bars", f"S_w{w}")
base = row("parent (no L filter)", "E_t1gnb_rs")

print("\n===== 2. CONTROL — is it L34, or any L-line in a 3-bar prefix? =====\n" + HDR, flush=True)
ctrl = {}
for ln in LINES:
    ctrl[ln] = row(f"{ln} in 3-bar prefix", f"S_{ln}")

print("\n===== 3. NARROW DSR — trial family = this parent's own cells only =====", flush=True)
srs = []
for w in range(1, 7):
    if plateau[w] is not None and len(plateau[w]) >= 15:
        srs.append(ofs.sharpe(plateau[w]["ret"].to_numpy()))
for ln in LINES:
    if ctrl[ln] is not None and len(ctrl[ln]) >= 15:
        srs.append(ofs.sharpe(ctrl[ln]["ret"].to_numpy()))
tr3 = plateau[3]
d = ofs.dsr(tr3["ret"].to_numpy(), srs)
print(f"  trials: {len(srs)}   cell SR {d.get('sr'):.4f}   sr* {d.get('sr_star'):.4f}"
      f"   DSR {d.get('dsr'):.3f}", flush=True)
print("\nDONE", flush=True)
