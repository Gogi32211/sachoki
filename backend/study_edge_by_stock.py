"""Which stocks does each Edge signal actually work on? (user request 2026-08-06)

Every live DISPLAY_SETUPS edge is path-simmed on the canonical frame (trail 10/25/25/60,
the board's own exits), and its trades are sliced two ways:
  1. by the 1D temperament segment (d1_russell2k_segments.csv — the widest labeling);
  2. by ticker — top contributors (sum of returns, n>=4 fires) and worst offenders.
Answers: does each edge live where the segmentation says it should (capitulation edges
on ⚡, breakout edges on 🎲), and which individual names carry each edge.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er

BASE = os.path.dirname(os.path.abspath(__file__))
SEG = pd.read_csv(os.path.join(BASE, "d1_russell2k_segments.csv"), index_col=0)["seg1d"] \
    if "seg1d" in pd.read_csv(os.path.join(BASE, "d1_russell2k_segments.csv"), index_col=0, nrows=1).columns \
    else pd.read_csv(os.path.join(BASE, "d1_russell2k_segments.csv"), index_col=0).iloc[:, 0]
print(f"segment labels: {len(SEG)} tickers · {SEG.value_counts().to_dict()}", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame {len(grp)} as_of {as_of}", flush=True)

SEGS_ORDER = ["🏦majority", "⚡panic", "🎲chase", "🎰lottery"]

seen_cols = set()
rows_out = []
for code, col in er.DISPLAY_SETUPS:
    if col in seen_cols:
        continue
    seen_cols.add(col)
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60)
    if len(tr) < 30:
        continue
    tr["seg"] = tr["ticker"].map(SEG).fillna("∅unlabeled")
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    base_med = tr["ret"].median() * 100
    line = {"edge": code, "n": len(tr), "med": base_med,
            "pf": (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")}
    print(f"\n##### {code}  n={len(tr)}  med{base_med:+.2f}", flush=True)
    for s in SEGS_ORDER:
        sub = tr[tr["seg"] == s]
        if len(sub) < 15:
            line[s] = None
            print(f"    {s:12s} n={len(sub):>5d}  — thin", flush=True)
            continue
        ws = sub["ret"] > 0
        d2 = -sub.loc[~ws, "ret"].sum()
        pf = (sub.loc[ws, "ret"].sum() / d2) if d2 > 0 else float("inf")
        med = sub["ret"].median() * 100
        line[s] = med
        print(f"    {s:12s} n={len(sub):>5d}  med{med:>+7.2f}  win{ws.mean()*100:>5.1f}"
              f"  pf{pf:>5.2f}  Δ{med-base_med:>+6.2f}", flush=True)
    # per-ticker contributors
    by_tk = tr.groupby("ticker").agg(n=("ret", "size"), tot=("ret", "sum"),
                                     med=("ret", "median"))
    by_tk = by_tk[by_tk["n"] >= 4]
    if len(by_tk):
        top = by_tk.sort_values("tot", ascending=False).head(8)
        bot = by_tk.sort_values("tot").head(4)
        print("    ტოპ:  " + " · ".join(
            f"{t}({SEG.get(t,'?')[:1]} n{int(r['n'])} {r['tot']*100:+.0f}%)"
            for t, r in top.iterrows()), flush=True)
        print("    ცუდი: " + " · ".join(
            f"{t}({SEG.get(t,'?')[:1]} n{int(r['n'])} {r['tot']*100:+.0f}%)"
            for t, r in bot.iterrows()), flush=True)
    rows_out.append(line)

M = pd.DataFrame(rows_out).set_index("edge")
print("\n\n===== MATRIX: edge × segment (med per trade, %) =====", flush=True)
with pd.option_context("display.width", 200):
    print(M.round(2).to_string(), flush=True)
M.round(3).to_csv(os.path.join(BASE, "edge_by_segment.csv"))
print("\nsaved -> edge_by_segment.csv", flush=True)
print("DONE", flush=True)
