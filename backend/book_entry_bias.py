"""Does the book stand on the opening print?

`_pathsim` enters at the open of the bar after the signal. That is the right decision — the
signal is defined by a close, so the open is the first price anyone can transact. But it also
means the entry price and the selection share a variable whenever a setup systematically fires
before a gap, and today that combination was shown to manufacture an entire fake edge: T4
looked like +0.92% and a 57.7% win rate on open entry, and was exactly a coin flip once the
opening print was removed from the outcome (close[i+1] entry: +0.000%, 49.95%).

Our edges are mostly "buy absorbed weakness", which is precisely the kind of bar that should
be followed by a gap down. So the question is not academic: how much of the book's measured
performance lives between the printed open and that same bar's close?

The test is a single substitution, applied to every family. `_pathsim` is not touched — this
runs its own arithmetic, with the same trade dates, so the only thing that changes is the
price we are assumed to pay:

    entry_open  = open[i+1]     (what the book uses)
    entry_close = close[i+1]    (one bar later, but no shared print)

A family whose edge is real gives up roughly one bar of drift and keeps the rest. A family
standing on the artifact collapses toward zero, and its median next-open gap will be the
thing that predicts how far it falls.

Holding is a plain 10 and 20 bars from entry — no trail, no stop, no cap. The book's ⚡ATR
exit is deliberately absent: this asks about the ENTRY price, and an exit rule would let a
stop interact with the very gap under examination.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er                       # noqa: E402

pd.set_option("display.width", 205)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp):,} tickers\n", flush=True)

FAM = {}
for name, col in er.SETUPS:
    FAM.setdefault(name.split("🏆")[0].split("🧱")[0].strip(), []).append((name, col))

rows = []
for i, (name, col) in enumerate(er.SETUPS):
    n_sig = o10 = o20 = c10 = c20 = 0
    R = {"o10": [], "o20": [], "c10": [], "c20": [], "gap": []}
    for tk, g in grp.items():
        if col not in g:
            continue
        m = g[col].fillna(False).to_numpy(bool)
        if not m.any():
            continue
        o = g["open"].to_numpy(float)
        c = g["close"].to_numpy(float)
        n = len(c)
        idx = np.where(m)[0]
        idx = idx[idx < n - 22]
        if not len(idx):
            continue
        eo = o[idx + 1]                       # what the book pays
        ec = c[idx + 1]                       # one bar later, no shared print
        good = np.isfinite(eo) & np.isfinite(ec) & (eo > 0) & (ec > 0)
        idx, eo, ec = idx[good], eo[good], ec[good]
        if not len(idx):
            continue
        R["gap"].append(eo / c[idx] - 1)
        R["o10"].append(c[idx + 10] / eo - 1)
        R["o20"].append(c[idx + 20] / eo - 1)
        R["c10"].append(c[idx + 11] / ec - 1)
        R["c20"].append(c[idx + 21] / ec - 1)
    if not R["o10"]:
        continue
    A = {k: np.concatenate(v) * 100 for k, v in R.items()}
    n = len(A["o10"])
    if n < 200:
        continue
    rows.append(dict(setup=name, n=n, gap=np.median(A["gap"]),
                     o10=np.median(A["o10"]), c10=np.median(A["c10"]),
                     o20=np.median(A["o20"]), c20=np.median(A["c20"]),
                     w_o10=(A["o10"] > 0).mean() * 100,
                     w_c10=(A["c10"] > 0).mean() * 100))
    if (i + 1) % 20 == 0:
        print(f"  ...{i + 1}/{len(er.SETUPS)}", flush=True)

D = pd.DataFrame(rows)
D["drop10"] = D.o10 - D.c10
D["kept10"] = np.where(D.o10 != 0, D.c10 / D.o10, np.nan)
D = D.sort_values("drop10", ascending=False)

print("\n" + "=" * 130, flush=True)
print("  ENTRY-PRICE SENSITIVITY — every setup, same trade dates, only the assumed fill "
      "changes", flush=True)
print("=" * 130, flush=True)
print(f"  {'setup':32s} {'n':>7s} {'gap':>7s} {'open 10b':>9s} {'close 10b':>10s} "
      f"{'drop':>7s} {'kept':>7s} {'win o':>6s} {'win c':>6s}", flush=True)
for _, r in D.iterrows():
    flag = "  🔴" if (r.o10 > 0 and r.c10 <= 0) else ("  ⚠" if r.kept10 < 0.5 else "")
    print(f"  {r.setup:32s} {r.n:>7,} {r.gap:>+7.3f} {r.o10:>+9.3f} {r.c10:>+10.3f} "
          f"{r.drop10:>+7.3f} {r.kept10:>7.1%} {r.w_o10:>6.2f} {r.w_c10:>6.2f}{flag}",
          flush=True)

print("\n" + "=" * 130, flush=True)
print("  SUMMARY", flush=True)
print("=" * 130, flush=True)
pos = D[D.o10 > 0]
died = pos[pos.c10 <= 0]
half = pos[(pos.c10 > 0) & (pos.kept10 < 0.5)]
print(f"    setups measured                       {len(D)}")
print(f"    positive on the book's open entry     {len(pos)}")
print(f"    → turn NEGATIVE on close[i+1] entry   {len(died)}  🔴")
print(f"    → keep less than half                 {len(half)}  ⚠")
print(f"    → keep half or more                   {len(pos) - len(died) - len(half)}  ✅")
print(f"\n    median gap across all setups: {D.gap.median():+.3f}%   "
      f"(setups fire before a gap {'DOWN' if D.gap.median() < 0 else 'UP'})")
print(f"    median kept: {pos.kept10.median():.1%}")

if len(D) > 3:
    rho = D[["gap", "drop10"]].corr(method="spearman").iloc[0, 1]
    print(f"\n    Spearman(next-open gap, how much is lost) = {rho:+.3f}")
    print("    A strongly NEGATIVE correlation is the fingerprint: the setups that gap down")
    print("    hardest are the ones that lose most when the opening print is removed.")
if len(died):
    print(f"\n    the ones that do not survive: {list(died.setup)}")
D.to_csv("book_entry_bias.csv", index=False)
print("\n  written: book_entry_bias.csv", flush=True)
print("\nDONE", flush=True)
