"""Does the live book contain the same class of fault the user's chart exposed?

A static scan of every signal-building file found exactly two places that look forward, and
they are not equivalent:

  edge_replay.py:350  — divergence pivots use a reversed rolling window to confirm a pivot low,
    which is unavoidable (a pivot is only a pivot once the right side exists). The signal is
    then written at index i + R, R bars AFTER the pivot, i.e. at the first bar on which it
    could actually be known. That is correct and needs no change.

  edge_replay.py:1469 — `_near(mask, w)` is documented as "mask fired within ±w bars" and
    implements it literally, adding shift(-1)…shift(-w). So `E_dualrec_rs` is true at bar i
    when the CCI reclaim happens at bar i+1 or i+2 — data that does not exist yet. It feeds
    🔄DualReclaim🏆RS and 🔄DualReclaim deep, both in SETUPS and both carrying an EDGE chip.

This measures the size of that. The same setup is rebuilt with a CAUSAL window (the reclaim
must have already happened, bars i−w … i) and run through the book's own engine — same trade
dates, same ⚡ATR×12 exit — against the version in production. Whatever the causal version
gives is what the setup is actually worth.

Nothing is changed here. This only measures.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
import edge_replay as er                              # noqa: E402
from analysis_kit import bootstrap_ci_clustered      # noqa: E402

pd.set_option("display.width", 200)
grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp):,} tickers\n", flush=True)

# ── contract on the book's own frame ────────────────────────────────────────
allrows = sum(len(g) for g in grp.values())
dups = sum(len(g) - len(g[["ticker", "date"]].drop_duplicates()) if
           {"ticker", "date"} <= set(g.columns) else 0 for g in grp.values())
gaps = []
for g in list(grp.values())[:400]:
    d = pd.to_datetime(g["date"])
    gaps.append((d.diff().dt.days <= 4).mean())
print("=" * 118, flush=True)
print("  A · DATA CONTRACT ON THE BOOK'S OWN FRAME", flush=True)
print("=" * 118, flush=True)
print(f"    rows {allrows:,} · duplicate (ticker,date) {dups:,}"
      f"  {'✅ clean' if dups == 0 else '🔴 DUPLICATED'}", flush=True)
print(f"    calendar-adjacent rows (400-ticker sample): {np.mean(gaps):.2%}"
      f"   — the dv floor removes bars mid-history, so any setup using a bare .shift()"
      f" inherits this", flush=True)

# ── rebuild the dual-reclaim causally ───────────────────────────────────────
print("\n" + "=" * 118, flush=True)
print("  B · 🔄 DUAL RECLAIM — production (±w, sees the future) vs causal (past only)",
      flush=True)
print("=" * 118, flush=True)
for tk, g in grp.items():
    r14, c20 = g["rsi_14"], g.get("cci20", g.get("cci_20"))
    pr, pc = r14.shift(1), c20.shift(1)
    rx35 = (pr < 35) & (r14 >= 35)
    rx30 = (pr < 30) & (r14 >= 30)
    cx = (pc < -100) & (c20 >= -100)
    q = g["close"].between(21, 377)
    rs = g["rs_intact"].fillna(False).astype(bool)

    def near_causal(m, w):
        out = m.astype(float).copy()
        for k in range(1, w + 1):
            out = out + m.astype(float).shift(k).fillna(0)
        return out > 0

    def near_both(m, w):                      # what production does
        out = m.astype(float).copy()
        for k in range(1, w + 1):
            out = out + m.astype(float).shift(k).fillna(0) \
                      + m.astype(float).shift(-k).fillna(0)
        return out > 0

    g["_dr_prod"] = rx35 & near_both(cx, 2) & rs & q
    g["_dr_causal"] = rx35 & near_causal(cx, 2) & rs & q
    g["_drd_prod"] = rx30 & near_both(cx, 3) & rs & q
    g["_drd_causal"] = rx30 & near_causal(cx, 3) & rs & q


def run(col):
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 50:
        return None
    ym = tr.groupby("yr")["ret"].median() * 100
    d = pd.to_datetime(tr["date_in"]).dt.strftime("%Y-%m-%d")
    lo, hi = bootstrap_ci_clustered(tr["ret"] * 100, d, stat="median", n_boot=500)
    return dict(n=len(tr), med=tr["ret"].median() * 100, lo=lo, hi=hi,
                win=(tr["ret"] > 0).mean() * 100, worst=ym.min(),
                yrs=int((ym > 0).sum()), nyr=len(ym))


print(f"  {'setup':34s} {'n':>7s} {'median':>8s} {'CI(days)':>17s} {'win':>7s} "
      f"{'years':>7s} {'worst':>8s}", flush=True)
rows = []
for label, prod, caus in (("🔄DualReclaim🏆RS", "_dr_prod", "_dr_causal"),
                          ("🔄DualReclaim deep", "_drd_prod", "_drd_causal")):
    a, b = run(prod), run(caus)
    for tag, r in (("production (±w)", a), ("CAUSAL (past only)", b)):
        if r is None:
            print(f"  {label + ' · ' + tag:34s}  too thin", flush=True); continue
        print(f"  {(label + ' · ' + tag):34s} {r['n']:>7,} {r['med']:>+8.3f} "
              f"[{r['lo']:>+6.2f},{r['hi']:>+6.2f}] {r['win']:>6.2f}% "
              f"{r['yrs']}/{r['nyr']:<5d} {r['worst']:>+8.2f}", flush=True)
    if a and b:
        rows.append((label, a, b))
        print(f"  {'':34s} → Δ median {b['med'] - a['med']:>+7.3f}pp · "
              f"signals kept {b['n'] / a['n']:.1%} · Δ worst year "
              f"{b['worst'] - a['worst']:>+6.2f}pp", flush=True)
    print(flush=True)

print("=" * 118, flush=True)
print("  VERDICT", flush=True)
print("=" * 118, flush=True)
for label, a, b in rows:
    loss = a["med"] - b["med"]
    share = loss / a["med"] * 100 if a["med"] else np.nan
    verdict = ("✅ the lookahead contributes nothing measurable — safe to make causal"
               if abs(loss) < 0.25 else
               f"🔴 {share:.0f}% of the reported edge comes from bars that had not happened")
    print(f"    {label}: production {a['med']:+.3f} → causal {b['med']:+.3f}   {verdict}",
          flush=True)
print("\n  Nothing was changed. edge_replay.py:1469 still reads ±w in production.", flush=True)
print("\nDONE", flush=True)
