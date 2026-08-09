"""Is the T6/T4 gap effect a real signal, or the bid-ask bounce wearing its clothes?

The previous run printed SIGNAL and passed every gate — six positive years, intervals clear
of the control, +1.48% at ten bars net of costs. Two things in the same output argue against
believing it.

First, the shape. On ordinary bars with no token at all, a gap DOWN wins 73.25% of the next
session and a gap UP wins 26.18%. A mirror that clean is not a property of markets; it is the
signature of measuring close-to-open across a spread. When the prior close prints at the ask
and the next open prints at the bid, an observer sees a negative gap followed by a positive
day with no trade having moved at all (Blume & Stambaugh). Consistently, 134-157% of the whole
20-bar move is already there by bar 3, and the effect is largest in the cheapest names, where
the spread is widest as a percentage.

Second, what the control did not control. Bars were matched on price × liquidity × year, not
on GAP DEPTH, and "gap < −0.5%" is unbounded below. If T6 sits at a median gap of −1.5% while
the control sits at −0.8%, then the "token effect" is simply a deeper bounce.

So two tests, and the claim has to survive both:

  A. narrow gap bins — compare T6 against non-T6 bars whose gap is the same SIZE, not merely
     the same sign. Any effect that is bounce disappears here.
  B. a liquid universe — $5 and $3M floors, declared rather than inherited. Spread-driven
     effects shrink hard; real ones should not.

A third diagnostic comes free: if this is the spread, the 1-bar effect must dominate and the
later horizons must add nothing. Real information keeps paying past the first close.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from naked_study import NakedStudy

BINS = [(-0.5, -1.0), (-1.0, -2.0), (-2.0, -4.0), (-4.0, -100.0)]
LBL = ["-0.5…-1%", "-1…-2%", "-2…-4%", "< -4%"]


def build(min_price=None, min_dv=None, tag=""):
    st = NakedStudy(f"T6/T4 gap effect vs the bid-ask bounce {tag}",
                    n_trials=4, columns=("t_sig", "z_sig"), horizons=(1, 3, 10, 20),
                    min_price=min_price, min_dollar_vol=min_dv)
    d = st.df
    T = d["t_sig"].fillna("").astype(str).to_numpy()
    Z = d["z_sig"].fillna("").astype(str).to_numpy()
    tok = np.where((T != "") & (T != "nan"), T, Z)
    nxo = d.groupby("ticker", sort=False)["open"].shift(-1).to_numpy()
    gap = (nxo / d["close"].to_numpy() - 1) * 100
    d["gap"], d["tok"] = gap, tok
    return st, d, np.isfinite(gap)


def table(st, d, ok, title):
    """T6/T4 against non-token bars inside the SAME gap bin — the deciding comparison."""
    t6 = (d.tok == "T6").to_numpy() & ok
    t4 = (d.tok == "T4").to_numpy() & ok
    nei = ~(t6 | t4) & ok
    g = d["gap"].to_numpy()
    print("\n" + "=" * 122, flush=True)
    print(f"  {title}", flush=True)
    print("=" * 122, flush=True)
    print(f"  {'gap bin':>10s} {'who':>9s} {'n':>9s} {'gap med':>8s} "
          + " ".join(f"{f'{h}b':>8s}" for h in st.hor) + f" {'vs ctl 10b':>11s}", flush=True)
    rows = []
    for (hi, lo), lbl in zip(BINS, LBL):
        m = (g < hi) & (g >= lo)
        ref = None
        for who, msk in (("NEITHER", nei & m), ("T6", t6 & m), ("T4", t4 & m)):
            sub = d[msk]
            if len(sub) < 300:
                print(f"  {lbl:>10s} {who:>9s} {len(sub):>9,}   (thin)", flush=True)
                continue
            med = [sub[f"r{h}"].median() * 100 for h in st.hor]
            if who == "NEITHER":
                ref = med
            dv = med[st.hor.index(10)] - ref[st.hor.index(10)] if ref else 0.0
            print(f"  {lbl:>10s} {who:>9s} {len(sub):>9,} {sub.gap.median():>+8.2f} "
                  + " ".join(f"{x:>+8.3f}" for x in med)
                  + (f" {dv:>+11.3f}" if who != "NEITHER" else f" {'—':>11s}"), flush=True)
            rows.append(dict(bin=lbl, who=who, n=len(sub), gap=sub.gap.median(),
                             **{f"m{h}": med[i] for i, h in enumerate(st.hor)},
                             vs_ctl=dv))
        print(flush=True)
    return pd.DataFrame(rows)


# ── A · the full tape, narrow gap bins ───────────────────────────────────────
st, d, ok = build(tag="(all bars)")
A = table(st, d, ok, "TEST A — same gap SIZE, not merely the same sign · ALL BARS")

# ── B · the same thing on a liquid universe ─────────────────────────────────
st2, d2, ok2 = build(min_price=5.0, min_dv=3_000_000, tag="(liquid)")
B = table(st2, d2, ok2, "TEST B — the identical comparison on $5 / $3M names")

# ── the summary that decides it ─────────────────────────────────────────────
print("\n" + "=" * 122, flush=True)
print("  DOES THE TOKEN SURVIVE ITS OWN GAP?", flush=True)
print("=" * 122, flush=True)
for name, R in (("all bars", A), ("liquid $5/$3M", B)):
    tk = R[R.who != "NEITHER"]
    print(f"\n  {name}:", flush=True)
    print(f"    token effect at 10 bars, per gap bin: "
          + " · ".join(f"{r['bin']} {r.who} {r.vs_ctl:+.2f}" for _, r in tk.iterrows()),
          flush=True)
    print(f"    median token effect {tk.vs_ctl.median():+.3f}pp · "
          f"positive in {int((tk.vs_ctl > 0).sum())}/{len(tk)} bins", flush=True)

print("\n" + "=" * 122, flush=True)
print("  IS IT THE SPREAD? — how much of the 20-bar move is already at bar 1", flush=True)
print("=" * 122, flush=True)
for name, R in (("all bars", A), ("liquid $5/$3M", B)):
    n = R[R.who == "NEITHER"]
    print(f"  {name} · NEITHER (pure gap, no token):", flush=True)
    for _, r in n.iterrows():
        share = r.m1 / r.m20 if r.m20 else np.nan
        print(f"      {r['bin']:>9s} gap {r.gap:>+6.2f}  1b {r.m1:>+6.3f}  20b {r.m20:>+6.3f}"
              f"   bar-1 share {share:>6.0%}"
              f"{'   ← all of it on day one' if share > 0.9 else ''}", flush=True)
A.to_csv("t6t4_bounce_all.csv", index=False)
B.to_csv("t6t4_bounce_liquid.csv", index=False)
print("\n  written: t6t4_bounce_all.csv · t6t4_bounce_liquid.csv", flush=True)
print("\nDONE", flush=True)
