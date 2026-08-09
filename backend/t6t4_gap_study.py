"""T6 and T4 on a tradeable entry: is it the signal, or is it the gap it sits on?

The _TZ replication left an odd residue. Scored close-to-close, T6 and T4 are the two worst
signals in the packages; scored from the next open — the first price anyone can actually
transact — they win 55.8% and 55.9% of the time on 183K and 411K bars, and beat a matched
control by +0.82pp and +0.91pp of median at ten bars.

But those are also the two signals that gap DOWN hardest overnight: −0.84% and −0.82% median.
So there is an obvious rival explanation, and it has to be killed before anything else is
worth measuring: buying after any sharp overnight gap down may pay the same, in which case
T6 is a gap detector and the token adds nothing.

Two tests settle it, and they are the first two things this script does:

  · the same gap band WITHOUT the token — if non-T6/T4 bars that gapped down equally do just
    as well, the signal is a proxy
  · the token WITHOUT the gap (gap ≥ 0) — if the edge vanishes there, it was never the token

Then the shape over 1/3/5/10/20 bars, which separates the two stories on its own: overnight
gap reversion is spent within a couple of bars, a real drift is not.

Naked throughout — no exit rule, no gate, no book column — with controls matched on
price × liquidity × year. Costs are checked against the median at the end, because a win rate
is not a profit.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from naked_study import NakedStudy

st = NakedStudy("T6/T4 on a tradeable entry — the signal, or the gap it sits on?",
                n_trials=14, columns=("t_sig", "z_sig"), horizons=(1, 3, 5, 10, 20))
d = st.df
T = d["t_sig"].fillna("").astype(str).to_numpy()
Z = d["z_sig"].fillna("").astype(str).to_numpy()
tok = np.where((T != "") & (T != "nan"), T, Z)

# overnight gap = the open we enter at, against the close that defined the signal.
# st.df drops only the trailing rows of each ticker, so the rows that remain are still
# contiguous and a within-ticker shift is the true next bar.
nxo = d.groupby("ticker", sort=False)["open"].shift(-1).to_numpy()   # NaN at each boundary
gap = (nxo / d["close"].to_numpy() - 1) * 100
d["gap"] = gap
ok = np.isfinite(gap)

t6, t4 = (tok == "T6") & ok, (tok == "T4") & ok
either = t6 | t4
print(f"\n  T6 {t6.sum():,} bars · T4 {t4.sum():,} · gap median "
      f"T6 {np.nanmedian(gap[t6]):+.3f}% · T4 {np.nanmedian(gap[t4]):+.3f}% · "
      f"all bars {np.nanmedian(gap[ok]):+.3f}%", flush=True)

BAND = -0.5          # "gapped down hard" — the band T6/T4 mostly live in
st.population(n_boot=300)

print("\n" + "─" * 122, flush=True)
print("  TEST 1 — the same gap band WITHOUT the token.  If these match, T6 is a gap proxy.",
      flush=True)
print("─" * 122, flush=True)
r_t6g = st.signal(f"T6 · gap < {BAND}%", t6 & (gap < BAND), n_boot=400)
r_t4g = st.signal(f"T4 · gap < {BAND}%", t4 & (gap < BAND), n_boot=400)
r_ctl = st.signal(f"NEITHER · gap < {BAND}%   ← the control", ~either & ok & (gap < BAND),
                  n_boot=400)

print("\n" + "─" * 122, flush=True)
print("  TEST 2 — the token WITHOUT the gap.  If the edge dies here, it was never the token.",
      flush=True)
print("─" * 122, flush=True)
r_t6f = st.signal("T6 · gap ≥ 0", t6 & (gap >= 0), n_boot=400)
r_t4f = st.signal("T4 · gap ≥ 0", t4 & (gap >= 0), n_boot=400)
r_ctlf = st.signal("NEITHER · gap ≥ 0   ← the control", ~either & ok & (gap >= 0),
                   n_boot=400)

print("\n" + "─" * 122, flush=True)
print("  the raw signals, for reference", flush=True)
print("─" * 122, flush=True)
r_t6 = st.signal("T6 (all)", t6, n_boot=400)
r_t4 = st.signal("T4 (all)", t4, n_boot=400)

# ── the two comparisons that decide it ───────────────────────────────────────
print("\n" + "=" * 122, flush=True)
print("  THE DECIDING TABLE — token effect at equal gap, and gap effect at equal token",
      flush=True)
print("=" * 122, flush=True)
print(f"  {'':34s} " + " ".join(f"{f'{h}b':>8s}" for h in st.hor), flush=True)


def line(lbl, res, ref=None, field="med"):
    v = [getattr(res[h], field) - (getattr(ref[h], field) if ref is not None else 0.0)
         for h in st.hor]
    print(f"  {lbl:34s} " + " ".join(f"{x:>+8.3f}" for x in v), flush=True)


print("  median return, %:")
line("T6 · gap<-0.5", r_t6g)
line("T4 · gap<-0.5", r_t4g)
line("NEITHER · gap<-0.5  (control)", r_ctl)
line("→ TOKEN EFFECT  T6 − control", r_t6g, r_ctl)
line("→ TOKEN EFFECT  T4 − control", r_t4g, r_ctl)
print()
line("T6 · gap≥0", r_t6f)
line("NEITHER · gap≥0  (control)", r_ctlf)
line("→ TOKEN EFFECT without a gap", r_t6f, r_ctlf)
print("\n  win rate, %:")
for lbl, r in (("T6 · gap<-0.5", r_t6g), ("NEITHER · gap<-0.5", r_ctl),
               ("T6 · gap≥0", r_t6f), ("NEITHER · gap≥0", r_ctlf)):
    print(f"  {lbl:34s} " + " ".join(f"{r[h].up * 100:>8.2f}" for h in st.hor), flush=True)

# ── shape: reversion dies fast, drift does not ──────────────────────────────
print("\n" + "=" * 122, flush=True)
print("  SHAPE — how much of the 20-bar move is already there by bar 3?", flush=True)
print("=" * 122, flush=True)
for lbl, r in (("T6 · gap<-0.5", r_t6g), ("T4 · gap<-0.5", r_t4g),
               ("NEITHER · gap<-0.5", r_ctl), ("T6 (all)", r_t6)):
    m = {h: r[h].med for h in st.hor}
    frac = m[3] / m[20] if m[20] else np.nan
    print(f"  {lbl:24s} 1b {m[1]:>+6.3f} · 3b {m[3]:>+6.3f} · 20b {m[20]:>+6.3f}   "
          f"3-bar share of 20-bar: {frac:>6.1%}"
          f"{'   ← spent early = reversion' if frac > 0.6 else ''}", flush=True)

# ── stability, descriptively ────────────────────────────────────────────────
print("\n" + "=" * 122, flush=True)
print("  STABILITY of the token effect at 10 bars (T6 · gap<-0.5 minus control)", flush=True)
print("=" * 122, flush=True)
A, B = d[t6 & (gap < BAND)], d[~either & ok & (gap < BAND)]
for name, key in (("year", "yr"), ):
    ya = A.groupby(key)["r10"].median() * 100
    yb = B.groupby(key)["r10"].median() * 100
    j = pd.concat([ya.rename("T6"), yb.rename("ctl")], axis=1).dropna()
    j["diff"] = j.T6 - j.ctl
    print(f"  {'year':>6s} {'T6':>8s} {'control':>9s} {'diff':>8s} {'n':>8s}")
    for y, r in j.iterrows():
        print(f"  {int(y):>6d} {r.T6:>+8.3f} {r.ctl:>+9.3f} {r['diff']:>+8.3f} "
              f"{int((A.yr == y).sum()):>8,}")
    print(f"  years with a positive token effect: {int((j['diff'] > 0).sum())}/{len(j)}")

pb = pd.cut(A["close"], [0, 8, 21, 89, 377, 1e9], labels=["<$8", "$8-21", "$21-89",
                                                          "$89-377", ">$377"])
pbb = pd.cut(B["close"], [0, 8, 21, 89, 377, 1e9], labels=["<$8", "$8-21", "$21-89",
                                                           "$89-377", ">$377"])
print(f"\n  {'price':>9s} {'T6':>8s} {'control':>9s} {'diff':>8s} {'n':>9s}")
ga = (A.groupby(pb, observed=True)["r10"].median() * 100)
gb = (B.groupby(pbb, observed=True)["r10"].median() * 100)
for k in ga.index:
    if k in gb.index:
        print(f"  {str(k):>9s} {ga[k]:>+8.3f} {gb[k]:>+9.3f} {ga[k] - gb[k]:>+8.3f} "
              f"{int((pb == k).sum()):>9,}")

# ── does it survive costs ───────────────────────────────────────────────────
print("\n" + "=" * 122, flush=True)
print("  COSTS — a win rate is not a profit", flush=True)
print("=" * 122, flush=True)
for lbl, r in (("T6 · gap<-0.5", r_t6g), ("T4 · gap<-0.5", r_t4g)):
    for h in (3, 10, 20):
        net = r[h].med - 0.15                      # round trip, spread + slippage
        print(f"  {lbl:16s} {h:>2d}b  median {r[h].med:>+6.3f}%  "
              f"CI [{r[h].lo:>+6.3f},{r[h].hi:>+6.3f}]  net of 0.15% costs {net:>+6.3f}%  "
              f"{'✅' if net > 0 else '❌ eaten by costs'}", flush=True)

st.verdict(r_t6g, "T6 · gap<-0.5", N=10, family="T6-gapdown")
print("\nDONE", flush=True)
