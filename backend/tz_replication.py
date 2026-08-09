"""Independent replication of the _TZ ANALYTICS findings on our own bars.

The packages are external data — a different vendor, a different universe definition, a
different measurement window. Four claims survived their internal audit, and every one of
them is a prediction about the market rather than about that dataset, so our own five years
can test them without asking the same question twice.

The claims, written down before the run, exactly as the audit left them:

  1. Z2G and Z4 are the only signals positive in all nine of their measurements
     (win-rate lift ≈ +1.24pp and +1.19pp over the average signal).
  2. T6, T4, T11, T10, T2, T2G are negative in all nine, and by a wider margin than the
     positives are positive — the useful half of the finding is the veto.
  3. Raw Z7 is poor, but Z7 with an L5 or L46 volume line and an ED suffix is the strongest
     thing in the packages: +5.3 to +8.2pp win rate over Z7 itself, on three universes and
     three timeframes.
  4. The Z family beats the T family everywhere (Mann-Whitney p ≈ 1e-15).

Thirteen cells, registered up front. Measured naked — no gate, no exit rule, no book column
— against a control matched on price × liquidity × year, because a signal that only appears
on expensive liquid names would otherwise be credited for its habitat.

The comparison that matters is the SIGN and the ORDER, not the exact number: the packages
measured a different universe over a different window, so agreeing to two decimal places
would be more suspicious than agreeing in direction.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from naked_study import NakedStudy

CLAIM = {                      # written before the run — package win-rate lift, pp
    "Z2G": +1.24, "Z4": +1.19,
    "T2G": -0.37, "T2": -1.17, "T10": -1.57, "T4": -1.56, "T11": -2.07, "T6": -1.97,
}

st = NakedStudy("do the _TZ package findings replicate on our own bars?",
                n_trials=16, columns=("t_sig", "z_sig", "l_sig", "full_suffix"),
                horizons=(5, 10, 20))
d = st.df
T = d["t_sig"].fillna("").astype(str)
Z = d["z_sig"].fillna("").astype(str)
L = d["l_sig"].fillna("").astype(str)
X = d["full_suffix"].fillna("").astype(str)
tok = np.where(T.ne("") & T.ne("nan"), T, Z)

print(f"\n  token coverage: {(tok != '').mean():.1%} of bars carry a T/Z token", flush=True)
st.population(n_boot=300)

out = {}
print("\n" + "─" * 122, flush=True)
print("  CLAIM 1 + 2 — the eight signals with perfect sign agreement in the packages",
      flush=True)
print("─" * 122, flush=True)
for s in ("Z2G", "Z4", "T2G", "T2", "T10", "T4", "T11", "T6"):
    out[s] = st.signal(s, tok == s, n_boot=400)

print("\n" + "─" * 122, flush=True)
print("  CLAIM 3 — raw Z7 is poor, Z7 + L-line + ED suffix is the packages' best", flush=True)
print("─" * 122, flush=True)
z7 = tok == "Z7"
out["Z7"] = st.signal("Z7 (raw)", z7, n_boot=400)
out["Z7L5ED"] = st.signal("Z7 + L5 + ED", z7 & (L == "L5") & (X == "ED"), n_boot=400)
out["Z7L46ED"] = st.signal("Z7 + L46 + ED", z7 & (L == "L46") & (X == "ED"), n_boot=400)

print("\n" + "─" * 122, flush=True)
print("  CLAIM 4 — the Z family against the T family", flush=True)
print("─" * 122, flush=True)
isT = pd.Series(tok, index=d.index).str.startswith("T") & (pd.Series(tok, index=d.index) != "")
isZ = pd.Series(tok, index=d.index).str.startswith("Z")
out["T*"] = st.signal("ALL T-signals", isT.to_numpy(), n_boot=400)
out["Z*"] = st.signal("ALL Z-signals", isZ.to_numpy(), n_boot=400)

# ── scorecard ────────────────────────────────────────────────────────────────
N = 10                                    # the packages' horizon
LBL = {"Z2G": "Z2G", "Z4": "Z4", "T2G": "T2G", "T2": "T2", "T10": "T10", "T4": "T4",
       "T11": "T11", "T6": "T6", "Z7": "Z7 (raw)", "Z7L5ED": "Z7 + L5 + ED",
       "Z7L46ED": "Z7 + L46 + ED", "T*": "ALL T-signals", "Z*": "ALL Z-signals"}
print("\n" + "=" * 122, flush=True)
print(f"  SCORECARD at {N} bars — every claim was written down before this ran", flush=True)
print("=" * 122, flush=True)
print(f"  {'signal':>14s} {'package':>9s} {'OURS Δ↑':>9s} {'Δmed':>8s} {'n':>9s} "
      f"{'n_eff':>8s} {'CI':>17s}  sign  verdict", flush=True)
rows, ok, tot = [], 0, 0
for s_, claim in CLAIM.items():
    r, b = out[s_][N], st.ctl_all[LBL[s_]][N]
    dup, dmed = (r.up - b.up) * 100, r.med - b.med
    same = np.sign(dup) == np.sign(claim)
    sep = (r.lo > b.hi) or (r.hi < b.lo)
    ok += bool(same); tot += 1
    print(f"  {s_:>14s} {claim:>+9.2f} {dup:>+9.2f} {dmed:>+8.3f} {r.n:>9,} {r.n_eff:>8,} "
          f"[{r.lo:>+6.2f},{r.hi:>+6.2f}] {'  ✅' if same else '  ❌'}  "
          f"{'REPLICATES' if (same and sep) else ('same sign, weak' if same else 'FAILS')}",
          flush=True)
    rows.append(dict(signal=s_, claim=claim, ours_up=dup, ours_med=dmed, n=r.n,
                     n_eff=r.n_eff, lo=r.lo, hi=r.hi, separate=sep, same_sign=same))
print(f"\n  sign agreement: {ok}/{tot}   "
      f"(coin flips would give {tot/2:.1f}; p = {0.5**tot * 2:.4f} for all-{tot})",
      flush=True)

print("\n  CLAIM 3 — the conditional effect:", flush=True)
z7b = st.ctl_all["Z7 (raw)"][N]
z7r = out["Z7"][N]
base_up = (z7r.up - z7b.up) * 100
for k, lab in (("Z7L5ED", "Z7 + L5 + ED"), ("Z7L46ED", "Z7 + L46 + ED")):
    r, b = out[k][N], st.ctl_all[lab][N]
    dup = (r.up - b.up) * 100
    print(f"    {lab:>16s}  Δ↑ vs matched {dup:>+6.2f}pp · vs RAW Z7 {dup - base_up:>+6.2f}pp"
          f"  (package said +5.3…+8.2 over raw Z7)   n={r.n:,}", flush=True)
print(f"    {'Z7 raw':>16s}  Δ↑ vs matched {base_up:>+6.2f}pp   n={z7r.n:,}", flush=True)

print("\n  CLAIM 4 — the family law:", flush=True)
tr, tb = out["T*"][N], st.ctl_all["ALL T-signals"][N]
zr, zb = out["Z*"][N], st.ctl_all["ALL Z-signals"][N]
tl, zl = (tr.up - tb.up) * 100, (zr.up - zb.up) * 100
print(f"    T-family Δ↑ {tl:>+6.2f}pp (n={tr.n:,}) · Z-family Δ↑ {zl:>+6.2f}pp (n={zr.n:,})"
      f" · gap {zl - tl:>+6.2f}pp   (package: +0.50 … +1.95pp)", flush=True)

pd.DataFrame(rows).to_csv("tz_replication.csv", index=False)
print("\n  written: tz_replication.csv", flush=True)
print("\nDONE", flush=True)
