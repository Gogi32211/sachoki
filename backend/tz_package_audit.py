"""Audit of the _TZ ANALYTICS 5YR packages (1D · 4H · 1H) under the new standard.

The three packages are the same 51-file analysis run on three timeframes, which makes them
a natural experiment the reports themselves never exploit: every rule was measured three
independent times. A real effect echoes across timeframes; a mined one does not.

Four questions, in the order that decides them fastest:

  1. k — how many cells were actually scored. No report in the package states this, and it
     is the number every grade in them depends on.

  2. The funnel. If the spread of rule medians is exactly what sampling noise produces at
     each n, then the differences BETWEEN rules are not real. Var(observed) = Var(true) +
     σ²/n, so fitting dispersion against 1/√n recovers Var(true) directly — no priors, no
     model of the market, just arithmetic on what is already in the CSVs.

  3. Replication. P(GOOD on another timeframe | GOOD on 1D) against the base rate of GOOD.
     A lift of 1.0 means the grade carries no information about the next measurement.

  4. Shrinkage. What the 1D winners are actually worth when measured again elsewhere.

Nothing here needs the database or any book column; the packages are self-contained.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

pd.set_option("display.width", 200)

S = ("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
     "aba6fbf4-ff3b-4f32-9bd8-48f188d02d96/scratchpad/tz")
PKG = {"1D": f"{S}/_TZ_ANALYTICS_5YR/_TZ ANALYTICS 5YR",
       "4H": f"{S}/_TZ_ANALYTICS_5YR_4H/_TZ ANALYTICS 5YR 4H",
       "1H": f"{S}/_TZ_ANALYTICS_5YR_1H/_TZ ANALYTICS 5YR 1H"}
KEY = ["universe", "signal", "seq3"]


def load(tf, name):
    p = os.path.join(PKG[tf], name)
    return pd.read_csv(p) if os.path.exists(p) else None


SEQ = {tf: load(tf, "rule_database_sequences_5yr.csv") for tf in PKG}
CMP = {tf: load(tf, "rule_database_composites_5yr.csv") for tf in PKG}
BAS = {tf: load(tf, "rich_baseline_all.csv") for tf in PKG}

print("=" * 118)
print("  1 · THE TRIAL COUNT — what every grade in these reports is conditioned on")
print("=" * 118)
tot = 0
for tf in PKG:
    cells = {}
    for f in os.listdir(PKG[tf]):
        if f.endswith(".csv"):
            n = len(pd.read_csv(os.path.join(PKG[tf], f)))
            cells[f] = n
    sub = sum(cells.values())
    tot += sub
    top = sorted(cells.items(), key=lambda x: -x[1])[:4]
    print(f"    {tf}: {sub:>8,} scored cells   ({', '.join(f'{k[:34]} {v:,}' for k, v in top)})")
print(f"\n    TOTAL ACROSS THE THREE PACKAGES: {tot:,} cells")
print(f"    Under a true null, cells landing in the best 1% by chance alone: "
      f"{tot // 100:,}")
print("    Not one of the reports states a trial count, so none of their grades can be")
print("    read as evidence without redoing the arithmetic below.")

# ── 2 · the funnel ──────────────────────────────────────────────────────────
print("\n" + "=" * 118)
print("  2 · THE FUNNEL — is the spread between rules real, or is it 1/√n ?")
print("=" * 118)
for tf in PKG:
    s = SEQ[tf].dropna(subset=["m10", "n"])
    s = s[s.n >= 20]
    q = pd.qcut(s.n, 8, duplicates="drop")
    g = s.groupby(q, observed=True).agg(n_med=("n", "median"), sd=("m10", "std"),
                                        cnt=("m10", "size"), mean=("m10", "mean"))
    g["inv_sqrt_n"] = 1 / np.sqrt(g.n_med)
    # Var(obs) = Var(true) + sigma^2/n  →  regress Var on 1/n; intercept = Var(true)
    x, y = 1 / g.n_med.to_numpy(), (g.sd ** 2).to_numpy()
    A = np.vstack([x, np.ones_like(x)]).T
    sig2, var_true = np.linalg.lstsq(A, y, rcond=None)[0]
    sd_true = np.sqrt(max(var_true, 0.0))
    print(f"\n    {tf}   (n≥20, {len(s):,} rules)")
    print(f"      {'n (median)':>11s} {'rules':>7s} {'SD of m10':>10s} {'predicted by noise':>19s}")
    for _, r in g.iterrows():
        pred = np.sqrt(max(sig2, 0) / r.n_med)
        print(f"      {r.n_med:>11,.0f} {r.cnt:>7,.0f} {r.sd:>10.3f} {pred:>19.3f}")
    print(f"      per-trade σ ≈ {np.sqrt(max(sig2, 0)):.2f}pp · "
          f"TRUE between-rule SD ≈ {sd_true:.3f}pp "
          f"({'≈ 0 — the ranking is noise' if sd_true < 0.25 else 'some real spread'})")
    print(f"      → of the observed SD at the median rule (n={s.n.median():.0f}), "
          f"{100 * (1 - sd_true / s.m10.std()):.1f}% is sampling noise")

# ── 3 · replication across timeframes ───────────────────────────────────────
print("\n" + "=" * 118)
print("  3 · REPLICATION — a grade earned on 1D, re-measured on 4H and 1H")
print("=" * 118)
base = SEQ["1D"][KEY + ["n", "m10", "win", "status", "regime"]].copy()
for tf in ("4H", "1H"):
    o = SEQ[tf][KEY + ["n", "m10", "win", "status"]].rename(
        columns={"n": f"n_{tf}", "m10": f"m10_{tf}", "win": f"win_{tf}",
                 "status": f"st_{tf}"})
    base = base.merge(o, on=KEY, how="left")
ov = base.dropna(subset=["m10_4H", "m10_1H"])
print(f"    rules present on all three timeframes: {len(ov):,} of {len(base):,}")
for tf in ("4H", "1H"):
    br = (SEQ[tf].status == "GOOD").mean()
    good1d = ov[ov.status == "GOOD"]
    rep = (good1d[f"st_{tf}"] == "GOOD").mean()
    rej = (good1d[f"st_{tf}"] == "REJECT").mean()
    print(f"\n    {tf}:  base rate of GOOD = {br:.2%}")
    print(f"        P(GOOD on {tf} | GOOD on 1D) = {rep:.2%}   "
          f"LIFT ×{rep / br if br else 0:.2f}"
          f"{'   ← no information' if abs(rep / br - 1) < 0.15 else ''}")
    print(f"        P(REJECT on {tf} | GOOD on 1D) = {rej:.2%}")
    pos = (good1d[f"m10_{tf}"] > 0).mean()
    print(f"        sign holds (m10 > 0 on {tf}): {pos:.2%}   (coin-flip = 50%)")

print("\n    the same test in the other direction, as a control:")
for src, dst in (("4H", "1D"), ("1H", "1D")):
    s = SEQ[src][KEY + ["status"]].rename(columns={"status": "st_src"})
    m = SEQ["1D"][KEY + ["status"]].merge(s, on=KEY).dropna()
    br = (m.status == "GOOD").mean()
    rep = (m[m.st_src == "GOOD"].status == "GOOD").mean()
    print(f"      GOOD on {src} → GOOD on {dst}: {rep:.2%} vs base {br:.2%}  "
          f"LIFT ×{rep / br if br else 0:.2f}")

# ── 4 · shrinkage ───────────────────────────────────────────────────────────
print("\n" + "=" * 118)
print("  4 · SHRINKAGE — what the 1D winners are worth when measured again")
print("=" * 118)
ov = ov.copy()
ov["dec"] = pd.qcut(ov.m10, 10, labels=False, duplicates="drop")
g = ov.groupby("dec").agg(n=("m10", "size"), d1=("m10", "median"),
                          h4=("m10_4H", "median"), h1=("m10_1H", "median"))
print(f"    {'1D decile':>10s} {'rules':>7s} {'1D m10':>9s} {'4H m10':>9s} {'1H m10':>9s} "
      f"{'kept':>7s}")
for d, r in g.iterrows():
    kept = (r.h4 + r.h1) / 2 / r.d1 if r.d1 else np.nan
    print(f"    {int(d) + 1:>10d} {r.n:>7,.0f} {r.d1:>+9.3f} {r.h4:>+9.3f} {r.h1:>+9.3f} "
          f"{kept:>7.1%}")
top = ov[ov.dec == g.index.max()]
print(f"\n    top decile on 1D: {top.m10.median():+.3f}  →  "
      f"4H {top.m10_4H.median():+.3f} · 1H {top.m10_1H.median():+.3f}")
print(f"    correlation of m10 across timeframes: "
      f"1D↔4H {ov.m10.corr(ov.m10_4H, method='spearman'):+.4f} · "
      f"1D↔1H {ov.m10.corr(ov.m10_1H, method='spearman'):+.4f} · "
      f"4H↔1H {ov.m10_4H.corr(ov.m10_1H, method='spearman'):+.4f}")

# ── 5 · what the package already admits, and what survives everything ───────
print("\n" + "=" * 118)
print("  5 · WHAT SURVIVES")
print("=" * 118)
for tf in PKG:
    s = SEQ[tf]
    print(f"    {tf}: {len(s):,} rules · GOOD {int((s.status == 'GOOD').sum()):,} "
          f"({(s.status == 'GOOD').mean():.1%}) · the package's own regime flag calls "
          f"{int((s.regime == '2025-ARTIFACT').sum()):,} of them a 2025 artifact "
          f"({(s.regime == '2025-ARTIFACT').mean():.1%})")
surv = ov[(ov.status == "GOOD") & (ov.st_4H == "GOOD") & (ov.st_1H == "GOOD")]
exp = len(ov) * (SEQ["1D"].status == "GOOD").mean() * (SEQ["4H"].status == "GOOD").mean() \
    * (SEQ["1H"].status == "GOOD").mean()
print(f"\n    GOOD on ALL THREE timeframes: {len(surv):,} rules")
print(f"    expected by chance if the three gradings were independent: {exp:,.0f}")
print(f"    ratio observed/chance: ×{len(surv) / exp if exp else 0:.2f}")
if len(surv):
    sv = surv.assign(avg=(surv.m10 + surv.m10_4H + surv.m10_1H) / 3,
                     nmin=surv[["n", "n_4H", "n_1H"]].min(axis=1))
    sv = sv.sort_values("avg", ascending=False)
    print(f"\n    the {min(20, len(sv))} strongest triple-survivors "
          f"(ranked by the mean of the three, not by 1D):")
    print(f"      {'universe':>10s} {'sig':>5s} {'seq3':>16s} {'n1D':>6s} {'n4H':>6s} "
          f"{'n1H':>6s} {'m10 1D':>8s} {'4H':>7s} {'1H':>7s} {'mean':>7s}")
    for _, r in sv.head(20).iterrows():
        print(f"      {r.universe:>10s} {r.signal:>5s} {r.seq3:>16s} {r.n:>6,.0f} "
              f"{r.n_4H:>6,.0f} {r.n_1H:>6,.0f} {r.m10:>+8.3f} {r.m10_4H:>+7.3f} "
              f"{r.m10_1H:>+7.3f} {r.avg:>+7.3f}")
    sv.to_csv("tz_triple_survivors.csv", index=False)
    print(f"\n    written: tz_triple_survivors.csv ({len(sv):,} rows)")
print("\nDONE")
