"""When does a trade fail, not just whether — competing risks on the mark-to-market grid.

Ninety per cent of our trades close on the 60-bar timer. In survival terms that is very heavy
right-censoring, and it is exactly the regime where comparing medians throws away the most:
two groups can end at the same median while one of them spends its first week under water and
the other does not. "Dilution does not change the chance of reaching target but sharply raises
the hazard of an early loss" is an actionable sentence; "median lower by 0.4%" is not.

The book trails rather than using a fixed stop and target, so the textbook STOP / TARGET /
TIMEOUT triple does not exist in our data. What does exist is the mtm grid: mtm_1 … mtm_60,
the value of the position at each horizon with a realised stop carried forward. From it the
two competing events can be reconstructed honestly:

    LOSS   the first grid point at which the position is down more than L
    GAIN   the first grid point at which it is up more than G
    censored at 60 bars if neither happens

These compete in the technical sense: reaching GAIN first does not merely censor the loss, it
changes what could have happened afterwards, so the right quantity is the CAUSE-SPECIFIC
cumulative incidence of each, not a Kaplan-Meier curve for one of them with the other treated
as censoring — that would overstate the loss risk by pretending the winners were still exposed.

Grid resolution is the honest limitation: events are located to the nearest of thirteen points,
so "bar 3" means "between bars 2 and 3". Fine for asking WHEN risk concentrates, too coarse
for anything that needs the exact bar.

PRE-REGISTERED, before looking (ledger, hypothesis_family=risk):
    target    P(LOSS before GAIN within 60 bars), L = 10%, G = 10%
    exposure  entry price in $8-21 vs $21-89
    claim     the exposed group's loss hazard is concentrated EARLY, not merely higher
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio_gates import Integrity, risk_stats, risk_verdict, BACKTEST  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GRID = np.array([1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60])
MTM = [f"mtm_{g}" for g in GRID]
LOSS, GAIN = -10.0, 10.0
pd.set_option("display.width", 200)

O = pd.read_parquet(os.path.join(ROOT, "data", "opportunities.parquet"))
O = O.dropna(subset=["mtm_60", "sig_close"]).reset_index(drop=True)
O = O.drop_duplicates("dup_group").reset_index(drop=True)      # one row per real trade
M = O[MTM].to_numpy(float) * 100
print(f"  {len(O):,} deduplicated trades · grid {list(GRID)}", flush=True)

# first grid index at which each event occurs; len(GRID) means "never"
hit_loss = np.where(M <= LOSS, np.arange(len(GRID)), len(GRID)).min(axis=1)
hit_gain = np.where(M >= GAIN, np.arange(len(GRID)), len(GRID)).min(axis=1)
event = np.where(hit_loss < hit_gain, 1, np.where(hit_gain < hit_loss, 2, 0))
tidx = np.minimum(hit_loss, hit_gain)
tidx = np.where(event == 0, len(GRID) - 1, tidx)
O["event"], O["tidx"] = event, tidx

band = pd.cut(O["sig_close"].astype(float), [0, 8, 21, 89, 1e9],
              labels=["<$8", "$8-21", "$21-89", ">$89"])
O["band"] = band.astype(str)
sub = O[O.band.isin(["$8-21", "$21-89"])].copy()
exp = (sub.band == "$8-21").to_numpy()
print(f"  exposed $8-21 {exp.sum():,} · control $21-89 {(~exp).sum():,}", flush=True)
print(f"  censored at 60 bars (neither ±10% reached): {(sub.event == 0).mean():.1%}",
      flush=True)


def cif(mask):
    """Cause-specific cumulative incidence — the share that has met each event by bar g.

    Not 1 − KM for one cause: treating the winners as censored would leave them 'at risk' of
    a loss they can no longer have, and inflate the very number this is meant to measure.
    """
    d = sub[mask]
    n = len(d)
    out = []
    for j in range(len(GRID)):
        out.append((GRID[j], (d.event.eq(1) & d.tidx.le(j)).sum() / n * 100,
                    (d.event.eq(2) & d.tidx.le(j)).sum() / n * 100))
    return pd.DataFrame(out, columns=["bar", "loss_pct", "gain_pct"])


A, B = cif(exp), cif(~exp)
print("\n" + "=" * 118, flush=True)
print(f"  CUMULATIVE INCIDENCE — reaching {LOSS:+.0f}% or {GAIN:+.0f}% first", flush=True)
print("=" * 118, flush=True)
print(f"  {'bar':>5s} | {'$8-21 loss':>11s} {'gain':>7s} | {'$21-89 loss':>12s} "
      f"{'gain':>7s} | {'Δloss':>7s} {'ratio':>6s}", flush=True)
for j in range(len(GRID)):
    a, b = A.iloc[j], B.iloc[j]
    rr = a.loss_pct / b.loss_pct if b.loss_pct else np.nan
    print(f"  {int(a.bar):>5d} | {a.loss_pct:>11.2f} {a.gain_pct:>7.2f} | "
          f"{b.loss_pct:>12.2f} {b.gain_pct:>7.2f} | {a.loss_pct-b.loss_pct:>+7.2f} "
          f"{rr:>6.2f}", flush=True)

print("\n" + "=" * 118, flush=True)
print("  WHERE THE RISK SITS — share of the whole 60-bar loss incidence already realised",
      flush=True)
print("=" * 118, flush=True)
for lbl, C in (("$8-21", A), ("$21-89", B)):
    tot = C.loss_pct.iloc[-1]
    by3 = C.loss_pct.iloc[2] / tot * 100
    by10 = C.loss_pct[C.bar == 10].iloc[0] / tot * 100
    print(f"    {lbl:8s} by bar 3: {by3:>5.1f}%   by bar 10: {by10:>5.1f}%   "
          f"total {tot:.2f}%", flush=True)
early = (A.loss_pct.iloc[2] / A.loss_pct.iloc[-1]) - (B.loss_pct.iloc[2] / B.loss_pct.iloc[-1])
print(f"\n    difference in EARLY concentration (by bar 3): {early*100:+.1f}pp", flush=True)
print("    → the pre-registered claim was that the exposed group fails EARLIER, not just "
      "more often", flush=True)

# ── discrete-time hazard ratio, interval by interval ────────────────────────
print("\n" + "=" * 118, flush=True)
print("  CAUSE-SPECIFIC HAZARD OF LOSS, per interval  (of those still at risk)", flush=True)
print("=" * 118, flush=True)
print(f"  {'interval':>10s} {'$8-21':>9s} {'$21-89':>9s} {'HR':>6s} {'at risk (exp)':>14s}",
      flush=True)
rows = []
for j in range(len(GRID)):
    h = []
    for m in (exp, ~exp):
        d = sub[m]
        at_risk = ((d.tidx >= j) | (d.event == 0)).sum() if j else len(d)
        at_risk = (d.tidx >= j).sum()
        ev = ((d.event == 1) & (d.tidx == j)).sum()
        h.append(ev / at_risk if at_risk else np.nan)
    hr = h[0] / h[1] if h[1] else np.nan
    lab = f"{GRID[j-1] if j else 0}-{GRID[j]}"
    print(f"  {lab:>10s} {h[0]*100:>8.2f}% {h[1]*100:>8.2f}% {hr:>6.2f} "
          f"{(sub[exp].tidx >= j).sum():>14,}", flush=True)
    rows.append(dict(interval=lab, h_exp=h[0], h_ctl=h[1], hr=hr))
pd.DataFrame(rows).to_csv("hazard_competing.csv", index=False)

# ── the verdict, on the pre-registered target ───────────────────────────────
bad = (sub.event == 1).to_numpy()
dates = sub.date_in.astype(str).str[:10].to_numpy()
s = risk_stats(bad, dates, exp)
yrs = pd.to_datetime(sub.date_in.astype(str).str[:10]).dt.year
per = [bad[exp & (yrs == y).to_numpy()].mean() - bad[~exp & (yrs == y).to_numpy()].mean()
       for y in sorted(yrs.unique())]
r = sub.ret.astype(float).to_numpy() * 100
rng = np.random.default_rng(0)
uq, gi = np.unique(dates, return_inverse=True)
diffs = []
for _ in range(600):
    w = rng.multinomial(len(uq), np.full(len(uq), 1 / len(uq)))[gi]
    a, b = np.repeat(r[exp], w[exp]), np.repeat(r[~exp], w[~exp])
    if len(a) > 50 and len(b) > 50:
        diffs.append(np.median(a) - np.median(b))
lo, hi = np.percentile(diffs, [2.5, 97.5])
risk_verdict(label=f"$8-21 → P(loss {LOSS:+.0f}% before gain {GAIN:+.0f}%) within 60 bars",
             stats=s, integrity=Integrity(), per_period=per,
             return_effect=np.median(r[exp]) - np.median(r[~exp]), return_ci=(lo, hi),
             evidence=BACKTEST, human_checked=True)
print("\nDONE", flush=True)
