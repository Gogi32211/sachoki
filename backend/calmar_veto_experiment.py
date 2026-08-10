"""Does a veto select, or does it just trade less? Four arms on a daily equity curve.

The question behind this is whether our real edge is "what to buy" or "what to avoid". Our
most universal findings are all on the avoid side — the season gate kills 14 of 14 setups in
Dec-Mar, the sub-200 rally suppressor hurts every one of them era-independently, ⚡ATR×12
improved 45 of 49 worst years — and none of them raises a median, so our return gates score
them NULL.

But a veto can improve a portfolio for three different reasons, and only one of them is a
finding:

  A  baseline              every opportunity is eligible
  B  veto + FORCED CASH    a vetoed pick leaves the slot EMPTY for the day
  C  veto + replacement    a vetoed pick is replaced by the next candidate
  D  RANDOM veto           the same fraction removed at random, replacement on

C-vs-A measures selection. B-vs-A measures exposure. D is the control that neither of the
first two can do without: if throwing away the same number of trades at random helps just as
much, the veto found nothing — it just traded less and concentrated less. Yesterday's lesson
about weak nulls applies to portfolio construction too.

Note B is artificial here on purpose. With ~400 deduplicated opportunities a day and ten
slots we are never short of candidates, so a veto does not naturally leave a slot empty — it
changes WHICH name fills it. Forcing the cash is the only way to isolate the exposure effect.

Selection inside each day is RANDOM. That is deliberate: the veto is then the single decision
being tested, and any Calmar difference cannot be a ranking in disguise. It also means these
are not our portfolio's returns and are not meant to be.

Equity is marked daily from the mtm grid, so the drawdown includes open positions. A realised-
only curve would hide exactly the losses a drawdown is supposed to measure.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

from data_contract import assert_time_aligned, keys_of

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPP = os.path.join(ROOT, "data", "opportunities.parquet")
SLOTS, HOLD, BAR2CAL, SEEDS = 10, 60, 1.45, 24
GRID = np.array([1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60])
MTM = [f"mtm_{g}" for g in GRID]
pd.set_option("display.width", 200)

O = pd.read_parquet(OPP)
O["d"] = pd.to_datetime(O["date_in"].astype(str).str[:10])
O = O.dropna(subset=["mtm_60", "sig_close"]).reset_index(drop=True)
O["veto"] = O["sig_close"].astype(float).between(8, 21, inclusive="left")
MTM_ARR = O[MTM].to_numpy(float)
DUP = O["dup_group"].to_numpy()
VETO = O["veto"].to_numpy()
BY_DAY = {d: g.index.to_numpy() for d, g in O.groupby("d", sort=True)}
# DAYS must come FROM the dict, not from .unique(): the latter returns datetime64 while the
# groupby keys are Timestamps, and dict.get() then misses every single day in silence — the
# same dtype mismatch that broke the as-of join, and just as quiet.
DAYS = keys_of(BY_DAY)
assert_time_aligned(BY_DAY, DAYS, name="calendar loop")

print("=" * 122, flush=True)
print(f"  CALMAR EXPERIMENT · {SLOTS} slots · hold {HOLD} bars · random selection · "
      f"{SEEDS} seeds", flush=True)
print(f"  veto = entry price in $8-21   ({VETO.mean():.1%} of {len(O):,} opportunities)",
      flush=True)
print(f"  {len(DAYS):,} trading days · {O.groupby('d').dup_group.nunique().mean():.0f} "
      f"deduplicated opportunities per day", flush=True)
print("=" * 122, flush=True)


def mark(idx: int, bars: float) -> float:
    j = int(np.abs(GRID - max(1.0, bars)).argmin())
    v = MTM_ARR[idx, j]
    return float(v) if np.isfinite(v) else 0.0


def run(arm: str, seed: int) -> dict:
    """One pass over the calendar. Returns the daily equity curve and trade log."""
    rng = np.random.default_rng(seed)
    if arm == "D":                       # random veto, matched on rate, drawn once per run
        drop = rng.random(len(O)) < VETO.mean()
    open_pos: list[dict] = []
    equity, n_open, n_trades, n_blocked = [], [], 0, 0

    realized = 0.0
    for day in DAYS:
        for p in [q for q in open_pos if (day - q["opened"]).days >= HOLD * BAR2CAL]:
            # BOOK THE RESULT. Differencing a curve that mixes realised and unrealised P&L
            # records every rotation as a loss: a position closing at +20% and a fresh one
            # opening at 0 looks like a 2pp down day on ten slots. Realised gains must stay.
            realized += mark(p["idx"], HOLD) / SLOTS
            open_pos.remove(p)
        held = {q["dup"] for q in open_pos}
        cand = BY_DAY.get(day, np.empty(0, int))
        if len(cand):
            cand = cand[~np.isin(DUP[cand], list(held))]
            rng.shuffle(cand)
            seen, uniq = set(), []
            for i in cand:                       # one position per real trade
                if DUP[i] not in seen:
                    seen.add(DUP[i]); uniq.append(i)
            cand = np.asarray(uniq, int)

        free = SLOTS - len(open_pos)
        if free > 0 and len(cand):
            if arm == "A":
                take = cand[:free]
            elif arm == "B":                     # the pick is made, then cash if vetoed
                picked = cand[:free]
                n_blocked += int(VETO[picked].sum())
                take = picked[~VETO[picked]]
            elif arm == "C":
                allowed = cand[~VETO[cand]]
                take = allowed[:free]
            else:                                # D
                allowed = cand[~drop[cand]]
                take = allowed[:free]
            for i in take:
                open_pos.append({"idx": int(i), "dup": DUP[i], "opened": day})
                n_trades += 1

        unreal = sum(mark(p["idx"], (day - p["opened"]).days / BAR2CAL) for p in open_pos)
        equity.append(1.0 + realized + unreal / SLOTS)   # each slot is 1/K of capital
        n_open.append(len(open_pos))

    # fixed fractional capital: the curve IS realised + unrealised, no compounding assumed
    curve = pd.Series(equity, index=pd.DatetimeIndex(DAYS))
    peak = curve.cummax()
    dd = (curve / peak - 1)
    yrs = (DAYS[-1] - DAYS[0]) / np.timedelta64(365, "D")
    cagr = max(curve.iloc[-1], 1e-6) ** (1 / yrs) - 1
    mdd = float(dd.min())
    return dict(arm=arm, seed=seed, cagr=cagr * 100, mdd=mdd * 100,
                calmar=cagr / abs(mdd) if mdd else np.nan,
                exposure=float(np.mean(n_open)) / SLOTS * 100,
                trades=n_trades, blocked=n_blocked, curve=curve, dd=dd)


rows = []
for arm in ("A", "B", "C", "D"):
    for s in range(SEEDS):
        rows.append(run(arm, s))
    r = pd.DataFrame([x for x in rows if x["arm"] == arm])
    print(f"  arm {arm}: CAGR {r.cagr.mean():>6.2f}%  MDD {r.mdd.mean():>7.2f}%  "
          f"Calmar {r.calmar.mean():>5.2f}  exposure {r.exposure.mean():>5.1f}%  "
          f"trades {r.trades.mean():>7,.0f}", flush=True)

R = pd.DataFrame([{k: v for k, v in r.items() if k not in ("curve", "dd")} for r in rows])
R.to_csv("calmar_veto_experiment.csv", index=False)

print("\n" + "=" * 122, flush=True)
print("  WHAT EACH COMPARISON MEANS", flush=True)
print("=" * 122, flush=True)
base = R[R.arm == "A"]


def cmp(arm, question):
    a = R[R.arm == arm]
    d_cal = a.calmar.mean() - base.calmar.mean()
    # paired by seed — the same calendar and the same shuffle, so the difference is the arm
    paired = (a.set_index("seed").calmar - base.set_index("seed").calmar).dropna()
    lo, hi = np.percentile(paired, [2.5, 97.5])
    print(f"  {arm} − A   ΔCalmar {d_cal:>+6.3f}  paired CI [{lo:>+6.3f},{hi:>+6.3f}]  "
          f"ΔCAGR {a.cagr.mean()-base.cagr.mean():>+6.2f}pp  "
          f"ΔMDD {a.mdd.mean()-base.mdd.mean():>+6.2f}pp   {question}", flush=True)
    return d_cal, lo, hi


b = cmp("B", "exposure effect (forced cash)")
c = cmp("C", "SELECTION effect (replaced)")
d = cmp("D", "random veto, same rate — the control")

print("\n" + "=" * 122, flush=True)
print("  READING", flush=True)
print("=" * 122, flush=True)
sel_real = c[1] > 0 and (c[0] - d[0]) > 0
print(f"    veto+replacement beats baseline: {'yes' if c[1] > 0 else 'no'}", flush=True)
print(f"    random veto beats baseline too : {'yes' if d[1] > 0 else 'no'}", flush=True)
if c[1] > 0 and d[1] > 0:
    print(f"    → BOTH help. The veto's advantage over RANDOM is {c[0]-d[0]:+.3f} Calmar — "
          f"that difference is the only part that is selection.", flush=True)
elif c[1] > 0:
    print("    → the veto helps and random does not: this is selection value.", flush=True)
else:
    print("    → the veto does not beat the baseline once slots are refilled.", flush=True)
print("\nDONE", flush=True)
