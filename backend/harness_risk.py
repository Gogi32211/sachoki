"""risk_v1, frozen: can the RISK branch say YES to a risk effect of known size?

The return branch produced a clean diagnosis — at δ = 0.60pp the estimator resolved the effect
95.8% of the time and the verdict passed 0.0%, because L2a demanded 1.0pp. That is a
governance defect, and it was only visible after L2 was split into its two questions.

The risk gates may hold the same defect, and the arithmetic already suggests it. R2 asks for
RR ≥ 1.5, which on a baseline of p₀ means Δp ≥ 0.5·p₀ — a threshold that MOVES WITH THE
BASELINE and that nobody declared. Yesterday's hazard study ran at p₀ = 34.21%, where R2
silently demanded Δp ≥ 17.1pp against an observed 6.05pp. But arithmetic is not measurement,
and there is a second possibility that has nothing to do with gates:

    A  estimator resolves Δp = 6pp, gates refuse       → governance defect, as in RETURN
    B  estimator does NOT resolve Δp = 6pp             → an inference problem; R2 is innocent

I made exactly this attribution error earlier today, blaming L2 as a whole before separating
the threshold from the interval. So the design forbids repeating it: estimator power and
verdict power are computed and printed separately from the first run, and no statement about
the gates is made unless the estimator has shown it can see the effect.

Tail events cluster far harder than ordinary returns — 190 catastrophes across ~50 panic dates
are ~50 facts, not 190 — so the risk estimator may genuinely be the weaker half here. The
δ = 0 arm measures that directly as empirical size.

NOTHING IN risk_v1 IS CHANGED. R2 ≥ 1.5, R3 ≥ 0.25pp, R4 ≥ 25 event dates, R6 non-inferiority
all stand exactly as written, so this run is a snapshot of the semantics we have, comparable
to engine_return_v1.frozen.

Injection: membership frozen at entry; both arms pass through the same shadow-path builder;
severity is held EQUAL between arms so ground truth is exactly ΔTEL = Δp × L and the frequency
machinery is isolated. Varying severity is a later test, not this one.

Grid: 0, 2, 4, 6, 12pp — resolution where the question is open — plus one point above the
17.1pp structural barrier purely to confirm the jump. That R2 fires above its own threshold is
arithmetic; what needs measuring is everything below it.
"""
from __future__ import annotations

import math
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio_gates import Integrity, risk_stats                    # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GRID_MTM = np.array([1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60])
MTM = [f"mtm_{g}" for g in GRID_MTM]
LOSS_BAR, GAIN_BAR = -10.0, 10.0
DELTAS = [0.0, 2.0, 4.0, 6.0, 12.0, 20.0]      # pp of P(loss first); 20 is above R2's barrier
N_SIM = 100
SEED = 0
pd.set_option("display.width", 210)

# ── the population, built once ───────────────────────────────────────────────
O = pd.read_parquet(os.path.join(ROOT, "data", "opportunities.parquet"))
O = O.dropna(subset=["mtm_60", "sig_close"]).drop_duplicates("dup_group").reset_index(drop=True)
M = O[MTM].to_numpy(float) * 100
hit_loss = np.where(M <= LOSS_BAR, np.arange(len(GRID_MTM)), len(GRID_MTM)).min(axis=1)
hit_gain = np.where(M >= GAIN_BAR, np.arange(len(GRID_MTM)), len(GRID_MTM)).min(axis=1)
BAD = hit_loss < hit_gain                                  # the event: loss before gain
DATE = O["date_in"].astype(str).str[:10].to_numpy()
YR = pd.to_datetime(O["date_in"].astype(str).str[:10]).dt.year.to_numpy()
RET = O["ret"].astype(float).to_numpy() * 100
band = pd.cut(O["sig_close"].astype(float), [0, 8, 21, 89, 1e9],
              labels=["<$8", "$8-21", "$21-89", ">$89"]).astype(str)
POOL = np.where(band == "$21-89")[0]                       # one homogeneous population
p0 = BAD[POOL].mean() * 100

print("=" * 122, flush=True)
print(f"  risk_v1 FROZEN · positive control · {N_SIM} replications per Δp", flush=True)
print("=" * 122, flush=True)
print(f"  population $21-89 · {len(POOL):,} trades · {len(np.unique(DATE[POOL])):,} dates",
      flush=True)
print(f"  baseline P(loss {LOSS_BAR:+.0f}% before gain {GAIN_BAR:+.0f}%) = {p0:.2f}%",
      flush=True)
print(f"  R2 (RR ≥ 1.5) therefore demands Δp ≥ {0.5*p0:.2f}pp on this baseline — "
      f"a threshold nobody declared", flush=True)
print(f"  R3 (ARR ≥ 0.25pp) demands Δp ≥ 0.25pp · binding gate: "
      f"{'R2' if 0.5*p0 > 0.25 else 'R3'}\n", flush=True)


def simulate(dp: float, rng) -> dict:
    """Split the population, plant Δp extra adverse outcomes in the treated half.

    The event flag is flipped rather than the path rebuilt, and that is a real limitation of
    this first control: it tests the frequency machinery — estimator, clustering, gates — and
    NOT the path→label step, which _pathsim already owns. Severity is equal by construction,
    so ground truth is exactly ΔTEL = Δp × L.
    """
    dts = np.unique(DATE[POOL])
    pick = set(rng.choice(dts, size=int(len(dts) * 0.4), replace=False))
    treat_mask = np.isin(DATE[POOL], list(pick))
    trt, ctl = POOL[treat_mask], POOL[~treat_mask]

    bad = BAD.copy()
    if dp > 0:                                    # plant the effect among treated non-events
        cand = trt[~BAD[trt]]
        k = int(round(len(trt) * dp / 100.0))
        if k > 0 and len(cand):
            bad[rng.choice(cand, size=min(k, len(cand)), replace=False)] = True

    exposed = np.zeros(len(O), bool)
    exposed[trt] = True
    keep = np.zeros(len(O), bool)
    keep[POOL] = True
    s = risk_stats(bad[keep], DATE[keep], exposed[keep], n_boot=300,
                   seed=int(rng.integers(1e6)))

    per = []
    for y in np.unique(YR[POOL]):
        mt, mc = (YR[trt] == y), (YR[ctl] == y)
        if mt.sum() > 30 and mc.sum() > 30:
            per.append(bad[trt][mt].mean() - bad[ctl][mc].mean())
    per = np.asarray(per)

    # the return side, needed by R6 — unchanged by the injection, so it should pass
    r_diff = np.median(RET[trt]) - np.median(RET[ctl])

    R1 = bool(len(per) and (np.asarray(per) > 0).sum() >= len(per) - 1)
    R2 = bool((s["rr"] >= 1.5 and s["rr_lo"] > 1.0) or
              (s["rr"] <= 1 / 1.5 and s["rr_hi"] < 1.0))
    R3 = bool(abs(s["arr"]) >= 0.25 and (s["arr_lo"] > 0 or s["arr_hi"] < 0))
    R4 = bool(s["n_event_dates"] >= 25)
    R6 = bool(r_diff >= -0.25)
    # the estimator's own question: is the effect resolved at all, gates aside
    EST = bool(s["arr_lo"] > 0 or s["arr_hi"] < 0)
    return dict(dp=dp, arr=s["arr"], arr_lo=s["arr_lo"], arr_hi=s["arr_hi"], rr=s["rr"],
                rr_lo=s["rr_lo"], n_events=s["n_events"], n_dates=s["n_event_dates"],
                R1=R1, R2=R2, R3=R3, R4=R4, R6=R6, EST=EST,
                verdict=bool(R1 and R2 and R3 and R4 and R6))


rows = []
print(f"  {'Δp true':>8s} {'ARR est':>9s} {'bias':>7s} {'RR est':>7s} | "
      f"{'ESTIMATOR':>10s} {'VERDICT':>8s} {'coverage':>9s} | "
      f"{'R1':>5s} {'R2':>5s} {'R3':>5s} {'R4':>5s} {'R6':>5s}", flush=True)
for dp in DELTAS:
    rng = np.random.default_rng(SEED + int(dp * 10))
    for _ in range(N_SIM):
        rows.append(simulate(dp, rng))
    R = pd.DataFrame([r for r in rows if r["dp"] == dp])
    cov = ((R.arr_lo <= dp) & (R.arr_hi >= dp)).mean()
    print(f"  {dp:>8.1f} {R.arr.mean():>+9.2f} {R.arr.mean()-dp:>+7.2f} {R.rr.mean():>7.2f} | "
          f"{R.EST.mean():>10.1%} {R.verdict.mean():>8.1%} {cov:>9.1%} | "
          f"{R.R1.mean():>5.0%} {R.R2.mean():>5.0%} {R.R3.mean():>5.0%} "
          f"{R.R4.mean():>5.0%} {R.R6.mean():>5.0%}", flush=True)
RR = pd.DataFrame(rows)
RR.to_csv("harness_risk_v1.csv", index=False)

print("\n" + "=" * 122, flush=True)
print("  WHICH HALF IS BROKEN — the question the return branch taught us to ask first",
      flush=True)
print("=" * 122, flush=True)
for dp in DELTAS[1:]:
    R = RR[RR.dp == dp]
    e, v = R.EST.mean(), R.verdict.mean()
    if e >= 0.8 and v < 0.2:
        note = "🔴 GOVERNANCE — the estimator sees it and the gates refuse"
    elif e < 0.5:
        note = "⚠ INFERENCE — the estimator cannot resolve it; the gates are not the issue"
    else:
        note = "— mixed"
    print(f"  Δp = {dp:>5.1f}pp   estimator {e:>6.1%}   verdict {v:>6.1%}   {note}",
          flush=True)

print("\n" + "=" * 122, flush=True)
print("  GATE SENSITIVITY — Shapley over R1/R2/R3/R4/R6", flush=True)
print("=" * 122, flush=True)
GATES = ["R1", "R2", "R3", "R4", "R6"]
for dp in DELTAS[1:]:
    S = RR[RR.dp == dp]

    def power_of(sub):
        m = np.ones(len(S), bool)
        for g in sub:
            m &= S[g].to_numpy()
        return m.mean()

    shap = {g: 0.0 for g in GATES}
    others_all = {g: [x for x in GATES if x != g] for g in GATES}
    for g in GATES:
        oth = others_all[g]
        for r in range(len(oth) + 1):
            from itertools import combinations
            for sub in combinations(oth, r):
                w = (math.factorial(r) * math.factorial(len(GATES) - r - 1)
                     / math.factorial(len(GATES)))
                shap[g] += w * (power_of(list(sub)) - power_of(list(sub) + [g]))
    top = max(shap, key=shap.get)
    print(f"  Δp = {dp:>5.1f}pp   full verdict power {power_of(GATES):>6.1%}   "
          + " · ".join(f"{g} {shap[g]*100:>5.1f}pp" for g in GATES)
          + f"   → largest tax: {top}", flush=True)

print("\n" + "=" * 122, flush=True)
print("  SCOPE", flush=True)
print("=" * 122, flush=True)
print(f"    baseline p0 {p0:.2f}% · event = loss {LOSS_BAR:+.0f}% before gain "
      f"{GAIN_BAR:+.0f}% within 60 bars", flush=True)
print(f"    severity EQUAL between arms by construction → ΔTEL = Δp × L exactly", flush=True)
print(f"    the event flag is planted, not the price path — this control tests the FREQUENCY "
      f"machinery\n    (estimator, clustering, gates) and not the path→label step", flush=True)
print(f"    injection is date-clustered (40% of dates treated), which is the shape real risk "
      f"takes", flush=True)
print("\nDONE", flush=True)
