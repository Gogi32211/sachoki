"""1c — the three checks that decide between the five diagnoses, cheaply.

Diagnostics 1 and 2 measure widths and ratios. Those narrow the field but do not, on their own,
distinguish an implementation bug from an estimand problem from an honest price of dependence.
Three near-free measurements do:

 D  PAIRED WEIGHTS, ASSERTED. Not "the widths are similar", not "the code reads as shared" —
    for every bootstrap replication b and every calendar date present in both arms, extract the
    weight each arm actually received and compare them elementwise. w_T[d] == w_C[d] for all d
    closes the hypothesis outright; a single mismatch names an implementation bug and ends the
    enquiry with a fix rather than a judgement.

 E  DISCRETENESS, PROPERLY. The count of distinct values is the weakest form of this question.
    A bootstrap distribution can have 180 distinct values out of 200 and still be unable to
    move its endpoints, if the mass sits on a few atoms and the values near the 2.5th and
    97.5th percentiles are separated by visible gaps. So four numbers, and the last two are the
    ones that matter:

        unique / B                    how much of the draw is even distinguishable
        largest point mass            is there an atom the distribution keeps returning to
        gap between adjacent values   at q2.5 and q97.5 — can the endpoint move smoothly
        share exactly = observed      how often resampling fails to move the statistic at all

 F  DATE INFLUENCE. 1,284 clusters of wildly unequal mass: median 26 rows, max 643. Formally
    1,284 clusters, informationally far fewer. Leave-one-date-out on the median gives the top
    influencers directly. Two readings, opposite conclusions:

        a few dates move the estimate a lot   → wide intervals are the honest price of
                                                dependence, and no bootstrap fix recovers them
        no date moves it much, CI still huge  → the width is not coming from the data, and
                                                suspicion returns to the construction

Both diag1 and diag2 print numbers that are CONDITIONAL on one fixed control draw except where
stated. That distinction is kept in the labels rather than collapsed, because the conditional
and unconditional questions are both legitimate and they have different answers.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sources as srcs                                          # noqa: E402
from studio_verdict import Substreams                           # noqa: E402

HOR, N_BOOT, DELTA, CTL_MULT = 10, 2000, 0.60, 3
N_REP_W, N_REP_E = 25, 40
pd.set_option("display.width", 205)


def wmed(v_sorted, w):
    c = np.cumsum(w)
    return float(v_sorted[np.searchsorted(c, c[-1] / 2.0)]) if c[-1] > 0 else np.nan


print("loading", flush=True)
df = srcs.bars("1d", columns=("t_sig", "z_sig"), min_price=5.0, min_dollar_vol=3_000_000,
               verbose=False)
tk = df["ticker"].to_numpy()
c, o = df["close"].to_numpy(float), df["open"].to_numpy(float)
ent = np.where(np.r_[tk[1:] == tk[:-1], False], np.r_[o[1:], np.nan], np.nan)
okh = np.r_[tk[HOR:] == tk[:-HOR], np.zeros(HOR, bool)]
ex = np.where(okh, np.r_[c[HOR:], np.full(HOR, np.nan)], np.nan)
base = (ex / ent - 1) * 100
T = df["t_sig"].fillna("").astype(str).to_numpy()
Z = df["z_sig"].fillna("").astype(str).to_numpy()
tok = np.where((T != "") & (T != "nan"), T, Z)
keep = np.isfinite(base)
RET, DATE = base[keep], df["date"].astype(str).str[:10].to_numpy()[keep]
EXP = tok[keep] == "T6"
TRT = np.where(EXP)[0]
CTL = np.sort(np.random.default_rng(0).choice(np.where(~EXP)[0], size=CTL_MULT * len(TRT),
                                              replace=False))
bar = "=" * 118
print(f"  treated {len(TRT):,} · control {len(CTL):,} (fixed draw — every number below that "
      f"is not\n  marked UNCONDITIONAL is conditional on it)\n", flush=True)

# ── D · paired weights, asserted elementwise ─────────────────────────────────
print(bar, flush=True)
print(f"  D · PAIRED CLUSTER WEIGHTS — asserted elementwise, not inferred from widths",
      flush=True)
print(bar, flush=True)
sub = Substreams(4242)
a, b = RET[TRT] + DELTA, RET[CTL]
da, db = DATE[TRT], DATE[CTL]
uq, gi = np.unique(np.r_[da, db], return_inverse=True)
gi_a, gi_b = gi[:len(a)], gi[len(a):]
shared = np.intersect1d(np.unique(da), np.unique(db))
sh_idx = np.searchsorted(uq, shared)
bs = sub("bootstrap")
p = np.full(len(uq), 1 / len(uq))
mismatch = 0
for _ in range(N_REP_W):
    w = bs.multinomial(len(uq), p).astype(float)
    wa, wb = w[gi_a], w[gi_b]                                 # what each arm actually receives
    # per shared date, the weight seen by arm A and by arm B
    fa = pd.Series(wa).groupby(pd.Series(da)).first().reindex(shared).to_numpy()
    fb = pd.Series(wb).groupby(pd.Series(db)).first().reindex(shared).to_numpy()
    mismatch += int((fa != fb).sum())
print(f"    dates present in both arms: {len(shared):,} of {len(uq):,} total", flush=True)
print(f"    replications checked: {N_REP_W} · elementwise weight mismatches: {mismatch:,}",
      flush=True)
print(f"\n    → {'IDENTICAL in every replication — the arms share one draw, the covariance is preserved,' if mismatch == 0 else 'MISMATCH FOUND — this is an implementation bug and it inflates Var(T−C)'}",
      flush=True)
if mismatch == 0:
    print(f"      and the paired-weights hypothesis is CLOSED.", flush=True)

# ── E · discreteness, four numbers ───────────────────────────────────────────
print("\n" + bar, flush=True)
print(f"  E · DISCRETENESS — can the endpoints move at all?  ({N_BOOT} draws × {N_REP_E} reps)",
      flush=True)
print(bar, flush=True)
oa, ob = np.argsort(a, kind="stable"), np.argsort(b, kind="stable")
a_s, b_s = a[oa], b[ob]
ga, gb = gi_a[oa], gi_b[ob]
obs = float(np.median(a) - np.median(b))
rows = []
for r in range(N_REP_E):
    rb = Substreams(9000 + r)("bootstrap")
    d = np.empty(N_BOOT)
    for k in range(N_BOOT):
        w = rb.multinomial(len(uq), p).astype(float)
        d[k] = wmed(a_s, w[ga]) - wmed(b_s, w[gb])
    v, cnt = np.unique(np.round(d, 9), return_counts=True)
    ds = np.sort(d)
    i_lo, i_hi = int(0.025 * N_BOOT), int(0.975 * N_BOOT) - 1
    # how far the endpoint jumps if it moves by one order statistic
    gap_lo = float(np.diff(np.unique(ds[max(0, i_lo - 3):i_lo + 4])).mean()) \
        if len(np.unique(ds[max(0, i_lo - 3):i_lo + 4])) > 1 else 0.0
    gap_hi = float(np.diff(np.unique(ds[i_hi - 3:i_hi + 4])).mean()) \
        if len(np.unique(ds[i_hi - 3:i_hi + 4])) > 1 else 0.0
    rows.append(dict(uniq=len(v) / N_BOOT, mass=cnt.max() / N_BOOT,
                     gap_lo=gap_lo, gap_hi=gap_hi,
                     exact=float(np.mean(np.isclose(d, obs, atol=1e-9))),
                     width=float(ds[i_hi] - ds[i_lo])))
E = pd.DataFrame(rows)
print(f"    observed statistic  {obs:+.4f}pp · mean bootstrap width {E.width.mean():.4f}pp",
      flush=True)
print(f"    unique / B                        {E.uniq.mean():>7.1%}", flush=True)
print(f"    largest point mass                {E.mass.mean():>7.1%}", flush=True)
print(f"    share exactly = observed          {E.exact.mean():>7.1%}", flush=True)
print(f"    gap between adjacent values @q2.5 {E.gap_lo.mean():>7.4f}pp   "
      f"@q97.5 {E.gap_hi.mean():>7.4f}pp", flush=True)
share = (E.gap_lo.mean() + E.gap_hi.mean()) / 2 / max(E.width.mean(), 1e-9)
print(f"    one step at the endpoint is {share:.1%} of the whole interval", flush=True)
print(f"\n    → {'STEPPED — the endpoints cannot move smoothly, and a step-function bootstrap is a wide one' if share > 0.05 or E.mass.mean() > 0.10 else 'SMOOTH — the endpoints move freely; discreteness is NOT the mechanism'}",
      flush=True)

# ── F · date influence ───────────────────────────────────────────────────────
print("\n" + bar, flush=True)
print("  F · DATE INFLUENCE — leave-one-date-out on the treated median", flush=True)
print(bar, flush=True)
theta = float(np.median(a))
dser = pd.Series(da)
infl = {}
for d0, idx in dser.groupby(dser).groups.items():
    infl[d0] = theta - float(np.median(np.delete(a, np.asarray(idx))))
I = pd.Series(infl).abs().sort_values(ascending=False)
n_per = dser.value_counts()
print(f"    full-sample treated median {theta:+.4f}pp", flush=True)
print(f"    {'date':<12s} {'n rows':>7s} {'Δ median':>10s}", flush=True)
for d0 in I.index[:10]:
    print(f"    {d0:<12s} {n_per[d0]:>7,d} {infl[d0]:>+10.5f}", flush=True)
top5 = I.iloc[:5].sum()
print(f"\n    top-5 dates move the median by {top5:.5f}pp in total · half-width of the "
      f"bootstrap CI {E.width.mean()/2:.4f}pp", flush=True)
print(f"    ratio {top5/(E.width.mean()/2):.3f}", flush=True)
print(f"\n    → {'a handful of dates carry the statistic — the wide interval is the honest price of dependence' if top5/(E.width.mean()/2) > 0.5 else 'no single date moves the median much, yet the interval is large — the width is NOT coming from date leverage'}",
      flush=True)

E.to_csv("inference_diag3_discrete.csv", index=False)
pd.DataFrame({"date": I.index, "abs_influence": I.to_numpy(),
              "n": [n_per[d0] for d0 in I.index]}).to_csv("inference_diag3_influence.csv",
                                                          index=False)
print("\nDONE", flush=True)
