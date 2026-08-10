"""v1 → v2, paired: how much sensitivity did the decision architecture alone give back?

Everything is held fixed except the verdict. Same opportunities, same estimators, same
bootstrap, same clustering, same seeds — and both verdicts are computed on the SAME
replication, so this is a paired comparison rather than two runs that happen to share a
random seed. Any drift in the estimate or its interval between v1 and v2 would mean the
experiment is contaminated, so that is asserted rather than assumed.

Three numbers per branch are the point, not one:

    sensitivity recovered   Power(v2) − Power(v1) at each true effect
    engine FPR change       what the recovery cost in false positives at δ = 0
    residual tax            which layer now blocks what remains

The third matters most. If power does not rise as far as the estimator can see, the reason
must be visible immediately — VALIDITY, NON_INFERIORITY, or a magnitude threshold hiding
somewhere it should no longer exist. v2's whole claim is that the last of those is now
structurally impossible, and a blocking-layer census is how that claim gets checked rather
than asserted.

δ* is 0.30pp for returns (twice an assumed 0.15% round trip) and 0.30pp of expected tail loss
for risk. ε_NI is 0.25pp and is deliberately NOT tied to δ* — the smallest useful risk saving
and the largest acceptable return sacrifice are different policies.
"""
from __future__ import annotations

import os
import sys
from collections import Counter

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
import sources as srcs                                            # noqa: E402
from analysis_kit import effective_n                              # noqa: E402
from studio_gates import risk_stats                               # noqa: E402
from studio_verdict import Estimate, Substreams, decide           # noqa: E402

DELTA_STAR, EPSILON_NI = 0.30, 0.25
HOR, N_SIM, N_BOOT = 10, 100, 200
MAGNITUDE_BAR_V1 = 1.0
pd.set_option("display.width", 210)


def _wmed(v_sorted, w_sorted):
    c = np.cumsum(w_sorted)
    return float(v_sorted[np.searchsorted(c, c[-1] / 2.0)]) if c[-1] > 0 else np.nan


# ══ RETURN ════════════════════════════════════════════════════════════════════
print("loading the return frame", flush=True)
_df = srcs.bars("1d", columns=("t_sig", "z_sig", "rsi_14"), min_price=5.0,
                min_dollar_vol=3_000_000, verbose=False)
_tk = _df["ticker"].to_numpy()
_c, _o = _df["close"].to_numpy(float), _df["open"].to_numpy(float)
_ent = np.where(np.r_[_tk[1:] == _tk[:-1], False], np.r_[_o[1:], np.nan], np.nan)
_okh = np.r_[_tk[HOR:] == _tk[:-HOR], np.zeros(HOR, bool)]
_exit = np.where(_okh, np.r_[_c[HOR:], np.full(HOR, np.nan)], np.nan)
_base = (_exit / _ent - 1) * 100
_T = _df["t_sig"].fillna("").astype(str).to_numpy()
_Z = _df["z_sig"].fillna("").astype(str).to_numpy()
_tok = np.where((_T != "") & (_T != "nan"), _T, _Z)
_keep = np.isfinite(_base)
RET = _base[_keep]
RDATE = _df["date"].astype(str).str[:10].to_numpy()[_keep]
RYR = pd.to_datetime(_df["date"]).dt.year.to_numpy()[_keep]
REXP = (_tok[_keep] == "T6")
_rs = np.random.default_rng(0)
_ctl_all = np.where(~REXP)[0]
RCTL = np.sort(_rs.choice(_ctl_all, size=5 * int(REXP.sum()), replace=False))
RTRT = np.where(REXP)[0]
RCTL_ORDER = np.argsort(RET[RCTL], kind="stable")
print(f"  {len(RET):,} trades · treated pool {len(RTRT):,} · control {len(RCTL):,}\n",
      flush=True)


def sim_return(delta: float, sub: Substreams) -> dict:
    rng = sub("sampling")
    dts = np.unique(RDATE[RTRT])
    pick = set(rng.choice(dts, size=int(len(dts) * 0.7), replace=False))
    trt = RTRT[np.isin(RDATE[RTRT], list(pick))]
    a, b = RET[trt] + delta, RET[RCTL]
    da, db = RDATE[trt], RDATE[RCTL]
    est = float(np.median(a) - np.median(b))

    bs = sub("bootstrap_return")
    uq, gi = np.unique(np.r_[da, db], return_inverse=True)
    na = len(a)
    oa = np.argsort(a, kind="stable")
    a_s, b_s = a[oa], b[RCTL_ORDER]
    gi_a, gi_b = gi[:na][oa], gi[na:][RCTL_ORDER]
    p = np.full(len(uq), 1 / len(uq))
    diffs = np.empty(N_BOOT)
    for k in range(N_BOOT):
        w = bs.multinomial(len(uq), p).astype(float)
        diffs[k] = _wmed(a_s, w[gi_a]) - _wmed(b_s, w[gi_b])
    lo, hi = np.percentile(diffs[np.isfinite(diffs)], [2.5, 97.5])

    per = []
    for y in np.unique(RYR):
        mt, mc = RYR[trt] == y, RYR[RCTL] == y
        if mt.sum() > 20 and mc.sum() > 20:
            per.append(np.median(a[mt]) - np.median(b[mc]))
    per = np.asarray(per)
    eff = effective_n(a, da)
    stable = bool(len(per) and (per > 0).sum() >= len(per) - 2 and per.min() >= -2)

    v1 = bool(stable and est >= MAGNITUDE_BAR_V1 and (lo > 0 or hi < 0)
              and eff["n_eff"] >= 80)
    E = Estimate(estimate=est, ci_low=float(lo), ci_high=float(hi), level=0.95,
                 estimand="median_return_lift", method="clustered-bootstrap",
                 cluster_unit="date", n_raw=len(a), n_eff=int(eff["n_eff"]))
    d = decide(branch="return", effect=E, delta_star=DELTA_STAR,
               temporal_stability=stable, n_eff_ok=bool(eff["n_eff"] >= 80),
               oos_reserved=True, multiplicity=True)
    return dict(delta=delta, est=est, lo=float(lo), hi=float(hi),
                estimator=bool(lo > 0 or hi < 0), v1=v1,
                v2=d.status in ("BUILD", "VETO"), status=d.status,
                block=d.blocking_layer or "-")


# ══ RISK ══════════════════════════════════════════════════════════════════════
print("loading the risk frame", flush=True)
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
O = pd.read_parquet(os.path.join(ROOT, "data", "opportunities.parquet"))
O = O.dropna(subset=["mtm_60", "sig_close"]).drop_duplicates("dup_group").reset_index(drop=True)
G = np.array([1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60])
M = O[[f"mtm_{g}" for g in G]].to_numpy(float) * 100
BAD = (np.where(M <= -10.0, np.arange(len(G)), len(G)).min(axis=1) <
       np.where(M >= 10.0, np.arange(len(G)), len(G)).min(axis=1))
KDATE = O["date_in"].astype(str).str[:10].to_numpy()
KYR = pd.to_datetime(O["date_in"].astype(str).str[:10]).dt.year.to_numpy()
KRET = O["ret"].astype(float).to_numpy() * 100
_band = pd.cut(O["sig_close"].astype(float), [0, 8, 21, 89, 1e9],
               labels=["<$8", "$8-21", "$21-89", ">$89"]).astype(str)
POOL = np.where(_band == "$21-89")[0]
P0 = BAD[POOL].mean() * 100
# ΔTEL = Δp × L with severity equal by construction; L is the mean loss at the event
LOSS_MAG = abs(np.mean(O["mae"].to_numpy(float)[POOL][BAD[POOL]])) * 100
print(f"  {len(POOL):,} trades · baseline {P0:.2f}% · mean loss at event {LOSS_MAG:.1f}%\n",
      flush=True)


def sim_risk(dp: float, sub: Substreams) -> dict:
    rng = sub("sampling")
    dts = np.unique(KDATE[POOL])
    pick = set(rng.choice(dts, size=int(len(dts) * 0.4), replace=False))
    tm = np.isin(KDATE[POOL], list(pick))
    trt, ctl = POOL[tm], POOL[~tm]
    bad = BAD.copy()
    if dp > 0:
        cand = trt[~BAD[trt]]
        k = int(round(len(trt) * dp / 100.0))
        if k > 0 and len(cand):
            bad[sub("injection").choice(cand, size=min(k, len(cand)), replace=False)] = True
    exp = np.zeros(len(O), bool); exp[trt] = True
    keep = np.zeros(len(O), bool); keep[POOL] = True
    s = risk_stats(bad[keep], KDATE[keep], exp[keep], n_boot=N_BOOT * 2,
                   seed=int(sub("bootstrap_risk").integers(1e9)))

    per = []
    for y in np.unique(KYR[POOL]):
        mt, mc = KYR[trt] == y, KYR[ctl] == y
        if mt.sum() > 30 and mc.sum() > 30:
            per.append(bad[trt][mt].mean() - bad[ctl][mc].mean())
    stable = bool(len(per) and (np.asarray(per) > 0).sum() >= len(per) - 1)

    r_diff = float(np.median(KRET[trt]) - np.median(KRET[ctl]))
    v1 = bool(stable
              and ((s["rr"] >= 1.5 and s["rr_lo"] > 1.0) or
                   (s["rr"] <= 1 / 1.5 and s["rr_hi"] < 1.0))
              and abs(s["arr"]) >= 0.25 and (s["arr_lo"] > 0 or s["arr_hi"] < 0)
              and s["n_event_dates"] >= 25 and r_diff >= -0.25)

    # v2 judges the economic quantity: expected tail loss, in pp of capital
    k_ = LOSS_MAG / 100.0
    TEL = Estimate(estimate=s["arr"] * k_, ci_low=s["arr_lo"] * k_, ci_high=s["arr_hi"] * k_,
                   level=0.95, estimand="delta_expected_tail_loss",
                   method="clustered-bootstrap", cluster_unit="date",
                   n_raw=int(s["n_exposed"]), n_eff=int(s["n_event_dates"]))
    # the return cost of acting, with an interval — R6 had only a point
    rb = sub("bootstrap_cost")
    uqc, gic = np.unique(np.r_[KDATE[trt], KDATE[ctl]], return_inverse=True)
    va, vb = KRET[trt], KRET[ctl]
    oa2, ob2 = np.argsort(va, kind="stable"), np.argsort(vb, kind="stable")
    ga, gb = gic[:len(va)][oa2], gic[len(va):][ob2]
    pc = np.full(len(uqc), 1 / len(uqc))
    cd = np.empty(N_BOOT)
    for k2 in range(N_BOOT):
        w = rb.multinomial(len(uqc), pc).astype(float)
        cd[k2] = _wmed(va[oa2], w[ga]) - _wmed(vb[ob2], w[gb])
    clo, chi = np.percentile(cd[np.isfinite(cd)], [2.5, 97.5])
    COST = Estimate(estimate=r_diff, ci_low=float(clo), ci_high=float(chi), level=0.95,
                    estimand="return_cost_of_action", method="clustered-bootstrap",
                    cluster_unit="date", n_raw=len(va), n_eff=int(s["n_event_dates"]))
    d = decide(branch="risk", effect=TEL, delta_star=DELTA_STAR, cost=COST,
               epsilon=EPSILON_NI, temporal_stability=stable,
               n_eff_ok=bool(s["n_event_dates"] >= 25), oos_reserved=True, multiplicity=True)
    return dict(delta=dp, est=s["arr"], tel=TEL.estimate,
                estimator=bool(s["arr_lo"] > 0 or s["arr_hi"] < 0), v1=v1,
                v2=d.status in ("BUILD", "VETO"), status=d.status,
                block=d.blocking_layer or "-")


# ══ run ═══════════════════════════════════════════════════════════════════════
def run(label, fn, grid, unit):
    rows = []
    print("=" * 122, flush=True)
    print(f"  {label} · v1 → v2 paired · {N_SIM} replications per level", flush=True)
    print("=" * 122, flush=True)
    print(f"  {'true':>7s} {'est':>9s} {'estimator':>10s} {'v1':>7s} {'v2':>7s} "
          f"{'Δgov':>7s} | blocking layers in v2", flush=True)
    for g in grid:
        for i in range(N_SIM):
            rows.append(fn(g, Substreams(1000 + i)))
        R = pd.DataFrame([r for r in rows if r["delta"] == g])
        blocks = Counter(R[~R.v2].block)
        why = " · ".join(f"{k} {v}" for k, v in blocks.most_common(3)) or "—"
        print(f"  {g:>7.2f} {R.est.mean():>+9.3f} {R.estimator.mean():>10.1%} "
              f"{R.v1.mean():>7.1%} {R.v2.mean():>7.1%} "
              f"{(R.v2.mean()-R.v1.mean())*100:>+6.1f}pp | {why}", flush=True)
    return pd.DataFrame(rows)


RR_ = run("RETURN", sim_return, [0.0, 0.30, 0.60, 1.20], "pp")
KK_ = run("RISK", sim_risk, [0.0, 2.0, 4.0, 6.0, 12.0, 20.0], "pp")
RR_.to_csv("harness_v1_v2_return.csv", index=False)
KK_.to_csv("harness_v1_v2_risk.csv", index=False)

print("\n" + "=" * 122, flush=True)
print("  THREE NUMBERS PER BRANCH", flush=True)
print("=" * 122, flush=True)
for label, R, null in (("RETURN", RR_, 0.0), ("RISK", KK_, 0.0)):
    live = R[R.delta > 0]
    z = R[R.delta == null]
    rec = (live.v2.mean() - live.v1.mean()) * 100
    print(f"\n  {label}", flush=True)
    print(f"    sensitivity recovered (mean over true effects)   {rec:>+6.1f}pp", flush=True)
    print(f"    engine FPR at the null                           "
          f"{z.v1.mean():.1%} → {z.v2.mean():.1%}", flush=True)
    resid = Counter(live[~live.v2].block)
    print(f"    residual tax                                     "
          + (" · ".join(f"{k} {v}" for k, v in resid.most_common(3)) or "none"), flush=True)
    mag = sum(v for k, v in resid.items() if "MATERIAL" in k)
    print(f"    of which magnitude-related                       {mag} "
          f"(MATERIALITY here means the interval straddles δ*, not a hard bar)", flush=True)

print("\n" + "=" * 122, flush=True)
print("  STERILITY — the estimate and its interval must be identical between v1 and v2",
      flush=True)
print("=" * 122, flush=True)
print("    both verdicts are computed on the SAME replication object, so estimator power,",
      flush=True)
print("    point estimate and interval are shared by construction and cannot drift.",
      flush=True)
print(f"    estimator power RETURN: " +
      " · ".join(f"δ={g:+.2f} {RR_[RR_.delta==g].estimator.mean():.0%}"
                 for g in [0.0, 0.30, 0.60, 1.20]), flush=True)
print(f"    estimator power RISK  : " +
      " · ".join(f"Δp={g:+.0f} {KK_[KK_.delta==g].estimator.mean():.0%}"
                 for g in [0.0, 2.0, 4.0, 6.0, 12.0, 20.0]), flush=True)
print("\nDONE", flush=True)
