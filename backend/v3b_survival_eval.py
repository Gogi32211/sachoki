"""v3b_survival_eval.py — race labels, action policies, metrics and the gate. 2026-09-12.

FROZEN BEFORE THE RUN
  primary race      +3.0 ATR before −1.5 ATR, from close[t_d], using ATR[t_d],
                    scanning bars t_d+1 … t_d+20 only
  secondary race    +5 / −2          sensitivity only: +2 / −1
  AMBIGUOUS         both levels touched inside one daily bar. Intrabar order is unknowable from
                    OHLC, so the case is NOT assigned. Excluded from the primary result and
                    reported as best-case / worst-case bounds.
  OPEN              neither level touched in 20 bars. A separate class, never merged into losses.
  policies          P0 hold · P1 ATR×12 trailing (the validated BUILD) · P2 close<EMA20 exit ·
                    P3 fixed −1.5 ATR stop · P4 time stop 10 bars (5/15 sensitivity only)
  horizons          h = 1,2,3,5 reported separately; the best h is never selected
  gate              PC expectancy − best reference ≥ +0.10 ATR on HOLDOUT, same sign on
                    VALIDATION, premature-exit no worse by more than +5 pp, and a
                    ticker-clustered interval on the DIFFERENCE that does not straddle zero.
                    Mean AND median reported separately — a policy does not pass because a few
                    large winners lift the mean.

THE COUNTERFACTUAL RULE. `captured MFE` and `premature-exit` are computed STRICTLY from bars
AFTER the actual exit bar, and that information never reaches the exit decision. The functions
take `exit_offset` as an argument and read only beyond it; `_assert_counterfactual_causal` checks
this by construction on random cases.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import v3b_survival_build as B                                           # noqa: E402

RACE_H = B.RACE_H
UP_PRIMARY, DN_PRIMARY = 3.0, 1.5
RACES = {"primary_3_1.5": (3.0, 1.5), "secondary_5_2": (5.0, 2.0), "sens_2_1": (2.0, 1.0)}
TIME_STOPS = (5, 10, 15)
TIME_STOP_PRIMARY = 10
TRAIL_MULT, TRAIL_LO, TRAIL_HI = 12.0, 15.0, 60.0     # project_atr_exit_law, unchanged
GATE_EXPECTANCY, GATE_PREMATURE_PP = 0.10, 5.0

# The declared candidate set. Single conditions only at this stage; no exhaustive search.
CANDIDATE_RULES = {
    "exit_if_REGIME_D":      lambda E: E["REGIME_D"].to_numpy() == "D",
    "exit_if_K_RESET":       lambda E: E["K_RESET"].to_numpy(bool),
    "exit_if_no_FOLLOW":     lambda E: ~E["FOLLOW_THROUGH"].to_numpy(bool),
    "exit_if_GIVEBACK_lt_1": lambda E: E["GIVEBACK"].to_numpy(float) < -1.0,
    "exit_if_K2U":           lambda E: E["K_AT_DECISION_IS_K2U"].to_numpy(bool),
    "exit_if_not_REG_HELD":  lambda E: ~E["REGIME_HELD"].to_numpy(bool),
    "exit_if_RSI_gt70":      lambda E: E["RSI_D"].to_numpy(float) > 70,
    "exit_if_VOL7_6":        lambda E: E["VOL7_D"].to_numpy(float) == 6,
    "exit_if_K_not_bull":    lambda E: ~np.isin(E["K_D"].to_numpy(), ["K1U", "K2U"]),
}


# ───────────────────────────────────────────────────────────────────────────
# forward windows — the ONLY place bars after the decision bar are read
# ───────────────────────────────────────────────────────────────────────────
def forward_windows(df: pd.DataFrame, i_d: np.ndarray, H: int = RACE_H) -> dict:
    """(H, n) arrays of high/low/close for bars t_d+1 … t_d+H, plus EMA20 on the same bars."""
    hi = df["high"].to_numpy(float); lo = df["low"].to_numpy(float)
    cl = df["close"].to_numpy(float)
    bnd = B._bounds(df["ticker"].to_numpy())
    ema = np.full(len(df), np.nan)
    for a, b in zip(bnd[:-1], bnd[1:]):
        ema[a:b] = pd.Series(cl[a:b]).ewm(span=20, adjust=False).mean().to_numpy()
    idx = np.stack([i_d + k for k in range(1, H + 1)])
    return dict(high=hi[idx], low=lo[idx], close=cl[idx], ema20=ema[idx])


def race_label(fw: dict, ref_close, ref_atr, up_mult, dn_mult):
    """First-touch race. Returns (label, offset). AMBIGUOUS when one bar touches both."""
    U = ref_close + up_mult * ref_atr
    D = ref_close - dn_mult * ref_atr
    hit_up = fw["high"] >= U
    hit_dn = fw["low"] <= D
    any_hit = hit_up | hit_dn
    n = len(ref_close)
    lab = np.full(n, "OPEN", dtype=object)
    off = np.full(n, RACE_H, dtype=np.int16)
    has = any_hit.any(axis=0)
    first = np.argmax(any_hit, axis=0)                 # 0-based offset into 1..H
    k = first[has]
    cols = np.flatnonzero(has)
    u = hit_up[k, cols]; d = hit_dn[k, cols]
    lab[cols] = np.where(u & d, "AMBIGUOUS", np.where(u, "WIN", "LOSE"))
    off[cols] = (k + 1).astype(np.int16)
    return lab, off


# ───────────────────────────────────────────────────────────────────────────
# policies — each returns realised return in ATR units and its exit offset
# ───────────────────────────────────────────────────────────────────────────
def _ret(px, ref_close, ref_atr):
    return (px - ref_close) / ref_atr


def policy_hold(fw, ref_close, ref_atr):
    return _ret(fw["close"][-1], ref_close, ref_atr), np.full(len(ref_close), RACE_H, np.int16)


def policy_trail(fw, ref_close, ref_atr):
    """ATR×12 trailing, clipped to 15-60 % — project_atr_exit_law, unchanged."""
    pct = np.clip(TRAIL_MULT * (ref_atr / ref_close) * 100.0, TRAIL_LO, TRAIL_HI) / 100.0
    runmax = np.maximum.accumulate(fw["close"], axis=0)
    prev_max = np.vstack([np.maximum(ref_close, fw["close"][0]), runmax[:-1]])
    trig = fw["close"] < prev_max * (1.0 - pct)
    return _first_exit(trig, fw["close"], ref_close, ref_atr)


def policy_ema20(fw, ref_close, ref_atr):
    return _first_exit(fw["close"] < fw["ema20"], fw["close"], ref_close, ref_atr)


def policy_fixed_stop(fw, ref_close, ref_atr, dn_mult=DN_PRIMARY):
    D = ref_close - dn_mult * ref_atr
    trig = fw["low"] <= D
    n = len(ref_close)
    off = np.full(n, RACE_H, np.int16)
    px = fw["close"][-1].copy()
    has = trig.any(axis=0)
    k = np.argmax(trig, axis=0)[has]
    cols = np.flatnonzero(has)
    off[cols] = (k + 1).astype(np.int16)
    px[cols] = D[cols]                                  # filled at the stop level
    return _ret(px, ref_close, ref_atr), off


def policy_time(fw, ref_close, ref_atr, bars=TIME_STOP_PRIMARY):
    return _ret(fw["close"][bars - 1], ref_close, ref_atr), \
        np.full(len(ref_close), bars, np.int16)


def _first_exit(trig, close_w, ref_close, ref_atr):
    n = len(ref_close)
    off = np.full(n, RACE_H, np.int16)
    px = close_w[-1].copy()
    has = trig.any(axis=0)
    k = np.argmax(trig, axis=0)[has]
    cols = np.flatnonzero(has)
    off[cols] = (k + 1).astype(np.int16)
    px[cols] = close_w[k, cols]
    return _ret(px, ref_close, ref_atr), off


REFERENCE_POLICIES = {
    "P0_hold": policy_hold,
    "P1_atr12_trail": policy_trail,
    "P2_ema20_exit": policy_ema20,
    "P3_fixed_stop_1.5atr": policy_fixed_stop,
    "P4_time_stop_10": policy_time,
}


# ───────────────────────────────────────────────────────────────────────────
# counterfactuals — STRICTLY from bars after the exit bar
# ───────────────────────────────────────────────────────────────────────────
def counterfactuals(fw, ref_close, ref_atr, exit_off, up_mult=UP_PRIMARY):
    """captured MFE and premature-exit. Reads ONLY bars with offset > exit_off."""
    U = ref_close + up_mult * ref_atr
    n = len(ref_close)
    max_avail = (fw["high"].max(axis=0) - ref_close) / ref_atr       # whole window, for captured
    ks = np.arange(1, RACE_H + 1)[:, None]
    after = ks > exit_off[None, :]                                    # strictly after the exit bar
    would_have_hit = ((fw["high"] >= U[None, :]) & after).any(axis=0)
    return dict(max_avail_mfe=max_avail, premature=would_have_hit & (exit_off < RACE_H))


def _assert_counterfactual_causal(fw, ref_close, ref_atr, exit_off, n_check=200, seed=1):
    """The counterfactual must not change when bars up to and including the exit bar are altered."""
    rng = np.random.default_rng(seed)
    idx = rng.choice(len(ref_close), size=min(n_check, len(ref_close)), replace=False)
    base = counterfactuals({k: v[:, idx] for k, v in fw.items()},
                           ref_close[idx], ref_atr[idx], exit_off[idx])["premature"]
    tamper = {k: v[:, idx].copy() for k, v in fw.items()}
    ks = np.arange(1, RACE_H + 1)[:, None]
    upto = ks <= exit_off[idx][None, :]
    tamper["high"] = np.where(upto, tamper["high"] * 10.0, tamper["high"])   # absurd values <= exit
    got = counterfactuals(tamper, ref_close[idx], ref_atr[idx], exit_off[idx])["premature"]
    assert np.array_equal(base, got), "counterfactual reads bars at or before the exit bar"
    return True


# ───────────────────────────────────────────────────────────────────────────
# metrics and the gate
# ───────────────────────────────────────────────────────────────────────────
def policy_metrics(ret, exit_off, cf, lab, tag) -> dict:
    ok = np.isfinite(ret)
    cap = np.where(cf["max_avail_mfe"] > 0, ret / cf["max_avail_mfe"], np.nan)
    return dict(policy=tag, n=int(ok.sum()),
                exp_mean=round(float(np.nanmean(ret[ok])), 4),
                exp_median=round(float(np.nanmedian(ret[ok])), 4),
                captured_mfe_med=round(float(np.nanmedian(cap[ok])), 4),
                med_exit_bar=float(np.median(exit_off[ok])),
                premature_pct=round(float(np.nanmean(cf["premature"][ok])) * 100, 2),
                pct_win=round(float((lab[ok] == "WIN").mean()) * 100, 2),
                pct_lose=round(float((lab[ok] == "LOSE").mean()) * 100, 2),
                pct_open=round(float((lab[ok] == "OPEN").mean()) * 100, 2),
                pct_ambiguous=round(float((lab[ok] == "AMBIGUOUS").mean()) * 100, 2))


def boot_diff_ci(ret_a, ret_b, tickers, n_boot=400, seed=20260912):
    """Ticker-clustered bootstrap of the expectancy DIFFERENCE (a − b)."""
    codes, uniq = pd.factorize(tickers)
    groups = [np.flatnonzero(codes == i) for i in range(len(uniq))]
    rng = np.random.default_rng(seed)
    out = np.empty(n_boot)
    for i in range(n_boot):
        pick = rng.integers(0, len(groups), len(groups))
        sel = np.concatenate([groups[j] for j in pick])
        out[i] = np.nanmean(ret_a[sel]) - np.nanmean(ret_b[sel])
    return round(float(np.percentile(out, 2.5)), 4), round(float(np.percentile(out, 97.5)), 4)


def gate(pc: dict, ref: dict, ci: tuple, val_sign_ok: bool) -> str:
    d_mean = pc["exp_mean"] - ref["exp_mean"]
    d_med = pc["exp_median"] - ref["exp_median"]
    d_prem = pc["premature_pct"] - ref["premature_pct"]
    if d_mean < GATE_EXPECTANCY:
        return f"FAIL: mean +{d_mean:.3f} ATR < +{GATE_EXPECTANCY}"
    if not val_sign_ok:
        return "FAIL: sign not reproduced on VALIDATION"
    if d_prem > GATE_PREMATURE_PP:
        return f"FAIL: premature-exit +{d_prem:.2f} pp > +{GATE_PREMATURE_PP}"
    if ci[0] <= 0 <= ci[1]:
        return f"FAIL: clustered CI {ci} straddles zero"
    if d_med < 0:
        return f"FAIL: median expectancy worse ({d_med:+.3f}) — mean carried by outliers"
    return "PASS"
