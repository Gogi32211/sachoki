"""
wyckoff_trig_engine.py — Wyckoff Structure Triggers (Spring / LPS / SOS / EVR).

Faithful port of Pine "260529_WYCK_TRIG", itself a port of
WyckoffTradingAgent core/wyckoff_v2_structure.py (FunnelConfig defaults).

NOT a state machine — it builds a trading range (median of recent swing
pivots + quality gate), then fires structural triggers inside a *valid* TR.
No-lookahead: swing pivots are confirmed `swWin` bars in the past; all
volume references exclude the current bar.

Per-bar outputs:
    wt_valid_tr (0/1)   wt_quality (0..1)   wt_support  wt_resistance
    wt_sos wt_spring wt_lps wt_evr (0/1, fire on the bar)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# ── FunnelConfig defaults (mirroring the Pine inputs) ─────────────────────────
_SW_WIN      = 3
_LOOKBACK    = 90
_N_SWING_MED = 5
_MAX_RANGE_PCT = 45.0
_MAX_DRIFT_PCT = 18.0
_MIN_RANGE_PCT = 4.0
_TEST_TOL    = 0.035

_SOS_VOL_WIN = 20
_SOS_PCT_MIN = 6.0
_SOS_VOL_RATIO = 2.5
_SOS_TOL     = 0.01
_SPR_VOL_RATIO = 1.3
_LPS_LB      = 3
_LPS_REF_WIN = 60
_LPS_DRY     = 0.50
_EVR_VOL_WIN = 20
_EVR_VOL_RATIO = 1.5

_COLS = ["wt_valid_tr", "wt_quality", "wt_support", "wt_resistance",
         "wt_sos", "wt_spring", "wt_lps", "wt_evr"]


def _pivot_low(low, lr):
    n = len(low); out = np.zeros(n, dtype=bool)
    for i in range(2 * lr, n):
        j = i - lr; lj = low[j]
        if (lj < low[j - lr:j]).all() and (lj < low[j + 1:j + lr + 1]).all():
            out[i] = True
    return out


def _pivot_high(high, lr):
    n = len(high); out = np.zeros(n, dtype=bool)
    for i in range(2 * lr, n):
        j = i - lr; hj = high[j]
        if (hj > high[j - lr:j]).all() and (hj > high[j + 1:j + lr + 1]).all():
            out[i] = True
    return out


def _median_last_n(vals: list[float], n: int):
    if len(vals) < 2:
        return np.nan
    take = vals[-min(n, len(vals)):]
    return float(np.median(take))


def compute_wyckoff_trig(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n < _LOOKBACK + 2 * _SW_WIN + 2:
        out = pd.DataFrame(0, index=df.index, columns=_COLS)
        out["wt_quality"] = 0.0; out["wt_support"] = np.nan; out["wt_resistance"] = np.nan
        return out

    h = df["high"].to_numpy(float); l = df["low"].to_numpy(float)
    c = df["close"].to_numpy(float)
    v = np.nan_to_num(df["volume"].to_numpy(float), nan=0.0)

    pl = _pivot_low(l, _SW_WIN)
    ph = _pivot_high(h, _SW_WIN)

    low90  = pd.Series(l).rolling(_LOOKBACK, min_periods=1).min().to_numpy()
    high90 = pd.Series(h).rolling(_LOOKBACK, min_periods=1).max().to_numpy()

    # vol ratio: volume / SMA(volume[1], win)  (ref excludes current bar)
    def vol_ratio(win):
        ref = pd.Series(v).shift(1).rolling(win, min_periods=win).mean().to_numpy()
        return np.where(ref > 0, v / ref, np.nan)
    vr_sos = vol_ratio(_SOS_VOL_WIN)
    vr_spr = vol_ratio(5)
    vr_evr = vol_ratio(_EVR_VOL_WIN)
    day_pct = np.concatenate([[0.0], np.where(c[:-1] > 0, (c[1:] - c[:-1]) / c[:-1] * 100.0, 0.0)])

    # LPS dry-volume: highest(vol,3) / highest(vol,60)[3]
    recent_vmax = pd.Series(v).rolling(_LPS_LB, min_periods=1).max().to_numpy()
    ref_vmax    = pd.Series(v).rolling(_LPS_REF_WIN, min_periods=1).max().shift(_LPS_LB).to_numpy()
    near_sup_low = pd.Series(l).rolling(_LPS_LB, min_periods=1).min().to_numpy()

    # ── per-bar support/resistance from rolling swing-pivot medians ──
    support = np.full(n, np.nan); resistance = np.full(n, np.nan)
    sw_lows: list[float] = []; sw_highs: list[float] = []
    for i in range(n):
        if pl[i]:
            sw_lows.append(l[i - _SW_WIN]); sw_lows[:] = sw_lows[-50:]
        if ph[i]:
            sw_highs.append(h[i - _SW_WIN]); sw_highs[:] = sw_highs[-50:]
        sup = _median_last_n(sw_lows, _N_SWING_MED)
        res = _median_last_n(sw_highs, _N_SWING_MED)
        if np.isnan(sup): sup = low90[i]
        if np.isnan(res): res = high90[i]
        support[i] = sup; resistance[i] = res

    width = resistance - support
    mid   = support + width / 2.0
    with np.errstate(all="ignore"):
        width_pct = np.where(support > 0, width / support * 100.0, np.nan)
    first_close = np.concatenate([[np.nan] * (_LOOKBACK - 1), c[:n - _LOOKBACK + 1]])
    with np.errstate(all="ignore"):
        drift_pct = np.where(first_close > 0, np.abs((c - first_close) / first_close * 100.0), np.nan)
    sup_tests = pd.Series((l <= support * (1 + _TEST_TOL)).astype(float)).rolling(_LOOKBACK, min_periods=1).sum().to_numpy()
    res_tests = pd.Series((h >= resistance * (1 - _TEST_TOL)).astype(float)).rolling(_LOOKBACK, min_periods=1).sum().to_numpy()

    test_score  = np.minimum((sup_tests + res_tests) / 8.0, 1.0)
    width_score = np.maximum(0.0, 1.0 - np.abs(np.nan_to_num(width_pct) - 18.0) / 30.0)
    drift_score = np.maximum(0.0, 1.0 - np.nan_to_num(drift_pct) / _MAX_DRIFT_PCT)
    quality = 0.45 * test_score + 0.35 * width_score + 0.20 * drift_score

    valid_tr = (
        (support > 0) & (resistance > support) & ~np.isnan(width_pct)
        & (width_pct >= _MIN_RANGE_PCT) & (width_pct <= _MAX_RANGE_PCT)
        & (np.nan_to_num(drift_pct) <= _MAX_DRIFT_PCT)
        & (sup_tests >= 2) & (res_tests >= 2)
    )

    # ── triggers (vectorised, inside a valid TR) ──
    sos = valid_tr & ~np.isnan(vr_sos) & (c >= resistance * (1 - _SOS_TOL)) \
        & (day_pct >= _SOS_PCT_MIN) & (vr_sos >= _SOS_VOL_RATIO)

    low_prev = np.concatenate([[l[0]], l[:-1]])
    pierced = np.minimum(l, low_prev) <= support * 0.995
    recovered = c > support * 1.005
    still_in = c < mid + width * 0.25
    spring = valid_tr & ~np.isnan(vr_spr) & pierced & recovered & still_in & (vr_spr >= _SPR_VOL_RATIO)

    with np.errstate(all="ignore"):
        dry_ratio = np.where(ref_vmax > 0, recent_vmax / ref_vmax, np.nan)
    near_sup = near_sup_low <= support + width * 0.35
    holds = c > support
    lps = valid_tr & ~np.isnan(dry_ratio) & near_sup & holds & (dry_ratio <= _LPS_DRY)

    evr = valid_tr & ~np.isnan(vr_evr) & (vr_evr >= _EVR_VOL_RATIO) & (c <= mid) & (c >= support * 0.98)

    res_df = pd.DataFrame(index=df.index)
    res_df["wt_valid_tr"]   = valid_tr.astype(np.int8)
    res_df["wt_quality"]    = np.round(quality, 3)
    res_df["wt_support"]    = np.round(support, 4)
    res_df["wt_resistance"] = np.round(resistance, 4)
    res_df["wt_sos"]    = sos.astype(np.int8)
    res_df["wt_spring"] = spring.astype(np.int8)
    res_df["wt_lps"]    = lps.astype(np.int8)
    res_df["wt_evr"]    = evr.astype(np.int8)
    return res_df
