"""
wyckoff_v2_engine.py — Wyckoff Accumulation V2 Soft.

Faithful translation of Pine "260529 V2 Soft — Wyckoff Accumulation (Bullish)".

Full cycle state machine, anchored to a real Selling Climax:
    0 idle → 1 SC → 2 AR → 3 ST → 4 SPRING → 5 SOS/JAC → 6 LPS → reset

Per-bar boolean outputs (fire on the CONFIRMATION bar — leak-free for screening;
the pivot-based stages (SC/AR/ST/LPS) are confirmed `pivotLen` bars after the
pivot they describe, exactly like the Pine):
    w2_sc w2_ar w2_st w2_spring w2_sos w2_jac w2_lps w2_evr
    w2_accum (state 1-4 active)   w2_break (state 5-6)   w2_state (0-6)
    w2_tr_quality (float 0..1)

Defaults mirror the Pine "V2 Soft": only the SOS/JAC breakout uses a hard volume
gate; SC/AR/ST/LPS volume filters are OFF (soft), CLV gates ON in soft mode.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# ── Pine input defaults ───────────────────────────────────────────────────────
_PIVOT_LEN   = 6
_CYCLE_MAX   = 160
_EMA_FAST    = 14
_EMA_SLOW    = 50
_ATR_LEN     = 14
_VOL_MA_LEN  = 20

_USE_VOL_CLIMAX = False    # SC / AR / SPRING climax vol gate
_USE_VOL_BREAK  = True     # SOS / JAC breakout vol gate
_USE_VOL_RETEST = False    # ST / LPS retest vol gate
_HI_VOL_MULT = 1.5
_LOW_VOL_MULT= 0.9
_AR_VOL_MULT = 0.9
_JAC_VOL_MULT= 1.7

_WIDE_SPREAD_ATR = 1.0
_SPRING_ATR  = 0.45
_RECLAIM_ATR = 0.05
_BREAK_BUF_ATR = 0.03
_JAC_EXTRA_ATR = 0.08
_LPS_TOL_ATR = 0.9
_AR_SPREAD_ATR = 0.5
_JAC_BODY_ATR  = 0.25

_USE_CLV = True
_SOFT    = True            # soft quality mode
# soft CLV thresholds
_SC_CLV  = -0.75
_ST_CLV  = -0.90
_SPR_CLV = -0.35
_SOS_CLV =  0.10
_JAC_CLV =  0.35
_LPS_CLV = -0.75
_AR_CLV  = -0.05

# TR-quality (gate off by default: q_min_score=0)
_Q_LOOKBACK = 90
_Q_MIN_SCORE = 0.0
_Q_TEST_TOL = 0.035
_Q_MAX_DRIFT = 18.0

# EVR
_USE_EVR = True
_EVR_VOL_MULT = 1.5
_EVR_MAX_MOVE = 2.0        # %

_COLS = ["w2_sc", "w2_ar", "w2_st", "w2_spring", "w2_sos", "w2_jac",
         "w2_lps", "w2_evr", "w2_accum", "w2_break", "w2_state", "w2_tr_quality"]


def _ema(a: np.ndarray, span: int) -> np.ndarray:
    return pd.Series(a).ewm(span=span, adjust=False).mean().values


def _atr(h, l, c, span=_ATR_LEN) -> np.ndarray:
    prev = np.concatenate([[c[0]], c[:-1]])
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev), np.abs(l - prev)))
    return pd.Series(tr).ewm(span=span, adjust=False).mean().values  # Pine ta.atr = RMA≈EMA


def _pivot_low(low: np.ndarray, lr: int) -> np.ndarray:
    """ta.pivotlow(low, lr, lr): True at the CONFIRMATION bar i (pivot sits at i-lr).
    Pivot bar j is a low if low[j] < low[j±k] for all k in 1..lr."""
    n = len(low)
    out = np.zeros(n, dtype=bool)
    for i in range(2 * lr, n):
        j = i - lr
        lj = low[j]
        if (lj < low[j - lr:j]).all() and (lj < low[j + 1:j + lr + 1]).all():
            out[i] = True
    return out


def _pivot_high(high: np.ndarray, lr: int) -> np.ndarray:
    n = len(high)
    out = np.zeros(n, dtype=bool)
    for i in range(2 * lr, n):
        j = i - lr
        hj = high[j]
        if (hj > high[j - lr:j]).all() and (hj > high[j + 1:j + lr + 1]).all():
            out[i] = True
    return out


def compute_wyckoff_v2(df: pd.DataFrame) -> pd.DataFrame:
    n = len(df)
    if n < _Q_LOOKBACK + _PIVOT_LEN + 2:
        return pd.DataFrame(0, index=df.index, columns=_COLS).astype(
            {**{k: np.int8 for k in _COLS if k != "w2_tr_quality"}, "w2_tr_quality": float})

    o = df["open"].to_numpy(float); h = df["high"].to_numpy(float)
    l = df["low"].to_numpy(float);  c = df["close"].to_numpy(float)
    v = np.nan_to_num(df["volume"].to_numpy(float), nan=0.0)

    atr   = _atr(h, l, c); atr = np.where(atr > 0, atr, 1e-9)
    emaF  = _ema(c, _EMA_FAST); emaS = _ema(c, _EMA_SLOW)
    volMA = pd.Series(v).rolling(_VOL_MA_LEN, min_periods=1).mean().to_numpy()
    volMA = np.where(volMA > 0, volMA, 1.0)

    rng   = h - l
    rsafe = np.where(rng > 0, rng, 1e-9)
    clv   = (2 * c - h - l) / rsafe
    body  = np.abs(c - o)
    closePos = (c - l) / rsafe
    upTrend  = emaF > emaS
    downTrend= emaF < emaS

    pL = _pivot_low(l, _PIVOT_LEN)
    pH = _pivot_high(h, _PIVOT_LEN)

    # TR quality (rolling)
    qSup = pd.Series(l).rolling(_Q_LOOKBACK, min_periods=_Q_LOOKBACK).min().to_numpy()
    qRes = pd.Series(h).rolling(_Q_LOOKBACK, min_periods=_Q_LOOKBACK).max().to_numpy()
    qFirst = np.concatenate([[np.nan] * (_Q_LOOKBACK - 1), c[:n - _Q_LOOKBACK + 1]])
    with np.errstate(all="ignore"):
        widthPct = np.where(qSup > 0, (qRes - qSup) / qSup * 100.0, np.nan)
        driftPct = np.where(qFirst > 0, np.abs((c - qFirst) / qFirst * 100.0), np.nan)
    supTests = pd.Series((l <= qSup * (1 + _Q_TEST_TOL)).astype(float)).rolling(_Q_LOOKBACK, min_periods=1).sum().to_numpy()
    resTests = pd.Series((h >= qRes * (1 - _Q_TEST_TOL)).astype(float)).rolling(_Q_LOOKBACK, min_periods=1).sum().to_numpy()
    testScore  = np.minimum((supTests + resTests) / 8.0, 1.0)
    widthScore = np.maximum(0.0, 1.0 - np.abs(np.nan_to_num(widthPct) - 18.0) / 30.0)
    driftScore = np.maximum(0.0, 1.0 - np.nan_to_num(driftPct) / _Q_MAX_DRIFT)
    trQuality  = 0.45 * testScore + 0.35 * widthScore + 0.20 * driftScore
    qualityOk  = (_Q_MIN_SCORE <= 0.0)  # gate off by default

    LR = _PIVOT_LEN
    def volHi(off_i, mult, use):   # f_volHighAt
        return (not use) or (v[off_i] >= volMA[off_i] * mult)
    def volLo(off_i, mult, use):
        return (not use) or (v[off_i] <= volMA[off_i] * mult)

    w2_sc = np.zeros(n, np.int8); w2_ar = np.zeros(n, np.int8); w2_st = np.zeros(n, np.int8)
    w2_spr = np.zeros(n, np.int8); w2_sos = np.zeros(n, np.int8); w2_jac = np.zeros(n, np.int8)
    w2_lps = np.zeros(n, np.int8); w2_evr = np.zeros(n, np.int8); w2_state = np.zeros(n, np.int8)

    st = 0; startA = -1; supA = np.nan; resA = np.nan; breakA = np.nan
    lastA = -1; scVolA = np.nan

    for i in range(2 * LR, n):
        a = atr[i]
        # reset
        if (st != 0 and startA >= 0 and i - startA > _CYCLE_MAX) or \
           (st >= 1 and not np.isnan(supA) and c[i] < supA - a * 2.0 and downTrend[i]):
            st = 0; startA = -1; supA = resA = breakA = np.nan; lastA = -1; scVolA = np.nan

        pj = i - LR  # pivot bar index (for pivot-based stages)

        # SC
        if st == 0 and pL[i] and downTrend[pj] and \
           (h[pj] - l[pj]) >= atr[pj] * _WIDE_SPREAD_ATR and \
           volHi(pj, _HI_VOL_MULT, _USE_VOL_CLIMAX) and \
           ((not _USE_CLV) or clv[pj] > _SC_CLV) and qualityOk:
            st = 1; startA = pj; supA = l[pj]; resA = np.nan; breakA = np.nan
            lastA = pj; scVolA = v[pj]; w2_sc[i] = 1

        # AR
        elif st == 1 and pH[i] and pj > lastA and h[pj] > (supA if not np.isnan(supA) else h[pj]) and \
             c[pj] >= o[pj] and (h[pj] - l[pj]) >= atr[pj] * _AR_SPREAD_ATR and \
             ((not _USE_CLV) or clv[pj] > _AR_CLV) and volHi(pj, _AR_VOL_MULT, _USE_VOL_CLIMAX):
            st = 2; resA = h[pj]; lastA = pj; w2_ar[i] = 1

        # ST
        elif st == 2 and pL[i] and not np.isnan(supA) and \
             abs(l[pj] - supA) <= atr[pj] * 0.9 and \
             volLo(pj, _LOW_VOL_MULT, _USE_VOL_RETEST) and \
             ((not _USE_CLV) or clv[pj] > _ST_CLV):
            st = 3; lastA = pj; w2_st[i] = 1

        # SPRING (state 2..4)
        if 2 <= st < 5 and not np.isnan(supA) and \
           l[i] < supA - a * _SPRING_ATR and c[i] > supA + a * _RECLAIM_ATR and \
           ((not _USE_CLV) or clv[i] > _SPR_CLV) and \
           ((not _USE_VOL_CLIMAX) or np.isnan(scVolA) or v[i] <= scVolA * 1.25):
            st = max(st, 4); lastA = i; w2_spr[i] = 1

        # SOS / JAC (state 2..5)
        if 2 <= st < 6 and not np.isnan(resA):
            jac_raw = c[i] > resA + a * (_BREAK_BUF_ATR + _JAC_EXTRA_ATR) and \
                      ((not _USE_CLV) or clv[i] > _JAC_CLV) and \
                      volHi(i, _JAC_VOL_MULT, _USE_VOL_BREAK) and upTrend[i] and \
                      body[i] >= a * _JAC_BODY_ATR and closePos[i] >= 0.60
            sos_raw = c[i] > resA + a * _BREAK_BUF_ATR and \
                      ((not _USE_CLV) or clv[i] > _SOS_CLV) and \
                      volHi(i, _HI_VOL_MULT, _USE_VOL_BREAK) and upTrend[i]
            if jac_raw or (sos_raw and not jac_raw):
                if jac_raw: w2_jac[i] = 1
                else:       w2_sos[i] = 1
                st = 5; breakA = resA; lastA = i

        # LPS
        if st == 5 and not np.isnan(breakA) and pL[i] and \
           abs(l[pj] - breakA) <= atr[pj] * _LPS_TOL_ATR and \
           volLo(pj, _LOW_VOL_MULT, _USE_VOL_RETEST) and \
           ((not _USE_CLV) or clv[pj] > _LPS_CLV):
            st = 6; lastA = pj; w2_lps[i] = 1

        # EVR (absorption) — state 1..5
        if _USE_EVR and 1 <= st < 6 and not np.isnan(supA) and not np.isnan(resA):
            mid = supA + (resA - supA) / 2.0
            dayMv = abs((c[i] - c[i - 1]) / c[i - 1] * 100.0) if c[i - 1] > 0 else 1e9
            if v[i] >= volMA[i] * _EVR_VOL_MULT and c[i] <= mid and \
               dayMv <= _EVR_MAX_MOVE and c[i] >= supA * 0.98:
                w2_evr[i] = 1

        # LPS completes the cycle → reset to idle for next cycle
        if st == 6:
            st = 0
        w2_state[i] = st

    res = pd.DataFrame(index=df.index)
    res["w2_sc"] = w2_sc; res["w2_ar"] = w2_ar; res["w2_st"] = w2_st
    res["w2_spring"] = w2_spr; res["w2_sos"] = w2_sos; res["w2_jac"] = w2_jac
    res["w2_lps"] = w2_lps; res["w2_evr"] = w2_evr
    res["w2_accum"] = ((w2_state >= 1) & (w2_state <= 4)).astype(np.int8)
    res["w2_break"] = (w2_state >= 5).astype(np.int8)
    res["w2_state"] = w2_state
    res["w2_tr_quality"] = np.round(trQuality, 3)
    return res
