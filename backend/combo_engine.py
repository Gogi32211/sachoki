"""
combo_engine.py — 260323 Pine Script combo signals ported to Python.

Signals computed:
  2809:   buy_2809, um_2809, svs_2809, conso_2809
  Bias:   cons_atr, bias_up, bias_down
  Breaks: atr_brk, bb_brk
  HILO:   hilo_buy, hilo_sell
  RTV:    rtv
  PREUP:  preup3, preup2, preup50, preup89
  ROCKET: rocket
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# ── Internal helpers ───────────────────────────────────────────────────────────

def _rma(series: pd.Series, period: int) -> pd.Series:
    """Wilder's Moving Average (alpha = 1/period). Used in RSI and ATR."""
    return series.ewm(alpha=1.0 / period, adjust=False).mean()


def _rsi(series: pd.Series, period: int) -> pd.Series:
    """RSI using Wilder's smoothing, matching Pine Script ta.rsi()."""
    delta = series.diff()
    up    = delta.clip(lower=0)
    down  = (-delta).clip(lower=0)
    avg_u = _rma(up, period)
    avg_d = _rma(down, period).replace(0, np.nan)
    rs    = avg_u / avg_d
    return 100.0 - (100.0 / (1.0 + rs))


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    """Average True Range using Wilder's smoothing."""
    prev_c = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_c).abs(),
        (low  - prev_c).abs(),
    ], axis=1).max(axis=1)
    return _rma(tr, period)


def _macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """Returns (macd_line, macd_signal, macd_hist)."""
    ema_f     = close.ewm(span=fast,   adjust=False).mean()
    ema_s     = close.ewm(span=slow,   adjust=False).mean()
    macd_line = ema_f - ema_s
    macd_sig  = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, macd_sig, macd_line - macd_sig


def _crossover(a: pd.Series, level: float) -> pd.Series:
    """True on bars where a crosses above level (a[i] > level and a[i-1] <= level)."""
    return (a > level) & (a.shift(1) <= level)


def _psar(high: np.ndarray, low: np.ndarray,
          af_start: float = 0.02, af_step: float = 0.02,
          af_max: float = 0.2) -> np.ndarray:
    """
    Parabolic SAR — sequential computation matching Pine Script ta.sar().
    Returns array of SAR values aligned with input arrays.
    """
    n    = len(high)
    psar = np.empty(n, dtype=np.float64)
    bull = True
    af   = af_start
    ep   = high[0]
    psar[0] = low[0]

    for i in range(1, n):
        if bull:
            psar[i] = psar[i - 1] + af * (ep - psar[i - 1])
            psar[i] = min(psar[i], low[i - 1])
            if i > 1:
                psar[i] = min(psar[i], low[i - 2])

            if low[i] < psar[i]:          # reversal → bearish
                bull    = False
                psar[i] = ep
                ep      = low[i]
                af      = af_start
            else:
                if high[i] > ep:
                    ep = high[i]
                    af = min(af + af_step, af_max)
        else:
            psar[i] = psar[i - 1] + af * (ep - psar[i - 1])
            psar[i] = max(psar[i], high[i - 1])
            if i > 1:
                psar[i] = max(psar[i], high[i - 2])

            if high[i] > psar[i]:         # reversal → bullish
                bull    = True
                psar[i] = ep
                ep      = high[i]
                af      = af_start
            else:
                if low[i] < ep:
                    ep = low[i]
                    af = min(af + af_step, af_max)

    return psar


def _apply_cooldown(series: pd.Series, cooldown: int) -> pd.Series:
    """Suppress signals within `cooldown` bars after each fired signal."""
    arr  = series.to_numpy(dtype=bool, copy=True)
    last = -(cooldown + 1)
    for i in range(len(arr)):
        if arr[i]:
            if i - last < cooldown:
                arr[i] = False
            else:
                last = i
    return pd.Series(arr, index=series.index)


# ── Main signal computation ────────────────────────────────────────────────────

def compute_combo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all 260323 combo signals from OHLCV data.

    Parameters
    ----------
    df : DataFrame with columns open, high, low, close, [volume]

    Returns
    -------
    DataFrame of boolean columns, one per signal, same index as df.
    """
    close  = df["close"]
    open_  = df["open"]
    high   = df["high"]
    low    = df["low"]
    volume = (
        df["volume"].fillna(0)
        if "volume" in df.columns
        else pd.Series(0.0, index=df.index)
    )

    # ── Common indicators ─────────────────────────────────────────────────────
    ema9  = close.ewm(span=9,  adjust=False).mean()
    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    ema89 = close.ewm(span=89, adjust=False).mean()

    avg_vol  = volume.rolling(20, min_periods=1).mean().replace(0, np.nan)
    rsi14    = _rsi(close, 14)
    atr14    = _atr(high, low, close, 14)

    bb_basis = close.rolling(20, min_periods=1).mean()
    bb_std   = close.rolling(20, min_periods=1).std()
    bb_upper = bb_basis + 2.0 * bb_std

    prev_close = close.shift(1).replace(0, np.nan)

    # ── 2809: upmove ──────────────────────────────────────────────────────────
    trend_up   = (ema9 > ema20) & (ema20 > ema50)
    roc_40     = (close - close.shift(40)) / close.shift(40).replace(0, np.nan) * 100.0
    vol_max40  = volume.rolling(40, min_periods=10).max()
    upmove_ok  = trend_up & (roc_40 >= 8.0) & (vol_max40 > avg_vol * 1.4)

    # UM phase label: first bar upmove_ok becomes True
    um_2809 = upmove_ok & ~upmove_ok.shift(1).fillna(False)

    # ── 2809: consolidation gate ──────────────────────────────────────────────
    cons_high = high.shift(1).rolling(6, min_periods=6).max()
    cons_low  = low.shift(1).rolling(6, min_periods=6).min()

    cons_range_pct = (cons_high - cons_low) / prev_close * 100.0
    tight_by_range = cons_range_pct <= 3.5

    atr_pct      = atr14 / prev_close * 100.0
    tight_by_atr = atr_pct <= 3.0

    ema_gap_pct  = (ema9.shift(1) - ema20.shift(1)).abs() / ema20.shift(1).replace(0, np.nan) * 100.0
    tight_by_ema = ema_gap_pct <= 2.0

    tight_gate = tight_by_range | tight_by_atr | tight_by_ema

    # CONSO phase label: first bar tight_gate becomes True
    conso_2809 = tight_gate & ~tight_gate.shift(1).fillna(False)

    # NR7 alternative gate
    bar_range = high - low
    nr_min    = bar_range.rolling(7, min_periods=4).min().shift(1)
    nr7_prev  = bar_range.shift(1) <= nr_min + 1e-10
    nr_pct_ok = (bar_range.shift(1) / prev_close * 100.0) <= 5.0
    alt_gate  = nr7_prev & nr_pct_ok

    # ── 2809: breakout + cooldown ─────────────────────────────────────────────
    brk_cons  = (high > cons_high) & (volume > avg_vol * 1.2)
    brk_nr    = (high > high.shift(1)) & (volume > avg_vol * 1.2)
    break_gate = (tight_gate & brk_cons) | (alt_gate & brk_nr)

    buy_raw  = upmove_ok & break_gate
    buy_2809 = _apply_cooldown(buy_raw, cooldown=6)

    # SVS: volume ratio crosses threshold on a green bar
    vol_ratio = volume / avg_vol
    svs_2809  = _crossover(vol_ratio, 1.4) & (close > open_)

    # ── CONS ATR + Bias ───────────────────────────────────────────────────────
    atr_ma14 = atr14.rolling(14, min_periods=1).mean().replace(0, np.nan)
    cons_atr  = atr14 < atr_ma14 * 0.80

    _, _, macd_hist = _macd(close)

    # Score: EMA trend=±2, BB pos=±1, RSI=±1, MACD=±1  → range -5..+5
    score = pd.Series(
        np.where((ema9 > ema20) & (ema20 > ema50),  2.0,
        np.where((ema9 < ema20) & (ema20 < ema50), -2.0, 0.0)),
        index=df.index,
    )
    score = score + np.where(close > bb_basis,  1.0, -1.0)
    score = score + np.where(rsi14 > 50,        1.0, -1.0)
    score = score + np.where(macd_hist > 0,     1.0, -1.0)

    bias_up   = cons_atr & (score >= 2)
    bias_down = cons_atr & (score <= -2)

    # ── ATR Breakout ──────────────────────────────────────────────────────────
    atr10           = _atr(high, low, close, 10)
    consol_atr      = atr10 < atr10.shift(1) * 0.6
    avg_vol20       = volume.rolling(20, min_periods=1).mean().replace(0, np.nan)
    highest_prev10  = high.shift(1).rolling(10, min_periods=1).max()
    brk_conf_atr    = (close > highest_prev10) & (close.shift(1) <= highest_prev10.shift(1))
    vol_ratio_atr   = volume / avg_vol20
    vol_conf_atr    = _crossover(vol_ratio_atr, 2.0)
    atr_brk         = consol_atr & brk_conf_atr & vol_conf_atr

    # ── BB Breakout ───────────────────────────────────────────────────────────
    vol_ma_boll      = volume.rolling(20, min_periods=1).mean().replace(0, np.nan)
    vol_ratio_boll   = volume / vol_ma_boll
    vol_conf_boll    = _crossover(vol_ratio_boll, 1.5)
    bb_brk           = (close > bb_upper) & vol_conf_boll & (rsi14 > 55)

    # ── HILO (RSI-2 anchors) ──────────────────────────────────────────────────
    rsi2      = _rsi(close, 2)
    hilo_buy  = _crossover(rsi2, 20.0)
    hilo_sell = (rsi2.shift(1) > 80) & (rsi2 < 80)

    # ── RTV (RSI-2 reversal + Williams VIX Fix) ───────────────────────────────
    rsi_buy_rtv = _crossover(rsi2, 20.0)
    bearish_rtv = (close.shift(1) < open_.shift(1)) | (close.shift(2) < open_.shift(2))
    reversal    = close > open_

    roll22_max  = close.rolling(22, min_periods=1).max().replace(0, np.nan)
    wvf         = (roll22_max - low) / roll22_max * 100.0
    wvf_mid     = wvf.rolling(20, min_periods=1).mean()
    wvf_upper   = wvf_mid + wvf.rolling(20, min_periods=1).std() * 2.0
    wvf_rng_hi  = wvf.rolling(50, min_periods=1).max() * 0.85
    vix_fired   = (wvf >= wvf_upper) | (wvf >= wvf_rng_hi)
    vix_conf    = vix_fired | vix_fired.shift(1).fillna(False)
    rtv         = rsi_buy_rtv & bearish_rtv & reversal & vix_conf

    # ── PREUP (EMA crosses) ───────────────────────────────────────────────────
    cx9  = (open_ < ema9)  & (close > ema9)
    cx20 = (open_ < ema20) & (close > ema20)
    cx50 = (open_ < ema50) & (close > ema50)
    cx89 = (open_ < ema89) & (close > ema89)

    preup3  = cx9 & cx20 & cx50
    preup2  = cx9 & cx20 & ~preup3
    preup50 = cx50 & ~cx9 & ~cx20
    preup89 = cx89

    # ── 3G (Gap above EMA9 + EMA20 + EMA50) ──────────────────────────────────
    # prev bar close fully below all 3 EMAs;
    # current bar opens AND closes fully above all 3 EMAs
    prev_below_all3  = (close.shift(1) < ema9.shift(1)) & \
                       (close.shift(1) < ema20.shift(1)) & \
                       (close.shift(1) < ema50.shift(1))
    curr_open_above  = (open_ > ema9) & (open_ > ema20) & (open_ > ema50)
    curr_close_above = (close > ema9) & (close > ema20) & (close > ema50)
    sig3g            = prev_below_all3 & curr_open_above & curr_close_above

    # ── ROCKET ────────────────────────────────────────────────────────────────
    psar_arr    = _psar(high.to_numpy(), low.to_numpy())
    psar_s      = pd.Series(psar_arr, index=df.index)
    strong_bull = (close - open_) / open_.replace(0, np.nan) * 100.0 > 2.0
    vol_burst   = volume > avg_vol * 2.0
    rocket      = buy_2809 & strong_bull & vol_burst & (close > psar_s)

    # ── Assemble ──────────────────────────────────────────────────────────────
    out = pd.DataFrame({
        "buy_2809":   buy_2809,
        "um_2809":    um_2809,
        "svs_2809":   svs_2809,
        "conso_2809": conso_2809,
        "cons_atr":   cons_atr,
        "bias_up":    bias_up,
        "bias_down":  bias_down,
        "atr_brk":    atr_brk,
        "bb_brk":     bb_brk,
        "hilo_buy":   hilo_buy,
        "hilo_sell":  hilo_sell,
        "rtv":        rtv,
        "preup3":     preup3,
        "preup2":     preup2,
        "preup50":    preup50,
        "preup89":    preup89,
        "sig3g":      sig3g,
        "rocket":     rocket,
    }, index=df.index)

    return out.fillna(False)


# ── Signal priority and labels (for scan display) ──────────────────────────────

COMBO_SIGNAL_PRIORITY = [
    "rocket", "buy_2809", "sig3g", "bb_brk", "atr_brk", "rtv",
    "preup3", "preup2", "preup50", "preup89",
    "hilo_buy", "hilo_sell", "bias_up", "bias_down",
    "cons_atr", "um_2809", "svs_2809", "conso_2809",
]

COMBO_LABELS = {
    "rocket":    "ROCKET",
    "buy_2809":  "BUY",
    "sig3g":     "3G",
    "bb_brk":    "BB↑",
    "atr_brk":   "ATR↑",
    "rtv":       "RTV",
    "preup3":    "P3",
    "preup2":    "P2",
    "preup50":   "P50",
    "preup89":   "P89",
    "hilo_buy":  "HILO↑",
    "hilo_sell": "HILO↓",
    "bias_up":   "↑BIAS",
    "bias_down": "↓BIAS",
    "cons_atr":  "CONS",
    "um_2809":   "UM",
    "svs_2809":  "SVS",
    "conso_2809":"CONSO",
}


def last_n_active(combo_df: pd.DataFrame, n: int = 7) -> dict[str, bool]:
    """
    For each signal column, return True if it fired at any point in the last n bars.
    State signals (cons_atr, bias_up, bias_down) use last bar only.
    """
    STATE_SIGNALS = {"cons_atr", "bias_up", "bias_down"}
    tail = combo_df.tail(n)
    last = combo_df.iloc[-1] if len(combo_df) > 0 else combo_df.iloc[0]

    result: dict[str, bool] = {}
    for col in combo_df.columns:
        if col in STATE_SIGNALS:
            result[col] = bool(last[col])
        else:
            result[col] = bool(tail[col].any())
    return result


def active_signal_labels(active: dict[str, bool]) -> list[str]:
    """Return ordered list of human-readable labels for active signals."""
    return [
        COMBO_LABELS[k]
        for k in COMBO_SIGNAL_PRIORITY
        if active.get(k)
    ]
