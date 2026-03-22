"""
wlnbb_engine.py — Volume Bollinger Bands L-signal engine.

Computes per bar:
  bucket label (W/L/N/B/VB)
  L34, L43, L64, L22  — combined L-signals
  BLUE, FRI34, UI
  FUCHSIA_RH, FUCHSIA_RL
  PRE_PUMP  (VSA accumulation cluster)
  CCI_READY
  BO_UP, BO_DN, BX_UP, BX_DN
  vol_zscore, rsi, cci_sma
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# ── Parameters ───────────────────────────────────────────────────────────────
_BB_PERIOD   = 20
_BB_STD      = 1
_RSI_PERIOD  = 14
_CCI_PERIOD  = 20
_CCI_SMA     = 14
_BLUE_Z      = 1.1
_BLUE_FLAT   = 5.0      # RSI range <= 5 over last 3 bars
_PP_WINDOW   = 20       # PRE_PUMP: count VSA hits in last N bars
_PP_MIN      = 2        # minimum hits to fire
_PP_COOL     = 6        # cooldown bars after firing


# ── Public API ────────────────────────────────────────────────────────────────

def compute_wlnbb(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parameters
    ----------
    df : DataFrame with lowercase columns open, high, low, close, volume.

    Returns
    -------
    DataFrame (same index) with all WLNBB columns.
    """
    df = _norm(df)
    o = df["open"]
    h = df["high"]
    l = df["low"]
    c = df["close"]
    v = df["volume"]

    # ── Volume Bollinger Bands ────────────────────────────────────────────
    vol_mid   = v.rolling(_BB_PERIOD, min_periods=1).mean()
    vol_std   = v.rolling(_BB_PERIOD, min_periods=1).std().fillna(0)
    vol_upper = vol_mid + _BB_STD * vol_std
    vol_lower = (vol_mid - _BB_STD * vol_std).clip(lower=0)

    # ── Volume Buckets (W=0 L=1 N=2 B=3 VB=4) ────────────────────────────
    bkt = np.zeros(len(v), dtype=np.int8)
    bkt = np.where(v.values >= vol_lower.values, 1, bkt)
    bkt = np.where(v.values >= vol_mid.values,   2, bkt)
    bkt = np.where(v.values >= vol_upper.values, 3, bkt)
    bkt = np.where(v.values >= (vol_upper + vol_mid).values, 4, bkt)
    bucket = pd.Series(bkt.astype(np.int8), index=df.index)

    # ── Volume Direction ──────────────────────────────────────────────────
    pb = bucket.shift(1)
    pv = v.shift(1)
    vol_up   = ((bucket > pb) | ((bucket == pb) & (v > pv))).fillna(False)
    vol_down = ((bucket < pb) | ((bucket == pb) & (v < pv))).fillna(False)

    # ── L-signal raws ─────────────────────────────────────────────────────
    l3r = vol_up & (c > c.shift(1))
    l4r = vol_up & (c <= h.shift(1))
    l6r = vol_up & (c < c.shift(1))

    # ── Combined L-signals ────────────────────────────────────────────────
    L34 = l3r & l4r & (c >= o)   # quiet bull accumulation
    L43 = l6r & l4r & (c >  o)   # bullish reversal
    L64 = l6r & l4r & (c <  o)   # bearish
    L22 = l3r & l4r & (c <  o)   # distribution (bear candle)

    # ── RSI (14) ──────────────────────────────────────────────────────────
    rsi = _rsi(c, _RSI_PERIOD)

    # ── Volume z-score ────────────────────────────────────────────────────
    vol_z = ((v - vol_mid) / vol_std.replace(0, np.nan)).fillna(0)

    # ── BLUE: z-spike + RSI flat ──────────────────────────────────────────
    rsi_range3 = (rsi.rolling(3, min_periods=1).max()
                  - rsi.rolling(3, min_periods=1).min())
    BLUE  = (vol_z >= _BLUE_Z) & (rsi_range3 <= _BLUE_FLAT)
    FRI34 = BLUE & L34
    UI    = (BLUE.astype(int).rolling(10, min_periods=1).sum() >= 2)

    # ── FUCHSIA ───────────────────────────────────────────────────────────
    rsi_roll_max = rsi.rolling(50, min_periods=1).max().shift(1)
    rsi_roll_min = rsi.rolling(50, min_periods=1).min().shift(1)
    FUCHSIA_RH = (rsi >= rsi_roll_max.fillna(rsi)) & ~vol_up
    FUCHSIA_RL = (rsi <= rsi_roll_min.fillna(rsi)) & ~vol_up

    # ── CCI ───────────────────────────────────────────────────────────────
    cci     = _cci(h, l, c, _CCI_PERIOD)
    cci_sma = cci.rolling(_CCI_SMA, min_periods=1).mean()
    cci_rng5 = (cci_sma.rolling(5, min_periods=1).max()
                - cci_sma.rolling(5, min_periods=1).min())
    CCI_READY = (
        (cci_sma >= -110) & (cci_sma <= -50)
        & (cci_rng5 <= 15)
        & (cci_sma.diff() > 0)
        & (c > o)
    )

    # ── VSA / PRE_PUMP ────────────────────────────────────────────────────
    avg_rng = (h - l).rolling(20, min_periods=1).mean()
    avg_vol = v.rolling(20, min_periods=1).mean()
    rng     = h - l
    mid_px  = (h + l) / 2.0

    squat    = (rng < avg_rng * 0.7) & (v > avg_vol * 1.5)
    nosupply = (rng < avg_rng * 0.7) & (v < avg_vol * 0.7) & (c > mid_px)
    nod      = (rng < avg_rng * 0.7) & (v < avg_vol * 0.7) & (c <= mid_px)
    climax   = (rng > avg_rng * 1.5) & (v > avg_vol * 2.0)

    vsa_hits = (squat | nosupply | nod | climax).astype(int)
    vsa_sum  = vsa_hits.rolling(_PP_WINDOW, min_periods=1).sum()
    PRE_PUMP = _cooldown(vsa_sum >= _PP_MIN, _PP_COOL)

    # ── BO / BX levels ────────────────────────────────────────────────────
    l34_hi = _ffill_when(h, L34)
    l34_lo = _ffill_when(l, L34)
    l43_hi = _ffill_when(h, L43)
    l43_lo = _ffill_when(l, L43)

    prev_above_l34 = (c.shift(1) > l34_hi.shift(1)).fillna(False)
    prev_below_l34 = (c.shift(1) < l34_lo.shift(1)).fillna(False)
    prev_above_l43 = (c.shift(1) > l43_hi.shift(1)).fillna(False)
    prev_below_l43 = (c.shift(1) < l43_lo.shift(1)).fillna(False)

    BO_UP = (c > l34_hi) & ~prev_above_l34 & ~L34 & (l34_hi > 0)
    BO_DN = (c < l34_lo) & ~prev_below_l34 & ~L34 & (l34_lo > 0)
    BX_UP = (c > l43_hi) & ~prev_above_l43 & ~L43 & (l43_hi > 0)
    BX_DN = (c < l43_lo) & ~prev_below_l43 & ~L43 & (l43_lo > 0)

    # ── Bucket label ──────────────────────────────────────────────────────
    _BKT = {0: "W", 1: "L", 2: "N", 3: "B", 4: "VB"}
    bucket_lbl = bucket.map(_BKT)

    return pd.DataFrame({
        "bucket":     bucket_lbl,
        "vol_zscore": vol_z.round(2),
        "rsi":        rsi.round(2),
        "cci_sma":    cci_sma.round(2),
        "L34": L34,  "L43": L43,  "L64": L64,  "L22": L22,
        "BLUE": BLUE, "FRI34": FRI34, "UI": UI,
        "FUCHSIA_RH": FUCHSIA_RH, "FUCHSIA_RL": FUCHSIA_RL,
        "PRE_PUMP": PRE_PUMP,
        "CCI_READY": CCI_READY,
        "BO_UP": BO_UP, "BO_DN": BO_DN,
        "BX_UP": BX_UP, "BX_DN": BX_DN,
    }, index=df.index)


def score_last_bar(sig_id: int, wlnbb: pd.DataFrame) -> tuple[int, int]:
    """
    Compute (bull_score, bear_score) for the last bar.
    wlnbb must be the full WLNBB output DataFrame.
    """
    if wlnbb.empty:
        return 0, 0

    last  = wlnbb.iloc[-1]
    last3 = wlnbb.tail(3)

    bull = 0
    bear = 0

    # T/Z signal
    if sig_id in (6, 8):           # T4, T6
        bull += 2
    elif 1 <= sig_id <= 11:        # other T
        bull += 1
    elif sig_id in (17, 19):       # Z4, Z6
        bear += 2
    elif 12 <= sig_id <= 25:       # other Z
        bear += 1

    # L-signals (last bar)
    if bool(last.get("L34")):   bull += 2
    if bool(last.get("L43")):   bull += 1
    if bool(last.get("CCI_READY")): bull += 2
    if bool(last.get("BLUE")):  bull += 1
    if bool(last.get("FRI34")): bull += 2   # BLUE + L34
    if bool(last.get("BO_UP")): bull += 1
    if bool(last.get("BX_UP")): bull += 1
    if bool(last.get("UI")):    bull += 1

    # PRE_PUMP in last 3 bars
    if last3["PRE_PUMP"].any():    bull += 1

    # Bear components
    if bool(last.get("L22")):        bear += 2
    if bool(last.get("L64")):        bear += 1
    if bool(last.get("FUCHSIA_RH")): bear += 1
    if bool(last.get("BO_DN")):      bear += 1
    if bool(last.get("BX_DN")):      bear += 1

    return min(bull, 10), bear


def l_signal_label(last_row: pd.Series) -> str:
    """Return the highest-priority L-signal name or ''."""
    for name in ("FRI34", "L34", "L43", "L64", "L22",
                 "CCI_READY", "BLUE", "BO_UP", "BO_DN",
                 "BX_UP", "BX_DN", "PRE_PUMP"):
        if bool(last_row.get(name)):
            return name
    return ""


# ── Helpers ──────────────────────────────────────────────────────────────────

def _norm(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    if "volume" not in df.columns:
        df["volume"] = 1.0
    return df


def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs    = gain / loss.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50)


def _cci(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> pd.Series:
    tp  = (high + low + close) / 3.0
    ma  = tp.rolling(period, min_periods=1).mean()
    md  = tp.rolling(period, min_periods=1).apply(
        lambda x: np.abs(x - x.mean()).mean(), raw=True
    )
    return ((tp - ma) / (0.015 * md.replace(0, np.nan))).fillna(0)


def _ffill_when(series: pd.Series, condition: pd.Series) -> pd.Series:
    """Forward-fill series values from the last bar where condition is True."""
    return series.where(condition).ffill().fillna(0)


def _cooldown(condition: pd.Series, cooldown: int) -> pd.Series:
    """Suppress re-firing for `cooldown` bars after each True."""
    arr = condition.values
    out = np.zeros(len(arr), dtype=bool)
    last = -(cooldown + 1)
    for i in range(len(arr)):
        if arr[i] and (i - last) > cooldown:
            out[i] = True
            last = i
    return pd.Series(out, index=condition.index)
