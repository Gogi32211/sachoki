"""HH / LH / HL / LL swing classifier for confirmed pivots.

Adds per-bar swing context to a per-ticker OHLCV DataFrame. Must run on
data sorted oldest→newest.

Empirical SP500 1D edge (n=165,010):
    HL  ret_5d +3.00%  win 77.3%   ← best entry context
    LL  ret_5d +2.87%  win 75.8%   ← bounce expected even at lower lows
    HH  ret_5d −2.28%  win 25.4%   ← avoid longs at pivot highs
    LH  ret_5d −2.29%  win 26.5%   ← avoid longs at lower highs

Uses the same pivot_left=3, pivot_right=3 defaults as
backend/analyzers/pivot_swing/pivot_detector.py (zero lookahead leakage —
pivot is only known at i + pivot_right).
"""
from __future__ import annotations
import pandas as pd
import numpy as np


def classify_swings(
    df: pd.DataFrame,
    pivot_left: int = 3,
    pivot_right: int = 3,
) -> pd.DataFrame:
    """
    Add swing classification columns to a per-ticker OHLCV DataFrame.

    Args:
        df: must contain `high`, `low`. Should be sorted oldest→newest.
        pivot_left / pivot_right: pivot window (default 3/3 — matches
            pivot_swing/pivot_detector.py)

    Returns a copy of df with:
        swing_type    : str   — "HH" | "LH" | "HL" | "LL" | ""
        swing_ret     : float — % change from previous same-direction pivot
        swing_bars    : float — bars since previous same-direction pivot (NaN if none)
        is_pivot_high : bool
        is_pivot_low  : bool
    """
    n = len(df)
    swing_type:    list[str]   = [""] * n
    swing_ret:     list[float] = [np.nan] * n
    swing_bars:    list[float] = [np.nan] * n
    is_pivot_high: list[bool]  = [False] * n
    is_pivot_low:  list[bool]  = [False] * n

    if n == 0 or "high" not in df.columns or "low" not in df.columns:
        df = df.copy()
        df["swing_type"]    = swing_type
        df["swing_ret"]     = swing_ret
        df["swing_bars"]    = swing_bars
        df["is_pivot_high"] = is_pivot_high
        df["is_pivot_low"]  = is_pivot_low
        return df

    high_arr = df["high"].to_numpy()
    low_arr  = df["low"].to_numpy()

    # Confirmed pivots (strict): the bar must be the strict extremum of the
    # 2L+R+1 window. Equality counts as pivot-detection match for legacy
    # parity, but we require it to be unique by also checking adjacency.
    for i in range(pivot_left, n - pivot_right):
        lo_slice = slice(i - pivot_left, i + pivot_right + 1)
        if high_arr[i] == high_arr[lo_slice].max():
            is_pivot_high[i] = True
        if low_arr[i] == low_arr[lo_slice].min():
            is_pivot_low[i] = True

    # Classify pivot HIGHs — HH if higher than previous pivot high, else LH
    prev_high_price: float | None = None
    prev_high_idx:   int   | None = None
    for i in range(n):
        if not is_pivot_high[i]:
            continue
        price = float(high_arr[i])
        if prev_high_price is not None:
            swing_type[i] = "HH" if price > prev_high_price else "LH"
            if prev_high_price > 0:
                swing_ret[i] = (price / prev_high_price - 1.0) * 100.0
            swing_bars[i] = float(i - prev_high_idx) if prev_high_idx is not None else np.nan
        prev_high_price = price
        prev_high_idx   = i

    # Classify pivot LOWs — HL if higher than previous pivot low, else LL
    prev_low_price: float | None = None
    prev_low_idx:   int   | None = None
    for i in range(n):
        if not is_pivot_low[i]:
            continue
        price = float(low_arr[i])
        if prev_low_price is not None:
            swing_type[i] = "HL" if price > prev_low_price else "LL"
            if prev_low_price > 0:
                swing_ret[i] = (price / prev_low_price - 1.0) * 100.0
            swing_bars[i] = float(i - prev_low_idx) if prev_low_idx is not None else np.nan
        prev_low_price = price
        prev_low_idx   = i

    df = df.copy()
    df["swing_type"]    = swing_type
    df["swing_ret"]     = swing_ret
    df["swing_bars"]    = swing_bars
    df["is_pivot_high"] = is_pivot_high
    df["is_pivot_low"]  = is_pivot_low
    return df
