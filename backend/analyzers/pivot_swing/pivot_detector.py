"""Confirmed swing pivot detection — no lookahead leakage.

A pivot LOW at index i is confirmed only when i+pivot_right bars have closed,
meaning bar i+pivot_right is the earliest bar at which we know the pivot is real.
A pivot HIGH at index i is similarly confirmed at bar i+pivot_right.

confirmed_at_index = i + pivot_right   (the bar when we KNOW the pivot)
pivot_index        = i                 (the bar where the price extreme occurred)
"""
import pandas as pd
import numpy as np


def detect_pivots(
    df: pd.DataFrame,
    pivot_left: int = 3,
    pivot_right: int = 3,
) -> pd.DataFrame:
    """
    Detect confirmed swing pivot highs and lows.

    Parameters
    ----------
    df : DataFrame with columns: date, open, high, low, close, volume
         (plus any signal columns — passed through unchanged)
    pivot_left  : bars to the left required to be lower/higher
    pivot_right : bars to the right required to confirm the pivot

    Returns
    -------
    df with new columns:
        pivot_low  (bool) — True at the bar that IS the pivot low
        pivot_high (bool) — True at the bar that IS the pivot high
        pivot_low_confirmed_at  (int index in df at which pivot was confirmed)
        pivot_high_confirmed_at (int index in df at which pivot was confirmed)
    """
    highs = df["high"].values
    lows = df["low"].values
    n = len(df)

    pivot_low = np.zeros(n, dtype=bool)
    pivot_high = np.zeros(n, dtype=bool)
    pivot_low_conf = np.full(n, -1, dtype=int)
    pivot_high_conf = np.full(n, -1, dtype=int)

    for i in range(pivot_left, n - pivot_right):
        # Pivot LOW: low[i] is less than all pivot_left bars before and pivot_right bars after
        lo = lows[i]
        left_ok = all(lows[i - j] > lo for j in range(1, pivot_left + 1))
        right_ok = all(lows[i + j] > lo for j in range(1, pivot_right + 1))
        if left_ok and right_ok:
            pivot_low[i] = True
            pivot_low_conf[i] = i + pivot_right

        # Pivot HIGH: high[i] is greater than all surrounding bars
        hi = highs[i]
        left_ok_h = all(highs[i - j] < hi for j in range(1, pivot_left + 1))
        right_ok_h = all(highs[i + j] < hi for j in range(1, pivot_right + 1))
        if left_ok_h and right_ok_h:
            pivot_high[i] = True
            pivot_high_conf[i] = i + pivot_right

    df = df.copy()
    df["pivot_low"] = pivot_low
    df["pivot_high"] = pivot_high
    df["pivot_low_confirmed_at"] = pivot_low_conf
    df["pivot_high_confirmed_at"] = pivot_high_conf
    return df
