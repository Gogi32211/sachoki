"""Signal-to-Pivot Analytics — return from any T/Z signal bar to the next pivot.

For every bar with a T or Z signal, finds the next confirmed pivot of the
relevant direction and computes:
  ret_to_pivot  : % from signal-bar's close to pivot price (RESEARCH_ONLY)
  bars_to_pivot : bars between signal bar and pivot
  next_pivot_type : HH | LH | HL | LL (classification of that pivot)

Convention:
  T signal (bull) → looks for the next confirmed pivot HIGH
  Z signal (bear) → looks for the next confirmed pivot LOW

Win-rate convention (in aggregator):
  T + HH/LH : ret_to_pivot > 0 = win (price rose to pivot high)
  Z + HL/LL : ret_to_pivot < 0 = win (price fell to pivot low, as expected)

**LOOKAHEAD policy:** `ret_to_pivot` and `bars_to_pivot` use future pivot prices.
RESEARCH_ONLY — must never appear in `turbo_engine.py` or `ultra_score.py`.
An automated test asserts this.

Empirical edge (SP500 1D):
  T12 → next HH  +11.73%, win 100%, avg 9.0 bars
  T5  → next HH  +10.98%, win 100%, avg 8.2 bars
  T2G → next HH   +7.17%, win 100%, avg 6.0 bars

Key insight vs `fwd_swing_ret`: that metric requires the *signal bar itself*
to be a confirmed pivot. `ret_to_pivot` works on EVERY signal bar — there are
many more T/Z signal firings than pivots, so the sample is much larger.
"""
from __future__ import annotations
from typing import Tuple
import numpy as np
import pandas as pd


def _detect_pivots(df: pd.DataFrame, left: int = 3, right: int = 3):
    """Returns (pivot_highs, pivot_lows) as lists of integer row indices.
    Same pivot_left=3/pivot_right=3 as the rest of the swing pipeline."""
    n = len(df)
    ph, pl = [], []
    if n < left + right + 1:
        return ph, pl
    high_arr = df["high"].to_numpy()
    low_arr  = df["low"].to_numpy()
    for i in range(left, n - right):
        w = slice(i - left, i + right + 1)
        if high_arr[i] == high_arr[w].max():
            ph.append(i)
        if low_arr[i] == low_arr[w].min():
            pl.append(i)
    return ph, pl


def _classify_pivot_types(df: pd.DataFrame, highs: list, lows: list) -> dict:
    """Returns {bar_index → (pivot_kind, swing_type, pivot_price)} for every
    pivot that has a same-direction predecessor (the first pivot of each
    direction is classified relative to nothing → excluded)."""
    result: dict = {}
    high_arr = df["high"].to_numpy()
    low_arr  = df["low"].to_numpy()

    prev_hp = None
    for i in highs:
        p = float(high_arr[i])
        if prev_hp is not None:
            result[i] = ("pivot_high", "HH" if p > prev_hp else "LH", p)
        prev_hp = p

    prev_lp = None
    for i in lows:
        p = float(low_arr[i])
        if prev_lp is not None:
            result[i] = ("pivot_low", "HL" if p > prev_lp else "LL", p)
        prev_lp = p

    return result


def compute_signal_to_pivot(
    df: pd.DataFrame,
    pivot_left: int = 3,
    pivot_right: int = 3,
) -> pd.DataFrame:
    """Per-ticker computation. df must be sorted by date ascending.

    Returns a long-form DataFrame with one row per (signal_bar, next_pivot) pair:
      ticker, date, signal_field ('t_signal'|'z_signal'), signal_value,
      next_pivot_type ('HH'|'LH'|'HL'|'LL'), ret_to_pivot (%), bars_to_pivot.
    """
    if len(df) == 0:
        return pd.DataFrame(columns=[
            "ticker", "date", "signal_field", "signal_value",
            "next_pivot_type", "ret_to_pivot", "bars_to_pivot",
        ])

    n = len(df)
    ticker = df["ticker"].iloc[0] if "ticker" in df.columns and n > 0 else "UNKNOWN"

    highs, lows = _detect_pivots(df, pivot_left, pivot_right)
    pivot_types  = _classify_pivot_types(df, highs, lows)

    # Classified pivot highs / lows in chronological order
    classified_ph = sorted(i for i, v in pivot_types.items() if v[0] == "pivot_high")
    classified_pl = sorted(i for i, v in pivot_types.items() if v[0] == "pivot_low")

    rows: list = []
    close_arr = df["close"].to_numpy()
    date_arr  = df["date"].to_numpy() if "date" in df.columns else np.arange(n)
    t_arr     = df["t_signal"].astype(object).to_numpy() if "t_signal" in df.columns else np.full(n, "")
    z_arr     = df["z_signal"].astype(object).to_numpy() if "z_signal" in df.columns else np.full(n, "")

    for i in range(n - pivot_right):
        t_val = t_arr[i]
        z_val = z_arr[i]
        # Treat empty string AND nan as "no signal"
        t_has = isinstance(t_val, str) and t_val != "" and t_val != "nan"
        z_has = isinstance(z_val, str) and z_val != "" and z_val != "nan"
        if not t_has and not z_has:
            continue

        close_i = float(close_arr[i])
        if not (close_i > 0):
            continue

        # T signal (bullish) → next classified pivot HIGH
        if t_has:
            nxt = [j for j in classified_ph if j > i]
            if nxt:
                j = nxt[0]
                _, stype, price = pivot_types[j]
                rows.append({
                    "ticker":          ticker,
                    "date":            date_arr[i],
                    "signal_field":    "t_signal",
                    "signal_value":    str(t_val),
                    "next_pivot_type": stype,
                    "ret_to_pivot":    (price / close_i - 1.0) * 100.0,
                    "bars_to_pivot":   int(j - i),
                })

        # Z signal (bearish) → next classified pivot LOW
        if z_has:
            nxt = [j for j in classified_pl if j > i]
            if nxt:
                j = nxt[0]
                _, stype, price = pivot_types[j]
                rows.append({
                    "ticker":          ticker,
                    "date":            date_arr[i],
                    "signal_field":    "z_signal",
                    "signal_value":    str(z_val),
                    "next_pivot_type": stype,
                    "ret_to_pivot":    (price / close_i - 1.0) * 100.0,
                    "bars_to_pivot":   int(j - i),
                })

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "ticker", "date", "signal_field", "signal_value",
        "next_pivot_type", "ret_to_pivot", "bars_to_pivot",
    ])


def aggregate_signal_to_pivot(
    raw: pd.DataFrame,
    min_count: int = 15,
) -> pd.DataFrame:
    """Aggregate raw observations into summary statistics.

    Win-rate convention:
      T + HH/LH : ret_to_pivot > 0 = win
      Z + HL/LL : ret_to_pivot < 0 = win (price fell to pivot low, as predicted)
    """
    if raw is None or len(raw) == 0:
        return pd.DataFrame(columns=[
            "signal_field", "signal_value", "next_pivot_type", "count",
            "avg_ret_to_pivot", "med_ret_to_pivot", "win_rate",
            "avg_bars_to_pivot", "pct25", "pct75",
        ])

    results: list = []
    for (sig_field, sig_val, pivot_type), grp in raw.groupby(
        ["signal_field", "signal_value", "next_pivot_type"]
    ):
        if len(grp) < min_count:
            continue
        if sig_field == "t_signal":
            win_rate = (grp["ret_to_pivot"] > 0).mean() * 100.0
        else:
            win_rate = (grp["ret_to_pivot"] < 0).mean() * 100.0
        results.append({
            "signal_field":      sig_field,
            "signal_value":      sig_val,
            "next_pivot_type":   pivot_type,
            "count":             len(grp),
            "avg_ret_to_pivot":  float(grp["ret_to_pivot"].mean()),
            "med_ret_to_pivot":  float(grp["ret_to_pivot"].median()),
            "win_rate":          float(win_rate),
            "avg_bars_to_pivot": float(grp["bars_to_pivot"].mean()),
            "pct25":             float(grp["ret_to_pivot"].quantile(0.25)),
            "pct75":             float(grp["ret_to_pivot"].quantile(0.75)),
        })

    if not results:
        return pd.DataFrame(columns=[
            "signal_field", "signal_value", "next_pivot_type", "count",
            "avg_ret_to_pivot", "med_ret_to_pivot", "win_rate",
            "avg_bars_to_pivot", "pct25", "pct75",
        ])
    return (pd.DataFrame(results)
              .sort_values("avg_ret_to_pivot", ascending=False)
              .reset_index(drop=True))


def run_signal_to_pivot_analytics(
    df_all: pd.DataFrame,
    pivot_left: int = 3,
    pivot_right: int = 3,
    min_count: int = 15,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Entry point. Accepts a multi-ticker stock_stat DataFrame.

    Returns (raw, summary):
      raw     : one row per (signal_bar, next_pivot) observation (not written to ZIP)
      summary : aggregated by (signal_field, signal_value, next_pivot_type), min_count
    """
    if "ticker" not in df_all.columns:
        raise ValueError("df_all must have a 'ticker' column")
    if len(df_all) == 0:
        empty = pd.DataFrame(columns=[
            "ticker", "date", "signal_field", "signal_value",
            "next_pivot_type", "ret_to_pivot", "bars_to_pivot",
        ])
        return empty, aggregate_signal_to_pivot(empty, min_count=min_count)

    sort_cols = ["ticker"]
    if "date" in df_all.columns:
        sort_cols.append("date")
    df_all = df_all.sort_values(sort_cols).reset_index(drop=True)

    raw_parts: list = []
    for _ticker, grp in df_all.groupby("ticker"):
        raw_parts.append(compute_signal_to_pivot(
            grp.reset_index(drop=True),
            pivot_left=pivot_left, pivot_right=pivot_right,
        ))

    raw = pd.concat(raw_parts, ignore_index=True) if raw_parts else pd.DataFrame()
    summary = aggregate_signal_to_pivot(raw, min_count=min_count)
    return raw, summary
