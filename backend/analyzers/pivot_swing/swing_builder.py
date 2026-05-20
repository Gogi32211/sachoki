"""Build alternating LOW→HIGH / HIGH→LOW swing segments from confirmed pivots.

Rules:
- Swings alternate: first swing from first pivot_low to next pivot_high, then
  pivot_high to next pivot_low, etc.
- Skip any pivot that would continue the same direction (take the more extreme one).
- Swing must satisfy min_swing_return_pct and min_swing_bars.
- All index references use confirmed_at_index (no lookahead).
"""
import pandas as pd
import numpy as np
from typing import List, Dict, Any


def build_swings(
    df: pd.DataFrame,
    min_swing_return_pct: float = 3.0,
    min_swing_bars: int = 2,
) -> List[Dict[str, Any]]:
    """
    Build alternating swing segments from a df that already has pivot_low / pivot_high columns.

    Returns a list of swing dicts:
        direction      : "UP" (low→high) or "DOWN" (high→low)
        start_pivot_idx: integer position of starting pivot in df
        end_pivot_idx  : integer position of ending pivot in df
        start_conf_idx : confirmed_at index of start pivot
        end_conf_idx   : confirmed_at index of end pivot
        start_price    : price at start pivot (low for UP, high for DOWN)
        end_price      : price at end pivot
        return_pct     : abs percent move
        bar_count      : end_pivot_idx - start_pivot_idx
        start_date     : date string of start pivot
        end_date       : date string of end pivot
    """
    highs = df["high"].values
    lows = df["low"].values
    dates = df["date"].values if "date" in df.columns else np.arange(len(df)).astype(str)

    # Collect all confirmed pivots sorted by pivot_index
    pivot_lows = [
        {"pivot_type": "LOW", "pivot_idx": i,
         "conf_idx": int(df["pivot_low_confirmed_at"].iloc[i]),
         "price": float(lows[i]),
         "date": str(dates[i])}
        for i in range(len(df)) if df["pivot_low"].iloc[i]
    ]
    pivot_highs = [
        {"pivot_type": "HIGH", "pivot_idx": i,
         "conf_idx": int(df["pivot_high_confirmed_at"].iloc[i]),
         "price": float(highs[i]),
         "date": str(dates[i])}
        for i in range(len(df)) if df["pivot_high"].iloc[i]
    ]

    # Merge and sort by pivot_idx
    all_pivots = sorted(pivot_lows + pivot_highs, key=lambda x: x["pivot_idx"])

    if not all_pivots:
        return []

    # Build alternating swing chain
    swings: List[Dict[str, Any]] = []
    # Determine starting direction from first pivot type
    # Walk through pivots, maintain current "anchor" and expected next type
    anchor = None
    expected_type = None

    for pv in all_pivots:
        if anchor is None:
            anchor = pv
            expected_type = "HIGH" if pv["pivot_type"] == "LOW" else "LOW"
            continue

        if pv["pivot_type"] != expected_type:
            # Same type as anchor — keep the more extreme one
            if anchor["pivot_type"] == "LOW":
                if pv["price"] < anchor["price"]:
                    anchor = pv
            else:  # HIGH
                if pv["price"] > anchor["price"]:
                    anchor = pv
            continue

        # Correct alternating type — build swing
        start = anchor
        end = pv

        bar_count = end["pivot_idx"] - start["pivot_idx"]
        if start["price"] <= 0:
            anchor = pv
            expected_type = "HIGH" if pv["pivot_type"] == "LOW" else "LOW"
            continue
        ret_pct = abs(end["price"] - start["price"]) / start["price"] * 100.0

        if ret_pct >= min_swing_return_pct and bar_count >= min_swing_bars:
            direction = "UP" if start["pivot_type"] == "LOW" else "DOWN"
            swings.append({
                "direction": direction,
                "start_pivot_idx": start["pivot_idx"],
                "end_pivot_idx": end["pivot_idx"],
                "start_conf_idx": start["conf_idx"],
                "end_conf_idx": end["conf_idx"],
                "start_price": start["price"],
                "end_price": end["price"],
                "return_pct": round(ret_pct, 3),
                "bar_count": bar_count,
                "start_date": start["date"],
                "end_date": end["date"],
            })

        anchor = pv
        expected_type = "HIGH" if pv["pivot_type"] == "LOW" else "LOW"

    return swings
