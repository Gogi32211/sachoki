"""Tests for 260523 v3.2 forward-pivot swing return refactor."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

import math
import numpy as np
import pandas as pd

from analyzers.tz_wlnbb.swing_classifier import classify_swings


def _ohlcv_from_close(close_arr):
    """OHLCV with high=close+0.5, low=close−0.5 (so high/low pivots track close)."""
    n = len(close_arr)
    return pd.DataFrame({
        "open":   list(close_arr),
        "high":   [c + 0.5 for c in close_arr],
        "low":    [c - 0.5 for c in close_arr],
        "close":  list(close_arr),
        "volume": [1000] * n,
    })


# ──────────────────────────────────────────────────────────────────────────
# Forward swing return direction + sign
# ──────────────────────────────────────────────────────────────────────────

def test_fwd_swing_ret_positive_at_low():
    """pivot LOW followed by a higher pivot HIGH → fwd_swing_ret > 0."""
    # Down 100→90, then up 90→110 → low at idx ~5, high at idx ~15
    closes = ([100, 98, 96, 94, 92, 90, 92, 94, 96, 98, 100,
               102, 104, 106, 108, 110, 108, 106, 104, 102])
    df = _ohlcv_from_close(closes)
    out = classify_swings(df)
    # Find the pivot low row
    low_rows = out[out["is_pivot_low"]].copy()
    assert len(low_rows) >= 1
    first_low = low_rows.iloc[0]
    fwd = first_low["fwd_swing_ret"]
    assert fwd is not None and not math.isnan(fwd), "fwd_swing_ret should be set at pivot low"
    assert fwd > 0, f"fwd_swing_ret at pivot LOW must be positive, got {fwd}"


def test_fwd_swing_ret_negative_at_high():
    """pivot HIGH followed by a lower pivot LOW → fwd_swing_ret < 0."""
    # Up 90→110 then down 110→85
    closes = ([90, 94, 98, 102, 106, 110, 106, 102, 98, 94,
               90, 88, 86, 85, 87, 90, 93, 96, 99, 102])
    df = _ohlcv_from_close(closes)
    out = classify_swings(df)
    high_rows = out[out["is_pivot_high"]]
    assert len(high_rows) >= 1
    first_high = high_rows.iloc[0]
    fwd = first_high["fwd_swing_ret"]
    assert fwd is not None and not math.isnan(fwd)
    assert fwd < 0, f"fwd_swing_ret at pivot HIGH must be negative, got {fwd}"


def test_fwd_swing_ret_is_nan_at_last_pivot():
    """Last pivot in the series has no future opposite pivot → NaN."""
    # Steady-down then up sequence ending at the final high (no future low)
    closes = list(range(100, 80, -1)) + list(range(80, 120))
    df = _ohlcv_from_close(closes)
    out = classify_swings(df)
    pivots = out[out["is_pivot_high"] | out["is_pivot_low"]]
    # The LAST pivot in the chain must have NaN fwd_swing_ret
    last_pivot = pivots.iloc[-1]
    assert math.isnan(last_pivot["fwd_swing_ret"]), \
        f"Last pivot should have NaN fwd_swing_ret, got {last_pivot['fwd_swing_ret']}"


# ──────────────────────────────────────────────────────────────────────────
# Backward metric — swing_ret_from_prev (unchanged behaviour)
# ──────────────────────────────────────────────────────────────────────────

def test_swing_ret_from_prev_backward():
    """swing_ret_from_prev measures % from previous same-direction pivot to this one.

    Need two distinct confirmed pivot highs with pivot_left=3, pivot_right=3.
    First high cluster centred around idx 6 (peak 105), second around idx 16 (peak 110).
    """
    closes = ([100, 101, 102, 103, 104, 105,    # rising
               104, 103, 102, 100,              # falling (1st high formed at idx 5)
               101, 103, 105, 107, 109, 110,    # rising
               108, 106, 104, 102])              # falling (2nd high formed at idx 15)
    df = _ohlcv_from_close(closes)
    out = classify_swings(df)
    high_rows = out[out["is_pivot_high"]].copy()
    assert len(high_rows) >= 2, f"Expected >=2 pivot highs, got {len(high_rows)} at indices {list(high_rows.index)}"
    second_high = high_rows.iloc[1]
    backward = second_high["swing_ret_from_prev"]
    assert backward is not None and not math.isnan(backward)
    assert second_high["swing_type"] == "HH"
    assert backward > 0


def test_required_columns_present():
    df = _ohlcv_from_close(list(range(100, 80, -1)))
    out = classify_swings(df)
    required = {"swing_type", "swing_ret_from_prev", "fwd_swing_ret",
                "fwd_swing_bars", "is_pivot_high", "is_pivot_low"}
    assert required.issubset(set(out.columns)), \
        f"missing columns: {required - set(out.columns)}"
    # OLD columns must NOT be present
    assert "swing_ret"  not in out.columns, "legacy 'swing_ret' column must be removed"
    assert "swing_bars" not in out.columns, "legacy 'swing_bars' column must be removed"


def test_fwd_swing_bars_matches_index_distance():
    """fwd_swing_bars should equal index distance between pivot and next opposite pivot."""
    closes = ([100, 98, 96, 94, 92, 90, 92, 94, 96, 98, 100,
               102, 104, 106, 108, 110, 108])
    df = _ohlcv_from_close(closes)
    out = classify_swings(df)
    low_rows = out[out["is_pivot_low"]].copy()
    high_rows = out[out["is_pivot_high"]].copy()
    if len(low_rows) >= 1 and len(high_rows) >= 1:
        first_low_idx = low_rows.index[0]
        first_high_after = high_rows[high_rows.index > first_low_idx]
        if len(first_high_after) >= 1:
            expected = float(first_high_after.index[0] - first_low_idx)
            actual = low_rows.iloc[0]["fwd_swing_bars"]
            assert actual == expected, \
                f"fwd_swing_bars={actual}, expected {expected}"


# ──────────────────────────────────────────────────────────────────────────
# Lookahead policy — fwd_swing_ret must not be used in live scoring
# ──────────────────────────────────────────────────────────────────────────

def test_fwd_not_used_in_live_score():
    """Sanity check: live scoring engines must not reference fwd_swing_ret /
    fwd_swing_bars in their source. Those are RESEARCH_ONLY (lookahead)."""
    backend_dir = os.path.join(os.path.dirname(__file__), "..", "backend")
    for fn in ("turbo_engine.py", "ultra_score.py"):
        path = os.path.join(backend_dir, fn)
        with open(path, "r") as fh:
            src = fh.read()
        assert "fwd_swing_ret"  not in src, f"{fn} uses lookahead fwd_swing_ret"
        assert "fwd_swing_bars" not in src, f"{fn} uses lookahead fwd_swing_bars"


def test_lookahead_columns_constant_includes_fwd():
    """stock_stat.LOOKAHEAD_COLUMNS must flag the new forward columns."""
    from analyzers.tz_wlnbb.stock_stat import LOOKAHEAD_COLUMNS
    assert "fwd_swing_ret"  in LOOKAHEAD_COLUMNS
    assert "fwd_swing_bars" in LOOKAHEAD_COLUMNS
    # Existing forward-return columns still flagged
    for col in ("ret_5d", "ret_10d", "mfe_5d", "mae_5d"):
        assert col in LOOKAHEAD_COLUMNS


# ──────────────────────────────────────────────────────────────────────────
# Empty / degenerate input safety
# ──────────────────────────────────────────────────────────────────────────

def test_empty_dataframe_safe():
    df = pd.DataFrame({"high": [], "low": [], "close": [], "open": [], "volume": []})
    out = classify_swings(df)
    assert len(out) == 0
    required = {"swing_type", "swing_ret_from_prev", "fwd_swing_ret",
                "fwd_swing_bars", "is_pivot_high", "is_pivot_low"}
    assert required.issubset(set(out.columns))
