"""Tests for 260523 v3.1: swing classifier + AD-CLUSTER bug fix + Spring fix."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

import numpy as np
import pandas as pd

from analyzers.tz_wlnbb.swing_classifier import classify_swings
from analyzers.tz_wlnbb.signal_extraction import compute_ad_fresh, compute_wyc_phase
from analyzers.tz_wlnbb.filters_260523 import apply_260523_filters


# ──────────────────────────────────────────────────────────────────────────
# Swing classifier — HH / LH / HL / LL detection
# ──────────────────────────────────────────────────────────────────────────

def _ohlcv_from_close(close_arr):
    """Helper: build OHLCV from a close array with high=close+0.5, low=close-0.5."""
    n = len(close_arr)
    return pd.DataFrame({
        "open":   [c for c in close_arr],
        "high":   [c + 0.5 for c in close_arr],
        "low":    [c - 0.5 for c in close_arr],
        "close":  list(close_arr),
        "volume": [1000] * n,
    })


def test_swing_classifier_hl_detection():
    """HL: a pivot low that is HIGHER than the previous pivot low."""
    # Pivot lows at index 5 (low=94.5) and index 15 (low=96.5) → HL
    closes = ([100]*3 + [98] + [97] + [95] + [98] + [99] + [100]*3 +
              [99] + [98] + [97] + [97] + [97] + [99]*5)
    df = _ohlcv_from_close(closes)
    out = classify_swings(df)
    pivot_lows = out[out["is_pivot_low"]]
    swing_types = pivot_lows["swing_type"].tolist()
    # We expect at least one HL among the labelled lows
    assert any(st == "HL" for st in swing_types if st), \
        f"Expected at least one HL pivot low, got {swing_types}"


def test_swing_classifier_ll_detection():
    """LL: a pivot low that is LOWER than the previous pivot low."""
    # Descending pivot lows: 95 then 90
    closes = ([100]*3 + [97] + [95] + [97] + [100] + [102]*3 +
              [99] + [95] + [90] + [92] + [95] + [100]*5)
    df = _ohlcv_from_close(closes)
    out = classify_swings(df)
    pivot_lows = out[out["is_pivot_low"]]
    swing_types = [s for s in pivot_lows["swing_type"].tolist() if s]
    assert any(st == "LL" for st in swing_types), \
        f"Expected an LL pivot low, got {swing_types}"


def test_swing_classifier_hh_detection():
    """HH: a pivot high that is HIGHER than the previous pivot high."""
    # Two pivot highs: 105 at index ~5, then 110 at index ~15
    closes = ([100]*3 + [102] + [105] + [102] + [100]*5 +
              [105] + [107] + [110] + [107] + [105] + [100]*5)
    df = _ohlcv_from_close(closes)
    out = classify_swings(df)
    pivot_highs = out[out["is_pivot_high"]]
    swing_types = [s for s in pivot_highs["swing_type"].tolist() if s]
    assert any(st == "HH" for st in swing_types), \
        f"Expected an HH pivot high, got {swing_types}"


def test_swing_classifier_lh_detection():
    """LH: a pivot high that is LOWER than the previous pivot high."""
    closes = ([100]*3 + [105] + [110] + [105] + [100]*5 +
              [102] + [105] + [107] + [105] + [102] + [100]*5)
    df = _ohlcv_from_close(closes)
    out = classify_swings(df)
    pivot_highs = out[out["is_pivot_high"]]
    swing_types = [s for s in pivot_highs["swing_type"].tolist() if s]
    assert any(st == "LH" for st in swing_types), \
        f"Expected an LH pivot high, got {swing_types}"


def test_swing_classifier_adds_required_columns():
    df = _ohlcv_from_close([100, 99, 98, 97, 96, 95, 96, 97, 98, 99])
    out = classify_swings(df)
    for col in ("swing_type", "swing_ret", "swing_bars",
                "is_pivot_high", "is_pivot_low"):
        assert col in out.columns, f"missing column: {col}"


def test_swing_classifier_empty_df():
    """Empty df should not crash."""
    df = pd.DataFrame({"high": [], "low": [], "close": [], "open": [], "volume": []})
    out = classify_swings(df)
    assert len(out) == 0
    for col in ("swing_type", "swing_ret", "swing_bars",
                "is_pivot_high", "is_pivot_low"):
        assert col in out.columns


# ──────────────────────────────────────────────────────────────────────────
# AD-CLUSTER bug fix — must AND with ad_fresh on the same bar
# ──────────────────────────────────────────────────────────────────────────

def test_ad_cluster_fixed_requires_ad_fresh():
    """After fix: ad_cluster=True must imply ad_fresh=True on the same row."""
    np.random.seed(42)
    n = 60
    closes = list(range(100, 100 - n, -1))  # downtrend → pos in lower half
    df = pd.DataFrame({
        "open":   closes,
        "high":   [c + 1 for c in closes],
        "low":    [c - 1 for c in closes],
        "close":  closes,
        "volume": [1000] * n,
        # Build a pattern with multiple AD-FRESH events
        "t_signal": ["T4" if i in (10, 14, 22, 30, 38) else "" for i in range(n)],
        "z_signal": ["Z1G" if i in (5, 11, 18, 26, 34) else "" for i in range(n)],
    })
    ad_fresh, ad_cluster = compute_ad_fresh(df)

    # Pre-fix bug: ad_cluster was True on bars where ad_fresh was False
    # Post-fix: every ad_cluster=True bar must also have ad_fresh=True
    for i in range(n):
        if ad_cluster.iloc[i]:
            assert ad_fresh.iloc[i], \
                f"Bar {i}: ad_cluster=True without ad_fresh=True (bug regression)"


def test_ad_cluster_count_is_subset_of_ad_fresh():
    """Cluster count must be ≤ fresh count."""
    np.random.seed(7)
    n = 80
    closes = list(range(120, 120 - n, -1))
    df = pd.DataFrame({
        "open":   closes,
        "high":   [c + 1 for c in closes],
        "low":    [c - 1 for c in closes],
        "close":  closes,
        "volume": [1000] * n,
        "t_signal": ["T4" if i in (12, 16, 22, 26) else "" for i in range(n)],
        "z_signal": ["Z2G" if i in (8, 13, 18, 23) else "" for i in range(n)],
    })
    ad_fresh, ad_cluster = compute_ad_fresh(df)
    assert int(ad_cluster.sum()) <= int(ad_fresh.sum())


# ──────────────────────────────────────────────────────────────────────────
# Spring tightening — vol < 2x or close in lower part of bar should suppress
# ──────────────────────────────────────────────────────────────────────────

def _spring_setup_df(vol_spike: float, close_pos: float):
    """Build a frame where Spring conditions can fire if thresholds permit:
    macro_down, T4 confirmation, low breaks 20-bar support, range > ATR.

    vol_spike  : volume multiplier on the candidate bar (vs 20-bar avg of 1000)
    close_pos  : fraction (0..1) of close position within the candidate bar's range
    """
    n = 80
    # Long downtrend → macro_down (ema50 < ema200) by the end
    closes = [200.0 - i * 1.5 for i in range(n)]
    highs = [c + 1.0 for c in closes]
    lows = [c - 1.0 for c in closes]
    opens = [c + 0.2 for c in closes]
    volumes = [1000.0] * n
    t_sigs = [""] * n
    z_sigs = [""] * n

    # Candidate Spring bar at index 70 (well into the downtrend)
    pivot_idx = 70
    # Break below the prior 20-bar low
    prior_min = min(lows[pivot_idx - 20:pivot_idx])
    spring_low = prior_min - 5.0
    spring_high = prior_min + 8.0   # range = 13, ATR ~ ~few units → expanded
    spring_close = spring_low + (spring_high - spring_low) * close_pos
    spring_open = spring_low + (spring_high - spring_low) * 0.10
    # ensure is_bull (close > open)
    if spring_close <= spring_open:
        spring_close = spring_open + 0.5

    highs[pivot_idx] = spring_high
    lows[pivot_idx]  = spring_low
    opens[pivot_idx] = spring_open
    closes[pivot_idx] = spring_close
    volumes[pivot_idx] = 1000.0 * vol_spike
    t_sigs[pivot_idx] = "T4"   # T-confirmation

    return pd.DataFrame({
        "open": opens, "high": highs, "low": lows,
        "close": closes, "volume": volumes,
        "t_signal": t_sigs, "z_signal": z_sigs,
    }), pivot_idx


def test_spring_strong_vol_and_close_position_fires():
    """With vol 2.5x and close in upper 80% → Spring should fire (post-fix)."""
    df, idx = _spring_setup_df(vol_spike=2.5, close_pos=0.80)
    phase = compute_wyc_phase(df)
    # The candidate bar's phase should be SPRING (or a later persisted SPRING)
    assert "SPRING" in phase.iloc[idx:].tolist(), \
        f"Expected SPRING after fix with strong vol + upper close, got {phase.iloc[idx]}"


def test_spring_weak_vol_does_not_fire():
    """vol 1.6x (below 2.0× threshold) → Spring must be suppressed (post-fix)."""
    df, idx = _spring_setup_df(vol_spike=1.6, close_pos=0.80)
    phase = compute_wyc_phase(df)
    # Phase at the candidate bar should NOT be SPRING (vol gate fails)
    assert phase.iloc[idx] != "SPRING", \
        f"Spring fired with weak vol — bug regression. phase[idx]={phase.iloc[idx]}"


def test_spring_close_in_lower_part_suppressed():
    """close in lower 40% (close_pos=0.30) → Spring must be suppressed."""
    df, idx = _spring_setup_df(vol_spike=2.5, close_pos=0.30)
    phase = compute_wyc_phase(df)
    assert phase.iloc[idx] != "SPRING", \
        f"Spring fired with close in lower half — bug regression. phase[idx]={phase.iloc[idx]}"


# ──────────────────────────────────────────────────────────────────────────
# swing_type filter (Task 5)
# ──────────────────────────────────────────────────────────────────────────

def test_apply_swing_type_filter_HL_only():
    rows = [
        {"ticker": "A", "swing_type": "HL"},
        {"ticker": "B", "swing_type": "LH"},
        {"ticker": "C", "swing_type": "HL"},
        {"ticker": "D", "swing_type": ""},
    ]
    out = apply_260523_filters(rows, swing_type="HL")
    assert {r["ticker"] for r in out} == {"A", "C"}


def test_apply_swing_type_filter_pivot():
    """'pivot' should match any non-empty swing_type."""
    rows = [
        {"ticker": "A", "swing_type": "HL"},
        {"ticker": "B", "swing_type": "LH"},
        {"ticker": "C", "swing_type": ""},
        {"ticker": "D", "swing_type": "LL"},
    ]
    out = apply_260523_filters(rows, swing_type="pivot")
    assert {r["ticker"] for r in out} == {"A", "B", "D"}
