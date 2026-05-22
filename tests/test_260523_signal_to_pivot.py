"""Tests for 260523 signal-to-pivot analytics."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

import pandas as pd
import numpy as np

from analyzers.tz_wlnbb.signal_to_pivot_analytics import (
    compute_signal_to_pivot,
    aggregate_signal_to_pivot,
    run_signal_to_pivot_analytics,
)


def _df_from_close(close_arr, t_signals=None, z_signals=None, ticker="TEST"):
    n = len(close_arr)
    t_signals = t_signals or {}
    z_signals = z_signals or {}
    return pd.DataFrame({
        "ticker":   [ticker] * n,
        "date":     pd.date_range("2024-01-01", periods=n, freq="D").astype(str),
        "open":     list(close_arr),
        "high":     [c + 0.5 for c in close_arr],
        "low":      [c - 0.5 for c in close_arr],
        "close":    list(close_arr),
        "volume":   [1000] * n,
        "t_signal": [t_signals.get(i, "") for i in range(n)],
        "z_signal": [z_signals.get(i, "") for i in range(n)],
    })


# ──────────────────────────────────────────────────────────────────────────
# Direction: T → next pivot HIGH; Z → next pivot LOW
# ──────────────────────────────────────────────────────────────────────────

def test_t_signal_maps_to_next_pivot_high():
    """T signal fires at idx 2; next confirmed pivot HIGH is around idx 8/16."""
    # Build a sequence with clear pivot highs at idx 8 (peak ~108) and 16 (~115)
    closes = [100, 102, 100, 101, 103, 105, 107, 108, 108, 106,
              104, 105, 108, 111, 113, 115, 115, 113, 110, 107]
    df = _df_from_close(closes, t_signals={2: "T4"})
    out = compute_signal_to_pivot(df)
    # Should have at least one T row, and it should target a pivot HIGH
    t_rows = out[out["signal_field"] == "t_signal"]
    assert len(t_rows) >= 1, "T signal should map to a pivot"
    assert t_rows.iloc[0]["next_pivot_type"] in ("HH", "LH"), \
        f"T signal must target a pivot HIGH (HH/LH), got {t_rows.iloc[0]['next_pivot_type']}"


def test_z_signal_maps_to_next_pivot_low():
    """Z signal fires at idx 2; should target the next pivot LOW."""
    # Pivot lows at idx 8 (low ~92) and idx 16 (low ~85)
    closes = [100, 98, 100, 99, 97, 95, 93, 92, 92, 94,
              96, 95, 92, 89, 87, 85, 85, 87, 90, 93]
    df = _df_from_close(closes, z_signals={2: "Z4"})
    out = compute_signal_to_pivot(df)
    z_rows = out[out["signal_field"] == "z_signal"]
    assert len(z_rows) >= 1, "Z signal should map to a pivot"
    assert z_rows.iloc[0]["next_pivot_type"] in ("HL", "LL"), \
        f"Z signal must target a pivot LOW (HL/LL), got {z_rows.iloc[0]['next_pivot_type']}"


# ──────────────────────────────────────────────────────────────────────────
# Win-rate convention
# ──────────────────────────────────────────────────────────────────────────

def test_win_rate_t_hh_is_high():
    """T signals that reach a pivot HIGH should mostly have ret_to_pivot > 0."""
    # Multiple T signals on a strong uptrend → all reach a higher pivot
    closes = ([100, 101, 102, 103, 104, 105, 104, 103,
               105, 107, 109, 111, 110, 108,
               110, 112, 114, 116, 115, 113]) * 3
    n = len(closes)
    # Fire T at idx 0, 14, 28 (well before each pivot high)
    t_sigs = {i: "T4" for i in (0, 14, 28) if i < n}
    df = _df_from_close(closes, t_signals=t_sigs)
    raw = compute_signal_to_pivot(df)
    summary = aggregate_signal_to_pivot(raw, min_count=1)
    t_hh = summary[(summary["signal_field"] == "t_signal") &
                   (summary["next_pivot_type"].isin(("HH", "LH")))]
    # On synthetic up-bias data the avg ret_to_pivot is positive
    if len(t_hh):
        assert t_hh["avg_ret_to_pivot"].iloc[0] > 0
        assert t_hh["win_rate"].iloc[0] >= 50, \
            f"T → pivot HIGH win_rate should be ≥50% on uptrend, got {t_hh['win_rate'].iloc[0]}"


def test_win_rate_z_ll_is_high():
    """For Z signals → pivot LOW, ret_to_pivot < 0 counts as a win
    (price fell to the lower pivot, as the bearish signal predicted).
    Win rate should therefore be HIGH (near 100%) on a downtrend."""
    closes = [100 - i*0.5 for i in range(60)]  # steady decline
    z_sigs = {i: "Z4" for i in (0, 10, 20, 30, 40)}
    df = _df_from_close(closes, z_signals=z_sigs)
    raw = compute_signal_to_pivot(df)
    summary = aggregate_signal_to_pivot(raw, min_count=1)
    z_ll = summary[(summary["signal_field"] == "z_signal") &
                   (summary["next_pivot_type"].isin(("HL", "LL")))]
    if len(z_ll):
        # On a steady decline, every Z → next pivot LOW should be ret<0 = correct
        assert z_ll["avg_ret_to_pivot"].iloc[0] < 0
        assert z_ll["win_rate"].iloc[0] >= 80, \
            f"Z → pivot LOW on downtrend should have win_rate ~100%, got {z_ll['win_rate'].iloc[0]}"


# ──────────────────────────────────────────────────────────────────────────
# Edge cases
# ──────────────────────────────────────────────────────────────────────────

def test_last_signal_before_eof_has_no_pivot():
    """Signal at the final bar of a series (no following confirmed pivot)
    must not appear in the output."""
    closes = list(range(100, 80, -1))   # 20 bars
    n = len(closes)
    # Signal at the very last bar — no future pivot can confirm
    df = _df_from_close(closes, t_signals={n - 1: "T4"})
    out = compute_signal_to_pivot(df)
    # No row should have date == last date
    if len(out):
        last_date = df["date"].iloc[-1]
        assert last_date not in out["date"].astype(str).tolist(), \
            "Signal at last bar must not produce a row (no future pivot)"


def test_empty_df_returns_empty_columns():
    df = pd.DataFrame({"ticker": [], "date": [], "open": [], "high": [],
                       "low": [], "close": [], "volume": [],
                       "t_signal": [], "z_signal": []})
    raw, summary = run_signal_to_pivot_analytics(df, min_count=1)
    expected_raw = {"ticker", "date", "signal_field", "signal_value",
                    "next_pivot_type", "ret_to_pivot", "bars_to_pivot"}
    expected_sum = {"signal_field", "signal_value", "next_pivot_type", "count",
                    "avg_ret_to_pivot", "med_ret_to_pivot", "win_rate",
                    "avg_bars_to_pivot", "pct25", "pct75"}
    assert expected_raw.issubset(set(raw.columns))
    assert expected_sum.issubset(set(summary.columns))


def test_run_signal_to_pivot_multi_ticker():
    """Multi-ticker run: signals must be partitioned per ticker (no cross-leak)."""
    n = 30
    closes_a = list(range(100, 100 - n, -1))
    closes_b = list(range(50, 50 + n))
    df_a = _df_from_close(closes_a, z_signals={3: "Z4"}, ticker="AAA")
    df_b = _df_from_close(closes_b, t_signals={3: "T4"}, ticker="BBB")
    df_all = pd.concat([df_a, df_b], ignore_index=True)
    raw, summary = run_signal_to_pivot_analytics(df_all, min_count=1)
    # AAA only Z, BBB only T
    aaa_rows = raw[raw["ticker"] == "AAA"]
    bbb_rows = raw[raw["ticker"] == "BBB"]
    if len(aaa_rows):
        assert (aaa_rows["signal_field"] == "z_signal").all()
    if len(bbb_rows):
        assert (bbb_rows["signal_field"] == "t_signal").all()


# ──────────────────────────────────────────────────────────────────────────
# Lookahead policy — ret_to_pivot must not be referenced in live scoring
# ──────────────────────────────────────────────────────────────────────────

def test_no_lookahead_in_live_score():
    """Live scoring engines must NOT reference ret_to_pivot or bars_to_pivot."""
    backend_dir = os.path.join(os.path.dirname(__file__), "..", "backend")
    for fn in ("turbo_engine.py", "ultra_score.py"):
        path = os.path.join(backend_dir, fn)
        with open(path, "r") as fh:
            src = fh.read()
        assert "ret_to_pivot"  not in src, f"{fn} references lookahead ret_to_pivot"
        assert "bars_to_pivot" not in src, f"{fn} references lookahead bars_to_pivot"


def test_aggregator_min_count_filter():
    """Aggregator should drop groups below min_count."""
    raw = pd.DataFrame({
        "ticker":          ["A"] * 10 + ["A"] * 30,
        "date":            ["d"] * 40,
        "signal_field":    ["t_signal"] * 10 + ["z_signal"] * 30,
        "signal_value":    ["T4"] * 10 + ["Z4"] * 30,
        "next_pivot_type": ["HH"] * 10 + ["LL"] * 30,
        "ret_to_pivot":    [5.0] * 10 + [-3.0] * 30,
        "bars_to_pivot":   [5] * 40,
    })
    summary = aggregate_signal_to_pivot(raw, min_count=15)
    # T4 group has 10 < 15 → excluded; Z4 group has 30 → included
    assert len(summary) == 1
    assert summary.iloc[0]["signal_value"] == "Z4"
