"""Tests for 260523 v3.4 pivot sequence + suffix analytics."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

import numpy as np
import pandas as pd

from analyzers.tz_wlnbb.pivot_sequence_analytics import (
    compute_suffix_perf,
    compute_body_wick_perf,
    compute_line5_perf,
    compute_sequence2_perf,
    compute_composite_suffix_perf,
    run_pivot_sequence_analytics,
)


def _synthetic_pivot_df(n_low_rows: int = 60, n_high_rows: int = 60) -> pd.DataFrame:
    """Build a synthetic stock_stat-shape DataFrame with pivot bars labelled."""
    rows = []
    # n_low_rows pivot LOWs with fwd > 0 (bounce)
    for i in range(n_low_rows):
        rows.append({
            "ticker":         "AAA",
            "date":           f"2024-01-{i+1:02d}",
            "swing_type":     "HL" if i % 2 == 0 else "LL",
            "fwd_swing_ret":  5.0 + (i % 7),    # always positive
            "full_suffix":    "ED",
            "bar_body_wick":  "XF",
            "bar_line5":      "VX-PS-R2X",
            "preup_signal":   "P3",
            "predn_signal":   "",
            "composite_core": "T4L46",
            "t_signal":       "T4",
            "z_signal":       "",
            "l_signal":       "L46",
        })
    # n_high_rows pivot HIGHs with fwd < 0 (drop)
    for i in range(n_high_rows):
        rows.append({
            "ticker":         "BBB",
            "date":           f"2024-02-{i+1:02d}",
            "swing_type":     "HH" if i % 2 == 0 else "LH",
            "fwd_swing_ret":  -5.0 - (i % 7),   # always negative
            "full_suffix":    "EDP",
            "bar_body_wick":  "TB",
            "bar_line5":      "VX-PS-R2L",
            "preup_signal":   "",
            "predn_signal":   "D3",
            "composite_core": "Z4L3",
            "t_signal":       "",
            "z_signal":       "Z4",
            "l_signal":       "L3",
        })
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────────────────────────────────
# Suffix
# ──────────────────────────────────────────────────────────────────────────

def test_suffix_perf_low_positive():
    """HL+LL pivots with positive fwd_swing_ret → suffix avg_fwd > 0."""
    df = _synthetic_pivot_df()
    low, high = compute_suffix_perf(df, min_n=10)
    assert len(low) >= 1
    assert (low["avg_fwd"] > 0).all(), \
        f"All pivot LOW suffix groups should have avg_fwd > 0, got {low['avg_fwd'].tolist()}"
    # Direction-aware win_rate for low side: ret > 0 = win
    assert (low["win_rate"] == 100.0).all()


def test_suffix_perf_high_negative():
    """HH+LH pivots with negative fwd_swing_ret → suffix avg_fwd < 0."""
    df = _synthetic_pivot_df()
    low, high = compute_suffix_perf(df, min_n=10)
    assert len(high) >= 1
    assert (high["avg_fwd"] < 0).all(), \
        f"All pivot HIGH suffix groups should have avg_fwd < 0, got {high['avg_fwd'].tolist()}"
    # Direction-aware win_rate for high side: ret < 0 = win
    assert (high["win_rate"] == 100.0).all()


# ──────────────────────────────────────────────────────────────────────────
# Body/Wick, Line5 — schema sanity
# ──────────────────────────────────────────────────────────────────────────

def test_body_wick_perf_schemas():
    df = _synthetic_pivot_df()
    low, high = compute_body_wick_perf(df, min_n=10)
    for out in (low, high):
        if len(out):
            assert {"bw", "count", "avg_fwd", "med_fwd",
                    "pct25", "pct75", "win_rate"}.issubset(set(out.columns))


def test_line5_perf_schemas():
    df = _synthetic_pivot_df()
    low, high = compute_line5_perf(df, min_n=10)
    for out in (low, high):
        if len(out):
            assert {"l5", "count", "avg_fwd", "med_fwd",
                    "pct25", "pct75", "win_rate"}.issubset(set(out.columns))


# ──────────────────────────────────────────────────────────────────────────
# Sequence-2 format
# ──────────────────────────────────────────────────────────────────────────

def test_seq2_uses_prev_bar_core():
    """`seq2` format must be 'prev_core|cur_core' (pipe-separated)."""
    # Build a small frame where row i=4 is a pivot (HL) and row i=3 is the prev bar
    n = 20
    df = pd.DataFrame({
        "ticker":         ["AAA"] * n,
        "date":           [f"2024-01-{i+1:02d}" for i in range(n)],
        "swing_type":     ["" ] * n,
        "fwd_swing_ret":  [np.nan] * n,
        "composite_core": [f"T{i}L46" for i in range(n)],
    })
    # Make row 5 a HL pivot with fwd_swing_ret
    df.loc[5, "swing_type"] = "HL"
    df.loc[5, "fwd_swing_ret"] = 7.0
    low, high = compute_sequence2_perf(df, min_n=1)
    assert len(low) >= 1
    # seq2 at row 5 should be 'T4L46|T5L46'
    seq2_values = low["seq2"].tolist()
    assert "T4L46|T5L46" in seq2_values, f"Expected 'T4L46|T5L46' in seq2 values, got {seq2_values}"


def test_seq2_separator_is_pipe():
    df = _synthetic_pivot_df()
    low, _ = compute_sequence2_perf(df, min_n=1)
    for s in low["seq2"].tolist():
        assert "|" in s, f"seq2 value missing pipe separator: {s!r}"


# ──────────────────────────────────────────────────────────────────────────
# Composite + suffix format
# ──────────────────────────────────────────────────────────────────────────

def test_composite_suffix_format():
    """core_suf format must be 'CORE+SUFFIX' not 'CORESUFFIX'."""
    df = _synthetic_pivot_df()
    low, high = compute_composite_suffix_perf(df, min_n=10)
    assert len(low) >= 1
    for v in low["core_suf"].tolist():
        assert "+" in v, f"core_suf missing '+' separator: {v!r}"
    # In our synthetic data the low side has composite_core='T4L46' + full_suffix='ED'
    assert "T4L46+ED" in low["core_suf"].tolist()


# ──────────────────────────────────────────────────────────────────────────
# pivot_side column on all outputs
# ──────────────────────────────────────────────────────────────────────────

def test_pivot_side_column_present():
    df = _synthetic_pivot_df()
    outputs = run_pivot_sequence_analytics(
        df, min_n_seq=10, min_n_suffix=10, min_n_bw=10, min_n_l5=10, min_n_preup=10,
    )
    expected = {
        "replay_tz_wlnbb_pivot_suffix_perf.csv",
        "replay_tz_wlnbb_pivot_body_wick_perf.csv",
        "replay_tz_wlnbb_pivot_line5_perf.csv",
        "replay_tz_wlnbb_pivot_preup_predn_perf.csv",
        "replay_tz_wlnbb_pivot_seq2_perf.csv",
        "replay_tz_wlnbb_pivot_composite_suffix_perf.csv",
    }
    assert expected.issubset(set(outputs.keys()))
    for name, df_out in outputs.items():
        if df_out is None or len(df_out) == 0:
            continue
        assert "pivot_side" in df_out.columns, f"{name} missing pivot_side"
        assert set(df_out["pivot_side"].unique()).issubset({"low", "high"})


# ──────────────────────────────────────────────────────────────────────────
# Lookahead policy
# ──────────────────────────────────────────────────────────────────────────

def test_fwd_not_in_live_score():
    """Live scoring engines must NOT reference fwd_swing_ret."""
    backend_dir = os.path.join(os.path.dirname(__file__), "..", "backend")
    for fn in ("turbo_engine.py", "ultra_score.py"):
        path = os.path.join(backend_dir, fn)
        with open(path, "r") as fh:
            src = fh.read()
        assert "fwd_swing_ret" not in src, f"{fn} references lookahead fwd_swing_ret"


def test_min_count_filters_small_groups():
    """Groups below min_n must be excluded from the output."""
    # Only 5 pivot LOW rows → below min_n=30
    rows = [{
        "ticker": "X", "date": f"2024-01-{i+1:02d}",
        "swing_type": "HL", "fwd_swing_ret": 5.0,
        "full_suffix": "ED", "bar_body_wick": "", "bar_line5": "",
        "preup_signal": "", "predn_signal": "", "composite_core": "T4L46",
    } for i in range(5)]
    df = pd.DataFrame(rows)
    low, _ = compute_suffix_perf(df, min_n=30)
    assert len(low) == 0


def test_run_returns_six_outputs():
    df = _synthetic_pivot_df()
    outputs = run_pivot_sequence_analytics(
        df, min_n_seq=10, min_n_suffix=10, min_n_bw=10, min_n_l5=10, min_n_preup=10,
    )
    assert len(outputs) == 6
    assert all(k.startswith("replay_tz_wlnbb_pivot_") and k.endswith(".csv") for k in outputs)
