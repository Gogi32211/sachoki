"""Unit tests for Pine 260523 AD-FRESH + WYC Phase + Z8-removal."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

import pandas as pd

from analyzers.tz_wlnbb.config import (
    TZ_WLNBB_VERSION, T_PRIORITY, Z_PRIORITY,
    AD_FRESH_LOOKBACK, AD_FRESH_POS_THR,
    AD_CLUSTER_WINDOW, AD_CLUSTER_MIN,
)
from analyzers.tz_wlnbb.signal_extraction import compute_ad_fresh, compute_wyc_phase


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def _make_df(n: int = 30, t_signals=None, z_signals=None, close_lower_half: bool = True):
    """Build a synthetic OHLCV frame, optionally with T/Z signals at specific bars
    and price in lower half of 20-bar range."""
    t_signals = t_signals or {}
    z_signals = z_signals or {}
    # Descending closes → close in lower half of 20-bar range naturally
    if close_lower_half:
        closes = list(range(100, 100 - n, -1))
    else:
        closes = list(range(100, 100 + n))
    df = pd.DataFrame({
        "open":   [c + 0.5 for c in closes],
        "high":   [c + 1.0 for c in closes],
        "low":    [c - 1.0 for c in closes],
        "close":  closes,
        "volume": [1000] * n,
        "t_signal": [t_signals.get(i, "") for i in range(n)],
        "z_signal": [z_signals.get(i, "") for i in range(n)],
    })
    return df


# ──────────────────────────────────────────────────────────────────────────
# Z8 removal
# ──────────────────────────────────────────────────────────────────────────

def test_z8_not_in_z_priority():
    """Z8 must be absent from config.Z_PRIORITY."""
    assert "Z8" not in Z_PRIORITY


def test_signal_count_is_25():
    """T_PRIORITY + Z_PRIORITY combined = 25 entries (Z8 removed)."""
    assert len(T_PRIORITY) + len(Z_PRIORITY) == 25


def test_version_string_is_260523():
    assert TZ_WLNBB_VERSION.startswith("260523_")


# ──────────────────────────────────────────────────────────────────────────
# AD-FRESH
# ──────────────────────────────────────────────────────────────────────────

def test_ad_fresh_requires_z1g_z2g_then_t4_t2g():
    """A-signal (Z1G/Z2G) must precede D-signal (T4/T6/T2G/T2) within lookback."""
    # Z1G at bar 5, T4 at bar 10 → 5 bars apart → within 12-bar lookback
    df = _make_df(n=30,
                  z_signals={5: "Z1G"},
                  t_signals={10: "T4"})
    ad_fresh, _ = compute_ad_fresh(df)
    assert ad_fresh.iloc[10], "AD-FRESH should fire at the D-signal bar"

    # T4 with NO preceding A-signal → should NOT fire
    df_no_a = _make_df(n=30, t_signals={10: "T4"})
    ad_fresh_no_a, _ = compute_ad_fresh(df_no_a)
    assert not ad_fresh_no_a.iloc[10], "AD-FRESH must not fire without A-signal"


def test_ad_fresh_respects_lookback_window():
    """If D-signal arrives MORE than lookback bars after A-signal, should not fire."""
    # Z1G at bar 2, T4 at bar 20 → 18 bars apart → beyond 12-bar lookback
    df = _make_df(n=30,
                  z_signals={2: "Z1G"},
                  t_signals={20: "T4"})
    ad_fresh, _ = compute_ad_fresh(df)
    assert not ad_fresh.iloc[20], "AD-FRESH must respect AD_FRESH_LOOKBACK"


def test_ad_fresh_requires_fresh_position():
    """pos_in_range >= 0.50 must suppress AD-FRESH (price in upper half)."""
    # Use ascending closes → price in UPPER half → not fresh
    df = _make_df(n=30,
                  z_signals={5: "Z1G"},
                  t_signals={10: "T4"},
                  close_lower_half=False)
    ad_fresh, _ = compute_ad_fresh(df)
    assert not ad_fresh.iloc[10], "AD-FRESH must require price in lower half"


def test_ad_fresh_accepts_alternate_d_signals():
    """D-signal must accept T4, T6, T2G, T2 — not other T variants."""
    for d_sig in ("T4", "T6", "T2G", "T2"):
        df = _make_df(n=30,
                      z_signals={5: "Z2G"},
                      t_signals={10: d_sig})
        ad_fresh, _ = compute_ad_fresh(df)
        assert ad_fresh.iloc[10], f"AD-FRESH must accept D-signal={d_sig}"

    # T1G is NOT an AD-FRESH D-signal
    df_t1g = _make_df(n=30,
                      z_signals={5: "Z1G"},
                      t_signals={10: "T1G"})
    ad_fresh_t1g, _ = compute_ad_fresh(df_t1g)
    assert not ad_fresh_t1g.iloc[10], "AD-FRESH must reject T1G as D-signal"


# ──────────────────────────────────────────────────────────────────────────
# AD-CLUSTER
# ──────────────────────────────────────────────────────────────────────────

def test_ad_cluster_requires_two_ad_fresh():
    """Single AD-FRESH should not trigger AD-CLUSTER; two within window should."""
    # Single AD-FRESH at bar 10
    df_one = _make_df(n=30,
                      z_signals={5: "Z1G"},
                      t_signals={10: "T4"})
    ad_fresh_one, ad_cluster_one = compute_ad_fresh(df_one)
    assert ad_fresh_one.iloc[10]
    # window after the single event — should NOT be a cluster
    assert not ad_cluster_one.iloc[10:18].any(), \
        "AD-CLUSTER must not fire with a single AD-FRESH"

    # Two AD-FRESH within 8-bar window: bars 10 and 14
    df_two = _make_df(n=30,
                      z_signals={5: "Z1G", 12: "Z2G"},
                      t_signals={10: "T4", 14: "T2G"})
    ad_fresh_two, ad_cluster_two = compute_ad_fresh(df_two)
    assert ad_fresh_two.iloc[10] and ad_fresh_two.iloc[14], \
        "Both AD-FRESH events must fire"
    assert ad_cluster_two.iloc[14], \
        "AD-CLUSTER must fire on second AD-FRESH within window"


# ──────────────────────────────────────────────────────────────────────────
# WYC Phase
# ──────────────────────────────────────────────────────────────────────────

def test_wyc_spring_requires_tz_confirmation():
    """Spring requires T1G/T4/T9 confirmation — bare price break does not fire."""
    # Build a frame where price breaks support without a T-signal
    n = 60
    closes = [100] * 40 + [80] + [82] * 19   # gap-down then recovery
    df = pd.DataFrame({
        "open":   [c - 0.5 for c in closes],
        "high":   [c + 1 for c in closes],
        "low":    [c - 2 for c in closes],
        "close":  closes,
        "volume": [1000] * 40 + [5000] + [1000] * 19,
        "t_signal": [""] * n,
        "z_signal": [""] * n,
    })
    phase = compute_wyc_phase(df)
    # No T-confirmation present anywhere — Spring should never appear
    assert "SPRING" not in set(phase.tolist()), \
        "Spring must not fire without T1G/T4/T9 confirmation"


def test_wyc_phase_persists_state():
    """Phase should not reset to NEUTRAL every bar — state machine."""
    # Descending then flat → should be MKDN, then once it transitions
    # it should persist rather than oscillating to NEUTRAL.
    n = 250
    closes = list(range(200, 200 - n, -1))  # long decline
    df = pd.DataFrame({
        "open":   closes, "high": [c + 1 for c in closes],
        "low":    [c - 1 for c in closes], "close": closes,
        "volume": [1000] * n,
        "t_signal": [""] * n, "z_signal": [""] * n,
    })
    phase = compute_wyc_phase(df)
    # Once any non-NEUTRAL phase is established, subsequent bars
    # should also be non-NEUTRAL (state persists).
    non_neutral = phase[phase != "NEUTRAL"]
    if len(non_neutral) > 0:
        first_set = non_neutral.index[0]
        tail = phase.iloc[first_set:]
        assert (tail != "NEUTRAL").all(), "Phase state must persist after being set"


def test_wyc_phase_values_are_valid():
    """All wyc_phase values must be from the known set."""
    valid = {"SPRING", "UTAD", "SOS", "ACC_TR", "DIST_TR",
             "MARKUP", "MKDN", "NEUTRAL"}
    df = _make_df(n=120)
    phase = compute_wyc_phase(df)
    assert set(phase.tolist()).issubset(valid), \
        f"Unexpected phase values: {set(phase.tolist()) - valid}"
