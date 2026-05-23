"""Tests for 260523 v3.5 PREBREAK + WYC additional filters and scoring."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

import pandas as pd
import numpy as np

from analyzers.tz_wlnbb.signal_extraction import compute_prebreak_signals
from analyzers.tz_wlnbb.filters_260523 import apply_260523_filters
from turbo_engine import _calc_turbo_score


def _base_score_row(**over):
    """Build a turbo-row dict with neutral scoring and overrides."""
    row = {
        # T/Z/L baseline left empty → score starts ~ 0
        "tz_bull": 1,
        "swing_type": "",
        "ad_fresh": False, "ad_cluster": False,
        "wyc_spring": False, "wyc_sos": False, "wyc_acc_tr": False, "wyc_markup": False,
        "pb_lvbo": False, "pb_stop_cause": False, "pb_wvf_confirm": False,
        "pb_macro_penalty": False, "wyc_in_tr": False, "wyc_sow": False,
        "prebreak_prime": False, "prebreak_ready": False, "prebreak_watch": False,
    }
    row.update(over)
    return row


# ──────────────────────────────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────────────────────────────

def test_prebreak_prime_filter():
    rows = [
        {"ticker": "A", "prebreak_prime": True},
        {"ticker": "B", "prebreak_prime": False},
        {"ticker": "C", "prebreak_prime": True},
    ]
    out = apply_260523_filters(rows, prebreak_prime=True)
    assert {r["ticker"] for r in out} == {"A", "C"}


def test_wyc_in_tr_filter():
    rows = [
        {"ticker": "A", "wyc_in_tr": True},
        {"ticker": "B", "wyc_in_tr": False},
        {"ticker": "C", "wyc_in_tr": True},
    ]
    out = apply_260523_filters(rows, wyc_in_tr=True)
    assert {r["ticker"] for r in out} == {"A", "C"}


def test_pb_lvbo_filter():
    rows = [
        {"ticker": "A", "pb_lvbo": True},
        {"ticker": "B", "pb_lvbo": False},
    ]
    out = apply_260523_filters(rows, pb_lvbo=True)
    assert [r["ticker"] for r in out] == ["A"]


def test_pb_macro_penalty_excludes_when_false():
    """pb_macro_penalty=False filters out rows WITH the penalty."""
    rows = [
        {"ticker": "A", "pb_macro_penalty": False},
        {"ticker": "B", "pb_macro_penalty": True},
        {"ticker": "C", "pb_macro_penalty": False},
    ]
    out = apply_260523_filters(rows, pb_macro_penalty=False)
    assert {r["ticker"] for r in out} == {"A", "C"}


def test_combined_prebreak_filters():
    """Combine prebreak_prime + pb_lvbo (AND semantics)."""
    rows = [
        {"ticker": "A", "prebreak_prime": True,  "pb_lvbo": True},
        {"ticker": "B", "prebreak_prime": True,  "pb_lvbo": False},
        {"ticker": "C", "prebreak_prime": False, "pb_lvbo": True},
        {"ticker": "D", "prebreak_prime": True,  "pb_lvbo": True},
    ]
    out = apply_260523_filters(rows, prebreak_prime=True, pb_lvbo=True)
    assert {r["ticker"] for r in out} == {"A", "D"}


# ──────────────────────────────────────────────────────────────────────────
# Scoring modifiers
# ──────────────────────────────────────────────────────────────────────────

def test_pb_macro_penalty_score_reduction():
    """pb_macro_penalty must multiply the score by 0.85."""
    # Build a turbo row with a known scoring contribution from pb_lvbo (+8)
    # and pb_stop_cause (+6) to give s a measurable baseline
    base = _base_score_row(pb_lvbo=True, pb_stop_cause=True)
    s_without = _calc_turbo_score(dict(base), profile="sp500")

    with_penalty = _base_score_row(pb_lvbo=True, pb_stop_cause=True,
                                    pb_macro_penalty=True)
    s_with = _calc_turbo_score(with_penalty, profile="sp500")

    # With macro penalty, score must be ~15% lower (modulo rounding/clamp)
    assert s_with < s_without, \
        f"Score with macro penalty ({s_with}) must be < without ({s_without})"
    # Check approximate ×0.85 ratio (allow ±2 absolute tolerance for rounding)
    expected = round(s_without * 0.85, 1)
    assert abs(s_with - expected) <= 2.0, \
        f"Macro penalty score {s_with} not ~0.85× base {s_without} (expected ~{expected})"


def test_wyc_sow_score_reduction():
    """wyc_sow must multiply the score by 0.80."""
    base = _base_score_row(pb_lvbo=True, pb_stop_cause=True)
    s_without = _calc_turbo_score(dict(base), profile="sp500")

    with_sow = _base_score_row(pb_lvbo=True, pb_stop_cause=True, wyc_sow=True)
    s_with = _calc_turbo_score(with_sow, profile="sp500")

    assert s_with < s_without
    expected = round(s_without * 0.80, 1)
    assert abs(s_with - expected) <= 2.0, \
        f"SOW score {s_with} not ~0.80× base {s_without} (expected ~{expected})"


def test_pb_lvbo_adds_score():
    """pb_lvbo must add to turbo score."""
    base = _base_score_row()
    s_base = _calc_turbo_score(dict(base), profile="sp500")

    with_lvbo = _base_score_row(pb_lvbo=True)
    s_lvbo = _calc_turbo_score(with_lvbo, profile="sp500")
    assert s_lvbo > s_base, f"pb_lvbo should raise score, got {s_lvbo} vs {s_base}"


def test_pb_wvf_confirm_adds_score():
    base = _base_score_row()
    s_base = _calc_turbo_score(dict(base), profile="sp500")

    with_wvf = _base_score_row(pb_wvf_confirm=True)
    s_wvf = _calc_turbo_score(with_wvf, profile="sp500")
    assert s_wvf > s_base


# ──────────────────────────────────────────────────────────────────────────
# compute_prebreak_signals() — column production
# ──────────────────────────────────────────────────────────────────────────

def test_compute_prebreak_adds_expected_columns():
    n = 40
    df = pd.DataFrame({
        "open":   [100 + i*0.1 for i in range(n)],
        "high":   [101 + i*0.1 for i in range(n)],
        "low":    [ 99 + i*0.1 for i in range(n)],
        "close":  [100 + i*0.1 for i in range(n)],
        "volume": [1000] * n,
        "bar_line5": ["VX-PS-R2X" if i % 5 == 0 else "" for i in range(n)],
        "wyc_phase": ["ACC_TR" if i % 7 == 0 else "MARKUP" for i in range(n)],
        "wyc_spring": [False] * n,
        "wyc_acc_tr": [i % 7 == 0 for i in range(n)],
    })
    out = compute_prebreak_signals(df)
    for col in ("prebreak_prime", "prebreak_ready", "prebreak_watch",
                "pb_lvbo", "pb_stop_cause", "pb_pp_rtv", "pb_fly_cd_c",
                "pb_wvf_confirm", "pb_follow_confirm", "pb_macro_penalty",
                "wyc_in_tr", "wyc_sow"):
        assert col in out.columns, f"missing column: {col}"
    # WVF spike should fire on bars where line5 contains "VX"
    assert out["pb_wvf_confirm"].iloc[0] is True or bool(out["pb_wvf_confirm"].iloc[0])
    # wyc_in_tr should fire on ACC_TR bars
    assert bool(out["wyc_in_tr"].iloc[0])  # idx 0 has ACC_TR


def test_compute_prebreak_no_score_safe():
    """When prebreak_score column is absent, score-tier flags default False
    but structural / WYC columns still compute."""
    df = pd.DataFrame({
        "open":   [100] * 30, "high": [101] * 30,
        "low":    [99]  * 30, "close": [100] * 30,
        "volume": [1000] * 30,
        "bar_line5": [""] * 30,
        "wyc_phase": ["MARKUP"] * 30,
    })
    out = compute_prebreak_signals(df)
    assert not out["prebreak_prime"].any()
    assert not out["prebreak_ready"].any()
    assert not out["prebreak_watch"].any()
    # But the columns still exist
    assert "wyc_in_tr" in out.columns


def test_prebreak_tier_assignment_score_thresholds():
    """compute_prebreak_signals now COMPUTES prebreak_score from existing flags
    (Pine PREBREAK-approximation). Verify thresholds (PRIME ≥45, READY ≥28,
    WATCH ≥18) map correctly to tier flags REGARDLESS of which flag combo
    produced the score."""
    df = pd.DataFrame({
        "open":   [100] * 4, "high": [101] * 4,
        "low":    [99]  * 4, "close": [102] * 4,
        "volume": [1000] * 4,
        "ad_cluster":  [True, False, False, False],
        "ad_fresh":    [False, True, True, False],
        "wyc_spring":  [True, True, False, False],
        "wyc_acc_tr":  [False] * 4,
        "wyc_phase":   ["SPRING", "SPRING", "", ""],
        "l_signal":    ["L43", "L43", "L43", ""],
        "bar_line5":   ["VX-PS-R2X", "", "", ""],
        "t_signal":    ["T4", "T4", "T4", ""],
        "is_pivot_high": [False] * 4,
        "is_pivot_low":  [False] * 4,
    })
    out = compute_prebreak_signals(df)
    scores = out["prebreak_score"].tolist()
    # For EVERY row, the tier flags must be consistent with the score
    for i, s in enumerate(scores):
        if s >= 45:
            assert out["prebreak_prime"].iloc[i] is True or bool(out["prebreak_prime"].iloc[i])
            assert not bool(out["prebreak_ready"].iloc[i])
            assert not bool(out["prebreak_watch"].iloc[i])
        elif s >= 28:
            assert bool(out["prebreak_ready"].iloc[i])
            assert not bool(out["prebreak_prime"].iloc[i])
            assert not bool(out["prebreak_watch"].iloc[i])
        elif s >= 18:
            assert bool(out["prebreak_watch"].iloc[i])
            assert not bool(out["prebreak_ready"].iloc[i])
            assert not bool(out["prebreak_prime"].iloc[i])
        else:
            assert not bool(out["prebreak_prime"].iloc[i])
            assert not bool(out["prebreak_ready"].iloc[i])
            assert not bool(out["prebreak_watch"].iloc[i])
    # And at least one PRIME and one non-tier exist in this set
    assert any(out["prebreak_prime"]), f"no PRIME row from scores {scores}"
    assert not all(out["prebreak_watch"]), f"all rows WATCH from scores {scores}"
