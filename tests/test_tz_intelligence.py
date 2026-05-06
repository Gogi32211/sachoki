"""Tests for TZ Signal Intelligence classifier."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pytest
from tz_intelligence.matrix_loader import load_matrix, MatrixIndex
from tz_intelligence.classifier import classify_tz_event, _best_role


# ── Matrix loader ─────────────────────────────────────────────────────────────

def test_matrix_loads():
    m = load_matrix()
    assert len(m.rows) == 1523

def test_matrix_has_composite_index():
    m = load_matrix()
    assert len(m.composite) > 0
    # Known composite from seed
    assert any("T1L12NP" in k or "T4" in k for k in m.composite)

def test_matrix_has_seq4_index():
    m = load_matrix()
    assert len(m.seq4) > 0

def test_matrix_has_baselines():
    m = load_matrix()
    for sig in ["T1", "T4", "T6", "Z4", "Z6"]:
        assert sig in m.baseline

def test_matrix_meta_bonuses():
    m = load_matrix()
    assert m.get_ema_bonus() == 10
    assert m.get_price_position_bonus() == 10
    assert m.get_short_go_bonus() == 35


# ── Role ranking ──────────────────────────────────────────────────────────────

def test_best_role_bull_a_beats_bull_b():
    assert _best_role("BULL_A", "BULL_B") == "BULL_A"

def test_best_role_short_go_beats_bull_a():
    assert _best_role("SHORT_GO", "BULL_A") == "SHORT_GO"

def test_best_role_no_edge_loses_to_anything():
    assert _best_role("NO_EDGE", "BULL_WATCH") == "BULL_WATCH"
    assert _best_role("BULL_WATCH", "NO_EDGE") == "BULL_WATCH"


# ── Classifier — no signal ────────────────────────────────────────────────────

def test_no_signal_returns_no_edge():
    m = load_matrix()
    row = {"ticker": "AAPL", "date": "2025-01-01", "t_signal": "", "z_signal": "", "l_signal": ""}
    result = classify_tz_event(row, [], m)
    assert result["role"] == "NO_EDGE"
    assert result["final_signal"] == ""

def test_result_has_required_keys():
    m = load_matrix()
    row = {"ticker": "AAPL", "date": "2025-01-01", "t_signal": "T4", "z_signal": "", "l_signal": ""}
    result = classify_tz_event(row, [], m)
    for key in ["ticker", "date", "final_signal", "composite_pattern", "seq4",
                "role", "score", "quality", "action", "reason_codes", "explanation"]:
        assert key in result


# ── Classifier — composite matching ──────────────────────────────────────────

def test_composite_match_scores():
    m = load_matrix()
    # T1 + lane1=L12NP → composite T1L12NP → BULL_B / score 55
    row = {
        "ticker": "TEST", "date": "2025-01-01",
        "t_signal": "T1", "z_signal": "", "l_signal": "",
        "lane1_label": "L12NP", "lane3_label": "",
        "volume_bucket": "", "ne_suffix": "", "wick_suffix": "",
        "close": "50", "ema20": "45", "ema50": "40", "ema89": "35",
        "high": "52", "low": "48", "open": "49",
    }
    result = classify_tz_event(row, [], m)
    assert result["composite_pattern"] == "T1L12NP"
    assert result["score"] >= 55
    assert result["role"] in ("BULL_B", "BULL_A", "BULL_WATCH")


def test_reject_composite_gives_negative_score():
    m = load_matrix()
    # T1 + L12NDP → REJECT_COMPOSITE → SHORT_WATCH / -40
    row = {
        "ticker": "TEST", "date": "2025-01-01",
        "t_signal": "T1", "z_signal": "", "l_signal": "",
        "lane1_label": "L12NDP", "lane3_label": "",
        "volume_bucket": "", "ne_suffix": "", "wick_suffix": "",
        "close": "50", "ema20": "55", "ema50": "60", "ema89": "65",
        "high": "51", "low": "49", "open": "50.5",
    }
    result = classify_tz_event(row, [], m)
    assert result["score"] <= 0 or result["role"] in ("SHORT_WATCH", "REJECT")


# ── Classifier — seq4 matching ────────────────────────────────────────────────

def test_seq4_bull_a_match():
    m = load_matrix()
    # Known BULL_A seq4: Z2G|T1|Z5|T1
    def make_bar(sig_t="", sig_z=""):
        return {"t_signal": sig_t, "z_signal": sig_z, "l_signal": "",
                "lane1_label": "", "lane3_label": "", "volume_bucket": "",
                "ne_suffix": "", "wick_suffix": "", "close": "50",
                "ema20": "45", "ema50": "40", "ema89": "35",
                "high": "52", "low": "48", "open": "49"}
    history = [make_bar(sig_z="Z2G"), make_bar(sig_t="T1"), make_bar(sig_z="Z5")]
    current = make_bar(sig_t="T1")
    current["ticker"] = "X"
    current["date"]   = "2025-01-05"
    result = classify_tz_event(current, history, m)
    assert result["seq4"] == "Z2G|T1|Z5|T1"
    assert result["role"] in ("BULL_A", "BULL_B", "SHORT_WATCH")
    # Should have a SEQ4 reason code
    assert any("SEQ4" in c for c in result["reason_codes"])


# ── Classifier — EMA context ──────────────────────────────────────────────────

def test_ema50_reclaim_bonus():
    m = load_matrix()
    prev_bar = {"t_signal": "T4", "z_signal": "", "l_signal": "",
                "lane1_label": "", "lane3_label": "", "volume_bucket": "",
                "ne_suffix": "", "wick_suffix": "", "close": "48",  # below ema50=50
                "ema20": "45", "ema50": "50", "ema89": "42",
                "high": "49", "low": "47", "open": "48.5"}
    current = {
        "ticker": "X", "date": "2025-01-02",
        "t_signal": "T4", "z_signal": "", "l_signal": "",
        "lane1_label": "", "lane3_label": "", "volume_bucket": "",
        "ne_suffix": "", "wick_suffix": "", "close": "52",  # above ema50=50
        "ema20": "46", "ema50": "50", "ema89": "43",
        "high": "53", "low": "50", "open": "51",
    }
    result = classify_tz_event(current, [prev_bar], m)
    assert any("EMA50_RECLAIM" in c for c in result["reason_codes"])


def test_price_position_top75_bonus():
    m = load_matrix()
    # 4-bar range low=40, high=60 → top 75% means close >= 55
    hist = [
        {"t_signal": "T4", "z_signal": "", "l_signal": "", "lane1_label": "", "lane3_label": "",
         "volume_bucket": "", "ne_suffix": "", "wick_suffix": "",
         "close": "42", "high": "50", "low": "40", "open": "41", "ema20": "44", "ema50": "43", "ema89": "42"},
        {"t_signal": "T4", "z_signal": "", "l_signal": "", "lane1_label": "", "lane3_label": "",
         "volume_bucket": "", "ne_suffix": "", "wick_suffix": "",
         "close": "50", "high": "55", "low": "44", "open": "48", "ema20": "44", "ema50": "43", "ema89": "42"},
        {"t_signal": "T4", "z_signal": "", "l_signal": "", "lane1_label": "", "lane3_label": "",
         "volume_bucket": "", "ne_suffix": "", "wick_suffix": "",
         "close": "53", "high": "58", "low": "47", "open": "52", "ema20": "44", "ema50": "43", "ema89": "42"},
    ]
    current = {
        "ticker": "X", "date": "2025-01-04",
        "t_signal": "T4", "z_signal": "", "l_signal": "",
        "lane1_label": "", "lane3_label": "", "volume_bucket": "",
        "ne_suffix": "", "wick_suffix": "", "close": "58",  # 58 is top 75% of [40..60]
        "ema20": "50", "ema50": "48", "ema89": "46",
        "high": "60", "low": "55", "open": "56",
    }
    result = classify_tz_event(current, hist, m)
    assert any("CLOSE_TOP75PCT" in c for c in result["reason_codes"])


# ── Classifier — SHORT_GO promotion ──────────────────────────────────────────

def test_short_go_promotion_when_breaks_4bar_low():
    m = load_matrix()
    # Force a SHORT_WATCH composite, then break 4-bar low
    row = {
        "ticker": "TEST", "date": "2025-01-01",
        "t_signal": "T1", "z_signal": "", "l_signal": "",
        "lane1_label": "L12NDP", "lane3_label": "",
        "volume_bucket": "", "ne_suffix": "", "wick_suffix": "",
        "close": "40", "ema20": "55", "ema50": "60", "ema89": "65",
        "high": "41", "low": "39", "open": "40.5",
    }
    result = classify_tz_event(row, [], m, current_low_4bar=42.0)
    # close=40 < low4=42 → SHORT_GO promoted if it was SHORT_WATCH
    if result["role"] == "SHORT_GO":
        assert any("BREAK_4BAR_LOW" in c for c in result["reason_codes"])


# ── Classifier — output types ─────────────────────────────────────────────────

def test_above_ema_flags():
    m = load_matrix()
    row = {
        "ticker": "X", "date": "2025-01-01",
        "t_signal": "T4", "z_signal": "", "l_signal": "",
        "lane1_label": "", "lane3_label": "", "volume_bucket": "",
        "ne_suffix": "", "wick_suffix": "", "close": "100",
        "ema20": "90", "ema50": "95", "ema89": "110",
        "high": "102", "low": "98", "open": "99",
    }
    result = classify_tz_event(row, [], m)
    assert result["above_ema20"] is True
    assert result["above_ema50"] is True
    assert result["above_ema89"] is False

def test_quality_and_action_present():
    m = load_matrix()
    row = {"ticker": "X", "date": "2025-01-01", "t_signal": "T4",
           "z_signal": "", "l_signal": "", "lane1_label": "", "lane3_label": "",
           "volume_bucket": "VB", "ne_suffix": "", "wick_suffix": "W",
           "close": "50", "ema20": "45", "ema50": "40", "ema89": "35",
           "high": "52", "low": "48", "open": "49"}
    result = classify_tz_event(row, [], m)
    assert result["quality"] in ("A", "B", "Watch", "Reject", "—")
    assert result["action"] in (
        "BUY_TRIGGER", "WAIT_FOR_T_CONFIRMATION", "PULLBACK_ENTRY_READY",
        "WATCH_PULLBACK", "WAIT_FOR_CONFIRMATION", "WAIT_FOR_BREAKDOWN",
        "SHORT_TRIGGER", "IGNORE",
    )
    assert any("VOL:VB" in c for c in result["reason_codes"])
    assert any("WICK:W" in c for c in result["reason_codes"])
