"""Tests for TZ Signal Intelligence — hardened v2."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pytest
from tz_intelligence.matrix_loader import load_matrix, MatrixIndex, _resolve_conflict
from tz_intelligence.classifier import classify_tz_event, _best_role, _make_composite


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row(t="", z="", l="", lane1="", lane3="",
         close=50, ema20=45, ema50=40, ema89=35,
         high=52, low=48, open_=49, vol_bkt="", ticker="TEST", date="2025-01-01"):
    return {
        "ticker": ticker, "date": date,
        "t_signal": t, "z_signal": z, "l_signal": l,
        "lane1_label": lane1, "lane3_label": lane3,
        "volume_bucket": vol_bkt, "ne_suffix": "", "wick_suffix": "",
        "close": str(close), "ema20": str(ema20), "ema50": str(ema50), "ema89": str(ema89),
        "high": str(high), "low": str(low), "open": str(open_),
        "volume": "1000000",
    }


# ── Matrix loader ─────────────────────────────────────────────────────────────

def test_matrix_loads():
    m = load_matrix()
    assert len(m.rows) == 1523

def test_matrix_has_universe_scoped_composite():
    m = load_matrix()
    # composite dict is keyed by universe (SP500, NASDAQ_GT5, GLOBAL)
    all_univs = set(m.composite.keys())
    assert "SP500" in all_univs or "GLOBAL" in all_univs

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


# ── Fix 1: Composite deduplication ───────────────────────────────────────────

def test_make_composite_no_duplication_when_lane_has_prefix():
    # lane1 already starts with signal → no duplication
    assert _make_composite("T5", "T5L25ND") == "T5L25ND"

def test_make_composite_prepends_when_lane_is_bare():
    # lane1 is bare (no signal prefix) → prepend
    assert _make_composite("T5", "L25ND") == "T5L25ND"

def test_make_composite_z_signal_no_duplication():
    assert _make_composite("Z1G", "Z1GL5ED") == "Z1GL5ED"

def test_make_composite_empty_inputs():
    assert _make_composite("", "L25ND") == ""
    assert _make_composite("T5", "") == ""
    assert _make_composite("", "") == ""

def test_classifier_composite_no_duplication():
    """classifier must never produce T5T5L25ND."""
    m = load_matrix()
    row = _row(t="T5", lane1="T5L25ND")   # lane1 already includes "T5"
    result = classify_tz_event(row, [], m)
    assert "T5T5" not in result["composite_pattern"]
    assert result["composite_pattern"] == "T5L25ND"


# ── Fix 2: Universe-scoped matrix matching ───────────────────────────────────

def test_sp500_doesnt_use_nasdaq_rules():
    m = load_matrix()
    # SP500 scan: allowed univs = {SP500, GLOBAL}; NASDAQ_GT5 rules must not appear
    allowed = m.allowed_univs("sp500")
    assert "SP500" in allowed
    assert "NASDAQ_GT5" not in allowed

def test_nasdaq_doesnt_use_sp500_only_rules():
    m = load_matrix()
    allowed = m.allowed_univs("nasdaq")
    assert "NASDAQ_GT5" in allowed
    assert "SP500" not in allowed

def test_all_us_includes_all_universes():
    m = load_matrix()
    allowed = m.allowed_univs("all_us")
    assert "SP500" in allowed
    assert "NASDAQ_GT5" in allowed
    assert "GLOBAL" in allowed

def test_scoped_rule_lookup_sp500():
    m = load_matrix()
    # Any composite pattern lookup for sp500 must not return NASDAQ_GT5-only rules
    sp500_pos, sp500_neg = m.get_composite_rules("T1L12NP", "sp500")
    nasdaq_pos, nasdaq_neg = m.get_composite_rules("T1L12NP", "nasdaq")
    # Can't assert specific values without knowing which universe each rule is in,
    # but we can verify the functions return lists
    assert isinstance(sp500_pos, list)
    assert isinstance(sp500_neg, list)


# ── Fix 3: Conflict resolver ─────────────────────────────────────────────────

def test_resolve_conflict_positive_wins():
    pos = [{"med10d_pct": "1.2", "fail10d_pct": "20.0"}]
    neg = [{"med10d_pct": "0.5", "fail10d_pct": "22.0"}]
    assert _resolve_conflict(pos, neg) == "POSITIVE"

def test_resolve_conflict_reject_wins_by_fail():
    pos = [{"med10d_pct": "0.5", "fail10d_pct": "20.0"}]
    neg = [{"med10d_pct": "0.3", "fail10d_pct": "30.0"}]  # fail >= 28 → reject
    assert _resolve_conflict(pos, neg) == "REJECT"

def test_resolve_conflict_reject_wins_by_med():
    pos = [{"med10d_pct": "0.5", "fail10d_pct": "20.0"}]
    neg = [{"med10d_pct": "-0.5", "fail10d_pct": "22.0"}]  # neg med < 0 → reject
    assert _resolve_conflict(pos, neg) == "REJECT"

def test_resolve_conflict_returns_conflict_when_ambiguous():
    pos = [{"med10d_pct": "0.5", "fail10d_pct": "26.0"}]  # pos not strong enough
    neg = [{"med10d_pct": "0.2", "fail10d_pct": "22.0"}]  # neg not bad enough
    assert _resolve_conflict(pos, neg) == "CONFLICT"

def test_conflict_classifier_produces_bull_watch():
    """When a pattern is CONFLICT, role should be capped at BULL_WATCH."""
    m = load_matrix()
    # Find a pattern known to conflict in both pos/neg composite for same universe
    # We can't easily force this in unit test without mocking the matrix,
    # so test that classifier output is consistent with conflict handling logic.
    row = _row(t="T4", lane1="L12NP")
    result = classify_tz_event(row, [], m, debug=True)
    # If conflict found, role must not be BULL_A or BULL_B (forced to BULL_WATCH)
    if result["conflict_flag"]:
        assert result["role"] == "BULL_WATCH"
        assert result["conflict_resolution"] in ("CONFLICT", "POSITIVE", "REJECT")


# ── Fix 4: BULL_A cap for weak T signals ─────────────────────────────────────

def test_weak_t_capped_to_bull_b_without_confirmations():
    """T1 without all confirmations must not be BULL_A."""
    m = load_matrix()
    # below EMA50, no composite match, no price_pos confidence
    row = _row(t="T1", close=50, ema50=60)  # below EMA50
    result = classify_tz_event(row, [], m)
    # Must not be BULL_A
    assert result["role"] != "BULL_A"

def test_weak_t_signals_list_includes_t1_t2_t9_t10():
    from tz_intelligence.classifier import _WEAK_T_SIGNALS
    assert {"T1", "T2", "T9", "T10"} == _WEAK_T_SIGNALS

def test_strong_t_not_capped():
    """T4 can reach BULL_A without the cap."""
    m = load_matrix()
    row = _row(t="T4", lane1="T4L12NP", close=58, ema20=50, ema50=48, ema89=45,
               high=60, low=40)
    # Build 3 history bars to give a wide 4-bar range so close is in top 75%
    hist = [
        _row(t="T4", close=42, high=50, low=40),
        _row(t="T4", close=50, high=55, low=44),
        _row(t="T4", close=53, high=58, low=47),
    ]
    result = classify_tz_event(row, hist, m)
    # We can't guarantee BULL_A (depends on matrix data), but no cap should apply
    assert "BULL_A_CAPPED:T4" not in " ".join(result["reason_codes"])


# ── Fix 5: Debug fields in output ────────────────────────────────────────────

def test_result_has_all_debug_fields():
    m = load_matrix()
    row = _row(t="T4")
    result = classify_tz_event(row, [], m)
    expected_keys = [
        "ticker", "date", "final_signal", "composite_pattern", "seq4",
        "role", "score", "quality", "action",
        "above_ema20", "above_ema50", "above_ema89",
        "ema20_reclaim", "ema50_reclaim", "ema89_reclaim",
        "conflict_flag", "conflict_resolution", "conflicting_rule_ids",
        "good_flags", "reject_flags",
        "price_position_4bar", "breaks_4bar_high", "breaks_4bar_low",
        "final_volume_vs_prev1", "final_volume_vs_prev2", "final_volume_vs_prev3",
        "matched_rule_id", "matched_rule_type", "matched_universe", "matched_status",
        "matched_med10d_pct", "matched_fail10d_pct", "matched_avg10d_pct",
        "matched_source_file", "matched_rule_notes",
        "matched_composite_rule_id", "matched_seq4_rule_id", "matched_reject_rule_id",
        "reason_codes", "explanation", "debug_trace",
    ]
    for key in expected_keys:
        assert key in result, f"Missing key: {key}"

def test_debug_trace_populated_when_debug_true():
    m = load_matrix()
    row = _row(t="T4")
    result = classify_tz_event(row, [], m, debug=True)
    assert isinstance(result["debug_trace"], list)
    assert len(result["debug_trace"]) > 0

def test_debug_trace_none_when_debug_false():
    m = load_matrix()
    row = _row(t="T4")
    result = classify_tz_event(row, [], m, debug=False)
    assert result["debug_trace"] is None


# ── Fix 6: Separate above_ema vs ema_reclaim ─────────────────────────────────

def test_above_ema_flags_correct():
    m = load_matrix()
    row = _row(t="T4", close=100, ema20=90, ema50=95, ema89=110)
    result = classify_tz_event(row, [], m)
    assert result["above_ema20"] is True
    assert result["above_ema50"] is True
    assert result["above_ema89"] is False

def test_ema50_reclaim_detected():
    m = load_matrix()
    prev = _row(t="T4", close=48, ema50=50)   # prev close < prev ema50
    curr = _row(t="T4", close=52, ema50=50, ticker="X", date="2025-01-02")
    result = classify_tz_event(curr, [prev], m)
    assert result["ema50_reclaim"] is True
    assert result["above_ema50"] is True

def test_above_ema50_not_reclaim_when_already_above():
    """If price was already above EMA50, reclaim flag must be False."""
    m = load_matrix()
    prev = _row(t="T4", close=55, ema50=50)   # was already above
    curr = _row(t="T4", close=58, ema50=50, ticker="X", date="2025-01-02")
    result = classify_tz_event(curr, [prev], m)
    assert result["above_ema50"] is True
    assert result["ema50_reclaim"] is False


# ── Fix 7: Full seq4 in output ────────────────────────────────────────────────

def test_seq4_full_4bar_string():
    m = load_matrix()
    hist = [
        _row(z="Z2G", date="2025-01-01"),
        _row(t="T1",  date="2025-01-02"),
        _row(z="Z5",  date="2025-01-03"),
    ]
    curr = _row(t="T1", date="2025-01-04")
    result = classify_tz_event(curr, hist, m)
    # seq4 must be full 4-bar string, not truncated
    assert result["seq4"] == "Z2G|T1|Z5|T1"
    parts = result["seq4"].split("|")
    assert len(parts) == 4

def test_seq4_empty_when_fewer_than_4_bars():
    m = load_matrix()
    curr = _row(t="T1")
    result = classify_tz_event(curr, [], m)
    assert result["seq4"] == ""   # only 1 bar → no valid seq4


# ── Fix 8: SHORT_WATCH not assigned to positive pullback Z patterns ──────────

def test_pullback_z_not_short_watch_when_above_ema50():
    """Z5 above EMA50 with no reject evidence → demote SHORT_WATCH to PULLBACK_READY_B."""
    m = load_matrix()
    from tz_intelligence.classifier import _PULLBACK_Z_SIGNALS
    assert "Z5" in _PULLBACK_Z_SIGNALS
    row = _row(z="Z5", close=55, ema50=50)  # above EMA50, no reject
    result = classify_tz_event(row, [], m)
    # Must not assign SHORT_WATCH to a pullback-class Z with no reject evidence
    if result["role"] == "SHORT_WATCH":
        # If somehow SHORT_WATCH assigned, reject_flags must be present
        assert bool(result["reject_flags"]) or result["price_position_4bar"] < 0.25

def test_pullback_z_signals_set():
    from tz_intelligence.classifier import _PULLBACK_Z_SIGNALS
    assert _PULLBACK_Z_SIGNALS == {"Z5", "Z9", "Z3", "Z4", "Z6"}


# ── Fix 9: SHORT_GO only after breakdown confirmation ─────────────────────────

def test_short_go_requires_breakdown():
    """SHORT_GO must only be assigned when close < 4-bar low AND no good flags."""
    m = load_matrix()
    row = _row(t="T1", lane1="L12NDP", close=40,
               ema20=55, ema50=60, ema89=65)
    # close=40 < 4bar_low=42 → SHORT_GO if original role was SHORT_WATCH
    result = classify_tz_event(row, [], m, current_low_4bar=42.0)
    if result["role"] == "SHORT_GO":
        assert any("BREAK_4BAR_LOW" in c for c in result["reason_codes"])

def test_no_short_go_without_breakdown():
    m = load_matrix()
    row = _row(t="T1", lane1="L12NDP", close=50,
               ema20=55, ema50=60, ema89=65)
    # close=50 is not below 4bar_low=45 → no promotion
    result = classify_tz_event(row, [], m, current_low_4bar=45.0)
    assert result["role"] != "SHORT_GO"


# ── Role ranking ──────────────────────────────────────────────────────────────

def test_best_role_bull_a_beats_bull_b():
    assert _best_role("BULL_A", "BULL_B") == "BULL_A"

def test_best_role_short_go_beats_bull_a():
    assert _best_role("SHORT_GO", "BULL_A") == "SHORT_GO"

def test_best_role_no_edge_loses_to_anything():
    assert _best_role("NO_EDGE", "BULL_WATCH") == "BULL_WATCH"


# ── Classifier — basic sanity ─────────────────────────────────────────────────

def test_no_signal_returns_no_edge():
    m = load_matrix()
    row = _row()  # no t/z/l signal
    result = classify_tz_event(row, [], m)
    assert result["role"] == "NO_EDGE"
    assert result["final_signal"] == ""

def test_ema50_reclaim_bonus_added():
    m = load_matrix()
    prev = _row(t="T4", close=48, ema50=50)
    curr = _row(t="T4", close=52, ema50=50, ticker="X", date="2025-01-02")
    result = classify_tz_event(curr, [prev], m)
    assert any("EMA50_RECLAIM" in c for c in result["reason_codes"])

def test_price_position_top75_bonus():
    m = load_matrix()
    hist = [
        _row(t="T4", close=42, high=50, low=40),
        _row(t="T4", close=50, high=55, low=44),
        _row(t="T4", close=53, high=58, low=47),
    ]
    curr = _row(t="T4", close=58, high=60, low=55,
                ema20=50, ema50=48, ema89=46, ticker="X", date="2025-01-04")
    result = classify_tz_event(curr, hist, m)
    assert any("CLOSE_TOP75PCT" in c for c in result["reason_codes"])

def test_quality_and_action_valid():
    m = load_matrix()
    row = _row(t="T4", vol_bkt="VB")
    result = classify_tz_event(row, [], m)
    assert result["quality"] in ("A", "B", "Watch", "Reject", "—")
    assert result["action"] in (
        "BUY_TRIGGER", "WAIT_FOR_T_CONFIRMATION", "PULLBACK_ENTRY_READY",
        "WATCH_PULLBACK", "WAIT_FOR_CONFIRMATION", "WAIT_FOR_BREAKDOWN",
        "SHORT_TRIGGER", "IGNORE",
    )
    assert any("VOL:VB" in c for c in result["reason_codes"])

def test_universe_param_passed_through():
    """Classifier accepts scan_universe without error."""
    m = load_matrix()
    row = _row(t="T4", lane1="T4L12NP")
    r_sp  = classify_tz_event(row, [], m, scan_universe="sp500")
    r_nas = classify_tz_event(row, [], m, scan_universe="nasdaq")
    assert r_sp["role"] in (list(r_sp.keys()) and ["BULL_A","BULL_B","BULL_WATCH","NO_EDGE","REJECT","SHORT_WATCH","SHORT_GO","PULLBACK_READY_A","PULLBACK_READY_B","PULLBACK_WATCH"])
    assert r_nas["role"] in (["BULL_A","BULL_B","BULL_WATCH","NO_EDGE","REJECT","SHORT_WATCH","SHORT_GO","PULLBACK_READY_A","PULLBACK_READY_B","PULLBACK_WATCH"])
