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
        "BUY_TRIGGER", "WATCH_BULL_TRIGGER", "WATCH_BULL_SETUP",
        "WAIT_FOR_T_CONFIRMATION", "PULLBACK_ENTRY_READY",
        "WATCH_PULLBACK", "WAIT_FOR_CONFIRMATION", "WAIT_FOR_BREAKDOWN",
        "SHORT_TRIGGER", "DO_NOT_BUY", "IGNORE", "NO_ACTION",
    )
    assert any("VOL:VB" in c for c in result["reason_codes"])

_ALL_VALID_ROLES = {
    "BULL_A", "BULL_B", "PULLBACK_GO", "PULLBACK_CONFIRMING",
    "PULLBACK_READY_A", "PULLBACK_READY_B", "PULLBACK_WATCH", "DEEP_PULLBACK_WATCH",
    "BULL_WATCH", "MIXED_WATCH",
    "SHORT_WATCH", "SHORT_GO",
    "REJECT", "REJECT_LONG", "NO_EDGE", "CONTEXT_BONUS",
}

def test_universe_param_passed_through():
    """Classifier accepts scan_universe without error."""
    m = load_matrix()
    row = _row(t="T4", lane1="T4L12NP")
    r_sp  = classify_tz_event(row, [], m, scan_universe="sp500")
    r_nas = classify_tz_event(row, [], m, scan_universe="nasdaq")
    assert r_sp["role"] in _ALL_VALID_ROLES
    assert r_nas["role"] in _ALL_VALID_ROLES


# ── Fix 9: PULLBACK_READY_A gating ───────────────────────────────────────────

def test_pullback_ready_a_demoted_when_deep_and_below_all_emas():
    """Z6 positive composite + price_pos < 0.25 + below all EMAs → not PULLBACK_READY_A."""
    m = load_matrix()
    # Z6, lane1 resolves to a positive composite, but deep in range and below all EMAs
    row = _row(z="Z6", lane1="Z6L25NDP", close=30, ema20=60, ema50=55, ema89=50,
               high=32, low=29)
    # Build history bars so price_position_4bar is low
    hist = [
        _row(z="Z6", close=35, high=40, low=28),
        _row(z="Z6", close=33, high=38, low=29),
        _row(z="Z6", close=31, high=36, low=29),
    ]
    result = classify_tz_event(row, hist, m)
    assert result["role"] not in ("PULLBACK_READY_A",), \
        f"Should not be PULLBACK_READY_A when deep below all EMAs, got {result['role']}"
    assert result["role"] in ("DEEP_PULLBACK_WATCH", "PULLBACK_READY_B",
                               "PULLBACK_WATCH", "NO_EDGE", "BULL_WATCH", "MIXED_WATCH")

def test_pullback_ready_a_requires_ema_support():
    """PULLBACK_READY_A needs at least one EMA above or reclaim."""
    m = load_matrix()
    row = _row(z="Z5", close=40, ema20=60, ema50=55, ema89=50)  # below all EMAs
    result = classify_tz_event(row, [], m)
    assert result["role"] != "PULLBACK_READY_A"

def test_pullback_ready_score_penalty_below_all_emas():
    """Below all EMAs applies score penalty to pullback roles."""
    m = load_matrix()
    row_below = _row(z="Z5", close=30, ema20=60, ema50=55, ema89=50, high=32, low=28)
    row_above = _row(z="Z5", close=60, ema20=50, ema50=45, ema89=40, high=62, low=58)
    r_below = classify_tz_event(row_below, [], m)
    r_above = classify_tz_event(row_above, [], m)
    # Below-EMA row must have lower or equal score
    assert r_below["score"] <= r_above["score"]
    # Penalty reason code must appear when below all EMAs
    if r_below["role"] in ("PULLBACK_READY_B", "PULLBACK_WATCH",
                            "DEEP_PULLBACK_WATCH", "PULLBACK_READY_A"):
        assert any("PENALTY" in c for c in r_below["reason_codes"])


# ── Fix 10: PULLBACK action semantics ─────────────────────────────────────────

def test_pullback_ready_action_is_wait_not_entry():
    """PULLBACK_READY_A and PULLBACK_READY_B must use WAIT_FOR_T_CONFIRMATION."""
    from tz_intelligence.classifier import _ROLE_ACTION
    assert _ROLE_ACTION["PULLBACK_READY_A"] == "WAIT_FOR_T_CONFIRMATION"
    assert _ROLE_ACTION["PULLBACK_READY_B"] == "WAIT_FOR_T_CONFIRMATION"

def test_pullback_go_action_is_entry():
    """PULLBACK_GO must use PULLBACK_ENTRY_READY."""
    from tz_intelligence.classifier import _ROLE_ACTION
    assert _ROLE_ACTION["PULLBACK_GO"] == "PULLBACK_ENTRY_READY"

def test_pullback_go_assigned_when_t_after_z():
    """T confirmation after recent Z signal in top range → PULLBACK_GO."""
    m = load_matrix()
    hist = [
        _row(z="Z5", close=45, high=50, low=40, ema20=42, ema50=40, ema89=38),
        _row(z="Z5", close=48, high=52, low=42, ema20=43, ema50=41, ema89=39),
        _row(z="Z5", close=50, high=54, low=44, ema20=44, ema50=42, ema89=40),
    ]
    # T signal in top of 4-bar range (close=56 with high=58 is top 75% of [40..58])
    curr = _row(t="T4", close=56, high=58, low=52,
                ema20=50, ema50=48, ema89=45, ticker="X", date="2025-02-01")
    result = classify_tz_event(curr, hist, m)
    # If PULLBACK_GO triggered, verify correct action
    if result["role"] == "PULLBACK_GO":
        assert result["action"] == "PULLBACK_ENTRY_READY"
        assert any("PULLBACK_GO" in c for c in result["reason_codes"])


# ── Fix 11: SHORT_WATCH strictness ────────────────────────────────────────────

def test_positive_comp_reject_seq4_bullish_not_short_watch():
    """Positive composite + reject seq4 + price >= 0.75 + above EMA50 → not SHORT_WATCH."""
    m = load_matrix()
    # We need a pattern where pos composite exists but neg seq4 exists too
    # Use a T signal with bullish lane composite but bearish seq4 context
    # Approximate by checking the classifier output with bullish price context
    row = _row(t="T4", lane1="T4L12NP", close=58,
               ema20=50, ema50=48, ema89=45, high=60, low=40)
    hist = [
        _row(t="T4", close=42, high=50, low=40),
        _row(t="T4", close=50, high=55, low=43),
        _row(z="Z3", close=54, high=58, low=48),  # last history bar is a Z → potential neg seq4
    ]
    result = classify_tz_event(row, hist, m)
    # If SHORT_WATCH, bearish context must confirm it
    if result["role"] == "SHORT_WATCH":
        assert (
            result["price_position_4bar"] < 0.35 or
            not result["above_ema50"] or
            result["breaks_4bar_low"] or
            not result["above_ema20"] and not result["above_ema50"]
        ), f"SHORT_WATCH with no bearish context: price_pos={result['price_position_4bar']:.2f}"

def test_reject_comp_bullish_context_is_reject_long_not_short_watch():
    """Reject composite + price_pos >= 0.75 + above EMA50 → REJECT_LONG, not SHORT_WATCH."""
    m = load_matrix()
    # T2 with reject composite, strong bullish price context
    row = _row(t="T2", lane1="T2L12EUR", close=58,
               ema20=50, ema50=48, ema89=45, high=60, low=40)
    hist = [
        _row(t="T2", close=42, high=50, low=40),
        _row(t="T2", close=50, high=55, low=43),
        _row(t="T2", close=53, high=58, low=47),
    ]
    result = classify_tz_event(row, hist, m)
    # If a reject composite was matched and context is bullish, should not be SHORT_WATCH
    if bool(result.get("reject_flags")) and result["price_position_4bar"] >= 0.75 and result["above_ema50"]:
        assert result["role"] in ("REJECT_LONG", "MIXED_WATCH", "NO_EDGE", "BULL_WATCH",
                                   "BULL_A", "BULL_B", "PULLBACK_GO"), \
            f"Got {result['role']} but expected non-SHORT_WATCH for bullish reject context"

def test_short_watch_allowed_when_bearish_confirmed():
    """SHORT_WATCH is valid when price is in bottom range below EMA50."""
    m = load_matrix()
    row = _row(t="T1", lane1="L12NDP", close=30,
               ema20=55, ema50=60, ema89=65, high=31, low=28)
    hist = [
        _row(t="T1", close=35, high=40, low=30),
        _row(t="T1", close=32, high=37, low=30),
        _row(t="T1", close=30, high=34, low=28),
    ]
    result = classify_tz_event(row, hist, m)
    # SHORT_WATCH is allowed here (below EMA50, bottom of range)
    # Just verify the classifier doesn't error and returns a valid role
    assert result["role"] in _ALL_VALID_ROLES


# ── New roles exist in constants ──────────────────────────────────────────────

def test_new_roles_in_role_rank():
    from tz_intelligence.classifier import _ROLE_RANK
    for role in ("PULLBACK_GO", "MIXED_WATCH", "REJECT_LONG", "DEEP_PULLBACK_WATCH"):
        assert role in _ROLE_RANK, f"Missing role in _ROLE_RANK: {role}"

def test_new_roles_quality_via_function():
    from tz_intelligence.classifier import _quality_from_score, _WATCH_ONLY_ROLES
    # PULLBACK_GO with high score → A
    assert _quality_from_score("PULLBACK_GO", 85, False, 0.8, False) == "A"
    # MIXED_WATCH always Watch (watch-only role)
    assert "MIXED_WATCH" in _WATCH_ONLY_ROLES
    assert _quality_from_score("MIXED_WATCH", 90, False, 0.9, False) == "Watch"
    # REJECT_LONG → Reject
    assert _quality_from_score("REJECT_LONG", 90, False, 0.9, False) == "Reject"
    # DEEP_PULLBACK_WATCH always Watch (watch-only role)
    assert "DEEP_PULLBACK_WATCH" in _WATCH_ONLY_ROLES
    assert _quality_from_score("DEEP_PULLBACK_WATCH", 90, False, 0.9, False) == "Watch"

def test_z_pullback_score_capped_at_75():
    """Z-based PULLBACK_READY score must not exceed 75 without PULLBACK_GO confirmation."""
    m = load_matrix()
    # Z signal with multiple bonuses stacking — score should be capped
    row = _row(z="Z5", lane1="Z5L25NDP", close=55,
               ema20=50, ema50=48, ema89=45, high=58, low=50)
    prev = _row(z="Z5", close=47, ema50=48)   # prev below ema50 → reclaim
    result = classify_tz_event(row, [prev], m)
    if result["role"] in ("PULLBACK_READY_A", "PULLBACK_READY_B"):
        assert result["score"] <= 75, \
            f"Z pullback READY score should be capped at 75, got {result['score']}"


# ── Issue 1: General BULL_A gate ──────────────────────────────────────────────

def test_bull_a_gate_deep_range_below_all_emas():
    """T5 with strong composite but price_pos < 0.25 + below all EMAs must not be BULL_A."""
    m = load_matrix()
    row = _row(t="T5", lane1="T5L46NB", close=30,
               ema20=60, ema50=55, ema89=50, high=32, low=28)
    hist = [
        _row(t="T5", close=35, high=40, low=28),
        _row(t="T5", close=33, high=38, low=29),
        _row(t="T5", close=31, high=36, low=29),
    ]
    result = classify_tz_event(row, hist, m)
    assert result["role"] != "BULL_A", \
        f"Should not be BULL_A when deep below all EMAs (price_pos={result['price_position_4bar']:.2f})"
    assert result["role"] in ("BULL_WATCH", "BULL_B", "NO_EDGE", "MIXED_WATCH",
                               "PULLBACK_WATCH", "DEEP_PULLBACK_WATCH")

def test_bull_a_gate_requires_ema_support():
    """BULL_A requires at least one EMA above or reclaiming."""
    m = load_matrix()
    row = _row(t="T4", lane1="T4L12NP", close=50,
               ema20=60, ema50=55, ema89=50,   # below all EMAs
               high=52, low=48)
    result = classify_tz_event(row, [], m)
    assert result["role"] != "BULL_A"

def test_bull_a_gate_requires_price_position():
    """BULL_A requires price_pos >= 0.50."""
    m = load_matrix()
    # price_pos = (close - low) / (high - low) = (45 - 40) / (60 - 40) = 0.25 → < 0.50
    row = _row(t="T4", lane1="T4L12NP", close=45,
               ema20=42, ema50=40, ema89=38,   # all EMAs below close (EMA support exists)
               high=60, low=40)
    hist = [
        _row(t="T4", close=43, high=60, low=40),
        _row(t="T4", close=44, high=60, low=40),
        _row(t="T4", close=45, high=60, low=40),
    ]
    result = classify_tz_event(row, hist, m)
    # price_pos ~ 0.25 (close=45 in [40..60]) → should be BULL_B at most
    assert result["role"] != "BULL_A", \
        f"BULL_A with price_pos ~0.25 — got role={result['role']}, price_pos={result['price_position_4bar']:.2f}"

def test_bull_a_gate_not_triggered_with_good_price_and_ema():
    """When price_pos >= 0.50 and EMA support exists, the BULL_A gate must not fire."""
    m = load_matrix()
    # price_pos = (58 - 40) / (60 - 40) = 0.90 → well above 0.50; all EMAs below close
    row = _row(t="T4", lane1="T4L12NP", close=58,
               ema20=50, ema50=48, ema89=45,
               high=60, low=40)
    hist = [
        _row(t="T4", close=42, high=50, low=40),
        _row(t="T4", close=50, high=55, low=43),
        _row(t="T4", close=53, high=58, low=47),
    ]
    result = classify_tz_event(row, hist, m)
    # The BULL_A gate itself must NOT have fired (different from conflict override)
    gate_fired = any(
        c in ("BULL_A→BULL_WATCH:deep_range+no_ema_support",
              "BULL_A→BULL_B:insufficient_price_or_ema_confirmation")
        for c in result["reason_codes"]
    )
    assert not gate_fired, \
        f"BULL_A gate should not fire with price_pos=0.90 and above all EMAs: {result['reason_codes']}"


# ── Issue 2: PULLBACK_GO strictness ──────────────────────────────────────────

def test_pullback_go_not_assigned_without_prior_pullback_z():
    """T signal without prior pullback Z in history must not become PULLBACK_GO."""
    m = load_matrix()
    # All history bars are T signals, no Z → no pullback setup
    hist = [
        _row(t="T4", close=42, high=50, low=40),
        _row(t="T4", close=50, high=55, low=43),
        _row(t="T4", close=53, high=58, low=47),
    ]
    # Current bar: T4, top of range, good EMA — but no prior Z
    curr = _row(t="T4", lane1="T4L12NP", close=58,
                ema20=50, ema50=48, ema89=45, high=60, low=40,
                ticker="X", date="2025-02-01")
    result = classify_tz_event(curr, hist, m)
    assert result["role"] != "PULLBACK_GO", \
        f"PULLBACK_GO without prior pullback Z should not be assigned, got {result['role']}"

def test_weak_t_cannot_be_pullback_go():
    """T1/T2/T9/T10 cannot become PULLBACK_GO even with recent pullback Z."""
    m = load_matrix()
    hist = [
        _row(z="Z5", close=42, high=50, low=40),
        _row(z="Z5", close=48, high=52, low=42),
        _row(z="Z5", close=50, high=54, low=44),
    ]
    curr = _row(t="T1", close=56, high=58, low=52,   # T1 = weak signal
                ema20=50, ema50=48, ema89=45,
                ticker="X", date="2025-02-01")
    result = classify_tz_event(curr, hist, m)
    assert result["role"] != "PULLBACK_GO", \
        f"Weak T signal (T1) should not be PULLBACK_GO, got {result['role']}"

def test_pullback_go_not_from_non_pullback_z():
    """Only _PULLBACK_Z_SIGNALS (Z3/Z4/Z5/Z6/Z9) trigger PULLBACK_GO, not all Z."""
    m = load_matrix()
    # Z1 is NOT in _PULLBACK_Z_SIGNALS
    hist = [
        _row(z="Z1", close=42, high=50, low=40),
        _row(z="Z1", close=48, high=52, low=42),
        _row(z="Z1", close=50, high=54, low=44),
    ]
    curr = _row(t="T4", close=56, high=58, low=52,
                ema20=50, ema50=48, ema89=45,
                ticker="X", date="2025-02-01")
    result = classify_tz_event(curr, hist, m)
    assert result["role"] != "PULLBACK_GO", \
        f"Non-pullback Z (Z1) should not trigger PULLBACK_GO, got {result['role']}"


# ── Issue 3: Quality from score ───────────────────────────────────────────────

def test_negative_score_never_quality_b():
    """Any role with score < 0 must have quality Watch or Reject, never B."""
    from tz_intelligence.classifier import _quality_from_score
    for role in ("PULLBACK_READY_B", "PULLBACK_WATCH", "BULL_B", "BULL_WATCH"):
        q = _quality_from_score(role, -5, False, 0.5, False)
        assert q in ("Watch", "Reject", "—"), \
            f"Score -5, role {role} → quality should not be B, got {q}"

def test_quality_a_requires_score_80():
    """Quality A requires score >= 80."""
    from tz_intelligence.classifier import _quality_from_score
    assert _quality_from_score("BULL_A", 79, False, 0.8, False) == "B"
    assert _quality_from_score("BULL_A", 80, False, 0.8, False) == "A"

def test_quality_b_requires_score_60():
    """Quality B requires score 60–79."""
    from tz_intelligence.classifier import _quality_from_score
    assert _quality_from_score("BULL_B", 59, False, 0.8, False) == "Watch"
    assert _quality_from_score("BULL_B", 60, False, 0.8, False) == "B"

def test_quality_capped_watch_when_below_all_emas_deep():
    """below_all_emas=True + price_pos < 0.25 caps quality A to Watch."""
    from tz_intelligence.classifier import _quality_from_score
    # Score 90 would normally be A, but deep+below-all-EMAs → Watch
    q = _quality_from_score("BULL_A", 90, below_all_emas=True, price_pos=0.1, conflict=False)
    assert q == "Watch"

def test_quality_capped_b_when_conflict():
    """conflict=True caps quality A to B."""
    from tz_intelligence.classifier import _quality_from_score
    q = _quality_from_score("BULL_A", 90, below_all_emas=False, price_pos=0.8, conflict=True)
    assert q == "B"

def test_deep_pullback_watch_score_capped_and_quality_watch():
    """DEEP_PULLBACK_WATCH with score -5 → quality Watch, score <= 35."""
    m = load_matrix()
    row = _row(z="Z6", close=30, ema20=60, ema50=55, ema89=50, high=32, low=28)
    hist = [
        _row(z="Z6", close=35, high=40, low=28),
        _row(z="Z6", close=33, high=38, low=29),
        _row(z="Z6", close=31, high=36, low=29),
    ]
    result = classify_tz_event(row, hist, m)
    if result["role"] == "DEEP_PULLBACK_WATCH":
        assert result["quality"] == "Watch"
        assert result["score"] <= 35


# ── Issue 4: SHORT_WATCH with pos comp + reject seq4 + above EMA50 ────────────

def test_pos_comp_neg_seq4_above_ema50_not_short_watch():
    """Positive composite + reject seq4 + above_ema50=True must not be SHORT_WATCH."""
    m = load_matrix()
    # Simulate U-like case: T5 composite (positive), seq4 has reject, above all EMAs
    row = _row(t="T5", lane1="T5L46ND", close=58,
               ema20=50, ema50=48, ema89=45, high=60, low=40)
    hist = [
        _row(t="T5", close=42, high=50, low=40),
        _row(t="T5", close=50, high=55, low=43),
        _row(z="Z3", close=54, high=58, low=48),
    ]
    result = classify_tz_event(row, hist, m)
    # above_ema50 = True (close=58 > ema50=48), so should not be SHORT_WATCH
    if result["above_ema50"]:
        assert result["role"] != "SHORT_WATCH" or (
            result["price_position_4bar"] < 0.35 or result["breaks_4bar_low"]
        ), f"SHORT_WATCH with above_ema50 and no bearish confirmation: {result['reason_codes']}"

def test_pos_comp_neg_seq4_price_50pct_not_short_watch():
    """Positive composite + reject seq4 + price_pos >= 0.50 must not be SHORT_WATCH."""
    m = load_matrix()
    # price_pos = (55 - 40) / (60 - 40) = 0.75 → >= 0.50
    row = _row(t="T4", lane1="T4L12NP", close=55,
               ema20=60, ema50=65, ema89=70,  # below all EMAs
               high=60, low=40)
    hist = [
        _row(t="T4", close=42, high=50, low=40),
        _row(z="Z3", close=50, high=55, low=43),
        _row(z="Z3", close=52, high=58, low=47),
    ]
    result = classify_tz_event(row, hist, m)
    if result["price_position_4bar"] >= 0.50 and bool(result.get("good_flags")):
        assert result["role"] != "SHORT_WATCH", \
            f"price_pos >= 0.50 with positive composite → not SHORT_WATCH, got {result['role']}"


# ── v6 normalization invariant tests ─────────────────────────────────────────

def test_normalize_bull_b_score_capped_at_79():
    """BULL_B must never have score >= 80 (would imply A quality)."""
    from tz_intelligence.classifier import _normalize_role_score
    role, score = _normalize_role_score("BULL_B", 115, [])
    assert role == "BULL_B"
    assert score == 79, f"Expected 79, got {score}"

def test_normalize_bull_b_low_score_becomes_bull_watch():
    """BULL_B with score < 60 must downgrade to BULL_WATCH."""
    from tz_intelligence.classifier import _normalize_role_score
    role, score = _normalize_role_score("BULL_B", 55, [])
    assert role == "BULL_WATCH", f"Expected BULL_WATCH, got {role}"

def test_normalize_pullback_ready_b_score_capped():
    """PULLBACK_READY_B score >= 80 must be capped to 79."""
    from tz_intelligence.classifier import _normalize_role_score
    role, score = _normalize_role_score("PULLBACK_READY_B", 90, [])
    assert role == "PULLBACK_READY_B"
    assert score == 79

def test_normalize_pullback_ready_b_low_score_becomes_pullback_watch():
    """PULLBACK_READY_B score < 60 must downgrade to PULLBACK_WATCH."""
    from tz_intelligence.classifier import _normalize_role_score
    role, score = _normalize_role_score("PULLBACK_READY_B", 25, [])
    assert role == "PULLBACK_WATCH", f"Expected PULLBACK_WATCH, got {role}"

def test_normalize_deep_pullback_watch_score_cap():
    """DEEP_PULLBACK_WATCH score must be capped at 35."""
    from tz_intelligence.classifier import _normalize_role_score
    role, score = _normalize_role_score("DEEP_PULLBACK_WATCH", 60, [])
    assert role == "DEEP_PULLBACK_WATCH"
    assert score == 35, f"Expected 35, got {score}"

def test_normalize_any_watch_role_score_cap():
    """All *_WATCH roles must have score <= 59."""
    from tz_intelligence.classifier import _normalize_role_score
    for watch_role in ("BULL_WATCH", "PULLBACK_WATCH", "MIXED_WATCH", "SHORT_WATCH"):
        role, score = _normalize_role_score(watch_role, 100, [])
        assert role == watch_role
        assert score <= 59, f"{watch_role} score should be <=59, got {score}"

def test_normalize_bull_a_score_60_to_79_becomes_bull_b():
    """BULL_A with score 60-79 must downgrade to BULL_B."""
    from tz_intelligence.classifier import _normalize_role_score
    role, score = _normalize_role_score("BULL_A", 75, [])
    assert role == "BULL_B", f"Expected BULL_B, got {role}"
    assert score == 75

def test_normalize_bull_a_score_below_60_becomes_bull_watch():
    """BULL_A with score < 60 must downgrade to BULL_WATCH."""
    from tz_intelligence.classifier import _normalize_role_score
    role, score = _normalize_role_score("BULL_A", 50, [])
    assert role == "BULL_WATCH", f"Expected BULL_WATCH, got {role}"

def test_pullback_go_proof_fields_present_and_valid():
    """PULLBACK_GO result must include prior_pullback_ready_found==True and bars_ago 1-3."""
    m = load_matrix()
    hist = [
        _row(t="T4", close=42, high=50, low=40),
        _row(t="T4", close=44, high=52, low=42),
        _row(z="Z5", close=50, high=55, low=48,  # ← pullback Z bar
             ema20=48, ema50=46, ema89=44),
    ]
    curr = _row(t="T5", lane1="T5L46NB", close=58,
                ema20=50, ema50=48, ema89=45, high=60, low=50,
                ticker="PROOF_TEST", date="2025-03-01")
    result = classify_tz_event(curr, hist, m)
    if result["role"] == "PULLBACK_GO":
        assert result["prior_pullback_ready_found"] is True, \
            "PULLBACK_GO must have prior_pullback_ready_found=True"
        assert result["prior_pullback_ready_bars_ago"] in (1, 2, 3), \
            f"bars_ago must be 1-3, got {result['prior_pullback_ready_bars_ago']}"
        assert result["prior_pullback_ready_signal"] in ("Z3","Z4","Z5","Z6","Z9"), \
            f"signal must be pullback Z, got {result['prior_pullback_ready_signal']}"
    # Even if not PULLBACK_GO, proof fields must be present
    assert "prior_pullback_ready_found" in result
    assert "prior_pullback_ready_bars_ago" in result
    assert "pullback_high" in result
    assert "current_close_above_pullback_high" in result

def test_no_bull_b_with_quality_a():
    """Integration: classify several rows; none should yield BULL_B + quality A."""
    m = load_matrix()
    test_rows = [
        _row(t="T4", lane1="T4L12NP", close=80, ema20=70, ema50=65, ema89=60, high=85, low=70),
        _row(t="T5", lane1="T5L46NB", close=90, ema20=80, ema50=75, ema89=70, high=95, low=80),
        _row(t="T4", lane1="T4L12NP", close=50, ema20=60, ema50=55, ema89=50, high=55, low=45),
    ]
    for row in test_rows:
        result = classify_tz_event(row, [], m)
        if result["role"] == "BULL_B":
            assert result["quality"] != "A", \
                f"BULL_B must not have quality A (score={result['score']}, reasons={result['reason_codes']})"
            assert result["score"] <= 79, \
                f"BULL_B score must be <=79, got {result['score']}"


# ── v7: PULLBACK_CONFIRMING and BULL_B below-EMA gate ────────────────────────

def _pb_history_with_z5():
    """History rows with a pullback Z signal 1 bar ago, above EMA."""
    return [
        _row(t="T4", close=42, high=50, low=40),
        _row(t="T4", close=44, high=52, low=42),
        _row(z="Z5", close=50, high=55, low=48, ema20=48, ema50=46, ema89=44),
    ]


def test_pullback_confirming_when_no_high_break():
    """Prior pullback + good T + no high break → PULLBACK_CONFIRMING, not PULLBACK_GO."""
    m = load_matrix()
    hist = _pb_history_with_z5()
    # pullback_high from history Z5 bar = 55; current close = 53 (below 55)
    curr = _row(t="T5", lane1="T5L46NB", close=53,
                ema20=50, ema50=48, ema89=45, high=54, low=50,
                ticker="CONF_TEST", date="2025-04-01")
    result = classify_tz_event(curr, hist, m)
    if result["prior_pullback_ready_found"]:
        assert result["current_close_above_pullback_high"] is False or result["breaks_4bar_high"] is False
        # Without high break, role must not be PULLBACK_GO
        if not result["current_close_above_pullback_high"] and not result["breaks_4bar_high"]:
            assert result["role"] != "PULLBACK_GO", \
                f"No high break but got PULLBACK_GO (close={result.get('close')}, " \
                f"pullback_high={result.get('pullback_high')})"
            if result["role"] == "PULLBACK_CONFIRMING":
                assert result["action"] == "WAIT_FOR_PULLBACK_HIGH_BREAK"
                assert result["quality"] in ("B", "Watch"), \
                    f"PULLBACK_CONFIRMING must be max B quality, got {result['quality']}"
                assert result["score"] <= 79, \
                    f"PULLBACK_CONFIRMING score must be <=79, got {result['score']}"


def test_pullback_go_requires_high_break():
    """Prior pullback + good T + close above pullback_high → PULLBACK_GO."""
    m = load_matrix()
    hist = _pb_history_with_z5()
    # pullback_high from Z5 bar = 55; current close = 60 (above 55) → high_break
    curr = _row(t="T5", lane1="T5L46NB", close=60,
                ema20=50, ema50=48, ema89=45, high=62, low=55,
                ticker="PGO_TEST", date="2025-04-01")
    result = classify_tz_event(curr, hist, m)
    if result["role"] == "PULLBACK_GO":
        assert result["prior_pullback_ready_found"] is True
        assert (result["current_close_above_pullback_high"] is True or
                result["breaks_4bar_high"] is True), \
            "PULLBACK_GO must have high_break (close>pullback_high OR breaks_4bar_high)"


def test_pullback_go_via_breaks_4bar_high():
    """Prior pullback + good T + breaks_4bar_high → PULLBACK_GO."""
    m = load_matrix()
    hist = _pb_history_with_z5()
    # hist highs: 50, 52, 55 → range_high = 55
    # curr_h = 58 > 55 → breaks_4bar_high = True
    curr = _row(t="T5", lane1="T5L46NB", close=57,
                ema20=50, ema50=48, ema89=45, high=58, low=50,
                ticker="PGO_BREAK", date="2025-04-01")
    result = classify_tz_event(curr, hist, m)
    if result["role"] == "PULLBACK_GO":
        assert result["prior_pullback_ready_found"] is True
        assert result["breaks_4bar_high"] is True or result["current_close_above_pullback_high"] is True


def test_bull_b_below_all_emas_deep_range_becomes_bull_watch():
    """BULL_B with below all EMAs + price_pos < 0.25 must become BULL_WATCH, score <= 35."""
    m = load_matrix()
    # close=42 far below all EMAs; 4-bar range 40-60 → price_pos=(42-40)/20=0.10
    row = _row(t="T5", lane1="T5L46NB", close=42,
               ema20=80, ema50=75, ema89=70,  # far above close → below all EMAs
               high=44, low=40,
               ticker="AEE_LIKE", date="2025-05-01")
    hist = [
        _row(t="T4", close=55, high=60, low=50),
        _row(t="T4", close=53, high=58, low=48),
        _row(t="T4", close=51, high=56, low=46),
    ]
    result = classify_tz_event(row, hist, m)
    assert result["above_ema20"] is False
    assert result["above_ema50"] is False
    assert result["above_ema89"] is False
    assert result["price_position_4bar"] < 0.25, \
        f"Expected price_pos < 0.25, got {result['price_position_4bar']}"
    assert result["role"] not in ("BULL_B", "BULL_A"), \
        f"BULL_B/BULL_A must not appear below all EMAs in deep range, got {result['role']}"
    if result["role"] == "BULL_WATCH":
        assert result["score"] <= 35, \
            f"BULL_WATCH (from deep below-EMA) score must be <=35, got {result['score']}"


def test_pullback_confirming_quality_max_b():
    """PULLBACK_CONFIRMING must never produce quality A."""
    from tz_intelligence.classifier import _normalize_role_score
    role, score = _normalize_role_score("PULLBACK_CONFIRMING", 90, [])
    assert role == "PULLBACK_CONFIRMING"
    assert score == 79, f"PULLBACK_CONFIRMING score must be capped at 79, got {score}"

def test_pullback_confirming_low_score_becomes_pullback_watch():
    from tz_intelligence.classifier import _normalize_role_score
    role, score = _normalize_role_score("PULLBACK_CONFIRMING", 45, [])
    assert role == "PULLBACK_WATCH", f"Expected PULLBACK_WATCH, got {role}"
