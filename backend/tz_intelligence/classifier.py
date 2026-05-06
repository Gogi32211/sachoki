"""TZ Signal Intelligence classifier — hardened v6.

Fixes vs v5:
  24. _normalize_role_score() final pass enforcing role/score/quality consistency:
       *_A: score >= 80 required; else → *_B (60-79) or *_WATCH (<60)
       *_B: score must be 60-79; >=80 → cap to 79; <60 → *_WATCH
       DEEP_PULLBACK_WATCH: score <= 35
       any WATCH role: score <= 59
  25. Action table updated: BULL_WATCH → WATCH_BULL_SETUP, NO_EDGE → NO_ACTION
  26. PULLBACK_GO proof fields verified and exposed in all outputs
"""
from __future__ import annotations
from typing import Optional
from .matrix_loader import MatrixIndex

# ── Role constants ────────────────────────────────────────────────────────────

_ROLE_RANK = {
    "NO_EDGE":              0,
    "CONTEXT_BONUS":        0,
    "REJECT_LONG":          1,
    "REJECT":               2,
    "DEEP_PULLBACK_WATCH":  3,
    "BULL_WATCH":           4,
    "MIXED_WATCH":          4,
    "PULLBACK_WATCH":       4,
    "PULLBACK_READY_B":     5,
    "PULLBACK_READY_A":     6,
    "PULLBACK_GO":          7,
    "BULL_B":               7,
    "BULL_A":               8,
    "SHORT_WATCH":          9,
    "SHORT_GO":             10,
}

_ROLE_ACTION = {
    "BULL_A":               "BUY_TRIGGER",
    "BULL_B":               "WATCH_BULL_TRIGGER",
    "PULLBACK_GO":          "PULLBACK_ENTRY_READY",
    "PULLBACK_READY_A":     "WAIT_FOR_T_CONFIRMATION",
    "PULLBACK_READY_B":     "WAIT_FOR_T_CONFIRMATION",
    "PULLBACK_WATCH":       "WATCH_PULLBACK",
    "DEEP_PULLBACK_WATCH":  "WATCH_PULLBACK",
    "BULL_WATCH":           "WATCH_BULL_SETUP",
    "MIXED_WATCH":          "WAIT_FOR_CONFIRMATION",
    "SHORT_WATCH":          "WAIT_FOR_BREAKDOWN",
    "SHORT_GO":             "SHORT_TRIGGER",
    "REJECT":               "IGNORE",
    "REJECT_LONG":          "DO_NOT_BUY",
    "NO_EDGE":              "NO_ACTION",
}

_WEAK_T_SIGNALS    = {"T1", "T2", "T9", "T10"}
_PULLBACK_Z_SIGNALS = {"Z5", "Z9", "Z3", "Z4", "Z6"}

# Watch-only roles: score cannot promote them above Watch quality
_WATCH_ONLY_ROLES = {
    "DEEP_PULLBACK_WATCH", "PULLBACK_WATCH", "BULL_WATCH",
    "MIXED_WATCH", "SHORT_WATCH",
}

# ── Normalization mappings ────────────────────────────────────────────────────

_A_TO_B = {
    "BULL_A":          "BULL_B",
    "PULLBACK_READY_A": "PULLBACK_READY_B",
}
_A_TO_WATCH = {
    "BULL_A":          "BULL_WATCH",
    "PULLBACK_READY_A": "PULLBACK_WATCH",
}
_B_TO_WATCH = {
    "BULL_B":          "BULL_WATCH",
    "PULLBACK_READY_B": "PULLBACK_WATCH",
}


def _best_role(a: str, b: str) -> str:
    return a if _ROLE_RANK.get(a, 0) >= _ROLE_RANK.get(b, 0) else b


def _score(r: dict) -> int:
    try:
        return int(r.get("score_base") or 0)
    except (ValueError, TypeError):
        return 0


def _normalize_role_score(role: str, score: int,
                           reason_codes: list) -> tuple[str, int]:
    """Final normalization pass — enforces role/score consistency.

    *_A  → needs score >= 80; else → *_B (60-79) or *_WATCH (<60)
    *_B  → score 60-79; >=80 cap to 79; <60 downgrade to *_WATCH
    DEEP_PULLBACK_WATCH → score <= 35
    any WATCH role      → score <= 59
    """
    # Step 1 – *_A roles
    if role in _A_TO_B:
        if score < 60:
            new = _A_TO_WATCH[role]
            reason_codes.append(f"NORMALIZE:{role}→{new}:score={score}<60")
            role = new
        elif score < 80:
            new = _A_TO_B[role]
            reason_codes.append(f"NORMALIZE:{role}→{new}:score={score}<80")
            role = new
        # score >= 80 → stays *_A, correct

    # Step 2 – *_B roles
    if role in _B_TO_WATCH:
        if score >= 80:
            reason_codes.append(f"NORMALIZE:SCORE_CAP:{role}:{score}→79")
            score = 79
        elif score < 60:
            new = _B_TO_WATCH[role]
            reason_codes.append(f"NORMALIZE:{role}→{new}:score={score}<60")
            role = new

    # Step 3 – WATCH role score caps
    if role == "DEEP_PULLBACK_WATCH":
        if score > 35:
            reason_codes.append(f"NORMALIZE:SCORE_CAP:DEEP_PULLBACK_WATCH:{score}→35")
            score = 35
    elif "WATCH" in role:
        if score > 59:
            reason_codes.append(f"NORMALIZE:SCORE_CAP:{role}:{score}→59")
            score = 59

    return role, score


def _quality_from_score(role: str, score: int,
                         below_all_emas: bool, price_pos: float,
                         conflict: bool) -> str:
    """Score + context → quality grade (called after normalization)."""
    if role in ("NO_EDGE", "CONTEXT_BONUS"):
        return "—"
    if role in ("REJECT", "REJECT_LONG"):
        return "Reject"
    if role in _WATCH_ONLY_ROLES:
        return "Watch"

    if score < 0:
        q = "Watch"
    elif score < 60:
        q = "Watch"
    elif score < 80:
        q = "B"
    else:
        q = "A"

    # Hard caps (belt-and-suspenders after normalization)
    if q == "A" and below_all_emas and price_pos < 0.25:
        q = "Watch"
    if q == "A" and conflict:
        q = "B"

    return q


def _make_composite(signal: str, lane: str) -> str:
    if not signal or not lane:
        return ""
    if lane.startswith(signal):
        return lane
    return signal + lane


# ── Main classifier ───────────────────────────────────────────────────────────

def classify_tz_event(
    row: dict,
    history_rows: list,
    matrix: MatrixIndex,
    current_low_4bar: Optional[float] = None,
    current_high_4bar: Optional[float] = None,
    scan_universe: str = "sp500",
    debug: bool = False,
) -> dict:
    ticker = row.get("ticker", "")
    date   = row.get("date", "")
    debug_trace: list[str] = []

    t_sig = row.get("t_signal") or ""
    z_sig = row.get("z_signal") or ""
    l_sig = row.get("l_signal") or ""
    lane1 = row.get("lane1_label") or ""
    lane3 = row.get("lane3_label") or ""
    vol_bkt = row.get("volume_bucket") or ""
    ne_sfx  = row.get("ne_suffix")     or ""
    wk_sfx  = row.get("wick_suffix")   or ""

    def _fv(key: str) -> float:
        try: return float(row.get(key) or 0)
        except (TypeError, ValueError): return 0.0

    close  = _fv("close")
    ema20  = _fv("ema20")
    ema50  = _fv("ema50")
    ema89  = _fv("ema89")
    curr_h = _fv("high")
    curr_l = _fv("low")
    curr_v = _fv("volume")

    final_signal = t_sig or z_sig or l_sig or ""

    if debug:
        debug_trace.append(f"INPUT: ticker={ticker} date={date} signal={final_signal} "
                           f"lane1={lane1} lane3={lane3} universe={scan_universe}")

    if not final_signal:
        return _build_result(ticker, date, "", "", "NO_EDGE", 0, [],
                             "No T/Z/L signal", debug_trace=debug_trace if debug else None)

    # ── Baseline ──────────────────────────────────────────────────────────────
    baseline = matrix.baseline.get(final_signal)
    base_role = baseline.get("role_seed", "NO_EDGE") if baseline else "NO_EDGE"

    lane_source = lane1 or lane3
    composite_pattern = _make_composite(final_signal, lane_source)

    # ── Composite rules ───────────────────────────────────────────────────────
    pos_comp_rules, neg_comp_rules = [], []
    if composite_pattern:
        pos_comp_rules, neg_comp_rules = matrix.get_composite_rules(
            composite_pattern, scan_universe)

    # ── SEQ4 ─────────────────────────────────────────────────────────────────
    def _bar_sig(b: dict) -> str:
        return b.get("t_signal") or b.get("z_signal") or b.get("l_signal") or "—"

    seq_bars = list(history_rows[-3:]) + [row]
    seq4_str = "|".join(_bar_sig(b) for b in seq_bars) if len(seq_bars) == 4 else ""

    pos_seq4_rules, neg_seq4_rules = [], []
    if seq4_str:
        pos_seq4_rules, neg_seq4_rules = matrix.get_seq4_rules(seq4_str, scan_universe)

    # ── Conflict resolution ───────────────────────────────────────────────────
    comp_conflict_resolution = ""
    comp_conflict_ids: list[str] = []
    seq4_conflict_resolution = ""
    seq4_conflict_ids: list[str] = []

    if composite_pattern and pos_comp_rules and neg_comp_rules:
        comp_conflict = matrix.get_conflict(composite_pattern, "COMP", scan_universe)
        comp_conflict_resolution = comp_conflict or "CONFLICT"
        comp_conflict_ids = (
            [r.get("rule_id","") for r in pos_comp_rules] +
            [r.get("rule_id","") for r in neg_comp_rules]
        )

    if seq4_str and pos_seq4_rules and neg_seq4_rules:
        seq4_conflict = matrix.get_conflict(seq4_str, "SEQ4", scan_universe)
        seq4_conflict_resolution = seq4_conflict or "CONFLICT"
        seq4_conflict_ids = (
            [r.get("rule_id","") for r in pos_seq4_rules] +
            [r.get("rule_id","") for r in neg_seq4_rules]
        )

    has_conflict = bool(comp_conflict_resolution == "CONFLICT" or
                        seq4_conflict_resolution == "CONFLICT")

    def _effective_rules(pos, neg, conflict_res):
        if not pos or not neg:
            return pos, neg
        if conflict_res == "POSITIVE":
            return pos, []
        if conflict_res == "REJECT":
            return [], neg
        return [], []

    eff_pos_comp, eff_neg_comp = _effective_rules(
        pos_comp_rules, neg_comp_rules, comp_conflict_resolution)
    eff_pos_seq4, eff_neg_seq4 = _effective_rules(
        pos_seq4_rules, neg_seq4_rules, seq4_conflict_resolution)

    matched_composite_rule_id = (eff_pos_comp + eff_neg_comp)[0].get("rule_id","") if (eff_pos_comp or eff_neg_comp) else ""
    matched_seq4_rule_id      = (eff_pos_seq4 + eff_neg_seq4)[0].get("rule_id","") if (eff_pos_seq4 or eff_neg_seq4) else ""
    matched_reject_rule_id    = ((eff_neg_comp or eff_neg_seq4) or [{}])[0].get("rule_id","")

    all_matched_rules = eff_pos_comp + eff_neg_comp + eff_pos_seq4 + eff_neg_seq4
    matched_rule_id   = all_matched_rules[0].get("rule_id","") if all_matched_rules else ""
    matched_rule_type = all_matched_rules[0].get("rule_type","") if all_matched_rules else ""
    matched_universe  = all_matched_rules[0].get("universe","") if all_matched_rules else ""
    matched_status    = all_matched_rules[0].get("status","") if all_matched_rules else ""
    matched_med10d    = all_matched_rules[0].get("med10d_pct","") if all_matched_rules else ""
    matched_fail10d   = all_matched_rules[0].get("fail10d_pct","") if all_matched_rules else ""
    matched_avg10d    = all_matched_rules[0].get("avg10d_pct","") if all_matched_rules else ""
    matched_source    = all_matched_rules[0].get("source_file","") if all_matched_rules else ""
    matched_notes     = all_matched_rules[0].get("notes","") if all_matched_rules else ""

    # ── Score accumulation ────────────────────────────────────────────────────
    total_score = 0
    best_role   = base_role
    reason_codes: list[str] = []
    good_flags:   list[str] = []
    reject_flags: list[str] = []

    for r in eff_pos_comp:
        s = _score(r); role = r.get("role_seed","")
        total_score += s; best_role = _best_role(best_role, role)
        reason_codes.append(f"COMP+:{composite_pattern}:{role}:{s:+d}")
        good_flags.append(f"COMP:{composite_pattern}")

    for r in eff_neg_comp:
        s = _score(r); role = r.get("role_seed","")
        total_score += s; best_role = _best_role(best_role, role)
        reason_codes.append(f"COMP-:{composite_pattern}:{role}:{s:+d}")
        reject_flags.append(f"COMP:{composite_pattern}")

    for r in eff_pos_seq4:
        s = _score(r); role = r.get("role_seed","")
        total_score += s; best_role = _best_role(best_role, role)
        reason_codes.append(f"SEQ4+:{seq4_str}:{role}:{s:+d}")
        good_flags.append(f"SEQ4:{seq4_str}")

    for r in eff_neg_seq4:
        s = _score(r); role = r.get("role_seed","")
        total_score += s; best_role = _best_role(best_role, role)
        reason_codes.append(f"SEQ4-:{seq4_str}:{role}:{s:+d}")
        reject_flags.append(f"SEQ4:{seq4_str}")

    if has_conflict:
        reason_codes.append("CONFLICT:score_stripped")

    # ── EMA positions ─────────────────────────────────────────────────────────
    above_ema20 = close > ema20 if ema20 > 0 else False
    above_ema50 = close > ema50 if ema50 > 0 else False
    above_ema89 = close > ema89 if ema89 > 0 else False

    prev_close = float(history_rows[-1].get("close") or 0) if history_rows else 0.0
    prev_ema20 = float(history_rows[-1].get("ema20") or 0) if history_rows else 0.0
    prev_ema50 = float(history_rows[-1].get("ema50") or 0) if history_rows else 0.0
    prev_ema89 = float(history_rows[-1].get("ema89") or 0) if history_rows else 0.0

    ema20_reclaim = above_ema20 and prev_ema20 > 0 and prev_close < prev_ema20
    ema50_reclaim = above_ema50 and prev_ema50 > 0 and prev_close < prev_ema50
    ema89_reclaim = above_ema89 and prev_ema89 > 0 and prev_close < prev_ema89

    if ema50_reclaim:
        bonus = matrix.get_ema_bonus()
        total_score += bonus
        reason_codes.append(f"EMA50_RECLAIM:+{bonus}")
        good_flags.append("EMA50_RECLAIM")

    # ── Price position in 4-bar range ─────────────────────────────────────────
    highs = [float(b.get("high") or 0) for b in seq_bars if b.get("high")]
    lows  = [float(b.get("low")  or 0) for b in seq_bars if b.get("low")]
    price_position_4bar = 0.0
    breaks_4bar_high = False
    breaks_4bar_low  = False
    if highs and lows:
        range_high = max(highs); range_low = min(lows)
        rng = range_high - range_low
        if rng > 0:
            price_position_4bar = (close - range_low) / rng
            breaks_4bar_high = curr_h > range_high
            breaks_4bar_low  = curr_l < range_low
            if price_position_4bar >= 0.75:
                bonus = matrix.get_price_position_bonus()
                total_score += bonus
                reason_codes.append(f"CLOSE_TOP75PCT_4BAR:+{bonus}")
                good_flags.append("PRICE_POS_TOP75")

    # ── Volume vs prev bars ───────────────────────────────────────────────────
    def _prev_vol(idx: int) -> float:
        if len(history_rows) > idx:
            return float(history_rows[-(idx+1)].get("volume") or 0)
        return 0.0

    prev1_v = _prev_vol(0); prev2_v = _prev_vol(1); prev3_v = _prev_vol(2)
    vol_vs_prev1 = round(curr_v / prev1_v, 2) if prev1_v > 0 else None
    vol_vs_prev2 = round(curr_v / prev2_v, 2) if prev2_v > 0 else None
    vol_vs_prev3 = round(curr_v / prev3_v, 2) if prev3_v > 0 else None

    # ── SHORT_WATCH: positive pullback Z with no reject → demote ──────────────
    if best_role == "SHORT_WATCH" and z_sig in _PULLBACK_Z_SIGNALS:
        if not reject_flags and price_position_4bar >= 0.25 and above_ema50:
            best_role = "PULLBACK_READY_B"
            reason_codes.append(f"SHORT_WATCH_DEMOTE:{z_sig}_is_pullback_Z_no_reject")

    # ── SHORT_GO: breakdown confirmation ──────────────────────────────────────
    if best_role == "SHORT_WATCH" and current_low_4bar is not None:
        if close < current_low_4bar and not good_flags:
            bonus = matrix.get_short_go_bonus()
            total_score += bonus
            best_role = "SHORT_GO"
            reason_codes.append(f"BREAK_4BAR_LOW:SHORT_GO:+{bonus}")

    # ── BULL_A cap for weak T signals ─────────────────────────────────────────
    if best_role == "BULL_A" and final_signal in _WEAK_T_SIGNALS:
        all_confirmed = (
            bool(good_flags) and
            price_position_4bar >= 0.75 and
            (above_ema20 and above_ema50 or ema50_reclaim) and
            not reject_flags and not has_conflict
        )
        if not all_confirmed:
            best_role = "BULL_B"
            reason_codes.append(f"BULL_A_CAPPED:{final_signal}_weak_signal_missing_confirm")

    # ── General BULL_A gate ───────────────────────────────────────────────────
    if best_role == "BULL_A":
        has_ema_support = above_ema20 or above_ema50 or ema20_reclaim or ema50_reclaim
        if price_position_4bar < 0.25 and not has_ema_support:
            best_role = "BULL_WATCH"
            reason_codes.append("BULL_A→BULL_WATCH:deep_range+no_ema_support")
        elif price_position_4bar < 0.50 or not has_ema_support:
            best_role = "BULL_B"
            reason_codes.append("BULL_A→BULL_B:insufficient_price_or_ema_confirmation")

    # ── PULLBACK_READY_A gating ───────────────────────────────────────────────
    if best_role == "PULLBACK_READY_A":
        has_ema_support = above_ema20 or above_ema50 or ema20_reclaim or ema50_reclaim
        if not has_ema_support and price_position_4bar < 0.25:
            best_role = "DEEP_PULLBACK_WATCH"
            reason_codes.append("PULLBACK_READY_A→DEEP_PULLBACK_WATCH:no_ema+deep_range")
        elif price_position_4bar < 0.50 or not has_ema_support:
            best_role = "PULLBACK_READY_B"
            reason_codes.append("PULLBACK_READY_A→PULLBACK_READY_B:insufficient_confirmation")

    # ── PULLBACK_READY_B gate ─────────────────────────────────────────────────
    if best_role == "PULLBACK_READY_B":
        has_ema_support = above_ema20 or above_ema50 or ema20_reclaim or ema50_reclaim
        if price_position_4bar < 0.25 and not has_ema_support:
            best_role = "DEEP_PULLBACK_WATCH"
            reason_codes.append("PULLBACK_READY_B→DEEP_PULLBACK_WATCH:deep_range+no_ema")

    # ── Score penalties for weak pullback context ─────────────────────────────
    if best_role in ("PULLBACK_READY_A", "PULLBACK_READY_B",
                     "PULLBACK_WATCH", "DEEP_PULLBACK_WATCH"):
        penalty = 0
        if price_position_4bar < 0.25:
            penalty += 25; reason_codes.append("PENALTY:price_deep_bottom:-25")
        if not above_ema20 and not above_ema50 and not above_ema89:
            penalty += 30; reason_codes.append("PENALTY:below_all_emas:-30")
        elif not above_ema20 and not above_ema50:
            penalty += 20; reason_codes.append("PENALTY:below_ema20_ema50:-20")
        if penalty:
            total_score -= penalty

    # ── Z pullback READY score cap ────────────────────────────────────────────
    if z_sig and best_role in ("PULLBACK_READY_A", "PULLBACK_READY_B"):
        if total_score > 75:
            total_score = 75
            reason_codes.append("SCORE_CAP:Z_pullback_ready_max_75")

    # ── PULLBACK_GO proof: scan history for prior pullback Z bar ──────────────
    prior_pb_found          = False
    prior_pb_bars_ago: Optional[int] = None
    prior_pb_signal         = ""
    prior_pb_composite      = ""
    prior_pb_role           = ""
    pullback_bar_high: Optional[float] = None

    for i, b in enumerate(reversed(history_rows[-3:])):
        if b.get("z_signal") in _PULLBACK_Z_SIGNALS:
            prior_pb_found    = True
            prior_pb_bars_ago = i + 1
            prior_pb_signal   = b.get("z_signal", "")
            lane_b            = b.get("lane1_label") or b.get("lane3_label") or ""
            prior_pb_composite = _make_composite(prior_pb_signal, lane_b)
            pb_close = float(b.get("close") or 0)
            pb_ema20 = float(b.get("ema20") or 0)
            pb_ema50 = float(b.get("ema50") or 0)
            pb_above = (
                (pb_close > pb_ema20 if pb_ema20 > 0 else False) or
                (pb_close > pb_ema50 if pb_ema50 > 0 else False)
            )
            prior_pb_role     = "PULLBACK_READY_A" if pb_above else "PULLBACK_READY_B"
            h_val = float(b.get("high") or 0)
            pullback_bar_high = h_val if h_val > 0 else None
            break

    current_close_above_pb_high = (close > pullback_bar_high) if pullback_bar_high else False

    # ── PULLBACK_GO — T confirmation after recent Z pullback ──────────────────
    if t_sig and not z_sig and t_sig not in _WEAK_T_SIGNALS:
        top_range  = price_position_4bar >= 0.75 or breaks_4bar_high
        no_reject  = not bool(reject_flags) and not has_conflict
        if (prior_pb_found and top_range and no_reject and
                best_role in ("BULL_A", "BULL_B", "PULLBACK_READY_A", "PULLBACK_READY_B")):
            go_bonus = 15
            total_score += go_bonus
            best_role = "PULLBACK_GO"
            reason_codes.append(
                f"PULLBACK_GO:T_after_{prior_pb_signal}_{prior_pb_bars_ago}bars_ago"
                f"+top_range:+{go_bonus}"
            )

    # ── SHORT_WATCH strictness ────────────────────────────────────────────────
    if best_role == "SHORT_WATCH":
        bearish_confirmed = (
            breaks_4bar_low or
            (not above_ema50) or
            price_position_4bar < 0.25 or
            (bool(eff_neg_comp) and bool(eff_neg_seq4))
        )
        moderate_bullish = above_ema50 or price_position_4bar >= 0.50
        strong_bullish   = price_position_4bar >= 0.75 and above_ema50

        if bool(eff_pos_comp) and bool(eff_neg_seq4) and moderate_bullish:
            best_role = "MIXED_WATCH"
            reason_codes.append("SHORT_WATCH→MIXED_WATCH:pos_comp+neg_seq4+bullish_context")
        elif bool(eff_neg_comp) and not bool(eff_neg_seq4) and strong_bullish:
            best_role = "REJECT_LONG"
            reason_codes.append("SHORT_WATCH→REJECT_LONG:reject_comp_only+bullish_context")
        elif not bearish_confirmed:
            if strong_bullish and bool(eff_neg_comp):
                best_role = "REJECT_LONG"
                reason_codes.append("SHORT_WATCH→REJECT_LONG:no_bearish_confirmation+strong_bullish")
            else:
                best_role = "MIXED_WATCH"
                reason_codes.append("SHORT_WATCH→MIXED_WATCH:no_bearish_confirmation")

    # ── Volume / wick / EMA summary ───────────────────────────────────────────
    if vol_bkt in ("VB", "B"):
        reason_codes.append(f"VOL:{vol_bkt}")
    if wk_sfx:
        reason_codes.append(f"WICK:{wk_sfx}")
    if ne_sfx:
        reason_codes.append(f"NE:{ne_sfx}")

    ema_parts = []
    if above_ema20: ema_parts.append("20✓")
    if above_ema50: ema_parts.append("50✓")
    if above_ema89: ema_parts.append("89✓")
    if ema_parts:
        reason_codes.append(f"EMA_ABOVE:{','.join(ema_parts)}")

    # ── Conflict role override ────────────────────────────────────────────────
    if has_conflict:
        if best_role in ("BULL_A", "BULL_B"):
            best_role = "BULL_WATCH"
            reason_codes.append("CONFLICT_OVERRIDE:role→BULL_WATCH")
        elif best_role in ("PULLBACK_READY_A", "PULLBACK_READY_B", "PULLBACK_GO"):
            best_role = "MIXED_WATCH"
            reason_codes.append("CONFLICT_OVERRIDE:role→MIXED_WATCH")
        elif best_role == "SHORT_WATCH":
            best_role = "MIXED_WATCH"
            reason_codes.append("CONFLICT_OVERRIDE:role→MIXED_WATCH")

    # ── Hard cap: below all EMAs + deep range → DEEP_PULLBACK_WATCH ──────────
    below_all_emas = not above_ema20 and not above_ema50 and not above_ema89
    if (below_all_emas and price_position_4bar < 0.25 and
            best_role not in ("REJECT", "REJECT_LONG", "NO_EDGE",
                              "SHORT_WATCH", "SHORT_GO", "DEEP_PULLBACK_WATCH")):
        best_role = "DEEP_PULLBACK_WATCH"
        reason_codes.append("HARD_CAP:below_all_emas+deep_range→DEEP_PULLBACK_WATCH")
        if debug:
            debug_trace.append(
                f"HARD_CAP → DEEP_PULLBACK_WATCH: below all EMAs, "
                f"price_pos={price_position_4bar:.2f}"
            )

    # ── Final normalization: enforce role/score/quality consistency ───────────
    best_role, total_score = _normalize_role_score(best_role, total_score, reason_codes)

    if debug:
        debug_trace.append(f"FINAL: role={best_role} score={total_score} "
                           f"conflict={has_conflict} below_all_emas={below_all_emas} "
                           f"good_flags={good_flags} reject_flags={reject_flags}")

    # ── Explanation ───────────────────────────────────────────────────────────
    expl_parts = []
    if composite_pattern and (pos_comp_rules or neg_comp_rules):
        status = "conflict" if comp_conflict_resolution == "CONFLICT" else ("pos" if eff_pos_comp else "neg")
        expl_parts.append(f"Composite {composite_pattern} ({status})")
    if seq4_str and (pos_seq4_rules or neg_seq4_rules):
        status = "conflict" if seq4_conflict_resolution == "CONFLICT" else ("pos" if eff_pos_seq4 else "neg")
        expl_parts.append(f"Seq4 [{seq4_str}] ({status})")
    if ema50_reclaim:
        expl_parts.append("EMA50 reclaim")
    if price_position_4bar >= 0.75:
        expl_parts.append(f"Close top {price_position_4bar:.0%} of 4-bar range")
    if not expl_parts:
        expl_parts.append(f"Baseline {final_signal}: {baseline.get('action','') if baseline else 'no rule'}")
    explanation = "; ".join(expl_parts)

    return _build_result(
        ticker=ticker, date=date,
        final_signal=final_signal, composite_pattern=composite_pattern,
        role=best_role, score=total_score,
        reason_codes=reason_codes, explanation=explanation,
        seq4_str=seq4_str, lane1=lane1, lane3=lane3,
        vol_bkt=vol_bkt, wk_sfx=wk_sfx,
        above_ema20=above_ema20, above_ema50=above_ema50, above_ema89=above_ema89,
        ema20_reclaim=ema20_reclaim, ema50_reclaim=ema50_reclaim, ema89_reclaim=ema89_reclaim,
        good_flags=good_flags, reject_flags=reject_flags,
        conflict_flag=has_conflict,
        conflict_resolution=comp_conflict_resolution or seq4_conflict_resolution,
        conflicting_rule_ids=comp_conflict_ids or seq4_conflict_ids,
        price_position_4bar=round(price_position_4bar, 4),
        breaks_4bar_high=breaks_4bar_high, breaks_4bar_low=breaks_4bar_low,
        vol_vs_prev1=vol_vs_prev1, vol_vs_prev2=vol_vs_prev2, vol_vs_prev3=vol_vs_prev3,
        matched_rule_id=matched_rule_id, matched_rule_type=matched_rule_type,
        matched_universe=matched_universe, matched_status=matched_status,
        matched_med10d=matched_med10d, matched_fail10d=matched_fail10d,
        matched_avg10d=matched_avg10d, matched_source=matched_source,
        matched_notes=matched_notes,
        matched_composite_rule_id=matched_composite_rule_id,
        matched_seq4_rule_id=matched_seq4_rule_id,
        matched_reject_rule_id=matched_reject_rule_id,
        prior_pullback_ready_found=prior_pb_found,
        prior_pullback_ready_bars_ago=prior_pb_bars_ago,
        prior_pullback_ready_signal=prior_pb_signal,
        prior_pullback_ready_composite=prior_pb_composite,
        prior_pullback_ready_role=prior_pb_role,
        pullback_high=pullback_bar_high,
        current_close_above_pullback_high=current_close_above_pb_high,
        debug_trace=debug_trace if debug else None,
    )


def _build_result(
    ticker, date, final_signal, composite_pattern,
    role, score, reason_codes, explanation,
    seq4_str="", lane1="", lane3="",
    vol_bkt="", wk_sfx="",
    above_ema20=False, above_ema50=False, above_ema89=False,
    ema20_reclaim=False, ema50_reclaim=False, ema89_reclaim=False,
    good_flags=None, reject_flags=None,
    conflict_flag=False, conflict_resolution="", conflicting_rule_ids=None,
    price_position_4bar=0.0,
    breaks_4bar_high=False, breaks_4bar_low=False,
    vol_vs_prev1=None, vol_vs_prev2=None, vol_vs_prev3=None,
    matched_rule_id="", matched_rule_type="", matched_universe="",
    matched_status="", matched_med10d="", matched_fail10d="",
    matched_avg10d="", matched_source="", matched_notes="",
    matched_composite_rule_id="", matched_seq4_rule_id="", matched_reject_rule_id="",
    prior_pullback_ready_found=False, prior_pullback_ready_bars_ago=None,
    prior_pullback_ready_signal="", prior_pullback_ready_composite="",
    prior_pullback_ready_role="", pullback_high=None,
    current_close_above_pullback_high=False,
    debug_trace=None,
) -> dict:
    below_all_emas = not above_ema20 and not above_ema50 and not above_ema89
    quality = _quality_from_score(role, score, below_all_emas, price_position_4bar, conflict_flag)
    action  = _ROLE_ACTION.get(role, "NO_ACTION")
    return {
        "ticker":            ticker,
        "date":              date,
        "final_signal":      final_signal,
        "composite_pattern": composite_pattern,
        "seq4":              seq4_str,
        "lane1":             lane1,
        "lane3":             lane3,
        "role":              role,
        "score":             score,
        "quality":           quality,
        "action":            action,
        "vol_bucket":        vol_bkt,
        "wick_suffix":       wk_sfx,
        "explanation":       explanation,
        "reason_codes":      reason_codes or [],
        "above_ema20":       above_ema20,
        "above_ema50":       above_ema50,
        "above_ema89":       above_ema89,
        "ema20_reclaim":     ema20_reclaim,
        "ema50_reclaim":     ema50_reclaim,
        "ema89_reclaim":     ema89_reclaim,
        "conflict_flag":          conflict_flag,
        "conflict_resolution":    conflict_resolution,
        "conflicting_rule_ids":   conflicting_rule_ids or [],
        "good_flags":        good_flags  or [],
        "reject_flags":      reject_flags or [],
        "price_position_4bar": price_position_4bar,
        "breaks_4bar_high":  breaks_4bar_high,
        "breaks_4bar_low":   breaks_4bar_low,
        "final_volume_vs_prev1": vol_vs_prev1,
        "final_volume_vs_prev2": vol_vs_prev2,
        "final_volume_vs_prev3": vol_vs_prev3,
        "matched_rule_id":           matched_rule_id,
        "matched_rule_type":         matched_rule_type,
        "matched_universe":          matched_universe,
        "matched_status":            matched_status,
        "matched_med10d_pct":        matched_med10d,
        "matched_fail10d_pct":       matched_fail10d,
        "matched_avg10d_pct":        matched_avg10d,
        "matched_source_file":       matched_source,
        "matched_rule_notes":        matched_notes,
        "matched_composite_rule_id": matched_composite_rule_id,
        "matched_seq4_rule_id":      matched_seq4_rule_id,
        "matched_reject_rule_id":    matched_reject_rule_id,
        # PULLBACK_GO proof fields
        "prior_pullback_ready_found":        prior_pullback_ready_found,
        "prior_pullback_ready_bars_ago":     prior_pullback_ready_bars_ago,
        "prior_pullback_ready_signal":       prior_pullback_ready_signal,
        "prior_pullback_ready_composite":    prior_pullback_ready_composite,
        "prior_pullback_ready_role":         prior_pullback_ready_role,
        "pullback_high":                     pullback_high,
        "current_close_above_pullback_high": current_close_above_pullback_high,
        "debug_trace":      debug_trace,
    }
