"""TZ Signal Intelligence classifier — hardened v2.

Fixes vs v1:
  1. Composite pattern deduplication (_make_composite)
  2. Universe-scoped matrix matching
  3. Matrix conflict resolver (97 known conflicts)
  4. BULL_A promotion cap for weak T signals (T1/T2/T9/T10)
  5. Separate above_ema vs ema_reclaim fields
  6. Full debug fields in every output row
  7. Stricter SHORT_WATCH / SHORT_GO conditions
  8. Backend debug mode (verbatim decision path)
"""
from __future__ import annotations
from typing import Optional
from .matrix_loader import MatrixIndex

# ── Role constants ────────────────────────────────────────────────────────────

_ROLE_RANK = {
    "NO_EDGE":         0,
    "CONTEXT_BONUS":   0,
    "REJECT":          1,
    "BULL_WATCH":      2,
    "PULLBACK_WATCH":  2,
    "PULLBACK_READY_B":3,
    "PULLBACK_READY_A":4,
    "BULL_B":          5,
    "BULL_A":          6,
    "SHORT_WATCH":     7,
    "SHORT_GO":        8,
}

_ROLE_QUALITY = {
    "BULL_A":           "A",
    "BULL_B":           "B",
    "PULLBACK_READY_A": "A",
    "PULLBACK_READY_B": "B",
    "PULLBACK_WATCH":   "Watch",
    "BULL_WATCH":       "Watch",
    "SHORT_GO":         "A",
    "SHORT_WATCH":      "Watch",
    "REJECT":           "Reject",
    "NO_EDGE":          "—",
}

_ROLE_ACTION = {
    "BULL_A":           "BUY_TRIGGER",
    "BULL_B":           "WAIT_FOR_T_CONFIRMATION",
    "PULLBACK_READY_A": "PULLBACK_ENTRY_READY",
    "PULLBACK_READY_B": "WATCH_PULLBACK",
    "PULLBACK_WATCH":   "WATCH_PULLBACK",
    "BULL_WATCH":       "WAIT_FOR_CONFIRMATION",
    "SHORT_WATCH":      "WAIT_FOR_BREAKDOWN",
    "SHORT_GO":         "SHORT_TRIGGER",
    "REJECT":           "IGNORE",
    "NO_EDGE":          "IGNORE",
}

# Signals that may NOT become BULL_A unless ALL confirmations present
_WEAK_T_SIGNALS = {"T1", "T2", "T9", "T10"}

# Z signals that carry positive pullback edge (not shorts)
_PULLBACK_Z_SIGNALS = {"Z5", "Z9", "Z3", "Z4", "Z6"}


def _best_role(a: str, b: str) -> str:
    return a if _ROLE_RANK.get(a, 0) >= _ROLE_RANK.get(b, 0) else b


def _score(r: dict) -> int:
    try:
        return int(r.get("score_base") or 0)
    except (ValueError, TypeError):
        return 0


# ── Fix 1: Composite deduplication ───────────────────────────────────────────

def _make_composite(signal: str, lane: str) -> str:
    """Build composite pattern without duplicating the signal prefix.

    lane1_label from stock_stat can already include the signal:
        T5L25ND  (already has T5)
    or be the lane-only part:
        L25ND    (needs T5 prepended)

    Never produce T5T5L25ND.
    """
    if not signal or not lane:
        return ""
    # If lane already starts with signal → use as-is
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
    """Classify a single ticker/date row.

    Args:
        row:               latest bar dict (t_signal, z_signal, l_signal,
                           lane1_label, lane3_label, close, ema20/50/89,
                           high, low, volume, …)
        history_rows:      previous bars oldest-first (up to 3)
        matrix:            loaded MatrixIndex
        current_low_4bar:  min(low) over 4 bars (for SHORT_GO check)
        current_high_4bar: max(high) over 4 bars (for breaks_4bar_high)
        scan_universe:     e.g. 'sp500' | 'nasdaq' — controls which rules apply
        debug:             include verbatim decision trace
    """
    ticker = row.get("ticker", "")
    date   = row.get("date", "")
    debug_trace: list[str] = []

    # ── Raw signal fields ─────────────────────────────────────────────────────
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

    # ── No signal → early exit ────────────────────────────────────────────────
    if not final_signal:
        return _build_result(ticker, date, "", "", "NO_EDGE", 0, [],
                             "No T/Z/L signal", debug_trace=debug_trace if debug else None)

    # ── Baseline ──────────────────────────────────────────────────────────────
    baseline = matrix.baseline.get(final_signal)
    base_role = baseline.get("role_seed", "NO_EDGE") if baseline else "NO_EDGE"
    if debug:
        debug_trace.append(f"BASELINE: signal={final_signal} base_role={base_role}")

    # ── Fix 1: Composite construction (no duplication) ────────────────────────
    lane_source = lane1 or lane3
    composite_pattern = _make_composite(final_signal, lane_source)
    if debug:
        debug_trace.append(f"COMPOSITE: lane_source={lane_source!r} "
                           f"composite={composite_pattern!r}")

    # ── Fix 2: Universe-scoped rule lookup ────────────────────────────────────
    pos_comp_rules, neg_comp_rules = [], []
    if composite_pattern:
        pos_comp_rules, neg_comp_rules = matrix.get_composite_rules(
            composite_pattern, scan_universe)
    if debug:
        debug_trace.append(f"COMP_RULES: pos={len(pos_comp_rules)} neg={len(neg_comp_rules)}")

    # ── SEQ4 ──────────────────────────────────────────────────────────────────
    def _bar_sig(b: dict) -> str:
        return b.get("t_signal") or b.get("z_signal") or b.get("l_signal") or "—"

    seq_bars = list(history_rows[-3:]) + [row]
    seq4_str = "|".join(_bar_sig(b) for b in seq_bars) if len(seq_bars) == 4 else ""

    pos_seq4_rules, neg_seq4_rules = [], []
    if seq4_str:
        pos_seq4_rules, neg_seq4_rules = matrix.get_seq4_rules(seq4_str, scan_universe)
    if debug:
        debug_trace.append(f"SEQ4: seq4={seq4_str!r} "
                           f"pos={len(pos_seq4_rules)} neg={len(neg_seq4_rules)}")

    # ── Fix 3: Conflict resolution ────────────────────────────────────────────
    comp_conflict  = None
    comp_conflict_resolution = ""
    comp_conflict_ids: list[str] = []

    seq4_conflict  = None
    seq4_conflict_resolution = ""
    seq4_conflict_ids: list[str] = []

    if composite_pattern and pos_comp_rules and neg_comp_rules:
        comp_conflict = matrix.get_conflict(composite_pattern, "COMP", scan_universe)
        comp_conflict_resolution = comp_conflict or "CONFLICT"
        comp_conflict_ids = (
            [r.get("rule_id","") for r in pos_comp_rules] +
            [r.get("rule_id","") for r in neg_comp_rules]
        )
        if debug:
            debug_trace.append(f"COMP_CONFLICT: resolution={comp_conflict_resolution}")

    if seq4_str and pos_seq4_rules and neg_seq4_rules:
        seq4_conflict = matrix.get_conflict(seq4_str, "SEQ4", scan_universe)
        seq4_conflict_resolution = seq4_conflict or "CONFLICT"
        seq4_conflict_ids = (
            [r.get("rule_id","") for r in pos_seq4_rules] +
            [r.get("rule_id","") for r in neg_seq4_rules]
        )
        if debug:
            debug_trace.append(f"SEQ4_CONFLICT: resolution={seq4_conflict_resolution}")

    has_conflict = bool(comp_conflict_resolution == "CONFLICT" or
                        seq4_conflict_resolution == "CONFLICT")

    # ── Decide which rules to apply after conflict resolution ─────────────────
    def _effective_rules(pos, neg, conflict_res):
        if not pos or not neg:
            return pos, neg
        if conflict_res == "POSITIVE":
            return pos, []
        if conflict_res == "REJECT":
            return [], neg
        # CONFLICT → don't apply either strongly; treat as watch only
        return [], []

    eff_pos_comp, eff_neg_comp = _effective_rules(
        pos_comp_rules, neg_comp_rules, comp_conflict_resolution)
    eff_pos_seq4, eff_neg_seq4 = _effective_rules(
        pos_seq4_rules, neg_seq4_rules, seq4_conflict_resolution)

    # For conflict rows that we stripped out, still count their IDs
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
        s    = _score(r)
        role = r.get("role_seed","")
        total_score += s
        best_role    = _best_role(best_role, role)
        reason_codes.append(f"COMP+:{composite_pattern}:{role}:{s:+d}")
        good_flags.append(f"COMP:{composite_pattern}")

    for r in eff_neg_comp:
        s    = _score(r)
        role = r.get("role_seed","")
        total_score += s
        best_role    = _best_role(best_role, role)
        reason_codes.append(f"COMP-:{composite_pattern}:{role}:{s:+d}")
        reject_flags.append(f"COMP:{composite_pattern}")

    for r in eff_pos_seq4:
        s    = _score(r)
        role = r.get("role_seed","")
        total_score += s
        best_role    = _best_role(best_role, role)
        reason_codes.append(f"SEQ4+:{seq4_str}:{role}:{s:+d}")
        good_flags.append(f"SEQ4:{seq4_str}")

    for r in eff_neg_seq4:
        s    = _score(r)
        role = r.get("role_seed","")
        total_score += s
        best_role    = _best_role(best_role, role)
        reason_codes.append(f"SEQ4-:{seq4_str}:{role}:{s:+d}")
        reject_flags.append(f"SEQ4:{seq4_str}")

    if has_conflict:
        reason_codes.append("CONFLICT:score_stripped")
        if debug:
            debug_trace.append(f"CONFLICT: both pos+neg rules stripped from score")

    # ── Fix 6: Separate above_ema vs ema_reclaim ─────────────────────────────
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
    if debug:
        debug_trace.append(f"EMA: above20={above_ema20} above50={above_ema50} above89={above_ema89} "
                           f"reclaim20={ema20_reclaim} reclaim50={ema50_reclaim} reclaim89={ema89_reclaim}")

    # ── Price position in 4-bar range ─────────────────────────────────────────
    highs = [float(b.get("high") or 0) for b in seq_bars if b.get("high")]
    lows  = [float(b.get("low")  or 0) for b in seq_bars if b.get("low")]
    price_position_4bar = 0.0
    breaks_4bar_high = False
    breaks_4bar_low  = False
    if highs and lows:
        range_high = max(highs)
        range_low  = min(lows)
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
    if debug:
        debug_trace.append(f"PRICE_POS: {price_position_4bar:.2f} "
                           f"breaks_high={breaks_4bar_high} breaks_low={breaks_4bar_low}")

    # ── Volume vs prev bars ───────────────────────────────────────────────────
    def _prev_vol(idx: int) -> float:
        if len(history_rows) > idx:
            return float(history_rows[-(idx+1)].get("volume") or 0)
        return 0.0

    prev1_v = _prev_vol(0)
    prev2_v = _prev_vol(1)
    prev3_v = _prev_vol(2)
    vol_vs_prev1 = round(curr_v / prev1_v, 2) if prev1_v > 0 else None
    vol_vs_prev2 = round(curr_v / prev2_v, 2) if prev2_v > 0 else None
    vol_vs_prev3 = round(curr_v / prev3_v, 2) if prev3_v > 0 else None

    # ── Fix 8: Stricter SHORT_WATCH ───────────────────────────────────────────
    # Positive pullback Z signals with no reject evidence → demote SHORT_WATCH
    if best_role == "SHORT_WATCH" and z_sig in _PULLBACK_Z_SIGNALS:
        has_reject_evidence = bool(reject_flags)
        low_price_pos = price_position_4bar < 0.25 if price_position_4bar else False
        below_ema50   = not above_ema50
        if not has_reject_evidence and not low_price_pos and not below_ema50:
            best_role = "PULLBACK_READY_B"
            reason_codes.append(f"SHORT_WATCH_DEMOTE:{z_sig}_is_pullback_Z_no_reject_evidence")
            if debug:
                debug_trace.append(f"SHORT_WATCH demoted to PULLBACK_READY_B: "
                                   f"{z_sig} is pullback Z with no reject evidence")

    # ── Fix 7: SHORT_GO requires breakdown confirmation ───────────────────────
    if best_role == "SHORT_WATCH" and current_low_4bar is not None:
        if close < current_low_4bar and not good_flags:
            bonus = matrix.get_short_go_bonus()
            total_score += bonus
            best_role = "SHORT_GO"
            reason_codes.append(f"BREAK_4BAR_LOW:SHORT_GO:+{bonus}")
            if debug:
                debug_trace.append(f"SHORT_GO promoted: close={close} < 4bar_low={current_low_4bar}")

    # ── Fix 4: BULL_A cap for weak T signals ─────────────────────────────────
    if best_role == "BULL_A" and final_signal in _WEAK_T_SIGNALS:
        # Need ALL confirmations to allow BULL_A
        all_confirmed = (
            bool(good_flags) and                       # composite or seq4 matched
            price_position_4bar >= 0.75 and            # close in top 75% of 4-bar
            (above_ema20 and above_ema50 or ema50_reclaim) and   # EMA above/reclaim
            not reject_flags and                        # no reject rules
            not has_conflict                            # no conflict
        )
        if not all_confirmed:
            best_role = "BULL_B"
            reason_codes.append(f"BULL_A_CAPPED:{final_signal}_weak_signal_missing_confirm")
            if debug:
                debug_trace.append(
                    f"BULL_A → BULL_B: {final_signal} is weak T, missing confirmations "
                    f"(good_flags={bool(good_flags)}, price_pos={price_position_4bar:.2f}, "
                    f"ema={above_ema20 and above_ema50}, reject={bool(reject_flags)}, "
                    f"conflict={has_conflict})"
                )

    # ── Volume / wick context ─────────────────────────────────────────────────
    if vol_bkt in ("VB", "B"):
        reason_codes.append(f"VOL:{vol_bkt}")
    if wk_sfx:
        reason_codes.append(f"WICK:{wk_sfx}")
    if ne_sfx:
        reason_codes.append(f"NE:{ne_sfx}")

    # ── EMA alignment summary ─────────────────────────────────────────────────
    ema_parts = []
    if above_ema20: ema_parts.append("20✓")
    if above_ema50: ema_parts.append("50✓")
    if above_ema89: ema_parts.append("89✓")
    if ema_parts:
        reason_codes.append(f"EMA_ABOVE:{','.join(ema_parts)}")

    # ── Conflict role override ────────────────────────────────────────────────
    if has_conflict and best_role in ("BULL_A", "BULL_B", "SHORT_WATCH"):
        best_role = "BULL_WATCH"
        reason_codes.append("CONFLICT_OVERRIDE:role→BULL_WATCH")

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

    if debug:
        debug_trace.append(f"FINAL: role={best_role} score={total_score} "
                           f"conflict={has_conflict} good_flags={good_flags} "
                           f"reject_flags={reject_flags}")

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
    debug_trace=None,
) -> dict:
    quality = _ROLE_QUALITY.get(role, "—")
    action  = _ROLE_ACTION.get(role, "IGNORE")
    return {
        # Core
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
        # Fix 6: EMA separate fields
        "above_ema20":       above_ema20,
        "above_ema50":       above_ema50,
        "above_ema89":       above_ema89,
        "ema20_reclaim":     ema20_reclaim,
        "ema50_reclaim":     ema50_reclaim,
        "ema89_reclaim":     ema89_reclaim,
        # Fix 3: conflict fields
        "conflict_flag":          conflict_flag,
        "conflict_resolution":    conflict_resolution,
        "conflicting_rule_ids":   conflicting_rule_ids or [],
        # Fix 4: classification flags
        "good_flags":        good_flags  or [],
        "reject_flags":      reject_flags or [],
        # Price position
        "price_position_4bar": price_position_4bar,
        "breaks_4bar_high":  breaks_4bar_high,
        "breaks_4bar_low":   breaks_4bar_low,
        # Volume vs history
        "final_volume_vs_prev1": vol_vs_prev1,
        "final_volume_vs_prev2": vol_vs_prev2,
        "final_volume_vs_prev3": vol_vs_prev3,
        # Debug fields
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
        # Debug trace
        "debug_trace":      debug_trace,
    }
