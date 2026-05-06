"""TZ Signal Intelligence classifier.

Classifies each ticker/day into:
  BULL_A, BULL_B, PULLBACK_READY_A, PULLBACK_READY_B, PULLBACK_WATCH,
  BULL_WATCH, SHORT_WATCH, SHORT_GO, REJECT, NO_EDGE

Uses the master matrix seed + contextual signals from the existing
TZ/WLNBB analysis rows.
"""
from __future__ import annotations
from typing import Optional
from .matrix_loader import MatrixIndex

# Role priority order for "best role" resolution (highest index wins)
_ROLE_RANK = {
    "NO_EDGE": 0,
    "CONTEXT_BONUS": 0,
    "REJECT": 1,
    "BULL_WATCH": 2,
    "PULLBACK_WATCH": 2,
    "PULLBACK_READY_B": 3,
    "PULLBACK_READY_A": 4,
    "BULL_B": 5,
    "BULL_A": 6,
    "SHORT_WATCH": 7,
    "SHORT_GO": 8,
}

_ROLE_QUALITY = {
    "BULL_A": "A",
    "BULL_B": "B",
    "PULLBACK_READY_A": "A",
    "PULLBACK_READY_B": "B",
    "PULLBACK_WATCH": "Watch",
    "BULL_WATCH": "Watch",
    "SHORT_GO": "A",
    "SHORT_WATCH": "Watch",
    "REJECT": "Reject",
    "NO_EDGE": "—",
}

_ROLE_ACTION = {
    "BULL_A":          "BUY_TRIGGER",
    "BULL_B":          "WAIT_FOR_T_CONFIRMATION",
    "PULLBACK_READY_A":"PULLBACK_ENTRY_READY",
    "PULLBACK_READY_B":"WATCH_PULLBACK",
    "PULLBACK_WATCH":  "WATCH_PULLBACK",
    "BULL_WATCH":      "WAIT_FOR_CONFIRMATION",
    "SHORT_WATCH":     "WAIT_FOR_BREAKDOWN",
    "SHORT_GO":        "SHORT_TRIGGER",
    "REJECT":          "IGNORE",
    "NO_EDGE":         "IGNORE",
}


def _best_role(a: str, b: str) -> str:
    return a if _ROLE_RANK.get(a, 0) >= _ROLE_RANK.get(b, 0) else b


def _score(r: dict) -> int:
    try:
        return int(r.get("score_base") or 0)
    except (ValueError, TypeError):
        return 0


def classify_tz_event(
    row: dict,
    history_rows: list,   # last 3 bars before current (oldest first)
    matrix: MatrixIndex,
    current_low_4bar: Optional[float] = None,  # min(low) over last 4 bars
) -> dict:
    """
    Classify a single ticker/date row.

    row keys used:
        t_signal, z_signal, l_signal,
        lane1_label, lane3_label,
        volume_bucket, ne_suffix, wick_suffix,
        close, ema20, ema50, ema89,
        high, low, open

    history_rows: list of previous bar dicts (same keys), oldest first.
    """
    ticker = row.get("ticker", "")
    date   = row.get("date", "")

    t_sig  = row.get("t_signal") or ""
    z_sig  = row.get("z_signal") or ""
    l_sig  = row.get("l_signal") or ""

    lane1  = row.get("lane1_label") or ""
    lane3  = row.get("lane3_label") or ""

    vol_bkt = row.get("volume_bucket") or ""
    ne_sfx  = row.get("ne_suffix") or ""
    wk_sfx  = row.get("wick_suffix") or ""

    try:
        close  = float(row.get("close") or 0)
        ema20  = float(row.get("ema20") or 0)
        ema50  = float(row.get("ema50") or 0)
        ema89  = float(row.get("ema89") or 0)
        curr_h = float(row.get("high") or 0)
        curr_l = float(row.get("low") or 0)
    except (TypeError, ValueError):
        close = ema20 = ema50 = ema89 = curr_h = curr_l = 0.0

    final_signal = t_sig or z_sig or l_sig or ""

    # ── 1. Baseline check ─────────────────────────────────────────────────────
    if not final_signal:
        return _build_result(ticker, date, final_signal, "", "NO_EDGE", 0, [], "No T/Z/L signal present")

    baseline = matrix.baseline.get(final_signal)
    if baseline:
        base_role = baseline.get("role_seed", "NO_EDGE")
    else:
        base_role = "NO_EDGE"

    # ── 2. Composite matching ─────────────────────────────────────────────────
    composite_pattern = ""
    if t_sig and lane1:
        composite_pattern = f"{t_sig}{lane1}"
    elif t_sig and lane3:
        composite_pattern = f"{t_sig}{lane3}"
    elif z_sig and lane1:
        composite_pattern = f"{z_sig}{lane1}"
    elif z_sig and lane3:
        composite_pattern = f"{z_sig}{lane3}"

    matched_composite_rules = []
    if composite_pattern:
        matched_composite_rules += matrix.composite.get(composite_pattern, [])
        matched_composite_rules += matrix.reject_composite.get(composite_pattern, [])

    # ── 3. SEQ4 matching ──────────────────────────────────────────────────────
    def _bar_signal(r: dict) -> str:
        return r.get("t_signal") or r.get("z_signal") or r.get("l_signal") or "—"

    seq_bars = list(history_rows[-3:]) + [row]  # up to 4 bars
    seq4_str = "|".join(_bar_signal(b) for b in seq_bars) if len(seq_bars) == 4 else ""

    matched_seq4_rules = []
    if seq4_str:
        matched_seq4_rules += matrix.seq4.get(seq4_str, [])
        matched_seq4_rules += matrix.reject_seq4.get(seq4_str, [])

    # ── 4. Score accumulation ─────────────────────────────────────────────────
    total_score = 0
    best_role   = base_role
    reason_codes: list[str] = []

    for r in matched_composite_rules:
        s = _score(r)
        total_score += s
        role = r.get("role_seed", "")
        best_role = _best_role(best_role, role)
        reason_codes.append(f"COMPOSITE:{composite_pattern}:{role}:{s:+d}")

    for r in matched_seq4_rules:
        s = _score(r)
        total_score += s
        role = r.get("role_seed", "")
        best_role = _best_role(best_role, role)
        reason_codes.append(f"SEQ4:{seq4_str}:{role}:{s:+d}")

    # ── 5. EMA context ────────────────────────────────────────────────────────
    ema_reclaim = (ema50 > 0 and close >= ema50 and
                   len(history_rows) > 0 and
                   float(history_rows[-1].get("close") or 0) < ema50)
    if ema_reclaim:
        bonus = matrix.get_ema_bonus()
        total_score += bonus
        reason_codes.append(f"EMA50_RECLAIM:+{bonus}")

    above_ema20  = close > ema20  if ema20  > 0 else False
    above_ema50  = close > ema50  if ema50  > 0 else False
    above_ema89  = close > ema89  if ema89  > 0 else False

    # ── 6. Price position (close in top 75% of 4-bar range) ──────────────────
    highs = [float(b.get("high") or 0) for b in seq_bars if b.get("high")]
    lows  = [float(b.get("low")  or 0) for b in seq_bars if b.get("low")]
    if highs and lows:
        range_high = max(highs)
        range_low  = min(lows)
        rng = range_high - range_low
        if rng > 0:
            pct_in_range = (close - range_low) / rng
            if pct_in_range >= 0.75:
                bonus = matrix.get_price_position_bonus()
                total_score += bonus
                reason_codes.append(f"CLOSE_TOP75PCT_4BAR:+{bonus}")

    # ── 7. SHORT_GO promotion ─────────────────────────────────────────────────
    if best_role == "SHORT_WATCH" and current_low_4bar is not None and curr_l > 0:
        if close < current_low_4bar:
            bonus = matrix.get_short_go_bonus()
            total_score += bonus
            best_role = "SHORT_GO"
            reason_codes.append(f"BREAK_4BAR_LOW:SHORT_GO:+{bonus}")

    # ── 8. Volume context note ────────────────────────────────────────────────
    if vol_bkt in ("VB", "B"):
        reason_codes.append(f"VOL:{vol_bkt}")
    if wk_sfx:
        reason_codes.append(f"WICK:{wk_sfx}")
    if ne_sfx:
        reason_codes.append(f"NE:{ne_sfx}")

    # ── 9. EMA alignment summary ─────────────────────────────────────────────
    ema_parts = []
    if above_ema20:  ema_parts.append("20✓")
    if above_ema50:  ema_parts.append("50✓")
    if above_ema89:  ema_parts.append("89✓")
    if ema_parts:
        reason_codes.append(f"EMA_ABOVE:{','.join(ema_parts)}")

    # ── 10. Build explanation ─────────────────────────────────────────────────
    explanation_parts = []
    if composite_pattern and matched_composite_rules:
        explanation_parts.append(f"Composite {composite_pattern} matched")
    if seq4_str and matched_seq4_rules:
        explanation_parts.append(f"Seq4 [{seq4_str}] matched")
    if ema_reclaim:
        explanation_parts.append("EMA50 reclaim")
    if not explanation_parts:
        explanation_parts.append(f"Baseline {final_signal}: {baseline.get('action','') if baseline else 'no rule'}")

    explanation = "; ".join(explanation_parts)

    return _build_result(
        ticker, date, final_signal, composite_pattern,
        best_role, total_score, reason_codes, explanation,
        seq4_str=seq4_str,
        lane1=lane1, lane3=lane3,
        vol_bkt=vol_bkt, wk_sfx=wk_sfx,
        above_ema20=above_ema20, above_ema50=above_ema50, above_ema89=above_ema89,
    )


def _build_result(
    ticker, date, final_signal, composite_pattern,
    role, score, reason_codes, explanation,
    seq4_str="", lane1="", lane3="",
    vol_bkt="", wk_sfx="",
    above_ema20=False, above_ema50=False, above_ema89=False,
) -> dict:
    quality = _ROLE_QUALITY.get(role, "—")
    action  = _ROLE_ACTION.get(role, "IGNORE")
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
        "above_ema20":       above_ema20,
        "above_ema50":       above_ema50,
        "above_ema89":       above_ema89,
        "reason_codes":      reason_codes,
        "explanation":       explanation,
    }
