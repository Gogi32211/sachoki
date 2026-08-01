"""
ultra_score.py — shared ULTRA Score helper.

Single source of truth for the ULTRA Score formula. Both:
  • live ULTRA orchestration (ultra_orchestrator.run_ultra_*_job)
  • historical Stock Stat / Bulk Signal CSV generation (main.api_stock_stat)
must call this module so the score is computed identically online and offline.

Hard rules
  • Pure read-only function. Never raises on missing fields — they
    contribute 0.
  • NO LOOKAHEAD: this module must NEVER read forward-return fields
    (ret_1d / ret_3d / ret_5d / ret_10d / mfe_* / mae_* / max_high_* /
    max_drawdown_*). They belong only to Replay Analytics.
  • Score is independent and additive — does not modify any input field.

Range: 0..100 integer, banded A (≥80) / B (≥65) / C (≥50) / D (<50).
"""
from __future__ import annotations

from typing import Iterable

# ─────────────────────────────────────────────────────────────────────────────
# Forward-return / future-bar fields. compute_ultra_score must NEVER read
# any of these (verified by tests). Any future analyst extending the formula
# should keep this list authoritative.
# ─────────────────────────────────────────────────────────────────────────────
_FORWARD_RETURN_FIELDS = frozenset({
    "ret_1d", "ret_3d", "ret_5d", "ret_10d",
    "mfe_5d", "mfe_10d", "mae_5d", "mae_10d",
    "max_high_5d", "max_high_10d", "max_drawdown_5d", "max_drawdown_10d",
    "clean_win_5d", "big_win_10d", "fail_5d", "fail_10d",
})


# ─────────────────────────────────────────────────────────────────────────────
# Signal-name normalisation
#
# The score must work on two row shapes:
#   • Live ULTRA rows — flat boolean keys: row['buy_2809']=1, row['bb_brk']=1
#   • Stock Stat bar rows — list-of-labels columns:
#         row['combo']=['BUY_2809', 'ROCKET', 'BB↑']
#         row['vabs']=['ABS', 'STR']
#     plus space-separated string variants.
#
# Both shapes are normalised here to a canonical set of uppercase tokens
# such as 'BUY_2809', 'ROCKET', 'BB_BRK', 'ABS', 'L34', 'TZ_BULL_FLIP'.
# ─────────────────────────────────────────────────────────────────────────────

# Live row flat-key → canonical token
_LIVE_KEY_TO_CANON = {
    # Breakout / trigger
    "buy_2809":     "BUY_2809",
    "rocket":       "ROCKET",
    "bb_brk":       "BB_BRK",
    "bx_up":        "BX_UP",
    "eb_bull":      "EB_BULL",
    "be_up":        "BE_UP",
    "bo_up":        "BO_UP",
    # Setup / accumulation
    "abs_sig":      "ABS",
    "va":           "VA",
    "svs_2809":     "SVS",
    "climb_sig":    "CLB",
    "load_sig":     "LD",
    "strong_sig":   "STR",
    "best_sig":     "BEST",
    "l34":          "L34",
    "fri34":        "FRI34",
    "tz_bull_flip": "TZ_BULL_FLIP",
    # Confirmation / quality
    "rs_strong":    "RS_STRONG",
    # Bonuses / context
    "already_extended": "EXTENDED",
}

# Stock Stat label → canonical token. Labels in stock_stat CSV columns
# (combo / vabs / l / b / f / g / fly / vol / wick / ultra) are emitted as
# uppercase tokens; arrows / unicode characters are preserved here.
_LABEL_TO_CANON = {
    "BUY_2809":  "BUY_2809",
    "BUY":       "BUY_2809",   # alternate compact label
    "ROCKET":    "ROCKET",
    "🚀":        "ROCKET",
    "BB↑":       "BB_BRK",
    "BX↑":       "BX_UP",
    "EB↑":       "EB_BULL",
    "BE↑":       "BE_UP",
    "BO↑":       "BO_UP",
    "ABS":       "ABS",
    "VA":        "VA",
    "SVS":       "SVS",
    "CLB":       "CLB",
    "LD":        "LD",
    "LOAD":      "LD",
    "STR":       "STR",
    "STRONG":    "STR",
    "BEST":      "BEST",
    "BEST★":     "BEST",
    "L34":       "L34",
    "FRI34":     "FRI34",
    "TZ→3":      "TZ_BULL_FLIP",
    "RS+":       "RS_STRONG",
    "RS":        "RS",          # plain RS, lower weight
    "EXT":       "EXTENDED",
}

# Stock Stat columns that carry list-of-labels (or space-separated strings)
_LABEL_COLUMNS = ("combo", "vabs", "vol", "l", "b", "f", "g", "fly",
                  "ultra", "wick")


def _to_iter(v) -> Iterable[str]:
    if v is None:
        return ()
    if isinstance(v, (list, tuple, set)):
        return (str(x) for x in v if x is not None and x != "")
    if isinstance(v, str):
        # Stock Stat sometimes flattens label lists with " ".join
        return (t for t in v.split() if t)
    return ()


def _truthy(v) -> bool:
    # Handle None first, then NaN / pd.NA (must precede `v == ""` — pd.NA == "" raises).
    if v is None:
        return False
    try:
        import pandas as _pd
        if _pd.isna(v):
            return False
    except Exception:
        pass
    # Sized non-string collections (list / set / ndarray / Series): truthy iff
    # non-empty. Handle before the `v == ""` check, which raises on arrays and
    # would otherwise fall through to a False return for any non-empty collection.
    if not isinstance(v, (str, bytes)) and hasattr(v, "__len__"):
        return len(v) > 0
    try:
        if v == "":
            return False
    except (TypeError, ValueError):
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0 and v == v  # excludes NaN
    if isinstance(v, str):
        return v.strip().lower() not in ("", "0", "0.0", "false", "none", "null", "nan")
    return bool(v)


def _signal_set(row: dict) -> set:
    """Return the canonical uppercase set of active signals on this row.

    Recognises both live flat booleans (live ULTRA shape) and Stock Stat
    compact label columns (T/Z/L/Combo/VABS/ULT/...). Delegates to the
    shared `ultra_signal_parser` so live + historical scoring stay in
    lockstep.
    """
    if not isinstance(row, dict):
        return set()
    from ultra_signal_parser import parse_stock_stat_signals
    p = parse_stock_stat_signals(row)
    s: set = set()

    # Map parser-flag key → canonical token name expected by compute_ultra_score.
    # The parser uses the same tokens compute_ultra_score reads, with a few
    # aliases (e.g. parser 'four_bf' / 'l88' → score doesn't care).
    _PARSER_TO_CANON = {
        # Breakouts
        "buy_2809":  "BUY_2809",
        "rocket":    "ROCKET",
        "bb_brk":    "BB_BRK",
        "bx_up":     "BX_UP",
        "eb_bull":   "EB_BULL",
        "be_up":     "BE_UP",
        "bo_up":     "BO_UP",
        # Setups
        "abs_sig":     "ABS",
        "va":          "VA",
        "svs_2809":    "SVS",
        "climb_sig":   "CLB",
        "load_sig":    "LD",
        "strong_sig":  "STR",
        "l34":         "L34",
        "fri34":       "FRI34",
        "tz_bull_flip":"TZ_BULL_FLIP",
        # 260523 — AD-FRESH / WYC Phase
        "ad_fresh":    "AD_FRESH",
        "ad_cluster":  "AD_CLUSTER",
        "wyc_spring":  "WYC_SPRING",
        "wyc_sos":     "WYC_SOS",
        "wyc_acc_tr":  "WYC_ACC_TR",
        "wyc_markup":  "WYC_MARKUP",
        # 260523 v3.5 — PREBREAK + WYC additional
        "pb_lvbo":          "PB_LVBO",
        "pb_stop_cause":    "PB_STOP_CAUSE",
        "pb_wvf_confirm":   "PB_WVF_CONFIRM",
        "pb_macro_penalty": "PB_MACRO_PENALTY",
        "wyc_in_tr":        "WYC_IN_TR",
        "wyc_sow":          "WYC_SOW",
        # Quality
        "rs_strong":   "RS_STRONG",
    }
    for parser_key, canon in _PARSER_TO_CANON.items():
        if p.get(parser_key):
            s.add(canon)

    # Pass-through fallback for live rows that still use the legacy flat
    # boolean keys not covered by the parser (e.g. best_sig, already_extended).
    for k, canon in _LIVE_KEY_TO_CANON.items():
        if _truthy(row.get(k)):
            s.add(canon)

    # tz_sig like "T4" — keep as a token so `compute_ultra_score` can
    # surface it in reasons even though it's not used in any combo gate.
    tz = row.get("tz_sig") or row.get("tz") or p.get("t_signal")
    if isinstance(tz, str) and tz.strip():
        s.add(tz.strip().upper())

    return s


def _safe_float(v, default=0.0) -> float:
    try:
        if v is None or v == "":
            return default
        f = float(v)
        return default if (f != f or f in (float("inf"), float("-inf"))) else f
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# Context lookups (TZ Intel role / pullback tier / rare tier / ABR cat)
# ─────────────────────────────────────────────────────────────────────────────

_INTEL_GO_READY    = frozenset({
    "BULL_A", "BULL_B", "BULL_CONTINUATION_A", "BULL_CONTINUATION_B",
    "PULLBACK_GO", "PULLBACK_CONFIRMING",
    "PULLBACK_READY_A", "PULLBACK_READY_B",
})
_INTEL_REJECT_LONG = frozenset({"REJECT_LONG", "REJECT"})
_INTEL_SHORT       = frozenset({"SHORT_WATCH", "SHORT_GO"})


def _intel_role(row: dict) -> str:
    intel = row.get("tz_intel")
    if isinstance(intel, dict):
        return (intel.get("role") or "").upper()
    # Stock Stat flat field
    return (row.get("tz_intel_role") or "").upper()


def _pullback(row: dict) -> tuple[str, str, bool]:
    """Return (tier, stage, active) for the row, accepting both shapes."""
    pb = row.get("pullback")
    if isinstance(pb, dict):
        return (
            (pb.get("evidence_tier") or "").upper(),
            (pb.get("pullback_stage") or "").upper(),
            bool(pb.get("is_currently_active")),
        )
    return (
        (row.get("pullback_evidence_tier") or "").upper(),
        (row.get("pullback_pullback_stage") or row.get("pullback_stage") or "").upper(),
        _truthy(row.get("pullback_is_currently_active")),
    )


def _rare(row: dict) -> tuple[str, bool]:
    rr = row.get("rare_reversal")
    if isinstance(rr, dict):
        return (
            (rr.get("evidence_tier") or "").upper(),
            bool(rr.get("is_currently_active")),
        )
    return (
        (row.get("rare_evidence_tier") or "").upper(),
        _truthy(row.get("rare_is_currently_active")),
    )


def _abr_category(row: dict) -> str:
    abr = row.get("abr")
    if isinstance(abr, dict):
        return (abr.get("category") or "").upper()
    return (row.get("abr_category") or "").upper()


def _profile_category(row: dict) -> str:
    return (row.get("profile_category") or "").upper()


# ─────────────────────────────────────────────────────────────────────────────
# Regime calibration (replay v2 — derived from SP500 1D historical replay).
# Strong regimes get the largest bonus; bearish regimes contribute a warning
# flag but no hard reject. Bonuses are additive on top of the existing A..F
# components.
# ─────────────────────────────────────────────────────────────────────────────

_STRONG_REGIMES = frozenset({"ACTIONABLE_SETUP", "SHAKEOUT_ABSORB", "CLEAN_ENTRY"})

_REGIME_BONUS = {
    "ACTIONABLE_SETUP":  (12, "REGIME:ACTIONABLE"),
    "SHAKEOUT_ABSORB":   (10, "REGIME:SHAKEOUT"),
    "CLEAN_ENTRY":       ( 8, "REGIME:CLEAN"),
    "REBOUND_SQUEEZE":   ( 5, "REGIME:REBOUND_SQUEEZE"),
    "RISK_REBOUND":      ( 3, "REGIME:RISK_REBOUND"),
    "ROCKET_WATCH":      ( 0, ""),
    # 260523 — Wyckoff phase regime bonuses
    "SPRING_CONFIRMED":  (12, "REGIME:SPRING"),
    "SOS_CONFIRMED":     ( 8, "REGIME:SOS"),
    "ACC_TR_CONTEXT":    ( 4, "REGIME:ACC_TR"),
}

_BEARISH_REGIMES = frozenset({"BEARISH_PHASE", "BEARISH_CONTEXT"})


def _final_regime(row: dict) -> str:
    return (row.get("FINAL_REGIME") or row.get("final_regime") or "").upper()


def _safe_change_pct(row: dict) -> float | None:
    v = row.get("change_pct")
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main scoring function
# ─────────────────────────────────────────────────────────────────────────────

def compute_ultra_score(row: dict) -> dict:
    """Return ULTRA Score dict for the row.

    Output keys:
      ultra_score                     int 0..100
      ultra_score_band                'A' | 'B' | 'C' | 'D'
      ultra_score_reasons             list[str]  (deduped, capped at 12)
      ultra_score_flags               list[str]  (combo / context flags)
      ultra_score_raw_before_penalty  int   sum of A+B+C+D+F (clamped 0..100)
      ultra_score_penalty_total       int   absolute value of E component

    The function never raises on missing fields.
    """
    if not isinstance(row, dict):
        return _empty_result()

    sigs    = _signal_set(row)
    intel_r = _intel_role(row)
    pb_tier, pb_stage, pb_active = _pullback(row)
    rr_tier, rr_active           = _rare(row)
    abr_cat                      = _abr_category(row)
    cat                          = _profile_category(row)
    pf                           = _safe_float(row.get("profile_score"), default=-1)

    reasons: list[str] = []
    flags:   list[str] = []

    def has(*tokens) -> bool:
        return any(t in sigs for t in tokens)

    # ── A. Breakout / Trigger (cap 35) ──────────────────────────────────────
    a = 0
    # BUY_2809 +20 and ROCKET +20 REMOVED 2026-08-01 — the two largest bonuses left in this
    # scheme after BB↑ (+15) fell, and both measured WORSE than baseline in the 46-signal
    # league (path-sim $21-377, 6yr, baseline −0.73):
    #   BUY_2809 (buy_here): median −1.05 / win 47.0 / 3-6yr / worst −6.4  (n=28,614)
    #   ROCKET:              median −1.46 / win 46.2 / 3-6yr / worst −3.5  (n=3,651)
    # That makes it 5 of 5 measured legacy weights empty or backwards (CCI0R, BB↑, SVS,
    # BUY_2809, ROCKET). The MOMO+CAT combination bonus below still references them as
    # category members — untouched, as with has_breakout_any: changing a combination's
    # membership changes other bonuses' behaviour and needs its own measurement.
    # if "BUY_2809" in sigs: a += 20
    # if "ROCKET"   in sigs: a += 20
    # BB↑ weight REMOVED 2026-07-29 — measured for the first time and it is NEGATIVE:
    # path-sim $21-377, 6yr, n=18,514 → median −1.55 / win 46.1 / pf 1.05 vs a −0.63 baseline
    # (0.92pp WORSE), and the NOT-BB↑ half is −0.56. It was the largest single bonus in this
    # whole scheme, pointing the wrong way. Same story as CCI0R (a89ef7d): a weight riding on
    # an unmeasured heuristic. Definition: close > BB_upper(20,2) & vol crosses 1.5x & RSI>55
    # — a pure strength-chase breakout, exactly what "fade strength" predicts should lose.
    # if "BB_BRK" in sigs: a += 15
    if "BX_UP"    in sigs: a += 12; reasons.append("BX↑")
    if "EB_BULL"  in sigs: a += 10; reasons.append("EB↑")
    if "BE_UP"    in sigs: a += 10; reasons.append("BE↑")
    if "BO_UP"    in sigs: a += 10; reasons.append("BO↑")
    a = min(a, 35)
    has_breakout = a > 0

    # ── B. Setup / Accumulation (cap 25) ────────────────────────────────────
    b = 0
    if "ABS"          in sigs: b += 10; reasons.append("ABS")
    if "VA"           in sigs: b += 8;  reasons.append("VA")
    # SVS weight REMOVED 2026-07-29 — measured empty: n=79,977, median −0.69 vs a −0.63
    # baseline, and NOT-SVS is −0.53. Zero content. (vol/avg20 crosses 1.4x on a GREEN bar —
    # the breakout side of the context-dependent volume law, where volume does not pay.)
    # if "SVS" in sigs: b += 8
    if "CLB"          in sigs: b += 7;  reasons.append("CLB")
    if "LD"           in sigs: b += 6;  reasons.append("LD")
    if "STR"          in sigs: b += 8;  reasons.append("STR")
    if "L34"          in sigs: b += 6;  reasons.append("L34")
    if "FRI34"        in sigs: b += 6;  reasons.append("FRI34")
    if "TZ_BULL_FLIP" in sigs: b += 10; reasons.append("TZ→3")
    # ── 260523: AD-FRESH / AD-CLUSTER / WYC Phase (Setup / Accumulation) ────
    if "AD_CLUSTER" in sigs: b += 15; reasons.append("AD-CLUSTER★★")
    elif "AD_FRESH" in sigs: b += 8;  reasons.append("AD-FRESH★")
    if "WYC_SPRING" in sigs: b += 10; reasons.append("WYC:SPRING")
    elif "WYC_SOS"  in sigs: b += 6;  reasons.append("WYC:SOS")
    b = min(b, 25)
    has_setup = b > 0

    # ── C. Confirmation / Quality (cap 25) ──────────────────────────────────
    c = 0
    if "RS_STRONG" in sigs:
        c += 8; reasons.append("RS+")
    if pf >= 0:
        if   pf >= 18: c += 12
        elif pf >= 12: c += 9
        elif pf >= 7:  c += 6
        elif pf >= 1:  c += 3
        if pf >= 1:
            reasons.append(f"PF={int(pf)}")
    if   cat == "SWEET_SPOT": c += 10; reasons.append("SWEET_SPOT")
    elif cat == "BUILDING":   c += 6;  reasons.append("BUILDING")
    elif cat == "WATCH":      c += 2
    c = min(c, 25)

    # ── D. Context (-20..+20) ───────────────────────────────────────────────
    d = 0
    if intel_r in _INTEL_GO_READY:
        d += 8; reasons.append(intel_r)
    elif intel_r == "BULL_WATCH":
        d += 6; reasons.append("BULL_WATCH")
    elif intel_r in ("PULLBACK_WATCH", "EXTENDED_WATCH",
                      "DEEP_PULLBACK_WATCH", "MIXED_WATCH"):
        d += 4

    if pb_tier == "CONFIRMED_PULLBACK":
        d += 10; reasons.append("CPB")
    elif "READY" in pb_stage or "GO" in pb_stage or pb_active:
        d += 8
        reasons.append("RPB" if "READY" in pb_stage else
                        "GPB" if "GO" in pb_stage else "PB-active")
    elif pb_tier == "ANECDOTAL_PULLBACK":
        d += 4; reasons.append("APB")
    elif pb_tier:
        d += 2

    if rr_tier in ("CONFIRMED_RARE", "CONFIRMED_PATTERN"):
        d += 8; reasons.append("CP")
    elif rr_active:
        d += 8; reasons.append("AP")
    elif "READY" in rr_tier:
        d += 6; reasons.append("RP")
    elif rr_tier in ("FORMING_PATTERN", "ANECDOTAL_RARE"):
        d += 2
    elif rr_tier == "WATCH_PATTERN":
        d += 1

    if   abr_cat == "B+": d += 6; reasons.append("ABR=B+")
    elif abr_cat == "B":  d += 4
    elif abr_cat == "A":  d += 3
    elif abr_cat == "R":  d -= 4

    # 260523 v3.1: swing context (HH/LH/HL/LL). Empirical SP500 1D shows
    # HL is the strongest entry context (win 77%, +3% avg 5d).
    swing_type = (row.get("swing_type") or "").strip()
    if swing_type == "HL":
        d += 8; reasons.append("SWING:HL")
    elif swing_type == "LL":
        d += 5; reasons.append("SWING:LL")
    elif swing_type == "LH":
        d -= 8; reasons.append("SWING:LH")
    elif swing_type == "HH":
        d -= 5; reasons.append("SWING:HH")

    # 260523 v3.5: PREBREAK + WYC additional context
    if "PB_LVBO" in sigs:
        d += 6; reasons.append("LVBO")
    if "PB_WVF_CONFIRM" in sigs:
        d += 5; reasons.append("WVF+")
    if "WYC_IN_TR" in sigs:
        d += 3; reasons.append("IN_TR")
    if "PB_MACRO_PENALTY" in sigs:
        d -= 8; reasons.append("MACRO-")
    if "WYC_SOW" in sigs:
        d -= 6; reasons.append("SOW")

    d = max(min(d, 20), -20)

    # ── F. Combination bonuses ──────────────────────────────────────────────
    f = 0
    has_breakout_any = has("BUY_2809", "ROCKET", "BB_BRK", "BX_UP",
                           "EB_BULL", "BE_UP", "BO_UP")
    has_setup_any   = has("ABS", "VA", "SVS", "CLB", "LD")
    rs_plus         = "RS_STRONG" in sigs

    if (has("BUY_2809", "ROCKET")) and cat in ("SWEET_SPOT", "BUILDING"):
        f += 12; reasons.append("MOMO+CAT"); flags.append("MOMENTUM_A")
    if has_setup_any and has_breakout_any and rs_plus:
        f += 15; reasons.append("REV-GROW"); flags.append("REVERSAL_GROWTH_A")
    if "TZ_BULL_FLIP" in sigs and has_breakout_any and rs_plus:
        f += 12; reasons.append("TRANSITION"); flags.append("TRANSITION_A")
    if pb_tier == "CONFIRMED_PULLBACK" and has_breakout_any \
            and cat in ("SWEET_SPOT", "BUILDING"):
        f += 12; reasons.append("PB-ENTRY"); flags.append("PULLBACK_ENTRY_A")
    # ── L34 / FRI34 calibrated tiering (replay v2) ──────────────────────────
    # Standalone L34/FRI34 is weak (win 10D ≈48%). Only confluence matters:
    #   alone:                        +2
    #   + breakout:                   +5
    #   + breakout + PF good (>=12):  +7
    #   + breakout + PF good + strong regime: +10
    has_l34 = has("L34", "FRI34")
    if has_l34:
        pf_good_l34 = (pf >= 12)
        regime_now  = _final_regime(row)
        strong_reg  = (regime_now in _STRONG_REGIMES)
        if has_breakout_any and pf_good_l34 and strong_reg:
            f += 10; reasons.append("L34→TRIG+REG"); flags.append("L34_TRIGGER_A")
        elif has_breakout_any and pf_good_l34:
            f += 7;  reasons.append("L34→TRIG+PF");  flags.append("L34_TRIGGER_A")
        elif has_breakout_any:
            f += 5;  reasons.append("L34→TRIG");     flags.append("L34_TRIGGER_A")
        else:
            f += 2;  reasons.append("L34")
    if has_setup_any and not has_breakout_any:
        flags.append("SETUP_ONLY")
    if has_breakout_any and not has_setup_any:
        flags.append("BREAKOUT_ONLY")

    # ── REVERSAL_GROWTH_A_NO_RS calibrated tiering (replay v2) ──────────────
    # When setup + breakout combine without RS+, the existing REV-GROW bonus
    # already fires (cap-ish at +15). Replay shows this group is moderate
    # (avg 10D +1.17%, win 10D 51.5%), so we pre-bake additive bonus tiers
    # that only matter when this branch fires WITHOUT the strict RS+ branch.
    rev_growth_no_rs = (has_setup_any and has_breakout_any and not rs_plus)
    if rev_growth_no_rs:
        flags.append("REVERSAL_GROWTH_A_NO_RS")
        regime_now = _final_regime(row)
        pf_good    = (pf >= 12)
        strong_reg = (regime_now in _STRONG_REGIMES)
        # alone +8, +PF good +10, +strong regime +14, both +18
        if pf_good and strong_reg:
            f += 18; reasons.append("REV-GROW(NO_RS)+PF+REG")
        elif strong_reg:
            f += 14; reasons.append("REV-GROW(NO_RS)+REG")
        elif pf_good:
            f += 10; reasons.append("REV-GROW(NO_RS)+PF")
        else:
            f += 8;  reasons.append("REV-GROW(NO_RS)")

    # ── E. Penalties ────────────────────────────────────────────────────────
    e = 0
    if intel_r in _INTEL_REJECT_LONG:
        e -= 10; reasons.append("REJECT(-)"); flags.append("REJECT_CONTEXT")
    elif intel_r in _INTEL_SHORT:
        e -= 8;  reasons.append("SHORT_WATCH(-)"); flags.append("SHORT_CONTEXT")
    if cat == "WATCH" and pf >= 0 and pf < 5:
        e -= 4; flags.append("WATCH_LOW_PF")
    if not has_breakout and not has_setup:
        e -= 5; flags.append("ISOLATED")
    if "EXTENDED" in sigs:
        flags.append("EXTENDED_MOVE")
    if _truthy(row.get("rsi_extended")):
        flags.append("RSI_EXTENDED")
    if _truthy(row.get("cci_extended")):
        flags.append("CCI_EXTENDED")

    # ── G. Regime bonus (replay v2) ─────────────────────────────────────────
    # FINAL_REGIME contributes a measured additive bonus. ROCKET_WATCH gets
    # zero (already-running). BEARISH gets a warning flag but no hard reject.
    regime = _final_regime(row)
    g = 0
    regime_bonus_label = ""
    if regime in _REGIME_BONUS:
        g, label = _REGIME_BONUS[regime]
        if label:
            reasons.append(label); regime_bonus_label = label
    elif regime in _BEARISH_REGIMES:
        flags.append("BEARISH_CONTEXT_WARN")
    strong_regime = (regime in _STRONG_REGIMES)

    # ── Light extension penalty (warning, not rejection) ────────────────────
    chg_pct = _safe_change_pct(row)
    if chg_pct is not None and chg_pct >= 25 and not strong_regime:
        e -= 4; flags.append("EXTENDED_PENALTY_LIGHT")

    raw = a + b + c + d + f + g
    raw_clamped = max(0, min(100, int(round(raw))))
    total = raw + e
    score = max(0, min(100, int(round(total))))

    # ── Confluence-aware caps (replay v2) ───────────────────────────────────
    momentum_a    = ("MOMENTUM_A" in flags)
    setup_only    = ("SETUP_ONLY" in flags)
    breakout_only = ("BREAKOUT_ONLY" in flags)
    pf_good       = (pf >= 12)
    sweet_spot    = (cat == "SWEET_SPOT")

    confluence_count = 0
    if has_setup_any:                         confluence_count += 1
    if has_breakout_any:                      confluence_count += 1
    if pf_good:                               confluence_count += 1
    if sweet_spot:                            confluence_count += 1

    caps_applied: list[str] = []
    cap_reasons: list[str]  = []

    # Rule 1: MOMENTUM_A without strong regime can't exceed 89 unless ≥2
    # additional strong confluences exist (setup + breakout + PF + SWEET_SPOT).
    if momentum_a and not strong_regime and score > 89 and confluence_count < 4:
        # MOMENTUM_A already counts breakout; require ≥2 of the others (setup,
        # PF good, sweet_spot). Confluence sources beyond breakout: count them.
        extra_conf = (1 if has_setup_any else 0) + (1 if pf_good else 0) + (1 if sweet_spot else 0)
        if extra_conf < 2:
            score = min(score, 89)
            caps_applied.append("CAP_MOMENTUM_A_NO_REGIME")
            cap_reasons.append("momentum_a:no_strong_regime:no_double_confluence")

    # Rule 2: SETUP_ONLY caps at 49 unless PF good + strong regime present.
    if setup_only and not (pf_good and strong_regime) and score > 49:
        score = 49
        caps_applied.append("CAP_SETUP_ONLY")
        cap_reasons.append("setup_only:no_pf_or_regime")

    # Rule 3: BREAKOUT_ONLY caps at 59 unless PF good + strong regime present.
    if breakout_only and not (pf_good and strong_regime) and score > 59:
        score = 59
        caps_applied.append("CAP_BREAKOUT_ONLY")
        cap_reasons.append("breakout_only:no_pf_or_regime")

    band = compute_ultra_score_band(score)
    band_v2, priority = compute_ultra_score_priority(score)

    # Dedupe + cap reasons
    seen = set()
    out_reasons: list[str] = []
    for r in reasons:
        if r not in seen:
            out_reasons.append(r); seen.add(r)
        if len(out_reasons) >= 12:
            break
    seen_f = set()
    out_flags: list[str] = []
    for fg in flags:
        if fg not in seen_f:
            out_flags.append(fg); seen_f.add(fg)

    return {
        "ultra_score":                    score,
        "ultra_score_band":               band,
        "ultra_score_band_v2":            band_v2,
        "ultra_score_priority":           priority,
        "ultra_score_reasons":            out_reasons,
        "ultra_score_flags":              out_flags,
        "ultra_score_raw_before_penalty": raw_clamped,
        "ultra_score_penalty_total":      abs(int(round(e))),
        "ultra_score_regime_bonus":       int(g),
        "ultra_score_caps_applied":       list(caps_applied),
        "ultra_score_cap_reason":         "|".join(cap_reasons),
    }


# ─────────────────────────────────────────────────────────────────────────────
# ULTRA Score v3 — the reweighted ranker (validated 2026-07-18, reweight.py).
#
# The v1/v2 score above is anchored on the BREAKOUT block (ROCKET+20, BUY_2809+20,
# BX_UP+12, BO/BE/EB +10). A 6-yr path-sim of every component (trail25) showed that block
# is mostly non- or ANTI-predictive: ROCKET 0/6 yrs (mLift −0.79), BUY_2809 1/6 (−0.52),
# BO_UP 0/6, VA/SVS negative. Reconstructed & banded, the whole breakout-heavy core has
# Spearman ≈ −0.004 vs forward return — it does NOT rank. The two STRONGEST rankers the old
# score ignores are RSI-oversold (RSI<35 mLift +0.72) and the PRICE zone ($21-89). Adding
# those + keeping only the components that earn their points (BX_UP 5/6 yrs, STR 5/6,
# d_absorb) lifts Spearman to +0.051 (Q5−Q1 med +3.49pp, 5/6 yrs). Adding this session's
# VALIDATED edge axes — 🏆RS-intact, 🎯cluster (conf_n), 🎋TLS — reaches +0.078 (Q5−Q1 med
# +6.74pp, win +11pp, monotone quintiles, Q5>Q1 6/6 yrs incl 2021-22).
#
# ⚠ Still MODEST in absolute terms (a better TRIAGE, not a standalone edge — top quintile
# med −0.72/win 48% vs bottom −7.46/win 37%). Shipped ALONGSIDE the old score, not replacing
# it. RS/cluster/TLS are read from the row when the serving layer injects them (they live in
# edge_replay, not the raw Ultra row) — the score degrades gracefully to the +0.051 core
# when they are absent. Weights are the validated-direction heuristics, not grid-tuned.
_V3_OSV = ((30, 20), (35, 15), (50, 8), (60, 0), (70, -8))   # RSI cut → oversold points
# price cut → zone points. Upper quality band widened 89 → 377 on 2026-07-27 after the
# Fibonacci-zone sweep (win% and catastrophe keep improving above $89; 233-377 has the best
# win% of all); anything ≥$377 falls to the default −6 (that band is 3/6yr on its own).
_V3_PXV = ((8, -12), (21, -6), (377, 10))
_V3_PX_DEFAULT = -6
# Intraday volume-event axes (2026-07-27). Validated across ALL 29 TZ/L signal codes: a day
# whose biggest 15m bar never reached 2.5× the session average drops every signal's median by
# 4-8 points, and on the v3 top band it is a med −4.01 cell. Measured on v3's own quintile
# spread (Q5−Q1 median): as-is +0.71 → +veto +1.12 → +bonus +1.56 → +all three **+2.32**,
# with Q5 median turning positive (−0.45 → +0.05) and win% 48.9 → 50.1. Additive, not
# redundant — each component helps alone. See [[project_volume_magnitude]].
_V3_VOL_EVENT_BONUS = 8
_V3_NO_VOL_VETO     = 25


def compute_ultra_score_v3(row: dict) -> dict:
    """Reweighted ULTRA ranker. Returns {ultra_score_v3, ultra_score_v3_band,
    ultra_score_v3_reasons}. Pure — reads only row fields (incl. optional rs_intact /
    conf_n / tls_bar injected by the serving layer). Never raises on a bad row."""
    try:
        sigs = _signal_set(row)
        reasons: list[str] = []
        # ── earners (the only breakout/setup components that path-sim-rank; cap 25) ──
        earn = 0
        if "BX_UP" in sigs:                     earn += 12; reasons.append("BX↑")
        if "STR" in sigs:                       earn += 8;  reasons.append("STR")
        if _truthy(row.get("d_absorb_bull")):   earn += 15; reasons.append("ABSORB")
        earn = min(earn, 25)
        # ── oversold (RSI) — the strongest ignored ranker ──
        rsi = _safe_float(row.get("rsi"), default=50.0)
        osv = -18
        for cut, pts in _V3_OSV:
            if rsi < cut:
                osv = pts; break
        if   osv >= 15: reasons.append(f"OVERSOLD{int(rsi)}")
        elif osv <= -8: reasons.append(f"OVERBOUGHT{int(rsi)}")
        # ── price zone (the Fib quality law) ──
        px = _safe_float(row.get("last_price") or row.get("price") or row.get("close"), 0.0)
        pzv = _V3_PX_DEFAULT
        for cut, pts in _V3_PXV:
            if px < cut:
                pzv = pts; break
        if   pzv == 10:            reasons.append("QZ$21-377")
        elif px >= 377:            reasons.append("MEGA$377+")
        elif pzv < 0:              reasons.append("CHEAP$-")
        # ── this session's validated edge axes (present only when the serving layer injects) ──
        bonus = 0
        if _truthy(row.get("rs_intact")):
            bonus += 12; reasons.append("🏆RS")
        cn = int(_safe_float(row.get("conf_n"), 0.0))
        if cn >= 3:
            bonus += min(cn, 6) * 4; reasons.append(f"🎯×{cn}fam")
        if _truthy(row.get("tls_bar")):
            bonus += 10; reasons.append("🎋TLS")
        # 💥/⛔ intraday volume event — the most universal axis we have (29/29 signal codes)
        if _truthy(row.get("no_vol_event")):
            bonus -= _V3_NO_VOL_VETO; reasons.append("⛔noVOL")
        elif _truthy(row.get("iv_vspike")):
            bonus += _V3_VOL_EVENT_BONUS; reasons.append("💥vol")
        score = max(0, min(100, int(round(earn + osv + pzv + bonus))))
        return {
            "ultra_score_v3":         score,
            "ultra_score_v3_band":    compute_ultra_score_v3_band(score),
            "ultra_score_v3_reasons": reasons[:10],
        }
    except Exception:
        return {"ultra_score_v3": 0, "ultra_score_v3_band": "D", "ultra_score_v3_reasons": []}


# ── 🎲 SCORE-HITS: how many of our rankers sit in THEIR OWN good zone ────────────────────
# 2026-07-27 (user's idea: "don't change any score — just map where each one is actually
# good, then combine those zones"). Every score was bucketed by quintile and path-simmed:
# most carry NO information (profile/beta/rtb_total/aes/prebreak_v4 are flat), and two are
# INVERTED — high ultra_score and high buy_score are traps, high prebreak_v2 is the worst
# cell of all (med −3.80/win 43%). So the zones below are NOT "high is good"; each is that
# score's own measured sweet spot.
#
# Zones were selected on TRAIN (2021-23) ONLY and the ensemble then tested on 2024-26:
#   full period  hits 0 → 5 : med −1.06 · −0.67 · +0.00 · +1.05 · +2.12 · +3.79  (monotone)
#                hits ≥4 = 6/6yr, BOTH bear years positive (2021 +2.0/+5.1, 2022 +1.6/+2.5)
#   OOS 2024-26  hits 0 → 5 : med +0.43 · +0.62 · +1.09 · +1.38 · +1.98 · +3.54  (holds)
# Individually every component is near-worthless (best bucket med −0.2..−1.4); the AGREEMENT
# is what carries. hits=5 is rare (~400/yr universe-wide) — that rarity is the point.
# Re-derived 2026-08-01 after ultra_score lost BB↑+15 / SVS+8 / BUY+20 / ROCKET+20 (the DB
# history is mixed-formula, so derivation ran on an ADJUSTED score: stored − removed-weight
# flags). Band shapes on fwd_10d medians, then the ladder path-sim-validated: the NEW ladder
# is monotone 0→5 (−1.22 → +3.69) and hits>=4 went +1.05/5-6yr/worst −0.8 (old zones) →
# +2.14/6-6yr/worst +1.0 — double the median and a positive worst year. turbo_score (pocket
# 9-15, +0.107) was tried as a 7th member and REJECTED: >=4 drops to +1.70 — it dilutes.
_HIT_ZONES = (
    ("ultra_score_v3", 25.0, 1e9),    # monotone ranker; the 18-25 shoulder is only +0.06 — was 18+
    ("ultra_score",     9.0, 22.0),   # INVERTED-U plateau +0.62..+0.68/win 55 — was 7-20
    ("buy_score",      39.0, 57.0),   # INVERTED-U: peak 39-46; >=66 is −0.21 — was 38-57
    ("prebreak_v2",    11.0, 13.0),   # ANTI-predictive: 25-44 is −1.23(!) — was 9-12
    ("prebreak_v3",     4.0,  5.0),   # unchanged; second (non-contiguous) pocket at 8-11 noted
    ("conf_n",          4.0, 1e9),    # unchanged
)


def compute_score_hits(row: dict) -> dict:
    """{score_hits, score_hits_of, score_hits_which} — count of rankers inside their own
    measured good zone. A FILTER, not a score: hits≥4 was 6/6yr with both bear years
    positive. Missing fields simply don't count (never raises)."""
    try:
        hit, which = 0, []
        for key, lo, hi in _HIT_ZONES:
            v = row.get(key)
            if v is None or v == "":
                continue
            v = _safe_float(v, default=None)
            if v is None:
                continue
            if lo < v <= hi:
                hit += 1; which.append(key)
        return {"score_hits": hit, "score_hits_of": len(_HIT_ZONES), "score_hits_which": which}
    except Exception:
        return {"score_hits": 0, "score_hits_of": len(_HIT_ZONES), "score_hits_which": []}


def compute_ultra_score_v3_band(score) -> str:
    """v3 has its OWN range (~0-100 but a strong name = quality-zone + oversold + cluster
    lands ~55-75, top-with-TLS ~85+), so it needs its own thresholds — the v1/v2 80/65/50
    cutoffs would band nearly everything D. Calibrated to v3's Q5/Q4/Q3 quintile edges."""
    s = _safe_float(score, default=0)
    if   s >= 60: return "A"
    elif s >= 45: return "B"
    elif s >= 30: return "C"
    else:         return "D"


def compute_ultra_score_band(score) -> str:
    s = _safe_float(score, default=0)
    if   s >= 80: return "A"
    elif s >= 65: return "B"
    elif s >= 50: return "C"
    else:         return "D"


def compute_ultra_score_priority(score) -> tuple[str, str]:
    """Return (band_v2, priority) using the replay v2 calibration.

    Bands:
      90+ → A+ / HIGH_PRIORITY
      80–89 → A / WATCH_A
      65–79 → B / STRONG_WATCH
      50–64 → C / CONTEXT_WATCH
      <50  → D / LOW
    """
    s = _safe_float(score, default=0)
    if   s >= 90: return ("A+", "HIGH_PRIORITY")
    elif s >= 80: return ("A",  "WATCH_A")
    elif s >= 65: return ("B",  "STRONG_WATCH")
    elif s >= 50: return ("C",  "CONTEXT_WATCH")
    else:         return ("D",  "LOW")


def compute_ultra_score_reasons(row: dict) -> list:
    return compute_ultra_score(row).get("ultra_score_reasons", [])


def compute_ultra_score_flags(row: dict) -> list:
    return compute_ultra_score(row).get("ultra_score_flags", [])


def _empty_result() -> dict:
    return {
        "ultra_score":                    0,
        "ultra_score_band":               "D",
        "ultra_score_band_v2":            "D",
        "ultra_score_priority":           "LOW",
        "ultra_score_reasons":            [],
        "ultra_score_flags":              [],
        "ultra_score_raw_before_penalty": 0,
        "ultra_score_penalty_total":      0,
        "ultra_score_regime_bonus":       0,
        "ultra_score_caps_applied":       [],
        "ultra_score_cap_reason":         "",
    }
