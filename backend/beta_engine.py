"""
beta_engine.py — BETA Score v1  (2026-05-09)

An alternative ranking score trained on NASDAQ+SP500 (478,909 bars).
Non-linear transform penalises over-extension so the optimal zone is 82-96,
not just "as high as possible".

Output fields
─────────────
  beta_score    int   0-100  (display after non-linear transform)
  beta_raw      int   pre-transform raw value
  beta_setup    int   0-60   structural quality component
  beta_momentum int   −5-50  momentum/regime component
  beta_excess   int   ≥0     extension penalty (momentum >> setup)
  beta_zone     str   OPTIMAL|BUY|WATCH|BUILDING|EXTENDED|SHORT_WATCH|NEUTRAL
  beta_auto_buy bool  True only inside strict multi-condition gate

Rule: NEVER reads ret_*, mfe_*, mae_* fields — no lookahead.
"""

from __future__ import annotations

BETA_SCORE_VERSION = "2026-05-09-v1"

# ─── T/Z weight tables ────────────────────────────────────────────────────────

_TZ_W_SP500: dict[str, float] = {
    "T9": 9, "T1": 8, "T1G": 8, "T2G": 8, "T11": 5,
    "T4": 7, "T6": 7, "T3": 4, "T12": 3, "T2": 5, "T10": 4, "T5": 1,
}

_TZ_W_NQ: dict[str, float] = {
    "T9": 7, "T1": 8, "T1G": 6, "T2G": 8, "T11": 3,
    "T4": 5, "T6": 5, "T3": 4, "T12": 3, "T2": 5, "T10": 4, "T5": 1,
}


def _get_tz_w(universe: str) -> dict[str, float]:
    return _TZ_W_SP500 if universe == "sp500" else _TZ_W_NQ


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _sf(row: dict, key: str, default: float = 0.0) -> float:
    try:
        v = row.get(key)
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _sb(row: dict, key: str) -> bool:
    return bool(row.get(key))


# ─── F-signal functions ───────────────────────────────────────────────────────

def _calc_f_setup(row: dict) -> float:
    pts = 0.0
    if _sb(row, "f9"):   pts += 5   # +3.2pp delta — best F
    elif _sb(row, "f1"): pts += 2   # +1.4pp
    if _sb(row, "f5"):   pts += 2   # +1.7pp (stacks with f1)
    if _sb(row, "f7"):   pts += 1   # +0.7pp
    return pts


def _calc_f_momentum_penalty(row: dict) -> float:
    pen = 0.0
    if _sb(row, "f4"):  pen += 5   # −4.6pp — worst F
    if _sb(row, "f6"):  pen += 4   # −3.7pp
    if _sb(row, "f11"): pen += 3   # −2.5pp
    if _sb(row, "f2"):  pen += 1
    if _sb(row, "f8"):  pen += 1
    return pen


# ─── Sequence bonus (cap 8) ───────────────────────────────────────────────────

def _calc_sequence_bonus(row: dict, history: list[dict], universe: str) -> float:
    """
    history: most-recent-first, history[0] = 1 bar ago.
    Each history entry needs "T" and "Z" string keys.
    """
    bonus = 0.0
    cur_t = str(row.get("T", "") or "")
    cur_z = str(row.get("Z", "") or "")

    def pt(n: int) -> str:
        return str((history[n - 1].get("T", "") if len(history) >= n else "") or "")

    def pz(n: int) -> str:
        return str((history[n - 1].get("Z", "") if len(history) >= n else "") or "")

    def has_t(n: int) -> bool:
        return bool(pt(n))

    def has_z(n: int) -> bool:
        return bool(pz(n))

    # ── Universal bonuses ─────────────────────────────────────────────────────
    if pz(1) == "Z2"  and cur_t == "T1": bonus = max(bonus, 4)   # +4.6pp
    if pz(1) == "Z1G" and cur_t == "T1": bonus = max(bonus, 4)   # +3.8pp
    if pz(1) == "Z2G" and cur_t == "T9": bonus = max(bonus, 3)   # +3.9pp

    # T9 in Z-sequence context
    if has_t(3) and has_z(2) and has_z(1) and cur_t == "T9":
        bonus = max(bonus, 3)   # TZZT→T9: +3.3pp
    if has_z(3) and has_z(2) and has_z(1) and cur_t == "T9":
        bonus = max(bonus, 3)   # ZZZT→T9: +2.8pp
    elif has_z(2) and has_z(1) and cur_t == "T9":
        bonus = max(bonus, 2)   # ZZT→T9: +2.5pp

    # T1 in Z-sequence context
    if has_t(3) and has_z(2) and has_z(1) and cur_t == "T1":
        bonus = max(bonus, 2)   # TZZT→T1: +2.3pp
    if has_z(3) and has_z(2) and has_z(1) and cur_t == "T1":
        bonus = max(bonus, 2)   # ZZZT→T1: +1.9pp
    elif has_z(2) and has_z(1) and cur_t == "T1":
        bonus = max(bonus, 2)   # ZZT→T1: +1.8pp

    # Recovery patterns
    if has_t(3) and has_z(2) and has_t(1) and cur_t == "T2G":
        bonus = max(bonus, 2)   # TZTT→T2G: +2.1pp
    if has_z(3) and has_z(2) and has_t(1) and cur_t == "T2G":
        bonus = max(bonus, 2)   # ZZTT→T2G: a10=+0.36%

    # ── Universal penalties ───────────────────────────────────────────────────
    if (has_z(2) and has_z(1)) and cur_t == "T4":
        bonus -= 4   # ZZT/ZZZT→T4: −4.7pp / −4.0pp
    if (has_t(3) and has_z(2) and has_z(1)) and cur_t == "T4":
        bonus -= 2   # TZZT→T4: −2.9pp
    if (has_z(3) and has_z(2) and has_z(1)) and cur_t == "T1G":
        bonus -= 3   # ZZZT→T1G: −4.1pp
    elif has_z(2) and has_z(1) and cur_t == "T1G":
        bonus -= 2   # ZZT→T1G: −2.2pp
    if (has_z(3) and has_z(2) and has_t(1)) and cur_t == "T6":
        bonus -= 3   # ZZTT→T6: −4.2pp

    # ── Universe-specific ─────────────────────────────────────────────────────
    if universe == "nasdaq":
        if has_t(2) and has_z(1) and cur_t == "T4":
            bonus -= 2   # TZT→T4 NQ: −2.8pp

    if universe == "sp500":
        if has_t(2) and has_z(1) and cur_t == "T4":
            bonus += 5   # TZT→T4 SP500: 73% win

    return min(8.0, bonus)


# ─── Setup component (0-60) ───────────────────────────────────────────────────

def _calc_beta_setup(row: dict, history: list[dict], universe: str) -> float:
    setup = 0.0

    # 2a. RTB component (cap 25)
    rtb_total = _sf(row, "rtb_total")
    rtb_phase = str(row.get("rtb_phase", "0") or "0")
    rtb_norm  = min(18.0, rtb_total * 0.47)
    phase_bonus = {"C": 7, "B": 4, "A": 2, "D": 0, "0": 0}.get(rtb_phase, 0)
    setup += min(25.0, rtb_norm + phase_bonus)

    # 2b. Profile component (cap 15)
    sweet_spot = _sb(row, "sweet_spot_active")
    btb        = _sb(row, "bear_to_bull_confirmed")
    pf_cat     = str(row.get("profile_category", "") or "")
    pf_score   = _sf(row, "profile_score")

    pf_pts = 0.0
    if sweet_spot:                pf_pts += 8
    elif pf_cat == "BUILDING":    pf_pts += 4
    if btb:                       pf_pts += 4
    if pf_score >= 25:            pf_pts += 3
    elif pf_score >= 18:          pf_pts += 2
    setup += min(15.0, pf_pts)

    # 2c. CLEAN_ENTRY_SCORE (cap 10)
    ces = _sf(row, "CLEAN_ENTRY_SCORE")
    if ces >= 25:   setup += 8
    elif ces >= 15: setup += 5
    elif ces >= 8:  setup += 2

    # 2d. Sequence bonuses (cap 8)
    setup += min(8.0, _calc_sequence_bonus(row, history, universe))

    # 2e. F signal setup (cap 5)
    setup += min(5.0, _calc_f_setup(row))

    # 2f. F1+F10 combo bonus
    if _sb(row, "f1") and _sb(row, "f10"):
        setup += 3

    # 2g. Direct TZ contribution (cap 6, avoids double-count with RTB)
    tz_sig = str(row.get("tz_sig", "") or "")
    tz_w   = _get_tz_w(universe)
    setup += min(6.0, tz_w.get(tz_sig, 0))

    return min(60.0, setup)


# ─── Momentum component (−5 to 50) ───────────────────────────────────────────

def _calc_beta_momentum(row: dict, universe: str) -> float:
    mom = 0.0
    rsi     = _sf(row, "rsi", 50.0)
    has_abs = _sb(row, "d_absorb_bull") or _sb(row, "d_spring")

    # ROCKET_SCORE graduated
    rs = _sf(row, "ROCKET_SCORE")
    if rs >= 33:   mom += 18   # win1d=46.9%, avg5d=+3.39%
    elif rs >= 25: mom += 12   # brk5=22.9%, avg10d=+1.60%
    elif rs >= 14: mom += 6
    elif rs == 8:  mom += 2    # below baseline, minimal
    elif rs == 6:  mom -= 3    # RS=6 alone: win1d=38.1% — penalise

    # be_up conditional
    if _sb(row, "be_up"):
        if rsi <= 70 and has_abs: mom += 10
        elif rsi <= 70:           mom += 6
        elif has_abs:             mom += 5
        else:                     mom += 2

    # FINAL_REGIME bonus (cap 12)
    regime = str(row.get("FINAL_REGIME", "") or "")
    regime_pts = {
        "ROCKET_WATCH":    10,
        "CONFIRMED_BULL":  12,
        "ACTIONABLE_SETUP": 8,
        "CLEAN_ENTRY":      6,
        "SHAKEOUT_ABSORB":  6,
        "REBOUND_SQUEEZE":  3,
        "RISK_REBOUND":     2,
    }.get(regime, 0)
    mom += min(12.0, regime_pts)

    # Volume spike (from VOL string e.g. "20×" "10×" "5×")
    vol_col = str(row.get("VOL", "") or "")
    if "20" in vol_col:   mom += 5
    elif "10" in vol_col: mom += 3
    elif "5" in vol_col:  mom += 1

    # F signal penalties
    mom -= min(8.0, _calc_f_momentum_penalty(row))

    return min(50.0, max(-5.0, mom))


# ─── Non-linear transform ─────────────────────────────────────────────────────

def _beta_transform(raw: float) -> int:
    if raw <= 85:
        return max(0, round(raw))
    elif raw <= 105:
        return round(85 + (raw - 85) * 0.5)    # 85-95 display
    elif raw <= 125:
        return round(95 - (raw - 105) * 1.5)   # 95-65 display
    else:
        return max(30, round(65 - (raw - 125) * 2))


# ─── Zone label ───────────────────────────────────────────────────────────────

def _beta_zone(display: int, row: dict) -> str:
    rtb_phase = str(row.get("rtb_phase", "0") or "0")
    if 85 <= display <= 96:  return "OPTIMAL"
    if 75 <= display < 85:   return "BUY"
    if 60 <= display < 75:   return "WATCH"
    if 40 <= display < 60:   return "BUILDING"
    if display > 96:         return "EXTENDED"
    if display < 40 and rtb_phase == "D": return "SHORT_WATCH"
    return "NEUTRAL"


# ─── Auto-buy gate ────────────────────────────────────────────────────────────

def _beta_auto_buy(display: int, setup: float, momentum: float,
                   row: dict, universe: str) -> bool:
    if not (82 <= display <= 96):                              return False
    if setup < 32:                                             return False
    if not (8 <= momentum <= 28):                              return False
    if max(0, momentum - setup * 0.8) >= 8:                   return False
    if str(row.get("rtb_phase", "0")) not in {"B", "C"}:      return False
    if not _sb(row, "sweet_spot_active"):                      return False
    if str(row.get("profile_category", "")) == "LATE":         return False
    if "BEARISH" in str(row.get("FINAL_REGIME", "")):          return False
    return True


# ─── Empty result ─────────────────────────────────────────────────────────────

def _empty_result() -> dict:
    return {
        "beta_score":    0,
        "beta_raw":      0,
        "beta_setup":    0,
        "beta_momentum": 0,
        "beta_excess":   0,
        "beta_zone":     "NEUTRAL",
        "beta_auto_buy": False,
    }


# ─── Public entry point ───────────────────────────────────────────────────────

def calc_beta_score(row: dict, history: list[dict], universe: str) -> dict:
    """
    row      — current bar dict (must include all signal/score fields).
    history  — list of previous bar dicts, most-recent-first (up to 5 entries).
    universe — "sp500" or "nasdaq".

    Never reads ret_*, mfe_*, mae_* — no lookahead guarantee.
    """
    try:
        # Derive T/Z from tz_sig if caller didn't pre-split them
        tz_sig = str(row.get("tz_sig", "") or "")
        if "T" not in row and "Z" not in row:
            row = dict(row)
            row["T"] = tz_sig if tz_sig.startswith("T") else ""
            row["Z"] = tz_sig if tz_sig.startswith("Z") else ""

        setup    = _calc_beta_setup(row, history, universe)
        momentum = _calc_beta_momentum(row, universe)
        excess   = max(0.0, momentum - setup * 0.8)
        raw      = setup * 1.40 + momentum * 0.85 - excess * 2.5
        display  = _beta_transform(raw)
        zone     = _beta_zone(display, row)
        auto_buy = _beta_auto_buy(display, setup, momentum, row, universe)

        return {
            "beta_score":    display,
            "beta_raw":      round(raw),
            "beta_setup":    round(setup),
            "beta_momentum": round(momentum),
            "beta_excess":   round(excess),
            "beta_zone":     zone,
            "beta_auto_buy": auto_buy,
        }
    except Exception:
        return _empty_result()
