"""
Sachoki profile playbook layer.

This module adds universe-specific and price-bucket-specific context on top of
the canonical scoring engine.

It must NOT replace FINAL_BULL_SCORE or any canonical score.
It must NOT mutate canonical score columns.
It only adds profile/playbook fields:
    profile_name, profile_score, profile_category, sweet_spot_active,
    late_warning, matched_profile_signals, matched_profile_pairs,
    profile_role, profile_description, profile_experimental,
    profile_preferred_preset, profile_suggested_tp, profile_suggested_sl,
    profile_max_hold
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional, Set, Tuple, List

# ── Signal aliases ────────────────────────────────────────────────────────────
SIGNAL_ALIASES: Dict[str, str] = {
    "VBO↑": "VBO_UP", "VBOUP": "VBO_UP", "VBO_UP": "VBO_UP",
    "VBO↓": "VBO_DN", "VBODN": "VBO_DN", "VBO_DN": "VBO_DN",
    "HILO↑": "HILO_UP", "HILO_UP": "HILO_UP",
    "HILO↓": "HILO_DN", "HILO_DN": "HILO_DN",
    "BB↑": "BB_UP", "BB_UP": "BB_UP",
    "BX↑": "BX_UP", "BX_UP": "BX_UP",
    "BX↓": "BX_DN", "BX_DN": "BX_DN",
    "BO↑": "BO_UP", "BO_UP": "BO_UP",
    "BO↓": "BO_DN", "BO_DN": "BO_DN",
    "BE↑": "BE_UP", "BE_UP": "BE_UP",
    "BE↓": "BE_DN", "BE_DN": "BE_DN",
    "EB↑": "EB_UP", "EB_UP": "EB_UP",
    "EB↓": "EB_DN", "EB_DN": "EB_DN",
    "FBO↑": "FBO_UP", "FBO_UP": "FBO_UP",
    "FBO↓": "FBO_DN", "FBO_DN": "FBO_DN",
    "4BF↓": "4BF_DN", "4BF_DN": "4BF_DN",
    "BEST★": "BEST_STAR", "BEST*": "BEST_STAR", "BEST_STAR": "BEST_STAR",
    "5×": "5X", "10×": "10X", "20×": "20X",
    "↓BIAS": "BIAS_DN", "BIAS↓": "BIAS_DN", "BIAS_DN": "BIAS_DN",
    "↑BIAS": "BIAS_UP", "BIAS↑": "BIAS_UP", "BIAS_UP": "BIAS_UP",
    "CONS": "CONSO", "CONSO": "CONSO",
}

# ── Profile definitions ───────────────────────────────────────────────────────
PROFILES: Dict[str, dict] = {
    "SP500_LT20": {
        "universe": "sp500",
        "price_range": (0.0, 20.0),
        "description": "High-volatility S&P low-price bucket. More breakout potential but high fail/drawdown.",
        "role": "watch/high-risk",
        "preferred_preset": "STRUCTURAL_BUILD",
        "suggested_tp": 0.05,
        "suggested_sl": 0.05,
        "max_hold": 20,
        "signal_weights": {
            "BUY": 4, "SVS": 4, "BX_UP": 4, "F9": 3, "EB_DN": 3,
            "BB_UP": 3, "FRI43": 3, "ABS": 3, "LOAD": 3, "RTV": 3,
            "VBO_UP": 3, "VBO_DN": 3,
        },
        "pair_bonuses": {
            ("BUY", "Z3"): 6, ("LOAD", "T10"): 6,
            ("SVS", "T10"): 6, ("FRI43", "T10"): 6,
        },
        "sweet_spot": (18, 38),
        "late_threshold": 48,
    },
    "SP500_20_50": {
        "universe": "sp500",
        "price_range": (20.0, 50.0),
        "description": "Small-price structural S&P bucket. Good breakout potential, still noisy.",
        "role": "structural-watch-or-entry",
        "preferred_preset": "STRUCTURAL_BUILD",
        "suggested_tp": 0.05,
        "suggested_sl": 0.05,
        "max_hold": 20,
        "signal_weights": {
            "BUY": 4, "SVS": 4, "260308": 4, "BX_UP": 4,
            "F9": 3, "EB_DN": 3, "BB_UP": 3, "FRI43": 3,
            "ABS": 3, "LOAD": 3, "RTV": 3, "VBO_UP": 3, "VBO_DN": 3,
            "CCI": 3, "G11": 3,
        },
        "pair_bonuses": {
            ("BL", "F10"): 10, ("CCI", "G11"): 9, ("CCI", "T5"): 8,
            ("260308", "F3"): 8, ("260308", "F4"): 8, ("B1", "CLM"): 8,
            ("LOAD", "T10"): 7, ("SVS", "T10"): 7, ("FRI43", "T10"): 7,
            ("G1", "SVS"): 7, ("G4", "LOAD"): 7,
        },
        "sweet_spot": (20, 42),
        "late_threshold": 52,
    },
    "SP500_50_150": {
        "universe": "sp500",
        "price_range": (50.0, 150.0),
        "description": "Best balanced actionable S&P bucket.",
        "role": "best-balanced-actionable",
        "preferred_preset": "STRUCTURAL_BUILD",
        "suggested_tp": 0.05,
        "suggested_sl": 0.05,
        "max_hold": 20,
        "signal_weights": {
            "FLY_BD": 5, "BB_UP": 5, "FRI43": 4, "BUY": 4, "SVS": 4,
            "260308": 4, "BX_UP": 4, "F9": 3, "EB_DN": 3, "SQ": 3,
            "SC": 3, "G11": 3, "ABS": 3, "LOAD": 3,
        },
        "pair_bonuses": {
            ("BL", "F10"): 10, ("CCI", "G11"): 9, ("CCI", "T5"): 8,
            ("260308", "F3"): 8, ("260308", "F4"): 8, ("B1", "CLM"): 8,
            ("CCI", "HILO_DN"): 7, ("LOAD", "T10"): 7, ("SVS", "T10"): 7,
            ("FRI43", "T10"): 7, ("G1", "SVS"): 7, ("G4", "LOAD"): 7,
        },
        "sweet_spot": (20, 45),
        "late_threshold": 55,
    },
    "SP500_150_300": {
        "universe": "sp500",
        "price_range": (150.0, 300.0),
        "description": "High-quality setup only. Works with strong clean/shakeout setups.",
        "role": "quality-setup-only",
        "preferred_preset": "STRUCTURAL_BUILD",
        "suggested_tp": 0.05,
        "suggested_sl": 0.05,
        "max_hold": 20,
        "signal_weights": {
            "FLY_BD": 5, "BB_UP": 5, "BUY": 4, "SVS": 4, "BX_UP": 4,
            "FRI43": 4, "F9": 3, "EB_DN": 3, "ABS": 3, "LOAD": 3,
            "SHAKEOUT_ABSORB": 4,
        },
        "pair_bonuses": {
            ("BL", "F10"): 10, ("CCI", "G11"): 9, ("CCI", "T5"): 8,
            ("LOAD", "T10"): 7, ("SVS", "T10"): 7,
            ("FRI43", "T10"): 7, ("G1", "SVS"): 7,
        },
        "sweet_spot": (20, 45),
        "late_threshold": 55,
    },
    "SP500_300_PLUS": {
        "universe": "sp500",
        "price_range": (300.0, float("inf")),
        "description": "High-price caution bucket. Current score/TP-SL edge is weak; combo-only.",
        "role": "combo-only-caution",
        "preferred_preset": "STRUCTURAL_BUILD",
        "suggested_tp": 0.05,
        "suggested_sl": 0.05,
        "max_hold": 20,
        "signal_weights": {
            "BUY": 2, "SVS": 2, "BX_UP": 2, "BB_UP": 2,
            "FRI43": 2, "ABS": 2, "LOAD": 2,
        },
        "pair_bonuses": {
            ("BL", "F10"): 10, ("CCI", "G11"): 9,
            ("LOAD", "T10"): 8, ("SVS", "T10"): 8, ("FRI43", "T10"): 8,
        },
        "sweet_spot": (15, 30),
        "late_threshold": 38,
    },
    "NASDAQ_PENNY": {
        "universe": "nasdaq",
        "price_range": (0.0, 5.0),
        "description": "NASDAQ penny/micro-cap experimental profile. High volatility; combo/momentum only.",
        "role": "experimental-high-risk",
        "experimental": True,
        "preferred_preset": "WIDE_MOMENTUM",
        "suggested_tp": 0.15,
        "suggested_sl": 0.05,
        "max_hold": 20,
        "signal_weights": {
            "20X": 6, "10X": 5, "5X": 4, "CONSO": 4, "SC": 4,
            "T11": 3, "Z7": 3, "BX_DN": 3, "FRI64": 2,
        },
        "pair_bonuses": {
            ("SC", "T1"): 10, ("5X", "CONSO"): 8, ("BIAS_DN", "VBO_UP"): 8,
        },
        "sweet_spot": (18, 38),
        "late_threshold": 48,
    },
    "NASDAQ_REAL": {
        "universe": "nasdaq",
        "price_range": (5.0, float("inf")),
        "description": "NASDAQ real-company experimental profile. Combo-first; score is context only.",
        "role": "experimental-combo-first",
        "experimental": True,
        "preferred_preset": "STRUCTURAL_BUILD",
        "suggested_tp": 0.05,
        "suggested_sl": 0.05,
        "max_hold": 20,
        "signal_weights": {
            "T11": 3, "BX_DN": 3, "B11": 3, "CONSO": 2, "L43": 2,
            "G4": 2, "BX_UP": 2, "G11": 2, "Z2G": 2, "F9": 2,
            "T3": 2, "L64": 3, "HILO_UP": 3,
        },
        "pair_bonuses": {
            ("B10", "BEST_STAR"): 10, ("B2", "RL"): 8, ("BUY", "Z3"): 8,
            ("BB_UP", "FRI34"): 8, ("BX_UP", "F5"): 8, ("BX_DN", "L555"): 8,
            ("5X", "Z3"): 7, ("CCIB", "UM"): 7, ("STRONG", "T11"): 7,
            ("B11", "T3"): 7, ("T11", "WC_UP"): 7,
        },
        "sweet_spot": (18, 40),
        "late_threshold": 50,
    },
}

# ── Turbo scan column → canonical signal name ─────────────────────────────────
# Maps boolean columns in turbo_scan_results to the signal name strings
# used in profile signal_weights / pair_bonuses.
_TURBO_SIGNAL_MAP: Dict[str, str] = {
    "buy_2809": "BUY",
    "svs_2809": "SVS",
    "um_2809":  "UM",
    "conso_2809": "CONSO",
    "bx_up": "BX_UP", "bx_dn": "BX_DN",
    "bo_up": "BO_UP", "bo_dn": "BO_DN",
    "be_up": "BE_UP", "be_dn": "BE_DN",
    "fri34": "FRI34", "fri43": "FRI43", "fri64": "FRI64",
    "l22": "L22", "l34": "L34", "l43": "L43", "l64": "L64", "l555": "L555",
    "blue": "BL",
    "cci_ready": "CCI",
    "cci_blue_turn": "CCIB",
    "abs_sig": "ABS",
    "climb_sig": "CLM",
    "load_sig": "LOAD",
    "best_sig": "BEST_STAR",
    "strong_sig": "STRONG",
    "vbo_up": "VBO_UP", "vbo_dn": "VBO_DN",
    "vol_spike_5x": "5X", "vol_spike_10x": "10X", "vol_spike_20x": "20X",
    "fly_bd": "FLY_BD", "fly_abcd": "FLY_ABCD",
    "fly_cd": "FLY_CD", "fly_ad": "FLY_AD",
    "eb_bull": "EB_UP", "eb_bear": "EB_DN",
    "fbo_bull": "FBO_UP", "fbo_bear": "FBO_DN",
    "bf_buy": "BF_BUY", "bf_sell": "4BF_DN",
    "hilo_buy": "HILO_UP", "hilo_sell": "HILO_DN",
    "sc": "SC", "bc": "BC", "sq": "SQ", "ns": "NS", "nd": "ND",
    "rtv": "RTV", "rocket": "ROCKET", "sig3g": "3G",
    "sig_260308": "260308", "sig_l88": "L88",
    "g1": "G1", "g2": "G2", "g4": "G4", "g6": "G6", "g11": "G11",
    "b1": "B1", "b2": "B2", "b3": "B3", "b4": "B4", "b5": "B5",
    "b6": "B6", "b7": "B7", "b8": "B8", "b9": "B9", "b10": "B10", "b11": "B11",
    "f1": "F1", "f2": "F2", "f3": "F3", "f4": "F4", "f5": "F5",
    "f6": "F6", "f7": "F7", "f8": "F8", "f9": "F9", "f10": "F10", "f11": "F11",
    "bias_up": "BIAS_UP", "bias_down": "BIAS_DN",
    "bb_brk": "BB_UP",
    "wick_bull": "WC_UP", "wick_bear": "WC_DN",
    "rs": "RS", "rs_strong": "RS_STRONG",
    "va": "VA",
    "para_prep": "PARA_PREP", "para_start": "PARA_START",
    "para_plus": "PARA_PLUS", "para_retest": "PARA_RETEST",
    "fuchsia_rl": "RL", "fuchsia_rh": "RH",
    "pre_pump": "PRE_PUMP",
    "atr_brk": "ATR_BRK",
    "bias_up": "BIAS_UP",
}


# ── Signal token parsing (for string-column sources like stock_stat CSV) ──────

def normalize_signal_token(token: str) -> str:
    if token is None:
        return ""
    t = str(token).strip()
    if not t:
        return ""
    t = t.replace(",", " ").replace("|", " ").strip()
    if t in SIGNAL_ALIASES:
        return SIGNAL_ALIASES[t]
    return t


def parse_signal_cell(value: Any) -> Set[str]:
    """Parse a signal cell (string) into a set of normalized tokens.

    Handles whitespace, comma, pipe and semicolon separators.
    """
    if value is None:
        return set()
    if not isinstance(value, str):
        return set()
    raw = value.strip()
    if not raw:
        return set()
    parts = re.split(r"[\s,|;]+", raw)
    out: Set[str] = set()
    for p in parts:
        n = normalize_signal_token(p)
        if n:
            out.add(n)
    return out


# ── Signal extraction from turbo scan row ─────────────────────────────────────

def extract_signals_from_turbo_row(row: dict) -> Set[str]:
    """Convert turbo_scan_results boolean columns to a set of signal name strings."""
    signals: Set[str] = set()

    for col, name in _TURBO_SIGNAL_MAP.items():
        v = row.get(col)
        if v and v != 0:
            signals.add(name)

    # tz_sig is a string like "T10", "Z3", "T11G" — add it directly
    tz = (row.get("tz_sig") or "").strip().upper()
    if tz:
        signals.add(tz)
        # Also add the base letter (T10 → T10 already, but "T1G" → add T1)
        base = re.sub(r"G$", "", tz)
        if base and base != tz:
            signals.add(base)

    return signals


def get_signals_5bar(rows_for_ticker: List[dict]) -> Set[str]:
    """Union of normalized signal tokens across the last 6 rows (current + 5 prior).

    Input: list of row dicts for a single ticker, sorted by date ascending.
    Uses extract_signals_from_turbo_row per row.
    Also parses string signal columns (T, Z, L, F, B, Combo, etc.) if present.
    """
    STRING_SIG_COLS = ["T", "Z", "L", "F", "B", "Combo", "ULT", "VOL", "VABS", "G", "FLY", "WICK"]
    window = rows_for_ticker[-6:]  # current bar + up to 5 prior
    signals: Set[str] = set()
    for row in window:
        signals |= extract_signals_from_turbo_row(row)
        for col in STRING_SIG_COLS:
            if col in row:
                signals |= parse_signal_cell(row.get(col))
    return signals


# ── Profile selection ─────────────────────────────────────────────────────────

def get_profile(row: dict, universe: str) -> str:
    """Assign profile based on universe and current row close (or last_price).

    Uses current close as primary.  median_price is fallback only.
    """
    price = (
        row.get("close") or row.get("Close")
        or row.get("last_price") or row.get("Last_Price")
        or row.get("median_price") or 50.0
    )
    try:
        price = float(price)
    except (TypeError, ValueError):
        price = 50.0

    uni = (universe or "").lower()

    if uni == "sp500":
        if price < 20:
            return "SP500_LT20"
        if price < 50:
            return "SP500_20_50"
        if price < 150:
            return "SP500_50_150"
        if price < 300:
            return "SP500_150_300"
        return "SP500_300_PLUS"

    # nasdaq, russell2k, all_us, split — experimental only
    if price < 5:
        return "NASDAQ_PENNY"
    return "NASDAQ_REAL"


# ── Profile score computation ─────────────────────────────────────────────────

def compute_profile_score(signals_5bar: Set[str], profile_name: str) -> dict:
    """Compute profile score from a set of active signal names.

    Does NOT modify canonical score columns.
    """
    profile = PROFILES.get(profile_name)
    if not profile:
        return {
            "profile_name": profile_name,
            "profile_score": 0,
            "profile_category": "WATCH",
            "sweet_spot_active": False,
            "late_warning": False,
            "profile_role": None,
            "profile_description": None,
            "profile_experimental": False,
            "profile_preferred_preset": None,
            "profile_suggested_tp": None,
            "profile_suggested_sl": None,
            "profile_max_hold": None,
            "matched_profile_signals": [],
            "matched_profile_pairs": [],
        }

    score = 0
    matched_signals: List[str] = []

    for sig, weight in profile["signal_weights"].items():
        if sig in signals_5bar:
            score += int(weight)
            matched_signals.append(sig)

    matched_pairs: List[str] = []
    for pair, bonus in profile.get("pair_bonuses", {}).items():
        a, b = pair
        if a in signals_5bar and b in signals_5bar:
            score += int(bonus)
            matched_pairs.append(f"{a}+{b}")

    sweet_low, sweet_high = profile["sweet_spot"]
    late_threshold = profile["late_threshold"]

    sweet_spot_active = sweet_low <= score <= sweet_high
    late_warning = score > late_threshold

    if late_warning:
        category = "LATE"
    elif sweet_spot_active:
        category = "SWEET_SPOT"
    elif score >= sweet_low * 0.70:
        category = "BUILDING"
    else:
        category = "WATCH"

    return {
        "profile_name":            profile_name,
        "profile_score":           int(score),
        "profile_category":        category,
        "sweet_spot_active":       bool(sweet_spot_active),
        "late_warning":            bool(late_warning),
        "profile_role":            profile.get("role"),
        "profile_description":     profile.get("description"),
        "profile_experimental":    bool(profile.get("experimental", False)),
        "profile_preferred_preset": profile.get("preferred_preset"),
        "profile_suggested_tp":    profile.get("suggested_tp"),
        "profile_suggested_sl":    profile.get("suggested_sl"),
        "profile_max_hold":        profile.get("max_hold"),
        "matched_profile_signals": matched_signals,
        "matched_profile_pairs":   matched_pairs,
    }


# ── Row enrichment ────────────────────────────────────────────────────────────

_CANONICAL_FIELDS = frozenset({
    "FINAL_BULL_SCORE", "TURBO_SCORE", "turbo_score",
    "SIGNAL_SCORE", "signal_score", "RTB_SCORE", "rtb_total",
    "FINAL_REGIME", "final_regime",
    "HARD_BEAR_SCORE", "ROCKET_SCORE", "GOG_SCORE",
})


def enrich_row_with_profile(
    row: dict,
    universe: str,
    signals: Optional[Set[str]] = None,
) -> dict:
    """Add profile playbook fields to a row dict.

    Does NOT overwrite any canonical score columns.
    If signals is None, extracts from turbo boolean columns automatically.
    """
    if signals is None:
        signals = extract_signals_from_turbo_row(row)

    profile_name = get_profile(row, universe)
    result = compute_profile_score(signals, profile_name)

    out = dict(row)
    for k, v in result.items():
        if k not in _CANONICAL_FIELDS:
            out[k] = v

    return out
