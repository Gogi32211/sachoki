"""ABR (A/B/B+/R) classification overlay for TZ Signal Intelligence.

Read-only: does NOT alter role, score, or any existing TZ field.
SP500 rules apply to sp500; NASDAQ rules apply to nasdaq_gt5 only.
"""
from __future__ import annotations
import csv
import os
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

# ── Universe mapping ──────────────────────────────────────────────────────────

ABR_UNIVERSE_MAP: Dict[str, str] = {
    "sp500":      "SP500",
    "nasdaq_gt5": "NASDAQ",
}
ABR_SUPPORTED = frozenset(ABR_UNIVERSE_MAP)

# ── Quality thresholds (med10d_pct) ───────────────────────────────────────────

_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "SP500":   {"STRONG": 0.8,  "GOOD": 0.3,  "AVERAGE": 0.0,  "REJECT": -1e9},
    "NASDAQ":  {"STRONG": 0.4,  "GOOD": 0.1,  "AVERAGE": -0.1, "REJECT": -1e9},
}

# Gate: minimum quality of prev1 to qualify for A/B/B+
_GATE: Dict[str, str] = {
    "SP500":  "STRONG",
    "NASDAQ": "GOOD",   # GOOD or STRONG
}

# ── ABR DB path discovery ─────────────────────────────────────────────────────

def _find_abr_csv() -> str:
    base = os.path.dirname(__file__)
    return os.path.join(base, "ABR_rule_database.csv")


# ── ABR DB loading ────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_abr_db() -> Dict[Tuple[str, str, str, str], dict]:
    """Return dict keyed by (universe, signal, sequence, category)."""
    path = _find_abr_csv()
    index: Dict[Tuple[str, str, str, str], dict] = {}
    if not os.path.exists(path):
        return index
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            cat = row.get("category", "").strip()
            if cat in ("BASELINE", "SEQ_BASELINE"):
                continue
            key = (
                row.get("universe", "").strip(),
                row.get("signal",   "").strip(),
                row.get("sequence", "").strip(),
                cat,
            )
            index[key] = row
    return index


# ── Quality classification ────────────────────────────────────────────────────

def _classify_quality(med: Optional[float], abr_universe: str) -> str:
    if med is None:
        return "UNKNOWN"
    thr = _THRESHOLDS.get(abr_universe, _THRESHOLDS["SP500"])
    if med >= thr["STRONG"]:
        return "STRONG"
    if med >= thr["GOOD"]:
        return "GOOD"
    if med >= thr["AVERAGE"]:
        return "AVERAGE"
    return "REJECT"


def _composite_med(lane3_label: str, matrix, scan_universe: str) -> Optional[float]:
    """Weighted average med10d_pct from positive composite rules for this pattern."""
    if not lane3_label:
        return None
    allowed = matrix.allowed_univs(scan_universe)
    total_n = 0.0
    weighted = 0.0
    found = False
    for univ in allowed:
        for rule in matrix.composite.get(univ, {}).get(lane3_label, []):
            try:
                n   = float(rule.get("n") or 0)
                med = float(rule.get("med10d_pct") or 0)
            except (TypeError, ValueError):
                continue
            if n > 0:
                weighted += med * n
                total_n  += n
                found = True
    if not found:
        # fall back to reject composite
        for univ in allowed:
            for rule in matrix.reject_composite.get(univ, {}).get(lane3_label, []):
                try:
                    n   = float(rule.get("n") or 0)
                    med = float(rule.get("med10d_pct") or 0)
                except (TypeError, ValueError):
                    continue
                if n > 0:
                    weighted += med * n
                    total_n  += n
                    found = True
    if not found or total_n == 0:
        return None
    return weighted / total_n


# ── ABR category logic ────────────────────────────────────────────────────────

def _gate_passes(prev1_quality: str, abr_universe: str) -> bool:
    gate = _GATE.get(abr_universe, "STRONG")
    if gate == "STRONG":
        return prev1_quality == "STRONG"
    # NASDAQ gate: GOOD or STRONG
    return prev1_quality in ("GOOD", "STRONG")


def _abr_category(prev1_quality: str, prev2_quality: str, abr_universe: str) -> str:
    if prev1_quality == "UNKNOWN" or prev2_quality == "UNKNOWN":
        return "UNKNOWN"
    if not _gate_passes(prev1_quality, abr_universe):
        return "R"
    if prev2_quality == "STRONG":
        return "B+"
    if prev2_quality == "GOOD":
        return "B"
    if prev2_quality in ("AVERAGE",):
        return "A"
    return "R"   # prev2 = REJECT


# ── ABR role suggestion (non-binding) ─────────────────────────────────────────

def _role_suggestion(category: str, current_role: str) -> str:
    if category == "A":
        return "BULL_CONTINUATION_CANDIDATE"
    if category == "B":
        return "MOMENTUM_CONTINUATION_CONTEXT"
    if category == "B+":
        return "MOMENTUM_CONTINUATION_CONTEXT"
    if category == "R":
        if "SHORT" in current_role:
            return "CHECK_SHORT_CONFLICT"
        return "REJECT_LONG_OR_SHORT_WATCH_IF_NEGATIVE"
    return ""


def _action_hint(category: str) -> str:
    return {
        "A":  "PRIMARY_LONG_CONTEXT",
        "B":  "SECONDARY_LONG_CONTEXT",
        "B+": "MOMENTUM_CONTINUATION_CONTEXT",
        "R":  "DO_NOT_BUY_OR_SHORT_WATCH_IF_NEGATIVE",
    }.get(category, "NO_ABR_EDGE")


# ── Main classifier ───────────────────────────────────────────────────────────

_EMPTY: dict = {
    "abr_category":       "UNKNOWN",
    "abr_sequence":       "",
    "abr_prev1_composite": "",
    "abr_prev2_composite": "",
    "abr_prev1_quality":  "UNKNOWN",
    "abr_prev2_quality":  "UNKNOWN",
    "abr_gate_pass":      False,
    "abr_rule_found":     False,
    "abr_n":              None,
    "abr_med10d_pct":     None,
    "abr_avg10d_pct":     None,
    "abr_fail10d_pct":    None,
    "abr_win10d_pct":     None,
    "abr_action_hint":    "NO_ABR_EDGE",
    "abr_role_suggestion": "",
}


def classify_abr(
    final_signal: str,
    seq4_str: str,
    history_rows: List[dict],
    matrix,
    scan_universe: str,
    current_role: str = "",
) -> dict:
    """Classify a bar's ABR category.

    Args:
        final_signal: signal on the current bar (e.g. "T3", "Z2G")
        seq4_str: "|"-joined 4-bar sequence string (prev3|prev2|prev1|current)
        history_rows: list of previous bar dicts (oldest first), each with lane3_label
        matrix: MatrixIndex instance
        scan_universe: scan universe key (e.g. "sp500", "nasdaq_gt5")
        current_role: TZ role already assigned (for role_suggestion only)
    """
    if scan_universe not in ABR_SUPPORTED or not final_signal:
        return dict(_EMPTY)

    abr_universe = ABR_UNIVERSE_MAP[scan_universe]

    # ABR sequence = first 3 parts of seq4_str (prev3|prev2|prev1)
    parts = seq4_str.split("|") if seq4_str else []
    abr_seq = "|".join(parts[:3]) if len(parts) >= 3 else ""

    # prev1 = parts[2], prev2 = parts[1] (positions in seq4_str)
    prev1_label = parts[2] if len(parts) > 2 else ""
    prev2_label = parts[1] if len(parts) > 1 else ""

    # Also get lane3_label from history rows as fallback/cross-check
    # history_rows is oldest-first; prev1 = history_rows[-1], prev2 = history_rows[-2]
    if not prev1_label and history_rows:
        prev1_label = history_rows[-1].get("lane3_label", "") or ""
    if not prev2_label and len(history_rows) >= 2:
        prev2_label = history_rows[-2].get("lane3_label", "") or ""

    prev1_med = _composite_med(prev1_label, matrix, scan_universe)
    prev2_med = _composite_med(prev2_label, matrix, scan_universe)

    prev1_quality = _classify_quality(prev1_med, abr_universe)
    prev2_quality = _classify_quality(prev2_med, abr_universe)

    gate_pass = _gate_passes(prev1_quality, abr_universe)
    category  = _abr_category(prev1_quality, prev2_quality, abr_universe)

    # Look up rule in ABR DB
    db = _load_abr_db()
    rule = db.get((abr_universe, final_signal, abr_seq, category))
    rule_found = rule is not None

    def _f(key: str) -> Optional[float]:
        if not rule:
            return None
        v = rule.get(key, "")
        try:
            return float(v) if v not in ("", None) else None
        except (TypeError, ValueError):
            return None

    return {
        "abr_category":        category,
        "abr_sequence":        abr_seq,
        "abr_prev1_composite": prev1_label,
        "abr_prev2_composite": prev2_label,
        "abr_prev1_quality":   prev1_quality,
        "abr_prev2_quality":   prev2_quality,
        "abr_gate_pass":       gate_pass,
        "abr_rule_found":      rule_found,
        "abr_n":               _f("n"),
        "abr_med10d_pct":      _f("med"),
        "abr_avg10d_pct":      _f("avg"),
        "abr_fail10d_pct":     _f("fail"),
        "abr_win10d_pct":      _f("win"),
        "abr_action_hint":     _action_hint(category),
        "abr_role_suggestion": _role_suggestion(category, current_role),
    }
