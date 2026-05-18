"""Statistical quality labeling for TZ+WLNBB composites, sequences, and signals."""
from __future__ import annotations
from typing import Optional

# ── Sample size thresholds ────────────────────────────────────────────────────
SAMPLE_ROBUST           = 100
SAMPLE_USABLE           = 50
SAMPLE_DIRECTIONAL_ONLY = 20


def compute_sample_confidence(count: int) -> str:
    """Return sample-size confidence tier."""
    if count >= SAMPLE_ROBUST:            return "ROBUST"
    if count >= SAMPLE_USABLE:            return "USABLE"
    if count >= SAMPLE_DIRECTIONAL_ONLY:  return "DIRECTIONAL_ONLY"
    return "LOW_SAMPLE"


def compute_stat_status(count: int,
                        median_10d: Optional[float],
                        fail_rate: Optional[float]) -> str:
    """Return statistical quality label based on thresholds.

    STRONG  : median_10d ≥ 1.0%, fail ≤ 20%, n ≥ 50
    GOOD    : median_10d ≥ 0.5%, fail ≤ 25%, n ≥ 50
    AVERAGE : median_10d ≥ 0.0%, fail ≤ 30%, n ≥ 30
    WEAK    : median between -0.25% and 0%, or fail > 30%
    REJECT  : median < -0.25% or fail ≥ 35%
    LOW_SAMPLE / UNKNOWN when data is missing.
    """
    if count < SAMPLE_DIRECTIONAL_ONLY:
        return "LOW_SAMPLE"
    if median_10d is None or fail_rate is None:
        return "UNKNOWN"
    if median_10d >= 1.0 and fail_rate <= 20.0 and count >= 50:
        return "STRONG"
    if median_10d >= 0.5 and fail_rate <= 25.0 and count >= 50:
        return "GOOD"
    if median_10d >= 0.0 and fail_rate <= 30.0 and count >= 30:
        return "AVERAGE"
    if median_10d < -0.25 or fail_rate >= 35.0:
        return "REJECT"
    return "WEAK"


def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _safe_int(v) -> Optional[int]:
    try:
        return int(float(v)) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def stat_status_from_rule(rule: dict) -> tuple[str, str]:
    """Return (stat_status, sample_confidence) from a matrix rule dict."""
    n    = _safe_int(rule.get("n"))          or 0
    med  = _safe_float(rule.get("med10d_pct"))
    fail = _safe_float(rule.get("fail10d_pct"))
    return compute_stat_status(n, med, fail), compute_sample_confidence(n)
