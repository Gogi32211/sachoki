"""
buy_score.py — the screener's FINAL BUY score (0-100), shared helper.

Built 2026-07-03 from the backtest-expert scoring analysis (validate_score_shapes /
validate_score_rsi_2d / validate_buyscore_pathsim / validate_buyscore_weights):
  · RSI position is the strongest, era-robust forward axis (monotone, spread 1.36pp,
    RSI<35 the only all-era-positive zone; RSI≥60 a whipsaw graveyard TR−3.4).
  · prebreak_v2 is the ONLY stored score additive beyond RSI (corr −0.30, base-building);
    its own >27 "HOT" band is a median-negative lottery → contribution SATURATES at 27.
  · vol=B is the strongest path-sim booster (+1.57→+2.52 in the oversold cell).
  · legacy turbo/ultra/beta/v3/rtb add nothing beyond RSI (extension-chasing counters).

Formula (weights sit on a validated PLATEAU — 3×3 grid mono r=+0.93..0.95 everywhere):
    raw  = 1.5·min(pbv2, 27) + 0.9·max(0, 55−RSI) + 12·(vol=B)      # ~0..77
    score = clip(raw × 1.3, 0, 100)
    VETO  RSI≥60 → cap 20, tag EXTENDED   (chasing strength — worst forward zone)
    GUARD RSI<28 → cap 60, tag KNIFE      (falling knife — extreme oversold)

Path-sim ladder (trail25/60, gap-realistic, 62mo, dv≥3M): perfectly monotone —
0-25 +0.59 → 25-50 +0.92 → 50-70 +1.14 → 70-85 +1.17 → 85+ +1.95/PF1.23/TR+0.21.
NOTE: this is a RANKING/venue score (where to look), not a standalone entry edge —
the validated Edge setups (GEM1, L43-TRIPLE, …) remain the trade triggers.
"""
from __future__ import annotations


def compute_buy_score(pbv2, rsi_14, vol_bucket) -> dict:
    """Pure function: (prebreak_v2, rsi_14, vol_bucket) → {'buy_score': int, 'buy_tag': str}."""
    try:
        pb = float(pbv2) if pbv2 is not None else 0.0
    except (TypeError, ValueError):
        pb = 0.0
    try:
        rsi = float(rsi_14)
    except (TypeError, ValueError):
        rsi = None
    vb = str(vol_bucket or "").upper() == "B"

    raw = 1.5 * min(max(pb, 0.0), 27.0) + 12.0 * vb
    if rsi is not None:
        raw += 0.9 * max(0.0, 55.0 - rsi)
    score = max(0.0, min(100.0, raw * 1.3))
    tag = ""
    if rsi is not None and rsi >= 60:
        score, tag = min(score, 20.0), "EXTENDED"
    elif rsi is not None and rsi < 28:
        score, tag = min(score, 60.0), "KNIFE"
    return {"buy_score": int(round(score)), "buy_tag": tag}
