"""
prebreak_v2.py — data-derived, OOS-validated PreBreakout score.

WHY THIS EXISTS
  The legacy turbo / ultra / beta scores were shown (empirically, on held-out
  data) to be ANTI-predictive for forward outcome: higher turbo → LOWER forward
  breakout rate (OOS decile monotonicity r = -0.60) and ~2x LOWER max-favourable
  excursion. They reward "many bullish signals fired" = already extended.

  prebreak_v2 is a logistic model fit on 2.31M nasdaq bars (train < 2025-06-01)
  and validated out-of-sample:
     OOS nasdaq : top-decile breakout-lift 1.47x, monotonicity r = +0.94
     OOS sp500  : top-decile breakout-lift 1.84x, r = +0.60   (cross-universe —
                  trained ONLY on nasdaq, so this confirms it is not overfit)

  It rewards accumulation / volume / weakness (wyc_in_tr, pb_stop_cause,
  vol_5x, bias_dn, climax) and penalises consolidation / extension
  (sig_conso is the single largest penalty, -0.84; price_gt_200 -0.32) — the
  OPPOSITE of the legacy additive scores.

TARGET LABEL it predicts
  BREAKOUT := mfe_20d >= 20%  AND  fwd_10d >= 0     (popped 20%+ and held)

NO LOOK-AHEAD
  Reads only point-in-time signal flags. NEVER reads forward returns or
  Williams pivot / swing fields (is_pivot_*, next_pivot_*, swing_type) — those
  are confirmed only by future bars and leak (they topped the raw-lift charts
  precisely because they are look-ahead).

OUTPUT  (compute_prebreak_v2)
  prebreak_v2        int 0..~45  — calibrated breakout probability ×100
  prebreak_v2_band   WATCH | BUY | HOT
      <15  WATCH  — below-baseline breakout odds, avoid
      15-27 BUY   — sweet spot: highest breakout rate WITH positive mean fwd_20d
      >27  HOT    — highest breakout rate & MFE but NEGATIVE mean fwd_20d
                    (lottery / overbought — only with an asymmetric exit).
  prebreak_v2_prob   float — the raw probability.

  NOTE: even the HOT band is a *screen*, not a buy-and-hold signal — the high-MFE
  names still bleed on a fixed-horizon hold (median negative). Pair with a
  target/stop exit. See the OOS analysis that produced this model.
"""
from __future__ import annotations

import math

SCORING_ENGINE = "prebreak_v2"
SCORING_VERSION = "2.0-nasdaq-20260531"

# ── Baked model (logistic regression, full-train refit) ──────────────────────
# (feature, weight). Binary features are 0/1; continuous are standardised.
_TERMS = [
    ("pb_stop_cause",    0.20371),
    ("sig_bias_dn",     -0.01252),
    ("rsi_le_35",       -0.02860),
    ("pb_macro_penalty",-0.02519),
    ("wyc_in_tr",        0.17957),
    ("vix_range",       -0.05442),
    ("sig_bc",           0.11131),
    ("sig_sc",           0.07069),
    ("sig_l555",         0.01775),
    ("sig_rl",          -0.00955),
    ("sig_z2",           0.06552),
    ("sig_z2g",          0.03815),
    ("sig_dd_dn_green",  0.02764),
    ("d_absorb_bear",    0.10810),
    ("d_strong_bear",    0.07289),
    ("sig_vol_5x",       0.12730),
    ("sig_vol_10x",      0.05795),
    ("sig_vol_20x",      0.02489),
    ("sig_bias_up",     -0.04285),
    ("sig_conso",       -0.84073),   # largest penalty — the legacy turbo "backbone"
    ("sig_para_start",  -0.11218),
    ("sig_para_plus",   -0.09105),
    ("sig_para_retest", -0.07917),
    ("three_g",          0.01321),
    ("bo_up",            0.03736),
    ("bx_up",           -0.00765),
    ("vbo_up",          -0.01435),
    ("eb_bull",         -0.05274),
    ("price_gt_200",    -0.32092),
    ("price_gt_89",     -0.11125),
    ("psar_bull",        0.01172),
    ("sig_z7",          -0.03653),
    ("g1l",             -0.06683),
    ("rsi_14",           0.02256),   # continuous (standardised)
    ("change_pct",      -0.01816),   # continuous (standardised)
]
_BIAS = -1.07019
# continuous standardisation (train mean / std), with clip bounds
_CONT = {
    "rsi_14":     {"mu": 48.52637, "sd": 13.12343, "lo": 0.0,  "hi": 100.0, "default": 50.0},
    "change_pct": {"mu": -0.03582, "sd": 4.78247,  "lo": -50.0, "hi": 50.0, "default": 0.0},
}


def _truthy(v) -> int:
    if v is None:
        return 0
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, (int, float)):
        return 1 if (v == v and v != 0) else 0
    if isinstance(v, str):
        return 0 if v.strip().lower() in ("", "0", "0.0", "false", "none", "null", "nan") else 1
    return 1 if v else 0


def _cont_val(row: dict, key: str) -> float:
    spec = _CONT[key]
    v = row.get(key)
    try:
        f = float(v)
        if f != f or f in (float("inf"), float("-inf")):
            f = spec["default"]
    except (TypeError, ValueError):
        f = spec["default"]
    f = max(spec["lo"], min(spec["hi"], f))
    return (f - spec["mu"]) / spec["sd"]


def compute_prebreak_v2(row: dict) -> dict:
    """Compute the PreBreakout v2 score for a signal-flag row. Never raises."""
    if not isinstance(row, dict):
        return {"prebreak_v2": 0, "prebreak_v2_band": "WATCH", "prebreak_v2_prob": 0.0}
    z = _BIAS
    for feat, w in _TERMS:
        if feat in _CONT:
            z += w * _cont_val(row, feat)
        else:
            z += w * _truthy(row.get(feat))
    z = max(-30.0, min(30.0, z))
    prob = 1.0 / (1.0 + math.exp(-z))
    score = int(round(prob * 100))
    band = "HOT" if score > 27 else ("BUY" if score >= 15 else "WATCH")
    return {
        "prebreak_v2":      score,
        "prebreak_v2_band": band,
        "prebreak_v2_prob": round(prob, 4),
    }
