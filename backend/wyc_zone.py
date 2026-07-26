"""
wyc_zone.py — shared Wyckoff Trading-Range zone classifier for the Edge scanners.

A bar sits in a validated range (wt_valid_tr=1, bounded by wt_support/wt_resistance).
The SC-zone (within ±5% of support = the Selling-Climax floor) is where several Edge
setups become a "SUPER" tier — validated 2026-07-03 (validate_super_edge_full /
validate_sc_super_robust): SC-gating lifts median (kills the negative tail) and, for
D+L1 / Spring / T1-CapBounce, holds a clean band-plateau + survives 2× slip.

ROBUST (build SC-super badge):  T1-CapBounce, D+L1, Spring
MARGINAL (spike, not plateau — NOT built): Parabola, P55 (only work at a tight ±3-4% band).

Columns needed in the scanner SELECT:
  coalesce(wt_valid_tr,0) vtr, coalesce(wt_support,0) wt_sup, coalesce(wt_resistance,0) wt_res
"""
BAND = 0.05


def sc_zone(close, wt_support, wt_resistance, wt_valid_tr, band: float = BAND) -> bool:
    """True if `close` sits within ±band of a valid range's support (the SC floor)."""
    try:
        c = float(close); s = float(wt_support); r = float(wt_resistance)
    except (TypeError, ValueError):
        return False
    if int(wt_valid_tr or 0) != 1 or not (r > s > 0):
        return False
    return abs(c / s - 1) <= band


# SQL fragment to add the three range columns to a scanner's SELECT
SELECT_COLS = ("coalesce(wt_valid_tr,0) AS vtr, coalesce(wt_support,0) AS wt_sup, "
               "coalesce(wt_resistance,0) AS wt_res")
