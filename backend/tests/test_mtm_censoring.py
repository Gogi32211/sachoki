"""mtm_N must never report a horizon the data has not reached.

THE DEFECT (found 2026-09-13). add_mtm's fill loop carried the last available mark forward
unconditionally, so a trade with only 14 bars of data left still got a number in `mtm_60`,
indistinguishable from a real 60-bar outcome. On the 2026-08-09 build of opportunities.parquet:
signals in 2026-07 were 96.6 % censored that way and 99.9 % had mtm_60 == mtm_50 == mtm_40 —
`mtm_60` was in truth a 14-bar return. A ranking study read those as resolved outcomes and
reported a "recent breakdown"; the breakdown was the censoring. The whole right edge of every
study built on that table was affected, and the served rank_v1 model was fitted through
2026-08-04, so its own training data contains the censored cohort.

THE DISTINCTION THESE PIN. Carry-forward is CORRECT after a realised exit: closing a stopped
trade later cannot change what it made, so mtm_20, mtm_30 and mtm_60 all equal the exit that
fired at bar 12. It is WRONG when the series simply ran out while the position was still open —
that horizon was never reached, and NaN is the only honest answer.

    resolved(N)  iff  exit fired at or before bar N,  OR  N real bars exist.

Hermetic: synthetic price paths, no DuckDB, no parquet, no network.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from add_mtm import GRID, MAXH, price_trade                          # noqa: E402


def _path(n_bars: int, step: float = 0.01, start: float = 100.0):
    """A calm, monotonically rising path — no trailing stop can fire on it, so every horizon's
    resolution is decided purely by whether the bars exist."""
    cl = start * (1.0 + step) ** np.arange(n_bars)
    return cl.copy(), cl * 1.001, cl * 0.999, cl          # o, hi, lo, cl


def _crash_path(n_bars: int, drop_at: int):
    """Rises, then drops hard enough on `drop_at` to take any sane trailing stop out."""
    o, hi, lo, cl = _path(n_bars)
    o = o.copy(); hi = hi.copy(); lo = lo.copy(); cl = cl.copy()
    for b in (drop_at, drop_at + 1):
        if b < n_bars:
            o[b] = cl[b - 1] * 0.50
            hi[b] = o[b]
            lo[b] = o[b] * 0.98
            cl[b] = o[b] * 0.99
    return o, hi, lo, cl


# ── 1 · a full horizon resolves ─────────────────────────────────────────────
def test_sixty_bars_available_resolves_every_horizon():
    o, hi, lo, cl = _path(MAXH + 5)
    mtm, bars, exit_bar = price_trade(o, hi, lo, cl, j0=0, risk=0.25)
    assert bars == MAXH, bars
    assert exit_bar is None, "a monotonically rising path must never stop out"
    for g in GRID:
        assert np.isfinite(mtm[g]), f"mtm_{g} must be resolved with {bars} bars priced"
    assert mtm[60] > mtm[20] > mtm[10], "a rising path must mark higher at longer horizons"


# ── 2 · THE REGRESSION. Too few bars → the long horizon is NOT an outcome ───
def test_fourteen_bars_leaves_the_sixty_bar_horizon_unresolved():
    o, hi, lo, cl = _path(15)                      # entry at 0 → 14 forward bars
    mtm, bars, exit_bar = price_trade(o, hi, lo, cl, j0=0, risk=0.25)
    assert bars == 14 and exit_bar is None, (bars, exit_bar)
    assert np.isnan(mtm[60]), (
        "mtm_60 on a 14-bar path is the defect this test exists for: the carry-forward used to "
        "make a 14-bar return indistinguishable from a 60-bar outcome")
    for g in (20, 25, 30, 40, 50, 60):
        assert np.isnan(mtm[g]), f"mtm_{g} must be NaN — only {bars} bars exist"


# ── 3 · …while the short horizons on the SAME record stay usable ────────────
def test_short_horizons_still_resolve_on_the_same_truncated_record():
    o, hi, lo, cl = _path(15)
    mtm, bars, _ = price_trade(o, hi, lo, cl, j0=0, risk=0.25)
    for g in (1, 2, 3, 5, 7, 10):
        assert np.isfinite(mtm[g]), f"mtm_{g} is within the {bars} bars that exist"
    assert np.isnan(mtm[20]), "the boundary must fall between the reached and unreached horizons"


# ── 4 · carry-forward after a real exit is CORRECT and must survive ─────────
def test_a_realised_exit_carries_forward_to_every_later_horizon():
    """The trade is over at bar 12. Holding it notionally to bar 60 cannot change the result, so
    mtm_20..mtm_60 must equal the realised exit — even though only 15 bars of data exist."""
    o, hi, lo, cl = _crash_path(15, drop_at=12)
    mtm, bars, exit_bar = price_trade(o, hi, lo, cl, j0=0, risk=0.25)
    assert exit_bar is not None and exit_bar <= 12, exit_bar
    assert bars == 14
    for g in (20, 25, 30, 40, 50, 60):
        assert np.isfinite(mtm[g]), f"mtm_{g} must be the realised exit, not NaN"
        assert mtm[g] == pytest.approx(mtm[exit_bar]), "a closed trade cannot change value"
    assert mtm[60] < 0, "the crash path must exit at a loss"


# ── 5 · the dataset edge must not masquerade as full-horizon truth ──────────
@pytest.mark.parametrize("avail", [1, 5, 14, 30, 59])
def test_edge_cohorts_never_report_an_unreached_horizon(avail):
    """Walk the last few fires of a dataset inward. Each must resolve exactly the horizons its
    own data covers and no more — this is the shape of the right edge that produced the fake
    'breakdown'."""
    o, hi, lo, cl = _path(avail + 1)
    mtm, bars, exit_bar = price_trade(o, hi, lo, cl, j0=0, risk=0.25)
    assert bars == avail and exit_bar is None
    for g in GRID:
        if g <= avail:
            assert np.isfinite(mtm[g]), f"{g} <= {avail} bars: must resolve"
        else:
            assert np.isnan(mtm[g]), f"{g} > {avail} bars: must NOT resolve"


def test_the_carry_forward_signature_is_gone():
    """The old bug left mtm_60 == mtm_50 == mtm_40 on truncated rows. With the fix those are all
    NaN instead, so the equality can no longer be produced by censoring."""
    o, hi, lo, cl = _path(20)
    mtm, _, _ = price_trade(o, hi, lo, cl, j0=0, risk=0.25)
    assert np.isnan(mtm[40]) and np.isnan(mtm[50]) and np.isnan(mtm[60])
    assert np.isfinite(mtm[15]) and np.isfinite(mtm[10])
