"""The display layer must compute exactly what the sealed family measured.

PV_MULTI_V1 was sealed on 2026-09-21 and its verdict (0 BUILD / 4 VETO_CANDIDATE / 5 NULL) is
printed in the tooltip of every mark this layer draws. If the two implementations drift, the chart
shows a shape while citing evidence about a DIFFERENT shape — the worst failure available to a
descriptive layer, and one no statistic downstream could ever catch.

So `pv_multi_build.masks` is pinned against `pv_multi_family.signal_masks` — the research module's
own code — on real bars. The research module is deliberately NOT imported by the builder and NOT
modified: its SEAL.json records a digest of that file, and a display change must not disturb it.

Also pinned here: the mutual-exclusivity guard, which is what lets one row show one code per bar.
"""
from __future__ import annotations
import os, sys
import numpy as np, pandas as pd, pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CODES = ("DIV", "UPP", "UPR", "REV", "RUP", "VUP", "TURN", "UP4", "RE2")


@pytest.fixture(scope="module")
def frame():
    """Real bars, several tickers, spanning splits and gaps — not a synthetic ramp."""
    import duckdb
    from studio.paths import ANALYTICS_DB
    if not os.path.exists(ANALYTICS_DB):
        pytest.skip("1D store not present")
    con = duckdb.connect(ANALYTICS_DB, read_only=True)
    df = con.execute("""
        WITH r AS (SELECT ticker, CAST(date AS VARCHAR) AS date, open, high, low, close, volume,
                          row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
                   FROM bars WHERE universe <> 'index'
                     AND ticker IN ('AAPL','AMD','NVDA','F','T','KO','NXXT','SPY'))
        SELECT * EXCLUDE (rn) FROM r WHERE rn = 1 ORDER BY ticker, date""").fetchdf()
    con.close()
    df["date"] = df["date"].str[:10]
    return df.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)


def test_ohlc4_matches_the_sealed_research_bit_for_bit(frame):
    import pv_multi_build as B
    import pv_multi_family as F
    price = (frame.open + frame.high + frame.low + frame.close) / 4.0
    mine = B.masks(price, frame.volume.astype(float), frame.ticker)
    theirs = F.signal_masks(frame)
    for c in CODES:
        assert mine[c].sum() > 0, f"{c} never fired on the sample — the test would prove nothing"
        diff = int((mine[c] != theirs[c]).sum())
        assert diff == 0, f"{c}: {diff} bars differ between the display layer and the sealed family"


def test_close_source_is_a_different_reading_not_a_copy(frame):
    """Two rows exist because the two price sources disagree. If they ever matched, one is wrong."""
    import pv_multi_build as B
    vol = frame.volume.astype(float)
    o = B.masks((frame.open + frame.high + frame.low + frame.close) / 4.0, vol, frame.ticker)
    c = B.masks(frame.close.astype(float), vol, frame.ticker)
    assert any(int((o[k] != c[k]).sum()) > 0 for k in CODES)


def test_codes_are_mutually_exclusive_on_real_bars(frame):
    import pv_multi_build as B
    vol = frame.volume.astype(float)
    for price in ((frame.open + frame.high + frame.low + frame.close) / 4.0, frame.close.astype(float)):
        M = B.masks(price, vol, frame.ticker)
        assert int(np.vstack([M[c] for c in CODES]).sum(axis=0).max()) <= 1


def test_overlap_guard_raises_rather_than_picking_one(frame):
    """If the shapes ever stop being exclusive, the row must fail loudly — never quietly display
    whichever code happens to come last in the loop."""
    import pv_multi_build as B
    M = {c: np.zeros(3, bool) for c in CODES}
    M["DIV"][1] = True
    M["VUP"][1] = True
    with pytest.raises(AssertionError, match="mutually exclusive"):
        B.codes_of(M, 3, "test")


def test_codes_of_marks_the_right_bars(frame):
    import pv_multi_build as B
    M = {c: np.zeros(4, bool) for c in CODES}
    M["REV"][0] = True
    M["TURN"][2] = True
    assert list(B.codes_of(M, 4, "t")) == ["REV", "", "TURN", ""]


def test_shifts_do_not_cross_ticker_boundaries(frame):
    """Per-ticker shift, or the first bars of each ticker would read the previous ticker's tail."""
    import pv_multi_build as B
    vol = frame.volume.astype(float)
    M = B.masks(frame.close.astype(float), vol, frame.ticker)
    first = frame.groupby("ticker", sort=False).head(2).index.to_numpy()
    for c in CODES:
        assert not M[c][first].any(), f"{c} fired within the first two bars of a ticker"
