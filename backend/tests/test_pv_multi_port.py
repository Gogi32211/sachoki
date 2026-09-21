"""The nine PV shapes must fire where the Pine fires, and nowhere else.

A ported signal that is subtly wrong produces a perfectly well-formed NULL, and nothing downstream
can tell the difference — the statistics validate whatever they are handed. So each shape is pinned
against a hand-built frame whose ordinals are chosen to satisfy exactly that shape, including the
user's own worked RE2 example (PRICE 20-18-17-19, VOLUME 5-8-4-9). Hermetic: no store, no vendor.
"""
from __future__ import annotations
import os, sys
import numpy as np, pandas as pd, pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="module")
def M():
    from pv_multi_family import signal_masks
    return signal_masks


def frame(price, volume, ticker="TEST"):
    """o=h=l=c so OHLC4 is exactly the given price — the ordinals are all these shapes read."""
    p = np.asarray(price, float)
    return pd.DataFrame(dict(ticker=ticker, session=[f"2026-01-{i+1:02d}" for i in range(len(p))],
                             open=p, high=p, low=p, close=p, volume=np.asarray(volume, float)))


CASES = [
    ("DIV",  [21, 20, 19, 18],         [1, 1, 2, 3]),
    ("UPP",  [18, 18, 19, 20],         [1, 1, 2, 3]),
    ("UPR",  [18, 18, 21, 20],         [1, 1, 2, 3]),
    ("REV",  [21, 20, 18, 17, 19],     [9, 9, 7, 5, 6]),
    ("RUP",  [20, 20, 18, 21, 22],     [9, 9, 7, 5, 6]),
    ("VUP",  [18, 18, 18, 19],         [9, 9, 7, 5]),
    ("TURN", [22, 22, 21, 20, 21, 22], [9, 9, 6, 8, 5, 7]),
    ("UP4",  [17, 17, 18, 19, 20],     [4, 4, 8, 6, 9]),
    ("RE2",  [20, 20, 18, 17, 19],     [5, 5, 8, 4, 9]),
]


@pytest.mark.parametrize("name,price,volume", CASES)
def test_shape_fires_on_its_own_last_bar(M, name, price, volume):
    S = M(frame(price, volume))
    assert S[name][-1], f"{name} did not fire on the bar built for it"


@pytest.mark.parametrize("name,price,volume", CASES)
def test_no_other_shape_fires_on_that_bar(M, name, price, volume):
    """The nine are mutually exclusive by construction — the whole design rests on it."""
    S = M(frame(price, volume))
    others = [k for k, v in S.items() if k != name and v[-1]]
    assert others == [], f"{name}'s own bar also fired {others}"


def test_users_worked_re2_example(M):
    """From the script's comment: PRICE 20 -> 18 -> 17 -> 19, VOLUME 5 -> 8 -> 4 -> 9 = RE2."""
    S = M(frame([20, 20, 18, 17, 19], [5, 5, 8, 4, 9]))
    assert S["RE2"][-1] and not S["REV"][-1]


def test_warmup_is_per_shape_not_global(M):
    """Each shape has its OWN depth, as in the Pine: the 3-bar shapes are legal on the 3rd bar and
    TURN only on the 5th. A uniform 4-bar warm-up would silently delete the shallow shapes at every
    series start — a deviation from the script no downstream statistic could reveal."""
    S = M(frame([10, 11, 12, 13, 14, 15], [1, 2, 3, 4, 5, 6]))
    for k, v in S.items():
        assert not v[:2].any(), f"{k} fired before it had three bars"
    assert S["UPP"][2], "UPP must be legal on the third bar, as in the Pine"
    assert not S["TURN"][:4].any(), "TURN needs five bars"


def test_shapes_do_not_leak_across_tickers(M):
    """Two tickers concatenated: B's first bars must not read A's tail as their history."""
    a = frame([21, 20, 19, 18], [1, 1, 2, 3], "AAA")
    b = frame([21, 20, 19, 18], [1, 1, 2, 3], "BBB")
    S = M(pd.concat([a, b], ignore_index=True))
    fired = {k: np.where(v)[0].tolist() for k, v in S.items() if v.any()}
    assert fired == {"DIV": [3, 7]}, fired


def test_flat_series_fires_nothing(M):
    """Every comparison is strict, so ties must select nothing rather than everything."""
    S = M(frame([10] * 8, [5] * 8))
    assert not any(v.any() for v in S.values())
