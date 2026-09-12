"""The ▽△ HISTORY must equal the ▽△ row the user reads on the chart.

The row is drawn from /api/day1h (SuperchartPanel.jsx calls it with days=300), computed live inside
main.py::api_day1h. anatomy_build.py TRANSCRIBES that endpoint so the verdict can be measured across
the book instead of only read one ticker at a time. Everything the ▽△ families concluded rests on
that transcription being faithful — and on 2026-09-11 it was not:

    `key` ("≥ 2 of the prior 25 lows within 1 % of the window low") was written as an ELEMENTWISE
    comparison, `near = lo_p <= wl * 1.01`, then rolled. That compares the low at row j against row
    j's OWN window low — a moving threshold — where the endpoint compares all 25 lows against ONE
    threshold, the window low at bar i. It agreed with the chart on 54.7 % of days, bidirectionally,
    so the a_loc axis and the 0-8 score were noise-corrupted in both directions. The VERDICT still
    agreed 99.6 %, because `key` only reaches at_floor when not held, pos < 0.40 and a_abs < 2.

    Consequence: every family measured on a pre-fix parquet is void for anything reading `s`,
    `loc`, `key` or `at_floor`.

The fast tests here need no DB: they compare `anatomy_build.location_axis` against a literal
transcription of the endpoint's own loop. The last test is the real end-to-end parity and runs only
when the parquet exists and the service is up.
"""
import json
import os
import sys
import urllib.error
import urllib.request

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import anatomy_build as AB                                        # noqa: E402

WIN = AB.WIN
BASE = "http://127.0.0.1:8080"
FIELDS = ["v", "s", "loc", "abs", "rev"]     # rs is excluded — see test_live_parity's docstring


def _reference(df):
    """main.py::api_day1h's daily block, transcribed line for line. Deliberately a slow loop."""
    held, key, pos = [], [], []
    for _tk, g in df.groupby("ticker", sort=False):
        _dh = g["high"].to_numpy(float); _dl = g["low"].to_numpy(float)
        _dc = g["close"].to_numpy(float)
        for i in range(len(g)):
            if i < WIN:
                held.append(False); key.append(False); pos.append(0.5)
                continue
            w_hi = float(_dh[i - WIN:i].max()); w_lo = float(_dl[i - WIN:i].min())
            rng = (w_hi - w_lo) / w_lo if w_lo > 0 else 9.0
            held.append(bool(rng <= 0.35 and (_dl[i] - w_lo) / w_lo <= 0.06))
            key.append(bool(sum(1 for x in _dl[i - WIN:i] if x <= w_lo * 1.01) >= 2))
            pos.append(float((_dc[i] - w_lo) / (w_hi - w_lo)) if w_hi > w_lo else 0.5)
    return np.array(held), np.array(key), np.array(pos)


def _frame(n=300, seed=11, tickers=("AAA", "BBB", "CCC")):
    """Reproducible bars. A low drift and frequent revisits of the floor so `key` actually fires."""
    rng = np.random.default_rng(seed)
    parts = []
    for tk in tickers:
        base = 40 + np.cumsum(rng.normal(0, 0.45, n))
        lo = base - np.abs(rng.normal(0, 0.5, n))
        hi = base + np.abs(rng.normal(0, 0.5, n))
        parts.append(pd.DataFrame(dict(
            ticker=tk, date=pd.date_range("2021-01-04", periods=n, freq="B").strftime("%Y-%m-%d"),
            high=hi, low=lo, close=rng.uniform(lo, hi))))
    return pd.concat(parts, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)


@pytest.mark.parametrize("seed", [11, 12, 13])
def test_location_axis_matches_the_endpoint(seed):
    """held / key / pos must be identical to the endpoint's own loop, bar for bar."""
    df = _frame(seed=seed)
    held, key, pos = AB.location_axis(df)
    r_held, r_key, r_pos = _reference(df)
    assert (held == r_held).all(), f"held differs on {(held != r_held).sum()} bars"
    assert (key == r_key).all(), f"key differs on {(key != r_key).sum()} bars"
    assert np.allclose(pos, r_pos), f"pos differs on {(~np.isclose(pos, r_pos)).sum()} bars"


def test_key_uses_one_fixed_threshold_not_a_moving_one():
    """The exact defect, pinned by construction.

    A falling floor: every bar's low is below the one before it, so each bar IS within 1 % of its
    own trailing-window low while almost none are within 1 % of the window low at the evaluation
    bar. The moving-threshold version says key=True on every bar; the endpoint says False.
    """
    n = 80
    low = 100.0 * (0.985 ** np.arange(n))          # −1.5 %/bar, comfortably outside the 1 % band
    df = pd.DataFrame(dict(ticker="AAA",
                           date=pd.date_range("2021-01-04", periods=n, freq="B").strftime("%Y-%m-%d"),
                           low=low, high=low * 1.03, close=low * 1.01))
    _, key, _ = AB.location_axis(df)
    _, r_key, _ = _reference(df)
    assert (key == r_key).all()
    assert not key[WIN:].any(), "a steadily falling low has no repeated floor — key must be False"

    # the defect, reproduced, so the test fails loudly if anyone reintroduces it
    lo_p = df.groupby("ticker", sort=False)["low"].shift(1)
    w_lo = lo_p.groupby(df["ticker"], sort=False).rolling(WIN, min_periods=WIN).min() \
               .reset_index(level=0, drop=True)
    moving = (df.assign(_n=(lo_p.to_numpy() <= w_lo.to_numpy() * 1.01).astype(float))
                .groupby("ticker", sort=False)["_n"].rolling(WIN, min_periods=WIN).sum()
                .reset_index(level=0, drop=True).to_numpy() >= 2)
    # from WIN+1 on: the buggy version's own warmup leaves only one True inside the first window
    assert moving[WIN + 1:].all(), "the buggy expression should say True here — that was the whole bug"


def test_warmup_bars_are_blank():
    """Before the window is full the endpoint emits held False, key False and no pos (read as 0.5)."""
    df = _frame(n=60)
    held, key, pos = AB.location_axis(df)
    first = df.groupby("ticker", sort=False).cumcount().to_numpy() < WIN
    assert not held[first].any() and not key[first].any()
    assert (pos[first] == 0.5).all()


def test_short_series_is_safe():
    """A ticker with fewer bars than the window must not raise and must stay blank."""
    df = _frame(n=WIN - 3, tickers=("AAA",))
    held, key, pos = AB.location_axis(df)
    assert not held.any() and not key.any() and (pos == 0.5).all()


def _service_up():
    try:
        with urllib.request.urlopen(f"{BASE}/api/health", timeout=3) as r:
            return r.status == 200
    except Exception:
        return False


@pytest.mark.skipif(not os.path.exists(AB.OUT), reason="anatomy_signals.parquet not built")
@pytest.mark.skipif(not _service_up(), reason="backend not running on :8080")
def test_live_parity():
    """End to end: the built history vs what /api/day1h actually returns, for real tickers.

    `rs` is not compared. The endpoint seeds its EMA(2/201) at its own cutoff — days*2+45 calendar
    days back — so with the chart's days=300 it has ~445 bars of seed and disagrees with the
    full-history EMA on ~3 % of days, rising with age. That is a property of the endpoint's window,
    not of the definition: at days=1400 the disagreement is 0.000 % over 12,161 rows. The parquet
    holds the better-seeded value.
    """
    hist = pd.read_parquet(AB.OUT, columns=["ticker", "date"] + FIELDS)
    pool = sorted(hist["ticker"].unique())
    sample = [t for t in ("AAPL", "NVDA", "AMD", "MSFT", "RKLB") if t in set(pool)] or pool[:5]
    by_tk = {t: g.set_index("date") for t, g in hist[hist["ticker"].isin(sample)].groupby("ticker")}

    compared = 0
    bad: list = []
    for tk in sample:
        with urllib.request.urlopen(f"{BASE}/api/day1h/{tk}?days=300", timeout=120) as r:
            days = (json.load(r) or {}).get("days") or []
        g = by_tk.get(tk)
        if g is None:
            continue
        for d in days:
            ds = d["date"]
            if ds not in g.index:
                continue
            h = g.loc[ds]
            h = h.iloc[0] if isinstance(h, pd.DataFrame) else h
            a = d.get("anat") or {}
            compared += 1
            for f in FIELDS:
                av = str(a.get(f) or "") if f == "v" else int(a.get(f))
                hv = str(h[f] or "") if f == "v" else int(h[f])
                if av != hv:
                    bad.append((tk, ds, f, av, hv))
    assert compared > 500, f"only {compared} overlapping days — nothing was really tested"
    assert not bad, f"{len(bad)} of {compared} disagree with the chart, e.g. {bad[:10]}"
