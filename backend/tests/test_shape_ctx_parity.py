"""The DISPLAY layer must compute exactly the shapes the RESEARCH measured.

shape_ctx_build.py (production, reads the app's analytics bars) and
shape_cluster_family.shape_flags (sealed research, reads the canonical parquet) implement the same
Pine definitions in two places. If they drift, the chart shows one thing and the four sealed
verdicts describe another — the exact failure the control work spent a day correcting.

These tests feed IDENTICAL frames to both and compare column by column. No DB, no parquet.
"""
import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shape_ctx_build as P                       # noqa: E402
from shape_cluster_family import shape_flags      # noqa: E402

SHAPES = ["mid", "exp", "con", "last", "wrap", "coil", "moth"]


def _frame(n=400, seed=7, tickers=("AAA", "BBB", "CCC")):
    """Random but reproducible bodies, wide enough to make every shape occur."""
    rng = np.random.default_rng(seed)
    parts = []
    for tk in tickers:
        base = 50 + np.cumsum(rng.normal(0, 1.0, n))
        half = np.abs(rng.normal(0, 1.2, n)) + 0.05
        o = base - half * rng.choice([-1, 1], n)
        c = base + half * rng.choice([-1, 1], n)
        hi = np.maximum(o, c) + np.abs(rng.normal(0, 0.4, n))
        lo = np.minimum(o, c) - np.abs(rng.normal(0, 0.4, n))
        parts.append(pd.DataFrame(dict(
            ticker=tk, date=pd.date_range("2021-01-04", periods=n, freq="B").strftime("%Y-%m-%d"),
            open=o, close=c, high=hi, low=lo,
            volume=rng.integers(1e5, 5e6, n).astype(float),
            avg_vol_20d=rng.integers(2e5, 3e6, n).astype(float),
            atr_14=np.abs(rng.normal(1.5, 0.3, n)) + 0.1,
            rsi_14=rng.uniform(10, 90, n),
            bench_close=100 + np.cumsum(rng.normal(0, 0.5, n)))))
    return pd.concat(parts, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)


def _both(df):
    prod = P.compute(df.copy(), log=lambda *a, **k: None)
    res = shape_flags(df.rename(columns={"date": "session"}).copy())
    return prod, res


def test_seven_shapes_match_the_sealed_research():
    prod, res = _both(_frame())
    for k in SHAPES:
        a = prod[f"s_{k}"].to_numpy(bool)
        b = res[f"s_{k}"].to_numpy(bool)
        # the research file has no bar_index>=3 guard on the individual flags; compare where both defined
        warm = prod.groupby("ticker", sort=False).cumcount().to_numpy() >= 3
        assert (a[warm] == b[warm]).all(), f"{k} differs on {int((a[warm] != b[warm]).sum())} bars"
        assert a.sum() > 0, f"{k} never fired — the fixture cannot test it"


def test_any_shape_and_cluster_axes_match():
    prod, res = _both(_frame())
    assert (prod["any_shape"].to_numpy() == res["any_shape"].to_numpy()).all()
    # research leaves partial windows NaN; production counts them (min_periods=1). Compare where full.
    full = res["cl_bars"].notna().to_numpy()
    assert (prod["cl_bars"].to_numpy()[full] == res["cl_bars"].to_numpy()[full]).all()
    assert (prod["cl_fam"].to_numpy()[full] == res["cl_fam"].to_numpy()[full]).all()
    assert (prod["by_fam"].to_numpy()[full] == res["cl_by_fam"].to_numpy()[full]).all()
    assert (prod["by_den"].to_numpy()[full] == res["cl_by_den"].to_numpy()[full]).all()


def test_display_chain_is_exclusive_and_ordered():
    prod, _ = _both(_frame())
    fired = prod["shape_code"].to_numpy() != ""
    assert (fired == prod["any_shape"].to_numpy()).all(), "a fire without a label, or vice versa"
    # MOTHER outranks everything; WRAP is last
    moth = prod["s_moth"].to_numpy()
    assert (prod.loc[moth, "shape_code"] == "MTH").all()
    wrap_only = prod["s_wrap"].to_numpy() & ~prod[[f"s_{k}" for k in SHAPES if k != "wrap"]].any(axis=1).to_numpy()
    assert (prod.loc[wrap_only, "shape_code"] == "WRP").all()


def test_arrow_only_on_the_swallow_shapes():
    prod, _ = _both(_frame())
    arrow = prod["shape_arrow"].to_numpy()
    can = prod["can_dir"].to_numpy()
    assert (arrow[~can] == "").all(), "an arrow on a shape Pine gives no direction"
    up = can & prod["dir_up"].to_numpy()
    dn = can & prod["dir_dn"].to_numpy()
    assert (arrow[up] == "↑").all() and (arrow[dn] == "↓").all()


def test_lstup_veto_is_exactly_the_measured_cell():
    """SWALLOW_DIR_V1's LST|UP: display-priority LASTNEST with close > open. This is the only
    shape cell with two-window evidence, so it must not drift."""
    prod, _ = _both(_frame())
    expect = (prod["shape_code"].to_numpy() == "LST") & prod["dir_up"].to_numpy()
    assert (prod["lstup_veto"].to_numpy() == expect).all()
    assert prod["lstup_veto"].sum() > 0


def test_knife_veto_only_on_the_directional_shapes():
    prod, _ = _both(_frame())
    v = prod["veto"].to_numpy()
    isdir = np.isin(prod["shape_code"].to_numpy(), ["MTH", "CL4"])
    assert (v == (isdir & (prod["rsi"].to_numpy() < P.KNIFE_MAX))).all()


def test_effort_grade_follows_the_pine_thresholds():
    prod, _ = _both(_frame())
    vr, atr = prod["vr"].to_numpy(), prod["atr_14"].to_numpy()
    rng_ = (prod["high"] - prod["low"]).to_numpy()
    assert (prod.loc[prod["grade"] == 0, "vr"] < P.DRY_MAX).all()
    ab = prod["grade"].to_numpy() == 2
    assert (vr[ab] >= P.VOL_MIN).all() and (rng_[ab] <= P.RANGE_MAX * atr[ab]).all()
    assert set(np.unique(prod["grade"].to_numpy())) <= {0, 1, 2}


def test_key_touches_use_absolute_distance():
    """Pine FIX B. A bar far BELOW the 20-bar low must NOT count as a touch of it."""
    n = 60
    lows = np.full(n, 100.0)
    lows[10] = 40.0                                  # a deep spike, far below everything after it
    df = pd.DataFrame(dict(
        ticker="AAA", date=pd.date_range("2021-01-04", periods=n, freq="B").strftime("%Y-%m-%d"),
        open=101.0, close=102.0, high=103.0, low=lows, volume=1e6, avg_vol_20d=1e6,
        atr_14=1.0, rsi_14=50.0, bench_close=100.0))
    out = P.compute(df, log=lambda *a, **k: None)
    # by the last bar the 20-bar low is 100 and every low equals it -> many touches, but the
    # 40-back spike at 40.0 must never be one of them
    assert out["touches"].max() <= P.KEY_LOOK
    row = out.iloc[-1]
    assert row["touches"] > 0
    signed = (lows[-P.KEY_LOOK:] - 100.0)
    assert (np.abs(signed) <= 0.5).sum() == row["touches"], "abs() not applied to the touch distance"


def test_pos20_and_floor():
    prod, _ = _both(_frame())
    p = prod["pos20"].to_numpy()
    ok = ~np.isnan(p)
    assert ((p[ok] >= -1e-9) & (p[ok] <= 1 + 1e-9)).all()
    assert (prod["floor"].to_numpy()[ok] == (p[ok] <= P.FLOOR_MAX)).all()


def test_no_bleed_across_tickers():
    """Ticker AAA's tail must not create shapes or cluster counts on BBB's head."""
    prod, _ = _both(_frame(n=40))
    for tk, g in prod.groupby("ticker"):
        assert not g.head(3)["any_shape"].any(), f"{tk} fired inside the 4-bar warmup"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
