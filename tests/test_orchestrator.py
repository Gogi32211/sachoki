"""Smoke test for main.compute_all_signals — the engine orchestrator (audit #5).

Verifies the extracted orchestrator:
  - returns a bundle exposing every expected engine output, and
  - never raises even if an individual engine fails (each is wrapped + falls back),
    so a broken engine degrades gracefully instead of 500-ing the bar_signals route.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
import pytest

_backend = os.path.join(os.path.dirname(__file__), "..", "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)

import main  # noqa: E402

_ATTRS = ["sig_df", "wlnbb", "f_sigs", "fly_sigs", "g_sigs", "b_sigs",
          "combo_df", "vabs", "wick", "ultra260", "ultraV2", "tz_state_ser"]


def _synthetic_ohlcv(n=120):
    idx = pd.date_range("2024-01-01", periods=n, freq="D")
    base = 100 + np.cumsum(np.linspace(0.1, 0.3, n))  # gentle uptrend
    close = base + np.sin(np.arange(n) / 5.0)
    high = close + 0.8
    low = close - 0.8
    openp = close.copy()
    openp[1:] = close[:-1]
    vol = np.full(n, 1_000_000.0)
    return pd.DataFrame(
        {"open": openp, "high": high, "low": low, "close": close, "volume": vol},
        index=idx,
    )


def test_orchestrator_returns_full_bundle():
    df = _synthetic_ohlcv()
    bundle = main.compute_all_signals(df, "TEST", "1d")
    for a in _ATTRS:
        assert hasattr(bundle, a), f"missing engine output: {a}"


def test_orchestrator_outputs_are_pandas_objects():
    df = _synthetic_ohlcv()
    bundle = main.compute_all_signals(df, "TEST", "1d")
    for a in _ATTRS:
        val = getattr(bundle, a)
        assert isinstance(val, (pd.DataFrame, pd.Series)), f"{a} is {type(val)}"


def test_orchestrator_never_raises_on_bad_input():
    # an empty / malformed frame must not blow up — each engine falls back
    bad = pd.DataFrame({"open": [], "high": [], "low": [], "close": [], "volume": []})
    bundle = main.compute_all_signals(bad, "BAD", "1d")
    assert hasattr(bundle, "sig_df")  # returned a bundle, no exception
