"""Regression: the Edge Replay day-clustered CONTROL must stay DE-PHASED.

2026-09-10. The control grid used to start at index 0 of every ticker's eligible sequence
(`np.where(close >= 21)[0][::40]`). Tickers share entry sessions, so the grids aligned and
control trades bunched onto a few days — median 14/day, only 31 % of days reaching the 20
a day-median needs. Measured over all 122 setups that flattered day_med_edge by a mean
1.06 pp, flipped 11 signs and moved 13 setups across the day-win 50 line.

These tests pin the two properties of the fix. They are pure unit tests — no frame build,
no DuckDB — so they run in milliseconds.
"""
import os
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import edge_replay as E  # noqa: E402
import control_keys as CK  # noqa: E402

STRIDE = 40


def _mask(ticker: str, n_eligible: int) -> np.ndarray:
    """Reproduce exactly what edge_replay builds for one ticker (all bars eligible)."""
    m = np.zeros(n_eligible, dtype=bool)
    e = np.arange(n_eligible)
    if len(e):
        m[e[E._ctrl_phase(ticker) % len(e)::STRIDE]] = True
    return m


def test_phase_matches_the_sealed_control_authority():
    """One rule, two call sites: research (control_keys) and serving (edge_replay)."""
    for tk in ("AAPL", "TSLA", "NVDA", "RKLB", "SNDK", "A", "ZZZZ"):
        assert E._ctrl_phase(tk) == CK.phase(tk, STRIDE)


def test_phase_is_stable_across_processes():
    """python hash() is salted per process — a restarted backend would resample the
    control and silently change every day_med_edge on the board."""
    assert E._ctrl_phase("AAPL") == 10
    assert E._ctrl_phase("NVDA") == 26
    assert E._ctrl_phase("RKLB") == 32
    assert all(0 <= E._ctrl_phase(t) < STRIDE for t in ("A", "MSFT", "GME", "BRK.B"))


def test_grids_do_not_align_across_tickers():
    """The defect itself: with phase 0 every ticker fires on the same bar index."""
    tickers = ["AAPL", "MSFT", "NVDA", "TSLA", "AMD", "GOOG", "META", "NFLX",
               "RKLB", "SNDK", "CAR", "RGTI"]
    first = {int(np.flatnonzero(_mask(t, 400))[0]) for t in tickers}
    assert len(first) > 1, "control grids still phase-aligned"
    # phase 0 would give exactly one distinct starting index
    assert first != {0}


def test_sampling_rate_is_unchanged():
    """De-phase, do not densify: still 1 bar in 40, so the estimand is the same."""
    for tk in ("AAPL", "TSLA", "RKLB"):
        m = _mask(tk, 4000)
        assert 99 <= m.sum() <= 100
        fired = np.flatnonzero(m)
        assert set(np.diff(fired).tolist()) == {STRIDE}


def test_short_series_do_not_crash_or_over_sample():
    """`phase % len(eligible)` keeps a ticker with fewer bars than the stride in range."""
    for n in (1, 2, 7, 39, 40, 41):
        m = _mask("RKLB", n)
        assert m.sum() >= 1
        assert len(m) == n


def test_min_control_guard_exists():
    """edge_replay used to take a day-median off however few control trades existed —
    sometimes one or two. A day now needs MIN_CTRL before it is a benchmark."""
    assert E.MIN_CTRL == CK.MIN_CONTROL == 20


def test_control_column_was_renamed():
    """Frames are cached (LRU) and live for the process. Reusing the old column name
    would serve a warm phase-0 mask after the fix shipped."""
    src = open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "edge_replay.py")).read()
    assert '"EDGE_CTRL_V2"' in src
    assert 'np.where(_g["close"].to_numpy() >= 21)[0][::40]' not in src


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
