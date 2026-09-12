"""§4 STRICT LEAKAGE TEST — a feature's value at bar T must not change when future bars exist.

THE TEST. Take a ticker's full daily history. Compute every per-bar engine twice:
    (a) on the frame TRUNCATED at T — what a live system could have known at T's close
    (b) on the FULL frame, future bars included — what the stored database row contains
The row for T must be IDENTICAL. Any difference is look-ahead, whatever the column is called.

WHY IT EXISTS. Column names do not tell you this. On 2026-09-11 the audit of this repository found
two fields that pass any name-based filter and still leak:
  · `acc_exit_class` / `acc_exit_in_n` (studio/enricher.py) walk BACKWARDS from the end of the
    frame and store, on bar i, the distance to a FUTURE markup bar — "BO_1" literally means "the
    breakout is tomorrow".
  · `aes_score` / `aes_leading` / `aes_trend_5d` / `aes_stage` weight each signal by `lift_2_3`,
    a lift measured from future outcomes, looked up PER TICKER — so the weight at T was fitted on
    that ticker's own future.
The repository had already found the same class of bug once before: prebreak_v2.py:24 records that
`is_pivot_*`, `next_pivot_*` and `swing_type` "topped the raw-lift charts precisely because they
are look-ahead".

SCOPE. This covers the engine bundle main.compute_all_signals — the single producer of the stored
per-bar signal columns (T/Z, WLNBB, F, FLY, G, B, combo, VABS, wick, ULTRA, TZ state). Fields
produced elsewhere (enricher, scores, research parquets) are covered by their own audits and, where
they failed, are excluded from modelling rather than tested here.

TWO TESTS, ONE PROPERTY (test contract, 2026-09-12). Truncation invariance is a property of the
ENGINES, not of any particular dataset, so the core check does not need the research corpus and
must not be skipped when the corpus is absent:

  test_no_future_dependence_hermetic   one deterministic in-repo 800-bar frame. Always runs, on
                                       any clean clone. This is the CI signal.
  test_no_future_dependence            the same property over six real tickers and twenty dates
                                       across five regimes. @pytest.mark.external_data — a wider
                                       net, skipped only when the dataset is genuinely absent.

Turning the whole thing into a skip would have traded a permanent guarantee for an occasional one.
"""
import os
import sys

import numpy as np
import pandas as pd
import pytest

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from external_data import external_conn, require_external_test_data   # noqa: E402
from synthetic_ohlcv import (CHECKSUM, MIN_MATURE_INDEX, N_BARS,      # noqa: E402
                             assert_rich_enough, checksum, synthetic_ohlcv)

# Truncation points for the hermetic frame. All above MIN_MATURE_INDEX so that ema200 — the
# longest lookback in the bundle — is past its seed, and all below the last bar so there is a
# future left to remove.
HERMETIC_INDICES = [620, 638, 655, 671, 688, 704, 719, 733, 748, 762, 777, 790]

# Dates chosen across regimes, not at random: 2022 bear, 2023 recovery, 2024 trend,
# 2025 chop, 2026 the window the current research uses.
DATES = [
    "2022-01-24", "2022-03-15", "2022-06-16", "2022-09-28", "2022-12-19",
    "2023-03-13", "2023-06-08", "2023-10-27",
    "2024-02-12", "2024-04-19", "2024-08-05", "2024-11-06",
    "2025-01-21", "2025-04-08", "2025-07-15", "2025-10-14",
    "2026-01-13", "2026-03-30", "2026-06-15", "2026-08-11",
]
TICKERS = ["AAPL", "NVDA", "AMD", "MU", "CAR", "RKLB"]


def _history(ticker: str) -> pd.DataFrame:
    import duckdb
    from studio.db import tf_db_path
    con = duckdb.connect(tf_db_path("1d"), read_only=True)
    try:
        df = con.execute("""
            SELECT substr(CAST(date AS VARCHAR),1,10) AS date, open, high, low, close, volume
            FROM bars WHERE ticker = ? AND universe <> 'index'
            QUALIFY row_number() OVER (PARTITION BY date ORDER BY
              CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2
                            WHEN 'russell2k' THEN 3 ELSE 4 END) = 1
            ORDER BY date""", [ticker]).fetchdf()
    finally:
        con.close()
    return df.reset_index(drop=True)


def _bundle_row(df: pd.DataFrame, ticker: str, idx: int) -> dict:
    """Every engine output for row `idx`, flattened to {engine.column: value}."""
    import main
    b = main.compute_all_signals(df, ticker=ticker, tf="1d")
    out = {}
    for name, obj in vars(b).items():
        if isinstance(obj, pd.DataFrame):
            if len(obj) <= idx:
                continue
            for c in obj.columns:
                out[f"{name}.{c}"] = obj[c].iloc[idx]
        elif isinstance(obj, pd.Series):
            if len(obj) <= idx:
                continue
            out[f"{name}"] = obj.iloc[idx]
    return out


def _same(a, b) -> bool:
    if a is None and b is None:
        return True
    fa, fb = isinstance(a, float), isinstance(b, float)
    if (fa or fb):
        try:
            a_, b_ = float(a), float(b)
        except (TypeError, ValueError):
            return str(a) == str(b)
        if np.isnan(a_) and np.isnan(b_):
            return True
        return bool(np.isclose(a_, b_, rtol=1e-9, atol=1e-12))
    try:
        if pd.isna(a) and pd.isna(b):
            return True
    except (TypeError, ValueError):
        pass
    return a == b


@pytest.mark.external_data
@pytest.mark.parametrize("ticker", TICKERS)
def test_no_future_dependence(ticker):
    """The same property as the hermetic test, over the real corpus: six tickers, twenty dates,
    five regimes. Wider net, but it needs the canonical dataset — so it is skipped when that is
    absent and FAILS when it is present and wrong."""
    require_external_test_data("1d")
    hist = _history(ticker)
    if len(hist) < 300:
        pytest.skip(f"{ticker}: only {len(hist)} bars")
    pos = {d: i for i, d in enumerate(hist["date"])}
    tested, bad = 0, []
    for d in DATES:
        i = pos.get(d)
        if i is None or i < 250 or i >= len(hist) - 1:     # need warmup, and a future to remove
            continue
        full = _bundle_row(hist, ticker, i)
        trunc = _bundle_row(hist.iloc[: i + 1].reset_index(drop=True), ticker, i)
        tested += 1
        for k in sorted(set(full) | set(trunc)):
            if not _same(full.get(k), trunc.get(k)):
                bad.append((d, k, full.get(k), trunc.get(k)))
    assert tested >= 10, f"{ticker}: only {tested} usable dates — the test did not really run"
    assert not bad, (f"{ticker}: {len(bad)} look-ahead differences over {tested} dates, "
                     f"e.g. {bad[:8]}")


@pytest.mark.external_data
def test_the_known_leaks_are_still_leaks():
    """A canary for the two fields the audit rejected. If either ever stops depending on the
    future, re-audit it — do not silently re-admit it.

    This one cannot be made hermetic: it is an assertion about what the STORED database actually
    contains, not about an engine's behaviour, so a synthetic frame could not express it."""
    with external_conn("1d") as con:
        n = con.execute("""SELECT count(*) FROM bars
                           WHERE acc_exit_class IN ('BO_1','BO_2_3','BO_4_5')
                             AND acc_exit_in_n > 0""").fetchone()[0]
    assert n > 0, ("acc_exit_class no longer carries a forward distance — the enricher changed; "
                   "re-run the leakage audit before using it")


# ── HERMETIC — the CI signal. No dataset, no mount, no network. ─────────────
def test_the_hermetic_fixture_is_still_deterministic_and_rich():
    """Guards the guard. If the fixture drifts or goes smooth, the causality test below would
    keep passing while testing less and less, so pin both properties explicitly."""
    df = synthetic_ohlcv()
    assert len(df) == N_BARS
    assert checksum(df) == CHECKSUM, (
        f"synthetic fixture drifted: {checksum(df)} != {CHECKSUM}. Either the generator changed "
        f"or numpy broke default_rng reproducibility — do not re-pin without knowing which.")
    assert_rich_enough(df)


def test_no_future_dependence_hermetic():
    """Truncation invariance on the in-repo 800-bar frame.

    Identical estimand to the corpus test: compute every engine on the frame truncated at T and
    on the full frame, and require the row at T to match. The property belongs to the engines,
    so it holds on any well-formed OHLCV — which is exactly why this version needs no data.
    """
    hist = synthetic_ohlcv()
    assert checksum(hist) == CHECKSUM
    assert HERMETIC_INDICES[0] >= MIN_MATURE_INDEX and HERMETIC_INDICES[-1] < len(hist) - 1

    tested, compared, bad = 0, 0, []
    for i in HERMETIC_INDICES:
        full = _bundle_row(hist, "SYNTH", i)
        trunc = _bundle_row(hist.iloc[: i + 1].reset_index(drop=True), "SYNTH", i)
        tested += 1
        keys = sorted(set(full) | set(trunc))
        compared += len(keys)
        for k in keys:
            if not _same(full.get(k), trunc.get(k)):
                bad.append((hist["date"].iloc[i], k, full.get(k), trunc.get(k)))

    assert tested == len(HERMETIC_INDICES)
    # A frame that produced nothing would make the comparison vacuous — pin the surface too.
    assert compared >= 100 * tested, f"only {compared} field comparisons over {tested} dates"
    assert not bad, (f"{len(bad)} look-ahead differences over {tested} truncation dates, "
                     f"e.g. {bad[:8]}")
