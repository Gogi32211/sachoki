"""Tests for Phase 2 incremental stock_stat scan (260523 v4.9)."""
import os
import sys
import tempfile
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

import numpy as np
import pandas as pd

from analyzers.tz_wlnbb.stock_stat import (
    generate_stock_stat,
    generate_stock_stat_incremental,
    _read_existing_last_dates,
    _read_ticker_tail,
    _append_rows,
)


def _fake_fetch_factory(end_date=None):
    """Build a fetch function whose data ends at end_date (default: today)."""
    if end_date is None:
        end_date = date.today()

    def _fetch(ticker, interval="1d", n_bars_or_kw=None, since=None):
        np.random.seed(hash(ticker) % 2**32)
        n = 80
        closes = 100 + np.cumsum(np.random.normal(0, 0.5, n))
        idx = pd.bdate_range(end=end_date, periods=n)
        df = pd.DataFrame({
            "open": closes, "high": closes + 0.5, "low": closes - 0.5,
            "close": closes, "volume": 1000000,
        }, index=idx)
        if since:
            df = df[df.index >= pd.Timestamp(since)]
        return df
    return _fetch


def test_cold_start_falls_back_to_full():
    """Calling incremental when CSV doesn't exist should run a full scan."""
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "s.csv")
        _, audit = generate_stock_stat_incremental(
            ["AAPL"], _fake_fetch_factory(), universe="sp500", tf="1d",
            output_path=out,
        )
        assert os.path.exists(out)
        df = pd.read_csv(out)
        assert len(df) > 0
        # cold start uses full-scan code path, not the incremental one;
        # audit shape is from generate_stock_stat
        assert "tickers_processed" in audit or "tickers_appended" in audit


def test_incremental_appends_new_bars():
    """After a full scan, trim N bars and verify incremental adds them back."""
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "s.csv")
        # Step 1: full scan
        generate_stock_stat_incremental(
            ["AAPL", "MSFT"], _fake_fetch_factory(), universe="sp500", tf="1d",
            output_path=out,
        )
        df1 = pd.read_csv(out)
        last_date = df1["date"].max()
        second_last = sorted(df1["date"].unique())[-2]
        # Step 2: trim last 2 unique dates
        df_trim = df1[df1["date"] < second_last]
        df_trim.to_csv(out, index=False)
        n_before = len(df_trim)
        # Step 3: incremental
        _, audit = generate_stock_stat_incremental(
            ["AAPL", "MSFT"], _fake_fetch_factory(), universe="sp500", tf="1d",
            output_path=out,
        )
        df3 = pd.read_csv(out)
        assert len(df3) > n_before
        # Either tickers_appended > 0 (true incremental) or cold-start used
        assert audit["tickers_appended"] > 0 or audit["tickers_cold_started"] > 0


def test_incremental_no_op_when_already_current():
    """Running incremental twice in a row → second run adds 0 rows."""
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "s.csv")
        generate_stock_stat_incremental(
            ["AAPL"], _fake_fetch_factory(), universe="sp500", tf="1d",
            output_path=out,
        )
        df_first = pd.read_csv(out)
        n_first = len(df_first)
        # Run again
        _, audit2 = generate_stock_stat_incremental(
            ["AAPL"], _fake_fetch_factory(), universe="sp500", tf="1d",
            output_path=out,
        )
        df_second = pd.read_csv(out)
        # No new bars should be added since dates are identical
        assert len(df_second) == n_first
        assert audit2["rows_added"] == 0


def test_read_existing_last_dates_groups_by_ticker():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "s.csv")
        df = pd.DataFrame({
            "ticker": ["A", "A", "B", "B"],
            "date":   ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-03"],
        })
        df.to_csv(out, index=False)
        d = _read_existing_last_dates(out)
        assert d == {"A": "2024-01-02", "B": "2024-01-03"}


def test_read_ticker_tail_returns_ohlcv_only():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "s.csv")
        df = pd.DataFrame({
            "ticker": ["A"] * 5,
            "date":   ["2024-01-01", "2024-01-02", "2024-01-03",
                       "2024-01-04", "2024-01-05"],
            "open":   [1, 2, 3, 4, 5],
            "high":   [1.5, 2.5, 3.5, 4.5, 5.5],
            "low":    [0.5, 1.5, 2.5, 3.5, 4.5],
            "close":  [1.2, 2.2, 3.2, 4.2, 5.2],
            "volume": [100, 200, 300, 400, 500],
            "t_signal": ["T4", "", "", "", ""],   # should NOT appear in tail
        })
        df.to_csv(out, index=False)
        tail = _read_ticker_tail(out, "A", n_bars=3)
        assert tail is not None
        assert list(tail.columns) == ["date", "open", "high", "low", "close", "volume"]
        assert len(tail) == 3
        assert tail.iloc[-1]["date"] == "2024-01-05"


def test_read_ticker_tail_missing_ticker():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "s.csv")
        pd.DataFrame({
            "ticker": ["A"], "date": ["2024-01-01"], "open": [1.0],
            "high": [1.5], "low": [0.5], "close": [1.2], "volume": [100],
        }).to_csv(out, index=False)
        assert _read_ticker_tail(out, "Z", n_bars=5) is None


def test_append_rows():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "s.csv")
        with open(out, "w") as f:
            f.write("a,b,c\n1,2,3\n")
        n = _append_rows(out, [["4", "5", "6"], ["7", "8", "9"]])
        assert n == 2
        with open(out) as f:
            lines = f.read().splitlines()
        assert lines == ["a,b,c", "1,2,3", "4,5,6", "7,8,9"]


def test_append_rows_empty():
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "s.csv")
        with open(out, "w") as f:
            f.write("a,b\n")
        n = _append_rows(out, [])
        assert n == 0
