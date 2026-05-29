"""Regression test for studio.seq_backtest — realized entry/exit/stop simulation.

Isolated in-memory DuckDB with a hand-built price path so the target / stop / time
exits are deterministic and checkable.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta

import duckdb
import pytest

_backend = os.path.join(os.path.dirname(__file__), "..", "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)

import studio.seq_backtest as bt


class _NoCloseConn:
    def __init__(self, c): self._c = c
    def __getattr__(self, k): return getattr(self._c, k)
    def close(self): pass


def _seed(rows):
    con = duckdb.connect(":memory:")
    con.execute("""CREATE TABLE bars (
        ticker VARCHAR, date DATE, universe VARCHAR,
        open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
        rsi_le_35 INTEGER, wyc_phase VARCHAR)""")
    for r in rows:
        con.execute("INSERT INTO bars VALUES (?,?,?,?,?,?,?,?,?)", r)
    return con


def test_target_hit_long(monkeypatch):
    # bar0 triggers (rsi_le_35=1); entry = next open (100); bar2 high reaches +10%
    rows = [
        ("AAA", "2024-01-01", "sp500", 100, 101, 99, 100, 1, "MARKUP"),  # trigger
        ("AAA", "2024-01-02", "sp500", 100, 105, 99, 102, 0, "MARKUP"),  # entry@100
        ("AAA", "2024-01-03", "sp500", 102, 112, 101, 111, 0, "MARKUP"), # high 112 >= 110 → target
        ("AAA", "2024-01-04", "sp500", 111, 113, 110, 112, 0, "MARKUP"),
    ]
    con = _seed(rows)
    monkeypatch.setattr(bt, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = bt.backtest(signals=["rsi_le_35"], universe="sp500", target_pct=10, stop_pct=5, max_hold=5)
    assert r["overall"]["n"] == 1
    assert r["overall"]["win_pct"] == 100.0
    assert r["exit_reasons"].get("target") == 1


def test_stop_hit_long(monkeypatch):
    rows = [
        ("BBB", "2024-01-01", "sp500", 100, 101, 99, 100, 1, "MARKUP"),
        ("BBB", "2024-01-02", "sp500", 100, 101, 99, 100, 0, "MARKUP"),  # entry@100
        ("BBB", "2024-01-03", "sp500", 99, 100, 94, 95, 0, "MARKUP"),    # low 94 <= 95 → stop -5%
        ("BBB", "2024-01-04", "sp500", 95, 96, 90, 92, 0, "MARKUP"),
    ]
    con = _seed(rows)
    monkeypatch.setattr(bt, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = bt.backtest(signals=["rsi_le_35"], universe="sp500", target_pct=10, stop_pct=5, max_hold=5)
    assert r["overall"]["n"] == 1
    assert r["overall"]["win_pct"] == 0.0
    assert r["exit_reasons"].get("stop") == 1
    assert r["overall"]["avg_ret"] == pytest.approx(-5.0, abs=1e-6)


def test_rejects_unknown_signal(monkeypatch):
    con = _seed([("AAA", "2024-01-01", "sp500", 100, 101, 99, 100, 1, "MARKUP")])
    monkeypatch.setattr(bt, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = bt.backtest(signals=["definitely_not_a_column; DROP TABLE bars"], universe="sp500")
    assert "error" in r  # no valid flags → safe error, no SQL execution of junk


def test_no_pyramiding(monkeypatch):
    # two consecutive triggers but the first trade is still open → second ignored
    rows = [
        ("CCC", "2024-01-01", "sp500", 100, 101, 99, 100, 1, "MARKUP"),  # trigger1
        ("CCC", "2024-01-02", "sp500", 100, 101, 99, 100, 1, "MARKUP"),  # entry1@100; also trigger2
        ("CCC", "2024-01-03", "sp500", 100, 101, 99, 100, 0, "MARKUP"),
        ("CCC", "2024-01-04", "sp500", 100, 101, 99, 100, 0, "MARKUP"),
        ("CCC", "2024-01-05", "sp500", 100, 101, 99, 100, 0, "MARKUP"),
    ]
    con = _seed(rows)
    monkeypatch.setattr(bt, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = bt.backtest(signals=["rsi_le_35"], universe="sp500", target_pct=10, stop_pct=5, max_hold=3)
    assert r["overall"]["n"] == 1  # only one position at a time


def _seed_alphabet(n_trades_per_ticker=15):
    """5 LOSER tickers that sort FIRST (AAA*) + 35 WINNER tickers that sort LAST (ZZZ*),
    each firing many trades. A plain alphabetical truncation would keep only the AAA
    losers; the deterministic shuffle must retain a representative mix."""
    base = date(2024, 1, 1)
    def d(i): return (base + timedelta(days=i)).isoformat()
    rows = []
    def add(tk, win):
        for t in range(n_trades_per_ticker):
            k = t * 3
            rows.append((tk, d(k),     "sp500", 100, 101, 99, 100, 1, "MARKUP"))  # trigger
            rows.append((tk, d(k + 1), "sp500", 100, 102, 99, 101, 0, "MARKUP"))  # entry@100
            if win:   # +8% target (high 110 >= 108)
                rows.append((tk, d(k + 2), "sp500", 101, 110, 100, 109, 0, "MARKUP"))
            else:     # -4% stop (low 95 <= 96)
                rows.append((tk, d(k + 2), "sp500", 101, 102, 95, 96, 0, "MARKUP"))
    for i in range(5):
        add(f"AAA{i}", win=False)
    for i in range(35):
        add(f"ZZZ{i:02d}", win=True)
    return _seed(rows)


def test_sampling_not_alphabetical_biased(monkeypatch):
    con = _seed_alphabet()
    monkeypatch.setattr(bt, "get_conn", lambda read_only=True: _NoCloseConn(con))
    # cap forces a sample (40 tickers × 15 trades = 600 ≫ 100)
    r1 = bt.backtest(signals=["rsi_le_35"], universe="sp500",
                     target_pct=8, stop_pct=4, max_hold=5, max_trades=100)
    assert r1["truncated"] is True
    # alphabetical-prefix truncation would yield only AAA losers → win 0%.
    # the shuffle must have sampled ZZZ winners too:
    assert r1["overall"]["win_pct"] > 0
    # sampling transparency for the UI ("PF on a sample of N"): the metrics come
    # from a strict subset of the 40 tickers, and n_trades matches overall.n.
    assert r1["n_tickers_total"] == 40
    assert r1["n_tickers"] < 40
    assert r1["n_trades"] == r1["overall"]["n"]
    # deterministic: same seed → identical result
    r2 = bt.backtest(signals=["rsi_le_35"], universe="sp500",
                     target_pct=8, stop_pct=4, max_hold=5, max_trades=100)
    assert r1["overall"]["win_pct"] == r2["overall"]["win_pct"]
    assert r1["n_tickers"] == r2["n_tickers"]
    # no cap → exhaustive, truncated flag off, every ticker represented
    r3 = bt.backtest(signals=["rsi_le_35"], universe="sp500",
                     target_pct=8, stop_pct=4, max_hold=5, max_trades=100000)
    assert r3["truncated"] is False
    assert r3["overall"]["n"] == 600
    assert r3["n_tickers"] == 40 and r3["n_tickers_total"] == 40
