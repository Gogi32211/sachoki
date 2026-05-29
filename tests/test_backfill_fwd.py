"""Regression test for studio.backfill_fwd (audit fix #7).

Verifies on an isolated in-memory DuckDB that the backfill:
  1. fills previously-NULL forward-return labels for bars whose future now exists,
     using the exact importer formula  fwd_Nd = (close[i+N]/close[i] - 1) * 100;
  2. leaves bars with no future (the very last N) as NULL;
  3. NEVER overwrites an already-populated value (purely additive).
"""
from __future__ import annotations

import os
import sys

import duckdb
import pytest

_backend = os.path.join(os.path.dirname(__file__), "..", "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)

import studio.backfill_fwd as bf

_FWD = ["fwd_1d", "fwd_3d", "fwd_5d", "fwd_10d", "fwd_20d", "fwd_30d", "fwd_60d", "fwd_90d"]
_MFE = ["mfe_5d", "mfe_10d", "mfe_20d", "mfe_30d", "mfe_60d"]
_MAE = ["mae_5d", "mae_10d", "mae_20d", "mae_30d"]
_FLAGS = ["hit_5pct_5d", "hit_10pct_5d", "hit_20pct_5d", "hit_30pct_10d",
          "hit_50pct_20d", "hit_2x_60d", "drop_10pct_5d", "drop_20pct_10d", "drop_30pct_20d"]


class _NoCloseConn:
    """Proxy so backfill's finally:conn.close() doesn't drop our in-memory DB."""
    def __init__(self, c): self._c = c
    def __getattr__(self, k): return getattr(self._c, k)
    def close(self): pass


def _seed_conn(n=30):
    con = duckdb.connect(":memory:")
    cols = ["id INTEGER", "ticker VARCHAR", "date DATE",
            "open DOUBLE", "high DOUBLE", "low DOUBLE", "close DOUBLE"]
    cols += [f"{c} DOUBLE" for c in _FWD + _MFE + _MAE]
    cols += [f"{c} BOOLEAN" for c in _FLAGS]
    con.execute(f"CREATE TABLE bars ({', '.join(cols)})")
    # one ticker, close = 100, 101, ... (monotonic +1 → fwd_5d == ~ +5/(100+i))
    for i in range(n):
        c = 100.0 + i
        con.execute(
            "INSERT INTO bars (id, ticker, date, open, high, low, close) "
            "VALUES (?, 'AAPL', DATE '2026-01-01' + ?, ?, ?, ?, ?)",
            [i, i, c, c + 0.5, c - 0.5, c],
        )
    return con


def test_backfill_fills_null_tail_with_correct_formula(monkeypatch):
    con = _seed_conn(30)
    monkeypatch.setattr(bf, "get_conn", lambda read_only=False: _NoCloseConn(con))

    res = bf.backfill_forward_returns(lookback_days=10_000)
    assert res["updated"] > 0

    # row 0: close 100 → close[+5]=105 → fwd_5d = 5.0
    r0 = con.execute("SELECT fwd_5d FROM bars WHERE id = 0").fetchone()[0]
    assert r0 == pytest.approx(5.0, abs=1e-9)
    # row 10: close 110 → close[+5]=115 → (115/110-1)*100
    r10 = con.execute("SELECT fwd_5d FROM bars WHERE id = 10").fetchone()[0]
    assert r10 == pytest.approx((115 / 110 - 1) * 100, abs=1e-9)


def test_backfill_leaves_unresolved_tail_null():
    con = _seed_conn(30)
    bf.get_conn = lambda read_only=False: _NoCloseConn(con)  # type: ignore
    bf.backfill_forward_returns(lookback_days=10_000)
    # last bar has no +5 future → still NULL
    last = con.execute("SELECT fwd_5d FROM bars WHERE id = 29").fetchone()[0]
    assert last is None


def test_backfill_never_overwrites_existing(monkeypatch):
    con = _seed_conn(30)
    # pre-populate row 3 with a sentinel value
    con.execute("UPDATE bars SET fwd_5d = 999.0 WHERE id = 3")
    monkeypatch.setattr(bf, "get_conn", lambda read_only=False: _NoCloseConn(con))

    bf.backfill_forward_returns(lookback_days=10_000)
    # sentinel must be untouched (guard is fwd_5d IS NULL)
    keep = con.execute("SELECT fwd_5d FROM bars WHERE id = 3").fetchone()[0]
    assert keep == pytest.approx(999.0)


def test_select_exprs_matches_importer_formula():
    expr = bf._select_exprs()
    assert "(LEAD(close, 5) OVER w / close - 1) * 100 AS fwd_5d" in expr
    assert "ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING" in expr  # mfe/mae windowing
