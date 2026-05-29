"""Regression test for studio.seq_lab — the TZ Sequence Lab (Studio 'Seq Lab' tab).

Uses an isolated in-memory DuckDB so it never touches the real analytics file.
Verifies: returns baseline + ranked rows with the right shape; input clamping;
bad horizon falls back safely; and malicious universe input is neutralised.
"""
from __future__ import annotations

import os
import sys

import duckdb
import pytest

_backend = os.path.join(os.path.dirname(__file__), "..", "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)

import studio.seq_lab as sl


class _NoCloseConn:
    def __init__(self, c): self._c = c
    def __getattr__(self, k): return getattr(self._c, k)
    def close(self): pass


def _seed(n=60):
    con = duckdb.connect(":memory:")
    con.execute("""
        CREATE TABLE bars (
            ticker VARCHAR, date DATE, universe VARCHAR,
            open DOUBLE, close DOUBLE, mfe_20d DOUBLE,
            fwd_1d DOUBLE, fwd_5d DOUBLE, fwd_swing_ret_3 DOUBLE,
            t_sig VARCHAR, z_sig VARCHAR, wyc_phase VARCHAR
        )
    """)
    for i in range(n):
        up = (i % 2 == 0)                       # alternating up/down bars
        openp, close = (100.0, 101.0) if up else (101.0, 100.0)
        fwd = 1.0 if (i % 3 == 0) else -0.5     # deterministic forward
        con.execute(
            "INSERT INTO bars VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            ["AAA", f"2024-01-{(i % 27) + 1:02d}", "sp500", openp, close, 5.0,
             fwd, fwd, fwd, "T2G" if up else "", "" if up else "Z4",
             "MARKUP" if up else "MKDN"],
        )
    return con


def test_returns_baseline_and_rows(monkeypatch):
    con = _seed()
    monkeypatch.setattr(sl, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = sl.seq_lab(universe="sp500", n_bars=2, mode="color", horizon="fwd_1d", min_occ=1)
    assert set(r["baseline"]) >= {"n", "win", "avg_ret", "mfe20"}
    assert r["baseline"]["n"] > 0
    assert isinstance(r["rows"], list) and len(r["rows"]) > 0
    for row in r["rows"]:
        assert set(row) >= {"seq", "n", "win", "avg_ret", "mfe20"}
        assert 0 <= row["win"] <= 100


def test_clamps_inputs(monkeypatch):
    con = _seed()
    monkeypatch.setattr(sl, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = sl.seq_lab(universe="sp500", n_bars=99, limit=9999, min_occ=1)
    assert r["params"]["n_bars"] <= 6          # clamped
    # limit is clamped to <=100 internally; rows can't exceed it
    assert len(r["rows"]) <= 100


def test_bad_horizon_falls_back(monkeypatch):
    con = _seed()
    monkeypatch.setattr(sl, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = sl.seq_lab(universe="sp500", horizon="; DROP TABLE bars;--", n_bars=2, min_occ=1)
    assert r["params"]["horizon"] == "fwd_1d"   # whitelisted fallback, table intact


def test_injection_universe_neutralised(monkeypatch):
    con = _seed()
    monkeypatch.setattr(sl, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = sl.seq_lab(universe="x'; DROP TABLE bars;--", n_bars=2, min_occ=1)
    assert r["params"]["universe"] == "all"     # not whitelisted → dropped
    # table still queryable afterwards
    assert con.execute("SELECT COUNT(*) FROM bars").fetchone()[0] > 0


def test_f_maps_nan_inf_to_none():
    # guards the "Out of range float values are not JSON compliant" 500 bug:
    # DuckDB AVG over illiquid names can yield NaN/Inf which must become None.
    assert sl._f(float("nan")) is None
    assert sl._f(float("inf")) is None
    assert sl._f(float("-inf")) is None
    assert sl._f(None) is None
    assert sl._f(1.5) == 1.5
    assert sl._f(0) == 0.0


def test_by_phase_adds_phase_column(monkeypatch):
    con = _seed()
    monkeypatch.setattr(sl, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = sl.seq_lab(universe="sp500", n_bars=2, min_occ=1, by_phase=True)
    if r["rows"]:
        assert "wyc_phase" in r["rows"][0]
