"""Regression test for studio.playbook — the Playbook engine.

Isolated in-memory DuckDB with a hand-built price path so the backtest gate and
live-ticker matching are deterministic. Verifies: a profitable, both-halves-positive
setup PASSES and gets a live match; a setup that never triggers is REJECTED; the
gate logic itself; and that a malicious universe string is neutralised.
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

import studio.playbook as pb

# union of every flag the predefined setups reference
_FLAGS = ["rsi_le_35", "wyc_in_tr", "d_spring", "pb_stop_cause", "d_absorb_bull",
          "sig_vol_20x", "d_blast_bear_grn", "d_absorb_bear", "sig_vol_10x", "sig_sc",
          "d_upthrust", "wvf_spike"]


class _NoCloseConn:
    def __init__(self, c): self._c = c
    def __getattr__(self, k): return getattr(self._c, k)
    def close(self): pass


def _make_con():
    con = duckdb.connect(":memory:")
    cols = ", ".join(f"{f} INTEGER" for f in _FLAGS)
    con.execute(f"""CREATE TABLE bars (
        ticker VARCHAR, date DATE, universe VARCHAR,
        open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
        volume DOUBLE, avg_vol_20d DOUBLE, wyc_phase VARCHAR, {cols})""")
    return con


def _ins(con, ticker, d, o, h, l, c, phase, **flags):
    vals = [ticker, d, "sp500", o, h, l, c, 1e6, 1e6, phase]
    vals += [int(flags.get(f, 0)) for f in _FLAGS]
    qs = ",".join(["?"] * (10 + len(_FLAGS)))
    con.execute(f"INSERT INTO bars VALUES ({qs})", vals)


def _seed():
    con = _make_con()
    base = date(2024, 1, 1)
    # 40 "oversold dip in uptrend" trades, ~70% winners, spread over distinct dates
    # so the time-split has trades in BOTH halves.
    for i in range(40):
        win = (i % 10) < 7
        d0 = base + timedelta(days=3 * i)
        _ins(con, f"T{i:02d}", d0,                 100, 101, 99, 100, "MARKUP", rsi_le_35=1)  # trigger
        _ins(con, f"T{i:02d}", d0 + timedelta(1),  100, 102, 99, 101, "MARKUP")               # entry@100
        if win:
            _ins(con, f"T{i:02d}", d0 + timedelta(2), 101, 110, 100, 109, "MARKUP")           # +8% target
        else:
            _ins(con, f"T{i:02d}", d0 + timedelta(2), 101, 102, 95,  96,  "MARKUP")           # -4% stop
    # a ticker whose LATEST bar currently matches the setup (→ live watchlist)
    _ins(con, "LIVE", base,                49, 50, 48, 49, "NEUTRAL")
    _ins(con, "LIVE", base + timedelta(1), 50, 51, 49, 50, "MARKUP", rsi_le_35=1)
    return con


def test_playbook_passes_validated_setup(monkeypatch):
    con = _seed()
    monkeypatch.setattr(pb, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = pb.build_playbook(universe="sp500", min_trades=30, min_price=5, min_volume=100000)

    assert r["universe"] == "sp500"
    byid = {s["id"]: s for s in r["setups"]}
    dip = byid["bottom_rsi_dip_markup"]
    assert dip["passed"] is True
    ov = dip["backtest"]["overall"]
    assert ov["n"] == 40
    assert ov["expectancy"] > 0
    assert ov["profit_factor"] > 1
    # both halves positive (the out-of-sample sniff the gate enforces)
    assert dip["backtest"]["first_half"]["expectancy"] > 0
    assert dip["backtest"]["second_half"]["expectancy"] > 0
    # the live watchlist picked up the ticker whose latest bar matches
    assert "LIVE" in [t["ticker"] for t in dip["live_tickers"]]
    # only the one fully-firing setup should pass in this seed
    assert r["n_passed"] == 1
    # sampling-transparency fields flow through _run_on_df; the tiny seed is well
    # under the trade cap so nothing is sampled (truncated False, all 40 trades).
    assert dip["backtest"]["truncated"] is False
    assert dip["backtest"]["n_trades"] == 40
    assert dip["backtest"]["n_tickers"] == 40


def test_playbook_rejects_no_trigger_setup(monkeypatch):
    con = _seed()
    monkeypatch.setattr(pb, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = pb.build_playbook(universe="sp500", min_trades=30)
    byid = {s["id"]: s for s in r["setups"]}
    t = byid["top_upthrust_wvf"]      # those flags never fire in the seed
    assert t["passed"] is False
    assert t["reject_reason"]
    assert t["live_tickers"] == []


def test_gate_logic():
    good = {"overall": {"n": 50, "expectancy": 3.0, "profit_factor": 1.5},
            "first_half": {"n": 25, "expectancy": 2.0},
            "second_half": {"n": 25, "expectancy": 4.0}}
    assert pb._gate(good, 30)[0] is True
    assert pb._gate({**good, "overall": {"n": 50, "expectancy": 3.0, "profit_factor": None}}, 30)[0] is False
    assert pb._gate({**good, "overall": {"n": 50, "expectancy": -1.0, "profit_factor": 1.5}}, 30)[0] is False
    assert pb._gate({**good, "second_half": {"n": 25, "expectancy": -1.0}}, 30)[0] is False
    assert pb._gate(good, 100)[0] is False                       # too few trades
    assert pb._gate({"error": "no trades triggered"}, 30)[0] is False


def test_injection_universe_neutralised(monkeypatch):
    con = _seed()
    monkeypatch.setattr(pb, "get_conn", lambda read_only=True: _NoCloseConn(con))
    r = pb.build_playbook(universe="x'; DROP TABLE bars;--", min_trades=30)
    assert r["universe"] == "sp500"   # not whitelisted → safe default, no injection
    assert con.execute("SELECT COUNT(*) FROM bars").fetchone()[0] > 0
