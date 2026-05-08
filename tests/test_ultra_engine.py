"""Tests for the ULTRA orchestrator (backend/ultra_orchestrator.py).

ULTRA is a *display-only* layer over Turbo. It must:
  • not introduce any new score, category, or context flag
  • not crash when individual sources are missing
  • show Turbo-only rows when secondary sources fail
  • expose the same Turbo score/category fields the canonical Turbo endpoint does
"""
from __future__ import annotations

import os
import sys
from typing import Any

import pytest

# Ensure backend/ is importable (mirrors the other test files).
_backend = os.path.join(os.path.dirname(__file__), "..", "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)

import ultra_orchestrator as uo


# ── Helpers ──────────────────────────────────────────────────────────────────

def _turbo_row(ticker="AAPL", score=42.0, tz_bull=1, **extra) -> dict:
    base = {
        "ticker": ticker,
        "turbo_score": score,
        "turbo_score_n3":  score, "turbo_score_n5":  score, "turbo_score_n10": score,
        "tz_bull": tz_bull,
        "last_price": 150.0,
        "avg_vol": 1_000_000.0,
        "rsi": 55.0, "cci": 45.0,
        "profile_score": 7,
        "profile_category": "BUILDING",
        "profile_name": "demo",
        "best_sig": 1,
    }
    base.update(extra)
    return base


def _stub_turbo_phase(monkeypatch, rows=None):
    rows = rows or [_turbo_row("AAPL"), _turbo_row("MSFT", score=10.0, tz_bull=0)]

    def fake_run_turbo_scan(*_a, **_kw):
        return len(rows)

    def fake_get_turbo_results(*_a, **_kw):
        return list(rows)

    def fake_get_last_turbo_scan_time(*_a, **_kw):
        return "2026-05-08T07:00:00"

    def fake_get_turbo_progress(*_a, **_kw):
        return {"done": len(rows), "total": len(rows)}

    monkeypatch.setattr("turbo_engine.run_turbo_scan",          fake_run_turbo_scan)
    monkeypatch.setattr("turbo_engine.get_turbo_results",       fake_get_turbo_results)
    monkeypatch.setattr("turbo_engine.get_last_turbo_scan_time", fake_get_last_turbo_scan_time)
    monkeypatch.setattr("turbo_engine.get_turbo_progress",       fake_get_turbo_progress)
    return rows


def _stub_stock_stat_already_exists(monkeypatch):
    """Force the orchestrator to think the stock_stat CSV is present (so it
    doesn't actually try to fetch market data in tests)."""
    monkeypatch.setattr(
        uo, "_resolve_tz_wlnbb_csv",
        lambda universe, tf, nasdaq_batch="": "/tmp/stub_stock_stat.csv",
    )


def _empty_secondary(monkeypatch):
    monkeypatch.setattr(uo, "_read_tz_wlnbb_latest", lambda *a, **kw: {})
    monkeypatch.setattr(
        "tz_intelligence.scanner.run_intelligence_scan",
        lambda **_kw: {"results": [], "error": "stock_stat CSV missing"},
    )
    monkeypatch.setattr(
        "analyzers.pullback_miner.miner.run_pullback_scan",
        lambda **_kw: {"results": [], "error": "stock_stat CSV missing"},
    )
    monkeypatch.setattr(
        "analyzers.rare_reversal.miner.run_rare_reversal_scan",
        lambda **_kw: {"results": [], "error": "stock_stat CSV missing"},
    )


# ── Test 1 — ULTRA with only Turbo available still shows Turbo rows ──────────

def test_ultra_only_turbo_rows_still_show(monkeypatch):
    rows = _stub_turbo_phase(monkeypatch)
    _stub_stock_stat_already_exists(monkeypatch)
    _empty_secondary(monkeypatch)

    resp = uo.run_ultra_scan_job(universe="sp500", tf="1d")
    tickers = [r["ticker"] for r in resp["results"]]
    assert tickers == [r["ticker"] for r in rows]
    # Source flags reflect missing secondaries
    for r in resp["results"]:
        flags = r["ultra_sources"]
        assert flags["has_turbo"] is True
        assert flags["has_tz_wlnbb"]      is False
        assert flags["has_tz_intel"]      is False
        assert flags["has_pullback"]      is False
        assert flags["has_rare_reversal"] is False
    assert resp["meta"]["sources"]["turbo"]["ok"] is True
    assert resp["meta"]["sources"]["pullback"]["ok"]      is False
    assert resp["meta"]["sources"]["rare_reversal"]["ok"] is False
    # Warnings list captures secondary failures
    assert any("Pullback Miner unavailable" in w for w in resp["warnings"])
    assert any("Rare Reversal unavailable"  in w for w in resp["warnings"])


# ── Test 2 — orchestrator triggers stock_stat generation when CSV missing ────

def test_ultra_triggers_stock_stat_when_missing(monkeypatch):
    _stub_turbo_phase(monkeypatch)

    # Simulate: CSV missing on first lookup, present after generation
    state = {"exists": False}

    def fake_resolve(universe, tf, nasdaq_batch=""):
        return "/tmp/stub_stock_stat.csv" if state["exists"] else None
    monkeypatch.setattr(uo, "_resolve_tz_wlnbb_csv", fake_resolve)

    calls: list = []

    def fake_generate_stock_stat(tickers, fetch, **kwargs):
        calls.append({"tickers": list(tickers), "kwargs": kwargs})
        state["exists"] = True
        return kwargs.get("output_path"), {}

    # Patch the symbol the orchestrator imports inside _phase_stock_stat
    import analyzers.tz_wlnbb.stock_stat as _ss
    monkeypatch.setattr(_ss, "generate_stock_stat", fake_generate_stock_stat)

    # Avoid touching real ticker source / market data
    import scanner as _scanner
    monkeypatch.setattr(_scanner, "get_universe_tickers", lambda u: ["AAPL", "MSFT"])
    monkeypatch.setattr("data_polygon.polygon_available", lambda: False)
    import data as _data
    monkeypatch.setattr(_data, "fetch_ohlcv", lambda *a, **kw: None)

    _empty_secondary(monkeypatch)

    resp = uo.run_ultra_scan_job(universe="sp500", tf="1d")
    assert calls, "stock_stat generation must be triggered when CSV missing"
    assert state["exists"] is True
    # Phase status is exposed
    status = uo.get_ultra_status()
    assert status["phases"]["stock_stat"]["state"] == "ok"


# ── Test 3 — enrichments appear once secondary sources return data ────────────

def test_enrichments_appear_when_sources_return_data(monkeypatch):
    _stub_turbo_phase(monkeypatch)
    _stub_stock_stat_already_exists(monkeypatch)

    monkeypatch.setattr(
        uo, "_read_tz_wlnbb_latest",
        lambda *a, **kw: {"AAPL": {
            "ticker": "AAPL", "t_signal": "T4", "z_signal": "",
            "l_signal": "L34", "preup_signal": "P89", "predn_signal": "",
            "lane1_label": "lane1", "lane3_label": "lane3",
            "volume_bucket": "B", "wick_suffix": "H",
        }},
    )
    monkeypatch.setattr(
        "tz_intelligence.scanner.run_intelligence_scan",
        lambda **_kw: {"results": [{
            "ticker": "AAPL", "role": "PULLBACK_GO", "quality": "A",
            "action": "BUY_TRIGGER", "score": 72,
            "matched_status": "GOOD", "matched_med10d_pct": 0.65,
            "matched_fail10d_pct": 20.5,
            "abr_category": "B+", "abr_med10d_pct": 0.8,
            "abr_fail10d_pct": 18.2, "abr_context_type": "ctx",
            "abr_action_hint": "buy", "abr_conflict_flag": False,
            "abr_confirmation_flag": True,
        }]},
    )
    monkeypatch.setattr(
        "analyzers.pullback_miner.miner.run_pullback_scan",
        lambda **_kw: {"results": [{
            "ticker": "AAPL", "evidence_tier": "CONFIRMED_PULLBACK",
            "pullback_stage": "PULLBACK_GO", "pattern_key": "Z5|T3|Z9|T4",
            "pattern_length": 4, "score": 64.3,
            "median_10d_return": 1.2, "win_rate_10d": 55.0,
            "fail_rate_10d": 21.0, "is_currently_active": True,
            "current_pattern_completion": "FULL_MATCH",
        }]},
    )
    monkeypatch.setattr(
        "analyzers.rare_reversal.miner.run_rare_reversal_scan",
        lambda **_kw: {"results": [{
            "ticker": "AAPL", "evidence_tier": "CONFIRMED_RARE",
            "base4_key": "wxyz", "extended5_key": "vwxyz",
            "extended6_key": None, "pattern_length": 5, "score": 58.2,
            "median_10d_return": 1.1, "fail_rate_10d": 22.0,
            "is_currently_active": True, "current_pattern_completion": 1.0,
        }]},
    )

    resp = uo.run_ultra_scan_job(universe="sp500", tf="1d")
    aapl = next(r for r in resp["results"] if r["ticker"] == "AAPL")
    msft = next(r for r in resp["results"] if r["ticker"] == "MSFT")

    assert aapl["ultra_sources"] == {
        "has_turbo": True, "has_tz_wlnbb": True, "has_tz_intel": True,
        "has_pullback": True, "has_rare_reversal": True,
    }
    assert aapl["tz_wlnbb"]["t_signal"] == "T4"
    assert aapl["tz_intel"]["role"]     == "PULLBACK_GO"
    assert aapl["abr"]["category"]      == "B+"
    assert aapl["pullback"]["evidence_tier"] == "CONFIRMED_PULLBACK"
    assert aapl["rare_reversal"]["evidence_tier"] == "CONFIRMED_RARE"

    # MSFT didn't have any secondary data — still in results, all enrichments None
    assert msft["tz_wlnbb"]      is None
    assert msft["tz_intel"]      is None
    assert msft["abr"]           is None
    assert msft["pullback"]      is None
    assert msft["rare_reversal"] is None
    assert msft["ultra_sources"]["has_turbo"] is True


# ── Test 4 — missing secondary source produces a warning, not empty results ──

def test_missing_secondary_produces_warning_not_empty(monkeypatch):
    _stub_turbo_phase(monkeypatch)
    _stub_stock_stat_already_exists(monkeypatch)

    # tz_wlnbb / tz_intel ok; pullback / rare reversal raise
    monkeypatch.setattr(uo, "_read_tz_wlnbb_latest", lambda *a, **kw: {})
    monkeypatch.setattr(
        "tz_intelligence.scanner.run_intelligence_scan",
        lambda **_kw: {"results": []},
    )

    def _boom_pb(**_kw):
        raise FileNotFoundError("stock_stat CSV missing — pullback")
    def _boom_rr(**_kw):
        raise RuntimeError("rare reversal exploded")
    monkeypatch.setattr("analyzers.pullback_miner.miner.run_pullback_scan",       _boom_pb)
    monkeypatch.setattr("analyzers.rare_reversal.miner.run_rare_reversal_scan",   _boom_rr)

    resp = uo.run_ultra_scan_job(universe="sp500", tf="1d")
    assert len(resp["results"]) == 2  # Turbo rows still present
    assert any("Pullback Miner unavailable" in w for w in resp["warnings"])
    assert any("Rare Reversal unavailable"  in w for w in resp["warnings"])
    assert resp["meta"]["sources"]["pullback"]["ok"]      is False
    assert resp["meta"]["sources"]["rare_reversal"]["ok"] is False


# ── Test 5 — Turbo score/category in ULTRA exactly match Turbo ───────────────

def test_turbo_score_and_category_pass_through_unchanged(monkeypatch):
    rows = _stub_turbo_phase(monkeypatch, rows=[
        _turbo_row("AAPL", score=88.5, profile_category="SWEET_SPOT"),
        _turbo_row("MSFT", score=12.0, profile_category="WATCH"),
    ])
    _stub_stock_stat_already_exists(monkeypatch)
    _empty_secondary(monkeypatch)

    resp = uo.run_ultra_scan_job(universe="sp500", tf="1d")
    by_ticker = {r["ticker"]: r for r in resp["results"]}

    for src in rows:
        out = by_ticker[src["ticker"]]
        # Canonical Turbo fields must be byte-identical, no recalculation
        assert out["turbo_score"]      == src["turbo_score"]
        assert out["profile_category"] == src["profile_category"]
        assert out["profile_score"]    == src["profile_score"]
        assert out["tz_bull"]          == src["tz_bull"]
        assert out["last_price"]       == src["last_price"]

    # No new score / category fields anywhere in the response
    forbidden = {"ultra_score", "ultra_context_score", "ultra_category"}

    def _check(d):
        if isinstance(d, dict):
            assert forbidden.isdisjoint(d.keys()), f"forbidden key in {sorted(d.keys())}"
            for v in d.values():
                _check(v)
        elif isinstance(d, list):
            for v in d:
                _check(v)

    _check(resp)


# ── Phase 1 / Phase 2 must execute in parallel, not strictly sequentially ────

def test_phase1_runs_turbo_and_stock_stat_in_parallel(monkeypatch):
    """Turbo and stock_stat generation must overlap inside Phase 1."""
    import threading
    import time as _t

    overlap_seen = {"v": False}
    in_flight    = {"turbo": False, "stock_stat": False}
    lock         = threading.Lock()

    def _mark(name: str, on: bool) -> None:
        with lock:
            in_flight[name] = on
            if in_flight["turbo"] and in_flight["stock_stat"]:
                overlap_seen["v"] = True

    def slow_turbo(*_a, **_kw):
        _mark("turbo", True)
        _t.sleep(0.20)
        _mark("turbo", False)
        return 1

    def slow_generate_stock_stat(tickers, fetch, **kwargs):
        _mark("stock_stat", True)
        _t.sleep(0.20)
        _mark("stock_stat", False)
        return kwargs.get("output_path"), {}

    # Stub Turbo entry points
    monkeypatch.setattr("turbo_engine.run_turbo_scan", slow_turbo)
    monkeypatch.setattr("turbo_engine.get_turbo_results",
                         lambda *a, **kw: [_turbo_row("AAPL")])
    monkeypatch.setattr("turbo_engine.get_last_turbo_scan_time",
                         lambda *a, **kw: "2026-05-08T07:00:00")
    monkeypatch.setattr("turbo_engine.get_turbo_progress",
                         lambda *a, **kw: {"done": 1, "total": 1})

    # CSV starts missing → forces stock_stat generation path
    state = {"exists": False}
    monkeypatch.setattr(
        uo, "_resolve_tz_wlnbb_csv",
        lambda u, tf, nasdaq_batch="": "/tmp/stub.csv" if state["exists"] else None,
    )
    import analyzers.tz_wlnbb.stock_stat as _ss

    def fake_gen(tickers, fetch, **kwargs):
        path, audit = slow_generate_stock_stat(tickers, fetch, **kwargs)
        state["exists"] = True
        return path, audit
    monkeypatch.setattr(_ss, "generate_stock_stat", fake_gen)

    import scanner as _scanner
    monkeypatch.setattr(_scanner, "get_universe_tickers", lambda u: ["AAPL"])
    monkeypatch.setattr("data_polygon.polygon_available", lambda: False)
    import data as _data
    monkeypatch.setattr(_data, "fetch_ohlcv", lambda *a, **kw: None)

    _empty_secondary(monkeypatch)

    uo.run_ultra_scan_job(universe="sp500", tf="1d")

    assert overlap_seen["v"], (
        "Turbo and stock_stat generation must run concurrently in Phase 1"
    )


def test_phase2_runs_secondaries_in_parallel(monkeypatch):
    """The four Phase 2 readers must execute concurrently, not back-to-back."""
    import threading
    import time as _t

    _stub_turbo_phase(monkeypatch)
    _stub_stock_stat_already_exists(monkeypatch)

    in_flight     = {"a": 0, "max": 0}
    lock          = threading.Lock()

    def _enter() -> None:
        with lock:
            in_flight["a"] += 1
            if in_flight["a"] > in_flight["max"]:
                in_flight["max"] = in_flight["a"]

    def _exit() -> None:
        with lock:
            in_flight["a"] -= 1

    def slow(_resp):
        _enter(); _t.sleep(0.10); _exit()
        return _resp

    monkeypatch.setattr(uo, "_read_tz_wlnbb_latest",
                         lambda *a, **kw: slow({}))
    monkeypatch.setattr("tz_intelligence.scanner.run_intelligence_scan",
                         lambda **_kw: slow({"results": []}))
    monkeypatch.setattr("analyzers.pullback_miner.miner.run_pullback_scan",
                         lambda **_kw: slow({"results": []}))
    monkeypatch.setattr("analyzers.rare_reversal.miner.run_rare_reversal_scan",
                         lambda **_kw: slow({"results": []}))

    uo.run_ultra_scan_job(universe="sp500", tf="1d", max_workers=4)
    assert in_flight["max"] >= 2, (
        f"Phase 2 should run at least 2 readers concurrently "
        f"(saw max={in_flight['max']})"
    )


def test_phase2_pending_state_and_merge_phase_present(monkeypatch):
    """Status surface must include Phase 2 pending pills + merge phase."""
    _stub_turbo_phase(monkeypatch)
    _stub_stock_stat_already_exists(monkeypatch)
    _empty_secondary(monkeypatch)
    uo.run_ultra_scan_job(universe="sp500", tf="1d")
    status = uo.get_ultra_status()
    # merge phase reaches 'ok' once results are merged
    assert status["phases"]["merge"]["state"] == "ok"
    # Phase 2 phases moved from 'pending' to a terminal state
    for k in ("tz_wlnbb", "tz_intelligence", "pullback", "rare_reversal"):
        assert status["phases"][k]["state"] in ("ok", "skipped", "error")


# ── Test 6 — existing Turbo endpoint module still imports / behaves ──────────

def test_existing_turbo_modules_still_work():
    import turbo_engine
    assert hasattr(turbo_engine, "run_turbo_scan")
    assert hasattr(turbo_engine, "get_turbo_results")
    assert hasattr(turbo_engine, "get_last_turbo_scan_time")
    assert hasattr(turbo_engine, "get_turbo_progress")


# ── Test 7 — TZ/WLNBB, TZ Intel, Pullback, Rare endpoints still untouched ────

def test_existing_secondary_endpoints_still_callable():
    from main import (
        api_tz_wlnbb_scan, api_tz_intelligence_scan,
        api_pullback_miner_scan, api_rare_reversal_scan,
    )
    # With no CSVs in cwd these return error/empty payloads — the contract is
    # that they still return a dict and don't throw.
    r = api_tz_wlnbb_scan(universe="__nonexistent__", tf="1d")
    assert isinstance(r, dict) and "results" in r
    r = api_tz_intelligence_scan(universe="sp500", tf="1d")
    assert isinstance(r, dict) and "results" in r
    r = api_pullback_miner_scan(universe="sp500", tf="1d")
    assert isinstance(r, dict)
    r = api_rare_reversal_scan(universe="sp500", tf="1d")
    assert isinstance(r, dict)


# ── Pristine ultra_engine module: signal-engine functions intact, no ULTRA
#    aggregator code added. Existing code paths (gog_engine, signal_stats_engine,
#    main.api_signals via compute_260308_l88, turbo_engine.compute_ultra_v2)
#    must keep working unchanged.

def test_ultra_engine_module_signal_engines_pristine():
    import ultra_engine
    assert hasattr(ultra_engine, "compute_260308_l88")
    assert hasattr(ultra_engine, "compute_ultra_v2")
    # No aggregator function leaked into ultra_engine
    assert not hasattr(ultra_engine, "run_ultra_scan")
    assert not hasattr(ultra_engine, "_load_turbo_block")
