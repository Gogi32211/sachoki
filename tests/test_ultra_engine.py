"""Tests for the ULTRA screener aggregator (backend/ultra_engine.py).

ULTRA is a read-only signal aggregation: it must NOT introduce a new score,
category, or context flag, and must NOT crash when individual sources are
missing.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import patch

import pytest

# Ensure backend/ is importable (mirrors the other test files).
_backend = os.path.join(os.path.dirname(__file__), "..", "backend")
if _backend not in sys.path:
    sys.path.insert(0, _backend)

from ultra_engine import (
    _extract_turbo_signals,
    _project_pullback,
    _project_rare,
    _project_tz_intel,
    _project_tz_wlnbb,
    _safe_float,
    run_ultra_scan,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _turbo_row(ticker="AAPL", score=42.0, tz_bull=1, **extra) -> dict:
    base = {
        "ticker": ticker,
        "turbo_score": score,
        "tz_bull": tz_bull,
        "last_price": 150.0,
        "avg_vol": 1_000_000.0,
        # a couple of truthy boolean signals
        "best_sig": 1,
        "tz_bull_flip": 1,
        "buy_2809": 0,
    }
    base.update(extra)
    return base


# ── Unit-level: signal extraction never invents new fields ────────────────────

def test_extract_turbo_signals_filters_zero_and_empty():
    row = {
        "best_sig": 1, "buy_2809": 0,
        "rocket": True, "fly_abcd": False,
        "vol_spike_5x": "0", "vol_spike_10x": "1",
        "tz_bull_flip": "True", "predn50": "",
        "ultra_3up": None,
    }
    sigs = _extract_turbo_signals(row)
    # Truthy: best_sig, rocket, vol_spike_10x, tz_bull_flip
    assert "best_sig" in sigs
    assert "rocket" in sigs
    assert "vol_spike_10x" in sigs
    assert "tz_bull_flip" in sigs
    # Falsy variants must be dropped
    assert "buy_2809"     not in sigs
    assert "fly_abcd"     not in sigs
    assert "vol_spike_5x" not in sigs
    assert "predn50"      not in sigs
    assert "ultra_3up"    not in sigs


def test_safe_float_handles_nan_inf_and_strings():
    assert _safe_float(None) == 0.0
    assert _safe_float("") == 0.0
    assert _safe_float(float("nan"), default=7.0) == 7.0
    assert _safe_float(float("inf"), default=7.0) == 7.0
    assert _safe_float("12.5") == 12.5


def test_projections_only_contain_documented_keys():
    """Projections must NOT add ultra_score, ultra_category, ultra_context_score."""
    forbidden = {"ultra_score", "ultra_context_score", "ultra_category"}
    for proj in (
        _project_tz_wlnbb({"t_signal": "T1"}),
        _project_tz_intel({"role": "BULL_A", "score": 10}),
        _project_pullback({"evidence_tier": "CONFIRMED_PULLBACK"}),
        _project_rare({"evidence_tier": "CONFIRMED_RARE", "base4_key": "abcd"}),
    ):
        assert forbidden.isdisjoint(proj.keys()), proj


# ── End-to-end: run_ultra_scan must never crash on missing sources ────────────

def _stub_get_turbo_results(*_a, **_kw):
    return [_turbo_row("AAPL"), _turbo_row("MSFT", score=10.0, tz_bull=0)]


def _stub_last_scan(*_a, **_kw):
    return "2026-05-08T00:00:00"


def test_ultra_scan_with_only_turbo_data(monkeypatch):
    """Test 1: /api/ultra-scan works with Turbo-only data (other sources missing)."""
    monkeypatch.setattr(
        "turbo_engine.get_turbo_results", _stub_get_turbo_results, raising=True
    )
    monkeypatch.setattr(
        "turbo_engine.get_last_turbo_scan_time", _stub_last_scan, raising=True
    )

    # Force every other source to fail
    def _missing_csv(*_a, **_kw):
        raise FileNotFoundError("stock_stat_tz_wlnbb CSV not found")
    monkeypatch.setattr("ultra_engine._load_tz_wlnbb_latest", _missing_csv)
    monkeypatch.setattr(
        "tz_intelligence.scanner.run_intelligence_scan",
        lambda **_kw: {"results": [], "error": "stock_stat missing"},
    )
    monkeypatch.setattr(
        "analyzers.pullback_miner.miner.run_pullback_scan",
        lambda **_kw: {"results": [], "error": "stock_stat CSV missing"},
    )
    monkeypatch.setattr(
        "analyzers.rare_reversal.miner.run_rare_reversal_scan",
        lambda **_kw: {"results": [], "error": "stock_stat CSV missing"},
    )

    resp = run_ultra_scan(universe="sp500", tf="1d")
    assert isinstance(resp, dict)
    # response is partial — turbo ok, others recorded as warnings
    assert resp["meta"]["sources"]["turbo"]["ok"] is True
    assert resp["meta"]["sources"]["pullback"]["ok"]      is False
    assert resp["meta"]["sources"]["rare_reversal"]["ok"] is False
    assert any("Pullback Miner unavailable"  in w for w in resp["warnings"])
    assert any("Rare Reversal unavailable"   in w for w in resp["warnings"])
    assert any("TZ/WLNBB unavailable"        in w for w in resp["warnings"])
    # Turbo rows still merged through
    tickers = [r["ticker"] for r in resp["results"]]
    assert "AAPL" in tickers and "MSFT" in tickers


def test_missing_pullback_csv_does_not_crash_response(monkeypatch):
    """Test 2: missing Pullback Miner CSV → graceful warning, not 500."""
    monkeypatch.setattr(
        "turbo_engine.get_turbo_results", _stub_get_turbo_results, raising=True
    )
    monkeypatch.setattr(
        "turbo_engine.get_last_turbo_scan_time", _stub_last_scan, raising=True
    )
    monkeypatch.setattr(
        "ultra_engine._load_tz_wlnbb_latest", lambda *a, **kw: {}
    )
    monkeypatch.setattr(
        "tz_intelligence.scanner.run_intelligence_scan",
        lambda **_kw: {"results": []},
    )
    monkeypatch.setattr(
        "analyzers.pullback_miner.miner.run_pullback_scan",
        lambda **_kw: {"results": [], "error": "stock_stat CSV missing"},
    )
    monkeypatch.setattr(
        "analyzers.rare_reversal.miner.run_rare_reversal_scan",
        lambda **_kw: {"results": []},
    )

    resp = run_ultra_scan(universe="sp500", tf="1d")
    assert resp["meta"]["sources"]["pullback"]["ok"] is False
    assert any("Pullback Miner unavailable" in w for w in resp["warnings"])
    # Other sources unaffected
    assert resp["meta"]["sources"]["turbo"]["ok"]         is True
    assert resp["meta"]["sources"]["rare_reversal"]["ok"] is True


def test_missing_rare_reversal_csv_does_not_crash_response(monkeypatch):
    """Test 3: missing Rare Reversal CSV → graceful warning, not 500."""
    monkeypatch.setattr(
        "turbo_engine.get_turbo_results", _stub_get_turbo_results, raising=True
    )
    monkeypatch.setattr(
        "turbo_engine.get_last_turbo_scan_time", _stub_last_scan, raising=True
    )
    monkeypatch.setattr(
        "ultra_engine._load_tz_wlnbb_latest", lambda *a, **kw: {}
    )
    monkeypatch.setattr(
        "tz_intelligence.scanner.run_intelligence_scan",
        lambda **_kw: {"results": []},
    )
    monkeypatch.setattr(
        "analyzers.pullback_miner.miner.run_pullback_scan",
        lambda **_kw: {"results": []},
    )
    # Raise inside the miner — exception path (not just error string)
    def _boom(**_kw):
        raise FileNotFoundError("stock_stat_tz_wlnbb CSV missing")
    monkeypatch.setattr(
        "analyzers.rare_reversal.miner.run_rare_reversal_scan", _boom
    )

    resp = run_ultra_scan(universe="sp500", tf="1d")
    assert resp["meta"]["sources"]["rare_reversal"]["ok"] is False
    assert any("Rare Reversal unavailable" in w for w in resp["warnings"])


def test_response_contains_no_ultra_score_or_category(monkeypatch):
    """Test 4: no ultra_score, ultra_context_score, or ultra_category fields."""
    monkeypatch.setattr(
        "turbo_engine.get_turbo_results", _stub_get_turbo_results, raising=True
    )
    monkeypatch.setattr(
        "turbo_engine.get_last_turbo_scan_time", _stub_last_scan, raising=True
    )
    monkeypatch.setattr(
        "ultra_engine._load_tz_wlnbb_latest",
        lambda *a, **kw: {"AAPL": {"ticker": "AAPL", "t_signal": "T1",
                                   "z_signal": "Z2", "l_signal": "",
                                   "preup_signal": "", "predn_signal": "",
                                   "lane1_label": "L1", "lane3_label": "",
                                   "volume_bucket": "high", "wick_suffix": ""}},
    )
    monkeypatch.setattr(
        "tz_intelligence.scanner.run_intelligence_scan",
        lambda **_kw: {"results": [{
            "ticker": "AAPL", "role": "BULL_A", "score": 12,
            "quality": "A", "action": "buy",
            "abr_category": "A", "abr_med10d_pct": 5.0, "abr_fail10d_pct": 20.0,
            "matched_status": "ANECDOTAL", "matched_med10d_pct": 4.0,
            "matched_fail10d_pct": 25.0,
        }]},
    )
    monkeypatch.setattr(
        "analyzers.pullback_miner.miner.run_pullback_scan",
        lambda **_kw: {"results": [{
            "ticker": "AAPL", "evidence_tier": "CONFIRMED_PULLBACK",
            "pullback_stage": "STAGE_2", "pattern_key": "abcd",
            "score": 7.5, "median_10d_return": 3.0, "win_rate_10d": 60.0,
            "fail_rate_10d": 30.0, "is_currently_active": True,
        }]},
    )
    monkeypatch.setattr(
        "analyzers.rare_reversal.miner.run_rare_reversal_scan",
        lambda **_kw: {"results": [{
            "ticker": "AAPL", "evidence_tier": "CONFIRMED_RARE",
            "base4_key": "wxyz", "extended5_key": "vwxyz",
            "extended6_key": None, "pattern_length": 5, "score": 6.0,
            "median_10d_return": 8.0, "fail_rate_10d": 15.0,
            "is_currently_active": False, "current_pattern_completion": 0.5,
        }]},
    )

    resp = run_ultra_scan(universe="sp500", tf="1d")
    forbidden = {"ultra_score", "ultra_context_score", "ultra_category"}

    def _check(d):
        if isinstance(d, dict):
            assert forbidden.isdisjoint(d.keys()), f"forbidden key in {d.keys()}"
            for v in d.values():
                _check(v)
        elif isinstance(d, list):
            for v in d:
                _check(v)

    _check(resp)
    # Source flags should reflect everything succeeded for AAPL
    aapl = next(r for r in resp["results"] if r["ticker"] == "AAPL")
    assert aapl["source_flags"] == {
        "has_turbo": True, "has_tz_wlnbb": True, "has_tz_intel": True,
        "has_pullback": True, "has_rare_reversal": True,
    }


# ── Existing endpoints' modules should still load and run ────────────────────

def test_existing_turbo_engine_module_still_imports():
    """Test 5: existing /api/turbo-scan handler still works (function importable)."""
    import turbo_engine
    assert hasattr(turbo_engine, "get_turbo_results")
    assert hasattr(turbo_engine, "get_last_turbo_scan_time")


def test_existing_tz_intelligence_module_still_imports():
    """Test 6: existing /api/tz-intelligence/scan still works."""
    from tz_intelligence import scanner
    assert hasattr(scanner, "run_intelligence_scan")


def test_existing_tz_wlnbb_endpoint_handler_still_callable():
    """Test 7: existing /api/tz-wlnbb/scan still works. It reads stock_stat CSV
    directly inside main.api_tz_wlnbb_scan, so we just verify the handler is
    importable and callable. With no CSV present it returns an error dict
    rather than raising."""
    from main import api_tz_wlnbb_scan
    resp = api_tz_wlnbb_scan(universe="__nonexistent__", tf="1d")
    assert isinstance(resp, dict)
    assert "results" in resp
