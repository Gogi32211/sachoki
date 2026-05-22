"""Tests for 260523 filter helpers, Massive provider, and SuperChart fields.

Uses the pure analyzers.tz_wlnbb.filters_260523 module so tests can run
without FastAPI installed.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))


# ──────────────────────────────────────────────────────────────────────────
# Filter helpers
# ──────────────────────────────────────────────────────────────────────────

from analyzers.tz_wlnbb.filters_260523 import (
    apply_260523_filters,
    enrich_with_260523,
    parse_line5_tokens,
)


def test_apply_260523_filters_ad_fresh_only():
    rows = [
        {"ticker": "A", "ad_fresh": True,  "ad_cluster": False, "wyc_phase": "NEUTRAL"},
        {"ticker": "B", "ad_fresh": False, "ad_cluster": False, "wyc_phase": "NEUTRAL"},
        {"ticker": "C", "ad_fresh": True,  "ad_cluster": False, "wyc_phase": "MARKUP"},
    ]
    out = apply_260523_filters(rows, ad_fresh=True)
    assert {r["ticker"] for r in out} == {"A", "C"}


def test_apply_260523_filters_wyc_spring():
    rows = [
        {"ticker": "A", "wyc_phase": "SPRING"},
        {"ticker": "B", "wyc_phase": "MARKUP"},
        {"ticker": "C", "wyc_phase": "SPRING"},
    ]
    out = apply_260523_filters(rows, wyc_phase="SPRING")
    assert {r["ticker"] for r in out} == {"A", "C"}


def test_apply_260523_filters_ad_cluster_and_phase():
    rows = [
        {"ticker": "A", "ad_cluster": True,  "wyc_phase": "SPRING"},
        {"ticker": "B", "ad_cluster": True,  "wyc_phase": "MARKUP"},
        {"ticker": "C", "ad_cluster": False, "wyc_phase": "SPRING"},
    ]
    out = apply_260523_filters(rows, ad_cluster=True, wyc_phase="SPRING")
    assert {r["ticker"] for r in out} == {"A"}


def test_apply_260523_filters_no_op():
    rows = [{"ticker": "A"}, {"ticker": "B"}]
    out = apply_260523_filters(rows)
    assert len(out) == 2


# ──────────────────────────────────────────────────────────────────────────
# Enrichment helper — fills missing 260523 columns from stock_stat CSV
# ──────────────────────────────────────────────────────────────────────────

def test_enrich_with_260523_defaults_when_no_csv():
    rows = [{"ticker": "AAPL"}, {"ticker": "MSFT"}]
    # In test env, no stock_stat CSV exists in this dir — every row gets defaults
    out = enrich_with_260523(rows, "sp500", "1d")
    for r in out:
        assert r.get("ad_fresh", False) is False
        assert r.get("ad_cluster") is False
        assert r.get("wyc_phase") == "NEUTRAL"
        for k in ("wyc_spring", "wyc_sos", "wyc_acc_tr", "wyc_markup"):
            assert r.get(k) is False


# ──────────────────────────────────────────────────────────────────────────
# SuperChart line5 token parser
# ──────────────────────────────────────────────────────────────────────────

def test_parse_line5_tokens_vix_spike():
    out = parse_line5_tokens("VX-PB-R2X")
    assert out["wvf_spike"] is True
    assert out["vix_range"] is False
    assert out["psar_bull"] is True
    assert out["rsi2_token"] == "R2X"


def test_parse_line5_tokens_empty():
    out = parse_line5_tokens("")
    assert out == {"wvf_spike": False, "vix_range": False,
                   "psar_bull": False, "rsi2_token": ""}


def test_parse_line5_tokens_vix_range():
    out = parse_line5_tokens("VR-PS-R2H")
    assert out["wvf_spike"] is False
    assert out["vix_range"] is True
    assert out["psar_bull"] is False
    assert out["rsi2_token"] == "R2H"


# ──────────────────────────────────────────────────────────────────────────
# Massive.com provider — fallback to yfinance on missing key
# ──────────────────────────────────────────────────────────────────────────

def test_massive_no_api_key_returns_fallback_shape():
    os.environ.pop("MASSIVE_API_KEY", None)
    import importlib
    import data_massive
    importlib.reload(data_massive)

    info = data_massive.get_ticker_info_massive("AAPL")
    expected_keys = {
        "ticker", "name", "sector", "industry", "market_cap",
        "float_shares", "avg_volume_30d", "exchange",
        "description", "logo_url", "source",
    }
    assert expected_keys.issubset(info.keys())
    assert info["ticker"] == "AAPL"
    assert info["source"] in {"yfinance", "error", "cache"}


def test_massive_cache_round_trip():
    import importlib
    import data_massive
    importlib.reload(data_massive)
    data_massive._cache_set("CACHED", {"ticker": "CACHED", "name": "Cached Co",
                                       "source": "massive"})
    out = data_massive.get_ticker_info_massive("CACHED")
    assert out["ticker"] == "CACHED"
    assert out["name"] == "Cached Co"
