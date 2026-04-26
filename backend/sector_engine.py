"""
sector_engine.py — U.S. Sector ETF analysis engine.
"""
from __future__ import annotations

import time
from typing import Optional

import numpy as np
import pandas as pd

from data import fetch_ohlcv

# ── Universe ───────────────────────────────────────────────────────────────────
SECTORS: dict[str, str] = {
    "XLC":  "Communication Services",
    "XLY":  "Consumer Discretionary",
    "XLP":  "Consumer Staples",
    "XLE":  "Energy",
    "XLF":  "Financials",
    "XLV":  "Health Care",
    "XLI":  "Industrials",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    "XLK":  "Technology",
    "XLU":  "Utilities",
}

BENCHMARKS: dict[str, str] = {
    "SPY": "S&P 500",
    "QQQ": "Nasdaq 100",
    "IWM": "Russell 2000",
    "DIA": "Dow Jones",
}

GROWTH    = {"XLK", "XLC", "XLY", "XLI"}
DEFENSIVE = {"XLU", "XLP", "XLV", "XLRE"}

# ── Static holdings fallback ───────────────────────────────────────────────────
HOLDINGS: dict[str, list[dict]] = {
    "XLC":  [
        {"symbol": "META",  "name": "Meta Platforms",      "weight": 23.1},
        {"symbol": "GOOGL", "name": "Alphabet A",          "weight": 12.5},
        {"symbol": "GOOG",  "name": "Alphabet C",          "weight": 10.8},
        {"symbol": "NFLX",  "name": "Netflix",             "weight": 4.9},
        {"symbol": "T",     "name": "AT&T",                "weight": 4.5},
        {"symbol": "TMUS",  "name": "T-Mobile US",         "weight": 4.2},
    ],
    "XLY":  [
        {"symbol": "AMZN",  "name": "Amazon",              "weight": 24.2},
        {"symbol": "TSLA",  "name": "Tesla",               "weight": 14.3},
        {"symbol": "HD",    "name": "Home Depot",          "weight": 8.1},
        {"symbol": "MCD",   "name": "McDonald's",          "weight": 4.2},
        {"symbol": "NKE",   "name": "Nike",                "weight": 3.1},
        {"symbol": "LOW",   "name": "Lowe's",              "weight": 3.0},
    ],
    "XLP":  [
        {"symbol": "PG",    "name": "Procter & Gamble",    "weight": 16.2},
        {"symbol": "KO",    "name": "Coca-Cola",           "weight": 10.1},
        {"symbol": "PEP",   "name": "PepsiCo",             "weight": 9.8},
        {"symbol": "WMT",   "name": "Walmart",             "weight": 9.2},
        {"symbol": "COST",  "name": "Costco",              "weight": 8.7},
        {"symbol": "PM",    "name": "Philip Morris",       "weight": 5.3},
    ],
    "XLE":  [
        {"symbol": "XOM",   "name": "Exxon Mobil",         "weight": 22.3},
        {"symbol": "CVX",   "name": "Chevron",             "weight": 14.8},
        {"symbol": "COP",   "name": "ConocoPhillips",      "weight": 8.1},
        {"symbol": "EOG",   "name": "EOG Resources",       "weight": 4.7},
        {"symbol": "SLB",   "name": "Schlumberger",        "weight": 4.3},
        {"symbol": "MPC",   "name": "Marathon Petroleum",  "weight": 4.1},
    ],
    "XLF":  [
        {"symbol": "BRK-B", "name": "Berkshire Hathaway",  "weight": 13.1},
        {"symbol": "JPM",   "name": "JPMorgan Chase",      "weight": 12.7},
        {"symbol": "V",     "name": "Visa",                "weight": 8.9},
        {"symbol": "MA",    "name": "Mastercard",          "weight": 6.2},
        {"symbol": "BAC",   "name": "Bank of America",     "weight": 4.8},
        {"symbol": "GS",    "name": "Goldman Sachs",       "weight": 3.9},
    ],
    "XLV":  [
        {"symbol": "LLY",   "name": "Eli Lilly",           "weight": 12.4},
        {"symbol": "UNH",   "name": "UnitedHealth",        "weight": 9.8},
        {"symbol": "JNJ",   "name": "Johnson & Johnson",   "weight": 7.1},
        {"symbol": "ABBV",  "name": "AbbVie",              "weight": 6.5},
        {"symbol": "MRK",   "name": "Merck",               "weight": 5.9},
        {"symbol": "TMO",   "name": "Thermo Fisher",       "weight": 4.1},
    ],
    "XLI":  [
        {"symbol": "RTX",   "name": "Raytheon",            "weight": 5.1},
        {"symbol": "CAT",   "name": "Caterpillar",         "weight": 4.8},
        {"symbol": "GE",    "name": "GE Aerospace",        "weight": 4.6},
        {"symbol": "UNP",   "name": "Union Pacific",       "weight": 4.2},
        {"symbol": "HON",   "name": "Honeywell",           "weight": 3.9},
        {"symbol": "ETN",   "name": "Eaton Corp",          "weight": 3.7},
    ],
    "XLB":  [
        {"symbol": "LIN",   "name": "Linde",               "weight": 17.2},
        {"symbol": "APD",   "name": "Air Products",        "weight": 5.8},
        {"symbol": "FCX",   "name": "Freeport-McMoRan",    "weight": 5.4},
        {"symbol": "SHW",   "name": "Sherwin-Williams",    "weight": 5.1},
        {"symbol": "NEM",   "name": "Newmont",             "weight": 4.3},
        {"symbol": "ECL",   "name": "Ecolab",              "weight": 4.0},
    ],
    "XLRE": [
        {"symbol": "PLD",   "name": "Prologis",            "weight": 9.8},
        {"symbol": "AMT",   "name": "American Tower",      "weight": 8.3},
        {"symbol": "EQIX",  "name": "Equinix",             "weight": 7.1},
        {"symbol": "WELL",  "name": "Welltower",           "weight": 5.9},
        {"symbol": "SPG",   "name": "Simon Property",      "weight": 4.7},
        {"symbol": "PSA",   "name": "Public Storage",      "weight": 4.5},
    ],
    "XLK":  [
        {"symbol": "NVDA",  "name": "NVIDIA",              "weight": 22.1},
        {"symbol": "AAPL",  "name": "Apple",               "weight": 20.8},
        {"symbol": "MSFT",  "name": "Microsoft",           "weight": 18.4},
        {"symbol": "AVGO",  "name": "Broadcom",            "weight": 5.7},
        {"symbol": "ORCL",  "name": "Oracle",              "weight": 3.2},
        {"symbol": "CRM",   "name": "Salesforce",          "weight": 2.8},
    ],
    "XLU":  [
        {"symbol": "NEE",   "name": "NextEra Energy",      "weight": 14.9},
        {"symbol": "SO",    "name": "Southern Company",    "weight": 7.2},
        {"symbol": "DUK",   "name": "Duke Energy",         "weight": 7.1},
        {"symbol": "AEP",   "name": "American Electric",   "weight": 5.4},
        {"symbol": "EXC",   "name": "Exelon",              "weight": 4.8},
        {"symbol": "SRE",   "name": "Sempra",              "weight": 4.6},
    ],
}

# Macro sensitivity matrix: +1 positive, -1 negative, 0 neutral
MACRO_MATRIX: dict[str, dict[str, int]] = {
    "XLE":  {"oil": 1,  "rates": -1, "dollar": -1, "inflation": 1,  "vix": -1},
    "XLF":  {"oil": 0,  "rates": 1,  "dollar": 1,  "inflation": 0,  "vix": -1},
    "XLU":  {"oil": 0,  "rates": -1, "dollar": 0,  "inflation": -1, "vix": 1},
    "XLRE": {"oil": 0,  "rates": -1, "dollar": 0,  "inflation": -1, "vix": 1},
    "XLK":  {"oil": 0,  "rates": -1, "dollar": 1,  "inflation": -1, "vix": -1},
    "XLP":  {"oil": 0,  "rates": 0,  "dollar": -1, "inflation": 0,  "vix": 1},
    "XLY":  {"oil": -1, "rates": -1, "dollar": 0,  "inflation": -1, "vix": -1},
    "XLI":  {"oil": 0,  "rates": 0,  "dollar": -1, "inflation": 1,  "vix": -1},
    "XLB":  {"oil": 1,  "rates": 0,  "dollar": -1, "inflation": 1,  "vix": -1},
    "XLV":  {"oil": 0,  "rates": 0,  "dollar": 0,  "inflation": 0,  "vix": 1},
    "XLC":  {"oil": 0,  "rates": -1, "dollar": 0,  "inflation": -1, "vix": -1},
}

# ── In-memory cache ────────────────────────────────────────────────────────────
_cache: dict = {}
_TTL = 300


def _cached(key: str, fn, ttl: int = _TTL):
    now = time.time()
    if key in _cache:
        val, ts = _cache[key]
        if now - ts < ttl:
            return val
    val = fn()
    _cache[key] = (val, now)
    return val


def invalidate_cache():
    _cache.clear()


# ── Core helpers ───────────────────────────────────────────────────────────────
def _safe_pct(df: pd.DataFrame, bars: int) -> Optional[float]:
    c = df["close"]
    if len(c) <= bars:
        return None
    return round((float(c.iloc[-1]) / float(c.iloc[-bars - 1]) - 1) * 100, 2)


def _ema_val(series: pd.Series, span: int) -> float:
    return float(series.ewm(span=span, adjust=False).mean().iloc[-1])


def _ema_stack_label(above20: bool, above50: bool, above200: bool) -> str:
    if above20 and above50 and above200:
        return "Bullish"
    if not above20 and not above50 and not above200:
        return "Bearish"
    if above50 and above200 and not above20:
        return "Pullback"
    return "Mixed"


def _trend_label(above20: bool, above50: bool, above200: bool, rs_ratio: float, rs_mom: float) -> str:
    if rs_ratio > 100 and rs_mom > 100:
        return "Leading"
    if rs_ratio > 100 and rs_mom <= 100:
        return "Weakening"
    if rs_ratio <= 100 and rs_mom <= 100:
        return "Lagging"
    return "Improving"


def _quadrant(rs_ratio: float, rs_mom: float) -> str:
    if rs_ratio > 100 and rs_mom > 100:
        return "Leading"
    if rs_ratio > 100 and rs_mom <= 100:
        return "Weakening"
    if rs_ratio <= 100 and rs_mom <= 100:
        return "Lagging"
    return "Improving"


def _bench_trend(close: float, ema50: float, ema200: float) -> str:
    if close > ema50 and close > ema200:
        return "Bullish"
    if close < ema200:
        return "Below key average"
    if close < ema50:
        return "Weak"
    return "Neutral"


# ── RRG calculation ────────────────────────────────────────────────────────────
def _compute_rrg_series(
    sector_df: pd.DataFrame,
    spy_df: pd.DataFrame,
    window: int = 10,
    mom_bars: int = 5,
) -> tuple[pd.Series, pd.Series]:
    common = sector_df.index.intersection(spy_df.index)
    sc = sector_df.loc[common, "close"]
    sp = spy_df.loc[common, "close"]

    rs = sc / sp
    rs_sma = rs.rolling(window, min_periods=1).mean()
    rs_ratio = (rs / rs_sma) * 100

    rs_mom = rs_ratio.diff(mom_bars).fillna(0) + 100

    return rs_ratio.fillna(100), rs_mom.fillna(100)


# ── Build sector row ───────────────────────────────────────────────────────────
def _build_row(ticker: str, name: str, df: pd.DataFrame, spy_df: pd.DataFrame) -> dict:
    c = df["close"]
    close = float(c.iloc[-1])
    prev  = float(c.iloc[-2]) if len(c) >= 2 else close

    d1   = _safe_pct(df, 1)
    d5   = _safe_pct(df, 5)
    d20  = _safe_pct(df, 20)
    d50  = _safe_pct(df, 50)
    d200 = _safe_pct(df, 200)

    spy_d1  = _safe_pct(spy_df, 1)
    spy_d5  = _safe_pct(spy_df, 5)
    spy_d20 = _safe_pct(spy_df, 20)

    def _vs(sec, bench):
        return round(sec - bench, 2) if sec is not None and bench is not None else None

    e20  = _ema_val(c, 20)
    e50  = _ema_val(c, 50)
    e200 = _ema_val(c, 200)

    above20  = close > e20
    above50  = close > e50
    above200 = close > e200

    rs_ratio_ser, rs_mom_ser = _compute_rrg_series(df, spy_df)
    rs_ratio = float(rs_ratio_ser.iloc[-1])
    rs_mom   = float(rs_mom_ser.iloc[-1])

    return {
        "ticker":       ticker,
        "name":         name,
        "close":        round(close, 2),
        "prev_close":   round(prev, 2),
        "d1":           d1,
        "d5":           d5,
        "d20":          d20,
        "d50":          d50,
        "d200":         d200,
        "vs_spy_1d":    _vs(d1,  spy_d1),
        "vs_spy_5d":    _vs(d5,  spy_d5),
        "vs_spy_20d":   _vs(d20, spy_d20),
        "ema20":        round(e20,  2),
        "ema50":        round(e50,  2),
        "ema200":       round(e200, 2),
        "above_ema20":  above20,
        "above_ema50":  above50,
        "above_ema200": above200,
        "ema_stack":    _ema_stack_label(above20, above50, above200),
        "rs_ratio":     round(rs_ratio, 2),
        "rs_mom":       round(rs_mom,   2),
        "quadrant":     _quadrant(rs_ratio, rs_mom),
        "trend":        _trend_label(above20, above50, above200, rs_ratio, rs_mom),
    }


# ── Public API ─────────────────────────────────────────────────────────────────
def get_sector_overview() -> dict:
    def _build():
        spy_df = fetch_ohlcv("SPY", interval="1d", bars=260)

        sectors: list[dict] = []
        for tkr, nm in SECTORS.items():
            try:
                df  = fetch_ohlcv(tkr, interval="1d", bars=260)
                sectors.append(_build_row(tkr, nm, df, spy_df))
            except Exception as exc:
                sectors.append({"ticker": tkr, "name": nm, "error": str(exc)})

        benchmarks: list[dict] = []
        for tkr, nm in BENCHMARKS.items():
            try:
                df = fetch_ohlcv(tkr, interval="1d", bars=260)
                c  = df["close"]
                close = float(c.iloc[-1])
                e50   = _ema_val(c, 50)
                e200  = _ema_val(c, 200)
                benchmarks.append({
                    "ticker": tkr,
                    "name":   nm,
                    "close":  round(close, 2),
                    "d1":     _safe_pct(df, 1),
                    "trend":  _bench_trend(close, e50, e200),
                })
            except Exception as exc:
                benchmarks.append({"ticker": tkr, "name": nm, "error": str(exc)})

        valid = [s for s in sectors if "error" not in s]
        growth_lead    = sum(1 for s in valid if s["ticker"] in GROWTH    and (s.get("vs_spy_20d") or 0) > 0)
        defensive_lead = sum(1 for s in valid if s["ticker"] in DEFENSIVE and (s.get("vs_spy_20d") or 0) > 0)
        spy_b = next((b for b in benchmarks if b["ticker"] == "SPY"), {})
        spy_bullish = spy_b.get("trend") in ("Bullish",)

        leading   = [s["ticker"] for s in valid if s["quadrant"] == "Leading"]
        lagging   = [s["ticker"] for s in valid if s["quadrant"] == "Lagging"]

        if growth_lead >= 2 and defensive_lead <= 1 and spy_bullish:
            regime = "RISK ON"
            explain = (
                "Growth and cyclical sectors are leading. "
                + (f"{', '.join(leading[:3])} show relative strength " if leading else "")
                + "while defensive sectors show relative weakness."
            )
        elif defensive_lead >= 2 and growth_lead <= 1:
            regime = "RISK OFF"
            explain = (
                "Defensive sectors are outperforming. "
                + "Utilities, Staples, and Health Care show relative strength "
                + "while growth sectors underperform."
            )
        else:
            regime = "NEUTRAL"
            explain = "Leadership is mixed with no clear sector rotation direction."

        return {
            "sectors":    sectors,
            "benchmarks": benchmarks,
            "regime":     regime,
            "regime_explain": explain,
            "leading":    leading,
            "lagging":    lagging,
            "updated_at": round(time.time()),
        }

    return _cached("overview", _build)


def get_sector_detail(ticker: str) -> dict:
    tkr = ticker.upper()

    def _build():
        df     = fetch_ohlcv(tkr, interval="1d", bars=260)
        spy_df = fetch_ohlcv("SPY", interval="1d", bars=260)
        name   = SECTORS.get(tkr, tkr)
        row    = _build_row(tkr, name, df, spy_df)

        common  = df.index.intersection(spy_df.index)
        sc      = df.loc[common, "close"]
        sp      = spy_df.loc[common, "close"]
        rs_ser  = (sc / sp * 100).round(3)

        n = 252
        dates  = [str(d)[:10] for d in common[-n:].tolist()]
        prices = sc.iloc[-n:].round(2).tolist()
        rs_vals = rs_ser.iloc[-n:].tolist()
        ema20s  = sc.ewm(span=20,  adjust=False).mean().iloc[-n:].round(2).tolist()
        ema50s  = sc.ewm(span=50,  adjust=False).mean().iloc[-n:].round(2).tolist()
        ema200s = sc.ewm(span=200, adjust=False).mean().iloc[-n:].round(2).tolist()

        return {
            **row,
            "history": {
                "dates":  dates,
                "prices": prices,
                "rs":     rs_vals,
                "ema20":  ema20s,
                "ema50":  ema50s,
                "ema200": ema200s,
            },
            "holdings": HOLDINGS.get(tkr, []),
        }

    return _cached(f"detail:{tkr}", _build)


def get_sector_rrg(trail: int = 12) -> list[dict]:
    def _build():
        spy_df = fetch_ohlcv("SPY", interval="1d", bars=260)
        result: list[dict] = []
        for tkr, nm in SECTORS.items():
            try:
                df = fetch_ohlcv(tkr, interval="1d", bars=260)
                rs_ratio_ser, rs_mom_ser = _compute_rrg_series(df, spy_df)

                trail_r = rs_ratio_ser.dropna().tail(trail).round(2).tolist()
                trail_m = rs_mom_ser.dropna().tail(trail).round(2).tolist()
                cur_r   = trail_r[-1] if trail_r else 100.0
                cur_m   = trail_m[-1] if trail_m else 100.0

                result.append({
                    "ticker":      tkr,
                    "name":        nm,
                    "rs_ratio":    round(cur_r, 2),
                    "rs_mom":      round(cur_m, 2),
                    "quadrant":    _quadrant(cur_r, cur_m),
                    "trail_ratio": trail_r,
                    "trail_mom":   trail_m,
                })
            except Exception as exc:
                result.append({
                    "ticker": tkr, "name": nm,
                    "rs_ratio": 100.0, "rs_mom": 100.0,
                    "quadrant": "Neutral",
                    "trail_ratio": [], "trail_mom": [],
                    "error": str(exc),
                })
        return result

    return _cached("rrg", _build)


def get_heatmap(metric: str = "d1") -> list[dict]:
    overview = get_sector_overview()
    return [
        {"ticker": s["ticker"], "name": s["name"], "value": s.get(metric)}
        for s in overview["sectors"]
    ]


def get_holdings(ticker: str) -> list[dict]:
    return HOLDINGS.get(ticker.upper(), [])


def get_macro_matrix() -> dict:
    return {
        "sectors": list(SECTORS.keys()),
        "factors": ["oil", "rates", "dollar", "inflation", "vix"],
        "matrix":  MACRO_MATRIX,
        "names":   SECTORS,
    }
