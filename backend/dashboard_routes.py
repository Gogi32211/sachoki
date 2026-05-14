"""
dashboard_routes.py — Trading Command Center API.

Endpoints aggregate data from scanner, ultra-scan, and sector engines
and optionally run them through the Claude AI analyst layer.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Query

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

# ── Simple in-memory cache ────────────────────────────────────────────────────
_cache: dict[str, tuple[float, Any]] = {}
_CACHE_TTL = 120  # seconds


def _cached(key: str, ttl: int = _CACHE_TTL):
    """Return (hit, value). hit=True means value is fresh."""
    entry = _cache.get(key)
    if entry and time.time() - entry[0] < ttl:
        return True, entry[1]
    return False, None


def _store(key: str, value: Any):
    _cache[key] = (time.time(), value)
    return value


# ── Market-open helpers ───────────────────────────────────────────────────────

def _market_status() -> dict:
    """
    Return market open/closed status and seconds-to-open/close (NYSE).
    Uses current UTC time; no external call needed.
    """
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("America/New_York")
    now = datetime.now(tz)
    wd = now.weekday()  # 0=Mon … 6=Sun

    # Pre-market 04:00-09:30, market 09:30-16:00, after 16:00-20:00
    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    pre_open     = now.replace(hour=4,  minute=0,  second=0, microsecond=0)
    after_close  = now.replace(hour=20, minute=0,  second=0, microsecond=0)

    is_weekend = wd >= 5

    if is_weekend:
        status = "closed"
        phase  = "weekend"
        secs_to_next = 0
    elif now < pre_open:
        status = "closed"
        phase  = "overnight"
        secs_to_next = int((pre_open - now).total_seconds())
    elif now < market_open:
        status = "pre_market"
        phase  = "pre_market"
        secs_to_next = int((market_open - now).total_seconds())
    elif now <= market_close:
        status = "open"
        phase  = "regular"
        secs_to_next = int((market_close - now).total_seconds())
    elif now <= after_close:
        status = "after_hours"
        phase  = "after_hours"
        secs_to_next = int((after_close - now).total_seconds())
    else:
        status = "closed"
        phase  = "overnight"
        secs_to_next = 0

    return {
        "status":       status,
        "phase":        phase,
        "secs_to_next": secs_to_next,
        "local_time":   now.strftime("%H:%M:%S"),
        "local_date":   now.strftime("%Y-%m-%d"),
        "day_of_week":  now.strftime("%A"),
    }


# ── Scanner status helper ──────────────────────────────────────────────────────

def _scanner_status() -> dict:
    try:
        from scanner import get_scan_progress, get_last_scan_time
        prog = get_scan_progress()
        last = get_last_scan_time("1d")
        return {
            "running":      prog.get("running", False),
            "done":         prog.get("done", 0),
            "total":        prog.get("total", 0),
            "found":        prog.get("found", 0),
            "last_scan":    last,
        }
    except Exception as exc:
        log.warning("scanner_status error: %s", exc)
        return {"running": False, "done": 0, "total": 0, "found": 0, "last_scan": None}


def _ultra_status() -> dict:
    try:
        from ultra_orchestrator import get_ultra_status
        s = get_ultra_status()
        return {
            "running":   s.get("running", False),
            "done":      s.get("done", 0),
            "total":     s.get("total", 0),
            "last_scan": s.get("completed_at"),
        }
    except Exception as exc:
        log.warning("ultra_status error: %s", exc)
        return {"running": False, "done": 0, "total": 0, "last_scan": None}


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get("/status")
def dashboard_status():
    """Market open/closed state + scanner status."""
    return {
        "market":  _market_status(),
        "scanner": _scanner_status(),
        "ultra":   _ultra_status(),
    }


@router.get("/pulse")
def dashboard_pulse():
    """
    Market pulse: price/change/momentum for SPY, QQQ, IWM, VIX.
    Cached 90 seconds.
    """
    hit, val = _cached("pulse", ttl=90)
    if hit:
        return val

    tickers = ["SPY", "QQQ", "IWM", "VIX", "DIA", "IWO"]
    results = []
    try:
        import yfinance as yf
        data = yf.download(
            tickers, period="5d", interval="1d",
            auto_adjust=True, progress=False, threads=True,
        )
        close = data["Close"] if "Close" in data else data.get("close", None)
        if close is None:
            raise ValueError("No close data")
        for t in tickers:
            if t not in close.columns:
                continue
            series = close[t].dropna()
            if len(series) < 2:
                continue
            prev  = float(series.iloc[-2])
            last  = float(series.iloc[-1])
            chg   = (last - prev) / prev * 100 if prev else 0.0
            chg5d = (last - float(series.iloc[0])) / float(series.iloc[0]) * 100 if len(series) >= 5 else chg
            results.append({
                "ticker":    t,
                "price":     round(last, 2),
                "change_1d": round(chg, 2),
                "change_5d": round(chg5d, 2),
                "trend":     "up" if chg > 0 else "down" if chg < 0 else "flat",
            })
    except Exception as exc:
        log.warning("pulse fetch error: %s", exc)

    payload = {"pulse": results, "fetched_at": datetime.now(timezone.utc).isoformat()}
    return _store("pulse", payload)


@router.get("/top50")
def dashboard_top50(
    tf: str = Query("1d"),
    limit: int = Query(50, ge=1, le=200),
):
    """Top N ultra-scan candidates ordered by ultra_score desc."""
    cache_key = f"top50_{tf}_{limit}"
    hit, val = _cached(cache_key, ttl=60)
    if hit:
        return val

    cards = []
    try:
        from ultra_orchestrator import get_ultra_results
        resp = get_ultra_results(universe="sp500", tf=tf, nasdaq_batch="")
        rows = resp.get("results", []) if isinstance(resp, dict) else []
        # Sort by ultra_score desc
        rows.sort(key=lambda r: float(r.get("ultra_score", 0) or 0), reverse=True)
        for r in rows[:limit]:
            score = float(r.get("ultra_score", 0) or 0)
            band  = r.get("ultra_score_band", "")
            profile = r.get("profile", "") or r.get("ultra_score_priority", "")
            cards.append({
                "ticker":        r.get("ticker", ""),
                "ultra_score":   round(score, 1),
                "band":          band,
                "profile":       profile,
                "last_price":    r.get("last_price"),
                "change_pct":    r.get("change_pct"),
                "volume":        r.get("volume"),
                "vol_bucket":    r.get("vol_bucket", ""),
                "abr":           r.get("abr", "") or r.get("abr_label", ""),
                "signals":       r.get("active_signals", []) or [],
                "ema_ok":        r.get("ema_ok", False),
                "scanned_at":    r.get("scanned_at", ""),
            })
    except Exception as exc:
        log.warning("top50 error: %s", exc)

    payload = {"cards": cards, "count": len(cards), "tf": tf}
    return _store(cache_key, payload)


@router.get("/sector-heat")
def dashboard_sector_heat():
    """Sector heatmap with hotness scoring for the dashboard."""
    hit, val = _cached("sector_heat", ttl=180)
    if hit:
        return val

    sectors = []
    try:
        from sector_engine import get_sector_overview, SECTORS
        overview = get_sector_overview()
        items = overview.get("sectors", []) if isinstance(overview, dict) else []
        for item in items:
            etf    = item.get("ticker", "")
            ret1d  = float(item.get("return_1d", 0) or 0)
            ret5d  = float(item.get("return_5d", 0) or 0)
            rs     = float(item.get("rs_score", 0) or 0)
            # Simple hotness: blend 1d return + 5d return + rs
            hotness = ret1d * 0.5 + ret5d * 0.3 + rs * 0.2
            sectors.append({
                "etf":      etf,
                "name":     SECTORS.get(etf, etf),
                "return_1d": round(ret1d, 2),
                "return_5d": round(ret5d, 2),
                "rs_score":  round(rs, 2),
                "hotness":   round(hotness, 2),
                "trend":     "hot" if hotness > 1 else "cold" if hotness < -1 else "neutral",
            })
        sectors.sort(key=lambda s: s["hotness"], reverse=True)
    except Exception as exc:
        log.warning("sector_heat error: %s", exc)

    payload = {"sectors": sectors}
    return _store("sector_heat", payload)


@router.get("/fresh-signals")
def dashboard_fresh_signals(
    limit: int = Query(30, ge=1, le=100),
):
    """Most recent bullish scan results sorted by bull_score desc."""
    hit, val = _cached(f"fresh_signals_{limit}", ttl=60)
    if hit:
        return val

    signals = []
    try:
        from scanner import get_results
        rows = get_results(interval="1d", limit=limit, tab="bull")
        for r in rows:
            signals.append({
                "ticker":     r.get("ticker", ""),
                "bull_score": r.get("bull_score", 0),
                "bear_score": r.get("bear_score", 0),
                "sig_name":   r.get("sig_name", ""),
                "last_price": r.get("last_price"),
                "change_pct": r.get("change_pct"),
                "vol_bucket": r.get("vol_bucket", ""),
                "scanned_at": r.get("scanned_at", ""),
            })
    except Exception as exc:
        log.warning("fresh_signals error: %s", exc)

    payload = {"signals": signals, "count": len(signals)}
    return _store(f"fresh_signals_{limit}", payload)


# ── AI Best Setups ─────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a professional stock market analyst assistant.
You analyze scan data and identify the top trading setups objectively.
Always respond with valid JSON only — no markdown fences, no prose.
Be concise. Do not invent data — only use what is provided."""

_SETUP_PROMPT_TEMPLATE = """
Given the following top ultra-scan candidates (JSON), select the 5 best trading setups.
For each setup provide:
- ticker: string
- action_bucket: one of BUY_READY | WATCH_CLOSELY | WAIT_CONFIRMATION | TOO_LATE | AVOID
- confidence: 1-10
- reason: one short sentence (max 15 words)
- risk: one short sentence (max 10 words)

Candidate data:
{candidates_json}

Respond with a JSON array of exactly 5 objects with keys: ticker, action_bucket, confidence, reason, risk.
"""


def _deterministic_best_setups(cards: list[dict]) -> list[dict]:
    """Fallback when Claude is unavailable: pure score-based selection."""
    ranked = sorted(cards, key=lambda c: float(c.get("ultra_score", 0) or 0), reverse=True)
    out = []
    for c in ranked[:5]:
        score = float(c.get("ultra_score", 0) or 0)
        if score >= 80:
            bucket = "BUY_READY"
        elif score >= 65:
            bucket = "WATCH_CLOSELY"
        elif score >= 50:
            bucket = "WAIT_CONFIRMATION"
        elif score >= 35:
            bucket = "TOO_LATE"
        else:
            bucket = "AVOID"
        out.append({
            "ticker":        c["ticker"],
            "action_bucket": bucket,
            "confidence":    min(10, max(1, int(score / 10))),
            "reason":        f"Ultra score {score:.0f} — {c.get('band', 'n/a')}",
            "risk":          "Low volume or extended" if c.get("vol_bucket") == "LOW" else "Standard risk",
            "source":        "deterministic",
        })
    return out


_ai_setup_cache: dict[str, tuple[float, list]] = {}
_AI_CACHE_TTL = 300  # 5 min


@router.get("/best-setups")
def dashboard_best_setups(tf: str = Query("1d")):
    """
    AI-selected best 5 setups. Uses Claude if key available, else deterministic fallback.
    Result cached 5 minutes per tf.
    """
    cache_key = f"best_setups_{tf}"
    entry = _ai_setup_cache.get(cache_key)
    if entry and time.time() - entry[0] < _AI_CACHE_TTL:
        return {"setups": entry[1], "cached": True}

    # Fetch top candidates
    try:
        from ultra_orchestrator import get_ultra_results
        resp = get_ultra_results(universe="sp500", tf=tf, nasdaq_batch="")
        rows = resp.get("results", []) if isinstance(resp, dict) else []
        rows.sort(key=lambda r: float(r.get("ultra_score", 0) or 0), reverse=True)
        candidates = []
        for r in rows[:20]:
            candidates.append({
                "ticker":      r.get("ticker", ""),
                "ultra_score": float(r.get("ultra_score", 0) or 0),
                "band":        r.get("ultra_score_band", ""),
                "change_pct":  r.get("change_pct"),
                "vol_bucket":  r.get("vol_bucket", ""),
                "ema_ok":      r.get("ema_ok", False),
            })
    except Exception as exc:
        log.warning("best_setups candidates error: %s", exc)
        candidates = []

    if not candidates:
        return {"setups": [], "cached": False}

    # Try Claude
    setups = None
    try:
        from claude_client import ask_json
        prompt = _SETUP_PROMPT_TEMPLATE.format(
            candidates_json=json.dumps(candidates, indent=2)
        )
        raw = ask_json(prompt, system=_SYSTEM_PROMPT, max_tokens=800)
        if isinstance(raw, list) and len(raw) >= 1:
            # Validate + augment
            setups = []
            valid_buckets = {"BUY_READY", "WATCH_CLOSELY", "WAIT_CONFIRMATION", "TOO_LATE", "AVOID"}
            for item in raw[:5]:
                if not isinstance(item, dict) or "ticker" not in item:
                    continue
                bucket = item.get("action_bucket", "WATCH_CLOSELY")
                if bucket not in valid_buckets:
                    bucket = "WATCH_CLOSELY"
                setups.append({
                    "ticker":        str(item["ticker"]),
                    "action_bucket": bucket,
                    "confidence":    max(1, min(10, int(item.get("confidence", 5)))),
                    "reason":        str(item.get("reason", ""))[:120],
                    "risk":          str(item.get("risk", ""))[:80],
                    "source":        "claude",
                })
    except Exception as exc:
        log.warning("Claude best_setups error: %s", exc)
        setups = None

    if not setups:
        setups = _deterministic_best_setups(candidates)

    _ai_setup_cache[cache_key] = (time.time(), setups)
    return {"setups": setups, "cached": False}


@router.get("/risk-alerts")
def dashboard_risk_alerts():
    """
    Risk alerts: extended movers, VIX spike, sector divergence, etc.
    """
    hit, val = _cached("risk_alerts", ttl=120)
    if hit:
        return val

    alerts = []
    try:
        import yfinance as yf
        spy_data = yf.download("SPY VIX", period="5d", interval="1d",
                               auto_adjust=True, progress=False, threads=True)
        close = spy_data.get("Close", spy_data)
        if "VIX" in close.columns:
            vix_series = close["VIX"].dropna()
            if len(vix_series) >= 2:
                vix_now  = float(vix_series.iloc[-1])
                vix_prev = float(vix_series.iloc[-2])
                if vix_now > 30:
                    alerts.append({"type": "vix_high", "severity": "high",
                                   "message": f"VIX elevated at {vix_now:.1f} — high fear regime"})
                elif vix_now > 20 and vix_now > vix_prev * 1.05:
                    alerts.append({"type": "vix_rising", "severity": "medium",
                                   "message": f"VIX rising to {vix_now:.1f} — watch volatility"})
        if "SPY" in close.columns:
            spy_series = close["SPY"].dropna()
            if len(spy_series) >= 5:
                spy5d = (float(spy_series.iloc[-1]) - float(spy_series.iloc[-5])) / float(spy_series.iloc[-5]) * 100
                if spy5d < -5:
                    alerts.append({"type": "spy_selloff", "severity": "high",
                                   "message": f"SPY down {abs(spy5d):.1f}% over 5 days — broad selloff"})
                elif spy5d > 5:
                    alerts.append({"type": "spy_extended", "severity": "medium",
                                   "message": f"SPY up {spy5d:.1f}% over 5 days — extended, caution on longs"})
    except Exception as exc:
        log.warning("risk_alerts error: %s", exc)

    if not alerts:
        alerts.append({"type": "ok", "severity": "low", "message": "No major risk signals detected"})

    payload = {"alerts": alerts, "count": len(alerts)}
    return _store("risk_alerts", payload)


@router.get("/summary")
def dashboard_summary():
    """
    Aggregated summary cards: bull count, bear count, ultra top band count,
    sector leaders, last scan age.
    """
    hit, val = _cached("summary", ttl=90)
    if hit:
        return val

    summary: dict[str, Any] = {}

    try:
        from scanner import get_results, get_last_scan_time
        all_rows = get_results(interval="1d", limit=500)
        bull_rows = [r for r in all_rows if r.get("bull_score", 0) >= 4]
        strong_rows = [r for r in all_rows if r.get("bull_score", 0) >= 6]
        summary["scan_total"]  = len(all_rows)
        summary["bull_count"]  = len(bull_rows)
        summary["strong_count"]= len(strong_rows)
        summary["last_scan"]   = get_last_scan_time("1d")
    except Exception as exc:
        log.warning("summary scan error: %s", exc)
        summary.update({"scan_total": 0, "bull_count": 0, "strong_count": 0, "last_scan": None})

    try:
        from ultra_orchestrator import get_ultra_results
        resp = get_ultra_results(universe="sp500", tf="1d", nasdaq_batch="")
        rows = resp.get("results", []) if isinstance(resp, dict) else []
        top_band = [r for r in rows if r.get("ultra_score_band") in ("A", "A+", "S")]
        summary["ultra_total"]    = len(rows)
        summary["ultra_top_band"] = len(top_band)
    except Exception as exc:
        log.warning("summary ultra error: %s", exc)
        summary.update({"ultra_total": 0, "ultra_top_band": 0})

    try:
        from sector_engine import get_sector_overview
        overview = get_sector_overview()
        items = overview.get("sectors", []) if isinstance(overview, dict) else []
        leaders = sorted(items, key=lambda s: float(s.get("return_1d", 0) or 0), reverse=True)
        summary["sector_leader"]  = leaders[0]["ticker"] if leaders else None
        summary["sector_laggard"] = leaders[-1]["ticker"] if leaders else None
    except Exception as exc:
        log.warning("summary sector error: %s", exc)
        summary.update({"sector_leader": None, "sector_laggard": None})

    return _store("summary", summary)
