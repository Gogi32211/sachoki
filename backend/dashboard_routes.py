"""
dashboard_routes.py — Trading Command Center API.

Endpoints aggregate data from scanner, ultra-scan, and sector engines
and optionally run them through the Claude AI analyst layer.
"""
from __future__ import annotations

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

# Per-ticker caches (sector/industry + news)
_ctx_cache:           dict[str, tuple[float, dict]] = {}
_news_cache:          dict[str, tuple[float, dict]] = {}
_news_analysis_cache: dict[str, tuple[float, dict]] = {}


def _cached(key: str, ttl: int = _CACHE_TTL):
    entry = _cache.get(key)
    if entry and time.time() - entry[0] < ttl:
        return True, entry[1]
    return False, None


def _store(key: str, value: Any):
    _cache[key] = (time.time(), value)
    return value


# ── Market-open helpers ───────────────────────────────────────────────────────

def _market_status() -> dict:
    from zoneinfo import ZoneInfo
    tz = ZoneInfo("America/New_York")
    now = datetime.now(tz)
    wd = now.weekday()

    market_open  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    pre_open     = now.replace(hour=4,  minute=0,  second=0, microsecond=0)
    after_close  = now.replace(hour=20, minute=0,  second=0, microsecond=0)

    is_weekend = wd >= 5

    if is_weekend:
        status, phase, secs_to_next = "closed", "weekend", 0
    elif now < pre_open:
        status, phase = "closed", "overnight"
        secs_to_next = int((pre_open - now).total_seconds())
    elif now < market_open:
        status, phase = "pre_market", "pre_market"
        secs_to_next = int((market_open - now).total_seconds())
    elif now <= market_close:
        status, phase = "open", "regular"
        secs_to_next = int((market_close - now).total_seconds())
    elif now <= after_close:
        status, phase = "after_hours", "after_hours"
        secs_to_next = int((after_close - now).total_seconds())
    else:
        status, phase, secs_to_next = "closed", "overnight", 0

    return {
        "status":       status,
        "phase":        phase,
        "secs_to_next": secs_to_next,
        "local_time":   now.strftime("%H:%M:%S"),
        "local_date":   now.strftime("%Y-%m-%d"),
        "day_of_week":  now.strftime("%A"),
    }


def _scanner_status() -> dict:
    try:
        from scanner import get_scan_progress, get_last_scan_time
        prog = get_scan_progress()
        last = get_last_scan_time("1d")
        return {
            "running":   prog.get("running", False),
            "done":      prog.get("done", 0),
            "total":     prog.get("total", 0),
            "found":     prog.get("found", 0),
            "last_scan": last,
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


# ── Endpoints ─────────────────────────────────────────────────────────────────

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
    """Market pulse: price/change/momentum for SPY, QQQ, IWM, VIX. Cached 90s."""
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
        rows.sort(key=lambda r: float(r.get("ultra_score", 0) or 0), reverse=True)
        for r in rows[:limit]:
            score = float(r.get("ultra_score", 0) or 0)
            bucket = (
                "BUY_READY"         if score >= 80 else
                "WATCH_CLOSELY"     if score >= 65 else
                "WAIT_CONFIRMATION" if score >= 50 else
                "TOO_LATE"          if score >= 35 else
                "AVOID"
            )
            cards.append({
                "ticker":        r.get("ticker", ""),
                "ultra_score":   round(score, 1),
                "band":          r.get("ultra_score_band", ""),
                "action_bucket": bucket,
                "profile":       r.get("profile", "") or r.get("ultra_score_priority", ""),
                "last_price":    r.get("last_price"),
                "change_pct":    r.get("change_pct"),
                "volume":        r.get("volume"),
                "vol_bucket":    r.get("vol_bucket", ""),
                "abr":           r.get("abr", "") or r.get("abr_label", ""),
                "signals":       r.get("active_signals", []) or [],
                "ema_ok":        r.get("ema_ok", False),
                "bull_score":    r.get("bull_score", 0),
                "scanned_at":    r.get("scanned_at", ""),
            })
    except Exception as exc:
        log.warning("top50 error: %s", exc)

    payload = {"cards": cards, "count": len(cards), "tf": tf}
    return _store(cache_key, payload)


@router.get("/sector-heat")
def dashboard_sector_heat():
    """Sector heatmap with hotness scoring."""
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
            hotness = ret1d * 0.5 + ret5d * 0.3 + rs * 0.2
            sectors.append({
                "etf":       etf,
                "name":      SECTORS.get(etf, etf),
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
def dashboard_fresh_signals(limit: int = Query(30, ge=1, le=100)):
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


@router.get("/risk-alerts")
def dashboard_risk_alerts():
    """Risk alerts from VIX spike, SPY trend, and extended movers."""
    hit, val = _cached("risk_alerts", ttl=120)
    if hit:
        return val

    alerts = []
    try:
        import yfinance as yf
        data = yf.download("SPY VIX", period="5d", interval="1d",
                           auto_adjust=True, progress=False, threads=True)
        close = data.get("Close", data)
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
                                   "message": f"SPY up {spy5d:.1f}% over 5 days — extended, caution on new longs"})
    except Exception as exc:
        log.warning("risk_alerts error: %s", exc)

    if not alerts:
        alerts.append({"type": "ok", "severity": "low", "message": "No major risk signals detected"})

    payload = {"alerts": alerts, "count": len(alerts)}
    return _store("risk_alerts", payload)


@router.get("/summary")
def dashboard_summary():
    """Aggregated summary cards: bull count, ultra top band, sector leader."""
    hit, val = _cached("summary", ttl=90)
    if hit:
        return val

    summary: dict[str, Any] = {}

    try:
        from scanner import get_results, get_last_scan_time
        all_rows     = get_results(interval="1d", limit=500)
        bull_rows    = [r for r in all_rows if r.get("bull_score", 0) >= 4]
        strong_rows  = [r for r in all_rows if r.get("bull_score", 0) >= 6]
        summary["scan_total"]   = len(all_rows)
        summary["bull_count"]   = len(bull_rows)
        summary["strong_count"] = len(strong_rows)
        summary["last_scan"]    = get_last_scan_time("1d")
    except Exception as exc:
        log.warning("summary scan error: %s", exc)
        summary.update({"scan_total": 0, "bull_count": 0, "strong_count": 0, "last_scan": None})

    try:
        from ultra_orchestrator import get_ultra_results
        resp     = get_ultra_results(universe="sp500", tf="1d", nasdaq_batch="")
        rows     = resp.get("results", []) if isinstance(resp, dict) else []
        top_band = [r for r in rows if r.get("ultra_score_band") in ("A", "A+", "S")]
        summary["ultra_total"]    = len(rows)
        summary["ultra_top_band"] = len(top_band)
    except Exception as exc:
        log.warning("summary ultra error: %s", exc)
        summary.update({"ultra_total": 0, "ultra_top_band": 0})

    try:
        from sector_engine import get_sector_overview
        overview  = get_sector_overview()
        items     = overview.get("sectors", []) if isinstance(overview, dict) else []
        leaders   = sorted(items, key=lambda s: float(s.get("return_1d", 0) or 0), reverse=True)
        summary["sector_leader"]  = leaders[0]["ticker"] if leaders else None
        summary["sector_laggard"] = leaders[-1]["ticker"] if leaders else None
    except Exception as exc:
        log.warning("summary sector error: %s", exc)
        summary.update({"sector_leader": None, "sector_laggard": None})

    return _store("summary", summary)


# ── AI Best Setups ─────────────────────────────────────────────────────────────

_SETUP_SYSTEM = """You are a professional stock market analyst assistant.
Analyze scan data and identify the top trading setups objectively.
Always respond with valid JSON only — no markdown fences, no prose outside JSON.
Do not invent data — only use what is provided."""

_SETUP_PROMPT = """
Given the following top ultra-scan candidates (JSON), select the 5 best trading setups.
For each setup provide:
- ticker: string
- category: one of BEST_PULLBACK | BEST_BREAKOUT | BEST_EMA_RECLAIM | BEST_ABR_B_PLUS | BEST_SECTOR_LEADER | BEST_FRESH_SIGNAL | BEST_RISK_REWARD | AVOID_TOO_LATE
- action_bucket: one of BUY_READY | WATCH_CLOSELY | WAIT_CONFIRMATION | TOO_LATE | AVOID
- confidence: 1-10
- why_selected: array of 2-4 short bullet strings
- risk_flags: array of 1-3 short bullet strings
- what_to_watch_next: array of 1-3 short bullet strings

Candidate data:
{candidates_json}

Respond with JSON: {{"setups": [...]}}
"""

_VALID_BUCKETS = {"BUY_READY", "WATCH_CLOSELY", "WAIT_CONFIRMATION", "TOO_LATE", "AVOID"}
_VALID_CATS = {
    "BEST_PULLBACK", "BEST_BREAKOUT", "BEST_EMA_RECLAIM", "BEST_ABR_B_PLUS",
    "BEST_SECTOR_LEADER", "BEST_FRESH_SIGNAL", "BEST_RISK_REWARD", "AVOID_TOO_LATE",
}

_setup_cache: dict[str, tuple[float, list]] = {}
_AI_CACHE_TTL = 300  # 5 min


def _deterministic_setups(cards: list[dict]) -> list[dict]:
    ranked = sorted(cards, key=lambda c: float(c.get("ultra_score", 0) or 0), reverse=True)
    out = []
    for c in ranked[:5]:
        score   = float(c.get("ultra_score", 0) or 0)
        band    = c.get("band", "")
        abr     = c.get("abr", "")
        ema_ok  = c.get("ema_ok", False)
        low_vol = c.get("vol_bucket", "") == "LOW"

        bucket = (
            "BUY_READY"         if score >= 80 else
            "WATCH_CLOSELY"     if score >= 65 else
            "WAIT_CONFIRMATION" if score >= 50 else
            "TOO_LATE"          if score >= 35 else
            "AVOID"
        )
        cat = (
            "BEST_ABR_B_PLUS"  if abr in ("B+", "A") else
            "BEST_EMA_RECLAIM" if ema_ok              else
            "BEST_PULLBACK"
        )

        reasons = []
        if band in ("S", "A+", "A"):
            reasons.append(f"Ultra {band}-band · score {score:.0f}")
        if abr in ("B+", "A"):
            reasons.append(f"ABR {abr} — strong accumulation profile")
        if ema_ok:
            reasons.append("EMA structure confirmed")
        if score >= 80 and not reasons:
            reasons.append(f"High-conviction score {score:.0f}")
        elif not reasons:
            reasons.append(f"Top ultra score {score:.0f}")

        risks = ["Low volume — wait for confirmation" if low_vol else "Standard execution risk"]

        out.append({
            "ticker":             c["ticker"],
            "category":           cat,
            "action_bucket":      bucket,
            "confidence":         min(10, max(1, int(score / 10))),
            "ultra_score":        round(score, 1),
            "band":               band,
            "why_selected":       reasons,
            "risk_flags":         risks,
            "what_to_watch_next": ["Monitor for volume expansion", "Watch EMA50 for support"],
            "source":             "deterministic",
        })
    return out


@router.get("/best-setups")
def dashboard_best_setups(tf: str = Query("1d")):
    """AI-selected best 5 setups. Cached 5 minutes. Falls back to deterministic."""
    cache_key = f"best_setups_{tf}"
    entry = _setup_cache.get(cache_key)
    if entry and time.time() - entry[0] < _AI_CACHE_TTL:
        return {"setups": entry[1], "cached": True}

    # Fetch top candidates
    candidates = []
    try:
        from ultra_orchestrator import get_ultra_results
        resp = get_ultra_results(universe="sp500", tf=tf, nasdaq_batch="")
        rows = resp.get("results", []) if isinstance(resp, dict) else []
        rows.sort(key=lambda r: float(r.get("ultra_score", 0) or 0), reverse=True)
        for r in rows[:20]:
            candidates.append({
                "ticker":      r.get("ticker", ""),
                "ultra_score": float(r.get("ultra_score", 0) or 0),
                "band":        r.get("ultra_score_band", ""),
                "change_pct":  r.get("change_pct"),
                "vol_bucket":  r.get("vol_bucket", ""),
                "ema_ok":      r.get("ema_ok", False),
                "abr":         r.get("abr", "") or r.get("abr_label", ""),
            })
    except Exception as exc:
        log.warning("best_setups candidates error: %s", exc)

    if not candidates:
        return {"setups": [], "cached": False}

    # Try Claude
    setups = None
    try:
        from claude_client import ask_json
        prompt = _SETUP_PROMPT.format(candidates_json=json.dumps(candidates, indent=2))
        raw = ask_json(prompt, system=_SETUP_SYSTEM, max_tokens=1200)
        if isinstance(raw, dict) and isinstance(raw.get("setups"), list):
            validated = []
            for item in raw["setups"][:5]:
                if not isinstance(item, dict) or "ticker" not in item:
                    continue
                validated.append({
                    "ticker":            str(item["ticker"]),
                    "category":          item.get("category", "BEST_PULLBACK") if item.get("category") in _VALID_CATS else "BEST_PULLBACK",
                    "action_bucket":     item.get("action_bucket", "WATCH_CLOSELY") if item.get("action_bucket") in _VALID_BUCKETS else "WATCH_CLOSELY",
                    "confidence":        max(1, min(10, int(item.get("confidence", 5)))),
                    "ultra_score":       next((c["ultra_score"] for c in candidates if c["ticker"] == item["ticker"]), None),
                    "band":              next((c["band"] for c in candidates if c["ticker"] == item["ticker"]), ""),
                    "why_selected":      [str(w)[:100] for w in (item.get("why_selected") or [])[:4]],
                    "risk_flags":        [str(r)[:80]  for r in (item.get("risk_flags") or [])[:3]],
                    "what_to_watch_next":[str(w)[:100] for w in (item.get("what_to_watch_next") or [])[:3]],
                    "source":            "claude",
                })
            if validated:
                setups = validated
    except Exception as exc:
        log.warning("Claude best_setups error: %s", exc)

    if not setups:
        setups = _deterministic_setups(candidates)

    _setup_cache[cache_key] = (time.time(), setups)
    return {"setups": setups, "cached": False}


# ── AI Market Brief ────────────────────────────────────────────────────────────

_BRIEF_SYSTEM = """You are a professional stock market analyst.
Generate a concise, data-driven market brief based only on the provided data.
Respond with valid JSON only. Do not invent data, news, or statistics."""

_BRIEF_PROMPT = """
Generate a market brief based on the following data:
- Bull signals (score >= 4): {bull_count}
- Strong setups (score >= 6): {strong_count}
- Ultra top band (A/A+/S): {ultra_top_band}
- SPY 1d change: {spy_1d}%
- QQQ 1d change: {qqq_1d}%
- Hot sectors: {hot_sectors}
- Active risk alerts: {risk_alerts}
- Top 5 candidates by ultra score: {top_candidates}

Return this exact JSON structure:
{{
  "market_tone": "Strongly Bullish | Mildly Bullish | Neutral | Mildly Bearish | Strongly Bearish",
  "focus_summary": "2-3 sentences summarizing today's key theme",
  "hot_sectors": ["up to 3 sector names"],
  "what_to_focus_on": ["3-5 concise bullet points"],
  "what_to_avoid": ["2-3 concise bullet points"],
  "key_risks": ["1-3 key risks"],
  "confidence": "LOW | MEDIUM | HIGH"
}}
"""

_brief_cache: dict[str, tuple[float, dict]] = {}
_BRIEF_CACHE_TTL = 300  # 5 min


def _deterministic_brief(ctx: dict) -> dict:
    spy  = ctx.get("spy_1d", 0.0)
    bull = ctx.get("bull_count", 0)
    strong = ctx.get("strong_count", 0)
    hs   = ctx.get("hot_sectors", [])
    tone = (
        "Strongly Bullish"  if spy > 1.5 else
        "Mildly Bullish"    if spy > 0.3 else
        "Strongly Bearish"  if spy < -1.5 else
        "Mildly Bearish"    if spy < -0.3 else
        "Neutral"
    )
    return {
        "market_tone":      tone,
        "focus_summary":    (
            f"Scanner shows {bull} bullish signals with {strong} strong setups. "
            + (f"Hot sectors today: {', '.join(hs[:2])}." if hs else "No clear sector rotation detected.")
        ),
        "hot_sectors":      hs,
        "what_to_focus_on": [
            f"{strong} strong setups found (bull_score ≥ 6) — prioritize these" if strong > 0
            else "Limited strong setups — wait for quality entries",
            f"Active hot sectors: {', '.join(hs)}" if hs else "No clear sector leadership",
            "Prioritize ABR B+ setups with EMA50 reclaim confirmation",
            "Use Top Candidates filter to find highest-quality entries",
        ],
        "what_to_avoid": [
            "Avoid parabolic names extended > 20% without consolidation",
            "Skip low-volume setups without sector confirmation",
        ],
        "key_risks":    ctx.get("risk_alerts", ["Monitor standard risk"]),
        "confidence":   "LOW" if bull < 5 else "MEDIUM" if bull < 20 else "HIGH",
        "source":       "deterministic",
    }


@router.get("/ai-brief")
def dashboard_ai_brief():
    """AI-generated market brief. Cached 5 minutes. Falls back to deterministic."""
    cache_key = "ai_brief"
    entry = _brief_cache.get(cache_key)
    if entry and time.time() - entry[0] < _BRIEF_CACHE_TTL:
        return {**entry[1], "cached": True}

    ctx: dict[str, Any] = {}

    # Scanner data
    try:
        from scanner import get_results
        all_rows = get_results(interval="1d", limit=500)
        ctx["bull_count"]   = len([r for r in all_rows if r.get("bull_score", 0) >= 4])
        ctx["strong_count"] = len([r for r in all_rows if r.get("bull_score", 0) >= 6])
    except Exception:
        ctx.update({"bull_count": 0, "strong_count": 0})

    # Ultra data
    try:
        from ultra_orchestrator import get_ultra_results
        resp     = get_ultra_results(universe="sp500", tf="1d", nasdaq_batch="")
        rows     = resp.get("results", []) if isinstance(resp, dict) else []
        top_band = [r for r in rows if r.get("ultra_score_band") in ("A", "A+", "S")]
        ctx["ultra_top_band"] = len(top_band)
        top5 = sorted(rows, key=lambda r: float(r.get("ultra_score", 0) or 0), reverse=True)[:5]
        ctx["top_candidates"] = [
            {"ticker": r.get("ticker"), "score": float(r.get("ultra_score", 0) or 0), "band": r.get("ultra_score_band", "")}
            for r in top5
        ]
    except Exception:
        ctx.update({"ultra_top_band": 0, "top_candidates": []})

    # Market pulse
    ctx.update({"spy_1d": 0.0, "qqq_1d": 0.0})
    hit, pulse_val = _cached("pulse", ttl=90)
    if hit and pulse_val:
        for item in pulse_val.get("pulse", []):
            if item["ticker"] == "SPY": ctx["spy_1d"] = item["change_1d"]
            if item["ticker"] == "QQQ": ctx["qqq_1d"] = item["change_1d"]

    # Hot sectors
    hot_sectors: list[str] = []
    try:
        from sector_engine import get_sector_overview, SECTORS
        overview = get_sector_overview()
        items    = overview.get("sectors", []) if isinstance(overview, dict) else []
        hot      = sorted(
            [s for s in items if float(s.get("return_1d", 0) or 0) > 0.5],
            key=lambda s: float(s.get("return_1d", 0) or 0), reverse=True
        )
        hot_sectors = [SECTORS.get(s["ticker"], s["ticker"]) for s in hot[:3]]
    except Exception:
        pass
    ctx["hot_sectors"] = hot_sectors

    # Risk alerts
    hit_r, risk_val = _cached("risk_alerts", ttl=120)
    if hit_r and risk_val:
        high = [a for a in risk_val.get("alerts", []) if a.get("severity") in ("high", "critical")]
        ctx["risk_alerts"] = [a["message"] for a in high[:3]] if high else ["None"]
    else:
        ctx["risk_alerts"] = ["None"]

    # Try Claude
    brief = None
    try:
        from claude_client import ask_json
        prompt = _BRIEF_PROMPT.format(**{
            k: json.dumps(v) if isinstance(v, (list, dict)) else v
            for k, v in ctx.items()
        })
        raw = ask_json(prompt, system=_BRIEF_SYSTEM, max_tokens=700)
        if isinstance(raw, dict) and "market_tone" in raw:
            brief = {**raw, "source": "claude"}
    except Exception as exc:
        log.warning("AI brief Claude error: %s", exc)

    if not brief:
        brief = _deterministic_brief(ctx)

    _brief_cache[cache_key] = (time.time(), brief)
    return {**brief, "cached": False}


@router.get("/news")
def dashboard_news():
    """
    Market news placeholder. Returns empty items until a news API is integrated.
    UI shows a clean empty state rather than an error.
    """
    return {
        "items":   [],
        "source":  "none",
        "message": "No relevant candidate news found. Connect a news API to enable this section.",
    }


@router.get("/ticker-context/{symbol}")
def dashboard_ticker_context(symbol: str):
    """Ticker metadata: sector, industry, company, theme, upcoming events. Cached 6h."""
    symbol = symbol.upper().strip()
    _CTX_TTL = 21600
    entry = _ctx_cache.get(symbol)
    if entry and time.time() - entry[0] < _CTX_TTL:
        return entry[1]

    data: dict = {
        "symbol":   symbol,
        "company":  None,
        "sector":   None,
        "industry": None,
        "theme":    None,
        "events":   [],
    }

    try:
        import yfinance as yf
        tkr  = yf.Ticker(symbol)
        info: dict = {}
        try:
            info = tkr.info or {}
        except Exception:
            pass

        data["company"]  = info.get("shortName") or info.get("longName")
        data["sector"]   = info.get("sector")
        data["industry"] = info.get("industry")

        ind = (data["industry"] or "").lower()
        co  = (data["company"]  or "").lower()
        if any(x in ind for x in ["biotech", "drug", "pharmaceutical", "biologic"]):
            data["theme"] = "Biotech"
        elif "semiconductor" in ind:
            data["theme"] = "Semis"
        elif any(x in ind for x in ["software", "artificial intelligence", "cloud"]):
            data["theme"] = "AI/SW"
        elif any(x in co for x in ["bitcoin", "crypto", "blockchain"]):
            data["theme"] = "Crypto"
        elif any(x in ind for x in ["gold", "silver", "mining"]):
            data["theme"] = "Metals"

        # Upcoming earnings
        try:
            cal = tkr.calendar
            if hasattr(cal, "to_dict"):
                cal = cal.to_dict()
            if isinstance(cal, dict):
                ed_raw = cal.get("Earnings Date")
                eds = []
                if ed_raw is not None:
                    if hasattr(ed_raw, "__iter__") and not isinstance(ed_raw, str):
                        eds = list(ed_raw)[:1]
                    else:
                        eds = [ed_raw]
                for ed in eds:
                    try:
                        from datetime import date as _date, datetime as _dt
                        if hasattr(ed, "date") and callable(ed.date):
                            ed = ed.date()
                        now_utc = datetime.now(timezone.utc).date()
                        if hasattr(ed, "year"):
                            delta = (ed - now_utc).days if hasattr(ed, "month") else 0
                        else:
                            continue
                        urgency = (
                            "TODAY"     if delta == 0 else
                            "TOMORROW"  if delta == 1 else
                            "THIS_WEEK" if delta <= 7 else
                            "NEXT_7D"   if delta <= 14 else
                            "UPCOMING"
                        )
                        if -14 <= delta <= 30:
                            data["events"].append({
                                "event_type": "EARNINGS",
                                "event_date": str(ed),
                                "urgency":    urgency,
                                "risk_level": "MEDIUM",
                                "label": (
                                    "Earnings Today"       if delta == 0 else
                                    "Earnings Tomorrow"    if delta == 1 else
                                    f"Earnings in {delta}D" if delta > 0 else
                                    f"Earnings {abs(delta)}D ago"
                                ),
                            })
                    except Exception:
                        pass
        except Exception:
            pass

    except Exception as exc:
        log.warning("ticker_context error %s: %s", symbol, exc)

    _ctx_cache[symbol] = (time.time(), data)
    return data


# ── Ticker News + AI Analysis ─────────────────────────────────────────────────

_NEWS_SYSTEM = (
    "You are a financial news analyst. Analyze the provided headlines for a stock ticker. "
    "Use ONLY the provided data. Do not invent information. "
    "Respond with valid JSON only, no prose outside JSON."
)

_NEWS_PROMPT = """\
Analyze these recent news headlines for {symbol}:

{headlines}

Return this exact JSON:
{{
  "sentiment": "BULLISH|MILDLY_BULLISH|NEUTRAL|MILDLY_BEARISH|BEARISH|RISKY|UNKNOWN",
  "catalyst_type": "EARNINGS|FDA|ANALYST_UPGRADE|ANALYST_DOWNGRADE|MERGER|OFFERING|INSIDER_BUY|INSIDER_SELL|CONTRACT|SECTOR_NEWS|GENERAL|UNKNOWN",
  "relevance": "HIGH|MEDIUM|LOW",
  "summary": "1-2 sentence summary",
  "why_it_matters": ["up to 2 short points"],
  "risks": ["up to 2 short risk points"],
  "setup_impact": "SUPPORTS_SETUP|WEAKENS_SETUP|RISK_ONLY|NO_CLEAR_IMPACT|UNKNOWN"
}}
If insufficient data, use UNKNOWN values.
"""

_VALID_SENTIMENTS = {"BULLISH","MILDLY_BULLISH","NEUTRAL","MILDLY_BEARISH","BEARISH","RISKY","UNKNOWN"}
_VALID_IMPACTS    = {"SUPPORTS_SETUP","WEAKENS_SETUP","RISK_ONLY","NO_CLEAR_IMPACT","UNKNOWN"}


def _fetch_yf_news(symbol: str) -> list[dict]:
    try:
        import yfinance as yf
        raw = yf.Ticker(symbol).news or []
        items = []
        for n in raw[:10]:
            pub = n.get("providerPublishTime") or n.get("publishedAt")
            pub_iso = None
            if pub:
                try:
                    pub_iso = datetime.fromtimestamp(int(pub), tz=timezone.utc).isoformat()
                except Exception:
                    pub_iso = str(pub)
            items.append({
                "headline":     n.get("title") or n.get("headline", ""),
                "source":       n.get("publisher") or n.get("source", ""),
                "published_at": pub_iso,
                "url":          n.get("link")  or n.get("url", ""),
                "category":     n.get("type")  or "",
            })
        return items
    except Exception as exc:
        log.warning("yf_news error %s: %s", symbol, exc)
        return []


def _news_hash(items: list[dict]) -> str:
    import hashlib
    hl = "|".join(i.get("headline", "") for i in items if i.get("headline"))
    return hashlib.md5(hl.encode()).hexdigest()[:12] if hl else ""


@router.get("/ticker-news/{symbol}")
def dashboard_ticker_news(symbol: str):
    """Recent yfinance news for a ticker + cached AI analysis if available."""
    symbol = symbol.upper().strip()
    _NEWS_TTL     = 1800
    _ANALYSIS_TTL = 86400

    entry = _news_cache.get(symbol)
    if entry and time.time() - entry[0] < _NEWS_TTL:
        return entry[1]

    items      = _fetch_yf_news(symbol)
    nh         = _news_hash(items)
    ai_summary = None

    if nh:
        ana = _news_analysis_cache.get(nh)
        if ana and time.time() - ana[0] < _ANALYSIS_TTL:
            ai_summary = ana[1]

    result = {
        "symbol":         symbol,
        "news_count":     len(items),
        "latest_news_at": items[0]["published_at"] if items else None,
        "ai_summary":     ai_summary,
        "news_hash":      nh,
        "items":          items,
    }
    _news_cache[symbol] = (time.time(), result)
    return result


@router.post("/ticker-news/{symbol}/analyze")
def dashboard_ticker_news_analyze(symbol: str):
    """Run Haiku AI analysis on ticker news. Cached 24h per news hash."""
    symbol        = symbol.upper().strip()
    _ANALYSIS_TTL = 86400

    news_result = dashboard_ticker_news(symbol)
    items       = news_result.get("items", [])
    nh          = news_result.get("news_hash", "")

    if not items:
        fallback = {
            "sentiment": "UNKNOWN", "catalyst_type": "UNKNOWN",
            "relevance": "LOW", "summary": "No recent news found.",
            "why_it_matters": [], "risks": [], "setup_impact": "UNKNOWN",
        }
        return {**news_result, "ai_summary": fallback}

    if nh:
        ana = _news_analysis_cache.get(nh)
        if ana and time.time() - ana[0] < _ANALYSIS_TTL:
            return {**news_result, "ai_summary": ana[1]}

    headlines = "\n".join(f"- {i['headline']}" for i in items if i.get("headline"))

    ai_summary = None
    try:
        from claude_client import ask_json
        raw = ask_json(
            _NEWS_PROMPT.format(symbol=symbol, headlines=headlines),
            system=_NEWS_SYSTEM,
            max_tokens=600,
        )
        if isinstance(raw, dict) and "sentiment" in raw:
            ai_summary = {
                "sentiment":      raw.get("sentiment")    if raw.get("sentiment")    in _VALID_SENTIMENTS else "UNKNOWN",
                "catalyst_type":  raw.get("catalyst_type", "UNKNOWN"),
                "relevance":      raw.get("relevance",     "MEDIUM"),
                "summary":        str(raw.get("summary", ""))[:300],
                "why_it_matters": [str(w)[:120] for w in (raw.get("why_it_matters") or [])[:2]],
                "risks":          [str(r)[:120] for r in (raw.get("risks")           or [])[:2]],
                "setup_impact":   raw.get("setup_impact") if raw.get("setup_impact") in _VALID_IMPACTS else "UNKNOWN",
            }
    except Exception as exc:
        log.warning("news analyze error %s: %s", symbol, exc)

    if not ai_summary:
        ai_summary = {
            "sentiment": "UNKNOWN", "catalyst_type": "UNKNOWN",
            "relevance": "LOW", "summary": "AI analysis unavailable.",
            "why_it_matters": [], "risks": [], "setup_impact": "UNKNOWN",
        }

    if nh:
        _news_analysis_cache[nh] = (time.time(), ai_summary)

    result = {**news_result, "ai_summary": ai_summary}
    _news_cache[symbol] = (time.time(), result)
    return result


@router.get("/watchlist")
def dashboard_watchlist(tickers: str = Query("")):
    """
    Watchlist snapshot: current T/Z signal status for the provided tickers.
    Accepts comma-separated ticker symbols as a query param.
    """
    if not tickers.strip():
        return {"items": []}

    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()][:50]
    items = []

    try:
        from scanner import get_results
        all_rows   = get_results(interval="1d", limit=1000)
        result_map = {r["ticker"]: r for r in all_rows}

        for ticker in ticker_list:
            r     = result_map.get(ticker)
            score = r.get("bull_score", 0) if r else 0
            chg   = (r.get("change_pct") or 0.0) if r else None

            status = (
                "improving"  if score >= 6 else
                "valid"      if score >= 4 else
                "weakening"  if score >= 2 else
                "review"
            )
            bucket = (
                "BUY_READY"         if score >= 7 else
                "WATCH_CLOSELY"     if score >= 5 else
                "WAIT_CONFIRMATION" if score >= 3 else
                "AVOID"
            )
            items.append({
                "ticker":       ticker,
                "status":       status,
                "bull_score":   score,
                "change_pct":   chg,
                "sig_name":     r.get("sig_name", "") if r else "",
                "action_bucket": bucket,
            })
    except Exception as exc:
        log.warning("dashboard watchlist error: %s", exc)
        items = [
            {"ticker": t, "status": "review", "bull_score": 0,
             "change_pct": None, "sig_name": "", "action_bucket": "WAIT_CONFIRMATION"}
            for t in ticker_list
        ]

    return {"items": items}
