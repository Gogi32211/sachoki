"""
ai_journal/api.py — FastAPI routes for the AI Journal UI.

Read endpoints (overview / knowledge / positions) are cheap DB reads.
Action endpoints (session / grade) run the deterministic + LLM loops on demand.
"""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter
from pydantic import BaseModel

from .db import get_journal_conn, ensure_schema

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/journal", tags=["ai_journal"])


def _rows(conn, sql, params=None):
    cur = conn.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/overview")
def overview():
    ensure_schema()
    c = get_journal_conn(read_only=True)
    try:
        state = _rows(c, "SELECT capital, start_capital, updated_at FROM journal_state WHERE id=1")
        state = state[0] if state else {"capital": 0, "start_capital": 0}
        positions = _rows(c, """
            SELECT id, ticker, decision_date, action, conviction, fingerprint,
                   entry_px, size_pct, shares, stop_px, target_px, horizon_days,
                   status, entry_mode, decided_session, filled_date, sector, mcap_bucket,
                   exit_date, exit_px, exit_reason, pnl_pct, verdict, thesis
            FROM journal_position ORDER BY id DESC LIMIT 300""")
        open_pos = [p for p in positions if p["status"] == "OPEN"]
        pending  = [p for p in positions if p["status"] == "PENDING_OPEN"]
        closed   = [p for p in positions if p["status"] == "CLOSED"]
        wins = [p for p in closed if p["verdict"] == "WIN"]
        win_rate = (len(wins) / len(closed) * 100) if closed else None
        avg_ret  = (sum(p["pnl_pct"] or 0 for p in closed) / len(closed)) if closed else None
        last_session = _rows(c, "SELECT ts, candidates_n, notes FROM journal_session_log ORDER BY ts DESC LIMIT 1")
        lessons = _rows(c, """SELECT lesson, scope_fingerprint, status, evidence_n, evidence_lift
                              FROM trade_lesson ORDER BY created_at DESC LIMIT 50""")
        try:
            from .regime import compute_regime
            regime = compute_regime()
        except Exception:
            regime = None
        return {
            "regime": regime,
            "state": state,
            "open_positions": open_pos,
            "pending_positions": pending,
            "closed_positions": closed,
            "stats": {"open": len(open_pos), "pending": len(pending), "closed": len(closed),
                      "win_rate": win_rate, "avg_ret_pct": avg_ret},
            "last_session": last_session[0] if last_session else None,
            "lessons": lessons,
        }
    finally:
        c.close()


@router.get("/knowledge")
def knowledge():
    """Tier-1 knowledge base, ranked by HH-continuation edge (where the real edge is)."""
    ensure_schema()
    c = get_journal_conn(read_only=True)
    try:
        rows = _rows(c, """
            SELECT predicate, category, n, round(rate_pct,1) AS rate_pct,
                   round(fwd5_med,2) AS fwd5_med, round(win5*100,1) AS win5,
                   round(big5*100,1) AS big5, round(lift_big5,2) AS lift_big5,
                   round(hh5*100,1) AS hh5, round(hh5_edge_pp,1) AS hh_edge_pp, as_of_date
            FROM signal_outcomes
            WHERE as_of_date = (SELECT max(as_of_date) FROM signal_outcomes)
            ORDER BY hh5_edge_pp DESC""")
        return {"as_of": rows[0]["as_of_date"] if rows else None, "predicates": rows}
    finally:
        c.close()


@router.get("/universe")
def universe():
    """Market-cap & sector view: distribution from ticker_meta + the validated
    forward stats per bucket/sector (joined with bars) that justify the rails."""
    import duckdb
    from .db import ANALYTICS_DB_PATH
    from .rails import MCAP_BLOCK
    ensure_schema()
    j = get_journal_conn(read_only=True)
    try:
        meta = j.execute("SELECT ticker, sector, mcap_bucket FROM ticker_meta").fetchdf()
        bucket_counts = _rows(j, "SELECT mcap_bucket AS bucket, count(*) AS n FROM ticker_meta GROUP BY 1 ORDER BY n DESC")
        sector_counts = _rows(j, "SELECT sector, count(*) AS n FROM ticker_meta WHERE sector<>'' GROUP BY 1 ORDER BY n DESC")
    finally:
        j.close()
    bucket_stats, sector_stats = [], []
    try:
        a = duckdb.connect(ANALYTICS_DB_PATH, read_only=True)
        a.register("meta", meta)
        bucket_stats = _rows(a, """
            SELECT m.mcap_bucket AS bucket,
              count(*) FILTER (WHERE b.prebreak_v3>=25) AS n,
              round(avg(CASE WHEN b.next_pivot_is_hh_5 THEN 1.0 ELSE 0 END) FILTER (WHERE b.prebreak_v3>=25)*100,1) AS hh5,
              round(median(b.fwd_5d) FILTER (WHERE b.prebreak_v3>=25),2) AS fwd5_med,
              round(avg(CASE WHEN b.fwd_5d>=10 THEN 1.0 ELSE 0 END) FILTER (WHERE b.prebreak_v3>=25)*100,1) AS big10
            FROM bars b JOIN meta m ON b.ticker=m.ticker
            WHERE b.fwd_5d IS NOT NULL AND b.fwd_5d BETWEEN -90 AND 500
            GROUP BY 1 ORDER BY hh5 DESC""")
        sector_stats = _rows(a, """
            SELECT m.sector,
              count(*) FILTER (WHERE b.prebreak_v3>=25) AS n,
              round(avg(CASE WHEN b.next_pivot_is_hh_5 THEN 1.0 ELSE 0 END) FILTER (WHERE b.prebreak_v3>=25)*100,1) AS setup_hh,
              round((avg(CASE WHEN b.next_pivot_is_hh_5 THEN 1.0 ELSE 0 END) FILTER (WHERE b.prebreak_v3>=25)
                     - avg(CASE WHEN b.next_pivot_is_hh_5 THEN 1.0 ELSE 0 END))*100,1) AS sig_lift
            FROM bars b JOIN meta m ON b.ticker=m.ticker
            WHERE b.fwd_5d IS NOT NULL AND b.fwd_5d BETWEEN -90 AND 500
            GROUP BY 1 HAVING count(*) FILTER (WHERE b.prebreak_v3>=25) >= 800 ORDER BY setup_hh DESC""")
        a.close()
    except Exception as e:
        log.warning("universe stats failed: %s", e)
    return {"blocked_buckets": sorted(MCAP_BLOCK), "bucket_counts": bucket_counts,
            "sector_counts": sector_counts, "bucket_stats": bucket_stats, "sector_stats": sector_stats}


@router.get("/ticker/{ticker}")
def ticker_detail(ticker: str):
    """Everything the drawer shows: meta + latest bar signals + positions + insider."""
    import duckdb
    from .db import ANALYTICS_DB_PATH
    ensure_schema()
    tk = ticker.upper()
    j = get_journal_conn(read_only=True)
    try:
        meta = _rows(j, "SELECT ticker, name, sector, industry, market_cap, employees, mcap_bucket FROM ticker_meta WHERE ticker=?", [tk])
        positions = _rows(j, """SELECT id, decision_date, action, conviction, status, entry_mode,
                                       entry_px, stop_px, target_px, size_pct, pnl_pct, verdict,
                                       sector, mcap_bucket, fingerprint, thesis
                                FROM journal_position WHERE ticker=? ORDER BY id DESC""", [tk])
        insider = _rows(j, """SELECT insider, title, tx_date, code, shares, price, value
                              FROM insider_tx WHERE ticker=? ORDER BY tx_date DESC LIMIT 20""", [tk])
    finally:
        j.close()
    latest = {}
    try:
        a = duckdb.connect(ANALYTICS_DB_PATH, read_only=True)
        r = a.execute("""SELECT date, close, change_pct, rsi_14, cci_20, atr_14, vol_bucket,
                                rtb_phase, t_sig, z_sig, prebreak_v3, prebreak_v3_reasons,
                                ultra_score, turbo_score
                         FROM bars WHERE ticker=? ORDER BY date DESC LIMIT 1""", [tk]).fetchdf()
        a.close()
        if len(r):
            latest = {k: (None if str(v) == 'nan' else v) for k, v in r.iloc[0].to_dict().items()}
    except Exception as e:
        log.warning("ticker_detail bars failed for %s: %s", tk, e)
    return {"ticker": tk, "meta": meta[0] if meta else None, "latest": latest,
            "positions": positions, "insider": insider}


class SessionReq(BaseModel):
    as_of: str | None = None
    top_n: int = 12


@router.post("/session")
def session(req: SessionReq):
    from .decide import run_session
    return run_session(as_of=req.as_of, top_n=req.top_n)


@router.post("/fill")
def fill():
    """Fill PENDING_OPEN positions at the next session's open (execution-realism)."""
    from .fills import fill_pending_open
    return fill_pending_open()


@router.post("/grade")
def grade():
    from .grading import grade_open_positions
    return grade_open_positions()


@router.post("/reflect")
def reflect():
    """Agent B: rebuild pattern memory → draft lessons → run the promotion gate."""
    from .lessons import reflect as _reflect
    return _reflect()


@router.get("/regime")
def regime():
    from .regime import compute_regime
    return compute_regime()


def _live_prices(tickers: list[str]) -> dict:
    """Current price per ticker from the Massive snapshot (fresh, no 15-min cache):
    day.c (regular session) → min.c (last trade) → prevDay.c. + today's change%."""
    out = {}
    if not tickers:
        return out
    try:
        from premarket_cache import _fetch_batch
        for i in range(0, len(tickers), 100):
            raw = _fetch_batch(tickers[i:i + 100]) or {}
            for tk, it in raw.items():
                day = it.get("day") or {}; mn = it.get("min") or {}; prev = it.get("prevDay") or {}
                def _f(x):
                    try: return float(x)
                    except (TypeError, ValueError): return 0.0
                px = _f(day.get("c")) or _f(mn.get("c")) or _f(prev.get("c"))
                if px > 0:
                    out[tk.upper()] = {"price": round(px, 4),
                                       "chg_pct": it.get("todaysChangePerc")}
    except Exception as e:
        log.warning("live_prices failed: %s", e)
    return out


@router.get("/live")
def live():
    """Live price + unrealized P&L for all open/pending position tickers."""
    j = get_journal_conn(read_only=True)
    try:
        rows = _rows(j, """SELECT ticker, entry_px, shares, status FROM journal_position
                           WHERE status IN ('OPEN','PENDING_OPEN')""")
    finally:
        j.close()
    prices = _live_prices(sorted({r["ticker"] for r in rows}))
    out = {}
    for r in rows:
        p = prices.get(r["ticker"])
        if not p:
            continue
        e = {"price": p["price"], "chg_pct": p["chg_pct"]}
        if r["entry_px"]:
            e["upnl_pct"] = round((p["price"] - r["entry_px"]) / r["entry_px"] * 100, 2)
            e["upnl"] = round((p["price"] - r["entry_px"]) * (r["shares"] or 0), 2)
        out[r["ticker"]] = e
    return {"prices": out}


@router.get("/pulse")
def pulse():
    """Industry Pulse: regime + sector heat + movers + market-cap (market context)."""
    from .industry_pulse import compute_pulse
    return compute_pulse()


@router.get("/insider")
def insider(days: int = 30):
    """Recent SEC Form 4 open-market buys + insider-cluster flags."""
    from .edgar import recent_insider
    return recent_insider(limit_days=days)


class InsiderIngestReq(BaseModel):
    days: int = 10


@router.post("/insider/ingest")
def insider_ingest(req: InsiderIngestReq):
    """Pull recent Form 4 filings from EDGAR into insider_tx (slow — SEC rate-limited)."""
    from .edgar import ingest_form4
    return ingest_form4(days=req.days)
