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
def knowledge(horizon: int = 10):
    """Tier-1 knowledge base — HH-edge (structural) + P&L-edge (the real edge)
    per predicate, joined. Sorted by P&L edge at the chosen horizon."""
    ensure_schema()
    c = get_journal_conn(read_only=True)
    try:
        # Ensure pnl table exists so join works even if pnl_metric never ran
        c.execute("""CREATE TABLE IF NOT EXISTS signal_outcomes_pnl (
            predicate VARCHAR, category VARCHAR, as_of_date DATE, horizon INTEGER,
            stop_pct DOUBLE, target_pct DOUBLE, n BIGINT, avg_raw_fwd DOUBLE,
            avg_clip DOUBLE, win_rate DOUBLE, big_rate DOUBLE, loss_rate DOUBLE,
            base_avg_clip DOUBLE, base_win_rate DOUBLE,
            edge_avg_clip DOUBLE, edge_win DOUBLE, updated_at TIMESTAMP,
            PRIMARY KEY (predicate, as_of_date, horizon))""")
        rows = _rows(c, """
            SELECT h.predicate, h.category, h.n,
                   round(h.hh5*100,1) AS hh5, round(h.hh5_edge_pp,1) AS hh_edge_pp,
                   round(h.lift_big5,2) AS lift_big5, round(h.fwd5_med,2) AS fwd5_med,
                   p.horizon, round(p.avg_clip,3) AS pnl_avg,
                   round(p.edge_avg_clip,3) AS pnl_edge, round(p.edge_win,1) AS pnl_edge_win,
                   round(p.win_rate*100,1) AS pnl_win, h.as_of_date
            FROM signal_outcomes h
            LEFT JOIN signal_outcomes_pnl p
              ON p.predicate = h.predicate AND p.horizon = ?
                 AND p.as_of_date = (SELECT max(as_of_date) FROM signal_outcomes_pnl WHERE horizon = ?)
            WHERE h.as_of_date = (SELECT max(as_of_date) FROM signal_outcomes)
            ORDER BY p.edge_avg_clip DESC NULLS LAST""", [horizon, horizon])
        return {"as_of": rows[0]["as_of_date"] if rows else None,
                "horizon": horizon, "predicates": rows}
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


@router.get("/insider/marks/{ticker}")
def insider_marks(ticker: str, from_date: str | None = None,
                  ensure: bool = False, lookback_days: int = 365):
    """Per-date open-market insider BUYS for one ticker — chart ★ markers.
    ensure=1 → first lazily pull this ticker's last-year Form 4 history from SEC
    (cached), so the chart works on-demand without a universe-wide backfill."""
    from .edgar import marks_for_ticker, fetch_ticker_form4
    fetch_info = None
    try:
        if ensure:
            fetch_info = fetch_ticker_form4(ticker, lookback_days=lookback_days)
        out = marks_for_ticker(ticker, from_date=from_date)
        if fetch_info is not None:
            out["fetch"] = fetch_info
        return out
    except Exception as e:
        log.warning("insider marks for %s failed: %s", ticker, e)
        return {"ticker": ticker.upper(), "marks": [], "error": str(e)}


class InsiderIngestReq(BaseModel):
    days: int = 10


@router.post("/insider/ingest")
def insider_ingest(req: InsiderIngestReq):
    """Pull recent Form 4 filings from EDGAR into insider_tx (slow — SEC rate-limited)."""
    from .edgar import ingest_form4
    return ingest_form4(days=req.days)


class InsiderBackfillReq(BaseModel):
    days: int = 365


@router.post("/insider/backfill")
def insider_backfill(req: InsiderBackfillReq):
    """Kick the long historical backfill as a BACKGROUND thread (resumable via
    insider_ingest_log). Returns immediately; poll /insider/backfill/status."""
    import threading
    from .edgar import run_backfill, backfill_status
    st = backfill_status()
    if st.get("running"):
        return {"started": False, "already_running": True, **st}
    threading.Thread(target=run_backfill, args=(req.days,), daemon=True,
                     name="insider-backfill").start()
    return {"started": True, "days": req.days}


@router.get("/insider/backfill/status")
def insider_backfill_status():
    """Live progress of the historical insider backfill."""
    from .edgar import backfill_status
    return backfill_status()
