"""
Paper Portfolio API — Sachoki Screener
FastAPI endpoints for paper trading tracker

Endpoints:
  POST /portfolio/entry        — add new signal entries
  POST /portfolio/entry-price  — set next-day open price (entry)
  POST /portfolio/daily-prices — upsert daily OHLCV
  POST /portfolio/daily-check  — check TP/SL/exit for open positions
  GET  /portfolio              — list all entries (filterable)
  GET  /portfolio/stats        — performance summary
  GET  /portfolio/open         — open positions only
  GET  /portfolio/export       — closed trades for replay analytics
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import date, timedelta

from db import get_db, USE_PG

router = APIRouter(prefix="/portfolio", tags=["paper_portfolio"])

# ── Models ──────────────────────────────────────────────────────────────────

class SignalEntry(BaseModel):
    signal_date:       date
    ticker:            str
    exchange:          str            = "NQ"
    ultra_score:       Optional[int]  = None
    ultra_band:        Optional[str]  = None
    ultra_priority:    Optional[str]  = None
    beta_score:        Optional[int]  = None
    beta_zone:         Optional[str]  = None
    tz_sig:            Optional[str]  = None
    turbo_score:       Optional[int]  = None
    rtb_total:         Optional[int]  = None
    rtb_phase:         Optional[str]  = None
    sweet_spot:        bool           = False
    tier:              str            = "TIER1"
    signal_reasons:    Optional[str]  = None
    signal_price:      Optional[float]= None
    signal_change_pct: Optional[float]= None
    hold_days:         int            = 10

class EntryPrice(BaseModel):
    ticker:     str
    entry_date: date
    open_price: float

class DailyPrice(BaseModel):
    ticker:     str
    price_date: date
    open:       float
    high:       float
    low:        float
    close:      float
    volume:     Optional[int] = None

# ── Routes ──────────────────────────────────────────────────────────────────

@router.post("/entry")
def add_entries(entries: list[SignalEntry]):
    """Add new daily signal picks to portfolio."""
    inserted = 0
    skipped  = 0
    with get_db() as db:
        for e in entries:
            db.execute(
                "SELECT id FROM paper_portfolio WHERE signal_date=? AND ticker=?",
                (str(e.signal_date), e.ticker)
            )
            if db.fetchone():
                skipped += 1
                continue

            max_ex = e.signal_date + timedelta(days=e.hold_days + 2)
            db.execute("""
                INSERT INTO paper_portfolio
                (signal_date, ticker, exchange, ultra_score, ultra_band, ultra_priority,
                 beta_score, beta_zone, tz_sig, turbo_score, rtb_total, rtb_phase,
                 sweet_spot, tier, signal_reasons, signal_price, signal_change_pct,
                 hold_days, max_exit_date, status)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING')
            """, (
                str(e.signal_date), e.ticker, e.exchange,
                e.ultra_score, e.ultra_band, e.ultra_priority,
                e.beta_score, e.beta_zone, e.tz_sig,
                e.turbo_score, e.rtb_total, e.rtb_phase,
                e.sweet_spot, e.tier, e.signal_reasons,
                e.signal_price, e.signal_change_pct,
                e.hold_days, str(max_ex)
            ))
            inserted += 1
        db.commit()
    return {"inserted": inserted, "skipped": skipped}


@router.post("/entry-price")
def set_entry_price(prices: list[EntryPrice]):
    """Set next-day market open price. Calculates TP/SL and opens position."""
    updated = 0
    with get_db() as db:
        for p in prices:
            tp_par  = round(p.open_price * 1.20, 4)
            tp_wide = round(p.open_price * 1.15, 4)
            sl      = round(p.open_price * 0.95, 4)
            db.execute("""
                UPDATE paper_portfolio
                SET entry_date=?, entry_price=?,
                    tp_parabolic=?, tp_wide=?, sl_price=?,
                    status='OPEN', updated_at=CURRENT_TIMESTAMP
                WHERE ticker=? AND status='PENDING'
                  AND signal_date = (
                    SELECT MAX(signal_date) FROM paper_portfolio
                    WHERE ticker=? AND status='PENDING'
                  )
            """, (str(p.entry_date), p.open_price, tp_par, tp_wide, sl,
                  p.ticker, p.ticker))
            if USE_PG:
                updated += db._cur.rowcount
            else:
                updated += db._last.rowcount
        db.commit()
    return {"updated": updated}


@router.post("/daily-prices")
def upsert_daily_prices(prices: list[DailyPrice]):
    """Upsert daily OHLCV data for TP/SL monitoring."""
    with get_db() as db:
        for p in prices:
            db.execute("""
                INSERT INTO paper_daily_prices
                    (ticker, price_date, open, high, low, close, volume)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT (ticker, price_date)
                DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high,
                              low=EXCLUDED.low, close=EXCLUDED.close,
                              volume=EXCLUDED.volume
            """, (p.ticker, str(p.price_date),
                  p.open, p.high, p.low, p.close, p.volume))
        db.commit()
    return {"upserted": len(prices)}


@router.post("/daily-check")
def daily_check():
    """
    Run TP/SL/hold-end check for all OPEN positions.
    Call once per day after market close.
    """
    with get_db() as db:
        db.execute("""
            SELECT pp.id, pp.entry_price, pp.tp_parabolic, pp.tp_wide,
                   pp.sl_price, pp.max_exit_date,
                   pp.exit_date_p, pp.exit_date_w,
                   p.high as d_high, p.low as d_low, p.close as d_close,
                   p.price_date
            FROM paper_portfolio pp
            JOIN paper_daily_prices p ON p.ticker = pp.ticker
            WHERE pp.status = 'OPEN'
              AND p.price_date > pp.entry_date
              AND (pp.exit_date_p IS NULL OR pp.exit_date_w IS NULL)
            ORDER BY pp.id, p.price_date
        """)
        rows = db.fetchall()

    updates: dict = {}
    for r in rows:
        pid   = r['id']
        d     = r['price_date']
        high  = float(r['d_high'])
        low   = float(r['d_low'])
        close = float(r['d_close'])
        max_d = str(r['max_exit_date'])

        if pid not in updates:
            updates[pid] = {'p': None, 'w': None}

        tp_p = float(r['tp_parabolic']) if r['tp_parabolic'] else None
        tp_w = float(r['tp_wide'])      if r['tp_wide']      else None
        sl   = float(r['sl_price'])     if r['sl_price']     else None
        ep   = float(r['entry_price'])  if r['entry_price']  else None

        if tp_p and sl and ep and updates[pid]['p'] is None:
            if high >= tp_p:
                updates[pid]['p'] = (d, tp_p, 'TP',       round((tp_p  - ep)/ep*100, 4))
            elif low <= sl:
                updates[pid]['p'] = (d, sl,   'SL',       round((sl    - ep)/ep*100, 4))
            elif str(d) >= max_d:
                updates[pid]['p'] = (d, close, 'HOLD_END', round((close - ep)/ep*100, 4))

        if tp_w and sl and ep and updates[pid]['w'] is None:
            if high >= tp_w:
                updates[pid]['w'] = (d, tp_w, 'TP',       round((tp_w  - ep)/ep*100, 4))
            elif low <= sl:
                updates[pid]['w'] = (d, sl,   'SL',       round((sl    - ep)/ep*100, 4))
            elif str(d) >= max_d:
                updates[pid]['w'] = (d, close, 'HOLD_END', round((close - ep)/ep*100, 4))

    closed = 0
    with get_db() as db:
        for pid, ex in updates.items():
            if ex['p']:
                d, pr, rs, ret = ex['p']
                db.execute("""
                    UPDATE paper_portfolio
                    SET exit_date_p=?, exit_price_p=?, exit_reason_p=?,
                        realized_return_p=?, updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                """, (str(d), pr, rs, ret, pid))
            if ex['w']:
                d, pr, rs, ret = ex['w']
                db.execute("""
                    UPDATE paper_portfolio
                    SET exit_date_w=?, exit_price_w=?, exit_reason_w=?,
                        realized_return_w=?, updated_at=CURRENT_TIMESTAMP
                    WHERE id=?
                """, (str(d), pr, rs, ret, pid))
            if ex['p'] and ex['w']:
                db.execute(
                    "UPDATE paper_portfolio SET status='CLOSED',"
                    " updated_at=CURRENT_TIMESTAMP WHERE id=?", (pid,))
                closed += 1
        db.commit()

    return {"checked": len(updates), "closed": closed}


@router.get("/open")
def get_open():
    """All open positions."""
    with get_db() as db:
        db.execute("""
            SELECT ticker, exchange, signal_date, entry_date, entry_price,
                   tp_parabolic, tp_wide, sl_price, max_exit_date,
                   ultra_score, ultra_band, beta_zone, tz_sig, tier, sweet_spot
            FROM paper_portfolio
            WHERE status = 'OPEN'
            ORDER BY entry_date DESC, ultra_score DESC
        """)
        data = db.fetchall()
    return {"count": len(data), "positions": data}


@router.get("/stats")
def get_stats(days: int = 90):
    """Performance summary — both strategies + breakdown by zone/signal/tier."""
    cutoff = str(date.today() - timedelta(days=days))
    with get_db() as db:
        db.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN status='CLOSED' THEN 1 END) as closed,
                COUNT(CASE WHEN status='OPEN'   THEN 1 END) as open,
                ROUND(AVG(realized_return_p), 2) as avg_ret_p,
                ROUND(AVG(realized_return_w), 2) as avg_ret_w,
                COUNT(CASE WHEN exit_reason_p='TP' THEN 1 END) as tp_count_p,
                COUNT(CASE WHEN exit_reason_p='SL' THEN 1 END) as sl_count_p,
                COUNT(realized_return_p) as closed_p,
                COUNT(CASE WHEN exit_reason_w='TP' THEN 1 END) as tp_count_w,
                COUNT(CASE WHEN exit_reason_w='SL' THEN 1 END) as sl_count_w,
                COUNT(realized_return_w) as closed_w
            FROM paper_portfolio
            WHERE signal_date >= ?
        """, (cutoff,))
        row = db.fetchone() or {}

        # compute rates in Python (avoids NULLIF / cast differences)
        def pct(num, den):
            return round(100.0 * num / den, 1) if den else None

        overall = {
            "total":     row.get("total"),
            "closed":    row.get("closed"),
            "open":      row.get("open"),
            "avg_ret_p": row.get("avg_ret_p"),
            "avg_ret_w": row.get("avg_ret_w"),
            "tp_rate_p": pct(row.get("tp_count_p", 0), row.get("closed_p", 0)),
            "sl_rate_p": pct(row.get("sl_count_p", 0), row.get("closed_p", 0)),
            "tp_rate_w": pct(row.get("tp_count_w", 0), row.get("closed_w", 0)),
            "sl_rate_w": pct(row.get("sl_count_w", 0), row.get("closed_w", 0)),
        }

        db.execute("""
            SELECT beta_zone, COUNT(*) as n,
                   ROUND(AVG(realized_return_p), 2) as avg_p,
                   ROUND(AVG(realized_return_w), 2) as avg_w,
                   COUNT(CASE WHEN exit_reason_p='TP' THEN 1 END) as tp_count,
                   COUNT(realized_return_p) as closed_p
            FROM paper_portfolio
            WHERE status='CLOSED' AND signal_date >= ?
            GROUP BY beta_zone ORDER BY avg_p DESC
        """, (cutoff,))
        by_zone = [
            {**r, "tp_rate_p": pct(r.get("tp_count", 0), r.get("closed_p", 0))}
            for r in db.fetchall()
        ]

        db.execute("""
            SELECT tz_sig, COUNT(*) as n,
                   ROUND(AVG(realized_return_p), 2) as avg_p,
                   ROUND(AVG(realized_return_w), 2) as avg_w
            FROM paper_portfolio
            WHERE status='CLOSED' AND signal_date >= ?
            GROUP BY tz_sig ORDER BY avg_p DESC
        """, (cutoff,))
        by_signal = db.fetchall()

        db.execute("""
            SELECT tier, COUNT(*) as n,
                   ROUND(AVG(realized_return_p), 2) as avg_p,
                   ROUND(AVG(realized_return_w), 2) as avg_w
            FROM paper_portfolio
            WHERE status='CLOSED' AND signal_date >= ?
            GROUP BY tier ORDER BY avg_p DESC
        """, (cutoff,))
        by_tier = db.fetchall()

    return {
        "period_days": days,
        "overall":    overall,
        "by_zone":    by_zone,
        "by_signal":  by_signal,
        "by_tier":    by_tier,
    }


@router.get("/export")
def export_for_replay(days: int = 90):
    """Export closed trades compatible with replay analytics."""
    cutoff = str(date.today() - timedelta(days=days))
    with get_db() as db:
        db.execute("""
            SELECT signal_date, ticker, exchange, beta_zone, beta_score, tz_sig,
                   ultra_score, ultra_band, tier, sweet_spot,
                   turbo_score, rtb_total, rtb_phase,
                   entry_price, hold_days,
                   realized_return_p, exit_reason_p,
                   realized_return_w, exit_reason_w
            FROM paper_portfolio
            WHERE status='CLOSED' AND signal_date >= ?
            ORDER BY signal_date
        """, (cutoff,))
        data = db.fetchall()
    return {"count": len(data), "trades": data}


@router.get("/")
def get_portfolio(
    status:    Optional[str] = None,
    beta_zone: Optional[str] = None,
    tz_sig:    Optional[str] = None,
    tier:      Optional[str] = None,
    exchange:  Optional[str] = None,
    days:      int           = 30,
):
    """List portfolio entries with optional filters."""
    cutoff = str(date.today() - timedelta(days=days))
    conditions = ["signal_date >= ?"]
    params: list = [cutoff]
    if status:    conditions.append("status=?");    params.append(status)
    if beta_zone: conditions.append("beta_zone=?"); params.append(beta_zone)
    if tz_sig:    conditions.append("tz_sig=?");    params.append(tz_sig)
    if tier:      conditions.append("tier=?");      params.append(tier)
    if exchange:  conditions.append("exchange=?");  params.append(exchange)

    sql = ("SELECT * FROM paper_portfolio WHERE "
           + " AND ".join(conditions)
           + " ORDER BY signal_date DESC, ultra_score DESC")
    with get_db() as db:
        db.execute(sql, params)
        data = db.fetchall()
    return {"count": len(data), "entries": data}
