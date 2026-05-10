"""
Paper Portfolio API — Sachoki Screener
FastAPI endpoints for paper trading tracker

Endpoints:
  POST /portfolio/entry       — add new signal entries
  POST /portfolio/entry-price — set next-day open price (entry)
  POST /portfolio/daily-prices — upsert daily OHLCV
  POST /portfolio/daily-check — check TP/SL/exit for open positions
  GET  /portfolio             — list all entries (filterable)
  GET  /portfolio/stats       — performance summary
  GET  /portfolio/open        — open positions only
  GET  /portfolio/export      — closed trades for replay analytics
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, timedelta
import psycopg2, psycopg2.extras, os

router = APIRouter(prefix="/portfolio", tags=["paper_portfolio"])

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

def dict_rows(cur):
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]

# ── Models ──────────────────────────────────────────────────────────────────

class SignalEntry(BaseModel):
    signal_date:     date
    ticker:          str
    exchange:        str = "NQ"
    ultra_score:     Optional[int]    = None
    ultra_band:      Optional[str]    = None
    ultra_priority:  Optional[str]    = None
    beta_score:      Optional[int]    = None
    beta_zone:       Optional[str]    = None
    tz_sig:          Optional[str]    = None
    turbo_score:     Optional[int]    = None
    rtb_total:       Optional[int]    = None
    rtb_phase:       Optional[str]    = None
    sweet_spot:      bool             = False
    tier:            str              = "TIER1"
    signal_reasons:  Optional[str]    = None
    signal_price:    Optional[float]  = None
    signal_change_pct: Optional[float] = None
    hold_days:       int              = 10  # 10=WATCH, 20=OPTIMAL

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
    conn = get_conn()
    cur  = conn.cursor()
    inserted = 0
    skipped  = 0
    for e in entries:
        # skip duplicate (same signal_date + ticker)
        cur.execute(
            "SELECT id FROM paper_portfolio WHERE signal_date=%s AND ticker=%s",
            (e.signal_date, e.ticker)
        )
        if cur.fetchone():
            skipped += 1
            continue

        hold   = e.hold_days
        max_ex = e.signal_date + timedelta(days=hold + 2)  # +2 for weekend buffer

        cur.execute("""
            INSERT INTO paper_portfolio
            (signal_date, ticker, exchange, ultra_score, ultra_band, ultra_priority,
             beta_score, beta_zone, tz_sig, turbo_score, rtb_total, rtb_phase,
             sweet_spot, tier, signal_reasons, signal_price, signal_change_pct,
             hold_days, max_exit_date, status)
            VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDING')
        """, (
            e.signal_date, e.ticker, e.exchange, e.ultra_score, e.ultra_band,
            e.ultra_priority, e.beta_score, e.beta_zone, e.tz_sig, e.turbo_score,
            e.rtb_total, e.rtb_phase, e.sweet_spot, e.tier, e.signal_reasons,
            e.signal_price, e.signal_change_pct, hold, max_ex
        ))
        inserted += 1

    conn.commit(); cur.close(); conn.close()
    return {"inserted": inserted, "skipped": skipped}


@router.post("/entry-price")
def set_entry_price(prices: list[EntryPrice]):
    """Set next-day market open price. Called morning after signal."""
    conn = get_conn()
    cur  = conn.cursor()
    updated = 0
    for p in prices:
        tp_par  = round(p.open_price * 1.20, 4)
        tp_wide = round(p.open_price * 1.15, 4)
        sl      = round(p.open_price * 0.95, 4)
        cur.execute("""
            UPDATE paper_portfolio
            SET entry_date=%s, entry_price=%s,
                tp_parabolic=%s, tp_wide=%s, sl_price=%s,
                status='OPEN', updated_at=NOW()
            WHERE ticker=%s AND status='PENDING'
              AND signal_date = (
                SELECT MAX(signal_date) FROM paper_portfolio
                WHERE ticker=%s AND status='PENDING'
              )
        """, (p.entry_date, p.open_price, tp_par, tp_wide, sl, p.ticker, p.ticker))
        updated += cur.rowcount
    conn.commit(); cur.close(); conn.close()
    return {"updated": updated}


@router.post("/daily-prices")
def upsert_daily_prices(prices: list[DailyPrice]):
    """Upsert daily OHLCV data for monitoring."""
    conn = get_conn()
    cur  = conn.cursor()
    for p in prices:
        cur.execute("""
            INSERT INTO paper_daily_prices (ticker, price_date, open, high, low, close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticker, price_date)
            DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high,
                          low=EXCLUDED.low, close=EXCLUDED.close,
                          volume=EXCLUDED.volume
        """, (p.ticker, p.price_date, p.open, p.high, p.low, p.close, p.volume))
    conn.commit(); cur.close(); conn.close()
    return {"upserted": len(prices)}


@router.post("/daily-check")
def daily_check():
    """
    Run TP/SL/hold-end check for all OPEN positions.
    Uses paper_daily_prices table. Call once per day after market close.
    """
    conn = get_conn()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("""
        SELECT pp.*, p.high as d_high, p.low as d_low, p.close as d_close, p.price_date
        FROM paper_portfolio pp
        JOIN paper_daily_prices p ON p.ticker=pp.ticker
        WHERE pp.status='OPEN'
          AND p.price_date > pp.entry_date
          AND (pp.exit_date_p IS NULL OR pp.exit_date_w IS NULL)
        ORDER BY pp.ticker, p.price_date
    """)
    rows = cur.fetchall()

    updates = {}
    for r in rows:
        pid   = r['id']
        d     = r['price_date']
        high  = float(r['d_high'])
        low   = float(r['d_low'])
        close = float(r['d_close'])
        max_d = r['max_exit_date']

        if pid not in updates:
            updates[pid] = {'p': None, 'w': None}

        # PARABOLIC check
        if updates[pid]['p'] is None:
            tp_p = float(r['tp_parabolic']) if r['tp_parabolic'] else None
            sl   = float(r['sl_price']) if r['sl_price'] else None
            ep   = float(r['entry_price']) if r['entry_price'] else None
            if tp_p and sl and ep:
                if high >= tp_p:
                    ret = round((tp_p - ep) / ep * 100, 4)
                    updates[pid]['p'] = (d, tp_p, 'TP', ret)
                elif low <= sl:
                    ret = round((sl - ep) / ep * 100, 4)
                    updates[pid]['p'] = (d, sl, 'SL', ret)
                elif d >= max_d:
                    ret = round((close - ep) / ep * 100, 4)
                    updates[pid]['p'] = (d, close, 'HOLD_END', ret)

        # WIDE check
        if updates[pid]['w'] is None:
            tp_w = float(r['tp_wide']) if r['tp_wide'] else None
            sl   = float(r['sl_price']) if r['sl_price'] else None
            ep   = float(r['entry_price']) if r['entry_price'] else None
            if tp_w and sl and ep:
                if high >= tp_w:
                    ret = round((tp_w - ep) / ep * 100, 4)
                    updates[pid]['w'] = (d, tp_w, 'TP', ret)
                elif low <= sl:
                    ret = round((sl - ep) / ep * 100, 4)
                    updates[pid]['w'] = (d, sl, 'SL', ret)
                elif d >= max_d:
                    ret = round((close - ep) / ep * 100, 4)
                    updates[pid]['w'] = (d, close, 'HOLD_END', ret)

    closed = 0
    for pid, ex in updates.items():
        p_done = ex['p'] is not None
        w_done = ex['w'] is not None
        if p_done:
            d,pr,rs,ret = ex['p']
            cur.execute("""
                UPDATE paper_portfolio
                SET exit_date_p=%s, exit_price_p=%s, exit_reason_p=%s,
                    realized_return_p=%s, updated_at=NOW()
                WHERE id=%s
            """, (d, pr, rs, ret, pid))
        if w_done:
            d,pr,rs,ret = ex['w']
            cur.execute("""
                UPDATE paper_portfolio
                SET exit_date_w=%s, exit_price_w=%s, exit_reason_w=%s,
                    realized_return_w=%s, updated_at=NOW()
                WHERE id=%s
            """, (d, pr, rs, ret, pid))
        if p_done and w_done:
            cur.execute(
                "UPDATE paper_portfolio SET status='CLOSED', updated_at=NOW() WHERE id=%s", (pid,))
            closed += 1

    conn.commit(); cur.close(); conn.close()
    return {"checked": len(updates), "closed": closed}


@router.get("/open")
def get_open():
    """All open positions."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT ticker, exchange, signal_date, entry_date, entry_price,
               tp_parabolic, tp_wide, sl_price, max_exit_date,
               ultra_score, ultra_band, beta_zone, tz_sig, tier, sweet_spot
        FROM paper_portfolio WHERE status='OPEN'
        ORDER BY entry_date DESC, ultra_score DESC
    """)
    data = dict_rows(cur)
    cur.close(); conn.close()
    return {"count": len(data), "positions": data}


@router.get("/stats")
def get_stats(days: int = 90):
    """Performance summary — both strategies + breakdown by zone/signal."""
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN status='CLOSED' THEN 1 END) as closed,
            COUNT(CASE WHEN status='OPEN'   THEN 1 END) as open,
            -- PARABOLIC
            ROUND(AVG(realized_return_p)::numeric,2) as avg_ret_p,
            ROUND(100.0 * COUNT(CASE WHEN exit_reason_p='TP' THEN 1 END)
                  / NULLIF(COUNT(realized_return_p),0),1) as tp_rate_p,
            ROUND(100.0 * COUNT(CASE WHEN exit_reason_p='SL' THEN 1 END)
                  / NULLIF(COUNT(realized_return_p),0),1) as sl_rate_p,
            -- WIDE
            ROUND(AVG(realized_return_w)::numeric,2) as avg_ret_w,
            ROUND(100.0 * COUNT(CASE WHEN exit_reason_w='TP' THEN 1 END)
                  / NULLIF(COUNT(realized_return_w),0),1) as tp_rate_w,
            ROUND(100.0 * COUNT(CASE WHEN exit_reason_w='SL' THEN 1 END)
                  / NULLIF(COUNT(realized_return_w),0),1) as sl_rate_w
        FROM paper_portfolio
        WHERE signal_date >= CURRENT_DATE - INTERVAL '%s days'
    """, (days,))
    overall = dict_rows(cur)[0]

    cur.execute("""
        SELECT beta_zone,
            COUNT(*) as n,
            ROUND(AVG(realized_return_p)::numeric,2) as avg_p,
            ROUND(AVG(realized_return_w)::numeric,2) as avg_w,
            ROUND(100.0*COUNT(CASE WHEN exit_reason_p='TP' THEN 1 END)
                  /NULLIF(COUNT(realized_return_p),0),1) as tp_rate_p
        FROM paper_portfolio
        WHERE status='CLOSED' AND signal_date >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY beta_zone ORDER BY avg_p DESC
    """, (days,))
    by_zone = dict_rows(cur)

    cur.execute("""
        SELECT tz_sig,
            COUNT(*) as n,
            ROUND(AVG(realized_return_p)::numeric,2) as avg_p,
            ROUND(AVG(realized_return_w)::numeric,2) as avg_w
        FROM paper_portfolio
        WHERE status='CLOSED' AND signal_date >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY tz_sig ORDER BY avg_p DESC
    """, (days,))
    by_signal = dict_rows(cur)

    cur.execute("""
        SELECT tier, COUNT(*) as n,
            ROUND(AVG(realized_return_p)::numeric,2) as avg_p,
            ROUND(AVG(realized_return_w)::numeric,2) as avg_w
        FROM paper_portfolio
        WHERE status='CLOSED' AND signal_date >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY tier ORDER BY avg_p DESC
    """, (days,))
    by_tier = dict_rows(cur)

    cur.close(); conn.close()
    return {
        "period_days": days,
        "overall":    overall,
        "by_zone":    by_zone,
        "by_signal":  by_signal,
        "by_tier":    by_tier
    }


@router.get("/export")
def export_for_replay(days: int = 90):
    """Export closed trades compatible with replay analytics."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT signal_date, ticker, exchange, beta_zone, beta_score, tz_sig,
               ultra_score, ultra_band, tier, sweet_spot,
               turbo_score, rtb_total, rtb_phase,
               entry_price, hold_days,
               realized_return_p, exit_reason_p,
               realized_return_w, exit_reason_w
        FROM paper_portfolio
        WHERE status='CLOSED'
          AND signal_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY signal_date
    """, (days,))
    data = dict_rows(cur)
    cur.close(); conn.close()
    return {"count": len(data), "trades": data}


@router.get("/")
def get_portfolio(
    status:    Optional[str]  = None,
    beta_zone: Optional[str]  = None,
    tz_sig:    Optional[str]  = None,
    tier:      Optional[str]  = None,
    exchange:  Optional[str]  = None,
    days:      int            = 30
):
    """List portfolio entries with optional filters."""
    conn = get_conn()
    cur  = conn.cursor()
    where = ["signal_date >= CURRENT_DATE - INTERVAL '%s days'"]
    params = [days]
    if status:    where.append("status=%s");    params.append(status)
    if beta_zone: where.append("beta_zone=%s"); params.append(beta_zone)
    if tz_sig:    where.append("tz_sig=%s");    params.append(tz_sig)
    if tier:      where.append("tier=%s");      params.append(tier)
    if exchange:  where.append("exchange=%s");  params.append(exchange)
    q = "SELECT * FROM paper_portfolio WHERE " + " AND ".join(where) + \
        " ORDER BY signal_date DESC, ultra_score DESC"
    cur.execute(q, params)
    data = dict_rows(cur)
    cur.close(); conn.close()
    return {"count": len(data), "entries": data}
