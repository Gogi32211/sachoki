"""
Chart Observations API v2 — auto-fill from stock_stat
User enters only: ticker + date → system prefills everything → user confirms.

Uses db.get_db() abstraction so it works on both PostgreSQL (Railway) and
SQLite (local dev). The `stock_stat` table must be loaded separately into the
backing database (CSV import on Railway Postgres).
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from datetime import date, timedelta

from db import get_db

router = APIRouter(prefix="/obs", tags=["chart_observations"])


# ── Auto-fill lookup ─────────────────────────────────────────────────────────

@router.get("/prefill")
def prefill(ticker: str, obs_date: str):
    """
    Lookup ticker+date from stock_stat table → return prefilled observation data.
    stock_stat table must exist in the DB (imported from CSV on Railway).
    """
    t = ticker.upper()

    select_cols = """
        ticker, date, T, Z, L, F, G, B,
        turbo_score, rtb_total, rtb_phase,
        beta_score, beta_zone, sweet_spot_active,
        signal_score, last_price,
        ultra_score, ultra_score_band_v2, ultra_score_priority,
        ultra_score_reasons
    """

    with get_db() as db:
        try:
            db.execute(
                f"SELECT {select_cols} FROM stock_stat "
                "WHERE ticker=? AND date=? LIMIT 1",
                (t, obs_date),
            )
            row = db.fetchone()

            if not row:
                db.execute(
                    f"SELECT {select_cols} FROM stock_stat "
                    "WHERE ticker=? AND date <= ? "
                    "ORDER BY date DESC LIMIT 1",
                    (t, obs_date),
                )
                row = db.fetchone()
        except Exception as exc:
            msg = str(exc).lower()
            if "stock_stat" in msg and ("does not exist" in msg or "no such table" in msg):
                raise HTTPException(
                    503,
                    "stock_stat data not loaded yet — import the scanner CSV into the DB before using Chart Observations.",
                )
            raise HTTPException(500, f"stock_stat query failed: {exc}")

        if not row:
            raise HTTPException(404, f"No data for {ticker} on {obs_date}")

        db.execute(
            "SELECT date, T, Z, turbo_score, beta_score, ultra_score "
            "FROM stock_stat WHERE ticker=? AND date < ? "
            "ORDER BY date DESC LIMIT 3",
            (t, row['date']),
        )
        prev_bars = db.fetchall()

    return {
        "ticker":          row['ticker'],
        "obs_date":        str(row['date']),
        "t_signal":        row.get('T') or row.get('t'),
        "z_prev_1":        (prev_bars[0].get('Z') or prev_bars[0].get('z')) if len(prev_bars) > 0 else None,
        "z_prev_2":        (prev_bars[1].get('Z') or prev_bars[1].get('z')) if len(prev_bars) > 1 else None,
        "t_prev_1":        (prev_bars[0].get('T') or prev_bars[0].get('t')) if len(prev_bars) > 0 else None,
        "l_signal":        row.get('L') or row.get('l'),
        "f_signal":        row.get('F') or row.get('f'),
        "gog_signal":      row.get('G') or row.get('g'),
        "turbo_score":     row.get('turbo_score'),
        "rtb_total":       row.get('rtb_total'),
        "rtb_phase":       row.get('rtb_phase'),
        "beta_score":      row.get('beta_score'),
        "beta_zone":       row.get('beta_zone'),
        "sweet_spot":      bool(row.get('sweet_spot_active')) if row.get('sweet_spot_active') is not None else False,
        "score_at":        row.get('ultra_score'),
        "entry_price":     row.get('last_price'),
        "ultra_band":      row.get('ultra_score_band_v2'),
        "signal_reasons":  row.get('ultra_score_reasons'),
        "sequence_label":  _build_sequence(prev_bars, row),
        "score_before":    prev_bars[0].get('ultra_score') if prev_bars else None,
    }


def _build_sequence(prev_bars, current):
    """Auto-build sequence string like Z2|Z6→T4."""
    def _sig(d, *keys):
        for k in keys:
            v = d.get(k)
            if v:
                return v
        return ''
    parts = []
    for b in reversed(prev_bars[:2]):
        sig = _sig(b, 'Z', 'z') or _sig(b, 'T', 't')
        if sig:
            parts.append(sig)
    cur_sig = _sig(current, 'T', 't') or _sig(current, 'Z', 'z')
    if cur_sig:
        parts.append(cur_sig)
    return '|'.join(parts) if parts else None


# ── Save observation ─────────────────────────────────────────────────────────

class ObsConfirm(BaseModel):
    obs_date:        str
    ticker:          str
    exchange:        str            = "NQ"
    # auto-filled (from prefill)
    t_signal:        Optional[str]  = None
    z_prev_1:        Optional[str]  = None
    z_prev_2:        Optional[str]  = None
    t_prev_1:        Optional[str]  = None
    sequence_label:  Optional[str]  = None
    l_signal:        Optional[str]  = None
    gog_signal:      Optional[str]  = None
    f_signal:        Optional[str]  = None
    lvbo_present:    bool           = False
    eb_reversal:     bool           = False
    vbo_present:     bool           = False
    score_before:    Optional[int]  = None
    score_at:        Optional[int]  = None
    turbo_score:     Optional[int]  = None
    rtb_phase:       Optional[str]  = None
    rtb_total:       Optional[int]  = None
    beta_zone:       Optional[str]  = None
    sweet_spot:      bool           = False
    entry_price:     Optional[float] = None
    # user-supplied:
    k_signal_match:  Optional[str]  = None   # K1..K11 or NONE
    k_fired:         bool           = False
    entry_quality:   Optional[str]  = None   # PERFECT/GOOD/OK/BAD
    notes:           Optional[str]  = None


@router.post("/save")
def save_observation(obs: ObsConfirm):
    delta = None
    if obs.score_at is not None and obs.score_before is not None:
        delta = obs.score_at - obs.score_before

    sql = """
        INSERT INTO chart_observations
        (obs_date, ticker, exchange,
         t_signal, z_prev_1, z_prev_2, t_prev_1, sequence_label,
         l_signal, gog_signal, f_signal,
         lvbo_present, eb_reversal, vbo_present,
         score_before, score_at, score_delta, turbo_score,
         rtb_phase, rtb_total, beta_zone, sweet_spot,
         entry_price, k_signal_match, k_fired, entry_quality, notes)
        VALUES
        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT (obs_date, ticker, t_signal)
        DO UPDATE SET k_signal_match=EXCLUDED.k_signal_match,
                      entry_quality=EXCLUDED.entry_quality,
                      k_fired=EXCLUDED.k_fired,
                      notes=EXCLUDED.notes,
                      updated_at=CURRENT_TIMESTAMP
    """
    params = (
        obs.obs_date, obs.ticker.upper(), obs.exchange,
        obs.t_signal, obs.z_prev_1, obs.z_prev_2, obs.t_prev_1, obs.sequence_label,
        obs.l_signal, obs.gog_signal, obs.f_signal,
        obs.lvbo_present, obs.eb_reversal, obs.vbo_present,
        obs.score_before, obs.score_at, delta, obs.turbo_score,
        obs.rtb_phase, obs.rtb_total, obs.beta_zone, obs.sweet_spot,
        obs.entry_price, obs.k_signal_match, obs.k_fired,
        obs.entry_quality, obs.notes,
    )
    with get_db() as db:
        db.execute(sql, params)
        db.commit()
    return {"status": "saved", "ticker": obs.ticker.upper()}


# ── Auto-update results from paper_portfolio ─────────────────────────────────

@router.post("/sync-results")
def sync_results_from_portfolio():
    """Auto-populate result_5d/10d from paper_portfolio closed trades."""
    with get_db() as db:
        db.execute("""
            UPDATE chart_observations
            SET result_5d  = pp.realized_return_p,
                result_10d = pp.realized_return_p,
                result_outcome = CASE
                    WHEN pp.realized_return_p > 2  THEN 'WIN'
                    WHEN pp.realized_return_p < -3 THEN 'LOSS'
                    ELSE 'NEUTRAL'
                END,
                updated_at = CURRENT_TIMESTAMP
            FROM paper_portfolio pp
            WHERE chart_observations.ticker   = pp.ticker
              AND chart_observations.obs_date = pp.signal_date
              AND pp.status    = 'CLOSED'
              AND chart_observations.result_10d IS NULL
        """)
        db.commit()
    return {"synced": "ok"}


# ── Stats & calibration ──────────────────────────────────────────────────────

@router.get("/stats")
def get_stats(days: int = 180):
    cutoff = str(date.today() - timedelta(days=days))
    with get_db() as db:
        db.execute("""
            SELECT t_signal, sequence_label, k_signal_match,
                   COUNT(*) as n,
                   ROUND(AVG(result_10d)::numeric, 2) as avg10d,
                   ROUND(AVG(score_delta)::numeric, 1) as avg_score_jump,
                   ROUND(100.0 * COUNT(CASE WHEN result_outcome='WIN' THEN 1 END)
                         / NULLIF(COUNT(result_outcome), 0), 1) as win_rate
            FROM chart_observations
            WHERE obs_date >= ?
            GROUP BY t_signal, sequence_label, k_signal_match
            ORDER BY avg10d DESC NULLS LAST
        """, (cutoff,))
        data = db.fetchall()
    return {"stats": data}


@router.get("/recent")
def get_recent(limit: int = 50):
    """Recent observations for review."""
    with get_db() as db:
        db.execute("""
            SELECT id, obs_date, ticker, t_signal, sequence_label,
                   k_signal_match, entry_quality, k_fired,
                   score_before, score_at, score_delta,
                   result_outcome, result_10d, notes
            FROM chart_observations
            ORDER BY obs_date DESC, id DESC
            LIMIT ?
        """, (limit,))
        data = db.fetchall()
    return {"observations": data}
