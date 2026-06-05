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
                   status, exit_date, exit_px, exit_reason, pnl_pct, verdict, thesis
            FROM journal_position ORDER BY opened_at DESC, id DESC LIMIT 200""")
        open_pos = [p for p in positions if p["status"] == "OPEN"]
        closed   = [p for p in positions if p["status"] == "CLOSED"]
        wins = [p for p in closed if p["verdict"] == "WIN"]
        win_rate = (len(wins) / len(closed) * 100) if closed else None
        avg_ret  = (sum(p["pnl_pct"] or 0 for p in closed) / len(closed)) if closed else None
        last_session = _rows(c, "SELECT ts, candidates_n, notes FROM journal_session_log ORDER BY ts DESC LIMIT 1")
        lessons = _rows(c, """SELECT lesson, scope_fingerprint, status, evidence_n, evidence_lift
                              FROM trade_lesson ORDER BY created_at DESC LIMIT 50""")
        return {
            "state": state,
            "open_positions": open_pos,
            "closed_positions": closed,
            "stats": {"open": len(open_pos), "closed": len(closed),
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


class SessionReq(BaseModel):
    as_of: str | None = None
    top_n: int = 12


@router.post("/session")
def session(req: SessionReq):
    from .decide import run_session
    return run_session(as_of=req.as_of, top_n=req.top_n)


@router.post("/grade")
def grade():
    from .grading import grade_open_positions
    return grade_open_positions()
