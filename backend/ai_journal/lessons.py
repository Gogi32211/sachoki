"""
ai_journal/lessons.py — Agent B (lesson drafting) + the promotion firewall.

Flow:
  1. generate_lessons_for_closed() — for each CLOSED trade without a lesson yet,
     Haiku drafts a PROVISIONAL lesson (what_worked / what_failed / lesson / tags)
     scoped to the trade's fingerprint.
  2. rebuild_pattern_memory() — aggregate our own closed trades by fingerprint
     (Tier-2): n / win_rate / avg_ret.
  3. promote_lessons() — the firewall: a lesson goes ACTIVE (influences decisions)
     only when Tier-2 backs it (n >= N_MIN AND win_rate clears baseline). Otherwise
     it stays PROVISIONAL (visible as a hypothesis, no effect on money). Active
     lessons that no longer clear the bar are RETIRED.

Run:  python -m ai_journal.lessons
"""
from __future__ import annotations

import json
import time
import logging

from .db import get_journal_conn, ensure_schema, next_id
from .llm import call_structured, MODEL_LESSON

log = logging.getLogger(__name__)

N_MIN          = 8      # min own closed trades on a fingerprint to trust a lesson
WIN_EDGE_MIN   = 8.0    # win_rate must clear 50% by this many pp to go active
BASE_WIN       = 50.0

_LESSON_SYSTEM = """You are the reflection engine of a disciplined paper-trading journal.
Given ONE closed trade (its setup fingerprint, signals, conviction, outcome and the
historical edge of its signals), write a SHORT, concrete lesson. Be specific and
honest; attribute to the SIGNAL/SETUP, not luck. One closed trade is an anecdote —
phrase the lesson as a hypothesis to be confirmed by more trades. Do not invent
edges not present in the evidence."""

_LESSON_SCHEMA = {
    "type": "object",
    "properties": {
        "what_worked": {"type": "string"},
        "what_failed": {"type": "string"},
        "lesson":      {"type": "string"},
        "tags":        {"type": "array", "items": {"type": "string"}},
    },
    "required": ["what_worked", "what_failed", "lesson", "tags"],
}


def generate_lessons_for_closed(limit: int = 50) -> dict:
    ensure_schema()
    j = get_journal_conn(read_only=False)
    made = 0
    try:
        done_ids = {r[0] for r in j.execute(
            "SELECT DISTINCT source_position_ids FROM trade_lesson WHERE source_position_ids IS NOT NULL").fetchall()}
        closed = j.execute("""
            SELECT id, ticker, fingerprint, conviction, entry_px, exit_px, pnl_pct,
                   verdict, exit_reason, thesis, evidence_json, sector, mcap_bucket
            FROM journal_position WHERE status='CLOSED' ORDER BY closed_at DESC LIMIT ?""",
            [limit]).fetchall()
        for row in closed:
            pid = row[0]
            if str(pid) in done_ids:
                continue
            trade = {
                "ticker": row[1], "fingerprint": row[2], "conviction": row[3],
                "entry": row[4], "exit": row[5], "pnl_pct": round(row[6] or 0, 2),
                "verdict": row[7], "exit_reason": row[8], "thesis": row[9],
                "evidence": json.loads(row[10] or "[]"),
                "sector": row[11], "mcap": row[12],
            }
            try:
                out, _ = call_structured(_LESSON_SYSTEM, json.dumps(trade, default=str),
                                         _LESSON_SCHEMA, model=MODEL_LESSON,
                                         tool_name="emit_lesson", max_tokens=600)
            except Exception as e:
                log.warning("lesson gen failed for pid=%s: %s", pid, e)
                continue
            lid = next_id(j, "trade_lesson")
            j.execute("""INSERT INTO trade_lesson
                (id, created_at, scope_fingerprint, what_worked, what_failed, lesson,
                 tags, confidence, status, evidence_n, evidence_lift, source_position_ids)
                VALUES (?, current_timestamp, ?, ?, ?, ?, ?, 0.3, 'provisional', 0, 0, ?)""",
                [lid, row[2], out["what_worked"], out["what_failed"], out["lesson"],
                 ",".join(out.get("tags", [])), str(pid)])
            made += 1
        j.commit()
    finally:
        j.close()
    log.info("generate_lessons: %d new provisional lessons", made)
    return {"lessons_created": made}


def rebuild_pattern_memory(as_of: str | None = None) -> dict:
    ensure_schema()
    j = get_journal_conn(read_only=False)
    try:
        if as_of is None:
            as_of = str(j.execute("SELECT COALESCE(max(exit_date), current_date) FROM journal_position").fetchone()[0])[:10]
        rows = j.execute("""
            SELECT fingerprint, count(*) n,
                   avg(CASE WHEN pnl_pct>0 THEN 1.0 ELSE 0 END) win_rate,
                   avg(pnl_pct) avg_ret, max(exit_date) last_seen
            FROM journal_position WHERE status='CLOSED' AND exit_date <= ?
            GROUP BY fingerprint""", [as_of]).fetchall()
        j.execute("DELETE FROM pattern_memory WHERE as_of_date = ?", [as_of])
        for fp, n, wr, ar, last in rows:
            j.execute("""INSERT INTO pattern_memory (fingerprint, as_of_date, n_trades, win_rate, avg_ret, last_seen)
                         VALUES (?,?,?,?,?,?)""", [fp, as_of, n, wr, ar, last])
        j.commit()
    finally:
        j.close()
    log.info("pattern_memory rebuilt: %d fingerprints (as_of %s)", len(rows), as_of)
    return {"fingerprints": len(rows), "as_of": as_of}


def promote_lessons() -> dict:
    """The firewall: lesson is ACTIVE only if Tier-2 (our own trades) backs it."""
    ensure_schema()
    j = get_journal_conn(read_only=False)
    promoted = retired = held = 0
    try:
        pm = {r[0]: {"n": r[1], "win": r[2]} for r in j.execute(
            """SELECT fingerprint, n_trades, win_rate FROM pattern_memory
               WHERE as_of_date = (SELECT max(as_of_date) FROM pattern_memory)""").fetchall()}
        for lid, scope, status in j.execute(
                "SELECT id, scope_fingerprint, status FROM trade_lesson WHERE status<>'retired'").fetchall():
            ev = pm.get(scope)
            n = ev["n"] if ev else 0
            win = (ev["win"] * 100) if ev else 0
            supported = (n >= N_MIN and (win - BASE_WIN) >= WIN_EDGE_MIN)
            new_status = "active" if supported else "provisional"
            if status == "active" and not supported:
                new_status, retired = "retired", retired + 1
            elif new_status == "active" and status != "active":
                promoted += 1
            else:
                held += 1
            j.execute("""UPDATE trade_lesson SET status=?, evidence_n=?, evidence_lift=?,
                         confidence=?, revalidated_at=current_timestamp WHERE id=?""",
                      [new_status, n, round(win - BASE_WIN, 1),
                       0.8 if supported else 0.3, lid])
        j.commit()
    finally:
        j.close()
    log.info("promote_lessons: %d promoted, %d retired, %d held", promoted, retired, held)
    return {"promoted": promoted, "retired": retired, "held": held}


def reflect() -> dict:
    """Full reflection pass: pattern_memory → lessons → promotion gate."""
    pm = rebuild_pattern_memory()
    lg = generate_lessons_for_closed()
    pr = promote_lessons()
    return {**pm, **lg, **pr}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(reflect())
