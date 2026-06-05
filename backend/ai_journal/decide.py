"""
ai_journal/decide.py — Agent A: the daily decision session.

Loads candidates as-of a date, attaches Tier-1/2 memory, asks Sonnet which of the
allowed setups to take (structured output), then the deterministic rails size and
open the paper positions. The LLM proposes; the code disposes.

Run:  python -m ai_journal.decide            # session on latest closed date
      python -m ai_journal.decide 2026-06-04
"""
from __future__ import annotations

import sys
import json
import time
import logging
from datetime import datetime, timezone

from .db import get_analytics_conn, get_journal_conn, ensure_schema, next_id
from . import memory as mem
from . import rails
from .llm import call_structured, MODEL_DECISION

log = logging.getLogger(__name__)

_SYSTEM = """You are the decision engine of a disciplined PAPER-trading journal.

GROUND TRUTH (measured on 8M+ historical bars, do not override with intuition):
- Our signals have a REAL structural edge: they predict whether price makes a
  higher-high continuation (HH edge, in percentage points vs baseline).
- They have ~NO directional 5-day edge (median forward return ≈ 0, big-move lift ≈ 1x).
- Therefore profit comes from ASYMMETRIC EXITS riding HH continuation, not from a
  raw directional bet. Size up only when the HH edge + conviction are genuinely strong.

YOUR JOB: for each candidate, decide BUY / WATCH / SKIP and give conviction 0-100
with a one-sentence thesis grounded in the provided Tier-1 evidence (each signal's
historical HH edge and big-move lift) and Tier-2 (our own past trades on this
fingerprint, if any). Be skeptical: a high V3 with weak/absent HH edge is a SKIP.
Do NOT invent edges not in the evidence. The code handles sizing, stops, and may
veto your BUYs — you only rank what is allowed."""

_SCHEMA = {
    "type": "object",
    "properties": {
        "decisions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string"},
                    "action": {"type": "string", "enum": ["BUY", "WATCH", "SKIP"]},
                    "conviction": {"type": "integer", "minimum": 0, "maximum": 100},
                    "thesis": {"type": "string"},
                },
                "required": ["ticker", "action", "conviction", "thesis"],
            },
        }
    },
    "required": ["decisions"],
}


def load_candidates(as_of: str, min_v3: int = rails.V3_MIN, limit: int = 200) -> list[dict]:
    a = get_analytics_conn()
    try:
        rows = a.execute("""
            SELECT ticker, universe, close AS last_price, atr_14, rsi_14 AS rsi,
                   vol_bucket, rtb_phase, t_sig, z_sig,
                   prebreak_v3, prebreak_v3_reasons, sector
            FROM bars
            WHERE date = ? AND prebreak_v3 >= ?
            ORDER BY prebreak_v3 DESC LIMIT ?
        """, [as_of, min_v3, limit]).fetchdf()
    finally:
        a.close()
    out = []
    for _, r in rows.iterrows():
        d = r.to_dict()
        d["tz_sig"] = d.get("t_sig") or d.get("z_sig") or ""
        out.append(d)
    return out


def run_session(as_of: str | None = None, top_n: int = rails.TOP_N) -> dict:
    ensure_schema()
    t0 = time.time()

    a = get_analytics_conn()
    try:
        if as_of is None:
            as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
    finally:
        a.close()

    j = get_journal_conn(read_only=True)
    try:
        capital = float(j.execute("SELECT capital FROM journal_state WHERE id=1").fetchone()[0])
        open_pos = [dict(zip(["id", "ticker", "sector"], r)) for r in
                    j.execute("SELECT id, ticker, NULL FROM journal_position WHERE status='OPEN'").fetchall()]
    finally:
        j.close()

    # Candidates + memory
    cands = rails.entry_filter(load_candidates(as_of), top_n=top_n)
    if not cands:
        return {"as_of": as_of, "candidates": 0, "note": "no eligible candidates"}
    t1 = mem.load_tier1_index(as_of)
    bl = {b["pattern"] for b in mem.active_blacklist()}

    prompt_cands = []
    for c in cands:
        fp = mem.fingerprint(c)
        ev = mem.candidate_evidence(c, t1)
        prompt_cands.append({
            "ticker": c["ticker"], "price": round(float(c["last_price"]), 2),
            "v3": int(c["prebreak_v3"] or 0), "reasons": c.get("prebreak_v3_reasons") or "",
            "tz": c.get("tz_sig"), "phase": c.get("rtb_phase"), "rsi": round(float(c["rsi"] or 0), 0),
            "fingerprint": fp, "tier1_evidence": ev[:6],
            "tier2_own": mem.tier2_for(fp, as_of),
        })

    user = {
        "as_of": as_of,
        "account": {"capital": capital, "open_positions": len(open_pos), "max_open": rails.MAX_OPEN},
        "active_lessons": mem.active_lessons(),
        "candidates": prompt_cands,
    }

    decisions, usage = call_structured(
        _SYSTEM, json.dumps(user, default=str), _SCHEMA,
        model=MODEL_DECISION, tool_name="emit_decisions", max_tokens=4096,
    )
    decmap = {d["ticker"]: d for d in decisions.get("decisions", [])}

    # ── Execution-realism rule ───────────────────────────────────────────────
    # We cannot trade while the exchange is closed. If the session is OPEN
    # (journal is meant to run ~30 min before the close), fill AT_DECISION at the
    # decision price. If CLOSED (pre/post-market, weekend), defer: PENDING_OPEN,
    # to be filled at the NEXT session's OPEN price (fills.fill_pending_open()).
    from premarket_cache import _regular_session_open
    session_open = bool(_regular_session_open())
    sess_state = "open" if session_open else "closed"

    cand_by_tk = {c["ticker"]: c for c in cands}
    opened, pending, refused = [], [], []
    now = datetime.now(timezone.utc)
    jw = get_journal_conn(read_only=False)
    try:
        for tk, d in decmap.items():
            if d["action"] != "BUY":
                continue
            c = cand_by_tk.get(tk)
            if not c:
                continue
            fp = mem.fingerprint(c)
            ev = mem.candidate_evidence(c, t1)
            best_hh = max([e["hh_edge_pp"] for e in ev], default=0.0)
            size_pct = rails.position_size(d["conviction"], best_hh, capital)
            ok, why = rails.can_open(tk, c.get("sector"), open_pos, bl, fp, size_pct, capital)
            if not ok:
                refused.append({"ticker": tk, "reason": why})
                continue
            atr = float(c.get("atr_14") or 0)
            pid = next_id(jw, "journal_position")
            if session_open:
                entry = float(c["last_price"])
                stop, target = rails.stop_target(entry, atr)
                shares = round(capital * size_pct / entry, 4)
                status, mode, opened_at = "OPEN", "AT_DECISION", now
                opened.append({"ticker": tk, "conviction": d["conviction"], "size_pct": size_pct,
                               "entry": entry, "stop": stop, "target": target})
            else:
                entry = stop = target = shares = None     # resolved at the next open
                status, mode, opened_at = "PENDING_OPEN", "NEXT_OPEN", None
                pending.append({"ticker": tk, "conviction": d["conviction"], "size_pct": size_pct})
            jw.execute("""INSERT INTO journal_position
                (id, ticker, universe, decision_date, opened_at, action, conviction,
                 fingerprint, entry_px, size_pct, shares, stop_px, target_px,
                 horizon_days, status, entry_mode, decided_session, decided_at,
                 atr_at_decision, verdict, thesis, evidence_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,current_timestamp,?,'PENDING',?,?)""",
                [pid, tk, c.get("universe"), as_of, opened_at, "BUY", d["conviction"], fp,
                 entry, size_pct, shares, stop, target, rails.HORIZON_DAYS, status, mode,
                 sess_state, atr, d["thesis"], json.dumps(ev)])
            open_pos.append({"id": pid, "ticker": tk, "sector": c.get("sector")})
        jw.execute("""INSERT INTO journal_session_log
                      (ts, candidates_n, decisions_json, capital_before, capital_after, notes)
                      VALUES (current_timestamp,?,?,?,?,?)""",
                   [len(cands), json.dumps(decisions, default=str), capital, capital,
                    f"as_of={as_of} session={sess_state} opened={len(opened)} "
                    f"pending={len(pending)} refused={len(refused)}"])
        jw.commit()
    finally:
        jw.close()

    dur = time.time() - t0
    log.info("session as_of=%s session=%s: %d cand, %d opened, %d pending_open, %d refused, %.1fs",
             as_of, sess_state, len(cands), len(opened), len(pending), len(refused), dur)
    return {"as_of": as_of, "session": sess_state, "candidates": len(cands),
            "opened": opened, "pending_open": pending, "refused": refused,
            "decisions": decisions.get("decisions", []), "usage": usage,
            "duration_sec": round(dur, 1)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    as_of = sys.argv[1] if len(sys.argv) > 1 else None
    res = run_session(as_of)
    print(json.dumps({k: v for k, v in res.items() if k != "decisions"}, indent=2, default=str))
    print("\n=== decisions ===")
    for d in res.get("decisions", []):
        print(f"  {d['ticker']:6} {d['action']:5} conv={d['conviction']:3}  {d['thesis'][:90]}")
