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

GROUND TRUTH (measured on 8M+ historical bars, walk-forward + Bonferroni-validated;
do NOT override with intuition):
- HH-edge (structural higher-high continuation) does NOT convert to P&L through
  tradeable ATR exits. Combo Lab proved this with n=20k OOS bars: top HH-passing
  combos return realized −1.6%..−3.1% avg P&L. Ignore HH-edge as a profit signal.
- REAL P&L edge is THIN but exists at LONG horizons (H=10d), not short. Top single
  predicates: blue +0.13%, squeeze +0.10%, clm +0.09%, fri34 +0.09%, best +0.07%
  (per-trade after asymmetric exit, vs baseline +0.95% on H=10d). Building blocks:
  L-codes (blue/fri34) and VABS (clm/best/squeeze).
- Multi-predicate combos can stack P&L edge to +0.5%..+0.9% on H=10 (greedy beam,
  see combo_catalog_pnl). Examples: blue+cci0r+conso+fly_abcd (n=772, +0.66%),
  clm+squeeze (n=11715, +0.14% most-stable).
- V3 is an HH-predictor, NOT a P&L-predictor (v3_ge40 edge -0.16%). Do NOT raise
  conviction just because V3 is high. Look at the candidate's `pnl_evidence` and
  `matched_combos` first.
- MARKET-CAP: mega/large best; micro is auto-blocked. Sector barely matters.
- HORIZON: hold ~10 days. Short holds (5d) kill the edge before it accrues.

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
    metamap = mem.load_ticker_meta()
    out, seen = [], set()
    for _, r in rows.iterrows():
        d = r.to_dict()
        if d["ticker"] in seen:      # a ticker can sit in >1 universe — keep highest V3 (rows are V3-desc)
            continue
        seen.add(d["ticker"])
        d["tz_sig"] = d.get("t_sig") or d.get("z_sig") or ""
        m = metamap.get(d["ticker"], {})
        d["sector"] = m.get("sector") or ""
        d["mcap_bucket"] = m.get("mcap_bucket") or "unknown"
        d["market_cap"] = m.get("market_cap")
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
                    j.execute("SELECT id, ticker, sector FROM journal_position "
                              "WHERE status IN ('OPEN','PENDING_OPEN')").fetchall()]
    finally:
        j.close()

    # Candidates + memory
    cands = rails.entry_filter(load_candidates(as_of), top_n=top_n)
    if not cands:
        return {"as_of": as_of, "candidates": 0, "note": "no eligible candidates"}
    t1 = mem.load_tier1_index(as_of)
    pnl_idx = mem.load_pnl_edges(horizon=rails.HORIZON_DAYS)
    passed_combos = mem.load_passed_combos(horizon=rails.HORIZON_DAYS)
    bl = {b["pattern"] for b in mem.active_blacklist()}
    from . import regime as regime_mod
    reg = regime_mod.compute_regime(as_of)

    prompt_cands = []
    for c in cands:
        fp = mem.fingerprint(c)
        ev = mem.candidate_evidence(c, t1)
        # find which atoms this candidate satisfies (by reasons-tag intersection)
        from .memory import TAG2PRED
        reasons_text = str(c.get("prebreak_v3_reasons") or "")
        atoms_here = {pred for tag, pred in TAG2PRED.items() if tag in reasons_text}
        if float(c.get("prebreak_v3") or 0) >= 40: atoms_here.add("v3_ge40")
        elif float(c.get("prebreak_v3") or 0) >= 30: atoms_here.add("v3_ge30")
        # P&L per-atom edges (the truth — Combo Lab validated)
        pnl_ev = []
        for a_ in sorted(atoms_here):
            s = pnl_idx.get(a_)
            if s:
                pnl_ev.append({"signal": a_, "n": s["n"],
                               "edge_pnl_pct": round(s["edge_avg"], 3),
                               "edge_win_pp": round(s["edge_win"], 1)})
        pnl_ev.sort(key=lambda e: -e["edge_pnl_pct"])
        # matched validated combos (atoms-subset of any passed combo)
        matched = [c2 for c2 in passed_combos if c2["atoms"].issubset(atoms_here)]
        matched.sort(key=lambda c2: -c2["edge"])
        matched_brief = [{"combo": ",".join(sorted(c2["atoms"])),
                          "edge_pnl_pct": round(c2["edge"], 3), "n": c2["n"]}
                         for c2 in matched[:3]]

        prompt_cands.append({
            "ticker": c["ticker"], "price": round(float(c["last_price"]), 2),
            "v3": int(c["prebreak_v3"] or 0), "reasons": c.get("prebreak_v3_reasons") or "",
            "tz": c.get("tz_sig"), "phase": c.get("rtb_phase"), "rsi": round(float(c["rsi"] or 0), 0),
            "sector": c.get("sector") or "?", "mcap": c.get("mcap_bucket") or "unknown",
            "fingerprint": fp,
            "pnl_evidence":   pnl_ev[:6],                          # ← REAL P&L per-atom edge (H=10)
            "matched_combos": matched_brief,                       # ← validated multi-predicate combos
            "hh_evidence":    ev[:4],                              # ← legacy HH (informational; do not trade on)
            "tier2_own":      mem.tier2_for(fp, as_of),
        })

    user = {
        "as_of": as_of,
        "account": {"capital": capital, "open_positions": len(open_pos), "max_open": rails.MAX_OPEN},
        "market_regime": {"label": reg["label"], "score": reg["score"], "breadth": reg["breadth"]},
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
            size_pct = round(min(size_pct * reg["conv_mult"], rails.MAX_POS_PCT), 4)  # regime gate
            ok, why = rails.can_open(tk, c.get("sector"), open_pos, bl, fp, size_pct,
                                     capital, mcap_bucket=c.get("mcap_bucket"))
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
                 atr_at_decision, sector, mcap_bucket, verdict, thesis, evidence_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,current_timestamp,?,?,?,'PENDING',?,?)""",
                [pid, tk, c.get("universe"), as_of, opened_at, "BUY", d["conviction"], fp,
                 entry, size_pct, shares, stop, target, rails.HORIZON_DAYS, status, mode,
                 sess_state, atr, c.get("sector"), c.get("mcap_bucket"),
                 d["thesis"], json.dumps(ev)])
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
    return {"as_of": as_of, "session": sess_state, "regime": reg["label"],
            "candidates": len(cands), "opened": opened, "pending_open": pending,
            "refused": refused, "decisions": decisions.get("decisions", []),
            "usage": usage, "duration_sec": round(dur, 1)}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    as_of = sys.argv[1] if len(sys.argv) > 1 else None
    res = run_session(as_of)
    print(json.dumps({k: v for k, v in res.items() if k != "decisions"}, indent=2, default=str))
    print("\n=== decisions ===")
    for d in res.get("decisions", []):
        print(f"  {d['ticker']:6} {d['action']:5} conv={d['conviction']:3}  {d['thesis'][:90]}")
