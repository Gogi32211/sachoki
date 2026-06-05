"""
ai_journal/memory.py — assemble the 3 memory tiers + retrieval by fingerprint.

The fingerprint is the shared key across all tiers. Tier-1 retrieval decomposes a
candidate into the validated predicates it satisfies (via its prebreak_v3 reasons
tags + tz + phase + V3 thresholds) and attaches the historical edge of each — so
the LLM reasons over real stats, not vibes.
"""
from __future__ import annotations

from .db import get_journal_conn

# reasons-tag (in prebreak_v3_reasons) → Tier-1 predicate name (bootstrap.PREDICATES)
TAG2PRED = {
    "V×20": "vol_20x", "V×10": "vol_10x", "V×5": "vol_5x",
    "ABCD": "fly_abcd", "CD": "fly_cd",
    "ABS+BC": "abs_and_bc", "ABS": "abs", "BC": "bc",
    "SVS": "svs", "CONSO": "conso",
    "PhaseD": "phase_D", "PhaseC": "phase_C",
    "WICK": "wick_up", "LOAD": "load", "SQ": "squeeze", "CCI0R": "cci0r",
    "P3": "preup_p3", "P2": "preup_p2", "P89": "preup_p89", "P50": "preup_p50",
    "BX↑": "bx_up", "FRI34": "fri34", "BL": "blue", "L34": "l34",
    "CLM": "clm", "BEST★": "best", "tLPS": "wt_lps",
}


def _bucket(v, edges, labels):
    for e, lab in zip(edges, labels):
        if v < e:
            return lab
    return labels[-1]


def fingerprint(row: dict) -> str:
    """Canonical key from a candidate/decision row (as-of the decision bar)."""
    v3 = float(row.get("prebreak_v3") or 0)
    rsi = float(row.get("rsi") or row.get("rsi_14") or 0)
    tz = row.get("tz_sig") or row.get("t_sig") or row.get("z_sig") or ""
    ph = row.get("rtb_phase") or ""
    vb = row.get("vol_bucket") or ""
    mc = row.get("mcap_bucket") or "?"
    v3b = _bucket(v3, [10, 20, 30, 40, 999], ["0-9", "10-19", "20-29", "30-39", "40-50"])
    rb  = _bucket(rsi, [30, 45, 55, 70, 999], ["<30", "30-45", "45-55", "55-70", ">70"])
    return f"V3:{v3b}|tz:{tz}|ph:{ph}|vol:{vb}|rsi:{rb}|mc:{mc}"


def load_ticker_meta() -> dict:
    """ticker -> {sector, mcap_bucket, market_cap} from journal.duckdb.ticker_meta."""
    j = get_journal_conn(read_only=True)
    try:
        rows = j.execute("SELECT ticker, sector, mcap_bucket, market_cap FROM ticker_meta").fetchall()
    finally:
        j.close()
    return {r[0]: {"sector": r[1] or "", "mcap_bucket": r[2] or "unknown",
                   "market_cap": r[3]} for r in rows}


def load_tier1_index(as_of: str) -> dict:
    """predicate -> stats dict, for the latest as_of available <= given date."""
    j = get_journal_conn(read_only=True)
    try:
        rows = j.execute("""
            SELECT predicate, n, fwd5_med, win5, big5, lift_big5, hh5, hh5_edge_pp
            FROM signal_outcomes
            WHERE as_of_date = (SELECT max(as_of_date) FROM signal_outcomes WHERE as_of_date <= ?)
        """, [as_of]).fetchall()
    finally:
        j.close()
    return {r[0]: {"n": r[1], "fwd5_med": r[2], "win5": r[3], "big5": r[4],
                   "lift_big5": r[5], "hh5": r[6], "hh5_edge_pp": r[7]} for r in rows}


def candidate_evidence(row: dict, t1: dict) -> list[dict]:
    """The Tier-1 stats for predicates this candidate satisfies (reasons tags +
    V3 thresholds). What the LLM gets as factual support."""
    ev = []
    reasons = str(row.get("prebreak_v3_reasons") or "")
    preds = set()
    for tag, pred in TAG2PRED.items():
        if tag in reasons:
            preds.add(pred)
    v3 = float(row.get("prebreak_v3") or 0)
    if v3 >= 40: preds.add("v3_ge40")
    elif v3 >= 30: preds.add("v3_ge30")
    for p in sorted(preds):
        s = t1.get(p)
        if s:
            ev.append({"signal": p, "n": s["n"], "lift_big5": round(s["lift_big5"], 2),
                       "hh_edge_pp": round(s["hh5_edge_pp"], 1), "fwd5_med": round(s["fwd5_med"] or 0, 2)})
    # strongest-edge first (by HH edge — that's where the real edge is)
    return sorted(ev, key=lambda e: -e["hh_edge_pp"])


def tier2_for(fp: str, as_of: str) -> dict | None:
    j = get_journal_conn(read_only=True)
    try:
        r = j.execute("""SELECT n_trades, win_rate, avg_ret FROM pattern_memory
                         WHERE fingerprint=? AND as_of_date<=? ORDER BY as_of_date DESC LIMIT 1""",
                      [fp, as_of]).fetchone()
    finally:
        j.close()
    return {"n_trades": r[0], "win_rate": r[1], "avg_ret": r[2]} if r else None


def active_lessons() -> list[dict]:
    j = get_journal_conn(read_only=True)
    try:
        rows = j.execute("""SELECT lesson, scope_fingerprint, evidence_n, evidence_lift
                            FROM trade_lesson WHERE status='active' ORDER BY evidence_lift DESC LIMIT 30""").fetchall()
    finally:
        j.close()
    return [{"lesson": r[0], "scope": r[1], "n": r[2], "lift": r[3]} for r in rows]


def active_blacklist() -> list[dict]:
    j = get_journal_conn(read_only=True)
    try:
        rows = j.execute("SELECT pattern, reason FROM signal_blacklist").fetchall()
    finally:
        j.close()
    return [{"pattern": r[0], "reason": r[1]} for r in rows]
