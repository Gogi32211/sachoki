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
- `anti_matched_combos` are combos the candidate SATISFIES but that were tested
  and REJECTED by the same walk-forward pipeline. Treat each as a warning:
    * reason="oos edge non-positive" — proven negative on OOS, strong negative
    * reason="oos edge collapsed"    — train edge gone OOS, evidence of overfit
    * reason="low_n"                 — too few bars, uncertain (modest weight)
    * reason="bonferroni p too high" — noise after multiple-testing, weak signal
  A candidate matching one good combo BUT also a severely-rejected combo is
  NOT a clean BUY — call it out and cap conviction.
- MARKET-CAP: mega/large best; micro is auto-blocked. Sector barely matters.
- HORIZON: hold ~10 days. Short holds (5d) kill the edge before it accrues.
- Some candidates carry a `zone_edge` block: an INDEPENDENT, OOS-validated setup
  (price retested a high-volume zone and T/Z flipped up within 3 bars). It backtests
  ~60% win at H=10 and HOLDS out-of-sample (n=336) — unlike HH-edge, treat it as
  REAL P&L evidence, comparable to a strong matched_combo. `insider_buy_90d=true`
  means a SEC Form-4 open-market insider buy in the last 90 days (extra conviction).
  This is a separate edge from the prebreak V3/V4 pipeline; weigh the confluence.
- Some candidates carry an `engulf_edge` block: a full-DB-measured sp500 "buy-
  weakness" setup — an engulf (T4/T6 or Z4/Z6) on BIG volume (vol=B) + an L-line
  code. Measured +0.6%..+2.1% MEDIAN @10d in sp500 (`edge_med10_pct`). KEY: when
  `contrarian=true` the trigger is a BEARISH engulf — counter-intuitive, but on big
  volume in sp500 it is a SHAKEOUT that bounces (the strongest variant, Z6+L5+B,
  is +2.1% / 65% win). This edge is sp500-only and direction-agnostic: the bar's
  bull/bear look is NOT the signal — the universe(sp500) × weakness × vol=B is.
  Treat `edge_med10_pct` as REAL P&L evidence (median, large-n). Do NOT down-rate a
  contrarian bear-engulf candidate for "looking bearish" — that look IS the edge.
  When `bars=4` the block is the 4-BAR PULLBACK-RESUME: a bull engulf (T4/T6) on big
  volume → a bearish pullback → a bullish T-turn TODAY (you enter on the confirmed
  turn). It is cleaner and higher-edge than the 2-bar (T4-trigger +2.29% / 58% win)
  — weight it as a strong matched setup.

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
        # Rank by V4 (P&L-weighted, validated) but accept any bar where V4 >= 8.
        # V3 is kept in the row for legacy display/comparison; V4 drives the rank.
        rows = a.execute("""
            SELECT ticker, universe, close AS last_price, atr_14, rsi_14 AS rsi,
                   vol_bucket, rtb_phase, t_sig, z_sig,
                   prebreak_v3, prebreak_v3_reasons,
                   prebreak_v4, prebreak_v4_reasons, sector
            FROM bars
            WHERE date = ? AND prebreak_v4 >= 8
            ORDER BY prebreak_v4 DESC, prebreak_v3 DESC LIMIT ?
        """, [as_of, limit]).fetchdf()
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


def zone_edge_candidates(as_of: str, max_age_days: int = 3, limit: int = 12) -> list[dict]:
    """Second candidate stream: CONFIRMED robust Zone-Edge setups (price retested a
    high-volume zone and T/Z flipped up within 3 bars, on a big-volume bar). Each
    carries its bar-row context + a `zone_edge` evidence block (OOS-validated
    pattern + zone + recent-insider flag) so the LLM can weigh the confluence."""
    from .zone_events import live_setups
    try:
        res = live_setups(event_type="retest", slots={"vol": "B"}, require_flip=True,
                          max_age_days=max_age_days, vol_min=5.0)
    except Exception as e:
        log.warning("zone_edge_candidates failed: %s", e)
        return []
    setups = {s["ticker"]: s for s in res.get("setups", []) if s.get("status") == "confirmed"}
    if not setups:
        return []
    tickers = list(setups)
    ph = ",".join("?" * len(tickers))
    a = get_analytics_conn()
    try:
        rows = a.execute(f"""
            SELECT ticker, universe, close AS last_price, atr_14, rsi_14 AS rsi,
                   vol_bucket, rtb_phase, t_sig, z_sig,
                   prebreak_v3, prebreak_v3_reasons, prebreak_v4, prebreak_v4_reasons, sector
            FROM bars WHERE date = ? AND ticker IN ({ph})
        """, [as_of] + tickers).fetchdf()
    finally:
        a.close()
    insiders: set = set()
    try:
        j = get_journal_conn(read_only=True)
        try:
            insiders = {r[0] for r in j.execute(
                f"SELECT DISTINCT ticker FROM insider_tx WHERE code='P' AND ticker IN ({ph}) "
                f"AND tx_date >= (DATE '{as_of}' - INTERVAL 90 DAY)", tickers).fetchall()}
        finally:
            j.close()
    except Exception:
        pass
    metamap = mem.load_ticker_meta()
    out, seen = [], set()
    for _, r in rows.iterrows():
        d = r.to_dict()
        tk = d["ticker"]
        if tk in seen:
            continue
        seen.add(tk)
        s = setups[tk]
        d["tz_sig"] = d.get("t_sig") or d.get("z_sig") or ""
        m = metamap.get(tk, {})
        d["sector"] = m.get("sector") or ""
        d["mcap_bucket"] = m.get("mcap_bucket") or "unknown"
        d["market_cap"] = m.get("market_cap")
        d["zone_edge"] = {
            "setup": "retest + T/Z-flip + vol=B",
            "validation": "OOS-validated ~60% win@10d, holds out-of-sample (n=336)",
            "vol_mult": s.get("z_mult"), "zone": [s.get("zone_low"), s.get("zone_high")],
            "days_ago": s.get("days_ago"), "insider_buy_90d": tk in insiders,
        }
        out.append(d)
    return out[:limit]


# Validated sp500 "buy-weakness" engulf setups — (engulf_sig, l_sig) → (label,
# validation, median fwd_10d %). Measured on the full DB (8.3M bars): an engulf on
# BIG volume + an L-line code in sp500 returns +0.6%..+2.1% median @10d. Contrarian:
# a BEARISH engulf on big volume in sp500 is a shakeout that bounces (the strongest
# longs). nasdaq's mirror (short bull-engulf×strength) is NOT paper-traded (long-only).
_ENGULF_SETUPS = {
    ("Z6", "L5"):  ("bear-engulf + L5 (vol↓+down) + vol=B", "+2.13% med@10d, 65% win (n=155, sp500)",  2.13),
    ("Z6", "L25"): ("bear-engulf + L25 + vol=B",            "+1.50% med@10d, 64% win (n=141, sp500)",  1.50),
    ("T6", "L12"): ("bull-engulf + L12 + vol=B",            "+1.13% med@10d, 59% win (n=190, sp500)",  1.13),
    ("Z4", "L25"): ("bear-engulf + L25 + vol=B",            "+1.08% med@10d, 58% win (n=249, sp500)",  1.08),
    ("Z4", "L46"): ("bear-engulf + L46 + vol=B",            "+0.75% med@10d, 56% win (n=3538, robust)", 0.75),
    ("Z4", "L5"):  ("bear-engulf + L5 + vol=B",             "+0.73% med@10d, 55% win (n=247, sp500)",  0.73),
    ("T4", "L12"): ("bull-engulf + L12 + vol=B",            "+0.62% med@10d, 54% win (n=505, sp500)",  0.62),
    ("Z6", "L46"): ("bear-engulf + L46 + vol=B",            "+0.61% med@10d, 55% win (n=1746, sp500)", 0.61),
}


def engulf_edge_candidates(as_of: str, limit: int = 12) -> list[dict]:
    """Fourth candidate stream: sp500 'buy-weakness' engulf setups — a bull OR bear
    engulf on BIG volume + a specific L-line code (see _ENGULF_SETUPS). Full-DB
    measured +0.6%..+2.1% median @10d, sp500 only. The bearish-engulf variants are
    CONTRARIAN (a shakeout that bounces). Each carries an `engulf_edge` evidence
    block so the LLM can weigh the confluence."""
    a = get_analytics_conn()
    try:
        rows = a.execute("""
            SELECT ticker, universe, close AS last_price, atr_14, rsi_14 AS rsi,
                   vol_bucket, rtb_phase, t_sig, z_sig, l_sig,
                   prebreak_v3, prebreak_v3_reasons, prebreak_v4, prebreak_v4_reasons, sector
            FROM bars
            WHERE date = ? AND universe = 'sp500' AND vol_bucket = 'B'
              AND (t_sig IN ('T4','T6') OR z_sig IN ('Z4','Z6'))
              AND l_sig IS NOT NULL AND l_sig <> ''
        """, [as_of]).fetchdf()
    except Exception as e:
        log.warning("engulf_edge_candidates failed: %s", e)
        return []
    finally:
        a.close()
    metamap = mem.load_ticker_meta()
    out, seen = [], set()
    for _, r in rows.iterrows():
        d = r.to_dict()
        tk = d["ticker"]
        eng = d.get("z_sig") if d.get("z_sig") in ("Z4", "Z6") else d.get("t_sig")
        key = (eng, d.get("l_sig"))
        if key not in _ENGULF_SETUPS or tk in seen:
            continue
        seen.add(tk)
        label, validation, edge = _ENGULF_SETUPS[key]
        d["tz_sig"] = d.get("t_sig") or d.get("z_sig") or ""
        m = metamap.get(tk, {})
        d["sector"] = m.get("sector") or d.get("sector") or ""
        d["mcap_bucket"] = m.get("mcap_bucket") or "unknown"
        d["market_cap"] = m.get("market_cap")
        d["engulf_edge"] = {
            "setup": label, "validation": validation, "edge_med10_pct": edge,
            "contrarian": eng in ("Z4", "Z6"),   # bear-engulf = contrarian long (shakeout)
        }
        out.append(d)
    out.sort(key=lambda c: c["engulf_edge"]["edge_med10_pct"], reverse=True)
    return out[:limit]


# 4-bar PULLBACK-RESUME (sp500), trigger engulf → median fwd_10d. A bull engulf on
# big volume, then a bearish pullback bar, then a bullish T-turn = momentum resume.
# Stronger and cleaner than the 2-bar buy-weakness (you enter on the confirmed turn).
_ENGULF_4BAR = {
    "T4": ("4-bar pullback-resume: T4 bull-engulf+B → bear pullback → bull-T turn", "+2.29% med@10d, 58% win (n=594, sp500)", 2.29),
    "T6": ("4-bar pullback-resume: T6 bull-engulf+B → bear pullback → bull-T turn", "+1.06% med@10d, 56% win (n=206, sp500)", 1.06),
}


def engulf_pullback_candidates(as_of: str, limit: int = 12) -> list[dict]:
    """Fifth stream: the 4-bar sp500 PULLBACK-RESUME completing TODAY. bar-2 = a bull
    engulf (T4/T6) on BIG volume, bar-1 = a bearish pullback, bar0(as_of) = a bullish
    T-turn (T9/T3/T4) on volume (not W) → enter at today's close. Full-DB measured
    +1.06%..+2.29% median @10d (sp500). Carries an `engulf_edge` block (bars=4)."""
    a = get_analytics_conn()
    try:
        rows = a.execute("""
            WITH x AS (
              SELECT ticker, close AS last_price, atr_14, rsi_14 AS rsi, vol_bucket,
                     rtb_phase, t_sig, z_sig, l_sig, sector, date,
                     prebreak_v3, prebreak_v3_reasons, prebreak_v4, prebreak_v4_reasons,
                     lag(CASE WHEN close < open THEN 1 ELSE 0 END, 1) OVER w AS p1_bear,
                     lag(t_sig, 2) OVER w AS p2_t, lag(vol_bucket, 2) OVER w AS p2_vol
              FROM bars WHERE universe = 'sp500'
              WINDOW w AS (PARTITION BY ticker ORDER BY date))
            SELECT * FROM x
            WHERE date = ? AND t_sig IN ('T9','T3','T4') AND vol_bucket <> 'W'
              AND p1_bear = 1 AND p2_t IN ('T4','T6') AND p2_vol = 'B'
        """, [as_of]).fetchdf()
    except Exception as e:
        log.warning("engulf_pullback_candidates failed: %s", e)
        return []
    finally:
        a.close()
    metamap = mem.load_ticker_meta()
    out, seen = [], set()
    for _, r in rows.iterrows():
        d = r.to_dict()
        tk = d["ticker"]
        trig = d.get("p2_t")
        if trig not in _ENGULF_4BAR or tk in seen:
            continue
        seen.add(tk)
        label, validation, edge = _ENGULF_4BAR[trig]
        d["universe"] = "sp500"
        d["tz_sig"] = d.get("t_sig") or ""
        m = metamap.get(tk, {})
        d["sector"] = m.get("sector") or d.get("sector") or ""
        d["mcap_bucket"] = m.get("mcap_bucket") or "unknown"
        d["market_cap"] = m.get("market_cap")
        d["engulf_edge"] = {
            "setup": label, "validation": validation, "edge_med10_pct": edge,
            "bars": 4, "turn": d.get("t_sig"), "trigger_engulf": trig, "contrarian": False,
        }
        out.append(d)
    out.sort(key=lambda c: c["engulf_edge"]["edge_med10_pct"], reverse=True)
    return out[:limit]


def _fp(c: dict) -> str:
    """Fingerprint; second-stream candidates get a prefix so their pattern memory
    and lessons stay isolated (each edge is validated by the journal separately)."""
    fp = mem.fingerprint(c)
    if c.get("zone_edge"):
        return "ZE|" + fp
    if c.get("engulf_edge"):
        return "EG|" + fp
    return fp


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
    # Second stream: OOS-validated Zone-Edge setups. They bypass the V3 entry gate
    # (validated independently), but the risk rails (can_open / micro-veto) still
    # apply downstream — so only the liquid ones actually get paper-traded.
    have = {c["ticker"]: c for c in cands}
    for z in zone_edge_candidates(as_of):
        if z["ticker"] in have:
            have[z["ticker"]]["zone_edge"] = z["zone_edge"]   # tag existing candidate
        else:
            cands.append(z); have[z["ticker"]] = z
    # Fourth stream: sp500 buy-weakness engulf×L×vol=B setups (same risk rails apply).
    for e in engulf_edge_candidates(as_of):
        if e["ticker"] in have:
            have[e["ticker"]]["engulf_edge"] = e["engulf_edge"]
        else:
            cands.append(e); have[e["ticker"]] = e
    # Fifth stream: 4-bar pullback-resume (sp500). If a ticker also matched the 2-bar
    # buy-weakness, keep whichever engulf_edge has the higher measured edge.
    for e in engulf_pullback_candidates(as_of):
        ex = have.get(e["ticker"])
        if ex:
            if e["engulf_edge"]["edge_med10_pct"] > ex.get("engulf_edge", {}).get("edge_med10_pct", -9):
                ex["engulf_edge"] = e["engulf_edge"]
        else:
            cands.append(e); have[e["ticker"]] = e
    if not cands:
        return {"as_of": as_of, "candidates": 0, "note": "no eligible candidates"}
    t1 = mem.load_tier1_index(as_of)
    pnl_idx = mem.load_pnl_edges(horizon=rails.HORIZON_DAYS)
    passed_combos   = mem.load_passed_combos(horizon=rails.HORIZON_DAYS)
    rejected_combos = mem.load_rejected_combos(horizon=rails.HORIZON_DAYS)
    bl = {b["pattern"] for b in mem.active_blacklist()}
    from . import regime as regime_mod
    reg = regime_mod.compute_regime(as_of)

    prompt_cands = []
    for c in cands:
        fp = _fp(c)
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
        # anti-matched combos: candidate satisfies a TESTED-AND-REJECTED combo.
        # These are warnings — the LLM should NOT treat them as edge. Prefer
        # combos rejected for substance (oos collapse / negative) over those
        # killed only by Bonferroni (those are statistically inconclusive, not
        # outright bad). Cap at 3 most informative.
        anti = [c2 for c2 in rejected_combos if c2["atoms"].issubset(atoms_here)]
        def _anti_severity(c2):
            r = (c2.get("reason") or "")
            if "non-positive" in r: return 3       # worst — edge actually negative OOS
            if "collapsed"    in r: return 2       # train edge collapsed OOS
            if "low_n"        in r: return 1       # uncertain, just thin
            return 0                               # bonferroni — noise, low signal value
        anti.sort(key=lambda c2: (-_anti_severity(c2), -c2["size"], -(c2["n"] or 0)))
        anti_brief = [{"combo": ",".join(sorted(c2["atoms"])),
                       "size": c2["size"], "reason": c2["reason"],
                       "oos_edge_pct": round(c2["oos_edge"] or 0, 3),
                       "train_edge_pct": round(c2["train_edge"] or 0, 3),
                       "n_oos": c2["n"]}
                      for c2 in anti[:3]]

        prompt_cands.append({
            "ticker": c["ticker"], "price": round(float(c["last_price"]), 2),
            "v4": int(c.get("prebreak_v4") or 0), "v4_reasons": c.get("prebreak_v4_reasons") or "",
            "v3": int(c["prebreak_v3"] or 0), "reasons": c.get("prebreak_v3_reasons") or "",
            "tz": c.get("tz_sig"), "phase": c.get("rtb_phase"), "rsi": round(float(c["rsi"] or 0), 0),
            "sector": c.get("sector") or "?", "mcap": c.get("mcap_bucket") or "unknown",
            "fingerprint": fp,
            "pnl_evidence":      pnl_ev[:6],                       # ← REAL P&L per-atom edge (H=10)
            "matched_combos":    matched_brief,                    # ← validated multi-predicate combos
            "anti_matched_combos": anti_brief,                     # ← TESTED-AND-REJECTED combos this satisfies (warnings)
            "hh_evidence":       ev[:4],                           # ← legacy HH (informational; do not trade on)
            "tier2_own":         mem.tier2_for(fp, as_of),
            **({"zone_edge": c["zone_edge"]} if c.get("zone_edge") else {}),
            **({"engulf_edge": c["engulf_edge"]} if c.get("engulf_edge") else {}),
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
            fp = _fp(c)
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
