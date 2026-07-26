"""brain/learn.py — the self-learning loop. The brain improves itself from two feedbacks:

  1. its OWN mistakes  — closed trades in journal.py: realized win/PnL per edge, blended with the
     historical prior (Bayesian shrinkage). As real trades accumulate they pull confidence toward
     what ACTUALLY happened; a decayed edge is demoted (core->watch->retired), a proven one promoted.
  2. the DATA          — a live edge whose recent-window path-sim goes negative while its history was
     positive is flagged as decayed (edge-decay), independent of whether we traded it.

Everything is written ONLY to the brain's own files (registry findings.json + learning.json) — it
reads market data and its own history, and touches nothing in the live app. Its growing memory is
learning.json: an append-only log of every observation + action, so the brain remembers WHY it
changed its mind. calibrate(apply=False) is a dry-run; apply=True commits.
"""
from __future__ import annotations
import json
import os
from collections import defaultdict

from . import registry, journal

_DIR = os.path.dirname(os.path.abspath(__file__))
_LOG = os.path.join(_DIR, "learning.json")

PRIOR_STRENGTH = 20        # ~trades before realized outcomes move confidence meaningfully
DEMOTE_WIN = 0.45          # posterior win below this (with enough n) -> demote
PROMOTE_WIN = 0.58         # watch edge whose posterior win clears this -> propose promote
TIER_DOWN = {"core": "watch", "watch": "retired"}
TIER_UP = {"watch": "core"}


def _log_read() -> list:
    if not os.path.exists(_LOG):
        return []
    with open(_LOG) as f:
        return json.load(f)


def _log_append(entry: dict) -> None:
    log = _log_read()
    log.append(entry)
    with open(_LOG, "w") as f:
        json.dump(log, f, indent=2, ensure_ascii=False)
        f.write("\n")


def learning_log() -> list:
    """The brain's own memory of what it has learned (why it changed its mind)."""
    return _log_read()


def outcome_stats() -> dict:
    """Per-edge realized performance from the brain's OWN closed trades."""
    closed = journal._load()["closed"]
    by = defaultdict(list)
    for c in closed:
        by[c.get("edge", "?")].append(c)
    out = {}
    for edge, trades in by.items():
        rets = [(t["exit"] / t["entry"] - 1) for t in trades if t.get("entry")]
        wins = sum(1 for r in rets if r > 0)
        out[edge] = {
            "n": len(trades),
            "realized_win": round(wins / len(trades), 3) if trades else None,
            "realized_median": round(100 * sorted(rets)[len(rets) // 2], 2) if rets else None,
            "realized_pnl": round(sum(t.get("pnl", 0) for t in trades), 2),
        }
    return out


def _posterior_win(hist_win: float | None, realized_win: float | None, n: int) -> float | None:
    """Bayesian blend: prior (historical) shrinks toward realized as n grows."""
    if hist_win is None and realized_win is None:
        return None
    if realized_win is None or n == 0:
        return hist_win
    if hist_win is None:
        return realized_win
    return (PRIOR_STRENGTH * hist_win + n * realized_win) / (PRIOR_STRENGTH + n)


def calibrate(apply: bool = False, date: str | None = None) -> dict:
    """The learning step. For each live edge blend historical + realized, decide tier changes,
    and (apply=True) write them to the registry + append lessons to the brain's memory.
    Returns the proposed/applied changes. Pure dry-run by default."""
    if date is None:
        from datetime import date as _d
        date = _d.today().isoformat()
    realized = outcome_stats()
    changes = []
    for e in registry.live_edges():
        eid = e["id"]
        hist_win = (e.get("stats", {}).get("win") or 0) / 100 if e.get("stats", {}).get("win") else None
        r = realized.get(eid, {})
        n, rwin = r.get("n", 0), r.get("realized_win")
        post = _posterior_win(hist_win, rwin, n)
        tier = e.get("tier")
        proposal = None
        if n >= 8 and post is not None and post < DEMOTE_WIN and tier in TIER_DOWN:
            proposal = {"action": "demote", "from": tier, "to": TIER_DOWN[tier],
                        "why": f"posterior win {post:.0%} < {DEMOTE_WIN:.0%} over {n} real trades"}
        elif n >= 12 and post is not None and post >= PROMOTE_WIN and tier in TIER_UP:
            proposal = {"action": "promote", "from": tier, "to": TIER_UP[tier],
                        "why": f"posterior win {post:.0%} >= {PROMOTE_WIN:.0%} over {n} real trades"}
        if proposal:
            proposal.update({"edge": eid, "n": n, "hist_win": hist_win,
                             "realized_win": rwin, "posterior_win": round(post, 3)})
            changes.append(proposal)
            if apply:
                registry.register({"id": eid, "tier": proposal["to"],
                                   "status": "retired" if proposal["to"] == "retired" else "live"})
                _log_append({"date": date, "kind": "calibration", "edge": eid,
                             "observation": proposal["why"], "action": f"{proposal['action']} {proposal['from']}->{proposal['to']}"})
    if not changes:
        summary = "no tier changes" + (" (no closed trades yet — outcome learning idle)"
                                       if not realized else "")
    else:
        summary = f"{len(changes)} change(s) {'applied' if apply else 'proposed'}"
    if apply:
        _log_append({"date": date, "kind": "calibrate_run", "observation": summary,
                     "n_closed": len(journal._load()['closed'])})
    return {"applied": apply, "summary": summary, "realized": realized, "changes": changes}


def revalidate(apply: bool = False, recent_years=("2025", "2026"), date: str | None = None) -> dict:
    """DATA-learning: re-path-sim every live edge on today's frame and compare its RECENT years to
    its full history. An edge that has gone negative recently while its history was positive is
    flagged as DECAYED — independent of whether we traded it. This is 'learns from the data that
    exists and will exist'. apply=True demotes decayed edges + writes recent stats + logs lessons.
    Reads market data + brain files only; touches nothing external."""
    if date is None:
        from datetime import date as _d
        date = _d.today().isoformat()
    try:
        import edge_replay as er
    except Exception as e:
        return {"error": f"edge_replay import failed: {e}", "changes": []}
    col2name = {col: name for name, col in er.SETUPS}

    def _find_row(o, name, d=0):
        if d > 4:
            return None
        if isinstance(o, dict):
            if o.get("setup") == name and "per_year" in o:
                return o
            for v in o.values():
                r = _find_row(v, name, d + 1)
                if r:
                    return r
        elif isinstance(o, list):
            for v in o:
                r = _find_row(v, name, d + 1)
                if r:
                    return r
        return None

    changes = []
    for e in registry.live_edges():
        name = col2name.get(e.get("col"))
        if not name:
            continue
        try:
            res = er.edge_replay(setup=name, months=64, dv_floor=3_000_000)
        except Exception:
            continue
        row = _find_row(res, name)
        if not row:
            continue
        py = row.get("per_year", {}) or {}
        recent_vals = [py[y] for y in recent_years if y in py]
        if not recent_vals:
            continue
        recent_med = sum(recent_vals) / len(recent_vals)
        hist_med = row.get("median", 0)
        rec = {"edge": e["id"], "hist_median": hist_med, "recent_median": round(recent_med, 2),
               "recent_years": recent_years, "worst_year": row.get("worst_year"),
               "pos_years": f"{row.get('pos_years')}/{row.get('total_years')}"}
        decayed = hist_med > 0.3 and recent_med < 0
        weakened = hist_med > 0.3 and 0 <= recent_med < hist_med * 0.4
        if decayed and e.get("tier") in TIER_DOWN:
            rec["verdict"] = "decayed"
            rec["action"] = f"demote {e['tier']}->{TIER_DOWN[e['tier']]}"
            if apply:
                registry.register({"id": e["id"], "tier": TIER_DOWN[e["tier"]],
                                   "stats": {**e.get("stats", {}), "recent_median": round(recent_med, 2)}})
                _log_append({"date": date, "kind": "data_decay", "edge": e["id"],
                             "observation": f"recent {recent_years} median {recent_med:+.2f} vs historical {hist_med:+.2f}",
                             "action": rec["action"]})
        elif weakened:
            rec["verdict"] = "weakened"
            if apply:
                registry.register({"id": e["id"],
                                   "stats": {**e.get("stats", {}), "recent_median": round(recent_med, 2)}})
                _log_append({"date": date, "kind": "data_weakened", "edge": e["id"],
                             "observation": f"recent {recent_med:+.2f} < 40% of historical {hist_med:+.2f}",
                             "action": "flag (kept tier), recent stats recorded"})
        else:
            rec["verdict"] = "ok"
        changes.append(rec)
    flagged = [c for c in changes if c.get("verdict") in ("decayed", "weakened")]
    summary = (f"{len(flagged)} decayed/weakened of {len(changes)} live edges"
               if flagged else f"all {len(changes)} live edges healthy on recent data")
    if apply:
        _log_append({"date": date, "kind": "revalidate_run", "observation": summary})
    return {"applied": apply, "summary": summary, "edges": changes}


if __name__ == "__main__":
    print("── outcome calibrate ──"); print(json.dumps(calibrate(apply=False), indent=2))
    print("── data revalidate ──");  print(json.dumps(revalidate(apply=False), indent=2))
