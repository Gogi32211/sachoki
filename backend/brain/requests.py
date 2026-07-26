"""brain/requests.py — the brain asking the USER for data it can't get itself. When introspection
finds a gap that a human can fill (a real fill price, an earnings/catalyst it can't see, missing
outcome logging, a config switch), it raises a REQUEST. The 🕸 Brain-Map tab shows these as a
'needs your input' inbox; the user answers; the answer is recorded and, where possible, APPLIED
back into the brain (e.g. a fill price corrects the book; a catalyst flags the position).

Isolated: reads gaps + writes ONLY brain/requests.json (+ book.json when applying a fill).
Idempotent: sync() never duplicates an open request with the same id.
"""
from __future__ import annotations
import json
import os
from datetime import datetime

from . import introspect, journal

_DIR = os.path.dirname(os.path.abspath(__file__))
_PATH = os.path.join(_DIR, "requests.json")


def _load() -> list:
    if not os.path.exists(_PATH):
        return []
    with open(_PATH) as f:
        return json.load(f)


def _save(items: list) -> None:
    with open(_PATH, "w") as f:
        json.dump(items, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def sync() -> list:
    """Regenerate open requests from the brain's current gaps. Keeps answered ones; adds new gaps;
    drops open requests whose gap has disappeared (stale)."""
    items = _load()
    by_id = {r["id"]: r for r in items}
    live_gap_ids = set()
    for g in introspect.gaps():
        ask = g.get("ask")
        if not ask:
            continue
        rid = g["id"]
        live_gap_ids.add(rid)
        if rid in by_id:
            # refresh the wording but preserve status/answer
            by_id[rid]["question"] = ask["question"]
            by_id[rid]["kind"] = ask["kind"]
            by_id[rid]["severity"] = g.get("severity")
            by_id[rid]["title"] = g.get("title")
        else:
            items.append({"id": rid, "created": _now(), "kind": ask["kind"],
                          "ticker": ask.get("ticker"), "question": ask["question"],
                          "title": g.get("title"), "severity": g.get("severity"),
                          "status": "open", "answer": None})
    # drop OPEN requests whose gap no longer exists (keep answered ones as history)
    items = [r for r in items if r.get("status") == "answered" or r["id"] in live_gap_ids]
    _save(items)
    return items


def raise_for_position(pos: dict) -> list:
    """Fire the per-position data requests the MOMENT a trade is opened — the brain asking for the
    two things it can't see itself: the real fill price and any earnings/catalyst before the target.
    Called from journal.open_position. Uses the same ids as the gap-scan (fill_/catalyst_) so it
    never double-asks; a fresh open replaces any stale answered request for that ticker."""
    tk = pos.get("ticker")
    if not tk:
        return []
    entry = pos.get("entry")
    target = pos.get("target")
    tgt = f" before your target ${target}" if target else " before your target"
    new = [
        {"id": f"fill_{tk}", "kind": "fill_price", "ticker": tk, "severity": "warn",
         "title": f"{tk}: confirm your real fill",
         "question": f"What price did you actually fill {tk}? (plan was ${entry})"},
        {"id": f"catalyst_{tk}", "kind": "catalyst", "ticker": tk, "severity": "info",
         "title": f"{tk}: earnings / catalyst?",
         "question": f"Does {tk} have earnings or a known catalyst{tgt}?"},
    ]
    items = [r for r in _load() if r["id"] not in {n["id"] for n in new}]  # drop stale same-id
    for n in new:
        n.update({"created": _now(), "status": "open", "answer": None})
        items.append(n)
    _save(items)
    return new


def list_requests(status: str | None = None) -> list:
    items = sync()
    if status:
        items = [r for r in items if r.get("status") == status]
    # open first, newest last
    return sorted(items, key=lambda r: (r.get("status") != "open", r.get("created", "")))


def answer(rid: str, value: str) -> dict:
    """Record the user's answer and APPLY it where the brain can act on it."""
    items = _load()
    r = next((x for x in items if x["id"] == rid), None)
    if r is None:
        return {"error": f"request {rid} not found"}
    r["answer"] = value
    r["status"] = "answered"
    r["answered_at"] = _now()
    applied = _apply(r, value)
    r["applied"] = applied
    _save(items)
    # remember the exchange in the brain's learning memory
    try:
        from .learn import _log_append
        _log_append({"date": r["answered_at"][:10], "kind": "user_data", "edge": None,
                     "ticker": r.get("ticker"), "observation": f"{r['kind']}: {r['question']}",
                     "action": f"answer: {value}" + (f" ({applied})" if applied else "")})
    except Exception:
        pass
    return r


def _apply(r: dict, value: str) -> str | None:
    """Act on an answer when possible. Returns a short note on what was applied, else None."""
    kind = r.get("kind")
    tk = r.get("ticker")
    if kind == "fill_price" and tk:
        try:
            px = float(str(value).replace("$", "").strip())
            doc = journal._load()
            p = next((x for x in doc["positions"] if x["ticker"] == tk), None)
            if p:
                p["entry"] = round(px, 4)
                p["filled"] = True
                p["position_value"] = round(p.get("shares", 0) * px, 2)
                journal._save(doc)
                return f"corrected {tk} fill to ${px}"
        except Exception:
            return None
    if kind == "catalyst" and tk:
        doc = journal._load()
        p = next((x for x in doc["positions"] if x["ticker"] == tk), None)
        if p:
            p["catalyst"] = value
            journal._save(doc)
            return f"noted catalyst on {tk}"
    return None
