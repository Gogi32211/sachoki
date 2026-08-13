"""What the Exact Sequence builder has already looked at.

Nine lines across up to six bars is a search space large enough that almost any combination
will eventually produce an interesting number — because it was looked for. Nothing on that
screen recorded how many were tried, which means every result read as if it were the first one.
This module is the counter, and it is deliberately the smallest honest version:

    k_distinct   how many DIFFERENT sequence claims have been asked
    queries      how many times the button was pressed

WHAT MAKES TWO QUERIES THE SAME CLAIM. The identity is the question, not the click. Re-running an
identical spec — a refresh, a page reload, the same search tomorrow — is the same claim looked at
twice and does not move `k`. What moves it is a change to what is being asked:

    CLAIM        the bar specs, which lines participate, timeframe, pivot variant
    POPULATION   universe, price band, years, months
    PRESENTATION match_rows — a display toggle, and the only field excluded

`match_rows` is out for the same reason `displayed_top_k` was: asking to SEE the matching rows
of a query already run adds no hypothesis. Everything else changes either what is claimed or on
whom, and both are search.

WHY THE COUNT IS NOT AN ALARM. It is not there to stop anyone from exploring — exploring is what
this screen is for. It is there so that a number found on the 200th sequence is read as the 200th
sequence and not as a discovery. The honest use of a large k is to say it out loud next to the
result, and the only way to say it is to have counted.

DURABLE, because a counter that resets is a counter that flatters. It uses the same append-only,
checksummed, hash-chained store as the research ledger; a vanished file is ABSENT rather than
zero.
"""
from __future__ import annotations

import hashlib
import json
import os

import research_store as RS

HERE = os.path.dirname(os.path.abspath(__file__))
LEDGER_DIR = os.path.join(HERE, "research_ledger")
LEDGER_PATH = os.path.join(LEDGER_DIR, "sequence_search.jsonl")

SESSION = "sequence-builder"
QUERY_EVENT = "SEQUENCE_QUERY"

# The one field that describes how a result is shown rather than what was asked.
PRESENTATION_ONLY_FIELDS = ("match_rows",)


def _ledger() -> RS.DurableLedger:
    return RS.DurableLedger(LEDGER_PATH)


def claim_spec(req) -> dict:
    """The question, canonicalised. Empty lines are dropped so that a blank field and an absent
    one are the same claim — otherwise a UI that starts sending `""` would double every count."""
    bars = []
    for b in (req.bars or []):
        bars.append({k: str(v).strip().upper()
                     for k, v in sorted((b or {}).items()) if str(v).strip()})
    strict = {k: bool(v) for k, v in sorted((req.strictness or {}).items()) if v}
    return {
        "bars": bars,
        "n_bars": len(bars),
        "lines": sorted(strict),
        "tf": (req.tf or "1d").lower(),
        "pivot_lr": int(req.pivot_lr or 3),
        "universe": (req.universe or "").lower(),
        "min_price": req.min_price,
        "max_price": req.max_price,
        "years": sorted(req.years) if req.years else None,
        "months": sorted(req.months) if req.months else None,
    }


def claim_hash(spec: dict) -> str:
    return hashlib.sha256(
        json.dumps(spec, sort_keys=True, separators=(",", ":"),
                   default=str).encode()).hexdigest()[:16]


def _events() -> list:
    if not os.path.exists(LEDGER_PATH):
        return []
    status, events = _ledger().status()
    if status == RS.CORRUPT:
        return []                       # accounting() reports the status separately
    return [e for e in events if e.event_type == QUERY_EVENT]


def accounting() -> dict:
    """Two numbers that are not the same number, and the state of the record behind them."""
    status = "ABSENT"
    if os.path.exists(LEDGER_PATH):
        status, _ = _ledger().status()
    events = _events()
    distinct = {e.claim_hash for e in events}
    return {"k_distinct": len(distinct), "queries": len(events),
            "ledger_status": status,
            "note": ("k counts DIFFERENT sequence claims; re-running the same one is the same "
                     "claim looked at twice. A result found on the k-th sequence is the k-th "
                     "sequence.")}


def record(req, *, matches: int | None = None) -> dict:
    """Append one query. Returns the accounting AFTER it, plus whether this claim is new."""
    spec = claim_spec(req)
    h = claim_hash(spec)
    events = _events()
    seen = [e for e in events if e.claim_hash == h]
    n = len(events)
    prior = events[-1].new_state_hash if events else ""
    _ledger().append(
        SESSION, SESSION, QUERY_EVENT, event_id=n,
        prior_state_hash=prior,
        new_state_hash=hashlib.sha256(f"{SESSION}|{n + 1}|{h}".encode()).hexdigest()[:16],
        claim_hash=h,
        payload={"spec": spec, "matches": matches, "repeat_of_claim": bool(seen)},
        state="EXPLORING")
    out = accounting()
    out.update({"claim_hash": h, "claim_is_new": not seen,
                "times_this_claim_asked": len(seen) + 1})
    return out


def safe_record(req, *, matches: int | None = None) -> dict:
    """Never let the counter break the search it is counting — but say so when it fails.

    A counter that silently stops counting is worse than no counter, because the number on screen
    keeps looking authoritative. So a failure returns a payload that says UNAVAILABLE rather than
    a smaller k.
    """
    try:
        return record(req, matches=matches)
    except Exception as e:                                            # noqa: BLE001
        return {"k_distinct": None, "queries": None, "ledger_status": "UNAVAILABLE",
                "error": str(e)[:200],
                "note": "the search ran; the count did not. Treat k as unknown, not as zero."}
