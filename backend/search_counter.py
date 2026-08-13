"""A durable k counter any search surface can carry.

The sequence builder got its counter first (`sequence_search_ledger`); the measurement console is
the second surface that needs one, and a third copy of the counting logic is how two counters
eventually disagree about what a repeat is. This is the shared core: same append-only,
checksummed, hash-chained store, same two numbers, same rule —

    the identity is the QUESTION, not the click.

Re-running an identical spec is the same claim looked at twice and does not move `k_distinct`.
The caller decides what a spec contains; this class only canonicalises, hashes and counts it.

Failure keeps the same shape as everywhere else in this project: a vanished ledger reads as
ABSENT (never as zero), and `safe_record` returns "k unknown — not counted" rather than a smaller
number, because a counter that silently stops counting is worse than none.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading

import research_store as RS

HERE = os.path.dirname(os.path.abspath(__file__))
LEDGER_DIR = os.path.join(HERE, "research_ledger")

QUERY_EVENT = "SEARCH_QUERY"


class SearchCounter:
    def __init__(self, filename: str, session: str, note: str = ""):
        self.path = os.path.join(LEDGER_DIR, filename)
        self.session = session
        # Found the hard way: two requests stuck behind one single-flight computation finish
        # together, both read the empty ledger, both append event_id 0, and the chain is CORRUPT
        # from its first two lines. read-head → append must be atomic per counter.
        self._lock = threading.Lock()
        self.note = note or ("k counts DIFFERENT claims; re-running the same one is the same "
                             "claim looked at twice.")

    def _ledger(self) -> RS.DurableLedger:
        return RS.DurableLedger(self.path)

    @staticmethod
    def claim_hash(spec: dict) -> str:
        return hashlib.sha256(
            json.dumps(spec, sort_keys=True, separators=(",", ":"),
                       default=str).encode()).hexdigest()[:16]

    def _events(self) -> list:
        if not os.path.exists(self.path):
            return []
        status, events = self._ledger().status()
        if status == RS.CORRUPT:
            return []
        return [e for e in events if e.event_type == QUERY_EVENT]

    def accounting(self) -> dict:
        status = "ABSENT"
        if os.path.exists(self.path):
            status, _ = self._ledger().status()
        events = self._events()
        return {"k_distinct": len({e.claim_hash for e in events}), "queries": len(events),
                "ledger_status": status, "note": self.note}

    def record(self, spec: dict, extra: dict | None = None) -> dict:
      with self._lock:
        h = self.claim_hash(spec)
        events = self._events()
        seen = sum(1 for e in events if e.claim_hash == h)
        n = len(events)
        self._ledger().append(
            self.session, self.session, QUERY_EVENT, event_id=n,
            prior_state_hash=events[-1].new_state_hash if events else "",
            new_state_hash=hashlib.sha256(
                f"{self.session}|{n + 1}|{h}".encode()).hexdigest()[:16],
            claim_hash=h,
            payload={"spec": spec, "repeat_of_claim": bool(seen), **(extra or {})},
            state="EXPLORING")
        out = self.accounting()
        out.update({"claim_hash": h, "claim_is_new": not seen,
                    "times_this_claim_asked": seen + 1})
        return out

    def safe_record(self, spec: dict, extra: dict | None = None) -> dict:
        """The search may run even when the counting cannot — but the payload says so."""
        try:
            return self.record(spec, extra)
        except Exception as e:                                        # noqa: BLE001
            return {"k_distinct": None, "queries": None, "ledger_status": "UNAVAILABLE",
                    "error": str(e)[:200],
                    "note": "the search ran; the count did not. Treat k as unknown, not zero."}
