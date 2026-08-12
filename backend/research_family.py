"""The object above the session: what a claim's selection history actually cost.

`ResearchSession` accounts for one working sitting. That was already the right unit for "did this
click create a new claim", and it is the wrong unit for "how many claims was this winner chosen
from", because a researcher does not stop thinking when a session closes.

    ResearchFamily F42
      s0001 EXPLORE   RSI ±5 · ±2 · ±1
      s0002 EXPLORE   forked, horizon 20 · 40
      s0003 REGISTER  claim RSI ±1 / horizon 40

Three sessions, one selection history. The multiplicity that a verdict on the third must survive
is the whole family, not the session that happened to be open when the winner was written down.

    k_family = | ∪ ClaimsSelectable(s) |     NOT  Σ k(s)   and NOT  k(current session)

The union is what makes this correct in both directions. Summing double-counts a specification
opened in two sessions; using the current session undercounts everything that came before it.

TWO KINDS OF PARENT, DELIBERATELY SEPARATE.

    session_parent_id     a technical fork: this session literally continues that one
    research_family_id    a statistical history: these sessions chose one claim together

A new UI session need not be a new family, and the button that creates one should not read
"New session" as though the only question were which tab you are in. Continuing the work in a
fresh session is the normal case and belongs in the same family; declaring independence is a
claim about what the researcher knows, and the system takes it as a declaration rather than a
fact — which is why family membership never decides confirmatory standing on its own. That is
`EvidenceBoundary`'s job, and it reads the whole durable history precisely so that renaming the
family changes nothing about which data has been seen.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from evidence_boundary import (EvidenceBoundary, ExposureRegistry,  # noqa: E402
                               confirmatory_verdict)

EXPOSED, SEARCH_RUN, FROZEN, FORKED = (
    "RESULT_EXPOSED", "SEARCH_RUN", "SESSION_FROZEN", "SESSION_FORKED")


@dataclass(frozen=True)
class FamilyAccounting:
    family_id: str
    session_ids: tuple
    k_family_exposed: int
    k_family_selectable: int
    k_family_selectable_is_bound: bool
    distinct_spaces: tuple
    registered_sessions: tuple
    fork_edges: tuple
    events: int

    @property
    def family_hash(self) -> str:
        blob = f"{self.family_id}|{'|'.join(self.session_ids)}|{self.k_family_selectable}"
        return hashlib.sha256(blob.encode()).hexdigest()[:16]


class ResearchFamily:
    """Read-only over the durable ledger. It computes; it never records."""

    def __init__(self, family_id: str, events: list):
        self.family_id = family_id
        self.events = [e for e in events if e.family_id == family_id]
        self.all_events = list(events)          # contamination is answered globally, not here

    # ── multiplicity across the whole selection history ─────────────────────
    def accounting(self) -> FamilyAccounting:
        sessions, exposed, spaces, registered, forks = [], set(), {}, [], []
        for e in self.events:
            if e.session_id not in sessions:
                sessions.append(e.session_id)
            if e.event_type == EXPOSED and e.claim_hash:
                exposed.add(e.claim_hash)
            elif e.event_type == SEARCH_RUN:
                key = (e.payload.get("space_id", ""), e.payload.get("space_hash", ""))
                spaces[key] = max(spaces.get(key, 0), int(e.payload.get("space_size", 0)))
            elif e.event_type == FROZEN and e.session_id not in registered:
                registered.append(e.session_id)
            elif e.event_type == FORKED and e.payload.get("child_session_id"):
                edge = (e.payload.get("parent_session_id", ""),
                        e.payload["child_session_id"])
                if edge not in forks:
                    forks.append(edge)

        # Distinct SPACES are summed, distinct CLAIMS are unioned. Two searches of the same space
        # contribute it once; two different spaces are summed because the ledger records their
        # sizes, not their members, so their true union is unknown. Summing is the conservative
        # direction — it can overstate multiplicity, never understate it — and the flag says so
        # rather than letting a bound be read as a count.
        spaces_total = sum(spaces.values())
        return FamilyAccounting(
            family_id=self.family_id, session_ids=tuple(sessions),
            k_family_exposed=len(exposed),
            k_family_selectable=max(spaces_total, len(exposed)),
            k_family_selectable_is_bound=len(spaces) > 1,
            distinct_spaces=tuple(sorted(f"{a}@{b}:{spaces[(a, b)]}" for a, b in spaces)),
            registered_sessions=tuple(registered), fork_edges=tuple(forks),
            events=len(self.events))

    # ── confirmatory standing ───────────────────────────────────────────────
    def registry(self) -> ExposureRegistry:
        """Built from EVERY event in the store, not from this family's.

        The whole point: pressing "start independent research" mints a new family_id and unsees
        nothing. If this line filtered by family, the laundering path would be a button.
        """
        return ExposureRegistry.from_events(self.all_events)

    def confirmatory(self, boundary: EvidenceBoundary | None) -> dict:
        from data_gateway import access_completeness
        acc = self.accounting()
        # completeness is asked of the WHOLE durable history, for the same reason contamination
        # is: a read that went around the gateway in another family is still a read
        v = confirmatory_verdict(registered=bool(acc.registered_sessions), boundary=boundary,
                                 registry=self.registry(),
                                 completeness=access_completeness(self.all_events))
        v["k_family_selectable"] = acc.k_family_selectable
        v["k_family_exposed"] = acc.k_family_exposed
        v["sessions_in_family"] = len(acc.session_ids)
        return v


def families(events: list) -> dict:
    """family_id → ResearchFamily, over one durable history."""
    ids = []
    for e in events:
        if e.family_id not in ids:
            ids.append(e.family_id)
    return {fid: ResearchFamily(fid, events) for fid in ids}
