"""What makes a verdict confirmatory: not a fresh session id, but evidence nobody has seen.

The fork rule closed one laundering path — look at forty cells, fork, and the child's counter is
clean. It did not close the one above it:

    s0001 EXPLORE   RSI ±5 weak · RSI ±2 better · RSI ±1 best
    close s0001
    s0002 NEW       preregister RSI ±1        ← formally clean, statistically not

A new session id does not erase what the researcher knows. The hypothesis was selected on the
outcome, and evaluating it again on the same history answers a question whose answer was already
read. `new session ≠ new evidence`.

So confirmatory standing stops being a property of the session and becomes a property of the
DATA. A registered claim needs an `EvidenceBoundary`: a validation window that no session, in
any family, has ever exposed a result from.

    CONTAMINATED   the validation window has been looked at, by anyone, ever
    CLEAN          registered, and the window is untouched in the durable history
    FORWARD        the window did not exist yet when the claim was registered

WHY THE REGISTRY IS GLOBAL AND NOT PER FAMILY. Family membership is a claim the user makes.
"Start independent research" is a button, and pressing it does not unsee 2024–2026. If
contamination were tracked per family, the laundering path would simply be renamed: same data,
new signboard. Exposure is a fact about a window of data, and the ledger already records every
one of them. So the question "has this evidence been used to choose a hypothesis" is answered
from the whole durable history, and no session can answer it in its own favour.

WHY UNKNOWN IS TREATED AS CONTAMINATED. An exposure recorded without a data window means the
system does not know what was looked at. Reading that as clean would make the weakest possible
bookkeeping produce the strongest possible claim, which is precisely backwards. Absence of
evidence about exposure is not evidence of absence.

FORWARD IS THE ONLY UNFAKEABLE ONE. `CLEAN` rests on the ledger being complete: it says nothing
in our records touched this window. `FORWARD` rests on time: the observations did not exist when
the claim was frozen, so no amount of prior looking could have used them. Where a study can
afford to wait, forward validation is the stronger instrument, and the two are reported
separately rather than collapsed into one green flag.
"""
from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass

CONTAMINATED, CLEAN, FORWARD, UNKNOWN = "CONTAMINATED", "CLEAN", "FORWARD", "UNKNOWN"


class EvidenceBoundaryError(Exception):
    """A boundary that cannot support the claim being made on it."""


@dataclass(frozen=True)
class DataWindow:
    """A slice of a named dataset. Dates are ISO strings, both ends inclusive."""
    data_id: str
    start: str
    end: str

    def __post_init__(self):
        for f, v in asdict(self).items():
            if not v or not isinstance(v, str):
                raise EvidenceBoundaryError(f"DataWindow.{f} is empty; a window with a hole in "
                                            f"it cannot be compared to anything")
        if self.start > self.end:
            raise EvidenceBoundaryError(f"window {self.start}..{self.end} runs backwards")

    def overlaps(self, other: "DataWindow") -> bool:
        return (self.data_id == other.data_id
                and self.start <= other.end and other.start <= self.end)

    def as_dict(self) -> dict:
        return asdict(self)

    @property
    def label(self) -> str:
        return f"{self.data_id}[{self.start}..{self.end}]"


@dataclass(frozen=True)
class EvidenceBoundary:
    """Where hypothesis formation stops and evaluation begins."""
    development: DataWindow
    validation: DataWindow
    claim_registered_at: str            # ISO datetime, when the claim was frozen
    data_available_at_registration: str  # latest observation that existed at that moment

    def __post_init__(self):
        if self.development.overlaps(self.validation):
            raise EvidenceBoundaryError(
                f"development {self.development.label} overlaps validation "
                f"{self.validation.label}. A boundary that contains itself is not a boundary.")
        if not self.claim_registered_at or not self.data_available_at_registration:
            raise EvidenceBoundaryError("a boundary must say when the claim was frozen and what "
                                        "data existed at that moment")

    @property
    def is_forward(self) -> bool:
        """The validation observations did not exist when the claim was registered."""
        return self.validation.start > self.data_available_at_registration

    @property
    def boundary_hash(self) -> str:
        blob = (f"{self.development.label}|{self.validation.label}|"
                f"{self.claim_registered_at}|{self.data_available_at_registration}")
        return hashlib.sha256(blob.encode()).hexdigest()[:16]


@dataclass(frozen=True)
class ExposureRecord:
    session_id: str
    family_id: str
    claim_hash: str
    window: DataWindow | None       # None = an exposure whose data footprint was not recorded


class ExposureRegistry:
    """Every window any session has ever produced a result from. Global, by construction."""

    EXPOSING = ("RESULT_EXPOSED", "SEARCH_RUN")

    def __init__(self, records: list):
        self.records = list(records)

    @classmethod
    def from_events(cls, events) -> "ExposureRegistry":
        out = []
        for e in events:
            if e.event_type not in cls.EXPOSING:
                continue
            w = e.payload.get("window")
            out.append(ExposureRecord(
                session_id=e.session_id, family_id=e.family_id, claim_hash=e.claim_hash,
                window=DataWindow(**w) if w else None))
        return cls(out)

    @property
    def unrecorded(self) -> list:
        return [r for r in self.records if r.window is None]

    def touching(self, window: DataWindow) -> list:
        return [r for r in self.records if r.window and r.window.overlaps(window)]

    def status_for(self, boundary: EvidenceBoundary) -> tuple:
        """(status, why). Order matters: an unknown footprint outranks a clean-looking search."""
        hits = self.touching(boundary.validation)
        if hits:
            who = ", ".join(sorted({r.session_id for r in hits})[:4])
            return CONTAMINATED, (
                f"{len(hits)} exposure(s) already read {boundary.validation.label} "
                f"(sessions: {who}). The answer to this question has been seen; registering the "
                f"question afterwards does not unsee it.")
        if boundary.is_forward:
            return FORWARD, (
                f"{boundary.validation.label} begins after {boundary.data_available_at_registration}, "
                f"which is everything that existed when the claim was frozen. No prior looking "
                f"could have used these observations.")
        if self.unrecorded:
            n = len(self.unrecorded)
            return UNKNOWN, (
                f"{n} exposure(s) were recorded without a data footprint, so the system cannot "
                f"say whether {boundary.validation.label} was among them. Unknown is treated as "
                f"contaminated: the weakest bookkeeping must not produce the strongest claim.")
        return CLEAN, (
            f"no exposure in the durable history touches {boundary.validation.label}. This rests "
            f"on the ledger being complete, which FORWARD would not need.")


CONFIRMATORY_OK = (CLEAN, FORWARD)


def confirmatory_verdict(*, registered: bool, boundary: EvidenceBoundary | None,
                         registry: ExposureRegistry) -> dict:
    """The single place that answers 'may this be reported as confirmatory'."""
    if not registered:
        return {"eligible": False, "status": "NOT_REGISTERED",
                "why": "no claim was frozen in advance, so there is nothing to confirm"}
    if boundary is None:
        return {"eligible": False, "status": "NO_BOUNDARY",
                "why": ("registration alone is not confirmatory standing. A frozen claim still "
                        "needs evidence that was not used to choose it — declare a validation "
                        "window.")}
    status, why = registry.status_for(boundary)
    return {"eligible": status in CONFIRMATORY_OK, "status": status, "why": why,
            "boundary_hash": boundary.boundary_hash,
            "validation": boundary.validation.label,
            "development": boundary.development.label}
