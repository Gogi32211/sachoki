"""What makes a verdict confirmatory: not a fresh session id, but evidence nobody has seen.

The fork rule closed one laundering path — look at forty cells, fork, and the child's counter is
clean. The session rule did not close the one above it:

    s0001 EXPLORE   RSI ±5 weak · RSI ±2 better · RSI ±1 best
    close s0001
    s0002 NEW       preregister RSI ±1        ← formally clean, statistically not

A new session id does not erase what the researcher knows. So confirmatory standing is a
property of the DATA, not of the session: a registered claim needs a validation window that no
session, in any family, has ever read.

    CONTAMINATED   an actual footprint overlaps the validation window
    FORWARD        the observations did not exist when the claim was frozen
    UNKNOWN        some exposure has no footprint, so the question is unanswerable
    CLEAN          nothing in the durable history touched it

THE BOUNDARY IS PART OF THE FREEZE, NOT AN ARGUMENT TO THE EVALUATION. Supplying it at
validation time let a researcher choose the convenient window after seeing the result and
present it as the plan. It is now declared before `register()`, hashed into `SESSION_FROZEN`,
and `validate(session_id)` loads it from the ledger. Evaluating a registered session against any
other boundary raises `EvidenceBoundaryDriftError` — the same shape as `SearchSpaceDriftError`,
for the same reason: the thing declared must be the thing paid for.

TWO FIELDS THE CALLER MAY NOT SUPPLY. `registered_at` and `data_cutoff_at_registration` are
produced by the server at freeze time, from the source itself. They decide whether a window is
FORWARD, which is the strongest verdict here, and a caller-asserted cutoff would mean the
strongest verdict rests on the field the caller most benefits from misstating.

CONTAMINATION READS ACTUAL FOOTPRINTS, NOT DECLARED WINDOWS. A declaration is controlled by the
researcher: declare 2024–2025, let a helper touch March 2026, and a validation starting in
January 2026 would come back CLEAN on the declaration and CONTAMINATED on the truth. Same
asymmetry as `k_declared` against `k_actual`, and the actual one governs.

THE ORDER OF THE VERDICTS is by what each one requires, and it is deliberate:

    INVALID_BOUNDARY  structural — nothing can be said
    CONTAMINATED      a fact in the record; the most specific thing we know
    FORWARD           rests on time, and therefore not on the ledger being complete
    UNKNOWN          a footprint is missing, so CLEAN cannot be earned
    CLEAN            rests on the ledger being complete

UNKNOWN sits directly above CLEAN and can never be promoted into it, because that promotion
would be earned by an ABSENCE of information. FORWARD outranks UNKNOWN for the opposite reason:
it is not earned by absence at all — the observations did not exist, and no amount of missing
bookkeeping changes that.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

from data_access import (CATALOG, DEVELOPMENT, VALIDATION,  # noqa: E402
                         DataAccessSpec, ExposureFootprint, SourceCatalog)

CONTAMINATED, CLEAN, FORWARD, UNKNOWN = "CONTAMINATED", "CLEAN", "FORWARD", "UNKNOWN"
INVALID_BOUNDARY = "INVALID_BOUNDARY"
TEMPORAL_CONTRACT_VERSION = "temporal_contract_v1"


class EvidenceBoundaryError(Exception):
    """A boundary that cannot support the claim being made on it."""


class EvidenceBoundaryDriftError(RuntimeError):
    """A registered session was evaluated against a boundary it did not freeze."""


@dataclass(frozen=True)
class EvidenceBoundary:
    """Where hypothesis formation stops and evaluation begins. Frozen with the claim."""
    development_access_spec: DataAccessSpec
    validation_access_spec: DataAccessSpec
    registered_at: str                  # server clock at SESSION_FROZEN
    data_snapshot_id: str               # server-derived identity of the source
    data_cutoff_at_registration: str    # server-derived latest observation
    temporal_contract_version: str = TEMPORAL_CONTRACT_VERSION

    def __post_init__(self):
        d, v = self.development_access_spec, self.validation_access_spec
        if d.purpose != DEVELOPMENT or v.purpose != VALIDATION:
            raise EvidenceBoundaryError(
                f"purposes are {d.purpose}/{v.purpose}; a boundary needs one of each")
        if d.source_id == v.source_id and d.start <= v.end and v.start <= d.end:
            raise EvidenceBoundaryError(
                f"development {d.label} overlaps validation {v.label}. A boundary that contains "
                f"itself is not a boundary.")
        for f in ("registered_at", "data_snapshot_id", "data_cutoff_at_registration"):
            if not getattr(self, f):
                raise EvidenceBoundaryError(
                    f"{f} is empty. It is produced by the server at freeze time and cannot be "
                    f"defaulted or supplied by the caller.")

    @property
    def validation(self) -> DataAccessSpec:
        return self.validation_access_spec

    @property
    def development(self) -> DataAccessSpec:
        return self.development_access_spec

    @property
    def is_forward(self) -> bool:
        """Compared against the SERVER's cutoff, so this is a fact and not an assertion."""
        return self.validation.start > self.data_cutoff_at_registration

    @property
    def boundary_hash(self) -> str:
        blob = "|".join([self.development.spec_hash, self.validation.spec_hash,
                         self.registered_at, self.data_snapshot_id,
                         self.data_cutoff_at_registration, self.temporal_contract_version])
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    def as_dict(self) -> dict:
        return {"development_access_spec": self.development.as_dict(),
                "validation_access_spec": self.validation.as_dict(),
                "registered_at": self.registered_at,
                "data_snapshot_id": self.data_snapshot_id,
                "data_cutoff_at_registration": self.data_cutoff_at_registration,
                "temporal_contract_version": self.temporal_contract_version,
                "boundary_hash": self.boundary_hash}

    @classmethod
    def from_dict(cls, d: dict) -> "EvidenceBoundary":
        return cls(
            development_access_spec=DataAccessSpec.from_dict(d["development_access_spec"]),
            validation_access_spec=DataAccessSpec.from_dict(d["validation_access_spec"]),
            registered_at=d["registered_at"], data_snapshot_id=d["data_snapshot_id"],
            data_cutoff_at_registration=d["data_cutoff_at_registration"],
            temporal_contract_version=d.get("temporal_contract_version",
                                            TEMPORAL_CONTRACT_VERSION))


def freeze_boundary(development: DataAccessSpec, validation: DataAccessSpec, *,
                    now: str, catalog: SourceCatalog = CATALOG) -> EvidenceBoundary:
    """Build the boundary at freeze time. The two server fields are filled in here or nowhere."""
    snap, cutoff = catalog.snapshot(validation.source_id)
    return EvidenceBoundary(development_access_spec=development,
                            validation_access_spec=validation,
                            registered_at=now, data_snapshot_id=snap,
                            data_cutoff_at_registration=cutoff)


# ── what has actually been read ─────────────────────────────────────────────
@dataclass(frozen=True)
class ExposureRecord:
    session_id: str
    family_id: str
    claim_hash: str
    footprint: ExposureFootprint | None    # None = an exposure with no recorded footprint


class ExposureRegistry:
    """Every footprint any session has ever produced. Global, by construction.

    Pressing "start independent research" mints a new family id and unsees nothing, so filtering
    this by family would turn the laundering path into a button.
    """

    EXPOSING = ("RESULT_EXPOSED", "SEARCH_RUN")
    FOOTPRINT_EVENT = "DATA_ACCESSED"

    def __init__(self, records: list):
        self.records = list(records)

    @classmethod
    def from_events(cls, events) -> "ExposureRegistry":
        """A session's footprints cover its exposures; an exposure with none is UNKNOWN.

        Footprints arrive as their own events because a read is not an exposure — a session may
        touch data several times before a result is shown, and the union of those reads is what
        contaminates, not the last one.
        """
        by_session: dict = {}
        for e in events:
            if e.event_type == cls.FOOTPRINT_EVENT and e.payload.get("footprint"):
                by_session.setdefault(e.session_id, []).append(
                    ExposureFootprint.from_dict(e.payload["footprint"]))

        out = []
        for e in events:
            if e.event_type not in cls.EXPOSING:
                continue
            fps = by_session.get(e.session_id)
            if not fps:
                out.append(ExposureRecord(e.session_id, e.family_id, e.claim_hash, None))
                continue
            for fp in fps:
                out.append(ExposureRecord(e.session_id, e.family_id, e.claim_hash, fp))
        return cls(out)

    @property
    def unrecorded(self) -> list:
        return [r for r in self.records if r.footprint is None]

    def touching(self, spec: DataAccessSpec) -> list:
        return [r for r in self.records if r.footprint
                and r.footprint.overlaps_range(spec.source_id, spec.universe,
                                               spec.start, spec.end)]

    def overreaching(self) -> list:
        """Footprints that went outside what their session declared. Reportable on its own."""
        return [r for r in self.records if r.footprint and r.footprint.exceeded_declaration]

    def status_for(self, boundary: EvidenceBoundary) -> tuple:
        hits = self.touching(boundary.validation)
        if hits:
            who = ", ".join(sorted({r.session_id for r in hits})[:4])
            over = any(r.footprint.exceeded_declaration for r in hits)
            extra = (" Some of those reads went beyond what their session declared, which is why "
                     "the declared window is not what this is measured against." if over else "")
            return CONTAMINATED, (
                f"{len(hits)} recorded read(s) already touched {boundary.validation.label} "
                f"(sessions: {who}). The answer to this question has been seen; registering the "
                f"question afterwards does not unsee it.{extra}")
        if boundary.is_forward:
            return FORWARD, (
                f"{boundary.validation.label} begins after {boundary.data_cutoff_at_registration}, "
                f"the source cutoff the server recorded at freeze time. These observations did "
                f"not exist when the claim was frozen, so no prior looking could have used them.")
        if self.unrecorded:
            n = len(self.unrecorded)
            return UNKNOWN, (
                f"{n} exposure(s) have no recorded data footprint, so the system cannot say "
                f"whether {boundary.validation.label} was among them. Unknown is never promoted "
                f"to clean: that promotion would be earned by an absence of information.")
        return CLEAN, (
            f"no recorded read touches {boundary.validation.label}. This rests on the ledger "
            f"being complete, which FORWARD would not need.")


CONFIRMATORY_OK = (CLEAN, FORWARD)


def confirmatory_verdict(*, registered: bool, boundary: EvidenceBoundary | None,
                         registry: ExposureRegistry, completeness: tuple = ()) -> dict:
    """The single place that answers 'may this be reported as confirmatory'.

    `completeness` is a separate axis and it gates this one. Contamination answers "was this
    window read"; completeness answers whether that question can be answered at all. A study
    with an unobserved read path produces a perfectly clean-looking contamination check, and
    that false CLEAN is exactly what this gate refuses.
    """
    if completeness:
        status_c, why_c = completeness
        if status_c != "COMPLETE":
            return {"eligible": False, "status": f"ACCESS_{status_c}", "why": why_c,
                    "access_completeness": status_c,
                    "boundary_hash": boundary.boundary_hash if boundary else "",
                    "validation": boundary.validation.label if boundary else "",
                    "development": boundary.development.label if boundary else "",
                    "overreaching_reads": len(registry.overreaching())}
    if not registered:
        return {"eligible": False, "status": "NOT_REGISTERED",
                "why": "no claim was frozen in advance, so there is nothing to confirm"}
    if boundary is None:
        return {"eligible": False, "status": "NO_BOUNDARY",
                "why": ("registration alone is not confirmatory standing. A frozen claim still "
                        "needs evidence that was not used to choose it.")}
    status, why = registry.status_for(boundary)
    return {"eligible": status in CONFIRMATORY_OK, "status": status, "why": why,
            "access_completeness": completeness[0] if completeness else "UNATTESTED",
            "boundary_hash": boundary.boundary_hash,
            "validation": boundary.validation.label,
            "development": boundary.development.label,
            "data_cutoff_at_registration": boundary.data_cutoff_at_registration,
            "registered_at": boundary.registered_at,
            "overreaching_reads": len(registry.overreaching())}
