"""Four questions that were nearly one enum, and the invariant that keeps them apart.

`evidence_origin` was doing too much work. Asking "may this be promoted" collapsed four
independent facts into one label, and a single label that decides everything is one stamp away
from being a bypass.

    evidence_origin              where did the DATA come from
    instrument_validation_basis  what was the INSTRUMENT validated on
    application_maturity         how well tested is this USE of the instrument
    result_role                  what was this particular RESULT produced for

ComboLab v2 makes the separation concrete rather than theoretical. Its sealed acceptance ran on
synthetic worlds — `Y = μ_setup + γ_date + ε` with a planted needle — and measured whether the
instrument detects a δ it was given. It never touched the real return column. So pointing that
validated instrument at real outcomes is:

    evidence_origin              HISTORICAL_RESEARCH
    instrument_validation_basis  SYNTHETIC_CAPABILITY_VALIDATED
    application_maturity         FIRST_HISTORICAL_APPLICATION

Three different sentences, and no single field could have said all three without lying about one.

AUTHORISATION IS AN INTERSECTION, NEVER A PROPERTY.

    allowed = origin_ceiling ∩ maturity_ceiling ∩ role_ceiling ∩ (gates the action depends on)

Each axis can only take rights away. That is what makes a label useless as a skeleton key.

THE INVARIANT THIS FILE EXISTS FOR. `result_role` is immutable, and qualifying the instrument
does not re-qualify results already produced by it.

    instrument qualification  ⇏  evidence qualification

The 31 real-y estimates pinned in `V2_CORE_ORACLE.json` were made available to a researcher
while establishing that the extraction is faithful. By this project's own definition — exposed
means made available, not read — they are exposed, and they were exposed for that purpose. When
the extraction later passes, it is the ENGINE that becomes qualified. Reaching back and saying
"those 31 are now confirmatory" would be retroactive preregistration wearing a different hat,
and `RetroactiveEvidenceUpgradeError` refuses it.

THE LEGITIMATE PATH IS NOT BLOCKED, and blocking it would be the mistake in the other direction.
Seeing an interesting cell on history and then freezing the specification for evaluation on data
that does not exist yet is exactly how honest research proceeds. `FREEZE_FORWARD_SPEC` is
available to exploratory evidence; what it produces is a new forward boundary, and the
historical result that suggested it stays exploratory forever.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

# ── the axes ────────────────────────────────────────────────────────────────
SYNTHETIC_FIXTURE = "SYNTHETIC_FIXTURE"
HISTORICAL_RESEARCH = "HISTORICAL_RESEARCH"
FROZEN_FORWARD = "FROZEN_FORWARD"

UNVALIDATED = "UNVALIDATED"
SYNTHETIC_CAPABILITY_VALIDATED = "SYNTHETIC_CAPABILITY_VALIDATED"
HISTORICAL_APPLICATION_VALIDATED = "HISTORICAL_APPLICATION_VALIDATED"

FIRST_HISTORICAL_APPLICATION = "FIRST_HISTORICAL_APPLICATION"
HISTORICAL_APPLICATION_QUALIFIED = "HISTORICAL_APPLICATION_QUALIFIED"
NOT_APPLICABLE = "NOT_APPLICABLE"

ENGINE_QUALIFICATION_EVIDENCE = "ENGINE_QUALIFICATION_EVIDENCE"
EXPLORATORY_HISTORICAL_EVIDENCE = "EXPLORATORY_HISTORICAL_EVIDENCE"
REGISTERED_VALIDATION_EVIDENCE = "REGISTERED_VALIDATION_EVIDENCE"
FROZEN_FORWARD_EVIDENCE = "FROZEN_FORWARD_EVIDENCE"

# ── the actions ─────────────────────────────────────────────────────────────
INSPECT, RERUN, CHANGE_CONTROLS = "inspect", "rerun", "change_controls"
RECORD_HISTORICAL_VERDICT = "record_historical_verdict"
NOMINATE_FOR_FORWARD_VALIDATION = "nominate_for_forward_validation"
FREEZE_FORWARD_SPEC = "freeze_forward_spec"
PROMOTE_AS_VALIDATED_EDGE = "promote_as_validated_edge"
BOOK = "book"

# Never reachable from a result, under any combination of axes. Both are the same move seen
# from two sides: turning something already looked at into something declared in advance.
REGISTER_CONFIRMATORY_STUDY = "register_confirmatory_study"
RETROACTIVE_CONFIRMATORY_REGISTRATION = "retroactive_confirmatory_registration"
NEVER_FROM_A_RESULT = frozenset({REGISTER_CONFIRMATORY_STUDY,
                                 RETROACTIVE_CONFIRMATORY_REGISTRATION})

READ_ONLY = (INSPECT, RERUN, CHANGE_CONTROLS)
# Freezing a specification for the future is NOT the same as calling a past result proven. The
# first is how a hypothesis is born; the second is a verdict. They were both called "freeze"
# until this file, which is how the two would eventually have been confused.
FORWARD_SEEDING = (NOMINATE_FOR_FORWARD_VALIDATION, FREEZE_FORWARD_SPEC)
CONSEQUENTIAL = (RECORD_HISTORICAL_VERDICT, *FORWARD_SEEDING, PROMOTE_AS_VALIDATED_EDGE, BOOK)

ORIGIN_CEILING = {
    SYNTHETIC_FIXTURE: set(READ_ONLY),
    HISTORICAL_RESEARCH: set(READ_ONLY) | {RECORD_HISTORICAL_VERDICT, *FORWARD_SEEDING,
                                           PROMOTE_AS_VALIDATED_EDGE},
    FROZEN_FORWARD: set(READ_ONLY) | set(CONSEQUENTIAL),
}

MATURITY_CEILING = {
    NOT_APPLICABLE: set(READ_ONLY) | set(CONSEQUENTIAL),
    # a first application may record what it found and may seed a forward study. It may not
    # declare an edge validated, because the use of the instrument is itself untested.
    FIRST_HISTORICAL_APPLICATION: set(READ_ONLY) | {RECORD_HISTORICAL_VERDICT, *FORWARD_SEEDING},
    HISTORICAL_APPLICATION_QUALIFIED: set(READ_ONLY) | set(CONSEQUENTIAL),
}

ROLE_CEILING = {
    # produced to check that an extraction is faithful. It can seed a forward study — noticing
    # something while testing a tool is allowed — and can never itself be a validated edge.
    ENGINE_QUALIFICATION_EVIDENCE: set(READ_ONLY) | {RECORD_HISTORICAL_VERDICT, *FORWARD_SEEDING},
    EXPLORATORY_HISTORICAL_EVIDENCE: set(READ_ONLY) | {RECORD_HISTORICAL_VERDICT,
                                                       *FORWARD_SEEDING},
    REGISTERED_VALIDATION_EVIDENCE: set(READ_ONLY) | {RECORD_HISTORICAL_VERDICT, *FORWARD_SEEDING,
                                                      PROMOTE_AS_VALIDATED_EDGE},
    FROZEN_FORWARD_EVIDENCE: set(READ_ONLY) | set(CONSEQUENTIAL),
}


class EvidenceStatusError(RuntimeError):
    """An action outside what the evidence can support."""


class RetroactiveEvidenceUpgradeError(RuntimeError):
    """Qualifying the instrument does not re-qualify what it already produced."""


@dataclass(frozen=True)
class EvidenceStatus:
    """All four axes together. Frozen, and `result_role` is frozen twice over."""
    evidence_origin: str
    instrument_validation_basis: str
    application_maturity: str
    result_role: str

    def ceiling(self) -> set:
        """The intersection. Every axis may remove a right and none may add one."""
        return (ORIGIN_CEILING.get(self.evidence_origin, set())
                & MATURITY_CEILING.get(self.application_maturity, set())
                & ROLE_CEILING.get(self.result_role, set())) - NEVER_FROM_A_RESULT

    def permits(self, action: str) -> bool:
        return action in self.ceiling()

    def assert_permits(self, action: str) -> None:
        if action in NEVER_FROM_A_RESULT:
            raise EvidenceStatusError(
                f"{action!r} is not an action on a result under any status. Declaring a "
                f"hypothesis in advance is a property of a session that has seen nothing, and "
                f"offering it beside results already looked at is the move the whole session "
                f"contract exists to refuse.")
        if self.permits(action):
            return
        blocking = [name for name, table in (
            ("evidence_origin", ORIGIN_CEILING.get(self.evidence_origin, set())),
            ("application_maturity", MATURITY_CEILING.get(self.application_maturity, set())),
            ("result_role", ROLE_CEILING.get(self.result_role, set())),
        ) if action not in table]
        raise EvidenceStatusError(
            f"{action!r} is above the ceiling set by {' and '.join(blocking)}. This evidence is "
            f"{self.evidence_origin} / {self.instrument_validation_basis} / "
            f"{self.application_maturity} / {self.result_role}. At most, here: "
            f"{', '.join(sorted(self.ceiling())) or 'nothing'}.")

    def with_maturity(self, maturity: str) -> "EvidenceStatus":
        """Maturity may advance for FUTURE results. It never re-labels one already produced."""
        return replace(self, application_maturity=maturity)

    def as_dict(self) -> dict:
        return {"evidence_origin": self.evidence_origin,
                "instrument_validation_basis": self.instrument_validation_basis,
                "application_maturity": self.application_maturity,
                "result_role": self.result_role,
                "ceiling": sorted(self.ceiling())}


def upgrade_result_role(status: EvidenceStatus, new_role: str, *, was_exposed: bool = True
                        ) -> EvidenceStatus:
    """The refusal this module was written for.

    Exposed evidence keeps the role it was produced under. When the extraction passes, the
    ENGINE becomes qualified; the estimates already made available while checking it do not
    become confirmatory because the check succeeded. That is retroactive preregistration one
    level up, and it is the most plausible-sounding version of it — everything really did pass.
    """
    if was_exposed and new_role != status.result_role:
        raise RetroactiveEvidenceUpgradeError(
            f"this evidence was produced and exposed as {status.result_role} and cannot become "
            f"{new_role}. Qualifying the instrument qualifies the instrument. The legitimate "
            f"path is {FREEZE_FORWARD_SPEC}: freeze the specification these results suggested "
            f"and evaluate it on evidence that has not been seen.")
    return replace(status, result_role=new_role)


# ── the statuses this system actually issues ────────────────────────────────
FIXTURE = EvidenceStatus(SYNTHETIC_FIXTURE, UNVALIDATED, NOT_APPLICABLE,
                         ENGINE_QUALIFICATION_EVIDENCE)

# ComboLab v2 pointed at the real return column for the first time. The instrument passed a
# sealed acceptance on synthetic worlds; this use of it has passed nothing yet.
V2_FIRST_HISTORICAL = EvidenceStatus(
    HISTORICAL_RESEARCH, SYNTHETIC_CAPABILITY_VALIDATED, FIRST_HISTORICAL_APPLICATION,
    EXPLORATORY_HISTORICAL_EVIDENCE)

# what `V2_CORE_ORACLE.json` holds: 31 real-y estimates made available while establishing that
# the extraction is faithful. Immutable, and never promotable.
V2_ORACLE_EVIDENCE = EvidenceStatus(
    HISTORICAL_RESEARCH, SYNTHETIC_CAPABILITY_VALIDATED, FIRST_HISTORICAL_APPLICATION,
    ENGINE_QUALIFICATION_EVIDENCE)
