"""The laundering path Gate 2 opened, and the identity that closes it.

Gate 2 made `run_v2(HISTORICAL_RESEARCH)` legal. That is correct for evidence that does not exist
yet, and it silently created a route for evidence that does:

    same data · same claims · same estimand · already exposed before Gate 2
        ↓  rerun after Gate 2
    a new execution timestamp, a new run id
        ↓
    apparently HISTORICAL_APPLICATION_QUALIFIED evidence

Nothing was recomputed into existence — the numbers are the same numbers — but the label got
cleaner, which is the whole point of the manoeuvre. So evidence identity has to be STRONGER than
execution identity: a rerun must land on the same evidence item it replays.

    EvidenceFingerprint = data lineage · outcome definition · population · claims · estimand

This is the run-level analogue of `evidence_claim_hash`, aggregated over cells rather than a
parallel ontology. What it leaves out is the part that matters:

    NOT IN THE IDENTITY   run id · execution timestamp · engine version · execution mode ·
                          gate state · RNG policy · bootstrap policy · decision policy

The rule generating that list is one line: ANYTHING A RERUN CAN CHANGE MUST NOT BE PART OF
EVIDENCE IDENTITY, OR IT BECOMES THE LAUNDERING KEY. Engine version is the uncomfortable one and
it is excluded deliberately — evidence is a question asked of data, not the software that
answered it, and "recomputed under v1.0.1" would otherwise wash 31 exposed estimates clean. A
consequence follows and is intended: fixing a genuine numerical bug produces the SAME fingerprint
with different numbers. That is a correction to an exposed evidence item, not a new one.

WHY `snapshot_id` IS CARRIED BUT NEVER HASHED. A snapshot id is a name, and names are free.
Copy the same rows on a later date, call it `...-2026-08-14`, and a name-based identity says
"new evidence". Content lineage says "the same rows". Contamination lives in the data, not in
the name of the run — the same reason declared windows lost to actual footprints.

NOVELTY AND SAMENESS ARE NOT SYMMETRIC, on purpose. Sameness can be established from dimensions
and a digest. Novelty additionally requires COVERAGE — proof that these observations are not the
ones already seen — because "different" is not "new": a filtered subset of exposed data is
different and not new. With no coverage recorded the comparison returns UNDETERMINED, and
UNDETERMINED is not novel. This fails closed against the direction that grants standing, and it
means the two artifacts already on disk can never be certified as prospective evidence, which is
the correct answer for them.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field

HERE = os.path.dirname(os.path.abspath(__file__))
REGISTRY = os.path.join(HERE, "EVIDENCE_FINGERPRINT_REGISTRY.json")

IDENTITY_VERSION = "evidence_fingerprint_v1"

# what a comparison of two lineages can say
SAME_CONTENT = "SAME_CONTENT"
EXTENDS_EXPOSED = "EXTENDS_EXPOSED"
DISJOINT = "DISJOINT"
UNDETERMINED = "UNDETERMINED"

# what a candidate run is, against everything already exposed
REPLAY_OF_EXPOSED_EVIDENCE = "REPLAY_OF_EXPOSED_EVIDENCE"
PARTIAL_REPLAY_OF_EXPOSED_EVIDENCE = "PARTIAL_REPLAY_OF_EXPOSED_EVIDENCE"
NOVEL_EVIDENCE = "NOVEL_EVIDENCE"
UNDETERMINED_NOT_NOVEL = "UNDETERMINED_NOT_NOVEL"

NOT_PART_OF_IDENTITY = {
    "run_id": "a rerun issues a new one; that is what makes it the obvious laundering key",
    "execution_timestamp": "the same reason, wearing a clock",
    "engine_version": ("evidence is a question asked of data, not the software that answered "
                       "it. Recomputing under a new version does not produce new evidence — it "
                       "produces the same evidence item, possibly corrected"),
    "execution_mode": "the mode a run declares cannot be what decides whether it is new",
    "gate_state": ("the gate governs what may be DONE with evidence, and cannot be part of what "
                   "the evidence IS, or opening it would relabel history"),
    "rng_policy": ("the intervals really do move when it changes, and they are still uncertainty "
                   "about the same estimand. Otherwise a new RNG namespace is a fresh start"),
    "bootstrap_policy": "more replications is a better measurement of one claim, not a second one",
    "decision_policy": ("kept out for the same reason `evidence_claim_hash` and "
                        "`decision_spec_hash` are separate hashes"),
}


class EvidenceReplayLaunderingError(RuntimeError):
    """Already-exposed evidence, re-executed, asked to come back with a cleaner status."""


class EvidenceLineageError(RuntimeError):
    """A lineage that cannot support the comparison being asked of it."""


def _h(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]


# ── data lineage ────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class DataLineage:
    """What the data IS. `snapshot_id` travels with it and never enters the hash."""
    snapshot_id: str
    rows: int
    dates: int
    content_digest: str = ""      # digest of the actual outcome vector, when a run emits one
    coverage_start: str = ""      # first observation; required to earn novelty
    coverage_end: str = ""        # last observation

    def content_hash(self) -> str:
        return _h({"rows": int(self.rows), "dates": int(self.dates),
                   "digest": self.content_digest})

    def has_coverage(self) -> bool:
        return bool(self.coverage_start and self.coverage_end)

    def as_dict(self) -> dict:
        return {"snapshot_id": self.snapshot_id, "rows": int(self.rows), "dates": int(self.dates),
                "content_digest": self.content_digest, "coverage_start": self.coverage_start,
                "coverage_end": self.coverage_end, "content_hash": self.content_hash(),
                "snapshot_id_in_hash": False}

    @classmethod
    def from_dict(cls, d: dict) -> "DataLineage":
        return cls(snapshot_id=d.get("snapshot_id", ""), rows=int(d["rows"]),
                   dates=int(d["dates"]), content_digest=d.get("content_digest", ""),
                   coverage_start=d.get("coverage_start", ""),
                   coverage_end=d.get("coverage_end", ""))


def lineage_from_vector(values, dates, snapshot_id: str) -> DataLineage:
    """The strong form, for any run that holds the data: digest the bytes, record the window.

    A run that goes through here can be certified novel. A run that only declares dimensions
    cannot, and that asymmetry is the incentive.
    """
    import numpy as np                                                # noqa: PLC0415
    v = np.ascontiguousarray(np.asarray(values, dtype=float))
    d = np.asarray(dates)
    return DataLineage(
        snapshot_id=snapshot_id, rows=int(v.size), dates=int(np.unique(d).size),
        content_digest=hashlib.sha256(v.tobytes()).hexdigest()[:32],
        coverage_start=str(np.min(d)), coverage_end=str(np.max(d)))


def compare_lineage(prior: DataLineage, candidate: DataLineage) -> str:
    """Sameness needs content. Novelty needs coverage as well — 'different' is not 'new'."""
    if prior.content_hash() == candidate.content_hash():
        return SAME_CONTENT
    if not (prior.has_coverage() and candidate.has_coverage()):
        # a filtered subset of exposed rows is different and not new, and without a window
        # there is no way to tell the two apart
        return UNDETERMINED
    if candidate.coverage_start <= prior.coverage_end:
        return EXTENDS_EXPOSED
    return DISJOINT


def novel_window(prior: DataLineage, candidate: DataLineage) -> tuple:
    """Of an extending window, only the tail is unseen. The head is exposed evidence."""
    if not (prior.has_coverage() and candidate.has_coverage()):
        raise EvidenceLineageError(
            "the unseen part of an extending window cannot be located without coverage on both "
            "sides; the whole candidate is treated as already exposed")
    if candidate.coverage_end <= prior.coverage_end:
        return ("", "")
    return (prior.coverage_end, candidate.coverage_end)


# ── the fingerprint ─────────────────────────────────────────────────────────
@dataclass(frozen=True)
class EvidenceFingerprint:
    """Stronger than execution identity, and deliberately blind to everything a rerun changes."""
    data_lineage: DataLineage
    outcome_definition: str
    population: str               # support/eligibility policy the population was cut by
    claim_identity: str           # canonical hash of the claim set
    estimand: str
    identity_version: str = IDENTITY_VERSION

    def components(self) -> dict:
        return {"data_content_hash": self.data_lineage.content_hash(),
                "outcome_definition": self.outcome_definition,
                "population": self.population,
                "claim_identity": self.claim_identity,
                "estimand": self.estimand,
                "identity_version": self.identity_version}

    def fingerprint(self) -> str:
        return _h(self.components())

    def as_dict(self) -> dict:
        return {"fingerprint": self.fingerprint(), "components": self.components(),
                "data_lineage": self.data_lineage.as_dict(),
                "not_part_of_identity": sorted(NOT_PART_OF_IDENTITY)}

    @classmethod
    def from_dict(cls, d: dict) -> "EvidenceFingerprint":
        c = d["components"]
        return cls(data_lineage=DataLineage.from_dict(d["data_lineage"]),
                   outcome_definition=c["outcome_definition"], population=c["population"],
                   claim_identity=c["claim_identity"], estimand=c["estimand"],
                   identity_version=c.get("identity_version", IDENTITY_VERSION))


def claim_set_identity(cells) -> str:
    """The claim set as a set. Order is an execution property and does not change what was asked."""
    return _h(sorted(str(c) for c in cells))


# ── the registry of what has already been exposed ───────────────────────────
@dataclass
class FingerprintRegistry:
    entries: list = field(default_factory=list)

    @classmethod
    def load(cls, path: str = REGISTRY) -> "FingerprintRegistry":
        if not os.path.exists(path):
            return cls([])
        with open(path) as f:
            return cls(json.load(f)["entries"])

    def save(self, path: str = REGISTRY) -> None:
        with open(path, "w") as f:
            json.dump({"identity_version": IDENTITY_VERSION, "entries": self.entries}, f,
                      indent=1, sort_keys=True)

    def append(self, entry: dict) -> dict:
        """Append-only. A fingerprint already present gains an exposure, never a replacement."""
        for e in self.entries:
            if e["fingerprint"] == entry["fingerprint"]:
                if entry["exposure_id"] not in e["exposure_ids"]:
                    e["exposure_ids"].append(entry["exposure_id"])
                return e
        self.entries.append({"fingerprint": entry["fingerprint"],
                             "exposure_ids": [entry["exposure_id"]],
                             "evidence_status": entry["evidence_status"],
                             "components": entry["components"],
                             "data_lineage": entry["data_lineage"],
                             "derived_from": entry["derived_from"],
                             "note": entry.get("note", "")})
        return self.entries[-1]

    def find(self, fp: str) -> dict | None:
        return next((e for e in self.entries if e["fingerprint"] == fp), None)

    def classify(self, candidate: EvidenceFingerprint) -> dict:
        """What this candidate run is, with respect to everything already exposed."""
        hit = self.find(candidate.fingerprint())
        if hit:
            return {"classification": REPLAY_OF_EXPOSED_EVIDENCE,
                    "matched": hit["exposure_ids"],
                    "prior_evidence_status": hit["evidence_status"],
                    "why": ("every identity component matches an exposure already on record. A "
                            "new execution of the same question against the same data is a "
                            "replay, whatever the run id says")}

        # same question, related data
        cand_lin = candidate.data_lineage
        best = None
        for e in self.entries:
            c = e["components"]
            same_question = (c["outcome_definition"] == candidate.outcome_definition
                             and c["population"] == candidate.population
                             and c["claim_identity"] == candidate.claim_identity
                             and c["estimand"] == candidate.estimand)
            if not same_question:
                continue
            rel = compare_lineage(DataLineage.from_dict(e["data_lineage"]), cand_lin)
            if rel == EXTENDS_EXPOSED:
                return {"classification": PARTIAL_REPLAY_OF_EXPOSED_EVIDENCE,
                        "matched": e["exposure_ids"],
                        "prior_evidence_status": e["evidence_status"],
                        "why": ("the window overlaps evidence already exposed. Only observations "
                                "after the exposed window can be prospective, and the overlap "
                                "keeps the standing it already has")}
            if rel == UNDETERMINED:
                best = best or {"classification": UNDETERMINED_NOT_NOVEL,
                                "matched": e["exposure_ids"],
                                "prior_evidence_status": e["evidence_status"],
                                "why": ("the same question against data whose coverage is not "
                                        "recorded. Novelty has to be shown, not assumed, so this "
                                        "is not novel evidence")}
        return best or {"classification": NOVEL_EVIDENCE, "matched": [],
                        "prior_evidence_status": None,
                        "why": "no exposure on record shares this identity"}


# ── the refusal ─────────────────────────────────────────────────────────────
CLEANER_THAN = {"FIRST_HISTORICAL_APPLICATION": 0, "HISTORICAL_APPLICATION_QUALIFIED": 1}
ROLE_RANK = {"ENGINE_QUALIFICATION_EVIDENCE": 0, "EXPLORATORY_HISTORICAL_EVIDENCE": 0,
             "REGISTERED_VALIDATION_EVIDENCE": 1, "FROZEN_FORWARD_EVIDENCE": 2}


def assert_no_replay_laundering(verdict: dict, proposed_status: dict) -> None:
    """A replay may be re-run. It may not come back with a cleaner label than it went in with.

    The temptation is structural rather than dishonest: `maturity_for_new_results()` legitimately
    returns HISTORICAL_APPLICATION_QUALIFIED once the gate is open, and a rerun of old evidence
    looks exactly like a new result to every field except its identity.
    """
    if verdict["classification"] not in (REPLAY_OF_EXPOSED_EVIDENCE,
                                         PARTIAL_REPLAY_OF_EXPOSED_EVIDENCE,
                                         UNDETERMINED_NOT_NOVEL):
        return
    prior = verdict.get("prior_evidence_status") or {}
    if not prior:
        return
    for field_, table in (("application_maturity", CLEANER_THAN), ("result_role", ROLE_RANK)):
        was, now = prior.get(field_), proposed_status.get(field_)
        if now is None or now == was:
            continue
        if table.get(now, -1) > table.get(was, -1) or (field_ == "result_role" and now != was):
            raise EvidenceReplayLaunderingError(
                f"this run is {verdict['classification']} of {', '.join(verdict['matched'])} and "
                f"asked for {field_} {was!r} → {now!r}. Qualification applies to the instrument "
                f"and to its application going forward; it does not reach back, and re-executing "
                f"already-exposed evidence is not a way to make it reach back. The identity that "
                f"caught this is stronger than the execution: same data, same claims, same "
                f"estimand. The legitimate path is evidence that has not been seen.")


def admissible_status(verdict: dict, gate_open_status: dict) -> dict:
    """The status a run may carry: the gate's answer for new evidence, the prior one for a replay.

    Fail-safe rather than fail-closed on purpose — a replay is allowed to happen, it just cannot
    profit from having happened.
    """
    if verdict["classification"] == NOVEL_EVIDENCE:
        return dict(gate_open_status)
    prior = verdict.get("prior_evidence_status")
    return dict(prior) if prior else dict(gate_open_status)
