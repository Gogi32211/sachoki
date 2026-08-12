"""The only door to research data. Not a convention — a construction.

The previous milestone could say UNKNOWN when it knew it did not know. That defends against
OBVIOUS incompleteness, and the dangerous case is not obvious:

    legal read A   → footprint recorded
    legal read B   → footprint recorded
    raw read C     → the access layer never saw it

The ledger does not look short. It looks like a finished history of A and B, and it answers
CLEAN while C has already contaminated the validation window. Fail-closed on a MISSING footprint
cannot see this, because nothing is missing from its point of view.

So the acceptance statement of this module is not about diligence:

    research execution cannot obtain research data by any path that does not produce an
    ExposureFootprint.

TWO FACTS THAT ARE NOT THE SAME, and conflating them is how the false CLEAN happens:

    footprints   what was read
    receipt      proof that the reads were completely registered

Without the second, `0 footprints` is ambiguous — an execution that honestly read nothing looks
exactly like an execution whose instrumentation never fired. With it, `read_count=0,
complete=true` is a positive statement, and a missing receipt is UNKNOWN rather than an empty
history that reads as clean.

HOW STRONG THE GUARANTEE ACTUALLY IS. Two words, and they are not synonyms:

    ENFORCED_IN_PROCESS   this module. While an execution is open, the direct paths into
                          protected sources raise. A research worker in this process cannot
                          reach the data another way, and a determined caller who removes the
                          guard obviously can — it is the same process.

    ISOLATED              a separate data service, the research worker holding no OS-level
                          access to the files at all. Then "every read is observable" is a
                          property of the deployment rather than of a patched function.

This file implements the first and is named honestly about it. `GUARANTEE` is reported in every
receipt so no reader can mistake one for the other later.

THE GUARD IS SCOPED TO EXECUTIONS ON PURPOSE. Arming it globally would break the rest of the
application, which reads these databases legitimately and is not research execution. It arms
when an execution opens and disarms when it closes, so the claim is precisely "a read that
happens while a study is running is a read the study accounted for".

DERIVED ARTIFACTS ARE THE QUIET BYPASS. Reading `opportunities.parquet` for 2025 is reading
bars for 2025, one materialisation later. A gateway that records only "read opportunities.parquet"
would let a study touch outcome-derived data with a clean conscience. So a registered artifact
carries its lineage, and reading it produces footprints for its SOURCES over the dates it covers.
"""
from __future__ import annotations

import hashlib
import os
import threading
from dataclasses import asdict, dataclass, field

from data_access import (CATALOG, DataAccessLayer, DataAccessSpec,  # noqa: E402
                         ExposureFootprint, SourceCatalog, inside_gateway, internal)

ENFORCED_IN_PROCESS = "ENFORCED_IN_PROCESS"
ISOLATED = "ISOLATED"
GUARANTEE = ENFORCED_IN_PROCESS

COMPLETE, UNKNOWN, VIOLATED = "COMPLETE", "UNKNOWN", "VIOLATED"

EXECUTION_STARTED = "EXECUTION_STARTED"
EXECUTION_RECEIPT = "EXECUTION_RECEIPT"
DIRECT_ACCESS_ATTEMPT = "DIRECT_ACCESS_ATTEMPT"


class DirectDataAccessError(RuntimeError):
    """A protected source was reached without going through the gateway."""


class UnregisteredSourceAccessError(RuntimeError):
    """A source nobody declared. Its lineage and cutoff are unknown, so it cannot be read."""


class CapabilityError(RuntimeError):
    """A capability that does not authorise what is being attempted."""


# ── what may be read, and what it is made of ────────────────────────────────
@dataclass(frozen=True)
class SourceRegistration:
    """A source the gateway will serve, and everything needed to account for reading it."""
    source_id: str
    path: str
    reader: object                       # callable(path, start, end, columns) -> (rows, dates)
    universe: str = "*"
    temporal_resolution: str = "1d"
    derived_from: tuple = ()             # source_ids this artifact was materialised from
    contains_outcome_derived_fields: bool = False
    artifact_version: str = ""

    @property
    def is_derived(self) -> bool:
        return bool(self.derived_from)


class ProtectedSourceRegistry:
    """Every source the gateway serves, and every path the guard must defend."""

    def __init__(self):
        self._by_id: dict = {}
        self._by_path: dict = {}

    def register(self, reg: SourceRegistration) -> None:
        self._by_id[reg.source_id] = reg
        if reg.path:
            self._by_path[os.path.abspath(reg.path)] = reg

    def get(self, source_id: str) -> SourceRegistration:
        if source_id not in self._by_id:
            raise UnregisteredSourceAccessError(
                f"source {source_id!r} is not registered. An unregistered source has no declared "
                f"lineage and no server-side cutoff, so a read from it cannot be accounted for "
                f"and cannot be permitted.")
        return self._by_id[source_id]

    def protects(self, path: str) -> SourceRegistration | None:
        try:
            return self._by_path.get(os.path.abspath(path))
        except Exception:                                            # noqa: BLE001
            return None

    def ids(self) -> tuple:
        return tuple(self._by_id)


REGISTRY = ProtectedSourceRegistry()


# ── the capability handed to research code ──────────────────────────────────
@dataclass(frozen=True)
class DataAccessCapability:
    """What research execution receives INSTEAD of a path or a connection.

    It carries no filesystem location. Code holding this can name a source and ask the gateway
    for it; it cannot open anything, which is the entire point — a capability that contained a
    path would be a suggestion.
    """
    session_id: str
    execution_id: str
    purpose: str
    allowed_sources: tuple
    access_spec_hash: str
    issued_state_hash: str

    def authorises(self, source_id: str) -> bool:
        return source_id in self.allowed_sources

    @property
    def capability_hash(self) -> str:
        return hashlib.sha256(
            f"{self.session_id}|{self.execution_id}|{self.purpose}|"
            f"{','.join(sorted(self.allowed_sources))}|{self.access_spec_hash}|"
            f"{self.issued_state_hash}".encode()).hexdigest()[:16]


@dataclass(frozen=True)
class ExecutionReceipt:
    """Proof that the reads of one execution were completely registered.

    `read_count = 0` with `complete = True` is a positive statement — this execution demonstrably
    read nothing. That is a different fact from "no footprints were recorded", which is what an
    execution with broken instrumentation also looks like.
    """
    execution_id: str
    session_id: str
    access_spec_hash: str
    capability_hash: str
    source_snapshot_ids: tuple
    footprint_hashes: tuple
    read_count: int
    execution_code_hash: str
    complete: bool
    guarantee: str = GUARANTEE
    violations: tuple = field(default_factory=tuple)

    @property
    def receipt_hash(self) -> str:
        return hashlib.sha256(
            f"{self.execution_id}|{self.session_id}|{self.access_spec_hash}|"
            f"{self.capability_hash}|{','.join(self.footprint_hashes)}|{self.read_count}|"
            f"{self.execution_code_hash}|{self.complete}|{self.guarantee}".encode()
        ).hexdigest()[:16]

    def as_dict(self) -> dict:
        d = asdict(self)
        d["source_snapshot_ids"] = list(self.source_snapshot_ids)
        d["footprint_hashes"] = list(self.footprint_hashes)
        d["violations"] = list(self.violations)
        d["receipt_hash"] = self.receipt_hash
        return d


# ── the guard ───────────────────────────────────────────────────────────────
_STATE = threading.local()


def _active() -> "ExecutionContext | None":
    return getattr(_STATE, "execution", None)


_ORIGINALS: dict = {}


def install_guards(registry: ProtectedSourceRegistry = REGISTRY) -> None:
    """Close the direct paths for the duration of any execution.

    Patching is honest about what it is: an in-process barrier. It is armed only while an
    execution is open, because the rest of the application reads these same files for reasons
    that are not research and must not be broken by a research contract.
    """
    def _wrap(module_name: str, attr: str, path_arg: int):
        try:
            mod = __import__(module_name)
        except Exception:                                            # noqa: BLE001
            return
        original = getattr(mod, attr, None)
        if original is None or (module_name, attr) in _ORIGINALS:
            return
        _ORIGINALS[(module_name, attr)] = original

        def guarded(*args, **kwargs):
            ex = _active()
            if ex is not None and not inside_gateway():
                target = args[path_arg] if len(args) > path_arg else kwargs.get("path", "")
                reg = registry.protects(str(target))
                if reg is not None:
                    ex.record_violation(f"{module_name}.{attr}", str(target), reg.source_id)
                    raise DirectDataAccessError(
                        f"{module_name}.{attr} was called on {reg.source_id!r} while execution "
                        f"{ex.execution_id} is open. Research data is reachable only through the "
                        f"gateway, which is what makes every read observable. This execution is "
                        f"now VIOLATED — not because the read succeeded, it did not, but because "
                        f"a path exists in this code that the accounting cannot see.")
            return original(*args, **kwargs)

        setattr(mod, attr, guarded)

    _wrap("duckdb", "connect", 0)
    _wrap("pandas", "read_parquet", 0)


def uninstall_guards() -> None:
    for (module_name, attr), original in list(_ORIGINALS.items()):
        setattr(__import__(module_name), attr, original)
        _ORIGINALS.pop((module_name, attr), None)


# ── the handle and the execution ────────────────────────────────────────────
class DatasetHandle:
    """The only readable object research code ever holds. It knows a source_id, not a path."""

    def __init__(self, ex: "ExecutionContext", reg: SourceRegistration):
        self._ex = ex
        self._reg = reg

    @property
    def source_id(self) -> str:
        return self._reg.source_id

    def read(self, start: str, end: str, columns: tuple = ()):
        return self._ex._read(self._reg, start, end, columns)


class ExecutionContext:
    """One study execution. Opens the guard, collects footprints, closes with a receipt."""

    def __init__(self, session, capability: DataAccessCapability, *,
                 registry: ProtectedSourceRegistry = REGISTRY,
                 catalog: SourceCatalog = CATALOG, code_hash: str = ""):
        self.session = session
        self.capability = capability
        self.execution_id = capability.execution_id
        self.registry = registry
        self.catalog = catalog
        self.code_hash = code_hash
        self._layers: dict = {}
        self._violations: list = []
        self._closed = False
        self._read_count = 0

    # ── lifecycle ───────────────────────────────────────────────────────────
    def __enter__(self) -> "ExecutionContext":
        if _active() is not None:
            raise CapabilityError("an execution is already open in this thread")
        _STATE.execution = self
        if self.session is not None:
            self.session.record_execution_started(self.capability)
        return self

    def __exit__(self, exc_type, exc, tb):
        _STATE.execution = None
        self.close()
        return False

    def open(self, source_id: str) -> DatasetHandle:
        if not self.capability.authorises(source_id):
            raise CapabilityError(
                f"capability {self.capability.capability_hash} does not authorise {source_id!r}. "
                f"It permits {sorted(self.capability.allowed_sources)}.")
        return DatasetHandle(self, self.registry.get(source_id))

    def record_violation(self, how: str, target: str, source_id: str) -> None:
        v = {"how": how, "target": os.path.basename(target), "source_id": source_id}
        self._violations.append(v)
        if self.session is not None:
            self.session.record_direct_access_attempt(self.execution_id, v)

    # ── reading ─────────────────────────────────────────────────────────────
    def _layer(self, source_id: str, universe: str, resolution: str,
               declared: DataAccessSpec) -> DataAccessLayer:
        if source_id not in self._layers:
            self._layers[source_id] = DataAccessLayer(declared, self.catalog)
        return self._layers[source_id]

    def _spec_for(self, reg: SourceRegistration) -> DataAccessSpec:
        base = DataAccessSpec.from_dict(self.session.access_spec) if (
            self.session is not None and self.session.access_spec) else None
        return DataAccessSpec(
            source_id=reg.source_id, universe=reg.universe or (base.universe if base else "*"),
            start=base.start if base else "1900-01-01", end=base.end if base else "2999-12-31",
            temporal_resolution=reg.temporal_resolution,
            purpose=(base.purpose if base else "DEVELOPMENT"))

    def _read(self, reg: SourceRegistration, start: str, end: str, columns: tuple):
        if self._closed:
            raise CapabilityError("this execution is closed; its receipt has already been issued")
        with internal():
            rows, dates = reg.reader(reg.path, start, end, columns)
        self._read_count += 1

        self._layer(reg.source_id, reg.universe, reg.temporal_resolution,
                    self._spec_for(reg)).record(start, end, dates=dates)

        # A derived artifact is its sources, one materialisation later. Recording only the
        # artifact would let a study read outcome-derived data and stay clean on paper.
        for parent_id in reg.derived_from:
            parent = self.registry.get(parent_id)
            self._layer(parent_id, parent.universe, parent.temporal_resolution,
                        self._spec_for(parent)).record(start, end, dates=dates)
        return rows

    # ── closing ─────────────────────────────────────────────────────────────
    def close(self) -> ExecutionReceipt:
        if self._closed:
            return self._receipt
        self._closed = True
        footprints, snapshots = [], []
        for source_id, layer in self._layers.items():
            fp = layer.footprint()
            if fp is None:
                continue
            footprints.append(fp)
            snapshots.append(layer.snapshot)
            if self.session is not None:
                self.session.record_footprint(fp.as_dict())

        self._receipt = ExecutionReceipt(
            execution_id=self.execution_id, session_id=self.capability.session_id,
            access_spec_hash=self.capability.access_spec_hash,
            capability_hash=self.capability.capability_hash,
            source_snapshot_ids=tuple(sorted(set(snapshots))),
            footprint_hashes=tuple(f.footprint_hash for f in footprints),
            read_count=self._read_count, execution_code_hash=self.code_hash,
            complete=not self._violations, violations=tuple(
                f"{v['how']}:{v['source_id']}" for v in self._violations))
        if self.session is not None:
            self.session.record_execution_receipt(self._receipt.as_dict())
        return self._receipt


def capability_for(session, execution_id: str, allowed_sources, purpose: str = "DEVELOPMENT"
                   ) -> DataAccessCapability:
    spec = session.access_spec or {}
    return DataAccessCapability(
        session_id=session.session_id, execution_id=execution_id, purpose=purpose,
        allowed_sources=tuple(sorted(allowed_sources)),
        access_spec_hash=DataAccessSpec.from_dict(spec).spec_hash if spec else "",
        issued_state_hash=session._state_hash())


# ── completeness, read from the durable history ─────────────────────────────
def access_completeness(events, session_ids=None) -> tuple:
    """(status, why). A separate axis from contamination, and it gates it.

    Contamination answers "was this window read". Completeness answers "can that question be
    answered at all". Folding the second into the first would let a study with unobserved reads
    produce CLEAN, which is the exact failure this module exists to prevent.
    """
    rows = [e for e in events if session_ids is None or e.session_id in session_ids]

    violations = [e for e in rows if e.event_type == DIRECT_ACCESS_ATTEMPT]
    if violations:
        who = sorted({e.session_id for e in violations})
        return VIOLATED, (
            f"{len(violations)} direct access attempt(s) bypassed the gateway in {who}. This is "
            f"not merely unknown: a read path exists that the accounting cannot see, so nothing "
            f"computed under it can be reported as confirmatory.")

    started = {e.payload.get("execution_id") for e in rows
               if e.event_type == EXECUTION_STARTED}
    receipted = {}
    for e in rows:
        if e.event_type == EXECUTION_RECEIPT and e.payload.get("receipt"):
            r = e.payload["receipt"]
            receipted[r.get("execution_id")] = r

    missing = {x for x in started if x and x not in receipted}
    if missing:
        return UNKNOWN, (
            f"{len(missing)} execution(s) started and issued no receipt. Footprints from the "
            f"others prove what WAS read; they do not prove that everything read was recorded.")

    incomplete = [r for r in receipted.values() if not r.get("complete")]
    if incomplete:
        return VIOLATED, f"{len(incomplete)} receipt(s) report an incomplete execution"

    exposures = [e for e in rows if e.event_type in ("RESULT_EXPOSED", "SEARCH_RUN")]
    if exposures and not receipted:
        return UNKNOWN, (
            "results were exposed and no execution receipt exists, so the reads behind them were "
            "never attested. A history with no gaps in it is not the same as a complete one.")

    return COMPLETE, (
        f"{len(receipted)} execution(s), each closed with a receipt; "
        f"{sum(int(r.get('read_count', 0)) for r in receipted.values())} read(s) registered "
        f"under guarantee {GUARANTEE}.")
