"""What a query was allowed to read, and what it actually read. Those are two different facts.

The evidence boundary used to compare a validation window against a DECLARED development window,
and a declaration is exactly the thing a researcher controls. Declare 2024–2025, let a helper
quietly touch March 2026, and validating from January 2026 comes back CLEAN. The declaration was
never wrong; it was just not what happened.

This is the same distinction the search side already makes, and naming the symmetry is the point:

    SEARCH      k_declared            what the frozen space permitted
                k_actual              what the algorithm ranked

    EVIDENCE    window_declared       DataAccessSpec
                footprint_actual      ExposureFootprint

In both pairs the ACTUAL one governs. A declaration is a commitment worth recording and worth
comparing against, and it is never the evidence that the commitment was kept.

WHY THE ACCESS LAYER RECORDS RATHER THAN REFUSES. A read outside the declared spec could be
blocked, and blocking is the right behaviour for a production gate. Here it would be the wrong
one: a refusal teaches the caller to declare a wider window, and a wider declaration is harmless
to the caller and useless to everyone else. Recording is what makes the overreach visible —
the footprint carries `exceeded_declaration`, the contamination registry reads the actual range,
and a study that touched what it said it would not touch is caught by the verdict rather than
by an exception nobody sees.

THE SNAPSHOT AND THE CUTOFF ARE SERVER FACTS. `data_cutoff_at_registration` decides whether a
validation window is FORWARD, which is the strongest verdict the system can issue. Taking it
from the request body would mean the strongest verdict rests on the caller's assertion about the
one thing the caller most benefits from misstating. So it is derived here, from the source, at
freeze time — and when it cannot be derived, registration is refused rather than defaulted.
"""
from __future__ import annotations

import hashlib
import os
from dataclasses import asdict, dataclass, field

DEVELOPMENT, VALIDATION = "DEVELOPMENT", "VALIDATION"


class DataAccessError(Exception):
    """A spec or a footprint that cannot mean what it claims."""


class SourceUnavailableError(Exception):
    """The source cannot state its cutoff, so nothing may be frozen against it."""


@dataclass(frozen=True)
class DataAccessSpec:
    """What a piece of research declares it will read. A commitment, not a measurement."""
    source_id: str
    universe: str
    start: str
    end: str
    temporal_resolution: str = "1d"
    fields: tuple = ()
    purpose: str = DEVELOPMENT

    def __post_init__(self):
        for f in ("source_id", "universe", "start", "end", "temporal_resolution"):
            if not getattr(self, f):
                raise DataAccessError(f"DataAccessSpec.{f} is empty")
        if self.start > self.end:
            raise DataAccessError(f"spec range {self.start}..{self.end} runs backwards")
        if self.purpose not in (DEVELOPMENT, VALIDATION):
            raise DataAccessError(f"unknown purpose {self.purpose!r}")

    @property
    def label(self) -> str:
        return f"{self.source_id}/{self.universe}@{self.temporal_resolution}" \
               f"[{self.start}..{self.end}]"

    @property
    def spec_hash(self) -> str:
        return hashlib.sha256(
            f"{self.label}|{','.join(sorted(self.fields))}|{self.purpose}".encode()
        ).hexdigest()[:16]

    def as_dict(self) -> dict:
        d = asdict(self)
        d["fields"] = list(self.fields)
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "DataAccessSpec":
        d = dict(d)
        d["fields"] = tuple(d.get("fields") or ())
        return cls(**d)

    def covers(self, start: str, end: str) -> bool:
        return self.start <= start and end <= self.end


@dataclass(frozen=True)
class ExposureFootprint:
    """What was actually touched. Produced by the access layer, never by the caller."""
    source_id: str
    universe: str
    temporal_resolution: str
    accessed_start: str
    accessed_end: str
    source_snapshot: str
    dates_touched: int
    declared_spec_hash: str = ""
    exceeded_declaration: bool = False
    reads: tuple = field(default_factory=tuple)

    @property
    def label(self) -> str:
        return (f"{self.source_id}/{self.universe}@{self.temporal_resolution}"
                f"[{self.accessed_start}..{self.accessed_end}]")

    @property
    def footprint_hash(self) -> str:
        return hashlib.sha256(
            f"{self.label}|{self.source_snapshot}|{self.dates_touched}|"
            f"{self.declared_spec_hash}".encode()).hexdigest()[:16]

    def as_dict(self) -> dict:
        d = asdict(self)
        d["reads"] = [list(r) for r in self.reads]
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "ExposureFootprint":
        d = dict(d)
        d["reads"] = tuple(tuple(r) for r in (d.get("reads") or ()))
        return cls(**d)

    def overlaps_range(self, source_id: str, universe: str, start: str, end: str) -> bool:
        """Universe '*' on either side means the whole market, which overlaps everything."""
        if self.source_id != source_id:
            return False
        if self.universe != universe and "*" not in (self.universe, universe):
            return False
        return self.accessed_start <= end and start <= self.accessed_end


# ── where a cutoff comes from ───────────────────────────────────────────────
class SourceCatalog:
    """Server-side truth about each source: its snapshot identity and its latest observation.

    Pluggable so a test can inject a source without a database, and NOT optional: a source with
    no provider raises rather than returning a permissive default. The failure mode of a wrong
    default here is a historical window being certified FORWARD.
    """

    def __init__(self):
        self._providers: dict = {}
        self._cache: dict = {}

    def register(self, source_id: str, provider) -> None:
        self._providers[source_id] = provider
        self._cache.pop(source_id, None)

    def snapshot(self, source_id: str) -> tuple:
        """(snapshot_id, cutoff_date). Raises if the source cannot answer."""
        if source_id in self._cache:
            return self._cache[source_id]
        if source_id not in self._providers:
            raise SourceUnavailableError(
                f"no provider for source {source_id!r}. A boundary cannot be frozen against a "
                f"source whose cutoff the server cannot establish — the alternative is trusting "
                f"the caller on the one field that decides FORWARD.")
        try:
            snap, cutoff = self._providers[source_id]()
        except Exception as e:                                       # noqa: BLE001
            raise SourceUnavailableError(f"source {source_id!r} could not be read: {e}") from e
        if not snap or not cutoff:
            raise SourceUnavailableError(f"source {source_id!r} returned an empty snapshot")
        self._cache[source_id] = (snap, cutoff)
        return snap, cutoff

    def invalidate(self, source_id: str = "") -> None:
        self._cache.pop(source_id, None) if source_id else self._cache.clear()


CATALOG = SourceCatalog()


def duckdb_bars_provider(db_path: str, table: str = "bars", tf: str = "1d"):
    """Cutoff = the latest bar the analytics database actually holds.

    Read-only, because the bars writer is the nightly launchd job and this must never be a second
    one. The snapshot identity is the file's size and mtime together with the max date, so a
    rebuild that keeps the same last bar still produces a different snapshot id.
    """
    def _provider():
        import duckdb
        st = os.stat(db_path)
        con = duckdb.connect(db_path, read_only=True)
        try:
            row = con.execute(f"SELECT max(date) FROM {table}").fetchone()
        finally:
            con.close()
        cutoff = str(row[0])[:10] if row and row[0] else ""
        snap = hashlib.sha256(
            f"{db_path}|{st.st_size}|{int(st.st_mtime)}|{tf}|{cutoff}".encode()).hexdigest()[:16]
        return snap, cutoff
    return _provider


# ── the access layer ────────────────────────────────────────────────────────
class DataAccessLayer:
    """Mediates reads and accumulates the footprint. One per session.

    It does not fetch anything itself; callers hand it the range they are about to read and it
    records the truth. That keeps it usable over DuckDB, a cached frame or a fixture, and it
    means the footprint describes the study rather than one storage engine.
    """

    def __init__(self, spec: DataAccessSpec, catalog: SourceCatalog = CATALOG):
        self.spec = spec
        self.catalog = catalog
        self.snapshot, self.cutoff = catalog.snapshot(spec.source_id)
        self._reads: list = []

    def record(self, start: str, end: str, dates: int = 0) -> tuple:
        """Register an actual read. Returns (start, end) unchanged; the point is the recording."""
        if start > end:
            raise DataAccessError(f"read range {start}..{end} runs backwards")
        self._reads.append((start, end, int(dates)))
        return start, end

    @property
    def touched(self) -> bool:
        return bool(self._reads)

    def footprint(self) -> ExposureFootprint | None:
        if not self._reads:
            return None
        lo = min(r[0] for r in self._reads)
        hi = max(r[1] for r in self._reads)
        return ExposureFootprint(
            source_id=self.spec.source_id, universe=self.spec.universe,
            temporal_resolution=self.spec.temporal_resolution,
            accessed_start=lo, accessed_end=hi, source_snapshot=self.snapshot,
            dates_touched=sum(r[2] for r in self._reads),
            declared_spec_hash=self.spec.spec_hash,
            exceeded_declaration=not self.spec.covers(lo, hi),
            reads=tuple(self._reads))
