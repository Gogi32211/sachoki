"""company_graph/store.py — where the graph lives.

Its own DuckDB file, `company_graph.duckdb`, and that is deliberate. The bars databases
have exactly two sanctioned writers and a nightly window nothing else may touch; a page
that harvests documents on demand has no business anywhere near them. Nothing in this
package opens a bars DB.

EDGES ARE APPENDED, NEVER OVERWRITTEN
    Re-harvesting a ticker does not replace what was there. Each edge row keeps the
    document it came from and the date that document was filed, so the same pair can
    legitimately hold several rows: CoreWeave's 10-Q supports CUSTOMER_OF, and the same
    filing separately supports NVIDIA OWNS CoreWeave. Collapsing those into one "edge"
    would be discarding the finding.

    `current_edges` does the deduplication at READ time, keeping the most recent evidence
    per (src, dst, rel_type, component). A relationship that stopped being mentioned is
    still in the table with its old date, which is the only way to notice that it went
    stale rather than silently inheriting it forever.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from typing import Iterable, Optional

import duckdb

from studio.paths import db_path

DB_FILE = "company_graph.duckdb"
SCHEMA_VERSION = 1

_lock = threading.Lock()          # DuckDB is single-writer; the page is multi-request

_DDL = """
CREATE TABLE IF NOT EXISTS entity (
    cik              VARCHAR PRIMARY KEY,
    ticker           VARCHAR,
    name             VARCHAR,
    sic              VARCHAR,
    sic_description  VARCHAR,
    country          VARCHAR,
    state            VARCHAR,
    city             VARCHAR,
    exchanges        VARCHAR,
    is_target        BOOLEAN DEFAULT FALSE,
    updated_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS edge (
    edge_id            BIGINT,
    graph_ticker       VARCHAR,      -- which target's harvest produced this row
    tier               INTEGER,
    src                VARCHAR,
    dst                VARCHAR,
    rel_type           VARCHAR,
    direction          VARCHAR,
    component          VARCHAR,
    product            VARCHAR,
    share_pct          DOUBLE,
    share_basis        VARCHAR,
    confidence         VARCHAR,
    claimed_confidence VARCHAR,
    ceiling_applied    BOOLEAN,
    status             VARCHAR,
    evidence_tier      VARCHAR,
    source_url         VARCHAR,
    source_label       VARCHAR,
    quote              VARCHAR,
    doc_date           VARCHAR,
    retrieved_at       VARCHAR,
    valid_from         VARCHAR,
    extractor          VARCHAR,
    contract_version   VARCHAR
);

CREATE TABLE IF NOT EXISTS harvest_run (
    run_id          BIGINT,
    ticker          VARCHAR,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          VARCHAR,
    params          VARCHAR,
    n_documents     INTEGER,
    n_candidates    INTEGER,
    n_processed     INTEGER,
    n_edges         INTEGER,
    n_rejected      INTEGER,
    rejections      VARCHAR,
    error           VARCHAR
);
"""


_db = None                        # one DuckDB instance for the process
_db_lock = threading.Lock()


def _database():
    global _db
    with _db_lock:
        if _db is None:
            _db = duckdb.connect(db_path(DB_FILE))
            _db.execute(_DDL)
        return _db


class _Cursor:
    """Context manager yielding a thread-local cursor on the one shared database."""

    def __enter__(self):
        self._c = _database().cursor()
        return self._c

    def __exit__(self, *exc):
        try:
            self._c.close()
        except Exception:                                             # noqa: BLE001
            pass
        return False


def connect(read_only: bool = False):
    """A cursor on the single process-wide database handle.

    `read_only` is accepted and ignored, on purpose. DuckDB refuses to open the same file
    twice with DIFFERENT configuration, so read-only readers and a read-write writer in
    one process are mutually exclusive — and the failure is at connect time, in whichever
    one arrives second.

    That is not theoretical. It killed a live harvest: the page polls progress every three
    seconds, each poll opened a read-only connection, and the background thread's next
    entity write raised

        Can't open a connection to same database file with a different configuration

    The whole point of harvesting in the background is that the page stays usable while it
    runs, so the reads that make the feature worthwhile were the reads that broke it.
    Cursors off one handle share the instance and are the documented way to use DuckDB
    from several threads.
    """
    return _Cursor()


def _columns_of(ddl: str, table: str) -> list[tuple]:
    body = ddl.split(f"CREATE TABLE IF NOT EXISTS {table} (", 1)[1].split(");", 1)[0]
    out = []
    for line in body.strip().splitlines():
        line = line.split("--", 1)[0].strip().rstrip(",")
        if not line:
            continue
        parts = line.split()
        if len(parts) >= 2:
            out.append((parts[0], " ".join(parts[1:])))
    return out


def _migrate(c) -> list[str]:
    """Add columns the DDL has gained since this file was created.

    CREATE TABLE IF NOT EXISTS is a no-op on an existing table, so a new column in the
    DDL simply never appears and the next INSERT fails on arity — at write time, halfway
    through a harvest, rather than at startup. This walks the declared schema and ALTERs
    in whatever is missing.
    """
    added = []
    for table in ("entity", "edge", "harvest_run"):
        try:
            have = {r[0] for r in c.execute(f"DESCRIBE {table}").fetchall()}
        except duckdb.CatalogException:
            continue
        for col, typ in _columns_of(_DDL, table):
            if col not in have:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ.split('DEFAULT')[0]}")
                added.append(f"{table}.{col}")
    return added


def init() -> list[str]:
    with _lock, connect() as c:
        c.execute(_DDL)
        return _migrate(c)


def _next_id(c, table: str, col: str) -> int:
    r = c.execute(f"SELECT COALESCE(MAX({col}), 0) + 1 FROM {table}").fetchone()
    return int(r[0]) if r else 1


def upsert_entity(ent: dict, is_target: bool = False) -> None:
    if not ent.get("cik"):
        return
    with _lock, connect() as c:
        c.execute(_DDL); _migrate(c)
        c.execute("DELETE FROM entity WHERE cik = ?", [ent["cik"]])
        # columns named, never positional — see the note above save_edges
        c.execute("""INSERT INTO entity
            (cik, ticker, name, sic, sic_description, country, state, city, exchanges,
             is_target, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""", [
            ent["cik"], ent.get("ticker", ""), ent.get("name", ""), str(ent.get("sic", "")),
            ent.get("sic_description", ""), ent.get("hq_country", "") or ent.get("country", ""),
            ent.get("hq_state", "") or ent.get("state", ""),
            ent.get("hq_city", "") or ent.get("city", ""),
            json.dumps(ent.get("exchanges") or []), is_target,
            datetime.now(timezone.utc)])


# Every INSERT in this file names its columns. Positional inserts are only safe while the
# table's PHYSICAL column order matches the declared one, and _migrate guarantees it will
# not: ALTER TABLE ADD COLUMN appends to the end, while the DDL declares new columns where
# they belong. Adding share_basis in the middle of the DDL therefore shifted every
# positional value one place on an already-migrated table, and the harvest died with
#
#     Conversion Error: Could not convert string 'HIGH' to BOOL
#
# — confidence landing in ceiling_applied, three columns downstream of the real mistake.
# The migration that was added to make schema changes safe is what created the hazard.
_EDGE_COLS = ["edge_id", "graph_ticker", "tier", "src", "dst", "rel_type", "direction",
              "component", "product", "share_pct", "share_basis", "confidence",
              "claimed_confidence", "ceiling_applied", "status", "evidence_tier",
              "source_url", "source_label", "quote", "doc_date", "retrieved_at",
              "valid_from", "extractor", "contract_version"]


def save_edges(graph_ticker: str, edges: Iterable, tier: int = 1) -> int:
    rows = [e.to_row() if hasattr(e, "to_row") else e for e in edges]
    if not rows:
        return 0
    sql = (f"INSERT INTO edge ({', '.join(_EDGE_COLS)}) "
           f"VALUES ({', '.join('?' * len(_EDGE_COLS))})")
    with _lock, connect() as c:
        c.execute(_DDL); _migrate(c)
        nid = _next_id(c, "edge", "edge_id")
        for i, r in enumerate(rows):
            vals = {**r, "edge_id": nid + i, "graph_ticker": graph_ticker, "tier": tier}
            c.execute(sql, [vals.get(col) for col in _EDGE_COLS])
    return len(rows)


def current_edges(graph_ticker: str) -> list[dict]:
    """Latest evidence per distinct relationship. See the module note on why not at write."""
    with connect() as c:
        try:
            df = c.execute("""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY src, dst, rel_type, component
                        ORDER BY COALESCE(doc_date, '') DESC, edge_id DESC) rn
                    FROM edge WHERE graph_ticker = ?
                ) WHERE rn = 1
                ORDER BY status DESC, confidence DESC, COALESCE(doc_date,'') DESC
            """, [graph_ticker]).fetchdf()
        except duckdb.CatalogException:
            return []
    return df.drop(columns=["rn"]).to_dict("records") if len(df) else []


def entities(ciks: Optional[list] = None) -> list[dict]:
    with connect() as c:
        try:
            if ciks:
                q = "SELECT * FROM entity WHERE cik IN (" + ",".join("?" * len(ciks)) + ")"
                df = c.execute(q, ciks).fetchdf()
            else:
                df = c.execute("SELECT * FROM entity").fetchdf()
        except duckdb.CatalogException:
            return []
    return df.to_dict("records") if len(df) else []


def start_run(ticker: str, params: dict) -> int:
    with _lock, connect() as c:
        c.execute(_DDL); _migrate(c)
        rid = _next_id(c, "harvest_run", "run_id")
        c.execute("INSERT INTO harvest_run (run_id, ticker, started_at, status, params) "
                  "VALUES (?,?,?,?,?)",
                  [rid, ticker, datetime.now(timezone.utc), "RUNNING", json.dumps(params)])
    return rid


def finish_run(run_id: int, **kw) -> None:
    sets, vals = [], []
    for k, v in kw.items():
        sets.append(f"{k} = ?")
        vals.append(json.dumps(v) if isinstance(v, (list, dict)) else v)
    sets.append("finished_at = ?")
    vals.append(datetime.now(timezone.utc))
    vals.append(run_id)
    with _lock, connect() as c:
        c.execute(f"UPDATE harvest_run SET {', '.join(sets)} WHERE run_id = ?", vals)


def last_run(ticker: str) -> Optional[dict]:
    with connect() as c:
        try:
            df = c.execute("SELECT * FROM harvest_run WHERE ticker = ? "
                           "ORDER BY run_id DESC LIMIT 1", [ticker]).fetchdf()
        except duckdb.CatalogException:
            return None
    return df.to_dict("records")[0] if len(df) else None


__all__ = ["init", "connect", "upsert_entity", "save_edges", "current_edges", "entities",
           "start_run", "finish_run", "last_run", "DB_FILE"]
