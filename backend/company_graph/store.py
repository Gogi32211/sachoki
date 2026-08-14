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


def connect(read_only: bool = False):
    return duckdb.connect(db_path(DB_FILE), read_only=read_only)


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
        c.execute("""INSERT INTO entity VALUES (?,?,?,?,?,?,?,?,?,?,?)""", [
            ent["cik"], ent.get("ticker", ""), ent.get("name", ""), str(ent.get("sic", "")),
            ent.get("sic_description", ""), ent.get("hq_country", "") or ent.get("country", ""),
            ent.get("hq_state", "") or ent.get("state", ""),
            ent.get("hq_city", "") or ent.get("city", ""),
            json.dumps(ent.get("exchanges") or []), is_target,
            datetime.now(timezone.utc)])


def save_edges(graph_ticker: str, edges: Iterable, tier: int = 1) -> int:
    rows = [e.to_row() if hasattr(e, "to_row") else e for e in edges]
    if not rows:
        return 0
    with _lock, connect() as c:
        c.execute(_DDL); _migrate(c)
        nid = _next_id(c, "edge", "edge_id")
        for i, r in enumerate(rows):
            c.execute("""INSERT INTO edge VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", [
                nid + i, graph_ticker, tier, r["src"], r["dst"], r["rel_type"],
                r["direction"], r["component"], r["product"], r["share_pct"],
                r.get("share_basis", ""),
                r["confidence"], r["claimed_confidence"], r["ceiling_applied"],
                r["status"], r["evidence_tier"], r["source_url"], r["source_label"],
                r["quote"], r["doc_date"], r["retrieved_at"], r["valid_from"],
                r["extractor"], r["contract_version"]])
    return len(rows)


def current_edges(graph_ticker: str) -> list[dict]:
    """Latest evidence per distinct relationship. See the module note on why not at write."""
    with connect(read_only=True) as c:
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
    with connect(read_only=True) as c:
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
    with connect(read_only=True) as c:
        try:
            df = c.execute("SELECT * FROM harvest_run WHERE ticker = ? "
                           "ORDER BY run_id DESC LIMIT 1", [ticker]).fetchdf()
        except duckdb.CatalogException:
            return None
    return df.to_dict("records")[0] if len(df) else None


__all__ = ["init", "connect", "upsert_entity", "save_edges", "current_edges", "entities",
           "start_run", "finish_run", "last_run", "DB_FILE"]
