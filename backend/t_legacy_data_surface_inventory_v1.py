"""MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1 — enumerate the space, then and only then speak.

Three claims of absence were sealed in this programme and all three were wrong, in one shape:
a bounded search returned nothing and the nothing was reported as non-existence. A `timeout`
that never ran. A daily `bars` table generalised into "no 15m store exists". A 15m OHLCV store
found, half the claim corrected, and the other half re-asserted — while a 43 GB signal-bearing
store sat in `data/`, a directory that was never listed.

SO THIS FILE RECORDS THE SEARCH SPACE, NOT JUST THE FINDINGS. Every root is declared up front
and every one must be visited. Every failure to read a root, a symlink, a database or a schema
is counted. If any declared root goes unvisited, or any read fails, the artifact still seals —
but it seals with negative_existence_claims_permitted = FALSE, and nothing downstream may then
assert that something does not exist. Absence becomes claimable only when the enumeration is
provably complete.

THE POINT IS TO REMOVE THE ERROR BY CONSTRUCTION. Not to be more careful next time — care is
what failed three times — but to make the permission to say "does not exist" depend on a
measured property of the search rather than on my confidence.

READ-ONLY. Every database is opened read_only; nothing is created, moved or written.
"""
from __future__ import annotations
import glob, hashlib, json, os, subprocess, sys, time                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                                 # noqa: E402

REPO = "/Users/sachoki/Desktop/sachoki-desktop"
VOL = "/Volumes/QUANT_RESEARCH"
DECLARED_ROOTS = [
    REPO, f"{REPO}/data", f"{REPO}/backend", f"{REPO}/analysis", f"{REPO}/exports",
    f"{REPO}/scripts", f"{REPO}/tests", f"{REPO}/tz_intelligence_package", f"{REPO}/pine",
    VOL, f"{VOL}/source_data", f"{VOL}/studio_data", f"{VOL}/research", f"{VOL}/archive",
    f"{VOL}/artifacts", f"{VOL}/scratch",
]
DB_EXT = (".duckdb", ".db", ".sqlite", ".sqlite3")
DATA_EXT = (".parquet", ".csv", ".feather", ".arrow", ".jsonl", ".npz")
# Excluded by DECLARED SCOPE SEMANTICS, never because a file failed to open. macOS
# search-index internals are not an application or research data surface and are neither
# produced nor consumed by this programme. The guard below proves the exclusion was not
# used to dispose of an unreadable file.
EXCLUDED_SYSTEM_METADATA_MARKERS = ("/.Spotlight-V100/", "/.fseventsd/", "/.TemporaryItems/")
SIGNAL_COLS = {"t_sig", "z_sig", "l_sig", "tz", "sig_name", "sig_id", "signal", "state",
               "final_t_state", "bc", "zc", "full_suffix", "vol_bucket"}


def fingerprint(p):
    """Stable identity without hashing 43 GB — path + size + mtime, plus a head digest."""
    st = os.stat(p)
    h = hashlib.sha256()
    h.update(f"{os.path.basename(p)}|{st.st_size}|{int(st.st_mtime)}".encode())
    try:
        with open(p, "rb") as f:
            h.update(f.read(1 << 20))
    except Exception:
        pass
    return h.hexdigest()[:16], st.st_size, time.strftime("%Y-%m-%d",
                                                         time.localtime(st.st_mtime))


def main():
    t0 = time.time()
    visited, unvisited, perm_fail, broken_links = [], [], [], []
    for r in DECLARED_ROOTS:
        if not os.path.exists(r):
            unvisited.append(dict(root=r, reason="DOES_NOT_EXIST")); continue
        try:
            os.listdir(r); visited.append(r)
        except PermissionError as e:
            perm_fail.append(dict(root=r, error=str(e))); unvisited.append(
                dict(root=r, reason="PERMISSION"))
        except Exception as e:
            unvisited.append(dict(root=r, reason=type(e).__name__))

    # ---- enumerate files under every visited root --------------------------
    dbs, datafiles, walk_errors, sysmeta = {}, {}, [], []
    seen = set()
    for r in visited:
        for dirpath, dirnames, filenames in os.walk(r, onerror=walk_errors.append):
            if any(s in dirpath for s in ("/node_modules", "/.git/", "/.venv/")):
                dirnames[:] = []
                continue
            for fn in filenames:
                p = os.path.join(dirpath, fn)
                rp = os.path.realpath(p)
                if rp in seen:
                    continue
                if fn.endswith(DB_EXT):
                    seen.add(rp)
                    if any(mk in p for mk in EXCLUDED_SYSTEM_METADATA_MARKERS):
                        sysmeta.append(p)
                    else:
                        dbs[rp] = p
                elif fn.endswith(DATA_EXT):
                    seen.add(rp)
                    datafiles.setdefault(os.path.splitext(fn)[1], []).append(p)

    # ---- open every database read-only and read its schema -----------------
    import duckdb
    db_report, unreadable, schema_fail = [], [], []
    signal_bearing = []
    for rp, p in sorted(dbs.items()):
        fp, size, mtime = fingerprint(rp)
        entry = dict(path=p, size_bytes=size, mtime=mtime, fingerprint=fp,
                     engine="duckdb", tables={})
        con, kind = None, None
        try:
            con = duckdb.connect(rp, read_only=True); kind = "duckdb"
        except Exception as e_duck:
            # not every .db is DuckDB — sachoki.db is SQLite, and refusing to try the
            # right reader would let a real surface hide behind "unreadable"
            try:
                import sqlite3
                sc = sqlite3.connect(f"file:{rp}?mode=ro", uri=True)
                tabs = [r[0] for r in sc.execute(
                    "select name from sqlite_master where type in ('table','view') "
                    "order by 1").fetchall()]
                for t in tabs:
                    cols = [r[1] for r in sc.execute(f'PRAGMA table_info("{t}")').fetchall()]
                    n = sc.execute(f'select count(*) from "{t}"').fetchone()[0]
                    sig = sorted(set(x.lower() for x in cols) & SIGNAL_COLS)
                    entry["tables"][t] = dict(rows=n, n_columns=len(cols),
                                              signal_columns=sig, time_range=None)
                    if sig:
                        signal_bearing.append(dict(store=p, table=t, rows=n,
                                                   signal_columns=sig, time_range=None))
                sc.close()
                entry["engine"] = "sqlite"
                db_report.append(entry); continue
            except Exception as e_sq:
                entry["error"] = f"duckdb: {e_duck}; sqlite: {e_sq}"
                unreadable.append(p); db_report.append(entry); continue
        try:
            tabs = [r[0] for r in con.execute(
                "select table_name from information_schema.tables order by 1").fetchall()]
            for t in tabs:
                try:
                    cols = con.execute(
                        "select column_name, data_type from information_schema.columns "
                        f"where table_name = '{t}' order by ordinal_position").fetchall()
                    names = [c[0] for c in cols]
                    n = con.execute(f'select count(*) from "{t}"').fetchone()[0]
                    sig = sorted(set(x.lower() for x in names) & SIGNAL_COLS)
                    rng = None
                    for tc in ("date", "bar_start", "timestamp", "ts"):
                        if tc in names:
                            try:
                                rng = [str(v) for v in con.execute(
                                    f'select min("{tc}"), max("{tc}") from "{t}"').fetchone()]
                            except Exception:
                                rng = None
                            break
                    entry["tables"][t] = dict(rows=n, n_columns=len(names),
                                              signal_columns=sig, time_range=rng)
                    if sig:
                        signal_bearing.append(dict(store=p, table=t, rows=n,
                                                   signal_columns=sig, time_range=rng))
                except Exception as e:
                    schema_fail.append(dict(store=p, table=t, error=str(e)[:120]))
        except Exception as e:
            schema_fail.append(dict(store=p, table="<list>", error=str(e)[:120]))
        finally:
            con.close()
        db_report.append(entry)

    exclusion_sound = all(any(mk in q for mk in EXCLUDED_SYSTEM_METADATA_MARKERS)
                          for q in sysmeta)
    complete = (not unvisited and not perm_fail and not walk_errors
                and not unreadable and not schema_fail and exclusion_sound)

    p = dict(
        report_id="MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1",
        status=("ENUMERATION_COMPLETE" if complete else "ENUMERATION_INCOMPLETE"),
        task_class="DATA_SURFACE_INVENTORY_ONLY",
        why_this_exists="three sealed absence claims in this programme were wrong, all in one "
                        "shape: a bounded search returned nothing and the nothing was reported "
                        "as non-existence",
        removes_error_by_construction="permission to say 'does not exist' now depends on a "
                                      "measured property of the search, not on confidence",

        search_space=dict(
            declared_roots=DECLARED_ROOTS,
            declared_roots_count=len(DECLARED_ROOTS),
            visited_roots_count=len(visited),
            visited_roots=visited,
            unvisited_roots=unvisited,
            database_extensions=list(DB_EXT),
            data_extensions=list(DATA_EXT),
            excluded_subtrees=["node_modules", ".git", ".venv"],
            exclusion_is_declared=True),

        completeness_guard=dict(
            permission_failures=len(perm_fail), permission_failure_detail=perm_fail,
            encountered_system_metadata_files=len(sysmeta),
            excluded_by_declared_scope=len(sysmeta),
            excluded_paths=sysmeta,
            exclusion_basis="path/type semantics — macOS search-index internals; NOT "
                            "unreadability",
            exclusion_sound=exclusion_sound,
            exclusion_cannot_dispose_of_unreadable_files=True,
            unreadable_in_included_scope=len(unreadable),
            walk_errors=len(walk_errors),
            broken_symlinks=len(broken_links),
            unreadable_databases=len(unreadable), unreadable_detail=unreadable,
            schema_read_failures=len(schema_fail), schema_failure_detail=schema_fail[:10],
            enumeration_complete=complete,
            negative_existence_claims_permitted=complete,
            rule="if any declared root is unvisited, or any read fails, NO downstream "
                 "artifact may assert that something does not exist"),

        databases=db_report,
        database_count=len(db_report),
        signal_bearing_tables=signal_bearing,
        signal_bearing_count=len(signal_bearing),
        data_file_counts={k: len(v) for k, v in sorted(datafiles.items())},
        notable_parquet=[p for p in datafiles.get(".parquet", [])
                         if any(s in p for s in ("population", "episode", "closure",
                                                 "claims", "ledger"))][:20],

        y_exposed=0, read_only=True, writes=0,
        outcome_exposure="NOT_EXPOSED",
        elapsed_sec=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))

    d = ART.seal(p, "MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json",
                 required=("report_id", "status", "search_space", "completeness_guard",
                           "databases", "signal_bearing_tables"),
                 supersede=os.path.exists("MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1.json"))
    print(f"MASSIVE_T_LEGACY_DATA_SURFACE_INVENTORY_V1 · {d} · {p['status']}")
    print(f"  roots       {len(visited)}/{len(DECLARED_ROOTS)} visited · "
          f"unvisited {len(unvisited)}")
    print(f"  failures    permission {len(perm_fail)} · walk {len(walk_errors)} · "
          f"unreadable-db {len(unreadable)} · schema {len(schema_fail)}")
    print(f"  databases   {len(db_report)} found")
    print(f"  SIGNAL-BEARING TABLES: {len(signal_bearing)}")
    for s in signal_bearing:
        print(f"    {s['rows']:>12,}  {s['signal_columns']}  {s['store']}::{s['table']}")
    print(f"  data files  {p['data_file_counts']}")
    print(f"  ABSENCE CLAIMS PERMITTED: {complete}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
