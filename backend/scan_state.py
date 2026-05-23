"""Per-(universe, tf) last-scan-date tracker for incremental scans.

Stored in the `scan_state` table (created on first use). Used by the
"scan today only" path to decide whether incremental is safe, and by
the UI to show "last scan: 2026-05-23 04:36" hints.
"""
from __future__ import annotations
import logging
from typing import Optional
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def _ensure_table(con) -> None:
    try:
        from db import USE_PG
    except Exception:
        USE_PG = False
    try:
        if USE_PG:
            con.execute("""
                CREATE TABLE IF NOT EXISTS scan_state (
                    universe       TEXT NOT NULL,
                    tf             TEXT NOT NULL,
                    nasdaq_batch   TEXT NOT NULL DEFAULT '',
                    last_scan_date TEXT,
                    last_scan_at   TEXT,
                    mode           TEXT,
                    notes          TEXT,
                    PRIMARY KEY (universe, tf, nasdaq_batch)
                )
            """)
        else:
            con.execute("""
                CREATE TABLE IF NOT EXISTS scan_state (
                    universe       TEXT NOT NULL,
                    tf             TEXT NOT NULL,
                    nasdaq_batch   TEXT NOT NULL DEFAULT '',
                    last_scan_date TEXT,
                    last_scan_at   TEXT,
                    mode           TEXT,
                    notes          TEXT,
                    PRIMARY KEY (universe, tf, nasdaq_batch)
                )
            """)
        try:
            con.commit()
        except Exception:
            pass
    except Exception as e:
        log.warning("_ensure_table(scan_state) failed: %s", e)


def get_last_scan(universe: str, tf: str, nasdaq_batch: str = "") -> Optional[dict]:
    """Return {last_scan_date, last_scan_at, mode} or None if never scanned."""
    try:
        from db import get_db
    except Exception:
        return None
    with get_db() as con:
        _ensure_table(con)
        row = con.execute(
            "SELECT last_scan_date, last_scan_at, mode, notes "
            "FROM scan_state WHERE universe=? AND tf=? AND nasdaq_batch=?",
            (universe, tf, nasdaq_batch or ""),
        ).fetchone()
        if not row:
            return None
        return {
            "universe": universe, "tf": tf,
            "nasdaq_batch": nasdaq_batch or "",
            "last_scan_date": row[0],
            "last_scan_at":   row[1],
            "mode":           row[2],
            "notes":          row[3],
        }


def set_last_scan(
    universe: str, tf: str,
    last_scan_date: str,
    mode: str = "full",
    nasdaq_batch: str = "",
    notes: str = "",
) -> None:
    try:
        from db import get_db, USE_PG
    except Exception:
        return
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with get_db() as con:
        _ensure_table(con)
        try:
            con.execute(
                "DELETE FROM scan_state WHERE universe=? AND tf=? AND nasdaq_batch=?",
                (universe, tf, nasdaq_batch or ""),
            )
            con.execute(
                "INSERT INTO scan_state (universe, tf, nasdaq_batch, "
                "last_scan_date, last_scan_at, mode, notes) VALUES (?,?,?,?,?,?,?)",
                (universe, tf, nasdaq_batch or "", last_scan_date, now, mode, notes),
            )
            try:
                con.commit()
            except Exception:
                pass
        except Exception as e:
            log.warning("set_last_scan failed: %s", e)


def list_all() -> list:
    """Return all (universe, tf, batch) last-scan rows for the admin UI."""
    try:
        from db import get_db
    except Exception:
        return []
    out = []
    with get_db() as con:
        _ensure_table(con)
        try:
            for row in con.execute(
                "SELECT universe, tf, nasdaq_batch, last_scan_date, last_scan_at, mode, notes "
                "FROM scan_state ORDER BY universe, tf, nasdaq_batch"
            ).fetchall():
                out.append({
                    "universe": row[0], "tf": row[1],
                    "nasdaq_batch": row[2],
                    "last_scan_date": row[3], "last_scan_at": row[4],
                    "mode": row[5], "notes": row[6],
                })
        except Exception as e:
            log.warning("scan_state.list_all failed: %s", e)
    return out
