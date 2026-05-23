"""DB pruner — delete rows older than N days from time-series tables.

Time-series tables grow without bound. This module exposes a registry of
prunable tables (name → timestamp column) plus two operations:

  list_db_stats()  → per-table row count + date range + size (best effort)
  prune_table()    → DELETE WHERE <ts_col> < (now - N days), with dry-run

Protected tables (user-authored data: chart_observations, paper_portfolio,
paper_portfolio_p_only, paper_portfolio_w_only) require an explicit
`allow_protected=True` flag.

Audited tables and time columns come from the 260523 audit:
  - turbo_engine.py        → turbo_scan_runs, turbo_scan_results
  - ultra_scan_migration   → ultra_scan_runs, ultra_scan_candidates
  - signal_replay_migration → signal_replay_runs, replay_artifacts
  - ultra_pump_migration   → ultra_pump_runs, ultra_research_episodes,
                              ultra_research_patterns
  - chart_obs_migration    → chart_observations (PROTECTED), stock_stat
  - paper_portfolio_migration → paper_portfolio (PROTECTED),
                                 paper_daily_prices
  - pooled_stats.py        → not time-series (rebuilt on demand) — SKIPPED
"""
from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)


# (table_name, timestamp_column, is_date_only, protected)
# is_date_only=True  → column is "YYYY-MM-DD" text/DATE
# is_date_only=False → column is full timestamp (epoch-ms or ISO)
PRUNABLE_TABLES: List[Dict[str, Any]] = [
    {"table": "turbo_scan_runs",         "ts_col": "started_at",    "date_only": False, "protected": False, "desc": "Turbo scan run metadata"},
    {"table": "turbo_scan_results",      "ts_col": "scanned_at",    "date_only": False, "protected": False, "desc": "Per-ticker turbo scan output"},
    {"table": "ultra_scan_runs",         "ts_col": "started_at",    "date_only": False, "protected": False, "desc": "ULTRA scan run metadata"},
    {"table": "ultra_scan_candidates",   "ts_col": "created_at",    "date_only": False, "protected": False, "desc": "ULTRA scan per-ticker rows"},
    {"table": "signal_replay_runs",      "ts_col": "started_at",    "date_only": False, "protected": False, "desc": "Replay run metadata"},
    {"table": "replay_artifacts",        "ts_col": "created_at",    "date_only": False, "protected": False, "desc": "Replay artifact registry"},
    {"table": "ultra_pump_runs",         "ts_col": "started_at",    "date_only": False, "protected": False, "desc": "Pump research run metadata"},
    {"table": "ultra_research_episodes", "ts_col": "created_at",    "date_only": False, "protected": False, "desc": "Pump research episodes"},
    {"table": "ultra_research_patterns", "ts_col": "created_at",    "date_only": False, "protected": False, "desc": "Pump research pattern stats"},
    {"table": "stock_stat",              "ts_col": "date",          "date_only": True,  "protected": False, "desc": "Per-bar signal stats"},
    {"table": "paper_daily_prices",      "ts_col": "price_date",    "date_only": True,  "protected": False, "desc": "Paper-trading OHLCV cache"},
    {"table": "chart_observations",      "ts_col": "obs_date",      "date_only": True,  "protected": True,  "desc": "User chart observations"},
    {"table": "paper_portfolio",         "ts_col": "signal_date",   "date_only": True,  "protected": True,  "desc": "Paper portfolio trades"},
]


def _table_exists(con, name: str) -> bool:
    """Check if a table exists (SQLite or Postgres)."""
    try:
        from db import USE_PG
    except Exception:
        USE_PG = False
    try:
        if USE_PG:
            row = con.execute(
                "SELECT EXISTS (SELECT FROM information_schema.tables "
                "WHERE table_name = %s)", (name,)
            ).fetchone()
            return bool(row[0] if row else False)
        else:
            row = con.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name=?", (name,)
            ).fetchone()
            return row is not None
    except Exception as e:
        log.warning("_table_exists(%s) failed: %s", name, e)
        return False


def _cutoff_value(older_than_days: int, date_only: bool):
    """Build the comparator value for the WHERE clause."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(older_than_days)))
    if date_only:
        return cutoff.strftime("%Y-%m-%d")
    return cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")


def list_db_stats() -> List[Dict[str, Any]]:
    """For each prunable table: rows, oldest_date, newest_date, exists.

    Best-effort — tables that don't exist or queries that fail are
    reported with `exists=False` / `error=str(exc)` and zero counts.
    """
    out: List[Dict[str, Any]] = []
    try:
        from db import get_db
    except Exception as e:
        log.warning("list_db_stats: cannot import db: %s", e)
        return [{"error": f"db import failed: {e}"}]

    with get_db() as con:
        for entry in PRUNABLE_TABLES:
            t       = entry["table"]
            ts_col  = entry["ts_col"]
            stat    = {**entry, "exists": False, "rows": 0,
                       "oldest": None, "newest": None}
            try:
                if not _table_exists(con, t):
                    out.append(stat)
                    continue
                stat["exists"] = True
                row = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()
                stat["rows"] = int(row[0] if row else 0)
                if stat["rows"] > 0:
                    row = con.execute(
                        f"SELECT MIN({ts_col}), MAX({ts_col}) FROM {t}"
                    ).fetchone()
                    if row:
                        stat["oldest"] = str(row[0]) if row[0] is not None else None
                        stat["newest"] = str(row[1]) if row[1] is not None else None
            except Exception as exc:
                stat["error"] = str(exc)[:200]
            out.append(stat)
    return out


def prune_table(
    table: str,
    older_than_days: int = 30,
    dry_run: bool = True,
    allow_protected: bool = False,
) -> Dict[str, Any]:
    """Delete rows from `table` older than `older_than_days` days.

    Args:
        table:            must be in PRUNABLE_TABLES
        older_than_days:  >=1
        dry_run:          if True, only COUNT rows that would be deleted
        allow_protected:  protected tables (chart_observations, paper_portfolio)
                          require this flag = True
    """
    entry = next((e for e in PRUNABLE_TABLES if e["table"] == table), None)
    if entry is None:
        return {"ok": False, "error": f"Unknown table '{table}'. "
                f"Allowed: {[e['table'] for e in PRUNABLE_TABLES]}"}
    if entry["protected"] and not allow_protected:
        return {"ok": False, "error": f"Table '{table}' is PROTECTED "
                f"(stores user-authored data). Pass allow_protected=true "
                f"to override."}
    if older_than_days < 1:
        return {"ok": False, "error": "older_than_days must be >= 1"}

    try:
        from db import get_db
    except Exception as e:
        return {"ok": False, "error": f"db import failed: {e}"}

    ts_col   = entry["ts_col"]
    cutoff   = _cutoff_value(older_than_days, entry["date_only"])

    with get_db() as con:
        if not _table_exists(con, table):
            return {"ok": False, "error": f"Table '{table}' does not exist"}
        try:
            row = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {ts_col} < ?",
                (cutoff,),
            ).fetchone()
            n_to_delete = int(row[0] if row else 0)
        except Exception as exc:
            return {"ok": False, "error": f"count failed: {exc}"}

        if dry_run:
            return {
                "ok": True, "dry_run": True, "table": table,
                "ts_col": ts_col, "cutoff": cutoff,
                "would_delete": n_to_delete,
            }

        try:
            con.execute(f"DELETE FROM {table} WHERE {ts_col} < ?", (cutoff,))
            try:
                con.commit()
            except Exception:
                pass
        except Exception as exc:
            return {"ok": False, "error": f"delete failed: {exc}"}

    log.info("db_pruner: deleted %d rows from %s older than %s",
             n_to_delete, table, cutoff)
    return {
        "ok": True, "dry_run": False, "table": table,
        "ts_col": ts_col, "cutoff": cutoff,
        "deleted": n_to_delete,
    }


def prune_all(
    older_than_days: int = 30,
    dry_run: bool = True,
    include_protected: bool = False,
) -> Dict[str, Any]:
    """Bulk prune every non-protected table (or all when include_protected=True)."""
    results: List[Dict[str, Any]] = []
    total = 0
    for entry in PRUNABLE_TABLES:
        if entry["protected"] and not include_protected:
            results.append({"table": entry["table"], "skipped": "protected"})
            continue
        r = prune_table(entry["table"], older_than_days=older_than_days,
                        dry_run=dry_run, allow_protected=include_protected)
        results.append(r)
        if r.get("ok"):
            total += r.get("would_delete", 0) + r.get("deleted", 0)
    return {
        "ok": True, "dry_run": dry_run,
        "older_than_days": older_than_days,
        "include_protected": include_protected,
        "total_affected": total,
        "per_table": results,
    }
