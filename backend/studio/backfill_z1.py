"""
backfill_z1.py — one-shot historical correction for the Z1 signal bug.

signal_engine.compute_signals had a wrong open-condition for Z1 (o <= prev_open
instead of o > prev_open), so Z1 NEVER surfaced in z_sig — across all tickers and
all history. The code is now fixed; this backfills the existing `bars` rows.

Strategy (safe & minimal):
  • recompute t_sig/z_sig per (ticker, universe) with the FIXED engine,
  • apply changes ONLY to rows where Z1 is involved (new == 'Z1' or old == 'Z1'),
    so nothing else in the DB is touched,
  • update z_sig and re-derive the sig_z* boolean flags for those rows.

Runs inside the backend process (the only one with DB access). Heavy compute is
done in pandas with no DB lock held; the write is a short final burst.
"""
from __future__ import annotations

import logging
import threading
import time

import pandas as pd

from studio.db import get_conn

log = logging.getLogger(__name__)

_Z_NAMES = ["Z1G", "Z2G", "Z1", "Z2", "Z3", "Z4", "Z5", "Z6",
            "Z7", "Z8", "Z9", "Z10", "Z11", "Z12"]

# Live job status (polled by the status endpoint)
STATUS: dict = {"running": False, "done": False, "phase": "idle",
                "scanned": 0, "updated": 0, "universes": [], "error": None,
                "started_at": None, "finished_at": None}
_LOCK = threading.Lock()


def _open_write_conn(retries: int = 30, delay: float = 1.0):
    """Open a read-write connection, retrying past the transient DuckDB
    'different configuration' race that happens if a read_only connection from a
    concurrent request is momentarily open on the same file."""
    last = None
    for _ in range(retries):
        try:
            return get_conn(read_only=False)
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(delay)
    raise RuntimeError(f"could not open write connection after {retries} tries: {last}")


def run_backfill(universes: list[str] | None = None) -> dict:
    """Recompute z_sig with the fixed engine and patch Z1-involved rows."""
    from signal_engine import compute_signals

    with _LOCK:
        if STATUS["running"]:
            return {"error": "already running"}
        STATUS.update(running=True, done=False, phase="loading", scanned=0,
                      updated=0, universes=universes or [], error=None,
                      started_at=time.time(), finished_at=None)

    try:
        # ── 1. discover universes ────────────────────────────────────────────
        rconn = get_conn(read_only=True)
        try:
            if not universes:
                universes = [r[0] for r in rconn.execute(
                    "SELECT DISTINCT universe FROM bars").fetchall()]
            STATUS["universes"] = universes
        finally:
            rconn.close()

        diffs: list[tuple[int, str]] = []   # (id, new_z_sig)
        scanned = 0

        # ── 2. per-universe recompute (caps memory) ──────────────────────────
        for uni in universes:
            STATUS["phase"] = f"computing {uni}"
            rconn = get_conn(read_only=True)
            try:
                df = rconn.execute(
                    "SELECT id, ticker, open, high, low, close, volume, "
                    "       coalesce(t_sig,'') AS t_sig, coalesce(z_sig,'') AS z_sig "
                    "FROM bars WHERE universe = ? ORDER BY ticker, date",
                    [uni]).fetchdf()
            finally:
                rconn.close()
            if df.empty:
                continue
            for col in ("open", "high", "low", "close", "volume"):
                df[col] = df[col].astype(float)

            for _tk, g in df.groupby("ticker", sort=False):
                scanned += len(g)
                sig = compute_signals(g[["open", "high", "low", "close", "volume"]]
                                      .reset_index(drop=True))
                names = sig["sig_name"].tolist()
                ids = g["id"].tolist()
                oldz = g["z_sig"].tolist()
                for k, nm in enumerate(names):
                    nz = nm if isinstance(nm, str) and nm.startswith("Z") else ""
                    oz = oldz[k] or ""
                    # ONLY Z1-involved changes — minimal, surgical blast radius
                    if nz != oz and (nz == "Z1" or oz == "Z1"):
                        diffs.append((int(ids[k]), nz))
            STATUS["scanned"] = scanned
            log.info("backfill_z1: %s scanned=%d diffs=%d", uni, scanned, len(diffs))

        # ── 3. apply (short write burst) ─────────────────────────────────────
        STATUS["phase"] = "writing"
        if diffs:
            dd = pd.DataFrame(diffs, columns=["id", "new_z"])
            wconn = _open_write_conn()
            try:
                wconn.register("z1_diffs", dd)
                wconn.execute("BEGIN TRANSACTION")
                wconn.execute(
                    "UPDATE bars SET z_sig = d.new_z FROM z1_diffs d WHERE bars.id = d.id")
                # re-derive sig_z* flags for the touched rows (match importer logic)
                for n in _Z_NAMES:
                    wconn.execute(
                        f"UPDATE bars SET sig_{n.lower()} = "
                        f"CASE WHEN z_sig = '{n}' THEN 1 ELSE 0 END "
                        f"FROM z1_diffs d WHERE bars.id = d.id")
                wconn.execute("COMMIT")
                wconn.unregister("z1_diffs")
            except Exception:
                try:
                    wconn.execute("ROLLBACK")
                except Exception:
                    pass
                raise
            finally:
                wconn.close()

        STATUS.update(updated=len(diffs), phase="done", done=True,
                      finished_at=time.time())
        log.info("backfill_z1: DONE scanned=%d updated=%d", scanned, len(diffs))
        return {"scanned": scanned, "updated": len(diffs), "universes": universes}
    except Exception as e:  # noqa: BLE001
        log.exception("backfill_z1 failed")
        STATUS["error"] = str(e)
        STATUS["phase"] = "error"
        return {"error": str(e)}
    finally:
        STATUS["running"] = False


def start_backfill_bg(universes: list[str] | None = None) -> dict:
    if STATUS["running"]:
        return {"started": False, "reason": "already running"}
    threading.Thread(target=run_backfill, args=(universes,), daemon=True).start()
    return {"started": True, "universes": universes or "all"}
