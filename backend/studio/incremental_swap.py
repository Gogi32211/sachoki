"""
studio/incremental_swap.py — zero-downtime 1D daily update via STAGING + ATOMIC SWAP.

The old path ran the delta append IN-PROCESS with a read-write connection to the LIVE
studio_analytics.duckdb, which conflicts with the backend's own read-only queries
(DuckDB "different configuration" / lock) and broke the scanner while the update ran.

New path:
  1. copy live DB → studio_analytics.staging.duckdb
  2. run the delta refresh in a SEPARATE process against the staging copy
     (STUDIO_DB_PATH=staging) — the live backend keeps serving reads uninterrupted
  3. os.replace(staging → live)  (atomic on the same filesystem)
The backend opens the analytics DB read-only per request, so the next request after the
swap transparently picks up the fresh file; in-flight reads finish on the old inode.
"""
from __future__ import annotations
import os, sys, json, time, shutil, signal, subprocess, logging
from studio.paths import ANALYTICS_DB, BACKEND_DIR

log = logging.getLogger(__name__)
_STAGING = ANALYTICS_DB + ".staging"
_TIMEOUT = int(os.environ.get("DELTA_SWAP_TIMEOUT", "18000"))   # 5h ceiling


def _rm(*paths):
    for p in paths:
        try:
            if os.path.exists(p):
                os.remove(p)
        except OSError:
            pass


def run_swap(universes: list[str], refetch_from: str | None = None) -> dict:
    t0 = time.time()
    live, staging = ANALYTICS_DB, _STAGING
    if not os.path.exists(live):
        raise FileNotFoundError(live)

    _rm(staging, staging + ".wal")
    log.info("incremental_swap: copying live → staging (%.1f GB)", os.path.getsize(live) / 1e9)
    shutil.copy2(live, staging)                       # staging is a full working copy

    env = {**os.environ, "STUDIO_DB_PATH": staging}
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "studio._delta_worker", json.dumps(universes),
             refetch_from or ""],
            cwd=BACKEND_DIR, env=env, capture_output=True, text=True, timeout=_TIMEOUT)
    except subprocess.TimeoutExpired:
        _rm(staging, staging + ".wal")
        raise RuntimeError(f"delta worker timed out after {_TIMEOUT}s")

    if proc.returncode != 0:
        _rm(staging, staging + ".wal")
        # 2026-07-29: the returncode used to be dropped, so a worker killed by a SIGNAL was
        # indistinguishable from one that raised — both surfaced as "delta worker failed"
        # with whatever stderr happened to be flushed (for a signal: no traceback at all).
        # Negative returncode == -N means killed by signal N (-9 SIGKILL, -15 SIGTERM).
        rc = proc.returncode
        how = f"killed by signal {-rc} ({signal.Signals(-rc).name})" if rc < 0 else f"exit code {rc}"
        raise RuntimeError(f"delta worker failed: {how}\n"
                           f"--- stderr (tail) ---\n{(proc.stderr or '(empty)')[-3000:]}\n"
                           f"--- stdout (tail) ---\n{(proc.stdout or '(empty)')[-1500:]}")

    # parse the worker's result payload
    res = {}
    for line in (proc.stdout or "").splitlines():
        if line.startswith("RESULT_JSON:"):
            res = json.loads(line[len("RESULT_JSON:"):])
            break

    if os.path.exists(staging + ".wal"):
        # worker didn't fully checkpoint — do NOT swap a DB that needs a separate WAL
        _rm(staging, staging + ".wal")
        raise RuntimeError("staging left a .wal after checkpoint — aborting swap (no data lost; live DB untouched)")

    os.replace(staging, live)                         # atomic swap
    res["_swap"] = {"ok": True, "seconds": round(time.time() - t0, 1),
                    "live": live}
    log.info("incremental_swap: swapped in fresh DB in %.1fs", time.time() - t0)
    return res


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--universes", default="sp500,nasdaq,russell2k")
    a = ap.parse_args()
    print(json.dumps(run_swap([u.strip() for u in a.universes.split(",") if u.strip()]), indent=2)[:1500])
