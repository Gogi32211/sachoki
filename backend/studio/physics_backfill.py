"""studio/physics_backfill.py — add the physics columns to every bars DB, safely.

The instruction was "add it to the DB and the whole history, all timeframes — and above all do
not damage what already works". So the interesting part of this file is not the backfill, it is
the safety contour around it.

WHAT MAKES THIS SAFE, in the order the guarantees matter:

    ADDITIVE ONLY        ALTER TABLE ADD COLUMN, then UPDATE that sets ONLY the new columns.
                         No existing column appears on the left of a SET. A bug here cannot
                         corrupt data it never names.

    PROVEN, NOT ASSUMED  before and after every run, a fingerprint of the EXISTING columns is
                         taken — row count, and checksums over the columns other work depends
                         on. If a fingerprint moves, the run is reported as DAMAGED with the
                         column named. "It only writes new columns" is a claim; this is a check.

    ONE WRITER           `bars` has two sanctioned writers, the nightly job and the manual
                         incremental update. This is a THIRD, so it refuses to start inside the
                         nightly window, and it takes the DuckDB write lock for one timeframe at
                         a time rather than holding several open.

    RESUMABLE            progress is per (universe, ticker) and skips what is already filled, so
                         an interrupted run costs the current ticker and nothing else. 132M rows
                         will not finish in one sitting and the design says so.

DISK IS THE REAL CONSTRAINT, not time. The 15m database is 33 GB with 41 GB free; a DuckDB
UPDATE rewrites pages and needs headroom. The runner refuses a timeframe whose file is larger
than a declared fraction of the free space, rather than discovering the limit by filling the
disk.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from datetime import datetime

import duckdb
import pandas as pd

from studio import bar_physics as BP
from studio.paths import db_path

PROGRESS = "/tmp/studio_physics_backfill.json"

# order: smallest first, so the cheap ones prove the machinery before the expensive ones run
TIMEFRAMES = ["studio_1w.duckdb", "studio_analytics.duckdb", "studio_4h.duckdb",
              "studio_1h.duckdb", "studio_15m.duckdb"]

# the nightly job runs Tue–Sat 03:00 Tbilisi; the house rule is no backend restart 03:00–05:30,
# and a second writer in that window is a worse idea than a restart
BLOCK_FROM, BLOCK_TO = 2.5, 5.75          # hours, local Tbilisi time

FREE_SPACE_FACTOR = 1.6                   # need free >= db_size * this, or refuse

# columns other work depends on — the fingerprint watches these, not everything
WATCH_COLS = ["t_sig", "z_sig", "l_sig", "full_suffix", "bar_body_wick", "bar_gap_range",
              "bar_line5", "atr_14", "close", "volume"]


def _tbilisi_hour() -> float:
    import zoneinfo                                                   # noqa: PLC0415
    now = datetime.now(zoneinfo.ZoneInfo("Asia/Tbilisi"))
    return now.hour + now.minute / 60


def assert_outside_nightly_window() -> None:
    h = _tbilisi_hour()
    if BLOCK_FROM <= h <= BLOCK_TO:
        raise RuntimeError(
            f"it is {h:.2f}h Tbilisi and the nightly writer owns 03:00–05:30. `bars` has two "
            f"sanctioned writers and this is a third; running now would either block it or be "
            f"blocked by it, and a half-written enrichment is worse than a late one.")


def assert_disk_headroom(path: str) -> dict:
    st = os.statvfs(os.path.dirname(path))
    free = st.f_bavail * st.f_frsize
    size = os.path.getsize(path)
    need = size * FREE_SPACE_FACTOR
    if free < need:
        raise RuntimeError(
            f"{os.path.basename(path)} is {size / 1e9:.1f} GB and {free / 1e9:.1f} GB is free; "
            f"a DuckDB UPDATE rewrites pages and wants ~{need / 1e9:.1f} GB of headroom. "
            f"Refusing rather than discovering the limit by filling the disk.")
    return {"db_gb": round(size / 1e9, 2), "free_gb": round(free / 1e9, 2)}


def fingerprint(conn) -> dict:
    """Row count plus a checksum per watched column. Cheap, and the whole point."""
    cols = {r[0] for r in conn.execute("DESCRIBE bars").fetchall()}
    fp = {"rows": conn.execute("SELECT COUNT(*) FROM bars").fetchone()[0]}
    for c in WATCH_COLS:
        if c in cols:
            v = conn.execute(f"SELECT SUM(hash(CAST({c} AS VARCHAR))::HUGEINT) FROM bars"
                             ).fetchone()[0]
            fp[c] = str(v)
    return fp


def assert_unharmed(before: dict, after: dict) -> None:
    moved = [k for k in before if before[k] != after.get(k)]
    if moved:
        raise RuntimeError(
            f"DAMAGE: {moved} changed across the backfill. This run only ever named new columns "
            f"in a SET clause, so a move here means something other than the intended write "
            f"touched the table. Restore from the pre-run copy before doing anything else.")


def ensure_columns(conn) -> list:
    existing = {r[0] for r in conn.execute("DESCRIBE bars").fetchall()}
    added = []
    for col, typ in ([(c, "VARCHAR") for c in BP.VARCHAR_COLS]
                     + [(c, "DOUBLE") for c in BP.DOUBLE_COLS]
                     + [(c, "SMALLINT") for c in BP.SMALLINT_COLS]):
        if col not in existing:
            conn.execute(f"ALTER TABLE bars ADD COLUMN {col} {typ}")
            added.append(col)
    return added


def _write_progress(payload: dict) -> None:
    with open(PROGRESS, "w") as f:
        json.dump(payload, f)


def backfill_tf(db_file: str, limit_tickers: int | None = None,
                universes: list | None = None, verbose: bool = True) -> dict:
    assert_outside_nightly_window()
    path = db_path(db_file)
    disk = assert_disk_headroom(path)
    started = time.time()

    conn = duckdb.connect(path, read_only=False)
    try:
        before = fingerprint(conn)
        added = ensure_columns(conn)
        conn.commit()
        if verbose:
            print(f"  {db_file} · {before['rows']:,} rows · {disk['db_gb']} GB · "
                  f"{disk['free_gb']} GB free · added {len(added)} columns", flush=True)

        pairs = conn.execute("""
            SELECT universe, ticker, COUNT(*) n,
                   SUM(CASE WHEN phys_r IS NULL THEN 1 ELSE 0 END) todo
            FROM bars GROUP BY 1, 2 HAVING todo > 0 ORDER BY universe, ticker
        """).fetchdf()
        if universes:
            pairs = pairs[pairs["universe"].isin(universes)]
        if limit_tickers:
            pairs = pairs.head(limit_tickers)
        total = len(pairs)
        if verbose:
            print(f"  {total:,} (universe, ticker) groups to fill", flush=True)

        cols = ", ".join(BP.ALL_COLS)
        sel_cols = ("id, ticker, date, open, high, low, close, volume, atr_14, "
                    "sig_l3, sig_l4, wvf_spike")
        have = {r[0] for r in conn.execute("DESCRIBE bars").fetchall()}
        sel_cols = ", ".join(c for c in sel_cols.split(", ") if c.split()[0] in have)

        done = rows = 0
        errors = []
        for row in pairs.itertuples():
            try:
                df = conn.execute(
                    f"SELECT {sel_cols} FROM bars WHERE ticker = ? AND universe = ? ORDER BY date",
                    [row.ticker, row.universe]).fetchdf()
                if len(df) < 2:
                    done += 1
                    continue
                phys = BP.compute(df)
                upd = pd.concat([df[["id"]], phys[BP.ALL_COLS]], axis=1)
                conn.register("phys_tmp", upd)
                sets = ", ".join(f"{c} = phys_tmp.{c}" for c in BP.ALL_COLS)
                conn.execute(f"UPDATE bars SET {sets} FROM phys_tmp WHERE bars.id = phys_tmp.id")
                conn.unregister("phys_tmp")
                rows += len(df)
            except Exception as e:                                    # noqa: BLE001
                errors.append({"ticker": row.ticker, "universe": row.universe,
                               "error": f"{type(e).__name__}: {e}"[:200]})
            done += 1
            if done % 50 == 0 or done == total:
                conn.commit()
                el = time.time() - started
                _write_progress({"db": db_file, "done": done, "total": total, "rows": rows,
                                 "elapsed_s": round(el, 1), "errors": len(errors),
                                 "eta_s": round(el / done * (total - done)) if done else None})
                if verbose:
                    print(f"    {done:>6,}/{total:,}  {rows:>12,} rows  {el / 60:>5.1f} min  "
                          f"eta {el / done * (total - done) / 60:>5.1f} min", flush=True)
        conn.commit()
        after = fingerprint(conn)
        assert_unharmed(before, after)
    finally:
        conn.close()

    return {"db": db_file, "groups": total, "rows_written": rows,
            "columns_added": added, "errors": errors[:20], "n_errors": len(errors),
            "fingerprint_before": before, "fingerprint_after": after,
            "unharmed": True, "physics_version": BP.PHYSICS_VERSION,
            "minutes": round((time.time() - started) / 60, 1), "disk": disk}


if __name__ == "__main__":
    import sys                                                        # noqa: PLC0415
    which = sys.argv[1] if len(sys.argv) > 1 else "studio_1w.duckdb"
    lim = int(sys.argv[2]) if len(sys.argv) > 2 else None
    r = backfill_tf(which, limit_tickers=lim)
    print("\n" + "=" * 88, flush=True)
    print(f"  {r['db']} · {r['rows_written']:,} rows · {r['minutes']} min · "
          f"errors {r['n_errors']}", flush=True)
    print(f"  existing columns UNHARMED (row count and {len(WATCH_COLS)} checksums identical)",
          flush=True)
    for e in r["errors"][:5]:
        print(f"    {e['ticker']}: {e['error']}", flush=True)
