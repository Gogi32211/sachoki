"""
backfill_intraday_fwd.py — compute forward-return labels (fwd/mfe/mae + hit/drop)
for an intraday DB (studio_1h.duckdb / studio_30m.duckdb).

The intraday builder runs the per-bar enricher but NOT the forward-return backfill,
so fwd_*/mfe_*/mae_* are all NULL — which leaves the Studio sequence-stats empty.

For intraday, "fwd_Nd" means N BARS ahead (e.g. on 1H, fwd_5d = 5 hourly bars ≈ a
few hours later), computed per ticker in chronological (timestamp) order. This is the
right unit for comparing a signal SEQUENCE's forward edge across timeframes.

Usage:  uv run python backfill_intraday_fwd.py [1h|30m]   (default 1h)
Idempotent: only fills rows whose fwd_5d IS NULL.
"""
import os, sys, time, duckdb

_FWD = [1, 3, 5, 10, 20, 30, 60, 90]
_MFE = [5, 10, 20, 30, 60]
_MAE = [5, 10, 20, 30]
_HIT = [
    ("hit_5pct_5d", "mfe_5d  >= 5"), ("hit_10pct_5d", "mfe_5d  >= 10"),
    ("hit_20pct_5d", "mfe_5d  >= 20"), ("hit_30pct_10d", "mfe_10d >= 30"),
    ("hit_50pct_20d", "mfe_20d >= 50"), ("hit_2x_60d", "mfe_60d >= 100"),
    ("drop_10pct_5d", "mae_5d  <= -10"), ("drop_20pct_10d", "mae_10d <= -20"),
    ("drop_30pct_20d", "mae_20d <= -30"),
]


def _select_exprs():
    parts = ["id"]
    for n in _FWD:
        parts.append(f"(LEAD(close,{n}) OVER w / close - 1)*100 AS fwd_{n}d")
    for n in _MFE:
        parts.append(f"(MAX(high) OVER (PARTITION BY ticker,universe ORDER BY date "
                     f"ROWS BETWEEN 1 FOLLOWING AND {n} FOLLOWING)/close - 1)*100 AS mfe_{n}d")
    for n in _MAE:
        parts.append(f"(MIN(low) OVER (PARTITION BY ticker,universe ORDER BY date "
                     f"ROWS BETWEEN 1 FOLLOWING AND {n} FOLLOWING)/close - 1)*100 AS mae_{n}d")
    return ",\n        ".join(parts)


def main(tf="1h"):
    db = os.path.expanduser(f"~/Downloads/studio_{tf}.duckdb")
    if not os.path.exists(db):
        print(f"no DB at {db}"); return
    con = duckdb.connect(db)
    con.execute("PRAGMA threads=8")
    avail = set(r[0] for r in con.execute("DESCRIBE bars").fetchall())
    todo = con.execute("SELECT count(*) FROM bars WHERE fwd_5d IS NULL").fetchone()[0]
    tot = con.execute("SELECT count(*) FROM bars").fetchone()[0]
    print(f"{tf}: {todo:,}/{tot:,} rows need fwd labels")
    if todo == 0:
        con.close(); print("nothing to do."); return

    sets = [f"fwd_{n}d=c.fwd_{n}d" for n in _FWD if f"fwd_{n}d" in avail]
    sets += [f"mfe_{n}d=c.mfe_{n}d" for n in _MFE if f"mfe_{n}d" in avail]
    sets += [f"mae_{n}d=c.mae_{n}d" for n in _MAE if f"mae_{n}d" in avail]
    t0 = time.time()
    print("computing window labels + UPDATE (this is the heavy step)…", flush=True)
    con.execute(f"""
        UPDATE bars AS b SET {', '.join(sets)}
        FROM (SELECT {_select_exprs()} FROM bars
              WINDOW w AS (PARTITION BY ticker,universe ORDER BY date)) AS c
        WHERE b.id = c.id AND b.fwd_5d IS NULL
    """)
    print(f"  fwd/mfe/mae done in {(time.time()-t0)/60:.1f}min", flush=True)
    for col, cond in _HIT:
        if col in avail:
            src = cond.split()[0]
            con.execute(f"UPDATE bars SET {col}=({cond}) WHERE {src} IS NOT NULL AND {col} IS NULL")
    con.execute("CHECKPOINT")
    filled = con.execute("SELECT count(fwd_5d) FROM bars").fetchone()[0]
    print(f"{tf}: fwd_5d now filled on {filled:,}/{tot:,} rows | {(time.time()-t0)/60:.1f}min total")
    con.close()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "1h")
