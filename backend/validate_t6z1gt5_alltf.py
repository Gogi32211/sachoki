"""T6→Z1G→T5 across all timeframes, per year. Match via SQL LAG, then trail25
path-sim (entry next-open, native maxh=60 bars) on the matched tickers only."""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import db_path, ANALYTICS_DB
SLIP = 0.0015
TFS = [("1w", "studio_1w.duckdb", 2_000_000), ("1d", ANALYTICS_DB, 2_000_000),
       ("4h", "studio_4h.duckdb", 500_000), ("1h", "studio_1h.duckdb", 200_000),
       ("15m", "studio_15m.duckdb", 50_000)]


def run(tf, dbname, dvfloor):
    path = dbname if dbname == ANALYTICS_DB else db_path(dbname)
    con = duckdb.connect(path, read_only=True)
    try:
        m = con.execute("""
            WITH u AS (SELECT ticker,date,close,close*volume dv,
                         CASE WHEN coalesce(t_sig,'')<>'' THEN t_sig
                              WHEN coalesce(z_sig,'')<>'' THEN z_sig ELSE '·' END tz
                       FROM (SELECT *,row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                             FROM bars) WHERE rn=1),
            s AS (SELECT *, lag(tz,1) OVER (PARTITION BY ticker ORDER BY date) tz1,
                            lag(tz,2) OVER (PARTITION BY ticker ORDER BY date) tz2 FROM u)
            SELECT ticker, CAST(date AS VARCHAR) d FROM s
            WHERE tz='T5' AND tz1='Z1G' AND tz2='T6' AND close>=3 AND dv>=?
            ORDER BY ticker, date
        """, [dvfloor]).fetchdf()
        if len(m) == 0:
            print(f"{tf:4s}  no matches"); return
        tks = tuple(sorted(m.ticker.unique()))
        ph = ",".join("?" * len(tks))
        bars = con.execute(f"""SELECT ticker, CAST(date AS VARCHAR) d, open,high,low,close FROM (
            SELECT *,row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
            WHERE rn=1 AND ticker IN ({ph}) ORDER BY ticker,date""", list(tks)).fetchdf()
    finally:
        con.close()
    idxmap = {}
    for tk, g in bars.groupby("ticker", sort=False):
        idxmap[tk] = (g["open"].to_numpy(float), g["high"].to_numpy(float),
                      g["low"].to_numpy(float), g["close"].to_numpy(float),
                      {d[:10]: i for i, d in enumerate(g["d"].to_numpy())})
    recs = []
    for _, r in m.iterrows():
        tk = r.ticker; day = r.d[:10]
        if tk not in idxmap:
            continue
        o, h, l, c, dm = idxmap[tk]
        i = dm.get(day)
        if i is None or i + 1 >= len(c):
            continue
        ep = o[i + 1] * (1 + SLIP)
        if ep <= 0:
            continue
        pk = ep; end = i + 1; ret = None
        for j in range(i + 1, min(i + 61, len(c))):
            end = j; tsl = pk * 0.75
            if j > i + 1 and o[j] <= tsl:
                ret = o[j] / ep - 1 - SLIP; break
            pk = max(pk, h[j]); ts = pk * 0.75
            if l[j] <= ts:
                ret = ts / ep - 1 - SLIP; break
        if ret is None:
            ret = c[end] / ep - 1 - SLIP
        recs.append((day[:4], ret * 100))
    R = pd.DataFrame(recs, columns=["yr", "ret"])
    tot = f"n={len(R)} mean {R.ret.mean():+.2f} med {R.ret.median():+.2f} win {(R.ret>0).mean()*100:.0f}%"
    print(f"\n### {tf.upper():4s}  {tot}")
    for y, g in R.groupby("yr"):
        print(f"    {y}: n={len(g):5d}  mean {g.ret.mean():+7.2f}  med {g.ret.median():+7.2f}  win {(g.ret>0).mean()*100:3.0f}%")


def main():
    t0 = time.time()
    print("T6→Z1G→T5 · trail25 · per-year × timeframe")
    for tf, db, dv in TFS:
        try:
            run(tf, db, dv)
        except Exception as e:
            print(f"{tf}: ERR {e}")
        print(f"  [{time.time()-t0:.0f}s]")


if __name__ == "__main__":
    main()
