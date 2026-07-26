"""tz5yr_engine2.py — RICH per-signal stats matching the old per-signal docs in
full (Avg 1/3/5/10d, median, win, big_win>10%, fail<-5%, MFE/MAE/RR), plus
composites+sequences with avg/fail, prev1, L/vol/suffix, MA50-reclaim, price bucket.
3 universes, 5-year history. Outputs to /tmp/tz5yr/data/. ANALYSIS ONLY."""
import duckdb, os
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"; D="/tmp/tz5yr/data/"; os.makedirs(D,exist_ok=True)
con=duckdb.connect(DB,read_only=True); con.execute("PRAGMA threads=6")
TS=['T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12']
ZS=['Z1','Z1G','Z2','Z2G','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11','Z12']
SIG="('"+"','".join(TS+ZS)+"')"

print("build rich base (window: lags + sma50 + reclaim + price bucket)...",flush=True)
con.execute("""CREATE TEMP TABLE b AS
WITH s AS (
  SELECT universe,ticker,date,year(date) yr, close,
    coalesce(nullif(t_sig,''),nullif(z_sig,''),'∅') code,
    l_sig,full_suffix,close_suffix,vol_bucket,
    fwd_1d,fwd_3d,fwd_5d,fwd_10d,fwd_20d,mfe_10d,mae_10d,
    avg(close) OVER (PARTITION BY universe,ticker ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) sma50,
    row_number() OVER (PARTITION BY universe,ticker,date ORDER BY date) rn
  FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500),
t AS (
  SELECT *,
    lag(code,1) OVER w p1, lag(code,2) OVER w p2, lag(code,3) OVER w p3,
    lag(close,1) OVER w pc, lag(sma50,1) OVER w psma
  FROM s WHERE rn=1 WINDOW w AS (PARTITION BY universe,ticker ORDER BY date))
SELECT *,
  CASE WHEN close>sma50 AND pc<=psma THEN 'MA50 reclaim' WHEN close>sma50 THEN 'above MA50' ELSE 'below MA50' END reclaim,
  CASE WHEN close<5 THEN '1_5' WHEN close<20 THEN '5_20' WHEN close<50 THEN '20_50'
       WHEN close<150 THEN '50_150' WHEN close<300 THEN '150_300' ELSE '300_PLUS' END pbucket
FROM t""")
print("  base rows:",con.execute("SELECT count(*) FROM b").fetchone()[0],flush=True)

M2="""count(*) n,
 round(avg(fwd_1d),3) a1, round(avg(fwd_3d),3) a3, round(avg(fwd_5d),3) a5, round(avg(fwd_10d),3) a10,
 round(median(fwd_5d),3) m5, round(median(fwd_10d),3) m10, round(median(fwd_20d),3) m20,
 round(100.0*avg(CASE WHEN fwd_10d>0 THEN 1 ELSE 0 END),1) win,
 round(100.0*avg(CASE WHEN fwd_10d>=10 THEN 1 ELSE 0 END),1) bigwin,
 round(100.0*avg(CASE WHEN fwd_10d<=-5 THEN 1 ELSE 0 END),1) fail,
 round(avg(mfe_10d),3) mfe, round(avg(mae_10d),3) mae"""

def dump(name,sql):
    con.execute(f"COPY ({sql}) TO '{D}{name}.csv' (HEADER)")
    print("  +",name,flush=True)

dump("rich_baseline", f"SELECT universe, code signal, {M2} FROM b WHERE code IN {SIG} GROUP BY 1,2")
dump("rich_composite", f"""SELECT universe, code signal, code||l_sig||full_suffix composite, {M2} FROM b
  WHERE code IN {SIG} AND l_sig IS NOT NULL AND full_suffix IS NOT NULL
  GROUP BY 1,2,3 HAVING count(*)>=40""")
dump("rich_sequence", f"""SELECT universe, code signal, p3||'|'||p2||'|'||p1 seq3, {M2} FROM b
  WHERE code IN {SIG} AND p1 IS NOT NULL AND p2 IS NOT NULL AND p3 IS NOT NULL
  GROUP BY 1,2,3 HAVING count(*)>=20""")
dump("rich_prev1", f"SELECT universe, code signal, p1 dim, {M2} FROM b WHERE code IN {SIG} AND p1 IS NOT NULL GROUP BY 1,2,3 HAVING count(*)>=30")
dump("rich_line5", f"SELECT universe, code signal, l_sig dim, {M2} FROM b WHERE code IN {SIG} AND l_sig IS NOT NULL AND CAST(l_sig AS VARCHAR)<>'' GROUP BY 1,2,3 HAVING count(*)>=30")
dump("rich_vol", f"SELECT universe, code signal, vol_bucket dim, {M2} FROM b WHERE code IN {SIG} AND vol_bucket IS NOT NULL GROUP BY 1,2,3 HAVING count(*)>=30")
dump("rich_suffix", f"SELECT universe, code signal, full_suffix dim, {M2} FROM b WHERE code IN {SIG} AND full_suffix IS NOT NULL GROUP BY 1,2,3 HAVING count(*)>=30")
dump("rich_aio", f"SELECT universe, code signal, close_suffix dim, {M2} FROM b WHERE code IN {SIG} AND close_suffix IS NOT NULL GROUP BY 1,2,3 HAVING count(*)>=30")
dump("rich_reclaim", f"SELECT universe, code signal, reclaim dim, {M2} FROM b WHERE code IN {SIG} GROUP BY 1,2,3 HAVING count(*)>=30")
dump("rich_pbucket", f"SELECT universe, code signal, pbucket dim, {M2} FROM b WHERE code IN {SIG} GROUP BY 1,2,3 HAVING count(*)>=20")
con.close()
print("ENGINE2 DONE")
