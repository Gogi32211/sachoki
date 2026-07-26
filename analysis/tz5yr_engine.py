"""
tz5yr_engine.py — full v5-style TZ+WLNBB research on the 5-year 8.3M-bar DB.
Replicates every v5 dimension (signal, Line5, suffix, A/I/O, vol_bucket, prev1,
VR/VX/PSAR/RSI2, composites, 4-bar sequences) ACROSS the full history + per-year.
Outputs CSVs to /tmp/tz5yr/data/. Phase 1 of the deliverable. ANALYSIS ONLY.
"""
import duckdb, os
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
OUT="/tmp/tz5yr/data"; os.makedirs(OUT,exist_ok=True)
con=duckdb.connect(DB,read_only=True)
con.execute("PRAGMA threads=6")

TS=['T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12']
ZS=['Z1','Z1G','Z2','Z2G','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11','Z12']

# ---- base table with unified code + lags + parsed bar_line5 (one heavy window pass) ----
print("building base table (window pass over 8.3M)...", flush=True)
con.execute("""
CREATE TEMP TABLE b AS
WITH s AS (
  SELECT universe,ticker,date, year(date) AS yr,
    coalesce(nullif(t_sig,''), nullif(z_sig,''), '∅') AS code,
    t_sig, z_sig, vol_bucket, full_suffix, l_sig, close_suffix, bar_line5,
    fwd_5d, fwd_10d, fwd_20d, mfe_10d, mae_10d,
    CASE WHEN bar_line5 LIKE 'VR%' THEN 'VR' WHEN bar_line5 LIKE 'VX%' THEN 'VX' ELSE 'none' END AS vrvx,
    CASE WHEN bar_line5 LIKE '%PS%' THEN 'PS' WHEN bar_line5 LIKE '%PB%' THEN 'PB' ELSE 'none' END AS psar,
    CASE WHEN regexp_matches(bar_line5,'R2[LHXD]') THEN regexp_extract(bar_line5,'R2[LHXD]') ELSE 'none' END AS rsi2,
    row_number() OVER (PARTITION BY universe,ticker,date ORDER BY date) rn
  FROM bars
  WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500
)
SELECT *,
  lag(code,1) OVER w AS p1, lag(code,2) OVER w AS p2, lag(code,3) OVER w AS p3
FROM s WHERE rn=1
WINDOW w AS (PARTITION BY universe,ticker ORDER BY date)
""")
n=con.execute("SELECT count(*) FROM b").fetchone()[0]
print(f"  base rows: {n:,}", flush=True)

M = """count(*) n, round(median(fwd_10d),3) med10, round(avg(fwd_10d),3) avg10,
 round(median(fwd_5d),3) med5, round(median(fwd_20d),3) med20,
 round(100.0*avg(CASE WHEN fwd_10d>0 THEN 1 ELSE 0 END),1) win,
 round(100.0*avg(CASE WHEN fwd_10d<=-5 THEN 1 ELSE 0 END),1) fail,
 round(100.0*avg(CASE WHEN fwd_10d>=5 THEN 1 ELSE 0 END),1) big_win"""

def dump(name, sql):
    df=con.execute(sql).fetchdf()
    df.to_csv(f"{OUT}/{name}.csv", index=False)
    print(f"  {name}: {len(df)} rows", flush=True)

SIGSET = "('"+ "','".join(TS+ZS) +"')"

# 1. baseline per signal
dump("baseline_signal", f"SELECT universe, code AS signal, {M} FROM b WHERE code IN {SIGSET} GROUP BY 1,2 ORDER BY 1,2")
# 2. baseline per signal per year
dump("baseline_year", f"SELECT universe, code AS signal, yr, {M} FROM b WHERE code IN {SIGSET} GROUP BY 1,2,3 ORDER BY 1,2,3")
# 3. dimensions
for nm,col in [("vol","vol_bucket"),("suffix","full_suffix"),("line5","l_sig"),
               ("aio","close_suffix"),("prev1","p1"),("vrvx","vrvx"),("psar","psar"),("rsi2","rsi2")]:
    dump(f"dim_{nm}", f"SELECT universe, code AS signal, {col} AS dim, {M} FROM b "
         f"WHERE code IN {SIGSET} AND {col} IS NOT NULL AND CAST({col} AS VARCHAR)<>'' GROUP BY 1,2,3 "
         f"HAVING count(*)>=30 ORDER BY 1,2,3")
# 4. composites: signal + Line5 + suffix
dump("composites", f"""SELECT universe, code AS signal, l_sig, full_suffix,
   code||l_sig||full_suffix AS composite, {M} FROM b
   WHERE code IN {SIGSET} AND l_sig IS NOT NULL AND full_suffix IS NOT NULL
   GROUP BY 1,2,3,4,5 HAVING count(*)>=40 ORDER BY 1,2,med10 DESC""")
# 5. composites + A/I/O (the v5 'next step')
dump("composites_aio", f"""SELECT universe, code AS signal, l_sig, full_suffix, close_suffix,
   code||l_sig||full_suffix||close_suffix AS composite, {M} FROM b
   WHERE code IN {SIGSET} AND l_sig IS NOT NULL AND full_suffix IS NOT NULL AND close_suffix IS NOT NULL
   GROUP BY 1,2,3,4,5,6 HAVING count(*)>=40 ORDER BY 1,2,med10 DESC""")
# 6. 4-bar sequences: [p3|p2|p1] -> signal
dump("sequences", f"""SELECT universe, code AS signal, p3||'|'||p2||'|'||p1 AS seq3, {M} FROM b
   WHERE code IN {SIGSET} AND p1 IS NOT NULL AND p2 IS NOT NULL AND p3 IS NOT NULL
   GROUP BY 1,2,3 HAVING count(*)>=25 ORDER BY 1,2,med10 DESC""")
# 7. prev1 x signal already in dim_prev1; add vol x signal already in dim_vol
con.close()
print("PHASE 1 DONE -> "+OUT)
