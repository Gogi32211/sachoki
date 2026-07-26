"""tz5yr_engine4.py — per-signal atomic-profile lift (baseline vs close=O vs
close=O+gap vs EO+gap), for the per-signal docs. ANALYSIS ONLY."""
import duckdb, os
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"; D="/tmp/tz5yr/data/"
con=duckdb.connect(DB,read_only=True); con.execute("PRAGMA threads=6")
TS=['T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12']
ZS=['Z1','Z1G','Z2','Z2G','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11','Z12']
SIG="('"+"','".join(TS+ZS)+"')"
con.execute(f"""CREATE TEMP TABLE b AS
SELECT universe, coalesce(nullif(t_sig,''),nullif(z_sig,''),'∅') code, fwd_10d,
  substr(full_suffix,1,1) ne, close_suffix cls, coalesce(nullif(bar_gap_class,''),'none') gap
FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500 AND code IN {SIG}""")
M="""count(*) n, round(median(fwd_10d),3) m10,
 round(100.0*avg(CASE WHEN fwd_10d>0 THEN 1 ELSE 0 END),1) win,
 round(100.0*avg(CASE WHEN fwd_10d<=-5 THEN 1 ELSE 0 END),1) fail"""
variants={'all':'1=1','close=O':"cls='O'","close=O+gap":"cls='O' AND gap IN ('G2','G3')",
          'EO+gap':"ne='E' AND cls='O' AND gap IN ('G2','G3')",
          'close=A (bear-side)':"cls='A'","A+gap":"cls='A' AND gap IN ('G2','G3')"}
import pandas as pd
rows=[]
for vn,w in variants.items():
    df=con.execute(f"SELECT universe, code signal, '{vn}' variant, {M} FROM b WHERE {w} GROUP BY 1,2 HAVING count(*)>=30").fetchdf()
    rows.append(df)
pd.concat(rows).to_csv(D+'rich_atomic.csv',index=False)
con.close(); print("rich_atomic.csv written")
