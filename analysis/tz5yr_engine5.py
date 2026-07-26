"""tz5yr_engine5.py — line5 (bar_line5) decomposed per-signal: PSAR(PS/PB),
RSI2(R2L/H/X/D), VIX(VR/VX), full bar_line5, + PSAR×RSI2 combo. Rich metrics
(m10 naming for the doc generator). 5-year, per universe. ANALYSIS ONLY."""
import duckdb, os
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"; D="/tmp/tz5yr/data/"
con=duckdb.connect(DB,read_only=True); con.execute("PRAGMA threads=6")
TS=['T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12']
ZS=['Z1','Z1G','Z2','Z2G','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11','Z12']
SIG="('"+"','".join(TS+ZS)+"')"
con.execute(f"""CREATE TEMP TABLE b AS
SELECT universe, coalesce(nullif(t_sig,''),nullif(z_sig,''),'∅') code, fwd_10d, bar_line5 bl,
  CASE WHEN bar_line5 LIKE 'VR%' THEN 'VR' WHEN bar_line5 LIKE 'VX%' THEN 'VX' ELSE 'none' END vrvx,
  CASE WHEN bar_line5 LIKE '%PS%' THEN 'PS' WHEN bar_line5 LIKE '%PB%' THEN 'PB' ELSE 'none' END psar,
  CASE WHEN regexp_matches(bar_line5,'R2[LHXD]') THEN regexp_extract(bar_line5,'R2[LHXD]') ELSE 'none' END rsi2
FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500 AND code IN {SIG}""")
M="""count(*) n, round(median(fwd_10d),3) m10, round(avg(fwd_10d),3) a10,
 round(100.0*avg(CASE WHEN fwd_10d>0 THEN 1 ELSE 0 END),1) win,
 round(100.0*avg(CASE WHEN fwd_10d<=-5 THEN 1 ELSE 0 END),1) fail,
 round(100.0*avg(CASE WHEN fwd_10d>=10 THEN 1 ELSE 0 END),1) bigwin"""
for nm,col in [('psar','psar'),('rsi2','rsi2'),('vrvx','vrvx'),('line5full','bl')]:
    con.execute(f"COPY (SELECT universe, code signal, {col} dim, {M} FROM b "
                f"WHERE {col} IS NOT NULL AND CAST({col} AS VARCHAR)<>'' GROUP BY 1,2,3 HAVING count(*)>=30) "
                f"TO '{D}rich_{nm}.csv' (HEADER)")
    print("  +",nm)
con.execute(f"COPY (SELECT universe, code signal, psar||'+'||rsi2 dim, {M} FROM b "
            f"WHERE psar<>'none' AND rsi2<>'none' GROUP BY 1,2,3 HAVING count(*)>=40) TO '{D}rich_psarrsi2.csv' (HEADER)")
print("  + psarrsi2")
con.close(); print("ENGINE5 DONE")
