"""atomic_profile.py — PART A: progressive stacking of the atomic bull profile
(close=O, EO, wick=D, gap G2/G3, body=M, vol=B) on T-signal bars. Close-based
med10 + lift + per-year, per universe — find where n holds & if regime-dependent."""
import duckdb, pandas as pd
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"
con=duckdb.connect(DB,read_only=True); con.execute("PRAGMA threads=6")
TS="('T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12')"
con.execute(f"""CREATE TEMP TABLE b AS
SELECT universe, year(date) yr, fwd_10d,
  substr(full_suffix,1,1) ne, close_suffix cls,
  coalesce(nullif(regexp_extract(full_suffix,'[BUD]'),''),'_') wick,
  coalesce(nullif(bar_gap_class,''),'none') gap, substr(bar_body_wick,1,1) body, vol_bucket vol
FROM bars WHERE t_sig IN {TS} AND fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500""")
B={r[0]:r[1] for r in con.execute("SELECT universe,median(fwd_10d) FROM b GROUP BY 1").fetchall()}
def stats(where):
    return con.execute(f"""SELECT universe, count(*) n, round(median(fwd_10d),3) med,
      round(100.0*avg(CASE WHEN fwd_10d>0 THEN 1 ELSE 0 END),1) win
      FROM b WHERE {where} GROUP BY 1""").fetchdf()
recipes=[
 ("R0 all T-signals","1=1"),
 ("R1 +close=O","cls='O'"),
 ("R2 +EO (escape & O)","ne='E' AND cls='O'"),
 ("R3 +wick=D","ne='E' AND cls='O' AND wick='D'"),
 ("R4 +gap G2/G3","ne='E' AND cls='O' AND wick='D' AND gap IN ('G2','G3')"),
 ("R5 +body=M","ne='E' AND cls='O' AND wick='D' AND gap IN ('G2','G3') AND body='M'"),
 ("R6 +vol=B","ne='E' AND cls='O' AND wick='D' AND gap IN ('G2','G3') AND body='M' AND vol='B'"),
 ("ALT gap-only (G2/G3 + O)","cls='O' AND gap IN ('G2','G3')"),
 ("ALT EO + gap","ne='E' AND cls='O' AND gap IN ('G2','G3')"),
]
print(f"{'recipe':32}  "+'  '.join(f'{u}' for u in ('sp500','nasdaq','russell2k')))
print(f"{'baseline med':32}  "+'  '.join(f'{round(B[u],3)}' for u in ('sp500','nasdaq','russell2k')))
print("-"*90)
for name,w in recipes:
    d=stats(w).set_index('universe')
    cells=[]
    for u in ('sp500','nasdaq','russell2k'):
        if u in d.index:
            r=d.loc[u]; cells.append(f"{r['med']:+.2f}/L{r['med']-B[u]:+.2f}/n{int(r['n'])}/w{r['win']}")
        else: cells.append("—")
    print(f"{name:32}  "+'  '.join(cells))

# per-year for the strongest viable recipe (ALT EO+gap, pooled nas+r2k)
print("\n=== per-year: EO + gap (G2/G3) + close=O, pooled nasdaq+russell2k ===")
dy=con.execute("""SELECT yr, count(*) n, round(median(fwd_10d),3) med, round(100.0*avg(CASE WHEN fwd_10d>0 THEN 1 ELSE 0 END),1) win
  FROM b WHERE universe IN ('nasdaq','russell2k') AND ne='E' AND cls='O' AND gap IN ('G2','G3') GROUP BY 1 ORDER BY 1""").fetchdf()
print(dy.to_string(index=False))
print("\n=== per-year: ALT gap-only (G2/G3 + O), pooled nas+r2k (bigger n) ===")
dy2=con.execute("""SELECT yr, count(*) n, round(median(fwd_10d),3) med, round(100.0*avg(CASE WHEN fwd_10d>0 THEN 1 ELSE 0 END),1) win
  FROM b WHERE universe IN ('nasdaq','russell2k') AND cls='O' AND gap IN ('G2','G3') GROUP BY 1 ORDER BY 1""").fetchdf()
print(dy2.to_string(index=False))
con.close()
