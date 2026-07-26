"""suffix_decomp.py — decompose the WLNBB suffix into atomic components and analyze
each SEPARATELY (ne / wick / pen / close) AND COMBINED (full + A/I/O subdivided),
over the 5-year history, per universe, T-side & Z-side. ANALYSIS ONLY."""
import duckdb, pandas as pd, os
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"; D="/tmp/tz5yr/data/"
con=duckdb.connect(DB,read_only=True); con.execute("PRAGMA threads=6")
TS=['T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12']
ZS=['Z1','Z1G','Z2','Z2G','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11','Z12']

print("build base with parsed suffix components...",flush=True)
con.execute(f"""CREATE TEMP TABLE b AS
SELECT universe, coalesce(nullif(t_sig,''),nullif(z_sig,''),'∅') code,
  CASE WHEN t_sig IS NOT NULL AND t_sig<>'' THEN 'T' WHEN z_sig IS NOT NULL AND z_sig<>'' THEN 'Z' ELSE '∅' END side,
  full_suffix, composite_full_suffix, close_suffix, year(date) yr, fwd_10d,
  substr(full_suffix,1,1) AS ne,
  coalesce(nullif(regexp_extract(full_suffix,'[BUD]'),''),'∅') AS wick,
  coalesce(nullif(regexp_extract(full_suffix,'[HPR]'),''),'∅') AS pen
FROM bars
WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500 AND full_suffix IS NOT NULL AND full_suffix<>''""")
print("  rows:",con.execute("SELECT count(*) FROM b").fetchone()[0],flush=True)
M="""count(*) n, round(median(fwd_10d),3) med10, round(avg(fwd_10d),3) avg10,
 round(100.0*avg(CASE WHEN fwd_10d>0 THEN 1 ELSE 0 END),1) win,
 round(100.0*avg(CASE WHEN fwd_10d<=-5 THEN 1 ELSE 0 END),1) fail,
 round(100.0*avg(CASE WHEN fwd_10d>=10 THEN 1 ELSE 0 END),1) bigwin"""

def q(sql): return con.execute(sql).fetchdf()

# baselines per universe per side (T and Z separately)
def base(side):
    return {r['universe']:r['med10'] for _,r in q(f"SELECT universe,{M} FROM b WHERE side='{side}' GROUP BY 1").iterrows()}
bT=base('T'); bZ=base('Z')

print("\n############ COMPONENT EFFECTS — separate lines (lift vs side baseline) ############")
for side,bl,lbl in [('T',bT,'T-signals (bullish, fwd UP)'),('Z',bZ,'Z-signals (bearish)')]:
    print(f"\n===== {lbl}  baseline med10: "+', '.join(f'{u}:{round(v,3)}' for u,v in bl.items())+" =====")
    for comp in ['ne','wick','pen','close_suffix']:
        df=q(f"SELECT universe,{comp} dim,{M} FROM b WHERE side='{side}' GROUP BY 1,2 HAVING count(*)>=200")
        piv=df.pivot_table(index='dim',columns='universe',values='med10')
        nn=df.pivot_table(index='dim',columns='universe',values='n')
        print(f"  -- {comp} --")
        for d in piv.index:
            cells=' '.join(f"{u}:{round(piv.loc[d,u],3)}(lift{round(piv.loc[d,u]-bl.get(u,0),2)},n{int(nn.loc[d,u]) if pd.notna(nn.loc[d,u]) else 0})" for u in ['sp500','nasdaq','russell2k'] if u in piv and pd.notna(piv.loc[d,u]))
            print(f"     {str(d):3} {cells}")

# combined: composite_full_suffix (A/I/O subdivided) — top per universe (T-side)
print("\n############ COMBINED — composite_full_suffix (A/I/O subdivided), T-side ############")
df=q(f"SELECT universe,composite_full_suffix sfx,{M} FROM b WHERE side='T' GROUP BY 1,2 HAVING count(*)>=2000")
for u in ['sp500','nasdaq','russell2k']:
    s=df[df.universe==u].sort_values('med10',ascending=False)
    print(f"\n -- {u} (baseline {round(bT[u],3)}) top/bottom subdivided suffix --")
    print("   TOP: "+'  '.join(f"{r.sfx}:{r.med10}(w{r.win},n{int(r.n)})" for r in s.head(6).itertuples()))
    print("   BOT: "+'  '.join(f"{r.sfx}:{r.med10}(w{r.win},n{int(r.n)})" for r in s.tail(5).itertuples()))

# 2-way atomic combo: ne x close (the two strongest axes) T-side
print("\n############ 2-WAY: ne × close (T-side) ############")
df=q(f"SELECT universe, ne||close_suffix combo,{M} FROM b WHERE side='T' GROUP BY 1,2 HAVING count(*)>=500")
for u in ['sp500','nasdaq','russell2k']:
    s=df[df.universe==u].sort_values('med10',ascending=False)
    print(f"  {u}: "+'  '.join(f"{r.combo}:{r.med10}(lift{round(r.med10-bT[u],2)},n{int(r.n)})" for r in s.itertuples()))
con.close()
print("\nDONE")
