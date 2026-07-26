"""tz5yr_engine3.py — extra dimensions for the per-signal docs + discoveries:
suffix components (ne/wick/pen/close), subdivided suffix (composite_full_suffix),
body/wick (line3) + body-size + wick-shape, gap/range (line4). Rich metrics,
per (universe, signal), 5-year. Also prints cross-signal discoveries. ANALYSIS ONLY."""
import duckdb, os
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"; D="/tmp/tz5yr/data/"; os.makedirs(D,exist_ok=True)
con=duckdb.connect(DB,read_only=True); con.execute("PRAGMA threads=6")
TS=['T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12']
ZS=['Z1','Z1G','Z2','Z2G','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11','Z12']
SIG="('"+"','".join(TS+ZS)+"')"

print("build base (suffix parse + body/wick + gap/range)...",flush=True)
con.execute(f"""CREATE TEMP TABLE b AS
SELECT universe, coalesce(nullif(t_sig,''),nullif(z_sig,''),'∅') code,
  CASE WHEN t_sig IS NOT NULL AND t_sig<>'' THEN 'T' WHEN z_sig IS NOT NULL AND z_sig<>'' THEN 'Z' ELSE '∅' END side,
  fwd_10d,
  substr(full_suffix,1,1) ne,
  coalesce(nullif(regexp_extract(full_suffix,'[BUD]'),''),'_') wick,
  coalesce(nullif(regexp_extract(full_suffix,'[HPR]'),''),'_') pen,
  close_suffix cls, composite_full_suffix sfx_aio,
  bar_body_wick bw, substr(bar_body_wick,1,1) body, coalesce(nullif(substr(bar_body_wick,2),''),'_') wshape,
  bar_gap_range gr, coalesce(nullif(bar_gap_class,''),'none') gap, bar_range_class rng
FROM bars
WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500 AND code IN {SIG}""")
M="""count(*) n, round(median(fwd_10d),3) m10, round(avg(fwd_10d),3) a10,
 round(100.0*avg(CASE WHEN fwd_10d>0 THEN 1 ELSE 0 END),1) win,
 round(100.0*avg(CASE WHEN fwd_10d<=-5 THEN 1 ELSE 0 END),1) fail,
 round(100.0*avg(CASE WHEN fwd_10d>=10 THEN 1 ELSE 0 END),1) bigwin"""

# per-signal rich dims (for the docs)
for nm,col in [('ne','ne'),('wickc','wick'),('penc','pen'),('cls','cls'),('sfxaio','sfx_aio'),
               ('bodywick','bw'),('bodysz','body'),('wshape','wshape'),('gaprange','gr')]:
    con.execute(f"COPY (SELECT universe, code signal, {col} dim, {M} FROM b "
                f"WHERE {col} IS NOT NULL AND CAST({col} AS VARCHAR)<>'' GROUP BY 1,2,3 HAVING count(*)>=30) "
                f"TO '{D}rich_{nm}.csv' (HEADER)")
    print("  +",nm,flush=True)
con.execute(f"COPY (SELECT universe, code signal, ne||cls combo, {M} FROM b GROUP BY 1,2,3 HAVING count(*)>=40) TO '{D}rich_neclose.csv' (HEADER)")

# ---- discoveries: body/wick + gap/range cross-signal (T-side & Z-side) ----
def base(side): return {r['universe']:r['m10'] for _,r in con.execute(f"SELECT universe,{M} FROM b WHERE side='{side}' GROUP BY 1").df().iterrows()}
bT,bZ=base('T'),base('Z')
print("\n############ DISCOVERIES — body/wick (line3) + gap/range (line4) ############")
for side,bl,lbl in [('T',bT,'T-signals (bullish)'),('Z',bZ,'Z-signals (bearish)')]:
    print(f"\n===== {lbl}  baseline: "+', '.join(f'{u}:{round(v,3)}' for u,v in bl.items())+" =====")
    for col,name in [('body','body-size'),('wshape','wick-shape'),('gap','gap-class'),('rng','range-class'),('gr','gap×range')]:
        df=con.execute(f"SELECT universe,{col} dim,{M} FROM b WHERE side='{side}' AND {col} IS NOT NULL AND CAST({col} AS VARCHAR)<>'' GROUP BY 1,2 HAVING count(*)>=300").df()
        piv=df.pivot_table(index='dim',columns='universe',values='m10'); nn=df.pivot_table(index='dim',columns='universe',values='n')
        rows=[]
        for d in piv.index:
            sp=piv.loc[d,'sp500'] if 'sp500' in piv else None
            rows.append((d,sp))
        rows.sort(key=lambda x:(x[1] if x[1] is not None else -99),reverse=True)
        print(f"  -- {name} (sorted by sp500 lift) --")
        for d,_ in rows[:8]:
            cells=' '.join(f"{u}:{round(piv.loc[d,u],3)}(L{round(piv.loc[d,u]-bl.get(u,0),2)},n{int(nn.loc[d,u]) if u in nn and nn.loc[d,u]==nn.loc[d,u] else 0})" for u in ['sp500','nasdaq','russell2k'] if u in piv and piv.loc[d,u]==piv.loc[d,u])
            print(f"     {str(d):4} {cells}")
con.close()
print("\nENGINE3 DONE")
