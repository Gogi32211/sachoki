"""tz5yr_phase2.py — per-year regime stability + cross-universe synthesis.
Adds composite×year, sequence×year, universal-pattern tables, regime flags.
Reads/writes /tmp/tz5yr/data/. ANALYSIS ONLY."""
import duckdb, pandas as pd, os
DB="/Users/sachoki/Downloads/studio_analytics.duckdb"; D="/tmp/tz5yr/data/"
con=duckdb.connect(DB,read_only=True); con.execute("PRAGMA threads=6")
TS=['T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12']
ZS=['Z1','Z1G','Z2','Z2G','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11','Z12']
SIG="('"+"','".join(TS+ZS)+"')"

print("rebuild base window table...",flush=True)
con.execute("""CREATE TEMP TABLE b AS
WITH s AS (
  SELECT universe,ticker,date,year(date) yr,
    coalesce(nullif(t_sig,''),nullif(z_sig,''),'∅') code,
    l_sig,full_suffix,fwd_5d,fwd_10d,fwd_20d,
    row_number() OVER (PARTITION BY universe,ticker,date ORDER BY date) rn
  FROM bars WHERE fwd_10d IS NOT NULL AND fwd_10d BETWEEN -90 AND 500)
SELECT *, lag(code,1) OVER w p1, lag(code,2) OVER w p2, lag(code,3) OVER w p3
FROM s WHERE rn=1 WINDOW w AS (PARTITION BY universe,ticker ORDER BY date)""")
MY="""count(*) n, round(median(fwd_10d),3) med10, round(100.0*avg(CASE WHEN fwd_10d>0 THEN 1 ELSE 0 END),1) win"""

# composite x year (cells n>=15)
con.execute(f"""COPY (SELECT universe, code signal, code||l_sig||full_suffix composite, yr, {MY}
  FROM b WHERE code IN {SIG} AND l_sig IS NOT NULL AND full_suffix IS NOT NULL
  GROUP BY 1,2,3,4 HAVING count(*)>=15 ORDER BY 1,3,4) TO '{D}composites_year.csv' (HEADER)""")
# sequence x year (cells n>=12)
con.execute(f"""COPY (SELECT universe, code signal, p3||'|'||p2||'|'||p1 seq3, yr, {MY}
  FROM b WHERE code IN {SIG} AND p1 IS NOT NULL AND p2 IS NOT NULL AND p3 IS NOT NULL
  GROUP BY 1,2,3,4 HAVING count(*)>=12 ORDER BY 1,2,3,4) TO '{D}sequences_year.csv' (HEADER)""")
con.close()
print("  per-year CSVs written",flush=True)

# ---- cross-universe synthesis (pandas) ----
def piv(fn,key):
    df=pd.read_csv(D+fn)
    p=df.pivot_table(index=key,columns='universe',values='med10',aggfunc='first')
    nn=df.pivot_table(index=key,columns='universe',values='n',aggfunc='first')
    for u in ['sp500','nasdaq','russell2k']:
        if u not in p: p[u]=None
        if u not in nn: nn[u]=None
    p['min3']=p[['sp500','nasdaq','russell2k']].min(axis=1)
    p['n_min']=nn[['sp500','nasdaq','russell2k']].min(axis=1)
    return p.reset_index()

# universal dims
for nm in ['vol','suffix','line5','aio','prev1']:
    df=pd.read_csv(D+f'dim_{nm}.csv')
    g=df.groupby('dim').agg(cross_med=('med10','mean'),uni=('universe','nunique'),tot_n=('n','sum'),avg_win=('win','mean')).reset_index()
    g=g[g.uni==3].sort_values('cross_med',ascending=False)
    g.round(3).to_csv(D+f'universal_{nm}.csv',index=False)

# universal composites (present all 3, ranked by min3)
c=pd.read_csv(D+'composites.csv')
cu=c[c.n>=40]
uc=piv('composites.csv','composite')
uc=uc[(uc[['sp500','nasdaq','russell2k']].notna().sum(axis=1)==3)].sort_values('min3',ascending=False)
uc.round(3).to_csv(D+'universal_composites.csv',index=False)

# regime flag per composite (pos-year count, all universes pooled per composite)
cy=pd.read_csv(D+'composites_year.csv')
def posyears(sub):
    yy=sub.groupby('yr').med10.median()  # median across universes per year
    return int((yy>0).sum()), int(yy.notna().sum())
rows=[]
for comp,sub in cy.groupby('composite'):
    if sub.n.sum()<150: continue
    pos,tot=posyears(sub)
    rows.append(dict(composite=comp,pos_yr=pos,tot_yr=tot,
        flag='STABLE' if (tot>=4 and pos/tot>=0.6) else ('2025-ARTIFACT' if pos<=tot*0.4 else 'MIXED'),
        n=int(sub.n.sum()),med_all=round(sub.med10.median(),3)))
pd.DataFrame(rows).sort_values('med_all',ascending=False).to_csv(D+'composite_regime.csv',index=False)

# sequence regime flag
sy=pd.read_csv(D+'sequences_year.csv')
rows=[]
for (sig,seq),sub in sy.groupby(['signal','seq3']):
    if sub.n.sum()<80: continue
    yy=sub.groupby('yr').med10.median(); pos=int((yy>0).sum()); tot=int(yy.notna().sum())
    rows.append(dict(signal=sig,seq3=seq,pos_yr=pos,tot_yr=tot,
        flag='STABLE' if (tot>=4 and pos/tot>=0.6) else ('2025-ARTIFACT' if pos<=tot*0.4 else 'MIXED'),
        n=int(sub.n.sum()),med_all=round(sub.med10.median(),3)))
pd.DataFrame(rows).sort_values('med_all',ascending=False).to_csv(D+'sequence_regime.csv',index=False)
print("PHASE 2 DONE")
