"""IN PROGRESS — physics/shape states crossed with the book's edges.

Kept so the work is not lost to a /tmp wipe. NOT a result: the gates this book
requires have not been run. What exists is a first screen on median fwd_5d,
which in this same session has already been shown to disagree with a path
simulation that used real stops.

Still to do: path-sim with stops · worst-year per cell · the other timeframes
(4h/1h/15m are backfilled) · the other universes · a multiplicity correction.

1_build_masks  pulls via edge_replay._pull and runs _prep to get the E_* masks
2_cross        joins the physics/shape columns and crosses them with the edges
3_windows      the same aggregate on the mining window and on the reserved one

No conclusion is recorded here on purpose.
"""
import sys; sys.path.insert(0,'/Users/sachoki/Desktop/sachoki-desktop/backend')
import duckdb, pandas as pd, numpy as np
from studio.paths import db_path
E=pd.read_parquet('/tmp/edge_frame.parquet')
E['date']=pd.to_datetime(E['date']).dt.normalize()
c=duckdb.connect(db_path('studio_analytics.duckdb'), read_only=True)
S=c.execute("""SELECT ticker, CAST(date AS DATE) AS date, fwd_5d, phys_r, phys_e, phys_c,
   bar_body_wick FROM (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
   FROM bars WHERE universe<>'index') WHERE rn=1 AND fwd_5d IS NOT NULL""").fetchdf()
S['date']=pd.to_datetime(S['date'])
d=E.merge(S, on=['ticker','date'], how='inner')
print(f"{len(d):,} rows joined  {d.date.min().date()} → {d.date.max().date()}")
MINE=d[d.date<'2024-01-01'].reset_index(drop=True)
EDGES=[x for x in ['E_qzcapit','E_dl1','E_t1capbounce','E_zabsorb','E_spring','E_g3abs',
       'E_engulfabs','E_confluence','E_washout','E_atomic','E_rtb_base','E_failbear'] if x in d.columns]
bw=MINE.bar_body_wick.fillna('')
ST={'RA':MINE.phys_r=='RA','RN':MINE.phys_r=='RN','RF':MINE.phys_r=='RF',
    'E2':MINE.phys_e.isin(['E2','E2★']),'BB':bw.str.contains('BB'),
    'TB':bw.str.contains('TB'),'MJ':bw=='MJ','C0':MINE.phys_c=='C0',
    'C1':MINE.phys_c=='C1','C2':MINE.phys_c=='C2'}
print(f"\nmining {len(MINE):,} rows · lift over the EDGE ALONE, median fwd5\n")
print(f"{'edge':<15}{'n':>7}{'alone':>8}"+"".join(f"{s:>7}" for s in ST))
best=[]
for e in EDGES:
    m=MINE[e].fillna(False).astype(bool)
    if m.sum()<300: continue
    a=MINE.loc[m,'fwd_5d'].median()
    row=f"{e[2:]:<15}{int(m.sum()):>7,}{a:>7.2f}%"
    for s,sm in ST.items():
        k=m&sm
        if k.sum()<150: row+=f"{'·':>7}"; continue
        v=MINE.loc[k,'fwd_5d'].median(); row+=f"{v-a:>+7.2f}"
        best.append((v-a,e,s,int(k.sum()),a,v))
    print(row)
best.sort(reverse=True)
print("\ntop 6:")
for l,e,s,n,a,v in best[:6]: print(f"  {e[2:]:<15} × {s:<3} n={n:>5,}  {a:+.2f}% → {v:+.2f}%  {l:+.2f}pp")
d.to_parquet('/tmp/edge_joined.parquet', index=False)
