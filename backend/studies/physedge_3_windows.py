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
import pandas as pd, numpy as np
d=pd.read_parquet('/tmp/edge_joined.parquet'); d['date']=pd.to_datetime(d['date'])
EDGES=[x for x in ['E_qzcapit','E_dl1','E_t1capbounce','E_zabsorb','E_spring','E_g3abs',
      'E_engulfabs','E_confluence','E_washout','E_atomic','E_rtb_base','E_failbear'] if x in d.columns]
def states(f):
    bw=f.bar_body_wick.fillna('')
    return {'RA':f.phys_r=='RA','RN':f.phys_r=='RN','RF':f.phys_r=='RF',
            'E2':f.phys_e.isin(['E2','E2★']),'BB':bw.str.contains('BB'),
            'TB':bw.str.contains('TB'),'MJ':bw=='MJ','C0':f.phys_c=='C0',
            'C1':f.phys_c=='C1','C2':f.phys_c=='C2'}
def agg(f,label):
    ST=states(f); rows={}
    for s,sm in ST.items():
        lifts=[]
        for e in EDGES:
            m=f[e].fillna(False).astype(bool)
            if m.sum()<300: continue
            k=m&sm
            if k.sum()<150: continue
            lifts.append(f.loc[k,'fwd_5d'].median()-f.loc[m,'fwd_5d'].median())
        if lifts: rows[s]=(np.mean(lifts), sum(1 for x in lifts if x>0), len(lifts))
    print(f"\n{label} — mean lift across edges, and how many edges it helped")
    print(f"  {'state':<6}{'mean lift':>11}{'edges helped':>15}")
    for s,(m,p,n) in sorted(rows.items(), key=lambda kv:-kv[1][0]):
        print(f"  {s:<6}{m:>+10.3f}pp{p:>10}/{n}")
    return rows
MINE=d[d.date<'2024-01-01'].reset_index(drop=True)
OOS =d[d.date>='2024-01-01'].reset_index(drop=True)
agg(MINE,"MINING 2021-05 → 2023-12")
agg(OOS ,"RESERVED 2024-01 → 2026-08   [scored once]")
