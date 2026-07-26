"""L34-CAMPAIGN (2026-07-21, user hypothesis): several L34 bars in a window, AT ROUGHLY
THE SAME PRICE LEVEL = systematic institutional absorption. Entry ON the completing
L34 bar (next open). Variants: window 10/20, count>=2/3, level-band 3%/5%/none(control),
red-current / all-red. Dual metric: path-sim trail25/-15/60/cooldown-5 + per-year,
TRAIN/TEST, price buckets, random-z."""
import numpy as np, pandas as pd, duckdb
S_=0.0015
def sim(o,hi,lo,cl,start):
    n=len(cl); e=o[start]*(1+S_)
    if e<=0 or start>=n: return None
    pk=e; hd=e*0.85; end=min(start+60,n)
    for j in range(start,end):
        if o[j]<=hd and j>start: return o[j]/e-1-S_
        if lo[j]<=hd: return -0.15-S_
        pk=max(pk,hi[j]); ts=pk*0.75
        if o[j]<=ts and j>start: return o[j]/e-1-S_
        if lo[j]<=ts: return ts/e-1-S_
    return cl[end-1]/e-1-S_
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume, coalesce(l_sig,'') l,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D['dv']=D.close*D.volume
VAR=[  # (name, W, N, band, red_current, all_red)
 ("V1 W20 N2 band5%",      20,2,0.05,False,False),
 ("V2 W20 N3 band5%",      20,3,0.05,False,False),
 ("V3 W20 N2 NO-band",     20,2,None,False,False),
 ("V4 W10 N2 band5%",      10,2,0.05,False,False),
 ("V5 W20 N2 b5% RED-cur", 20,2,0.05,True,False),
 ("V6 W20 N2 b5% ALL-RED", 20,2,0.05,False,True),
 ("V7 W20 N2 band3%",      20,2,0.03,False,False),
]
trades={k:[] for k,*_ in [(v[0],) for v in VAR]}
trades={v[0]:[] for v in VAR}
pool=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<60: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    dv=g.dv.to_numpy(float); yr=g.date.astype(str).str[:4].tolist()
    isl=(g.l=='L34').to_numpy()
    red=isl&(cl<o)
    idx=np.nonzero(isl)[0]
    last={v[0]:-99 for v in VAR}
    for i in range(25,n-1):
        if dv[i]<3e6: continue
        if i%31==0:
            r=sim(o,hi,lo,cl,i+1)
            if r is not None: pool.append(r)
        if not isl[i]: continue
        for name,W,N,band,redc,allred in VAR:
            if redc and not red[i]: continue
            prev=[j for j in idx if i-W<=j<i]
            camp=prev+[i]
            if len(camp)<N: continue
            if allred and not all(red[j] for j in camp): continue
            if band is not None:
                px=cl[camp]
                if px.max()/px.min()-1>band: continue
            if i-last[name]<5: continue
            r=sim(o,hi,lo,cl,i+1)
            if r is not None:
                trades[name].append((yr[i],cl[i],r)); last[name]=i
P=np.array(pool); rng=np.random.default_rng(9)
print(f"random pool n={len(P):,} mean {100*P.mean():+.2f}%\n")
for name,*_ in VAR:
    T=pd.DataFrame(trades[name],columns=["yr","px","ret"])
    if len(T)<80: print(f"{name:22} n={len(T)} too few"); continue
    yrs=T.groupby('yr').ret.mean()*100
    tr=T[T.yr.isin(('2021','2022','2023'))]; te=T[T.yr.isin(('2024','2025','2026'))]
    trm=100*tr.ret.mean() if len(tr)>=30 else float('nan')
    tem=100*te.ret.mean() if len(te)>=30 else float('nan')
    pfd=-T.ret[T.ret<=0].sum(); pf=T.ret[T.ret>0].sum()/pfd if pfd>0 else float('nan')
    bs=np.array([P[rng.integers(0,len(P),len(T))].mean() for _ in range(2000)])
    z=(T.ret.mean()-bs.mean())/bs.std()
    q1=T[(T.px>=5)&(T.px<21)]; q2=T[(T.px>=21)&(T.px<89)]; q3=T[T.px>=89]
    qm=lambda q: f"{100*q.ret.mean():+.1f}" if len(q)>=30 else "·"
    print(f"{name:22} n={len(T):6,} mean {100*T.ret.mean():+.2f}% med {100*T.ret.median():+.2f}% "
          f"win {100*(T.ret>0).mean():.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr z={z:+.1f}σ | "
          f"TR {trm:+.2f} TE {tem:+.2f} | $5-21 {qm(q1)} $21-89 {qm(q2)} $89+ {qm(q3)}")
    print(f"{'':22} "+" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items()))
