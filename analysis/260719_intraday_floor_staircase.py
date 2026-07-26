"""RIDE ONSET on intraday floors. Hypothesis (from SNDK): the transition A→B is marked by
the INTRADAY RSI FLOOR starting a monotone staircase — during accumulation the 1h/4h RSI
keeps dipping (that's why ▲ triggers fire); at onset the dips STOP and each day's intraday
low-RSI is higher than the previous day's.
Part 1: show the 1h/4h daily RSI-floor staircase around each known onset (SNDK/RKLB/IONQ/MXL).
Part 2: universe test, pre-declared rule: 3 consecutive rising 1h-RSI-floors, all >=40,
        last >=45, AND daily close > close 5 bars ago → enter next open, trail25/-15%/60d.
"""
import numpy as np, pandas as pd, duckdb
def day_floor(db):
    c=duckdb.connect(f'../data/studio_{db}.duckdb',read_only=True)
    df=c.execute("""SELECT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d,
        min(rsi_14) rlo FROM bars WHERE close>=5 GROUP BY 1,2""").fetchdf()
    c.close()
    return {(t,d_):r for t,d_,r in zip(df.ticker,df.d,df.rlo)}
print("floors 1h...",flush=True); F1=day_floor('1h'); print(len(F1),flush=True)
print("floors 4h...",flush=True); F4=day_floor('4h'); print(len(F4),flush=True)
a=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,close*volume dv,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D["date"]=D["date"].astype(str).str[:10]
CASES=[('SNDK','2025-08-13','2025-09-03'),('RKLB','2025-11-26','2025-12-12'),
       ('IONQ','2026-04-06','2026-04-21'),('MXL','2026-04-01','2026-04-20')]
print("\n=== the 1h-floor staircase around each onset (1h-floor / 4h-floor / close):")
for tk,lo,hi in CASES:
    g=D[(D.ticker==tk)&(D.date>=lo)&(D.date<=hi)]
    print(f"\n{tk}:")
    prev=None
    for _,r in g.iterrows():
        f1=F1.get((tk,r.date)); f4=F4.get((tk,r.date))
        up='↑' if (prev is not None and f1 is not None and f1>prev) else ' '
        print(f"  {r.date}  1hFloor {f1 if f1 is not None else float('nan'):5.1f}{up}  4hFloor {f4 if f4 is not None else float('nan'):5.1f}  close {r.close:8.2f}")
        prev=f1
S_=0.0015
def sim(o,hi_,lo_,cl,start,entry):
    n=len(cl); e=entry*(1+S_)
    if e<=0 or start>=n: return None
    pk=e; hd=e*0.85; end=min(start+60,n)
    for j in range(start,end):
        if o[j]<=hd and j>start: return o[j]/e-1-S_
        if lo_[j]<=hd: return -0.15-S_
        pk=max(pk,hi_[j]); ts=pk*0.75
        if o[j]<=ts and j>start: return o[j]/e-1-S_
        if lo_[j]<=ts: return ts/e-1-S_
    return cl[end-1]/e-1-S_
print("\n=== UNIVERSE TEST: 3 rising 1h-floors (all>=40, last>=45) + daily 5d-up → path-sim",flush=True)
rec=[]; pool=[]
rng=np.random.default_rng(3)
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<30: continue
    o,hi_,lo_,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    dv=g.dv.to_numpy(float); ds=g.date.tolist()
    fl=np.array([F1.get((tk,d_),np.nan) for d_ in ds])
    for i in range(0,n-1,13):
        if dv[i]>=3e6:
            r=sim(o,hi_,lo_,cl,i+1,o[i+1])
            if r is not None: pool.append(r)
    last=-99
    for i in range(6,n-1):
        if i-last<5 or dv[i]<3e6: continue
        f=fl[i-2:i+1]
        if np.isnan(f).any(): continue
        if not (f[0]<f[1]<f[2] and f.min()>=40 and f[2]>=45): continue
        if not cl[i]>cl[i-5]: continue
        r=sim(o,hi_,lo_,cl,i+1,o[i+1])
        if r is None: continue
        rec.append((ds[i][:4],cl[i],r)); last=i
R=pd.DataFrame(rec,columns=["yr","px","ret"]); pool=np.array(pool)
print(f"signals n={len(R)}  baseline pool {len(pool)} mean {pool.mean()*100:+.2f}%")
w=(R.ret>0).mean()*100; pfd=-R.ret[R.ret<=0].sum(); pf=R.ret[R.ret>0].sum()/pfd if pfd>0 else float('nan')
draws=np.array([rng.choice(pool,len(R),replace=False).mean() for _ in range(300)])*100
z=(R.ret.mean()*100-draws.mean())/draws.std()
print(f"mean {R.ret.mean()*100:+.2f}%  med {R.ret.median()*100:+.2f}%  win {w:.0f}%  PF {pf:.2f}  vs random {draws.mean():+.2f}±{draws.std():.2f} → {z:+.1f}σ")
yrs=R.groupby('yr').ret.mean()*100
print("per-yr: "+" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items())+f"  ({int((yrs>0).sum())}/{len(yrs)}+)")
tr=R[R.yr.isin(['2021','2022','2023'])]; te=R[R.yr.isin(['2024','2025','2026'])]
print(f"TRAIN {tr.ret.mean()*100:+.2f}% (n={len(tr)})  TEST {te.ret.mean()*100:+.2f}% (n={len(te)})")
for lo_,hi2,bl in [(5,21,'$5-21'),(21,89,'$21-89'),(89,1e9,'$89+')]:
    q=R[(R.px>=lo_)&(R.px<hi2)]
    if len(q)>=30: print(f"  {bl:8} n={len(q):5} mean {q.ret.mean()*100:+.2f}%  med {q.ret.median()*100:+.2f}%  win {(q.ret>0).mean()*100:.0f}%")
