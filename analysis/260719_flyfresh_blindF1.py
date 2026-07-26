"""BLIND F1 — the clean tradeable arm: enter on EVERY first FLY after >=15-bar absence
(no knowledge of whether it returns). Path-sim trail25/-15%/60d, per-year, TRAIN/TEST,
price buckets, random control. Plus the 21-89 quality-zone variant."""
import numpy as np, pandas as pd, duckdb
S_=0.0015
def sim(o,hi,lo,cl,start,entry):
    n=len(cl); e=entry*(1+S_)
    if e<=0 or start>=n: return None
    pk=e; hd=e*0.85; end=min(start+60,n)
    for j in range(start,end):
        if o[j]<=hd and j>start: return o[j]/e-1-S_
        if lo[j]<=hd: return -0.15-S_
        pk=max(pk,hi[j]); ts=pk*0.75
        if o[j]<=ts and j>start: return o[j]/e-1-S_
        if lo[j]<=ts: return ts/e-1-S_
    return cl[end-1]/e-1-S_
a=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,close*volume dv,rsi_14,
  CASE WHEN coalesce(fly_sig,'')<>'' THEN 1 ELSE 0 END fly,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D["date"]=D["date"].astype(str).str[:10]
rec=[]; pool=[]
rng=np.random.default_rng(9)
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<40: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    fly=g.fly.to_numpy(int); dv=g.dv.to_numpy(float); ds=g.date.tolist(); rs=g.rsi_14.to_numpy(float)
    for i in range(0,n-1,17):
        if dv[i]>=3e6:
            r=sim(o,hi,lo,cl,i+1,o[i+1])
            if r is not None: pool.append(r)
    for i in range(16,n-1):
        if fly[i]==1 and fly[i-15:i].sum()==0 and dv[i]>=3e6:
            r=sim(o,hi,lo,cl,i+1,o[i+1])
            if r is not None: rec.append((ds[i][:4],cl[i],rs[i],r))
R=pd.DataFrame(rec,columns=["yr","px","rsi","ret"]); pool=np.array(pool)
print(f"blind-F1 n={len(R)} · baseline {pool.mean()*100:+.2f}%\n")
def rep(lab,s):
    if len(s)<100: print(f"{lab:22} n={len(s)} too few"); return
    w=(s.ret>0).mean()*100; pfd=-s.ret[s.ret<=0].sum(); pf=s.ret[s.ret>0].sum()/pfd if pfd>0 else float('nan')
    draws=np.array([rng.choice(pool,len(s),replace=False).mean() for _ in range(300)])*100
    z=(s.ret.mean()*100-draws.mean())/draws.std()
    yrs=s.groupby('yr').ret.mean()*100
    tr=s[s.yr.isin(['2021','2022','2023'])]; te=s[s.yr.isin(['2024','2025','2026'])]
    print(f"{lab:22} n={len(s):6} mean {s.ret.mean()*100:+.2f}% med {s.ret.median()*100:+.2f}% win {w:.0f}% PF {pf:.2f} {z:+.1f}σ {int((yrs>0).sum())}/{len(yrs)}yr | TR {tr.ret.mean()*100:+.2f} TE {te.ret.mean()*100:+.2f}")
    print("   per-yr: "+" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items()))
rep("blind F1 (ALL)",R)
rep("F1 · $21-89",R[(R.px>=21)&(R.px<89)])
rep("F1 · $21+",R[R.px>=21])
rep("F1 · RSI<55 (not ext)",R[R.rsi<55])
rep("F1 · $21+ & RSI<55",R[(R.px>=21)&(R.rsi<55)])
