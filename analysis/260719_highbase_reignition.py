"""HIGH-BASE RE-IGNITION (the SNDK-Aug-20 composition), universe path-sim.
Pre-declared rules (from the 4-case anatomy, NOT grid-searched):
  S (ignition): turbo_score >= 50
  hold (S..D]: min(close) >= close[S]*0.97  AND  min(RSI) > 48  AND  min(CCI) > 0
  D (quiet entry): 4..9 bars after S, volume <= 0.8*avg20 AND turbo <= 10
  liquidity at D: dv>=3M, px>=5. Enter next open, exit trail25/-15%/60d.
Ablations (multiple-testing honesty — reported, not cherry-picked): drop-CCI, drop-RSI.
Random same-size control + per-year + price buckets + TRAIN/TEST.
"""
import sys, numpy as np, pandas as pd, duckdb
S_,TRAIL,HARD,MAXH=0.0015,0.25,0.15,60
def sim(o,hi,lo,cl,start,entry):
    n=len(cl); e=entry*(1+S_)
    if e<=0 or start>=n: return None
    pk=e; hard=e*(1-HARD); end=min(start+MAXH,n)
    for j in range(start,end):
        if o[j]<=hard and j>start: return o[j]/e-1-S_
        if lo[j]<=hard: return -HARD-S_
        pk=max(pk,hi[j]); ts=pk*(1-TRAIL)
        if o[j]<=ts and j>start: return o[j]/e-1-S_
        if lo[j]<=ts: return ts/e-1-S_
    return cl[end-1]/e-1-S_
a=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume,avg_vol_20d,
  rsi_14,cci_20,turbo_score,close*volume dv,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D["date"]=D["date"].astype(str).str[:10]
recs={'full':[], 'noCCI':[], 'noRSI':[]}
allrets=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<80: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    rs=g.rsi_14.to_numpy(float); cc=g.cci_20.to_numpy(float)
    tb=g.turbo_score.to_numpy(float); v=g.volume.to_numpy(float); av=g.avg_vol_20d.to_numpy(float)
    dv=g.dv.to_numpy(float); ds=g.date.tolist()
    # baseline pool
    for i in range(0,n-1,7):
        if dv[i]>=3e6:
            r=sim(o,hi,lo,cl,i+1,o[i+1])
            if r is not None: allrets.append(r)
    lastD=-99
    for s in np.flatnonzero(tb>=50):
        if s+5>=n: continue
        for d_ in range(s+4,min(s+10,n-1)):
            if d_-lastD<5: continue
            hold_px=cl[s+1:d_+1].min()>=cl[s]*0.97
            hold_rsi=np.nanmin(rs[s+1:d_+1])>48
            hold_cci=np.nanmin(cc[s+1:d_+1])>0
            quiet=(v[d_]<=0.8*max(av[d_],1)) and tb[d_]<=10
            if not quiet or dv[d_]<3e6: continue
            r=sim(o,hi,lo,cl,d_+1,o[d_+1])
            if r is None: continue
            yr=ds[d_][:4]; px=cl[d_]
            if hold_px and hold_rsi and hold_cci:
                recs['full'].append((yr,px,r)); lastD=d_; break
            if hold_px and hold_rsi:
                recs['noCCI'].append((yr,px,r))
            if hold_px and hold_cci:
                recs['noRSI'].append((yr,px,r))
allr=np.array(allrets)
print(f"baseline pool: n={len(allr)}  mean {allr.mean()*100:+.2f}%  med {np.median(allr)*100:+.2f}%  win {(allr>0).mean()*100:.0f}%\n")
rng=np.random.default_rng(7)
for name,rows in recs.items():
    R=pd.DataFrame(rows,columns=["yr","px","ret"])
    if len(R)<50: print(f"{name}: n={len(R)} too few"); continue
    yrs=R.groupby("yr").ret.mean()*100
    w=(R.ret>0).mean()*100; pfd=-R.ret[R.ret<=0].sum()
    pf=R.ret[R.ret>0].sum()/pfd if pfd>0 else float('nan')
    draws=np.array([rng.choice(allr,len(R),replace=False).mean() for _ in range(300)])*100
    z=(R.ret.mean()*100-draws.mean())/draws.std()
    tr=R[R.yr.isin(['2021','2022','2023'])]; te=R[R.yr.isin(['2024','2025','2026'])]
    print(f"=== {name}  n={len(R)}")
    print(f"  mean {R.ret.mean()*100:+.2f}%  med {R.ret.median()*100:+.2f}%  win {w:.0f}%  PF {pf:.2f}  vs random {draws.mean():+.2f}±{draws.std():.2f} → {z:+.1f}σ")
    print(f"  per-yr: "+" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items())+f"  ({int((yrs>0).sum())}/{len(yrs)}+)")
    if len(tr)>30 and len(te)>30:
        print(f"  TRAIN {tr.ret.mean()*100:+.2f}% (n={len(tr)})  TEST {te.ret.mean()*100:+.2f}% (n={len(te)})")
    for lo_,hi_,bl in [(5,21,'$5-21'),(21,89,'$21-89'),(89,1e9,'$89+')]:
        q=R[(R.px>=lo_)&(R.px<hi_)]
        if len(q)>=30: print(f"    {bl:8} n={len(q):5} mean {q.ret.mean()*100:+.2f}%  med {q.ret.median()*100:+.2f}%  win {(q.ret>0).mean()*100:.0f}%")
    print()
