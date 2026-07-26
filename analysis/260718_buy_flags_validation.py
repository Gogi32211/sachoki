"""Historical quality of the EXACT built flags (not zone bins), path-sim per year.
rev_buy = min5-RSI<38 & RSI 30-55 & up-bar & beta_score<=13
brk_buy = RSI crosses 50 up & up-bar & turbo_score<=28
trail25/-15%/60bar, liquid $3M+, 6yr, TRAIN/TEST + random-same-size control + price bucket.
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn
S,TRAIL,HARD,MAXH=0.0015,0.25,0.15,60
def outcomes(o,hi,lo,cl):
    n=len(o); out=np.full(n,np.nan)
    for i in range(n-1):
        e=o[i+1]
        if e<=0: continue
        e*=(1+S); pk=e; hard=e*(1-HARD); end=min(i+1+MAXH,n); r=None
        for j in range(i+1,end):
            if j>i+1 and o[j]<=hard: r=o[j]/e-1-S; break
            if lo[j]<=hard: r=-HARD-S; break
            pk=max(pk,hi[j]); ts=pk*(1-TRAIL)
            if j>i+1 and o[j]<=ts: r=o[j]/e-1-S; break
            if lo[j]<=ts: r=ts/e-1-S; break
        out[i]= r if r is not None else cl[end-1]/e-1-S
    return out
a=get_analytics_conn()
df=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,close*volume dv,
   turbo_score,beta_score,
   row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
   FROM bars WHERE close>0 AND date>=DATE '2026-07-16'-INTERVAL 2270 DAY)
   SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
df["date"]=df["date"].astype(str).str[:10]; df["yr"]=df["date"].str[:4]
rets=[];rp=[];cp=[];m5=[]
for tk,g in df.groupby("ticker",sort=False):
    g=g.reset_index(drop=True)
    rets.append(pd.Series(outcomes(g.open.to_numpy(float),g.high.to_numpy(float),g.low.to_numpy(float),g.close.to_numpy(float)),index=g.index))
    rs=g.rsi_14.to_numpy(float); cl=g.close.to_numpy(float)
    rp.append(pd.Series(np.concatenate([[np.nan],rs[:-1]]),index=g.index))
    cp.append(pd.Series(np.concatenate([[np.nan],cl[:-1]]),index=g.index))
    m5.append(pd.Series(pd.Series(rs).rolling(5,min_periods=2).min().to_numpy(),index=g.index))
df["ret"]=pd.concat(rets).values; df["rp"]=pd.concat(rp).values; df["cp"]=pd.concat(cp).values; df["m5"]=pd.concat(m5).values
D=df[(df.ret.notna())&(df.dv>=3_000_000)&(df.rsi_14.notna())&(df.cp.notna())].copy()
bm=D.ret.mean()*100
REV=(D.m5<38)&(D.rsi_14.between(30,55))&(D.close>D.cp)&(D.beta_score<=13)
BRK=(D.rp<50)&(D.rsi_14>=50)&(D.close>D.cp)&(D.turbo_score<=28)
rng=np.random.default_rng(4)
print(f"liquid baseline fwd mean {bm:+.2f}%  n={len(D)}\n")
def rep(name,mask):
    s=D[mask]
    if len(s)<30: print(f"{name}: n={len(s)} too few"); return
    yrs=s.groupby('yr').ret.mean()*100
    w=(s.ret>0).mean()*100; pfd=-s.ret[s.ret<=0].sum(); pf=s.ret[s.ret>0].sum()/pfd if pfd>0 else float('nan')
    draws=np.array([rng.choice(D.ret.to_numpy(),len(s),replace=False).mean() for _ in range(300)])*100
    z=(s.ret.mean()*100-draws.mean())/draws.std()
    print(f"{name}")
    print(f"  n={len(s)}  mean {s.ret.mean()*100:+.2f}%  med {s.ret.median()*100:+.2f}%  win {w:.0f}%  PF {pf:.2f}  ·  vs random {draws.mean():+.2f}±{draws.std():.2f} → {z:+.1f}σ")
    print(f"  per-yr: "+" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items())+f"  ({int((yrs>0).sum())}/{len(yrs)}+)")
    for lo,hi,bl in [(5,21,'$5-21'),(21,89,'$21-89'),(89,1e9,'$89+')]:
        q=s[(s.close>=lo)&(s.close<hi)]
        if len(q)>=30: print(f"    {bl:8} n={len(q):5} mean {q.ret.mean()*100:+.2f}%  med {q.ret.median()*100:+.2f}%  win {(q.ret>0).mean()*100:.0f}%")
    print()
rep("🟢 REV-buy (oversold bounce + beta≤13)",REV)
rep("🔵 BRK-buy (RSI>50 cross + turbo≤28)",BRK)
