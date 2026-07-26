"""Two zones formally, ALL scores. REVERSAL = bouncing off oversold; BREAKOUT = RSI crossing
50 up. For each zone, per score, which value-range best predicts the forward move (trail25).
Scores: SCORE(buy_score), UV3-core, turbo, prebreak_v2, prebreak_v3, rtb_total, beta_score
(ULTRA not stored at scale — it tracked turbo in AMD). Liquid $3M+, 6yr, TRAIN/TEST.
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn
from buy_score import compute_buy_score
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
have=set(r[0] for r in a.execute("DESCRIBE bars").fetchall())
comp=[c for c in ["bx_up","sig_strong","d_absorb_bull"] if c in have]
csel=",".join(f"coalesce({c},0) {c}" for c in comp)
df=a.execute(f"""WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,vol_bucket,close*volume dv,
   turbo_score,prebreak_v2,prebreak_v3,rtb_total,beta_score,{csel},
   row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
   FROM bars WHERE close>0 AND date>=DATE '2026-07-16'-INTERVAL 2270 DAY)
   SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
df["date"]=df["date"].astype(str).str[:10]; df["yr"]=df["date"].str[:4]
# per-ticker forward outcome + rsi features
rets=[]; feat=[]
for tk,g in df.groupby("ticker",sort=False):
    g=g.reset_index(drop=True)
    o=g.open.to_numpy(float);hi=g.high.to_numpy(float);lo=g.low.to_numpy(float);cl=g.close.to_numpy(float)
    rs=g.rsi_14.to_numpy(float)
    rets.append(pd.Series(outcomes(o,hi,lo,cl),index=g.index))
    rsi_prev=np.concatenate([[np.nan],rs[:-1]])
    cl_prev=np.concatenate([[np.nan],cl[:-1]])
    min5=pd.Series(rs).rolling(5,min_periods=2).min().to_numpy()
    feat.append(pd.DataFrame({"rsi_prev":rsi_prev,"cl_prev":cl_prev,"min5rsi":min5},index=g.index))
df["ret"]=pd.concat(rets).values
F=pd.concat(feat); df["rsi_prev"]=F.rsi_prev.values; df["cl_prev"]=F.cl_prev.values; df["min5rsi"]=F.min5rsi.values
# UV3-core
rsi=df.rsi_14.fillna(50)
osv=np.select([rsi<30,rsi<35,rsi<50,rsi<60,rsi<70],[20,15,8,0,-8],default=-18)
pzv=np.select([df.close<8,df.close<21,df.close<89],[-12,-6,10],default=3)
gc=lambda c: df[c] if c in df else 0
df["uv3"]=(12*gc("bx_up")+8*gc("sig_strong")+15*gc("d_absorb_bull")).clip(upper=25)+osv+pzv

D=df[(df.ret.notna())&(df.dv>=3_000_000)&(df.rsi_14.notna())&(df.cl_prev.notna())].copy()
# buy_score on the liquid set (subset, module)
D["buy"]=[compute_buy_score(v2,r,vb)["buy_score"] for v2,r,vb in zip(D.prebreak_v2,D.rsi_14,D.vol_bucket)]
bm=D.ret.mean()*100
print(f"liquid baseline fwd mean {bm:+.2f}%  med {D.ret.median()*100:+.2f}%  n={len(D)}\n")

REV = (D.min5rsi<38)&(D.rsi_14.between(30,55))&(D.close>D.cl_prev)          # bouncing off oversold
BRK = (D.rsi_prev<50)&(D.rsi_14>=50)&(D.close>D.cl_prev)                    # RSI crosses 50 up
SCORES=[("SCORE(buy)","buy"),("UV3","uv3"),("turbo","turbo_score"),
        ("prebreak_v2","prebreak_v2"),("prebreak_v3","prebreak_v3"),("rtb","rtb_total"),("beta","beta_score")]

for zname,zmask in [("REVERSAL (bounce off oversold)",REV),("BREAKOUT (RSI crosses 50 up)",BRK)]:
    z=D[zmask].copy()
    zb=z.ret.mean()*100
    print("="*100)
    print(f"{zname}   n={len(z)} ({len(z)/len(D)*100:.1f}% of bars)  ·  zone fwd mean {zb:+.2f}%  med {z.ret.median()*100:+.2f}%  win {(z.ret>0).mean()*100:.0f}%")
    print("="*100)
    for sname,scol in SCORES:
        s=z[z[scol].notna()].copy()
        if len(s)<200: print(f"  {sname:12} too few"); continue
        try: s["_q"]=pd.qcut(s[scol].rank(method='first'),5,labels=['Q1','Q2','Q3','Q4','Q5'])
        except: continue
        # best & worst quintile forward mean + their value ranges
        gg=s.groupby('_q',observed=True)
        m=gg.ret.mean()*100; best=m.idxmax(); worst=m.idxmin()
        blo,bhi=s[s._q==best][scol].min(),s[s._q==best][scol].max()
        bwin=(s[s._q==best].ret>0).mean()*100
        # per-year robustness of best-vs-zone
        yp=0;yt=0
        for y in ['2021','2022','2023','2024','2025','2026']:
            sy=s[(s._q==best)&(s.yr==y)].ret; zy=z[z.yr==y].ret
            if len(sy)>=15 and len(zy): yt+=1; yp+=int(sy.mean()>zy.mean())
        lift=m[best]-zb
        print(f"  {sname:12} best {best} @ {blo:.0f}-{bhi:.0f}: fwd {m[best]:+.2f}% (lift {lift:+.2f}) win {bwin:.0f}% {yp}/{yt}yr | worst {worst} {m[worst]:+.2f}")
    print()
