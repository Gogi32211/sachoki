"""For each score system, WHAT VALUE RANGE is interesting, and is it a REVERSAL or a
BREAKOUT signal? Bin each score into quintiles over 6yr; per bin report forward path-sim
(trail25/-15%/60bar) + the ENTRY STATE (median RSI, median distance below the 20-day high).
Low RSI + far below high = fires in a REVERSAL/oversold state; high RSI + at the high =
fires in a BREAKOUT/continuation state. Liquid $3M+; MEASUREMENT.
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
# stored scores + components for v3-core, + rsi/price
have=set(r[0] for r in a.execute("DESCRIBE bars").fetchall())
comp=[c for c in ["bx_up","sig_strong","d_absorb_bull"] if c in have]
csel=",".join(f"coalesce({c},0) {c}" for c in comp)
df=a.execute(f"""WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,close*volume dv,
   prebreak_v2, prebreak_v3, rtb_total, beta_score, turbo_score, {csel},
   row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
   FROM bars WHERE close>0 AND date>=DATE '2026-07-16'-INTERVAL 2270 DAY)
   SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
# forward outcome + dist from 20d high (state at entry)
rets=[]; dist=[]
for tk,g in df.groupby("ticker",sort=False):
    g=g.reset_index(drop=True)
    o=g.open.to_numpy(float);hi=g.high.to_numpy(float);lo=g.low.to_numpy(float);cl=g.close.to_numpy(float)
    rets.append(pd.Series(outcomes(o,hi,lo,cl),index=g.index))
    h20=g.high.rolling(20,min_periods=5).max().to_numpy()
    dist.append(pd.Series((cl/h20-1.0)*100,index=g.index))   # % below 20d high (0=at high, -30=30% below)
df["ret"]=pd.concat(rets).values
df["dist"]=pd.concat(dist).values
# v3-core (no edge axes) so we can characterize its zone too
rsi=df.rsi_14.fillna(50)
osv=np.select([rsi<30,rsi<35,rsi<50,rsi<60,rsi<70],[20,15,8,0,-8],default=-18)
pzv=np.select([df.close<8,df.close<21,df.close<89],[-12,-6,10],default=3)
gc=lambda c: df[c] if c in df else 0
df["uv3_core"]=(12*gc("bx_up")+8*gc("sig_strong")+15*gc("d_absorb_bull")).clip(upper=25)+osv+pzv
D=df[(df.ret.notna())&(df.dv>=3_000_000)&(df.dist.notna())].copy()
bm=D.ret.mean()*100
print(f"baseline fwd mean {bm:+.2f}%  n={len(D)}\n")

SCORES=[("Score(prebreak_v2)","prebreak_v2"),("V3(prebreak_v3)","prebreak_v3"),
        ("RTB(rtb_total)","rtb_total"),("BETA(beta_score)","beta_score"),
        ("TURBO(turbo_score)","turbo_score"),("UV3-core(mine)","uv3_core")]
for name,col in SCORES:
    d=D[D[col].notna()].copy()
    if len(d)<500: print(f"{name}: too few"); continue
    try:
        d["_q"]=pd.qcut(d[col].rank(method='first'),5,labels=['Q1','Q2','Q3','Q4','Q5'])
    except Exception:
        print(f"{name}: cannot bin"); continue
    sp=d[col].corr(d.ret,method='spearman')
    print("="*96)
    print(f"{name}   (Spearman score↔fwd {sp:+.3f})")
    print(f"  {'bin':4} {'value':>12} {'n':>7} {'fwd_mean':>9}{'win':>5} | {'medRSI':>7}{'dist<20dHi':>10}  state")
    for q in ['Q1','Q2','Q3','Q4','Q5']:
        s=d[d._q==q]
        vlo,vhi=s[col].min(),s[col].max()
        mrsi=s.rsi_14.median(); mdist=s.dist.median()
        # label state
        st = "REVERSAL/oversold" if (mrsi<45 and mdist<-8) else ("BREAKOUT/at-high" if (mrsi>=50 and mdist>-5) else "mid")
        print(f"  {q:4} {vlo:5.0f}-{vhi:<6.0f} {len(s):>7} {s.ret.mean()*100:>+8.2f}%{(s.ret>0).mean()*100:>4.0f}% | {mrsi:>6.0f} {mdist:>+8.1f}%  {st}")
    best=d.groupby('_q',observed=True).ret.mean().idxmax()
    print(f"  → best zone: {best}")
    print()
