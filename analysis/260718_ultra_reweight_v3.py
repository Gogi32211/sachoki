"""Does REWEIGHTING ultra_score rank better? Build TWO scores historically from the
bars-available A/B-block components and path-sim by quintile (trail25/-15%/60bar), 6yr,
TRAIN/TEST + Spearman rank-corr(score, fwd_ret). MEASUREMENT.
  CURRENT   = the score's breakout+setup weights as-is (ROCKET+20, BUY_2809+20, BX_UP+12…)
  PROPOSED  = drop the anti-predictive breakouts, keep BX_UP/STR/d_absorb, ADD the two
              strongest validated rankers this session showed the score IGNORES: oversold
              (RSI) + price-zone.
Fair caveat: only the A/B core + these adds — the C/D/F profile/intel/pullback blocks are
not in bars. This tests the REWEIGHT of the bars-driven core, which is what I diagnosed.
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

cols=["rocket","sig_buy","bx_up","eb_bull","be_up","bo_up","sig_abs","sig_strong",
      "sig_va","sig_svs","l34","sig_fri34","d_absorb_bull"]
a=get_analytics_conn()
have=set(r[0] for r in a.execute("DESCRIBE bars").fetchall())
cols=[c for c in cols if c in have]
sel=",".join(f"coalesce({c},0) {c}" for c in cols)
df=a.execute(f"""WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,close*volume dv,{sel},
   row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
   FROM bars WHERE close>0 AND date>=DATE '2026-07-16'-INTERVAL 2270 DAY)
   SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
df["date"]=df["date"].astype(str).str[:10]; df["yr"]=df["date"].str[:4]
rets=[]
for tk,g in df.groupby("ticker",sort=False):
    g=g.reset_index(drop=True)
    rets.append(pd.Series(outcomes(g.open.to_numpy(float),g.high.to_numpy(float),g.low.to_numpy(float),g.close.to_numpy(float)),index=g.index))
df["ret"]=pd.concat(rets).values
D=df[(df.ret.notna())&(df.dv>=3_000_000)].copy()
g=lambda c: D[c] if c in D else 0
# CURRENT — breakout+setup weights as in ultra_score.py (A cap35 + B cap25 core)
A=(20*g("rocket")+20*g("sig_buy")+12*g("bx_up")+10*g("eb_bull")+10*g("be_up")+10*g("bo_up")).clip(upper=35)
B=(10*g("sig_abs")+8*g("sig_strong")+8*g("sig_va")+8*g("sig_svs")+6*g("l34")+6*g("sig_fri34")+15*g("d_absorb_bull")).clip(upper=25)
D["cur"]=A+B
# PROPOSED — keep earners, drop dead breakouts, ADD oversold + price zone
rsi=D.rsi_14.fillna(50)
osv=np.select([rsi<30,rsi<35,rsi<50,rsi<60,rsi<70],[20,15,8,0,-8],default=-18)
px=D.close
pzv=np.select([px<8,px<21,px<89],[-12,-6,10],default=3)
D["prop"]=(12*g("bx_up")+8*g("sig_strong")+15*g("d_absorb_bull")).clip(upper=25)+osv+pzv
def report(scorecol,name):
    print(f"\n=== {name}  (Spearman rank-corr score↔fwd: {D[scorecol].corr(D.ret,method='spearman'):+.4f})")
    D["_q"]=pd.qcut(D[scorecol].rank(method='first'),5,labels=['Q1','Q2','Q3','Q4','Q5'])
    print(f"  {'bin':4}{'n':>8}{'mean':>7}{'med':>7}{'win':>6} | {'TRAINm':>8}{'TESTm':>8}")
    for q in ['Q1','Q2','Q3','Q4','Q5']:
        s=D[D._q==q]
        tr=s[s.yr.isin(['2021','2022','2023'])].ret.mean()*100
        te=s[s.yr.isin(['2024','2025','2026'])].ret.mean()*100
        print(f"  {q:4}{len(s):>8}{s.ret.mean()*100:>+7.2f}{s.ret.median()*100:>+7.2f}{(s.ret>0).mean()*100:>5.0f}% |{tr:>+8.2f}{te:>+8.2f}")
    q5=D[D._q=='Q5'].ret; q1=D[D._q=='Q1'].ret
    print(f"  → Q5−Q1 spread: mean {(q5.mean()-q1.mean())*100:+.2f}pp  med {(q5.median()-q1.median())*100:+.2f}pp  win {(q5>0).mean()*100-(q1>0).mean()*100:+.0f}pp")
report("cur","CURRENT weights (breakout-heavy)")
report("prop","PROPOSED reweight (drop dead breakouts + oversold + price-zone)")
