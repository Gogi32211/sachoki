"""Add our VALIDATED axes to the reweight. Compare 3 scores by quintile path-sim (trail25),
6yr, Spearman rank-corr:
  CURRENT      breakout-heavy core (as-is)
  PROP v3      drop dead breakouts + oversold + price-zone
  PROP v4      v3 + 🏆RS-intact + 🎯cluster(conf_n) + 🎋TLS-bar   (this session's edges)
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn
import edge_replay as ER
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

# 1) bars with ultra sig columns + OHLC
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

# 2) merge validated axes from edge_replay frame
grp,as_of=ER._frame(72,3_000_000)
parts=[]
for tk,g in grp.items():
    parts.append(pd.DataFrame({"ticker":tk,"date":g["date"].astype(str).str[:10],
        "rs_intact":g["rs_intact"].fillna(False).astype(bool).values,
        "conf_n":g["conf_n"].values,
        "tls_bar":g["tls_bar"].values if "tls_bar" in g else False}))
E=pd.concat(parts,ignore_index=True)
D=D.merge(E,on=["ticker","date"],how="left")
D["rs_intact"]=D.rs_intact.fillna(False); D["conf_n"]=D.conf_n.fillna(0); D["tls_bar"]=D.tls_bar.fillna(False)

g=lambda c: D[c] if c in D else 0
A=(20*g("rocket")+20*g("sig_buy")+12*g("bx_up")+10*g("eb_bull")+10*g("be_up")+10*g("bo_up")).clip(upper=35)
B=(10*g("sig_abs")+8*g("sig_strong")+8*g("sig_va")+8*g("sig_svs")+6*g("l34")+6*g("sig_fri34")+15*g("d_absorb_bull")).clip(upper=25)
D["cur"]=A+B
rsi=D.rsi_14.fillna(50)
osv=np.select([rsi<30,rsi<35,rsi<50,rsi<60,rsi<70],[20,15,8,0,-8],default=-18)
pzv=np.select([D.close<8,D.close<21,D.close<89],[-12,-6,10],default=3)
keep=(12*g("bx_up")+8*g("sig_strong")+15*g("d_absorb_bull")).clip(upper=25)
D["v3"]=keep+osv+pzv
# validated adds
rsb=np.where(D.rs_intact,12,0)
clb=np.clip(D.conf_n.astype(float),0,6)*4      # +4 per family, cap 24
tlb=np.where(D.tls_bar.astype(bool),10,0)
D["v4"]=keep+osv+pzv+rsb+clb+tlb

def report(sc,name):
    print(f"\n=== {name}  (Spearman {D[sc].corr(D.ret,method='spearman'):+.4f})")
    D["_q"]=pd.qcut(D[sc].rank(method='first'),5,labels=['Q1','Q2','Q3','Q4','Q5'])
    for q in ['Q1','Q2','Q3','Q4','Q5']:
        s=D[D._q==q]
        print(f"  {q} n={len(s):>7} mean {s.ret.mean()*100:>+6.2f} med {s.ret.median()*100:>+6.2f} win {(s.ret>0).mean()*100:>3.0f}%")
    q5=D[D._q=='Q5'].ret; q1=D[D._q=='Q1'].ret
    yp=0
    for y in ['2021','2022','2023','2024','2025','2026']:
        s5=D[(D._q=='Q5')&(D.yr==y)].ret; s1=D[(D._q=='Q1')&(D.yr==y)].ret
        if len(s5)>15 and len(s1)>15: yp+=int(s5.mean()>s1.mean())
    print(f"  → Q5−Q1: mean {(q5.mean()-q1.mean())*100:+.2f}pp med {(q5.median()-q1.median())*100:+.2f}pp win {(q5>0).mean()*100-(q1>0).mean()*100:+.0f}pp · Q5>Q1 {yp}/6yr")
report("cur","CURRENT (breakout-heavy)")
report("v3","PROP v3 (oversold + price)")
report("v4","PROP v4 (+ 🏆RS + 🎯cluster + 🎋TLS)")
