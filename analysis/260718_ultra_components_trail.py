"""ultra_score components, CLEAN exit (trail25 / -15% / 60bar — no +100% jackpot distortion).
Report median (robust to tail) + mean + win + per-year + $21-89. MEASUREMENT."""
import sys, numpy as np, pandas as pd
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
from ai_journal.db import get_analytics_conn
S,TRAIL,HARD,MAXH=0.0015,0.25,0.15,60

def outcomes(o,hi,lo,cl):
    n=len(o); out=np.full(n,np.nan)
    for i in range(n-1):
        e=o[i+1]
        if e<=0: continue
        e*= (1+S); pk=e; hard=e*(1-HARD); end=min(i+1+MAXH,n); r=None
        for j in range(i+1,end):
            if j>i+1 and o[j]<=hard: r=o[j]/e-1-S; break
            if lo[j]<=hard: r=-HARD-S; break
            pk=max(pk,hi[j]); ts=pk*(1-TRAIL)
            if j>i+1 and o[j]<=ts: r=o[j]/e-1-S; break
            if lo[j]<=ts: r=ts/e-1-S; break
        out[i]= r if r is not None else cl[end-1]/e-1-S
    return out

COMP=[("ROCKET","rocket",20),("BUY_2809","sig_buy",20),("BX_UP","bx_up",12),
      ("EB_BULL","eb_bull",10),("BO_UP","bo_up",10),("BE_UP","be_up",10),
      ("ABS","sig_abs",10),("STR","sig_strong",8),("VA","sig_va",8),("SVS","sig_svs",8),
      ("L34","l34",6),("FRI34","sig_fri34",6),("d_absorb_bull","d_absorb_bull",15)]
a=get_analytics_conn()
allc=set(r[0] for r in a.execute("DESCRIBE bars").fetchall())
COMP=[(l,c,w) for l,c,w in COMP if c in allc]
sel=",".join(f"coalesce({c},0) {c}" for _,c,_ in COMP)
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
bm=D.ret.mean()*100; bmd=D.ret.median()*100
print(f"baseline: mean {bm:+.2f}%  med {bmd:+.2f}%  win {(D.ret>0).mean()*100:.0f}%  n={len(D)}\n")
print(f"{'component':15}{'wt':>4}{'n':>8}{'mean':>7}{'med':>7}{'mLift':>7}{'win':>5} |{'TRAINm':>8}{'TESTm':>7} {'y+':>4} $21-89m")
print("-"*92)
def md(s): s=s.dropna(); return (s.mean()*100 if len(s) else np.nan, s.median()*100 if len(s) else np.nan)
for lab,col,wt in sorted(COMP,key=lambda x:-x[2]):
    sub=D[D[col]==1]
    if len(sub)<50: print(f"{lab:15}{wt:>4}{len(sub):>8} few"); continue
    sm,smd=md(sub.ret)
    trm=md(sub[sub.yr.isin(['2021','2022','2023'])].ret)[0]-md(D[D.yr.isin(['2021','2022','2023'])].ret)[0]
    tem=md(sub[sub.yr.isin(['2024','2025','2026'])].ret)[0]-md(D[D.yr.isin(['2024','2025','2026'])].ret)[0]
    yp=yt=0
    for y in ['2021','2022','2023','2024','2025','2026']:
        sy=sub[sub.yr==y].ret.dropna(); by=D[D.yr==y].ret.dropna()
        if len(sy)>=15 and len(by): yt+=1; yp+=int(sy.mean()>by.mean())
    q=sub[(sub.close>=21)&(sub.close<89)]; qm=md(q.ret)[0] if len(q)>=30 else np.nan
    print(f"{lab:15}{wt:>4}{len(sub):>8}{sm:>+7.2f}{smd:>+7.2f}{sm-bm:>+7.2f}{(sub.ret>0).mean()*100:>4.0f}% |{trm:>+8.2f}{tem:>+7.2f} {yp:>2}/{yt} {qm:+.2f}")
print("\nREFERENCE:")
for lab,mask in [("RSI<30",D.rsi_14<30),("RSI<35",D.rsi_14<35),("RSI 35-50",(D.rsi_14>=35)&(D.rsi_14<50)),("RSI>70",D.rsi_14>70),("$21-89",(D.close>=21)&(D.close<89)),("$5-21",(D.close>=5)&(D.close<21)),("$89+",D.close>=89)]:
    sub=D[mask]; sm,smd=md(sub.ret)
    print(f"  {lab:10} n={len(sub):7} mean {sm:+.2f} (lift {sm-bm:+.2f}) med {smd:+.2f} win {(sub.ret>0).mean()*100:.0f}%")
