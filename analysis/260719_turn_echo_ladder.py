"""LOOSE daily up-turn (close up + RSI rising + RSI<55, NO depth requirement — the AMD case)
× how many intraday TFs (4h/1h/15m) printed the strict REV-turn on D or D-1. Ladder 0-3.
REV day-sets computed in pure SQL (window funcs) — fast even on the 89M-row 15m DB."""
import sys, numpy as np, pandas as pd, duckdb
S,TRAIL,HARD,MAXH=0.0015,0.25,0.15,60
def sim(o,hi,lo,cl,start,entry):
    n=len(cl); e=entry*(1+S)
    if e<=0 or start>=n: return None
    pk=e; hard=e*(1-HARD); end=min(start+MAXH,n)
    for j in range(start,end):
        if o[j]<=hard and j>start: return o[j]/e-1-S
        if lo[j]<=hard: return -HARD-S
        pk=max(pk,hi[j]); ts=pk*(1-TRAIL)
        if o[j]<=ts and j>start: return o[j]/e-1-S
        if lo[j]<=ts: return ts/e-1-S
    return cl[end-1]/e-1-S
REV_SQL="""SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d FROM (
  SELECT ticker,date,rsi_14,close,
    MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) cp,
    LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date) rp
  FROM bars WHERE close>=5)
 WHERE m5<38 AND rsi_14 BETWEEN 30 AND 55 AND close>cp AND rsi_14>rp"""
def revset(db):
    c=duckdb.connect(db,read_only=True)
    df=c.execute(REV_SQL).fetchdf(); c.close()
    return set(zip(df.ticker,df.d))
print("4h...",flush=True); C4=revset('../data/studio_4h.duckdb'); print(len(C4),flush=True)
print("1h...",flush=True); C1=revset('../data/studio_1h.duckdb'); print(len(C1),flush=True)
print("15m...",flush=True); C15=revset('../data/studio_15m.duckdb'); print(len(C15),flush=True)
ad=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=ad.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,close*volume dv,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
ad.close()
rec=[]
for tk,gd in D.groupby("ticker",sort=False):
    gd=gd.reset_index(drop=True)
    if len(gd)<30: continue
    o,hi,lo,cl=(gd[c].to_numpy(float) for c in("open","high","low","close"))
    rs=gd.rsi_14.to_numpy(float); dv=gd.dv.to_numpy(float)
    ds=[str(x)[:10] for x in gd.date]
    for di in range(1,len(gd)-1):
        if not (cl[di]>cl[di-1] and rs[di]>rs[di-1] and rs[di]<55) or dv[di]<3e6: continue
        r=sim(o,hi,lo,cl,di+1,o[di+1])
        if r is None: continue
        n_=sum(1 for Cs in (C4,C1,C15) if (tk,ds[di]) in Cs or (tk,ds[di-1]) in Cs)
        rec.append((ds[di][:4],cl[di],n_,r))
R=pd.DataFrame(rec,columns=["yr","px","n","ret"])
print(f"\nloose daily up-turns: n={len(R)}\n")
for k in (0,1,2,3):
    s=R[R.n==k]
    yp=0;yt=0
    for y in sorted(R.yr.unique()):
        sy=s[s.yr==y]; by=R[R.yr==y]
        if len(sy)>=50: yt+=1; yp+=int(sy.ret.mean()>by.ret.mean())
    q=s[(s.px>=21)&(s.px<89)]
    print(f"  {k}/3  n={len(s):7}  mean {s.ret.mean()*100:+.2f}%  med {s.ret.median()*100:+.2f}%  win {(s.ret>0).mean()*100:.0f}%  beats {yp}/{yt}yr  $21-89 {q.ret.mean()*100:+.2f}%")
