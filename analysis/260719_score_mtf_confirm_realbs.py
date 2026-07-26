"""RE-RUN with the REAL intraday buy_score (prebreak_v2 now filled on 4h/1h).
1D condition: buy_score>=60. Confirm: the SAME buy_score formula on 4H/1H >= 60
on day D or D-1 (computed in SQL from the intraday v2+rsi+vol_bucket)."""
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

BS_SQL = """LEAST(GREATEST((1.5*LEAST(GREATEST(COALESCE(prebreak_v2,0),0),27)
  + 12*(CASE WHEN upper(COALESCE(vol_bucket,''))='B' THEN 1 ELSE 0 END)
  + 0.9*GREATEST(0, 55-COALESCE(rsi_14,50)))*1.3, 0), 100)"""
VETO = f"""CASE WHEN rsi_14>=60 THEN LEAST({BS_SQL},20)
  WHEN rsi_14<28 THEN LEAST({BS_SQL},60) ELSE {BS_SQL} END"""

def confirm_days(db):
    c=duckdb.connect(db,read_only=True)
    df=c.execute(f"""SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d
        FROM bars WHERE close>=5 AND ({VETO})>=60""").fetchdf()
    c.close()
    return set(zip(df.ticker,df.d))
print("4h buy_score>=60 days...",flush=True); C4=confirm_days('../data/studio_4h.duckdb'); print(len(C4),flush=True)
print("1h buy_score>=60 days...",flush=True); C1=confirm_days('../data/studio_1h.duckdb'); print(len(C1),flush=True)

ad=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=ad.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,close*volume dv,buy_score,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
ad.close()
rec=[]
for tk,gd in D.groupby("ticker",sort=False):
    gd=gd.reset_index(drop=True)
    if len(gd)<30: continue
    o,hi,lo,cl=(gd[c].to_numpy(float) for c in("open","high","low","close"))
    dv=gd.dv.to_numpy(float); bs=gd.buy_score.to_numpy(float)
    dstr=[str(x)[:10] for x in gd.date]
    for di in range(1,len(gd)-1):
        if not (bs[di]>=60) or dv[di]<3e6: continue
        r=sim(o,hi,lo,cl,di+1,o[di+1])
        if r is None: continue
        c4=(tk,dstr[di]) in C4 or (tk,dstr[di-1]) in C4
        c1=(tk,dstr[di]) in C1 or (tk,dstr[di-1]) in C1
        rec.append((dstr[di][:4],cl[di],c4,c1,r))
R=pd.DataFrame(rec,columns=["yr","px","c4","c1","ret"])
print(f"\n1D buy_score>=60: n={len(R)} · 4h-conf {R.c4.mean()*100:.0f}% · 1h-conf {R.c1.mean()*100:.0f}%\n")
def rep(lab,s):
    if len(s)<100: print(f"  {lab:12} n={len(s)} too few"); return
    yp=0;yt=0
    for y in sorted(R.yr.unique()):
        sy=s[s.yr==y]; by=R[R.yr==y]
        if len(sy)>=50: yt+=1; yp+=int(sy.ret.mean()>by.ret.mean())
    q=s[(s.px>=21)&(s.px<89)]
    print(f"  {lab:12} n={len(s):6}  mean {s.ret.mean()*100:+.2f}%  med {s.ret.median()*100:+.2f}%  win {(s.ret>0).mean()*100:.0f}%  beats {yp}/{yt}yr  $21-89 {q.ret.mean()*100:+.2f}%")
rep("ALL",R)
rep("no confirm",R[~R.c4&~R.c1])
rep("4h only",R[R.c4&~R.c1])
rep("1h only",R[~R.c4&R.c1])
rep("BOTH",R[R.c4&R.c1])
