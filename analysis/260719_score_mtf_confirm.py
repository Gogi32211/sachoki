"""Does INTRADAY score-confirmation improve a GOOD 1D score?
1D conditions (both reported): buy_score>=60 · ultra_score>=24  (the validated reversal
diapasons, now stored historically). Confirm = 4H / 1H turbo_score in 10..40 ("awake but
not extended") on day D or D-1 — the intraday analogue of the good score reading, from the
100%-filled intraday columns (prebreak_v2 is empty intraday, so intraday buy_score is not
computable). Groups none/4h/1h/both → path-sim trail25/-15%/60d. dv>=3M, px>=5.
"""
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

def confirm_days(db):
    c=duckdb.connect(db,read_only=True)
    df=c.execute("""SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d
                    FROM bars WHERE turbo_score BETWEEN 10 AND 40 AND close>=5""").fetchdf()
    c.close()
    return set(zip(df.ticker,df.d))
print("4h confirm days...",flush=True); C4=confirm_days('../data/studio_4h.duckdb'); print(len(C4),flush=True)
print("1h confirm days...",flush=True); C1=confirm_days('../data/studio_1h.duckdb'); print(len(C1),flush=True)

ad=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=ad.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,close*volume dv,
  buy_score,ultra_score,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
ad.close()
rec=[]
for tk,gd in D.groupby("ticker",sort=False):
    gd=gd.reset_index(drop=True)
    if len(gd)<30: continue
    o,hi,lo,cl=(gd[c].to_numpy(float) for c in("open","high","low","close"))
    dv=gd.dv.to_numpy(float); bs=gd.buy_score.to_numpy(float); us=gd.ultra_score.to_numpy(float)
    dstr=[str(x)[:10] for x in gd.date]
    for di in range(1,len(gd)-1):
        b_ok=bs[di]>=60; u_ok=us[di]>=24
        if not (b_ok or u_ok) or dv[di]<3e6: continue
        r=sim(o,hi,lo,cl,di+1,o[di+1])
        if r is None: continue
        c4=(tk,dstr[di]) in C4 or (tk,dstr[di-1]) in C4
        c1=(tk,dstr[di]) in C1 or (tk,dstr[di-1]) in C1
        rec.append((dstr[di][:4],cl[di],b_ok,u_ok,c4,c1,r))
R=pd.DataFrame(rec,columns=["yr","px","b_ok","u_ok","c4","c1","ret"])
def block(title,sub):
    print("\n"+"="*92); print(title, f"n={len(sub)}"); print("="*92)
    def rep(lab,s):
        if len(s)<100: print(f"  {lab:12} n={len(s)} too few"); return
        yp=0;yt=0
        for y in sorted(sub.yr.unique()):
            sy=s[s.yr==y]; by=sub[sub.yr==y]
            if len(sy)>=50: yt+=1; yp+=int(sy.ret.mean()>by.ret.mean())
        q=s[(s.px>=21)&(s.px<89)]
        print(f"  {lab:12} n={len(s):6}  mean {s.ret.mean()*100:+.2f}%  med {s.ret.median()*100:+.2f}%  win {(s.ret>0).mean()*100:.0f}%  beats-cond {yp}/{yt}yr  $21-89 {q.ret.mean()*100:+.2f}%")
    rep("ALL",sub)
    rep("no confirm",sub[~sub.c4&~sub.c1])
    rep("4h only",sub[sub.c4&~sub.c1])
    rep("1h only",sub[~sub.c4&sub.c1])
    rep("BOTH",sub[sub.c4&sub.c1])
block("1D buy_score>=60",R[R.b_ok])
block("1D ULTRA>=24",R[R.u_ok])
