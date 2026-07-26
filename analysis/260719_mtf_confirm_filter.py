"""Is INTRADAY CONFIRMATION a useful FILTER on a good 1D buy moment?
1D condition = daily REV-turn (min5-RSI<38, RSI 30-55, up bar, dv>=3M, px>=5) — the buy moment.
Confirm = the SAME state fired on 4H / 1H on day D or D-1.
Groups: none / 4h-only / 1h-only / both → forward path-sim (trail25/-15%/60d, next daily open).
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
def rev_mask(rs,cl):
    n=len(rs); m=np.zeros(n,bool)
    if n<6: return m
    m5=pd.Series(rs).rolling(5,min_periods=2).min().shift(1).to_numpy()
    up=np.concatenate([[False],cl[1:]>cl[:-1]])
    ris=np.concatenate([[False],rs[1:]>rs[:-1]])
    m=(m5<38)&(rs>=30)&(rs<=55)&up&ris
    m[np.isnan(m5)|np.isnan(rs)]=False
    return m

def intraday_days(db):
    """{(ticker, 'YYYY-MM-DD')} where the TF printed a REV-turn — chunked, low memory."""
    con=duckdb.connect(db,read_only=True)
    tks=[r[0] for r in con.execute("SELECT DISTINCT ticker FROM bars").fetchall()]
    out=set()
    CH=300
    for ci in range(0,len(tks),CH):
        ch=tks[ci:ci+CH]; ph=",".join("?"*len(ch))
        df=con.execute(f"""WITH r AS (SELECT ticker,date,close,rsi_14,
            row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
            FROM bars WHERE ticker IN ({ph}) AND close>=5)
            SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""",ch).fetchdf()
        for tk,g in df.groupby("ticker",sort=False):
            m=rev_mask(g.rsi_14.to_numpy(float),g.close.to_numpy(float))
            for d_ in {str(x)[:10] for x in g.date[m]}: out.add((tk,d_))
    con.close()
    return out

print("building 4h confirm set...",flush=True)
S4=intraday_days('../data/studio_4h.duckdb'); print("4h days:",len(S4),flush=True)
print("building 1h confirm set...",flush=True)
S1=intraday_days('../data/studio_1h.duckdb'); print("1h days:",len(S1),flush=True)

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
    dstr=[str(x)[:10] for x in gd.date]
    md=rev_mask(rs,cl)
    for di in np.flatnonzero(md):
        if di+1>=len(gd) or dv[di]<3e6: continue
        r=sim(o,hi,lo,cl,di+1,o[di+1])
        if r is None: continue
        c4=(tk,dstr[di]) in S4 or (di>0 and (tk,dstr[di-1]) in S4)
        c1=(tk,dstr[di]) in S1 or (di>0 and (tk,dstr[di-1]) in S1)
        rec.append((dstr[di][:4],cl[di],c4,c1,r))
R=pd.DataFrame(rec,columns=["yr","px","c4","c1","ret"])
print(f"\ndaily REV events: {len(R)}  · with 4h confirm {R.c4.mean()*100:.0f}%  · with 1h {R.c1.mean()*100:.0f}%\n")
def rep(lab,s):
    if len(s)<100: print(f"{lab:14} n={len(s)} too few"); return
    yp=0;yt=0
    base=R
    for y in sorted(s.yr.unique()):
        sy=s[s.yr==y]; by=base[base.yr==y]
        if len(sy)>=50: yt+=1; yp+=int(sy.ret.mean()>by.ret.mean())
    q=s[(s.px>=21)&(s.px<89)]
    print(f"{lab:14} n={len(s):6}  mean {s.ret.mean()*100:+.2f}%  med {s.ret.median()*100:+.2f}%  win {(s.ret>0).mean()*100:.0f}%  beats-all {yp}/{yt}yr  $21-89 {q.ret.mean()*100:+.2f}%")
rep("ALL daily REV",R)
rep("no confirm",R[~R.c4&~R.c1])
rep("4h only",R[R.c4&~R.c1])
rep("1h only",R[~R.c4&R.c1])
rep("4h+1h BOTH",R[R.c4&R.c1])
rep("any intraday",R[R.c4|R.c1])
