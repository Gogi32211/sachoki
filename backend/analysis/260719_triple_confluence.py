"""TRIPLE CONFLUENCE: BUY signal × heavy-L (l_sig∈{L34,L43,L64,L22} same bar or ≤3 back)
× ▲4H (4h REV-turn fired that day). Bases: (A) 🟢REV flag · (B) buy_score>=60 day.
Cells: alone / +L / +▲ / +L+▲. Path-sim trail25/-15%/60d, per-year, TRAIN/TEST, $21-89."""
import numpy as np, pandas as pd, duckdb
S_=0.0015
def sim(o,hi,lo,cl,start,entry):
    n=len(cl); e=entry*(1+S_)
    if e<=0 or start>=n: return None
    pk=e; hd=e*0.85; end=min(start+60,n)
    for j in range(start,end):
        if o[j]<=hd and j>start: return o[j]/e-1-S_
        if lo[j]<=hd: return -0.15-S_
        pk=max(pk,hi[j]); ts=pk*0.75
        if o[j]<=ts and j>start: return o[j]/e-1-S_
        if lo[j]<=ts: return ts/e-1-S_
    return cl[end-1]/e-1-S_
print("4h REV day-set...",flush=True)
c=duckdb.connect('../data/studio_4h.duckdb',read_only=True)
r4=c.execute("""SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d FROM (
  SELECT ticker,date,rsi_14,close,
    MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) cp,
    LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date) rp
  FROM bars WHERE close>=5)
 WHERE m5<38 AND rsi_14 BETWEEN 30 AND 55 AND close>cp AND rsi_14>rp""").fetchdf()
c.close(); C4=set(zip(r4.ticker,r4.d)); print(len(C4),flush=True)
a=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,beta_score,buy_score,
  coalesce(l_sig,'') l, close*volume dv,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D["date"]=D["date"].astype(str).str[:10]
HEAVY={'L34','L43','L64','L22'}
rows=[]; pool=[]
rng=np.random.default_rng(4)
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<40: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    rs=g.rsi_14.to_numpy(float); bt=g.beta_score.to_numpy(float); bs=g.buy_score.to_numpy(float)
    dv=g.dv.to_numpy(float); ds=g.date.tolist()
    lh=np.array([1 if x in HEAVY else 0 for x in g.l])
    lh3=pd.Series(lh).rolling(4,min_periods=1).max().to_numpy()   # same bar or ≤3 back
    for i in range(0,n-1,17):
        if dv[i]>=3e6:
            r=sim(o,hi,lo,cl,i+1,o[i+1])
            if r is not None: pool.append(r)
    m5=pd.Series(rs).rolling(5,min_periods=2).min().shift(1).to_numpy()
    for i in range(6,n-1):
        if dv[i]<3e6: continue
        up=cl[i]>cl[i-1]; ris=rs[i]>rs[i-1]
        rev = (m5[i]==m5[i]) and m5[i]<38 and 30<=rs[i]<=55 and up and ris and bt[i]<=13
        b60 = bs[i]>=60
        if not (rev or b60): continue
        r=sim(o,hi,lo,cl,i+1,o[i+1])
        if r is None: continue
        h4=(tk,ds[i]) in C4
        rows.append((ds[i][:4],cl[i],rev,b60,bool(lh[i]),bool(lh3[i]),h4,r))
R=pd.DataFrame(rows,columns=["yr","px","rev","b60","L0","L3","h4","ret"])
pool=np.array(pool)
print(f"pool {len(pool)} mean {pool.mean()*100:+.2f}%\n")
def rep(lab,s):
    if len(s)<80: print(f"  {lab:22} n={len(s):6}  too few"); return
    w=(s.ret>0).mean()*100; pfd=-s.ret[s.ret<=0].sum(); pf=s.ret[s.ret>0].sum()/pfd if pfd>0 else float('nan')
    yrs=s.groupby('yr').ret.mean()*100
    tr=s[s.yr.isin(['2021','2022','2023'])]; te=s[s.yr.isin(['2024','2025','2026'])]
    q=s[(s.px>=21)&(s.px<89)]
    trm=tr.ret.mean()*100 if len(tr)>=30 else float('nan')
    tem=te.ret.mean()*100 if len(te)>=30 else float('nan')
    print(f"  {lab:22} n={len(s):6} mean {s.ret.mean()*100:+.2f}% med {s.ret.median()*100:+.2f}% win {w:.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr | TR {trm:+.2f} TE {tem:+.2f} | $21-89 {q.ret.mean()*100:+.2f}%")
for base,mask,nm in (("A) 🟢REV flag",R.rev,"REV"),("B) buy_score≥60",R.b60,"b60")):
    print(base)
    s=R[mask]
    rep("alone",s)
    rep("+heavyL (same bar)",s[s.L0])
    rep("+heavyL (≤3 back)",s[s.L3])
    rep("+▲4H",s[s.h4])
    rep("+L(≤3) +▲4H",s[s.L3&s.h4])
    rep("+L(same) +▲4H",s[s.L0&s.h4])
    print()
