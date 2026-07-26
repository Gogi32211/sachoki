"""L34 green (close>open) vs red (close<open): frequency overall + inside the
triple confluence, and does the color change the edge?"""
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
a=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
# 1) overall frequency
f=a.execute("""SELECT
  sum(CASE WHEN close>open THEN 1 ELSE 0 END) grn,
  sum(CASE WHEN close<open THEN 1 ELSE 0 END) red,
  sum(CASE WHEN close=open THEN 1 ELSE 0 END) doji, count(*) n
 FROM bars WHERE l_sig='L34'""").fetchone()
print(f"ALL L34 bars n={f[3]:,}: green {100*f[0]/f[3]:.1f}% · red {100*f[1]/f[3]:.1f}% · doji {100*f[2]/f[3]:.1f}%")
f2=a.execute("""SELECT
  sum(CASE WHEN close>open THEN 1 ELSE 0 END) grn,
  sum(CASE WHEN close<open THEN 1 ELSE 0 END) red, count(*) n
 FROM bars WHERE l_sig='L34' AND close>=5 AND close*volume>=3e6""").fetchone()
print(f"L34 liquid($5+,3M$): green {100*f2[0]/f2[2]:.1f}% · red {100*f2[1]/f2[2]:.1f}%\n")
c=duckdb.connect('../data/studio_4h.duckdb',read_only=True)
r4=c.execute("""SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d FROM (
  SELECT ticker,date,rsi_14,close,
    MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) cp,
    LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date) rp
  FROM bars WHERE close>=5)
 WHERE m5<38 AND rsi_14 BETWEEN 30 AND 55 AND close>cp AND rsi_14>rp""").fetchdf()
c.close(); C4=set(zip(r4.ticker,r4.d))
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,beta_score,
  coalesce(l_sig,'') l, close*volume dv,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D["date"]=D["date"].astype(str).str[:10]
rows=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<40: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    rs=g.rsi_14.to_numpy(float); bt=g.beta_score.to_numpy(float)
    dv=g.dv.to_numpy(float); ds=g.date.tolist()
    l34=(g.l=='L34').to_numpy()
    m5=pd.Series(rs).rolling(5,min_periods=2).min().shift(1).to_numpy()
    for i in range(6,n-1):
        if dv[i]<3e6 or not l34[i]: continue
        rev=(m5[i]==m5[i]) and m5[i]<38 and 30<=rs[i]<=55 and cl[i]>cl[i-1] and rs[i]>rs[i-1] and bt[i]<=13
        if not rev: continue
        r=sim(o,hi,lo,cl,i+1,o[i+1])
        if r is None: continue
        rows.append((ds[i][:4],cl[i],cl[i]>o[i],cl[i]<o[i],(tk,ds[i]) in C4,r))
R=pd.DataFrame(rows,columns=["yr","px","grn","red","h4","ret"])
def rep(lab,s):
    if len(s)<80: print(f"  {lab:26} n={len(s):6}  too few"); return
    w=(s.ret>0).mean()*100; pfd=-s.ret[s.ret<=0].sum(); pf=s.ret[s.ret>0].sum()/pfd if pfd>0 else float('nan')
    yrs=s.groupby('yr').ret.mean()*100
    tr=s[s.yr.isin(['2021','2022','2023'])]; te=s[s.yr.isin(['2024','2025','2026'])]
    trm=tr.ret.mean()*100 if len(tr)>=30 else float('nan')
    tem=te.ret.mean()*100 if len(te)>=30 else float('nan')
    print(f"  {lab:26} n={len(s):6} mean {s.ret.mean()*100:+.2f}% med {s.ret.median()*100:+.2f}% win {w:.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr | TR {trm:+.2f} TE {tem:+.2f}")
print(f"🟢REV+L34 fires: green {100*R.grn.mean():.1f}% · red {100*R.red.mean():.1f}%")
print(f"triple (＋▲4H):  green {100*R[R.h4].grn.mean():.1f}% · red {100*R[R.h4].red.mean():.1f}%\n")
rep("REV+L34 green",R[R.grn])
rep("REV+L34 red",R[R.red])
rep("triple green",R[R.grn&R.h4])
rep("triple red",R[R.red&R.h4])
