"""Which L type carries the triple confluence? Per-type legs on the 🟢REV base:
L34 (per-bar l_sig) vs event flags FRI34/FRI43/FRI64/L555(=L22 family), ±▲4H."""
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
c=duckdb.connect('../data/studio_4h.duckdb',read_only=True)
r4=c.execute("""SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d FROM (
  SELECT ticker,date,rsi_14,close,
    MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) cp,
    LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date) rp
  FROM bars WHERE close>=5)
 WHERE m5<38 AND rsi_14 BETWEEN 30 AND 55 AND close>cp AND rsi_14>rp""").fetchdf()
c.close(); C4=set(zip(r4.ticker,r4.d))
a=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,beta_score,
  coalesce(l_sig,'') l, close*volume dv,
  coalesce(CAST(sig_fri34 AS INT),0) f34, coalesce(CAST(sig_fri43 AS INT),0) f43,
  coalesce(CAST(sig_fri64 AS INT),0) f64, coalesce(CAST(sig_l555 AS INT),0) l555,
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
    f34=g.f34.to_numpy(); f43=g.f43.to_numpy(); f64=g.f64.to_numpy(); l5=g.l555.to_numpy()
    m5=pd.Series(rs).rolling(5,min_periods=2).min().shift(1).to_numpy()
    for i in range(6,n-1):
        if dv[i]<3e6: continue
        rev=(m5[i]==m5[i]) and m5[i]<38 and 30<=rs[i]<=55 and cl[i]>cl[i-1] and rs[i]>rs[i-1] and bt[i]<=13
        if not rev: continue
        r=sim(o,hi,lo,cl,i+1,o[i+1])
        if r is None: continue
        rows.append((ds[i][:4],cl[i],bool(l34[i]),bool(f34[i]),bool(f43[i]),bool(f64[i]),bool(l5[i]),(tk,ds[i]) in C4,r))
R=pd.DataFrame(rows,columns=["yr","px","L34","F34","F43","F64","L555","h4","ret"])
def rep(lab,s):
    if len(s)<80: print(f"  {lab:24} n={len(s):6}  too few"); return
    w=(s.ret>0).mean()*100; pfd=-s.ret[s.ret<=0].sum(); pf=s.ret[s.ret>0].sum()/pfd if pfd>0 else float('nan')
    yrs=s.groupby('yr').ret.mean()*100
    tr=s[s.yr.isin(['2021','2022','2023'])]; te=s[s.yr.isin(['2024','2025','2026'])]
    trm=tr.ret.mean()*100 if len(tr)>=30 else float('nan')
    tem=te.ret.mean()*100 if len(te)>=30 else float('nan')
    print(f"  {lab:24} n={len(s):6} mean {s.ret.mean()*100:+.2f}% med {s.ret.median()*100:+.2f}% win {w:.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr | TR {trm:+.2f} TE {tem:+.2f}")
print("🟢REV base, per L-type leg (same bar):")
s=R
rep("alone",s)
for col,nm in (("L34","L34 (l_sig 9.7%)"),("F34","FRI34 event"),("F43","FRI43 event"),("F64","FRI64 event"),("L555","L555/L22 event")):
    rep("+"+nm,s[s[col]])
    rep("+"+nm+" +▲4H",s[s[col]&s.h4])
ev=s.F34|s.F43|s.F64|s.L555
rep("+ANY event-L",s[ev])
rep("+ANY event-L +▲4H",s[ev&s.h4])
rep("+L34 +ANYev +▲4H",s[s.L34&ev&s.h4])
