"""EDGE × BUY confluence: each DISPLAY_SETUPS edge fire split by 🟢REV flag /
▲4H / both on the SAME bar. Frame = edge_replay._frame(60,3M) — the exact masks
the backtest & UI use. Path-sim trail25/-15/60b/15bps, per-year, TRAIN/TEST."""
import sys, numpy as np, pandas as pd, duckdb
sys.path.insert(0, '/Users/sachoki/Desktop/sachoki-desktop/backend')
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

print("4h REV day-set...", flush=True)
c=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_4h.duckdb',read_only=True)
r4=c.execute("""SELECT DISTINCT ticker, strftime(CAST(date AS TIMESTAMP),'%Y-%m-%d') d FROM (
  SELECT ticker,date,rsi_14,close,
    MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) cp,
    LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date) rp
  FROM bars WHERE close>=5)
 WHERE m5<38 AND rsi_14 BETWEEN 30 AND 55 AND close>cp AND rsi_14>rp""").fetchdf()
c.close(); C4=set(zip(r4.ticker,r4.d)); print(len(C4), flush=True)

print("beta map...", flush=True)
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
bmap=a.execute("""SELECT ticker, strftime(date,'%Y-%m-%d') d, max(beta_score) b
  FROM bars WHERE close>=5 GROUP BY 1,2""").fetchdf()
a.close()
BM={(t,d):v for t,d,v in zip(bmap.ticker,bmap.d,bmap.b)}
del bmap

print("edge frame (60,3M) — builds once, few minutes...", flush=True)
from edge_replay import _frame, DISPLAY_SETUPS
grp, as_of = _frame(60, 3_000_000)
print("frame ready, as_of", as_of, "tickers", len(grp), flush=True)

recs=[]
for tk,g in grp.items():
    n=len(g)
    if n<40: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    rs=g["rsi_14"].to_numpy(float)
    ds=g["date"].astype(str).str[:10].to_numpy()
    m5=pd.Series(rs).rolling(5,min_periods=2).min().shift(1).to_numpy()
    rsp=np.concatenate([[np.nan],rs[:-1]])
    pc=np.concatenate([[np.nan],cl[:-1]])
    emasks={code:g[col].to_numpy(bool) for code,col in DISPLAY_SETUPS if col in g}
    anye=np.zeros(n,bool)
    for e in emasks.values(): anye|=e
    idx=np.nonzero(anye)[0]
    for i in idx:
        if i>=n-1: continue
        r=sim(o,hi,lo,cl,i+1,o[i+1])
        if r is None: continue
        bt=BM.get((tk,ds[i]))
        rev=(m5[i]==m5[i]) and m5[i]<38 and 30<=rs[i]<=55 and cl[i]>pc[i] and rs[i]>rsp[i] \
            and bt is not None and float(bt)<=13
        h4=(tk,ds[i]) in C4
        codes=[cd for cd,e in emasks.items() if e[i]]
        recs.append((ds[i][:4],cl[i],rev,h4,r,tuple(codes)))
R=pd.DataFrame(recs,columns=["yr","px","rev","h4","ret","codes"])
print("total edge-fire bars:", len(R), flush=True)

def rep(lab,s):
    if len(s)<80: print(f"  {lab:16} n={len(s):6}  too few"); return
    w=(s.ret>0).mean()*100; pfd=-s.ret[s.ret<=0].sum(); pf=s.ret[s.ret>0].sum()/pfd if pfd>0 else float('nan')
    yrs=s.groupby('yr').ret.mean()*100
    tr=s[s.yr.isin(['2021','2022','2023'])]; te=s[s.yr.isin(['2024','2025','2026'])]
    trm=tr.ret.mean()*100 if len(tr)>=30 else float('nan')
    tem=te.ret.mean()*100 if len(te)>=30 else float('nan')
    q=s[(s.px>=21)&(s.px<89)]
    qm=q.ret.mean()*100 if len(q)>=30 else float('nan')
    print(f"  {lab:16} n={len(s):6} mean {s.ret.mean()*100:+.2f}% med {s.ret.median()*100:+.2f}% win {w:.0f}% PF {pf:.2f} "
          f"{int((yrs>0).sum())}/{len(yrs)}yr | TR {trm:+.2f} TE {tem:+.2f} | $21-89 {qm:+.2f}")

print("\n══ ANY edge fire (pooled) ══")
rep("alone(all)",R)
rep("+🟢REV",R[R.rev])
rep("+▲4H",R[R.h4])
rep("+REV+▲4H",R[R.rev&R.h4])
rep("no REV no ▲",R[~R.rev&~R.h4])

print("\n══ per setup ══")
for code,_ in DISPLAY_SETUPS:
    has=R.codes.apply(lambda t: code in t)
    s=R[has]
    if len(s)<200: continue
    print(f"{code} (n={len(s)}, overlap REV {100*s.rev.mean():.0f}% · ▲4H {100*s.h4.mean():.0f}%)")
    rep("  alone",s)
    rep("  +🟢REV",s[s.rev])
    rep("  +▲4H",s[s.h4])
    rep("  +REV+▲4H",s[s.rev&s.h4])
