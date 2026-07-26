"""(2026-07-21) Two user requests:
 A) SC/ND/L46 CHAIN family full validation (grand-sweep survivors → build candidates):
    SC→ZCAP · NS→SC · ND→SC · SC→L46 · G3→L46 · ND→SC→L46 (+ plateau: gap-1-allowed)
 B) BO/BX/BE body-breaks LOCATION-CONDITIONED (user: worth testing even if late):
    near 25-bar low · after ZCAP <=3 bars · setup context NS/ND
Standard: path-sim trail25/-15/60/15bps, cooldown-5, dv>=3M, per-year, TR/TE, random-z."""
import numpy as np, pandas as pd, duckdb
S_=0.0015
def sim(o,hi,lo,cl,start):
    n=len(cl); e=o[start]*(1+S_)
    if e<=0 or start>=n: return None
    pk=e; hd=e*0.85; end=min(start+60,n)
    for j in range(start,end):
        if o[j]<=hd and j>start: return o[j]/e-1-S_
        if lo[j]<=hd: return -0.15-S_
        pk=max(pk,hi[j]); ts=pk*0.75
        if o[j]<=ts and j>start: return o[j]/e-1-S_
        if lo[j]<=ts: return ts/e-1-S_
    return cl[end-1]/e-1-S_
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume,rsi_14,
  coalesce(z_sig,'') z, coalesce(l_sig,'') l, coalesce(bar_gap_class,'') gap,
  coalesce(CAST(w2_sc AS TINYINT),0) sc, coalesce(CAST(sig_ns_vabs AS TINYINT),0) ns,
  coalesce(CAST(sig_nd_vabs AS TINYINT),0) nd,
  coalesce(CAST(bo_up AS TINYINT),0) bo, coalesce(CAST(bx_up AS TINYINT),0) bx,
  coalesce(CAST(be_up AS TINYINT),0) be,
  min(low) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 25 PRECEDING AND 1 PRECEDING) lo25,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D['dv']=D.close*D.volume
ZC=('Z1G','Z2G','Z9','Z11')
def masks(g):
    n=len(g)
    sc=(g.sc==1).to_numpy(); ns=(g.ns==1).to_numpy(); nd=(g.nd==1).to_numpy()
    l46=(g.l=='L46').to_numpy(); g3=(g.gap=='G3').to_numpy()
    zc=g.z.isin(ZC).to_numpy()
    bo=(g.bo==1).to_numpy(); bx=(g.bx==1).to_numpy(); be=(g.be==1).to_numpy()
    anyb=bo|bx|be
    nearlow=((g.close/g.lo25-1)<=0.10).fillna(False).to_numpy()
    p=lambda a_,k: np.concatenate([[False]*k,a_[:-k]]) if k<n else np.zeros(n,bool)
    zc3=p(zc,1)|p(zc,2)|p(zc,3)
    return {
     "SC→ZCAP":      p(sc,1)&zc,
     "SC→ZCAP(≤2)":  (p(sc,1)|p(sc,2))&zc,
     "NS→SC":        p(ns,1)&sc,
     "ND→SC":        p(nd,1)&sc,
     "SC→L46":       p(sc,1)&l46,
     "G3→L46":       p(g3,1)&l46,
     "ND→SC→L46":    p(nd,2)&p(sc,1)&l46,
     "BOBXBE ფსკერზე":   anyb&nearlow,
     "BOBXBE ZCAP≤3":    anyb&zc3,
     "BOBXBE ფსკ+ZCAP":  anyb&nearlow&zc3,
     "BOBXBE +NS/ND":    anyb&(ns|nd),
     "BE↑ ფსკერზე":      be&nearlow,
    }
trades={}; pool=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<40: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    dv=g.dv.to_numpy(float); yr=g.date.astype(str).str[:4].tolist()
    M=masks(g)
    last={k:-99 for k in M}
    for i in range(4,n-1):
        if dv[i]<3e6: continue
        if i%33==0:
            r=sim(o,hi,lo,cl,i+1)
            if r is not None: pool.append(r)
        for k,m in M.items():
            if m[i] and i-last[k]>=5:
                r=sim(o,hi,lo,cl,i+1)
                if r is not None:
                    trades.setdefault(k,[]).append((yr[i],cl[i],r)); last[k]=i
P=np.array(pool); rng=np.random.default_rng(13)
print(f"random pool n={len(P):,} mean {100*P.mean():+.2f}%\n")
for k in masks(D.head(50)).keys():
    T=pd.DataFrame(trades.get(k,[]),columns=["yr","px","ret"])
    if len(T)<80: print(f"{k:18} n={len(T)} too few"); continue
    yrs=T.groupby('yr').ret.mean()*100
    tr=T[T.yr.isin(('2021','2022','2023'))]; te=T[T.yr.isin(('2024','2025','2026'))]
    trm=100*tr.ret.mean() if len(tr)>=30 else float('nan')
    tem=100*te.ret.mean() if len(te)>=30 else float('nan')
    pfd=-T.ret[T.ret<=0].sum(); pf=T.ret[T.ret>0].sum()/pfd if pfd>0 else float('nan')
    bs=np.array([P[rng.integers(0,len(P),len(T))].mean() for _ in range(2000)])
    z=(T.ret.mean()-bs.mean())/bs.std()
    q2=T[(T.px>=21)&(T.px<89)]
    qm=f"{100*q2.ret.mean():+.1f}" if len(q2)>=30 else "·"
    print(f"{k:18} n={len(T):6,} mean {100*T.ret.mean():+.2f}% med {100*T.ret.median():+.2f}% "
          f"win {100*(T.ret>0).mean():.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr z={z:+.1f}σ | "
          f"TR {trm:+.2f} TE {tem:+.2f} | $21-89 {qm} | "+" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items()))
