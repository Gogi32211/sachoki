"""FULL VALIDATION (2026-07-21): two candidate edges from the confluence sweeps.
  A) 🧲 vol-doji-absorption: giant volume into quiet/absorbing structure
  B) pb_pp_rtv × ns_vabs (prebreak PP/RTV + no-supply)
Standard: path-sim trail25/-15%/60b/15bps, entry next-open, cooldown-5/ticker,
per-year, TRAIN 21-23/TEST 24-26, price buckets, random-same-n control, plateau variants."""
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
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume,
  coalesce(CAST("load" AS TINYINT),0) ld, coalesce(CAST(sig_vol_20x AS TINYINT),0) v20,
  coalesce(CAST(sig_vol_10x AS TINYINT),0) v10, coalesce(CAST(sig_vol_5x AS TINYINT),0) v5,
  coalesce(CAST(sig_conso AS TINYINT),0) co, coalesce(CAST(sig_bias_up AS TINYINT),0) bu,
  coalesce(CAST(sig_nd_vabs AS TINYINT),0) nd, coalesce(CAST(sig_ns_vabs AS TINYINT),0) ns,
  coalesce(CAST(pb_pp_rtv AS TINYINT),0) ppr, coalesce(CAST(sig_rl AS TINYINT),0) rl,
  coalesce(CAST(pb_wvf_confirm AS TINYINT),0) wvf,
  coalesce(z_sig,'') z,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D['dv']=D.close*D.volume
print(f"rows {len(D):,}",flush=True)
SET={
 "A1 v20×LOAD":        lambda d:(d.v20==1)&(d.ld==1),
 "A2 v10×LOAD":        lambda d:(d.v10==1)&(d.ld==1),
 "A3 v10+×absorb":     lambda d:((d.v10==1)|(d.v20==1))&((d.ld==1)|(d.nd==1)|(d.ns==1)),
 "A4 v20×LOAD×bias":   lambda d:(d.v20==1)&(d.ld==1)&(d.bu==1),
 "A5 conso×v20×bias":  lambda d:(d.co==1)&(d.v20==1)&(d.bu==1),
 "A6 doji-Z7×v10+":    lambda d:((d.v10==1)|(d.v20==1))&(d.z=='Z7'),
 "B1 ppr×ns":          lambda d:(d.ppr==1)&(d.ns==1),
 "B2 ppr×rl":          lambda d:(d.ppr==1)&(d.rl==1),
 "B3 ppr×wvf":         lambda d:(d.ppr==1)&(d.wvf==1),
}
trades={k:[] for k in SET}
pool=[]
rng=np.random.default_rng(11)
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<40: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    dv=g.dv.to_numpy(float); ds=g.date.astype(str).str[:10].tolist()
    masks={k:f(g).to_numpy() for k,f in SET.items()}
    last={k:-99 for k in SET}
    for i in range(5,n-1):
        if dv[i]<3e6: continue
        if i%29==0:
            r=sim(o,hi,lo,cl,i+1)
            if r is not None: pool.append((ds[i][:4],r))
        for k in SET:
            if masks[k][i] and i-last[k]>=5:
                r=sim(o,hi,lo,cl,i+1)
                if r is not None:
                    trades[k].append((ds[i][:4],cl[i],r)); last[k]=i
P=pd.DataFrame(pool,columns=["yr","ret"])
print(f"random pool n={len(P):,} mean {100*P.ret.mean():+.2f}%\n")
def rep(k,T):
    T=pd.DataFrame(T,columns=["yr","px","ret"])
    if len(T)<80: print(f"{k:20} n={len(T)} too few"); return
    m=100*T.ret.mean(); md=100*T.ret.median(); w=100*(T.ret>0).mean()
    pfd=-T.ret[T.ret<=0].sum(); pf=T.ret[T.ret>0].sum()/pfd if pfd>0 else float('nan')
    yrs=T.groupby('yr').ret.mean()*100
    tr=T[T.yr.isin(('2021','2022','2023'))]; te=T[T.yr.isin(('2024','2025','2026'))]
    trm=100*tr.ret.mean() if len(tr)>=30 else float('nan')
    tem=100*te.ret.mean() if len(te)>=30 else float('nan')
    # random control z
    n=len(T); bs=np.array([P.ret.sample(n,replace=True).mean() for _ in range(2000)])
    z=(T.ret.mean()-bs.mean())/bs.std()
    q1=T[(T.px>=5)&(T.px<21)]; q2=T[(T.px>=21)&(T.px<89)]; q3=T[T.px>=89]
    def qm(q): return f"{100*q.ret.mean():+.1f}" if len(q)>=30 else "·"
    print(f"{k:20} n={len(T):6,} mean {m:+.2f}% med {md:+.2f}% win {w:.0f}% PF {pf:.2f} "
          f"{int((yrs>0).sum())}/{len(yrs)}yr | TR {trm:+.2f} TE {tem:+.2f} | z={z:+.1f}σ | $5-21 {qm(q1)} $21-89 {qm(q2)} $89+ {qm(q3)}")
    print(f"{'':20} per-yr: "+" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items()))
for k in SET: rep(k,trades[k])
