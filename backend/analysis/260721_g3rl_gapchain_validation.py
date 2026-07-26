"""FULL VALIDATION: G3+RL (same bar) and G3→G3 gap-chain (+tiers/plateau/veto).
Standard: path-sim trail25/-15/60/15bps, next-open entry, cooldown-5/ticker, dv>=3M,
per-year, TRAIN/TEST, price buckets, random-same-n z-control. 2026-07-21."""
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
  coalesce(bar_gap_class,'') gap, coalesce(CAST(sig_rl AS TINYINT),0) rl,
  coalesce(CAST(sig_vol_10x AS TINYINT),0) v10, coalesce(CAST(sig_vol_20x AS TINYINT),0) v20,
  coalesce(z_sig,'') z, coalesce(t_sig,'') t,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D['dv']=D.close*D.volume
ZC=('Z1G','Z2G','Z9','Z11')
def build_masks(g):
    g3=(g.gap=='G3').to_numpy(); g2=(g.gap=='G2').to_numpy()
    rl=(g.rl==1).to_numpy(); v=(g.v10==1)|(g.v20==1); v=v.to_numpy()
    zc=g.z.isin(ZC).to_numpy()
    n=len(g3)
    p=lambda arr,k: np.concatenate([[False]*k,arr[:-k]]) if k<n else np.zeros(n,bool)
    return {
     "C1 G3+RL":            g3&rl,
     "C2 G3→G3":            p(g3,1)&g3,
     "C3 G3 2x≤3ბარში":     g3&(p(g3,1)|p(g3,2)|p(g3,3)),
     "C4 G3→G3→RL":         p(g3,2)&p(g3,1)&rl,
     "C5 RL→G3":            p(rl,1)&g3,
     "C6 G2+RL (პლატო)":    g2&rl,
     "C7 G3+RL+ZCAP":       g3&rl&zc,
     "C8 G3→G3 −volveto":   p(g3,1)&g3&~v&~p(v,1),
     "C9 G3+RL −volveto":   g3&rl&~v&~p(v,1),
    }
trades={}; pool=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<40: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    dv=g.dv.to_numpy(float); yr=g.date.astype(str).str[:4].tolist()
    M=build_masks(g)
    last={k:-99 for k in M}
    for i in range(4,n-1):
        if dv[i]<3e6: continue
        if i%31==0:
            r=sim(o,hi,lo,cl,i+1)
            if r is not None: pool.append(r)
        for k,m in M.items():
            if m[i] and i-last[k]>=5:
                r=sim(o,hi,lo,cl,i+1)
                if r is not None:
                    trades.setdefault(k,[]).append((yr[i],cl[i],r)); last[k]=i
P=np.array(pool); rng=np.random.default_rng(5)
print(f"random pool n={len(P):,} mean {100*P.mean():+.2f}%\n")
for k in ["C1 G3+RL","C2 G3→G3","C3 G3 2x≤3ბარში","C4 G3→G3→RL","C5 RL→G3",
          "C6 G2+RL (პლატო)","C7 G3+RL+ZCAP","C8 G3→G3 −volveto","C9 G3+RL −volveto"]:
    T=pd.DataFrame(trades.get(k,[]),columns=["yr","px","ret"])
    if len(T)<80: print(f"{k:20} n={len(T)} too few"); continue
    yrs=T.groupby('yr').ret.mean()*100
    tr=T[T.yr.isin(('2021','2022','2023'))]; te=T[T.yr.isin(('2024','2025','2026'))]
    trm=100*tr.ret.mean() if len(tr)>=30 else float('nan')
    tem=100*te.ret.mean() if len(te)>=30 else float('nan')
    pfd=-T.ret[T.ret<=0].sum(); pf=T.ret[T.ret>0].sum()/pfd if pfd>0 else float('nan')
    bs=np.array([P[rng.integers(0,len(P),len(T))].mean() for _ in range(2000)])
    z=(T.ret.mean()-bs.mean())/bs.std()
    q1=T[(T.px>=5)&(T.px<21)]; q2=T[(T.px>=21)&(T.px<89)]; q3=T[T.px>=89]
    qm=lambda q: f"{100*q.ret.mean():+.1f}" if len(q)>=30 else "·"
    print(f"{k:20} n={len(T):6,} mean {100*T.ret.mean():+.2f}% med {100*T.ret.median():+.2f}% "
          f"win {100*(T.ret>0).mean():.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr z={z:+.1f}σ | "
          f"TR {trm:+.2f} TE {tem:+.2f} | $5-21 {qm(q1)} $21-89 {qm(q2)} $89+ {qm(q3)}")
    print(f"{'':20} "+" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items()))
