"""CAUSAL / TRADEABLE version. A TLS completes on bar j (cl[j-3]>cl[j-2]>cl[j-1], green,
cl[j]>high[j-3]) AND an edge fired within the prior 5 bars [j-5, j]. THEN enter next-open
(j+1). Everything known at entry. Compare to all edge fires entered immediate (baseline) +
per year + TRAIN/TEST + price bucket + random-same-n control. Exit trail25/-15%/60bar.
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
import edge_replay as ER
S,TRAIL,HARD,MAXH,LB=0.0015,0.25,0.15,60,5

def sim(i,o,hi,lo,cl,n):
    if i+1>=n: return None
    e=o[i+1]*(1+S)
    if e<=0: return None
    pk=e; hard=e*(1-HARD); end=min(i+1+MAXH,n)
    for j in range(i+1,end):
        if j>i+1 and o[j]<=hard: return o[j]/e-1-S
        if lo[j]<=hard: return -HARD-S
        pk=max(pk,hi[j]); ts=pk*(1-TRAIL)
        if j>i+1 and o[j]<=ts: return o[j]/e-1-S
        if lo[j]<=ts: return ts/e-1-S
    return cl[end-1]/e-1-S

EDGES=[("G3-Abs","E_g3abs"),("QZ-Capit","E_qzcapit"),("Atomic","E_atomic"),("Cluster3",None)]
grp,as_of=ER._frame(72,3_000_000)
print(f"as_of {as_of} · CAUSAL TLS-entry (edge fired ≤5 bars before the TLS, enter after TLS)\n")

def blk(a):
    a=np.asarray([x for x in a if x is not None])
    if len(a)<15: return None
    return dict(n=len(a),mean=a.mean()*100,med=np.median(a)*100,win=(a>0).mean()*100,
                pf=(a[a>0].sum()/-a[a<=0].sum()) if (a<=0).any() else np.nan)
def line(lab,d):
    print(f"   {lab:16} "+("n<15" if not d else
        f"n={d['n']:5} mean {d['mean']:+6.2f}%  med {d['med']:+6.2f}%  win {d['win']:4.0f}%  PF {d['pf']:4.2f}"))
rng=np.random.default_rng(9)

for en,ec in EDGES:
    trades=[]; immall=[]
    for tk,g in grp.items():
        g=g.reset_index(drop=True); n=len(g)
        if n<30: continue
        o=g.open.to_numpy(float);hi=g.high.to_numpy(float);lo=g.low.to_numpy(float);cl=g.close.to_numpy(float)
        yr=g.date.astype(str).str[:4].to_numpy()
        em=(g.conf_n>=3).to_numpy() if ec is None else g[ec].to_numpy(bool)
        firecum=np.concatenate([[0],np.cumsum(em)])
        last=-99
        for D in np.flatnonzero(em):     # immediate baseline (deduped)
            if D-last>=5:
                last=D; r=sim(D,o,hi,lo,cl,n)
                if r is not None: immall.append((yr[D],cl[D],r))
        lastT=-99
        for j in range(3,n):
            if not (cl[j-3]>cl[j-2]>cl[j-1] and cl[j]>o[j] and cl[j]>hi[j-3]): continue
            lo_i=max(0,j-LB)
            if firecum[j+1]-firecum[lo_i] <= 0: continue   # an edge fired in [j-5, j]
            if j-lastT<5: continue
            lastT=j
            r=sim(j,o,hi,lo,cl,n)
            if r is not None: trades.append((yr[j],cl[j],r))
    T=pd.DataFrame(trades,columns=["yr","px","ret"]); I=pd.DataFrame(immall,columns=["yr","px","ret"])
    print("="*92); print(f"{en}: {len(I)} edge fires · {len(T)} with a causal TLS-entry ({len(T)/len(I)*100:.0f}%)"); print("="*92)
    line("edge→immediate", blk(I.ret)); line("edge→TLS entry", blk(T.ret))
    dt=blk(T.ret); di=blk(I.ret)
    print(f"   → TLS-entry lift vs immediate: {dt['mean']-di['mean']:+.2f}pp mean, {dt['pf']-di['pf']:+.2f} PF")
    for w in ["TRAIN 21-23","TEST 24-26"]:
        ys=["2021","2022","2023"] if "TRAIN" in w else ["2024","2025","2026"]
        dtt=blk(T[T.yr.isin(ys)].ret); dii=blk(I[I.yr.isin(ys)].ret)
        if dtt and dii: print(f"     {w:12} TLS n={dtt['n']:4} mean {dtt['mean']:+6.2f}%  vs imm {dii['mean']:+6.2f}%  lift {dtt['mean']-dii['mean']:+5.2f}pp  PF {dtt['pf']:.2f}")
    yp=yt=0
    for y in ["2021","2022","2023","2024","2025","2026"]:
        dtt=blk(T[T.yr==y].ret); dii=blk(I[I.yr==y].ret)
        if dtt and dii: yt+=1; yp+=int(dtt['mean']-dii['mean']>0); 
        if dtt and dii: print(f"       {y} TLS {dtt['mean']:+6.2f}% vs imm {dii['mean']:+6.2f}%  ({dtt['mean']-dii['mean']:+.2f}pp, n={dtt['n']})")
    print(f"     → lift positive {yp}/{yt} years")
    for lo_,hi_,bl in [(5,21,"$5-21"),(21,89,"$21-89"),(89,1e9,"$89+")]:
        line("  "+bl,blk(T[(T.px>=lo_)&(T.px<hi_)].ret))
    base=I.ret.to_numpy(); k=len(T)
    draws=np.array([rng.choice(base,k,replace=False).mean() for _ in range(200)])*100
    z=(dt['mean']-draws.mean())/draws.std() if draws.std()>0 else float('nan')
    print(f"   RANDOM same-n from immediate fires: {draws.mean():+.2f}% ± {draws.std():.2f}  →  TLS-entry {z:+.2f}σ above\n")
