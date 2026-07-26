"""DECISIVE — is 'Three Line Strike marks the best edge fires' a real 6yr selector, or a
2024-26 era artifact like the sequences? Enter IMMEDIATE (next-open) on the edge fire, but
split fires into TLS-marked (a Three Line Strike completes within (D,D+5]) vs the rest.
Compare per year + TRAIN 21-23 / TEST 24-26 + price bucket + a random-same-size control
(does picking ANY random 10% of fires match TLS's lift?). Exit trail25/-15%/60bar.
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
import edge_replay as ER
S,TRAIL,HARD,MAXH,WIN=0.0015,0.25,0.15,60,5

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

def tls_within(D,o,hi,lo,cl,n):
    for j in range(D+1,min(D+WIN+1,n)):
        if j>=3 and cl[j-3]>cl[j-2]>cl[j-1] and cl[j]>o[j] and cl[j]>hi[j-3]:
            return True
    return False

EDGES=[("G3-Abs","E_g3abs"),("QZ-Capit","E_qzcapit"),("Atomic","E_atomic"),("Cluster3",None)]
grp,as_of=ER._frame(72,3_000_000)
print(f"as_of {as_of}\n")

def blk(a):
    a=np.asarray([x for x in a if x is not None])
    if len(a)<15: return None
    return dict(n=len(a),mean=a.mean()*100,med=np.median(a)*100,win=(a>0).mean()*100,
                pf=(a[a>0].sum()/-a[a<=0].sum()) if (a<=0).any() else np.nan)
def line(lab,d):
    if not d: print(f"   {lab:16} n<15"); return
    print(f"   {lab:16} n={d['n']:5} mean {d['mean']:+6.2f}%  med {d['med']:+6.2f}%  win {d['win']:4.0f}%  PF {d['pf']:4.2f}")

rng=np.random.default_rng(5)
for en,ec in EDGES:
    rec=[]
    for tk,g in grp.items():
        g=g.reset_index(drop=True); n=len(g)
        if n<30: continue
        o=g.open.to_numpy(float);hi=g.high.to_numpy(float);lo=g.low.to_numpy(float);cl=g.close.to_numpy(float)
        yr=g.date.astype(str).str[:4].to_numpy()
        em=(g.conf_n>=3).to_numpy() if ec is None else g[ec].to_numpy(bool)
        last=-99
        for D in np.flatnonzero(em):
            if D-last<5: continue
            last=D
            r=sim(D,o,hi,lo,cl,n)
            if r is None: continue
            rec.append((yr[D],tls_within(D,o,hi,lo,cl,n),cl[D],r))
    R=pd.DataFrame(rec,columns=["yr","tls","px","ret"])
    tl=R[R.tls]; rest=R[~R.tls]; frac=R.tls.mean()
    print("="*92); print(f"{en}: {len(R)} fires · TLS-marked {len(tl)} ({frac*100:.0f}%)"); print("="*92)
    line("ALL fires", blk(R.ret)); line("TLS-marked", blk(tl.ret)); line("not-TLS", blk(rest.ret))
    print(f"   → TLS lift vs all: {blk(tl.ret)['mean']-blk(R.ret)['mean']:+.2f}pp mean")
    # per year + train/test
    for w in ["2021","2022","2023","2024","2025","2026","TRAIN 21-23","TEST 24-26"]:
        if "TRAIN" in w: sub=tl[tl.yr.isin(["2021","2022","2023"])]; al=R[R.yr.isin(["2021","2022","2023"])]
        elif "TEST" in w: sub=tl[tl.yr.isin(["2024","2025","2026"])]; al=R[R.yr.isin(["2024","2025","2026"])]
        else: sub=tl[tl.yr==w]; al=R[R.yr==w]
        d=blk(sub.ret); da=blk(al.ret)
        if d and da:
            print(f"     {w:12} TLS n={d['n']:4} mean {d['mean']:+6.2f}%  vs all {da['mean']:+6.2f}%  lift {d['mean']-da['mean']:+5.2f}pp")
    # price bucket (TEST era only, where n is big)
    print("   price buckets (TLS-marked, all yrs):")
    for lo_,hi_,bl in [(5,21,"$5-21"),(21,89,"$21-89"),(89,1e9,"$89+")]:
        d=blk(tl[(tl.px>=lo_)&(tl.px<hi_)].ret); line("  "+bl,d)
    # random control: pick frac of fires at random, 200x, does mean match TLS?
    base=R.ret.to_numpy(); k=len(tl)
    draws=np.array([rng.choice(base,k,replace=False).mean() for _ in range(200)])*100
    z=(blk(tl.ret)['mean']-draws.mean())/draws.std() if draws.std()>0 else float('nan')
    print(f"   RANDOM {frac*100:.0f}% of fires: mean {draws.mean():+.2f}% ± {draws.std():.2f}  →  TLS is {z:+.2f}σ above random")
    print()
