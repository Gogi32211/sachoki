"""The 6 named candlestick patterns as ENTRY TRIGGERS on our edges — MEASUREMENT, not a
gate. Each pattern is a pre-specified mechanism (not cherry-picked from a haystack), so
this is a fair low-multiple-testing test. Edge fires on bar D (STATE); within (D, D+5] we
wait for the pattern (WHEN) and enter next-open. Exit constant for all: trail25/-15%/60bar.
Report: coverage, FULL path-sim, and MATCHED vs immediate (same fires) + per-year sign.
No pass/fail — the numbers are the answer; sizing/tradeability is the user's call.
"""
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
import edge_replay as ER
S, TRAIL, HARD, MAXH, WIN = 0.0015, 0.25, 0.15, 60, 5

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

def trig(pol,D,o,hi,lo,cl,n):
    if pol=="IMM": return D
    for j in range(D+1,min(D+WIN+1,n)):
        grn = cl[j]>o[j]
        body=abs(cl[j]-o[j]); rng=hi[j]-lo[j]
        if pol=="PB"  and lo[j]<=lo[D] and grn: return j                      # pullback-reclaim (Phase1 winner)
        if pol=="ML"  and j>=1 and abs(lo[j]-lo[j-1])<=0.004*cl[j] and grn and cl[j]>=cl[j-1]:
            return j                                                          # Matching Low (double-bottom)
        if pol=="TLS" and j>=3 and cl[j-3]>cl[j-2]>cl[j-1] and grn and cl[j]>hi[j-3]:
            return j                                                          # Three Line Strike bull
        if pol=="AB"  and j>=2 and hi[j-1]<lo[j-2] and lo[j]>hi[j-1] and grn:
            return j                                                          # Abandoned Baby (island)
        if pol=="ENG" and j>=1 and grn and cl[j]>hi[j-1] and o[j]<cl[j-1] and body>abs(cl[j-1]-o[j-1]):
            return j                                                          # Engulfing (Phase1 loser, ref)
    return None

POLS=["IMM","PB","ML","TLS","AB","ENG"]
EDGES=[("G3-Abs","E_g3abs"),("QZ-Capit","E_qzcapit"),("Atomic","E_atomic"),("Cluster3",None)]
grp,as_of=ER._frame(72,3_000_000)
print(f"as_of {as_of} · {len(grp)} tickers · MEASUREMENT (no gate)\n")

def st(a):
    a=np.asarray([x for x in a if x is not None])
    if len(a)<20: return None
    return dict(n=len(a),mean=a.mean()*100,med=np.median(a)*100,win=(a>0).mean()*100,
                pf=(a[a>0].sum()/-a[a<=0].sum()) if (a<=0).any() else np.nan)

for en,ec in EDGES:
    recs=[]
    for tk,g in grp.items():
        g=g.reset_index(drop=True); n=len(g)
        if n<30: continue
        o=g.open.to_numpy(float);hi=g.high.to_numpy(float);lo=g.low.to_numpy(float);cl=g.close.to_numpy(float)
        yr=g.date.astype(str).str[:4].to_numpy()
        em=(g.conf_n>=3).to_numpy() if ec is None else g[ec].to_numpy(bool)
        last=-99
        for D in np.flatnonzero(em):
            if D-last<5: continue
            last=D; imm=sim(D,o,hi,lo,cl,n)
            for p in POLS:
                j=trig(p,D,o,hi,lo,cl,n)
                r=sim(j,o,hi,lo,cl,n) if j is not None else None
                recs.append((yr[D],p,r,imm,r is not None))
    R=pd.DataFrame(recs,columns=["yr","pol","ret","imm","trig"])
    base=st(R[R.pol=="IMM"].ret)
    print("="*100)
    print(f"{en}: {base['n']} fires · immediate baseline: mean {base['mean']:+.2f}%  win {base['win']:.0f}%  PF {base['pf']:.2f}")
    print("="*100)
    print(f"{'trigger':6} {'cover':>6} | FULL {'n':>5} {'mean':>7} {'med':>7} {'win':>5} {'PF':>5} | MATCHED-vs-IMM Δmean  yrs+")
    for p in POLS:
        if p=="IMM": continue
        sub=R[R.pol==p]; cov=sub.trig.mean()*100
        f=st(sub[sub.trig].ret)
        m=sub[sub.trig & sub.imm.notna() & sub.ret.notna()]
        dmu=(m.ret.mean()-m.imm.mean())*100 if len(m) else np.nan
        yp=yt=0
        for y,gy in m.groupby("yr"):
            if len(gy)<15: continue
            yt+=1; yp+=int((gy.ret.mean()-gy.imm.mean())>0)
        fs=f"{f['n']:5} {f['mean']:+6.2f} {f['med']:+6.2f} {f['win']:4.0f}% {f['pf']:4.2f}" if f else "  too few"
        ms=f"{dmu:+6.2f}pp  {yp}/{yt}" if len(m)>=20 else "  too few"
        print(f"{p:6} {cov:5.0f}% | {fs} | {ms}")
    print()
