"""A) FLY second-appearance rule: F1 = FLY after >=15-bar absence; if the F1 run dies
   within <=3 bars and FLY returns (F2) within 8 bars of F1 -> user's entry = F2.
   Compare: enter@F1 (naive) vs enter@F2 vs F1-without-return (fizzle).
B) Heavy-L cluster: >=2 bars with l_sig in {L34,L43,L64,L22} within trailing 7 bars,
   entry on an up-bar; variant with a buy-context (buy_score>=60 in the window).
Path-sim trail25/-15%/60d, per-year, TRAIN/TEST, price buckets, random control."""
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
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,close*volume dv,
  CASE WHEN coalesce(fly_sig,'')<>'' THEN 1 ELSE 0 END fly,
  coalesce(l_sig,'') l, buy_score,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D["date"]=D["date"].astype(str).str[:10]
HEAVY={'L34','L43','L64','L22'}
A1=[];A2=[];AF=[];B1=[];B2=[];pool=[]
rng=np.random.default_rng(5)
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<60: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    fly=g.fly.to_numpy(int); dv=g.dv.to_numpy(float); ds=g.date.tolist()
    lh=np.array([1 if x in HEAVY else 0 for x in g.l])
    bs=g.buy_score.to_numpy(float)
    for i in range(0,n-1,17):
        if dv[i]>=3e6:
            r=sim(o,hi,lo,cl,i+1,o[i+1])
            if r is not None: pool.append(r)
    # A) FLY logic
    i=16
    while i<n-1:
        if fly[i]==1 and fly[i-15:i].sum()==0 and dv[i]>=3e6:
            F1=i
            r1=sim(o,hi,lo,cl,F1+1,o[F1+1])
            # F1 run length
            j=F1
            while j+1<n and fly[j+1]==1: j+=1
            runlen=j-F1+1
            F2=None
            if runlen<=3 and j+1<n:
                for k in range(j+1,min(F1+9,n)):
                    if fly[k]==1: F2=k; break
            if F2 is not None and F2+1<n and dv[F2]>=3e6:
                r2=sim(o,hi,lo,cl,F2+1,o[F2+1])
                if r1 is not None: A1.append((ds[F1][:4],cl[F1],r1))
                if r2 is not None: A2.append((ds[F2][:4],cl[F2],r2))
                i=F2+5
            else:
                if r1 is not None: AF.append((ds[F1][:4],cl[F1],r1))
                i=j+5
        else: i+=1
    # B) heavy-L cluster
    hl7=pd.Series(lh).rolling(7,min_periods=1).sum().to_numpy()
    bs7=pd.Series(bs).rolling(7,min_periods=1).max().to_numpy()
    last=-99
    for i in range(7,n-1):
        if hl7[i]>=2 and cl[i]>cl[i-1] and dv[i]>=3e6 and i-last>=5:
            r=sim(o,hi,lo,cl,i+1,o[i+1])
            if r is None: continue
            B1.append((ds[i][:4],cl[i],r)); last=i
            if bs7[i]>=60: B2.append((ds[i][:4],cl[i],r))
pool=np.array(pool)
print(f"baseline pool n={len(pool)} mean {pool.mean()*100:+.2f}%\n")
def rep(lab,rows):
    R=pd.DataFrame(rows,columns=["yr","px","ret"])
    if len(R)<50: print(f"{lab:28} n={len(R)} too few"); return
    w=(R.ret>0).mean()*100; pfd=-R.ret[R.ret<=0].sum(); pf=R.ret[R.ret>0].sum()/pfd if pfd>0 else float('nan')
    draws=np.array([rng.choice(pool,len(R),replace=False).mean() for _ in range(300)])*100
    z=(R.ret.mean()*100-draws.mean())/draws.std()
    tr=R[R.yr.isin(['2021','2022','2023'])]; te=R[R.yr.isin(['2024','2025','2026'])]
    yrs=R.groupby('yr').ret.mean()*100
    q=R[(R.px>=21)&(R.px<89)]
    print(f"{lab:28} n={len(R):6} mean {R.ret.mean()*100:+.2f}% med {R.ret.median()*100:+.2f}% win {w:.0f}% PF {pf:.2f} {z:+.1f}σ {int((yrs>0).sum())}/{len(yrs)}yr | TR {tr.ret.mean()*100:+.2f} TE {te.ret.mean()*100:+.2f} | $21-89 {q.ret.mean()*100:+.2f}%")
print("A) FLY reappearance:")
rep("F1 (first, naive)",A1)
rep("F2 (SECOND = user's rule)",A2)
rep("F1-fizzle (never returned)",AF)
print("\nB) heavy-L cluster (≥2 of L34/L43/L64/L22 in 7 bars, up-bar):")
rep("cluster alone",B1)
rep("cluster + buy_score≥60 ctx",B2)
