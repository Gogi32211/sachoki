"""Does the 4H reversal-turn LEAD the 1D one, and is entering EARLY worth it?
REV-turn (both TFs, symmetric, no beta gate — beta semantics differ per TF):
    min-5-bar RSI < 38, RSI rising, up bar, RSI 30-55.
Arms:
  A) POLICY: enter on EVERY 4h REV trigger (next 4h open), exit on the DAILY path
     (trail25 / -15% / 60d) starting next trading day. n-capped by 10-bar cooldown.
  B) MATCHED: daily REV on day D that had a 4h REV in [D-1, D] — compare
     ret_std  (enter next DAILY open after D)  vs
     ret_early(enter at the 4h trigger's next-4h-open price, same daily walk).
Liquid $3M+, price>=5. Per year + $21-89. Approximation note: early entry's first
partial day is not intraday-walked (stop checks start next daily bar) — slightly
optimistic for BOTH arms equally on that day.
"""
import sys, numpy as np, pandas as pd, duckdb, datetime
sys.path.insert(0,"/Users/sachoki/Desktop/sachoki-desktop/backend")
S,TRAIL,HARD,MAXH=0.0015,0.25,0.15,60

def sim(o,hi,lo,cl,start,entry):
    n=len(cl); e=entry*(1+S)
    if e<=0 or start>=n: return None
    pk=e; hard=e*(1-HARD); end=min(start+MAXH,n)
    for j in range(start,end):
        if o[j]<=hard and j>start: return o[j]/e-1-S
        if lo[j]<=hard: return -HARD-S
        pk=max(pk,hi[j]); ts=pk*(1-TRAIL)
        if o[j]<=ts and j>start: return o[j]/e-1-S
        if lo[j]<=ts: return ts/e-1-S
    return cl[end-1]/e-1-S

def rev_mask(rs,cl):
    n=len(rs); m=np.zeros(n,bool)
    if n<6: return m
    m5=pd.Series(rs).rolling(5,min_periods=2).min().shift(1).to_numpy()
    up=np.concatenate([[False],cl[1:]>cl[:-1]])
    ris=np.concatenate([[False],rs[1:]>rs[:-1]])
    m=(m5<38)&(rs>=30)&(rs<=55)&up&ris
    m[np.isnan(m5)|np.isnan(rs)]=False
    return m

# ── daily ──
ad=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=ad.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,rsi_14,close*volume dv,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
ad.close()
D["d"]=D["date"].astype(str).str[:10]
print("daily rows",len(D),flush=True)

# ── 4h ──
a4=duckdb.connect('../data/studio_4h.duckdb',read_only=True)
H=a4.execute("""WITH r AS (SELECT ticker,date,open,close,rsi_14,close*volume dv,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a4.close()
print("4h rows",len(H),flush=True)

recA=[]; recB=[]
h_by=dict(tuple(H.groupby("ticker",sort=False)))
done=0
for tk,gd in D.groupby("ticker",sort=False):
    gd=gd.reset_index(drop=True)
    gh=h_by.get(tk)
    if gh is None or len(gd)<30 or len(gh)<30: continue
    gh=gh.reset_index(drop=True)
    o,hi,lo,cl=(gd[c].to_numpy(float) for c in("open","high","low","close"))
    rs=gd.rsi_14.to_numpy(float); dv=gd.dv.to_numpy(float); dstr=gd.d.tolist()
    didx={s:i for i,s in enumerate(dstr)}
    ho=gh.open.to_numpy(float); hc=gh.close.to_numpy(float); hrs=gh.rsi_14.to_numpy(float)
    hdv=gh.dv.to_numpy(float)
    hday=[str(x)[:10] for x in gh.date]
    mh=rev_mask(hrs,hc); md=rev_mask(rs,cl)
    yr=[s[:4] for s in dstr]
    # A: every 4h trigger (cooldown 10 4h-bars), enter next-4h-open, daily walk from next day
    last=-99
    for k in np.flatnonzero(mh):
        if k-last<10 or k+1>=len(gh): continue
        last=k
        di=didx.get(hday[k])
        if di is None or di+1>=len(gd) or dv[di]<3e6: continue
        r=sim(o,hi,lo,cl,di+1,ho[k+1])
        if r is not None: recA.append((yr[di],cl[di],r))
    # B: matched — daily REV day D with a 4h trigger on D or D-1
    hset={}
    for k in np.flatnonzero(mh):
        if k+1<len(gh): hset.setdefault(hday[k],k)
    for di in np.flatnonzero(md):
        if di+1>=len(gd) or dv[di]<3e6: continue
        k=hset.get(dstr[di]) or (hset.get(dstr[di-1]) if di>0 else None)
        if k is None: continue
        r_std=sim(o,hi,lo,cl,di+1,o[di+1])
        r_early=sim(o,hi,lo,cl,di+1,ho[k+1])
        if r_std is not None and r_early is not None:
            recA_lead=(gh.date.iloc[k].toordinal() if hasattr(gh.date.iloc[k],'toordinal') else 0)
            recB.append((yr[di],cl[di],r_std,r_early,(o[di+1]-ho[k+1])/ho[k+1]*100))
    done+=1
    if done%800==0: print(f"  {done} tickers",flush=True)

A=pd.DataFrame(recA,columns=["yr","px","ret"])
B=pd.DataFrame(recB,columns=["yr","px","std","early","entry_adv_pct"])
print(f"\n=== A) POLICY: enter on every 4h REV (daily-walk exit)  n={len(A)}")
print(f"  mean {A.ret.mean()*100:+.2f}%  med {A.ret.median()*100:+.2f}%  win {(A.ret>0).mean()*100:.0f}%")
for y,g in A.groupby("yr"): print(f"    {y}: {g.ret.mean()*100:+.2f}% (n={len(g)})")
q=A[(A.px>=21)&(A.px<89)]
print(f"  $21-89: mean {q.ret.mean()*100:+.2f}% med {q.ret.median()*100:+.2f}% win {(q.ret>0).mean()*100:.0f}% n={len(q)}")
print(f"\n=== B) MATCHED daily-REV with 4h lead  n={len(B)}  (how often daily REV had a 4h lead: computed below)")
print(f"  entry-price advantage (std open vs early 4h fill): mean {B.entry_adv_pct.mean():+.2f}%  med {B.entry_adv_pct.median():+.2f}%")
print(f"  ret STANDARD: mean {B['std'].mean()*100:+.2f}%  med {B['std'].median()*100:+.2f}%  win {(B['std']>0).mean()*100:.0f}%")
print(f"  ret EARLY:    mean {B.early.mean()*100:+.2f}%  med {B.early.median()*100:+.2f}%  win {(B.early>0).mean()*100:.0f}%")
print(f"  Δ(early−std): mean {(B.early-B['std']).mean()*100:+.2f}pp  med {(B.early-B['std']).median()*100:+.2f}pp")
yp=0;yt=0
for y,g in B.groupby("yr"):
    if len(g)<50: continue
    d_=(g.early-g['std']).mean()*100; yt+=1; yp+=int(d_>0)
    print(f"    {y}: Δ {d_:+.2f}pp (n={len(g)})")
print(f"  → early beats standard {yp}/{yt} years")
q=B[(B.px>=21)&(B.px<89)]
print(f"  $21-89: Δ mean {(q.early-q['std']).mean()*100:+.2f}pp  med {(q.early-q['std']).median()*100:+.2f}pp  n={len(q)}")
