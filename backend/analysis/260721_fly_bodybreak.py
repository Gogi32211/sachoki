"""(2026-07-21) USER's spotted combo — FLY chain + body-break (BO/BX/BE↑) same bar.
This exact pair was NEVER measured (fell between the sweep groups). Variants:
context (uptrend/high-base per the 4 examples), ✦fresh, T2/T2G bar, window-FLY.
Dual metric: path-sim trail25/-15/60/cooldown-5 + per-year + TR/TE + random-z."""
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
  coalesce(t_sig,'') t,
  coalesce(CAST(bo_up AS TINYINT),0) bo, coalesce(CAST(bx_up AS TINYINT),0) bx,
  coalesce(CAST(be_up AS TINYINT),0) be,
  coalesce(CAST(sig_fly_abcd AS TINYINT),0)+coalesce(CAST(sig_fly_cd AS TINYINT),0)
   +coalesce(CAST(sig_fly_bd AS TINYINT),0)+coalesce(CAST(sig_fly_ad AS TINYINT),0) flyn,
  max(high) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) hi20,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D['dv']=D.close*D.volume
trades={}; pool=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<60: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    rs=g.rsi_14.to_numpy(float); dv=g.dv.to_numpy(float); yr=g.date.astype(str).str[:4].tolist()
    fly=(g.flyn.to_numpy()>0)
    upbrk=((g.bo==1)|(g.bx==1)|(g.be==1)).to_numpy()
    t2=(g.t.isin(('T2','T2G'))).to_numpy()
    hi20=g.hi20.to_numpy(float)
    e200=pd.Series(cl).ewm(span=200,adjust=False).mean().to_numpy()
    highbase=(cl>e200)&(cl>=hi20*0.85)
    # fresh FLY: first fly after >=15 silent bars
    fresh=np.zeros(n,bool); lastf=-99
    for i in range(n):
        if fly[i]:
            fresh[i]=(i-lastf)>15; lastf=i
    flyw=pd.Series(fly.astype(float)).shift(1).rolling(5,min_periods=1).max().fillna(0).to_numpy()==1
    V={
     "V1 FLY+upBRK":            fly&upbrk,
     "V2 FLY+upBRK+highbase":   fly&upbrk&highbase,
     "V3 FLY+upBRK+T2/T2G":     fly&upbrk&t2,
     "V4 FLY+upBRK+HB+T2":      fly&upbrk&highbase&t2,
     "V5 ✦fresh+upBRK":         fresh&upbrk,
     "V6 FLYwin5+upBRK+HB":     flyw&upbrk&highbase,
     "V7 upBRK+HB (FLY-გარეშე კონტროლი)": upbrk&highbase&~fly&~flyw,
     "V8 FLY+HB (BRK-გარეშე კონტროლი)":   fly&highbase&~upbrk,
    }
    last={k:-99 for k in V}
    for i in range(21,n-1):
        if dv[i]<3e6: continue
        if i%29==0:
            r=sim(o,hi,lo,cl,i+1)
            if r is not None: pool.append(r)
        for k,m in V.items():
            if m[i] and i-last[k]>=5:
                r=sim(o,hi,lo,cl,i+1)
                if r is not None:
                    trades.setdefault(k,[]).append((yr[i],cl[i],r)); last[k]=i
P=np.array(pool); rng=np.random.default_rng(23)
print(f"random pool n={len(P):,} mean {100*P.mean():+.2f}%\n")
for k in ["V1 FLY+upBRK","V2 FLY+upBRK+highbase","V3 FLY+upBRK+T2/T2G","V4 FLY+upBRK+HB+T2",
          "V5 ✦fresh+upBRK","V6 FLYwin5+upBRK+HB","V7 upBRK+HB (FLY-გარეშე კონტროლი)","V8 FLY+HB (BRK-გარეშე კონტროლი)"]:
    T=pd.DataFrame(trades.get(k,[]),columns=["yr","px","ret"])
    if len(T)<80: print(f"{k:34} n={len(T)} too few"); continue
    yrs=T.groupby('yr').ret.mean()*100
    tr=T[T.yr.isin(('2021','2022','2023'))]; te=T[T.yr.isin(('2024','2025','2026'))]
    trm=100*tr.ret.mean() if len(tr)>=25 else float('nan')
    tem=100*te.ret.mean() if len(te)>=25 else float('nan')
    pfd=-T.ret[T.ret<=0].sum(); pf=T.ret[T.ret>0].sum()/pfd if pfd>0 else float('nan')
    bs=np.array([P[rng.integers(0,len(P),len(T))].mean() for _ in range(2000)])
    z=(T.ret.mean()-bs.mean())/bs.std()
    print(f"{k:34} n={len(T):6,} mean {100*T.ret.mean():+.2f}% med {100*T.ret.median():+.2f}% "
          f"win {100*(T.ret>0).mean():.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr z={z:+.1f}σ | TR {trm:+.2f} TE {tem:+.2f}")
