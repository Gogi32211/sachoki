"""FINAL VALIDATION (2026-07-21): the refined failed-bear-turn cell
   pressure>=N (7-bar) + price HELD + T1G turn + oversold level (CCI<-100 / RSI 30-40)
Plateau: window 5/7/10 · HELD 0.93/0.95 · CCI -80/-100/-120 · P3/P4 · bullT variant.
Plus L34+upBRK@CCI<-100 (WATCH candidate). Standard: path-sim, cooldown-5, random-z,
per-year, TR/TE, price buckets."""
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
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume,rsi_14,cci_20,
  coalesce(z_sig,'') z, coalesce(t_sig,'') t, coalesce(l_sig,'') l,
  coalesce(CAST(bo_up AS TINYINT),0) bo, coalesce(CAST(bx_up AS TINYINT),0) bx,
  coalesce(CAST(be_up AS TINYINT),0) be,
  coalesce(CAST(sig_fbo_dn AS TINYINT),0) fbod, coalesce(CAST(sig_eb_dn AS TINYINT),0) ebd,
  coalesce(CAST(sig_vbo_dn AS TINYINT),0) vbod, coalesce(CAST(sig_any_d AS TINYINT),0) anyd,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D['dv']=D.close*D.volume
BEARZ=('Z2','Z2G','Z4','Z6','Z10','Z12')
BULLT=('T1G','T1','T2G','T3','T5','T9','T11','T12')
trades={}; pool=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<40: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    rs=g.rsi_14.to_numpy(float); cc=g.cci_20.to_numpy(float)
    dv=g.dv.to_numpy(float); yr=g.date.astype(str).str[:4].tolist()
    isl=(g.l=='L34').to_numpy()
    upbrk=((g.bo==1)|(g.bx==1)|(g.be==1)).to_numpy()
    press=((g.fbod==1)|(g.ebd==1)|(g.vbod==1)|(g.anyd==1)|g.z.isin(BEARZ)).to_numpy()
    t1g=(g.t=='T1G').to_numpy(); bullt=g.t.isin(BULLT).to_numpy()
    pser=pd.Series(press.astype(float))
    prW={W:pser.shift(1).rolling(W,min_periods=1).sum().fillna(0).to_numpy() for W in (5,7,10)}
    heldT={}
    for th in (0.93,0.95):
        hh=np.zeros(n,bool); hh[7:]=cl[7:]>=cl[:-7]*th
        heldT[th]=hh
    ccio={th:(cc<th) for th in (-80,-100,-120)}
    r3040=(rs>=30)&(rs<40); r3045=(rs>=30)&(rs<45)
    V={
     "F1 P3·H95·T1G·CCI<-100":  (prW[7]>=3)&heldT[0.95]&t1g&ccio[-100],
     "F2 P4·H95·T1G·CCI<-100":  (prW[7]>=4)&heldT[0.95]&t1g&ccio[-100],
     "F3 P3·H95·T1G·RSI30-40":  (prW[7]>=3)&heldT[0.95]&t1g&r3040,
     "F4 P4·H95·T1G·RSI30-40":  (prW[7]>=4)&heldT[0.95]&t1g&r3040,
     "F5 P3·H95·T1G·CCI&RSI":   (prW[7]>=3)&heldT[0.95]&t1g&ccio[-100]&r3045,
     "F6 (პლატო W5)":           (prW[5]>=3)&heldT[0.95]&t1g&ccio[-100],
     "F7 (პლატო W10)":          (prW[10]>=4)&heldT[0.95]&t1g&ccio[-100],
     "F8 (პლატო H93)":          (prW[7]>=4)&heldT[0.93]&t1g&ccio[-100],
     "F9 (პლატო CCI<-80)":      (prW[7]>=4)&heldT[0.95]&t1g&ccio[-80],
     "F10 (პლატო CCI<-120)":    (prW[7]>=4)&heldT[0.95]&t1g&ccio[-120],
     "F11 bullT·CCI<-100":      (prW[7]>=3)&heldT[0.95]&bullt&ccio[-100],
     "F12 L34+upBRK·CCI<-100":  isl&upbrk&ccio[-100],
    }
    last={k:-99 for k in V}
    for i in range(11,n-1):
        if dv[i]<3e6: continue
        if i%29==0:
            r=sim(o,hi,lo,cl,i+1)
            if r is not None: pool.append(r)
        for k,m in V.items():
            if m[i] and i-last[k]>=5:
                r=sim(o,hi,lo,cl,i+1)
                if r is not None:
                    trades.setdefault(k,[]).append((yr[i],cl[i],r)); last[k]=i
P=np.array(pool); rng=np.random.default_rng(21)
print(f"random pool n={len(P):,} mean {100*P.mean():+.2f}%\n")
order=["F1 P3·H95·T1G·CCI<-100","F2 P4·H95·T1G·CCI<-100","F3 P3·H95·T1G·RSI30-40",
 "F4 P4·H95·T1G·RSI30-40","F5 P3·H95·T1G·CCI&RSI","F6 (პლატო W5)","F7 (პლატო W10)",
 "F8 (პლატო H93)","F9 (პლატო CCI<-80)","F10 (პლატო CCI<-120)","F11 bullT·CCI<-100","F12 L34+upBRK·CCI<-100"]
for k in order:
    T=pd.DataFrame(trades.get(k,[]),columns=["yr","px","ret"])
    if len(T)<80: print(f"{k:26} n={len(T)} too few"); continue
    yrs=T.groupby('yr').ret.mean()*100
    tr=T[T.yr.isin(('2021','2022','2023'))]; te=T[T.yr.isin(('2024','2025','2026'))]
    trm=100*tr.ret.mean() if len(tr)>=25 else float('nan')
    tem=100*te.ret.mean() if len(te)>=25 else float('nan')
    pfd=-T.ret[T.ret<=0].sum(); pf=T.ret[T.ret>0].sum()/pfd if pfd>0 else float('nan')
    bs=np.array([P[rng.integers(0,len(P),len(T))].mean() for _ in range(2000)])
    z=(T.ret.mean()-bs.mean())/bs.std()
    q2=T[(T.px>=21)&(T.px<89)]
    qm=f"{100*q2.ret.mean():+.1f}" if len(q2)>=30 else "·"
    print(f"{k:26} n={len(T):6,} mean {100*T.ret.mean():+.2f}% med {100*T.ret.median():+.2f}% "
          f"win {100*(T.ret>0).mean():.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr z={z:+.1f}σ | "
          f"TR {trm:+.2f} TE {tem:+.2f} | $21-89 {qm} | "+" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items()))
