"""(2026-07-21) User's two chart observations:
 A) SAME-BAR L-row stacks: L34 with BE↑/BX↑/BO↑, +FRI34, +BLUE — do stacks matter?
 B) FAILED BEAR CAMPAIGN → turn: several down-pressure bars in a 7-bar window
    (fbo_dn/eb_dn/vbo_dn events, D-signals, bear-Z chain) where price HELD, then a
    bullish turn bar (T1G / bull-T), optionally with an up-break event.
Path-sim standard + random-z."""
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
  coalesce(z_sig,'') z, coalesce(t_sig,'') t, coalesce(l_sig,'') l,
  coalesce(CAST(bo_up AS TINYINT),0) bo, coalesce(CAST(bx_up AS TINYINT),0) bx,
  coalesce(CAST(be_up AS TINYINT),0) be, coalesce(CAST(sig_fri34 AS TINYINT),0) f34,
  coalesce(CAST(sig_blue AS TINYINT),0) blu,
  coalesce(CAST(sig_fbo_dn AS TINYINT),0) fbod, coalesce(CAST(sig_eb_dn AS TINYINT),0) ebd,
  coalesce(CAST(sig_vbo_dn AS TINYINT),0) vbod, coalesce(CAST(sig_any_d AS TINYINT),0) anyd,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D['dv']=D.close*D.volume
BEARZ=('Z2','Z2G','Z4','Z6','Z10','Z12')
BULLT=('T1G','T1','T2G','T3','T5','T9','T11','T12')
def masks(g):
    n=len(g)
    isl=(g.l=='L34').to_numpy()
    upbrk=((g.bo==1)|(g.bx==1)|(g.be==1)).to_numpy()
    f34=(g.f34==1).to_numpy(); blu=(g.blu==1).to_numpy()
    press=((g.fbod==1)|(g.ebd==1)|(g.vbod==1)|(g.anyd==1)|g.z.isin(BEARZ)).to_numpy()
    t1g=(g.t=='T1G').to_numpy(); bullt=g.t.isin(BULLT).to_numpy()
    cl=g.close.to_numpy(float)
    pr=pd.Series(press.astype(float)).shift(1).rolling(7,min_periods=1).sum().fillna(0).to_numpy()
    held=np.zeros(n,bool)
    held[7:]=cl[7:]>=cl[:-7]*0.95
    return {
     "a1 L34+upBRK":      isl&upbrk,
     "a2 L34+FRI34":      isl&f34,
     "a3 L34+FRI34+BX":   isl&f34&upbrk,
     "a4 L34+BLUE":       isl&blu,
     "b1 წნეხი≥3→T1G":     (pr>=3)&t1g,
     "b2 წნეხი≥3→bullT":   (pr>=3)&bullt,
     "b3 წნეხი≥3+HELD→T1G":(pr>=3)&held&t1g,
     "b4 წნეხი≥3+HELD→bullT+brk": (pr>=3)&held&bullt&upbrk,
     "b5 წნეხი≥4+HELD→T1G":(pr>=4)&held&t1g,
     "b6 kontr: წნეხი≥3 collapse→T1G": (pr>=3)&~held&t1g,
    }
trades={}; pool=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<40: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    dv=g.dv.to_numpy(float); yr=g.date.astype(str).str[:4].tolist()
    M=masks(g)
    last={k:-99 for k in M}
    for i in range(8,n-1):
        if dv[i]<3e6: continue
        if i%33==0:
            r=sim(o,hi,lo,cl,i+1)
            if r is not None: pool.append(r)
        for k,m in M.items():
            if m[i] and i-last[k]>=5:
                r=sim(o,hi,lo,cl,i+1)
                if r is not None:
                    trades.setdefault(k,[]).append((yr[i],cl[i],r)); last[k]=i
P=np.array(pool); rng=np.random.default_rng(17)
print(f"random pool n={len(P):,} mean {100*P.mean():+.2f}%\n")
for k in masks(D.head(60)).keys():
    T=pd.DataFrame(trades.get(k,[]),columns=["yr","px","ret"])
    if len(T)<80: print(f"{k:28} n={len(T)} too few"); continue
    yrs=T.groupby('yr').ret.mean()*100
    tr=T[T.yr.isin(('2021','2022','2023'))]; te=T[T.yr.isin(('2024','2025','2026'))]
    trm=100*tr.ret.mean() if len(tr)>=30 else float('nan')
    tem=100*te.ret.mean() if len(te)>=30 else float('nan')
    pfd=-T.ret[T.ret<=0].sum(); pf=T.ret[T.ret>0].sum()/pfd if pfd>0 else float('nan')
    bs=np.array([P[rng.integers(0,len(P),len(T))].mean() for _ in range(2000)])
    z=(T.ret.mean()-bs.mean())/bs.std()
    print(f"{k:28} n={len(T):6,} mean {100*T.ret.mean():+.2f}% med {100*T.ret.median():+.2f}% "
          f"win {100*(T.ret>0).mean():.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr z={z:+.1f}σ | TR {trm:+.2f} TE {tem:+.2f}")
