"""(2026-07-21) Rerun of the stack/pressure patterns SLICED BY RSI and CCI levels
(user: different levels should behave differently). Every bin compared against ITS
OWN bin baseline (random bars in the same RSI/CCI bin) — level-fair comparison.
Cells: L34+upBRK · L34+FRI34 · pressure≥3+HELD→T1G · pressure≥4+HELD→T1G · pressure≥3→bullT."""
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
  coalesce(CAST(be_up AS TINYINT),0) be, coalesce(CAST(sig_fri34 AS TINYINT),0) f34,
  coalesce(CAST(sig_fbo_dn AS TINYINT),0) fbod, coalesce(CAST(sig_eb_dn AS TINYINT),0) ebd,
  coalesce(CAST(sig_vbo_dn AS TINYINT),0) vbod, coalesce(CAST(sig_any_d AS TINYINT),0) anyd,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D['dv']=D.close*D.volume
BEARZ=('Z2','Z2G','Z4','Z6','Z10','Z12')
BULLT=('T1G','T1','T2G','T3','T5','T9','T11','T12')
rows=[]; pool=[]
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True); n=len(g)
    if n<40: continue
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    rs=g.rsi_14.to_numpy(float); cc=g.cci_20.to_numpy(float)
    dv=g.dv.to_numpy(float); yr=g.date.astype(str).str[:4].tolist()
    isl=(g.l=='L34').to_numpy()
    upbrk=((g.bo==1)|(g.bx==1)|(g.be==1)).to_numpy()
    f34=(g.f34==1).to_numpy()
    press=((g.fbod==1)|(g.ebd==1)|(g.vbod==1)|(g.anyd==1)|g.z.isin(BEARZ)).to_numpy()
    t1g=(g.t=='T1G').to_numpy(); bullt=g.t.isin(BULLT).to_numpy()
    pr=pd.Series(press.astype(float)).shift(1).rolling(7,min_periods=1).sum().fillna(0).to_numpy()
    held=np.zeros(n,bool); held[7:]=cl[7:]>=cl[:-7]*0.95
    cells={
     "L34+upBRK":    isl&upbrk,
     "L34+FRI34":    isl&f34,
     "P3+HELD→T1G":  (pr>=3)&held&t1g,
     "P4+HELD→T1G":  (pr>=4)&held&t1g,
     "P3→bullT":     (pr>=3)&bullt,
    }
    last={k:-99 for k in cells}
    for i in range(8,n-1):
        if dv[i]<3e6: continue
        if i%29==0:
            r=sim(o,hi,lo,cl,i+1)
            if r is not None: pool.append((yr[i],rs[i],cc[i],r))
        for k,m in cells.items():
            if m[i] and i-last[k]>=5:
                r=sim(o,hi,lo,cl,i+1)
                if r is not None:
                    rows.append((k,yr[i],rs[i],cc[i],r)); last[k]=i
R=pd.DataFrame(rows,columns=["cell","yr","rsi","cci","ret"])
P=pd.DataFrame(pool,columns=["yr","rsi","cci","ret"])
def binlab_rsi(v): 
    return "<30" if v<30 else "30-40" if v<40 else "40-50" if v<50 else "50-60" if v<60 else "60+"
def binlab_cci(v):
    return "<-100" if v<-100 else "-100..0" if v<0 else "0..100" if v<100 else ">100"
R['rb']=R.rsi.map(binlab_rsi); R['cb']=R.cci.map(binlab_cci)
P['rb']=P.rsi.map(binlab_rsi); P['cb']=P.cci.map(binlab_cci)
def table(dim):
    order=["<30","30-40","40-50","50-60","60+"] if dim=='rb' else ["<-100","-100..0","0..100",">100"]
    print(f"\n{'='*8} ჭრილი: {'RSI' if dim=='rb' else 'CCI'} {'='*8}")
    base={b:100*P[P[dim]==b].ret.mean() for b in order if len(P[P[dim]==b])>=200}
    print("bin-baseline: "+" ".join(f"{b}:{v:+.2f}" for b,v in base.items()))
    for cell in R.cell.unique():
        parts=[]
        for b in order:
            s=R[(R.cell==cell)&(R[dim]==b)]
            if len(s)<80: parts.append(f"{b}: n{len(s)}"); continue
            m=100*s.ret.mean(); d=m-base.get(b,0)
            tr=s[s.yr.isin(('2021','2022','2023'))]; te=s[s.yr.isin(('2024','2025','2026'))]
            trm=100*tr.ret.mean() if len(tr)>=25 else float('nan')
            parts.append(f"{b}: {m:+.2f}(Δ{d:+.2f},n{len(s)},TR{trm:+.1f})")
        print(f"  {cell:14} "+" | ".join(parts))
table('rb'); table('cb')
