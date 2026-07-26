"""BO↑/BX↑/BE↑/VBO↑ (heavy-L body-break events) — solo per-year + partners vs the
amplifier set + fwd-20 AND path-sim spot-check on best cells. Liquid dv>=3M."""
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
T=['bo_up','bx_up','be_up','vbo_up']
AMP=['sig_bias_up','sig_vol_10x','sig_vol_20x','load','sig_conso','sig_nd_vabs','sig_ns_vabs',
     'sig_rl','pb_pp_rtv','sig_cci','sig_cci0r','bf_buy','hilo_buy']
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
sel=", ".join(f'coalesce(CAST("{s}" AS TINYINT),0) AS "{s}"' for s in T+AMP)
D=a.execute(f"""WITH r AS (SELECT ticker,date,open,high,low,close,volume,rsi_14,
  lead(close,20) OVER (PARTITION BY ticker ORDER BY date) f20, {sel},
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT ticker, strftime(date,'%Y') yr, open,high,low,close, close*volume dv, rsi_14,
        f20/close-1 r20, {", ".join(chr(34)+s+chr(34) for s in T+AMP)}
 FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
L=D[(D.dv>=3e6)&D.r20.notna()]
r20=L.r20.to_numpy(); yr=L.yr.to_numpy()
YRS=['2021','2022','2023','2024','2025','2026']
ymask={y:(yr==y) for y in YRS}
base_y={y:100*(r20[ymask[y]]>0).mean() for y in YRS}
M={s:(L[s].to_numpy()==1) for s in T+AMP}
def yrs_above(m,minn=15):
    k=0;t=0
    for y in YRS:
        mm=m&ymask[y]; n=int(mm.sum())
        if n<minn: continue
        t+=1
        if 100*(r20[mm]>0).mean()>base_y[y]: k+=1
    return k,t
def yline(m,minn=15):
    return "/".join(f"{100*(r20[m&ymask[y]]>0).mean():.0f}" if (m&ymask[y]).sum()>=minn else "·" for y in YRS)
solo={}
print("══ SOLO (fwd-20, per-year) ══")
for s in T+AMP:
    m=M[s]; n=int(m.sum())
    if n<200: continue
    u=100*(r20[m]>0).mean(); solo[s]=u
    if s in T:
        k,t=yrs_above(m)
        print(f"  {s:8} n={n:8,} up {u:4.1f}% med {100*np.median(r20[m]):+5.2f}% | {k}/{t}yr+ | {yline(m)}")
print("\n══ პარტნიორები (syn≥2.5, წლები≥4, n≥250) ══")
res=[]
for s in T:
    for o_ in AMP+[x for x in T if x!=s]:
        if o_ not in solo or s not in solo: continue
        m=M[s]&M[o_]; n=int(m.sum())
        if n<250: continue
        u=100*(r20[m]>0).mean(); syn=u-max(solo[s],solo[o_])
        k,t=yrs_above(m)
        res.append((syn,u,s,o_,n,k,t,m))
res.sort(key=lambda x:-x[0])
best_cells=[]
for syn,u,s,o_,n,k,t,m in res[:14]:
    mark="✓" if (syn>=2.5 and k>=4) else " "
    print(f" {mark}{s:8}+{o_:14} n={n:6,} up {u:4.1f}% syn {syn:+4.1f} {k}/{t}yr | {yline(m)}")
    if syn>=2.5 and k>=4: best_cells.append((s,o_))
print("\n══ path-sim ტოპ-უჯრებზე (trail25/-15/60, cooldown-5) ══",flush=True)
for s,o_ in best_cells[:4]:
    trades=[]
    for tk,g in D[(D[s]==1)|(True)].groupby("ticker",sort=False):
        g=g.reset_index(drop=True); n=len(g)
        if n<40: continue
        o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
        dv=g.dv.to_numpy(float); ds=g.yr.tolist()
        mk=((g[s]==1)&(g[o_]==1)).to_numpy()
        last=-99
        for i in range(5,n-1):
            if not mk[i] or dv[i]<3e6 or i-last<5: continue
            r=sim(o,hi,lo,cl,i+1)
            if r is not None: trades.append((ds[i],r)); last=i
    Tt=pd.DataFrame(trades,columns=["yr","ret"])
    if len(Tt)<80: print(f"  {s}+{o_}: n={len(Tt)} too few"); continue
    yrs=Tt.groupby('yr').ret.mean()*100
    pfd=-Tt.ret[Tt.ret<=0].sum(); pf=Tt.ret[Tt.ret>0].sum()/pfd if pfd>0 else float('nan')
    print(f"  {s}+{o_:14} n={len(Tt):5,} mean {100*Tt.ret.mean():+.2f}% med {100*Tt.ret.median():+.2f}% "
          f"win {100*(Tt.ret>0).mean():.0f}% PF {pf:.2f} {int((yrs>0).sum())}/{len(yrs)}yr | "
          +" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items()))
