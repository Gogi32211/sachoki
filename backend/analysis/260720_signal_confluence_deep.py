"""DEEP pass (2026-07-20g): (1) FULL qualifying pair list; (2) best-partner table for
EVERY signal; (3) ORDERED window pairs A→B (A fired ≤5 bars BEFORE B) — sequencing,
not just same-bar; (4) triples seeded from top pairs. fwd-20 up%, per-year."""
import numpy as np, pandas as pd, duckdb
SIGS = ['sig_best','sig_strong','sig_abs','sig_clm','sig_ns_vabs','sig_nd_vabs','sig_sc','sig_bc',
 'sig_best_up','sig_fbo_up','sig_eb_up','sig_3up','sig_vbo_dn','sig_fri34','sig_fri43','sig_fri64',
 'sig_l555','sig_l2l4','sig_blue','sig_cci','sig_cci0r','sig_ccib','sig_rl','sig_rh','sig_pp',
 'sig_g1','sig_g2','sig_g4','sig_g6','sig_g11','sig_gog_plus','sig_fly_abcd','sig_fly_cd',
 'sig_fly_bd','sig_fly_ad','sig_wk_up','sig_x1','sig_x2','sig_x1g','sig_x3','sig_tz_flip',
 'sig_bias_up','sig_p55','sig_p66','sig_p89','sig_any_p','sig_any_d','sig_buy','sig_3g',
 'sig_conso','sig_svs','sig_cd','sig_ca','sig_cw','sig_vol_5x','sig_vol_10x','sig_vol_20x',
 'sig_flp_up','sig_org_up','sig_dd_up_red','sig_d_up_red','sig_cisd_cplus','sig_para_prep',
 'sig_para_start','sig_para_plus','sig_para_retest','sig_l88','sig_260308',
 'rocket','hilo_buy','load','bf_buy','best_long']
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
sel=", ".join(f'coalesce(CAST("{s}" AS TINYINT),0) AS "{s}"' for s in SIGS)
D=a.execute(f"""WITH r AS (SELECT ticker,date,close,volume,
  lead(close,20) OVER (PARTITION BY ticker ORDER BY date) f20, {sel},
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT ticker, strftime(date,'%Y') yr, f20/close-1 r20, {", ".join(chr(34)+s+chr(34) for s in SIGS)}
 FROM r WHERE rn=1 ORDER BY ticker, date""").fetchdf()
a.close()
print(f"bars: {len(D):,}",flush=True)
have=D.r20.notna().to_numpy()
r20=D.r20.to_numpy(); yr=D.yr.to_numpy()
YRS=['2021','2022','2023','2024','2025','2026']
ymask={y:(yr==y)&have for y in YRS}
base_y={y:100*(r20[ymask[y]]>0).mean() for y in YRS}
M={s:(D[s].to_numpy()==1) for s in SIGS}
# window: A fired in the 5 bars BEFORE current (exclusive) — per-ticker rolling
g=D.groupby('ticker')
W={}
for s in SIGS:
    W[s]=g[s].transform(lambda v: v.shift(1).rolling(5,min_periods=1).max()).fillna(0).to_numpy()==1
print("window masks done",flush=True)
def nm(s): return s[4:] if s.startswith('sig_') else s
def yrs_above(mask,minn=15):
    k=0;t=0
    for y in YRS:
        m=mask&ymask[y]; n=int(m.sum())
        if n<minn: continue
        t+=1
        if 100*(r20[m]>0).mean()>base_y[y]: k+=1
    return k,t
def yline(mask,minn=15):
    out=[]
    for y in YRS:
        m=mask&ymask[y]; n=int(m.sum())
        out.append(f"{100*(r20[m]>0).mean():.0f}" if n>=minn else "·")
    return "/".join(out)
solo={}
for s in SIGS:
    m=M[s]&have
    if m.sum()>=300: solo[s]=100*(r20[m]>0).mean()
keys=list(solo)
# ── 1) FULL qualifying same-bar pairs (syn>=+2.5, yrs+>=4) ──
res=[]
for i in range(len(keys)):
    for j in range(i+1,len(keys)):
        a1,b1=keys[i],keys[j]
        m=M[a1]&M[b1]&have; n=int(m.sum())
        if n<300: continue
        u=100*(r20[m]>0).mean(); syn=u-max(solo[a1],solo[b1])
        if syn<2.5: continue
        k,t=yrs_above(m)
        if k<4: continue
        res.append((syn,u,a1,b1,n,k,t,m))
res.sort(key=lambda x:-x[0])
print(f"\n══ 1) ყველა კვალიფიციური same-bar წყვილი (syn≥2.5, წლები≥4): {len(res)} ══")
for syn,u,a1,b1,n,k,t,m in res:
    print(f"  {nm(a1):12}+{nm(b1):14} n={n:6,} up {u:4.1f}% syn {syn:+4.1f} {k}/{t}yr | {yline(m)}")
# ── 2) best partner for EVERY signal ──
print(f"\n══ 2) საუკეთესო პარტნიორი თითო სიგნალზე ══")
for s in keys:
    best=None
    for o in keys:
        if o==s: continue
        m=M[s]&M[o]&have; n=int(m.sum())
        if n<300: continue
        u=100*(r20[m]>0).mean()
        if best is None or u>best[0]: best=(u,o,n,m)
    if best:
        u,o,n,m=best
        k,t=yrs_above(m)
        print(f"  {nm(s):14} solo {solo[s]:4.1f}% → +{nm(o):14} up {u:4.1f}% (Δ{u-solo[s]:+4.1f}) n={n:6,} {k}/{t}yr")
# ── 3) ordered window pairs: A(≤5 წინ) → B(დღეს) ──
print(f"\n══ 3) მიმდევრობითი წყვილები A(≤5 ბარი წინ)→B(დღეს), syn≥3, წლები≥4 ══",flush=True)
res2=[]
for a1 in keys:
    for b1 in keys:
        if a1==b1: continue
        m=W[a1]&M[b1]&have; n=int(m.sum())
        if n<400: continue
        u=100*(r20[m]>0).mean(); syn=u-max(solo[a1],solo[b1])
        if syn<3: continue
        k,t=yrs_above(m)
        if k<4: continue
        res2.append((syn,u,a1,b1,n,k,t,m))
res2.sort(key=lambda x:-x[0])
for syn,u,a1,b1,n,k,t,m in res2[:30]:
    print(f"  {nm(a1):12}→{nm(b1):14} n={n:6,} up {u:4.1f}% syn {syn:+4.1f} {k}/{t}yr | {yline(m)}")
print(f"  (სულ კვალიფიციური: {len(res2)})")
# ── 4) triples seeded from top-10 same-bar pairs ──
print(f"\n══ 4) სამეულები (ტოპ-წყვილი + მესამე, syn vs წყვილი ≥+2, n≥200) ══",flush=True)
seen=set()
for syn,u,a1,b1,n,k,t,m in res[:10]:
    for c1 in keys:
        if c1 in (a1,b1): continue
        mm=m&M[c1]; nn=int(mm.sum())
        if nn<200: continue
        uu=100*(r20[mm]>0).mean()
        if uu-u<2: continue
        kk,tt=yrs_above(mm)
        if kk<3: continue
        key=tuple(sorted((a1,b1,c1)))
        if key in seen: continue
        seen.add(key)
        print(f"  {nm(a1)}+{nm(b1)}+{nm(c1):12} n={nn:5,} up {uu:4.1f}% (წყვილზე {uu-u:+.1f}) {kk}/{tt}yr | {yline(mm)}")
