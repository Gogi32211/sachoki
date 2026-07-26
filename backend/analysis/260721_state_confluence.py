"""STATE-layer confluence sweep (2026-07-21): Wyckoff phases/events (w2_/wt_), AD,
prebreak states, swing types, tz_bull — solo per-year + pairs among themselves AND
vs the master amplifiers (bias_up, vol spikes, LOAD, conso, nd/ns_vabs, rl, para).
Liquid base (dv>=3M). fwd-20 up%, per-year 2021-2026."""
import numpy as np, pandas as pd, duckdb
BOOL=['wyc_spring','wyc_sos','wyc_in_tr','wyc_sow','w2_sc','w2_ar','w2_st','w2_spring','w2_sos',
 'w2_jac','w2_lps','w2_evr','w2_accum','w2_break','wt_sos','wt_spring','wt_lps','wt_evr','wt_valid_tr',
 'ad_fresh','ad_cluster','prebreak_prime','prebreak_ready','prebreak_watch','pb_lvbo','pb_wvf_confirm',
 'pb_pp_rtv','pb_fly_cd_c','pb_follow_confirm','tz_bull','hilo_buy']
BRIDGE=['sig_bias_up','sig_vol_5x','sig_vol_10x','sig_vol_20x','load','sig_conso','sig_nd_vabs',
 'sig_ns_vabs','sig_rl','sig_para_start','sig_cci','bf_buy']
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
bsel=", ".join(f'coalesce(CAST("{s}" AS TINYINT),0) AS "{s}"' for s in BOOL+BRIDGE)
D=a.execute(f"""WITH r AS (SELECT ticker,date,close,volume,
  lead(close,20) OVER (PARTITION BY ticker ORDER BY date) f20,
  coalesce(wyc_phase,'') wp, coalesce(swing_type,'') st, coalesce(prebreak_v3,0) pv3, {bsel},
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT strftime(date,'%Y') yr, f20/close-1 r20, wp, st, pv3,
        {", ".join(chr(34)+s+chr(34) for s in BOOL+BRIDGE)}
 FROM r WHERE rn=1 AND f20 IS NOT NULL AND close*volume>=3000000""").fetchdf()
a.close()
print(f"liquid bars: {len(D):,}",flush=True)
r20=D.r20.to_numpy(); yr=D.yr.to_numpy()
YRS=['2021','2022','2023','2024','2025','2026']
ymask={y:(yr==y) for y in YRS}
base_y={y:100*(r20[ymask[y]]>0).mean() for y in YRS}
M={}
for s in BOOL+BRIDGE: M[s]=(D[s].to_numpy()==1)
for ph in ('MARKUP','MKDN','ACC_TR','DIST_TR','SPRING','SOS','UTAD'):
    M['wyc='+ph]=(D.wp.to_numpy()==ph)
for sw in ('HH','HL','LH','LL'):
    M['swing='+sw]=(D.st.to_numpy()==sw)
M['pv3>=20']=(D.pv3.to_numpy()>=20)
STATES=[k for k in M if k not in BRIDGE]
def nm(s): return s.replace('sig_','')
def yrs_above(mask,minn=15):
    k=0;t=0
    for y in YRS:
        m=mask&ymask[y]; n=int(m.sum())
        if n<minn: continue
        t+=1
        if 100*(r20[m]>0).mean()>base_y[y]: k+=1
    return k,t
def yline(mask,minn=15):
    return "/".join(f"{100*(r20[mask&ymask[y]]>0).mean():.0f}" if (mask&ymask[y]).sum()>=minn else "·" for y in YRS)
solo={}
print("\n══ STATE SOLO (per-year) ══")
rows=[]
for s in STATES:
    m=M[s]; n=int(m.sum())
    if n<300: continue
    u=100*(r20[m]>0).mean(); solo[s]=u
    md=100*np.median(r20[m]); k,t=yrs_above(m)
    rows.append((u,s,n,md,k,t,yline(m)))
rows.sort(reverse=True)
for u,s,n,md,k,t,yl in rows:
    print(f"  {nm(s):16} n={n:9,} up {u:4.1f}% med {md:+5.2f}% | {k}/{t}yr+ | {yl}")
for s in BRIDGE:
    m=M[s]
    if m.sum()>=300: solo[s]=100*(r20[m]>0).mean()
print("\n══ PAIRS: state×state და state×amplifier (syn≥2.5, წლები≥4, n≥300) ══",flush=True)
res=[]
keys=[s for s in solo]
import itertools
for a1,b1 in itertools.combinations(keys,2):
    if a1 in BRIDGE and b1 in BRIDGE: continue
    m=M[a1]&M[b1]; n=int(m.sum())
    if n<300: continue
    u=100*(r20[m]>0).mean(); syn=u-max(solo[a1],solo[b1])
    if syn<2.5: continue
    k,t=yrs_above(m)
    if k<4: continue
    res.append((syn,u,a1,b1,n,k,t,m))
res.sort(key=lambda x:-x[0])
for syn,u,a1,b1,n,k,t,m in res[:35]:
    print(f"  {nm(a1):16}+{nm(b1):16} n={n:6,} up {u:4.1f}% syn {syn:+4.1f} {k}/{t}yr | {yline(m)}")
print(f"  (სულ: {len(res)})")
print("\n══ საუკეთესო პარტნიორი თითო STATE-ზე ══")
for s in STATES:
    if s not in solo: continue
    best=None
    for o in keys:
        if o==s: continue
        m=M[s]&M[o]; n=int(m.sum())
        if n<300: continue
        u=100*(r20[m]>0).mean()
        if best is None or u>best[0]: best=(u,o,n,m)
    if best:
        u,o,n,m=best
        k,t=yrs_above(m)
        print(f"  {nm(s):16} solo {solo[s]:4.1f}% → +{nm(o):16} up {u:4.1f}% (Δ{u-solo[s]:+5.1f}) n={n:6,} {k}/{t}yr")
