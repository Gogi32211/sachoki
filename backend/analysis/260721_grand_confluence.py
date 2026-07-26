"""GRAND unified confluence+sequence analysis (2026-07-21, user: 'tavidan wesieri analizi').
Curated 20-element alphabet across ALL families · same-bar pairs/triples · STRICTLY
CONSECUTIVE ordered chains A(t-1)→B(t) and A(t-2)→B(t-1)→C(t) · both directions.
DUAL METRIC on every cell: fwd-20 up% AND precomputed per-bar path-sim (trail25/-15/60).
Gates: n, per-year consistency (own-year baseline), both metrics must agree."""
import numpy as np, pandas as pd, duckdb, time
t0=time.time(); S_=0.0015
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,volume,
  coalesce(z_sig,'') z, coalesce(t_sig,'') t, coalesce(l_sig,'') l,
  coalesce(bar_gap_class,'') gap,
  coalesce(CAST("load" AS TINYINT),0) ld, coalesce(CAST(sig_vol_10x AS TINYINT),0) v10,
  coalesce(CAST(sig_vol_20x AS TINYINT),0) v20, coalesce(CAST(sig_ns_vabs AS TINYINT),0) ns,
  coalesce(CAST(sig_nd_vabs AS TINYINT),0) nd, coalesce(CAST(sig_rl AS TINYINT),0) rl,
  coalesce(CAST(pb_pp_rtv AS TINYINT),0) ppr, coalesce(CAST(sig_cci AS TINYINT),0) cci,
  coalesce(CAST(sig_conso AS TINYINT),0) co, coalesce(CAST(sig_bias_up AS TINYINT),0) bu,
  coalesce(CAST(sig_fly_cd AS TINYINT),0)+coalesce(CAST(sig_fly_abcd AS TINYINT),0)
   +coalesce(CAST(sig_fly_bd AS TINYINT),0)+coalesce(CAST(sig_fly_ad AS TINYINT),0) fly,
  coalesce(CAST(sig_para_start AS TINYINT),0) para, coalesce(CAST(bf_buy AS TINYINT),0) bf,
  coalesce(CAST(w2_sc AS TINYINT),0) sc, coalesce(CAST(ad_fresh AS TINYINT),0) adf,
  coalesce(CAST(hilo_buy AS TINYINT),0) hilo,
  lead(close,20) OVER (PARTITION BY ticker ORDER BY date) f20,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
print(f"frame {len(D):,} ({time.time()-t0:.0f}s)",flush=True)
# per-bar path-sim (entry next open, trail25/-15/60) — vectorized-ish per ticker
o=D.open.to_numpy(float);h=D.high.to_numpy(float);lo=D.low.to_numpy(float);c=D.close.to_numpy(float)
tk=D.ticker.to_numpy(); n=len(D)
ps=np.full(n,np.nan)
i=0
while i<n:
    j=i
    while j+1<n and tk[j+1]==tk[i]: j+=1
    for b in range(i,j):
        e=o[b+1]*(1+S_)
        if e<=0: continue
        pk=e; hd=e*0.85; end=min(b+61,j+1); r=None
        for q in range(b+1,end):
            if q>b+1 and o[q]<=hd: r=o[q]/e-1-S_; break
            if lo[q]<=hd: r=-0.15-S_; break
            pk=max(pk,h[q]); ts=pk*0.75
            if q>b+1 and o[q]<=ts: r=o[q]/e-1-S_; break
            if lo[q]<=ts: r=ts/e-1-S_; break
        ps[b]=r if r is not None else c[end-1]/e-1-S_
    i=j+1
print(f"path-sim all bars done ({time.time()-t0:.0f}s)",flush=True)
D['ps']=ps
D['dv']=D.close*D.volume
D['r20']=D.f20/D.close-1
D['yr']=D.date.astype(str).str[:4]
# alphabet
ZCAP=('Z1G','Z2G','Z9','Z11'); TRES=('T3','T5','T11','T12'); TSTR=('T4','T6')
A={}
A['ZCAP']=D.z.isin(ZCAP).to_numpy(); A['Z7']=(D.z=='Z7').to_numpy()
A['TRES']=D.t.isin(TRES).to_numpy(); A['TSTR']=D.t.isin(TSTR).to_numpy()
A['L34R']=((D.l=='L34')&(D.close<D.open)).to_numpy(); A['L46']=(D.l=='L46').to_numpy()
A['G3']=(D.gap=='G3').to_numpy()
for k,col in (('LOAD','ld'),('V10','v10'),('V20','v20'),('NS','ns'),('ND','nd'),('RL','rl'),
              ('PPR','ppr'),('CCI','cci'),('CONSO','co'),('BIAS','bu'),('PARA','para'),
              ('4BF','bf'),('SC','sc'),('ADF','adf'),('HILO','hilo')):
    A[k]=(D[col].to_numpy()==1)
A['FLY']=(D.fly.to_numpy()>0)
liq=(D.dv.to_numpy()>=3e6)
hav=D.r20.notna().to_numpy()&~np.isnan(ps)&liq
r20=D.r20.to_numpy(); yr=D.yr.to_numpy()
YRS=['2021','2022','2023','2024','2025','2026']
ym={y:(yr==y)&hav for y in YRS}
by={y:100*(r20[ym[y]]>0).mean() for y in YRS}
u0=100*(r20[hav]>0).mean(); p0=100*ps[hav].mean()
print(f"liquid bars {hav.sum():,} · base up {u0:.1f}% · base ps {p0:+.2f}%",flush=True)
# prev-bar shifted masks (per-ticker safe: shift breaks at ticker boundary)
tkprev=np.concatenate([['#'],tk[:-1]]); same1=(tk==tkprev)
tkprev2=np.concatenate([['#','#'],tk[:-2]]); same2=(tk==tkprev2)
P1={k:np.concatenate([[False],v[:-1]])&same1 for k,v in A.items()}
P2={k:np.concatenate([[False,False],v[:-2]])&same2 for k,v in A.items()}
def stats(m):
    mm=m&hav; nn=int(mm.sum())
    if nn==0: return None
    u=100*(r20[mm]>0).mean(); pm=100*ps[mm].mean(); pmed=100*np.median(ps[mm])
    k=0;t=0
    for y in YRS:
        my=mm&ym[y]
        if my.sum()<15: continue
        t+=1
        if 100*(r20[my]>0).mean()>by[y]: k+=1
    tr=mm&(np.isin(yr,('2021','2022','2023'))); te=mm&(np.isin(yr,('2024','2025','2026')))
    ptr=100*ps[tr].mean() if tr.sum()>=30 else float('nan')
    pte=100*ps[te].mean() if te.sum()>=30 else float('nan')
    return nn,u,pm,pmed,k,t,ptr,pte
keys=list(A)
solo={}
print("\n══ ანბანის სოლო (up% · ps_mean) ══")
for k in keys:
    st=stats(A[k])
    if st is None: continue
    nn,u,pm,pmed,ky,ty,ptr,pte=st
    solo[k]=(u,pm)
    print(f"  {k:5} n={nn:8,} up {u:4.1f}% ps {pm:+5.2f}% med {pmed:+5.2f}% {ky}/{ty}yr | psTR {ptr:+.2f} psTE {pte:+.2f}")
def qualify(st,base_u,min_n=300):
    if st is None: return False
    nn,u,pm,pmed,k,t,ptr,pte=st
    return nn>=min_n and k>=4 and (u-base_u)>=3 and pm>0.8 and (ptr>0 or np.isnan(ptr)) and pte>0
def anti(st,base_u,min_n=300):
    if st is None: return False
    nn,u,pm,pmed,k,t,ptr,pte=st
    return nn>=min_n and (t-k)>=4 and (u-base_u)<=-3 and pm<-0.5
import itertools
print("\n══ SAME-BAR წყვილები — ორმაგი გეიტი (up-lift≥3 & ps>0.8 & TE>0 & წლები≥4) ══",flush=True)
bull=[]
for a1,b1 in itertools.combinations(keys,2):
    m=A[a1]&A[b1]; st=stats(m)
    bu_=max(solo[a1][0],solo[b1][0])
    if qualify(st,bu_):
        bull.append((st[2],a1,b1,st)); 
bull.sort(reverse=True)
for pm,a1,b1,st in bull[:16]:
    nn,u,pmm,pmed,k,t,ptr,pte=st
    print(f"  ✅ {a1}+{b1:6} n={nn:6,} up {u:.1f}% ps {pmm:+.2f}% med {pmed:+.2f} {k}/{t}yr TR{ptr:+.1f}/TE{pte:+.1f}")
print(f"  (გადარჩა {len(bull)})")
print("\n══ SAME-BAR სამეულები (ტოპ-წყვილებზე, ps>1.2) ══",flush=True)
tri=[]
for pm,a1,b1,_ in bull[:8]:
    for c1 in keys:
        if c1 in (a1,b1): continue
        st=stats(A[a1]&A[b1]&A[c1])
        if st and st[0]>=200 and st[2]>1.2 and st[4]>=3 and st[7]>0:
            tri.append((st[2],f"{a1}+{b1}+{c1}",st))
tri.sort(reverse=True)
seen=set()
for pm,nm_,st in tri[:12]:
    if nm_ in seen: continue
    seen.add(nm_)
    nn,u,pmm,pmed,k,t,ptr,pte=st
    print(f"  ✅ {nm_:18} n={nn:5,} up {u:.1f}% ps {pmm:+.2f}% {k}/{t}yr TR{ptr:+.1f}/TE{pte:+.1f}")
print("\n══ მიმდევრობითი ჯაჭვები A(t-1)→B(t) — bullish ══",flush=True)
ch=[]
for a1 in keys:
    for b1 in keys:
        m=P1[a1]&A[b1]; st=stats(m)
        bu_=max(solo[a1][0],solo[b1][0])
        if qualify(st,bu_):
            ch.append((st[2],a1,b1,st))
ch.sort(reverse=True)
for pm,a1,b1,st in ch[:16]:
    nn,u,pmm,pmed,k,t,ptr,pte=st
    print(f"  ✅ {a1}→{b1:6} n={nn:6,} up {u:.1f}% ps {pmm:+.2f}% med {pmed:+.2f} {k}/{t}yr TR{ptr:+.1f}/TE{pte:+.1f}")
print(f"  (გადარჩა {len(ch)})")
print("\n══ 3-ბარიანი ჯაჭვები A(t-2)→B(t-1)→C(t) (ტოპ 2-ჯაჭვებზე) ══",flush=True)
ch3=[]
for pm,a1,b1,_ in ch[:10]:
    for c1 in keys:
        m=P2[a1]&P1[b1]&A[c1]; st=stats(m)
        if st and st[0]>=150 and st[2]>1.2 and st[4]>=3 and st[7]>0:
            ch3.append((st[2],f"{a1}→{b1}→{c1}",st))
    for z1 in keys:
        m=P2[z1]&P1[a1]&A[b1]; st=stats(m)
        if st and st[0]>=150 and st[2]>1.2 and st[4]>=3 and st[7]>0:
            ch3.append((st[2],f"{z1}→{a1}→{b1}",st))
ch3.sort(reverse=True)
seen=set()
cnt=0
for pm,nm_,st in ch3:
    if nm_ in seen: continue
    seen.add(nm_); cnt+=1
    if cnt>12: break
    nn,u,pmm,pmed,k,t,ptr,pte=st
    print(f"  ✅ {nm_:20} n={nn:5,} up {u:.1f}% ps {pmm:+.2f}% {k}/{t}yr TR{ptr:+.1f}/TE{pte:+.1f}")
print("\n══ BEARISH/SUPPRESSOR მხარე (same-bar + ჯაჭვები) ══",flush=True)
bear=[]
for a1,b1 in itertools.combinations(keys,2):
    st=stats(A[a1]&A[b1])
    bu_=min(solo[a1][0],solo[b1][0])
    if anti(st,bu_): bear.append((st[2],f"{a1}+{b1}",st))
for a1 in keys:
    for b1 in keys:
        st=stats(P1[a1]&A[b1])
        bu_=min(solo[a1][0],solo[b1][0])
        if anti(st,bu_): bear.append((st[2],f"{a1}→{b1}",st))
bear.sort()
for pm,nm_,st in bear[:14]:
    nn,u,pmm,pmed,k,t,ptr,pte=st
    print(f"  ⛔ {nm_:14} n={nn:6,} up {u:.1f}% ps {pmm:+.2f}% med {pmed:+.2f} {t-k}/{t}yr− TR{ptr:+.1f}/TE{pte:+.1f}")
print(f"\ndone {time.time()-t0:.0f}s")
