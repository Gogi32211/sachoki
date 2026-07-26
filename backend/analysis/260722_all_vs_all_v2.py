"""ALL-vs-ALL v2 (2026-07-22): rerun with the 19 newly-persisted engine signals —
um_2809, ev_l22/l43/l64/l34, bo_dn/bx_dn/be_dn, buy_here, atr_brk, bb_brk, rtv,
svs_raw, cons_atr, gog1-3 + setup tokens (su=A/SM/N/MX) + context tokens
(cx=LD/WRC/SVS/LRC/LDS/SQB/LDC/F8C/BCT/LDP/LRP).
ONE pass writes BOTH outputs:
  backend/data/allpairs_qualified.csv  (dual-gate validated core — CONF colored bands)
  backend/data/allpairs_all.csv        (no-gate full dump — CONF gray ext tier)
Same standards as 260721: fwd-20 up% + per-bar path-sim (trail25/-15/60/15bps),
per-year consistency, dv>=3M, close>=5, no index universe, lookahead cols excluded."""
import numpy as np, pandas as pd, duckdb, time, itertools
t0=time.time(); S_=0.0015
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
cols=[r[1] for r in a.execute("PRAGMA table_info(bars)").fetchall()]
SIGC=[c for c in cols if c.startswith('sig_')]
EXTRA=[c for c in ('bo_up','bx_up','be_up','vbo_up','fbo_bull','fbo_bear','load','bf_buy','bf_sell',
 'rocket','hilo_buy','best_long','tz_bull','w2_sc','w2_ar','w2_st','w2_spring','w2_sos','w2_jac','w2_lps',
 'w2_evr','w2_accum','w2_break','wt_sos','wt_spring','wt_lps','wt_evr','ad_fresh','ad_cluster',
 'prebreak_prime','prebreak_ready','prebreak_watch','pb_lvbo','pb_wvf_confirm','pb_pp_rtv','pb_fly_cd_c',
 'pb_follow_confirm','wyc_spring','wyc_sos','wyc_in_tr','wyc_sow') if c in cols]
# 2026-07-22: the newly persisted engine-only signals
NEWBIN=[c for c in ('um_2809','ev_l22','ev_l43','ev_l64','ev_l34','bo_dn','bx_dn','be_dn','buy_here',
 'atr_brk','bb_brk','rtv','svs_raw','cons_atr','gog1','gog2','gog3') if c in cols]
ALLBIN=list(dict.fromkeys(SIGC+EXTRA+NEWBIN))
BINSEL=", ".join(f'coalesce(CAST("{c}" AS TINYINT),0) AS "{c}"' for c in ALLBIN)
D=a.execute(f"""WITH r AS (SELECT ticker,date,open,high,low,close,volume,rsi_14,cci_20,
  coalesce(t_sig,'') tt, coalesce(z_sig,'') zz, coalesce(l_sig,'') ll,
  coalesce(bar_gap_class,'') gap, coalesce(vol_bucket,'') vb, coalesce(wyc_phase,'') wp,
  coalesce(setup_tokens,'') sut, coalesce(context_tokens,'') cxt,
  lead(close,20) OVER (PARTITION BY ticker ORDER BY date) f20, {BINSEL},
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
print(f"frame {len(D):,} cols {len(D.columns)} ({time.time()-t0:.0f}s)",flush=True)
o=D.open.to_numpy(float);h=D.high.to_numpy(float);lo_=D.low.to_numpy(float);c=D.close.to_numpy(float)
tk=D.ticker.to_numpy(); n=len(D); ps=np.full(n,np.nan)
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
            if lo_[q]<=hd: r=-0.15-S_; break
            pk=max(pk,h[q]); ts=pk*0.75
            if q>b+1 and o[q]<=ts: r=o[q]/e-1-S_; break
            if lo_[q]<=ts: r=ts/e-1-S_; break
        ps[b]=r if r is not None else c[end-1]/e-1-S_
    i=j+1
print(f"path-sim done ({time.time()-t0:.0f}s)",flush=True)
r20=(D.f20/D.close-1).to_numpy(); yr=D.date.astype(str).str[:4].to_numpy()
dv=(D.close*D.volume).to_numpy()
hav=(dv>=3e6)&~np.isnan(r20)&~np.isnan(ps)
YRS=['2021','2022','2023','2024','2025','2026']
ym={y:(yr==y)&hav for y in YRS}
by={y:100*(r20[ym[y]]>0).mean() for y in YRS}
F={}
for cname in ALLBIN:
    v=(D[cname].to_numpy()==1)
    if v.sum()>=1000: F[cname[4:] if cname.startswith('sig_') else cname]=v
# token one-hots (padded-space contains — LD is a prefix of LDS/LDC/LDP, word-boundary needed)
sut=(' '+D.sut+' ').to_numpy(); cxt=(' '+D.cxt+' ').to_numpy()
for t_ in ('A','SM','N','MX'):
    v=np.char.find(sut.astype(str),f' {t_} ')>=0
    if v.sum()>=1000: F['su='+t_]=v
for t_ in ('LD','WRC','SVS','LRC','LDS','SQB','LDC','F8C','BCT','LDP','LRP'):
    v=np.char.find(cxt.astype(str),f' {t_} ')>=0
    if v.sum()>=1000: F['cx='+t_]=v
tta=D.tt.to_numpy(); zza=D.zz.to_numpy(); lla=D.ll.to_numpy()
for t_ in ('T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12'):
    F['T='+t_]=(tta==t_)
for z_ in ('Z1','Z1G','Z2','Z2G','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11'):
    F['Z='+z_]=(zza==z_)
for l_ in ('L12','L46','L25','L3','L5','L34'):
    F['L='+l_]=(lla==l_)
F['L34red']=(lla=='L34')&(c<o)
for gp in ('G1','G2','G3'):
    F['gap='+gp]=(D.gap.to_numpy()==gp)
for vb_ in ('B','VB','W','N','L'):
    F['vol='+vb_]=(D.vb.to_numpy()==vb_)
rs=D.rsi_14.to_numpy(float); cc=D.cci_20.to_numpy(float)
F['RSI<30']=rs<30; F['RSI30-40']=(rs>=30)&(rs<40); F['RSI40-50']=(rs>=40)&(rs<50)
F['RSI50-60']=(rs>=50)&(rs<60); F['RSI60+']=rs>=60
F['CCI<-100']=cc<-100; F['CCI-100..0']=(cc>=-100)&(cc<0); F['CCI0..100']=(cc>=0)&(cc<100); F['CCI>100']=cc>=100
e20=pd.Series(c).groupby(tk).transform(lambda s: s.ewm(span=20,adjust=False).mean()).to_numpy()
e50=pd.Series(c).groupby(tk).transform(lambda s: s.ewm(span=50,adjust=False).mean()).to_numpy()
e200=pd.Series(c).groupby(tk).transform(lambda s: s.ewm(span=200,adjust=False).mean()).to_numpy()
F['c>e200']=c>e200; F['c<e200']=c<=e200; F['stack e20>e50>e200']=(e20>e50)&(e50>e200)
F['dip c<e20 up']=(c<e20)&(e50>e200)
F['px5-21']=(c>=5)&(c<21); F['px21-89']=(c>=21)&(c<89); F['px89+']=c>=89
for w_ in ('MARKUP','MKDN','ACC_TR','DIST_TR'):
    F['wyc='+w_]=(D.wp.to_numpy()==w_)
F={k:v for k,v in F.items() if (v&hav).sum()>=500}
keys=list(F)
NEWKEYS={k for k in keys if k in NEWBIN or k.startswith(('su=','cx='))}
print(f"features: {len(keys)} (new: {len(NEWKEYS)}) → pairs {len(keys)*(len(keys)-1)//2:,} ({time.time()-t0:.0f}s)",flush=True)
def stats(m):
    mm=m&hav; nn=int(mm.sum())
    if nn<300: return None
    u=100*(r20[mm]>0).mean(); pm=100*ps[mm].mean()
    k=0;t=0
    for y in YRS:
        my=mm&ym[y]
        if my.sum()<15: continue
        t+=1
        if 100*(r20[my]>0).mean()>by[y]: k+=1
    tr=mm&np.isin(yr,('2021','2022','2023')); te=mm&np.isin(yr,('2024','2025','2026'))
    ptr=100*ps[tr].mean() if tr.sum()>=30 else np.nan
    pte=100*ps[te].mean() if te.sum()>=30 else np.nan
    return nn,u,pm,k,t,ptr,pte
solo={}
for k in keys:
    st=stats(F[k])
    if st: solo[k]=st[1]
keys=[k for k in keys if k in solo]
qual=[]; alld=[]
cnt=0
for a1,b1 in itertools.combinations(keys,2):
    cnt+=1
    if cnt%5000==0: print(f"  ..{cnt:,} pairs ({time.time()-t0:.0f}s)",flush=True)
    m=F[a1]&F[b1]
    if (m&hav).sum()<300: continue
    st=stats(m)
    if st is None: continue
    nn,u,pm,kk,tt,ptr,pte=st
    base=max(solo[a1],solo[b1]); lo_base=min(solo[a1],solo[b1])
    row_all=(("BULL" if pm>0 else "BEAR"),round(u-(base if pm>0 else lo_base),1),a1,b1,nn,round(u,1),round(pm,2),kk,tt,
             round(ptr,2) if not np.isnan(ptr) else None,round(pte,2) if not np.isnan(pte) else None)
    alld.append(row_all)
    bull=(u-base>=3) and pm>0.8 and (np.isnan(ptr) or ptr>-0.3) and pte>0 and kk>=4
    bear=(u-lo_base<=-3) and pm<-0.5 and (tt-kk)>=4
    if bull or bear:
        qual.append((("BULL" if bull else "BEAR"),round(u-base if bull else u-lo_base,1),a1,b1,nn,round(u,1),round(pm,2),kk,tt,
                     round(ptr,2) if not np.isnan(ptr) else None,round(pte,2) if not np.isnan(pte) else None))
print(f"pairs scanned {cnt:,} → qualified {len(qual)} · all-dump {len(alld)} ({time.time()-t0:.0f}s)",flush=True)
COLS=["dir","lift","a","b","n","up","ps","yrs_ok","yrs_tot","psTR","psTE"]
Q=pd.DataFrame(qual,columns=COLS)
Q.to_csv('/Users/sachoki/Desktop/sachoki-desktop/backend/data/allpairs_qualified.csv',index=False)
A=pd.DataFrame(alld,columns=COLS)
A.to_csv('/Users/sachoki/Desktop/sachoki-desktop/backend/data/allpairs_all.csv',index=False)
QB=Q[Q.dir=="BULL"].sort_values("ps",ascending=False)
QR=Q[Q.dir=="BEAR"].sort_values("ps")
def isnew(r): return (r.a in NEWKEYS) or (r.b in NEWKEYS)
print(f"\n══ BULL qualified: {len(QB)} — NEW-feature cells: {sum(1 for _,r in QB.iterrows() if isnew(r))} ══")
for _,r in QB.head(40).iterrows():
    tag='🆕' if isnew(r) else '  '
    print(f"{tag}{r.a:22}+{r.b:22} n={r.n:6,} up {r.up}% (lift +{r.lift}) ps {r.ps:+.2f}% {r.yrs_ok}/{r.yrs_tot}yr TR{r.psTR} TE{r.psTE}")
print(f"\n══ NEW-feature BULL cells (all, by ps) ══")
NB=QB[[isnew(r) for _,r in QB.iterrows()]]
for _,r in NB.head(40).iterrows():
    print(f"  {r.a:22}+{r.b:22} n={r.n:6,} up {r.up}% (lift +{r.lift}) ps {r.ps:+.2f}% {r.yrs_ok}/{r.yrs_tot}yr TR{r.psTR} TE{r.psTE}")
print(f"\n══ BEAR qualified: {len(QR)} — NEW-feature cells: {sum(1 for _,r in QR.iterrows() if isnew(r))} (top 25) ══")
for _,r in QR.head(25).iterrows():
    tag='🆕' if isnew(r) else '  '
    print(f"{tag}{r.a:22}+{r.b:22} n={r.n:6,} up {r.up}% ({r.lift}) ps {r.ps:+.2f}% TR{r.psTR} TE{r.psTE}")
print(f"\n══ NEW-feature BEAR cells (top 25 by ps) ══")
NR=QR[[isnew(r) for _,r in QR.iterrows()]]
for _,r in NR.head(25).iterrows():
    print(f"  {r.a:22}+{r.b:22} n={r.n:6,} up {r.up}% ({r.lift}) ps {r.ps:+.2f}% TR{r.psTR} TE{r.psTE}")
