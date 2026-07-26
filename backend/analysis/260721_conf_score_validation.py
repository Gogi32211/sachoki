"""CONF score validation (2026-07-21): aggregate the 812 qualified all-vs-all cells
into one per-bar score; test DECILE monotonicity vs path-sim, per-year, before any UI.
Anti-double-count: each FEATURE contributes once per side (its best cell), not once
per pair. Score = sum(ps of active bull features' best cells) + sum(bear, negative)."""
import numpy as np, pandas as pd, duckdb, time
t0=time.time(); S_=0.0015
Q=pd.read_csv('/Users/sachoki/Desktop/sachoki-desktop/backend/data/allpairs_qualified.csv')
print(f"qualified cells: {len(Q)} (bull {(Q.dir=='BULL').sum()} bear {(Q.dir=='BEAR').sum()})")
a=duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',read_only=True)
cols=[r[1] for r in a.execute("PRAGMA table_info(bars)").fetchall()]
SIGC=[c for c in cols if c.startswith('sig_')]
EXTRA=[c for c in ('bo_up','bx_up','be_up','vbo_up','fbo_bull','fbo_bear','load','bf_buy','bf_sell',
 'rocket','hilo_buy','best_long','tz_bull','w2_sc','w2_ar','w2_st','w2_spring','w2_sos','w2_jac','w2_lps',
 'w2_evr','w2_accum','w2_break','wt_sos','wt_spring','wt_lps','wt_evr','ad_fresh','ad_cluster',
 'prebreak_prime','prebreak_ready','prebreak_watch','pb_lvbo','pb_wvf_confirm','pb_pp_rtv','pb_fly_cd_c',
 'pb_follow_confirm','wyc_spring','wyc_sos','wyc_in_tr','wyc_sow') if c in cols]
BINSEL=", ".join(f'coalesce(CAST("{c}" AS TINYINT),0) AS "{c}"' for c in dict.fromkeys(SIGC+EXTRA))
D=a.execute(f"""WITH r AS (SELECT ticker,date,open,high,low,close,volume,rsi_14,cci_20,
  coalesce(t_sig,'') tt, coalesce(z_sig,'') zz, coalesce(l_sig,'') ll,
  coalesce(bar_gap_class,'') gap, coalesce(vol_bucket,'') vb, coalesce(wyc_phase,'') wp,
  {BINSEL},
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5 AND universe<>'index')
 SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
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
# rebuild features exactly as all_vs_all
F={}
for cname in dict.fromkeys(SIGC+EXTRA):
    v=(D[cname].to_numpy()==1)
    if v.sum()>=1000: F[cname[4:] if cname.startswith('sig_') else cname]=v
tta=D.tt.to_numpy(); zza=D.zz.to_numpy(); lla=D.ll.to_numpy()
for t_ in ('T1','T1G','T2','T2G','T3','T4','T5','T6','T9','T10','T11','T12'): F['T='+t_]=(tta==t_)
for z_ in ('Z1','Z1G','Z2','Z2G','Z3','Z4','Z5','Z6','Z7','Z9','Z10','Z11'): F['Z='+z_]=(zza==z_)
for l_ in ('L12','L46','L25','L3','L5','L34'): F['L='+l_]=(lla==l_)
F['L34red']=(lla=='L34')&(c<o)
for gp in ('G1','G2','G3'): F['gap='+gp]=(D.gap.to_numpy()==gp)
for vb_ in ('B','VB','W','N','L'): F['vol='+vb_]=(D.vb.to_numpy()==vb_)
rs=D.rsi_14.to_numpy(float); cc=D.cci_20.to_numpy(float)
F['RSI<30']=rs<30; F['RSI30-40']=(rs>=30)&(rs<40); F['RSI40-50']=(rs>=40)&(rs<50)
F['RSI50-60']=(rs>=50)&(rs<60); F['RSI60+']=rs>=60
F['CCI<-100']=cc<-100; F['CCI-100..0']=(cc>=-100)&(cc<0); F['CCI0..100']=(cc>=0)&(cc<100); F['CCI>100']=cc>=100
e20=pd.Series(c).groupby(tk).transform(lambda s: s.ewm(span=20,adjust=False).mean()).to_numpy()
e50=pd.Series(c).groupby(tk).transform(lambda s: s.ewm(span=50,adjust=False).mean()).to_numpy()
e200=pd.Series(c).groupby(tk).transform(lambda s: s.ewm(span=200,adjust=False).mean()).to_numpy()
F['c>e200']=c>e200; F['c<e200']=c<=e200; F['stack e20>e50>e200']=(e20>e50)&(e50>e200)
F['dip c<e20 up']=(c<e20)&(e50>e200); F['c>e200 near-hi']=F['c>e200']
F['px5-21']=(c>=5)&(c<21); F['px21-89']=(c>=21)&(c<89); F['px89+']=c>=89
for w_ in ('MARKUP','MKDN','ACC_TR','DIST_TR'): F['wyc='+w_]=(D.wp.to_numpy()==w_)
# score: per bar, per side, each FEATURE counts once (best |ps| cell it belongs to)
score=np.zeros(n)
sidecells={'BULL':{}, 'BEAR':{}}
for _,r in Q.iterrows():
    if r.a not in F or r.b not in F: continue
    m=F[r.a]&F[r.b]
    w=float(r.ps)
    for feat in (r.a,r.b):
        d_=sidecells[r.dir].setdefault(feat,[])
        d_.append((abs(w),w,m))
bull_contrib=np.zeros(n); bear_contrib=np.zeros(n)
for side,dd in sidecells.items():
    for feat,lst in dd.items():
        best=np.zeros(n)
        for aw,w,m in lst:
            cand=np.where(m,w,0.0)
            best=np.where(np.abs(cand)>np.abs(best),cand,best)
        if side=='BULL': bull_contrib+=np.maximum(best,0)
        else: bear_contrib+=np.minimum(best,0)
score=bull_contrib+bear_contrib
D['conf']=score
yr=D.date.astype(str).str[:4].to_numpy()
dv=(c*D.volume.to_numpy())
hav=(dv>=3e6)&~np.isnan(ps)
S=pd.DataFrame({'yr':yr[hav],'conf':score[hav],'ps':ps[hav]*100})
nz=S[S.conf!=0]
print(f"\nbars with nonzero CONF: {len(nz):,} ({100*len(nz)/len(S):.0f}%)")
print("\n══ დეცილების კიბე (nonzero CONF) ══")
nz['dec']=pd.qcut(nz.conf,10,labels=False,duplicates='drop')
for d_,g_ in nz.groupby('dec'):
    yrs=g_.groupby('yr').ps.mean()
    print(f"  D{int(d_)}: conf[{g_.conf.min():+.1f}..{g_.conf.max():+.1f}] n={len(g_):8,} ps {g_.ps.mean():+.2f}% med {g_.ps.median():+.2f}% | {int((yrs>0).sum())}/{len(yrs)}yr+")
print("\n══ zero-CONF კონტროლი ══")
z_=S[S.conf==0]
print(f"  n={len(z_):,} ps {z_.ps.mean():+.2f}%")
print("\n══ კიდეები per-year ══")
top=nz[nz.conf>=nz.conf.quantile(0.95)]; bot=nz[nz.conf<=nz.conf.quantile(0.05)]
for nm_,g_ in (("TOP-5%",top),("BOT-5%",bot)):
    yrs=g_.groupby('yr').ps.mean()
    print(f"  {nm_}: n={len(g_):,} ps {g_.ps.mean():+.2f}% | "+" ".join(f"{y}:{v:+.1f}" for y,v in yrs.items()))
