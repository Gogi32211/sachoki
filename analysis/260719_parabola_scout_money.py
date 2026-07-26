"""MONEY TEST for the parabola-scout stack: path-sim (trail25/-15%/60bar) at every ladder
tier, per-year, TRAIN/TEST, random-same-size control. Also a parabola-friendly exit variant
(trail 30%, -20% initial) since the target is the fat tail."""
import numpy as np, pandas as pd, duckdb
SP='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/5b6f6b5f-eb52-4041-9fed-b0cbcf6a28fc/scratchpad'
E=pd.read_parquet(f'{SP}/reignite_events.parquet')
S_=0.0015
def sim(o,hi,lo,cl,start,entry,trail,hard,maxh=60):
    n=len(cl); e=entry*(1+S_)
    if e<=0 or start>=n: return None
    pk=e; hd=e*(1-hard); end=min(start+maxh,n)
    for j in range(start,end):
        if o[j]<=hd and j>start: return o[j]/e-1-S_
        if lo[j]<=hd: return -hard-S_
        pk=max(pk,hi[j]); ts=pk*(1-trail)
        if o[j]<=ts and j>start: return o[j]/e-1-S_
        if lo[j]<=ts: return ts/e-1-S_
    return cl[end-1]/e-1-S_
a=duckdb.connect('../data/studio_analytics.duckdb',read_only=True)
D=a.execute("""WITH r AS (SELECT ticker,date,open,high,low,close,close*volume dv,
  row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
  FROM bars WHERE close>=5) SELECT * EXCLUDE rn FROM r WHERE rn=1 ORDER BY ticker,date""").fetchdf()
a.close()
D["date"]=D["date"].astype(str).str[:10]
idx={}
for tk,g in D.groupby("ticker",sort=False):
    g=g.reset_index(drop=True)
    idx[tk]=(g, {d_:i for i,d_ in enumerate(g.date)})
def path(ev,trail,hard):
    out=[]
    for tk,d_ in zip(ev.tk,ev.d):
        gi=idx.get(tk)
        if gi is None: out.append(None); continue
        g,dm=gi; i=dm.get(d_)
        if i is None or i+1>=len(g): out.append(None); continue
        o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
        out.append(sim(o,hi,lo,cl,i+1,o[i+1],trail,hard))
    return np.array([x for x in out if x is not None]), out
# baseline pool for random control (liquid bars, same exit)
rng=np.random.default_rng(11)
pool=[]
for tk,(g,dm) in list(idx.items()):
    o,hi,lo,cl=(g[c].to_numpy(float) for c in("open","high","low","close"))
    dvv=g.dv.to_numpy(float)
    for i in range(0,len(g)-1,17):
        if dvv[i]>=3e6:
            r=sim(o,hi,lo,cl,i+1,o[i+1],0.25,0.15)
            if r is not None: pool.append(r)
pool=np.array(pool)
print(f"baseline pool n={len(pool)} mean {pool.mean()*100:+.2f}%\n")
q=E.atr_pct.quantile(2/3)
tiers=[('ALL events',E),
 ('ATR top-1/3',E[E.atr_pct>=q]),
 ('+spike>=4%',E[(E.atr_pct>=q)&(E.spike_gain>=0.04)]),
 ('+v2>=15',E[(E.atr_pct>=q)&(E.spike_gain>=0.04)&(E.v2>=15)]),
 ('+offhigh<=-8%',E[(E.atr_pct>=q)&(E.spike_gain>=0.04)&(E.v2>=15)&(E.off_high<=-0.08)]),
 ('FULL stack',E[(E.atr_pct>=q)&(E.spike_gain>=0.04)&(E.v2>=15)&(E.off_high<=-0.08)&(E.conso_streak<=6)])]
print(f"{'tier':14}{'n':>6} | trail25/-15: {'mean':>7}{'med':>7}{'win':>5}{'PF':>6}{'σ':>6} {'yrs+':>5} | trail30/-20 mean")
for lab,ev in tiers:
    if len(ev)<40: print(f"{lab:14}{len(ev):>6}  too few"); continue
    r,raw=path(ev,0.25,0.15)
    r2,_=path(ev,0.30,0.20)
    w=(r>0).mean()*100; pfd=-r[r<=0].sum(); pf=r[r>0].sum()/pfd if pfd>0 else float('nan')
    draws=np.array([rng.choice(pool,len(r),replace=False).mean() for _ in range(300)])*100
    z=(r.mean()*100-draws.mean())/draws.std()
    ev2=ev.copy(); ev2['ret']=[x for x in raw]
    ev2=ev2[ev2.ret.notna()]
    yrs=ev2.groupby('yr').ret.mean()*100
    print(f"{lab:14}{len(r):>6} | {r.mean()*100:>+7.2f}{np.median(r)*100:>+7.2f}{w:>4.0f}%{pf:>6.2f}{z:>+6.1f} {int((yrs>0).sum())}/{len(yrs)} | {r2.mean()*100:+.2f}%")
    if lab=='FULL stack':
        print('   per-yr: '+' '.join(f'{y}:{v:+.1f}' for y,v in yrs.items()))
        tr=ev2[ev2.yr.isin(['2021','2022','2023'])]; te=ev2[ev2.yr.isin(['2024','2025','2026'])]
        print(f'   TRAIN {tr.ret.mean()*100:+.2f}% (n={len(tr)})   TEST {te.ret.mean()*100:+.2f}% (n={len(te)})')
        print(f'   tail: P(ret>=+25%) {(ev2.ret>=0.25).mean()*100:.1f}%  P(ret>=+50%) {(ev2.ret>=0.5).mean()*100:.1f}%')
