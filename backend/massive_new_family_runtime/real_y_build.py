"""REAL Y_ATR PRODUCTION — frozen semantics.
INFERENTIAL_PRESPEC 8f112d7e760953b5 + A1 5e6a3788f06c4dc4 + A2 b082eae1574f0223
Y_ATR = max( max_t(HIGH_t - ENTRY), max_t(ENTRY - LOW_t) ) / ATR_PRIOR
  ENTRY = OPEN of bar r+1 ; t = entry .. that session's OWN final regular bar, inclusive
  ATR_PRIOR = Wilder ATR(14) at the PREVIOUS session's OWN final regular bar, finite and > 0
*** FIRST REAL OUTCOME READ -> Y_EXPOSED = 1 ***"""
import os,sys,json,time
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
import duckdb, numpy as np, pandas as pd
RIGHT={0:1,1:2,2:3}      # position pair -> right-hand bar index
def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}",flush=True)
con=duckdb.connect(S+'/POS4.duckdb',read_only=True)
sess=con.execute("select distinct k,session_date from pos4 order by k,session_date").df()
SKEY={(r.k,r.session_date):i for i,r in enumerate(sess.itertuples())}
n=len(sess); log(f"episodes {n:,}")
c2=duckdb.connect(S+'/stageA_input.duckdb',read_only=True); c2.execute("pragma threads=8")
fin=c2.execute("select session_date, max(interval_index) mx from bars group by 1").df()
FINAL={r.session_date:int(r.mx) for r in fin.itertuples()}
keys=[r[0] for r in c2.execute("select distinct k from bars order by k").fetchall()]
Y=np.full((3,n),np.nan); ST=np.zeros((3,n),np.int8)   # 0 VALID,1 PATH_UNAVAIL,2 ATR_UNAVAIL,3 NO_ENTRY
EXPOSED=False
t0=time.time()
for si,k in enumerate(keys):
    d=c2.execute("""select session_date,interval_index,coverage_state,open,high,low,close
                    from bars where k=? order by session_date,interval_index""",[k]).df()
    if not len(d): continue
    if not EXPOSED:
        EXPOSED=True
        json.dump({'Y_EXPOSED':1,'at':time.strftime('%Y-%m-%dT%H:%M:%S%z'),
                   'event':'first successful real post-entry outcome read'},open(S+'/Y_EXPOSED.json','w'),indent=1)
        log("*** Y_EXPOSED 0 -> 1 (irreversible) ***")
    ok=(d.coverage_state=='COMPLETE').to_numpy()
    h=d.high.to_numpy(); l=d.low.to_numpy(); c=d.close.to_numpy(); o=d.open.to_numpy()
    # Wilder ATR(14) per contiguous COMPLETE segment; age>=14 required
    seg=np.cumsum(~ok); atr=np.full(len(d),np.nan); age=np.zeros(len(d),int); a=0
    for i in range(len(d)): a=a+1 if ok[i] else 0; age[i]=a
    for _,idx in pd.Series(np.arange(len(d))).groupby(seg):
        ii=idx.to_numpy(); ii=ii[ok[ii]]
        if len(ii)<2: continue
        hh=pd.Series(h[ii]); ll=pd.Series(l[ii]); cc=pd.Series(c[ii]); pc=cc.shift(1)
        tr=pd.concat([(hh-ll).abs(),(hh-pc).abs(),(ll-pc).abs()],axis=1).max(axis=1)
        atr[ii]=tr.ewm(alpha=1/14,adjust=False).mean().to_numpy()
    atr[age<14]=np.nan
    d=d.assign(_row=np.arange(len(d)))
    bysess={s:g for s,g in d.groupby('session_date')}
    sdates=sorted(bysess)
    prev_atr={}
    for i,s in enumerate(sdates):
        if i==0: prev_atr[s]=np.nan; continue
        pg=bysess[sdates[i-1]]; pf=FINAL.get(sdates[i-1])
        row=pg[pg.interval_index==pf]
        prev_atr[s]= float(atr[int(row._row.iloc[0])]) if len(row) else np.nan
    for s in sdates:
        ep=SKEY.get((k,s))
        if ep is None: continue
        g=bysess[s]; fidx=FINAL.get(s)
        gi={int(r.interval_index):r for r in g.itertuples()}
        ap=prev_atr[s]
        for p in range(3):
            e=RIGHT[p]+1
            if e>fidx: ST[p,ep]=3; continue
            if not (np.isfinite(ap) and ap>0): ST[p,ep]=2; continue
            need=list(range(e,fidx+1))
            miss=[t for t in need if t not in gi or gi[t].coverage_state!='COMPLETE']
            if miss: ST[p,ep]=1; continue
            entry=gi[e].open
            hi=max(gi[t].high for t in need); lo=min(gi[t].low for t in need)
            Y[p,ep]=max(hi-entry, entry-lo)/ap
    if (si+1)%50==0: log(f"  {si+1}/{len(keys)} el={time.time()-t0:.0f}s")
AV=(ST==0)&np.isfinite(Y)
np.save(S+'/mm_Y.npy',np.nan_to_num(Y,nan=0.0)); np.save(S+'/mm_AV.npy',AV)
np.save(S+'/real_Y_status.npy',ST)
cnt={}
for p in range(3):
    cnt[f'M{p+1}->M{p+2}']={'VALID':int((ST[p]==0).sum()),'PATH_UNAVAILABLE':int((ST[p]==1).sum()),
        'ATR_PRIOR_UNAVAILABLE':int((ST[p]==2).sum()),'NO_ENTRY_BAR':int((ST[p]==3).sum())}
json.dump({'episodes':n,'per_position':cnt,'elapsed_s':round(time.time()-t0)},open(S+'/REAL_Y_BUILD.json','w'),indent=1)
log(f"DONE real Y build {time.time()-t0:.0f}s")
print(json.dumps(cnt,indent=1))
