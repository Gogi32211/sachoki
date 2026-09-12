"""Gate 2 Stage A — build and cache the episode structure. X-only, no Y."""
import os, sys, json, time
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
import duckdb, numpy as np
TOK=json.load(open(S+'/TOKEN_DICTIONARY_123.json')); tok_ids=sorted(TOK)
TIDX={t:i for i,t in enumerate(tok_ids)}
man=json.load(open(S+'/CANONICAL_69_MANIFEST_V2.json')); feats=sorted(man); FIDX={f:i for i,f in enumerate(feats)}
BOOL_T={TOK[t]['source_feature']:TIDX[t] for t in tok_ids if TOK[t]['feature_type']=='BOOLEAN'}
CAT_T={(TOK[t]['source_feature'],TOK[t]['value']):TIDX[t] for t in tok_ids if TOK[t]['feature_type']=='CATEGORICAL'}
ET=['09:30','09:45','10:00','10:15']; EIDX={e:i for i,e in enumerate(ET)}
con=duckdb.connect(S+'/POS4.duckdb',read_only=True); con.execute("pragma threads=8")
sess=con.execute("select distinct k,session_date from pos4 order by k,session_date").df()
n=len(sess); print(f"episodes (security,session): {n:,}",flush=True)
SKEY={(r.k,r.session_date):i for i,r in enumerate(sess.itertuples())}
secs=sorted(sess.k.unique()); SIDX={s:i for i,s in enumerate(secs)}
dates=sorted(sess.session_date.unique()); DIDX={d:i for i,d in enumerate(dates)}
sec_of=np.array([SIDX[r.k] for r in sess.itertuples()],np.int32)
date_of=np.array([DIDX[r.session_date] for r in sess.itertuples()],np.int32)
TT=np.zeros((4,len(tok_ids),n),bool); FV=np.zeros((4,len(feats),n),bool)
t0=time.time()
for si,k in enumerate(secs):
    d=con.execute("select session_date,et,feature,bval,sval,availability from pos4 where k=?",[k]).df()
    if not len(d): continue
    col=d.session_date.map(lambda x: SKEY[(k,x)]).to_numpy()
    ei=d.et.map(EIDX).to_numpy(); fi=d.feature.map(FIDX).to_numpy()
    ok=(d.availability.to_numpy()=='AVAILABLE_VALID')
    FV[ei[ok],fi[ok],col[ok]]=True
    bv=d.bval.to_numpy(); sv=d.sval.to_numpy(); ft=d.feature.to_numpy()
    for j in np.nonzero(ok)[0]:
        f=ft[j]; ti=BOOL_T.get(f)
        if ti is not None:
            if bv[j]==1: TT[ei[j],ti,col[j]]=True
        else:
            ti=CAT_T.get((f, sv[j] if sv[j] is not None else ''))
            if ti is not None: TT[ei[j],ti,col[j]]=True
    if (si+1)%50==0: print(f"  {si+1}/{len(secs)} el={time.time()-t0:.0f}s",flush=True)
# ACTIVE WINDOW — X-side only, Y-INDEPENDENT: first/last session with any COMPLETE opening-hour bar
anyv=FV.any(axis=(0,1))
aw_lo=np.full(len(secs),10**9,np.int32); aw_hi=np.full(len(secs),-1,np.int32)
for i in np.nonzero(anyv)[0]:
    s=sec_of[i]; d=date_of[i]
    if d<aw_lo[s]: aw_lo[s]=d
    if d>aw_hi[s]: aw_hi[s]=d
# session_length_class per DATE (date-level property)
lastidx=con.execute("""select session_date, max(interval_index) mx from
  (select session_date, interval_index from range(0) ) where 1=0""").df() if False else None
c2=duckdb.connect(S+'/stageA_input.duckdb',read_only=True)
sl=c2.execute("select session_date, max(interval_index) mx from bars group by 1").df()
cls=np.zeros(len(dates),np.int8)   # 0 = FULL_REGULAR, 1 = EARLY_CLOSE
m={r.session_date:r.mx for r in sl.itertuples()}
for d,i in DIDX.items(): cls[i]= 0 if m.get(d,25)==25 else 1
np.savez_compressed(S+'/SMOKE_EPISODES.npz', TT=np.packbits(TT,axis=-1), FV=np.packbits(FV,axis=-1),
  n=n, sec_of=sec_of, date_of=date_of, aw_lo=aw_lo, aw_hi=aw_hi, date_class=cls,
  n_sec=len(secs), n_date=len(dates))
print(f"\ncached. episodes={n:,} securities={len(secs)} dates={len(dates)}")
print(f"  FULL_REGULAR dates {int((cls==0).sum())}   EARLY_CLOSE dates {int((cls==1).sum())}")
print(f"  elapsed {time.time()-t0:.0f}s")
