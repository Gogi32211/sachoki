"""X-only support census over the frozen 45,387 claim universe.
Reads NO outcome data. Applies NO floor. Drops NO claim."""
import os, sys, json, time
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
import duckdb, numpy as np
RUN='SUPPORT_CENSUS'; J=f'{S}/{RUN}.json'
def atomic(o):
    t=J+'.tmp'
    with open(t,'w') as f: json.dump(o,f); f.flush(); os.fsync(f.fileno())
    os.replace(t,J)
def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}",flush=True)
TOK=json.load(open(S+'/TOKEN_DICTIONARY_123.json'))
tok_ids=sorted(TOK); TIDX={t:i for i,t in enumerate(tok_ids)}
feats=sorted({TOK[t]['source_feature'] for t in tok_ids} | set(json.load(open(S+'/CANONICAL_69_MANIFEST_V2.json'))))
FIDX={f:i for i,f in enumerate(feats)}
NF, NT = len(feats), len(tok_ids)
assert NT==123, NT
# token lookup: boolean feature -> token idx ; (categorical feature, value) -> token idx
BOOL_T={}; CAT_T={}
for t in tok_ids:
    m=TOK[t]
    if m['feature_type']=='BOOLEAN': BOOL_T[m['source_feature']]=TIDX[t]
    else: CAT_T[(m['source_feature'],m['value'])]=TIDX[t]
ET=['09:30','09:45','10:00','10:15']; EIDX={e:i for i,e in enumerate(ET)}
PP=[(0,1),(1,2),(2,3)]; PPN=['M1->M2','M2->M3','M3->M4']
con=duckdb.connect(S+'/POS4.duckdb',read_only=True); con.execute("pragma threads=8")
keys=[r[0] for r in con.execute("select distinct k from pos4 order by k").fetchall()]
NS=len(keys); log(f"securities: {NS}")
SC={'DEV':{}, 'HOLDOUT':{}}
for sc in SC:
    SC[sc]['EV']=np.zeros((3,NS,NF,NF),np.int32)
    SC[sc]['PO']=np.zeros((3,NS,NT,NT),np.int32)
T={'run':RUN,'done':0,'sec':NS}; t0=last=time.time()
for si,k in enumerate(keys):
    d=con.execute("""select session_date, et, feature, bval, sval, availability
                     from pos4 where k=? order by session_date""",[k]).df()
    if not len(d): T['done']+=1; continue
    sess=sorted(d.session_date.unique()); SI={s:i for i,s in enumerate(sess)}; n=len(sess)
    dev=np.array([s<='2026-08-06' for s in sess])
    si_arr=d.session_date.map(SI).to_numpy()
    ei=d.et.map(EIDX).to_numpy(); fi=d.feature.map(FIDX).to_numpy()
    valid=(d.availability.to_numpy()=='AVAILABLE_VALID')
    V=np.zeros((4,NF,n),np.float32)
    V[ei[valid],fi[valid],si_arr[valid]]=1.0
    Tm=np.zeros((4,NT,n),np.float32)
    bv=d.bval.to_numpy(); sv=d.sval.to_numpy(); ft=d.feature.to_numpy()
    for j in np.nonzero(valid)[0]:
        f=ft[j]
        ti=BOOL_T.get(f)
        if ti is not None:
            if bv[j]==1: Tm[ei[j],ti,si_arr[j]]=1.0
        else:
            ti=CAT_T.get((f,sv[j] if sv[j] is not None else ''))
            if ti is not None: Tm[ei[j],ti,si_arr[j]]=1.0
    for sc,msk in (('DEV',dev),('HOLDOUT',~dev)):
        if not msk.any(): continue
        for p,(L,R) in enumerate(PP):
            SC[sc]['EV'][p,si]=(V[L][:,msk] @ V[R][:,msk].T).astype(np.int32)
            SC[sc]['PO'][p,si]=(Tm[L][:,msk] @ Tm[R][:,msk].T).astype(np.int32)
    T['done']+=1; now=time.time()
    if (si+1)%25==0 or now-last>=60:
        last=now; log(f"HB {T['done']}/{NS} el={now-t0:.0f}s")
        atomic(dict(T=T,el=round(now-t0),FINISHED=False))
log("aggregating")
SRC=np.array([FIDX[TOK[t]['source_feature']] for t in tok_ids])
out={}
for sc in ('DEV','HOLDOUT'):
    EV=SC[sc]['EV']; PO=SC[sc]['PO']
    nsec_top=max(1,int(np.ceil(0.10*NS)))
    res={}
    for p,pn in enumerate(PPN):
        ev_tot=EV[p].sum(0); po_tot=PO[p].sum(0)
        ev_nsec=(EV[p]>0).sum(0); po_nsec=(PO[p]>0).sum(0)
        ev_top=np.sort(EV[p],axis=0)[::-1][:nsec_top].sum(0)
        po_top=np.sort(PO[p],axis=0)[::-1][:nsec_top].sum(0)
        res[pn]=dict(ev_tot=ev_tot,po_tot=po_tot,ev_nsec=ev_nsec,po_nsec=po_nsec,ev_top=ev_top,po_top=po_top)
    out[sc]=res
rows=[]
for p,pn in enumerate(PPN):
    for ai,a in enumerate(tok_ids):
        for bi,b in enumerate(tok_ids):
            r={'claim_id':f"{a}@{pn.split('->')[0]}->{b}@{pn.split('->')[1]}",
               'token_a':a,'token_b':b,'position_pair':pn,
               'type_pair':f"{TOK[a]['feature_type'].lower()}->{TOK[b]['feature_type'].lower()}"}
            for sc in ('DEV','HOLDOUT'):
                R=out[sc][pn]; fa,fb=SRC[ai],SRC[bi]
                ev=int(R['ev_tot'][fa,fb]); po=int(R['po_tot'][ai,bi])
                r[f'{sc}_evaluable']=ev; r[f'{sc}_positive']=po
                r[f'{sc}_positive_rate']=(po/ev if ev>0 else None)
                r[f'{sc}_sec_evaluable']=int(R['ev_nsec'][fa,fb]); r[f'{sc}_sec_positive']=int(R['po_nsec'][ai,bi])
                r[f'{sc}_top10_share_evaluable']=(float(R['ev_top'][fa,fb])/ev if ev>0 else None)
                r[f'{sc}_top10_share_positive']=(float(R['po_top'][ai,bi])/po if po>0 else None)
            rows.append(r)
import pandas as pd
df=pd.DataFrame(rows)
o=duckdb.connect(f'{S}/{RUN}.duckdb'); o.execute("drop table if exists census")
o.register('df',df); o.execute("create table census as select * from df"); o.close()
atomic(dict(T=T,rows=len(df),el=round(time.time()-t0),FINISHED=True))
log(f"DONE census rows {len(df)}")
print(f"DONE rows {len(df)}")
