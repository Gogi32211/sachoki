"""Gate 2 — FULL REGISTRY synthetic-Y smoke. REAL X, SYNTHETIC Y, no outcome source touched."""
import os,sys,json,time,hashlib
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
import numpy as np, duckdb
from smoke_perm import (substream, date_map_timelocal, guard_shape, guard_permutation, guard_class_stratum,
                        guard_fixed_max_set, guard_time_locality, guard_cyclic_structure, guard_early_close_identity)
from outcome_builder import OutcomeProvider
OutcomeProvider('SYNTHETIC',smoke=True)          # exposure guard: REAL would raise here
B_SCALE, B_NULL = 20, 30
def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}",flush=True)
z=np.load(S+'/SMOKE_EPISODES.npz')
n=int(z['n']); TT=np.unpackbits(z['TT'],axis=-1,count=n).astype(bool)
FV=np.unpackbits(z['FV'],axis=-1,count=n).astype(bool)
sec_of=z['sec_of']; date_of=z['date_of']; aw_lo=z['aw_lo']; aw_hi=z['aw_hi']
date_class=z['date_class']; n_sec=int(z['n_sec']); n_date=int(z['n_date'])
log(f"episodes {n:,}  TT {TT.shape}  FV {FV.shape}")
con=duckdb.connect(S+'/POS4.duckdb',read_only=True)
sess=con.execute("select distinct k,session_date from pos4 order by k,session_date").df()
dev=(sess.session_date<='2026-08-06').to_numpy()
log(f"DEV episodes {int(dev.sum()):,}")
_d=sorted(set(sess.session_date)); _m=sorted({x[:7] for x in _d}); _MI={m:i for i,m in enumerate(_m)}
_dl=sorted(duckdb.connect(S+'/stageA_input.duckdb',read_only=True).execute('select distinct session_date from bars').df().session_date)
month_id=np.array([_MI[x[:7]] for x in _dl],np.int32)
log(f'months {len(_m)}  time-local circular map ACTIVE')
EPI=np.full((n_sec,n_date),-1,np.int32); EPI[sec_of,date_of]=np.arange(n,dtype=np.int32)
TOK=json.load(open(S+'/TOKEN_DICTIONARY_123.json')); tok_ids=sorted(TOK); TIDX={t:i for i,t in enumerate(tok_ids)}
man=json.load(open(S+'/CANONICAL_69_MANIFEST_V2.json')); feats=sorted(man); FIDX={f:i for i,f in enumerate(feats)}
reg=duckdb.connect(S+'/SUPPORT_CENSUS.duckdb',read_only=True).execute(
  "select claim_id,token_a,token_b,position_pair from registry where support_eligible order by claim_id").df()
K=len(reg); log(f"registered claims k={K:,}")
PPN=['M1->M2','M2->M3','M3->M4']; PIDX={p:i for i,p in enumerate(PPN)}
ca=np.array([TIDX[t] for t in reg.token_a]); cb=np.array([TIDX[t] for t in reg.token_b])
cp=np.array([PIDX[p] for p in reg.position_pair])
fa=np.array([FIDX[TOK[t]['source_feature']] for t in reg.token_a])
fb=np.array([FIDX[TOK[t]['source_feature']] for t in reg.token_b])
# --- precompute POSITIVE index lists (X is FIXED across draws) ---
log("precomputing positive sets")
t0=time.time(); POS=[]; 
for j in range(K):
    p=cp[j]
    POS.append(np.flatnonzero(TT[p,ca[j]] & TT[p+1,cb[j]] & dev).astype(np.int32))
log(f"  positives total {sum(len(x) for x in POS):,}  ({time.time()-t0:.0f}s)")
pool_key={}
for j in range(K): pool_key.setdefault((fa[j],fb[j],cp[j]),[]).append(j)
log(f"  distinct pools {len(pool_key):,}")
# --- synthetic Y (SHA256 keyed uniforms) ---
log("building synthetic Y")
def _u(tag,*k):
    return int.from_bytes(hashlib.sha256(('|'.join([tag]+[str(x) for x in k])).encode()).digest()[:8],'big')/2**64
ud=np.array([_u('DATE',d) for d in range(n_date)]); us=np.array([_u('SEC',s) for s in range(n_sec)])
up=np.array([_u('POS',p) for p in range(3)])
Y=np.zeros((3,n)); AV=np.zeros((3,n),bool)
rs=np.random.default_rng(12345)   # only for CELL/MISS hashing surrogate, seeded deterministically
for p in range(3):
    cell=np.array([_u('CELL',sec_of[i],date_of[i],p) for i in range(n)])
    miss=np.array([_u('MISS',sec_of[i],date_of[i],p) for i in range(n)])
    Y[p]=0.50*ud[date_of]+0.20*us[sec_of]+0.10*up[p]+0.20*cell+0.10*(date_class[date_of]==1)
    AV[p]=miss>=0.01
log(f"  Y range [{Y.min():.4f}, {Y.max():.4f}]   availability {AV.mean()*100:.2f}%")
def theta_for_draw(pi):
    """Returns theta (K,) with np.nan where undefined."""
    src_date=pi[date_of]; src=EPI[sec_of,src_date]
    win=(date_of>=aw_lo[sec_of])&(date_of<=aw_hi[sec_of])&(src_date>=aw_lo[sec_of])&(src_date<=aw_hi[sec_of])
    th=np.full(K,np.nan)
    for p in range(3):
        ok=(src>=0)&win&dev
        Yp=np.zeros(n); avp=np.zeros(n,bool)
        good=np.flatnonzero(ok)
        Yp[good]=Y[p][src[good]]; avp[good]=AV[p][src[good]]
        part=ok&avp
        for (A,Bf,pp),js in pool_key.items():
            if pp!=p: continue
            pool_idx=np.flatnonzero(FV[p,A]&FV[p+1,Bf]&part)
            if len(pool_idx)==0: continue
            poolY=Yp[pool_idx]
            for j in js:
                pj=POS[j]; pj=pj[part[pj]]
                if len(pj)==0 or len(pj)>=len(pool_idx): continue
                loc=np.searchsorted(pool_idx,pj)
                m=np.ones(len(pool_idx),bool); m[loc]=False
                if not m.any(): continue
                th[j]=np.median(Yp[pj])-np.median(poolY[m])
    return th
log("SCALE phase")
TH_S=np.empty((B_SCALE,K))
for b in range(B_SCALE):
    pi=date_map_timelocal(date_class,month_id,substream('SCALE',b))
    assert guard_shape(pi,n_date)[0] and guard_permutation(pi,n_date)[0] and guard_class_stratum(pi,date_class)[0]
    assert guard_time_locality(pi,month_id,date_class)[0] and guard_cyclic_structure(pi,month_id,date_class)[0]
    assert guard_early_close_identity(pi,date_class)[0]
    TH_S[b]=theta_for_draw(pi); log(f"  scale {b+1}/{B_SCALE} defined={int(np.isfinite(TH_S[b]).sum()):,}")
c=np.nanmedian(TH_S,axis=0)
s=1.4826*np.nanmedian(np.abs(TH_S-c),axis=0)
def_scale=np.isfinite(TH_S).sum(axis=0)
log("NULL phase")
TH_N=np.empty((B_NULL,K))
for b in range(B_NULL):
    pi=date_map_timelocal(date_class,month_id,substream('NULL',b))
    assert guard_class_stratum(pi,date_class)[0] and guard_time_locality(pi,month_id,date_class)[0]
    assert guard_cyclic_structure(pi,month_id,date_class)[0] and guard_early_close_identity(pi,date_class)[0]
    TH_N[b]=theta_for_draw(pi); log(f"  null {b+1}/{B_NULL} defined={int(np.isfinite(TH_N[b]).sum()):,}")
def_null=np.isfinite(TH_N).sum(axis=0)
stable=(def_scale==B_SCALE)&(def_null==B_NULL)&(s>0)&np.isfinite(s)
log(f"stable claims {int(stable.sum()):,} / {K:,}")
TH_OBS=theta_for_draw(np.arange(n_date,dtype=np.int32))
T_obs=np.where(stable,(TH_OBS-c)/np.where(s>0,s,1),-np.inf)
T_null=np.where(stable[None,:],(TH_N-c[None,:])/np.where(s>0,s,1)[None,:],-np.inf)
M=T_null.max(axis=1)
p_adj=np.array([(1+int((M>=T_obs[j]).sum()))/(B_NULL+1) if stable[j] else 1.0 for j in range(K)])
p_unadj=np.array([(1+int((T_null[:,j]>=T_obs[j]).sum()))/(B_NULL+1) if stable[j] else 1.0 for j in range(K)])
out=dict(K=K,stable=int(stable.sum()),unstable=int((~stable).sum()),
  B_SCALE=B_SCALE,B_NULL=B_NULL,
  p_adj_min=float(p_adj.min()),p_adj_max=float(p_adj.max()),
  p_zero=int((p_adj<=0).sum()),
  monotone_violations=int(((p_adj<p_unadj-1e-12)&stable).sum()),
  grid_ok=bool(np.all(np.isclose(p_adj[stable]*31,np.round(p_adj[stable]*31)))),
  M_matches_max=bool(np.allclose(M,T_null.max(axis=1))),
  fixed_max_set=bool(guard_fixed_max_set([np.where(np.isfinite(T_null[b]),T_null[b],np.nan) for b in range(B_NULL)])[0]),
  defined_scale_min=int(def_scale.min()),defined_null_min=int(def_null.min()))
out['map']='TIME_LOCAL_CIRCULAR'
json.dump(out,open(S+'/SMOKE_FULL_TL.json','w'),indent=1)
np.savez_compressed(S+'/SMOKE_FULL_TL_ARRAYS.npz',p_adj=p_adj,T_obs=T_obs,M=M,stable=stable,c=c,s=s)
log("DONE"); print(json.dumps(out,indent=1))
