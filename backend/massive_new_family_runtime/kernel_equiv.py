"""Single-draw REFERENCE vs EXACT FAST equivalence. Shared upstream layer, two kernels."""
import sys,json,time,hashlib
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
import numpy as np, duckdb
from smoke_perm import substream, date_map_timelocal
from fast_kernel import median_of_complement, median_of_subset
def log(m): print(f"[{time.strftime('%H:%M:%S')}] {m}",flush=True)
z=np.load(S+'/SMOKE_EPISODES.npz'); n=int(z['n'])
TT=np.unpackbits(z['TT'],axis=-1,count=n).astype(bool); FV=np.unpackbits(z['FV'],axis=-1,count=n).astype(bool)
sec_of=z['sec_of']; date_of=z['date_of']; aw_lo=z['aw_lo']; aw_hi=z['aw_hi']
date_class=z['date_class']; n_sec=int(z['n_sec']); n_date=int(z['n_date'])
con=duckdb.connect(S+'/POS4.duckdb',read_only=True)
sess=con.execute("select distinct k,session_date from pos4 order by k,session_date").df()
dev=(sess.session_date<='2026-08-06').to_numpy()
EPI=np.full((n_sec,n_date),-1,np.int32); EPI[sec_of,date_of]=np.arange(n,dtype=np.int32)
_dl=sorted(duckdb.connect(S+'/stageA_input.duckdb',read_only=True).execute(
    'select distinct session_date from bars').df().session_date)
_m=sorted({x[:7] for x in _dl}); MI={m:i for i,m in enumerate(_m)}
month_id=np.array([MI[x[:7]] for x in _dl],np.int32)
TOK=json.load(open(S+'/TOKEN_DICTIONARY_123.json')); tok_ids=sorted(TOK); TIDX={t:i for i,t in enumerate(tok_ids)}
man=json.load(open(S+'/CANONICAL_69_MANIFEST_V2.json')); feats=sorted(man); FIDX={f:i for i,f in enumerate(feats)}
reg=duckdb.connect(S+'/SUPPORT_CENSUS.duckdb',read_only=True).execute(
  "select claim_id,token_a,token_b,position_pair from registry where support_eligible order by claim_id").df()
K=len(reg); PPN=['M1->M2','M2->M3','M3->M4']; PIDX={p:i for i,p in enumerate(PPN)}
ca=np.array([TIDX[t] for t in reg.token_a]); cb=np.array([TIDX[t] for t in reg.token_b])
cp=np.array([PIDX[p] for p in reg.position_pair])
fa=np.array([FIDX[TOK[t]['source_feature']] for t in reg.token_a])
fb=np.array([FIDX[TOK[t]['source_feature']] for t in reg.token_b])
log(f"claims {K:,}")
POS=[np.flatnonzero(TT[cp[j],ca[j]] & TT[cp[j]+1,cb[j]] & dev).astype(np.int32) for j in range(K)]
pool_key={}
for j in range(K): pool_key.setdefault((fa[j],fb[j],cp[j]),[]).append(j)
log(f"pools {len(pool_key):,}  positives {sum(len(x) for x in POS):,}")
def _u(tag,*k): return int.from_bytes(hashlib.sha256(('|'.join([tag]+[str(x) for x in k])).encode()).digest()[:8],'big')/2**64
ud=np.array([_u('DATE',d) for d in range(n_date)]); us=np.array([_u('SEC',s) for s in range(n_sec)])
up=np.array([_u('POS',p) for p in range(3)])
Y=np.zeros((3,n)); AV=np.zeros((3,n),bool)
for p in range(3):
    cell=np.array([_u('CELL',sec_of[i],date_of[i],p) for i in range(n)])
    miss=np.array([_u('MISS',sec_of[i],date_of[i],p) for i in range(n)])
    Y[p]=0.50*ud[date_of]+0.20*us[sec_of]+0.10*up[p]+0.20*cell+0.10*(date_class[date_of]==1)
    AV[p]=miss>=0.01
# ---------- SHARED UPSTREAM LAYER ----------
pi=date_map_timelocal(date_class,month_id,substream('SCALE',0))
src_date=pi[date_of]; src=EPI[sec_of,src_date]
win=(date_of>=aw_lo[sec_of])&(date_of<=aw_hi[sec_of])&(src_date>=aw_lo[sec_of])&(src_date<=aw_hi[sec_of])
Yp=np.zeros((3,n)); part=np.zeros((3,n),bool)
for p in range(3):
    ok=(src>=0)&win&dev; g=np.flatnonzero(ok)
    Yp[p][g]=Y[p][src[g]]; part[p][g]=AV[p][src[g]]
    part[p]&=ok
log("shared upstream layer built")
def run(kernel):
    med_p=np.full(K,np.nan); med_n=np.full(K,np.nan); th=np.full(K,np.nan)
    cnt=np.zeros((K,3),np.int64); dfn=np.zeros(K,bool)
    for (A,Bf,pp),js in pool_key.items():
        pool_idx=np.flatnonzero(FV[pp,A]&FV[pp+1,Bf]&part[pp])
        if pool_idx.size==0: continue
        poolY=Yp[pp][pool_idx]
        if kernel=='fast':
            o=np.argsort(poolY,kind='stable'); sY=poolY[o]
            inv=np.empty_like(o); inv[o]=np.arange(o.size)
        for j in js:
            pj=POS[j]; pj=pj[part[pp][pj]]
            if pj.size==0 or pj.size>=pool_idx.size: continue
            loc=np.searchsorted(pool_idx,pj)
            cnt[j]=(pool_idx.size,pj.size,pool_idx.size-pj.size)
            if kernel=='ref':
                m=np.ones(pool_idx.size,bool); m[loc]=False
                mp=float(np.median(Yp[pp][pj])); mn=float(np.median(poolY[m]))
            else:
                Rs=np.sort(inv[loc])
                mp=median_of_subset(sY,Rs); mn=median_of_complement(sY,Rs)
            med_p[j]=mp; med_n[j]=mn; th[j]=mp-mn; dfn[j]=True
    return med_p,med_n,th,cnt,dfn
t0=time.time(); r=run('ref');  t_ref=time.time()-t0; log(f"REFERENCE kernel {t_ref:.1f}s")
t0=time.time(); f=run('fast'); t_fast=time.time()-t0; log(f"FAST kernel      {t_fast:.1f}s   speedup {t_ref/t_fast:.2f}x")
def cmp_exact(a,b):
    bad=int(np.sum((np.isnan(a)!=np.isnan(b)) | ((~np.isnan(a)) & (a!=b)))); return bad
res={'claims':K,'defined_ref':int(r[4].sum()),'defined_fast':int(f[4].sum()),
 'defined_mask_mismatch':int((r[4]!=f[4]).sum()),
 'counts_mismatch':int((r[3]!=f[3]).sum()),
 'median_positive_mismatch':cmp_exact(r[0],f[0]),
 'median_nonpositive_mismatch':cmp_exact(r[1],f[1]),
 'theta_mismatch':cmp_exact(r[2],f[2]),
 'ref_seconds':round(t_ref,1),'fast_seconds':round(t_fast,1),'speedup':round(t_ref/t_fast,2)}
res['ALL_EXACT']= all(res[k]==0 for k in ['defined_mask_mismatch','counts_mismatch',
  'median_positive_mismatch','median_nonpositive_mismatch','theta_mismatch'])
json.dump(res,open(S+'/KERNEL_EQUIV_1DRAW.json','w'),indent=1)
print(json.dumps(res,indent=1))
