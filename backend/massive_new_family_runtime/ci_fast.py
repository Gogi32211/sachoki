"""CI_WEIGHT_ONLY_EXACT_KERNEL — draw-level. Qualification authority: WEIGHTED_MEDIAN_V2."""
import os,sys,json
for v in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','VECLIB_MAXIMUM_THREADS'):
    os.environ.setdefault(v,'1')
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
import numpy as np
from ci_weight_kernel import complement_weighted_median
EMPTY='EMPTY_EFFECTIVE_GROUP'
def wmed_sorted(sv, sw):
    """WEIGHTED_MEDIAN_V2 on ALREADY-SORTED values. Same semantics, no re-sort."""
    W=int(sw.sum())
    if W<=0: return np.nan, EMPTY
    cw=np.cumsum(sw); half=W//2
    if W%2==0:
        hit=np.flatnonzero((cw==half)&(sw>0))
        if hit.size:
            i=int(hit[0]); nxt=np.flatnonzero((np.arange(sw.size)>i)&(sw>0))
            return float((sv[i]+sv[int(nxt[0])])/2), 'OK'
    j=int(np.searchsorted(cw, W/2.0, side='right'))
    while j<sw.size and sw[j]==0: j+=1
    return float(sv[j]), 'OK'
_C=None
def ctx():
    global _C
    if _C is None:
        import ref_kernel, ci_kernel
        A,Yp,part=ci_kernel.observed_layer()
        _C=dict(A=A,Yp=Yp,part=part,
            keys=[tuple(k) for k in json.load(open(S+'/ci_pool_meta.json'))['keys']],
            off=np.load(S+'/ci_pool_off.npy'),
            SR=np.load(S+'/ci_sorted_rows.npy',mmap_mode='r'),
            RK=np.load(S+'/ci_claim_ranks.npy',mmap_mode='r'),
            ROFF=np.load(S+'/ci_claim_rank_off.npy'),
            CPOOL=np.load(S+'/ci_claim_pool.npy'))
    return _C
def theta_star_fast(m_sec,m_date):
    C=ctx(); A=C['A']; K=A['meta']['K']
    W=(m_sec[A['sec_of']].astype(np.int64)*m_date[A['date_of']].astype(np.int64))
    th=np.full(K,np.nan); st=np.zeros(K,np.int8)
    off=C['off']; SR=C['SR']; RK=C['RK']; ROFF=C['ROFF']
    for i,(Af,Bf,pp) in enumerate(C['keys']):
        sr=np.asarray(SR[off[i]:off[i+1]])
        if sr.size==0: continue
        sY=C['Yp'][pp][sr]; sW=W[sr]; C_pool=np.cumsum(sW)
        for j in A['pool_key'][(Af,Bf,pp)]:
            Pr=np.asarray(RK[ROFF[j]:ROFF[j+1]])
            if Pr.size==0: continue
            Pw=sW[Pr]; Pc=np.cumsum(Pw)
            a,sa=wmed_sorted(sY[Pr],Pw)
            b,sb=complement_weighted_median(sY,C_pool,Pr,Pc)
            if sa==EMPTY or sb==EMPTY: st[j]=1
            else: th[j]=a-b
    return th,st
