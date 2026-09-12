"""REFERENCE CI KERNEL — straightforward WEIGHTED_MEDIAN_V2 path, semantics IMMUTABLE.
Bootstrap reweights the OBSERVED data; it never permutes."""
import os
for v in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','VECLIB_MAXIMUM_THREADS','NUMEXPR_NUM_THREADS'):
    os.environ.setdefault(v,'1')
import numpy as np, json, sys
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
from weighted_median_v2 import weighted_median_v2, EMPTY
import ref_kernel

def observed_layer():
    """The OBSERVED (identity-map) participation and Y. Shared upstream layer."""
    A=ref_kernel.arrays(); meta=A['meta']; n=meta['n']
    sec_of=A['sec_of']; date_of=A['date_of']; dev=A['dev']
    src=np.asarray(A['EPI'])[sec_of,date_of]
    win=(date_of>=A['aw_lo'][sec_of])&(date_of<=A['aw_hi'][sec_of])
    Yp=np.zeros((3,n)); part=np.zeros((3,n),bool)
    for p in range(3):
        ok=(src>=0)&win&dev; g=np.flatnonzero(ok)
        Yp[p][g]=np.asarray(A['Y'][p])[src[g]]; part[p][g]=np.asarray(A['AV'][p])[src[g]]
        part[p]&=ok
    return A,Yp,part

def theta_star(m_sec,m_date,A,Yp,part,claim_subset=None):
    """REFERENCE bootstrap replicate. w(s,d) = m_security(s) * m_date(d)."""
    meta=A['meta']; K=meta['K']
    W=(m_sec[A['sec_of']].astype(np.int64)*m_date[A['date_of']].astype(np.int64))
    th=np.full(K,np.nan); st=np.zeros(K,np.int8)   # 0 OK, 1 EMPTY
    FV=A['FV']; POSf=A['POSf']; POSo=A['POSo']
    for (Af,Bf,pp),js in A['pool_key'].items():
        if claim_subset is not None and not any(j in claim_subset for j in js): continue
        pool_idx=np.flatnonzero(np.asarray(FV[pp,Af])&np.asarray(FV[pp+1,Bf])&part[pp])
        if pool_idx.size==0: continue
        poolY=Yp[pp][pool_idx]; poolW=W[pool_idx]
        for j in js:
            if claim_subset is not None and j not in claim_subset: continue
            pj=np.asarray(POSf[POSo[j]:POSo[j+1]]); pj=pj[part[pp][pj]]
            if pj.size==0 or pj.size>=pool_idx.size: continue
            loc=np.searchsorted(pool_idx,pj)
            m=np.ones(pool_idx.size,bool); m[loc]=False
            a,sa=weighted_median_v2(Yp[pp][pj],W[pj])
            b,sb=weighted_median_v2(poolY[m],poolW[m])
            if sa==EMPTY or sb==EMPTY: st[j]=1
            else: th[j]=a-b
    return th,st
