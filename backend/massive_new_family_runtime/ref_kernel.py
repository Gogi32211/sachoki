"""QUALIFIED REFERENCE STATISTIC KERNEL — semantics IMMUTABLE.
Used identically by the single-process oracle and by every parallel worker."""
import os
for v in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','VECLIB_MAXIMUM_THREADS','NUMEXPR_NUM_THREADS'):
    os.environ.setdefault(v,'1')
import numpy as np, json
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
_A=None
def arrays():
    global _A
    if _A is None:
        L=lambda n: np.load(f'{S}/mm_{n}.npy', mmap_mode='r')
        meta=json.load(open(f'{S}/mm_meta.json'))
        pk={}
        fa=np.asarray(L('fa')); fb=np.asarray(L('fb')); cp=np.asarray(L('cp'))
        for j in range(meta['K']): pk.setdefault((int(fa[j]),int(fb[j]),int(cp[j])),[]).append(j)
        _A=dict(meta=meta,TT=L('TT'),FV=L('FV'),Y=L('Y'),AV=L('AV'),EPI=L('EPI'),
                sec_of=np.asarray(L('sec_of')),date_of=np.asarray(L('date_of')),
                aw_lo=np.asarray(L('aw_lo')),aw_hi=np.asarray(L('aw_hi')),
                dev=np.asarray(L('dev')),POSf=L('POS_flat'),POSo=np.asarray(L('POS_off')),
                pool_key=pk)
    return _A

def theta_for_map(pi):
    """REFERENCE kernel. Returns (theta, defined, counts)."""
    A=arrays(); meta=A['meta']; K=meta['K']; n=meta['n']
    sec_of=A['sec_of']; date_of=A['date_of']; dev=A['dev']
    src_date=pi[date_of]; src=np.asarray(A['EPI'])[sec_of,src_date]
    win=((date_of>=A['aw_lo'][sec_of])&(date_of<=A['aw_hi'][sec_of])
         &(src_date>=A['aw_lo'][sec_of])&(src_date<=A['aw_hi'][sec_of]))
    th=np.full(K,np.nan); dfn=np.zeros(K,bool); cnt=np.zeros((K,3),np.int64)
    FV=A['FV']; Y=A['Y']; AV=A['AV']; POSf=A['POSf']; POSo=A['POSo']
    for p in range(3):
        ok=(src>=0)&win&dev; g=np.flatnonzero(ok)
        Yp=np.zeros(n); part=np.zeros(n,bool)
        Yp[g]=np.asarray(Y[p])[src[g]]; part[g]=np.asarray(AV[p])[src[g]]
        part&=ok
        for (Af,Bf,pp),js in A['pool_key'].items():
            if pp!=p: continue
            pool_idx=np.flatnonzero(np.asarray(FV[p,Af])&np.asarray(FV[p+1,Bf])&part)
            if pool_idx.size==0: continue
            poolY=Yp[pool_idx]
            for j in js:
                pj=np.asarray(POSf[POSo[j]:POSo[j+1]])
                pj=pj[part[pj]]
                if pj.size==0 or pj.size>=pool_idx.size: continue
                loc=np.searchsorted(pool_idx,pj)
                m=np.ones(pool_idx.size,bool); m[loc]=False
                th[j]=float(np.median(Yp[pj]))-float(np.median(poolY[m]))
                dfn[j]=True; cnt[j]=(pool_idx.size,pj.size,pool_idx.size-pj.size)
    return th,dfn,cnt
