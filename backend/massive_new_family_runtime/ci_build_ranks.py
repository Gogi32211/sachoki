"""ONE-TIME: per claim, the sorted RANKS of its positive rows within its pool's frozen order."""
import os,sys,json,time
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
import numpy as np, ref_kernel, ci_kernel
t0=time.time(); A,Yp,part=ci_kernel.observed_layer()
meta=json.load(open(S+'/ci_pool_meta.json')); keys=[tuple(k) for k in meta['keys']]
off=np.load(S+'/ci_pool_off.npy'); SR=np.load(S+'/ci_sorted_rows.npy',mmap_mode='r')
POSf=A['POSf']; POSo=A['POSo']; K=A['meta']['K']
lens=np.zeros(K,np.int64); tmp=[None]*K
for i,(Af,Bf,pp) in enumerate(keys):
    sr=np.asarray(SR[off[i]:off[i+1]])
    if sr.size==0: continue
    ordr=np.argsort(sr,kind='stable')      # row_id -> rank lookup
    srt=sr[ordr]
    for j in A['pool_key'][(Af,Bf,pp)]:
        pj=np.asarray(POSf[POSo[j]:POSo[j+1]]); pj=pj[part[pp][pj]]
        if pj.size==0 or pj.size>=sr.size: continue
        pos=np.searchsorted(srt,pj)
        ranks=np.sort(ordr[pos]).astype(np.int32)
        tmp[j]=ranks; lens[j]=ranks.size
    if (i+1)%1000==0: print(f"  {i+1}/{len(keys)} el={time.time()-t0:.0f}s",flush=True)
roff=np.zeros(K+1,np.int64); roff[1:]=np.cumsum(lens)
flat=np.lib.format.open_memmap(S+'/ci_claim_ranks.npy',mode='w+',dtype=np.int32,shape=(int(roff[-1]),))
for j in range(K):
    if tmp[j] is not None: flat[roff[j]:roff[j+1]]=tmp[j]
flat.flush(); np.save(S+'/ci_claim_rank_off.npy',roff)
cp=np.zeros(K,np.int32)
for i,(Af,Bf,pp) in enumerate(keys):
    for j in A['pool_key'][(Af,Bf,pp)]: cp[j]=i
np.save(S+'/ci_claim_pool.npy',cp)
print(f"DONE ranks: {int(roff[-1]):,} entries, {os.path.getsize(S+'/ci_claim_ranks.npy')/1e9:.2f} GB, {time.time()-t0:.0f}s")
