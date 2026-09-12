"""ONE-TIME: freeze each observed pool's Y sort order to disk (mmap-able)."""
import os,sys,json,time
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
import numpy as np, ref_kernel, ci_kernel
t0=time.time()
A,Yp,part=ci_kernel.observed_layer()
FV=A['FV']; keys=sorted(A['pool_key'].keys())
sizes=[]; t1=time.time()
tot=0
for (Af,Bf,pp) in keys:
    pool_idx=np.flatnonzero(np.asarray(FV[pp,Af])&np.asarray(FV[pp+1,Bf])&part[pp])
    sizes.append(pool_idx.size); tot+=pool_idx.size
print(f"pools {len(keys):,}  total pool rows {tot:,}  ({tot*4/1e9:.2f} GB as int32)  scan {time.time()-t1:.0f}s",flush=True)
off=np.zeros(len(keys)+1,np.int64); off[1:]=np.cumsum(sizes)
out=np.lib.format.open_memmap(S+'/ci_sorted_rows.npy',mode='w+',dtype=np.int32,shape=(int(off[-1]),))
t1=time.time()
for i,(Af,Bf,pp) in enumerate(keys):
    pool_idx=np.flatnonzero(np.asarray(FV[pp,Af])&np.asarray(FV[pp+1,Bf])&part[pp])
    if pool_idx.size==0: continue
    y=Yp[pp][pool_idx]
    if np.isnan(y).any(): raise ValueError("NaN Y in pool")
    o=np.argsort(y,kind='stable')          # primary Y asc, secondary row_id asc (pool_idx ascending)
    out[off[i]:off[i+1]]=pool_idx[o]
    if (i+1)%1000==0: print(f"  {i+1}/{len(keys)} el={time.time()-t1:.0f}s",flush=True)
out.flush()
np.save(S+'/ci_pool_off.npy',off)
json.dump({'pools':len(keys),'rows':int(off[-1]),
           'keys':[[int(a),int(b),int(c)] for a,b,c in keys]},open(S+'/ci_pool_meta.json','w'))
print(f"DONE one-time order build: {time.time()-t0:.0f}s  file {os.path.getsize(S+'/ci_sorted_rows.npy')/1e9:.2f} GB")
