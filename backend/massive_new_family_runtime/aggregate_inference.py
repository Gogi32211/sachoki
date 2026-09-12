import sys,json,os,hashlib,numpy as np
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
Z=np.load(S+'/PROD_CS.npz'); c=Z['c']; s=Z['s']; ds=Z['defined_scale']
sh=[f'{S}/prod_shards/NULL_{b:04d}.npz' for b in range(1800)]
missing=[p for p in sh if not os.path.exists(p)]
assert not missing, f"NULL incomplete: {len(missing)} missing"
TH=np.stack([np.load(p)['theta'] for p in sh])
dn=np.isfinite(TH).sum(axis=0)
stable=(ds==200)&(dn==1800)&(s>0)&np.isfinite(s)
import ref_kernel
dc=np.load(S+'/mm_date_class.npy'); ident=np.arange(len(dc),dtype=np.int32)
tho,_,_=ref_kernel.theta_for_map(ident)
sd=np.where(s>0,s,1)
T_obs=np.where(stable,(tho-c)/sd,-np.inf)
T_null=np.where(stable[None,:],(TH-c[None,:])/sd[None,:],-np.inf)
M=T_null.max(axis=1)
p_adj=np.array([(1+int((M>=T_obs[j]).sum()))/1801 if stable[j] else 1.0 for j in range(len(T_obs))])
np.savez_compressed(S+'/PROD_INFER.npz',theta_obs=tho,T_obs=T_obs,M=M,p_adj=p_adj,stable=stable,
                    defined_scale=ds,defined_null=dn)
dig=lambda a: hashlib.sha256(np.ascontiguousarray(a).tobytes()).hexdigest()[:16]
json.dump({'claims':len(T_obs),'stable':int(stable.sum()),
  'permutation_support_unstable':int(((ds<200)|(dn<1800)).sum()),
  'degenerate_null_scale':int(((s<=0)|~np.isfinite(s)).sum()),
  'M_digest':dig(M),'p_adj_digest':dig(p_adj),'T_obs_digest':dig(T_obs),
  'B_NULL':1800,'min_possible_p_adj':1/1801},open(S+'/PROD_INFER.json','w'),indent=1)
print(f"  inference aggregated: stable {int(stable.sum()):,}/{len(T_obs):,}")
