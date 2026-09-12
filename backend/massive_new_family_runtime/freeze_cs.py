import sys,json,glob,os,hashlib,numpy as np
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sh=[f'{S}/prod_shards/SCALE_{b:04d}.npz' for b in range(200)]
missing=[p for p in sh if not os.path.exists(p)]
assert not missing, f"SCALE incomplete: {len(missing)} missing — NULL MUST NOT LAUNCH"
TH=np.stack([np.load(p)['theta'] for p in sh])
c=np.nanmedian(TH,axis=0); s=1.4826*np.nanmedian(np.abs(TH-c),axis=0)
ds=np.isfinite(TH).sum(axis=0)
np.savez_compressed(S+'/PROD_CS.npz',c=c,s=s,defined_scale=ds)
d=hashlib.sha256(np.ascontiguousarray(c).tobytes()+np.ascontiguousarray(s).tobytes()).hexdigest()[:16]
json.dump({'SCALE_draws':200,'complete':True,'c_s_digest':d,
  'defined_scale_200of200':int((ds==200).sum()),'claims':len(c),
  'degenerate_scale_s_le_0':int(((s<=0)|~np.isfinite(s)).sum())},open(S+'/PROD_CS.json','w'),indent=1)
print(f"  c_j/s_j FROZEN  digest {d}  defined 200/200 on {int((ds==200).sum()):,}/{len(c):,} claims")
