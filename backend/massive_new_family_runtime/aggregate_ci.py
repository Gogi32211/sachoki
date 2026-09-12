import sys,json,os,hashlib,numpy as np
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sh=[f'{S}/prod_ci/CI_{b:04d}.npz' for b in range(2000)]
missing=[p for p in sh if not os.path.exists(p)]
assert not missing, f"CI incomplete: {len(missing)} missing"
TH=np.stack([np.load(p)['theta'] for p in sh])
defined=np.isfinite(TH).sum(axis=0)
gate=(defined==2000)
lo=np.where(gate,np.quantile(np.nan_to_num(TH,nan=0.0),0.025,axis=0,method='linear'),np.nan)
hi=np.where(gate,np.quantile(np.nan_to_num(TH,nan=0.0),0.975,axis=0,method='linear'),np.nan)
np.savez_compressed(S+'/PROD_CI.npz',lo=lo,hi=hi,defined=defined,gate=gate)
dig=lambda a: hashlib.sha256(np.ascontiguousarray(a).tobytes()).hexdigest()[:16]
json.dump({'B_CI':2000,'claims':len(lo),
  'CI_STABILITY_GATE_pass':int(gate.sum()),'bootstrap_support_unstable':int((~gate).sum()),
  'quantile_method':'linear / Hyndman-Fan Type 7 (explicit, not library default)',
  'lo_digest':dig(lo),'hi_digest':dig(hi)},open(S+'/PROD_CI.json','w'),indent=1)
print(f"  CI aggregated: gate pass {int(gate.sum()):,}/{len(lo):,}")
