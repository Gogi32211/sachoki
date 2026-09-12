import os, sys, json, hashlib, shutil, time
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
D='/Users/sachoki/MASSIVE_DATA'
def sha(p, bs=1<<22):
    h=hashlib.sha256()
    with open(p,'rb') as f:
        while True:
            b=f.read(bs)
            if not b: break
            h.update(b)
    return h.hexdigest()
TIER={
 'ROOT_INPUT':['stageA_input.duckdb','pd_input.duckdb'],
 'PRODUCER_OUTPUT':['STAGE_D_R2.duckdb','pd12.duckdb','stageC4.duckdb','PHASE1_R1.duckdb'],
 'CANONICAL_X':['CANON69.duckdb'],
 'KERNEL_INPUT':['mm_TT.npy','mm_FV.npy','mm_Y.npy','mm_AV.npy','mm_EPI.npy','mm_POS_flat.npy',
   'mm_POS_off.npy','mm_sec_of.npy','mm_date_of.npy','mm_aw_lo.npy','mm_aw_hi.npy','mm_dev.npy',
   'mm_fa.npy','mm_fb.npy','mm_cp.npy','mm_date_class.npy','mm_month_id.npy','mm_meta.json',
   'real_Y_status.npy'],
 'SEALED_RESULT':['PROD_INFER.npz','PROD_CI.npz','PROD_CS.npz','PROD_DENOM.npz',
   'DRAW_MANIFEST.npz','CI_MANIFEST.npz',
   'VIOLATION_PROD_INFER.npz','VIOLATION_PROD_CI.npz','VIOLATION_PROD_CS.npz','VIOLATION_DRAW_MANIFEST.npz'],
}
CLASSIFIED_REPRODUCIBLE={
 'ci_sorted_rows.npy':'CI rank scaffold — rebuildable from CANON69 + ci_build_order/ci_build_ranks',
 'ci_claim_ranks.npy':'same','ci_claim_pool.npy':'same','ci_claim_rank_off.npy':'same',
 'prod_shards/':'per-draw shards — superseded by the aggregated PROD_* npz',
 'prod_ci/':'same',
 'VIOLATION_prod_shards/':'quarantined SCOPE_VIOLATION shards; metadata retained in VIOLATION_PROD_*.npz',
 'VIOLATION_prod_ci/':'same',
 'synthetic_backup/':'synthetic-Y smoke; qualification already sealed',
 'POS4.duckdb':'intermediate position store','ema3489.duckdb':'intermediate feature store',
 'shards_ref/ shards_p4/ shards_crash/':'equivalence-test shards; verdicts already sealed',
}
man={'artifact':'MASSIVE_NEW_FAMILY_DATA_PERSISTENCE_MANIFEST_V1','data_root':D,
     'source_scratchpad':S,'files':{},'classified_reproducible':CLASSIFIED_REPRODUCIBLE}
t0=time.time(); tot=0
for tier,files in TIER.items():
    for f in files:
        src=os.path.join(S,f)
        if not os.path.exists(src):
            man['files'][f]={'tier':tier,'status':'ABSENT_AT_SOURCE'}; print('MISSING',f,flush=True); continue
        dst=os.path.join(D,'historical',f)
        sz=os.path.getsize(src)
        print(f'[{time.time()-t0:6.0f}s] copy {f} ({sz/1e9:.2f} GB)',flush=True)
        shutil.copy2(src,dst)
        ds=sha(src); dd=sha(dst)
        ok = ds==dd
        man['files'][f]={'tier':tier,'path':dst,'bytes':sz,'sha256':ds,'digest_match':ok}
        tot+=sz
        print(f'          sha {ds[:16]}  match={ok}',flush=True)
        if not ok: sys.exit('DIGEST MISMATCH '+f)
man['total_bytes']=tot; man['elapsed_s']=round(time.time()-t0)
json.dump(man,open(S+'/_persist_manifest.json','w'),indent=1)
print(f'DONE {tot/1e9:.2f} GB in {time.time()-t0:.0f}s',flush=True)
