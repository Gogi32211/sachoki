"""PARALLEL REFERENCE EXECUTION — orchestration only. Statistic kernel UNCHANGED.
Workers make ZERO RNG calls and duplicate ZERO semantic logic."""
import os,sys,json,time,hashlib,argparse
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
import numpy as np
import os as _os

def build_manifest(path):
    """PARENT ONLY. All randomization generated and sealed BEFORE any worker starts."""
    from smoke_perm import substream, date_map_scoped, guard_inferential_scope
    dc=np.load(f'{S}/mm_date_class.npy'); mi=np.load(f'{S}/mm_month_id.npy')
    scope=np.load(f'{S}/mm_in_scope.npy')      # AMENDMENT_3: frozen inferential scope (DEV)
    recs=[]; maps={}
    for phase,B in (('SCALE',int(os.environ.get('B_SCALE',20))),('NULL',int(os.environ.get('B_NULL',30)))):
        for b in range(B):
            pi=date_map_scoped(dc,mi,scope,substream(phase,b))
            ok,msg=guard_inferential_scope(pi,scope)
            assert ok, f'{did if "did" in dir() else phase}: {msg}'
            did=f"{phase}_{b:04d}"
            maps[did]=pi
            recs.append(dict(draw_id=did,phase=phase,index=b,master_seed=20260829,substream=phase,
                             map_digest=hashlib.sha256(pi.tobytes()).hexdigest()[:16]))
    np.savez_compressed(path+'.npz',**maps)
    json.dump(dict(n_draws=len(recs),draws=recs,
        manifest_digest=hashlib.sha256(json.dumps(recs,sort_keys=True).encode()).hexdigest()[:16]),
        open(path+'.json','w'),indent=1)
    return recs

def worker(args):
    did,path,outdir=args
    import ref_kernel
    Z=np.load(path+'.npz'); pi=Z[did]
    man={r['draw_id']:r for r in json.load(open(path+'.json'))['draws']}
    exp=man[did]['map_digest']; got=hashlib.sha256(pi.tobytes()).hexdigest()[:16]
    if exp!=got: return (did,'MAP_DIGEST_MISMATCH',None)
    t0=time.time(); th,dfn,cnt=ref_kernel.theta_for_map(pi); el=time.time()-t0
    chk=hashlib.sha256(np.ascontiguousarray(th).tobytes()+np.ascontiguousarray(dfn).tobytes()).hexdigest()[:16]
    tmp=os.path.join(outdir,did+'.tmp.npz'); fin=os.path.join(outdir,did+'.npz')
    np.savez(tmp,theta=th,defined=dfn,counts=cnt,
             header=np.array([did,man[did]['phase'],exp,chk,str(len(th)),'COMPLETED']))
    os.replace(tmp,fin)
    return (did,'COMPLETED',round(el,1))

if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--workers',type=int,default=4)
    ap.add_argument('--outdir',default=S+'/shards'); ap.add_argument('--draws',default='')
    a=ap.parse_args()
    os.makedirs(a.outdir,exist_ok=True)
    mp_path=S+'/DRAW_MANIFEST'
    if not os.path.exists(mp_path+'.json'):
        build_manifest(mp_path); print("manifest sealed",flush=True)
    recs=json.load(open(mp_path+'.json'))['draws']
    ids=[r['draw_id'] for r in recs]
    if a.draws: ids=[i for i in ids if i in set(a.draws.split(','))]
    todo=[i for i in ids if not os.path.exists(os.path.join(a.outdir,i+'.npz'))]
    print(f"draws total {len(ids)}  todo {len(todo)}  workers {a.workers}",flush=True)
    t0=time.time()
    if a.workers==1:
        res=[worker((d,mp_path,a.outdir)) for d in todo]
    else:
        import multiprocessing as MP
        MP.set_start_method('spawn',force=True)
        with MP.Pool(a.workers) as pool:
            res=pool.map(worker,[(d,mp_path,a.outdir) for d in todo],chunksize=1)
    el=time.time()-t0
    bad=[r for r in res if r[1]!='COMPLETED']
    print(f"completed {len(res)-len(bad)}  failed {len(bad)}  wall {el:.1f}s  "
          f"sec/draw {el/max(1,len(todo)):.1f}  draws/hour {3600*len(todo)/max(el,1e-9):.1f}",flush=True)
    if bad: print("FAILURES:",bad,flush=True)
