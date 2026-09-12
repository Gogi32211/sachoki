"""PARALLEL CI EXECUTION. Workers make ZERO RNG calls; all CI randomization sealed first."""
import os,sys,json,time,hashlib,argparse
S='/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad'
sys.path.insert(0,S)
import numpy as np

def build_ci_manifest(path,B):
    from smoke_perm import substream
    meta=json.load(open(S+'/mm_meta.json')); n_sec=meta['n_sec']; n_date=meta['n_date']
    recs=[]; W={}
    for b in range(B):
        g=substream('CI',b)
        m_s=g.multinomial(n_sec,np.ones(n_sec)/n_sec).astype(np.int32)
        scope=np.load(S+'/mm_in_scope.npy')          # AMENDMENT_3
        dev_idx=np.flatnonzero(scope); nd=len(dev_idx)
        m_d=np.zeros(n_date,np.int32)
        m_d[dev_idx]=g.multinomial(nd,np.ones(nd)/nd).astype(np.int32)
        did=f"CI_{b:04d}"; W[did+'_s']=m_s; W[did+'_d']=m_d
        recs.append(dict(draw_id=did,index=b,substream='CI',master_seed=20260829,
            sec_digest=hashlib.sha256(m_s.tobytes()).hexdigest()[:16],
            date_digest=hashlib.sha256(m_d.tobytes()).hexdigest()[:16]))
    np.savez_compressed(path+'.npz',**W)
    json.dump(dict(B=B,draws=recs,
        manifest_digest=hashlib.sha256(json.dumps(recs,sort_keys=True).encode()).hexdigest()[:16]),
        open(path+'.json','w'),indent=1)
    return recs

def worker(args):
    did,path,outdir,kernel=args
    Z=np.load(path+'.npz'); m_s=Z[did+'_s']; m_d=Z[did+'_d']
    man={r['draw_id']:r for r in json.load(open(path+'.json'))['draws']}
    if (hashlib.sha256(m_s.tobytes()).hexdigest()[:16]!=man[did]['sec_digest'] or
        hashlib.sha256(m_d.tobytes()).hexdigest()[:16]!=man[did]['date_digest']):
        return (did,'WEIGHT_DIGEST_MISMATCH',None)
    t0=time.time()
    if kernel=='fast':
        import ci_fast; th,st=ci_fast.theta_star_fast(m_s,m_d)
    else:
        import ci_kernel
        A,Yp,part=ci_kernel.observed_layer(); th,st=ci_kernel.theta_star(m_s,m_d,A,Yp,part)
    el=time.time()-t0
    chk=hashlib.sha256(np.ascontiguousarray(th).tobytes()+np.ascontiguousarray(st).tobytes()).hexdigest()[:16]
    tmp=os.path.join(outdir,did+'.tmp.npz'); fin=os.path.join(outdir,did+'.npz')
    np.savez(tmp,theta=th,status=st,header=np.array([did,kernel,man[did]['sec_digest'],
        man[did]['date_digest'],chk,str(len(th)),'COMPLETED']))
    os.replace(tmp,fin)
    return (did,'COMPLETED',round(el,1))

if __name__=='__main__':
    ap=argparse.ArgumentParser(); ap.add_argument('--workers',type=int,default=4)
    ap.add_argument('--outdir',required=True); ap.add_argument('--kernel',default='fast')
    ap.add_argument('--B',type=int,default=20)
    a=ap.parse_args(); os.makedirs(a.outdir,exist_ok=True)
    mp=S+'/CI_MANIFEST'
    if not os.path.exists(mp+'.json'): build_ci_manifest(mp,a.B); print("CI manifest sealed",flush=True)
    ids=[r['draw_id'] for r in json.load(open(mp+'.json'))['draws']][:a.B]
    todo=[i for i in ids if not os.path.exists(os.path.join(a.outdir,i+'.npz'))]
    print(f"kernel={a.kernel} draws {len(ids)} todo {len(todo)} workers {a.workers}",flush=True)
    t0=time.time()
    if a.workers==1: res=[worker((d,mp,a.outdir,a.kernel)) for d in todo]
    else:
        import multiprocessing as MP; MP.set_start_method('spawn',force=True)
        with MP.Pool(a.workers) as pool: res=pool.map(worker,[(d,mp,a.outdir,a.kernel) for d in todo],chunksize=1)
    el=time.time()-t0; bad=[r for r in res if r[1]!='COMPLETED']
    print(f"completed {len(res)-len(bad)} failed {len(bad)} wall {el:.1f}s "
          f"sec/draw {el/max(1,len(todo)):.1f} draws/hour {3600*len(todo)/max(el,1e-9):.1f}",flush=True)
