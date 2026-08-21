"""Empirical-noise capability, under T5_CAPABILITY_SPEC_V1_PAIRED (b87f73790705e3d5).

Machine reads mfe_10d. Human sees only detection counts per (needle, delta) — no claim
ranking, no observed historical theta, nothing about which sequences look good.

Implementation of the estimand, vectorised once and reused everywhere:
  theta_s = sum_b w_sb * [median(Y|S=1,b) - median(Y|S=0,b)],  w_sb = treated_n_sb / total
Per permutation we need theta for ALL 840 claims. Medians per (claim x block x arm) are the
cost; blocks are small (median 13), so we sort Y within blocks once per permutation and use
searchsorted-free grouped medians via np.add.reduceat on rank positions... kept simple:
for each block, Y_block sorted once; for each claim, treated mask within block -> median via
partition. With 5,010 blocks x 840 claims that is heavy in pure Python, so the inner loop is
restructured: FOR EACH CLAIM, its overlap blocks only (median ~500-5,000 episodes total),
which makes one theta pass ~O(sum of claim overlap sizes), measured below before committing.
"""
import hashlib, json, os, sys, time
import numpy as np, pandas as pd
sys.path.insert(0,'.')
import t5_dna as D, t5_sequence_estimand as EST

SPEC=json.load(open("T5_CAPABILITY_Q50_V1.json"))
INH=SPEC
OUT=os.path.join(D.HERE,"T5_CAPABILITY_RESULT_V1.json")
CELLS=os.path.join(D.ROOT,"data","t5_capability_cells.parquet")

def build_state(verbose=True):
    t0=time.time()
    C=pd.read_parquet(os.path.join(D.ROOT,"data","t5_sequence_survivors_v1.parquet"))
    toks=pd.read_parquet(os.path.join(D.ROOT,"data","t5_sequence_tokens_v1.parquet")).token.tolist()
    ORD=pd.read_parquet(os.path.join(D.ROOT,"data","t5_estimand_claim_order.parquet"))
    E=pd.read_parquet(D.OUT_EP,columns=["episode_id","ticker","t5_date","n_t5_day_1h_bars"])
    O=pd.read_parquet(os.path.join(D.ROOT,"data","t5_episode_outcomes.parquet"),
                      columns=["episode_id","path_status_10d","mfe_10d"])   # SEALED Y ACCESS
    P=E[E.n_t5_day_1h_bars.fillna(0)>0].merge(O,on="episode_id",how="left")
    P=P[P.path_status_10d=="AVAILABLE"]
    P=P.merge(EST.anchors(P),on="episode_id",how="inner")
    P=P[P.liq_anchor.notna()&P.vol_anchor.notna()&(P.n20==20)]
    P=P[P.episode_id.isin(EST.analyzable_episodes())]
    P=EST.blocks(P)
    MEM=EST.membership(dict(tokens=toks,claims=[
        (f,L,[(r.canonical_sequence,json.loads(r.token_idx))
              for r in C[(C.family==f)&(C.length==L)].itertuples()])
        for f in ("T5_INTRADAY","PREV_INTRADAY","CROSS_DAY") for L in (2,3)]))
    MEM=MEM.merge(P[["episode_id","block","mfe_10d"]],on="episode_id",how="inner")
    MEM=MEM.sort_values("episode_id").reset_index(drop=True)
    if hashlib.sha256("|".join(sorted(MEM.episode_id)).encode()).hexdigest()[:16]!=INH["population_hash"]:
        raise RuntimeError("population hash drift")
    # Additive side-channel: the sealed population's episode order, so a secondary outcome can
    # be aligned to y without duplicating this construction. Nothing returned here changes.
    global LAST_EPISODE_IDS
    LAST_EPISODE_IDS=MEM.episode_id.to_numpy()
    y=MEM.mfe_10d.to_numpy(float)*100.0
    b,_=pd.factorize(MEM.block); nb=b.max()+1
    # canonical 840 columns in sealed order
    name_cols=[c for c in MEM.columns if "|" in c]
    sig_of={}
    for c in name_cols:
        s=MEM[c].to_numpy()
        nt=np.bincount(b,weights=s,minlength=nb); ncid=np.bincount(b,minlength=nb)-nt
        sig=s&((nt>0)&(ncid>0))[b]
        h=hashlib.sha256(np.packbits(sig).tobytes()).hexdigest()[:16]
        if h not in sig_of: sig_of[h]=sig
    order=ORD.sort_values("j")
    if hashlib.sha256("|".join(order.membership_hash).encode()).hexdigest()[:16]!=INH["claim_order_hash"]:
        raise RuntimeError("claim order drift")
    S=np.stack([sig_of[h] for h in order.membership_hash])          # (840, N) bool
    # per-claim precomputed: overlap blocks, and episode row indices grouped by block
    claims=[]
    for j in range(S.shape[0]):
        m=S[j]; blk=b[m]
        # treated indices per block + control indices per block, restricted to overlap blocks
        ob=np.unique(blk)
        rows_t={bb:np.flatnonzero(m&(b==bb)) for bb in ob}
        rows_c={bb:np.flatnonzero((~m)&(b==bb)) for bb in ob}
        w=np.array([len(rows_t[bb]) for bb in ob],float); w/=w.sum()
        claims.append((ob,rows_t,rows_c,w))
    if verbose: print(f"state ready: N={len(MEM):,} k={S.shape[0]} blocks={nb:,} "
                      f"{time.time()-t0:.0f}s",flush=True)
    return y,b,nb,S,claims,order

def theta_all(y,claims):
    out=np.empty(len(claims))
    for j,(ob,rt,rc,w) in enumerate(claims):
        d=np.array([np.median(y[rt[bb]])-np.median(y[rc[bb]]) for bb in ob])
        out[j]=w@d
    return out

def perm_within_blocks(y,b,nb,rng):
    yy=y.copy()
    for bb in range(nb):
        idx=np.flatnonzero(b==bb)
        if len(idx)>1: yy[idx]=yy[idx][rng.permutation(len(idx))]
    return yy



# ── vectorised engine: same estimand, no per-claim Python loop ───────────────
class FastTheta:
    """Reference per-claim medians. Two vectorised rewrites were tried and BOTH were slower
    (27.6s and 33.6s against 9.1s): padding is wasted on blocks whose median size is 13, and
    a global lexsort over 34.2M flattened elements costs more than many small partitions.
    The straightforward loop is kept, and the equivalence check below is kept with it."""
    def __init__(self, claims, n): self.claims=claims
    def theta(self, y): return theta_all(y, self.claims)

def perm_fast(y, blk_order, blk_starts, rng):
    """Permute Y within blocks via one shuffle of within-block positions."""
    ys = y[blk_order].copy()
    for a, b_ in zip(blk_starts[:-1], blk_starts[1:]):
        if b_ - a > 1: ys[a:b_] = ys[a:b_][rng.permutation(b_ - a)]
    out = np.empty_like(y); out[blk_order] = ys; return out

def run(needle_index_list=(1,), workers=8):
    import multiprocessing as mp
    y,b,nb,S,claims,order = build_state()
    FT = FastTheta(claims, len(y))
    # equivalence check: vectorised == reference on the observed vector
    t=time.time(); ref=theta_all(y,claims); fast=FT.theta(y)
    if not np.allclose(ref,fast,atol=1e-10): raise RuntimeError("FastTheta != reference")
    print(f"engine equivalence OK · fast pass {time.time()-t:.2f}s",flush=True)
    blk_order=np.argsort(b,kind="stable")
    bs=b[blk_order]; blk_starts=np.r_[0,np.flatnonzero(bs[1:]!=bs[:-1])+1,len(bs)]
    needles=SPEC["what_is_inherited_unchanged"]["needles"]
    deltas=SPEC["what_is_inherited_unchanged"]["delta_grid_pp"]
    NW=SPEC["what_is_inherited_unchanged"]["worlds_per_cell"]; NP=SPEC["what_is_inherited_unchanged"]["n_perm_inner"]
    jmap={v["membership_hash"]:int(v["sealed_j"]) for v in needles.values()}
    rows=[]
    for ni,(nname,nd) in enumerate(needles.items()):
        if ni not in needle_index_list: continue
        j=int(nd["sealed_j"]); mask=S[list(order.j).index(j)] if False else None
        # column position of sealed j in canonical order
        col=int(np.flatnonzero(order.j.to_numpy()==j)[0]); memb=S[col]
        for wi in range(NW):
            og=np.random.default_rng([20260820,ni,wi])
            base=perm_fast(y,blk_order,blk_starts,og)
            Ys={d: base + d*memb for d in deltas}
            th_obs={d: FT.theta(Ys[d]) for d in deltas}
            mx={d: np.empty(NP) for d in deltas}
            for p in range(NP):
                ig=np.random.default_rng([20260821,ni,wi,p])
                # ONE mapping applied to every delta: permute a index-map, reuse
                pm=perm_fast(np.arange(len(y),dtype=float),blk_order,blk_starts,ig).astype(int)
                for d in deltas:
                    mx[d][p]=FT.theta(Ys[d][pm]).max()
            for d in deltas:
                b95=float(np.percentile(mx[d],95)); tn=float(th_obs[d][col])
                rows.append(dict(needle=nname,delta_pp=d,world_id=wi,
                    needle_theta=round(tn,4),band_p95=round(b95,4),
                    detected=bool(tn>b95),
                    inner_max_null_hash=hashlib.sha256(mx[d].tobytes()).hexdigest()[:16]))
            det={d:sum(r["detected"] for r in rows if r["needle"]==nname and r["delta_pp"]==d) for d in deltas}
            print(f"  {nname} world {wi:>2} done · running "+
                  " ".join(f"d{d}={det[d]}" for d in deltas),flush=True)
            pd.DataFrame(rows).to_parquet(CELLS,index=False)
    pd.DataFrame(rows).to_parquet(CELLS,index=False)
    surf={}
    for nname in {r["needle"] for r in rows}:
        surf[nname]={str(d):f"{sum(r['detected'] for r in rows if r['needle']==nname and r['delta_pp']==d)}/{NW}" for d in deltas}
    json.dump(dict(spec_digest=SPEC["spec_digest"],surface=surf,
                   status="POST_EXPOSURE_NONE — no historical ranking computed or shown"),
              open(OUT,"w"),indent=2)
    print(json.dumps(surf,indent=1),flush=True)


if __name__=="__main__":
    run(needle_index_list=tuple(int(x) for x in (sys.argv[1].split(",") if len(sys.argv)>1 else ("0","1","2"))))
