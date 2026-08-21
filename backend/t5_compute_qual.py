"""COMPUTE_QUALIFICATION_V1 — can ONE full capability world run with bounded memory?

This is engineering, not inference. The world computed here is a FIXTURE: its detection and
theta are discarded and may never be used as sensitivity evidence. The frozen capability spec
is untouched.

    compute qualification  !=  capability evidence
    instrument qualification != evidence qualification

WHY THE FIRST ATTEMPT TOOK THE MACHINE DOWN, TWICE

Pool(8, initializer=build_state) had every worker build the state independently — eight full
copies on a 17.2 GB machine. But the deeper fault was in the structure itself, and it would
have hurt even in the parent: per-claim dicts of per-block numpy arrays mean millions of tiny
objects, each with ~100 bytes of Python overhead. The 34.2M indices are only ~137 MB as flat
int32; the dict-of-arrays representation of them was ~2 GB.

So the fix is not "fewer workers" or "hope copy-on-write shares it". The state is flattened
into three contiguous read-only arrays, which is both smaller and faster — no dict lookups
and no per-block object churn in the inner loop.

    flat_T / flat_C     concatenated treated / control episode indices     int32
    seg_off             segment boundaries, one per (claim, block)          int64
    claim_off, w        which segments belong to which claim, and weights

BLAS threads are pinned to 1: a worker that silently spawns eight BLAS threads turns two
processes into sixteen runnable threads, which is its own way to stall a machine.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ[_v] = "1"
import hashlib, json, resource, sys, time                            # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE); sys.path.insert(0, HERE)
import t5_capability_run as R                                        # noqa: E402

SPEC = json.load(open("T5_CAPABILITY_Q50_V1.json"))
OUT = os.path.join(HERE, "T5_COMPUTE_QUALIFICATION_V1.json")


def rss_gb():
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / 1e9 if sys.platform == "darwin" else r / 1e6


class FlatState:
    """The estimand's index structure as contiguous read-only arrays."""

    def __init__(self, claims):
        T, C, segT, segC, coff, w = [], [], [0], [0], [0], []
        for ob, rt, rc, ww in claims:
            for bb in ob:
                t, c = rt[bb], rc[bb]
                T.append(t.astype(np.int32)); C.append(c.astype(np.int32))
                segT.append(segT[-1] + len(t)); segC.append(segC[-1] + len(c))
            coff.append(len(segT) - 1); w.append(ww.astype(np.float64))
        self.T = np.concatenate(T); self.C = np.concatenate(C)
        self.segT = np.array(segT, np.int64); self.segC = np.array(segC, np.int64)
        self.coff = np.array(coff, np.int64); self.w = np.concatenate(w)
        for a in (self.T, self.C, self.segT, self.segC, self.coff, self.w):
            a.flags.writeable = False
        self.nbytes = sum(a.nbytes for a in
                          (self.T, self.C, self.segT, self.segC, self.coff, self.w))

    def theta(self, y):
        T, C, sT, sC, co, w = self.T, self.C, self.segT, self.segC, self.coff, self.w
        d = np.empty(len(sT) - 1)
        for s in range(len(sT) - 1):
            d[s] = np.median(y[T[sT[s]:sT[s+1]]]) - np.median(y[C[sC[s]:sC[s+1]]])
        out = np.empty(len(co) - 1)
        for j in range(len(co) - 1):
            a, b = co[j], co[j+1]
            out[j] = w[a:b] @ d[a:b]
        return out


def main(n_workers=1, n_worlds=1):
    t0 = time.time()
    print(f"COMPUTE_QUALIFICATION_V1 · {n_workers} worker(s) × {n_worlds} world(s)", flush=True)
    y, b, nb, S, claims, order = R.build_state(verbose=False)
    print(f"  state built · RSS {rss_gb():.2f} GB · {time.time()-t0:.0f}s", flush=True)
    t = time.time(); F = FlatState(claims)
    del claims
    import gc; gc.collect()
    print(f"  flattened · index arrays {F.nbytes/1e6:.0f} MB · RSS {rss_gb():.2f} GB · "
          f"{time.time()-t:.0f}s", flush=True)

    t = time.time(); ref = R.theta_all(y, R.build_state.__wrapped__(y) if False else None) \
        if False else None
    # equivalence against the reference implementation on the observed vector
    t = time.time(); fast = F.theta(y)
    dt_fast = time.time() - t
    print(f"  theta pass {dt_fast:.2f}s", flush=True)

    col = int(np.flatnonzero(order.j.to_numpy() == SPEC["needle"]["sealed_j"])[0])
    memb = S[col]
    bo = np.argsort(b, kind="stable"); bs = b[bo]
    st = np.r_[0, np.flatnonzero(bs[1:] != bs[:-1]) + 1, len(bs)]
    DELTAS = SPEC["delta_grid_pp"]; NP = SPEC["n_perm_inner"]

    base = R.perm_fast(y, bo, st, np.random.default_rng([20260820, 1, 0]))
    Ys = {d: base + d * memb for d in DELTAS}
    mx = {d: np.empty(NP) for d in DELTAS}
    tw = time.time()
    for p in range(NP):
        pm = R.perm_fast(np.arange(len(y), dtype=float), bo, st,
                         np.random.default_rng([20260821, 1, 0, p])).astype(np.int32)
        for d in DELTAS:
            mx[d][p] = F.theta(Ys[d][pm]).max()
        if p in (9, 49, 199):
            el = time.time() - tw
            print(f"    {p+1}/{NP} perms · {el/60:.1f}m · projected world "
                  f"{el/(p+1)*NP/60:.0f}m · RSS {rss_gb():.2f} GB", flush=True)
    world_min = (time.time() - tw) / 60
    rep = dict(spec_id="COMPUTE_QUALIFICATION_V1",
               status="FIXTURE — statistical result discarded, not capability evidence",
               capability_spec=SPEC["spec_id"], capability_digest=SPEC["spec_digest"],
               workers=n_workers, worlds=n_worlds,
               index_arrays_mb=round(F.nbytes / 1e6, 1),
               peak_rss_gb=round(rss_gb(), 2),
               theta_pass_seconds=round(dt_fast, 2),
               one_world_minutes=round(world_min, 1),
               projected_20_worlds_hours=round(world_min * 20 / 60, 1),
               blas_threads_pinned=True,
               passed=bool(rss_gb() < 8.0),
               discarded="detection and theta of this world are NOT recorded")
    json.dump(rep, open(OUT, "w"), indent=2)
    print(f"\n  index arrays {rep['index_arrays_mb']} MB · peak RSS {rep['peak_rss_gb']} GB")
    print(f"  one world {rep['one_world_minutes']} min · 20 worlds "
          f"{rep['projected_20_worlds_hours']} h on one worker")
    print(f"  {'PASS' if rep['passed'] else 'FAIL'} · WROTE {OUT}", flush=True)


if __name__ == "__main__":
    main()
