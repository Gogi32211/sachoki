"""T5_CAPABILITY_Q50_V1 runner — median-support empirical-noise capability.

Lives in the repo, not in /tmp. The first launcher was written to /tmp and vanished with it,
which is the same failure as the grammar survivors: something needed for reuse placed
somewhere transient.

Machine reads mfe_10d. Stdout carries detection counts for the frozen q50 needle only —
no claim ranking, no observed historical theta, nothing about which sequences look good.

Parallel over worlds. Worlds are independent and every RNG stream is keyed
[seed, needle_index, world_index, perm_index], so the worker count cannot move a number.
"""
from __future__ import annotations
import hashlib, json, multiprocessing as mp, os, sys, time
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
os.chdir(HERE); sys.path.insert(0, HERE)
import t5_capability_run as R                                        # noqa: E402

SPEC = json.load(open("T5_CAPABILITY_Q50_V1.json"))
CELLS = os.path.join(os.path.dirname(HERE), "data", "t5_capability_cells.parquet")
OUT = os.path.join(HERE, "T5_CAPABILITY_Q50_RESULT.json")
NEEDLE_IDX = 1                                    # q50 in the sealed needle ordering
DELTAS = SPEC["delta_grid_pp"]; NW = SPEC["worlds"]; NP = SPEC["n_perm_inner"]

_ST = None


def _init():
    global _ST
    _ST = R.build_state(verbose=False)


def _world(wi: int):
    y, b, nb, S, claims, order = _ST
    col = int(np.flatnonzero(order.j.to_numpy() == SPEC["needle"]["sealed_j"])[0])
    memb = S[col]
    bo = np.argsort(b, kind="stable"); bs = b[bo]
    st = np.r_[0, np.flatnonzero(bs[1:] != bs[:-1]) + 1, len(bs)]
    base = R.perm_fast(y, bo, st, np.random.default_rng([20260820, NEEDLE_IDX, wi]))
    Ys = {d: base + d * memb for d in DELTAS}
    th = {d: float(R.theta_all(Ys[d], claims)[col]) for d in DELTAS}
    mx = {d: np.empty(NP) for d in DELTAS}
    for p in range(NP):
        pm = R.perm_fast(np.arange(len(y), dtype=float), bo, st,
                         np.random.default_rng([20260821, NEEDLE_IDX, wi, p])).astype(int)
        for d in DELTAS:                       # ONE mapping, SEPARATE band per delta
            mx[d][p] = R.theta_all(Ys[d][pm], claims).max()
    out = []
    for d in DELTAS:
        b95 = float(np.percentile(mx[d], 95))
        out.append(dict(needle="q50", delta_pp=d, world_id=wi,
                        needle_theta=round(th[d], 4), band_p95=round(b95, 4),
                        detected=bool(th[d] > b95),
                        inner_max_null_hash=hashlib.sha256(mx[d].tobytes()).hexdigest()[:16]))
    return out


def main(workers: int = 8):
    t0 = time.time()
    print(f"  {SPEC['spec_id']} {SPEC['spec_digest']} · needle {SPEC['needle_claim']} "
          f"(support {SPEC['needle_support']}) · k={SPEC['k']}", flush=True)
    rows = []
    if os.path.exists(CELLS):
        rows = pd.read_parquet(CELLS).to_dict("records")
        done = {r["world_id"] for r in rows}
        print(f"  resuming · {len(done)} worlds already on disk", flush=True)
    else:
        done = set()
    todo = [w for w in range(NW) if w not in done]
    with mp.get_context("fork").Pool(workers, initializer=_init) as pool:
        for res in pool.imap_unordered(_world, todo):
            rows += res
            pd.DataFrame(rows).to_parquet(CELLS, index=False)
            n = len({r["world_id"] for r in rows})
            det = {d: sum(r["detected"] for r in rows if r["delta_pp"] == d) for d in DELTAS}
            print(f"  {n}/{NW} worlds · " + " ".join(f"d{d}={det[d]}" for d in DELTAS)
                  + f" · {(time.time()-t0)/60:.0f}m", flush=True)
    surf = {str(d): f"{sum(r['detected'] for r in rows if r['delta_pp']==d)}/{NW}"
            for d in DELTAS}
    json.dump(dict(spec_id=SPEC["spec_id"], spec_digest=SPEC["spec_digest"],
                   needle=SPEC["needle_claim"], needle_support=SPEC["needle_support"],
                   k=SPEC["k"], n_perm=NP, worlds=NW, surface=surf,
                   scope=SPEC["verdict_scope"], forbidden_claim=SPEC["forbidden_claim"],
                   deferred=SPEC["deferred"],
                   historical_ranking="NOT COMPUTED, NOT EXPOSED",
                   seconds=round(time.time() - t0, 1)), open(OUT, "w"), indent=2)
    print("\n  DETECTION SURFACE (q50, treated n=682)", flush=True)
    for d in DELTAS:
        print(f"    δ {d:>+4.1f} pp   {surf[str(d)]}", flush=True)
    print(f"\n  WROTE {OUT} · {(time.time()-t0)/60:.0f} min", flush=True)


if __name__ == "__main__":
    main(workers=int(sys.argv[1]) if len(sys.argv) > 1 else 8)
