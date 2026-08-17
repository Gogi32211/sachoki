"""POST-M4 · what breadth costs. band(k) on the registered null.

The strategic conclusion of the whole exercise — that an exhaustive universe buys integrity
with sensitivity — has so far been a sentence. This turns it into a curve:

    band_p95(k)   for nested random sub-universes of the frozen 4,636 claims

M4 stored only the MAXIMUM of each permutation, which is why this could not be recovered
afterwards and needs its own run. Here every permutation keeps the full per-claim theta
vector, so the band of any subset is a slice rather than a new simulation.

WHICH k CLAIMS, AND WHY RANDOM

"A universe of 50" is not one object — its band depends on which 50 and on how correlated
they are. A single hand-picked subset would answer about that subset and be read as answering
about the size. So each k is drawn R times at random from the frozen universe, seeded and
declared, and the curve reports the median with its spread. That preserves the average
correlation structure of the real space, which is the thing that actually sets the maximum.

POST-EXPOSURE, NO NEW EXPOSURE. The permuted historical outcome is used, and it was opened by
M4. Nothing here is evidence about any claim, and nothing here may revisit M4.

THE TRAP THIS CURVE SITS NEXT TO, NAMED SO IT IS NOT WALKED INTO

It is tempting to find the k at which band(k) drops below M4's top estimate of +4.411 and
conclude that a universe of that size "would have found something". That is choosing the
search space after seeing which claim won — post-selection reuse, and it confers exactly
nothing. The curve is for designing the NEXT instrument on an untouched population or
forward, not for re-reading this one.
"""
from __future__ import annotations

import json
import multiprocessing as mp
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_capability as CAP                                       # noqa: E402
import combo_m4 as M4                                                # noqa: E402
import combo_universe as CU                                          # noqa: E402
import combolab_v2 as V2I                                            # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "COMBO_BAND_VS_K.json")

N_PERM = 120
SEED = 20260818
K_GRID = (1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 4636)
R_DRAWS = 50


_W: dict = {}


def _perm_full(p: int) -> np.ndarray:
    """One permutation, returning the FULL per-claim theta vector."""
    y = _W["y"].copy()
    rng = np.random.default_rng([_W["seed"], p])
    for a, b in _W["blocks"]:
        if b - a > 1:
            y[a:b] = rng.permutation(y[a:b])
    return _W["sup"].theta(y[_W["inv"]]).to_numpy()


def main(workers: int = 8):
    t0 = time.time()
    m4 = json.load(open(M4.OUT))
    P, M, fam, dates, ids, ph = CAP.load(verbose=False)
    if ph != m4["population_hash"]:
        raise RuntimeError("population moved")
    S = CU.Strata(M, fam, dates, verbose=False)
    uni = json.load(open(os.path.join(HERE, "COMBO_UNIVERSE_MATURE.json")))
    classes, stats = CAP.canonical_claims(M, ids, uni, S)
    k_all = len(classes)
    if k_all != m4["k_selectable"]:
        raise RuntimeError("universe moved")
    print(f"  universe {k_all:,} · population {len(P):,} · M4 {m4['result_digest']}",
          flush=True)

    masks = {c["claim_id"]: c["mask"] for c in classes.values()}
    sup = V2I.Support(P[["family"]].copy(), dates, masks, verbose=False)
    del masks
    for c in classes.values():
        c.pop("mask", None)
    y = M4.load_outcome(P)
    order = np.lexsort((dates, fam))
    inv = np.argsort(order)
    kv = (pd.Series(fam[order]).astype(str) + "|" + pd.Series(dates[order]).astype(str)
          ).to_numpy()
    cuts = np.r_[0, np.flatnonzero(kv[1:] != kv[:-1]) + 1, len(kv)]
    blocks = list(zip(cuts[:-1].tolist(), cuts[1:].tolist()))
    print(f"  Support ready · {time.time()-t0:.0f}s · running {N_PERM} permutations "
          f"keeping FULL theta vectors…", flush=True)

    global _W
    _W = dict(y=y[order], inv=inv, blocks=blocks, sup=sup, seed=SEED)
    with mp.get_context("fork").Pool(workers) as pool:
        T = np.vstack(pool.map(_perm_full, range(N_PERM), chunksize=2))   # (120, k)
    print(f"  theta matrix {T.shape} · {time.time()-t0:.0f}s", flush=True)

    rng = np.random.default_rng(SEED)
    rows = []
    for k in K_GRID:
        if k > T.shape[1]:
            continue
        bands = []
        draws = 1 if k == T.shape[1] else R_DRAWS
        for _ in range(draws):
            idx = (np.arange(T.shape[1]) if k == T.shape[1]
                   else rng.choice(T.shape[1], size=k, replace=False))
            bands.append(float(np.percentile(T[:, idx].max(axis=1), 95)))
        rows.append(dict(k=int(k), draws=draws,
                         band_median=round(float(np.median(bands)), 4),
                         band_p10=round(float(np.percentile(bands, 10)), 4),
                         band_p90=round(float(np.percentile(bands, 90)), 4)))
        print(f"    k={k:>6,}  band p95 = {rows[-1]['band_median']:+7.4f} pp   "
              f"[{rows[-1]['band_p10']:+.4f}, {rows[-1]['band_p90']:+.4f}]", flush=True)

    full = [r for r in rows if r["k"] == T.shape[1]][0]["band_median"]
    out = dict(spec="POST_M4_BAND_VS_K_V1", status="POST_EXPOSURE_CHARACTERIZATION",
               confirmatory_standing="NONE",
               m4_digest=m4["result_digest"], m4_band=m4["band_p95"],
               m4_max_theta_hat=m4["theta_max"], m4_verdict="IMMUTABLE",
               population_hash=ph, k_full=k_all, n_perm=N_PERM, seed=SEED,
               draws_per_k=R_DRAWS,
               subset_rule="uniform random without replacement from the frozen universe, "
                           "seeded; the median over draws is reported because 'a universe "
                           "of k' is a size, not a particular subset",
               null_generator=CAP.NULL_GENERATOR,
               curve=rows,
               band_full_universe=full,
               may_not="be read backwards — finding the k at which band(k) falls below "
                       "M4's top estimate and concluding a smaller search 'would have "
                       "found something' is post-selection reuse and confers nothing",
               use="designing the next instrument on an untouched population or forward",
               seconds=round(time.time() - t0, 1))
    with open(OUT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    print(f"\n{'='*70}", flush=True)
    print(f"  band_p95(k) on the registered null · {out['seconds']/60:.0f} min", flush=True)
    for r in rows:
        drop = full - r["band_median"]
        print(f"    k={r['k']:>6,}   {r['band_median']:+7.4f} pp   "
              f"(−{drop:.3f} vs the full universe)", flush=True)
    print(f"\n  WROTE {OUT}", flush=True)
    print("=" * 70, flush=True)


if __name__ == "__main__":
    main(workers=int(sys.argv[1]) if len(sys.argv) > 1 else 8)
