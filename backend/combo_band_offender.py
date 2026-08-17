"""POST-M4 · who sets the band. Identifying the claim that taxes the whole search.

band(k) showed a hard ceiling at +4.7872 whose appearance across random subsets is predicted
exactly by ONE claim being the permutation maximum. This finds it, and asks the only question
that matters afterwards: is it recognisable from X ALONE, in advance?

If it is — if the offender is simply a claim whose theta has enormous variance under the null
because its treated arm is thin in few strata — then the entire sensitivity loss is repairable
by a support floor declared before the search, at almost no cost in breadth. That would be a
far cheaper lever than shrinking the universe, and it would be legitimate: a floor set from
NULL variance and support geometry never consults the association between tokens and outcome.

WHAT THIS MAY AND MAY NOT DO

    may     identify the claim, measure what makes it extreme, and price a support floor
            for the DESIGN OF THE NEXT INSTRUMENT
    may not be applied backwards. M4's universe was frozen and its verdict is immutable.
            Removing a claim after seeing the result and recomputing the band would be
            post-selection reuse whatever the claim's provenance.

The distinction is worth stating precisely because it is easy to blur: the offender is found
from PERMUTED outcomes, so the finding itself is not outcome-driven. That makes a floor
derived from it a legitimate prior design choice for a future run. It does not make it
retroactive.
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
import combo_band_vs_k as BK                                         # noqa: E402
import combo_capability as CAP                                       # noqa: E402
import combo_m4 as M4                                                # noqa: E402
import combo_universe as CU                                          # noqa: E402
import combolab_v2 as V2I                                            # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "COMBO_BAND_OFFENDER.json")
N_PERM, SEED = 120, 20260818          # identical to band(k), so the ceiling reproduces


def main(workers: int = 8):
    t0 = time.time()
    m4 = json.load(open(M4.OUT))
    P, M, fam, dates, ids, ph = CAP.load(verbose=False)
    S = CU.Strata(M, fam, dates, verbose=False)
    uni = json.load(open(os.path.join(HERE, "COMBO_UNIVERSE_MATURE.json")))
    classes, stats = CAP.canonical_claims(M, ids, uni, S)
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
    print(f"  Support ready · {time.time()-t0:.0f}s · {N_PERM} permutations…", flush=True)

    BK._W = dict(y=y[order], inv=inv, blocks=blocks, sup=sup, seed=SEED)
    with mp.get_context("fork").Pool(workers) as pool:
        T = np.vstack(pool.map(BK._perm_full, range(N_PERM), chunksize=2))
    cells = list(sup.cells)
    full_band = float(np.percentile(T.max(axis=1), 95))
    print(f"  theta matrix {T.shape} · band {full_band:+.4f} "
          f"(band(k) recorded {json.load(open(os.path.join(HERE,'COMBO_BAND_VS_K.json')))['band_full_universe']:+.4f})",
          flush=True)

    am = T.argmax(axis=1)
    won = pd.Series([cells[i] for i in am]).value_counts()
    print(f"\n  who is the permutation maximum, over {N_PERM} draws", flush=True)
    for cid, n in won.head(8).items():
        print(f"      {cid:<28} {n:>4} / {N_PERM}  ({n/N_PERM:>5.1%})", flush=True)

    # leave-worst-out: the band after removing the 1, 2, 5, 10 highest p95 claims
    p95 = np.percentile(T, 95, axis=0)
    worst = np.argsort(-p95)
    ladder = []
    for r in (0, 1, 2, 5, 10, 25):
        keep = np.setdiff1d(np.arange(len(cells)), worst[:r])
        b = float(np.percentile(T[:, keep].max(axis=1), 95))
        ladder.append(dict(removed=r, band=round(b, 4),
                           gain=round(full_band - b, 4)))
    print(f"\n  band after removing the r highest-variance claims", flush=True)
    for l in ladder:
        print(f"      r={l['removed']:>3}   {l['band']:+7.4f} pp   "
              f"(−{l['gain']:.3f})", flush=True)

    smap = stats.set_index("claim_id")
    rows = []
    for j in worst[:10]:
        cid = cells[j]
        s = smap.loc[cid]
        rows.append(dict(claim_id=cid, permuted_p95=round(float(p95[j]), 4),
                         permuted_sd=round(float(T[:, j].std()), 4),
                         times_max=int((am == j).sum()),
                         eligible_setups=int(s.eligible_setups),
                         support_total=int(s.support_total),
                         treated_n=int(s.treated_n),
                         treated_dates=int(s.treated_dates),
                         top_date_share=round(float(s.top_date_share), 4)))
    print(f"\n  the ten highest-variance claims, and what they look like from X", flush=True)
    print(f"      {'claim':<26} {'p95':>7} {'sd':>6} {'max':>4} {'setups':>7} "
          f"{'support':>8} {'dates':>6}", flush=True)
    for r in rows:
        print(f"      {r['claim_id']:<26} {r['permuted_p95']:>+7.3f} "
              f"{r['permuted_sd']:>6.3f} {r['times_max']:>4} {r['eligible_setups']:>7} "
              f"{r['support_total']:>8,} {r['treated_dates']:>6}", flush=True)

    # is high null variance recognisable from support alone?
    sup_tot = smap.loc[cells, "support_total"].to_numpy(float)
    n_set = smap.loc[cells, "eligible_setups"].to_numpy(float)
    rho_s = float(pd.Series(p95).corr(pd.Series(sup_tot), method="spearman"))
    rho_n = float(pd.Series(p95).corr(pd.Series(n_set), method="spearman"))
    print(f"\n  Spearman(permuted p95, support_total)   {rho_s:+.3f}", flush=True)
    print(f"  Spearman(permuted p95, eligible_setups) {rho_n:+.3f}", flush=True)

    # what a declared floor would have cost and bought
    floors = []
    for f in (5, 6, 7, 8, 10, 12):
        keep = np.flatnonzero(n_set >= f)
        if len(keep) < 100:
            continue
        b = float(np.percentile(T[:, keep].max(axis=1), 95))
        floors.append(dict(min_eligible_setups=f, k=int(len(keep)),
                           k_share=round(len(keep) / len(cells), 4),
                           band=round(b, 4), gain=round(full_band - b, 4)))
    print(f"\n  a support floor declared in advance — breadth kept vs sensitivity bought",
          flush=True)
    print(f"      {'min setups':>11} {'k':>7} {'kept':>7} {'band':>9} {'gain':>7}",
          flush=True)
    for f in floors:
        print(f"      {f['min_eligible_setups']:>11} {f['k']:>7,} {f['k_share']:>6.1%} "
              f"{f['band']:>+9.4f} {f['gain']:>7.3f}", flush=True)

    out = dict(spec="POST_M4_BAND_OFFENDER_V1", status="POST_EXPOSURE_CHARACTERIZATION",
               confirmatory_standing="NONE", m4_verdict="IMMUTABLE",
               m4_digest=m4["result_digest"], population_hash=ph,
               n_perm=N_PERM, seed=SEED, full_band=round(full_band, 4),
               argmax_counts={str(k): int(v) for k, v in won.head(12).items()},
               leave_worst_out=ladder, highest_variance_claims=rows,
               spearman_p95_support=round(rho_s, 4),
               spearman_p95_eligible_setups=round(rho_n, 4),
               support_floor_ladder=floors,
               may_not="be applied backwards. M4's universe was frozen and its verdict is "
                       "immutable; removing a claim after seeing the result and recomputing "
                       "the band is post-selection reuse whatever the claim's provenance.",
               may="inform the declared support floor of the NEXT instrument, because the "
                   "offender is identified from PERMUTED outcomes and so the finding is not "
                   "outcome-driven",
               seconds=round(time.time() - t0, 1))
    with open(OUT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\n  WROTE {OUT} · {out['seconds']/60:.0f} min", flush=True)


if __name__ == "__main__":
    main(workers=int(sys.argv[1]) if len(sys.argv) > 1 else 8)
