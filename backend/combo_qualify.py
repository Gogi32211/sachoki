"""M2.5 · Gate C — what the search-wide band COSTS, measured without opening Y.

The question M4 cannot be planned without: a max-band over the frozen universe needs

    121 permutations × k_selectable θ computations

and nobody has ever run this estimator on more than 37 claims. Benchmarking it on the real
`ret` would spend the one thing that cannot be un-spent — an exposure of the historical
outcome to a search space that is not yet qualified — for information that has nothing to
do with the outcome's VALUES. So the cost is measured on a synthetic world instead.

WHY composition_world AND NOT A PLAIN RNG

`combolab_v2.composition_world` is the REGISTERED negative world: Y = μ_setup + γ_date + ε,
with the cell over-represented in the strong setups and δ_incremental = 0 by construction.
Drawing plain noise would measure the same wall-clock and prove nothing else. Drawing from
the registered world means the same run is also the v2 regression test carried up to the new
universe — v1's marginal estimand is entitled to see an association there, and v2's
incremental one is required to return θ ≈ 0. Cost and negative control for one execution.

WHAT THIS DOES NOT ESTABLISH, stated because the distinction is the whole point

    G1 null GENERATOR       previously qualified, reused unchanged
    4,7xx-claim MAX-BAND    NOT QUALIFIED. FWER 0.065 was measured on 31 claims and is a
                            property of that space, not of this one.

Calibrating a band for the new universe is a separate, larger run — many worlds, each with
its own permutation band — and it belongs to M5. This file sizes it; it does not do it.
"""
from __future__ import annotations

import json
import os
import resource
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_tokens_spec as TS                                       # noqa: E402
import combo_universe as CU                                          # noqa: E402
import combolab_v2 as V2I                                            # noqa: E402
import combolab_v2_spec as V2                                        # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "COMBO_COST_QUALIFICATION.json")
N_PERM = 121                     # 120 permutations + the observed statistic
SAMPLES = (50, 100, 200, 400)    # claim counts to measure the scaling on


def _rss_gb() -> float:
    r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return r / 1e9 if sys.platform == "darwin" else r / 1e6


def support_bytes(sup) -> int:
    """The footprint of the Support object itself, counted rather than inferred.

    Peak RSS cannot answer this: ru_maxrss is monotone, so a structure that grows and is
    freed each round leaves the high-water mark from whatever allocated most — here the
    population frame, ~1.8 GB before a single claim exists. Measured that way the answer
    reads as a flat 1.80 GB at every n, which is not "memory is free", it is "the
    instrument has no resolution at this scale".

    Support stores TWO explicit index arrays per (claim × eligible stratum): the treated
    arm and the control arm. The control arm is nearly a whole stratum, so the total is
    about one int64 per population row per claim, and that is what actually scales.
    """
    tot = 0
    for cid, st in sup.strata.items():
        for _, i, j in st:
            tot += int(i.nbytes) + int(j.nbytes)
    for w in sup.weights.values():
        tot += int(w.nbytes)
    return tot


def build_masks(M, ids, universe) -> dict:
    """Rebuild the frozen universe's masks from the recorded token names."""
    col = {t: i for i, t in enumerate(ids)}
    masks = {}
    for pair in universe["surviving_depth2"]:
        a, b = pair
        masks["+".join(pair)] = (M[:, col[a]] & M[:, col[b]]).astype(bool)
    return masks


def main():
    t0 = time.time()
    uni = json.load(open(CU.OUT_JSON))
    P, M, fam, dates, ids, pop_hash = CU.load_population()
    if pop_hash != uni.get("population_hash"):
        raise RuntimeError(f"population moved: {pop_hash} vs {uni.get('population_hash')}. "
                           f"A cost measured on a different row set is not this universe's "
                           f"cost.")
    masks_all = build_masks(M, ids, uni)
    order = sorted(masks_all)                     # deterministic, name-sorted
    print(f"\n  universe {len(order):,} depth-2 claims · population {len(P):,} rows",
          flush=True)

    O = P[["family"]].copy()
    d = P["sig_date"].to_numpy()

    rows = []
    for n in SAMPLES:
        if n > len(order):
            break
        sub = {k: masks_all[k] for k in order[:n]}
        t = time.time()
        sup = V2I.Support(O, d, sub, verbose=False)
        t_sup = time.time() - t

        y = V2I.composition_world(O, d, seed=20260816, noise=True)
        t = time.time()
        th = sup.theta(y)
        t_theta = time.time() - t

        sb = support_bytes(sup)
        rows.append(dict(n_claims=n, selectable=len(sup.cells),
                         support_build_s=round(t_sup, 2),
                         theta_once_s=round(t_theta, 3),
                         support_gb=round(sb / 1e9, 3),
                         peak_rss_gb=round(_rss_gb(), 2),
                         theta_abs_median=round(float(th.abs().median()), 4),
                         theta_abs_max=round(float(th.abs().max()), 4)))
        print(f"    {n:>4} claims · support {t_sup:>6.1f}s · θ {t_theta:>6.2f}s · "
              f"Support {sb/1e9:>5.2f} GB · |θ| med {th.abs().median():.3f} "
              f"max {th.abs().max():.3f}", flush=True)
        del sup

    K = uni["universe"]["claim_equivalence"]["distinct_claims"]
    last, first = rows[-1], rows[0]
    # SLOPE, not ratio. Dividing a total by n charges every claim a share of the fixed
    # cost — the token matrix and the population are ~1.5 GB before a single claim
    # exists, and at n=50 that alone would read as 30 MB per claim. Peak RSS in
    # particular is monotone (ru_maxrss never falls), so only the increment between two
    # sample points is attributable to the claims.
    dn = max(last["n_claims"] - first["n_claims"], 1)
    per_claim_sup = (last["support_build_s"] - first["support_build_s"]) / dn
    per_claim_theta = (last["theta_once_s"] - first["theta_once_s"]) / dn
    per_claim_rss = (last["support_gb"] - first["support_gb"]) / dn
    base_rss = first["peak_rss_gb"] - first["support_gb"]

    est = dict(k_selectable=K,
               support_build_s=round(per_claim_sup * K, 1),
               band_121_perm_s=round(per_claim_theta * K * N_PERM, 1),
               band_121_perm_min=round(per_claim_theta * K * N_PERM / 60, 1),
               peak_rss_gb=round(base_rss + per_claim_rss * K, 1),
               fixed_rss_gb=round(base_rss, 2),
               per_claim_rss_mb=round(per_claim_rss * 1000, 2),
               basis=f"slope between n={first['n_claims']} and n={last['n_claims']}")

    print(f"\n  EXTRAPOLATION to k_selectable = {K:,}", flush=True)
    print(f"    Support build      {est['support_build_s']:>9,.0f} s", flush=True)
    print(f"    121-perm band      {est['band_121_perm_s']:>9,.0f} s "
          f"({est['band_121_perm_min']:.0f} min)", flush=True)
    print(f"    peak RSS           {est['peak_rss_gb']:>9,.1f} GB", flush=True)
    if est["peak_rss_gb"] > 16:
        print(f"\n    ⚠ Support stores an explicit index array for BOTH arms of every "
              f"(claim × stratum). The control arm is nearly a whole stratum, so memory "
              f"grows as k × n_rows and the structure that was 86 MB at 37 claims does "
              f"not fit at {K:,}. M5 needs a packed-mask Support before the band can run.",
              flush=True)

    out = dict(population_hash=pop_hash, registry_digest=TS.digest(),
               k_selectable=K, n_perm=N_PERM, samples=rows, extrapolation=est,
               world="composition_only_negative (δ=0)",
               null_generator="G1 · REUSED, previously qualified on 31 claims",
               band_status="NOT QUALIFIED for this universe — FWER 0.065 is a property "
                           "of the 31-claim space, not of this one",
               outcome_exposure="NONE — synthetic Y only",
               seconds=round(time.time() - t0, 1))
    with open(OUT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\n  WROTE {OUT}\n  {out['seconds']}s · NO HISTORICAL OUTCOME TOUCHED",
          flush=True)


if __name__ == "__main__":
    main()
