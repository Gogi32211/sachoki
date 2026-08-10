"""The needle test — can the search layer find an effect whose location it was never told?

Estimator validation asked whether the statistic recovers a known δ when handed the right
sample. Decision validation asked whether governance classifies a known δ correctly. Both
passed. Neither answers the question that stands between them and a real study:

    if a real effect is hidden among 46 competing candidates, does the search bring it up?

Everything about the experiment was frozen in `combolab_spec.py` and committed before
`combo_lab.py` had a line in it. This file only executes it.

    membership frozen  →  needle_location picks a cell  →  δ added to that cell's outcomes
                       →  ComboLab sees ordinary research objects

Three things this harness refuses to do, each because the alternative is a way of scoring
better than the truth:

 1  It does not count a promoted NEIGHBOUR of the needle as a false discovery. Cells overlap by
    construction, injection adds δ to every row of the needle cell, and a cell drawing 40% of
    its rows from there moves for an honest reason. Only UNRELATED promotions — exposure below
    τ = 0.20 — count against the search.

 2  It does not report recall at δ = 0. There is no needle there. A pseudo-location landing at
    rank 1 by chance would manufacture a meaningless "recall under null"; δ = 0 measures false
    promotion and nothing else.

 3  It does not score a legitimate REJECT:VALIDITY as a search failure. RANK, PROMOTED and
    FINAL_ACCEPTED are counted separately, so a cell that ranks first, survives multiplicity,
    and is then refused by governance is recorded as exactly that.

SMOKE and DEVELOPMENT only. The acceptance seeds do not exist until the freeze commit does, and
`acceptance_seeds("")` raises rather than returning a default.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_lab as CL                                              # noqa: E402
import combolab_spec as SPEC                                        # noqa: E402
from sampling_target import descriptive_metric, finite_population_subsample  # noqa: E402
from studio_verdict import Substreams                               # noqa: E402

pd.set_option("display.width", 210)


def one_replication(lab, v0, exposure, seed: int, delta: float, *, n_perm: int,
                    n_boot: int, workers: int) -> dict:
    """One (seed, δ): plant, search, score."""
    sub = Substreams(seed)
    has_needle = delta > 0
    # a location is drawn even at δ = 0 so the RNG stream stays aligned across the grid, but at
    # δ = 0 it is a pseudo-location and is never treated as true.
    pick = sub("needle_location").integers(0, len(lab.ids))
    needle = lab.ids[int(pick)] if has_needle else None
    pseudo = lab.ids[int(pick)]

    v = v0.copy()
    if has_needle:
        v[lab.masks[pseudo]] += delta      # constant shift; sub("needle_effect") reserved for shaped injections

    R, band = lab.search(v, int(sub("search").integers(1, 2**31 - 1)),
                         sub("bootstrap"), n_perm=n_perm, n_boot=n_boot, workers=workers)

    row = dict(seed=seed, delta=delta, needle=needle or "", pseudo=pseudo,
               band_p95=band["band_p95"], best_est=float(R["estimate"].iloc[0]),
               best_cell=str(R["cell_id"].iloc[0]))

    prom = R[R["promoted"]]
    kinds = [SPEC.classify_promotion(c, needle, exposure) for c in prom["cell_id"]]
    row["n_promoted"] = int(len(prom))
    # At δ = 0 there is no needle, so nothing can be related to one: every promotion counts.
    #
    # WHAT δ = 0 IS AND IS NOT. It is "no ADDED effect", not a null world: the base population
    # keeps whatever real structure it has, and rsi<35&rs clears the band on untouched data at
    # +3.84 against +3.17. So this is NOT a false-positive rate — and it is not an upper bound on
    # one either, which was the first correction I reached for and it is wrong. Nothing
    # guarantees Promotions(real H) >= Promotions(global null): a real effect adds promotions but
    # also competes for the top-5, and it can DISPLACE chance promotions rather than accompany
    # them. There is no monotonicity to lean on in either direction.
    #
    #     semantic_status  CONTAMINATED_BY_REAL_STRUCTURE
    #     not_interpretable_as  FPR, nor a bound on FPR
    #
    # The frozen field name is kept for spec compatibility. A real false-positive rate comes only
    # from the structured-null experiment, which is the next validation stage.
    row["unrelated_false_promotions"] = int(sum(
        k in ("UNRELATED_PROMOTION", "NULL_PROMOTION") for k in kinds))
    row["overlap_affected_promotions"] = int(sum(k == "OVERLAP_AFFECTED" for k in kinds))

    if has_needle:
        hit = R[R["cell_id"] == needle]
        rank = int(hit["rank"].iloc[0])
        row["true_cell_rank"] = rank
        row["rank_found"] = bool(rank <= SPEC.TOP_K)
        row["search_promoted"] = bool(hit["promoted"].iloc[0])
        row["final_accepted"] = bool(hit["verdict"].iloc[0] == "BUILD")
        row["needle_verdict"] = str(hit["verdict"].iloc[0])
        row["needle_n"] = int(hit["n"].iloc[0])
    else:
        row["true_cell_rank"] = np.nan          # undefined — no needle exists
        row["rank_found"] = row["search_promoted"] = row["final_accepted"] = np.nan
        row["needle_verdict"] = ""
        row["needle_n"] = np.nan
    return row


def summarise(D: pd.DataFrame) -> pd.DataFrame:
    """The nine preregistered outcomes and nothing else. Anything extra is exploratory."""
    out = []
    for delta, G in D.groupby("delta"):
        r = dict(delta=delta, n=len(G))
        if delta > 0:
            rk = G["true_cell_rank"].astype(float)
            r.update({
                "rank_recall": G["rank_found"].mean(),
                "search_recall": G["search_promoted"].mean(),
                "final_accept": G["final_accepted"].mean(),
                "p_rank_1": (rk == 1).mean(),
                "p_rank_le_3": (rk <= 3).mean(),
                "p_rank_le_5": (rk <= 5).mean(),
                "median_rank": rk.median(),
                "p90_rank": rk.quantile(0.90),
            })
        else:
            r.update({k: np.nan for k in ("rank_recall", "search_recall", "final_accept",
                                          "p_rank_1", "p_rank_le_3", "p_rank_le_5",
                                          "median_rank", "p90_rank")})
        r["unrelated_fp"] = G["unrelated_false_promotions"].mean()
        r["overlap_fp"] = G["overlap_affected_promotions"].mean()
        r["any_promo"] = (G["n_promoted"] > 0).mean()
        out.append(r)
    return pd.DataFrame(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--set", default="smoke", choices=("smoke", "development"))
    ap.add_argument("--n-perm", type=int, default=CL.N_PERM)
    ap.add_argument("--n-boot", type=int, default=CL.N_BOOT)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--deltas", default="")
    a = ap.parse_args()

    seeds = SPEC.SEEDS[a.set]
    deltas = ([float(x) for x in a.deltas.split(",")] if a.deltas else list(SPEC.DELTA_GRID))
    bad = [d for d in deltas if d not in SPEC.DELTA_GRID]
    if bad:
        raise SystemExit(f"δ {bad} is not in the frozen grid {SPEC.DELTA_GRID}")

    print("=" * 122, flush=True)
    print(f"  NEEDLE TEST · {a.set.upper()} · {len(seeds)} seeds × {len(deltas)} δ · "
          f"k = {SPEC.DECLARED_K} claims", flush=True)
    print(f"  spec digest {SPEC.spec_digest()[:16]}… · perms {a.n_perm} · boots {a.n_boot}",
          flush=True)
    print("=" * 122, flush=True)

    O, v0, d = CL.load_base()
    masks = CL.build_masks(O)
    lab = CL.ComboLab(O, d, masks)
    exposure = SPEC.exposure_matrix(masks)
    print(f"  exposure matrix {exposure.shape[0]}×{exposure.shape[1]} · "
          f"cells ≥τ of the median cell: "
          f"{int((exposure.to_numpy() >= SPEC.EXPOSURE_THRESHOLD).sum(1).mean())} on average",
          flush=True)

    rows, t0 = [], time.time()
    for delta in deltas:
        for i, s in enumerate(seeds):
            rows.append(one_replication(lab, v0, exposure, int(s), delta,
                                        n_perm=a.n_perm, n_boot=a.n_boot, workers=a.workers))
            if i == 0 or (i + 1) % 10 == 0:
                print(f"    δ={delta:<5.2f} {i+1:>3d}/{len(seeds)}  "
                      f"({time.time()-t0:.0f}s)", flush=True)
    D = pd.DataFrame(rows)
    D.to_csv(f"needle_{a.set}.csv", index=False)

    S = summarise(D)
    print("\n" + "=" * 122, flush=True)
    print("  OUTCOMES — the nine preregistered, in the preregistered names", flush=True)
    print("=" * 122, flush=True)
    print(S.to_string(index=False, float_format=lambda x: f"{x:.3f}"), flush=True)

    # the sampling contract applies to our own test, not only to other people's code
    tgt = finite_population_subsample(0.999, "realized_history_2021_2026")
    for _, r in S.iterrows():
        if r["delta"] > 0:
            m = descriptive_metric("conditional_rank_recall", float(r["rank_recall"]),
                                   target=tgt, n_replications=int(r["n"]))
            print(f"    δ={r['delta']:.2f}  {m}", flush=True)
    print("\n  These are conditional detection rates on one realised history. sampling_target "
          "refuses\n  to let them be called power, and they are not.", flush=True)
    print("\nDONE", flush=True)


if __name__ == "__main__":
    main()
