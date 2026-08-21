"""T5_15M_SURVIVOR_MAGNITUDE_SIGN_AUDIT_V1 — how many survivors have a non-positive theta.

POST_EXPOSURE DESCRIPTIVE. Promotion is FROZEN and UNCHANGED. No new search, no new
clustering: the 171 cluster labels are read from the frozen T5_15M_SURVIVOR_OVERLAP_V1.

WHY A POSITIVE Z WITH A NEGATIVE THETA IS NOT A CONTRADICTION

Z_rank asks how often treated observations sit above control observations WITHIN blocks.
theta asks what the weighted average of the per-block median differences is, in percentage
points. A claim whose treated bars are slightly above control in most blocks, while a few
blocks carry a large negative median difference, can be positive on the first and negative on
the second:

    distributional ordering  !=  median-shift magnitude

That separation is exactly why V2 was chosen — a robust evidence statistic for selection, raw
theta kept as the economic magnitude. This audit measures how often the two disagree in SIGN.

ZERO IS A TOLERANCE, NOT AN EQUALITY

theta is a float64 weighted average of medians, so exact zero is not a meaningful test.
NEGATIVE is theta < -1e-12, ZERO is |theta| <= 1e-12, POSITIVE is theta > +1e-12. The
tolerance is recorded in the artifact rather than left implicit.
"""
from __future__ import annotations
import json, os, sys, time                                            # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                # noqa: E402

EXP  = json.load(open("T5_15M_HISTORICAL_EXPOSED.json"))
OVL  = json.load(open("T5_15M_SURVIVOR_OVERLAP_V1.json"))
DATA = os.path.join(D.ROOT, "data")
SURV = os.path.join(DATA, "t5_15m_survivors.parquet")
OUT  = "T5_15M_SURVIVOR_MAGNITUDE_SIGN_AUDIT_V1.json"
CLOUT = os.path.join(DATA, "t5_15m_sign_by_cluster.parquet")
TOL = 1e-12


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    S = pd.read_parquet(SURV)
    if len(S) != EXP["survivors"]:
        raise RuntimeError(f"survivor table {len(S)} != exposed {EXP['survivors']}")
    if "cluster" not in S.columns:
        raise RuntimeError("cluster labels missing — run the overlap audit first")
    if int(S.cluster.nunique()) != OVL["headline"]["n_overlap_clusters"]:
        raise RuntimeError("cluster labels do not match the frozen overlap audit")
    if S.theta_raw_obs.isna().any():
        raise RuntimeError("theta missing for some survivors")

    th = S.theta_raw_obs.to_numpy()
    n = len(th)
    neg = th < -TOL
    zer = np.abs(th) <= TOL
    pos = th > TOL
    print(f"T5_15M_SURVIVOR_MAGNITUDE_SIGN_AUDIT_V1 · POST_EXPOSURE DESCRIPTIVE · "
          f"{n} survivors · {OVL['headline']['n_overlap_clusters']} frozen clusters",
          flush=True)

    print(f"\n  SIGN CENSUS   tolerance |θ| <= {TOL:g} counts as zero")
    print(f"    θ > 0     {int(pos.sum()):>5}   {pos.mean():>6.1%}")
    print(f"    θ = 0     {int(zer.sum()):>5}   {zer.mean():>6.1%}")
    print(f"    θ < 0     {int(neg.sum()):>5}   {neg.mean():>6.1%}")

    qs = [0, 10, 25, 50, 75, 90, 100]
    qv = {f"p{q}": round(float(np.percentile(th, q)), 4) for q in qs}
    print(f"\n  θ QUANTILES (pp)   " + "  ".join(f"{k} {v}" for k, v in qv.items()))

    negblk = dict(n=int(neg.sum()))
    if neg.any():
        Sn = S[neg]
        negblk.update(theta_min=round(float(Sn.theta_raw_obs.min()), 4),
                      theta_max=round(float(Sn.theta_raw_obs.max()), 4),
                      max_Z_rank=round(float(Sn.Z_rank_obs.max()), 4),
                      min_Z_rank=round(float(Sn.Z_rank_obs.min()), 4),
                      median_support=int(Sn.support.median()),
                      support_min=int(Sn.support.min()),
                      support_max=int(Sn.support.max()))
        print(f"\n  NEGATIVE-θ SURVIVORS")
        print(f"    n {negblk['n']} · θ {negblk['theta_min']} … {negblk['theta_max']} pp")
        print(f"    Z_rank {negblk['min_Z_rank']} … {negblk['max_Z_rank']}")
        print(f"    support median {negblk['median_support']:,} "
              f"({negblk['support_min']:,} … {negblk['support_max']:,})")

    # ── concentration in the FROZEN clusters ───────────────────────────────
    S = S.assign(_neg=neg, _zer=zer)
    C = (S.groupby("cluster")
          .agg(n_survivors=("j", "size"), n_theta_negative=("_neg", "sum"),
               n_theta_zero=("_zer", "sum"),
               theta_min=("theta_raw_obs", "min"), theta_median=("theta_raw_obs", "median"),
               Z_min=("Z_rank_obs", "min"), Z_max=("Z_rank_obs", "max"))
          .reset_index())
    C["share_theta_negative"] = (C.n_theta_negative / C.n_survivors).round(4)
    C = C.round(4).sort_values(["n_theta_negative", "n_survivors"], ascending=False)
    C.to_parquet(CLOUT, index=False)

    ncl_with_neg = int((C.n_theta_negative > 0).sum())
    top = C.head(10)
    covered = int(top.n_theta_negative.head(3).sum())
    print(f"\n  CONCENTRATION — frozen 171-cluster assignment, no re-clustering")
    print(f"    clusters containing at least one negative-θ survivor: "
          f"{ncl_with_neg} of {int(S.cluster.nunique())}")
    if neg.any():
        print(f"    the 3 largest contributors hold {covered} of {int(neg.sum())} "
              f"({covered/max(int(neg.sum()),1):.1%})")
    print(f"    {'cl':>4} {'n':>5} {'neg':>5} {'share':>7} {'θ min':>8} {'θ med':>8} "
          f"{'Z range':>15}")
    for r in top.itertuples():
        print(f"    {int(r.cluster):>4} {int(r.n_survivors):>5} {int(r.n_theta_negative):>5} "
              f"{r.share_theta_negative:>7.3f} {r.theta_min:>8.3f} {r.theta_median:>8.3f} "
              f"{r.Z_min:>6.2f}–{r.Z_max:<8.2f}")

    payload = dict(
        spec_id="T5_15M_SURVIVOR_MAGNITUDE_SIGN_AUDIT_V1",
        status="POST_EXPOSURE_DESCRIPTIVE",
        promotion="FROZEN / UNCHANGED",
        new_search="NONE", new_clustering="NONE — cluster labels read from "
                                          "T5_15M_SURVIVOR_OVERLAP_V1",
        overlap_audit_digest=ART.file_digest("T5_15M_SURVIVOR_OVERLAP_V1.json"),
        exposed_digest=ART.file_digest("T5_15M_HISTORICAL_EXPOSED.json"),
        survivors=n, clusters=int(S.cluster.nunique()),
        tolerance=dict(rule="NEGATIVE θ < -1e-12 · ZERO |θ| <= 1e-12 · POSITIVE θ > +1e-12",
                       value=TOL,
                       why="theta is a float64 weighted average of medians, so exact equality "
                           "to zero is not a meaningful test"),
        census=dict(positive=int(pos.sum()), zero=int(zer.sum()), negative=int(neg.sum()),
                    share_negative=round(float(neg.mean()), 4)),
        theta_quantiles_pp=qv,
        negative_theta=negblk,
        clusters_with_negative=ncl_with_neg,
        by_cluster=C.head(40).to_dict("records"),
        by_cluster_artifact=os.path.basename(CLOUT),
        why_not_a_contradiction="Z_rank measures within-block ordering; theta measures the "
                                "weighted average of per-block median differences. A claim "
                                "slightly above control in most blocks, with a few blocks "
                                "carrying a large negative median difference, is positive on "
                                "the first and negative on the second. Distributional "
                                "ordering is not median-shift magnitude — which is exactly "
                                "why V2 separates evidence from magnitude.",
        reporting_consequence=dict(
            wrong="733 positive MFE effects survived",
            right="733 claims showed a positive rank-based historical association surviving "
                  "search-wide multiplicity control. Their raw median-shift magnitudes are "
                  "heterogeneous, including sign-discordant cases.",
            negative_theta_survivors="statistical ordering evidence without a positive raw "
                                     "median-shift magnitude; not automatically bad claims, "
                                     "but the economic reading is clearly weaker"),
        not_done=["terminal return", "MAE", "ATR normalization",
                  "volatility / liquidity decomposition",
                  "pattern search inside the negative-θ survivors"],
        minutes=round((time.time() - t0) / 60, 1))
    dig = ART.seal(payload, OUT, required=("spec_id", "census", "tolerance"))
    print(f"\n  WROTE {OUT} · {dig}\n        {CLOUT}")


if __name__ == "__main__":
    main()
