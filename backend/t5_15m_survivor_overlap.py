"""T5_15M_SURVIVOR_OVERLAP_V1 — how much of the 733 is one thing wearing many names.

POST_EXPOSURE DESCRIPTIVE. Promotion is FROZEN and UNCHANGED. No new hypothesis is searched,
no threshold is reconsidered, no subgroup is hunted inside the survivors.

WHAT IS AND IS NOT BEING SAID

    "733 discoveries collapsed to X discoveries"        WRONG
    "733 surviving claims form X overlap clusters at
     descriptive Jaccard >= 0.50"                       RIGHT

The clusters do not revise the test. Every one of the 733 cleared the registered search-wide
band on its own; clustering only describes how much episode geometry they share.

THE THRESHOLD IS BORROWED, NOT REGISTERED

Jaccard >= 0.50 is the convention used on the 1H survivors, reused here so the two resolutions
are comparable. It was NOT preregistered for 15m and carries no inferential weight. Because
of that, the numbers that do not depend on it — nearest-neighbour Jaccard and exact
containment counts — are reported alongside the cluster count rather than under it.
"""
from __future__ import annotations
import os, sys
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"          # descriptive matmul only; sums are exact in float32
import json, time                                                     # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                # noqa: E402
import t5_15m_par_qualify as PQ                                       # noqa: E402

EXP  = json.load(open("T5_15M_HISTORICAL_EXPOSED.json"))
DATA = os.path.join(D.ROOT, "data")
SURV = os.path.join(DATA, "t5_15m_survivors.parquet")
OUT  = "T5_15M_SURVIVOR_OVERLAP_V1.json"
PAIR = os.path.join(DATA, "t5_15m_survivor_pairs.parquet")
CLUS = os.path.join(DATA, "t5_15m_survivor_clusters.parquet")
J_THR = 0.50


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    S = pd.read_parquet(SURV).reset_index(drop=True)
    n = len(S)
    if n != EXP["survivors"]:
        raise RuntimeError(f"survivor table {n} != exposed count {EXP['survivors']}")
    print(f"T5_15M_SURVIVOR_OVERLAP_V1 · POST_EXPOSURE DESCRIPTIVE · {n} survivors", flush=True)

    z = np.load(PQ.BUNDLE)
    eidx = z["eidx"]; seg_ptr = z["seg_ptr"]; csp = z["claim_seg_ptr"]
    nE = int(z["nE"][0])

    # membership over the block-sorted population, exactly the vectors the test used
    M = np.zeros((n, nE), np.float32)
    for r, j in enumerate(S.j.to_numpy()):
        a, b = csp[j], csp[j + 1]
        M[r, eidx[seg_ptr[a]:seg_ptr[b]]] = 1.0
    sz = M.sum(1).astype(np.int64)
    if not np.array_equal(sz, S.support.to_numpy()):
        raise RuntimeError("rebuilt membership sizes disagree with the sealed support")
    print(f"  membership rebuilt · {M.nbytes/1e6:.0f} MB · sizes match the sealed support",
          flush=True)

    inter = (M @ M.T).astype(np.int64)          # sums <= 112,621 < 2^24 → exact in float32
    union = sz[:, None] + sz[None, :] - inter
    jac = inter / union
    np.fill_diagonal(jac, 0.0)

    # containment: rows i fully inside j (and strictly smaller)
    contained = (inter == sz[:, None]) & (sz[:, None] < sz[None, :])
    n_contain = int(contained.sum())
    nn = jac.max(axis=1)

    # single linkage at the borrowed threshold
    lab = np.arange(n)
    ii, jj = np.where(np.triu(jac >= J_THR, 1))
    for a, b in zip(ii, jj):
        ra, rb = lab[a], lab[b]
        if ra != rb:
            lab[lab == rb] = ra
    _, lab = np.unique(lab, return_inverse=True)
    nclus = int(lab.max() + 1)
    sizes = np.bincount(lab)

    print(f"\n  1 · overlap clusters at descriptive Jaccard >= {J_THR}   {nclus}")
    print(f"  2 · largest cluster                                   {sizes.max()}")
    print(f"  3 · median nearest-neighbour Jaccard                  {np.median(nn):.4f}")
    print(f"  4 · strict containment relations (i fully inside j)   {n_contain:,}")
    print(f"      claims with a neighbour at J >= {J_THR}            "
          f"{int((nn >= J_THR).sum())} of {n}")
    print(f"      nearest-neighbour Jaccard  p10 {np.percentile(nn,10):.3f} · "
          f"p50 {np.median(nn):.3f} · p90 {np.percentile(nn,90):.3f}")

    S["cluster"] = lab
    S["nn_jaccard"] = nn
    rows = []
    for c in range(nclus):
        m = lab == c
        sub = S[m].sort_values("Z_rank_obs", ascending=False)
        jj_sub = jac[np.ix_(m, m)]
        rows.append(dict(
            cluster=c, n_claims=int(m.sum()),
            representatives=" | ".join(sub.claim_id.head(3)),
            support_min=int(sub.support.min()), support_max=int(sub.support.max()),
            Z_min=round(float(sub.Z_rank_obs.min()), 3),
            Z_max=round(float(sub.Z_rank_obs.max()), 3),
            theta_min=round(float(sub.theta_raw_obs.min()), 3),
            theta_max=round(float(sub.theta_raw_obs.max()), 3),
            max_pairwise_J=round(float(jj_sub.max()) if m.sum() > 1 else 0.0, 4)))
    C = pd.DataFrame(rows).sort_values("n_claims", ascending=False).reset_index(drop=True)
    C.to_parquet(CLUS, index=False)
    S.to_parquet(SURV, index=False)

    keep = jac >= 0.10
    pi, pj = np.where(np.triu(keep, 1))
    pd.DataFrame(dict(i=S.j.to_numpy()[pi], j=S.j.to_numpy()[pj],
                      jaccard=jac[pi, pj].round(5),
                      inter=inter[pi, pj],
                      p_i_given_j=(inter[pi, pj] / sz[pj]).round(5),
                      p_j_given_i=(inter[pi, pj] / sz[pi]).round(5))).to_parquet(PAIR, index=False)

    print(f"\n  TOP CLUSTERS BY SIZE")
    print(f"    {'cl':>4} {'n':>5} {'supp range':>17} {'Z range':>15} {'θ range':>13} "
          f"{'maxJ':>6}  representatives")
    for r in C.head(12).itertuples():
        print(f"    {r.cluster:>4} {r.n_claims:>5} "
              f"{r.support_min:>7,}–{r.support_max:<9,} "
              f"{r.Z_min:>6.2f}–{r.Z_max:<7.2f} {r.theta_min:>5.2f}–{r.theta_max:<6.2f} "
              f"{r.max_pairwise_J:>6.3f}  {r.representatives[:52]}")

    singles = int((sizes == 1).sum())
    payload = dict(
        spec_id="T5_15M_SURVIVOR_OVERLAP_V1", status="POST_EXPOSURE_DESCRIPTIVE",
        promotion="FROZEN / UNCHANGED — every one of the 733 cleared the registered band on "
                  "its own; clustering describes shared episode geometry, it does not revise "
                  "the test",
        new_hypothesis_search="FORBIDDEN",
        exposed_artifact=EXP["spec_id"], exposed_digest=ART.file_digest(
            "T5_15M_HISTORICAL_EXPOSED.json"),
        survivors=n, pairs_evaluated=int(n * (n - 1) // 2),
        threshold=J_THR,
        threshold_role="POST_EXPOSURE DESCRIPTIVE; borrowed from the 1H survivor audit for "
                       "comparability; NOT a preregistered 15m inferential threshold",
        wording="733 surviving claims form N overlap clusters at descriptive Jaccard >= 0.50. "
                "NOT '733 discoveries collapsed to N discoveries'.",
        headline=dict(n_overlap_clusters=nclus,
                      largest_cluster=int(sizes.max()),
                      median_nearest_neighbour_jaccard=round(float(np.median(nn)), 4),
                      strict_containment_relations=n_contain),
        threshold_free=dict(
            nn_jaccard_p10=round(float(np.percentile(nn, 10)), 4),
            nn_jaccard_p50=round(float(np.median(nn)), 4),
            nn_jaccard_p90=round(float(np.percentile(nn, 90)), 4),
            claims_with_neighbour_above_threshold=int((nn >= J_THR).sum()),
            note="nearest-neighbour Jaccard and containment counts do not depend on the "
                 "borrowed 0.50, so they are reported beside the cluster count rather than "
                 "under it"),
        cluster_size_distribution=dict(singletons=singles,
                                       size_p50=int(np.median(sizes)),
                                       size_p90=int(np.percentile(sizes, 90)),
                                       size_max=int(sizes.max())),
        top_clusters=C.head(40).to_dict("records"),
        artifacts=dict(clusters=os.path.basename(CLUS), pairs=os.path.basename(PAIR),
                       survivors=os.path.basename(SURV)),
        pairs_stored="every pair with Jaccard >= 0.10, with both conditional probabilities",
        not_done=["new 15m token search", "3-bar grammar", "support threshold change",
                  "horizon change", "hunting subgroups inside the survivors",
                  "filtering 15m survivors by 1H winners"],
        minutes=round((time.time() - t0) / 60, 1))
    dig = ART.seal(payload, OUT, required=("spec_id", "headline", "threshold_role"))
    print(f"\n  singletons {singles} · cluster size p50 {int(np.median(sizes))} · "
          f"p90 {int(np.percentile(sizes,90))}")
    print(f"  WROTE {OUT} · {dig}\n        {CLUS}\n        {PAIR}")


if __name__ == "__main__":
    main()
