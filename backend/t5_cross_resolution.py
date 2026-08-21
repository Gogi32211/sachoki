"""T5_CROSS_RESOLUTION_COMPATIBILITY_V1 — do the 1H survivors localize in the 15m landscape?

POST_EXPOSURE DESCRIPTIVE. Both promotions are FROZEN and UNCHANGED. This is compatibility
and localization only. It is NOT replication, NOT independent evidence, NOT confirmation, and
it opens NO new promotion path — both resolutions read the same T5 episode universe and the
same 10-day outcome, so an agreement between them cannot be independent.

MATCHING IS ON EPISODE MEMBERSHIP, NEVER ON TOKEN NAMES

Lining up "BUY" against "BUY" or "VOL_W" against something that looks like it would be
interpretation deciding the result before the measurement. Two claims are compared only by
which EPISODES they select.

THE TWO POPULATIONS ARE NOT THE SAME SET

1H runs on 118,849 episodes, 15m on 112,621. An episode outside the 15m population cannot be
a 15m member, so raw overlap would confuse "different population" with "different membership".
Everything below is computed on U = the intersection of the two populations, and the
restriction losses are reported.

EXPECTED OVERLAP IS THE BASELINE, NOT ZERO

Two large sets intersect a lot by construction. E[|A n B|] = |A||B| / |U| under independence,
and observed/expected is what says whether a 1H cluster CONCENTRATES in a 15m medoid.

PAIRS ARE RANKED BY OVERLAP ALONE

No Z and no theta enters the pair selection or the ordering. Ranking by result would surface
the pairs that look best rather than the pairs that overlap most.
"""
from __future__ import annotations
import os, sys
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import gc, json, time                                                 # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                # noqa: E402
import t5_15m_par_qualify as PQ                                       # noqa: E402

OVL1H = json.load(open("SURVIVOR_OVERLAP_V1.json"))
OVL15 = json.load(open("T5_15M_SURVIVOR_OVERLAP_V1.json"))
DATA  = os.path.join(D.ROOT, "data")
OUT   = "T5_CROSS_RESOLUTION_COMPATIBILITY_V1.json"
PAIRS = os.path.join(DATA, "t5_cross_resolution_pairs.parquet")
BINS  = (0.10, 0.25, 0.50)


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    print("T5_CROSS_RESOLUTION_COMPATIBILITY_V1 · POST_EXPOSURE DESCRIPTIVE", flush=True)

    # ── 1H side: membership of the 9 survivors, then medoid per frozen cluster ──
    import t5_capability_run as R
    y, b, nb, S1, claims, order = R.build_state(verbose=False)
    ids1 = R.LAST_EPISODE_IDS
    ordj = order.sort_values("j").reset_index(drop=True)
    clusters1 = [list(c) for c in OVL1H["clusters"]]
    js1 = sorted({j for c in clusters1 for j in c})
    col_of = {j: int(np.flatnonzero(ordj.j.to_numpy() == j)[0]) for j in js1}
    memb1 = {j: ids1[S1[col_of[j]]] for j in js1}          # episode_id arrays
    name1 = {j: ordj.representative.iloc[col_of[j]] for j in js1}
    pop1h = np.array(ids1)                    # the FULL 1H population, kept before freeing
    del y, b, S1, claims, order, ids1
    gc.collect()
    print(f"  1H · {len(js1)} survivors in {len(clusters1)} frozen clusters", flush=True)

    def jac_sets(a, b_):
        i = len(np.intersect1d(a, b_, assume_unique=False))
        return i / (len(a) + len(b_) - i) if (len(a) + len(b_) - i) else 0.0

    reps1 = []
    for ci, cl in enumerate(clusters1):
        if len(cl) == 1:
            pick, mj = cl[0], np.nan
        else:
            mjs = [np.mean([jac_sets(memb1[j], memb1[k]) for k in cl if k != j]) for j in cl]
            m = max(mjs)
            cand = [cl[i] for i, v in enumerate(mjs) if v == m]
            pick, mj = min(cand), float(m)
        reps1.append(dict(cluster_1h=ci, n_claims=len(cl), j=pick, claim=name1[pick],
                          mean_jaccard=None if np.isnan(mj) else round(mj, 4)))
        print(f"    1H cluster {ci} (n={len(cl)}) → medoid j {pick} · {name1[pick]}", flush=True)

    # ── 15m side: the 171 frozen medoids ───────────────────────────────────
    REP = pd.read_parquet(os.path.join(DATA, "t5_15m_cluster_representatives.parquet"))
    z = np.load(PQ.BUNDLE)
    eidx = z["eidx"]; seg_ptr = z["seg_ptr"]; csp = z["claim_seg_ptr"]
    P = pd.read_parquet(os.path.join(DATA, "t5_15m_population.parquet"))
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    ids15 = P.episode_id.to_numpy()
    memb15 = {}
    for r in REP.itertuples():
        a, b_ = csp[r.j], csp[r.j + 1]
        memb15[int(r.j)] = ids15[eidx[seg_ptr[a]:seg_ptr[b_]]]

    # ── common universe = the intersection of the two POPULATIONS ──────────
    # NOT the intersection of the memberships: that would define the universe out of the very
    # sets being compared, and every Jaccard would be inflated by construction.
    U = np.intersect1d(pop1h, ids15)
    print(f"  populations · 1H {len(pop1h):,} · 15m {len(ids15):,} · "
          f"intersection {len(U):,}", flush=True)
    uidx = {e: i for i, e in enumerate(U)}
    nU = len(U)

    def to_mask(arr):
        m = np.zeros(nU, np.float32)
        k = [uidx[e] for e in arr if e in uidx]
        m[k] = 1.0
        return m, len(k), len(arr)

    A = np.zeros((len(reps1), nU), np.float32); a_sz = []; a_loss = []
    for i, r in enumerate(reps1):
        m, kept, tot = to_mask(memb1[r["j"]])
        A[i] = m; a_sz.append(kept); a_loss.append(tot - kept)
    B = np.zeros((len(REP), nU), np.float32); b_sz = []
    for i, jj in enumerate(REP.j.to_numpy()):
        m, kept, tot = to_mask(memb15[int(jj)])
        B[i] = m; b_sz.append(kept)
    a_sz = np.array(a_sz); b_sz = np.array(b_sz)
    print(f"  common universe U = {nU:,} episodes · 1H members dropped by restriction "
          f"{sum(a_loss):,}", flush=True)

    inter = (A @ B.T).astype(np.int64)
    union = a_sz[:, None] + b_sz[None, :] - inter
    jac = np.where(union > 0, inter / np.maximum(union, 1), 0.0)
    p_b_given_a = inter / np.maximum(a_sz[:, None], 1)
    p_a_given_b = inter / np.maximum(b_sz[None, :], 1)
    expected = a_sz[:, None] * b_sz[None, :] / nU
    oe = inter / np.maximum(expected, 1e-9)

    print(f"\n  AGGREGATE — {len(reps1)} 1H clusters x {len(REP)} 15m medoids = "
          f"{len(reps1)*len(REP)} pairs")
    print(f"    {'1H cl':>5} {'1H representative':<32} {'maxJ':>6} {'maxP(15|1H)':>12} "
          f"{'maxP(1H|15)':>12} {'>2x enr':>8} " +
          "".join(f"{'J>=' + str(t):>8}" for t in BINS))
    agg = []
    for i, r in enumerate(reps1):
        row = dict(cluster_1h=r["cluster_1h"], representative=r["claim"], j=r["j"],
                   n_members_in_U=int(a_sz[i]),
                   max_jaccard=round(float(jac[i].max()), 4),
                   max_p15_given_1h=round(float(p_b_given_a[i].max()), 4),
                   max_p1h_given_15=round(float(p_a_given_b[i].max()), 4),
                   n_enriched_2x=int((oe[i] > 2).sum()),
                   median_obs_over_exp=round(float(np.median(oe[i])), 3))
        for t in BINS:
            row[f"n_J_ge_{t}"] = int((jac[i] >= t).sum())
        agg.append(row)
        print(f"    {r['cluster_1h']:>5} {r['claim']:<32} {row['max_jaccard']:>6.3f} "
              f"{row['max_p15_given_1h']:>12.3f} {row['max_p1h_given_15']:>12.3f} "
              f"{row['n_enriched_2x']:>8} " +
              "".join(f"{row[f'n_J_ge_{t}']:>8}" for t in BINS))

    ii, jj = np.unravel_index(np.argsort(-jac, axis=None)[:40], jac.shape)
    PT = pd.DataFrame(dict(
        cluster_1h=[reps1[i]["cluster_1h"] for i in ii],
        rep_1h=[reps1[i]["claim"] for i in ii],
        cluster_15m=REP.cluster.to_numpy()[jj],
        medoid_15m=REP.claim_id.to_numpy()[jj],
        n_15m_cluster=REP.n_claims.to_numpy()[jj],
        intersection=inter[ii, jj], jaccard=jac[ii, jj].round(4),
        p_15m_given_1h=p_b_given_a[ii, jj].round(4),
        p_1h_given_15m=p_a_given_b[ii, jj].round(4),
        expected=expected[ii, jj].round(1), obs_over_exp=oe[ii, jj].round(3)))
    PT.to_parquet(PAIRS, index=False)
    print(f"\n  TOP COMPATIBILITY PAIRS — ranked by Jaccard ONLY, no Z or θ in the ordering")
    print(f"    {'1H':>3} {'1H rep':<26} {'15m medoid':<26} {'n∩':>7} {'J':>6} "
          f"{'P(15|1H)':>9} {'P(1H|15)':>9} {'o/e':>7}")
    for r in PT.head(15).itertuples():
        print(f"    {r.cluster_1h:>3} {r.rep_1h[:26]:<26} {r.medoid_15m[:26]:<26} "
              f"{r.intersection:>7,} {r.jaccard:>6.3f} {r.p_15m_given_1h:>9.3f} "
              f"{r.p_1h_given_15m:>9.3f} {r.obs_over_exp:>7.2f}")

    dig = ART.seal(dict(
        spec_id="T5_CROSS_RESOLUTION_COMPATIBILITY_V1", status="POST_EXPOSURE_DESCRIPTIVE",
        role="compatibility / localization only",
        is_not=["replication", "independent evidence", "confirmation",
                "a new promotion path"],
        why_not_independent="both resolutions read the same T5 episode universe and the same "
                            "10-day outcome",
        promotion_1h="FROZEN / UNCHANGED", promotion_15m="FROZEN / UNCHANGED",
        matching="episode membership only; token names play no part",
        representative_rule="highest mean within-cluster Jaccard, tie-break lowest sealed j; "
                            "outcome-blind selection within already outcome-selected clusters",
        common_universe=dict(n=nU, note="the two populations differ (1H 118,849 vs 15m "
                                        "112,621); everything is computed on the "
                                        "intersection so population mismatch is not read as "
                                        "membership mismatch",
                             members_dropped_by_restriction=int(sum(a_loss))),
        baseline="E[|A n B|] = |A||B| / |U| under independence; observed/expected is what says "
                 "whether a 1H cluster CONCENTRATES in a 15m medoid",
        bins_note=f"J >= {BINS} are DESCRIPTIVE bins, not significance thresholds",
        pair_ranking="Jaccard alone; no Z and no theta enters selection or ordering",
        n_1h_clusters=len(reps1), n_15m_medoids=len(REP),
        representatives_1h=reps1,
        aggregate=agg,
        top_pairs=PT.head(40).to_dict("records"),
        artifact=os.path.basename(PAIRS),
        minutes=round((time.time() - t0) / 60, 1)),
        OUT, required=("spec_id", "role", "aggregate"))
    print(f"\n  WROTE {OUT} · {dig}\n        {PAIRS}")


if __name__ == "__main__":
    main()
