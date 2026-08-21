"""T5_CROSS_RESOLUTION_COMPATIBILITY_COMPLETION_V1 — all 684 pairs, no truncation.

POST_EXPOSURE DESCRIPTIVE. Promotion UNCHANGED. This is not a new search: the pair set was
already defined by T5_CROSS_RESOLUTION_COMPATIBILITY_V1, which then stored only the top 40 by
Jaccard. That truncation left two of the four 1H clusters with no visible distribution at all,
so the verdict could not be written. Storing all 684 costs nothing.

THE REPORTING HIERARCHY IS DELIBERATE

    PRIMARY          P(15m | 1H)  and  observed/expected
    SECONDARY        P(1H | 15m)
    GEOMETRY CONTEXT Jaccard

Jaccard was leading the first report and it should not have been. A 1H cluster holds 311 to
5,451 episodes while a 15m medoid can cover 10-30% of U, and at that asymmetry high
containment with low Jaccard is the normal case, not a weak one.

TWO BASELINES, AND THE EARLIER REPORT CONFLATED THEM

observed/expected is measured against INDEPENDENCE. Saying a pair sits at "21-29x its own
baseline" was wrong: VOL_W's median o/e across its 171 candidate medoids is 3.85, so those
pairs are 21-29x independence and roughly 5.5-7.5x its own typical 15m overlap. Both numbers
are reported per cluster so the two cannot be mixed again.

MEDOID PREVALENCE IS SHOWN

P(15m|1H) = 0.71 means little without knowing how common that medoid is in U. o/e already
adjusts for it, but the raw prevalence is carried so the adjustment is visible rather than
implicit.
"""
from __future__ import annotations
import os, sys
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import gc, hashlib, json, time                                        # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                # noqa: E402
import t5_15m_par_qualify as PQ                                       # noqa: E402

PRIOR = json.load(open("T5_CROSS_RESOLUTION_COMPATIBILITY_V1.json"))
OVL1H = json.load(open("SURVIVOR_OVERLAP_V1.json"))
DATA  = os.path.join(D.ROOT, "data")
MEMB1H = os.path.join(DATA, "t5_1h_survivor_membership.parquet")
IDENT1H = "T5_1H_MEMBERSHIP_CACHE_V1.json"
OUT   = "T5_CROSS_RESOLUTION_COMPATIBILITY_COMPLETION_V1.json"
PAIRS = os.path.join(DATA, "t5_cross_resolution_pairs_all.parquet")


def frozen_1h_identity():
    """What the cache must match. Assembled from the sealed 1H artifacts, never from the DB."""
    SEAL = json.load(open("T5_SEQUENCE_ESTIMAND_SEAL_V1.json"))
    return dict(
        source_role="FROZEN_1H_RESEARCH_STATE",
        source_population_hash=SEAL["population_hash"],
        claim_order_hash=SEAL["claim_order_hash"],
        block_assignment_hash=SEAL["block_assignment_hash"],
        t5_definition_hash=D.T5_DEF_HASH,
        survivor_overlap_digest=ART.file_digest("SURVIVOR_OVERLAP_V1.json"),
        cluster_assignment_digest=hashlib.sha256(
            json.dumps(OVL1H["clusters"], sort_keys=True).encode()).hexdigest()[:16],
        builder_code_digest=ART.file_digest("t5_capability_run.py"))


def load_1h_membership():
    """Episode ids of the 9 1H survivors plus the 1H population, with a frozen-identity gate.

    Cached because build_state peaks near 7.9 GB — the profile that took this machine down
    twice — so it is paid ONCE. But a cache without an identity check trades a performance
    problem for a provenance one, which is worse: this audit needs the ALREADY-FROZEN 1H
    evidence geometry, not whatever the current database would produce.

    That risk is real and not hypothetical. build_state calls EST.anchors(), which queries the
    LIVE studio_analytics.duckdb for the T5-2 liquidity and volatility used to filter the
    population. Membership itself comes from the frozen microstructure parquet, but the anchor
    query can change which episodes pass the filter. build_state already raises on
    population-hash drift; that guard is now recorded in the cache and re-checked on every
    load, and any mismatch is a hard failure rather than a silent rebuild from current data.
    """
    want = frozen_1h_identity()
    if os.path.exists(MEMB1H) and os.path.exists(IDENT1H):
        got = json.load(open(IDENT1H))
        bad = {k: (want[k], got.get(k)) for k in want if got.get(k) != want[k]}
        if got.get("cache_file_digest") != ART.file_digest(MEMB1H):
            bad["cache_file_digest"] = (ART.file_digest(MEMB1H), got.get("cache_file_digest"))
        if bad:
            raise RuntimeError(
                f"1H membership cache identity mismatch — refusing to use it and refusing to "
                f"silently rebuild from the current database. Differing: {bad}")
        T = pd.read_parquet(MEMB1H)
        pop = T[T.kind == "population"].episode_id.to_numpy()
        memb = {int(j): T[(T.kind == "member") & (T.j == j)].episode_id.to_numpy()
                for j in T[T.kind == "member"].j.unique()}
        names = dict(T[T.kind == "member"][["j", "claim_id"]].drop_duplicates()
                     .itertuples(index=False, name=None))
        print(f"  1H membership from cache · identity verified · {len(memb)} survivors · "
              f"population {len(pop):,}", flush=True)
        return pop, memb, {int(k): v for k, v in names.items()}

    import t5_capability_run as R
    y, b, nb, S1, claims, order = R.build_state(verbose=False)
    ids1 = R.LAST_EPISODE_IDS
    ordj = order.sort_values("j").reset_index(drop=True)
    js = sorted({j for c in OVL1H["clusters"] for j in c})
    col = {j: int(np.flatnonzero(ordj.j.to_numpy() == j)[0]) for j in js}
    memb = {j: ids1[S1[col[j]]] for j in js}
    names = {j: ordj.representative.iloc[col[j]] for j in js}
    pop = np.array(ids1)
    del y, b, S1, claims, order, ids1
    gc.collect()
    rows = [dict(kind="population", j=-1, claim_id="", episode_id=e) for e in pop]
    for j in js:
        rows += [dict(kind="member", j=j, claim_id=names[j], episode_id=e) for e in memb[j]]
    pd.DataFrame(rows).to_parquet(MEMB1H, index=False)
    ident = dict(want)
    ident.update(spec_id="T5_1H_MEMBERSHIP_CACHE_V1",
                 n_population=int(len(pop)), n_claims_used=len(memb),
                 episode_id_digest=hashlib.sha256(
                     "|".join(sorted(pop)).encode()).hexdigest()[:16],
                 cache_file_digest=ART.file_digest(MEMB1H),
                 population_hash_guard="build_state raises on population-hash drift; that "
                                       "guard is what makes the live EST.anchors() query safe",
                 live_db_touched="EST.anchors() reads studio_analytics.duckdb for the T5-2 "
                                 "liquidity and volatility used in the population filter")
    ART.seal(ident, IDENT1H, required=("spec_id", "source_population_hash",
                                       "cache_file_digest"))
    if ident["episode_id_digest"] != want["source_population_hash"]:
        print(f"  NOTE cache episode digest {ident['episode_id_digest']} vs sealed "
              f"population_hash {want['source_population_hash']} — build_state's own guard "
              f"already asserted the population; the digests differ only if the orderings do",
              flush=True)
    print(f"  1H membership built and cached · identity sealed · {len(memb)} survivors",
          flush=True)
    return pop, memb, names


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    print("T5_CROSS_RESOLUTION_COMPATIBILITY_COMPLETION_V1 · all 684 pairs", flush=True)
    pop1h, memb1, name1 = load_1h_membership()

    def jac_sets(a, b_):
        i = len(np.intersect1d(a, b_))
        u = len(a) + len(b_) - i
        return i / u if u else 0.0

    reps1 = []
    for ci, cl in enumerate(OVL1H["clusters"]):
        cl = list(cl)
        if len(cl) == 1:
            pick = cl[0]
        else:
            mjs = [np.mean([jac_sets(memb1[j], memb1[k]) for k in cl if k != j]) for j in cl]
            m = max(mjs)
            pick = min([cl[i] for i, v in enumerate(mjs) if v == m])
        reps1.append(dict(cluster_1h=ci, n_claims=len(cl), j=pick, claim=name1[pick]))

    REP = pd.read_parquet(os.path.join(DATA, "t5_15m_cluster_representatives.parquet"))
    z = np.load(PQ.BUNDLE)
    eidx = z["eidx"]; seg_ptr = z["seg_ptr"]; csp = z["claim_seg_ptr"]
    P = pd.read_parquet(os.path.join(DATA, "t5_15m_population.parquet"))
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    ids15 = P.episode_id.to_numpy()

    U = np.intersect1d(pop1h, ids15)
    nU = len(U)
    uidx = {e: i for i, e in enumerate(U)}
    print(f"  populations · 1H {len(pop1h):,} · 15m {len(ids15):,} · U {nU:,}", flush=True)

    def mask(arr):
        m = np.zeros(nU, np.float32)
        k = [uidx[e] for e in arr if e in uidx]
        m[k] = 1.0
        return m, len(k)

    A = np.zeros((len(reps1), nU), np.float32); a_sz = []
    for i, r in enumerate(reps1):
        m, kept = mask(memb1[r["j"]]); A[i] = m; a_sz.append(kept)
    B = np.zeros((len(REP), nU), np.float32); b_sz = []
    for i, jj in enumerate(REP.j.to_numpy()):
        a, b_ = csp[jj], csp[jj + 1]
        m, kept = mask(ids15[eidx[seg_ptr[a]:seg_ptr[b_]]]); B[i] = m; b_sz.append(kept)
    a_sz = np.array(a_sz); b_sz = np.array(b_sz)

    inter = (A @ B.T).astype(np.int64)
    union = a_sz[:, None] + b_sz[None, :] - inter
    jac = np.where(union > 0, inter / np.maximum(union, 1), 0.0)
    p15_1h = inter / np.maximum(a_sz[:, None], 1)
    p1h_15 = inter / np.maximum(b_sz[None, :], 1)
    prev15 = b_sz / nU
    expected = a_sz[:, None] * b_sz[None, :] / nU
    oe = inter / np.maximum(expected, 1e-9)

    # ── all 684 pairs, no truncation ───────────────────────────────────────
    ii, jj = np.meshgrid(np.arange(len(reps1)), np.arange(len(REP)), indexing="ij")
    ii = ii.ravel(); jj = jj.ravel()
    ALL = pd.DataFrame(dict(
        cluster_1h=[reps1[i]["cluster_1h"] for i in ii],
        rep_1h=[reps1[i]["claim"] for i in ii],
        j_1h=[reps1[i]["j"] for i in ii],
        cluster_15m=REP.cluster.to_numpy()[jj],
        medoid_15m=REP.claim_id.to_numpy()[jj],
        j_15m=REP.j.to_numpy()[jj],
        n_15m_cluster=REP.n_claims.to_numpy()[jj],
        n_1h=a_sz[ii], n_15m=b_sz[jj], prevalence_15m=prev15[jj].round(5),
        intersection=inter[ii, jj],
        p_15m_given_1h=p15_1h[ii, jj].round(5),
        p_1h_given_15m=p1h_15[ii, jj].round(5),
        expected=expected[ii, jj].round(2),
        obs_over_exp=oe[ii, jj].round(4),
        jaccard=jac[ii, jj].round(5)))
    ALL.to_parquet(PAIRS, index=False)

    print(f"\n  PER 1H CLUSTER — PRIMARY: P(15m|1H) and o/e · SECONDARY: P(1H|15m) · "
          f"CONTEXT: Jaccard")
    agg = []
    for i, r in enumerate(reps1):
        o = oe[i]; k = int(np.argmax(o))
        row = dict(
            cluster_1h=r["cluster_1h"], representative=r["claim"], n_1h_in_U=int(a_sz[i]),
            max_oe=round(float(o.max()), 3), median_oe=round(float(np.median(o)), 3),
            p90_oe=round(float(np.percentile(o, 90)), 3),
            top_over_median=round(float(o.max() / max(np.median(o), 1e-9)), 2),
            top_pair_percentile=round(float((o <= o.max()).mean() * 100), 2),
            max_p15_given_1h=round(float(p15_1h[i].max()), 4),
            argmax_oe_medoid=REP.claim_id.iloc[k],
            argmax_oe_prevalence_15m=round(float(prev15[k]), 5),
            argmax_oe_p15_given_1h=round(float(p15_1h[i, k]), 4),
            max_jaccard=round(float(jac[i].max()), 4))
        agg.append(row)
        print(f"    cl{r['cluster_1h']}  {r['claim']:<32} n {int(a_sz[i]):>5,}")
        print(f"        o/e   max {row['max_oe']:>7.2f} · median {row['median_oe']:>5.2f} · "
              f"p90 {row['p90_oe']:>5.2f} · top/median {row['top_over_median']:>5.2f}×")
        print(f"        best-o/e medoid  {row['argmax_oe_medoid']:<28} "
              f"P(15m) {row['argmax_oe_prevalence_15m']:.4f} · "
              f"P(15m|1H) {row['argmax_oe_p15_given_1h']:.3f}")
        print(f"        max P(15m|1H) {row['max_p15_given_1h']:.3f} · "
              f"max Jaccard {row['max_jaccard']:.3f}")

    dig = ART.seal(dict(
        spec_id="T5_CROSS_RESOLUTION_COMPATIBILITY_COMPLETION_V1",
        status="POST_EXPOSURE_DESCRIPTIVE", promotion="UNCHANGED",
        completes="T5_CROSS_RESOLUTION_COMPATIBILITY_V1",
        completes_digest=ART.file_digest("T5_CROSS_RESOLUTION_COMPATIBILITY_V1.json"),
        why="the earlier run stored only the top 40 pairs by Jaccard, which left two of the "
            "four 1H clusters with no visible distribution and made the verdict unwritable",
        pairs_stored=int(len(ALL)), pairs_expected=len(reps1) * len(REP),
        reporting_hierarchy=dict(
            primary=["P(15m | 1H)", "observed/expected"],
            secondary=["P(1H | 15m)"], geometry_context=["Jaccard"],
            why="a 1H cluster holds 311-5,451 episodes while a 15m medoid can cover 10-30% of "
                "U; at that asymmetry high containment with low Jaccard is normal, so Jaccard "
                "must not lead"),
        two_baselines=dict(
            independence="observed/expected, the ratio reported per pair",
            own_typical="that cluster's median o/e across its 171 candidate medoids",
            earlier_error="the first report called a 21-29x independence ratio '21-29x its "
                          "own baseline'. Those are different denominators and both are now "
                          "reported per cluster."),
        common_universe=nU,
        per_cluster=agg,
        artifact=os.path.basename(PAIRS),
        cache=os.path.basename(MEMB1H),
        minutes=round((time.time() - t0) / 60, 1)),
        OUT, required=("spec_id", "per_cluster", "pairs_stored"))
    print(f"\n  WROTE {OUT} · {dig}\n        {PAIRS} ({len(ALL)} rows)")


if __name__ == "__main__":
    main()
