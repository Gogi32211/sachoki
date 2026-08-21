"""Cluster medoids, then mechanism characterization on them. Two sealed artifacts.

POST_EXPOSURE DESCRIPTIVE throughout. Promotion is FROZEN and UNCHANGED.

THE REPRESENTATIVE IS CHOSEN ON X ALONE

    representative = the cluster member with the highest MEAN Jaccard to the other members
    tie-break      = lowest sealed j
    singleton      = itself

Picking by max Z or max theta would be an outcome-based cherry-pick: the loudest historical
result in each family would become the family's face, and every downstream number would
inherit that selection. Jaccard is membership only, and sealed j is membership only, so the
representative is the most CENTRAL episode geometry rather than the prettiest result.

Single-linkage means two members of one cluster can have low pairwise Jaccard with each other,
so the medoid needs the FULL within-cluster matrix, not the J >= 0.10 pair file.

STATUS OF EACH OUTCOME IS NOT UNIFORM

    MFE_10D        primary historical magnitude
    MFE_ATR_10D    registered secondary characterization
    MAE_10D        POST_EXPOSURE DESCRIPTIVE ONLY — cannot affect promotion
    ret_10d        POST_EXPOSURE DESCRIPTIVE ONLY — cannot affect promotion

TWO QUESTIONS, AND WHAT THE ANSWERS CANNOT BE

Whether large raw theta is partly a volatility-scale effect: the pre-treatment variables are
read at the T5-2 anchor, BEFORE the window opens, so a difference there is composition. This
is a volatility-scale MECHANISM DIAGNOSTIC, not a control for confounding — adjusting for a
consequence of the studied session would remove part of what is being measured.

Whether MFE potential reaches terminal return: the estimand declares no exit policy, so a
large MFE with a flat ret_10d is not a defect in the estimand. It is a statement about
tradeability, which the estimand never claimed to measure.
"""
from __future__ import annotations
import os, sys
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "4"
import json, time                                                     # noqa: E402
import duckdb, numpy as np, pandas as pd                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                # noqa: E402
import t5_15m_par_qualify as PQ                                       # noqa: E402
import t5_15m_historical as H                                         # noqa: E402
import t5_sequence_estimand as EST                                    # noqa: E402

OVL  = json.load(open("T5_15M_SURVIVOR_OVERLAP_V1.json"))
EXP  = json.load(open("T5_15M_HISTORICAL_EXPOSED.json"))
DATA = os.path.join(D.ROOT, "data")
SURV = os.path.join(DATA, "t5_15m_survivors.parquet")
REP_OUT = "T5_15M_CLUSTER_REPRESENTATIVES_V1.json"
MEC_OUT = "T5_15M_SURVIVOR_MECHANISM_CHARACTERIZATION_V1.json"
REP_PQ  = os.path.join(DATA, "t5_15m_cluster_representatives.parquet")
MEC_PQ  = os.path.join(DATA, "t5_15m_mechanism.parquet")


def pre_treatment(E):
    """Price and 20-session realized volatility at the T5-2 close. Nothing from the window."""
    conn = duckdb.connect(D.DB1D, read_only=True)
    try:
        conn.register("ep", E[["episode_id", "ticker", "t5_date"]])
        q = """
        WITH b AS (
          SELECT * FROM (
            SELECT ticker, date, close,
                   row_number() OVER (PARTITION BY ticker,date ORDER BY
                     CASE universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1 ELSE 2 END) rn
            FROM bars WHERE universe IN ('sp500','nasdaq','russell2k')
          ) WHERE rn=1
        ), r AS (
          SELECT ticker, date, close,
                 CASE WHEN close > 0
                       AND lag(close) OVER (PARTITION BY ticker ORDER BY date) > 0
                      THEN ln(close / lag(close) OVER (PARTITION BY ticker ORDER BY date))
                 END lr
          FROM b
        ), s AS (
          SELECT ticker, date, close,
                 stddev_samp(lr) OVER (PARTITION BY ticker ORDER BY date
                     ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) rv20
          FROM r
        ), a AS (
          SELECT e.episode_id, s.close AS px_anchor, s.rv20,
                 row_number() OVER (PARTITION BY e.episode_id ORDER BY s.date DESC) k
          FROM ep e JOIN s ON s.ticker=e.ticker AND s.date < CAST(e.t5_date AS DATE)
          QUALIFY k = 2
        )
        SELECT episode_id, px_anchor, rv20 FROM a"""
        return conn.execute(q).fetchdf()
    finally:
        conn.close()


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    S = pd.read_parquet(SURV).reset_index(drop=True)
    ncl = int(S.cluster.nunique())
    if ncl != OVL["headline"]["n_overlap_clusters"] or len(S) != EXP["survivors"]:
        raise RuntimeError("survivor/cluster tables do not match the frozen audits")
    print(f"CLUSTER MEDOIDS + MECHANISM · {len(S)} survivors · {ncl} frozen clusters",
          flush=True)

    z = np.load(PQ.BUNDLE)
    eidx = z["eidx"]; seg_ptr = z["seg_ptr"]; seg_nc = z["seg_nc"]
    csp = z["claim_seg_ptr"]; nE = int(z["nE"][0])

    M = np.zeros((len(S), nE), np.float32)
    for r, j in enumerate(S.j.to_numpy()):
        a, b = csp[j], csp[j + 1]
        M[r, eidx[seg_ptr[a]:seg_ptr[b]]] = 1.0
    sz = M.sum(1).astype(np.int64)
    if not np.array_equal(sz, S.support.to_numpy()):
        raise RuntimeError("rebuilt membership disagrees with the sealed support")

    # ── medoid per cluster, X-only ─────────────────────────────────────────
    rows = []
    for c in range(ncl):
        idx = np.flatnonzero(S.cluster.to_numpy() == c)
        if len(idx) == 1:
            pick, mj = int(idx[0]), 1.0
        else:
            sub = M[idx]
            it = (sub @ sub.T).astype(np.int64)
            s2 = sz[idx]
            un = s2[:, None] + s2[None, :] - it
            J = it / un
            np.fill_diagonal(J, 0.0)
            mean_j = J.sum(1) / (len(idx) - 1)
            best = np.flatnonzero(mean_j == mean_j.max())
            # tie-break: lowest sealed j
            pick = int(idx[best[np.argmin(S.j.to_numpy()[idx[best]])]])
            mj = float(mean_j.max())
        rows.append(dict(cluster=c, n_claims=int(len(idx)),
                         rep_row=pick, j=int(S.j.iloc[pick]),
                         claim_id=S.claim_id.iloc[pick],
                         position_family=S.position_family.iloc[pick],
                         support=int(S.support.iloc[pick]),
                         Z_rank=float(S.Z_rank_obs.iloc[pick]),
                         theta_mfe=float(S.theta_raw_obs.iloc[pick]),
                         mean_jaccard_to_cluster=round(mj, 4)))
    REP = pd.DataFrame(rows)
    REP.to_parquet(REP_PQ, index=False)
    ART.seal(dict(
        spec_id="T5_15M_CLUSTER_REPRESENTATIVES_V1", status="POST_EXPOSURE_DESCRIPTIVE",
        promotion="FROZEN / UNCHANGED",
        rule="within each frozen J>=0.50 cluster, the member with the highest MEAN Jaccard to "
             "the other members (membership medoid); tie-break lowest sealed j; singleton = "
             "itself",
        selection_is_x_only="Jaccard is membership only and sealed j is membership only. No "
                            "outcome enters the choice — picking by max Z or max theta would "
                            "make the loudest result the face of each family and every "
                            "downstream number would inherit that selection.",
        full_matrix_note="single linkage lets two members of one cluster have low pairwise "
                         "Jaccard, so the medoid uses the FULL within-cluster matrix, not the "
                         "J>=0.10 pair file",
        overlap_audit_digest=ART.file_digest("T5_15M_SURVIVOR_OVERLAP_V1.json"),
        clusters=ncl, representatives=len(REP),
        mean_jaccard=dict(p10=round(float(REP.mean_jaccard_to_cluster.quantile(.10)), 4),
                          p50=round(float(REP.mean_jaccard_to_cluster.median()), 4),
                          p90=round(float(REP.mean_jaccard_to_cluster.quantile(.90)), 4)),
        artifact=os.path.basename(REP_PQ)),
        REP_OUT, required=("spec_id", "rule", "clusters"))
    print(f"  medoids frozen · mean-Jaccard p10/p50/p90 "
          f"{REP.mean_jaccard_to_cluster.quantile(.10):.3f}/"
          f"{REP.mean_jaccard_to_cluster.median():.3f}/"
          f"{REP.mean_jaccard_to_cluster.quantile(.90):.3f}", flush=True)

    # ── variables on the sealed population ─────────────────────────────────
    P = pd.read_parquet(os.path.join(DATA, "t5_15m_population.parquet"))
    P = P.sort_values(["block", "episode_id"], kind="stable").reset_index(drop=True)
    blk = P.block.to_numpy(np.int64)
    nb = int(blk.max()) + 1
    blk_n = np.bincount(blk, minlength=nb)
    blk_start = np.r_[0, np.cumsum(blk_n)[:-1]]
    EP = P.merge(pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", "t5_date"]),
                 on="episode_id", how="left")
    A = EST.anchors(EP).set_index("episode_id").reindex(P.episode_id)
    Q = pre_treatment(EP).set_index("episode_id").reindex(P.episode_id)
    O = pd.read_parquet(os.path.join(DATA, "t5_episode_outcomes.parquet"),
                        columns=["episode_id", "mfe_10d", "mae_10d", "ret_10d",
                                 "mfe_atr_10d"]).set_index("episode_id").reindex(P.episode_id)

    VARS = [
        ("PRE",  "atr_pct_T5m2",   A.vol_anchor.to_numpy(float) * 100),
        ("PRE",  "rv20_pct_T5m2",  Q.rv20.to_numpy(float) * 100),
        ("PRE",  "dollar_vol_M",   A.liq_anchor.to_numpy(float) / 1e6),
        ("PRE",  "price_T5m2",     Q.px_anchor.to_numpy(float)),
        ("PRIM", "mfe_10d_pp",     O.mfe_10d.to_numpy(float) * 100),
        ("SEC",  "mfe_atr_10d",    O.mfe_atr_10d.to_numpy(float)),
        ("DESC", "mae_10d_pp",     O.mae_10d.to_numpy(float) * 100),
        ("DESC", "ret_10d_pp",     O.ret_10d.to_numpy(float) * 100),
    ]
    bad = [n for _, n, v in VARS if np.isnan(v).any()]
    if bad:
        print(f"  NOTE variables with missing values, medians taken on the non-missing rows "
              f"within each block: {bad}", flush=True)

    reps = REP.j.to_numpy()
    out = REP.copy()
    for kind, nm, v in VARS:
        vv = np.where(np.isnan(v), np.nanmedian(v), v)
        d, t, c = H.theta_for(reps, eidx, seg_ptr, seg_nc, csp, vv, blk, blk_start, blk_n)
        out[f"{kind}_{nm}"] = np.round(d, 4)
    out.to_parquet(MEC_PQ, index=False)

    def q(col, qs=(10, 25, 50, 75, 90)):
        return {f"p{x}": round(float(np.percentile(out[col], x)), 4) for x in qs}

    ret = out["DESC_ret_10d_pp"].to_numpy()
    npos = int((ret > 0.25).sum()); nneg = int((ret < -0.25).sum())
    nnear = int(len(ret) - npos - nneg)

    print(f"\n  AGGREGATE over {len(out)} cluster representatives — treated − control")
    print(f"    raw θ  MFE pp     " + "  ".join(f"{k} {v}" for k, v in q("PRIM_mfe_10d_pp").items()))
    print(f"    MFE_ATR θ         " + "  ".join(f"{k} {v}" for k, v in
                                                q("SEC_mfe_atr_10d", (10, 50, 90)).items()))
    print(f"    MAE pp θ          " + "  ".join(f"{k} {v}" for k, v in
                                                q("DESC_mae_10d_pp", (10, 50, 90)).items()))
    print(f"    ret10 pp θ        " + "  ".join(f"{k} {v}" for k, v in
                                                q("DESC_ret_10d_pp", (10, 50, 90)).items()))
    print(f"      ret10 sign (|θ| <= 0.25 pp = near-zero):  "
          f"positive {npos} · near-zero {nnear} · negative {nneg}")
    print(f"    pre ATR% Δ        " + "  ".join(f"{k} {v}" for k, v in
                                                q("PRE_atr_pct_T5m2", (10, 50, 90)).items()))
    print(f"    pre rv20% Δ       " + "  ".join(f"{k} {v}" for k, v in
                                                q("PRE_rv20_pct_T5m2", (10, 50, 90)).items()))

    big = out.sort_values("n_claims", ascending=False).head(10)
    print(f"\n  LARGEST CLUSTERS — representative chosen by membership medoid, not by result")
    print(f"    {'cl':>4} {'n':>4} {'representative':<30} {'supp':>7} {'Z':>7} {'θMFE':>7} "
          f"{'θATR':>6} {'θret':>7} {'θMAE':>7} {'ΔATR%':>7} {'Δrv20':>7}")
    for r in big.itertuples():
        print(f"    {r.cluster:>4} {r.n_claims:>4} {r.claim_id:<30} {r.support:>7,} "
              f"{r.Z_rank:>7.2f} {r.theta_mfe:>7.2f} {getattr(r,'SEC_mfe_atr_10d'):>6.3f} "
              f"{getattr(r,'DESC_ret_10d_pp'):>7.2f} {getattr(r,'DESC_mae_10d_pp'):>7.2f} "
              f"{getattr(r,'PRE_atr_pct_T5m2'):>7.3f} {getattr(r,'PRE_rv20_pct_T5m2'):>7.3f}")

    dig = ART.seal(dict(
        spec_id="T5_15M_SURVIVOR_MECHANISM_CHARACTERIZATION_V1",
        status="POST_EXPOSURE_DESCRIPTIVE", promotion="FROZEN / UNCHANGED",
        population=f"{len(out)} cluster representatives, NOT the 733 survivors — "
                   f"characterising every survivor would analyse the same episode family "
                   f"many times over",
        representatives_digest=ART.file_digest(REP_OUT),
        outcome_status=dict(MFE_10D="primary historical magnitude",
                            MFE_ATR_10D="registered secondary characterization",
                            MAE_10D="POST_EXPOSURE DESCRIPTIVE ONLY — cannot affect promotion",
                            ret_10d="POST_EXPOSURE DESCRIPTIVE ONLY — cannot affect promotion"),
        comparison="estimand blocks and treated-episode weights applied to each variable "
                   "instead of MFE",
        pre_treatment_anchor="T5-2 close, before the window opens — a difference there is "
                             "COMPOSITION. This is a volatility-scale mechanism diagnostic, "
                             "NOT a control for confounding.",
        aggregate=dict(theta_mfe_pp=q("PRIM_mfe_10d_pp"),
                       theta_mfe_atr=q("SEC_mfe_atr_10d"),
                       theta_mae_pp=q("DESC_mae_10d_pp"),
                       theta_ret10_pp=q("DESC_ret_10d_pp"),
                       ret10_sign=dict(positive=npos, near_zero=nnear, negative=nneg,
                                       near_zero_band_pp=0.25),
                       pre_atr_pct=q("PRE_atr_pct_T5m2"),
                       pre_rv20_pct=q("PRE_rv20_pct_T5m2"),
                       pre_dollar_vol_M=q("PRE_dollar_vol_M"),
                       pre_price=q("PRE_price_T5m2")),
        largest_clusters=big.round(4).to_dict("records"),
        variables_with_missing=bad,
        exit_policy_note="the estimand declares NO exit policy, so a large MFE with a flat "
                         "ret_10d is not a defect in it — it is a statement about "
                         "tradeability, which the estimand never claimed to measure",
        not_done=["1H<->15m matching", "token-family success rates",
                  "which 15m pattern is best", "3-bar grammar", "new thresholds",
                  "winner selection inside a cluster"],
        artifact=os.path.basename(MEC_PQ),
        minutes=round((time.time() - t0) / 60, 1)),
        MEC_OUT, required=("spec_id", "aggregate", "outcome_status"))
    print(f"\n  WROTE {REP_OUT}\n        {MEC_OUT} · {dig}\n        {MEC_PQ}")


if __name__ == "__main__":
    main()
