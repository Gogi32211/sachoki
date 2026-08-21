"""POST-EXPOSURE diagnostics on the 9 survivors. Two artifacts, no promotion changes.

BOTH ARE POST_EXPOSURE AND MUST BE LABELLED SO FOREVER

Nothing here can promote, demote, or re-rank. No cluster count becomes a new k. "Four claims
collapse to one motif" is an interpretation of what survived, not a correction to the test
that was already run and already frozen.

    SURVIVOR_OVERLAP_V1
        how many distinct historical motifs are the 9 survivors really? Measured on exact
        episode membership, never inferred from shared token prefixes. Four claims beginning
        VOL_W-> may be one motif or four conditional paths; the names cannot tell us.

    SURVIVOR_MAGNITUDE_DECOMPOSITION_V1
        why is VOL_W's raw theta ~15-18pp but only ~0.8-1.0 ATR? Three readings remain open
        and this diagnostic does not decide between them:
            A  spurious volatility scaling
            B  genuine effect heterogeneity — the same rank advantage in more volatile
               episodes simply produces larger absolute MFE
            C  a mixture
        So this is a VOLATILITY-SCALE MECHANISM DIAGNOSTIC, not a confound audit.

PRE-TREATMENT VS POST-SEQUENCE IS THE WHOLE POINT OF THE SPLIT

Pre-treatment characteristics are measured at the T5-2 anchor, before the sequence window
opens, so a difference there is a COMPOSITION difference. Post-sequence characteristics are
measured at or after entry and describe the scale in which the outcome realises.

Neither is a legitimate adjustment variable for the V1/V2 estimand. Post-sequence volatility
is a consequence of the same session being studied; adjusting for it would condition on a
consequence. This is characterization only.

THE COMPARISON REUSES THE ESTIMAND'S OWN WEIGHTS

For every characteristic X the same block-restricted, treated-weighted contrast that defines
theta is applied to X instead of MFE. That keeps the descriptive comparison on exactly the
population and weighting the test used, rather than an unweighted pooled average that would
mix in blocks the estimand never compared.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import json, sys, time                                                # noqa: E402
import duckdb, numpy as np, pandas as pd                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_capability_run as R, t5_compute_qual as Q, t5_dna as D       # noqa: E402
import t5_sequence_estimand as EST                                     # noqa: E402

EXP = json.load(open("T5_1H_HISTORICAL_V2_EXPOSED.json"))
DATA = os.path.join(os.path.dirname(HERE), "data")
OUT_OV = os.path.join(HERE, "SURVIVOR_OVERLAP_V1.json")
OUT_MG = os.path.join(HERE, "SURVIVOR_MAGNITUDE_DECOMPOSITION_V1.json")
JACC_CLUSTER = 0.50            # declared before looking at the matrix


def arm_medians(flat, j, x):
    """Weighted treated and control medians of x for ONE claim, estimand weights and blocks."""
    a, c = flat.coff[j], flat.coff[j + 1]
    wt = wc = 0.0
    for s in range(a, c):
        t = flat.T[flat.segT[s]:flat.segT[s + 1]]
        k = flat.C[flat.segC[s]:flat.segC[s + 1]]
        wt += flat.w[s] * np.median(x[t])
        wc += flat.w[s] * np.median(x[k])
    return float(wt), float(wc)


def pre_treatment(E):
    """Price and 20-session realized volatility at the T5-2 close, alongside the two anchors
    already registered as block variables. Nothing from the sequence window itself."""
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
          -- a zero or missing close makes the log return undefined, not zero. Guarding it as
          -- NULL drops that one observation from the window instead of injecting a fake 0%.
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
    print("POST-EXPOSURE SURVIVOR DIAGNOSTICS · no promotion status changes", flush=True)
    y, b, nb, S, claims, order = R.build_state(verbose=False)
    flat = Q.FlatState(claims); del claims
    import gc; gc.collect()
    ids = R.LAST_EPISODE_IDS
    ordj = order.sort_values("j").reset_index(drop=True)

    surv = [r for r in EXP["top20"] if r["status"] == "SURVIVES_SEARCH_WIDE"]
    if len(surv) != EXP["n_claims_above_p95"]:
        raise RuntimeError("survivor count mismatch against the exposed artifact")
    cols = {int(r["j"]): int(np.flatnonzero(ordj.j.to_numpy() == int(r["j"]))[0])
            for r in surv}
    name = {int(r["j"]): r["claim_id"] for r in surv}
    js = [int(r["j"]) for r in surv]
    n = len(js)
    print(f"  survivors {n} · population {len(y):,}", flush=True)

    EP = pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", "t5_date"]) \
           .set_index("episode_id").reindex(ids)
    tk = pd.factorize(EP.ticker)[0]
    dt = pd.factorize(EP.t5_date)[0]

    # ── SURVIVOR_OVERLAP_V1 ─────────────────────────────────────────────────
    M = np.stack([S[cols[j]] for j in js])
    inter = (M.astype(np.int32) @ M.T.astype(np.int32))
    sz = np.diag(inter).copy()
    union = sz[:, None] + sz[None, :] - inter
    jac = inter / union
    cond = inter / sz[:, None]                      # P(S_j | S_i), row = conditioning claim
    itk = np.zeros((n, n), int); idt = np.zeros((n, n), int)
    for i in range(n):
        for k in range(n):
            m = M[i] & M[k]
            itk[i, k] = len(np.unique(tk[m])); idt[i, k] = len(np.unique(dt[m]))

    # single-linkage on Jaccard at a threshold declared before the matrix was seen
    lab = list(range(n))
    for i in range(n):
        for k in range(i + 1, n):
            if jac[i, k] >= JACC_CLUSTER:
                old, new = lab[k], lab[i]
                lab = [new if v == old else v for v in lab]
    cl = {}
    for i, L in enumerate(lab):
        cl.setdefault(L, []).append(js[i])
    clusters = [sorted(v) for v in cl.values()]

    print("\nSURVIVOR_OVERLAP_V1 · pairwise Jaccard on exact episode membership")
    print("      " + "".join(f"{j:>7}" for j in js))
    for i, j in enumerate(js):
        print(f"  {j:>4}" + "".join(f"{jac[i,k]:>7.3f}" for k in range(n)))
    print("\n  P(S_col | S_row)")
    print("      " + "".join(f"{j:>7}" for j in js))
    for i, j in enumerate(js):
        print(f"  {j:>4}" + "".join(f"{cond[i,k]:>7.3f}" for k in range(n)))
    print(f"\n  single-linkage clusters at Jaccard >= {JACC_CLUSTER} (threshold declared "
          f"before the matrix)")
    for c in clusters:
        print(f"    {{{', '.join(str(x) for x in c)}}}  " +
              " · ".join(name[x] for x in c))
    print(f"  distinct motifs by this rule: {len(clusters)} of {n} surviving claims")

    json.dump(dict(spec_id="SURVIVOR_OVERLAP_V1", status="POST_EXPOSURE_DIAGNOSTIC",
                   cannot="change promotion status; cluster count is NOT a new k and NOT a "
                          "corrected discovery count",
                   sealed_artifact=EXP["sealed_artifact_hash"],
                   survivors=[dict(j=j, claim=name[j], support=int(sz[i]))
                              for i, j in enumerate(js)],
                   jaccard={str(js[i]): {str(js[k]): round(float(jac[i, k]), 4)
                                         for k in range(n)} for i in range(n)},
                   conditional_prob_col_given_row={
                       str(js[i]): {str(js[k]): round(float(cond[i, k]), 4)
                                    for k in range(n)} for i in range(n)},
                   intersection_episodes={str(js[i]): {str(js[k]): int(inter[i, k])
                                                       for k in range(n)} for i in range(n)},
                   intersection_tickers={str(js[i]): {str(js[k]): int(itk[i, k])
                                                      for k in range(n)} for i in range(n)},
                   intersection_dates={str(js[i]): {str(js[k]): int(idt[i, k])
                                                    for k in range(n)} for i in range(n)},
                   cluster_rule=f"single linkage, Jaccard >= {JACC_CLUSTER}, threshold "
                                f"declared before inspecting the matrix",
                   clusters=[[int(x) for x in c] for c in clusters],
                   n_clusters=len(clusters)),
              open(OUT_OV, "w"), indent=2, ensure_ascii=False)

    # ── SURVIVOR_MAGNITUDE_DECOMPOSITION_V1 ─────────────────────────────────
    EPI = pd.DataFrame(dict(episode_id=ids)).merge(
        pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", "t5_date"]),
        on="episode_id", how="left")
    A = EST.anchors(EPI).set_index("episode_id").reindex(ids)
    P2 = pre_treatment(EPI).set_index("episode_id").reindex(ids)
    O = pd.read_parquet(os.path.join(DATA, "t5_episode_outcomes.parquet"),
                        columns=["episode_id", "entry_atr_pct", "mfe_10d", "mae_10d",
                                 "mfe_atr_10d", "mae_atr_10d", "ret_10d"]) \
          .set_index("episode_id").reindex(ids)

    VARS = [
        ("PRE", "vol_anchor_atr_pct_T5m2", A.vol_anchor.to_numpy(float) * 100),
        ("PRE", "rv20_daily_pct_T5m2", P2.rv20.to_numpy(float) * 100),
        ("PRE", "dollar_vol_20d_M_T5m2", A.liq_anchor.to_numpy(float) / 1e6),
        ("PRE", "price_T5m2", P2.px_anchor.to_numpy(float)),
        ("POST", "entry_atr_pct", O.entry_atr_pct.to_numpy(float)),
        ("POST", "mfe_10d_pp", O.mfe_10d.to_numpy(float) * 100),
        ("POST", "mae_10d_pp", O.mae_10d.to_numpy(float) * 100),
        ("POST", "range_10d_pp", (O.mfe_10d - O.mae_10d).to_numpy(float) * 100),
        ("POST", "mfe_atr_10d", O.mfe_atr_10d.to_numpy(float)),
        ("POST", "ret_10d_pp", O.ret_10d.to_numpy(float) * 100),
    ]
    bad = [nm for _, nm, v in VARS if np.isnan(v).any()]
    rows = []
    print("\nSURVIVOR_MAGNITUDE_DECOMPOSITION_V1 · estimand blocks and weights, "
          "treated vs control")
    if bad:
        print(f"  NOTE variables with missing values in the sealed population: {bad} — "
              f"their medians are computed on the non-missing rows within each block")
    for j in js:
        c = cols[j]
        rec = dict(j=j, claim=name[j])
        for kind, nm, v in VARS:
            vv = np.where(np.isnan(v), np.nanmedian(v), v)
            t_, k_ = arm_medians(flat, c, vv)
            rec[f"{kind}_{nm}_treated"] = round(t_, 4)
            rec[f"{kind}_{nm}_control"] = round(k_, 4)
            rec[f"{kind}_{nm}_diff"] = round(t_ - k_, 4)
        rows.append(rec)
    MG = pd.DataFrame(rows)

    print(f"\n  PRE-TREATMENT (T5-2 anchor · composition) — treated − control")
    print(f"    {'j':>4} {'claim':<32} {'ATR%':>7} {'rv20%':>7} {'$vol M':>9} {'price':>8}")
    for r in MG.itertuples():
        print(f"    {r.j:>4} {r.claim:<32} "
              f"{getattr(r,'PRE_vol_anchor_atr_pct_T5m2_diff'):>7.3f} "
              f"{getattr(r,'PRE_rv20_daily_pct_T5m2_diff'):>7.3f} "
              f"{getattr(r,'PRE_dollar_vol_20d_M_T5m2_diff'):>9.1f} "
              f"{getattr(r,'PRE_price_T5m2_diff'):>8.2f}")
    print(f"\n  POST-SEQUENCE (scale in which the outcome realises) — treated − control")
    print(f"    {'j':>4} {'claim':<32} {'entryATR%':>10} {'MFE pp':>8} {'MAE pp':>8} "
          f"{'rng pp':>8} {'MFE/ATR':>8} {'ret pp':>7}")
    for r in MG.itertuples():
        print(f"    {r.j:>4} {r.claim:<32} "
              f"{getattr(r,'POST_entry_atr_pct_diff'):>10.3f} "
              f"{getattr(r,'POST_mfe_10d_pp_diff'):>8.2f} "
              f"{getattr(r,'POST_mae_10d_pp_diff'):>8.2f} "
              f"{getattr(r,'POST_range_10d_pp_diff'):>8.2f} "
              f"{getattr(r,'POST_mfe_atr_10d_diff'):>8.3f} "
              f"{getattr(r,'POST_ret_10d_pp_diff'):>7.2f}")

    json.dump(dict(spec_id="SURVIVOR_MAGNITUDE_DECOMPOSITION_V1",
                   status="POST_EXPOSURE_DIAGNOSTIC",
                   framing="VOLATILITY-SCALE MECHANISM DIAGNOSTIC, not a confound audit. It "
                           "does not distinguish (A) spurious volatility scaling, (B) genuine "
                           "effect heterogeneity where the same rank advantage in more "
                           "volatile episodes produces larger absolute MFE, or (C) a mixture.",
                   cannot="change promotion status. Post-sequence volatility is a consequence "
                          "of the studied session and is NOT a legitimate adjustment variable "
                          "for the V1/V2 estimand.",
                   sealed_artifact=EXP["sealed_artifact_hash"],
                   comparison="estimand blocks and treated-episode weights, applied to each "
                              "characteristic instead of MFE",
                   variables_with_missing=bad,
                   rows=MG.to_dict("records")),
              open(OUT_MG, "w"), indent=2, ensure_ascii=False)
    print(f"\n  WROTE {OUT_OV}\n        {OUT_MG}")
    print(f"  {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
