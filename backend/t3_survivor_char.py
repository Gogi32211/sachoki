"""T3 post-exposure survivor characterization — closes the historical branch. Nothing else.

    survivor set is Y-SELECTED, so the medoid rule is stated precisely:
    outcome-blind medoid selection WITHIN the post-exposure survivor set —
    membership geometry only; never Z, theta, support or return.

DOES NOT: alter selection · run new search · compare T3 vs T5 inferentially ·
touch T3/T1/T4 · expose any other family's outcomes.
"""
from __future__ import annotations
import hashlib, json, os, sys, time
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t3_dna as D9
import t3_capability_run as CR, t3_sequence_estimand as E9
import t5_sequence_estimand as E5
import duckdb

OUT = "T3_SURVIVOR_CHARACTERIZATION_V1.json"
JACC = 0.50


def pre_treatment_extras(P):
    """Price and 20-session realized volatility at the SAME T3-2 anchor the estimand uses.
    Same dedup rule, same 'date < t3_date, second-to-last' selection — pre-treatment by
    construction, nothing from the window."""
    conn = duckdb.connect(D9.DB1D, read_only=True)
    try:
        conn.register("ep", P[["episode_id", "ticker", "t3_date"]])
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
                 CASE WHEN close > 0 AND lag(close) OVER (PARTITION BY ticker
                        ORDER BY date) > 0
                      THEN ln(close / lag(close) OVER (PARTITION BY ticker
                                ORDER BY date)) END lr
          FROM b
        ), v AS (
          SELECT ticker, date, close,
                 stddev_samp(lr) OVER (PARTITION BY ticker ORDER BY date
                     ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) rv20
          FROM r
        ), a AS (
          SELECT e.episode_id, v.close price_anchor, v.rv20 rv20_anchor,
                 row_number() OVER (PARTITION BY e.episode_id ORDER BY v.date DESC) k
          FROM ep e JOIN v ON v.ticker=e.ticker AND v.date < CAST(e.t3_date AS DATE)
          QUALIFY k = 2
        )
        SELECT episode_id, price_anchor, rv20_anchor FROM a"""
        return conn.execute(q).fetchdf()
    finally:
        conn.close()

def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    V = pd.read_parquet(os.path.join(D9.ROOT, "data", "t3_historical_vectors.parquet"))
    surv = V[V.survives].sort_values("j").reset_index(drop=True)
    ids, bcode, nb, y, S, ORDER = CR.build_state(verbose=False)
    Nb = np.bincount(bcode, minlength=nb)

    # ── 1 · exact OVERLAP-RESTRICTED memberships for the 8 (the sealed definition) ──
    memb = {}
    for x in surv.itertuples():
        k = int(ORDER.index[ORDER.j == x.j][0])
        s = S[:, k]
        nt = np.bincount(bcode, weights=s, minlength=nb)
        nc = Nb - nt
        inov = ((nt > 0) & (nc > 0))[bcode]
        memb[int(x.j)] = s & inov
    js = list(memb)

    # ── 2 · Jaccard>=0.50 clusters + 3 · containment ──────────────────────
    n = len(js)
    Jm = np.zeros((n, n))
    cont = []
    for a in range(n):
        for b in range(a + 1, n):
            A, B = memb[js[a]], memb[js[b]]
            inter = int((A & B).sum()); union = int((A | B).sum())
            Jm[a, b] = Jm[b, a] = inter / union if union else 0
            if inter == A.sum(): cont.append(f"j{js[a]} ⊂ j{js[b]}")
            if inter == B.sum(): cont.append(f"j{js[b]} ⊂ j{js[a]}")
    # single-link clusters at JACC
    parent = list(range(n))
    def find(i):
        while parent[i] != i: parent[i] = parent[parent[i]]; i = parent[i]
        return i
    for a in range(n):
        for b in range(a + 1, n):
            if Jm[a, b] >= JACC: parent[find(a)] = find(b)
    clusters = {}
    for i in range(n): clusters.setdefault(find(i), []).append(i)
    clusters = list(clusters.values())
    nnJ = [float(max(Jm[i][Jm[i] > 0], default=0)) for i in range(n)]

    # ── 4 · outcome-blind medoid per cluster (membership geometry only) ───
    medoids = []
    for cl in clusters:
        if len(cl) == 1: mi = cl[0]
        else:
            best = None
            for i in cl:
                mj = np.mean([Jm[i, jx] for jx in cl if jx != i])
                key = (-mj, surv.j.iloc[i])          # tie-break lowest sealed j
                if best is None or key < best[0]: best = (key, i)
            mi = best[1]
        medoids.append(mi)

    # ── outcomes + anchors for characterization ───────────────────────────
    OC = pd.read_parquet(E9.OC, columns=["episode_id","mfe_10d","mfe_atr_10d","mae_10d","ret_10d"])
    P = pd.read_parquet(os.path.join(D9.ROOT, "data", "t3_estimand_population.parquet"))
    P = P.sort_values(["block","episode_id"], kind="stable").reset_index(drop=True)
    A5 = E5.anchors(P.rename(columns={"t3_date":"t5_date"}))
    EX = pre_treatment_extras(P)          # price and rv20 at the SAME T3-2 anchor
    F = (P.merge(OC, on="episode_id")
          .merge(A5[["episode_id","liq_anchor","vol_anchor"]], on="episode_id")
          .merge(EX, on="episode_id", how="left"))
    F = F.set_index("episode_id").reindex(ids)
    bvec = bcode

    def theta(mask, col):
        df = pd.DataFrame(dict(b=bvec, m=mask, v=F[col].to_numpy()))
        th = wsum = 0.0
        for b, g in df.groupby("b"):
            nt = g.m.sum(); ncb = len(g) - nt
            if nt > 0 and ncb > 0 and g.v.notna().all():
                th += nt * (g.v[g.m].median() - g.v[~g.m].median()); wsum += nt
        return th / wsum if wsum else np.nan

    rows = []
    for ci, cl in enumerate(clusters):
        mi = medoids[ci]
        x = surv.iloc[mi]; m = memb[int(x.j)]
        r = dict(cluster=ci, members=[f"j{surv.j.iloc[i]}·{surv.representative.iloc[i]}"
                                      for i in cl],
                 medoid=f"{x.family}|{x.length}|{x.representative}", j=int(x.j),
                 support=int(x.support), z_rank=round(float(x.z_rank_obs), 3),
                 theta_mfe_pp=round(theta(m, "mfe_10d") * 100, 2),
                 theta_mfe_atr=round(theta(m, "mfe_atr_10d"), 3),
                 theta_mae_pp=round(theta(m, "mae_10d") * 100, 2),
                 theta_ret10_pp=round(theta(m, "ret_10d") * 100, 2),
                 d_pre_atr_pct=round(theta(m, "vol_anchor") * 100, 3),
                 d_pre_rv20_pct=round(theta(m, "rv20_anchor") * 100, 3),
                 d_liq_log=round(float(np.log(F.liq_anchor[m].median()
                                              / F.liq_anchor[~m].median())), 3),
                 d_price_log=round(float(np.log(F.price_anchor[m].median()
                                                / F.price_anchor[~m].median())), 3),
                 median_pre_atr_pct=round(float(F.vol_anchor[m].median() * 100), 3),
                 median_pre_rv20_pct=round(float(F.rv20_anchor[m].median() * 100), 3),
                 median_price=round(float(F.price_anchor[m].median()), 2))
        r["ret10_over_mfe"] = round(r["theta_ret10_pp"] / r["theta_mfe_pp"], 3) \
            if r["theta_mfe_pp"] else None
        rows.append(r)
        print(f"  C{ci} medoid {r['medoid']:<30} Z {r['z_rank']} · θMFE {r['theta_mfe_pp']:+.2f}pp "
              f"· θMAE {r['theta_mae_pp']:+.2f} · θRET10 {r['theta_ret10_pp']:+.2f} "
              f"· ret/mfe {r['ret10_over_mfe']} · θMFE_ATR {r['theta_mfe_atr']:+.3f} "
              f"· ΔpreATR {r['d_pre_atr_pct']:+.3f}pp · Δrv20 {r['d_pre_rv20_pct']:+.3f}pp "
              f"· Δlog-liq {r['d_liq_log']:+.2f} · Δlog-px {r['d_price_log']:+.2f}",
              flush=True)

    # ── family-wise max-statistic p-value, plus-one convention, stated as the rule ──
    mxnull = pd.read_parquet(os.path.join(D9.ROOT, "data",
                                          "t3_historical_maxnull.parquet")).max_z_null
    z_top = float(surv.z_rank_obs.max())
    B = int(len(mxnull))
    maxT = dict(
        rule="p = (1 + #{max-Z_null >= max Z_obs}) / (1 + B), the plus-one finite-"
             "permutation convention; B = 999 permutations registered before exposure",
        B=B, max_z_obs=round(z_top, 4),
        n_null_ge_obs=int((mxnull >= z_top).sum()),
        p_value=round((1 + int((mxnull >= z_top).sum())) / (1 + B), 6),
        scope="ONE family-wise number for the maximum statistic over all k claims — it is "
              "not a per-claim p-value and says nothing about the other survivors")
    print(f"  family-wise maxT p (plus-one, B={B}): {maxT['p_value']}", flush=True)

    # ── the two readings the characterization must state ──────────────────
    vol_reading = [
        (f"{r['medoid']}: raw θMFE {r['theta_mfe_pp']:+.2f}pp with pre-treatment ATR "
         f"{r['d_pre_atr_pct']:+.3f}pp and rv20 {r['d_pre_rv20_pct']:+.3f}pp vs controls; "
         f"ATR-normalized θMFE_ATR {r['theta_mfe_atr']:+.3f} — "
         + ("the association SURVIVES volatility normalization"
            if r["theta_mfe_atr"] > 0 else
            "the association DOES NOT survive volatility normalization"))
        for r in rows]
    ret_reading = [
        (f"{r['medoid']}: θRET10 {r['theta_ret10_pp']:+.2f}pp against θMFE "
         f"{r['theta_mfe_pp']:+.2f}pp — terminal capture "
         f"{r['ret10_over_mfe']}" if r["ret10_over_mfe"] is not None else
         f"{r['medoid']}: terminal capture undefined (θMFE 0)")
        for r in rows]

    # BUY occurrence — POST-HOC DESCRIPTIVE, labeled
    all_buy = int(ORDER.representative.str.contains("BUY").sum())
    surv_buy = int(surv.representative.str.contains("BUY").sum())

    body = dict(
        spec_id="T3_SURVIVOR_CHARACTERIZATION_V1", status="POST_EXPOSURE_DESCRIPTIVE",
        selection_rule="outcome-blind medoid selection WITHIN the post-exposure survivor "
                       "set — membership geometry only, tie-break lowest sealed j; the "
                       "survivor set itself is Y-selected and this wording says so",
        n_survivors=n, jaccard_threshold=JACC,
        n_clusters=len(clusters),
        n_singleton_clusters=int(sum(1 for cl in clusters if len(cl) == 1)),
        cluster_sizes=sorted((len(cl) for cl in clusters), reverse=True),
        cluster_structure=[[f"j{surv.j.iloc[i]}" for i in cl] for cl in clusters],
        containment=cont,
        nearest_neighbor_jaccard={f"j{surv.j.iloc[i]}": round(nnJ[i], 3) for i in range(n)},
        jaccard_matrix=[[round(float(Jm[a][b]), 3) for b in range(n)] for a in range(n)],
        medoid_characterization=rows,
        aggregate=dict(
            theta_mfe_median_pp=round(float(np.median([r["theta_mfe_pp"] for r in rows])), 2),
            theta_ret10_median_pp=round(float(np.median([r["theta_ret10_pp"] for r in rows])), 2),
            ret10_over_mfe_median=round(float(np.median([r["ret10_over_mfe"] for r in rows
                                                         if r["ret10_over_mfe"]])), 3)),
        buy_occurrence_POST_HOC_DESCRIPTIVE=dict(
            buy_in_grammar=all_buy, k=int(len(ORDER)), buy_in_survivors=surv_buy,
            label="within-T3 post-hoc description only — the survivor set is "
                  "outcome-selected, so this is not an enrichment test; no comparison "
                  "with any other family is made or implied here"),
        family_wise_maxT_p_value=maxT,
        volatility_scale_reading=vol_reading,
        terminal_return_reading=ret_reading,
        cross_family="NOT EXAMINED HERE — any recurrence of a structure across families "
                     "is a separate, separately-logged family-wide analysis with its own "
                     "multiplicity surface; the words replication, confirmation and "
                     "independent evidence are not used in this artifact",
        branch="T3 HISTORICAL CHARACTERIZATION CLOSED",
        seconds=round(time.time() - t0, 1))
    d = ART.seal(body, OUT, required=("spec_id","n_clusters","medoid_characterization"))
    print(f"\n  clusters {len(clusters)} · containment {cont or 'none'}")
    print(f"  BUY (post-hoc desc.): {surv_buy}/{n} survivors vs {all_buy}/853 grammar")
    print(f"  T3_SURVIVOR_CHARACTERIZATION_V1 · {d} · {time.time()-t0:.0f}s")

if __name__ == "__main__":
    main()
