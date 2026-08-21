"""T9 post-exposure survivor characterization — closes the historical branch. Nothing else.

    survivor set is Y-SELECTED, so the medoid rule is stated precisely:
    outcome-blind medoid selection WITHIN the post-exposure survivor set —
    membership geometry only; never Z, theta, support or return.

DOES NOT: alter selection · run new search · compare T9 vs T5 inferentially ·
touch T3/T1/T4 · expose any other family's outcomes.
"""
from __future__ import annotations
import hashlib, json, os, sys, time
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t9_dna as D9
import t9_capability_run as CR, t9_sequence_estimand as E9
import t5_sequence_estimand as E5
import duckdb

OUT = "T9_SURVIVOR_CHARACTERIZATION_V1.json"
JACC = 0.50

def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    V = pd.read_parquet(os.path.join(D9.ROOT, "data", "t9_historical_vectors.parquet"))
    surv = V[V.survives].sort_values("j").reset_index(drop=True)
    ids, bcode, nb, y, S, ORDER = CR.build_state(verbose=False)
    Nb = np.bincount(bcode, minlength=nb)

    # ── 1 · exact OVERLAP-RESTRICTED memberships for the 6 (the sealed definition) ──
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
    P = pd.read_parquet(os.path.join(D9.ROOT, "data", "t9_estimand_population.parquet"))
    P = P.sort_values(["block","episode_id"], kind="stable").reset_index(drop=True)
    A5 = E5.anchors(P.rename(columns={"t9_date":"t5_date"}))
    F = P.merge(OC, on="episode_id").merge(A5[["episode_id","liq_anchor","vol_anchor"]],
                                           on="episode_id")
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
                 d_liq_ratio=round(float(np.log(F.liq_anchor[m].median()
                                    / F.liq_anchor[~m & (np.ones(len(m),bool))].median())), 3))
        r["ret10_over_mfe"] = round(r["theta_ret10_pp"] / r["theta_mfe_pp"], 3) \
            if r["theta_mfe_pp"] else None
        rows.append(r)
        print(f"  C{ci} medoid {r['medoid']:<30} Z {r['z_rank']} · θMFE {r['theta_mfe_pp']:+.2f}pp "
              f"· θMAE {r['theta_mae_pp']:+.2f} · θRET10 {r['theta_ret10_pp']:+.2f} "
              f"· ret/mfe {r['ret10_over_mfe']} · ΔpreATR {r['d_pre_atr_pct']:+.3f}pp "
              f"· θMFE_ATR {r['theta_mfe_atr']:+.3f}", flush=True)

    # BUY enrichment — POST-HOC DESCRIPTIVE, labeled
    all_buy = int(ORDER.representative.str.contains("BUY").sum())
    surv_buy = int(surv.representative.str.contains("BUY").sum())

    body = dict(
        spec_id="T9_SURVIVOR_CHARACTERIZATION_V1", status="POST_EXPOSURE_DESCRIPTIVE",
        selection_rule="outcome-blind medoid selection WITHIN the post-exposure survivor "
                       "set — membership geometry only, tie-break lowest sealed j; the "
                       "survivor set itself is Y-selected and this wording says so",
        n_survivors=n, jaccard_threshold=JACC,
        n_clusters=len(clusters),
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
        buy_enrichment_POST_HOC_DESCRIPTIVE=dict(
            buy_in_890=all_buy, buy_in_survivors=surv_buy,
            label="post-hoc descriptive only — the survivor set is outcome-selected and "
                  "this is not an enrichment test"),
        t5_echo_DESCRIPTIVE_ONLY=dict(
            observations=["PREV_INTRADAY|2|BUY→Z2 appears verbatim among both T5 and T9 "
                          "survivors", "CROSS_DAY VOL_W→* appears in both families"],
            label="cross-family STRUCTURAL RECURRENCE — same market, same period, same "
                  "token ecosystem, overlapping tickers/dates; NOT replication, NOT "
                  "confirmation, NOT independent evidence; formal comparison is a "
                  "separate, separately-logged family-wide analysis"),
        branch="T9 HISTORICAL CHARACTERIZATION CLOSED",
        seconds=round(time.time() - t0, 1))
    d = ART.seal(body, OUT, required=("spec_id","n_clusters","medoid_characterization"))
    print(f"\n  clusters {len(clusters)} · containment {cont or 'none'}")
    print(f"  BUY (post-hoc desc.): {surv_buy}/6 survivors vs {all_buy}/890 grammar")
    print(f"  T9_SURVIVOR_CHARACTERIZATION_V1 · {d} · {time.time()-t0:.0f}s")

if __name__ == "__main__":
    main()
