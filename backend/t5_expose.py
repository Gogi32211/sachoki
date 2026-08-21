"""EXPOSE — T5_1H_HISTORICAL_V2. The preregistered four blocks, in order, and nothing else.

The promotion rule is applied EXACTLY as frozen: strictly greater than the max-null 95th
percentile. A claim landing exactly on p95 does not survive. The comparison operator is not
a detail to be settled after seeing the numbers.

NO POST-HOC COLUMNS. Not "best theta among non-survivors", not "best family", not "best
2-bar", not "best high-support claim". Those are legitimate later descriptive questions and
illegitimate now: the first verdict must come from the frozen search-wide rule alone, or the
rule was never binding.

Z_rank is the evidence statistic. theta is the magnitude. Neither substitutes for the other,
and the secondary MFE_ATR characterization cannot alter promotion status.
"""
from __future__ import annotations
import os
for _v in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
           "VECLIB_MAXIMUM_THREADS"):
    os.environ[_v] = "1"
import json, sys, time                                                # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_capability_run as R, t5_compute_qual as Q, t5_jit_engine as JE   # noqa: E402

SEALED = json.load(open("T5_1H_SEALED_HISTORICAL_V2.json"))
V2 = json.load(open("SEQUENCE_INFERENCE_V2.json"))
CAP = json.load(open("T5_CAPABILITY_RANK_V2.json"))
DATA = os.path.join(os.path.dirname(HERE), "data")
OUT = os.path.join(HERE, "T5_1H_HISTORICAL_V2_EXPOSED.json")


def main():
    t0 = time.time()
    OB = pd.read_parquet(os.path.join(DATA, "t5_hist_observed.parquet")).sort_values("j")
    ZP = pd.read_parquet(os.path.join(DATA, "t5_hist_Z_rank_perm.parquet")).to_numpy()
    if ZP.shape != (SEALED["n_permutations"], 840) or len(OB) != 840:
        raise RuntimeError("sealed artifact shape drift")

    # ── 1 · SEARCH-WIDE RESULT ──────────────────────────────────────────────
    max_null = ZP.max(axis=1)
    p95 = float(np.percentile(max_null, 95))
    z = OB.Z_rank_obs.to_numpy()
    surv = z > p95                                    # STRICTLY greater, exactly as frozen
    n_surv = int(surv.sum())
    print("EXPOSE — T5_1H_HISTORICAL_V2")
    print(f"  sealed artifact {SEALED['artifact_hash']} · inference {V2['spec_digest']} "
          f"· capability {CAP['spec_digest']}\n")
    print("1 · SEARCH-WIDE RESULT")
    print(f"    k                       840")
    print(f"    max observed Z_rank     {z.max():.3f}")
    print(f"    max-null p95            {p95:.3f}")
    print(f"    n claims > p95          {n_surv}")
    print(f"    rule                    Z_rank > p95(max-null), one-sided positive, "
          f"strict inequality")

    # ── 2 · TOP 20 BY Z_rank ────────────────────────────────────────────────
    CL = pd.read_parquet(os.path.join(DATA, "t5_sequence_estimand_claims.parquet"))
    CL = CL[["claim", "max_treated_ticker_share", "treated_ticker_hhi",
             "median_episodes_per_treated_ticker"]]
    T = OB.assign(status=np.where(surv, "SURVIVES_SEARCH_WIDE", "not_promoted")) \
          .sort_values("Z_rank_obs", ascending=False).head(20) \
          .merge(CL, left_on="claim_id", right_on="claim", how="left")
    T["family"] = T.claim_id.str.split("|").str[0]
    T["length"] = T.claim_id.str.split("|").str[1].astype(int)
    print("\n2 · TOP 20 BY Z_rank")
    print(f"    {'j':>4} {'claim':<34} {'fam':<5} {'supp':>6} {'Z_rank':>7} "
          f"{'θ pp':>7} {'trtMed':>7} {'ctlMed':>7} {'tkMax':>6} {'hhi':>7}  status")
    for r in T.itertuples():
        print(f"    {int(r.j):>4} {r.claim_id:<34} {r.family[:5]:<5} {int(r.support):>6,} "
              f"{r.Z_rank_obs:>7.3f} {r.theta_raw_obs:>7.2f} "
              f"{r.weighted_treated_median:>7.2f} {r.weighted_control_median:>7.2f} "
              f"{r.max_treated_ticker_share:>6.4f} {r.treated_ticker_hhi:>7.5f}  {r.status}")

    # ── 3 · CAPABILITY CONTEXT ──────────────────────────────────────────────
    L = pd.read_parquet(os.path.join(DATA, "t5_capability_rank_v2_ledger.parquet"))
    NW = CAP["worlds"]
    print("\n3 · CAPABILITY CONTEXT — registered pre-exposure, homogeneous additive "
          "MFE-shift alternative")
    print(f"    {'needle':<6} {'support':>8}  " +
          "  ".join(f"{d:>+5.1f}" for d in CAP["delta_grid_pp"]))
    cap = {}
    for n, nd in CAP["needles"].items():
        cells = [f"{int(L[(L.needle==n)&(L.delta_pp==d)].detected.sum())}/{NW}"
                 for d in CAP["delta_grid_pp"]]
        cap[n] = dict(claim=nd["claim"], support=nd["treated_overlap"],
                      surface=dict(zip([f"{d:+.1f}" for d in CAP["delta_grid_pp"]], cells)))
        print(f"    {n:<6} {nd['treated_overlap']:>8,}  " +
              "  ".join(f"{c:>5}" for c in cells))

    # ── 4 · SECONDARY CHARACTERIZATION ──────────────────────────────────────
    y, b, nb, S, claims, order = R.build_state(verbose=False)
    flat = Q.FlatState(claims); del claims
    import gc; gc.collect()
    # Align the secondary outcome to the SEALED population order. build_state records that
    # order after it has verified the population hash, so reusing it cannot silently pick up
    # a different or reordered population.
    ids = R.LAST_EPISODE_IDS
    if len(ids) != len(y):
        raise RuntimeError("secondary alignment: episode order length mismatch")
    A = pd.read_parquet(os.path.join(DATA, "t5_episode_outcomes.parquet"),
                        columns=["episode_id", "mfe_atr_10d"])
    ya = A.set_index("episode_id").mfe_atr_10d.reindex(ids).to_numpy(float)
    n_nan = int(np.isnan(ya).sum())
    print("\n4 · SECONDARY CHARACTERIZATION — MFE_ATR_10D · cannot alter promotion status")
    if n_nan:
        th_atr = None
        print(f"    NOT COMPUTED — mfe_atr_10d missing for {n_nan:,} of {len(ya):,} sealed "
              f"episodes. A silently imputed secondary would be worse than none.")
    else:
        zj = np.zeros(len(ya)); DL0 = np.array([0.0])
        J = JE.gate(flat, flat.theta, ya, zj, DL0)
        th_atr = J.theta6(ya, zj, DL0)[0].copy()
        print(f"    {'j':>4} {'claim':<34} {'θ_MFE pp':>9} {'θ_MFE_ATR':>10}")
        for r in T.itertuples():
            print(f"    {int(r.j):>4} {r.claim_id:<34} {r.theta_raw_obs:>9.2f} "
                  f"{th_atr[int(r.j)]:>10.3f}")

    rep = dict(spec_id="T5_1H_HISTORICAL_V2_EXPOSED",
               sealed_artifact_hash=SEALED["artifact_hash"],
               inference_digest=V2["spec_digest"], capability_digest=CAP["spec_digest"],
               k=840, n_permutations=SEALED["n_permutations"],
               max_observed_Z_rank=round(float(z.max()), 4),
               max_null_p95=round(p95, 4), n_claims_above_p95=n_surv,
               promotion_rule="Z_rank > p95(max-null), one-sided positive, strict",
               top20=T[["j", "claim_id", "family", "length", "support", "Z_rank_obs",
                        "status", "theta_raw_obs", "weighted_treated_median",
                        "weighted_control_median", "max_treated_ticker_share",
                        "treated_ticker_hhi", "median_episodes_per_treated_ticker"]]
               .round(5).to_dict("records"),
               capability_context=cap,
               secondary_mfe_atr_10d=({int(r.j): round(float(th_atr[int(r.j)]), 4)
                                       for r in T.itertuples()}
                                      if th_atr is not None else "NOT COMPUTED"),
               interpretation=dict(
                   zero_survivors="informative negative historical result under the "
                                  "registered V2 test, within the known capability bounds",
                   some_survivors="historical association survived search-wide multiplicity "
                                  "control",
                   neither="neither outcome means a validated trading edge",
                   statistics="Z_rank is the evidence statistic, theta is the magnitude; "
                              "neither substitutes for the other",
                   theta_interval="no selection-adjusted interval on theta has been built; "
                                  "any bootstrap CI would be descriptive only"),
               minutes=round((time.time() - t0) / 60, 1))
    json.dump(rep, open(OUT, "w"), indent=2, default=str)
    print(f"\n  WROTE {OUT}")


if __name__ == "__main__":
    main()
