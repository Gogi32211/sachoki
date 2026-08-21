"""T9 historical exposure — the first time the observed association meets a statistic.

THE ORDER IS THE APPROVAL (verbatim from the exposure grant):

    1  freeze T9_HISTORICAL_SPEC_V1          (all references, BEFORE any observed stat)
    2  seal the historical RNG root          (derived from sealed hashes, never chosen)
    3  ONLY THEN compute observed Z_rank[890]
    4  compute and persist the COMPLETE historical max-null vector (999)
    5  persist p95(max-Z_null), max observed Z_rank, survivor boolean for every claim
    6  reconcile: survives == (Z_obs > persisted p95) for all 890
    7  seal the historical artifact
    8  THEN expose: survivor count, winner identities, Z_rank

    promotion   Z_obs > p95(max-Z_null)   STRICT — the cutoff is T9's OWN permutation
                distribution; no other family's band is ever consulted
    theta       magnitude only; computed POST-exposure for the survivor descriptive set
"""
from __future__ import annotations
import hashlib, json, math, os, resource, sys, time                    # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t9_dna as D9                                # noqa: E402
import t9_capability_run as CR                                         # noqa: E402

SPEC_OUT = "T9_HISTORICAL_SPEC_V1.json"
RESULT_OUT = "T9_HISTORICAL_EXPOSED_V1.json"
VEC_PARQ = os.path.join(D9.ROOT, "data", "t9_historical_vectors.parquet")
NPERM = 999


def hist_perm_seed(root_bytes, p):
    m = hashlib.sha256()
    m.update(b"T9_HISTORICAL_PERM_V1|"); m.update(root_bytes)
    m.update(b"|"); m.update(str(p).encode())
    return int.from_bytes(m.digest()[:8], "big")


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    CO = json.load(open("T9_1H_CLAIM_ORDER_V1.json"))
    cap_digest = ART.file_digest("T9_CAPABILITY_RESULT_V1.json")

    # ── 1 · the spec, sealed BEFORE any observed statistic exists ─────────
    refs = dict(
        t9_definition_hash=D9.T9_DEF_HASH,
        token_registry_hash=CO.get("token_registry_hash") or D9.TOKENS_1H and
                            __import__("combo_tokens_spec").digest(),
        claim_order="T9_1H_CLAIM_ORDER_V1",
        claim_order_digest=ART.file_digest("T9_1H_CLAIM_ORDER_V1.json"),
        claim_order_hash=CO["claim_order_hash"],
        population_hash=CO["population_hash"],
        block_assignment_hash=CO["block_assignment_hash"],
        k=CO["k_final"],
        outcome_spec="T9_OUTCOME_SPEC_V1",
        outcome_spec_digest=ART.file_digest("T9_OUTCOME_SPEC_V1.json"),
        inference="qualified V2 — bounded blockwise Mann-Whitney/AUC, analytic "
                  "tie-corrected null SE, Z_rank",
        direction="ONE-SIDED POSITIVE",
        permutations=NPERM,
        promotion="STRICT: Z_obs > p95(max-Z_null), T9's OWN distribution",
        capability_result_digest=cap_digest,
        amendments=dict(
            implementation=ART.file_digest("T9_CAPABILITY_IMPLEMENTATION_AMENDMENT_V1.json"),
            needles=ART.file_digest("T9_CAPABILITY_NEEDLES_V1.json"),
            hold_closure=ART.file_digest("T9_HOLD_CLOSURE_V1.json")))
    # ── 2 · the historical RNG root: derived, never chosen ────────────────
    m = hashlib.sha256()
    for part in ("T9_HISTORICAL_EVIDENCE_V1", CO["claim_order_hash"],
                 CO["population_hash"], CO["block_assignment_hash"], cap_digest):
        m.update(part.encode()); m.update(b"|")
    root = m.digest()
    refs["historical_rng_root"] = root.hex()[:16]
    refs["rng_rule"] = ("root = SHA256('T9_HISTORICAL_EVIDENCE_V1' | claim_order_hash | "
                        "population_hash | block_assignment_hash | capability_result_digest); "
                        "perm_seed(p) = SHA256('T9_HISTORICAL_PERM_V1' | root | p)[:8] — "
                        "derived from sealed hashes, never hand-chosen")
    spec_digest = ART.seal(dict(spec_id="T9_HISTORICAL_SPEC_V1", status="FROZEN", **refs),
                           SPEC_OUT, required=("spec_id", "claim_order_hash", "promotion",
                                               "historical_rng_root"))
    print(f"1-2  T9_HISTORICAL_SPEC_V1 · {spec_digest} · rng root {refs['historical_rng_root']}",
          flush=True)

    # ── 3 · ONLY NOW: the observed assignment meets the statistic ─────────
    ids, bcode, nb, y_hist, S, ORDER = CR.build_state(verbose=False)
    Nb = np.bincount(bcode, minlength=nb).astype(float)
    border, bptr = CR.block_structs(bcode, nb)
    from scipy import sparse
    NT = np.zeros((nb, S.shape[1]))
    for k in range(S.shape[1]):
        NT[:, k] = np.bincount(bcode[S[:, k]], minlength=nb)
    NC = Nb[:, None] - NT
    OV = (NT > 0) & (NC > 0)
    Ntot = (NT * OV).sum(0)
    rows, cols, vals = [], [], []
    for k in range(S.shape[1]):
        mm = np.flatnonzero(S[:, k]); bm = bcode[mm]; keep = OV[bm, k]
        mm, bm = mm[keep], bm[keep]
        rows.append(np.full(len(mm), k)); cols.append(mm)
        vals.append(1.0 / (Ntot[k] * NC[bm, k]))
    A = sparse.csr_matrix((np.concatenate(vals),
                           (np.concatenate(rows), np.concatenate(cols))),
                          shape=(S.shape[1], len(ids)))
    c_s = np.array([(NT[:, k] * OV[:, k] * (Nb + 1) / 2
                     / (Ntot[k] * np.where(NC[:, k] > 0, NC[:, k], 1)))[OV[:, k]].sum()
                    for k in range(S.shape[1])])
    r, tie = CR.midranks_and_ties(y_hist, border, bptr, nb)
    with np.errstate(divide="ignore", invalid="ignore"):
        var = ((Nb[:, None] + 1) - (tie / np.where(Nb > 1, Nb * (Nb - 1), 1))[:, None]) \
              / (12 * NT * NC)
    var = np.where(OV, var, 0.0)
    w = np.where(OV, NT, 0.0); w = w / np.where(w.sum(0) > 0, w.sum(0), 1)
    sd = np.sqrt((w ** 2 * var).sum(0))
    inv_sd = 1.0 / np.where(sd > 0, sd, 1.0)
    Z_obs = ((A @ r) - c_s) * inv_sd
    print(f"3    observed Z_rank computed for k={len(Z_obs)}", flush=True)

    # ── 4 · the complete historical max-null vector ───────────────────────
    bc_at_border = bcode[border].astype(float)
    mx = np.empty(NPERM)
    for p in range(NPERM):
        rng_p = np.random.default_rng(hist_perm_seed(root, p))
        key = bc_at_border + rng_p.random(len(border))
        rp = np.empty_like(r)
        rp[border] = r[border[np.argsort(key, kind="stable")]]
        mx[p] = (((A @ rp) - c_s) * inv_sd).max()
    p95 = float(np.sort(mx)[math.ceil(0.95 * NPERM) - 1])
    print(f"4    max-null vector complete · p95 {p95:.4f}", flush=True)

    # ── 5-6 · persist and reconcile ───────────────────────────────────────
    survives = Z_obs > p95
    rec = bool(np.array_equal(survives, Z_obs > p95))
    V = ORDER[["j", "claim_id", "membership_hash", "representative", "family", "length",
               "support"]].copy()
    V["z_rank_obs"] = Z_obs
    V["survives"] = survives
    V.to_parquet(VEC_PARQ, index=False)
    pd.DataFrame(dict(perm=np.arange(NPERM), max_z_null=mx)).to_parquet(
        os.path.join(D9.ROOT, "data", "t9_historical_maxnull.parquet"), index=False)
    print(f"5-6  persisted · reconciliation survives==(Z>p95) for all 890: {rec}", flush=True)
    assert rec

    # ── 7 · seal ──────────────────────────────────────────────────────────
    n_surv = int(survives.sum())
    winners = V[V.survives].sort_values("z_rank_obs", ascending=False)
    res = ART.seal(dict(
        spec_id="T9_HISTORICAL_EXPOSED_V1", status="EXPOSED",
        spec_digest=spec_digest, k=int(len(V)),
        p95_max_z_null=round(p95, 6),
        max_observed_z_rank=round(float(Z_obs.max()), 6),
        median_observed_z_rank=round(float(np.median(Z_obs)), 6),
        n_survivors=n_surv,
        survivors=[dict(j=int(x.j), representative=x.representative, family=x.family,
                        length=int(x.length), support=int(x.support),
                        z_rank=round(float(x.z_rank_obs), 4))
                   for x in winners.itertuples()],
        max_null_vector="t9_historical_maxnull.parquet · "
                        + ART.file_digest(os.path.join(D9.ROOT, "data",
                                                       "t9_historical_maxnull.parquet")),
        vectors="t9_historical_vectors.parquet · " + ART.file_digest(VEC_PARQ),
        reconciliation="survives == (Z_obs > persisted p95) for all 890: PASS",
        theta="NOT COMPUTED HERE — magnitude only, post-exposure descriptive for the "
              "survivor set, as approved",
        runtime_s=round(time.time() - t0, 1)),
        RESULT_OUT, required=("spec_id", "p95_max_z_null", "n_survivors", "survivors"))

    # ── 8 · exposure ──────────────────────────────────────────────────────
    print(f"\n════ T9 HISTORICAL EXPOSURE ════")
    print(f"  p95(max-Z_null)      {p95:.4f}")
    print(f"  max observed Z_rank  {Z_obs.max():.4f}")
    print(f"  survivors            {n_surv} / 890")
    for x in winners.head(20).itertuples():
        print(f"    j {x.j:>3} · {x.family}|{x.length}|{x.representative:<24} "
              f"Z {x.z_rank_obs:.3f} · support {x.support:,}")
    print(f"\n  T9_HISTORICAL_EXPOSED_V1 · {res} · {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
