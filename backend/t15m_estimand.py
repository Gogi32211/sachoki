"""{F}_15M_ESTIMAND_V1 and {F}_15M_CAPABILITY_PROTOCOL_V1 — the last two seals before Y.

No new methodology. Every clause is the T5 15m precedent, with this family's own frozen
numbers substituted: same primary outcome, same rank statistic, same search-wide max-Z
multiplicity, same nullization and injection semantics, same 3 x 6 x 20 capability with 999
inner permutations.

The needle diagnostics are RECOMPUTED here from the same code path that built the order,
and each one's membership hash is asserted equal to the sealed hash. A needle whose identity
could not be reproduced from X would not be a needle.

The capability is NOT launched. This module ends at the seal.

    usage:  python t15m_estimand.py t9
"""
from __future__ import annotations
import hashlib, importlib, json, os, sys, time                          # noqa: E402
import numpy as np, pandas as pd                                        # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                               # noqa: E402
import t15m_x as X, t15m_gates as G                                     # noqa: E402

RULING = "T15M_ELIGIBILITY_RULING_V1.json"
DELTAS_PP = [0.5, 1.0, 2.0, 3.0, 4.0, 5.0]
WORLDS, N_PERM = 20, 999


def needle_diagnostics(fam: str, order: dict) -> dict:
    """Rebuild the three needle memberships from X and prove each hash."""
    F = fam.upper()
    D = importlib.import_module(f"{fam}_dna")
    E1 = importlib.import_module(f"{fam}_sequence_estimand")
    E, M, T = X.load_opening_hour(fam, D, verbose=False)
    REG, keep = X.searchable(M, T, D)
    tok = [D.TOKENS_1H[i].token_id for i in keep]
    tix = {t: i for i, t in enumerate(tok)}

    piv = M.pivot_table(index="episode_id", columns="pos", values="et_time",
                        aggfunc="first")
    for p in (1, 2, 3, 4):
        if p not in piv.columns:
            piv[p] = np.nan
    ep_ids = piv.index.to_numpy()
    eix = {e: i for i, e in enumerate(ep_ids)}
    nE = len(ep_ids)
    B = np.zeros((4, nE, len(tok)), dtype=bool)
    B[M.pos.to_numpy() - 1, M.episode_id.map(eix).to_numpy()] = \
        T.to_numpy(dtype=bool)[:, keep]
    first = M.drop_duplicates("episode_id").set_index("episode_id").loc[ep_ids]
    ep_tk = pd.factorize(first.ticker)[0]
    ep_dt = pd.factorize(first.sess_date)[0]

    P = X.build_population(fam, D, E1, ep_ids)
    pop = set(P.episode_id)
    idx_pop = np.flatnonzero(np.array([e in pop for e in ep_ids]))
    blk = P.set_index("episode_id").block.reindex(ep_ids[idx_pop]).to_numpy()
    bfac, _ = pd.factorize(blk)
    nb = int(bfac.max()) + 1
    blk_size = np.bincount(bfac, minlength=nb).astype(float)
    tk_pop, dt_pop = ep_tk[idx_pop], ep_dt[idx_pop]

    out = {}
    for q in ("q10", "q50", "q90"):
        n = order["needles"][q]
        pf, toks = n["representative"].split("|")
        a, b = pf.split("→")
        ti, tj = toks.split("→")
        v = B[int(a[1:]) - 1][:, tix[ti]] & B[int(b[1:]) - 1][:, tix[tj]]
        vp = v[idx_pop]
        nt_b = np.bincount(bfac, weights=vp.astype(float), minlength=nb)
        ov = (nt_b > 0) & (blk_size - nt_b > 0)
        inov = ov[bfac]
        m = vp & inov
        e = np.flatnonzero(m)
        h = hashlib.sha256(e.tobytes()).hexdigest()[:16]
        assert h == n["membership_hash"], f"{q}: rebuilt {h} != sealed {n['membership_hash']}"
        assert int(m.sum()) == n["support"], q
        out[q] = dict(
            quantile=float(q[1:]) / 100.0, nearest_rank_index=n["index_zero_based"],
            sealed_j=n["j"], claim_uid=n["claim_uid"],
            membership_hash=h, membership_hash_rebuilt_from_X=True,
            representative=n["representative"], position_family=n["position_family"],
            treated_overlap=int(m.sum()), control_overlap=int(((~vp) & inov).sum()),
            n_overlap_blocks=int(ov.sum()),
            n_tickers=int((np.bincount(tk_pop[e], minlength=ep_tk.max() + 1) > 0).sum()),
            n_dates=int((np.bincount(dt_pop[e], minlength=ep_dt.max() + 1) > 0).sum()),
            n_names=1 + len([x for x in (n.get("aliases") or "").split(";") if x]))
        print(f"    {q}: j {n['j']:>6,} · {n['representative']:<34} · treated "
              f"{int(m.sum()):>6,} · control {int(((~vp) & inov).sum()):>7,} · blocks "
              f"{int(ov.sum()):>5,} · hash {h} REBUILT", flush=True)
    return out


def main(fam: str):
    t0 = time.time()
    F = fam.upper()
    D = importlib.import_module(f"{fam}_dna")
    order = json.load(open(f"{F}_15M_CLAIM_ORDER_V1.json"))
    kc = json.load(open(f"{F}_15M_K_CLOSURE_V1.json"))
    gates = json.load(open(f"{F}_15M_PRE_Y_GATES_V1.json"))
    assert gates["result"] == "PASS", "pre-Y gates have not passed"
    k = order["k"]
    print(f"{F} 15m estimand + capability protocol · k {k:,}", flush=True)

    nd = needle_diagnostics(fam, order)
    root = G.rng_root(F)

    governing = dict(
        prespec=ART.file_digest(f"{F}_15M_PRESPEC_V1.json"),
        definition_hash=getattr(D, f"{F}_DEF_HASH"),
        x_only=ART.file_digest(f"{F}_15M_X_ONLY_V1.json"),
        k_closure=ART.file_digest(f"{F}_15M_K_CLOSURE_V1.json"),
        eligibility_ruling=ART.file_digest(RULING),
        claim_order=ART.file_digest(f"{F}_15M_CLAIM_ORDER_V1.json"),
        pre_y_gates=ART.file_digest(f"{F}_15M_PRE_Y_GATES_V1.json"),
        population_hash=kc["surfaces"]["population_digest"],
        block_assignment_hash=kc["surfaces"]["blocks_digest"],
        claim_order_hash=order["order"]["claim_order_hash"],
        rank_implementation=ART.file_digest("t5_rank_engine.py"),
        order_code=ART.file_digest("t15m_order.py"),
        x_code=ART.file_digest("t15m_x.py"))

    est = ART.seal(dict(
        spec_id=f"{F}_15M_ESTIMAND_V1", status="FROZEN_BEFORE_ANY_15M_Y_ACCESS", family=F,
        governing=governing, k=k,
        eligibility_surface="FINAL_ESTIMAND_POPULATION",
        support_variable="t_ov — treated overlap on the final estimand population",
        population=dict(episodes=kc["population_census"]["ESTIMAND_POPULATION"],
                        blocks=kc["population_census"]["blocks"],
                        census=kc["population_census"]),
        blocks=dict(dimensions=["decision_date", "pre_liquidity", "pre_volatility"],
                    source="t5_sequence_estimand.blocks on the family's own episodes",
                    overlap_rule="a block contributes only with >=1 treated AND >=1 control"),
        primary_outcome="MFE_10D",
        secondary_outcome=dict(name="MFE_ATR_10D",
                               role="registered secondary CHARACTERIZATION only; cannot "
                                    "alter promotion"),
        statistic="one-sided Z_rank = R_s / sqrt(V_s), bounded blockwise Mann-Whitney / AUC, "
                  "analytically studentized",
        direction="one-sided positive",
        multiplicity="search-wide max Z_rank over all k claims, ONE permutation mapping "
                     "applied to every claim",
        n_perm=N_PERM,
        promotion="Z_rank_obs > p95( max-null Z_rank ) from THIS run's own permutations, "
                  "STRICTLY greater",
        raw_theta="magnitude only; never a promotion criterion",
        y_status="NO OUTCOME VALUE READ — this artifact defines the estimand, it does not "
                 "evaluate it",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        f"{F}_15M_ESTIMAND_V1.json",
        required=("spec_id", "governing", "k", "statistic", "primary_outcome"),
        supersede=os.path.exists(f"{F}_15M_ESTIMAND_V1.json"))

    prot = ART.seal(dict(
        spec_id=f"{F}_15M_CAPABILITY_PROTOCOL_V1",
        status="FROZEN_BEFORE_ANY_15M_Y_ACCESS", family=F,
        governing=dict(governing, estimand=est),
        search_universe=dict(k=k, families=["M1→M2", "M2→M3", "M3→M4"],
                             deferred=["M1→M2→M3", "M2→M3→M4"]),
        selection_statistic="bounded blockwise Mann-Whitney / AUC, analytically studentized",
        multiplicity="search-wide max Z_rank over all k claims, one permutation mapping "
                     "applied to every claim",
        direction="one-sided positive",
        needles=nd,
        needle_selection_rule=dict(
            source="sealed 15m claim order",
            variable="treated_overlap on the final estimand population",
            order="support ascending, tie-break sealed j",
            nearest_rank="idx(q) = ceil(q * k) - 1",
            independence="1H survivors play no part in needle selection"),
        delta_grid_pp=DELTAS_PP, worlds=WORLDS, n_perm_inner=N_PERM,
        logical_cells=3 * len(DELTAS_PP) * WORLDS,
        outer_noise="real MFE_10D with the sequence<->Y association destroyed first by "
                    "permutation within the frozen blocks",
        injection="additive shift of the raw MFE_10D on the needle's treated episodes; "
                  "never on U or R",
        detection="Z_rank(needle, delta) > p95( max_s Z_rank ) in the same inner-null world",
        denominator=dict(
            form="exact combinatorial null variance with the rank-sum tie correction",
            formula="Var_0(U_sb) = [ (N+1) - sum(t^3-t)/(N(N-1)) ] / (12 n_t n_c)",
            recomputation="RECOMPUTED PER (needle, world, delta) on that injected multiset — "
                          "injection can break or create ties, and the conditional "
                          "randomization variance must belong to the multiset it divides",
            fixed_within="the 999 inner permutations of a (needle, world, delta) cell share "
                         "that one SE",
            not_empirical="never estimated from the permutation numerator"),
        rng=dict(root=root.hex()[:16],
                 rule="SHA256 over the sealed chain (prespec, x_only, k_closure, ruling, "
                      "claim_order) — derived, never chosen",
                 manifest_digest=G.manifest_digest(F),
                 n_seeds=len(G.seed_manifest(F, root)),
                 namespaces=["T15M_NULL_V1", "T15M_PERM_V1"],
                 builtin_hash_on_stochastic_path=0),
        acceptance=dict(
            at_0_5pp="descriptive sensitivity only",
            at_1pp="reported per needle; the T5 precedent read q50 +1pp 20/20",
            note="the capability measures finite-suite sensitivity under a registered "
                 "homogeneous additive shift. It is NOT guaranteed power against any real "
                 "alternative and is never the promotion cutoff."),
        frozen_and_may_not_change=[
            "support floors", "2-bar scope", "tokens", "position families",
            f"k = {k:,}", "worlds and permutations once the capability begins",
            "needle identities now that they are sealed", "delta grid",
            "the eligibility surface fixed by T15M_ELIGIBILITY_RULING_V1"],
        forbidden_claims=[
            "the 15m result confirms or validates any 1H claim",
            f"15m is an independent replication (same {F} episode universe, same 10-day "
            "outcome)",
            "a lower band is more power"],
        not_launched="the capability run is NOT started here",
        y_status="NO OUTCOME VALUE READ",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        f"{F}_15M_CAPABILITY_PROTOCOL_V1.json",
        required=("spec_id", "governing", "needles", "delta_grid_pp", "detection"),
        supersede=os.path.exists(f"{F}_15M_CAPABILITY_PROTOCOL_V1.json"))

    print(f"\n  {F}_15M_ESTIMAND_V1            · {est}")
    print(f"  {F}_15M_CAPABILITY_PROTOCOL_V1 · {prot}")
    print(f"  rng root {root.hex()[:16]} · manifest {G.manifest_digest(F)} · "
          f"{(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    for f in (sys.argv[1:] or ["t9"]):
        main(f)
