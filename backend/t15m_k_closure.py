"""{F}_15M_K_CLOSURE_V1 — is the enumerated k a FINAL-ESTIMAND k, or a pre-estimand one?

The X run reported, per family, `support-qualified names = classes + aliases`. That identity
is true by construction and therefore proves nothing on its own. The question the closure
actually has to answer is WHICH SURFACE each side of it was computed on.

    S0  ENUMERATION SURFACE       every opening-hour-observable episode
    S1  ESTIMAND POPULATION       S0 & outcome-status AVAILABLE & anchored & n20 >= 20
    S2  FINAL ESTIMAND SURFACE    S1 restricted to blocks that hold treated AND control
                                  — the overlap-restricted membership, which is the only
                                  surface the statistic can ever see

The 1H predicate that every one of these three families is already sealed under evaluates
the floors on S2: treated floor AND CONTROL floor on the overlap rows, tickers, dates and
max-date-share all counted on the overlap rows (t9/t3/t1_sequence_estimand.support_eligible
plus the three floors). The 15m runner hashed memberships on S2 but applied its floors on
S0 — so its k is membership-distinct on the right surface while its ELIGIBILITY was decided
on the wrong one.

This module recomputes both surfaces for every candidate and reports:

    restriction_merges        S0-distinct memberships that become identical on S2
    restriction_splits        S0-identical memberships that become distinct on S2
                              (must be 0: S2 membership is a deterministic function of S0
                              membership, so equal inputs cannot produce unequal outputs)
    whole_classes_removed     S2 classes dropped entirely by the S2 floors
    partial_class_removals    S2 classes where some names are dropped and others kept
                              (must be 0: every floor input is a function of the S2
                              membership, which is by definition shared inside a class —
                              the class-whole removal invariant)

No Y value is read. Only path_status_10d, and only as a status.

    usage:  python t15m_k_closure.py [t9 t3 t1]
"""
from __future__ import annotations
import hashlib, importlib, json, os, sys, time                        # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402
import t15m_x as X                                                    # noqa: E402

PAIRS = X.PAIRS


def _h(idx: np.ndarray) -> str:
    return hashlib.sha256(idx.tobytes()).hexdigest()[:16]


def analyse(fam: str):
    t0 = time.time()
    F = fam.upper()
    D = importlib.import_module(f"{fam}_dna")
    G1 = importlib.import_module(f"{fam}_sequence_grammar")
    E1 = importlib.import_module(f"{fam}_sequence_estimand")
    sealed = json.load(open(f"{F}_15M_X_ONLY_V1.json"))
    print(f"{F} 15m k-closure · sealed X {ART.file_digest(f'{F}_15M_X_ONLY_V1.json')}",
          flush=True)

    E, M, T = X.load_opening_hour(fam, D)
    REG, keep = X.searchable(M, T, D)
    tok = [D.TOKENS_1H[i].token_id for i in keep]

    piv = M.pivot_table(index="episode_id", columns="pos", values="et_time",
                        aggfunc="first")
    for p in (1, 2, 3, 4):
        if p not in piv.columns:
            piv[p] = np.nan
    have = piv[[1, 2, 3, 4]].notna().to_numpy()
    full_m1_m4 = int(have.all(axis=1).sum())
    any_pair = have[:, 0] & have[:, 1]
    for a, b in ((1, 2), (2, 3)):
        any_pair = any_pair | (have[:, a] & have[:, b])
    n_any_pair = int(any_pair.sum())

    ep_ids = piv.index.to_numpy()
    eix = {e: i for i, e in enumerate(ep_ids)}
    nE = len(ep_ids)
    B = np.zeros((4, nE, len(tok)), dtype=bool)
    arr = T.to_numpy(dtype=bool)[:, keep]
    B[M.pos.to_numpy() - 1, M.episode_id.map(eix).to_numpy()] = arr

    first = M.drop_duplicates("episode_id").set_index("episode_id").loc[ep_ids]
    ep_tk = pd.factorize(first.ticker)[0]
    ep_dt = pd.factorize(first.sess_date)[0]
    n_tk, n_dt = int(ep_tk.max()) + 1, int(ep_dt.max()) + 1
    MIN_EP, MIN_TK, MIN_DT = G1.MIN_EP, G1.MIN_TK, G1.MIN_DT
    MAX_DS = G1.MAX_DATE_SHARE

    # ── the surfaces ──────────────────────────────────────────────────────
    P = X.build_population(fam, D, E1, ep_ids)
    pop = set(P.episode_id)
    in_pop = np.array([e in pop for e in ep_ids])
    idx_pop = np.flatnonzero(in_pop)
    blk = P.set_index("episode_id").block.reindex(ep_ids[idx_pop]).to_numpy()
    bfac, _ = pd.factorize(blk)
    nb = int(bfac.max()) + 1
    blk_size = np.bincount(bfac, minlength=nb).astype(float)
    tk_pop, dt_pop = ep_tk[idx_pop], ep_dt[idx_pop]
    n_pair_estimand = int(any_pair[idx_pop].sum())
    n_full_estimand = int(have[idx_pop].all(axis=1).sum())

    pop_hash = hashlib.sha256("|".join(sorted(P.episode_id)).encode()).hexdigest()[:16]
    blk_hash = hashlib.sha256(
        "|".join(sorted(P.episode_id + ":" + P.block)).encode()).hexdigest()[:16]
    same_surface = dict(
        population=pop_hash == sealed["digests"]["population"],
        blocks=blk_hash == sealed["digests"]["blocks"])
    print(f"  same surface as the sealed X run: population {same_surface['population']} · "
          f"blocks {same_surface['blocks']}", flush=True)
    print(f"  estimand {len(P):,} of {nE:,} · blocks {nb:,} · FULL_M1_M4 {full_m1_m4:,} · "
          f"ANY_PAIR {n_any_pair:,}", flush=True)

    # ── both eligibility verdicts, for every candidate that can possibly pass ──
    rows = []
    n_cand = 0
    for (a, b) in PAIRS:
        ia, ib = int(a[1:]) - 1, int(b[1:]) - 1
        Ba, Bb = B[ia], B[ib]
        for i, ti in enumerate(tok):
            va = Ba[:, i]
            if not va.any():
                continue
            for j, tj in enumerate(tok):
                n_cand += 1
                v = va & Bb[:, j]
                nt = int(v.sum())
                if nt < MIN_EP:
                    continue                    # necessary on BOTH surfaces: S2 subset S0
                # ---- S0, exactly the sealed X predicate ----
                tc = np.bincount(ep_tk[v], minlength=n_tk)
                dc = np.bincount(ep_dt[v], minlength=n_dt)
                s0_tk, s0_dt = int((tc > 0).sum()), int((dc > 0).sum())
                s0_mds = float(dc.max() / nt)
                s0_ok = (nE - nt >= MIN_EP and s0_tk >= MIN_TK and s0_dt >= MIN_DT
                         and s0_mds <= MAX_DS)
                # ---- S2, the frozen 1H estimand predicate ----
                vp = v[idx_pop]
                w = vp.astype(float)
                nt_b = np.bincount(bfac, weights=w, minlength=nb)
                inov = ((nt_b > 0) & (blk_size - nt_b > 0))[bfac]
                m = vp & inov
                t_ov = int(m.sum())
                c_ov = int((~vp & inov).sum())
                e = np.flatnonzero(m)
                if len(e):
                    tc2 = np.bincount(tk_pop[e], minlength=n_tk)
                    dc2 = np.bincount(dt_pop[e], minlength=n_dt)
                    s2_tk, s2_dt = int((tc2 > 0).sum()), int((dc2 > 0).sum())
                    s2_mds = float(dc2.max() / len(e))
                else:
                    s2_tk = s2_dt = 0
                    s2_mds = float("nan")
                fails = []
                if t_ov < MIN_EP: fails.append("TREATED_FLOOR")
                if c_ov < MIN_EP: fails.append("CONTROL_FLOOR")
                if s2_tk < MIN_TK: fails.append("TICKER_FLOOR")
                if s2_dt < MIN_DT: fails.append("DATE_FLOOR")
                if not (s2_mds <= MAX_DS): fails.append("DATE_SHARE")
                rows.append(dict(
                    claim_id=f"{a}→{b}|{ti}→{tj}", position_family=f"{a}→{b}",
                    s0_support=nt, s0_ok=bool(s0_ok),
                    s0_hash=_h(np.flatnonzero(v)),
                    s2_support=t_ov, s2_control=c_ov, s2_tickers=s2_tk, s2_dates=s2_dt,
                    s2_max_date_share=None if s2_mds != s2_mds else round(s2_mds, 6),
                    s2_ok=not fails, s2_fail=",".join(fails),
                    s2_hash=_h(e)))
    C = pd.DataFrame(rows)
    out_parq = os.path.join(D.ROOT, "data", f"{fam}_15m_k_closure.parquet")
    C.to_parquet(out_parq, index=False)

    # ── identity on the surface the X run sealed ──────────────────────────
    S0 = C[C.s0_ok]
    k_sealed = S0.s2_hash.nunique()
    id_sealed = dict(
        support_qualified_syntactic_names=int(len(S0)),
        membership_distinct_classes=int(k_sealed),
        aliases=int(len(S0) - k_sealed),
        identity_holds=bool(len(S0) == k_sealed + (len(S0) - k_sealed)),
        matches_sealed_artifact=bool(
            len(S0) == sealed["enumeration"]["support_qualified_names"]
            and k_sealed == sealed["enumeration"]["membership_distinct_classes"]),
        eligibility_surface="S0 — ENUMERATION",
        class_identity_surface="S2 — FINAL ESTIMAND POPULATION")

    # ── the four counters, S0 -> S2 ───────────────────────────────────────
    g = S0.groupby("s0_hash").s2_hash
    splits = int((g.nunique() > 1).sum())
    s0_classes = int(S0.s0_hash.nunique())
    # with splits == 0 the map S0-class -> S2-class is a surjection, so the number of
    # S0-classes lost to collapsing is exactly the difference
    merges = int(s0_classes - k_sealed)
    cls = S0.groupby("s2_hash").s2_ok
    whole_removed = int((~cls.any()).sum())
    partial = int((cls.any() & ~cls.all()).sum())

    # ── k on the frozen predicate's own surface ───────────────────────────
    S2 = C[C.s2_ok]
    k_final = int(S2.s2_hash.nunique())
    added = C[C.s2_ok & ~C.s0_ok]
    dropped = C[C.s0_ok & ~C.s2_ok]
    fail_mix = {str(k): int(v) for k, v in
                dropped.s2_fail.value_counts().head(8).to_dict().items()}

    empty_restricted = int((S0.s2_support == 0).sum())
    ok = (same_surface["population"] and same_surface["blocks"] and splits == 0
          and partial == 0 and id_sealed["matches_sealed_artifact"])
    agrees = k_final == k_sealed

    print(f"  S0-eligible names {len(S0):,} · S2 classes {k_sealed:,} · aliases "
          f"{len(S0)-k_sealed:,}")
    print(f"  merges {merges:,} · splits {splits} · whole classes removed "
          f"{whole_removed:,} · partial removals {partial}")
    print(f"  k on the frozen 1H predicate's own surface: {k_final:,} "
          f"({'AGREES' if agrees else 'DIFFERS from ' + format(k_sealed, ',')})")
    if len(dropped):
        print(f"  dropped by the S2 floors: {len(dropped):,} names · {fail_mix}")
    if len(added):
        print(f"  admitted only on S2: {len(added):,} names")

    body = dict(
        spec_id=f"{F}_15M_K_CLOSURE_V1", status="X_ONLY_PRE_Y", family=F,
        question="is the enumerated k a FINAL-ESTIMAND k, or a pre-estimand enumeration k?",
        sealed_x_artifact=ART.file_digest(f"{F}_15M_X_ONLY_V1.json"),
        surfaces=dict(
            S0="ENUMERATION — every opening-hour-observable episode",
            S1="ESTIMAND POPULATION — S0 & path_status_10d AVAILABLE & anchored & n20>=20",
            S2="FINAL ESTIMAND — S1 restricted to blocks holding treated AND control",
            same_surface_as_sealed_x=same_surface,
            population_digest=pop_hash, blocks_digest=blk_hash),
        CLASS_IDENTITY_SURFACE="FINAL_ESTIMAND_POPULATION",
        class_identity_evidence="every membership_hash in this closure and in the sealed X "
                                "run is sha256 over the S2 overlap-restricted index set; "
                                "the population and block digests above are asserted equal "
                                "to the sealed run's, so it is the same S2",
        funnel=dict(
            CANONICAL_EPISODES=int(len(E)),
            FIFTEEN_M_OBSERVABLE=int(nE),
            FULL_M1_M4_COMPLETE=full_m1_m4,
            ANY_REGISTERED_PAIR_ANALYZABLE=n_any_pair,
            ESTIMAND_POPULATION=int(len(P)),
            ESTIMAND_AND_ANY_REGISTERED_PAIR=n_pair_estimand,
            ESTIMAND_AND_FULL_M1_M4=n_full_estimand,
            blocks=nb,
            pair_bearing_note="the estimand rows that can host NO registered pair are still "
                              "population — they can only ever be controls, never treated, "
                              "which is what ESTIMAND_AND_ANY_REGISTERED_PAIR measures",
            naming_note="the sealed X artifact called ESTIMAND_POPULATION "
                        "'sequence_analyzable', which read as a slot-coverage step and made "
                        "the funnel look non-monotone. It is not a slot filter at all: it "
                        "is the outcome-status/anchor/block filter. "
                        "ANY_REGISTERED_PAIR_ANALYZABLE is computed here for the first time "
                        "and is the number the old name suggested."),
        identity_on_sealed_surface=id_sealed,
        restriction_accounting=dict(
            s0_distinct_memberships=s0_classes,
            s2_distinct_memberships=int(k_sealed),
            restriction_merges=merges,
            restriction_splits=splits,
            splits_are_impossible="S2 membership is a deterministic function of the S0 "
                                  "membership, so two names with identical S0 memberships "
                                  "cannot separate; measured, not assumed",
            whole_classes_removed=whole_removed,
            partial_class_removals=partial,
            class_whole_removal_invariant="every S2-floor input is a function of the S2 "
                                          "membership, which is shared inside a class, so "
                                          "removal is always whole-class; measured",
            names_with_empty_restricted_membership=empty_restricted),
        eligibility_surface_finding=dict(
            sealed_x_applied_floors_on="S0 — the enumeration surface",
            frozen_1h_predicate_applies_floors_on="S2 — the overlap-restricted estimand "
                                                  "memberships (treated AND control floor, "
                                                  "tickers, dates, max-date-share)",
            precedent="t9/t3/t1_sequence_estimand.support_eligible(t_ov, c_ov) with "
                      "n_tickers_overlap / n_dates_overlap / max_date_share_overlap; "
                      "k_final there is keyed on (s & inov)",
            prespec_says=json.load(open(f"{F}_15M_PRESPEC_V1.json"))["support_rules"],
            k_on_sealed_x_predicate=int(k_sealed),
            k_on_frozen_1h_predicate=k_final,
            agree=bool(agrees),
            names_dropped_by_s2_floors=int(len(dropped)),
            drop_reasons=fail_mix,
            names_admitted_only_on_s2=int(len(added)),
            admitted_only_on_s2_reason="a name can fail S0's max-date-share and pass S2's, "
                                       "because the restriction removes block-constant "
                                       "rows; the floors are not monotone in that one "
                                       "dimension"),
        floors=dict(min_episodes=MIN_EP, min_tickers=MIN_TK, min_dates=MIN_DT,
                    max_date_share=MAX_DS,
                    control_floor="MIN_EP on the S2 control rows — present in the frozen 1H "
                                  "predicate, absent from the sealed 15m run"),
        table=dict(path=os.path.basename(out_parq), digest=ART.file_digest(out_parq)),
        mechanical_checks=dict(
            same_population_as_sealed_x=same_surface["population"],
            same_blocks_as_sealed_x=same_surface["blocks"],
            reproduces_sealed_counts=id_sealed["matches_sealed_artifact"],
            no_restriction_splits=splits == 0,
            no_partial_class_removals=partial == 0),
        verdict=("IDENTITY RECONCILED — k unchanged on both predicates" if ok and agrees
                 else "IDENTITY RECONCILED — but the two predicates give different k"
                 if ok else "CLOSURE FAILED"),
        y_status="NO OUTCOME VALUE READ — only path_status_10d, as a status",
        runtime_min=round((time.time() - t0) / 60, 1))
    d = ART.seal(body, f"{F}_15M_K_CLOSURE_V1.json",
                 required=("spec_id", "surfaces", "identity_on_sealed_surface",
                           "restriction_accounting", "eligibility_surface_finding"),
                 supersede=os.path.exists(f"{F}_15M_K_CLOSURE_V1.json"))
    print(f"  {F}_15M_K_CLOSURE_V1 · {d} · {body['verdict']} · "
          f"{(time.time()-t0)/60:.1f} min\n", flush=True)
    return d


if __name__ == "__main__":
    for f in (sys.argv[1:] or ["t9", "t3", "t1"]):
        analyse(f)
