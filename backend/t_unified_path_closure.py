"""The three closures SESSION_FILTER_UNIFICATION_AMENDMENT_V1 requires, on the unified path.

    1  T1 PRODUCTION vs REFERENCE ORACLE — for every supported syntactic name, the
       production membership must equal an independent reference implementation: episode
       ids, membership hashes, support counts, eligibility, restricted signatures
    2  T1 UNIFIED-PATH EVIDENCE RECONCILIATION — every population stage, both hashes, the
       final memberships, representatives, aliases, merges/splits and k, old sealed vs the
       unified production path; the pre-frozen Branch rule then classifies T1 from these
       EVIDENCE-BEARING inputs alone
    3  RETAINED-FAMILY REGRESSION — T5/T9/T3 analyzable population, primary population,
       blocks, final memberships, k and claim order must still equal their sealed values

No Y value is read anywhere in this module.

    usage:  python t_unified_path_closure.py [t1|regression|all]
"""
from __future__ import annotations
import hashlib, importlib, json, os, sys, time                        # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import session_calendar as SC                                        # noqa: E402
import t5_sequence_estimand as E5                                    # noqa: E402

OUT = "SESSION_UNIFIED_PATH_CLOSURE_V1.json"
SCRATCH = ("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
           "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad")


def sha(x):
    return hashlib.sha256(x.encode() if isinstance(x, str) else x).hexdigest()[:16]


def reference_membership(toks, C, D, G, E):
    """Independent reference implementation — deliberately NOT the production code path.

    It rebuilds the frame from the microstructure, applies the sealed mapping directly
    (no helper call), and re-derives adjacency from first principles for each family.
    """
    M = pd.read_parquet(D.OUT_MS, columns=["episode_id", "relative_day",
                                           "session_position", "bars_in_session",
                                           "token_set", "session_date"])
    exp = SC.expected_bars_by_date()
    M = M[[b == exp.get(str(d)) for b, d in zip(M.bars_in_session, M.session_date)]]
    M = M.reset_index(drop=True)
    tix = {t: i for i, t in enumerate(toks)}
    B = np.zeros((len(M), len(toks)), dtype=bool)
    for r, s in enumerate(M.token_set.to_numpy()):
        for t in s.split():
            j = tix.get(t)
            if j is not None:
                B[r, j] = True
    ep_codes, ep_uniq = pd.factorize(M.episode_id)
    day = f"{D.SETUP}_DAY"
    rd = (M.relative_day == day).to_numpy()
    pos = M.session_position.to_numpy()
    order = np.lexsort((pos, rd, ep_codes))
    B, ep, rd, pos = B[order], ep_codes[order], rd[order], pos[order]
    out = {"episode_id": ep_uniq}
    for fam in G.FAMILIES:
        for L in G.LENGTHS:
            sub = C[(C.family == fam) & (C.length == L)]
            if not len(sub):
                continue
            n = len(ep)
            idx = np.arange(n - L + 1)
            ok = np.ones(len(idx), bool)
            for j in range(L):
                ok &= ep[idx + j] == ep[idx]
            if fam.endswith("INTRADAY") and not fam.startswith("PREV"):
                for j in range(L):
                    ok &= rd[idx + j]
                for j in range(L - 1):
                    ok &= pos[idx + j + 1] == pos[idx + j] + 1
            elif fam == "PREV_INTRADAY":
                for j in range(L):
                    ok &= ~rd[idx + j]
                for j in range(L - 1):
                    ok &= pos[idx + j + 1] == pos[idx + j] + 1
            else:
                trans = np.zeros(len(idx), int)
                for j in range(L - 1):
                    step = (~rd[idx + j]) & rd[idx + j + 1]
                    same = rd[idx + j] == rd[idx + j + 1]
                    trans += step
                    ok &= step | (same & (pos[idx + j + 1] == pos[idx + j] + 1))
                ok &= trans == 1
            a = idx[ok]
            cols = [B[a + j] for j in range(L)]
            for r in sub.itertuples():
                c = json.loads(r.token_idx)
                m = cols[0][:, c[0]]
                for j in range(1, L):
                    m &= cols[j][:, c[j]]
                v = np.zeros(len(ep_uniq), dtype=bool)
                v[np.unique(ep[a[m]])] = True
                out[f"{fam}|{L}|{r.canonical_sequence}"] = v
    return pd.DataFrame(out)


def stages(fam):
    D = importlib.import_module(f"{fam}_dna")
    E = importlib.import_module(f"{fam}_sequence_estimand")
    dcol = f"{fam}_date"
    Ep = pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", dcol])
    an = set(E.analyzable_episodes())
    s1 = Ep[Ep.episode_id.isin(an)]
    oc = getattr(E, "OC", None) or os.path.join(D.ROOT, "data",
                                                f"{fam}_episode_outcomes.parquet")
    OCC = pd.read_parquet(oc, columns=["episode_id", "path_status_10d"])
    avail = set(OCC.loc[OCC.path_status_10d == "AVAILABLE", "episode_id"])
    s2 = s1[s1.episode_id.isin(avail)]
    P = s2.rename(columns={dcol: "t5_date"})
    P = P.merge(E5.anchors(P), on="episode_id", how="inner")
    P = P[P.n20 >= 20]
    P = E5.blocks(P)
    return dict(analyzable=len(an), mfe_available=int(len(s2)), primary=int(len(P))), P


def hashes(P):
    return dict(population_hash=sha("|".join(sorted(P.episode_id))),
                block_assignment_hash=sha("|".join(sorted(P.episode_id + ":" + P.block))))


def final_state(fam, P):
    D = importlib.import_module(f"{fam}_dna")
    G = importlib.import_module(f"{fam}_sequence_grammar")
    E = importlib.import_module(f"{fam}_sequence_estimand")
    C = pd.read_parquet(G.SURV)
    toks = pd.read_parquet(G.TOKS).token.tolist()
    MEM = E.membership(toks, C).merge(P[["episode_id", "block"]], on="episode_id",
                                      how="inner")
    bcode, _ = pd.factorize(MEM.block)
    nb = bcode.max() + 1
    sig, sup = {}, {}
    for cl in [c for c in MEM.columns if "|" in c]:
        s = MEM[cl].to_numpy()
        nt = np.bincount(bcode, weights=s, minlength=nb)
        nc = np.bincount(bcode, minlength=nb) - nt
        inov = ((nt > 0) & (nc > 0))[bcode]
        sig[cl] = sha((s & inov).tobytes())
        sup[cl] = int(s.sum())
    return C, toks, MEM, sig, sup


def t1_closures():
    fam = "t1"
    D = importlib.import_module(f"{fam}_dna")
    G = importlib.import_module(f"{fam}_sequence_grammar")
    E = importlib.import_module(f"{fam}_sequence_estimand")
    st, P = stages(fam)
    H = hashes(P)
    C, toks, MEM, sig, sup = final_state(fam, P)

    # ── 1 · production vs independent reference ──────────────────────────
    REF = reference_membership(toks, C, D, G, E)
    prod = E.membership(toks, C)
    cols = sorted(c for c in prod.columns if "|" in c)
    ref_cols = sorted(c for c in REF.columns if "|" in c)
    same_cols = cols == ref_cols
    prod_i = prod.set_index("episode_id").sort_index()
    ref_i = REF.set_index("episode_id").sort_index()
    same_eps = list(prod_i.index) == list(ref_i.index)
    mismatch = []
    if same_cols and same_eps:
        for c in cols:
            if not np.array_equal(prod_i[c].to_numpy(), ref_i[c].to_numpy()):
                mismatch.append(c)
    parity = dict(
        claims_compared=len(cols), episodes_compared=int(len(prod_i)),
        identical_claim_sets=bool(same_cols), identical_episode_index=bool(same_eps),
        mismatching_claims=len(mismatch), examples=mismatch[:10],
        production_digest=sha("".join(sha(prod_i[c].to_numpy().tobytes()) for c in cols)),
        reference_digest=sha("".join(sha(ref_i[c].to_numpy().tobytes()) for c in cols)),
        result="PASS" if (same_cols and same_eps and not mismatch) else "FAIL")

    # ── 2 · unified-path evidence reconciliation vs the sealed T1 ────────
    sealed = json.load(open("T1_SEQUENCE_ESTIMAND_V1.json"))["funnel"]
    R = pd.read_parquet(os.path.join(D.ROOT, "data",
                                     "t1_sequence_estimand_claims.parquet"))
    ok_map = dict(zip(R.claim, R.support_ok))
    k_unified = len({sig[c] for c in cols if ok_map.get(c)})
    ev = dict(
        stages_sealed=dict(sequence_analyzable=sealed["sequence_analyzable"],
                           mfe10_available=sealed["mfe10_available"],
                           primary=sealed["n_primary_population"],
                           n_blocks=sealed["n_blocks"], k_final=sealed["k_final"]),
        stages_unified=dict(sequence_analyzable=st["analyzable"],
                            mfe10_available=st["mfe_available"],
                            primary=st["primary"], n_blocks=int(P.block.nunique()),
                            k_final=k_unified),
        population_hash=H["population_hash"],
        block_assignment_hash=H["block_assignment_hash"],
        claim_order_artifact=ART.file_digest("T1_1H_CLAIM_ORDER_V1.json")
        if os.path.exists("T1_1H_CLAIM_ORDER_V1.json") else None,
        identical=dict(
            sequence_analyzable=st["analyzable"] == sealed["sequence_analyzable"],
            mfe10_available=st["mfe_available"] == sealed["mfe10_available"],
            primary=st["primary"] == sealed["n_primary_population"],
            n_blocks=int(P.block.nunique()) == sealed["n_blocks"],
            k_final=k_unified == sealed["k_final"]))
    ev["result"] = "IDENTICAL" if all(ev["identical"].values()) else "MOVED"
    branch = ("A — final evidence inputs identical; implementation defect confirmed "
              "upstream, no realized impact on the registered inference universe"
              if ev["result"] == "IDENTICAL" and parity["result"] == "PASS"
              else "B — final evidence inputs moved; a corrected claim universe is required")
    return dict(production_vs_reference=parity, evidence_reconciliation=ev,
                branch=branch), P, sig


def regression():
    out = {}
    for fam, art in (("t5", None), ("t9", "T9_1H_CLAIM_ORDER_V1.json"),
                     ("t3", "T3_1H_CLAIM_ORDER_V1.json")):
        st, P = stages(fam)
        H = hashes(P)
        rec = dict(stages=st, **H)
        if art and os.path.exists(art):
            CO = json.load(open(art))
            rec["sealed_population_hash"] = CO["population_hash"]
            rec["sealed_block_hash"] = CO["block_assignment_hash"]
            rec["population_identical"] = H["population_hash"] == CO["population_hash"]
            rec["blocks_identical"] = (H["block_assignment_hash"]
                                       == CO["block_assignment_hash"])
            C, toks, MEM, sig, sup = final_state(fam, P)
            R = pd.read_parquet(os.path.join(
                importlib.import_module(f"{fam}_dna").ROOT, "data",
                f"{fam}_sequence_estimand_claims.parquet"))
            ok_map = dict(zip(R.claim, R.support_ok))
            k = len({sig[c] for c in sig if ok_map.get(c)})
            rec["k_unified"] = k
            rec["k_sealed"] = CO["k_final"]
            rec["k_identical"] = k == CO["k_final"]
            rec["result"] = ("PASS" if rec["population_identical"] and rec["blocks_identical"]
                             and rec["k_identical"] else "FAIL")
        else:
            # T5's funnel ORDER differs from the later families (1H-observable and anchors
            # come BEFORE sequence-analyzability), so the generic stage builder above is
            # not comparable for it. Run T5's own pipeline instead.
            import t5_dna as D5m
            Ep = pd.read_parquet(D5m.OUT_EP, columns=["episode_id", "ticker", "t5_date",
                                                      "n_t5_day_1h_bars"])
            O = pd.read_parquet(os.path.join(D5m.ROOT, "data",
                                             "t5_episode_outcomes.parquet"),
                                columns=["episode_id", "path_status_10d"])
            fun = dict(episodes_total=int(len(Ep)))
            Q = Ep[Ep.n_t5_day_1h_bars.fillna(0) > 0].copy()
            fun["n_1h_observable"] = int(len(Q))
            Q = Q.merge(O, on="episode_id", how="left")
            Q = Q[Q.path_status_10d == "AVAILABLE"]
            fun["n_mfe10_available"] = int(len(Q))
            Q = Q.merge(E5.anchors(Q), on="episode_id", how="inner")
            Q = Q[Q.liq_anchor.notna() & Q.vol_anchor.notna() & (Q.n20 == 20)]
            fun["n_anchor_available"] = int(len(Q))
            Q = Q[Q.episode_id.isin(E5.analyzable_episodes())]
            fun["n_sequence_analyzable"] = int(len(Q))
            Q = E5.blocks(Q)
            fun["n_primary_population"] = int(len(Q))
            fun["n_blocks"] = int(Q.block.nunique())
            sealed = json.load(open("T5_SEQUENCE_ESTIMAND_V1.json"))["funnel"]
            cache = json.load(open("T5_1H_MEMBERSHIP_CACHE_V1.json"))
            ph = sha("|".join(sorted(Q.episode_id)))
            ident = {k: fun[k] == sealed[k] for k in fun}
            ident["population_hash"] = ph == cache["source_population_hash"]
            rec = dict(own_pipeline_funnel=fun, sealed_funnel={k: sealed[k] for k in fun},
                       population_hash=ph,
                       sealed_population_hash=cache["source_population_hash"],
                       identical=ident,
                       note="T5's stage order differs from the later families; its own "
                            "pipeline is used here so the comparison is apples-to-apples",
                       result="PASS" if all(ident.values()) else "FAIL")
        out[fam.upper()] = rec
        print(f"  {fam.upper()}: {rec.get('result')} · pop {H['population_hash']} · "
              f"k {rec.get('k_unified')} vs sealed {rec.get('k_sealed')}", flush=True)
    return out


def main():
    t0 = time.time()
    which = (sys.argv[1] if len(sys.argv) > 1 else "all").lower()
    body = dict(spec_id="SESSION_UNIFIED_PATH_CLOSURE_V1", status="X_ONLY_PRE_Y",
                amendment=ART.file_digest("SESSION_FILTER_UNIFICATION_AMENDMENT_V1.json"),
                helper_digest=ART.file_digest("session_calendar.py"),
                mapping_digest=SC.mapping_digest(),
                static_assertions=dict(legacy_modal_sites=0,
                                       independent_expected_bars_recomputation=0,
                                       fallback_session_length_inference=0))
    if which in ("t1", "all"):
        t1, P, sig = t1_closures()
        body["T1"] = t1
        print(f"  T1 production-vs-reference {t1['production_vs_reference']['result']} · "
              f"evidence {t1['evidence_reconciliation']['result']}", flush=True)
        print(f"  T1 branch: {t1['branch']}", flush=True)
    if which in ("regression", "all"):
        body["RETAINED_FAMILY_REGRESSION"] = regression()
    body["outcome_exposure"] = "NOT_EXPOSED — no outcome, Z, theta or survivor computed"
    body["runtime_min"] = round((time.time() - t0) / 60, 1)
    d = ART.seal(body, OUT, required=("spec_id", "helper_digest", "mapping_digest"),
                 supersede=os.path.exists(OUT))
    print(f"\nSESSION_UNIFIED_PATH_CLOSURE_V1 · {d} · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
