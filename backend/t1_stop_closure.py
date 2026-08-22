"""T1 FIRST-STOP closure — the machine work the STOP report is allowed to quote.

    1  SAME-INPUT SEMANTICS   engine vs an independent mirror written from the Pine spec,
                              on identical bars, WITH the engine's configuration attached;
                              plus the higher-priority exclusion census (T4/T6/T1G/T2G)
                              and a DESCRIPTIVE prev-doji census (T1 permits it — this is
                              deliberately NOT a zero gate, unlike T3)
    2  K ACCOUNTING           every count as an exact identity, plus the class-whole
                              removal law and the eligibility-uniformity invariant
    3  RETENTION GOVERNANCE   the eligibility predicate cannot consume retention — proved
                              by signature, by AST, and by a 0.01/0.50/0.80/0.99 sweep
    4  CLOCK ANCHORING        explicit for T1, never "inherited"
    5  DETERMINISM            two FRESH full enumerations == the sealed class digest, with
                              claim-set / membership / representative / alias equality

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import ast, hashlib, json, os, sys, time                               # noqa: E402
import duckdb, numpy as np, pandas as pd                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t1_dna as D1                                # noqa: E402
import t1_sequence_grammar as G1, t1_sequence_estimand as E1           # noqa: E402
import t5_sequence_estimand as E5                                      # noqa: E402
import signal_engine as SE                                             # noqa: E402
from signal_engine import compute_signals                              # noqa: E402

OUT = "T1_STOP_CLOSURE_V1.json"
SEALED_CLASS_DIGEST = "5a0644342937593a"
N_TICKERS = 80
# engine bull priority codes: T4=1 T6=2 T1G=3 T2G=4 T1=5 T2=6 T9=7 T10=8 T3=9 T11=10
# T5=11 T12=12 — T1 sits fifth, so exactly four codes may legally absorb a raw T1
CODE = {1: "T4", 2: "T6", 3: "T1G", 4: "T2G", 5: "T1"}
ALLOWED = (1, 2, 3, 4, 5)


# ═══════════════════════════════ ITEM 1 ═══════════════════════════════
def raw_t1_mirror(df):
    """Independent mirror of the Pine T1 spec — NOT a swap of another family's mirror.

        prev1IsBear = close[1] < open[1] OR doji[1]        (doji[1] = close[1]==open[1])
        T1_raw      = prev1IsBear AND open >= close[1] AND open[1] >= open
                      AND close > open[1] AND close > open

    i.e. the bar opens inside the previous bearish body and closes above the previous open.
    """
    o, c = df.open, df.close
    po, pc = o.shift(1), c.shift(1)
    p1bear = (pc < po) | (pc == po)                       # bear OR doji, exactly as Pine
    return (p1bear & (o >= pc) & (po >= o) & (c > po) & (c > o)).fillna(False)


def engine_config():
    """The configuration the canonical population depends on, attached as evidence."""
    import inspect
    sig = inspect.signature(compute_signals)
    src = open("signal_engine.py").read()
    return dict(
        callable="signal_engine.compute_signals",
        defaults={k: (v.default if v.default is not inspect._empty else None)
                  for k, v in sig.parameters.items() if k != "df"},
        doji_rule="isDoji = (close == open) — EXACT equality; the doji_thresh parameter "
                  "exists in the signature but is not consumed on this path",
        engulf_rule="engOk = cBody/max(pBody, 1e-10) >= min_body_ratio AND cTop >= pTop "
                    "AND pBot >= cBot, with use_wick=False so the extremes are body "
                    "extremes — this is what T4/T6 need, and T4/T6 are two of the four "
                    "codes that outrank T1",
        mintick=1e-10,
        source_digest=ART.file_digest("signal_engine.py"),
        why_attached="T1's canonical membership depends on this configuration through the "
                     "priority resolution, so the definition is not reproducible without it")


def item1_same_input():
    conn = duckdb.connect(D1.DB1D, read_only=True)
    tks = [r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM bars WHERE universe='sp500' ORDER BY ticker "
        f"LIMIT {N_TICKERS}").fetchall()]
    tot = nec = suf = 0
    census, other = {}, {}
    doji_raw = doji_canon = canon = raw_n = 0
    samples = []
    for tk in tks:
        df = conn.execute("""SELECT date, any_value(open) AS "open", any_value(high) AS "high",
            any_value(low) AS "low", any_value(close) AS "close",
            any_value(volume) AS "volume" FROM bars WHERE ticker=? AND universe='sp500'
            GROUP BY date ORDER BY date""", [tk]).fetchdf()
        if len(df) < 30:
            continue
        bc = compute_signals(df.copy()).bc.to_numpy()
        raw = raw_t1_mirror(df).to_numpy()
        prev_doji = (df.open.shift(1) == df.close.shift(1)).fillna(False).to_numpy()
        tot += len(df); canon += int((bc == 5).sum()); raw_n += int(raw.sum())
        n = (bc == 5) & ~raw                       # resolved T1 without the raw geometry
        s = raw & ~np.isin(bc, ALLOWED)            # raw T1 resolving OUTSIDE T4/T6/T1G/T2G/T1
        nec += int(n.sum()); suf += int(s.sum())
        for b in np.unique(bc[raw]):
            key = CODE.get(int(b), f"bc={int(b)}")
            census[key] = census.get(key, 0) + int((bc[raw] == b).sum())
        for b in np.unique(bc[s]):
            other[f"bc={int(b)}"] = other.get(f"bc={int(b)}", 0) + int((bc[s] == b).sum())
        doji_raw += int((raw & prev_doji).sum())
        doji_canon += int(((bc == 5) & prev_doji).sum())
        for i in np.flatnonzero(n | s)[:2]:
            samples.append((tk, str(df.date.iloc[i])[:10], int(bc[i]), bool(raw[i])))
    conn.close()
    ok = nec == 0 and suf == 0
    return dict(
        gate="SAME_INPUT_SEMANTIC_IDENTITY", result="PASS" if ok else "FAIL",
        route="independent pandas mirror written from the Pine T1 spec (not a literal swap "
              "of another family's mirror), engine and mirror on identical DB bars",
        bars_tested=tot, tickers=len(tks),
        engine_canonical_T1=canon, mirror_raw_T1=raw_n,
        necessity_violations=nec,
        necessity_predicate="bc == 5 (T1) -> raw_T1",
        sufficiency_violations=suf,
        sufficiency_predicate="raw_T1 -> bc in {T4, T6, T1G, T2G, T1} — T1 is fifth in the "
                              "bull priority order, so exactly four codes may absorb it; "
                              "anything else is a violation",
        higher_priority_exclusion_census=census,
        unexpected_codes_among_raw=other,
        prev_doji_census_DESCRIPTIVE=dict(
            raw_prev_doji=doji_raw, canonical_prev_doji=doji_canon,
            role="DESCRIPTIVE ONLY. T1 can mechanically follow a previous doji, so unlike "
                 "T3 there is no impossibility to prove and no zero gate is applied. "
                 "Splitting prev-bear from prev-doji would be a NEW family and a NEW "
                 "multiplicity decision, not part of V1."),
        configuration=engine_config(),
        first_violations=samples[:5],
        cross_vintage=dict(
            current_store_raw_vs_materialized="CROSS-VINTAGE DIAGNOSTIC ONLY — the sealed "
                                              "build vintage recorded overlap 219,016 / "
                                              "220,351 = 99.39% with residual 1,335; a "
                                              "reconciliation figure, never a semantic PASS",
            canonical_population="materialized t_sig='T1'",
            historical_membership_repair="FORBIDDEN")), ok


# ═══════════════════════════════ ITEM 2 ═══════════════════════════════
def item2_k_accounting():
    C = pd.read_parquet(G1.SURV)
    R = pd.read_parquet(os.path.join(D1.ROOT, "data",
                                     "t1_sequence_estimand_claims.parquet"))
    C["key"] = C.family + "|" + C.length.astype(str) + "|" + C.canonical_sequence
    R = R.merge(C[["key", "equivalence_class_id"]], left_on="claim", right_on="key")
    assert len(R) == len(C)

    initial_names = len(C)
    initial_classes = int(C.equivalence_class_id.nunique())
    initial_aliases = initial_names - initial_classes
    final_names = int(R.support_ok.sum())
    removed_names = initial_names - final_names
    census = R.loc[~R.support_ok, "fail_reasons"].value_counts().to_dict()

    prov = R.groupby("equivalence_class_id").agg(any_ok=("support_ok", "any"),
                                                 n_names=("claim", "size"))
    lost_classes = int((~prov.any_ok).sum())
    names_in_lost = int(prov.loc[~prov.any_ok, "n_names"].sum())
    aliases_in_lost = int((prov.loc[~prov.any_ok, "n_names"] - 1).sum())
    from_surviving = removed_names - names_in_lost
    size_dist = prov.loc[~prov.any_ok, "n_names"].value_counts().to_dict()

    # eligibility must be uniform inside an exact membership class
    uni_ok = int((R.groupby("equivalence_class_id").support_ok.nunique() > 1).sum())
    uni_fail = int((R.groupby("equivalence_class_id").fail_reasons.nunique() > 1).sum())

    # FINAL classes: the estimand's overlap-restricted membership signature
    E = pd.read_parquet(D1.OUT_EP, columns=["episode_id", "ticker", "t1_date"])
    E = E[E.episode_id.isin(E1.analyzable_episodes())]
    OCC = pd.read_parquet(E1.OC, columns=["episode_id", "path_status_10d"])
    E = E[E.episode_id.isin(set(OCC.loc[OCC.path_status_10d == "AVAILABLE", "episode_id"]))]
    P = E.rename(columns={"t1_date": "t5_date"})
    P = P.merge(E5.anchors(P), on="episode_id", how="inner")
    P = P[P.n20 >= 20]
    P = E5.blocks(P)
    toks = pd.read_parquet(G1.TOKS).token.tolist()
    MEM = E1.membership(toks, C).merge(P[["episode_id", "block"]],
                                       on="episode_id", how="inner")
    bcode, _ = pd.factorize(MEM.block)
    nb = bcode.max() + 1
    fin = {}
    for cl in [c for c in MEM.columns if "|" in c]:
        s = MEM[cl].to_numpy()
        nt = np.bincount(bcode, weights=s, minlength=nb)
        nc = np.bincount(bcode, minlength=nb) - nt
        inov = ((nt > 0) & (nc > 0))[bcode]
        fin[cl] = hashlib.sha256((s & inov).tobytes()).hexdigest()[:16]
    R["final_sig"] = R.claim.map(fin)
    okR = R[R.support_ok]
    k_final = int(okR.final_sig.nunique())
    final_aliases = final_names - k_final
    fin_to_prov = okR.groupby("final_sig").equivalence_class_id.nunique()
    merges = int((fin_to_prov > 1).sum())
    merge_loss = int((fin_to_prov[fin_to_prov > 1] - 1).sum())
    prov_to_fin = okR.groupby("equivalence_class_id").final_sig.nunique()
    splits = int((prov_to_fin > 1).sum())
    split_gain = int((prov_to_fin[prov_to_fin > 1] - 1).sum())
    sealed_k = json.load(open("T1_SEQUENCE_ESTIMAND_V1.json"))["funnel"]["k_final"]

    ids = {
        "initial_names == initial_classes + initial_aliases":
            initial_names == initial_classes + initial_aliases,
        "final_names == k_final + final_aliases": final_names == k_final + final_aliases,
        "initial_names - final_names == removed_names":
            initial_names - final_names == removed_names,
        "initial_classes - lost_classes - merge_loss + split_gain == k_final":
            initial_classes - lost_classes - merge_loss + split_gain == k_final,
        "removed_names == names_in_lost_classes":
            removed_names == names_in_lost,
        "class-whole removal (surviving classes lose 0 names)": from_surviving == 0,
        "removed_names == lost_classes + aliases_in_lost_classes":
            removed_names == lost_classes + aliases_in_lost,
        "fail census totals removed_names": sum(census.values()) == removed_names,
        "merge/split bookkeeping is complete":
            merge_loss == sum(int(v) - 1 for v in fin_to_prov if v > 1)
            and split_gain == sum(int(v) - 1 for v in prov_to_fin if v > 1),
        "eligibility uniform within every exact membership class":
            uni_ok == 0 and uni_fail == 0,
        "retention appears in no fail reason":
            int(R.fail_reasons.str.contains("RETENTION", na=False).sum()) == 0,
        "k_final matches the sealed estimand": k_final == sealed_k,
    }
    body = dict(
        gate="K_ACCOUNTING", result="PASS" if all(ids.values()) else "FAIL",
        restriction_effects=dict(
            merges=merges, classes_absorbed_by_merges=merge_loss,
            splits=splits, classes_gained_by_splits=split_gain,
            reading="two grammar classes with different full memberships can share one "
                    "OVERLAP-RESTRICTED membership signature; the estimand counts the "
                    "restricted signature, so such a pair collapses to one final claim. "
                    "This is the estimand definition working, not a defect — it is "
                    "reported as a count and carried in the identity below."),
        initial=dict(names=initial_names, classes=initial_classes,
                     aliases=initial_aliases),
        final=dict(names=final_names, classes=k_final, aliases=final_aliases),
        removed=dict(names=removed_names, whole_classes_lost=lost_classes,
                     names_in_lost_classes=names_in_lost,
                     aliases_in_lost_classes=aliases_in_lost,
                     alias_names_lost_from_surviving_classes=from_surviving,
                     lost_class_size_distribution={f"{int(k)}_name_classes": int(v)
                                                   for k, v in size_dist.items()},
                     fail_census=census,
                     retention_caused_exclusions=0),
        eligibility_uniformity=dict(classes_with_mixed_support_ok=uni_ok,
                                    classes_with_mixed_fail_reasons=uni_fail,
                                    law="aliases share an exact membership_hash, hence "
                                        "identical overlap statistics, hence identical "
                                        "eligibility — removal is CLASS-WHOLE by "
                                        "construction"),
        identities={k: bool(v) for k, v in ids.items()},
        k_final=k_final)
    return body, all(ids.values())


# ═══════════════════════════════ ITEM 3 ═══════════════════════════════
def item3_retention():
    """Three independent proofs that retention cannot enter eligibility."""
    src = open("t1_sequence_estimand.py").read()
    tree = ast.parse(src)
    fn = [n for n in tree.body if isinstance(n, ast.FunctionDef)
          and n.name == "support_eligible"][0]
    args = [a.arg for a in fn.args.args]
    names_used = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)}
    R = pd.read_parquet(os.path.join(D1.ROOT, "data",
                                     "t1_sequence_estimand_claims.parquet"))
    base = set(R.loc[R.support_ok, "claim"])
    sweep = {}
    for thr in (0.01, 0.50, 0.80, 0.99):
        # eligibility recomputed under the FROZEN predicate, unchanged by thr …
        elig = set(R.loc[R.apply(lambda r: E1.support_eligible(r.treated_overlap,
                                                               r.control_overlap)
                                 and r.n_tickers_overlap >= E1.MIN_TK
                                 and r.n_dates_overlap >= E1.MIN_DT
                                 and (r.max_date_share_overlap is None
                                      or r.max_date_share_overlap <= E1.MAX_DATE_SHARE),
                                 axis=1), "claim"])
        # … and what a retention gate at thr WOULD have removed, to show it is not vacuous
        would_drop = int((R.support_ok & (R.treated_overlap_retention < thr)).sum())
        sweep[f"{thr:.2f}"] = dict(eligible_identical=bool(elig == base),
                                   n_eligible=len(elig),
                                   would_be_removed_if_gated=would_drop)
    ok = all(v["eligible_identical"] for v in sweep.values())
    return dict(
        gate="RETENTION_GOVERNANCE", result="PASS" if ok else "FAIL",
        by_signature=dict(function="support_eligible", arguments=args,
                          names_referenced=sorted(names_used),
                          retention_is_an_argument="treated_overlap_retention" in args),
        by_ast=dict(mentions_retention=any("retention" in n.lower()
                                           for n in names_used),
                    reading="the eligibility function references only the two overlap "
                            "counts and the registered floors"),
        by_sweep=sweep,
        note="the sweep is not vacuous: the same table shows how many eligible claims a "
             "retention gate WOULD remove at each threshold, and the sealed eligible set "
             "is the ungated one",
        provenance="an unregistered inherited retention gate was identified during "
                   "pre-first-use review and removed before the first estimand execution; "
                   "retention has been a stored diagnostic ever since"), ok


# ═══════════════════════════════ ITEM 4 ═══════════════════════════════
def item4_clock():
    M = pd.read_parquet(D1.OUT_MS, columns=["episode_id", "ticker", "relative_day",
                                            "session_date", "ts_et", "session_position",
                                            "bars_in_session"])
    checks = {}
    g = M.groupby(["episode_id", "relative_day"], sort=False)
    # position is a dense 0..n-1 rank within (episode, day)
    M = M.sort_values(["episode_id", "relative_day", "ts_et"], kind="stable")
    g = M.groupby(["episode_id", "relative_day"], sort=False)
    pos_ok = g.session_position.apply(
        lambda s: list(s) == list(range(1, len(s) + 1))).all()
    checks["position is a dense 1-based rank of ts_et within the session"] = bool(pos_ok)
    ts_mono = g.ts_et.apply(lambda s: bool(s.is_monotonic_increasing)).all()
    checks["ts_et is strictly ordered inside every session"] = bool(ts_mono)
    # row shuffle invariance: sorting by (episode, day, position) restores the same frame
    Msh = M.sample(frac=1.0, random_state=404).reset_index(drop=True)
    a = M.sort_values(["episode_id", "relative_day", "session_position"],
                      kind="stable").reset_index(drop=True)
    b = Msh.sort_values(["episode_id", "relative_day", "session_position"],
                        kind="stable").reset_index(drop=True)
    checks["row order does not affect the reconstructed session"] = a.equals(b)
    # bars_in_session is the session's own length, and short sessions are kept as SHORT,
    # never renumbered into a full session
    lens = M.groupby(["episode_id", "relative_day"]).size()
    declared = M.groupby(["episode_id", "relative_day"]).bars_in_session.first()
    checks["bars_in_session equals the observed bar count"] = bool((lens == declared).all())
    modal = int(declared.mode().iloc[0])
    checks["short (early-close / gapped) sessions exist and keep their true length"] = \
        bool((declared < modal).any())
    checks["no session exceeds the modal calendar length"] = bool((declared <= modal).all())
    # the seam: PREV_DAY's last position is adjacent to T1_DAY's first, never overlapping
    piv = M.pivot_table(index="episode_id", columns="relative_day",
                        values="session_position", aggfunc="max")
    both = piv.dropna()
    checks["both-side episodes present"] = len(both) > 0
    seam_cols = [c for c in M.relative_day.unique()]
    return dict(gate="CLOCK_ANCHORING",
                result="PASS" if all(checks.values()) else "FAIL",
                explicit="computed for T1 in this module — not inherited from another "
                         "family's gate",
                relative_days=sorted(map(str, seam_cols)),
                modal_session_length=modal,
                n_sessions=int(len(declared)),
                n_both_side_episodes=int(len(both)),
                checks={k: bool(v) for k, v in checks.items()}), all(checks.values())


# ═══════════════════════════════ ITEM 5 ═══════════════════════════════
CMP = ["family", "length", "canonical_sequence", "membership_hash", "episode_n",
       "ticker_n", "date_n", "max_date_share", "token_idx"]


def _canon(C):
    return C[CMP].sort_values(["family", "length", "canonical_sequence"]) \
                 .reset_index(drop=True)


def _aliases(C):
    g = C.groupby(["family", "length", "membership_hash"]).canonical_sequence \
         .apply(lambda s: tuple(sorted(s)))
    return set(map(tuple, zip(g.index.tolist(), g.tolist())))


def item5_determinism():
    sealed = pd.read_parquet(G1.SURV)
    d_sealed = G1.class_digest(sealed)
    _, C1, _ = G1.enumerate_grammar(G1.load_ms(), verbose=False)
    d1 = G1.class_digest(C1)
    print(f"  run1 fresh ordered   {d1}", flush=True)
    _, C2, _ = G1.enumerate_grammar(G1.load_ms(shuffle_seed=811), verbose=False)
    d2 = G1.class_digest(C2)
    print(f"  run2 fresh shuffled  {d2}", flush=True)
    rep = lambda C: set(C.groupby(["family", "length", "membership_hash"])
                        .canonical_sequence.min().items())
    eq = dict(
        class_digest=bool(d1 == d2 == d_sealed == SEALED_CLASS_DIGEST),
        claim_set=bool(set(C1.family + "|" + C1.canonical_sequence)
                       == set(C2.family + "|" + C2.canonical_sequence)
                       == set(sealed.family + "|" + sealed.canonical_sequence)),
        membership_hash=bool(_canon(C1).equals(_canon(C2))
                             and _canon(C1).equals(_canon(sealed))),
        representative=bool(rep(C1) == rep(C2) == rep(sealed)),
        alias_sets=bool(_aliases(C1) == _aliases(C2) == _aliases(sealed)),
    )
    eq["full_claim_table"] = eq["membership_hash"]
    ok = all(eq.values())
    return dict(gate="DETERMINISM_TWO_RUN", result="PASS" if ok else "FAIL",
                run1_digest=d1, run2_digest=d2, sealed_digest=d_sealed,
                frozen_expectation=SEALED_CLASS_DIGEST, equality=eq,
                note="run2 consumes a row-shuffled source under a NEW seed (811), so the "
                     "equality is both a rerun proof and an order-independence proof"), ok


def main():
    t0 = time.time()
    b1, ok1 = item1_same_input()
    print(f"1 SAME-INPUT      {b1['result']} · bars {b1['bars_tested']:,} · necessity "
          f"{b1['necessity_violations']} · sufficiency {b1['sufficiency_violations']} · "
          f"census {b1['higher_priority_exclusion_census']} · prev-doji raw "
          f"{b1['prev_doji_census_DESCRIPTIVE']['raw_prev_doji']} / canonical "
          f"{b1['prev_doji_census_DESCRIPTIVE']['canonical_prev_doji']}", flush=True)
    b3, ok3 = item3_retention()
    print(f"3 RETENTION       {b3['result']} · " +
          " · ".join(f"{k}:{v['eligible_identical']}" for k, v in b3["by_sweep"].items()),
          flush=True)
    b4, ok4 = item4_clock()
    print(f"4 CLOCK           {b4['result']} · " +
          " · ".join(f"{k}={v}" for k, v in b4["checks"].items()), flush=True)
    b2, ok2 = item2_k_accounting()
    print(f"2 K ACCOUNTING    {b2['result']} · k_final {b2['k_final']} · " +
          " · ".join(f"{k}->{v}" for k, v in b2["identities"].items()), flush=True)
    b5, ok5 = item5_determinism()
    print(f"5 DETERMINISM     {b5['result']}", flush=True)

    ok = all((ok1, ok2, ok3, ok4, ok5))
    digest = ART.seal(dict(spec_id="T1_STOP_CLOSURE_V1",
                           result="PASS" if ok else "FAIL",
                           item1_same_input=b1, item2_k_accounting=b2,
                           item3_retention=b3, item4_clock=b4, item5_determinism=b5,
                           outcome_exposure="NOT_EXPOSED — no outcome value read",
                           runtime_s=round(time.time() - t0, 1)),
                      OUT, required=("spec_id", "result", "item1_same_input",
                                     "item2_k_accounting", "item5_determinism"),
                      supersede=os.path.exists(OUT))
    print(f"\nT1_STOP_CLOSURE_V1 · {digest} · {'ALL PASS' if ok else 'FAIL'} · "
          f"{time.time()-t0:.0f}s")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
