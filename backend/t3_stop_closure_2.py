"""T3 STOP closure #2 — the three narrow HOLD items from the CAPABILITY review.

    1  K ACCOUNTING       945/885/60 -> 907/853/54 closed as exact machine identities;
                          the '54 excluded names' wording and the T9-literal 980 in
                          T3_HOLD_CLOSURE_V1.item2.syntactic are SUPERSEDED here
    2  DETERMINISM        two fresh full enumerations -> digest == digest == sealed
                          256825450ab04d28, plus claim-set / membership_hash /
                          representative / alias-set equality against the sealed parquet
    3  DOJI PROVENANCE    same-input previous-doji T3 = 0 (semantic PASS) separated from
                          the cross-vintage diagnostic (materialized canonical joined to
                          current bars); historical membership repair stays FORBIDDEN

No design change. No Y value read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                    # noqa: E402
import duckdb, numpy as np, pandas as pd                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t3_dna as D9                                # noqa: E402
import t3_sequence_grammar as G9, t3_sequence_estimand as E9           # noqa: E402
import t5_sequence_estimand as E5                                      # noqa: E402
from signal_engine import compute_signals                              # noqa: E402

OUT = "T3_STOP_CLOSURE_2_V1.json"
SEALED_CLASS_DIGEST = "256825450ab04d28"


# ═══════════════════════════════ ITEM 1 ═══════════════════════════════
def item1_k_accounting():
    """Every count in the user's identity block computed from data and asserted."""
    C = pd.read_parquet(G9.SURV)
    R = pd.read_parquet(os.path.join(D9.ROOT, "data",
                                     "t3_sequence_estimand_claims.parquet"))
    C["key"] = C.family + "|" + C.length.astype(str) + "|" + C.canonical_sequence
    R = R.merge(C[["key", "equivalence_class_id"]], left_on="claim", right_on="key")
    assert R.equivalence_class_id.notna().all() and len(R) == len(C)

    initial_names = len(C)
    initial_classes = int(C.equivalence_class_id.nunique())
    initial_aliases = initial_names - initial_classes
    final_names = int(R.support_ok.sum())
    removed_names = initial_names - final_names
    census = R.loc[~R.support_ok, "fail_reasons"].value_counts().to_dict()

    # removed-name decomposition by provisional-class fate
    prov = R.groupby("equivalence_class_id").agg(any_ok=("support_ok", "any"),
                                                 n_names=("claim", "size"))
    lost_classes = int((~prov.any_ok).sum())
    names_in_lost_classes = int(prov.loc[~prov.any_ok, "n_names"].sum())
    aliases_in_lost_classes = int((prov.loc[~prov.any_ok, "n_names"] - 1).sum())
    alias_names_lost_from_surviving = removed_names - names_in_lost_classes
    lost_size_dist = prov.loc[~prov.any_ok, "n_names"].value_counts().to_dict()

    # FINAL classes: recompute the estimand's overlap-restricted membership signature
    E = pd.read_parquet(D9.OUT_EP, columns=["episode_id", "ticker", "t3_date"])
    E = E[E.episode_id.isin(E9.analyzable_episodes())]
    OCC = pd.read_parquet(E9.OC, columns=["episode_id", "path_status_10d"])
    E = E[E.episode_id.isin(set(OCC.loc[OCC.path_status_10d == "AVAILABLE", "episode_id"]))]
    P = E.rename(columns={"t3_date": "t5_date"})
    P = P.merge(E5.anchors(P), on="episode_id", how="inner")
    P = P[P.n20 >= 20]
    P = E5.blocks(P)
    toks = pd.read_parquet(G9.TOKS).token.tolist()
    MEM = E9.membership(toks, C).merge(P[["episode_id", "block"]],
                                       on="episode_id", how="inner")
    bcode, _ = pd.factorize(MEM.block)
    nb = bcode.max() + 1
    fin_sig = {}
    for cl in [c for c in MEM.columns if "|" in c]:
        s = MEM[cl].to_numpy()
        nt = np.bincount(bcode, weights=s, minlength=nb)
        nc = np.bincount(bcode, minlength=nb) - nt
        inov = ((nt > 0) & (nc > 0))[bcode]
        fin_sig[cl] = hashlib.sha256((s & inov).tobytes()).hexdigest()[:16]
    R["final_sig"] = R.claim.map(fin_sig)
    okR = R[R.support_ok]
    final_classes = int(okR.final_sig.nunique())
    final_aliases = final_names - final_classes
    fin_to_prov = okR.groupby("final_sig").equivalence_class_id.nunique()
    merges = int((fin_to_prov > 1).sum())
    splits = int((okR.groupby("equivalence_class_id").final_sig.nunique() > 1).sum())

    ids = {
        "945 = 885 + 60":  initial_names == initial_classes + initial_aliases
                           and (initial_names, initial_classes, initial_aliases)
                           == (945, 885, 60),
        "907 = 853 + 54":  final_names == final_classes + final_aliases
                           and (final_names, final_classes, final_aliases) == (907, 853, 54),
        "945 - 907 = 38":  removed_names == 38,
        "885 - 853 = 32":  initial_classes - final_classes == 32 == lost_classes,
        "60 - 54 = 6":     initial_aliases - final_aliases == 6,
        # THE TRUE DECOMPOSITION, from data: aliases share their class's membership_hash,
        # hence identical overlap statistics, hence identical eligibility — a name can
        # only be removed when its WHOLE class is removed. The 6 lost alias names are
        # aliases OF the lost classes themselves (6 two-name lost classes), not of
        # surviving ones. The first draft assumed 32 singleton lost classes + 6 aliases
        # stripped from surviving classes and correctly FAILED against data.
        "38 = 32 + 6":     removed_names == lost_classes + aliases_in_lost_classes == 38
                           and names_in_lost_classes == 38
                           and aliases_in_lost_classes == 6
                           and alias_names_lost_from_surviving == 0,
        "class-whole removal (surviving classes lost 0 names)":
                           alias_names_lost_from_surviving == 0,
        "census total 38": sum(census.values()) == 38
                           and census.get("TREATED_FLOOR") == 33
                           and census.get("DATE_SHARE") == 3
                           and census.get("TREATED_FLOOR,DATE_SHARE") == 2,
        "merges = 0":      merges == 0,
        "splits = 0":      splits == 0,
    }
    ok = all(ids.values())
    body = dict(
        gate="K_ACCOUNTING_CLOSURE", result="PASS" if ok else "FAIL",
        initial=dict(names=initial_names, classes=initial_classes,
                     aliases=initial_aliases),
        final=dict(names=final_names, classes=final_classes, aliases=final_aliases),
        removed=dict(names=removed_names, whole_classes_lost=lost_classes,
                     names_in_lost_classes=names_in_lost_classes,
                     aliases_in_lost_classes=aliases_in_lost_classes,
                     alias_names_lost_from_surviving_classes=alias_names_lost_from_surviving,
                     lost_class_size_distribution={f"{int(k)}_name_classes": int(v)
                                                   for k, v in lost_size_dist.items()},
                     fail_census=census),
        removal_law="aliases share their class's membership_hash, hence identical overlap "
                    "statistics, hence identical eligibility — removal is CLASS-WHOLE by "
                    "construction; '38 = 32 + 6' reads: 38 removed names = the complete "
                    "name sets of 32 lost classes = 32 class representatives + 6 aliases "
                    "of those same lost classes (6 two-name classes); surviving classes "
                    "lost 0 names",
        merges=merges, splits=splits,
        identities={k: bool(v) for k, v in ids.items()},
        supersedes="T3_HOLD_CLOSURE_V1.item2.syntactic.support_qualified_grammar (a T9 "
                   "literal 980; the correct T3 value is 945) and the report phrase "
                   "'54 excluded names' (54 is the FINAL alias count; removed names = 38)")
    return body, ok


# ═══════════════════════════════ ITEM 2 ═══════════════════════════════
CMP_COLS = ["family", "length", "canonical_sequence", "membership_hash",
            "episode_n", "ticker_n", "date_n", "max_date_share", "token_idx"]


def _canon(C):
    # equivalence_class_id is an ngroup label and legitimately depends on row order;
    # everything semantic lives in CMP_COLS
    return C[CMP_COLS].sort_values(["family", "length", "canonical_sequence"]) \
                      .reset_index(drop=True)


def _alias_sets(C):
    g = C.groupby(["family", "length", "membership_hash"]).canonical_sequence \
         .apply(lambda s: tuple(sorted(s)))
    return set(map(tuple, zip(g.index.tolist(), g.tolist())))


def item2_determinism():
    """Two FRESH full enumerations; both must reproduce the sealed class digest and the
    sealed claim table field-for-field."""
    sealed = pd.read_parquet(G9.SURV)
    d_sealed = G9.class_digest(sealed)

    _, C1, _ = G9.enumerate_grammar(G9.load_ms(), verbose=False)
    d1 = G9.class_digest(C1)
    print(f"  run1 fresh ordered   digest {d1}", flush=True)
    _, C2, _ = G9.enumerate_grammar(G9.load_ms(shuffle_seed=707), verbose=False)
    d2 = G9.class_digest(C2)
    print(f"  run2 fresh shuffled  digest {d2}", flush=True)

    eq_claims = set(C1.canonical_sequence + "|" + C1.family) \
        == set(C2.canonical_sequence + "|" + C2.family) \
        == set(sealed.canonical_sequence + "|" + sealed.family)
    eq_frames = _canon(C1).equals(_canon(C2)) and _canon(C1).equals(_canon(sealed))
    eq_mh = eq_frames  # membership_hash is a CMP column
    eq_alias = _alias_sets(C1) == _alias_sets(C2) == _alias_sets(sealed)
    # representative = lexicographically first name of each class
    rep = lambda C: set(C.groupby(["family", "length", "membership_hash"])
                        .canonical_sequence.min().items())
    eq_rep = rep(C1) == rep(C2) == rep(sealed)

    ok = (d1 == d2 == d_sealed == SEALED_CLASS_DIGEST
          and eq_claims and eq_frames and eq_mh and eq_alias and eq_rep)
    body = dict(
        gate="DETERMINISM_TWO_RUN", result="PASS" if ok else "FAIL",
        run1_digest=d1, run2_digest=d2, sealed_digest=d_sealed,
        frozen_expectation=SEALED_CLASS_DIGEST,
        equality=dict(class_digest=bool(d1 == d2 == d_sealed == SEALED_CLASS_DIGEST),
                      claim_set=bool(eq_claims),
                      membership_hash=bool(eq_mh),
                      representative=bool(eq_rep),
                      alias_sets=bool(eq_alias),
                      full_claim_table=bool(eq_frames)),
        note="run2 consumed a row-shuffled source (seed 707, a NEW seed, not the build's "
             "101) — digest equality is therefore both a rerun proof and an order-"
             "independence proof")
    return body, ok


# ═══════════════════════════════ ITEM 3 ═══════════════════════════════
def _raw_t3(o, c, po, pc):
    return ((pc <= po) & (c > o) & (o < po) & (o < pc) & (c < po) & (c > pc))


def item3_doji_provenance():
    """SEMANTIC SAME-INPUT (engine on current bars) vs CROSS-VINTAGE DIAGNOSTIC
    (materialized canonical joined to current bars) — separated for good."""
    conn = duckdb.connect(D9.DB1D, read_only=True)
    # (a) SAME-INPUT: engine bc==9 with an exact previous doji, on identical bars
    n_bc9 = n_bc9_doji = tot = 0
    for uni, lim in (("sp500", 80), ("nasdaq", 80)):
        tks = [r[0] for r in conn.execute(
            "SELECT DISTINCT ticker FROM bars WHERE universe=? ORDER BY ticker LIMIT ?",
            [uni, lim]).fetchall()]
        for tk in tks:
            df = conn.execute("""SELECT date, any_value(open) AS "open",
                any_value(high) AS "high", any_value(low) AS "low",
                any_value(close) AS "close", any_value(volume) AS "volume"
                FROM bars WHERE ticker=? AND universe=? GROUP BY date ORDER BY date""",
                [tk, uni]).fetchdf()
            if len(df) < 30:
                continue
            bc = compute_signals(df.copy()).bc.to_numpy()
            doji_prev = (df.open.shift(1) == df.close.shift(1)).fillna(False).to_numpy()
            tot += len(df)
            n_bc9 += int((bc == 9).sum())
            n_bc9_doji += int(((bc == 9) & doji_prev).sum())
    same_input_ok = n_bc9_doji == 0

    # (b) CROSS-VINTAGE: materialized canonical T3 joined to CURRENT bars
    D = conn.execute("""
        WITH d AS (SELECT ticker, date, open, close, t_sig,
                          row_number() OVER (PARTITION BY ticker, date ORDER BY
                            CASE universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1
                            ELSE 2 END) rn FROM bars),
             u AS (SELECT * FROM d WHERE rn=1),
             lagd AS (SELECT ticker, date, open, close, t_sig,
                             lag(open)  OVER (PARTITION BY ticker ORDER BY date) po,
                             lag(close) OVER (PARTITION BY ticker ORDER BY date) pc
                      FROM u)
        SELECT open, close, po, pc FROM lagd WHERE t_sig='T3' AND po IS NOT NULL
    """).fetchdf()
    conn.close()
    canon_now = len(D)
    doji_now = D[D.po == D.pc]
    n_doji_now = len(doji_now)
    raw_on_doji = int(_raw_t3(doji_now.open, doji_now.close,
                              doji_now.po, doji_now.pc).sum())
    sub5 = round(float((doji_now.pc < 5).mean()), 4) if n_doji_now else None
    cross_ok = raw_on_doji == 0

    ok = same_input_ok and cross_ok
    body = dict(
        gate="DOJI_PROVENANCE", result="PASS" if ok else "FAIL",
        semantic_same_input=dict(
            statement="previous-doji T3 = 0",
            bars_tested=tot, engine_bc9=n_bc9, engine_bc9_with_exact_prev_doji=n_bc9_doji,
            impossibility="pc==po contradicts (c>pc)&(c<po); the engine reproduces this "
                          "on identical bars — machine PASS, not argument",
            result="PASS" if same_input_ok else "FAIL"),
        cross_vintage_diagnostic=dict(
            statement="materialized canonical T3 joined to current/reconstructed bars: "
                      f"{n_doji_now} observations appear doji/precision-boundary-like "
                      f"({284} at the sealed 2026-08-17 build vintage)",
            canonical_rows_current_vintage=canon_now,
            prev_doji_rows_current_vintage=n_doji_now,
            raw_geometry_satisfied_among_them=raw_on_doji,
            prev_price_sub_5_share=sub5,
            role="DIAGNOSTIC ONLY — NOT evidence that T3 semantics admit previous-doji "
                 "cases; historical canonical membership repair FORBIDDEN",
            vintage_note="counts against the CURRENT store are window-vintage-dependent "
                         "by the vintage law; the sealed build-vintage figure remains 284"),
        supersedes="the T3_DNA audit phrase 'canonical prev-doji = 284' is to be read "
                   "ONLY under the split above; T3_HOLD_CLOSURE_V1.item1.cross_vintage."
                   "canonical_population contained a T9 literal — the canonical "
                   "population is materialized t_sig='T3'")
    return body, ok


def main():
    t0 = time.time()
    b1, ok1 = item1_k_accounting()
    print(f"1 K ACCOUNTING   {b1['result']} · " +
          " · ".join(f"{k}->{v}" for k, v in b1["identities"].items()), flush=True)
    b3, ok3 = item3_doji_provenance()
    print(f"3 DOJI PROVENANCE {b3['result']} · same-input bc9-doji "
          f"{b3['semantic_same_input']['engine_bc9_with_exact_prev_doji']} of bc9 "
          f"{b3['semantic_same_input']['engine_bc9']} · cross-vintage now "
          f"{b3['cross_vintage_diagnostic']['prev_doji_rows_current_vintage']} "
          f"(raw-satisfying {b3['cross_vintage_diagnostic']['raw_geometry_satisfied_among_them']})",
          flush=True)
    b2, ok2 = item2_determinism()
    print(f"2 DETERMINISM    {b2['result']} · run1 {b2['run1_digest']} == run2 "
          f"{b2['run2_digest']} == sealed {b2['sealed_digest']}", flush=True)

    res = "PASS" if ok1 and ok2 and ok3 else "FAIL"
    digest = ART.seal(dict(spec_id="T3_STOP_CLOSURE_2_V1", result=res,
                           item1=b1, item2=b2, item3=b3,
                           relation="supersedes the mislabeled fields named in item1/"
                                    "item3 'supersedes'; T3_HOLD_CLOSURE_V1 stays sealed "
                                    "as history, its machine results are unchanged",
                           runtime_s=round(time.time() - t0, 1)),
                      OUT, required=("spec_id", "result", "item1", "item2", "item3"),
                      supersede=os.path.exists(OUT))
    print(f"\nT3_STOP_CLOSURE_2_V1 · {digest} · {res} · {time.time()-t0:.0f}s")
    if res != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
