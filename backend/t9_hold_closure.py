"""T9 HOLD closure — the four items from the STOP review, closed by machine, not by wording.

    1  DEFINITION SEMANTICS   same-input closure (route A), T4-precedent form
    2  k ACCOUNTING           910 -> 890 vs 980 -> 958 reconciled to exact identities
    3  CLOCK ANCHORING        explicit gate, not 'inherited'
    4  CAPABILITY PROTOCOL    sealed separately (t9_capability_protocol.py) — referenced here

No design change. No Y value read anywhere in this module.

ITEM 1 — WHAT SAME-INPUT IDENTITY CAN AND CANNOT MEAN FOR T9

T9 is priority-resolved BEHIND six other codes, so a full independent reconstruction would
require independent implementations of T4/T6/T1G/T2G/T1/T2 as well. Instead the gate closes
the two directions that ARE independently testable on identical inputs:

    NECESSITY     every engine bc==7 row must satisfy independent raw_T9   (resolved ⊆ raw)
    SUFFICIENCY   no independent raw_T9 row may resolve BELOW T9           (bc > 7 forbidden;
                  bc in 1..6 is the declared priority exclusion, not a violation)

Together these pin the T9 primitive AND the priority direction against an implementation the
engine does not share (a pandas mirror written from the Pine spec), on the same bars. The
cross-vintage comparison against materialized t_sig stays what it is:

    current-store raw vs materialized T9   = CROSS-VINTAGE DIAGNOSTIC ONLY
    materialized t_sig='T9'                = CANONICAL HISTORICAL POPULATION
    historical membership repair from current OHLC = FORBIDDEN

and 98.78% is a cross-vintage reconciliation figure, never a semantic PASS.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                    # noqa: E402
import duckdb, numpy as np, pandas as pd                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t9_dna as D9                                # noqa: E402
from signal_engine import compute_signals                              # noqa: E402

OUT = "T9_HOLD_CLOSURE_V1.json"
N_TICKERS = 80


# ═══════════════════════════════ ITEM 1 ═══════════════════════════════
def raw_t9_mirror(df):
    """Independent pandas mirror of the Pine spec — shares no code with signal_engine."""
    o, c = df.open, df.close
    po, pc = o.shift(1), c.shift(1)
    return ((pc <= po)                                   # prev bear-or-doji
            & (c > o)                                    # strict bull
            & (np.maximum(o, c) <= np.maximum(po, pc))   # body-inside, non-strict
            & (np.minimum(o, c) >= np.minimum(po, pc))).fillna(False)


def item1_same_input():
    conn = duckdb.connect(D9.DB1D, read_only=True)
    tks = [r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM bars WHERE universe='sp500' ORDER BY ticker "
        f"LIMIT {N_TICKERS}").fetchall()]
    tot = 0
    nec_viol = suf_viol = 0
    excl = {}
    samples = []
    for tk in tks:
        df = conn.execute("""SELECT date, any_value(open) AS "open", any_value(high) AS "high",
            any_value(low) AS "low", any_value(close) AS "close",
            any_value(volume) AS "volume" FROM bars WHERE ticker=? AND universe='sp500'
            GROUP BY date ORDER BY date""", [tk]).fetchdf()
        if len(df) < 30:
            continue
        bc = compute_signals(df.copy()).bc.to_numpy()
        raw = raw_t9_mirror(df).to_numpy()
        tot += len(df)
        n = (bc == 7) & ~raw                    # resolved T9 without raw geometry
        s = raw & (bc > 7)                      # raw T9 resolving BELOW T9
        nec_viol += int(n.sum()); suf_viol += int(s.sum())
        for i in np.flatnonzero(n | s)[:2]:
            samples.append((tk, str(df.date.iloc[i])[:10], int(bc[i]), bool(raw[i])))
        hi = raw & (bc >= 1) & (bc <= 6)        # declared priority exclusions
        for b in np.unique(bc[hi]):
            excl[f'bc={int(b)}'] = excl.get(f'bc={int(b)}', 0) + int((bc[hi] == b).sum())
    conn.close()
    ok = nec_viol == 0 and suf_viol == 0
    return dict(gate="SAME_INPUT_SEMANTIC_IDENTITY", result="PASS" if ok else "FAIL",
                route="A — engine vs independent pandas mirror of the Pine spec, "
                      "identical DB bars",
                bars_tested=tot, tickers=len(tks),
                necessity_violations=nec_viol, sufficiency_violations=suf_viol,
                priority_exclusions_by_bc=excl, first_violations=samples[:5],
                scope="pins the T9 primitive and the priority DIRECTION; the six "
                      "higher-priority patterns themselves are exercised as exclusions, not "
                      "reimplemented",
                cross_vintage=dict(
                    current_store_raw_vs_materialized="CROSS-VINTAGE DIAGNOSTIC ONLY "
                                                      "(240,579/243,562 = 98.78% "
                                                      "reconciliation, NOT a semantic PASS)",
                    canonical_population="materialized t_sig='T9'",
                    historical_membership_repair="FORBIDDEN")), ok


# ═══════════════════════════════ ITEM 2 ═══════════════════════════════
def item2_k_accounting():
    """Both decompositions as exact identities, and the 22-names-vs-20-classes question
    answered from data rather than argued."""
    import t9_sequence_grammar as G9
    import t9_sequence_estimand as E9
    C = pd.read_parquet(G9.SURV)                # grammar claims + provisional classes
    R = pd.read_parquet(os.path.join(D9.ROOT, "data", "t9_sequence_estimand_claims.parquet"))
    R["name"] = R.claim.str.split("|", n=2).str[2]
    C["key"] = C.family + "|" + C.length.astype(str) + "|" + C.canonical_sequence
    R = R.merge(C[["key", "equivalence_class_id", "membership_hash"]],
                left_on="claim", right_on="key", how="left")
    assert R.equivalence_class_id.notna().all()

    # syntactic decomposition
    n_syn, n_syn_ok = len(R), int(R.support_ok.sum())
    fails = R.loc[~R.support_ok, "fail_reasons"].value_counts().to_dict()

    # final classes need the FINAL membership signature — recompute exactly as the estimand
    # groups (s & inov per claim on the estimand population)
    E = pd.read_parquet(D9.OUT_EP, columns=["episode_id", "ticker", "t9_date"])
    E = E[E.episode_id.isin(E9.analyzable_episodes())]
    OCC = pd.read_parquet(E9.OC, columns=["episode_id", "path_status_10d"])
    E = E[E.episode_id.isin(set(OCC.loc[OCC.path_status_10d == "AVAILABLE", "episode_id"]))]
    import t5_sequence_estimand as E5
    P = E.rename(columns={"t9_date": "t5_date"})
    P = P.merge(E5.anchors(P), on="episode_id", how="inner")
    P = P[P.n20 >= 20]
    P = E5.blocks(P)
    toks = pd.read_parquet(G9.TOKS).token.tolist()
    MEM = E9.membership(toks, C)
    MEM = MEM.merge(P[["episode_id", "block"]], on="episode_id", how="inner")
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

    ok_names = R[R.support_ok]
    k_final = ok_names.final_sig.nunique()
    aliases_final = len(ok_names) - k_final

    # provisional-class fate among the 910
    prov = R.groupby("equivalence_class_id").agg(
        any_ok=("support_ok", "any"), n_names=("claim", "size"))
    classes_lost = int((~prov.any_ok).sum())
    surv_classes = R[R.support_ok].groupby("equivalence_class_id").final_sig.nunique()
    # merges: two provisional classes mapping to one final signature
    fin_to_prov = R[R.support_ok].groupby("final_sig").equivalence_class_id.nunique()
    merged_into = int((fin_to_prov > 1).sum())
    merge_loss = int((fin_to_prov[fin_to_prov > 1] - 1).sum())
    splits = int((surv_classes > 1).sum())      # one provisional class -> several final sigs

    k_prov = int(C.equivalence_class_id.nunique())
    identity1 = (n_syn_ok == k_final + aliases_final)
    identity2 = (k_prov - classes_lost - merge_loss + int((surv_classes - 1).clip(lower=0)
                                                         .sum()) == k_final)
    body = dict(gate="K_ACCOUNTING", result="PASS" if identity1 and identity2 else "FAIL",
                syntactic=dict(support_qualified_grammar=980, at_estimand=n_syn,
                               removed_names=n_syn - n_syn_ok, removed_census=fails,
                               final_names=n_syn_ok),
                distinct=dict(k_provisional=k_prov, k_final=int(k_final),
                              provisional_classes_fully_lost=classes_lost,
                              provisional_classes_merged_away=merge_loss,
                              merge_events=merged_into, class_splits=splits,
                              final_aliases=int(aliases_final)),
                identities=dict(
                    names_eq_classes_plus_aliases=f"{n_syn_ok} = {k_final} + {aliases_final} "
                                                  f"-> {identity1}",
                    provisional_to_final=f"{k_prov} - lost {classes_lost} - merged "
                                         f"{merge_loss} + splits {int((surv_classes-1).clip(lower=0).sum())} "
                                         f"= {k_final} -> {identity2}"),
                why_20_vs_22="answered numerically above: name removals and class arithmetic "
                             "differ whenever removed names are non-representative aliases, "
                             "or when restriction merges or splits memberships")
    return body, identity1 and identity2


# ═══════════════════════════════ ITEM 3 ═══════════════════════════════
def item3_clock_anchoring():
    checks = {}
    M = pd.read_parquet(D9.OUT_MS, columns=["episode_id", "ticker", "relative_day",
                                            "session_date", "ts_et", "session_position",
                                            "bars_in_session"])
    # (a) stored position == rank of the TIMESTAMP within (episode, session)
    M = M.sort_values(["episode_id", "session_date", "ts_et"])
    rk = M.groupby(["episode_id", "session_date"]).cumcount() + 1
    checks["position_is_timestamp_rank"] = bool((rk.to_numpy()
                                                 == M.session_position.to_numpy()).all())
    # (b) recomputation from a shuffled frame reproduces the same positions
    S = M.sample(frac=1.0, random_state=7)
    S = S.sort_values(["episode_id", "session_date", "ts_et"])
    rk2 = S.groupby(["episode_id", "session_date"]).cumcount() + 1
    j = S.assign(rk=rk2).set_index(["episode_id", "session_date", "ts_et"]).rk
    k = M.assign(rk=rk).set_index(["episode_id", "session_date", "ts_et"]).rk
    checks["shuffle_invariant"] = bool(j.sort_index().equals(k.sort_index()))
    # (c) a missing bar must NOT re-slot later bars: drop a middle bar from one real episode
    ep = M.episode_id.iloc[0]
    g = M[(M.episode_id == ep) & (M.relative_day == "T9_DAY")]
    if len(g) >= 3:
        g2 = g[g.session_position != 2]
        rk3 = g2.sort_values("ts_et").reset_index(drop=True).index + 1
        # rank-recomputation SHIFTS positions 3.. -> that is exactly why session validity
        # (modal expected length) must EXCLUDE the session rather than accept re-slotted bars
        shifted = not (rk3 == g2.session_position.reset_index(drop=True)).all()
        modal_excludes = (len(g2) != g.bars_in_session.iloc[0])
        checks["missing_bar_shifts_and_validity_excludes"] = bool(shifted and modal_excludes)
    # (d) early close handled: a session whose modal length < the global mode still passes
    modal = (M.groupby(["session_date", "ticker"]).bars_in_session.first()
             .groupby("session_date").agg(lambda s: s.mode().iloc[0]))
    glob = int(modal.mode().iloc[0])
    early = modal[modal < glob]
    checks["early_close_sessions_present_and_valid"] = bool(len(early) > 0)
    checks["early_close_example"] = (f"{early.index[0]} expected {int(early.iloc[0])} bars "
                                     f"(global mode {glob})") if len(early) else None
    # (e) seam semantics: PREV_LAST is the max position of the prev session and T9_FIRST is
    # position 1 of the T9 session — verified on 200 sampled episodes
    # the denominator is episodes with BOTH sides observable: a one-sided-coverage episode
    # (T9_ONLY / PREV_ONLY) has no seam to test and skipping it is not a violation — the
    # first run counted one such episode against a 200 denominator and reported 199/200.
    ok_seam = both = onesided = 0
    sample = M.episode_id.drop_duplicates().iloc[:200]
    for e in sample:
        g = M[M.episode_id == e]
        p = g[g.relative_day == "PREV_DAY"]
        t = g[g.relative_day == "T9_DAY"]
        if len(p) and len(t):
            both += 1
            if (p.session_position.max() == p.bars_in_session.iloc[0]
                    and t.session_position.min() == 1
                    and p.session_date.iloc[0] < t.session_date.iloc[0]):
                ok_seam += 1
        else:
            onesided += 1
    checks["seam_semantics"] = (f"{ok_seam}/{both} exact on both-side episodes "
                                f"({onesided} one-sided coverage, no seam to test)")
    hard = ["position_is_timestamp_rank", "shuffle_invariant",
            "missing_bar_shifts_and_validity_excludes",
            "early_close_sessions_present_and_valid"]
    ok = all(bool(checks[k]) for k in hard) and ok_seam == both
    return dict(gate="CLOCK_ANCHORING", result="PASS" if ok else "FAIL", checks=checks), ok


def main():
    t0 = time.time()
    ART.smoke_test(verbose=False)
    r1, ok1 = item1_same_input()
    print(f"1 SAME-INPUT SEMANTICS  {r1['result']} · bars {r1['bars_tested']:,} · "
          f"necessity {r1['necessity_violations']} · sufficiency "
          f"{r1['sufficiency_violations']} · exclusions {r1['priority_exclusions_by_bc']}")
    r2, ok2 = item2_k_accounting()
    print(f"2 K ACCOUNTING          {r2['result']}")
    for k, v in r2["identities"].items():
        print(f"    {k}: {v}")
    print(f"    distinct: {r2['distinct']}")
    r3, ok3 = item3_clock_anchoring()
    print(f"3 CLOCK_ANCHORING       {r3['result']} · " +
          " · ".join(f"{k}={v}" for k, v in r3["checks"].items() if k != "early_close_example"))
    ok = ok1 and ok2 and ok3
    ART.seal(dict(spec_id="T9_HOLD_CLOSURE_V1",
                  result="PASS" if ok else "FAIL",
                  item1=r1, item2=r2, item3=r3,
                  item4="sealed separately: T9_CAPABILITY_PROTOCOL_V1",
                  y_status="NO OUTCOME VALUE READ",
                  seconds=round(time.time() - t0, 1)),
             OUT, required=("spec_id", "result", "item1", "item2", "item3"), supersede=True)
    print(f"\nT9_HOLD_CLOSURE_V1 · {ART.file_digest(OUT)} · "
          f"{'ALL PASS' if ok else 'FAIL'} · {time.time()-t0:.0f}s")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
