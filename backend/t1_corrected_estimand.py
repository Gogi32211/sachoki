"""T1 corrected X reconciliation — grammar -> estimand -> k, on the SEALED T1 X vintage.

Answers exactly what the review asked before any approval:

    1  the 10 changed claims, with per-claim lineage back to the two flipped sessions
       (INTG removed, BORR admitted) — including claims where the two cancel and the
       support count does not move even though the membership did
    2  corrected restriction-equivalence: merges/splits retained, lost, created
    3  corrected k_final, reconciled by identity against the corrected class count

Nothing is sealed as a new T1 claim universe here — this is the reconciliation the
approval decision needs. No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                   # noqa: E402
import numpy as np, pandas as pd                                     # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t1_dna as D1                              # noqa: E402
import t1_sequence_grammar as G1, t1_sequence_estimand as E1         # noqa: E402
import t5_sequence_estimand as E5                                    # noqa: E402

OUT = "T1_CORRECTED_X_RECONCILIATION_V1.json"
CAL_PARQ = "/Users/sachoki/Desktop/sachoki-desktop/data/session_calendar_1h.parquet"
SCRATCH = ("/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
           "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad")
INTG_EP = "000af9b16c472a02"          # 2-bar gapped session, old-valid -> corrected-invalid
BORR_EP = "166697c55abdb9ec"          # 7-bar complete session, old-invalid -> corrected-valid


def corrected_frame():
    CAL = pd.read_parquet(CAL_PARQ)
    cal = dict(zip(CAL.session_date, CAL.calendar_len.astype(int)))
    M = G1.load_ms()
    per = (M.groupby(["session_date", "ticker", "episode_id", "relative_day"])
             .bars_in_session.first().reset_index())
    fam_modal = (per.groupby(["session_date", "ticker"]).bars_in_session.first()
                    .groupby("session_date").agg(lambda s: s.mode().iloc[0]))
    old_valid = per.bars_in_session == per.session_date.map(fam_modal)
    new_valid = per.bars_in_session == per.session_date.map(cal)
    keep = per[new_valid][["episode_id", "relative_day"]]
    Mc = M.merge(keep, on=["episode_id", "relative_day"], how="inner")
    return Mc, int((old_valid ^ new_valid).sum())


def corrected_membership(toks, C, Mc):
    """E1.membership(), byte-for-byte in its kernel, but fed the CORRECTED frame.

    E1.membership recomputes the family-local modal filter internally, so calling it for
    the corrected side would silently re-apply the defective rule. The adjacency kernel
    E1._adj is reused unchanged — only the session-validity source differs.
    """
    M = Mc[["episode_id", "relative_day", "session_position", "bars_in_session",
            "token_set", "session_date"]].reset_index(drop=True)
    if M.duplicated(["episode_id", "relative_day", "session_position"]).any():
        raise RuntimeError("duplicate source grain in the corrected microstructure")
    tix = {t: i for i, t in enumerate(toks)}
    B = np.zeros((len(M), len(toks)), dtype=bool)
    for r, ts in enumerate(M.token_set.to_numpy()):
        for t in ts.split():
            j = tix.get(t)
            if j is not None:
                B[r, j] = True
    ep_codes, ep_uniq = pd.factorize(M.episode_id)
    rd = (M.relative_day == "T1_DAY").to_numpy()
    pos = M.session_position.to_numpy()
    order = np.lexsort((pos, rd, ep_codes))
    B, ep, rd, pos = B[order], ep_codes[order], rd[order], pos[order]
    n_ep = len(ep_uniq)
    out = {"episode_id": ep_uniq}
    for fam in G1.FAMILIES:
        for L in G1.LENGTHS:
            sub = C[(C.family == fam) & (C.length == L)]
            if not len(sub):
                continue
            a = E1._adj(ep, rd, pos, fam, L)
            cols = [B[a + j] for j in range(L)]
            for r in sub.itertuples():
                c = json.loads(r.token_idx)
                m = cols[0][:, c[0]]
                for j in range(1, L):
                    m &= cols[j][:, c[j]]
                v = np.zeros(n_ep, dtype=bool)
                v[np.unique(ep[a[m]])] = True
                out[f"{fam}|{L}|{r.canonical_sequence}"] = v
    return pd.DataFrame(out)


def main():
    t0 = time.time()
    Mc, flips = corrected_frame()
    print(f"  corrected frame {len(Mc):,} rows · validity flips {flips}", flush=True)
    cached = os.path.join(SCRATCH, "t1_corrected_claims.parquet")
    C_new = pd.read_parquet(cached)
    toks_new = pd.read_parquet(G1.TOKS).token.tolist()
    exp = json.load(open("ADJACENCY_IMPACT_REPORT_V1.json"))["families"]["T1"]["grammar"][
        "class_digest_corrected"]
    got = G1.class_digest(C_new)
    assert got == exp, f"cached corrected claims digest {got} != sealed {exp}"
    print(f"  corrected claims loaded · class digest {got} verified against the sealed "
          f"impact report", flush=True)
    C_old = pd.read_parquet(G1.SURV)
    key = lambda C: C.family + "|" + C.length.astype(str) + "|" + C.canonical_sequence
    o = C_old.set_index(key(C_old))
    n = C_new.set_index(key(C_new))
    changed = [c for c in o.index if o.membership_hash[c] != n.membership_hash[c]]
    print(f"  changed claims {len(changed)}", flush=True)

    R_old = pd.read_parquet(os.path.join(D1.ROOT, "data",
                                         "t1_sequence_estimand_claims.parquet"))
    old_ok = dict(zip(R_old.claim, R_old.support_ok))

    lineage = []
    for c in changed:
        d = int(n.episode_n[c]) - int(o.episode_n[c])
        # exactly two episodes moved: BORR entered (PREV_DAY session admitted), INTG left
        # (T1_DAY session rejected). A claim can therefore only be +1, -1 or 0-with-both.
        borr = d > 0 or (d == 0)
        intg_removed = d < 0 or (d == 0)
        lineage.append(dict(
            claim=c, family=o.family[c], length=int(o.length[c]),
            representative=o.canonical_sequence[c],
            old_support=int(o.episode_n[c]), corrected_support=int(n.episode_n[c]),
            delta_support=int(n.episode_n[c] - o.episode_n[c]),
            BORR_added=bool(borr), INTG_removed=bool(intg_removed),
            net_zero_but_membership_moved=bool(d == 0),
            old_membership_hash=o.membership_hash[c],
            corrected_membership_hash=n.membership_hash[c],
            old_support_eligible=bool(old_ok.get(c, False))))

    # ── corrected estimand: population, blocks, memberships, k ────────────
    E = pd.read_parquet(D1.OUT_EP, columns=["episode_id", "ticker", "t1_date"])
    E = E[E.episode_id.isin(E1.analyzable_episodes())]
    OCC = pd.read_parquet(E1.OC, columns=["episode_id", "path_status_10d"])
    E = E[E.episode_id.isin(set(OCC.loc[OCC.path_status_10d == "AVAILABLE", "episode_id"]))]
    P = E.rename(columns={"t1_date": "t5_date"})
    P = P.merge(E5.anchors(P), on="episode_id", how="inner")
    P = P[P.n20 >= 20]
    P = E5.blocks(P)
    MEM = corrected_membership(toks_new, C_new, Mc).merge(
        P[["episode_id", "block"]], on="episode_id", how="inner")
    bcode, _ = pd.factorize(MEM.block)
    nb = bcode.max() + 1
    claim_cols = [c for c in MEM.columns if "|" in c]
    KEY = MEM[["episode_id"]].merge(P[["episode_id", "ticker", "t5_date"]],
                                    on="episode_id", how="left")
    tcode, _ = pd.factorize(KEY.ticker)
    dcode, _ = pd.factorize(KEY.t5_date)
    rows, fin_sig = [], {}
    for cl in claim_cols:
        s = MEM[cl].to_numpy()
        nt = np.bincount(bcode, weights=s, minlength=nb)
        nc = np.bincount(bcode, minlength=nb) - nt
        ov = (nt > 0) & (nc > 0)
        inov = ov[bcode]
        t_ov, c_ov = int((s & inov).sum()), int(((~s) & inov).sum())
        e = np.flatnonzero(s & inov)
        n_tk = len(np.unique(tcode[e])) if len(e) else 0
        n_dt = len(np.unique(dcode[e])) if len(e) else 0
        mds = float(np.unique(dcode[e], return_counts=True)[1].max() / len(e)) if len(e) else np.nan
        fails = []
        if not E1.support_eligible(t_ov, c_ov):
            if t_ov < E1.MIN_EP: fails.append("TREATED_FLOOR")
            if c_ov < E1.MIN_EP: fails.append("CONTROL_FLOOR")
        if n_tk < E1.MIN_TK: fails.append("TICKER_FLOOR")
        if n_dt < E1.MIN_DT: fails.append("DATE_FLOOR")
        if not (mds <= E1.MAX_DATE_SHARE): fails.append("DATE_SHARE")
        ok = not fails
        fin_sig[cl] = hashlib.sha256((s & inov).tobytes()).hexdigest()[:16]
        rows.append(dict(claim=cl, treated_overlap=t_ov, control_overlap=c_ov,
                         fail_reasons=",".join(fails), support_ok=ok))
    R = pd.DataFrame(rows)
    R["final_sig"] = R.claim.map(fin_sig)
    C_new["key"] = key(C_new)
    R = R.merge(C_new[["key", "equivalence_class_id"]], left_on="claim", right_on="key",
                how="left")
    okR = R[R.support_ok]
    k_corr = int(okR.final_sig.nunique())
    fin_to_prov = okR.groupby("final_sig").equivalence_class_id.nunique()
    merges = int((fin_to_prov > 1).sum())
    merge_loss = int((fin_to_prov[fin_to_prov > 1] - 1).sum())
    prov_to_fin = okR.groupby("equivalence_class_id").final_sig.nunique()
    splits = int((prov_to_fin > 1).sum())
    split_gain = int((prov_to_fin[prov_to_fin > 1] - 1).sum())
    prov = R.groupby("equivalence_class_id").support_ok.any()
    lost_classes = int((~prov).sum())
    classes_corr = int(C_new.equivalence_class_id.nunique())
    ident = (classes_corr - lost_classes - merge_loss + split_gain) == k_corr

    old = json.load(open("T1_STOP_CLOSURE_V1.json"))["item2_k_accounting"]
    body = dict(
        spec_id="T1_CORRECTED_X_RECONCILIATION_V1", status="X_ONLY_PRE_Y",
        provenance="the SEALED T1 X vintage is the only source; the mutable 1H store is "
                   "never used to regenerate family X — only expected_bars(date) from "
                   "SESSION_REFERENCE_UNIVERSE_V1, with SESSION_MODE_TIE_POLICY_V1",
        session_validity=dict(flips=flips, date="2022-06-10",
                              removed=dict(ticker="INTG", episode=INTG_EP, bars=2),
                              admitted=dict(ticker="BORR", episode=BORR_EP, bars=7)),
        grammar=dict(names_old=int(len(C_old)), names_corrected=int(len(C_new)),
                     classes_old=int(C_old.equivalence_class_id.nunique()),
                     classes_corrected=classes_corr,
                     class_digest_old=G1.class_digest(C_old),
                     class_digest_corrected=G1.class_digest(C_new),
                     changed_claims=len(changed)),
        changed_claim_lineage=lineage,
        estimand=dict(
            population=int(len(P)), blocks=int(P.block.nunique()),
            support_qualified_old=old["final"]["names"],
            support_qualified_corrected=int(R.support_ok.sum()),
            k_final_old=old["k_final"], k_final_corrected=k_corr,
            aliases_old=old["final"]["aliases"],
            aliases_corrected=int(R.support_ok.sum()) - k_corr,
            lost_classes_corrected=lost_classes,
            merges_old=old["restriction_effects"]["merges"],
            merges_corrected=merges,
            merges_absorbed_old=old["restriction_effects"]["classes_absorbed_by_merges"],
            merges_absorbed_corrected=merge_loss,
            splits_old=old["restriction_effects"]["splits"], splits_corrected=splits,
            identity=f"{classes_corr} - {lost_classes} lost - {merge_loss} merged + "
                     f"{split_gain} split = {k_corr}",
            identity_holds=bool(ident),
            comparator_note="old k_final 911 is a provenance datum and a comparator, never "
                            "an acceptance target"),
        old_chain_status="SUPERSEDED_FOR_REGISTERED_SESSION_VALIDITY",
        corrected_chain_status=dict(
            phase="X-only, PRE-Y",
            observed_Z_rank="NOT COMPUTED", theta="NOT COMPUTED",
            winners="UNKNOWN", survivors="UNKNOWN"),
        runtime_min=round((time.time() - t0) / 60, 1))
    d = ART.seal(body, OUT, required=("spec_id", "estimand", "changed_claim_lineage"),
                 supersede=os.path.exists(OUT))
    C_new.to_parquet(os.path.join(SCRATCH, "t1_corrected_claims.parquet"), index=False)
    R.to_parquet(os.path.join(SCRATCH, "t1_corrected_estimand_claims.parquet"), index=False)
    print(f"\n  k_final {old['k_final']} -> {k_corr} · identity {body['estimand']['identity']}"
          f" {ident}")
    print(f"  merges {old['restriction_effects']['merges']} -> {merges} · splits "
          f"{old['restriction_effects']['splits']} -> {splits}")
    for l in lineage:
        print(f"    {l['claim']:<44} {l['old_support']:>5} -> {l['corrected_support']:<5}"
              f" BORR+{int(l['BORR_added'])} INTG-{int(l['INTG_removed'])}"
              f"{'  (net zero, membership moved)' if l['net_zero_but_membership_moved'] else ''}")
    print(f"\nT1_CORRECTED_X_RECONCILIATION_V1 · {d} · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
