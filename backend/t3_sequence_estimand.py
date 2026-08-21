"""T3_SEQUENCE_ESTIMAND_V1 — comparator, blocks, mobility, k_final. X-only, mirror of T5's.

    Delta_sb = median(MFE_10D | S=1, b) - median(MFE_10D | S=0, b)
    theta_s  = sum_b w_sb * Delta_sb            (treated-episode weights)
    blocks   = exact T3 date x pre-liquidity LOW/HIGH x pre-volatility LOW/HIGH
    anchor   = T3 - 2 sessions (dv20 median dollar volume; ATR14/close)
    split    = deterministic value-then-ticker tie-break, rank(method='first')

REUSE: anchors() and blocks() are CALLED from t5_sequence_estimand on frames whose columns are
temporarily renamed to its vocabulary (t3_date -> t5_date), so the pre-treatment anchor SQL
and the deterministic split run byte-identical. Membership is recomputed with T3 literals via
the same adjacency kernel the grammar used.

Support floors here add the CONTROL side (>=300 control episodes in overlap blocks). UNLIKE
T5's estimand, there is NO overlap-retention requirement: T5 registered >=0.80 in its own
freeze, this family's registration did not and forbids post-hoc mobility cutoffs — the
inherited filter was removed before this module ever ran. Mobility quantities are reported
as DIAGNOSTICS of operational permutation freedom — not proof of exchangeability; the
exchangeability assumption is stated verbatim in the artifact.

NO OUTCOME VALUE IS READ. The sidecar contributes path_status_10d — availability — only.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                    # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_sequence_estimand as E5                                      # noqa: E402
import t3_dna as D9                                                    # noqa: E402
import t3_sequence_grammar as G9                                       # noqa: E402

def support_eligible(t_ov, c_ov):
    """Registered episode floors ONLY. Retention is not an input BY SIGNATURE: after an
    unregistered inherited retention gate was identified during pre-first-use review and
    removed, eligibility was factored into a function whose argument list is the guarantee —
    a retention term cannot re-enter without changing this signature, which the regression
    test asserts."""
    return t_ov >= MIN_EP and c_ov >= MIN_EP


SPEC = os.path.join(HERE, "T3_SEQUENCE_ESTIMAND_V1.json")
CLAIMS_OUT = os.path.join(D9.ROOT, "data", "t3_sequence_estimand_claims.parquet")
OC = os.path.join(D9.ROOT, "data", "t3_outcomes.parquet")
MIN_EP, MIN_TK, MIN_DT = G9.MIN_EP, G9.MIN_TK, G9.MIN_DT
MAX_DATE_SHARE = G9.MAX_DATE_SHARE
MIN_OVERLAP_RETENTION = E5.MIN_OVERLAP_RETENTION
N_PERM, PRIMARY = E5.N_PERM, E5.PRIMARY


def analyzable_episodes() -> set:
    M = pd.read_parquet(D9.OUT_MS, columns=["episode_id", "session_date", "bars_in_session"])
    modal = M.groupby("session_date")["bars_in_session"].agg(lambda s: s.mode().iloc[0])
    return set(M.loc[M.bars_in_session == M.session_date.map(modal), "episode_id"])


def membership(toks, C) -> pd.DataFrame:
    M = pd.read_parquet(D9.OUT_MS, columns=[
        "episode_id", "relative_day", "session_position", "bars_in_session",
        "token_set", "session_date"])
    modal = M.groupby("session_date")["bars_in_session"].agg(lambda s: s.mode().iloc[0])
    M = M[M.bars_in_session == M.session_date.map(modal)].reset_index(drop=True)
    if M.duplicated(["episode_id", "relative_day", "session_position"]).any():
        raise RuntimeError("duplicate source grain in the microstructure")
    tix = {t: i for i, t in enumerate(toks)}
    B = np.zeros((len(M), len(toks)), dtype=bool)
    for r, s in enumerate(M.token_set.to_numpy()):
        for t in s.split():
            j = tix.get(t)
            if j is not None:
                B[r, j] = True
    ep_codes, ep_uniq = pd.factorize(M.episode_id)
    rd = (M.relative_day == "T3_DAY").to_numpy()
    pos = M.session_position.to_numpy()
    order = np.lexsort((pos, rd, ep_codes))
    B, ep, rd, pos = B[order], ep_codes[order], rd[order], pos[order]
    n_ep = len(ep_uniq)
    out = {"episode_id": ep_uniq}
    for fam in G9.FAMILIES:
        for L in G9.LENGTHS:
            sub = C[(C.family == fam) & (C.length == L)]
            if not len(sub):
                continue
            # the adjacency kernel is the grammar's own — same family literals
            a = _adj(ep, rd, pos, fam, L)
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


def _adj(ep, rd, pos, family, span):
    n = len(ep)
    idx = np.arange(n - span + 1)
    ok = np.ones(len(idx), bool)
    for j in range(span):
        ok &= ep[idx + j] == ep[idx]
    if family == "T3_INTRADAY":
        for j in range(span):
            ok &= rd[idx + j]
        for j in range(span - 1):
            ok &= pos[idx + j + 1] == pos[idx + j] + 1
    elif family == "PREV_INTRADAY":
        for j in range(span):
            ok &= ~rd[idx + j]
        for j in range(span - 1):
            ok &= pos[idx + j + 1] == pos[idx + j] + 1
    else:
        trans = np.zeros(len(idx), int)
        for j in range(span - 1):
            step = (~rd[idx + j]) & rd[idx + j + 1]
            same = rd[idx + j] == rd[idx + j + 1]
            trans += step
            ok &= step | (same & (pos[idx + j + 1] == pos[idx + j] + 1))
        ok &= trans == 1
    return idx[ok]


def main():
    t0 = time.time()
    fun = {}
    E = pd.read_parquet(D9.OUT_EP, columns=["episode_id", "ticker", "t3_date"])
    fun["episodes_total"] = int(len(E))

    ana = analyzable_episodes()
    E = E[E.episode_id.isin(ana)]
    fun["sequence_analyzable"] = int(len(E))          # BEFORE block construction, as frozen

    # availability only — never a value
    OCC = pd.read_parquet(OC, columns=["episode_id", "path_status_10d"])
    ok_ids = set(OCC.loc[OCC.path_status_10d == "AVAILABLE", "episode_id"])
    E = E[E.episode_id.isin(ok_ids)]
    fun["mfe10_available"] = int(len(E))

    # anchors + blocks: REUSED from T5's estimand on renamed frames
    P = E.rename(columns={"t3_date": "t5_date"})
    A = E5.anchors(P)
    P = P.merge(A, on="episode_id", how="inner")
    P = P[P.n20 >= 20]                                # full pre-window required
    fun["anchored_full_prewindow"] = int(len(P))
    P = E5.blocks(P)
    fun["n_primary_population"] = int(len(P))
    fun["n_blocks"] = int(P.block.nunique())
    fun["median_block_size"] = int(P.groupby("block").size().median())
    print("FUNNEL " + " · ".join(f"{k}={v:,}" for k, v in fun.items()), flush=True)

    C = pd.read_parquet(G9.SURV)
    toks = pd.read_parquet(G9.TOKS).token.tolist()
    fun["grammar_claims"] = int(C.equivalence_class_id.nunique())
    MEM = membership(toks, C)
    MEM = MEM.merge(P[["episode_id", "ticker", "t5_date", "block"]],
                    on="episode_id", how="inner")
    print(f"  membership joined: {len(MEM):,} episodes × "
          f"{len([c for c in MEM.columns if '|' in c])} claims · {time.time()-t0:.0f}s",
          flush=True)

    bcode, _ = pd.factorize(MEM.block)
    tcode, _ = pd.factorize(MEM.ticker)
    dcode, _ = pd.factorize(MEM.t5_date)
    nb = bcode.max() + 1
    claim_cols = [c for c in MEM.columns if "|" in c]
    rows, keep_sig = [], {}
    for c in claim_cols:
        s = MEM[c].to_numpy()
        nt = np.bincount(bcode, weights=s, minlength=nb)
        nc = np.bincount(bcode, minlength=nb) - nt
        ov = (nt > 0) & (nc > 0)
        inov = ov[bcode]
        t_all, t_ov = int(s.sum()), int((s & inov).sum())
        c_ov = int(((~s) & inov).sum())
        ret = t_ov / t_all if t_all else 0.0
        # RETENTION IS DIAGNOSTIC, NOT A FILTER — identified during pre-first-use review.
        # T5's estimand registered treated_overlap_retention >= 0.80 in its own
        # freeze; the T3 registration did NOT, and explicitly forbids a post-hoc mobility
        # cutoff. The inherited constant was silently sitting in the k_final path and was
        # removed BEFORE any T3 estimand ever ran. Retention is computed, stored per claim
        # and reported as a distribution — never selected on.
        # every registered condition evaluated UNCONDITIONALLY and stored per claim, so the
        # k_provisional -> k_final reduction decomposes exactly at the STOP report and any
        # other inherited gate would surface QUANTITATIVELY, not by trust. Diagnostic fields
        # only — the eligibility predicate is support_eligible() plus the three floors below,
        # nothing else.
        e = np.flatnonzero(s & inov)
        n_tk_ov = len(np.unique(tcode[e])) if len(e) else 0
        n_dt_ov = len(np.unique(dcode[e])) if len(e) else 0
        if len(e):
            _, dcnt = np.unique(dcode[e], return_counts=True)
            mds_ov = float(dcnt.max() / len(e))
        else:
            mds_ov = float("nan")
        fails = []
        if not support_eligible(t_ov, c_ov):
            if t_ov < MIN_EP: fails.append("TREATED_FLOOR")
            if c_ov < MIN_EP: fails.append("CONTROL_FLOOR")
        if n_tk_ov < MIN_TK: fails.append("TICKER_FLOOR")
        if n_dt_ov < MIN_DT: fails.append("DATE_FLOOR")
        if not (mds_ov <= MAX_DATE_SHARE): fails.append("DATE_SHARE")
        ok = not fails
        if ok:
            keep_sig.setdefault((s & inov).tobytes(), []).append(c)
        e_ov = np.flatnonzero(s & inov)
        tk, tc = (np.unique(tcode[e_ov], return_counts=True)
                  if len(e_ov) else (np.array([]), np.array([])))
        hhi = float(((tc / tc.sum()) ** 2).sum()) if len(tc) else np.nan
        rows.append(dict(claim=c, treated_all=t_all, treated_overlap=t_ov,
                         control_overlap=c_ov,
                         treated_overlap_retention=round(ret, 4),
                         block_constant_treated_share=round(1.0 - ret, 4),
                         n_overlap_blocks=int(ov.sum()),
                         n_tickers_overlap=int(n_tk_ov), n_dates_overlap=int(n_dt_ov),
                         max_date_share_overlap=round(mds_ov, 5) if mds_ov == mds_ov else None,
                         fail_reasons=",".join(fails),
                         max_treated_ticker_share=round(float(tc.max() / tc.sum()), 5)
                         if len(tc) else np.nan,
                         treated_ticker_hhi=round(hhi, 6) if len(tc) else np.nan,
                         support_ok=bool(ok)))
    R = pd.DataFrame(rows)
    k_final = len(keep_sig)
    fun["support_qualified"] = int(R.support_ok.sum())
    fun["k_final"] = k_final
    R.to_parquet(CLAIMS_OUT, index=False)

    q = [0.0, .1, .25, .5, .75, .9, 1.0]
    mob = {f"q{int(x*100)}": round(float(R.treated_overlap_retention.quantile(x)), 4)
           for x in q}
    bcq = {f"q{int(x*100)}": round(float(R.block_constant_treated_share.quantile(x)), 4)
           for x in q}
    print(f"\nk_final {k_final} of {fun['grammar_claims']} grammar classes "
          f"(support-qualified {fun['support_qualified']})", flush=True)
    print("  retention " + " · ".join(f"{k}={v}" for k, v in mob.items()), flush=True)

    json.dump(dict(
        spec_id="T3_SEQUENCE_ESTIMAND_V1", family_id="T3_MICROSTRUCTURE_DNA_V1",
        funnel=fun, split_rule=E5.SPLIT_RULE,
        anchor="T3 - 2 sessions · dv20 median dollar volume · ATR14/close · reused "
               "byte-identical from the qualified T5 estimand on renamed frames",
        support=dict(treated=MIN_EP, control=MIN_EP, tickers=MIN_TK, dates=MIN_DT,
                     max_date_share=MAX_DATE_SHARE),
        retention_is_not_a_gate=dict(
            rule="treated_overlap_retention is DIAGNOSTIC ONLY in this family",
            t5_contrast="T5 registered >=0.80 in its own estimand freeze; this family's "
                        "registration did not and forbids post-hoc mobility cutoffs",
            corrected="Unregistered inherited retention gate identified during pre-first-use review and removed before the first estimand execution."),
        mobility=dict(role="DIAGNOSTIC of operational permutation freedom — NOT proof of "
                           "exchangeability; no post-hoc mobility cutoff is invented",
                      treated_permutable_retention=mob,
                      block_constant_treated_share=bcq),
        exchangeability_assumption=(
            "Within the registered date x pre-liquidity x pre-volatility blocks, episode "
            "outcomes are exchangeable with respect to sequence membership under the null. "
            "This does NOT claim ticker heterogeneity is controlled: one ticker contributes "
            "many episodes, and both sequence propensity and future MFE may be "
            "ticker-specific."),
        k_final=k_final, n_perm_planned=N_PERM, primary=PRIMARY,
        outcome_exposure="NOT_EXPOSED — only path_status_10d was read",
        seconds=round(time.time() - t0, 1)),
        open(SPEC, "w"), indent=2)
    print(f"  WROTE {SPEC} · {time.time()-t0:.0f}s · NO OUTCOME VALUE READ", flush=True)


if __name__ == "__main__":
    main()
