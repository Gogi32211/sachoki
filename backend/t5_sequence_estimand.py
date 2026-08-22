"""SEQUENCE_ESTIMAND_V1 — the comparator, the blocks, and k_final. Qualified X-only.

MFE is never read here. What is decided: which episodes are comparable to which, how many
claims survive that restriction, whether the proposed null has anything to permute, and
whether the support machinery is immune to the grain bug that already bit once.

WHY THE STRATA ANCHOR AT T5-2 AND NOT ON THE T5 DAY

Stratifying on the T5 day's own liquidity or ATR would be post-treatment adjustment: the
sequence being studied is part of that same session's microstructure, so those variables can
be a CONSEQUENCE of it. Conditioning on a consequence removes part of the thing being
measured. The sequence window opens on the previous day, so the anchor is placed before it:

    anchor          the close of the session immediately before PREV_DAY (T5-2)
    liquidity       20-session median dollar volume ending at the anchor
    volatility      ATR14 / close, as known at the anchor

FORBIDDEN as strata: T5-day dollar volume, T5-day ATR, PREV-day volume.

THE BLOCK IS THE EXACT DATE, NOT A YEAR BUCKET

    block = T5_DATE x pre_window_liquidity{LOW,HIGH} x pre_window_volatility{LOW,HIGH}

The exact trading date controls market regime far harder than any coarse bucket, and the two
halves are cut cross-sectionally WITHIN that date so the split cannot drift with the market.

OVERLAP IS REQUIRED, AND ITS RETENTION IS A GATE

A block enters a claim's estimand only when it holds at least one S=1 and one S=0 episode.
Comparing a sequence-positive block against controls from another date is composition drift
wearing a comparison. And if only a third of a sequence's episodes survive into comparable
blocks, theta no longer describes that sequence — so treated_overlap_retention >= 0.80.

EQUIVALENCE IS RECOMPUTED, NOT INHERITED

863 claims were distinct on the grammar population. The estimand population is smaller
(MFE availability, anchors, overlap), and two claims that differed there can be identical
here. k_final is counted after the restriction, never before it.

THE METAMORPHIC TEST EXISTS BECAUSE OF A REAL BUG

Support was once computed by selecting BARS where an episode matched, which inflated the
date-concentration numerator ~14x and left 1 survivor out of 16,546. It was caught only
because the result was absurd. So: replicate every 1H bar 2x, 5x and 10x, and every support
metric must come out bit-identical. A grain bug that produces a plausible number would
otherwise pass unnoticed.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import time

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import t5_dna as D                                                   # noqa: E402
import t5_sequence_grammar as G                                      # noqa: E402
import session_calendar as SC                                            # noqa: E402

SPEC = os.path.join(D.HERE, "T5_SEQUENCE_ESTIMAND_V1.json")
OUT_MEM = os.path.join(D.ROOT, "data", "t5_sequence_membership.parquet")

MIN_EP, MIN_TK, MIN_DT, MAX_DATE_SHARE = 300, 50, 50, 0.10
MIN_OVERLAP_RETENTION = 0.80
N_PERM = 999
PRIMARY = "mfe_10d"


class DuplicateGrainError(RuntimeError):
    """A source row appeared twice at the same (episode, day-role, session position)."""


def analyzable_episodes() -> set:
    """Episodes whose sessions are complete by the calendar rule — the grammar's own
    population. Computed BEFORE block construction: ranking LOW/HIGH over episodes that are
    then dropped would make the halves depend on rows the analysis never sees."""
    M = pd.read_parquet(D.OUT_MS, columns=["episode_id", "session_date", "bars_in_session"])
    # SESSION_FILTER_UNIFICATION_AMENDMENT_V1 — same helper as the grammar
    return set(SC.complete_sessions(M)["episode_id"])


def anchors(E: pd.DataFrame) -> pd.DataFrame:
    """Liquidity and volatility as known at the T5-2 close. Nothing from the window itself."""
    conn = duckdb.connect(D.DB1D, read_only=True)
    try:
        conn.register("ep", E[["episode_id", "ticker", "t5_date"]])
        q = """
        WITH b AS (
          SELECT * FROM (
            SELECT ticker, date, close, volume, atr_14,
                   row_number() OVER (PARTITION BY ticker,date ORDER BY
                     CASE universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1 ELSE 2 END) rn
            FROM bars WHERE universe IN ('sp500','nasdaq','russell2k')
          ) WHERE rn=1
        ), r AS (
          SELECT ticker, date, close, volume, atr_14,
                 median(close*volume) OVER (PARTITION BY ticker ORDER BY date
                     ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) dv20,
                 count(*) OVER (PARTITION BY ticker ORDER BY date
                     ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) n20
          FROM b
        ), a AS (
          SELECT e.episode_id, r.date anchor_date, r.dv20 liq_anchor,
                 r.atr_14/nullif(r.close,0) vol_anchor, r.n20,
                 row_number() OVER (PARTITION BY e.episode_id ORDER BY r.date DESC) k
          FROM ep e JOIN r ON r.ticker=e.ticker AND r.date < CAST(e.t5_date AS DATE)
          QUALIFY k = 2                                   -- T5-2, before the window opens
        )
        SELECT episode_id, CAST(anchor_date AS VARCHAR) anchor_date,
               liq_anchor, vol_anchor, n20 FROM a"""
        A = conn.execute(q).fetchdf()
    finally:
        conn.close()
    return A


def membership(claims_by_fam) -> pd.DataFrame:
    """Episode-level binary membership for every grammar survivor. Recomputed, then stored."""
    M = pd.read_parquet(D.OUT_MS, columns=[
        "episode_id", "relative_day", "session_position", "bars_in_session", "token_set",
        "session_date"])
    # SESSION_FILTER_UNIFICATION_AMENDMENT_V1 — same helper as the grammar
    M = SC.complete_sessions(M).reset_index(drop=True)
    if M.duplicated(["episode_id", "relative_day", "session_position"]).any():
        raise DuplicateGrainError(
            "duplicate (episode_id, relative_day, session_position) in the microstructure. "
            "A duplicated 1H bar must never enter the grammar silently: it would create "
            "adjacencies that do not exist and inflate occurrence counts.")
    toks = claims_by_fam["tokens"]
    tix = {t: i for i, t in enumerate(toks)}
    B = np.zeros((len(M), len(toks)), dtype=bool)
    for r, s in enumerate(M.token_set.to_numpy()):
        for t in s.split():
            j = tix.get(t)
            if j is not None:
                B[r, j] = True
    ep_codes, ep_uniq = pd.factorize(M.episode_id)
    rd = (M.relative_day == "T5_DAY").to_numpy()
    pos = M.session_position.to_numpy()
    order = np.lexsort((pos, rd, ep_codes))
    B, ep, rd, pos = B[order], ep_codes[order], rd[order], pos[order]
    n_ep = len(ep_uniq)
    out = {"episode_id": ep_uniq}
    for fam, L, seqs in claims_by_fam["claims"]:
        a = G_adjacency(ep, rd, pos, fam, L)
        cols = [B[a + j] for j in range(L)]
        for name, c in seqs:
            m = cols[0][:, c[0]]
            for j in range(1, L):
                m &= cols[j][:, c[j]]
            v = np.zeros(n_ep, dtype=bool)
            v[np.unique(ep[a[m]])] = True
            out[f"{fam}|{L}|{name}"] = v
    return pd.DataFrame(out)


def G_adjacency(ep, rd, pos, family, span):
    n = len(ep)
    idx = np.arange(n - span + 1)
    ok = np.ones(len(idx), bool)
    for j in range(span):
        ok &= ep[idx + j] == ep[idx]
    if family == "T5_INTRADAY":
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


# ── blocks: rank-based halves, deterministic in ties ─────────────────────────
SPLIT_RULE = ("within each T5 date, rank(anchor value, method='first') after sorting by "
              "(value, ticker); LOW = rank <= n/2. Ties resolve by ticker alphabetically so "
              "parquet row order can never be a hidden input.")


def blocks(P: pd.DataFrame) -> pd.DataFrame:
    P = P.sort_values(["t5_date", "liq_anchor", "ticker"]).copy()
    g = P.groupby("t5_date")
    P["liq_half"] = np.where(g["liq_anchor"].rank(method="first")
                             <= g["liq_anchor"].transform("size") / 2, "LOW", "HIGH")
    P = P.sort_values(["t5_date", "vol_anchor", "ticker"])
    g = P.groupby("t5_date")
    P["vol_half"] = np.where(g["vol_anchor"].rank(method="first")
                             <= g["vol_anchor"].transform("size") / 2, "LOW", "HIGH")
    P["block"] = P["t5_date"] + "|" + P["liq_half"] + "|" + P["vol_half"]
    return P


def main():
    t0 = time.time()
    E = pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", "t5_date",
                                           "n_t5_day_1h_bars"])
    O = pd.read_parquet(os.path.join(D.ROOT, "data", "t5_episode_outcomes.parquet"),
                        columns=["episode_id", "path_status_10d"])   # STATUS ONLY
    C = pd.read_parquet(os.path.join(D.ROOT, "data", "t5_sequence_survivors_v1.parquet"))
    toks = pd.read_parquet(os.path.join(D.ROOT, "data",
                                        "t5_sequence_tokens_v1.parquet")).token.tolist()
    fun = dict(grammar_claims=int(C.equivalence_class_id.nunique()),
               episodes_total=int(len(E)))

    P = E[E.n_t5_day_1h_bars.fillna(0) > 0].copy()
    fun["n_1h_observable"] = int(len(P))
    P = P.merge(O, on="episode_id", how="left")
    P = P[P.path_status_10d == "AVAILABLE"]
    fun["n_mfe10_available"] = int(len(P))
    A = anchors(P)
    P = P.merge(A, on="episode_id", how="inner")
    P = P[P.liq_anchor.notna() & P.vol_anchor.notna() & (P.n20 == 20)]
    fun["n_anchor_available"] = int(len(P))
    P = P[P.episode_id.isin(analyzable_episodes())]
    fun["n_sequence_analyzable"] = int(len(P))
    P = blocks(P)
    fun["n_primary_population"] = int(len(P))
    fun["n_blocks"] = int(P.block.nunique())
    fun["median_block_size"] = int(P.groupby("block").size().median())
    print("FUNNEL " + " · ".join(f"{k}={v:,}" for k, v in fun.items()), flush=True)

    MEM = membership(dict(tokens=toks, claims=[
        (fam, L, [(r.canonical_sequence, json.loads(r.token_idx))
                  for r in C[(C.family == fam) & (C.length == L)].itertuples()])
        for fam in ("T5_INTRADAY", "PREV_INTRADAY", "CROSS_DAY") for L in (2, 3)]))
    MEM = MEM.merge(P[["episode_id", "ticker", "t5_date", "block"]], on="episode_id",
                    how="inner")
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
        ov = (nt > 0) & (nc > 0)                      # overlap blocks
        inov = ov[bcode]
        t_all, t_ov = int(s.sum()), int((s & inov).sum())
        c_all, c_ov = int((~s).sum()), int(((~s) & inov).sum())
        ret = t_ov / t_all if t_all else 0.0
        # mobility: treated episodes sitting in a block that has an S=0 peer
        bc_share = 1.0 - ret
        ok = (t_ov >= MIN_EP and c_ov >= MIN_EP and ret >= MIN_OVERLAP_RETENTION)
        if ok:
            e = np.flatnonzero(s & inov)
            ok = (len(np.unique(tcode[e])) >= MIN_TK
                  and len(np.unique(dcode[e])) >= MIN_DT)
            if ok:
                _, dcnt = np.unique(dcode[e], return_counts=True)
                ok = dcnt.max() / len(e) <= MAX_DATE_SHARE
            if ok:
                keep_sig.setdefault((s & inov).tobytes(), []).append(c)
        e_ov = np.flatnonzero(s & inov)
        tk, tc = np.unique(tcode[e_ov], return_counts=True) if len(e_ov) else (np.array([]), np.array([]))
        hhi = float(((tc / tc.sum()) ** 2).sum()) if len(tc) else np.nan
        rows.append(dict(claim=c, treated_all=t_all, treated_overlap=t_ov,
                         max_treated_ticker_share=round(float(tc.max()/tc.sum()), 5) if len(tc) else np.nan,
                         treated_ticker_hhi=round(hhi, 6) if len(tc) else np.nan,
                         median_episodes_per_treated_ticker=float(np.median(tc)) if len(tc) else np.nan,
                         control_all=c_all, control_overlap=c_ov,
                         treated_overlap_retention=round(ret, 4),
                         block_constant_treated_share=round(bc_share, 4),
                         n_overlap_blocks=int(ov.sum()), support_ok=bool(ok)))
    R = pd.DataFrame(rows)
    k_final = len(keep_sig)
    fun["support_qualified"] = int(R.support_ok.sum())
    fun["k_final"] = k_final
    R.to_parquet(os.path.join(D.ROOT, "data", "t5_sequence_estimand_claims.parquet"),
                 index=False)

    q = [0.0, .1, .25, .5, .75, .9, .95, 1.0]
    mob = {f"q{int(x*100)}": round(float(R.treated_overlap_retention.quantile(x)), 4)
           for x in q}
    bcq = {f"q{int(x*100)}": round(float(R.block_constant_treated_share.quantile(x)), 4)
           for x in q}
    below = {f"below_{int(t*100)}pct": int((R.treated_overlap_retention < t).sum())
             for t in (.25, .5, .75, .9)}
    print(f"\nk_final {k_final} of {fun['grammar_claims']} grammar claims "
          f"(support-qualified {fun['support_qualified']})", flush=True)
    print(f"  treated_permutable_retention " + " · ".join(f"{k}={v}" for k, v in mob.items()),
          flush=True)
    print(f"  block_constant_treated_share  " + " · ".join(f"{k}={v}" for k, v in bcq.items()),
          flush=True)
    print(f"  claims below retention: " + " · ".join(f"{k}={v}" for k, v in below.items()),
          flush=True)

    # TEST B — episode-grain invariance. Replicating an OCCURRENCE must not change any
    # episode statistic; the previous bug turned occurrence rows into episode support.
    inv = {}
    for mult in (2, 5, 10):
        col = claim_cols[0]
        s0 = MEM[col].to_numpy()
        idx = np.repeat(np.arange(len(MEM)), mult)
        e2, b2 = np.arange(len(MEM))[idx], bcode[idx]
        nt = np.bincount(b2, weights=s0[idx], minlength=nb)
        nc = np.bincount(b2, minlength=nb) - nt
        ov2 = (nt > 0) & (nc > 0)
        t_ov2 = len(np.unique(e2[(s0[idx]) & ov2[b2]]))
        inv[f"x{mult}"] = int(t_ov2)
    base = int((MEM[claim_cols[0]].to_numpy() & ((np.bincount(bcode, weights=MEM[claim_cols[0]].to_numpy(), minlength=nb) > 0) & (np.bincount(bcode, minlength=nb) - np.bincount(bcode, weights=MEM[claim_cols[0]].to_numpy(), minlength=nb) > 0))[bcode]).sum())
    testB = all(v == base for v in inv.values())
    print(f"  METAMORPHIC B (episode-grain invariance): base={base:,} " +
          " ".join(f"x{k[1:]}={v:,}" for k, v in inv.items()) +
          f" -> {'PASS' if testB else 'FAIL'}", flush=True)
    if not testB:
        raise RuntimeError("episode statistics changed under occurrence replication")

    json.dump(dict(funnel=fun, split_rule=SPLIT_RULE,
                   metamorphic_B=dict(base=base, replicated=inv, passed=testB),
                   exchangeability_assumption=(
                       "Within the registered date x pre-liquidity x pre-volatility blocks, "
                       "episode outcomes are exchangeable with respect to sequence membership "
                       "under the null. This does NOT claim that ticker heterogeneity is "
                       "controlled: one ticker contributes many episodes, and both sequence "
                       "propensity and future MFE may be ticker-specific."),
                   permutable_retention_quantiles=mob,
                   block_constant_quantiles=bcq, claims_below=below,
                   k_final=k_final, n_perm_planned=N_PERM, primary=PRIMARY,
                   outcome_exposure="NOT_EXPOSED — only path_status_10d was read",
                   seconds=round(time.time() - t0, 1)),
              open(SPEC, "w"), indent=2)
    print(f"\n  WROTE {SPEC} · {time.time()-t0:.0f}s · NO OUTCOME VALUE READ", flush=True)


if __name__ == "__main__":
    main()
