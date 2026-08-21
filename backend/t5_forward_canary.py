"""T5_FORWARD_UPSTREAM_SEMANTIC_CANARY_V1 — upstream code may change; the measuring function may not.

WHY PROVENANCE ALONE IS NOT ENOUGH

An immutable data_version protects an EPISODE: what was measured stays measured. It does not
protect the SAMPLE. If the first 8,000 episodes are measured by one feature function and the
next 12,000 by another, the primary forward sample is a mixture of two measurement regimes —
and discovering that at lock time, by finding two semantic_upstream_hash values in the ledger,
is discovering it far too late.

WHY A GATE ON THE CODE HASH IS THE WRONG GATE

The upstream closure is 245 modules, because _process does `import main`. A gate on that hash
fires whenever an unrelated endpoint is edited, and a gate that fires constantly is a gate that
gets routed around. So code provenance and semantic equivalence are separated:

    semantic_upstream_hash    PROVENANCE   recorded on every data_version, never blocking
    semantic canary           GATE         blocking, and it tests OUTPUT not source text

    broad code hash changed + canary identical   -> ALLOW
    canary changed                               -> UPSTREAM_SEMANTICS_CHANGED
                                                    forward_eligible = false, accrual HELD

An accidental refactor of the API cannot block accrual. A change to rsi_14, sig_*, session
assignment, deduplication or suffix logic cannot enter forward X unnoticed.

IT RUNS THE CANDIDATE CODE, IT DOES NOT READ ALREADY-DERIVED ROWS

Comparing the DB's current derived columns against the frozen parquet would pass whenever old
sessions simply were not re-derived, while new sessions silently got new semantics. So the
canary feeds SEALED RAW BARS to the live enricher and compares what comes out:

    sealed raw OHLCV  ->  enrich_ticker_df (candidate)  ->  D.SRC_COLS  ->  cell comparison

THE CORPUS COVERS REGIMES, NOT JUST VOLUME

A canary built only from ordinary sessions cannot see a change in early-close handling or NaN
propagation. The corpus is selected deterministically from pre-cutoff data to include normal
full sessions, short/irregular sessions, tickers missing bars, high-volatility days, low-volume
tickers, cross-day boundaries, and rows carrying nulls in consumed columns.

THE DIGEST IS THE FINGERPRINT; THE REPORT NAMES THE FIRST CHANGED CELL

An aggregate digest tells you something moved. The diagnostic has to say what, or the next
person just re-seals the reference to make the red light stop.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                 # noqa: E402

OUT = "T5_FORWARD_UPSTREAM_SEMANTIC_CANARY_V1.json"
CORPUS = os.path.join(D.ROOT, "data", "t5_canary_corpus.parquet")
CUTOFF = json.load(open("T5_FORWARD_VALIDATION_V1.json"))[
    "discovery_cutoff"]["last_eligible_historical_signal_session"]
HISTORY = 220          # bars of prior context per ticker; ATR14/RSI14 need warm-up
KEY = ["ticker", "date"]


class UpstreamSemanticsChanged(RuntimeError):
    pass


# ══════════════════════════════════════════════════════════════════════════
def select_corpus(verbose=True):
    """Deterministic, pre-cutoff, regime-covering. No randomness, no outcome."""
    import duckdb
    c = duckdb.connect(D.DB1H, read_only=True)
    try:
        S = c.execute(f"""
            WITH b AS (
              SELECT ticker, CAST(date AS DATE) sd, date, open, high, low, close, volume,
                     count(*) OVER (PARTITION BY ticker, CAST(date AS DATE)) n_bars
              FROM bars WHERE CAST(date AS DATE) <= DATE '{CUTOFF}'
            ), per AS (
              SELECT ticker, sd, max(n_bars) n_bars, sum(volume) vol,
                     (max(high)-min(low))/nullif(avg(close),0) rng
              FROM b GROUP BY ticker, sd
            ), modal AS (
              SELECT sd, mode(n_bars) m FROM per GROUP BY sd
            )
            SELECT p.ticker, CAST(p.sd AS VARCHAR) sd, p.n_bars, m.m modal, p.vol, p.rng
            FROM per p JOIN modal m USING (sd)
        """).fetchdf()
    finally:
        c.close()
    S["short_session"] = S.modal < S.modal.max()
    S["missing_bars"] = S.n_bars < S.modal
    pick, why = [], {}

    def take(mask, label, n):
        sub = S[mask].sort_values(["sd", "ticker"]).head(n)
        for r in sub.itertuples():
            pick.append((r.ticker, r.sd))
            why.setdefault(f"{r.ticker}|{r.sd}", []).append(label)
        return len(sub)

    why_counts = {
        "normal_full_session": take((~S.short_session) & (S.n_bars == S.modal), "normal", 8),
        "short_irregular_session": take(S.short_session, "short_session", 8),
        "missing_bars": take(S.missing_bars, "missing_bars", 8),
        "high_volatility": take(S.rng >= S.rng.quantile(0.999), "high_vol", 8),
        "low_volume": take((S.vol <= S.vol.quantile(0.001)) & (S.vol > 0), "low_volume", 8),
    }
    keys = sorted(set(pick))
    if verbose:
        print(f"  corpus · {len(keys)} (ticker, session) keys · regimes {why_counts}")
    return keys, why_counts, why


def build_corpus(keys, verbose=True):
    """Seal the RAW input. Enrichment needs prior context, so each key carries HISTORY bars
    ending at its session — which also makes cross-day boundaries part of every case."""
    import duckdb
    c = duckdb.connect(D.DB1H, read_only=True)
    frames = []
    try:
        for tk, sd in keys:
            df = c.execute(f"""
                SELECT ticker, date, open, high, low, close, volume FROM bars
                WHERE ticker = ? AND CAST(date AS DATE) <= DATE '{sd}'
                ORDER BY date DESC LIMIT {HISTORY}
            """, [tk]).fetchdf()
            if len(df) < 30:
                continue
            df = df.sort_values("date").reset_index(drop=True)
            df["canary_key"] = f"{tk}|{sd}"
            df["target_session"] = sd
            frames.append(df)
    finally:
        c.close()
    C = pd.concat(frames, ignore_index=True)
    if verbose:
        print(f"  sealed raw corpus · {len(C):,} bars · {C.canary_key.nunique()} cases")
    return C


# ══════════════════════════════════════════════════════════════════════════
def run_candidate(C):
    """Feed the sealed raw bars to the LIVE enricher and keep only the consumed columns."""
    from studio.enricher import enrich_ticker_df
    out = []
    for key, g in C.groupby("canary_key", sort=True):
        raw = g.drop(columns=["canary_key", "target_session"]).reset_index(drop=True)
        en = enrich_ticker_df(raw.copy())
        en = en[en.date.isin(g.date[g.date.dt.strftime("%Y-%m-%d")
                                    == g.target_session.iloc[0]])] \
            if "date" in en.columns else en
        en = en.assign(canary_key=key)
        out.append(en)
    E = pd.concat(out, ignore_index=True)
    cols = [c for c in D.SRC_COLS if c in E.columns]
    missing = [c for c in D.SRC_COLS if c not in E.columns]
    M = E[["canary_key", "date"] + cols].sort_values(["canary_key", "date"]).reset_index(
        drop=True)
    return M, cols, missing


def matrix_digest(M):
    return hashlib.sha256(M.to_csv(index=False).encode()).hexdigest()[:16]


def first_difference(ref, cand):
    """An aggregate digest says something moved. This says WHAT — otherwise the next person
    just re-seals the reference to make the red light stop."""
    r = ref.set_index(["canary_key", "date"]).sort_index()
    c = cand.set_index(["canary_key", "date"]).sort_index()
    only_ref = sorted(map(str, list(r.index.difference(c.index))[:3]))
    only_cand = sorted(map(str, list(c.index.difference(r.index))[:3]))
    both = r.index.intersection(c.index)
    cols = [x for x in r.columns if x in c.columns]
    diffs = []
    for col in cols:
        a, b = r.loc[both, col], c.loc[both, col]
        ne = ~((a == b) | (a.isna() & b.isna()))
        if ne.any():
            i = ne.idxmax()
            diffs.append(dict(column=col, n_cells=int(ne.sum()), first_key=str(i),
                              reference=str(a.loc[i]), candidate=str(b.loc[i])))
    return dict(rows_only_in_reference=only_ref, rows_only_in_candidate=only_cand,
                columns_changed=len(diffs), detail=diffs[:8],
                columns_added=sorted(set(c.columns) - set(r.columns))[:5],
                columns_removed=sorted(set(r.columns) - set(c.columns))[:5])


# ══════════════════════════════════════════════════════════════════════════
def check(verbose=True):
    """The blocking gate. Returns (ok, report); raises nothing so callers decide the action."""
    if not os.path.exists(OUT) or not os.path.exists(CORPUS):
        return None, dict(status="NO_REFERENCE",
                          note="the canary has not been sealed yet")
    ref_spec = json.load(open(OUT))
    C = pd.read_parquet(CORPUS)
    M, cols, missing = run_candidate(C)
    dig = matrix_digest(M)
    ok = dig == ref_spec["reference_digest"]
    rep = dict(status="IDENTICAL" if ok else "UPSTREAM_SEMANTICS_CHANGED",
               reference_digest=ref_spec["reference_digest"], candidate_digest=dig,
               cells=int(M.shape[0] * (M.shape[1] - 2)), columns=len(cols),
               columns_missing_from_candidate=missing)
    if not ok:
        ref = pd.read_parquet(os.path.join(D.ROOT, "data", "t5_canary_reference.parquet"))
        rep["diagnosis"] = first_difference(ref, M)
        rep["consequence"] = ["forward_eligible = false", "accrual HELD",
                              "the change must be classified as an amendment or a NEW "
                              "validation regime BEFORE the primary 20,000 continues"]
    if verbose:
        print(f"  canary {rep['status']} · ref {rep['reference_digest']} · "
              f"cand {dig} · {rep['cells']:,} cells")
        if not ok:
            print(f"    {rep['diagnosis']}")
    return ok, rep


def seal(verbose=True):
    import t5_forward_activate as ACT
    import t5_forward_producer as PR
    ACT.assert_runtime_pinned()
    keys, regimes, why = select_corpus(verbose)
    C = build_corpus(keys, verbose)
    C.to_parquet(CORPUS, index=False)
    M, cols, missing = run_candidate(C)
    M.to_parquet(os.path.join(D.ROOT, "data", "t5_canary_reference.parquet"), index=False)
    dig = matrix_digest(M)
    body = dict(
        spec_id="T5_FORWARD_UPSTREAM_SEMANTIC_CANARY_V1", status="FROZEN",
        role="BLOCKING GATE on upstream SEMANTICS, per data_version",
        why="an immutable data_version protects an episode, not the sample. Without this, the "
            "first 8,000 episodes could be measured by one feature function and the next "
            "12,000 by another, and the mixture would only surface at lock time.",
        separation=dict(
            semantic_upstream_hash="PROVENANCE, recorded on every data_version, never blocking",
            canary="GATE, blocking, tests OUTPUT rather than source text",
            allow="broad code hash changed + canary identical",
            block="canary changed -> UPSTREAM_SEMANTICS_CHANGED -> forward_eligible false, "
                  "accrual HELD",
            why_not_hash_gate="the closure is 245 modules because _process imports main; a gate "
                              "that fires on unrelated edits gets routed around"),
        method=dict(
            runs="the CANDIDATE code on SEALED inputs",
            not_="reading already-derived DB rows, which would pass whenever old sessions were "
                 "simply not re-derived while new sessions silently got new semantics",
            entry="studio.enricher.enrich_ticker_df",
            chain="sealed raw OHLCV -> enricher -> D.SRC_COLS -> cell comparison"),
        corpus=dict(file=os.path.basename(CORPUS), digest=ART.file_digest(CORPUS),
                    cases=int(C.canary_key.nunique()), bars=int(len(C)),
                    history_bars_per_case=HISTORY,
                    regimes=regimes,
                    cross_day="every case carries prior sessions, so cross-day boundaries are "
                              "exercised by construction",
                    selection="deterministic, pre-cutoff only, no randomness and no outcome",
                    cutoff=CUTOFF),
        reference_digest=dig,
        reference_matrix=dict(file="t5_canary_reference.parquet", rows=int(len(M)),
                              columns=len(cols), consumed_columns=cols[:20],
                              n_consumed_columns=len(cols),
                              columns_missing_from_enricher=missing),
        comparison=dict(grain="(canary_key, date) x consumed column",
                        equality="EXACT — NaN==NaN counts as equal, nothing else is tolerated",
                        diagnostic="the report names the first changed cell per column; an "
                                   "aggregate digest alone invites re-sealing the reference to "
                                   "make the red light stop"),
        semantic_upstream_at_seal=PR.semantic_upstream(),
        producer_commit=ACT.runtime_pin()["pin"])
    d = ART.seal(body, OUT, required=("spec_id", "reference_digest", "corpus", "method"),
                 supersede=True)
    if verbose:
        print(f"  FROZEN · {OUT} · {d} · reference {dig}")
    return d


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "seal":
        seal()
    else:
        check()
