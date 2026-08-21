"""T5_FORWARD_SEMANTIC_GATE_V1 — the two-tier drift gate, decision table frozen before use.

WHAT THE CANARY ACTUALLY PROVES, SAID PRECISELY

'Upstream semantic invariance' is too strong a name for a 40-case corpus. The canary proves

    no semantic drift detected on the frozen canary corpus

and nothing more: code can change so that those 40 cases stay identical while a combination of
X first seen after 2026-08-21 behaves differently. So the gate has two tiers, and the decision
table is frozen HERE, before any change has happened that anyone might want to wave through:

    FAST — every data_version
        frozen canary, exact cell equality, plus schema contract
        (column names, order, dtypes — float64 quietly becoming float32, a timezone dropped,
         NULL becoming NaN, category becoming string: all same-looking values, different
         downstream semantics)

    SLOW — only when semantic_upstream_hash changes
        candidate chain over the FULL sealed pre-cutoff replay corpus,
        guarded columns only, exact equality against the sealed reference

    upstream hash unchanged + fast PASS                          -> ALLOW
    upstream hash changed   + fast FAIL                          -> HOLD
    upstream hash changed   + fast PASS + slow exact             -> ALLOW as
                                                                    SEMANTICALLY_REPLAY_EQUIVALENT
    upstream hash changed   + slow differs                       -> HOLD — the primary 20,000
                                                                    regime cannot silently continue

WHY THE SLOW REFERENCE IS CHAIN-GENERATED AND NOT THE FROZEN MICROSTRUCTURE

This was measured, not assumed. A probe replayed 6 frozen (ticker, session) pairs through the
pinned chain: 58 of 1,554 cells differed while the RAW OHLCV was byte-identical — and for ZM
2026-08-17 the store's OWN rsi_14 today differs from the value the frozen build captured. The
incremental delta re-derives a rolling window, and Wilder-type recursions differ slightly with
the window start, so store-derived columns are WINDOW-VINTAGE-DEPENDENT for recently-touched
sessions. The frozen X is one vintage of that process and is not exactly reproducible even by
identical code. Chain-vs-chain on fixed sealed inputs IS deterministic, so the reference is
what the PINNED chain produces on the sealed corpus, sealed once, and every candidate is
measured against that.
"""
from __future__ import annotations
import hashlib, json, os, sys                                          # noqa: E402
import numpy as np, pandas as pd                                       # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D                                 # noqa: E402
import t5_forward_canary as CN, t5_forward_producer as PR              # noqa: E402

OUT = "T5_FORWARD_SEMANTIC_GATE_V1.json"
SLOW_CORPUS = os.path.join(D.ROOT, "data", "t5_slow_corpus.parquet")
SLOW_REF = os.path.join(D.ROOT, "data", "t5_slow_reference.parquet")
N_SLOW_PAIRS = 240
HISTORY = 260


class SemanticDrift(RuntimeError):
    pass


def guarded_columns():
    return json.load(open("T5_FORWARD_RULE_FEATURE_DEPENDENCY_V1.json"))["guarded_feature_set"]


def schema_fingerprint(M):
    """Names, ORDER, dtypes, index dtype. Values can look identical across a float64->float32
    or tz-drop change; this cannot."""
    parts = [f"{c}:{M[c].dtype}" for c in M.columns]
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


# ══════════════════════════════════════════════════════════════════════════
# SLOW CORPUS — deterministic spread over the WHOLE frozen evidence
# ══════════════════════════════════════════════════════════════════════════
def build_slow_corpus(verbose=True):
    MS = pd.read_parquet(D.OUT_MS, columns=["ticker", "session_date"])
    pairs = MS.drop_duplicates().sort_values(["session_date", "ticker"]).reset_index(drop=True)
    idx = np.linspace(0, len(pairs) - 1, N_SLOW_PAIRS).astype(int)
    sel = [tuple(x) for x in pairs.iloc[sorted(set(idx))].to_numpy()]
    import duckdb
    c = duckdb.connect(D.DB1H, read_only=True)
    frames = []
    try:
        for tk, sd in sel:
            df = c.execute(f"""
                SELECT ticker, any_value(universe) AS universe, date,
                       any_value(open) AS "open", any_value(high) AS "high",
                       any_value(low) AS "low", any_value(close) AS "close",
                       any_value(volume) AS "volume"
                FROM bars WHERE ticker = ? AND CAST(date AS DATE) <= DATE '{sd}'
                GROUP BY ticker, date ORDER BY date DESC LIMIT {HISTORY}
            """, [tk]).fetchdf()
            if len(df) < 30:
                continue
            df = df.sort_values("date").reset_index(drop=True)
            df["slow_key"] = f"{tk}|{sd}"
            df["target_session"] = sd
            frames.append(df)
    finally:
        c.close()
    C = pd.concat(frames, ignore_index=True)
    if verbose:
        print(f"  slow corpus · {C.slow_key.nunique()} pairs spread over "
              f"{pairs.session_date.min()}..{pairs.session_date.max()} · {len(C):,} raw bars")
    return C


def run_chain(C, verbose=True):
    """Same invocation contract as the canary: fixed raw input -> _process -> guarded columns."""
    os.environ.setdefault("SACHOKI_BARS_ONLY", "1")
    import build_intraday_db as B
    if B._DB_COLS is None:
        import duckdb
        c = duckdb.connect(D.DB1H, read_only=True)
        try:
            B._DB_COLS = {r[0] for r in c.execute("DESCRIBE bars").fetchall()}
        finally:
            c.close()
    g_cols = guarded_columns()
    out, done = [], 0
    for key, g in C.groupby("slow_key", sort=True):
        raw = g[["date", "open", "high", "low", "close", "volume"]].copy()
        raw = raw.set_index(pd.to_datetime(raw.pop("date"), utc=True))
        try:
            en = B._process(g.ticker.iloc[0], g.universe.iloc[0], raw, "1h")
        except Exception:
            continue
        if en is None or not len(en):
            continue
        ts = pd.to_datetime(en.date).dt.tz_localize("UTC").dt.tz_convert("America/New_York")
        en = en.assign(session_date=ts.dt.strftime("%Y-%m-%d"),
                       et_time=ts.dt.strftime("%H:%M"))
        en = en[en.session_date == str(g.target_session.iloc[0])]
        if len(en):
            keep = ["et_time"] + [c for c in g_cols if c in en.columns]
            out.append(en[keep].assign(slow_key=key))
        done += 1
        if verbose and done % 60 == 0:
            print(f"    {done} pairs …", flush=True)
    M = pd.concat(out, ignore_index=True).sort_values(["slow_key", "et_time"]).reset_index(
        drop=True)
    return M


def seal_slow_reference(verbose=True):
    import t5_forward_activate as ACT
    ACT.assert_runtime_pinned()
    C = build_slow_corpus(verbose)
    C.to_parquet(SLOW_CORPUS, index=False)
    M = run_chain(C, verbose)
    M.to_parquet(SLOW_REF, index=False)
    return dict(corpus_digest=ART.file_digest(SLOW_CORPUS),
                reference_digest=ART.file_digest(SLOW_REF),
                schema_fingerprint=schema_fingerprint(M),
                pairs=int(M.slow_key.nunique()), rows=int(len(M)),
                cells=int(len(M) * (M.shape[1] - 2)))


def slow_gate(verbose=True):
    """Only invoked when the broad upstream hash has moved. Exact, or HOLD."""
    ref = pd.read_parquet(SLOW_REF)
    C = pd.read_parquet(SLOW_CORPUS)
    M = run_chain(C, verbose=False)
    sf_ref, sf_new = schema_fingerprint(ref), schema_fingerprint(M)
    r = ref.set_index(["slow_key", "et_time"]).sort_index()
    m = M.set_index(["slow_key", "et_time"]).sort_index()
    same_rows = r.index.equals(m.index)
    mism = 0
    detail = []
    if same_rows:
        for col in r.columns:
            if col not in m.columns:
                detail.append(dict(column=col, issue="missing"))
                continue
            a, b = r[col], m[col]
            ne = ~((a.astype(str) == b.astype(str)) | (a.isna() & b.isna()))
            if ne.any():
                i = ne.idxmax()
                mism += int(ne.sum())
                detail.append(dict(column=col, n_cells=int(ne.sum()), first=str(i),
                                   reference=str(a.loc[i]), candidate=str(b.loc[i])))
    ok = same_rows and mism == 0 and sf_ref == sf_new
    return ok, dict(status="SEMANTICALLY_REPLAY_EQUIVALENT" if ok else "SLOW_GATE_FAILED",
                    row_index_identical=bool(same_rows), cell_mismatches=mism,
                    schema_reference=sf_ref, schema_candidate=sf_new,
                    schema_identical=sf_ref == sf_new, detail=detail[:8])


# ══════════════════════════════════════════════════════════════════════════
def evaluate(verbose=True):
    """The frozen decision table, executed. Returns (allow, verdict_dict)."""
    spec = json.load(open(OUT))
    sealed_hash = spec["sealed_upstream_hash"]
    cur = PR.semantic_upstream()["hash"]
    fast_ok, fast = CN.check(verbose=False)
    if fast_ok is None:
        return False, dict(verdict="HOLD", why="no sealed canary reference")
    if cur == sealed_hash:
        v = dict(verdict="ALLOW" if fast_ok else "HOLD",
                 tier="FAST", upstream_hash="unchanged", fast=fast["status"])
        return bool(fast_ok), v
    if not fast_ok:
        return False, dict(verdict="HOLD", tier="FAST", upstream_hash="CHANGED",
                           fast=fast["status"],
                           why="upstream changed and the canary already differs")
    if verbose:
        print(f"  upstream hash changed {sealed_hash} -> {cur} · canary PASS · "
              f"running the SLOW gate …", flush=True)
    slow_ok, slow = slow_gate(verbose)
    if slow_ok:
        return True, dict(verdict="ALLOW", tier="SLOW", upstream_hash="CHANGED",
                          status="SEMANTICALLY_REPLAY_EQUIVALENT", slow=slow)
    return False, dict(verdict="HOLD", tier="SLOW", upstream_hash="CHANGED", slow=slow,
                       why="the primary 20,000 regime cannot silently continue under changed "
                           "semantics; classify the change as an amendment or a new validation "
                           "regime first")


def seal(slow_meta, verbose=True):
    import t5_forward_activate as ACT
    pin = ACT.assert_runtime_pinned()
    body = dict(
        spec_id="T5_FORWARD_SEMANTIC_GATE_V1", status="FROZEN",
        precision_of_claim="the canary proves 'no semantic drift detected on the frozen canary "
                           "corpus' — NOT global semantic invariance. The slow tier exists "
                           "because code can keep 40 cases identical while a post-cutoff "
                           "combination of X behaves differently.",
        decision_table={
            "unchanged + fast PASS": "ALLOW",
            "changed + fast FAIL": "HOLD",
            "changed + fast PASS + slow exact": "ALLOW as SEMANTICALLY_REPLAY_EQUIVALENT",
            "changed + slow differs": "HOLD — the primary 20,000 regime cannot silently "
                                      "continue"},
        frozen_before="any upstream change existed that anyone might want to wave through",
        fast_tier=dict(runs="every data_version", what="frozen canary, exact cell equality",
                       schema_contract="column names, order, dtypes fingerprinted; "
                                       "float64->float32, tz-drop, NULL->NaN, "
                                       "category->string all change the fingerprint while "
                                       "leaving values looking identical",
                       canary_digest=ART.file_digest(CN.OUT)),
        slow_tier=dict(runs="only when semantic_upstream_hash changes",
                       corpus=dict(file=os.path.basename(SLOW_CORPUS),
                                   pairs=slow_meta["pairs"], rows=slow_meta["rows"],
                                   cells=slow_meta["cells"],
                                   selection=f"{N_SLOW_PAIRS} (ticker, session) pairs spread "
                                             f"deterministically over the whole frozen "
                                             f"evidence by linspace over the sorted pair "
                                             f"list — no randomness, no outcome",
                                   digest=slow_meta["corpus_digest"]),
                       reference=dict(file=os.path.basename(SLOW_REF),
                                      digest=slow_meta["reference_digest"],
                                      schema_fingerprint=slow_meta["schema_fingerprint"]),
                       columns="guarded feature set only, from "
                               "T5_FORWARD_RULE_FEATURE_DEPENDENCY_V1"),
        why_reference_is_chain_generated=dict(
            measured="a probe replayed 6 frozen (ticker, session) pairs through the pinned "
                     "chain: 58/1,554 cells differed while RAW OHLCV was byte-identical, and "
                     "for ZM 2026-08-17 the store's own rsi_14 today differs from what the "
                     "frozen build captured",
            mechanism="the incremental delta re-derives a rolling window; Wilder-type "
                      "recursions differ slightly with the window start, so store-derived "
                      "columns are WINDOW-VINTAGE-DEPENDENT for recently-touched sessions",
            consequence="the frozen X is one vintage of that process and is not exactly "
                        "reproducible even by identical code; chain-vs-chain on fixed sealed "
                        "inputs IS deterministic, so that is the reference"),
        sealed_upstream_hash=PR.semantic_upstream()["hash"],
        dependency_digest=ART.file_digest("T5_FORWARD_RULE_FEATURE_DEPENDENCY_V1.json"),
        producer_commit=pin["pin"])
    d = ART.seal(body, OUT, required=("spec_id", "decision_table", "sealed_upstream_hash",
                                      "slow_tier"), supersede=True)
    if verbose:
        print(f"  FROZEN · {OUT} · {d}")
    return d


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "seal":
        meta = seal_slow_reference()
        print(f"  slow reference · {meta}")
        seal(meta)
    else:
        allow, v = evaluate()
        print(json.dumps(v, indent=2, default=str))
