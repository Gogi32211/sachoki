"""T9_MICROSTRUCTURE_DNA_V1 — X-only foundation. A NEW research family, independent of T5.

    family_id            T9_MICROSTRUCTURE_DNA_V1
    setup                FINAL_PRIORITY_RESOLVED_T9   (bullCode == 7, t_sig == 'T9')
    phase                X ONLY — no outcome, no MFE, no Z, no ranking
    T5 relationship      infrastructure and statistical METHODOLOGY are reused;
                         no T5 winning token, family, Z, theta, cluster or outcome
                         result selects any T9 hypothesis

WHAT CANONICAL T9 IS (source of truth: the current Pine implementation)

    prev1IsBear = close[1] < open[1] OR doji[1]          (doji counts as bear)
    isBull      = close > open                            (strict)
    isInside    = BODY-inside: max(o,c) <= max(o1,c1) AND min(o,c) >= min(o1,c1)
                  wicks do NOT participate
    T9_raw      = prev1IsBear AND isBull AND isInside
    T9          = bullCode == 7 — raw T9 with T4, T6, T1G, T2G, T1, T2 excluded first

VERIFIED AGAINST THE MATERIALIZED COLUMN, NOT ASSUMED

    canonical t_sig='T9'                  243,562
    raw OHLC reconstruction               248,966
    canonical AND raw                     240,579   (98.78% of canonical)
    canonical NOT raw                       2,983   residual — same feed/tick-boundary class
                                                    as T5's 1,460 (T5 agreed 99.25%)
    raw resolving to HIGHER priority      T4 4,694 · T1 959   (priority working as declared)
    raw carrying t_sig='T3' (lower)         2,733   residual of the same class

The materialized priority-resolved t_sig='T9' is the research population. Raw T9 is NOT used
as a substitute; the reconstruction exists to prove the definition, and its residuals are
recorded rather than smoothed over.

REUSE, NOT REIMPLEMENTATION

The token registry, microstructure builder, feature extraction and audit pattern are IMPORTED
from the qualified T5 infrastructure (t5_dna). Internal column names keep t5_dna's vocabulary
during the build and are renamed at write time (t5_date -> t9_date, T5_DAY -> T9_DAY), so the
shared functions run byte-identical code. The episode SQL is t5_dna's statement with exactly
one literal changed ('T5' -> 'T9'); a gate asserts that is the only difference.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                    # noqa: E402
import duckdb, numpy as np, pandas as pd                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import combo_tokens as CT, combo_tokens_spec as TS                     # noqa: E402
import t5_dna as D5                                                    # noqa: E402

ROOT = D5.ROOT
DB1D, DB1H = D5.DB1D, D5.DB1H
OUT_EP = os.path.join(ROOT, "data", "t9_episodes.parquet")
OUT_MS = os.path.join(ROOT, "data", "t9_1h_microstructure.parquet")
AUDIT = os.path.join(HERE, "T9_DNA_AUDIT.json")

SETUP = "T9"
T9_DEFINITION = (
    "t_sig=='T9' as materialized in bars (priority-resolved import of the Pine 'T' column; "
    "bullCode==7). Raw semantics: prev1IsBear = close[1]<open[1] OR doji[1]; isBull = "
    "close>open strict; isInside = BODY-inside non-strict (max(o,c)<=max(o1,c1) AND "
    "min(o,c)>=min(o1,c1)); wicks excluded. Canonical = raw with T4,T6,T1G,T2G,T1,T2 "
    "resolved first. Reconstruction agreement 240,579/243,562 = 98.78%; 2,983 canonical "
    "residual + 2,733 raw-carrying-T3 residual, same feed/tick-boundary class as T5's 1,460. "
    "Universe rows deduplicated to one per (ticker,date) with priority sp500<nasdaq<russell2k."
)
T9_DEF_HASH = hashlib.sha256(T9_DEFINITION.encode()).hexdigest()[:16]

TOKENS_1H = D5.TOKENS_1H            # SAME canonical registry — no T9-specific tokens
SRC_COLS = D5.SRC_COLS
FAMILY_COLS = D5.FAMILY_COLS

# t5_dna's statement with exactly ONE literal changed. Asserted, not promised.
_EPISODE_SQL = D5._EPISODE_SQL.replace("l.t_sig = 'T5'", "l.t_sig = 'T9'")
assert _EPISODE_SQL != D5._EPISODE_SQL
assert _EPISODE_SQL.replace("l.t_sig = 'T9'", "l.t_sig = 'T5'") == D5._EPISODE_SQL, \
    "the T9 episode SQL must differ from t5_dna's by the t_sig literal ONLY"


def episode_id(ticker: str, d: str) -> str:
    return hashlib.sha256(f"{ticker}|{d}|{SETUP}|{T9_DEF_HASH}".encode()).hexdigest()[:16]


def build_episodes(conn, verbose=True, sql=None) -> pd.DataFrame:
    E = conn.execute(sql or _EPISODE_SQL).fetchdf()
    E.insert(0, "episode_id", [episode_id(t, d) for t, d in
                               zip(E["ticker"], E["t5_date"])])
    E["setup_1d"] = SETUP
    if E["episode_id"].duplicated().any():
        raise RuntimeError("episode_id is not unique — the grain is broken")
    if verbose:
        print(f"  episodes {len(E):,} · tickers {E.ticker.nunique():,} · "
              f"{E.t5_date.min()} .. {E.t5_date.max()}", flush=True)
    return E


def _rename_out(df: pd.DataFrame) -> pd.DataFrame:
    """Shared T5 builders speak t5_date/T5_DAY internally; the written tables speak T9."""
    ren = {c: c.replace("t5_", "t9_") for c in df.columns if c.startswith("t5_")}
    df = df.rename(columns=ren)
    if "relative_day" in df.columns:
        df["relative_day"] = df["relative_day"].str.replace("T5_DAY", "T9_DAY", regex=False)
    if "relative_position" in df.columns:
        df["relative_position"] = df["relative_position"].str.replace("T5_", "T9_", regex=False)
    return df


def main():
    t0 = time.time()
    print(f"T9_MICROSTRUCTURE_DNA_V1 · definition {T9_DEF_HASH} · registry {TS.digest()}",
          flush=True)
    conn = duckdb.connect(DB1D, read_only=True)
    conn.execute(f"ATTACH '{DB1H}' AS h (READ_ONLY)")
    try:
        E = build_episodes(conn)
        M, T = D5.build_microstructure(conn, E)      # SAME builder, byte-identical code
    finally:
        conn.close()

    F = D5.episode_features(M)
    B = D5.boundary_features(M, E)
    E = E.merge(F, on="episode_id", how="left").merge(B, on="episode_id", how="left")
    E["complete_prev_session"] = E["n_prev_day_1h_bars"].fillna(0) > 0
    E["complete_t5_session"] = E["n_t5_day_1h_bars"].fillna(0) > 0
    E = E.rename(columns={"complete_t5_session": "complete_t9_session",
                          "n_t5_day_1h_bars": "n_t9_day_1h_bars"})
    E["feature_data_as_of"] = "2026-08-17"
    E["token_registry_hash"] = TS.digest()
    E["t9_definition_hash"] = T9_DEF_HASH

    fwd = [c for c in list(E.columns) + list(M.columns) if c.startswith(("fwd_", "mtm_", "ret"))]
    if fwd:
        raise RuntimeError(f"outcome columns leaked into the X tables: {fwd}")

    E = _rename_out(E)
    M = _rename_out(M.drop(columns=["_tz"], errors="ignore"))
    E.to_parquet(OUT_EP, index=False, compression="zstd")
    M.to_parquet(OUT_MS, index=False, compression="zstd")

    # ── §1 audit ───────────────────────────────────────────────────────────
    has_t9 = E["n_t9_day_1h_bars"].fillna(0) > 0
    has_pv = E["n_prev_day_1h_bars"].fillna(0) > 0
    funnel = dict(BOTH=int((has_t9 & has_pv).sum()), T9_ONLY=int((has_t9 & ~has_pv).sum()),
                  PREV_ONLY=int((~has_t9 & has_pv).sum()), NONE=int((~has_t9 & ~has_pv).sum()))
    yearly = E.t9_date.str[:4].value_counts().sort_index().to_dict()

    # coverage bias: is 1H-covered vs missing biased on price / liquidity proxies?
    cov = E.assign(covered=(has_t9 & has_pv))
    bias = {}
    for col, label in (("t9_close", "price"), ("prev1d_close", "prev_price")):
        a = cov[cov.covered][col].median()
        b = cov[~cov.covered][col].median()
        bias[label] = dict(covered_median=round(float(a), 2),
                           missing_median=round(float(b), 2) if pd.notna(b) else None)

    audit = dict(
        spec_id="T9_DNA_AUDIT_V1", family_id="T9_MICROSTRUCTURE_DNA_V1",
        definition=T9_DEFINITION, definition_hash=T9_DEF_HASH,
        token_registry_hash=TS.digest(), n_tokens_1h=len(TOKENS_1H),
        reconstruction=dict(canonical=243562, raw=248966, agree=240579,
                            agree_pct=98.78, canonical_residual=2983, raw_t3_residual=2733),
        episodes=int(len(E)), tickers=int(E.ticker.nunique()),
        yearly=yearly, funnel=funnel,
        episode_id_unique=bool(~E.episode_id.duplicated().any()),
        coverage_bias=bias,
        m_rows=int(len(M)), m_episodes=int(M.episode_id.nunique()),
        y_status="NO OUTCOME COLUMN EXISTS IN THESE TABLES",
        runtime_min=round((time.time() - t0) / 60, 1))
    json.dump(audit, open(AUDIT, "w"), indent=2)
    print(f"  funnel {funnel}")
    print(f"  wrote {OUT_EP}\n        {OUT_MS}\n        {AUDIT}")
    print(f"  {audit['runtime_min']} min", flush=True)


# ── §1 no-future metamorphic test — the SAME statement against a truncated view ──────────
def no_future(cutoff="2024-12-31"):
    """Building X with an earlier historical cutoff must reproduce identical overlapping
    episode IDs and X fields. Injected into the SAME statement, not a re-typed copy."""
    cut_sql = _EPISODE_SQL.replace(
        "FROM bars WHERE universe IN ('sp500','nasdaq','russell2k')",
        f"FROM bars WHERE universe IN ('sp500','nasdaq','russell2k') "
        f"AND date <= DATE '{cutoff}'")
    if cut_sql.count(cutoff) != 2:
        raise RuntimeError("cutoff was not applied to both `FROM bars` sites")
    conn = duckdb.connect(DB1D, read_only=True)
    try:
        full = build_episodes(conn, verbose=False)
        trunc = build_episodes(conn, verbose=False, sql=cut_sql)
    finally:
        conn.close()
    a = full[full.t5_date <= cutoff].set_index("episode_id").sort_index()
    b = trunc.set_index("episode_id").sort_index()
    same_ids = set(a.index) == set(b.index)
    # in_sp500/in_nasdaq/in_russell2k are EVER-member flags computed over the whole table —
    # structurally NOT point-in-time (a ticker that joins an index later flips them). The
    # first run of this gate caught exactly that: 105/8,518/13,305 rows differed and nothing
    # else did. They are classified NON_PIT_CONVENIENCE: excluded from the X research
    # contract (never a feature, block variable or filter) and excluded from this
    # comparison. The same property is inherited by T5's table from the identical CTE.
    NON_PIT = {"in_sp500", "in_nasdaq", "in_russell2k"}
    cols = [c for c in a.columns if c in b.columns and c not in NON_PIT]
    same_x = a[cols].equals(b[cols]) if same_ids else False
    return dict(cutoff=cutoff, episodes_full_pre_cutoff=int(len(a)),
                episodes_truncated_build=int(len(b)),
                identical_ids=bool(same_ids), identical_x_pit=bool(same_x),
                non_pit_convenience_columns=sorted(NON_PIT),
                non_pit_rule="excluded from the X research contract — never a feature, "
                             "block variable or filter")


if __name__ == "__main__":
    main()
    print("  no-future:", no_future(), flush=True)
