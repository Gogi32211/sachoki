"""T4_MICROSTRUCTURE_DNA_V1 — X-only foundation. A FIFTH independent research family.

    family_id     T4_MICROSTRUCTURE_DNA_V1
    setup         FINAL_PRIORITY_RESOLVED_T4   (bullCode == 1, t_sig == 'T4')
    phase         X ONLY — no outcome, no MFE, no Z, no ranking

ENGULF CONFIGURATION — LOCATED IN PRODUCTION, NOT ASSUMED (T4_ENGULF_CONFIG_V1)

    use_wick        False        analyzers/tz_wlnbb/config.py:51 and signal_engine.py:67
    min_body_ratio  1.0          config.py:52 and signal_engine.py:68
    mintick         1e-10        signal_engine.py:97  (prevBodySafe = max(prevBody, 1e-10))
    isDoji          close==open  signal_engine.py:99 — doji_thresh exists as a parameter but
                                 does NOT participate in isDoji; verified in source
    T4_raw          p1Bear AND isBull AND (cTop>=pTop) AND (pBot>=cBot)
                                 AND cBody/max(pBody,1e-10) >= 1.0

T4 RAW/CANONICAL INVARIANT — AMENDED (user decision 2026-08-21)

The original cross-vintage assertion

    raw_T4(current_store_OHLC) == materialized_canonical_T4

is INVALID: historical materialized labels and the current OHLC store do not share an
identical input vintage. The valid semantic invariant is

    identical input bars + identical engulf configuration + identical T4 implementation
        =>  raw_T4 == canonical_T4

and it PASSED: engine_vs_SQL_same_input = 78,181/78,181, mismatches 0.

The materialized t_sig='T4' population REMAINS the canonical research population.
Reconstruction from the current OHLC store is DIAGNOSTIC ONLY and MUST NOT replace or
"repair" historical materialized T4 membership — the 250,162 historical episodes are never
rebuilt into the 260,013 current-OHLC population. Same law as T5's frozen X: a historical
derived state cannot be corrected with today's input vintage. This amendment reformulates
the gate; it grants NO earlier access to T4 outcomes.

THE MEASUREMENT THAT FORCED THE AMENDMENT

T4 is FIRST in bullish priority, so raw and canonical membership must be identical under
identical semantics AND identical inputs. Measured against the materialized store:

    canonical 250,162 · raw-on-current-OHLC 260,013 · agree 249,495
    canon_not_raw    667   (0.27% — the smallest residual of the five families)
    raw_not_canon 10,518   (4.0% of raw) — carried by EVERY other T-code, 88.9% within 0.01
                           of the engulf boundary, median margin EXACTLY 0, rising by vintage

    THE DECISIVE SEPARATION: signal_engine.compute_signals vs the SQL reconstruction on
    IDENTICAL current DB bars = 78,181/78,181, ZERO mismatches. Semantics are extensionally
    identical. The entire bilateral residual is therefore INPUT-VINTAGE: the stored t_sig
    was produced from feed-of-the-day prices, and subsequent OHLC revisions (split/adjustment
    class) moved boundary-tied rows while the labels stayed frozen. T4's priority-first
    position is what makes this residual VISIBLE bilaterally; the other four families carry
    the same class one-sidedly.

    Research population: the CANONICAL MATERIALIZED t_sig='T4' (same rule as all families).
    The definition is NOT modified to improve reconstruction.

PREVIOUS-BAR AUDIT (descriptive only — no T4_BEAR/T4_DOJI/... sub-families in V1)

    prev strict-bear 218,885 · prev doji 31,276 (12.5%) — a doji body is engulfed by any
    bull bar whose body straddles it, so the doji share is structurally large here.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                    # noqa: E402
import duckdb, numpy as np, pandas as pd                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import combo_tokens as CT, combo_tokens_spec as TS                     # noqa: E402
import t5_dna as D5                                                    # noqa: E402

ROOT = D5.ROOT
DB1D, DB1H = D5.DB1D, D5.DB1H
OUT_EP = os.path.join(ROOT, "data", "t4_episodes.parquet")
OUT_MS = os.path.join(ROOT, "data", "t4_1h_microstructure.parquet")
AUDIT = os.path.join(HERE, "T4_DNA_AUDIT.json")

SETUP = "T4"
T4_DEFINITION = (
    "t_sig=='T4' as materialized in bars (priority-resolved import of the Pine 'T' column; "
    "bullCode==1, FIRST in bullish priority). Raw: prevBear(close[1]<open[1] OR doji[1]) AND "
    "close>open AND max(o,c)>=max(o1,c1) AND min(o1,c1)>=min(o,c) AND "
    "|c-o|/max(|c1-o1|,1e-10)>=1.0 with use_wick=False, min_body_ratio=1.0 "
    "(T4_ENGULF_CONFIG_V1, from production config.py/signal_engine.py). Engine-vs-SQL on "
    "identical DB bars: 78,181/78,181 exact — semantics proven; bilateral store residual "
    "(667 canon-not-raw 0.27%; 10,518 raw-not-canon, 88.9% within 0.01 of the engulf "
    "boundary, median margin 0) is INPUT-VINTAGE, visible bilaterally only because T4 is "
    "priority-first. Prev-doji 31,276 (12.5%) is structural and descriptive-only. Universe "
    "rows deduplicated one per (ticker,date), sp500<nasdaq<russell2k."
)
T4_DEF_HASH = hashlib.sha256(T4_DEFINITION.encode()).hexdigest()[:16]

TOKENS_1H = D5.TOKENS_1H            # SAME canonical registry — no T9-specific tokens
SRC_COLS = D5.SRC_COLS
FAMILY_COLS = D5.FAMILY_COLS

# t5_dna's statement with exactly ONE literal changed. Asserted, not promised.
_EPISODE_SQL = D5._EPISODE_SQL.replace("l.t_sig = 'T5'", "l.t_sig = 'T4'")
assert _EPISODE_SQL != D5._EPISODE_SQL
assert _EPISODE_SQL.replace("l.t_sig = 'T4'", "l.t_sig = 'T5'") == D5._EPISODE_SQL, \
    "the T4 episode SQL must differ from t5_dna's by the t_sig literal ONLY"


def episode_id(ticker: str, d: str) -> str:
    return hashlib.sha256(f"{ticker}|{d}|{SETUP}|{T4_DEF_HASH}".encode()).hexdigest()[:16]


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
    ren = {c: c.replace("t5_", "t4_") for c in df.columns if c.startswith("t5_")}
    df = df.rename(columns=ren)
    if "relative_day" in df.columns:
        df["relative_day"] = df["relative_day"].str.replace("T5_DAY", "T4_DAY", regex=False)
    if "relative_position" in df.columns:
        df["relative_position"] = df["relative_position"].str.replace("T5_", "T4_", regex=False)
    return df


def main():
    t0 = time.time()
    print(f"T4_MICROSTRUCTURE_DNA_V1 · definition {T4_DEF_HASH} · registry {TS.digest()}",
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
    E = E.rename(columns={"complete_t5_session": "complete_t4_session",
                          "n_t5_day_1h_bars": "n_t4_day_1h_bars"})
    E["feature_data_as_of"] = "2026-08-17"
    E["token_registry_hash"] = TS.digest()
    E["t4_definition_hash"] = T4_DEF_HASH

    fwd = [c for c in list(E.columns) + list(M.columns) if c.startswith(("fwd_", "mtm_", "ret"))]
    if fwd:
        raise RuntimeError(f"outcome columns leaked into the X tables: {fwd}")

    E = _rename_out(E)
    M = _rename_out(M.drop(columns=["_tz"], errors="ignore"))
    E.to_parquet(OUT_EP, index=False, compression="zstd")
    M.to_parquet(OUT_MS, index=False, compression="zstd")

    # ── §1 audit ───────────────────────────────────────────────────────────
    has_t9 = E["n_t4_day_1h_bars"].fillna(0) > 0
    has_pv = E["n_prev_day_1h_bars"].fillna(0) > 0
    funnel = dict(BOTH=int((has_t9 & has_pv).sum()), T4_ONLY=int((has_t9 & ~has_pv).sum()),
                  PREV_ONLY=int((~has_t9 & has_pv).sum()), NONE=int((~has_t9 & ~has_pv).sum()))
    yearly = E.t4_date.str[:4].value_counts().sort_index().to_dict()

    # coverage bias: is 1H-covered vs missing biased on price / liquidity proxies?
    cov = E.assign(covered=(has_t9 & has_pv))
    bias = {}
    for col, label in (("t4_close", "price"), ("prev1d_close", "prev_price")):
        a = cov[cov.covered][col].median()
        b = cov[~cov.covered][col].median()
        bias[label] = dict(covered_median=round(float(a), 2),
                           missing_median=round(float(b), 2) if pd.notna(b) else None)

    audit = dict(
        spec_id="T4_DNA_AUDIT_V1", family_id="T4_MICROSTRUCTURE_DNA_V1",
        definition=T4_DEFINITION, definition_hash=T4_DEF_HASH,
        token_registry_hash=TS.digest(), n_tokens_1h=len(TOKENS_1H),
        reconstruction=dict(canonical=250162, raw_on_current_ohlc=260013, agree=249495, canon_not_raw=667, raw_not_canon=10518, engine_vs_sql_identical_inputs='78181/78181 exact', residual_class='INPUT_VINTAGE', boundary_share_lt_001=0.889, prev_doji=31276, prev_strict_bear=218885),
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
