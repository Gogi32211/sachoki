"""T1_MICROSTRUCTURE_DNA_V1 — X-only foundation. A FOURTH independent research family.

    family_id     T1_MICROSTRUCTURE_DNA_V1
    setup         FINAL_PRIORITY_RESOLVED_T1   (bullCode == 5, t_sig == 'T1')
    phase         X ONLY — no outcome, no MFE, no Z, no ranking
    independence  T5/T9/T3 contribute infrastructure and methodology ONLY

WHAT CANONICAL T1 IS (source of truth: the current Pine implementation)

    prev1IsBear = close[1] < open[1] OR doji[1]
    T1_raw      = prev1IsBear AND open >= close[1] AND open[1] >= open
                  AND close > open[1] AND close > open
    T1          = bullCode == 5 — after T4, T6, T1G, T2G only

    Structure: the bullish bar opens INSIDE the previous bearish body and closes ABOVE the
    previous open — a full bullish reclaim of the previous body. (Explanatory only; the
    implementation is authoritative.)

MACHINE-VERIFIED, NOT ASSERTED

    canonical t_sig='T1'          220,351
    raw OHLC reconstruction       268,854
    canonical AND raw             219,016   (99.39% of canonical — best of the four families)
    canonical NOT raw               1,335   (0.61%) feed/tick residual, smallest class yet
    priority exclusions by code   T4 46,160 · T1G 2,008 · T6 632 · T2G 313
    raw carrying t_sig='T2'           725   residual of the same feed/tick class

THE DOJI CASE — MEASURED, NOT ASSUMED, AND KEPT DESCRIPTIVE

    Unlike T3, T1 CAN mechanically follow a previous doji (open==open[1]==close[1] with
    close>open[1]). Measured:  RAW prev-doji 12,619 (4.7%) · CANONICAL prev-doji 682 (0.31%)
    · canonical prev-strict-bear 219,669. This split is an AUDIT FIELD ONLY — treating
    prev-bear vs prev-doji as separate inferential families would be a NEW research family
    and a NEW multiplicity decision, not part of V1.

REUSE, NOT REIMPLEMENTATION — identical to the T9/T3 pattern: t5_dna builders run
byte-identical and columns are renamed at write time; the episode SQL differs from t5_dna's
by the t_sig literal ONLY (asserted below).
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                    # noqa: E402
import duckdb, numpy as np, pandas as pd                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import combo_tokens as CT, combo_tokens_spec as TS                     # noqa: E402
import t5_dna as D5                                                    # noqa: E402

ROOT = D5.ROOT
DB1D, DB1H = D5.DB1D, D5.DB1H
OUT_EP = os.path.join(ROOT, "data", "t1_episodes.parquet")
OUT_MS = os.path.join(ROOT, "data", "t1_1h_microstructure.parquet")
AUDIT = os.path.join(HERE, "T1_DNA_AUDIT.json")

SETUP = "T1"
T1_DEFINITION = (
    "t_sig=='T1' as materialized in bars (priority-resolved import of the Pine 'T' column; "
    "bullCode==5). Raw: prev1IsBear (close[1]<open[1] OR doji[1]) AND open>=close[1] AND "
    "open[1]>=open AND close>open[1] AND close>open. Canonical = raw with T4,T6,T1G,T2G "
    "resolved first. Machine-verified: overlap 219,016/220,351 = 99.39%; residual 1,335 "
    "(0.61%) feed/tick class; priority exclusions T4 46,160 / T1G 2,008 / T6 632 / T2G 313; "
    "raw-carrying-T2 725 same residual class. Prev-doji is POSSIBLE and measured: raw 12,619, "
    "canonical 682 (0.31%) vs strict-bear 219,669 — descriptive audit split only, never a "
    "separate inferential family in V1. Universe rows deduplicated one per (ticker,date), "
    "sp500<nasdaq<russell2k."
)
T1_DEF_HASH = hashlib.sha256(T1_DEFINITION.encode()).hexdigest()[:16]

TOKENS_1H = D5.TOKENS_1H            # SAME canonical registry — no T9-specific tokens
SRC_COLS = D5.SRC_COLS
FAMILY_COLS = D5.FAMILY_COLS

# t5_dna's statement with exactly ONE literal changed. Asserted, not promised.
_EPISODE_SQL = D5._EPISODE_SQL.replace("l.t_sig = 'T5'", "l.t_sig = 'T1'")
assert _EPISODE_SQL != D5._EPISODE_SQL
assert _EPISODE_SQL.replace("l.t_sig = 'T1'", "l.t_sig = 'T5'") == D5._EPISODE_SQL, \
    "the T1 episode SQL must differ from t5_dna's by the t_sig literal ONLY"


def episode_id(ticker: str, d: str) -> str:
    return hashlib.sha256(f"{ticker}|{d}|{SETUP}|{T1_DEF_HASH}".encode()).hexdigest()[:16]


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
    ren = {c: c.replace("t5_", "t1_") for c in df.columns if c.startswith("t5_")}
    df = df.rename(columns=ren)
    if "relative_day" in df.columns:
        df["relative_day"] = df["relative_day"].str.replace("T5_DAY", "T1_DAY", regex=False)
    if "relative_position" in df.columns:
        df["relative_position"] = df["relative_position"].str.replace("T5_", "T1_", regex=False)
    return df


def main():
    t0 = time.time()
    print(f"T1_MICROSTRUCTURE_DNA_V1 · definition {T1_DEF_HASH} · registry {TS.digest()}",
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
    E = E.rename(columns={"complete_t5_session": "complete_t1_session",
                          "n_t5_day_1h_bars": "n_t1_day_1h_bars"})
    E["feature_data_as_of"] = "2026-08-17"
    E["token_registry_hash"] = TS.digest()
    E["t1_definition_hash"] = T1_DEF_HASH

    fwd = [c for c in list(E.columns) + list(M.columns) if c.startswith(("fwd_", "mtm_", "ret"))]
    if fwd:
        raise RuntimeError(f"outcome columns leaked into the X tables: {fwd}")

    E = _rename_out(E)
    M = _rename_out(M.drop(columns=["_tz"], errors="ignore"))
    E.to_parquet(OUT_EP, index=False, compression="zstd")
    M.to_parquet(OUT_MS, index=False, compression="zstd")

    # ── §1 audit ───────────────────────────────────────────────────────────
    has_t9 = E["n_t1_day_1h_bars"].fillna(0) > 0
    has_pv = E["n_prev_day_1h_bars"].fillna(0) > 0
    funnel = dict(BOTH=int((has_t9 & has_pv).sum()), T1_ONLY=int((has_t9 & ~has_pv).sum()),
                  PREV_ONLY=int((~has_t9 & has_pv).sum()), NONE=int((~has_t9 & ~has_pv).sum()))
    yearly = E.t1_date.str[:4].value_counts().sort_index().to_dict()

    # coverage bias: is 1H-covered vs missing biased on price / liquidity proxies?
    cov = E.assign(covered=(has_t9 & has_pv))
    bias = {}
    for col, label in (("t1_close", "price"), ("prev1d_close", "prev_price")):
        a = cov[cov.covered][col].median()
        b = cov[~cov.covered][col].median()
        bias[label] = dict(covered_median=round(float(a), 2),
                           missing_median=round(float(b), 2) if pd.notna(b) else None)

    audit = dict(
        spec_id="T1_DNA_AUDIT_V1", family_id="T1_MICROSTRUCTURE_DNA_V1",
        definition=T1_DEFINITION, definition_hash=T1_DEF_HASH,
        token_registry_hash=TS.digest(), n_tokens_1h=len(TOKENS_1H),
        reconstruction=dict(canonical=220351, raw=268854, agree=219016, agree_pct=99.39, canonical_residual=1335, priority_exclusions=dict(T4=46160, T1G=2008, T6=632, T2G=313), raw_carrying_T2=725, prev_doji_raw=12619, prev_doji_canonical=682, prev_strict_bear_canonical=219669),
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
