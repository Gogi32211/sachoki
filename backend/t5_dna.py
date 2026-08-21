"""1D SETUP -> 1H MICROSTRUCTURE DNA. The canonical, X-only data foundation for T5.

No sequence mining. No outcome ranking. No UI. This milestone builds the table that later
questions read, so that "which 1H bar belongs to which T5 day", "what is the previous
session", "which token is the final priority signal" are decided ONCE, here, and never
re-litigated per analysis.

THE EVENT DEFINITION, AND WHY IT IS TAKEN AS STORED

`t_sig` is not computed in this repo: studio/importer.py maps a CSV column "T" onto it, so
the Pine priority engine already ran at the source. Three measurements say the stored value
is the FINAL label rather than a raw candidate:

    13 distinct values, max length 3      one code per bar, not a set
    0 bars carry both t_sig and z_sig     bull and bear resolved against each other
    1,906 bars satisfying T5_raw carry T3 T3 outranks T5 (9th vs 11th), so the engine ran

So `t_sig = 'T5'` IS `bullCode == 11`, and re-deriving it would add a second definition
without adding information.

ONE DISCREPANCY IS RECORDED RATHER THAN SILENTLY RESOLVED. Reproducing the supplied Pine
T5_raw from daily OHLC with STRICT inequalities leaves 14,076 of 195,584 stored T5 events
unexplained (7.2%). Diagnosis: not missing sessions — the gap distribution is identical to
the matched bars — but doji. In 100% of the failures |close - open| < 0.1% with median
exactly 0.0%. Switching both bull/bear tests to non-strict explains 194,124 of 195,584
(99.25%), residual 1,460 (0.75%) still unexplained and most likely split/revision drift.

    supplied     pc <  po   AND close >  open     explains 92.8%
    materialized pc <= po   AND close >= open     explains 99.25%   <- chosen, on instruction

The choice was made by the user, not by this file, and the definition hash records which one
so a later table built on the other assumption cannot be mistaken for this one.

THE WINDOW IS TWO SESSIONS, NOT ONE

A 1D T5 is defined against the PREVIOUS daily bar, so its intraday evidence cannot be only
the T5 day. Both sessions are retrieved, and the previous session comes from the ticker's own
bar series — never `date - 1`. Measured on this calendar: 1,024 one-day gaps, 235 three-day
gaps and a maximum of 4, so calendar arithmetic would silently mis-assign every Monday.

SESSION SHAPE IS NOT ASSUMED

Bars per session are counted, not fixed: 750,024 sessions carry 7 bars, 5,977 carry 4 (early
closes) and 434 are otherwise irregular. The final bar starts 15:30 ET and is half an hour
long, so anything that treats the day as seven equal hours is wrong at the close.
`session_fraction` exists so a shortened session remains comparable.

TIMESTAMPS ARE CONVERTED, NOT SLICED

1H is stored in UTC and the exchange open lands at 13:30 UTC in summer and 14:30 in winter.
Cutting by UTC hour would put the DST boundary inside the data. Everything here converts to
America/New_York first.

NO OUTCOME IS READ. `bars` carries fwd_1d..fwd_90d in the same table as the features, so the
column list here is explicit and `SELECT *` is never used. Outcomes live in their own sidecar,
built separately and only after the entry semantics are chosen.
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
import combo_tokens as CT                                            # noqa: E402
import combo_tokens_spec as TS                                       # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DB1D = os.path.join(ROOT, "data", "studio_analytics.duckdb")
DB1H = os.path.join(ROOT, "data", "studio_1h.duckdb")
OUT_EP = os.path.join(ROOT, "data", "t5_episodes.parquet")
OUT_MS = os.path.join(ROOT, "data", "t5_1h_microstructure.parquet")
AUDIT = os.path.join(HERE, "T5_DNA_AUDIT.json")

SETUP = "T5"
# What "a T5 event" means here, hashed so a table built on the other reading is distinguishable.
T5_DEFINITION = (
    "t_sig=='T5' as materialized in bars (priority-resolved import of the Pine 'T' column; "
    "bullCode==11). Reproducible from daily OHLC as: prev_close<=prev_open AND close>=open "
    "AND open<prev_open AND open<prev_close AND close<prev_open AND prev_close>=close "
    "(NON-STRICT bull/bear, 99.25% agreement, 1,460 residual). Universe rows deduplicated to "
    "one per (ticker,date) with priority sp500<nasdaq<russell2k."
)
T5_DEF_HASH = hashlib.sha256(T5_DEFINITION.encode()).hexdigest()[:16]

# the registry's `bars`-sourced tokens are exactly the ones the 1H table can carry
TOKENS_1H = [t for t in TS.REGISTRY if t.source == "bars"]

# Section 7 asks for BOTH representations, and they do not come from the same columns.
# The registry reads T/Z through the per-code flags (`sig_t4`, `sig_z7`, …) because that is
# what its predicates need; the FAMILY view wants the categorical string the chart prints
# (`t_sig` = 'T4'). Deriving one from the other at read time is how a display label and a
# searchable token drift apart, so both are materialised from the source.
FAMILY_COLS = ["t_sig", "z_sig", "l_sig", "full_suffix", "ne_suffix", "wick_suffix",
               "close_suffix", "bar_body_wick", "bar_gap_range", "bar_line5", "vol_bucket",
               "phys_r", "phys_regime", "phys_m", "phys_e", "phys_k", "phys_c", "phys_h",
               "phys_s", "phys_ad", "phys_gap_true", "phys_wyc", "wyc_phase", "rsi_14"]
SRC_COLS = sorted({t.column for t in TOKENS_1H} | set(FAMILY_COLS))


def episode_id(ticker: str, d: str) -> str:
    return hashlib.sha256(f"{ticker}|{d}|{SETUP}|{T5_DEF_HASH}".encode()).hexdigest()[:16]


# ── 1 · episodes ─────────────────────────────────────────────────────────────
# Extracted so the no-future audit runs the SAME statement against a truncated view rather
# than a copy of it — a re-typed query would be testing a different thing than production.
_EPISODE_SQL = """
    WITH d AS (
      SELECT * FROM (
        SELECT ticker, date, universe, open, high, low, close,
               coalesce(t_sig,'') t_sig, coalesce(l_sig,'') l_sig,
               coalesce(full_suffix,'') full_suffix, coalesce(vol_bucket,'') vol_bucket,
               row_number() OVER (PARTITION BY ticker, date ORDER BY
                 CASE universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1 ELSE 2 END) rn
        FROM bars WHERE universe IN ('sp500','nasdaq','russell2k')
      ) WHERE rn = 1
    ),
    mem AS (
      SELECT ticker,
             max(CASE WHEN universe='sp500'     THEN 1 ELSE 0 END) in_sp500,
             max(CASE WHEN universe='nasdaq'    THEN 1 ELSE 0 END) in_nasdaq,
             max(CASE WHEN universe='russell2k' THEN 1 ELSE 0 END) in_russell2k
      FROM bars WHERE universe IN ('sp500','nasdaq','russell2k') GROUP BY 1
    ),
    l AS (
      SELECT *,
             lag(date)  OVER w AS prev_session_date,
             lag(open)  OVER w AS prev1d_open,  lag(high)  OVER w AS prev1d_high,
             lag(low)   OVER w AS prev1d_low,   lag(close) OVER w AS prev1d_close
      FROM d WINDOW w AS (PARTITION BY ticker ORDER BY date)
    )
    SELECT l.ticker, CAST(l.date AS VARCHAR) t5_date,
           CAST(l.prev_session_date AS VARCHAR) prev_session_date,
           l.universe primary_universe, m.in_sp500, m.in_nasdaq, m.in_russell2k,
           l.l_sig d1_l_sig, l.full_suffix d1_full_suffix, l.vol_bucket d1_vol_bucket,
           l.open t5_open, l.high t5_high, l.low t5_low, l.close t5_close,
           l.prev1d_open, l.prev1d_high, l.prev1d_low, l.prev1d_close,
           date_diff('day', l.prev_session_date, l.date) session_gap_days
    FROM l JOIN mem m ON m.ticker = l.ticker
    WHERE l.t_sig = 'T5' AND l.prev_session_date IS NOT NULL
    ORDER BY 1, 2"""


def build_episodes(conn, verbose=True) -> pd.DataFrame:
    """One row per T5 event. The previous session is the ticker's own previous bar."""
    E = conn.execute(_EPISODE_SQL).fetchdf()
    E.insert(0, "episode_id", [episode_id(t, d) for t, d in
                               zip(E["ticker"], E["t5_date"])])
    E["setup_1d"] = SETUP
    if E["episode_id"].duplicated().any():
        raise RuntimeError("episode_id is not unique — the grain is broken")
    if verbose:
        print(f"  episodes {len(E):,} · tickers {E.ticker.nunique():,} · "
              f"{E.t5_date.min()} .. {E.t5_date.max()}", flush=True)
    return E


# ── 2 · microstructure ───────────────────────────────────────────────────────
def build_microstructure(conn, E: pd.DataFrame, verbose=True) -> pd.DataFrame:
    """Both sessions of every episode, one row per 1H bar, with tokens.

    The join is on (ticker, session_date) alone: the 1H store holds each ticker under exactly
    one universe, so constraining the universe as well would drop AAPL's intraday data for
    every episode whose daily row happened to resolve to nasdaq.
    """
    keys = pd.concat([
        E[["episode_id", "ticker", "t5_date"]].assign(
            session_date=E["t5_date"], relative_day="T5_DAY"),
        E[["episode_id", "ticker", "t5_date"]].assign(
            session_date=E["prev_session_date"], relative_day="PREV_DAY"),
    ], ignore_index=True).dropna(subset=["session_date"])
    conn.register("keys", keys)
    sel = ", ".join(f"x.{c}" for c in SRC_COLS)
    q = f"""
    WITH hh AS (
      SELECT x.ticker, CAST(x.date AS DATE) session_date, x.date ts_utc,
             row_number() OVER (PARTITION BY x.ticker, CAST(x.date AS DATE)
                                ORDER BY x.date) session_position,
             count(*)   OVER (PARTITION BY x.ticker, CAST(x.date AS DATE)) bars_in_session,
             x.open, x.high, x.low, x.close, x.volume, {sel}
      FROM h.bars x
      JOIN (SELECT DISTINCT ticker, CAST(session_date AS DATE) sd FROM keys) k
        ON k.ticker = x.ticker AND CAST(x.date AS DATE) = k.sd
    )
    SELECT k.episode_id, hh.ticker, k.t5_date, k.relative_day,
           CAST(hh.session_date AS VARCHAR) session_date,
           strftime(hh.ts_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
                    '%Y-%m-%d %H:%M') ts_et,
           strftime(hh.ts_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
                    '%H:%M') et_time,
           hh.session_position, hh.bars_in_session,
           hh.open, hh.high, hh.low, hh.close, hh.volume, {', '.join(f'hh.{c}' for c in SRC_COLS)}
    FROM keys k
    JOIN hh ON hh.ticker = k.ticker AND hh.session_date = CAST(k.session_date AS DATE)
    ORDER BY k.episode_id, CASE k.relative_day WHEN 'PREV_DAY' THEN 0 ELSE 1 END,
             hh.session_position"""
    M = conn.execute(q).fetchdf()
    conn.unregister("keys")
    M["session_fraction"] = M["session_position"] / M["bars_in_session"]
    M["relative_position"] = (M["relative_day"].str.replace("_DAY", "", regex=False)
                              + "_H" + M["session_position"].astype(str))
    if verbose:
        print(f"  1H bars {len(M):,} · episodes with intraday "
              f"{M.episode_id.nunique():,}", flush=True)

    # tokens, from the canonical registry — not a second dictionary
    cols = {}
    for t in TOKENS_1H:
        cols[t.token_id] = CT._evaluate(M, t).astype(np.uint8)
    T = pd.DataFrame(cols, index=M.index)
    ids = np.array([t.token_id for t in TOKENS_1H])
    arr = T.to_numpy(dtype=bool)
    M["token_set"] = [" ".join(sorted(ids[row])) for row in arr]
    M["n_tokens"] = arr.sum(1).astype(np.int16)
    if verbose:
        print(f"  tokens attached: {len(TOKENS_1H)} declared · "
              f"mean {M.n_tokens.mean():.1f} true per bar", flush=True)
    return M, T


# ── 3 · X-only episode features ──────────────────────────────────────────────
def episode_features(M: pd.DataFrame, verbose=True) -> pd.DataFrame:
    """Ordered per-family sequences and counts. Descriptive, unranked, unfiltered."""
    M = M.sort_values(["episode_id", "relative_day", "session_position"],
                      key=lambda s: s.map({"PREV_DAY": 0, "T5_DAY": 1}) if s.name == "relative_day" else s)
    fam = {"tz_sequence": None, "l_sequence": "l_sig", "r_sequence": "phys_r",
           "c_sequence": "phys_c", "h_sequence": "phys_h", "bodywick_sequence": "bar_body_wick"}
    tz = np.where(M["t_sig"].fillna("") != "", M["t_sig"].fillna(""),
                  np.where(M["z_sig"].fillna("") != "", M["z_sig"].fillna(""), "·"))
    M = M.assign(_tz=tz)
    g = M.groupby("episode_id", sort=False)
    out = pd.DataFrame(index=g.size().index)
    out["n_1h_bars"] = g.size()
    out["n_prev_day_1h_bars"] = g["relative_day"].apply(lambda s: int((s == "PREV_DAY").sum()))
    out["n_t5_day_1h_bars"] = g["relative_day"].apply(lambda s: int((s == "T5_DAY").sum()))
    out["tz_sequence"] = g["_tz"].apply(lambda s: " ".join(s))
    for name, col in fam.items():
        if col:
            out[name] = g[col].apply(lambda s: " ".join(s.fillna("").replace("", "·")))
    out["n_T_signals"] = g["_tz"].apply(lambda s: int(s.str.startswith("T").sum()))
    out["n_Z_signals"] = g["_tz"].apply(lambda s: int(s.str.startswith("Z").sum()))
    def _trans(s, a, b):
        v = list(s)
        return sum(1 for i in range(len(v) - 1)
                   if v[i].startswith(a) and v[i + 1].startswith(b))
    out["bull_to_bear"] = g["_tz"].apply(lambda s: _trans(s, "T", "Z"))
    out["bear_to_bull"] = g["_tz"].apply(lambda s: _trans(s, "Z", "T"))
    out["distinct_tz_states"] = g["_tz"].apply(lambda s: s.nunique())
    out["token_diversity"] = g["token_set"].apply(
        lambda s: len({t for row in s for t in row.split() if t}))
    if verbose:
        print(f"  episode features on {len(out):,} episodes", flush=True)
    return out.reset_index()


# ── 4 · boundary ─────────────────────────────────────────────────────────────
def boundary_features(M: pd.DataFrame, E: pd.DataFrame) -> pd.DataFrame:
    """The PREV close -> T5 open seam, kept explicitly so it is not lost between sessions."""
    prev = M[M.relative_day == "PREV_DAY"].sort_values(["episode_id", "session_position"])
    t5 = M[M.relative_day == "T5_DAY"].sort_values(["episode_id", "session_position"])
    tz = lambda d: np.where(d["t_sig"].fillna("") != "", d["t_sig"].fillna(""),
                            np.where(d["z_sig"].fillna("") != "", d["z_sig"].fillna(""), "·"))
    pl = prev.assign(_tz=tz(prev)).groupby("episode_id").tail(2)
    tf = t5.assign(_tz=tz(t5)).groupby("episode_id").head(2)
    b = pd.DataFrame(index=sorted(set(pl.episode_id) | set(tf.episode_id)))
    b.index.name = "episode_id"
    gl = pl.groupby("episode_id")
    gf = tf.groupby("episode_id")
    b["prev_last2_tz"] = gl["_tz"].apply(lambda s: " ".join(s))
    b["prev_last_tokenset"] = gl["token_set"].apply(lambda s: s.iloc[-1])
    b["t5_first2_tz"] = gf["_tz"].apply(lambda s: " ".join(s))
    b["t5_first_tokenset"] = gf["token_set"].apply(lambda s: s.iloc[0])
    b["prev_day_last_1h_close"] = gl["close"].apply(lambda s: float(s.iloc[-1]))
    b["t5_day_first_1h_open"] = gf["open"].apply(lambda s: float(s.iloc[0]))
    b = b.reset_index()
    b["overnight_gap"] = b["t5_day_first_1h_open"] / b["prev_day_last_1h_close"] - 1
    return b


def main():
    t0 = time.time()
    print(f"  T5 definition hash {T5_DEF_HASH} · registry {TS.digest()}", flush=True)
    conn = duckdb.connect(DB1D, read_only=True)
    conn.execute(f"ATTACH '{DB1H}' AS h (READ_ONLY)")
    try:
        E = build_episodes(conn)
        M, T = build_microstructure(conn, E)
    finally:
        conn.close()

    F = episode_features(M)
    B = boundary_features(M, E)
    E = E.merge(F, on="episode_id", how="left").merge(B, on="episode_id", how="left")
    E["complete_prev_session"] = E["n_prev_day_1h_bars"].fillna(0) > 0
    E["complete_t5_session"] = E["n_t5_day_1h_bars"].fillna(0) > 0
    E["feature_data_as_of"] = "2026-08-17"
    E["token_registry_hash"] = TS.digest()
    E["t5_definition_hash"] = T5_DEF_HASH

    fwd = [c for c in list(E.columns) + list(M.columns) if c.startswith(("fwd_", "mtm_", "ret"))]
    if fwd:
        raise RuntimeError(f"outcome columns leaked into the X tables: {fwd}")

    E.to_parquet(OUT_EP, index=False, compression="zstd")
    M.drop(columns=["_tz"], errors="ignore").to_parquet(OUT_MS, index=False,
                                                        compression="zstd")
    print(f"\n  WROTE {OUT_EP}  {os.path.getsize(OUT_EP)/1e6:.1f} MB", flush=True)
    print(f"  WROTE {OUT_MS}  {os.path.getsize(OUT_MS)/1e6:.1f} MB", flush=True)
    print(f"  {time.time()-t0:.0f}s · NO OUTCOME READ", flush=True)
    return E, M, T


if __name__ == "__main__":
    main()
