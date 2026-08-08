"""
edge_replay.py — ONE unified backtest engine for ALL Edge-board setups.

Replaces the scattered per-journal replays (each with its own conventions) with a
single path-sim engine + a signature registry that mirrors the LIVE scanners, so
backtest == what you'd actually trade (no live≠replay drift).

Every setup is reduced to a lookahead-free ENTRY mask on a shared per-ticker bar
frame. The SAME path-sim (entry@next-open, stop-first, 15bps, cooldown-5) runs on
all of them. Two exit modes: 'trail' (trailing-% over maxh bars) or 'bracket'
(fixed stop/target over maxh bars). Returns mean/win/PF/per-year/concentration so
setups can be compared head-to-head.

READ-ONLY on bars.
"""
from __future__ import annotations
import json
import logging
import os
import threading
import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

SLIP = 0.0015
_BULLT = ("T1", "T1G", "T2", "T2G", "T3", "T5", "T9", "T10", "T11", "T12")
_ANCH = ("Z11", "Z3", "Z1G", "Z5")
_CONF = ("T3", "T5")
_RES = ("T11", "T12")


def _load_h1() -> dict:
    path = os.path.join(os.path.dirname(__file__), "onehour_capit.json")
    try:
        with open(path) as f:
            return json.load(f).get("events", {})
    except Exception:
        return {}


_M15RSI = None


def _load_m15rsi() -> pd.Series:
    """(ticker|day) -> day's MIN 15m RSI, from the m15_dayrsi.duckdb cache (built off the
    enriched studio_15m DB; refresh: rebuild cache after a 15m re-enrich). Empty on miss."""
    global _M15RSI
    if _M15RSI is None:
        try:
            import duckdb
            from studio.paths import db_path
            p = os.path.join(os.path.dirname(db_path("studio_15m.duckdb")), "m15_dayrsi.duckdb")
            c = duckdb.connect(p, read_only=True)
            t = c.execute("SELECT ticker, d, rsi15 FROM day_rsi").fetchdf()
            c.close()
            _M15RSI = pd.Series(t["rsi15"].to_numpy(),
                                index=(t["ticker"] + "|" + t["d"]).to_numpy())
        except Exception:
            _M15RSI = pd.Series(dtype=float)
    return _M15RSI


_QZCAP = None


def _load_qzcap():
    """(ticker|day) frozensets: hcap = day had a 1H Z2G/Z1G (intraday capitulation);
    hrev = day had a 1H REVERSAL-CLUSTER signal (T1G/T5/T11/Z11/Z1G — validated 2026-07-11 to
    be ~interchangeable; T1G alone was a mild overfit, the cluster is 5× broader & more robust).
    One-time DB load, cached."""
    global _QZCAP
    if _QZCAP is None:
        try:
            import duckdb
            from studio.paths import db_path
            c = duckdb.connect(db_path("studio_1h.duckdb"), read_only=True)
            q = ("SELECT DISTINCT ticker || '|' || "
                 "CAST(CAST(date - INTERVAL 5 HOUR AS DATE) AS VARCHAR) k FROM bars WHERE ")
            cap = c.execute(q + "z_sig IN ('Z2G','Z1G')").fetchdf()
            rev = c.execute(q + "t_sig IN ('T1G','T5','T11') OR z_sig IN ('Z11','Z1G')").fetchdf()
            c.close()
            _QZCAP = (frozenset(cap["k"].to_numpy()), frozenset(rev["k"].to_numpy()))
        except Exception:
            _QZCAP = (frozenset(), frozenset())
    return _QZCAP


_M15_ZDOM = None


def _load_m15_zdom():
    """(ticker|day) frozenset of days whose 15m session was Z-DOMINANT (T/(Z+T) < 0.5) — i.e.
    intraday absorption still outweighs clean demand. The novel ingredient of the watch-tier
    Engulf-Absorb-Reversal setup. One-time whole-universe 15m aggregate (~7s), cached."""
    global _M15_ZDOM
    if _M15_ZDOM is None:
        try:
            import duckdb
            from studio.db import tf_db_path
            c = duckdb.connect(tf_db_path("15m"), read_only=True)
            mr = c.execute(
                "SELECT ticker || '|' || substr(CAST(date AS VARCHAR),1,10) k, "
                "sum(CASE WHEN coalesce(t_sig,'') LIKE 'T%' THEN 1 ELSE 0 END) tc, "
                "sum(CASE WHEN coalesce(z_sig,'')<>'' THEN 1 ELSE 0 END) zc "
                "FROM bars GROUP BY k").fetchdf()
            c.close()
            _M15_ZDOM = frozenset(
                r.k for r in mr.itertuples() if (r.tc + r.zc) > 0 and r.tc / (r.tc + r.zc) < 0.50)
        except Exception:
            log.exception("m15_zdom load failed — retrying on next call")
            return frozenset()      # never cache a failure — see _load_h1_dr
    return _M15_ZDOM


_IV_LINES = None
_H1_QUIET = None


def _load_h1_quiet():
    """{TICKER|YYYY-MM-DD} where the trailing 10 sessions of 1H tape were QUIET —
    max(1h volume) / avg(1h volume) < 4.

    Found by inverting a spike hunt (2026-07-30). Chasing what precedes a +40% day led to a
    1H volume extreme days earlier: at $21+ it appears in 18.8% of pre-spike windows vs 2.0%
    of price-matched controls, a 9.4× lift on spike FREQUENCY. Path-sim then said those same
    states LOSE — monotonically worse as the threshold rises (≥6× −1.04 · ≥8× −1.48 ·
    ≥10× −1.65 · ≥15× −1.68, against a −0.71 baseline) — because a volume event predicts
    VOLATILITY, not direction. The complement was the best cell in the whole table:
    quiet tape −0.22 / win 49.4 on n=285,087.
    So the harvest is the inverse of the search, and it is the same thing the ⛔ vol-adjacency
    veto already says, reached from a new direction.
    15m is the wrong granularity for this: ≥6× fires on ~90% of ALL windows there, spike or
    not. 1D averages the event away. 1H is where it is legible.
    """
    global _H1_QUIET
    if _H1_QUIET is None:
        try:
            import duckdb
            from studio.db import tf_db_path
            c = duckdb.connect(tf_db_path("1h"), read_only=True)
            df = c.execute("""
                WITH day AS (
                  SELECT ticker, CAST(date AS DATE) d,
                         max(volume) mx, sum(volume) sv, count(*) nb
                  FROM bars WHERE volume > 0 GROUP BY ticker, CAST(date AS DATE)),
                r AS (
                  SELECT ticker, d,
                    max(mx) OVER (PARTITION BY ticker ORDER BY d
                                  ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) rmx,
                    sum(sv) OVER (PARTITION BY ticker ORDER BY d
                                  ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) rsv,
                    sum(nb) OVER (PARTITION BY ticker ORDER BY d
                                  ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) rnb
                  FROM day)
                SELECT ticker, CAST(d AS VARCHAR) dt
                FROM r
                WHERE rnb >= 30 AND rmx / NULLIF(rsv / NULLIF(rnb, 0), 0) < 4.0
            """).fetchdf()
            c.close()
            _H1_QUIET = frozenset(df["ticker"] + "|" + df["dt"].str[:10])
            log.info("h1_quiet map: %s ticker-days", len(_H1_QUIET))
        except Exception:
            # never cache a failure — see _load_h1_dr
            log.exception("h1_quiet load failed — retrying on next call")
            return frozenset()
    return _H1_QUIET


_H1_DR = None


def _load_h1_dr():
    """(2026-07-28) Per (ticker|day): did a 1H 🔄 DUAL RECLAIM fire that session?

    1H RSI(14) crossing back up through 35 with a 1H CCI(20) reclaim of −100 within ±14 bars
    (±2 calendar days). The RS half is NOT applied here — it is a daily state and is ANDed in
    _prep, which keeps this loader a pure function of the 1h DB and therefore cacheable.

    Standalone the intraday DR is NOT tradeable (5-day-hold excess +0.08..+0.19, under the
    0.5-1% round-trip cost) — but as a CONFIRMATION on a daily setup it is the second
    universal booster we have found, alongside the 💥 intraday volume event:
      63 board setups tested, 52 improved (83%), median Δ +1.34 with the base restricted to
      the SAME 2022+ window the 1h DB covers (unrestricted it is +1.45 — so this is not the
      2021 sample simply dropping out; that control was run precisely because it could have been).
      Washout −0.16 → +3.45 · L43-TRIPLE +2.70 → +6.61 · Zone-Retest +0.23 → +2.18 ·
      RTB-Base🧱OB +5.72 → +9.49.
    The ~10 setups it does NOT help are almost all 🏆RS variants (QZ-Capit🏆RS −0.07,
    Cluster🏆RS −0.16, D+L1🏆RS −0.45) — the DR already carries RS, so on an RS-gated setup
    there is nothing left to add. That pattern is evidence the mechanism is real, not noise.

    Same lazy whole-universe aggregate as _load_intraday_lines; the 1h DB is refreshed nightly.
    """
    global _H1_DR
    if _H1_DR is None:
        try:
            import duckdb
            from studio.db import tf_db_path
            c = duckdb.connect(tf_db_path("1h"), read_only=True)
            df = c.execute(
                "SELECT ticker, CAST(date AS VARCHAR) dt, rsi_14, cci_20 FROM bars "
                "WHERE rsi_14 IS NOT NULL AND cci_20 IS NOT NULL ORDER BY ticker, date").fetchdf()
            c.close()
            keys = set()
            for tk, g in df.groupby("ticker", sort=False):
                r = g["rsi_14"].to_numpy(float); cc = g["cci_20"].to_numpy(float)
                pr = np.concatenate([[np.nan], r[:-1]])[:len(r)]
                pc = np.concatenate([[np.nan], cc[:-1]])[:len(cc)]
                rx = (pr < 35) & (r >= 35)
                cx = (pc < -100) & (cc >= -100)
                nx = cx.copy()
                for k in range(1, 15):                      # ±14 bars ≈ ±2 calendar days
                    nx |= np.concatenate([cx[k:], np.zeros(k, bool)])[:len(cx)]
                    nx |= np.concatenate([np.zeros(k, bool), cx[:-k]])[:len(cx)]
                hit = rx & nx
                day = g["dt"].str[:10].to_numpy()
                for d in day[hit]:
                    keys.add(f"{tk}|{d}")
            _H1_DR = frozenset(keys)
            log.info("h1_dr map: %s ticker-days", len(_H1_DR))
        except Exception:
            # Do NOT cache the failure. The commonest cause is the nightly holding the 1h DB
            # lock ("Could not set lock"); caching frozenset() there would leave the 🕐DR gate
            # silently dead for the life of the process — every gated setup would stop firing
            # with nothing in the UI to say so. Returning empty for THIS call degrades the gate
            # to never-fires only until the next call, which will retry.
            log.exception("h1_dr load failed — retrying on next call (gate empty meanwhile)")
            return frozenset()
    return _H1_DR


def _load_intraday_lines():
    """(2026-07-26) Per (ticker|day) 15m VSA-line presence — the validated MTF veto layer.
    Returns three frozensets of 'TICKER|YYYY-MM-DD' keys:
      l34g — a GREEN L34 (demand line) printed at least once intraday
      l34  — any L34 printed intraday
      l46r — a RED L46 (supply being worked) printed at least once intraday
    Validated 2026-07-26 (intraday_l34_in_l46.py / intraday_color.py). On a daily L46 day the
    intraday session must show BOTH sides or the daily signal is hollow:
      · 15m RED L46 absent  → med −8.84 / pf 0.52 / 1-6yr  (supply never actually worked)
      · 15m GREEN L34 absent → med −1.79 / worst −3.9      (buyers never stepped in)
    The "no intraday L34" cell is a veto on several edges: washout **1/6yr med −5.28 pf 0.72**,
    zoneretest 3/6 med −1.72, atomic 3/6 med −2.54, D+L1 2/6 med −2.40 (NOT qzcapit/coilfloor —
    edge-specific, like every gate). Same lazy whole-universe aggregate as _load_m15_zdom (~7s,
    cached per process); the 15m DB is refreshed by the nightly, so no extra job is needed."""
    global _IV_LINES
    if _IV_LINES is None:
        try:
            import duckdb
            from studio.db import tf_db_path
            c = duckdb.connect(tf_db_path("15m"), read_only=True)
            mr = c.execute(
                "SELECT ticker || '|' || substr(CAST(date AS VARCHAR),1,10) k, "
                "sum(CASE WHEN coalesce(l_sig,'')='L34' AND close>open THEN 1 ELSE 0 END) g34, "
                "sum(CASE WHEN coalesce(l_sig,'')='L34' THEN 1 ELSE 0 END) a34, "
                "sum(CASE WHEN coalesce(l_sig,'')='L46' AND close<open THEN 1 ELSE 0 END) r46, "
                "max(volume) / nullif(avg(volume),0) vsp "
                "FROM bars WHERE volume > 0 GROUP BY k").fetchdf()
            c.close()
            _IV_LINES = (frozenset(r.k for r in mr.itertuples() if r.g34 >= 1),
                         frozenset(r.k for r in mr.itertuples() if r.a34 >= 1),
                         frozenset(r.k for r in mr.itertuples() if r.r46 >= 1),
                         frozenset(r.k for r in mr.itertuples() if r.vsp >= 4),
                         # DRY session = the severe veto cell (<2.5×, only ~3% of days). Kept
                         # separate from the ≥4× positive gate on purpose: the 2.5-4× middle band
                         # is merely mediocre, so flagging it as ⛔ would cry wolf on 28% of days.
                         frozenset(r.k for r in mr.itertuples() if r.vsp < 2.5))
        except Exception:
            log.exception("intraday line map failed — retrying on next call")
            # never cache a failure — see _load_h1_dr
            return (frozenset(), frozenset(), frozenset(), frozenset(), frozenset())
    return _IV_LINES


def _pull(months: int, dv_floor: float, ticker: str = None) -> pd.DataFrame:
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        _tkf = f"AND ticker = '{str(ticker).upper().replace(chr(39), '')}'" if ticker else ""
        df = a.execute(f"""
            WITH r AS (
              SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
              FROM bars
              WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= {dv_floor}
                AND universe <> 'index'   -- ETF/index rows must not enter stock backtest stats
                AND date >= DATE '{as_of}' - INTERVAL {int(months)*31 + 40} DAY
                {_tkf}
            )
            SELECT universe, ticker, date, open, high, low, close, volume, rsi_14, atr_14,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z, coalesce(l_sig,'') l,
                   coalesce(vol_bucket,'') vb, coalesce(bar_gap_class,'') gap,
                   coalesce(close_suffix,'') csfx, coalesce(bar_line5,'') l5,
                   coalesce(full_suffix,'') fsfx,
                   coalesce(w2_spring,0) spring,
                   coalesce(sig_t11,0) t11, coalesce(sig_t12,0) t12, coalesce(sig_eb_up,0) ebu,
                   coalesce(sig_any_d,0) anyd, coalesce(sig_l1,0) l1,
                   coalesce(sig_p55,0) p55, coalesce(sig_para_start,0) para,
                   CASE WHEN sig_l6=1 AND sig_l4=1 AND close>=open THEN 1 ELSE 0 END l43,
                   coalesce(wt_valid_tr,0) vtr, coalesce(wt_support,0) wt_sup,
                   coalesce(wt_resistance,0) wt_res, coalesce(rtb_phase,'0') rtb_ph,
                   coalesce(wt_evr,0) wtevr,
                   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
                        THEN 1 ELSE 0 END supp,
                   coalesce(CAST(pb_pp_rtv AS TINYINT),0) ppr,
                   coalesce(CAST(sig_ns_vabs AS TINYINT),0) nsv,
                   coalesce(CAST(sig_rl AS TINYINT),0) rlv,
                   coalesce(CAST(w2_sc AS TINYINT),0) w2sc,
                   coalesce(cci_20,0) cci20,
                   CASE WHEN sig_fbo_dn=1 OR sig_eb_dn=1 OR sig_vbo_dn=1 OR sig_any_d=1
                        THEN 1 ELSE 0 END pressev,
                   coalesce(CAST(sig_nd_vabs AS TINYINT),0) ndv,
                   CASE WHEN sig_vol_10x=1 OR sig_vol_20x=1 THEN 1 ELSE 0 END vspk,
                   coalesce(beta_score,0) betas,
                   coalesce(CAST(sig_conso AS TINYINT),0) conso
            FROM r WHERE rn = 1 ORDER BY ticker, date
        """).fetchdf()
        return df, as_of
    finally:
        a.close()


_DIV_L, _DIV_R, _DIV_MAXGAP = 1, 4, 60


def _divergence_arrays(df: pd.DataFrame, osc: str = "rsi_14"):
    """Causal oscillator divergence on confirmed price pivots (2026-07-28).

    A pivot at bar i is only KNOWN at i+R, so the flag is raised on bar i+R and the
    RSI compared is the RSI *at the pivot* — nothing peeks forward. Pivot detection is
    vectorised per ticker; only the handful of pivot bars are then walked in order to
    compare each low/high with the previous one (>_DIV_MAXGAP bars apart = not comparable).

    Returns (bull, bear, rsi_at_low, rsi_at_high) aligned to df's rows.
    """
    n_all = len(df)
    bull = np.zeros(n_all, bool); bear = np.zeros(n_all, bool)
    rlo = np.full(n_all, np.nan); rhi = np.full(n_all, np.nan)
    L, R, GAP = _DIV_L, _DIV_R, _DIV_MAXGAP
    for _, idx in df.groupby("ticker", sort=False).indices.items():
        idx = np.asarray(idx)
        n = len(idx)
        if n < 4 * R + 10:
            continue
        lo = df["low"].to_numpy(float)[idx]
        hi = df["high"].to_numpy(float)[idx]
        rs = df[osc].to_numpy(float)[idx]
        lS, hS = pd.Series(lo), pd.Series(hi)
        lo_prev = lS.shift(1).rolling(L).min().to_numpy()
        lo_next = lS[::-1].rolling(R).min()[::-1].shift(-1).to_numpy()
        hi_prev = hS.shift(1).rolling(L).max().to_numpy()
        hi_next = hS[::-1].rolling(R).max()[::-1].shift(-1).to_numpy()
        pl = (lo < lo_prev) & (lo < lo_next) & np.isfinite(lo_prev) & np.isfinite(lo_next)
        ph = (hi > hi_prev) & (hi > hi_next) & np.isfinite(hi_prev) & np.isfinite(hi_next)
        for piv, price, better, out, val in (
                (pl, lo, np.less, bull, rlo),          # bull: price LOWER low, RSI HIGHER low
                (ph, hi, np.greater, bear, rhi)):      # bear: price HIGHER high, RSI LOWER high
            prev_i = -1
            for i in np.nonzero(piv)[0]:
                c = i + R
                if c >= n:
                    break
                if prev_i >= 0 and (i - prev_i) <= GAP and better(price[i], price[prev_i]):
                    # bull wants rsi[i] > rsi[prev]; bear wants rsi[i] < rsi[prev]
                    hit = (rs[i] > rs[prev_i]) if out is bull else (rs[i] < rs[prev_i])
                    if hit:
                        out[idx[c]] = True
                        val[idx[c]] = rs[i]
                prev_i = i
    return bull, bear, rlo, rhi


def _prep(df: pd.DataFrame) -> pd.DataFrame:
    """Add helper + lag columns + entry masks for every setup (lookahead-free)."""
    _refresh_mined()                       # pick up any freshly-promoted mined combos
    g = df.groupby("ticker", sort=False)
    df["pc"] = g["close"].shift(1)
    df["disp"] = (df["open"] - df["pc"]).abs() / df["atr_14"].replace(0, np.nan)
    df["sweet"] = df["disp"].between(0.5, 1.5)
    df["revt"] = ((df["t11"] == 1) | (df["t12"] == 1) | (df["ebu"] == 1))
    df["bullt"] = df["t"].isin(_BULLT)
    df["clean"] = df["supp"] == 0
    df["nonvb"] = df["vb"] != "VB"
    # sequence lags for Z11-T11
    df["z2"] = g["z"].shift(2)
    df["rsi2"] = g["rsi_14"].shift(2)
    df["t1"] = g["t"].shift(1)
    # 1H confirmation (h1-bottom) from cache
    h1 = _load_h1()
    dstr = df["date"].astype(str).str[:10]
    df["h1c"] = [d in h1.get(t, ()) for t, d in zip(df["ticker"], dstr)]

    base = df["clean"] & df["nonvb"]
    df["E_l43triple"] = base & (df["l43"] == 1) & df["revt"] & df["sweet"] & (df["rsi_14"] < 40)
    df["E_z11t11"] = (base & df["z2"].isin(_ANCH) & df["rsi2"].between(30, 45)
                      & df["t1"].isin(_CONF) & df["t"].isin(_RES))
    df["E_washout"] = (base & df["rsi_14"].between(20, 36) & df["bullt"]
                       & (df["l5"].str.contains("VX|VR", regex=True, na=False))
                       & df["l5"].str.contains("R2", na=False))
    df["E_dl1"] = base & (df["anyd"] == 1) & (df["l1"] == 1)
    df["E_g3"] = base & (df["gap"] == "G3") & df["bullt"] & (df["rsi_14"] < 45)
    df["E_atomic"] = base & df["bullt"] & (df["csfx"] == "O") & df["gap"].isin(("G2", "G3"))
    df["E_h1bottom"] = base & (df["rsi_14"] < 35) & df["bullt"] & df["h1c"]
    df["E_spring"] = base & (df["spring"] == 1) & (df["rsi_14"] < 60) & df["bullt"]
    df["E_p55"] = base & (df["p55"] == 1) & df["bullt"]
    df["E_parabola"] = base & (df["para"] == 1)
    # Atomic-R — the refined atomic (validated 2026-07-01, project_atomic_edge_validated):
    # weak-close gap-up is a CAPITULATION-REVERSION edge. Base Atomic + 3 gates that ~2x it:
    #   vol=B (controlled) · price $21-89 (quality zone; $8-21 dead) · breadth risk-OFF (fear).
    # breadth = causal fraction of universe with +20d trailing return (computed inline here).
    d = df.sort_values(["ticker", "date"])
    _up = (d["close"] > d.groupby("ticker")["close"].shift(20)).astype(float)
    _br = _up.groupby(d["date"]).transform("mean").reindex(df.index)
    df["risk_off"] = (_br < 0.50).fillna(False)
    df["E_atomicR"] = (df["E_atomic"] & (df["vb"] == "B")
                       & df["close"].between(21, PRICE_CAP) & df["risk_off"])
    # 🥊 Engulf-Absorption (validated 2026-07-01, project_engulf_absorption): a bull-T that
    # RANGE-engulfs the prior 2 bars (outside bar) AND swallows a fresh Edge signal in them,
    # in the quality band (≥$21, RSI<45). +edge vs no-edge: +5.07/med+3.24/PF1.99 vs +1.18/med−0.41.
    # Washout- and G3-absorption are the reliable stars. ANY bull-T (incl T4/T6).
    _any_bt = df["t"].str.match(r"^T\d").fillna(False)
    _h2 = g["high"].transform(lambda s: s.shift(1).rolling(2).max())
    _l2 = g["low"].transform(lambda s: s.shift(1).rolling(2).min())
    _eng2 = (df["high"] >= _h2) & (df["low"] <= _l2)
    _E = ["E_l43triple", "E_z11t11", "E_washout", "E_dl1", "E_g3", "E_atomic",
          "E_h1bottom", "E_spring", "E_p55", "E_parabola", "E_atomicR"]
    _ae = df[_E].any(axis=1).astype(float).groupby(df["ticker"])
    _edge_in2 = (_ae.shift(1).fillna(0) + _ae.shift(2).fillna(0)) > 0
    df["E_engulfabs"] = (_any_bt & base & _eng2 & (df["close"] >= 21)
                         & (df["rsi_14"] < 45) & _edge_in2)
    # GEM1 — T1 capitulation-bounce (validated 2026-07-01, project_capitulation_bounce): a SMALL
    # T1 (body < 0.5× the prior Z bar's body = a modest bounce off a big bear/capitulation),
    # moderate-oversold RSI 30-50, controlled vol=B. 6/6yr positive (+5..+9 each), TRAIN≈TEST
    # (era-independent), all 3 universes. med +5.43 / win 60 / PF 2.32 — the most robust edge found.
    _body = (df["close"] - df["open"]).abs()
    _ratio = _body / _body.groupby(df["ticker"]).shift(1).replace(0, np.nan)   # T body / prior body
    _prevZ = g["z"].shift(1).fillna("") != ""
    df["E_t1capbounce"] = ((df["t"] == "T1") & df["clean"] & _prevZ & (_ratio < 0.5)
                           & df["rsi_14"].between(30, 50) & (df["vb"] == "B"))
    # GEM2 — engulf-abs where the SWALLOWED (absorbed) bar carried L46 VSA (strong, era-tilted).
    _swL = g["l"].shift(1).fillna("").where(g["l"].shift(1).fillna("") != "", g["l"].shift(2).fillna(""))
    df["E_engulfL46"] = df["E_engulfabs"] & (_swL == "L46")
    # 💤 Z-Absorb-Turn (validated 2026-07-22, this session): the PRIOR bar is a deep-bear
    # Z5/Z11 carrying the wt_evr end-of-reaction + a RED L34 absorption (institutional soak
    # into a no-result down bar) — then THIS bar confirms the turn with T3 or T9. Entry here.
    #   n=142 · ps +5.54% · med +2.51% · win 57.7% · 5/6yr (2022 +5.99) · TRAIN+3.0 TEST+7.9
    #   ablation: the wt_evr+L34red anchor 3× the bare Z5|Z11→T3/T9 (+1.83→+5.54); exit-
    #   invariant (5 exit families +); bootstrap 95%CI [+2.02,+9.32]; z=+2.69. 1D-native
    #   (cross-TF echo weak — like GEM1). Z3/Z4 never co-occur with wt_evr+L34red so add nothing.
    _pz = g["z"].shift(1).fillna("")
    _pwtevr = g["wtevr"].shift(1).fillna(0)
    _pl = g["l"].shift(1).fillna("")
    _pclose = g["close"].shift(1)
    _popen = g["open"].shift(1)
    _prior_absorb = (_pz.isin(("Z5", "Z11")) & (_pwtevr == 1)
                     & (_pl == "L34") & (_pclose < _popen))
    df["E_zabsorb"] = base & _prior_absorb & df["t"].isin(("T3", "T9"))
    # 🔥 Engulf-Abs-Lⁿ (validated 2026-07-07): Engulf-Abs whose RANGE sweeps up ≥2 high-volume
    # L46/L34 VSA bars in the last 21 sessions — the bar absorbed the recent volume distribution
    # in one move. Counter-intuitive but plateau- & 2×-slip-robust: heavy overhead L IMPROVES the
    # TIGHT engulf (opposite of loose-Goga where L-cluster mutes). L>=2: +3.64/med+1.93/PF1.64/5-6yr
    # vs base +3.04/PF1.52; plateau L2-L3 (L4 thins out); 2×-slip holds +3.14/PF1.53. Whole-range
    # absorption = strength, not resistance. (validate_goga_edge.py)
    df["_isL_tmp"] = df["l"].isin(("L46", "L34")).astype(float)
    _lo = df["low"].to_numpy(float); _hi = df["high"].to_numpy(float)
    _swLn = np.zeros(len(df))
    for _k in range(1, 22):                      # 21-bar overhead window
        _po = g["open"].shift(_k).to_numpy(float); _pcl = g["close"].shift(_k).to_numpy(float)
        _fl = g["_isL_tmp"].shift(_k).to_numpy(float)
        _sw = ((_po >= _lo) & (_po <= _hi)) | ((_pcl >= _lo) & (_pcl <= _hi))
        _swLn += (_sw & ~np.isnan(_po) & ~np.isnan(_pcl) & (_fl == 1))
    df.drop(columns=["_isL_tmp"], inplace=True)
    df["engabs_swLn"] = _swLn
    df["E_engulfabs_Lheavy"] = df["E_engulfabs"] & (_swLn >= 2)
    # FailedBear-Turn (2026-07-21, USER's chart observation → failbear_final_validation):
    # >=3 bear-pressure bars in 7 (down-break events / D-signals / bear-Z chain) where the
    # price HELD (close >= 95% of 7 bars ago) → T1G turn bar, in the MODERATE-oversold
    # zone (CCI<-100 & RSI 30-45). +2.77%/med+1.99/win55/PF1.60/6-6yr/z=+3.8σ,
    # TRAIN +2.78 ≈ TEST +2.76 — the best era balance of the 2026-07 additions.
    # Level structure is the key: deep oversold (RSI<30) is a KNIFE (Δ−3.3), high is dead.
    # Plateau-proven (W5/7/10, HELD 93/95, P3/P4). ~165 fires/yr.
    _bearz = df["z"].isin(("Z2", "Z2G", "Z4", "Z6", "Z10", "Z12"))
    _press = (df["pressev"] == 1) | _bearz
    df["_pr_tmp"] = _press.astype(float)
    _prcnt = g["_pr_tmp"].transform(lambda s: s.shift(1).rolling(7, min_periods=1).sum()).fillna(0)
    _held = (df["close"] >= g["close"].shift(7) * 0.95)
    df["E_failbear"] = (_prcnt >= 3) & _held & (df["t"] == "T1G")                        & (df["cci20"] < -100) & df["rsi_14"].between(30, 45)
    df.drop(columns=["_pr_tmp"], inplace=True)
    # SC-chain family (2026-07-21, sc_chains_bobxbe.py — Wyckoff textbook grammar in
    # consecutive-bar form; TEST-era-tilted like the G3 family, TRAIN mildly positive):
    #   ND→SC→L46  +7.10%/med+4.36/win58/PF2.52/z+6.1σ, $21-89 +8.9  (premium, ~68/yr)
    #   NS→SC      +3.55%/PF1.61, TRAIN +1.68 (best era balance of the family)
    #   G3→L46     +2.67%/PF1.47, n=31.6k, z=+16.7σ (the big-n workhorse)
    _l46bar = df["l"] == "L46"
    _sc_p1 = g["w2sc"].shift(1).fillna(0)
    _nd_p2 = g["ndv"].shift(2).fillna(0)
    _ns_p1 = g["nsv"].shift(1).fillna(0)
    df["E_ndscl46"] = (_nd_p2 == 1) & (_sc_p1 == 1) & _l46bar
    df["E_nssc"] = (_ns_p1 == 1) & (df["w2sc"] == 1)
    df["E_g3l46"] = g["gap"].shift(1).eq("G3") & _l46bar
    # L34camp→REV (2026-07-21, l34_campaign_entries.py): an all-red L34 CAMPAIGN at one
    # price level (current red L34 + a prior red L34 within 20 bars, closes within 5%)
    # RESOLVED by the strict REV-turn within <=3 bars — entry ON the turn bar.
    # +3.26%/med+2.27/PF1.62/5-6yr · TRAIN +3.94 / TEST +2.62 (era-balanced, 2021 +3.3
    # and 2022 +3.6 both positive) · ~78 fires/yr. The campaign alone (no turn) is NOT
    # tradeable (+0.4..+1.4, TRAIN-negative) — institution visits mark the zone, the
    # turn is the trigger. BO↑-gate entry tested and REJECTED (matched −2.5pp).
    _redl34 = (df["l"] == "L34") & (df["close"] < df["open"])
    df["_rl_tmp"] = _redl34.astype(float)
    df["_rlc_tmp"] = df["close"].where(_redl34)
    _prior_rl = g["_rl_tmp"].transform(lambda s: s.shift(1).rolling(20, min_periods=1).max()).fillna(0)
    _last_rlc = g["_rlc_tmp"].transform(lambda s: s.shift(1).ffill())
    _camp = _redl34 & (_prior_rl == 1) & ((df["close"] / _last_rlc - 1).abs() <= 0.05)
    df["_camp_tmp"] = _camp.astype(float)
    _m5 = g["rsi_14"].transform(lambda s: s.rolling(5, min_periods=2).min().shift(1))
    _rev = (_m5 < 38) & df["rsi_14"].between(30, 55) & (df["close"] > df["pc"])            & (df["rsi_14"] > g["rsi_14"].shift(1)) & (df["betas"] <= 13)
    _c1 = g["_camp_tmp"].shift(1).fillna(0)
    _c2 = g["_camp_tmp"].shift(2).fillna(0)
    _c3 = g["_camp_tmp"].shift(3).fillna(0)
    df["E_l34camp_rev"] = _rev & ((_c1 == 1) | (_c2 == 1) | (_c3 == 1))
    df.drop(columns=["_rl_tmp", "_rlc_tmp", "_camp_tmp"], inplace=True)
    # G3+RL / G3→G3 gap-chain family (2026-07-21, grand confluence → g3rl_gapchain_validation):
    #   G3+RL       +4.63%/med+2.47/win56/PF1.99/6-6yr/z=+16σ, TR+1.94, all price zones +4-5,
    #               2022 +0.8 (bear-year positive).
    #   G3→G3       back-to-back large gaps (the campaign/ignition grammar) +3.94%/PF1.72/z=29σ;
    #               served WITH the vol-spike veto (V10/V20 on either bar kills it — the ⛔
    #               vol-adjacency law, 6/6yr− on the toxic side) → 6/6yr.
    #   G3→G3→RL    the premium tier: +8.35%/med+7.17/win64/PF3.09, TR+2.19.
    # Plateau verified: G2+RL collapses (gap size matters), RL→G3 weaker (order matters),
    # loose 3-bar adjacency weaker (strictness matters).
    _g3bar = df["gap"] == "G3"
    _g3p1 = g["gap"].shift(1).eq("G3")
    _g3p2 = g["gap"].shift(2).eq("G3")
    _vspk_p1 = g["vspk"].shift(1).fillna(0).astype(int)
    df["E_g3rl"] = _g3bar & (df["rlv"] == 1)
    df["E_g3g3"] = _g3p1 & _g3bar & (df["vspk"] == 0) & (_vspk_p1 == 0)
    df["E_g3g3rl"] = _g3p2 & _g3p1 & (df["rlv"] == 1)
    # ppr×NS (2026-07-21, confluence sweeps → magnet_val.py): prebreak PP/RTV component
    # + VABS no-supply on the same bar. Path-sim +2.21%/PF1.41/+6.4σ vs random, 5/6yr,
    # both quality price zones positive — but REGIME-TILTED (TRAIN +0.09 / TEST +3.94):
    # a 2024-26-era edge, NOT era-independent like GEM1. Label carries the warning.
    df["E_ppr_ns"] = (df["ppr"] == 1) & (df["nsv"] == 1)
    # 🔁 Zone-Retest (validated 2026-07-07, project_zone_retest): buy the RETEST (2nd+ touch) of a
    # support zone, NOT the first drop (a knife: first-touch median −1.80 vs retest −0.45). Support
    # = causal 25-bar low (shift 3); touch = low within +3% & ≥−10% & CLOSE held above & green bar;
    # retest = touch with ≥1 prior touch in the last 15 bars. Base +0.99/PF1.15/win48/4-6yr. The
    # E-tier (retest whose range swallows a validated EDGE signal in the last 10 bars) = +1.66/
    # PF1.24. reclaim(pierce-below) & RSI<40 & L-absorb all HURT — plain touch-and-hold is best.
    _rl = g["low"].transform(lambda s: s.rolling(25, min_periods=15).min().shift(3))
    df["zr_ref_low"] = _rl
    _touch = (df["low"] <= _rl * 1.03) & (df["low"] >= _rl * 0.90)
    df["_touch_tmp"] = _touch.astype(float)
    _prior = g["_touch_tmp"].transform(lambda s: s.shift(1).rolling(15, min_periods=1).sum()).fillna(0)
    df["zr_prior_touch"] = _prior
    _zr_entry = _touch & (df["close"] >= _rl) & (df["close"] > df["open"]) & _rl.notna()
    df["E_zoneretest"] = _zr_entry & (_prior >= 1)
    # E-absorb tier: the retest range sweeps up a validated EDGE-signal bar in the last 10 sessions
    df["_anyE_tmp"] = df[["E_l43triple", "E_z11t11", "E_washout", "E_dl1", "E_g3", "E_atomic",
                          "E_h1bottom", "E_spring", "E_p55", "E_parabola", "E_atomicR",
                          "E_t1capbounce"]].any(axis=1).astype(float)
    _swE = np.zeros(len(df))
    for _k in range(1, 11):                       # 10-bar absorption window
        _po = g["open"].shift(_k).to_numpy(float); _pcl = g["close"].shift(_k).to_numpy(float)
        _fe = g["_anyE_tmp"].shift(_k).to_numpy(float)
        _sw = ((_po >= _lo) & (_po <= _hi)) | ((_pcl >= _lo) & (_pcl <= _hi))
        _swE += (_sw & ~np.isnan(_po) & ~np.isnan(_pcl) & (_fe == 1))
    df.drop(columns=["_touch_tmp", "_anyE_tmp"], inplace=True)
    df["zr_swE"] = _swE
    df["E_zoneretest_E"] = df["E_zoneretest"] & (_swE >= 1)
    # 📉 DiT tier (validated 2026-07-07): retest inside a DIP-IN-TREND EMA geometry e50>e20>e200
    # (short-term pulled back below the medium-term = a real dip, but still above the long-term =
    # primary uptrend intact). The single best STATE filter found: retest & DiT = +2.06/med+0.73/
    # PF1.36 vs base retest +1.36/med−0.17/PF1.21 (universe, trail25). DiT beats golden-cross
    # (e50>e200 alone, +1.77) AND the full stack (e20>e50>e200, +1.05 — that's extension not dip).
    _e20 = g["close"].transform(lambda s: s.ewm(span=20, adjust=False).mean())
    _e50 = g["close"].transform(lambda s: s.ewm(span=50, adjust=False).mean())
    _e200 = g["close"].transform(lambda s: s.ewm(span=200, adjust=False).mean())
    df["dip_in_trend"] = (_e50 > _e20) & (_e20 > _e200)
    df["E_zoneretest_dit"] = df["E_zoneretest"] & df["dip_in_trend"]
    # 🧗 High-Base 15m-Dip (validated 2026-07-08, project_highbase_15m_dip): a strong name in a
    # HIGH base whose intraday dip is deep on 15m while the daily stays calm — fills the RGTI-2025
    # coverage gap (uptrend re-accumulation never gets daily-oversold, so the whole board is
    # silent). ctx = close>e200 · RSI_1d 40-60 · close≥85% of 20d-high · green; trigger = the
    # day's MIN 15m RSI ≤ 28 (from the m15_dayrsi cache). +1.84/med+0.27/PF1.31/5-6yr vs
    # random-same-size +1.37±0.08 → 6.0σ. Threshold plateau 25-28 (22 declines). Modest tier
    # (Zone-Retest-E class), unique niche: the board's first HIGH-BASE setup.
    _m15 = _load_m15rsi()
    if len(_m15):
        _key = (df["ticker"] + "|" + df["date"].astype(str).str[:10]).to_numpy()
        df["m15rsi"] = pd.Series(_key).map(_m15).to_numpy()
    else:
        df["m15rsi"] = np.nan
    _hi20hb = g["high"].transform(lambda s: s.shift(1).rolling(20).max())
    df["E_highbase15"] = ((df["close"] > _e200) & df["rsi_14"].between(40, 60)
                          & (df["close"] >= 0.85 * _hi20hb)
                          & (df["close"] > df["open"]) & (df["m15rsi"] <= 28))
    # 🏗️ RTB-Base Oversold (validated 2026-07-09): the ONLY RTB signal that survives path-sim.
    # RTB's whole thesis (buy the pre-breakout C / breakout D) is anti-predictive — phases rank
    # BACKWARDS (A/B > C > D, med −1.66) and rtb_total is monotonically anti-predictive. But the
    # EARLY phases (A=accumulation build, B=turn) + oversold IS a real modest edge: it lifts plain
    # RSI<35 from med −0.87 to +0.48, win 48→51, PF 1.22→1.29, 4→5/6yr (2021-22 stop bleeding),
    # +3.2σ vs random-same-size-from-RSI<35 pool, TRAIN +1.61 ≈ TEST +1.88 (OOS-holds). Zone-Retest-E
    # tier — modest, median-positive. RTB used here as a STATE gate, not a score. (inline validation)
    df["E_rtb_base"] = (df["clean"] & df["rtb_ph"].isin(("A", "B")) & (df["rsi_14"] < 35))
    # 🎯 QZ-Capit-Reversal (validated 2026-07-11): born from an LLY chart observation, universe-
    # dissolved (1H-L46 ubiquitous, opens-T1G negative), then RESCUED by price-bucketing. In the
    # QUALITY zone $21-89 (pooled median was −0.9 = cheap-stock lottery), a daily-Z oversold pullback
    # to a FRESH 15d-low whose intraday 1H shows a Z2G/Z1G CAPITULATION + a REVERSAL-CLUSTER signal.
    # STATE (oversold+freshlow+$21-89) carries the edge; the 1H reversal signal flips median −1.32→+0.73
    # (with-signal +1.97/med+0.73/win52 vs no-signal +0.76/med−1.32/win46), 5-6yr. NB: T1G alone was a
    # mild overfit (+2.24, z+0.6 vs random) — the cluster {T1G,T5,T11,Z11,Z1G} is ~interchangeable, 5×
    # broader & more robust (why_t1g.py / cluster.py). "any bull-T" is too broad (+1.67). STATE>SHAPE.
    _hcap, _hrev = _load_qzcap()
    _keys = (df["ticker"].astype(str) + "|" + dstr).to_numpy()
    df["hcap"] = [k in _hcap for k in _keys]
    df["hrev"] = [k in _hrev for k in _keys]
    _lo15 = g["low"].transform(lambda s: s.rolling(15).min())
    df["freshlow15"] = df["low"] <= _lo15 * 1.02
    # Core (price-free) conditions, then two price variants: the BASE keeps $21-89 (widening it
    # costs a year: 5/6→4/6), while the RS/dwell-gated variants use $21-377 — those gates filter
    # out exactly the high-priced cases that hurt the raw setup (2026-07-26 cap sweep).
    _qzc_core = (df["clean"] & df["hcap"] & df["hrev"] & df["freshlow15"]
                 & (df["close"] >= 21) & (df["rsi_14"] < 45)
                 & df["z"].astype(str).str.startswith("Z"))
    df["E_qzcapit"]     = _qzc_core & (df["close"] <= PRICE_CAP)
    df["_qzcapit_wide"] = _qzc_core & (df["close"] <= PRICE_CAP_WIDE)
    # 🌀 SC-SUPER variants (2026-07-03, project_wyckoff_range_super): the setup fires within ±5%
    # of the Wyckoff range support (SC floor). Validated median-lifting tier (band-plateau +
    # 2×-slip-safe) for these 6 — a "more consistent / lower tail-risk" version, not higher mean.
    _sc = ((df["vtr"] == 1) & (df["wt_res"] > df["wt_sup"]) & (df["wt_sup"] > 0)
           & ((df["close"] / df["wt_sup"].replace(0, np.nan) - 1).abs() <= 0.05)).fillna(False)
    df["_sc"] = _sc
    df["E_t1capbounce_SC"] = df["E_t1capbounce"] & _sc
    df["E_dl1_SC"]         = df["E_dl1"] & _sc
    df["E_spring_SC"]      = df["E_spring"] & _sc
    df["E_atomic_SC"]      = df["E_atomic"] & _sc
    df["E_h1bottom_SC"]    = df["E_h1bottom"] & _sc
    df["E_washout_SC"]     = df["E_washout"] & _sc
    # 🎯 Confluence / Cluster-Bottom (validated 2026-07-12, the user's hypothesis): a REAL bottom is
    # marked not by one edge but by SEVERAL distinct edge FAMILIES firing inside a tight window. Count
    # the distinct families fired in the trailing 10 bars; forward edge rises MONOTONICALLY with the
    # count and — unlike any single family — survives 2022 AND cluster-dedup. $21-89, trail25, 6yr
    # (dedup, independent trades): ≥2 fam +2.51/med+1.02/win53/PF1.45/6-6yr · ≥3 +3.28/med+1.66/win54/
    # 5-6yr · ≥4 +4.47/med+3.05/win58/PF1.92/6-6yr. NOT an oversold proxy (≥4&RSI<50 ≈ ≥4 any-RSI).
    # Families are DE-DUPLICATED (Zone-Retest's 3 variants = 1 family; capit groups 5 reversal setups)
    # so variants can't double-count. Entry only on a family-event bar (conf_anyfam) with trailing
    # density ≥ tier. This is what "how many edges are in the green accumulation box" measures. cf conf.py
    _FAMILIES = {
        "capit":  ["E_qzcapit", "E_washout", "E_dl1", "E_t1capbounce", "E_h1bottom"],
        "retest": ["E_zoneretest"],
        "spring": ["E_spring"],
        "gap":    ["E_g3"],
        "atomic": ["E_atomic"],
        "oseq":   ["E_z11t11"],
        "l43":    ["E_l43triple"],
        "engulf": ["E_engulfabs"],
    }
    gg = df.groupby("ticker", sort=False)
    _conf = np.zeros(len(df))
    _anyfam = np.zeros(len(df), dtype=bool)
    for _fam, _cols in _FAMILIES.items():
        _cols = [c for c in _cols if c in df.columns]
        if not _cols:
            continue
        _hit = df[_cols].fillna(False).any(axis=1)
        _anyfam |= _hit.to_numpy()
        df["_famhit_tmp"] = _hit.astype(float)
        _recent = gg["_famhit_tmp"].transform(
            lambda s: s.rolling(10, min_periods=1).max()).fillna(0).to_numpy() >= 1
        df["cf_" + _fam] = _recent           # did this family fire in the trailing 10 bars
        _conf += _recent.astype(float)
    df.drop(columns=["_famhit_tmp"], inplace=True)
    df["conf_n"] = _conf.astype(int)
    df["conf_anyfam"] = _anyfam
    # Gate extended $21-89 → $21+ (2026-07-13): the $89+ bucket ladder holds — ×3 +3.77/med+2.42/
    # win57 5-5yr (2022 +4.3!) · ×5 +5.90 (2022 +7.2) · ×6+ med+6.66 win68 — confluence is the one
    # place the Fib price-zone law bends: single signals at $89+ are weak, but 3-4 independent edges
    # stacking on a quality large-cap is rare & informative (AMD Feb-Mar'26 ×4 @$195 → +50% was
    # being excluded). <$21 stays out (lottery). See lad89.py.
    df["E_confluence"]    = _anyfam & (df["conf_n"] >= 3) & (df["close"] >= 21) & df["clean"]
    df["E_confluence_p"]  = _anyfam & (df["conf_n"] >= 4) & (df["close"] >= 21) & df["clean"]
    df["E_confluence_hi"] = _anyfam & (df["conf_n"] >= 3) & (df["close"] >= 89) & df["clean"]
    # 🏆 Relative-Strength flag (validated 2026-07-13, rs_test.py/rs_sec.py, see main._rs_flags for
    # the live-scan twin): rs = close/benchmark (sector ETF, SPY fallback) above its own EMA200 =
    # a QUALITY name in a temporary dip vs a structural-laggard knife (OKLO vs CAG). RS-intact
    # turns Cluster/QZ-Capit/G3-Abs 2022-POSITIVE; the un-gated halves bleed in 2021-22.
    # NB the ETF parquet starts 2021-07 → +120-bar warmup means RS-gated masks fire from ~2022.
    _rs_flag = np.zeros(len(df), dtype=bool)
    _rs_spy_flag = np.zeros(len(df), dtype=bool)
    try:
        _px, _smap = _load_rs_ref()
        if _px is not None:
            _S2E = {"Technology": "XLK", "Healthcare": "XLV", "Financials": "XLF",
                    "Industrials": "XLI", "Materials": "XLB", "Consumer Discretionary": "XLY",
                    "Consumer Staples": "XLP", "Energy": "XLE", "Utilities": "XLU",
                    "Communication Services": "XLC", "Real Estate": "XLRE"}
            def _intact(_c, _b):
                with np.errstate(invalid="ignore", divide="ignore"):
                    _rs = _c / _b
                _e = pd.Series(_rs).ewm(span=200, adjust=False, min_periods=120).mean().to_numpy()
                return (_rs > _e) & ~np.isnan(_e)
            for _tk, _gg in df.groupby("ticker", sort=False):
                _ds = _gg["date"].astype(str).str[:10]
                _c = _gg["close"].to_numpy(float)
                _bspy = _ds.map(_px["SPY"]).to_numpy(float)
                _rs_spy_flag[_gg.index] = _intact(_c, _bspy)
                _et = _S2E.get(_smap.get(_tk, ""), None)
                if _et and _et in _px.columns:
                    _rs_flag[_gg.index] = _intact(_c, _ds.map(_px[_et]).to_numpy(float))
                else:
                    _rs_flag[_gg.index] = _rs_spy_flag[_gg.index]   # no sector → SPY fallback
    except Exception:
        pass                                   # no RS data → gated masks simply stay empty
    df["rs_intact"] = _rs_flag                 # sector-RS (SPY fallback) — the primary gate
    df["rs_spy_intact"] = _rs_spy_flag         # SPY-RS — for the 💪sec-lead split in the scanner

    # ── 🥇 SECTOR-LAG: the second RS level (2026-08-06, user's own hypothesis) ──────────
    # rs_intact says the STOCK is strong vs its sector. This says the SECTOR is weak vs SPY
    # (20d relative change < −1%). Together = "leader inside a laggard group" — the one
    # configuration the book had never tested, and the strongest result of the macro study.
    # Four-quadrant gradient on the pooled reversal family (n=217k, ATR exit):
    #   stock strong × sector LAGGING +3.61 6/6yr worst +1.29   <- this cell
    #   stock strong × sector leading +2.38 6/6yr worst +0.39
    #   stock weak   × sector lagging +1.60 4/6yr worst −3.09
    #   stock weak   × sector leading +0.91 4/6yr worst −4.08
    # Per-edge (the deciding test): median lift 7/7 edges, SR lift 7/7, and DSR crosses from
    # 0.000 to 0.88–0.95 on G3 / G3-Abs / L43. ⚠ NOT universal: it degrades D+L1 (3/5yr).
    # NB the broad-index version is the OPPOSITE (nasdaq names do better when QQQ LEADS) —
    # the sector ETF is the meaningful benchmark, not the index.
    _sec_lag = np.zeros(len(df), dtype=bool)
    _vix_up = np.zeros(len(df), dtype=bool)
    _S2E_L = {"Technology": "XLK", "Healthcare": "XLV", "Financials": "XLF",
              "Industrials": "XLI", "Materials": "XLB", "Consumer Discretionary": "XLY",
              "Consumer Staples": "XLP", "Energy": "XLE", "Utilities": "XLU",
              "Communication Services": "XLC", "Real Estate": "XLRE"}
    try:
        _px, _smap = _load_rs_ref()
        if _px is not None:
            _spy = _px["SPY"]
            _rel20 = {et: (_px[et] / _spy).pct_change(20) for et in _px.columns if et != "SPY"}
            for _tk, _gg in df.groupby("ticker", sort=False):
                _et = _S2E_L.get(_smap.get(_tk, ""), None)
                if _et and _et in _rel20:
                    _v = _gg["date"].astype(str).str[:10].map(_rel20[_et]).to_numpy(float)
                    _sec_lag[_gg.index] = _v < -0.01
    except Exception:
        pass
    df["sector_lag"] = _sec_lag
    df["lead_in_lag"] = _rs_flag & _sec_lag    # the composite the study validated

    # 🌡️ MACRO VIX-UP (same study): VIXY 5d change > +3% AND NOT a vspike day. 210 sessions —
    # the 68% of rising-VIX days the existing vspike gate does NOT cover. It does NOT move
    # Sharpe (DSR lift 0/7) but it converts 5/6 -> 6/6 positive years with a POSITIVE worst
    # year on QZC (−0.09→+0.59), G3 (−0.45→+1.57), WSH (−2.79→+0.89), G3-Abs (−0.92→+1.77).
    # Stabiliser, not amplifier → report-only in the brain, never a size multiplier.
    # ⚠ it HURTS L43 (worst +2.27 → −5.71) — do not apply blindly.
    try:
        _vx = _load_vix_ref()
        if _vx is not None:
            _c5 = _vx.pct_change(5)
            _sp = (_vx.pct_change() > 0.05) | (
                _vx.rolling(252).apply(lambda x: (x[-1] > x[:-1]).mean(), raw=True) > 0.80)
            _flag = ((_c5 > 0.03) & ~_sp.fillna(False))
            _vix_up = df["date"].astype(str).str[:10].map(_flag).fillna(False).to_numpy(bool)
    except Exception:
        pass
    df["macro_vix_up"] = _vix_up
    # 🧊 Coil-Floor Absorption (validated 2026-07-23, project_coil_floor_absorption): born from AMD's
    # classic accumulations. A daily-Z absorption at the FLOOR of a HELD compressed base — prior-25-bar
    # range ≤35% (a coil, not a trending drop) AND this bar's low within 6% of the 25-bar base low (at
    # the floor, not mid-range) — with rs_intact + rsi<40. The held-base STRUCTURE is orthogonal to
    # QZ-Capit's fresh-15-low & Washout's VIX-panic (16% / 0% overlap): the portion DISJOINT from the
    # capit family is the strongest cohort — med +3.35/mean+4.86/win58/5-5yr incl 2022 +2.1, TR+3.4/
    # TE+3.2. PBO 0.014 (92% OOS-retention). floor-entry ≫ breakout-exit; deep-shakeout base HURTS
    # (base must HOLD). STATE>SHAPE: it's structure(held coil)+location(floor)+state(RS,oversold), NOT
    # the signal identity (T*L46/L34 levels & red/Z absorption alone = random ~54%). coilfloor.py
    _ABSZ = ("Z1", "Z1G", "Z2", "Z2G", "Z5", "Z10", "Z11")
    _cf_hi = g["high"].transform(lambda s: s.rolling(25, min_periods=25).max().shift(1)).to_numpy(float)
    _cf_lo = g["low"].transform(lambda s: s.rolling(25, min_periods=25).min().shift(1)).to_numpy(float)
    with np.errstate(invalid="ignore", divide="ignore"):
        _cf_rng = (_cf_hi - _cf_lo) / _cf_lo                       # base compression (prior 25 bars)
        _cf_floor = (df["low"].to_numpy(float) - _cf_lo) / _cf_lo  # this bar's low vs the base low
    df["E_coilfloor"] = (
        df["z"].astype(str).isin(_ABSZ)
        & (df["rsi_14"] < 40) & (df["rsi_14"] > 0)
        & df["rs_intact"]
        & (_cf_rng <= 0.35) & (_cf_floor <= 0.06)
    ).fillna(False)
    # 🌀 Engulf-Absorb-Reversal (WATCH-tier, 2026-07-23, project_bottom_anatomy_mtf): a bull-engulf
    # (T4/T6) whose INTRADAY 15m is still Z-DOMINANT (T/(Z+T)<0.5 = absorption, NOT clean demand) =
    # a genuine reversal-FROM-absorption, on a quality name (rs_intact) in a base (rsi<60, prior-10d
    # <+8%), $21-89. The counter-intuitive tell: clean-demand intraday is WORSE (extended); Z-dominant
    # is better (buy absorbed weakness, engulf-version). 100% ORTHOGONAL to coilfloor/engulfabs/capit
    # (0% overlap). Overfit-pass (DSR~1.0, PBO 0.13, 81% OOS-retention) but MODEST: +0.29 med/win51/
    # TEST+1.5, small-n (~2.6k), 2022/2024 negative → WATCH/display, NOT auto-buy. cf 15m-Z-dominant.
    _zdom = _load_m15_zdom()
    _kz = (df["ticker"].astype(str) + "|" + dstr).to_numpy()
    df["m15_zdom"] = [k in _zdom for k in _kz]
    # 🔎 intraday VSA-line presence (2026-07-26) — the MTF veto layer, see _load_intraday_lines
    _g34, _a34, _r46, _vsp, _dry = _load_intraday_lines()
    df["iv_l34g"] = [k in _g34 for k in _kz]     # green L34 (demand) printed intraday
    df["iv_l34"]  = [k in _a34 for k in _kz]     # any L34 intraday
    df["iv_l46r"] = [k in _r46 for k in _kz]     # red L46 (supply worked) intraday
    # 💥 intraday VOLUME EVENT: the session's biggest 15m bar is ≥4× that session's own average.
    # The single most UNIVERSAL filter found (2026-07-26, volume_deep.py) — unlike RS/dissonance
    # (edge-specific), it improves EVERY edge tested and its absence is a loss cell everywhere:
    #   raw daily L46 by intraday spike: <2.5× med −5.85/win39/pf0.90 → 2.5-4× −1.31 → 4-6× −0.10
    #   → 6×+ med +0.36/win51/pf1.22 (monotone ladder). Same shape on L34 (<2.5× → pf 0.81, 3/6yr).
    # On built edges (spike<2.5 vs ≥4): washout med −0.59→+0.42 & worst −2.2→−0.0 · t3_rs_dip
    # +1.66→+2.02/worst +0.9 · coilfloor +2.82→+3.16/worst +1.8 · qzcapit_dwell +1.15→+1.45.
    # Reads: "was there a REAL volume event inside the day, or just a flat session?"
    df["iv_vspike"] = [k in _vsp for k in _kz]     # ≥4× (positive gate)
    df["iv_dry"]    = [k in _dry for k in _kz]     # <2.5× = the severe veto cell (~3% of days)
    _prior10 = g["close"].transform(lambda s: s / s.shift(10) - 1)
    df["E_engulf_absorb_rev"] = (
        df["t"].isin(["T4", "T6"]) & df["m15_zdom"] & df["rs_intact"]
        & (df["rsi_14"] < 60) & (df["rsi_14"] > 0) & (_prior10 < 0.08)
        & df["close"].between(21, PRICE_CAP)
    ).fillna(False)
    # ⚡ G3-Abs — the "contradiction bar" (validated 2026-07-13, user's AMD-Mar-2 chart obs, contra.py):
    # an aggressive G3 gap-up that closes WEAK (Atomic O-close) on the SAME bar = the gap got
    # absorbed — buyers attacked, sellers unloaded all day, price held anyway. Synergy is real:
    # G3∧Atomic +4.24/med+2.33/win56/PF1.83/z+11.9/5-6yr (n=11253, $21+) beats G3 alone (+3.10)
    # and dwarfs Atomic alone (+1.60/med+0.13, z−13). Inside a conf≥3 cluster → +4.52/PF1.93.
    # $21-89 +4.98/PF1.98. L43 adds nothing (absorption already in the O-close); SC-zone dilutes
    # the mean (G3 is zone-agnostic) but keeps the median. 2022 ~flat (−0.5), not a bear edge.
    df["E_g3abs"] = df["E_g3"] & df["E_atomic"] & (df["close"] >= 21)

    # ── 🥇 LEAD-in-LAG variants (2026-08-06, BUILT on the user's hypothesis) ────────────
    # parent & rs_intact & sector_lag. Per-edge validation vs the 112-setup board family
    # (ATR×12 exit, 75 pre-specified macro trials as the DSR denominator):
    #   G3    +3.34 5/6yr DSR 0.000  ->  +5.35 5/5yr worst +2.32 DSR 0.925
    #   G3A   +3.23 5/6yr DSR 0.000  ->  +5.47 5/5yr worst +2.70 DSR 0.884
    #   L43   +4.51 6/6yr DSR 0.017  ->  +6.36 5/5yr worst +2.27 DSR 0.947
    # Only these three crossed DSR 0.6; QZC/WSH improve but stay below (0.150–0.507) and are
    # left ungated. D+L1 DEGRADES under the gate — deliberately excluded.
    df["E_g3_lead"] = df["E_g3"] & df["lead_in_lag"]
    df["E_g3abs_lead"] = df["E_g3abs"] & df["lead_in_lag"]
    df["E_l43triple_lead"] = df["E_l43triple"] & df["lead_in_lag"]
    # 🔑 KEY-LEVEL flag (validated 2026-07-15, keylvl.py — the real core of the "smart-money
    # liquidity sweep" infographic): support = causal 25-bar low (shift 3); a level is a KEY
    # level if it was TESTED ≥2× in the last 25 bars (resting buy orders), vs a weak/incidental
    # level. Discriminates hold-vs-knife: Spring med +1.00 (key) vs −3.90 (weak, win43!); QZ-Capit
    # key → 6/6yr; D+L1 key med +1.31 vs −0.10. "Deep wick below support" HURTS (that's a real
    # breakdown, not a stop-hunt) — location quality > wick drama. STATE>SHAPE again.
    # LEVEL-ANCHORED count (must match keylvl.py): for bar i, how many of the PRIOR 25 bars' lows
    # sit at/below THIS bar's support level sup[i] (±1%). Not a plain rolling sum — each bar uses
    # its own level as the threshold — so we slide a 25-window of prior lows and compare to sup[i].
    from numpy.lib.stride_tricks import sliding_window_view as _swv
    _sup = g["low"].transform(lambda s: s.rolling(25, min_periods=15).min().shift(3)).to_numpy(float)
    _low = df["low"].to_numpy(float)
    _kt = np.zeros(len(df))
    for _tk, _idx in df.groupby("ticker", sort=False).indices.items():
        _idx = np.sort(_idx)
        lo_ = _low[_idx]; sp_ = _sup[_idx]; n_ = len(_idx)
        if n_ < 26:
            continue
        _w = _swv(lo_, 25)                       # rows k = lo_[k:k+25]; bar i uses window k=i-25
        _thr = sp_[25:] * 1.01                   # threshold = this bar's support, i=25..n-1
        _cnt = (_w[:len(_thr)] <= _thr[:, None]).sum(axis=1)
        _out = np.zeros(n_); _out[25:] = _cnt
        _kt[_idx] = _out
    df["key_touches"] = _kt
    df["key_level"] = df["key_touches"] >= 2
    # $21+ floor: the key-level lift was validated in the quality zone (keylvl.py); below it the
    # parent setups drag on cheap stocks. QZ-Capit already has its $21-89 gate.
    df["E_spring_key"]   = df["E_spring"] & df["key_level"] & (df["close"] >= 21)
    df["E_qzcapit_key"]  = df["E_qzcapit"] & df["key_level"]
    df["E_dl1_key"]      = df["E_dl1"] & df["key_level"] & (df["close"] >= 21)
    # 🏛️ BOS-up (Break of Structure, validated 2026-07-15, bos.py — from the Chart.Logic PAU
    # series' most-repeated concept): a downtrend swing-HIGH (2-2 fractal formed while close<EMA50)
    # broken by a daily close = structure shifted bullish. As a gate it's SELECTIVE: it CONFIRMS
    # deep-capitulation reversals (QZ-Capit + BOS≤6d: med+0.52→+1.98, win51→55, 5/6→6/6yr) but
    # HURTS bottom-tick setups (Spring/D+L1/G3-Abs) — by the time structure breaks, the exact low
    # is gone ("confirmation costs"). So gate ONLY QZ-Capit. Recent BOS within 6d.
    _bos_recent = np.zeros(len(df), dtype=bool)
    _e50f = g["close"].transform(lambda s: s.ewm(span=50, adjust=False).mean()).to_numpy(float)
    for _tk, _ix in df.groupby("ticker", sort=False).indices.items():
        _ix = np.sort(_ix)
        _h = df["high"].to_numpy()[_ix]; _c = df["close"].to_numpy()[_ix]; _e = _e50f[_ix]
        _n = len(_ix)
        if _n < 6:
            continue
        _bos = np.zeros(_n, dtype=bool)
        _last = np.nan; _last_dn = False
        for _i in range(_n):
            if _i >= 4 and _h[_i - 2] == max(_h[_i - 4:_i + 1]):   # 2-2 fractal high, confirmed at i
                _last = _h[_i - 2]; _last_dn = _c[_i - 2] < _e[_i - 2]
            if (not np.isnan(_last)) and _last_dn and _c[_i] > _last and _c[_i - 1] <= _last:
                _bos[_i] = True; _last = np.nan
        # BOS active if fired in the last 6 bars (inclusive)
        _rw = pd.Series(_bos.astype(float)).rolling(6, min_periods=1).max().to_numpy() >= 1
        _bos_recent[_ix] = _rw
    df["bos_up"] = _bos_recent
    df["E_qzcapit_bos"] = df["E_qzcapit"] & df["bos_up"]
    # 🧱 ORDER BLOCK retest (validated 2026-07-15, fvgob.py — Chart.Logic 'last down candle before
    # the move' = institutional absorption zone; price returns to retest it). Precomputed dayset
    # (data/ob_days.json, ticker→dates where price re-tapped a ≤8-bar-old bullish OB) to keep _prep
    # cheap — a simple membership map, no per-ticker loop. As a gate: STRONG on capitulation/absorption
    # edges — QZ-Capit +OB med −0.11→+4.46/win59/PF2.70/6-6yr, Cluster +2.16/PF1.92/6-6yr, D+L1 +2.16/
    # PF2.12/6-6yr — but HURTS bottom-tick (Spring/G3-Abs). Gate only QZ/Cluster/D+L1. (FVG tested
    # same day = no edge, not built.)
    _ob = _load_ob_days()
    _obkeys = (df["ticker"].astype(str) + "|" + dstr).to_numpy()
    df["ob_retest"] = np.array([k in _ob for k in _obkeys], dtype=bool)
    df["E_qzcapit_ob"]   = df["E_qzcapit"] & df["ob_retest"]
    df["E_confluence_ob"] = df["E_confluence"] & df["ob_retest"]
    df["E_dl1_ob"]       = df["E_dl1"] & df["ob_retest"] & (df["close"] >= 21)
    # 🏆 RS-gated variants — the separate-edge versions (user request 2026-07-13): each is its
    # parent setup restricted to RS-intact names. Validated split (episode-level, 2022-26):
    # Cluster≥3 intact +4.49/med+3.29/win58/PF1.96 5-5yr · QZ-Capit intact 2022 +1.0 (broken
    # 2021 −10.4) · G3-Abs intact +4.93/med+3.59/win59/PF2.06.
    df["E_confluence_rs"] = df["E_confluence"] & df["rs_intact"]
    df["E_qzcapit_rs"]    = df["_qzcapit_wide"] & df["rs_intact"]   # $21-377 (cap sweep 2026-07-26)
    df["E_g3abs_rs"]      = df["E_g3abs"] & df["rs_intact"]
    # 🔑 Gate-strengthened variants (validated 2026-07-22, systematic OB/RS/quality sweep across
    # every base setup — built ONLY where the gate is era-BALANCED, improves the worst year, and
    # lifts ps with adequate n; not a blanket application). Two clean patterns emerged:
    #   🧱OB (order-block retest) = the biggest AMPLIFIER — turns strong setups elite.
    #   🏆RS (relative-strength intact) = the best worst-year RESCUER — flips median-negative
    #        Tier-4 setups (Washout/Spring/D+L1/Z11-T11/Engulf-Abs) to 5/5yr all-positive.
    # 🧱OB amplifiers (already-strong bases → even stronger, worst-year held ≥0):
    df["E_rtb_base_ob"] = df["E_rtb_base"] & df["ob_retest"]        # +1.79→+7.33/med+5.06/6-6yr/worst+3.6
    df["E_failbear_ob"] = df["E_failbear"] & df["ob_retest"]        # +3.19→+8.38/med+4.27/6-6yr/worst+0.3
    df["E_g3g3rl_ob"]   = df["E_g3g3rl"] & df["ob_retest"]          # +8.70→+21.4/med+17.9/6-6yr/worst+5.5
    # 🏆RS rescuers (Tier-4 median-negative bases → 5/5yr all-positive, worst-year flipped +):
    df["E_rtb_base_rs"] = df["E_rtb_base"] & df["rs_intact"]        # +1.79→+4.25/med+3.61/5-5yr/worst+1.8
    df["E_z11t11_rs"]   = df["E_z11t11"] & df["rs_intact"]          # +7.04→+8.76/med+8.01/win75/5-5yr/worst+1.5
    df["E_washout_rs"]  = df["E_washout"] & df["rs_intact"]         # +1.67(med−0.5)→+4.33/med+3.32/5-5yr/worst+1.4
    df["E_dl1_rs"]      = df["E_dl1"] & df["rs_intact"]             # +1.92(med−0.1)→+3.98/med+2.12/5-5yr/worst+0.1
    df["E_spring_rs"]   = df["E_spring"] & df["rs_intact"]          # +1.06(med−1.1)→+4.25/med+2.40/5-5yr/worst+0.5
    df["E_engulfabs_rs"] = df["E_engulfabs"] & df["rs_intact"]      # +4.99(worst−4.1)→+7.05/med+5.72/5-5yr/worst+0.2
    df["E_l43triple_rs"] = df["E_l43triple"] & df["rs_intact"]      # +4.86→+6.41/med+5.86/win67/5-5yr/worst+1.9
    # 🏆💎 Atomic-RS-Q (2026-07-26 strengthening sweep): base Atomic (n61k) had NO gated variant and
    # was the weakest big edge (med −0.08, worst −1.5). RS-intact + $21-89 quality → med +0.78,
    # expR +0.10, pf 1.42, worst −1.5→−0.8 (still 5/6; 2022 −0.8 residual). Modest but real —
    # median flips positive, worst-year halved. Consolidation, not a new edge (subset of Atomic).
    df["E_atomic_rsq"] = df["E_atomic"] & df["rs_intact"] & df["close"].between(21, PRICE_CAP)
    # 🔵 DWELL booster (2026-07-26, AM-GM/cup study #2): "rounded bottom" = price DWELLS at the floor
    # for many days (absorption) vs a sharp V-spike. dwell = # of last-10 bars whose low is within
    # 3% of the trailing-20 low; ≥5 = a rounded/held base. DAILY-computed (no intraday infra), and
    # ORTHOGONAL to the intraday dissonance-confirm (Jaccard 0.29). Worst-year rescuer on 3 edges:
    # qzcapit 5/6 worst−0.8 → 6/6 worst+0.2 (n26k); dl1 4/6 worst−1.0 → 5/6 worst−0.1 (2021 +0.5,
    # where late-reclaim FAILED −3.2); l43 worst+0.9 → +1.9. Complements dissonance (different edges).
    _dw_min20 = g["low"].transform(lambda s: s.rolling(20).min())
    _dw_near = (df["low"] <= _dw_min20 * 1.03).astype(float)
    _dwell5 = (_dw_near.groupby(df["ticker"]).transform(lambda s: s.rolling(10).sum()) >= 5).fillna(False)
    df["E_qzcapit_dwell"]   = df["_qzcapit_wide"] & _dwell5  # $21-377: 6/6yr worst+0.1/med+1.15/n40k
    df["E_dl1_dwell"]       = df["E_dl1"] & _dwell5          # 5/6yr worst−0.1/2021+0.5 (dissonance couldn't)
    df["E_l43triple_dwell"] = df["E_l43triple"] & _dwell5    # 6/6yr worst+1.9/med+2.94
    # 🎯 T1-RS-Dip (2026-07-25 discovery): raw T1 demand bar is era-dependent noise (med −1.34),
    # but a T1 at RSI<45 with RS-intact in the $21-89 quality zone = a quality-dip in a strong name.
    # 5/5yr all-positive (incl 2022 +1.4), med +2.56, n1697, DSR 1.00, family PBO 0.41, plateau-robust
    # (RSI 35/40/45 all +). Cheap buckets fail (2021 −24) → quality zone is load-bearing.
    df["E_t1_rs_dip"] = ((df["t"] == "T1") & (df["rsi_14"] < 45) & df["rs_intact"]
                         & df["close"].between(21, PRICE_CAP))
    # 🎯 T3-RS-Dip (2026-07-25): T3 continuation-demand at RSI<45 with RS-intact in $21-89. Raw T3
    # is era-dependent noise (med −1.43); RS+quality → 6/6yr all-positive (incl 2021 +10.5, 2022 +0.1)
    # med +1.21, n3387, DSR 1.00, plateau-robust across 5 price buckets + RSI 40/50. family PBO 0.54
    # (near-dup-variant artifact, not overfit — the wide plateau + DSR are the real evidence).
    df["E_t3_rs_dip"] = ((df["t"] == "T3") & (df["rsi_14"] < 45) & df["rs_intact"]
                         & df["close"].between(21, PRICE_CAP_WIDE))
    # 🏆 L34→L34 continuity (2026-07-25, l34_validate.py): a T1 demand bar whose PRIOR bar was a
    # Z-absorption, with the SAME L34 VSA volume-line on BOTH bars ($21-89). The CONTINUITY is the
    # edge — T1-bar L34 alone = null (+0.59/med−0.91/4-6yr); requiring L34 on the absorption AND
    # the demand bar → mean +2.60/med +0.53/5-6yr with BOTH bear years positive (2021 +2.65,
    # 2022 +2.52), worst −0.5, DSR 0.84. NOT RSI-subsumed (beats RSI<45 state on every axis;
    # 2021 flips −2.4→+2.6). Price-bucket law holds (<8 & 8-21 dead, $21-89 only, 89+ weaker med).
    # L46/L25 never persist across Z→T1 (n0); L3→L3 fails (−0.92). Clean continuity-family PBO 0.5.
    _pz_l34 = g["z"].shift(1).fillna("")
    _pl_l34 = g["l"].shift(1).fillna("")
    df["E_l34cont"] = ((df["t"] == "T1") & (_pz_l34 != "") & (_pl_l34 == "L34")
                       & (df["l"] == "L34") & df["close"].between(21, PRICE_CAP_WIDE))
    # 🏆 +RS flagship: RS-intact → 5/5yr ALL-positive (worst +2.60), med +1.90, n112, DSR 0.93 —
    # the universal worst-year rescuer again ([[project_rs_gate]]).
    df["E_l34cont_rs"] = df["E_l34cont"] & df["rs_intact"]
    # 🟢 Zone-Retest × GREEN-L46 (2026-07-26, user's "a green L46 and a red L46 are two different
    # signals" insight). Zone-Retest was our weakest big edge (4/6yr, worst −1.7) and RS is a TRAP
    # on it (2021 −11..−22). The right gate turned out to be the L46 VSA volume-line: every ZRT bar
    # is already green (its definition needs an up bar), so ZRT∧L46 = green-L46 by construction.
    # ZRT+L46 → 6/6yr worst +0.1; +$21-89 → med +1.03/worst +0.6; +dwell (price hugging the 20d low
    # ≥5 of last 10 bars = a genuinely repeated retest) → med +1.32/expR +0.10/pf1.48/worst +0.9.
    # DSR 1.00 vs 35 swept conditioners, family PBO 0.243, plateau across all 4 T-codes (green L46
    # is always T5/T10/T11/T12) and both dwell thresholds. Discovered as "green L46 + dwell" which
    # was 94% overlapping ZRT (dwell at the 20d low IS a retest) → built as a ZRT gate, not a new edge.
    _l46g = (df["l"] == "L46") & (df["close"] > df["open"])
    _min20 = g["low"].transform(lambda s: s.rolling(20).min())
    _dwell5 = ((df["low"] <= _min20 * 1.03).astype(float)
               .groupby(df["ticker"]).transform(lambda s: s.rolling(10).sum()) >= 5).fillna(False)
    # PRICE GATE $21-377 (widened from $21-89 on 2026-07-26 after a Fibonacci-zone sweep — the
    # user's point that ">$89 was still fine" in the old price research). Per-trade quality keeps
    # IMPROVING with price (win% 53.3→54.6→55.2→55.2→56.7, catastrophe ≤−20% falls 10.8→9.3→6.6→
    # 7.0→6.7→7.6% across 21-34…233-377) — the 21-89 cap was leaving good trades on the table.
    # Widening to 377: n 6771→10689 (+58%), median +1.32→+1.35, catastrophe 8.8→8.2%, still 6/6yr
    # (worst +0.9→+0.7). **377+ is excluded deliberately**: on its own it is 3/6yr with 2021 −6.8,
    # and an uncapped gate drags worst-year down. Under $21 stays out (8-21: win ~45%, cat ~16%,
    # median −1.7..−1.9) — the cheap-lottery zone of [[project_fib_price_zones]].
    df["E_zrt_l46"]    = df["E_zoneretest"] & _l46g & df["close"].between(21, 377)
    df["E_zrt_l46_dw"] = df["E_zrt_l46"] & _dwell5
    # 🔎 intraday-demand CONFIRMED variants (2026-07-26). The veto is "no demand line printed
    # intraday" — validated as a genuine loss cell, not just a weak one:
    #   washout & NO 15m L34 → 1/6yr, med −5.28, pf 0.72   (removing it: worst −2.2 → −1.1)
    #   ZRT🟢 & NO 15m green-L34 → 3/6yr, worst −2.0        (removing it: worst +0.6 → +0.9)
    # Edge-SPECIFIC as always: qzcapit/coilfloor are unaffected (their no-L34 cell is fine), so
    # only the two validated ones are gated. Degrades safely — if the 15m map is empty the
    # iv_* columns are all False and these masks simply never fire (base edges untouched).
    df["E_washout_iv"]  = df["E_washout"] & df["iv_l34"]
    df["E_zrt_l46_iv"]  = df["E_zrt_l46"] & df["iv_l34g"]
    # 💥 Washout × intraday volume event — the most dramatic single application of iv_vspike:
    # med −0.59 → +0.42, win 48.6 → 50.9, pf 1.24 → 1.35, worst-year −2.2 → −0.0 (n19732).
    # Washout was our most "expensive" edge (buy panic, wide heat); requiring a real intraday
    # volume event separates a genuine climax from a slow drift down.
    df["E_washout_vs"]  = df["E_washout"] & df["iv_vspike"]
    # 🎬 Confirmed stopping-volume (2026-07-26). From the VSA-video test: of ALL 13 narrated
    # patterns built raw (no priors), this was one of only two that weren't coin-flips — and the
    # single-candle ones (absorption, no-supply, pin bar) all failed. A prior WIDE-RANGE DOWN bar
    # on EXTREME volume (selling climax) FOLLOWED by a GREEN bar on LOWER volume (demand confirms).
    # Raw shape = coin-flip (+0.05/4-6yr); it needs our STATE gate. +Q$21-89+RS → +2.31/med+0.73/
    # expR+0.09/pf1.37/4-5yr, +RSI<45 → +3.92/med+2.46/pf1.74. DSR 0.997; overlap with the
    # capitulation family only 26% (+Q+RS) — 74% NOVEL, disjoint part strong. 2021 sparse, 2022 soft.
    _svr = df["high"] - df["low"]
    _avg_rng_bp = _svr.groupby(df["ticker"]).transform(lambda s: s.rolling(20).mean().shift(2))
    _vmax_bp    = df["volume"].groupby(df["ticker"]).transform(lambda s: s.rolling(20).max().shift(2))
    _climax_p = ((g["close"].shift(1) < g["open"].shift(1))                       # prior bar red
                 & (_svr.groupby(df["ticker"]).shift(1) >= 1.5 * _avg_rng_bp)     # prior wide-range
                 & (g["volume"].shift(1) >= _vmax_bp))                            # prior extreme vol
    _confirm = (df["close"] > df["open"]) & (df["volume"] < g["volume"].shift(1)) # green, lower vol
    df["E_stopvol_confirm"] = (_climax_p & _confirm
                               & df["close"].between(21, PRICE_CAP_WIDE) & df["rs_intact"]).fillna(False)
    df["E_stopvol_confirm_deep"] = df["E_stopvol_confirm"] & (df["rsi_14"] < 45)
    # 💎 quality-price rescuer for Z-Absorb (OB variant n too thin + worst worsened; $21-89 is the
    # clean lift — improves era-balance and worst year −4.2→−2.6, per the booster study).
    df["E_zabsorb_q"]   = df["E_zabsorb"] & df["close"].between(21, PRICE_CAP)   # +5.75→+8.69/med+4.39/worst−2.6
    # 📐 OSCILLATOR DIVERGENCE × 🏆RS (validated 2026-07-28, rsi_divergence.py / div_validate.py
    # / beardiv.py). Came out of reviewing two "RSI trendline breakout" Pine scripts: their
    # breakout thesis is null (the slope is worth +0.09pp over a FLAT line), but the divergence
    # block one of them had — broken and switched off — points the right way.
    #
    # BULL: a confirmed price pivot low that is LOWER than the prior one while RSI/CCI makes a
    # HIGHER low = selling pressure stopped producing result = absorbed weakness.
    # Alone it is WORSE than its own opposite cell (div −0.64 vs "RSI confirms the low" −0.43)
    # and deeper oversold makes it WORSE (rsi<30 → −2.26): naked divergence catches knives.
    # 🏆RS flips the sign — it removes the structural knives and leaves quality dips:
    #   rsi<35 +4.14 · <40 +3.34 · <45 +2.58 · <50 +1.94 · <55 +1.42  — monotone, 5/5yr at four
    #   cuts, and beats BOTH matched controls (conf+RS +1.48, plain+RS +1.48) by ~1.1pp.
    #   TRAIN 2022-23 +2.84 ≈ TEST 2024-26 +2.26 (no era tilt) · DSR 1.000 vs a 32-variant family
    #   · only 17% overlap with edges we already own (ZRT 12%) · $8-21 dead, 21-89 and 89-377 both 5/5.
    # REPLICATES ON CCI (a different formula entirely): alone −0.45, +RS +1.76, +cci<−100 +2.45,
    # same monotone shape — so it is a momentum-exhaustion effect, not an RSI artifact. Requiring
    # BOTH oscillators does NOT help (+1.77, 4/5) — they are redundant, so we ship the RSI one.
    #
    # BEAR (mirror, a SUPPRESSOR not an edge — our short side is closed 0/29): price HIGHER high +
    # RSI LOWER high + RS BROKEN. Monotone the other way: rsi>55 −1.78 · >60 −2.09 · >65 −2.94 ·
    # >70 −3.71/win41/pf0.84/1-6yr. RS-intact instead → +0.16, so the RS direction flips exactly
    # as it should (intact = quality dip → buy · broken = failing leadership → distribution).
    # Used as "do not open a long / consider exiting here", never as a short entry.
    _div_bull, _div_bear, _dv_rsi_lo, _dv_rsi_hi = _divergence_arrays(df, "rsi_14")
    df["div_bull"]   = _div_bull                  # RSI bull divergence on a confirmed pivot low
    df["div_bear"]   = _div_bear                  # RSI bear divergence on a confirmed pivot high
    df["dv_rsi_lo"]  = _dv_rsi_lo                 # RSI AT that pivot low  (NaN when no fire)
    df["dv_rsi_hi"]  = _dv_rsi_hi                 # RSI AT that pivot high (NaN when no fire)
    _dvc_bull, _dvc_bear, _dv_cci_lo, _dv_cci_hi = _divergence_arrays(df, "cci20")
    df["divc_bull"]  = _dvc_bull                  # the same on CCI — it validated independently
    df["divc_bear"]  = _dvc_bear                  # (+1.76/5-5yr with RS; alone −0.45, like RSI)
    df["dv_cci_lo"]  = _dv_cci_lo
    df["dv_cci_hi"]  = _dv_cci_hi

    # ── graduated STAGE per bar (2026-07-28, user: "the RSI/CCI interaction should be visible on
    # every bar — it often reaches the zone but no signal fires because of the restrictions").
    # The fired edge is rare by construction (~0.16 buy/ticker/YEAR), so showing only completions
    # hides 12 of every 13 occurrences. These stages expose the whole funnel:
    #   1 raw divergence · 2 in the oversold/overbought zone but BLOCKED by the RS gate ·
    #   3 full signal · 4 deep tier.  Stage 2 is the interesting one — it is the near-miss.
    def _stages(bull, bear, lo, hi, os_th, deep_th, ob_th):
        b = np.zeros(len(df), np.int8); t = np.zeros(len(df), np.int8)
        rsi_ok = df["rs_intact"].to_numpy(bool)
        inz = bull & (lo < os_th)
        b[bull] = 1
        b[inz & ~rsi_ok] = 2                      # reached the zone, RS broken → blocked
        b[inz & rsi_ok] = 3
        b[inz & rsi_ok & (lo < deep_th)] = 4
        inzt = bear & (hi > ob_th)
        t[bear] = 1
        t[inzt & rsi_ok] = 2                      # overbought divergence but RS still INTACT
        t[inzt & ~rsi_ok] = 3                     # ...and leadership broken → the suppressor
        return b, t
    df["dvr_b"], df["dvr_t"] = _stages(_div_bull, _div_bear, _dv_rsi_lo, _dv_rsi_hi, 45, 40, 65)
    df["dvc_b"], df["dvc_t"] = _stages(_dvc_bull, _dvc_bear, _dv_cci_lo, _dv_cci_hi, 0, -100, 100)
    _dv_q = df["close"].between(21, PRICE_CAP_WIDE)
    df["E_rsidiv_rs"]      = df["div_bull"] & (df["dv_rsi_lo"] < 45) & df["rs_intact"] & _dv_q
    df["E_rsidiv_rs_deep"] = df["div_bull"] & (df["dv_rsi_lo"] < 40) & df["rs_intact"] & _dv_q
    # 🔻 the suppressor flag. rsi>65 (not >70) is the shipped cut: −2.94/pf0.85/2-6yr and ~400
    # fires a year, where >70 is stronger (−3.71/pf0.84/1-6yr) but too rare to be a useful badge.
    df["div_top"] = df["div_bear"] & (df["dv_rsi_hi"] > 65) & ~df["rs_intact"]

    # 🕐 1H-DR CONFIRMATION gate (validated 2026-07-28, h1dr_boost.py / h1dr_ctrl.py). A 1H dual
    # reclaim on this session or the previous one, ANDed with today's RS. Standalone the intraday
    # DR is not tradeable; as a confirmation it lifted 52 of 63 board setups (median Δ +1.34 on a
    # period-matched base). Built on the two where the base was weakest and the lift largest.
    _h1 = _load_h1_dr()
    _dkey = df["ticker"] + "|" + df["date"].astype(str).str[:10]
    _h1_today = _dkey.isin(_h1).to_numpy()
    _h1_yest = np.concatenate([[False], _h1_today[:-1]])[:len(_h1_today)]
    # a ticker boundary must not leak yesterday's flag into the next ticker's first bar
    _first = ~df["ticker"].eq(df["ticker"].shift(1)).fillna(False).to_numpy()
    _h1_yest[_first] = False
    df["h1_dr"] = (_h1_today | _h1_yest) & df["rs_intact"].to_numpy(bool)
    # NB the 1h DB starts 2021-07, so 2021 carries a HANDFUL of gated fires (ZRT🕐DR: exactly 1,
    # at −24%) and _stats scores that single trade as a whole "worst year". Read these two by
    # 2022-2026, where every year is positive; the 2021 cell is not a signal about the gate.
    df["E_washout_h1dr"] = df["E_washout"] & df["h1_dr"]
    df["E_zrt_h1dr"]     = df["E_zoneretest"] & df["h1_dr"] & df["close"].between(21, PRICE_CAP_WIDE)
    # Four more, chosen on the TIER-1 bar (5/5 positive years AND a positive worst year on the
    # period-matched 2022+ base). Only BASE setups — gating an already-gated variant just stacks
    # correlated filters and thins n, and we have a standing rule against proliferation here.
    #   L43-TRIPLE   +2.70 → +6.61  worst +5.0      RTB-Base  +0.91 → +3.70  worst +2.1
    #   G3→G3        +2.74 → +5.26  worst +1.1      QZ-Capit  +0.55 → +1.80  worst +0.3 (4/5→5/5)
    # Deliberately NOT gated despite a real lift, because the worst year stays negative:
    #   P55 +1.98Δ (worst −2.3) · G3-gap +1.54Δ (−0.8) · D+L1 +1.18Δ (−0.9) · Atomic +0.81Δ (−0.5)
    df["E_l43triple_h1dr"] = df["E_l43triple"] & df["h1_dr"]
    df["E_rtb_base_h1dr"]  = df["E_rtb_base"] & df["h1_dr"]
    df["E_g3g3_h1dr"]      = df["E_g3g3"] & df["h1_dr"]
    df["E_qzcapit_h1dr"]   = df["E_qzcapit"] & df["h1_dr"]

    # ── 🧊 COMPRESSION gate (2026-07-29) ────────────────────────────────────────────────
    # sig_conso = the combo_engine tight gate: 6-bar range<=3.5% OR ATR%<=3.0 OR
    # |ema9-ema20|/ema20<=2.0. It fires on 69% of bars, so it is a REGIME, not a signal —
    # its own median is +0.03, i.e. zero. What carries is the OTHER side: NOT-CONSO is
    # −3.67/win 43.6 over the same window, a genuine suppressor. So this is a veto on the
    # expansion state, read as a gate.
    # Across 11 base setups it helped 8, median Δ +0.31, and the split is mechanical:
    # it helps every absorption/capitulation setup and HURTS the three that need range
    # expansion by their own definition (Engulf-Abs −1.32, L43-TRIPLE −0.44, G3-Abs −0.27).
    # Only these two clear the TIER-1 bar (6/6 years AND a positive worst year).
    # AS BUILT (these masks, no price filter — the numbers this code actually produces):
    #   Washout  −0.53 → +1.54  win 48.8→54.8  pf 1.24→1.48  worst −2.2 → +0.7  (5/6 → 6/6)
    #   RTB-Base +0.72 → +1.09  win 51.9→53.1  pf 1.32→1.35  worst −0.1 → +0.4  (5/6 → 6/6)
    # Sliced to $21-377 the same two read +0.27 → +1.79 and +1.27 → +1.51 (worst +0.6 / +0.8),
    # so the gate holds across the whole price range, not just the quality band.
    # Real lift but NOT gated, worst year still negative ($21-377): Spring +1.17Δ (−0.7, 4/6) ·
    # QZ-Capit +0.61Δ (−0.9) · Zone-Retest +0.59Δ (−0.6) · Atomic +0.38Δ (−1.7) · D+L1 +0.25Δ.
    df["conso"] = df["conso"].fillna(0).astype(bool)
    df["E_washout_conso"]  = df["E_washout"] & df["conso"]
    df["E_rtb_base_conso"] = df["E_rtb_base"] & df["conso"]

    # ── 🔇 QUIET 1H TAPE gate (2026-07-30) ──────────────────────────────────────────────
    # See _load_h1_quiet for how a spike hunt produced its own inverse. As a gate the
    # mechanism is GENERAL — it improved 10 of 10 base setups (Δ +0.18 to +0.93) and the
    # ordering is monotone everywhere: quiet > base > loud. That complement ordering, not
    # the deltas, is what makes it credible.
    # Two cleared TIER-1 (6/6 years AND positive worst). Only ONE cleared DSR against 20
    # honest trials (10 setups × quiet/loud):
    #   L43-TRIPLE  +2.72 → +3.43  worst +0.1  6/6   sr 0.2512 vs sr* 0.1406  DSR 1.000 ✓
    #   RTB-Base    +0.68 → +1.51  worst +1.3  6/6   sr 0.1292 vs sr* 0.1406  DSR 0.101 ✗
    # RTB-Base is the second gate this week to look perfect on every eyeball criterion and
    # fail deflation — same shape as RTB-Base🌀ROUGH that morning. Not built.
    _q = _load_h1_quiet()
    df["h1_quiet"] = (df["ticker"] + "|" + df["date"].astype(str).str[:10]).isin(_q).to_numpy()
    df["E_l43triple_quiet"] = df["E_l43triple"] & df["h1_quiet"]

    # ── 🥪 T2G-SANDWICH (2026-08-02, the from-scratch TZ×L correlation session) ─────────
    # T2G → T10 → T2G: one distribution bar swallowed between two gap-up closes, entered on
    # the second T2G, in a LEADER (rs_intact) that is already overbought (RSI≥70), $21-89.
    # The book's first OVERBOUGHT-momentum setup — strength is buyable only after a
    # tested-and-absorbed washout. med +1.70/win57/pf1.93/5-5yr/worst +0.0 (2021 thin, n196),
    # DSR 0.947 vs sr*=0.115 with N=27 honest chain trials, Δ vs the same-RS-same-RSI
    # complement +2.73 (sign 5/5), overlap with the G3 gap family 0%. RSI plateau: direction
    # holds 65-72 (medians +0.14..+0.47, pf 1.3-1.5), magnitude peaks at 70.
    # ⚠ RS is a REQUIRED component, not a booster: without rs_intact the same pattern is
    # toxic (med −1.74, worst −11.5, 2/6yr). Do not ship an un-gated variant.
    _t_p1 = g["t"].shift(1).fillna("")
    _t_p2 = g["t"].shift(2).fillna("")
    df["E_t2gsand_rs"] = ((df["t"] == "T2G") & (_t_p1 == "T10") & (_t_p2 == "T2G")
                          & (df["rsi_14"] >= 70) & df["rs_intact"]
                          & df["close"].between(21, 89))

    # ── 🌉 Z1G→T4 (2026-08-04, prefix-sweep line; BUILT AT THE USER'S EXPLICIT REQUEST) ──
    # Three specific 4-bar sequences ending in a T4 reversal, all carrying a Z1G (absorbed
    # gap-down) in the prefix: T6→Z1G→Z2G→T4 · Z1G→T1G→Z5→T4 · Z1G→Z6→Z2G→T4. $21-377.
    # Path-sim: med +7.74/win 64.5/pf 5.5/5-6yr/worst −3.2 (n=166); Δ vs other-prefix T4
    # +8.33 (sign 5/6).
    # ⚠ WATCH-TIER BY THE BOOK, TRADED BY USER DECISION: (a) selection-circular — these are
    # the top-3 of a 1,512-cell sweep verified on the SAME window; (b) worst-year −3.2
    # fails the ≥−2 gate; (c) 2025 contributes +29.4. The user accepts the tail with a
    # structural stop ("stoplossit sheval da tu ramea gaminusdeba") and treats it as an
    # 📈emerging-style candidate. Revisit after ~6 months of live fires.
    _c1 = g["t"].shift(1).fillna(""); _z1 = g["z"].shift(1).fillna("")
    _c2 = g["t"].shift(2).fillna(""); _z2 = g["z"].shift(2).fillna("")
    _c3 = g["t"].shift(3).fillna(""); _z3 = g["z"].shift(3).fillna("")
    _b1 = np.where(_c1 != "", _c1, _z1); _b2 = np.where(_c2 != "", _c2, _z2)
    _b3 = np.where(_c3 != "", _c3, _z3)
    df["E_z1gt4"] = ((df["t"] == "T4") & df["close"].between(21, 377)
                     & (((_b3 == "T6") & (_b2 == "Z1G") & (_b1 == "Z2G"))
                        | ((_b3 == "Z1G") & (_b2 == "T1G") & (_b1 == "Z5"))
                        | ((_b3 == "Z1G") & (_b2 == "Z6") & (_b1 == "Z2G"))))

    # ── 🧲 Z9-HL + 🌉v2 (2026-08-04, prefix-sweep series; BUILT AT THE USER'S REQUEST) ────
    # Same WATCH-TIER contract as E_z1gt4: spectacular medians, failed worst-year gates,
    # selection-circular (tops of ~1,200-cell same-window sweeps), 2025-heavy. The user
    # trades them with structural stops as 📈emerging-style bets; revisit on live fires.
    # 🧲 Z9-HL — the higher-low grammar (reversal → absorbed Z9 retest → reversal), the two
    #   biggest-n cells of the whole series: Z3→T4→Z9→T3 (n=340 fwd) · T4→Z9→T3→Z5 (n=314).
    #   Path-sim $21-377: med +12.89/win 73/pf 5.26/4-6yr/worst −10.7 (2022!), n=270.
    # 🌉v2 — the Z1G family on the new endings: T6→Z1G→Z2G→T3 · T6→Z1G→T5→T6 ·
    #   Z1G→T1→T2G→T6. Path-sim $21-377: med +15.36/win 75.5/pf 6.52/5-6yr/worst −6.1, n=188.
    _b0 = np.where(df["t"] != "", df["t"], df["z"])
    df["E_z9hl"] = (df["close"].between(21, 377)
                    & (((_b3 == "Z3") & (_b2 == "T4") & (_b1 == "Z9") & (_b0 == "T3"))
                       | ((_b3 == "T4") & (_b2 == "Z9") & (_b1 == "T3") & (_b0 == "Z5"))))
    df["E_z1gt36"] = (df["close"].between(21, 377)
                      & (((_b3 == "T6") & (_b2 == "Z1G") & (_b1 == "Z2G") & (_b0 == "T3"))
                         | ((_b3 == "T6") & (_b2 == "Z1G") & (_b1 == "T5") & (_b0 == "T6"))
                         | ((_b3 == "Z1G") & (_b2 == "T1") & (_b1 == "T2G") & (_b0 == "T6"))))

    # 🧺 SEQ-20 collection (2026-08-04): the REMAINING top triples of the 8-ending sweep
    # (n>=39, med>=+2.6 each on fwd10), 20 pre-registered sequences pooled. Path-sim
    # $21-377: med +2.92/win 56.1/pf 1.88/4-6yr/worst −2.3 (n=594) — the tamest of the
    # three user-requested WATCH builds, one whisker off the worst>=−2 gate.
    _SEQ20 = {("Z1","Z2G","T1","T6"),("T4","Z3","T1G","T6"),
              ("T1G","T6","Z3","T3"),("Z6","T3","Z1G","T3"),("Z1G","T5","Z3","T3"),
              ("Z9","T3","Z5","T9"),("T5","Z3","Z6","T9"),
              ("T6","Z1","T5","Z5"),("Z1G","T5","T11","Z5"),("T4","Z4","T5","Z5"),
              ("T12","T2G","Z4","T1"),("T3","T6","Z9","T1"),
              ("Z6","T3","Z1","T1G"),("T5","T2","Z1","T1G"),
              ("T11","Z5","T1G","T2"),("T1","T2G","T12","T2"),
              ("T12","Z3","T1G","T2G"),("Z3","T9","T11","T2G"),
              ("T6","T11","T2G","T2G"),("T10","Z3","T1G","T2G")}
    _quad = pd.Series(list(zip(_b3, _b2, _b1, _b0)), index=df.index)
    df["E_seq20"] = df["close"].between(21, 377) & _quad.isin(_SEQ20).to_numpy()

    # 👑 Z1G-CROWN (2026-08-04, the prefix series' closing find; same user WATCH contract).
    # The BIG-N family: double absorbed gap-down -> green attempt -> SOFT RED entry bar
    # (no confirmation premium): Z1G>Z2G>T5>Z3 · Z1G>Z2G>T3>Z3 · Z1G>Z2G>T5>Z4 ·
    # T5>Z3>T4>Z9 (+5 siblings added same day — the FULL Z3/Z4/Z9 top table, 9 sequences).
    # $8-377. With the lower-half-close filter, 9-seq path-sim $21-377: med +14.53/win 72.9/
    # pf 5.83/worst −2.5 (n=853, 3/6yr — 2025 +26.8); $8-21: +12.57/5-6yr (2021-25 all
    # positive, n=250). Δ vs other Z3/Z4/Z9 endings +14..+16 (sign 4-5/6).
    # ⚠ era-tilted like its siblings — traded by user decision with structural stops.
    _CROWN = {("Z1G","Z2G","T5","Z3"),("Z1G","Z2G","T3","Z3"),
              ("Z1G","Z2G","T5","Z4"),("T5","Z3","T4","Z9"),
              # +5 (2026-08-04, user: the rest of the Z3/Z4/Z9 top table belongs here too)
              ("T12","Z1G","T5","Z3"),("Z1G","T5","T11","Z3"),
              ("T12","Z1G","T3","Z3"),("Z1G","T5","T12","Z4"),
              ("T3","Z3","T4","Z9")}
    # intraday-anatomy filter (2026-08-04, crown_intraday.py on 982 fires): an entry day
    # that ALREADY recovered into the upper half of its range is the family's weak subset
    # (med +3.57 vs +6.66; with a same-day 1H REV on top it collapses to +0.98) — buy the
    # still-compressed close, not the half-bounced one. Daily-computable, no intraday dep.
    # (Same study: same-day 1H REV-turn adds +2.3pp — flows to the brain via mtf_echo;
    # 15m vol-event is NOT required here — the volume drama happened on the Z1G/Z2G bars.)
    _rng = (df["high"] - df["low"])
    _cpos = ((df["close"] - df["low"]) / _rng.where(_rng > 0)).fillna(1.0)
    df["E_z1gcrown"] = (df["close"].between(8, 377) & _quad.isin(_CROWN).to_numpy()
                        & (_cpos < 0.5))

    # ── 🪨 T1G-NB (2026-08-03, WLNBB suffix league) ─────────────────────────────────────
    # A gap-up T1 whose bar prints the NB suffix (No-effort + Both wicks): the gap was
    # tested both ways on no effort and HELD — absorbed-and-accepted strength. The suffix
    # league's only double-REAL: Δ+1.19 vs other T1G (sign 6/6), while the SAME suffix on
    # gapless T1 is a 6/6 SUPPRESSOR (−1.24) — the gap context flips the meaning.
    # +🏆RS: med +2.42/win 56.9/pf 1.91/5-5yr ALL positive/worst +2.3/n339 (2021 thin);
    # DSR 0.982 (sr*=0.063, N=20 honest suffix-league trials); overlap with Atomic/GEM1/G3
    # 0.0% — fully disjoint. ⚠ RS REQUIRED: without it worst −4.8 (2022), do not un-gate.
    df["E_t1gnb_rs"] = ((df["t"] == "T1G") & (df["fsfx"] == "NB") & df["rs_intact"]
                        & df["close"].between(21, 89))

    # 🪨+ T1G-NB with an L34 ABSORPTION BAR IN THE PRECEDING 3 BARS (2026-08-05).
    # Found by the full-descriptor re-audit: the 5 sequence edges had been mined on t/z
    # codes ALONE, ignoring the L-line and suffix layers. Slicing all 7 edges by those
    # layers (112 cells) surfaced exactly one cell that survived scrutiny:
    #   L34 in prefix  n=94  med +6.40 win 66.0 pf 2.52  5/5yr ALL positive  worst +2.4
    #   no L34         n=247 med +1.12 win 54.3 pf 1.77  2/5yr               worst -0.9
    #   parent         n=340 med +2.79                   5/5yr               worst +0.0
    # Δ vs complement +5.28, Δ vs parent +3.61. Buckets hold both sides ($21-40 +9.36,
    # $40-89 +5.28). Overlap with E_l34cont / E_l34cont_rs = 0.0% — a genuinely new cell,
    # not a relabel of the L34-continuity edge (though it replicates that edge's idea:
    # supply absorbed on an L34 bar, then demand confirms).
    # PLATEAU (the gate that decided it): L34 within w bars, w=1..6 →
    #   +8.77(n32,3/5) · +5.85(64,5/5) · +6.40(94,5/5) · +5.54(113,5/5) · +5.19(131,5/5)
    #   · +4.95(152,5/5) — smooth monotone decay, every window beats the parent, every
    #   worst-year >= +0.4. Noise does not produce a family like that.
    # CONTROL: it is L34 SPECIFICALLY, not "any L in the prefix" —
    #   L34 +6.40(5/5,+2.4) >> L46 +3.74(4/5) > L3 +2.78 > L25 +2.46 > L12 +2.42 > L5 +1.35(2/5)
    #   (L12/L25/L3 sit at the parent's +2.79, i.e. no effect at all).
    # ⚠ HONEST CAVEAT: DSR over the FULL 87-cell search family = 0.000 (sr* 0.671 vs cell
    # SR 0.336) — it FAILS wide deflation. Narrow DSR, over this parent's own 12 cells,
    # is 0.982. The wide family mixes parents whose medians differ 5x (CROWN +17% vs
    # SAND +1.8%), so sr* is set by CROWN's scale rather than by this search. Built on the
    # strength of the plateau + control + 0% overlap, NOT on the wide DSR. Watch it live.
    _pre34 = (g["l"].shift(1).eq("L34") | g["l"].shift(2).eq("L34") | g["l"].shift(3).eq("L34"))
    df["E_t1gnb_l34pre"] = df["E_t1gnb_rs"] & _pre34.to_numpy()

    # ── 🌀 PATH ROUGHNESS — Hurst, variance-ratio (2026-07-30) ─────────────────────────
    # std of overlapping k-step log returns scales as k^H, so H = slope of log(std_k) on
    # log(k) over lags 1,2,4,8 in a trailing 60-bar window. Measured on 2.7M bars the raw
    # ladder is MONOTONE on median, win%, pf AND positive-year count simultaneously —
    #   H<0.35 +0.14/win50.4/pf1.24 · .35-.45 −0.31 · .45-.55 −0.74 (baseline −0.71)
    #   · .55-.65 −0.93/3-6yr · H>0.65 −2.37/win44.8/pf0.90/3-6yr
    # — replicates on a 40-bar window (−2.33/pf0.91) and is NOT volatility: corr(H,ATR%)
    # = −0.008 and the H<0.45 vs H>0.55 spread survives INSIDE every ATR% tercile, widening
    # with vol (lo 0.25pp · mid 0.96pp · hi 3.14pp).
    #
    # ⚠ NO GATE IS BUILT ON THIS. RTB-Base🌀ROUGH was built and then REMOVED the same day,
    # 2026-07-30, because it FAILED DSR: sr 0.1202 vs sr* 0.1436 over 30 honest trials
    # (10 setups x 2 directions + 5 H60 bands + 5 H40 bands) → DSR 0.010 against a 0.6 bar.
    # Its Sharpe was the HIGHEST of the three gates measured that day and still sat BELOW
    # what picking the best of 30 variants produces by chance. Everything an eyeball checks
    # said yes — 6/6 positive years, worst +0.5, Δ+0.63, pf 1.31→1.44, win 51.8→53.5 — and
    # it was selection noise. Compare the two CONSO gates, chosen from 11 trials: sr 0.1193
    # and 0.1017 vs sr* 0.0943 → DSR 0.944 and 0.891, which is why those two stand.
    # The LAW stands on different evidence and is unaffected: a monotone ladder across five
    # bands on four metrics at once, replicated on a second window, orthogonal to vol. That
    # is not a max-pick, so DSR's selection critique does not apply to it.
    # For the record, the per-setup deltas that tempted the build: D+L1 +1.11Δ (worst −0.0)
    # · Washout +0.90Δ (−2.1) · L43-TRIPLE +0.69Δ (−1.5) · RTB-Base +0.68Δ (+0.6, 6/6) ·
    # Atomic +0.62Δ (−1.1) · G3-Abs +0.61Δ (−0.7) · Zone-Retest +0.43Δ · QZ-Capit +0.41Δ ·
    # Engulf-Abs +0.34Δ. Wyckoff Spring alone prefers SMOOTH (+0.11→+1.58 at H>0.55).
    # `hurst`/`rough` stay as COLUMNS: the law is real, the Superchart/brain read the state,
    # and a future walk-forward replay can calibrate it. They just gate nothing.
    _lags = (1, 2, 4, 8)
    _hx = np.log(np.asarray(_lags, float)); _hxc = _hx - _hx.mean(); _hden = float((_hxc ** 2).sum())
    _lp = np.log(df["close"].where(df["close"] > 0))
    _hnum = np.zeros(len(df)); _hok = np.ones(len(df), bool)
    for _j, _k in enumerate(_lags):
        _sd = _lp.groupby(df["ticker"], sort=False).diff(_k) \
                 .groupby(df["ticker"], sort=False).rolling(60, min_periods=30).std() \
                 .reset_index(level=0, drop=True).to_numpy(float)
        _hok &= np.isfinite(_sd) & (_sd > 0)
        _hnum += _hxc[_j] * np.log(np.where(_sd > 0, _sd, np.nan))
    df["hurst"] = np.where(_hok, _hnum / _hden, np.nan)
    df["rough"] = df["hurst"] < 0.45

    # ── 📐 ADX / DI regime (2026-08-07, from the user's Pine v6 port 260807 V1) ─────────
    # Wilder TR/+DM/−DM with the classic accumulator (x − x/n + new), DI = smDM/smTR·100,
    # DX = |DI+−DI−|/(DI++DI−)·100, ADX = RMA(DX, 14). TWO CORRECTIONS vs the script:
    #   (a) it used ta.sma(dx) — textbook ADX is RMA. The two disagree on the regime call
    #       for 20.4% of bars, so the deviation matters; we use the correct RMA.
    #   (b) it had no warmup guard, so the first ~len bars of every ticker were garbage
    #       (harmless on a chart, contamination in a backtest) — masked to −1 for 3×len.
    # regime: 1 TREND-UP (adx≥25 & DI+>DI−) · 2 TREND-DN · 3 RANGE (adx≤20) · 0 transition.
    #
    # NOT a duplicate of hurst: agreement with hurst>0.55 is only 63.5% and corr(adx,hurst)
    # is +0.20 — genuinely separate information (conso agreement 59.0%).
    #
    # THE SCRIPT'S HYPOTHESIS IS REFUTED. It claimed TREND-UP favours breakout/momentum
    # edges. Measured on the ATR exit, TREND-UP is the WORST regime for BOTH families:
    #   REVERSAL (base +1.87): RANGE +2.27 (6/6yr, worst +0.15) · TREND-DN +1.87
    #                          · TREND-UP −0.83 (2/6yr, n=703 thin)
    #   MOMENTUM (base +1.86): TREND-DN +3.19 (5/6) · RANGE +1.79 · TREND-UP −0.03
    #                          (3/6yr, worst −5.81, n=10,242)
    # Reading: our whole book — including what we call "momentum" (G3 gap-reclaim, Atomic
    # weak-close gap-up, L43) — BUYS ABSORBED WEAKNESS, not strength. In a strong uptrend it
    # has nothing to buy. Per-edge on TREND-UP: QZC −3.25 · D+L1 −2.08 · ATM −1.38.
    # RANGE is NOT built as a booster: inconsistent, and it HURTS the gap family
    # (G3 −0.37, G3A −0.59). DSR is 0.000 for every cell → nothing here is a size lever.
    _n = 14
    _pc = g["close"].shift(1); _ph = g["high"].shift(1); _pl = g["low"].shift(1)
    _tr = np.maximum(np.maximum(df["high"] - df["low"], (df["high"] - _pc).abs()),
                     (df["low"] - _pc).abs()).fillna(0.0)
    _up = (df["high"] - _ph).fillna(0.0); _dn = (_pl - df["low"]).fillna(0.0)
    _dmp = np.where((_up > _dn) & (_up > 0), _up, 0.0)
    _dmm = np.where((_dn > _up) & (_dn > 0), _dn, 0.0)
    # Wilder accumulation == an EWM with alpha=1/n scaled by n; use ewm for speed
    _sTR = _tr.groupby(df["ticker"]).transform(lambda s: s.ewm(alpha=1 / _n, adjust=False).mean())
    _sP = pd.Series(_dmp, index=df.index).groupby(df["ticker"]).transform(
        lambda s: s.ewm(alpha=1 / _n, adjust=False).mean())
    _sM = pd.Series(_dmm, index=df.index).groupby(df["ticker"]).transform(
        lambda s: s.ewm(alpha=1 / _n, adjust=False).mean())
    with np.errstate(invalid="ignore", divide="ignore"):
        _dip = np.where(_sTR != 0, _sP / _sTR * 100, 0.0)
        _dim = np.where(_sTR != 0, _sM / _sTR * 100, 0.0)
        _sum = _dip + _dim
        _dx = np.where(_sum != 0, np.abs(_dip - _dim) / _sum * 100, 0.0)
    _adx = pd.Series(_dx, index=df.index).groupby(df["ticker"]).transform(
        lambda s: s.ewm(alpha=1 / _n, adjust=False).mean()).to_numpy()
    _bar_i = g.cumcount().to_numpy()
    _warm = _bar_i >= 3 * _n
    _reg = np.zeros(len(df), dtype=np.int8)
    _reg = np.where((_adx >= 25) & (_dip > _dim), 1, _reg)
    _reg = np.where((_adx >= 25) & (_dim > _dip), 2, _reg)
    _reg = np.where(_adx <= 20, 3, _reg)
    df["adx"] = np.where(_warm, _adx, np.nan)
    df["di_plus"] = np.where(_warm, _dip, np.nan)
    df["di_minus"] = np.where(_warm, _dim, np.nan)
    df["adx_regime"] = np.where(_warm, _reg, -1).astype(np.int8)
    df["adx_trend_up"] = df["adx_regime"] == 1      # the report-only suppressor state

    # ── 🌊 WaveTrend (LazyBear) — the "Market Cipher B" oscillator, honest version ──────
    # The user brought four Pine scripts; all four share ONE core. LazyBear's is the
    # original, the two "Market Cipher B" reskins are byte-identical to it in maths, and
    # the WeloTrades build changes three params (close/9/sma2 vs hlc3/10/sma4) and adds a
    # fake "Money Flow" (no volume in its formula at all) plus divergences drawn with
    # offset=-10 (visually prescient, known 10 bars late). We take the ORIGINAL params.
    #   ap=hlc3 · esa=EMA(ap,10) · d=EMA(|ap−esa|,10) · ci=(ap−esa)/(0.015·d)
    #   wt1=EMA(ci,21) · wt2=SMA(wt1,4)
    # wt1 is literally a twice-smoothed CCI: (price−MA)/(0.015·mean-deviation) IS the CCI
    # formula, so this is EMA21(CCI10) — a different smoothing of something we already have
    # as cci_20, which is exactly why it has to be overlap-tested before it means anything.
    # `d != 0` guard is from the v6 port; the original divides by zero on a flat bar.
    _wn1, _wn2, _wml = 10, 21, 4
    _ap = (df["high"] + df["low"] + df["close"]) / 3.0
    _esa = _ap.groupby(df["ticker"]).transform(lambda s: s.ewm(span=_wn1, adjust=False).mean())
    _dv = (_ap - _esa).abs().groupby(df["ticker"]).transform(
        lambda s: s.ewm(span=_wn1, adjust=False).mean())
    _ci = np.where(_dv.to_numpy() != 0, (_ap - _esa) / (0.015 * _dv), 0.0)
    _wt1 = pd.Series(_ci, index=df.index).groupby(df["ticker"]).transform(
        lambda s: s.ewm(span=_wn2, adjust=False).mean())
    _wt2 = _wt1.groupby(df["ticker"]).transform(lambda s: s.rolling(_wml).mean())
    df["wt1"] = _wt1
    df["wt2"] = _wt2
    _p1 = _wt1.groupby(df["ticker"]).shift(1)
    _p2 = _wt2.groupby(df["ticker"]).shift(1)
    _xup = (_wt1 > _wt2) & (_p1 <= _p2)          # wt1 crosses ABOVE wt2 = bullish
    df["wt_cross_up"] = _xup.fillna(False)
    # the zone-gated rule is the only one of the four scripts that defines an actual TRADE:
    # a cross only counts inside oversold, with a two-tier strength (−45 / −60) that is a
    # free plateau test the author built in.
    df["wt_bull_dot"] = df["wt_cross_up"] & (_wt2 <= -45)
    df["wt_bull_strong"] = df["wt_cross_up"] & (_wt2 <= -60)
    # 💥 the one component only the WeloTrades build has, and it is lookahead-free:
    # wt1 AND price both stop making new 28-bar lows on the same bar.
    _w28 = _wt1.groupby(df["ticker"]).transform(lambda s: s.rolling(28, min_periods=28).min())
    _c28 = df["close"].groupby(df["ticker"]).transform(
        lambda s: s.rolling(28, min_periods=28).min())
    _pw28 = _w28.groupby(df["ticker"]).shift(1)
    _pc28 = _c28.groupby(df["ticker"]).shift(1)
    df["wt_dbl_reclaim"] = ((_wt1 > _pw28) & (_p1 <= _pw28)
                            & (df["close"] > _pc28)
                            & (df["close"].groupby(df["ticker"]).shift(1) <= _pc28)).fillna(False)
    # (E_rtb_base_hurst deliberately NOT defined — see the DSR note above.)
    # ONE display chip for the whole family — a bar where the gate was on AND one of the six
    # gated bases fired. Six separate chips would just duplicate the base codes already shown
    # ("WSH 🕐DR" reads better than "WSH Washout🕐DR"), and this is what makes the gate visible
    # historically on the Superchart EDGE row and in the CSV, not only in the Replay backtest.
    df["h1dr_chip"] = df["h1_dr"] & (df["E_washout"] | df["E_zoneretest"] | df["E_l43triple"]
                                     | df["E_rtb_base"] | df["E_g3g3"] | df["E_qzcapit"])

    # 🔄 DUAL OVERSOLD RECLAIM × 🏆RS (validated 2026-07-28, dual_reclaim.py / dr_val.py /
    # dr_disj.py — the user's own read: "show where RSI and CCI both come back from oversold
    # into the zone; the advance usually starts there"). Not divergence: a RECLAIM, i.e. each
    # oscillator crossing back UP through its oversold threshold, the two within a few bars.
    #
    # Either one alone is near-nothing (RSI35 reclaim +0.22 · CCI−100 reclaim +0.04); TOGETHER
    # +0.89, and the window is a smooth plateau (±0 +0.77 · ±1 +0.70 · ±2 +0.89 · ±3 +0.91 ·
    # ±5 +0.73) rather than a knife-edge. 🏆RS then does what it always does:
    #   RSI35∧CCI ±2 +RS  +3.52/win59/pf1.97 · 5/5yr · worst +1.6   (vs RSI-alone+RS +2.99,
    #   CCI-alone+RS +1.83/4-5yr) · deep RSI30∧CCI ±3 +3.86/pf2.42/worst +3.2
    #   +💥 intraday volume event → +3.86/win60/pf2.04/worst +2.2
    # Plateau 9/9 variants 5/5yr positive-worst · TRAIN 2022-23 +2.80 & TEST 2024-26 +4.38 both
    # positive · DSR 1.000 vs a 24-variant family · $8-21 dead, $21-89 +2.85, $89-377 +4.22.
    #
    # NOVELTY (the check that usually kills things here): 73% of fires coincide with an existing
    # edge, 64% with Zone-Retest — high enough to suspect a relabel. It is not: the DISJOINT 27%
    # pays just as well (+3.23/pf2.08/5-5yr/worst +1.9), while ZRT-that-is-NOT-this is only
    # +0.44/4-6yr. So the reclaim selects the good part of ZRT *and* finds an equally good set
    # outside it. Also note ZRT+RS carries a −11.4 2021 (RS is a known trap on ZRT) where this
    # is +1.6 — different animal despite the co-occurrence.
    _r14 = df["rsi_14"]; _c20 = df["cci20"]
    _pr14 = g["rsi_14"].shift(1); _pc20 = g["cci20"].shift(1)
    _rx35 = (_pr14 < 35) & (_r14 >= 35)          # RSI reclaims 35
    _rx30 = (_pr14 < 30) & (_r14 >= 30)
    _cx100 = (_pc20 < -100) & (_c20 >= -100)     # CCI reclaims −100

    def _near(mask, w):
        """mask fired within ±w bars, per ticker (the 'together' window)."""
        m = mask.astype(float)
        out = m.copy()
        for k in range(1, w + 1):
            out = out + m.groupby(df["ticker"]).shift(-k).fillna(0) \
                      + m.groupby(df["ticker"]).shift(k).fillna(0)
        return out > 0

    _cx_w2 = _near(_cx100, 2)
    _cx_w3 = _near(_cx100, 3)
    _dr_q = df["close"].between(21, PRICE_CAP_WIDE)
    df["dual_reclaim"] = _rx35 & _cx_w2                       # raw state, for display
    df["E_dualrec_rs"]      = df["dual_reclaim"] & df["rs_intact"] & _dr_q
    df["E_dualrec_rs_deep"] = _rx30 & _cx_w3 & df["rs_intact"] & _dr_q

    # 🕯️ MID-CLOSE gate (validated 2026-07-27, breakout_closepos.py / midclose_validate.py).
    # Born from a "STRONG vs WEAK BREAKOUT" infographic claiming a breakout is tradeable only if
    # the candle closes ≥62% of its range beyond the broken level. Tested raw: REFUTED — every
    # 20d/50d breakout × close-position cell is negative (STRONG −0.88 vs a −0.75 baseline) and
    # the colour rule runs BACKWARDS (green breakout −0.88 vs red −0.68; the picture's "do not
    # enter" cells are the LESS bad ones). Same law as always: fade strength, buy absorbed
    # weakness ([[project_what_actually_works]]).
    # But as a GATE on real edges the ladder is an INVERTED-U — the MIDDLE wins, not the strong
    # close and not the weak one. cp = (close − low) / (high − low):
    #   strong close (cp≥62) HURTS 6/8 edges (D+L1 −0.90, G3-Abs −0.48, Washout −0.40)
    #   mid   close (38-62)  helps 6/8, and the plateau is wide (30-70 … 45-55 all work)
    # Reads as "demand showed up but is not yet exhausted" — the absorbed-effort law in the
    # close-position axis, and the THIRD inverted-U in the system after volume magnitude and
    # the score zones ([[project_volume_magnitude]], [[project_score_ensemble]]).
    # ROLE IS EDGE-SPECIFIC (as with RS / dwell / TLS): G3-Abs and L43 carry it alone; on
    # Washout and ZRT mid-close ALONE is a trap (4/6 worst −3.6 · 3/6 worst −1.0) and only pays
    # together with the 💥 intraday volume event (Washout 4/6 worst −2.0 → 6/6 worst +1.2 · ZRT
    # +0.2 → +1.2). Those two combined variants are **deliberately NOT built**: DSR 0.541 / 0.537
    # against the 24-variant combined family (SR +0.156 vs SR* +0.154) is below our 0.6 trust bar,
    # so the worst-year rescue is real in-sample but not selection-proof. WATCH tier.
    # Not built either: D+L1 (TRAIN −0.30, 2021 worsens −1.0→−2.6 — era-dependent) and QZ-Capit
    # (marginal, 2021 worsens 0.0→−0.5).
    _midclose = (df["close"] - df["low"]) / (df["high"] - df["low"]).clip(lower=1e-9)
    df["mid_close"] = _midclose.between(0.38, 0.62, inclusive="right")
    # G3-Abs: med +2.00→+3.39, win 55.1→59.6, pf 1.73→2.30, 5/6yr worst −0.8 → 6/6yr worst +1.6.
    # DSR 1.000 vs the 24-variant mid-band family. Plateau: 6/6yr at ALL six cuts tested.
    # Survives BOTH price buckets ($21-89 6/6 worst +2.1 · $89-377 6/6 worst +0.7). TRAIN 3/3.
    df["E_g3abs_mid"] = df["E_g3abs"] & df["mid_close"]
    # L43-TRIPLE: med +2.77→+4.16, win 57.8→62.6, pf 1.91→2.52, worst +0.3 → +1.8, DSR 0.999.
    # $21-89 ONLY — the $89-377 bucket is 4/6yr worst −2.0, so the price cap stays tight here
    # (the opposite of E_g3abs_mid, which widens cleanly). TRAIN +2.71 3/3.
    df["E_l43triple_mid"] = df["E_l43triple"] & df["mid_close"] & df["close"].between(21, PRICE_CAP)
    # 🎋 THREE-LINE-STRIKE entry (validated 2026-07-17, tls2.py — from the user's "candlestick
    # patterns as entry triggers" idea). A Three Line Strike completes on bar j: 3 consecutively
    # LOWER closes (j-3>j-2>j-1) then a GREEN bar that closes ABOVE the high of bar j-3 (engulfs
    # the 3-bar decline). It is a bad TIMER (matched vs immediate −3.8pp: waiting makes you chase)
    # but, as a CAUSAL ENTRY GATE on top of an edge that fired ≤5 bars earlier, it adds real lift
    # on TWO setups and nothing on the others — an edge-SPECIFIC entry filter, not universal:
    #   QZ-Capit🎋TLS  +1.10pp (TRAIN +0.38 · TEST +1.71, BOTH positive) · 4/6yr · +2.98σ → BUILT
    #   G3-Abs🎋TLS    +3.04pp but TRAIN −0.55 / TEST +4.79 (2024-26-only) → WATCH, era-tilted
    #   Atomic/Cluster: −0.03 / +0.21pp, σ<1 → nothing (not built)
    # NOTE the earlier lookahead trap: labelling a fire by whether a TLS forms in the NEXT 5 bars
    # gave a fake +6.4%/12σ — you'd be entering BEFORE the TLS. This gate enters AFTER it (j+1).
    o_ = df["open"].to_numpy(float); h_ = df["high"].to_numpy(float); c_ = df["close"].to_numpy(float)
    tls = np.zeros(len(df), bool)
    for _tk, _idx in df.groupby("ticker", sort=False).indices.items():
        _idx = np.sort(_idx); m = len(_idx)
        if m < 4:
            continue
        oo = o_[_idx]; hh = h_[_idx]; cc = c_[_idx]
        t = np.zeros(m, bool)
        t[3:] = ((cc[:-3] > cc[1:-2]) & (cc[1:-2] > cc[2:-1])   # 3 lower closes j-3>j-2>j-1
                 & (cc[3:] > oo[3:])                            # bar j green
                 & (cc[3:] > hh[:-3]))                          # closes above high[j-3]
        tls[_idx] = t
    df["tls_bar"] = tls
    # "an edge fired in the trailing 6 bars (this bar + prior 5), causal" — rolling-OR
    def _recent6(col):
        return (df.groupby("ticker", sort=False)[col]
                  .transform(lambda s: s.astype(float).rolling(6, min_periods=1).max()) > 0)
    df["E_qzcapit_tls"] = df["tls_bar"] & _recent6("E_qzcapit")            # robust (built live)
    df["E_g3abs_tls"]   = df["tls_bar"] & _recent6("E_g3abs")             # 🟡 watch (era-tilted)
    # ── self-learned mined combos (base ∧ conditioner masks the brain validated OOS and
    #    promoted via brain/miner.py). Built here so the spine can path-sim + fire them. ──
    for _mc in _MINED_COMBOS:
        b, c = _mc.get("base_col"), _mc.get("cond_col")
        if b in df.columns and c in df.columns:
            df[_mc["id"]] = df[b].fillna(False).astype(bool) & df[c].fillna(False).astype(bool)
    return df


# display name + the entry column for each setup
SETUPS = [
    ("L43-TRIPLE", "E_l43triple"), ("Z11-T11", "E_z11t11"), ("Washout", "E_washout"),
    ("D+L1", "E_dl1"), ("G3-gap", "E_g3"), ("Atomic", "E_atomic"),
    ("H1-bottom", "E_h1bottom"), ("Spring", "E_spring"), ("P55", "E_p55"),
    ("Parabola", "E_parabola"), ("Atomic-R", "E_atomicR"), ("Engulf-Abs", "E_engulfabs"),
    ("T1-CapBounce", "E_t1capbounce"), ("Engulf-L46", "E_engulfL46"),
    ("Engulf-Abs-Lⁿ", "E_engulfabs_Lheavy"),
    ("Zone-Retest", "E_zoneretest"), ("Zone-Retest-E", "E_zoneretest_E"),
    ("Zone-Retest-DiT", "E_zoneretest_dit"), ("HighBase-15mDip", "E_highbase15"),
    ("RTB-Base", "E_rtb_base"), ("QZ-Capit-Rev", "E_qzcapit"),
    ("🎯Confluence≥3", "E_confluence"), ("🎯Confluence≥4", "E_confluence_p"),
    ("🎯Confluence💎89+", "E_confluence_hi"), ("⚡G3-Abs", "E_g3abs"),
    # 🏆 RS-gated variants (rs=close/sector-ETF above its EMA200; ETF data starts 2021-07)
    ("🎯Cluster🏆RS", "E_confluence_rs"), ("QZ-Capit🏆RS", "E_qzcapit_rs"),
    ("⚡G3-Abs🏆RS", "E_g3abs_rs"),
    # 🔑 KEY-LEVEL variants (support tested ≥2× = real level, not a knife)
    ("Spring🔑", "E_spring_key"), ("QZ-Capit🔑", "E_qzcapit_key"), ("D+L1🔑", "E_dl1_key"),
    ("QZ-Capit🏛️BOS", "E_qzcapit_bos"),
    ("QZ-Capit🧱OB", "E_qzcapit_ob"), ("🎯Cluster🧱OB", "E_confluence_ob"), ("D+L1🧱OB", "E_dl1_ob"),
    # 🎋 THREE-LINE-STRIKE entry gate (edge fired ≤5 bars before, enter after the TLS completes)
    ("QZ-Capit🎋TLS", "E_qzcapit_tls"), ("G3-Abs🎋TLS", "E_g3abs_tls"),
    # 🌀 SC-SUPER variants — the 6 setups gated to the Wyckoff SC zone (±5% support)
    ("ppr×NS 🕐24-26", "E_ppr_ns"),
    ("G3+RL", "E_g3rl"), ("G3→G3", "E_g3g3"), ("G3→G3→RL", "E_g3g3rl"),
    ("L34camp→REV", "E_l34camp_rev"),
    ("ND→SC→L46 🕐", "E_ndscl46"), ("NS→SC", "E_nssc"), ("G3→L46 🕐", "E_g3l46"),
    ("FailedBear-Turn", "E_failbear"),
    ("Z-Absorb-Turn", "E_zabsorb"),
    ("T1-CapBounce🌀SC", "E_t1capbounce_SC"), ("D+L1🌀SC", "E_dl1_SC"),
    ("Spring🌀SC", "E_spring_SC"), ("Atomic🌀SC", "E_atomic_SC"),
    ("H1-bottom🌀SC", "E_h1bottom_SC"), ("Washout🌀SC", "E_washout_SC"),
    # 🔑 Gate-strengthened variants (2026-07-22 sweep — era-balanced, worst-year improved):
    ("RTB-Base🧱OB", "E_rtb_base_ob"), ("FailedBear🧱OB", "E_failbear_ob"),
    ("G3→G3→RL🧱OB", "E_g3g3rl_ob"),
    ("RTB-Base🏆RS", "E_rtb_base_rs"), ("Z11-T11🏆RS", "E_z11t11_rs"),
    ("Washout🏆RS", "E_washout_rs"), ("D+L1🏆RS", "E_dl1_rs"), ("Spring🏆RS", "E_spring_rs"),
    ("Engulf-Abs🏆RS", "E_engulfabs_rs"), ("L43-TRIPLE🏆RS", "E_l43triple_rs"),
    ("Atomic🏆RS💎", "E_atomic_rsq"),
    ("QZ-Capit🔵dwell", "E_qzcapit_dwell"), ("D+L1🔵dwell", "E_dl1_dwell"),
    ("L43🔵dwell", "E_l43triple_dwell"),
    ("ZRT🟢L46", "E_zrt_l46"), ("ZRT🟢L46🔵dw", "E_zrt_l46_dw"),
    ("Washout🔎iv", "E_washout_iv"), ("ZRT🟢🔎iv", "E_zrt_l46_iv"),
    ("Washout💥vol", "E_washout_vs"),
    ("Z-Absorb💎$21-89", "E_zabsorb_q"),
    ("🧊Coil-Floor", "E_coilfloor"),
    ("🌀Engulf-AbsRev🟡", "E_engulf_absorb_rev"),
    ("🎯T1-RS-Dip", "E_t1_rs_dip"),
    ("🎯T3-RS-Dip", "E_t3_rs_dip"),
    ("🏆L34→L34", "E_l34cont"),
    ("🏆L34→L34+RS", "E_l34cont_rs"),
    ("G3-Abs🕯️mid", "E_g3abs_mid"), ("L43-TRIPLE🕯️mid", "E_l43triple_mid"),
    ("📐RSI-Div🏆RS", "E_rsidiv_rs"), ("📐RSI-Div🏆RS deep", "E_rsidiv_rs_deep"),
    ("🔄DualReclaim🏆RS", "E_dualrec_rs"), ("🔄DualReclaim deep", "E_dualrec_rs_deep"),
    ("Washout🕐DR", "E_washout_h1dr"), ("Zone-Retest🕐DR", "E_zrt_h1dr"),
    ("L43-TRIPLE🕐DR", "E_l43triple_h1dr"), ("RTB-Base🕐DR", "E_rtb_base_h1dr"),
    ("G3→G3🕐DR", "E_g3g3_h1dr"), ("QZ-Capit🕐DR", "E_qzcapit_h1dr"),
    ("🎬StopVol-Confirm", "E_stopvol_confirm"),
    ("🎬StopVol-Deep", "E_stopvol_confirm_deep"),
    ("Washout🧊CONSO", "E_washout_conso"), ("RTB-Base🧊CONSO", "E_rtb_base_conso"),
    ("L43-TRIPLE🔇QUIET", "E_l43triple_quiet"),
    ("🥇G3·LEAD-in-LAG", "E_g3_lead"),
    ("🥇G3A·LEAD-in-LAG", "E_g3abs_lead"),
    ("🥇L43·LEAD-in-LAG", "E_l43triple_lead"),
    ("🥪T2G-Sandwich🏆RS", "E_t2gsand_rs"),
    ("🪨T1G-NB🏆RS", "E_t1gnb_rs"),
    ("🪨+ T1G-NB·L34pre", "E_t1gnb_l34pre"),
    ("🌉Z1G→T4 🟡watch", "E_z1gt4"),
    ("🧲Z9-HL 🟡watch", "E_z9hl"),
    ("🌉v2 Z1G→T3/T6 🟡watch", "E_z1gt36"),
    ("🧺SEQ-20 🟡watch", "E_seq20"),
    ("👑Z1G-CROWN 🟡watch", "E_z1gcrown"),
]


# ── EDGE-fire display maps (Superchart EDGE row + Ultra screener EDGE column) ──
# Base setups only — the gated variants (🏆RS/🔑/🧱/🎋/🌀) are subsets of the same
# fires and would only duplicate chips. Short codes keep the chip row readable.
DISPLAY_SETUPS = [
    # 🔄 dual oversold reclaim (2026-07-28) — fires ~2.4x as often as the 📐 divergence edge,
    # so it earns a chip on the Superchart / Ultra EDGE row rather than a row of its own.
    ("🔄DR", "E_dualrec_rs"), ("🕐DR", "h1dr_chip"),
    ("CAP",  "E_t1capbounce"), ("QZC", "E_qzcapit"),  ("D+L1", "E_dl1"),
    ("G3",   "E_g3"),          ("⚡G3A", "E_g3abs"),   ("ATM",  "E_atomic"),
    ("ATMR", "E_atomicR"),     ("SPR", "E_spring"),   ("Z11",  "E_z11t11"),
    ("L43",  "E_l43triple"), ("L43🔇", "E_l43triple_quiet"),   ("WSH", "E_washout"),  ("H1B",  "E_h1bottom"),
    ("ENG",  "E_engulfabs"),   ("EL46", "E_engulfL46"), ("ZRT", "E_zoneretest"),
    ("HB15", "E_highbase15"),  ("RTB", "E_rtb_base"), ("P55",  "E_p55"),
    ("PAR",  "E_parabola"),    ("🎯3", "E_confluence"), ("🎯4", "E_confluence_p"),
    ("G3RL", "E_g3rl"), ("G3²", "E_g3g3"), ("G3²RL", "E_g3g3rl"),
    ("💠L34C", "E_l34camp_rev"),
    ("SC46", "E_ndscl46"), ("NSSC", "E_nssc"), ("G3L46", "E_g3l46"),
    ("⚔️FBT", "E_failbear"),
    ("💤ZAT", "E_zabsorb"),
    ("🧊CF", "E_coilfloor"),
    ("🌀EAR", "E_engulf_absorb_rev"),
    ("🎯T1RS", "E_t1_rs_dip"),
    ("🎯T3RS", "E_t3_rs_dip"),
    ("🏆L34C", "E_l34cont"),
    ("🎬SVC", "E_stopvol_confirm"),
    # 🥪 exception to "base setups only": the RS-gated variant IS the setup here — the
    # un-gated pattern is toxic (worst −11.5), so the chip must carry the gate.
    ("🥇G3", "E_g3_lead"), ("🥇G3A", "E_g3abs_lead"), ("🥇L43", "E_l43triple_lead"),
    ("🥪SAND", "E_t2gsand_rs"),
    # 🪨 same exception: RS is a REQUIRED component (worst −4.8 without), chip carries the gate
    ("🪨GNB", "E_t1gnb_rs"),
    ("🪨+L34", "E_t1gnb_l34pre"),
    # 🌉 WATCH-tier at the user's request — the chip carries the 🟡 so the screen says so too
    ("🌉Z1G4🟡", "E_z1gt4"),
    ("🧲Z9HL🟡", "E_z9hl"),
    ("🌉v2🟡", "E_z1gt36"),
    ("🧺SEQ🟡", "E_seq20"),
    ("👑Z1G🟡", "E_z1gcrown"),
    # QUALITY marker (exception to the "base setups only" rule, 2026-07-26): ZRT fires are
    # common (n138k, 4/6yr) and the L46 gate is what separates the 6/6yr subset — without this
    # chip a CSV/chart review cannot tell a good ZRT from a plain one. The tighter +dwell
    # variant is a subset of this and stays board-only to avoid stacking three chips per bar.
    ("ZRT🟢", "E_zrt_l46"),
    # 🔎 intraday-demand confirmed washout — the un-confirmed cell is a 1/6yr loser (med −5.28,
    # pf 0.72), so knowing WHICH washout has intraday demand matters on a chart/CSV review.
    ("WSH🔎", "E_washout_iv"),
    # 💥 washout with a real intraday volume event (the universal filter) — its absence is a
    # 3/6yr med −5.04 cell, so this marker matters on a chart/CSV review.
    ("WSH💥", "E_washout_vs"),
]

# ── self-learned mined combos: brain/miner.py validates base×conditioner masks OOS (walk-forward
# + worst-year + DSR + family-PBO) and writes the survivors to brain/mined_combos.json. Loaded here
# so _prep builds their masks and the brain spine can fire them. Kept OUT of DISPLAY_SETUPS so they
# never clutter the Superchart/Ultra EDGE chip rows — they ride a separate MINED_DISPLAY list that
# only latest_edges_map (the brain's feed) consumes. Fully guarded: absent/bad file = no-op. ──
_MINED_COMBOS = []
MINED_DISPLAY = []


def _refresh_mined():
    """(Re)load the promoted mined combos from brain/mined_combos.json and register them as
    scoreable setups + the brain's fire feed. Called at import and at every frame build, so a
    freshly-promoted combo becomes live on the next _frame() without needing a code change."""
    global _MINED_COMBOS, MINED_DISPLAY
    import json as _json, os as _os
    path = _os.path.join(_os.path.dirname(__file__), "brain", "mined_combos.json")
    try:
        _MINED_COMBOS = (_json.load(open(path)) or []) if _os.path.exists(path) else []
    except Exception:
        _MINED_COMBOS = []
    MINED_DISPLAY = [(mc["id"], mc["id"]) for mc in _MINED_COMBOS]   # code == col == id
    have = {c for _, c in SETUPS}
    for mc in _MINED_COMBOS:                                         # scoreable by revalidate
        if mc["id"] not in have:
            SETUPS.append((mc.get("display", mc["id"]), mc["id"]))


_refresh_mined()

_EDGE_TK_CACHE: dict = {}          # ticker -> (built_ts, {'YYYY-MM-DD': [codes]})
_EDGE_MAP_CACHE: list = [0.0, {}]  # [built_ts, {ticker: [(code, age_bars)]}]


def _collapse_codes(codes: list) -> list:
    """Collapse NESTED display chips so a bar shows each family once, at its strongest tier.

    Several DISPLAY_SETUPS entries are strict subsets of one another, so they always fire
    together and the chip row repeats itself (user, 2026-07-28: "WSH shows three times", "are
    🎯3 and 🎯4 not the same thing?"). They were:
      E_confluence_p (conf_n>=4) ⊂ E_confluence (conf_n>=3)      → 🎯4 implies 🎯3
      E_washout_iv / E_washout_vs ⊂ E_washout                     → WSH🔎 / WSH💥 imply WSH
    Rules: keep only the HIGHEST confluence tier, and fold the washout qualifiers into ONE
    chip ("WSH🔎💥") instead of three. 🔎 and 💥 are NOT nested in each other (intraday demand
    line vs intraday volume event), so a bar can legitimately carry either or both — which is
    why they are appended as markers rather than collapsed away.
    """
    out = list(codes)
    # confluence ladder — highest tier wins
    for lo, hi in (("🎯3", "🎯4"),):
        if hi in out and lo in out:
            out.remove(lo)
    # washout qualifiers — one chip carrying its markers
    quals = [q for c, q in (("WSH🔎", "🔎"), ("WSH💥", "💥")) if c in out]
    if quals:
        out = [c for c in out if c not in ("WSH🔎", "WSH💥")]
        merged = "WSH" + "".join(quals)
        out = [merged if c == "WSH" else c for c in out]
        if merged not in out:
            out.append(merged)
    # L43 quiet-tape qualifier (2026-07-30) — ONE chip at two strengths rather than a second
    # chip, because L43🔇 is a strict subset of L43 and a separate entry would just repeat it.
    # The quiet variant is the validated one: +2.72 → +3.43, pf 1.88 → 2.08, 6/6 years,
    # worst +0.9 → +0.1, DSR 1.000 vs 20 trials. The frontend brightens on the 🔇 suffix.
    if "L43🔇" in out:
        out = [c for c in out if c != "L43"]
    return out


def _edges_from_group(g) -> dict:
    """{date: [codes]} for one prepped per-ticker frame."""
    out = {}
    ds = g["date"].astype(str).str[:10].to_numpy()
    for code, col in DISPLAY_SETUPS:
        if col not in g:
            continue
        for d in ds[g[col].to_numpy(bool)]:
            out.setdefault(d, []).append(code)
    return {d: _collapse_codes(c) for d, c in out.items()}


def ticker_edges(ticker: str, months: int = 16) -> dict:
    """Per-bar Edge fires for ONE ticker: {'YYYY-MM-DD': ['G3', '🎯3', …]}.
    Uses the warm (60, 3M) frame when the ticker is in it (zero cost); otherwise a
    single-ticker build with the SAME 3M dv floor — the floor drops low-volume days,
    which shifts every rolling feature, so a floorless fallback produces fire dates
    that disagree with the backtest and the screener (EOLS 2026-07-20: RTB showed
    07-15/16 floorless vs 07-16/17 in the real frame). TTL 1h."""
    import time
    tk = str(ticker).upper()
    hit = _EDGE_TK_CACHE.get(tk)
    if hit and (time.time() - hit[0]) < 3600:
        return hit[1]
    out = {}
    from_warm = False
    try:
        warm = _CACHE.get((60, 3_000_000))
        if warm and tk in warm[0]:
            out = _edges_from_group(warm[0][tk])
            from_warm = True
        else:
            df, _ = _pull(months, 3_000_000, ticker=tk)
            if len(df) >= 60:
                out = _edges_from_group(_prep(df))
    except Exception:
        log.warning("ticker_edges failed for %s", tk, exc_info=True)
    # never cache failures: an empty result off the FALLBACK path (warm frame still
    # pre-warming ~230s after restart, or a build error) used to get cached for 1h and the
    # chart showed no EDGE chips for an hour (caught live 2026-08-04 on DXCM). An empty
    # result from the WARM frame is a real "no fires" and is safe to cache.
    if not out and not from_warm:
        return out
    _EDGE_TK_CACHE[tk] = (time.time(), out)
    while len(_EDGE_TK_CACHE) > 400:
        _EDGE_TK_CACHE.pop(next(iter(_EDGE_TK_CACHE)))
    return out


_ATR_MAP_CACHE = [0.0, None]
def latest_atr_map() -> dict:
    """{ticker: atr_pct} (ATR14/close of the latest bar) from the warm (60,3M) frame — for the
    ATR time-to-target forecast on the Ultra screener. Cold frame → {} (no blocking build). TTL 1h."""
    import time
    if _ATR_MAP_CACHE[1] and (time.time() - _ATR_MAP_CACHE[0]) < 3600:
        return _ATR_MAP_CACHE[1]
    if (60, 3_000_000) not in _CACHE:
        return {}
    try:
        grp, _ = _frame(60, 3_000_000)
        m = {}
        for tk, g in grp.items():
            if len(g):
                cl = float(g["close"].iloc[-1]); at = float(g["atr_14"].iloc[-1])
                if cl > 0 and at == at:
                    m[tk] = round(at / cl, 4)
        _ATR_MAP_CACHE[0] = time.time(); _ATR_MAP_CACHE[1] = m
        return m
    except Exception:
        log.debug("latest_atr_map failed", exc_info=True)
        return {}


_DIV_MAP_CACHE: list = [0.0, {}]


def latest_div_map(lookback: int = 5) -> dict:
    """{ticker: {"buy": age|None, "deep": bool, "top": age|None, "rsi": float}} — the 📐 divergence
    state of the last `lookback` bars, for the Ultra column and the Superchart row (2026-07-28).

    `buy` = E_rsidiv_rs fired (bull divergence + RS-intact + rsi<45) · `deep` = the rsi<40 tier ·
    `top` = the 🔻 suppressor (bear divergence + rsi>65 + RS BROKEN). Ages are in bars, 0 = today.
    Cold frame → {} (never blocks a scan to build one). TTL 1h.
    """
    import time
    if _DIV_MAP_CACHE[1] and (time.time() - _DIV_MAP_CACHE[0]) < 3600:
        return _DIV_MAP_CACHE[1]
    if (60, 3_000_000) not in _CACHE:
        return {}
    try:
        grp, _ = _frame(60, 3_000_000)
        m = {}
        for tk, g in grp.items():
            n = len(g)
            if not n:
                continue
            tail = g.iloc[-lookback:]
            ent = {}
            for col, key in (("E_rsidiv_rs", "buy"), ("E_rsidiv_rs_deep", "deep"), ("div_top", "top")):
                if col not in tail.columns:
                    continue
                v = tail[col].to_numpy(bool)
                w = np.nonzero(v)[0]
                if len(w):
                    ent[key] = int(len(v) - 1 - w[-1])          # bars ago, 0 = latest bar
            if ent:
                # the RSI at the pivot that produced the freshest fire — what the tooltip shows
                for c in ("dv_rsi_lo", "dv_rsi_hi"):
                    if c in tail.columns:
                        vals = tail[c].to_numpy(float)
                        ok = np.isfinite(vals)
                        if ok.any():
                            ent["rsi_lo" if c == "dv_rsi_lo" else "rsi_hi"] = round(float(vals[ok][-1]), 1)
                m[tk] = ent
        _DIV_MAP_CACHE[0] = time.time(); _DIV_MAP_CACHE[1] = m
        return m
    except Exception:
        log.debug("latest_div_map failed", exc_info=True)
        return {}


_H1DR_DAYS_CACHE: list = [0.0, frozenset()]


def h1_dr_days(lookback: int = 45) -> frozenset:
    """{'TICKER|YYYY-MM-DD'} for which the 🕐 1H-DR confirmation is ON, over the last
    `lookback` sessions — so the live Edge-board scanners can show/filter the same gate the
    Replay board and the backtest use (2026-07-28).

    The gate = a 1H dual reclaim that session or the previous one, AND that day's RS intact.
    Reads the warm frame only; a cold frame returns an empty set and the badge simply never
    shows rather than blocking a scan. TTL 1h.
    """
    import time
    if _H1DR_DAYS_CACHE[1] and (time.time() - _H1DR_DAYS_CACHE[0]) < 3600:
        return _H1DR_DAYS_CACHE[1]
    if (60, 3_000_000) not in _CACHE:
        return frozenset()
    try:
        grp, _ = _frame(60, 3_000_000)
        out = set()
        for tk, g in grp.items():
            if "h1_dr" not in g.columns or not len(g):
                continue
            tail = g.iloc[-lookback:]
            m = tail["h1_dr"].to_numpy(bool)
            if m.any():
                for d in tail["date"].astype(str).str[:10].to_numpy()[m]:
                    out.add(f"{tk}|{d}")
        _H1DR_DAYS_CACHE[0] = time.time(); _H1DR_DAYS_CACHE[1] = frozenset(out)
        return _H1DR_DAYS_CACHE[1]
    except Exception:
        log.debug("h1_dr_days failed", exc_info=True)
        return frozenset()


def latest_edges_map(lookback: int = 5, build: bool = False) -> dict:
    """{ticker: [(code, age_bars)]} — Edge fires within the last `lookback` bars,
    from the warm (60, 3M) frame. With build=False (screener path) a cold frame
    returns {} instead of blocking the scan for a multi-minute build; the startup
    warmer fills it shortly after boot. TTL 1h."""
    import time
    if _EDGE_MAP_CACHE[1] and (time.time() - _EDGE_MAP_CACHE[0]) < 3600:
        return _EDGE_MAP_CACHE[1]
    if not build and (60, 3_000_000) not in _CACHE:
        return {}
    try:
        from datetime import date as _date
        grp, as_of = _frame(60, 3_000_000)
        # Age = CALENDAR days behind the global as_of, NOT bars behind the ticker's
        # own last frame row (EOLS bug 2026-07-20: a ticker whose latest day fails
        # the dv floor has an older "last bar", so index-age mislabels an old fire
        # as "today" — and the EDGE🟢 premium combo lights on a bar with no edge).
        _asof = _date.fromisoformat(str(as_of)[:10])
        m = {}
        for tk, g in grp.items():
            n = len(g)
            fires = []
            ds = g["date"].astype(str).str[:10].to_numpy()
            for code, col in DISPLAY_SETUPS + MINED_DISPLAY:
                if col not in g:
                    continue
                e = g[col].to_numpy(bool)
                for i in range(max(0, n - lookback - 3), n):
                    if e[i]:
                        age = (_asof - _date.fromisoformat(ds[i])).days
                        if age <= lookback:
                            fires.append((code, age))
            if fires:
                # collapse nested chips WITHIN each age bucket (a 🎯4 and the 🎯3 it implies
                # always share a bar, so they always share an age) — same rule the per-bar
                # Superchart row uses, so the two views cannot disagree.
                by_age = {}
                for code, age in fires:
                    by_age.setdefault(age, []).append(code)
                fires = [(c, a) for a, cs in by_age.items() for c in _collapse_codes(cs)]
                m[tk] = sorted(fires, key=lambda x: x[1])
        _EDGE_MAP_CACHE[0] = time.time()
        _EDGE_MAP_CACHE[1] = m
        return m
    except Exception:
        log.debug("latest_edges_map failed", exc_info=True)
        return {}


def _pathsim(grp: dict, col: str, mode: str, stop: float, target: float,
             trail: float, maxh: int, slip: float = None,
             atr_k: float = None) -> pd.DataFrame:
    """GAP-REALISTIC fills (2026-07-03 backtest-expert audit fix):
    when a bar OPENS through the stop/trail level (overnight gap), the fill is the
    OPEN (what you'd actually get), not the stop price. Target fills stay AT target
    (no gap-up bonus) — pessimism is one-sided by design. `slip` overrides SLIP for
    stress runs (e.g. 2×=30bps each way). Trades carry date_in/date_out for
    portfolio-level simulation.

    ATR-ADAPTIVE TRAIL (2026-08-06, user-approved build): `atr_k` set (trail mode only)
    makes each trade's trail = clip(atr_k * atr_14/close at the SIGNAL bar, 15%, 60%)
    instead of the fixed `trail`. Validated over 6yr on all 49 board setups: med
    improved 49/49, worst-year 45/49 (pooled REV family +0.19→+1.43, worst −2.60→−1.06,
    2022 −2.60→−0.72). k-plateau flat over 10..16 (clip saturates) → k=12 chosen.
    Root cause it fixes: a fixed 25% trail sits inside a volatile name's noise band —
    the "⚡-segment edge weakness" was entirely this exit artifact (hold-20-no-stop
    equalized all segments). Fixed trail stays the DEFAULT; this is additive."""
    S = SLIP if slip is None else slip
    risk = trail if mode == "trail" else stop     # planned initial risk per trade (for R-multiple)
    trades = []
    for tk, gdf in grp.items():
        if col not in gdf:
            continue
        o = gdf["open"].to_numpy(float); hi = gdf["high"].to_numpy(float)
        lo = gdf["low"].to_numpy(float); cl = gdf["close"].to_numpy(float)
        if atr_k is not None and mode == "trail" and "atr_14" in gdf:
            with np.errstate(invalid="ignore", divide="ignore"):
                _tr_arr = np.clip(np.nan_to_num(
                    atr_k * gdf["atr_14"].to_numpy(float) / np.where(cl > 0, cl, np.nan),
                    nan=trail), 0.15, 0.60)
        else:
            _tr_arr = None
        ent = gdf[col].to_numpy(bool); n = len(gdf); last = -99
        dfull = gdf["date"].astype(str).to_numpy()
        dts = gdf["date"].astype(str).str[:4].to_numpy()
        for i in range(n - 1):
            if not ent[i] or i + 1 >= n or i - last < 5:
                continue
            ep = o[i + 1]
            if ep <= 0:
                continue
            last = i
            _tr = trail if _tr_arr is None else float(_tr_arr[i])   # per-trade trail (ATR mode)
            entry = ep * (1 + S); ret = None; end = min(i + 1 + maxh, n); pk = entry
            jout = end - 1; mlo = entry; mhi = entry        # trough/crest for MAE/MFE (heat)
            for j in range(i + 1, end):
                if lo[j] < mlo: mlo = lo[j]                  # track path excursion up to (incl.) exit bar
                if hi[j] > mhi: mhi = hi[j]
                if mode == "trail":
                    ts_prev = pk * (1 - _tr)            # trail level from PRIOR peak
                    if j > i + 1 and o[j] <= ts_prev:    # gapped through overnight → fill at open
                        ret = o[j] / entry - 1 - S; jout = j; break
                    pk = max(pk, hi[j]); ts = pk * (1 - _tr)
                    if lo[j] <= ts:                      # intrabar touch → fill at trail level
                        ret = ts / entry - 1 - S; jout = j; break
                else:
                    sl = entry * (1 - stop)
                    if j > i + 1 and o[j] <= sl:         # gapped through the stop → fill at open
                        ret = o[j] / entry - 1 - S; jout = j; break
                    if lo[j] <= sl:
                        ret = -stop - S; jout = j; break
                    if hi[j] >= entry * (1 + target):    # target fills AT target (no gap-up bonus)
                        ret = target - S; jout = j; break
            if ret is None:
                ret = cl[end - 1] / entry - 1 - S
            trades.append({"ticker": tk, "ret": ret, "yr": dts[i],
                           "date_in": dfull[i + 1], "date_out": dfull[jout],
                           "mae": mlo / entry - 1,          # max adverse excursion (≤0 heat taken)
                           "mfe": mhi / entry - 1,          # max favorable excursion
                           "hold": int(jout - i),           # bars held (entry@i+1 → exit@jout)
                           "risk": (_tr if mode == "trail" else risk)})
    return pd.DataFrame(trades)


def _stats(name: str, tr: pd.DataFrame) -> dict:
    if len(tr) == 0:
        return {"setup": name, "n": 0}
    wins = tr["ret"] > 0
    pf_n = tr.loc[wins, "ret"].sum(); pf_d = -tr.loc[~wins, "ret"].sum()
    pf = round(pf_n / pf_d, 2) if pf_d > 0 else None
    yrs = tr.groupby("yr")["ret"].mean()
    pos_yrs = int((yrs > 0).sum())
    # ── risk-reward block (path-aware; supersedes fixed-horizon fwd-return) ──
    _mret = float(tr["ret"].mean())
    _risk = float(tr["risk"].iloc[0]) if "risk" in tr.columns else 0.10
    exp_r = round(_mret / _risk, 3) if _risk > 0 else None            # expectancy in R (mean ÷ planned risk)
    _aw = tr.loc[wins, "ret"].mean(); _al = tr.loc[~wins, "ret"].mean()
    payoff = round(float(_aw / -_al), 2) if (wins.any() and (~wins).any() and _al < 0) else None
    _dn = tr.loc[tr["ret"] < 0, "ret"]                               # downside deviation → Sortino
    _dd = float(_dn.std(ddof=1)) if len(_dn) > 1 else 0.0
    sortino = round(_mret / _dd, 3) if _dd > 0 else None
    med_mae = round(float(tr["mae"].median()) * 100, 2) if "mae" in tr.columns else None   # typical heat
    med_mfe = round(float(tr["mfe"].median()) * 100, 2) if "mfe" in tr.columns else None
    avg_hold = round(float(tr["hold"].mean()), 1) if "hold" in tr.columns else None
    # concentration: top-10% of tickers' share of total positive PnL
    by_tk = tr.groupby("ticker")["ret"].sum().sort_values(ascending=False)
    tot = by_tk[by_tk > 0].sum()
    top10 = by_tk.head(max(1, len(by_tk) // 10)).clip(lower=0).sum()
    conc = round(top10 / tot * 100, 0) if tot > 0 else None
    return {
        "setup": name, "n": int(len(tr)),
        "mean": round(float(tr["ret"].mean()) * 100, 2),
        "median": round(float(tr["ret"].median()) * 100, 2),
        "win": round(float(wins.mean()) * 100, 1),
        "pf": pf, "pos_years": pos_yrs, "total_years": int(len(yrs)),
        "best_year": round(float(yrs.max()) * 100, 1),
        "worst_year": round(float(yrs.min()) * 100, 1),
        "conc_top10pct": conc,
        # risk-reward (path-aware): expectancy in R, payoff ratio, Sortino, typical heat (MAE), hold
        "exp_r": exp_r, "payoff": payoff, "sortino": sortino,
        "med_mae": med_mae, "med_mfe": med_mfe, "avg_hold": avg_hold,
        "per_year": {y: round(float(v) * 100, 2) for y, v in yrs.items()},
    }


# Upper price gate shared by the quality-zone edges. 89 was the original "quality zone" cap
# ([[project_fib_price_zones]]); a 2026-07-26 Fibonacci sweep showed per-trade quality keeps
# improving above it, so this is a single knob to sweep/raise for ALL capped edges at once.
PRICE_CAP = 89
# Widened cap for the edges where a 2026-07-26 Fibonacci sweep showed $21-377 is as good or
# BETTER than $21-89 (per-trade quality keeps improving with price: win% up, catastrophe down).
# Applied per-edge, NOT globally — qzcapit(base)/qzcapit_key/atomic_rsq/t1_rs_dip degrade to 4/6yr
# (t1_rs_dip catastrophically: worst −24) and deliberately stay at 89. 377+ is excluded everywhere
# (3/6yr on its own, 2021 −6.8). See [[project_fib_price_zones]].
PRICE_CAP_WIDE = 377

from collections import OrderedDict
_CACHE: "OrderedDict" = OrderedDict()
_CACHE_MAX = 2   # LRU: the CANONICAL frame is pinned (never evicted) + at most 1 other.
                 # (2026-07-04 fix: was single-entry `_CACHE.clear()` → any window switch evicted
                 # the others, so a warmed 24mo vanished the moment 36mo was requested.)
                 # (2026-07-29: was 3. Each frame is the WHOLE universe of bars in pandas —
                 # ~2-3 GB — so three of them plus the boot pre-warm took the backend to a
                 # 19 GB peak on a 16 GB machine. The machine went to swap and the nightly
                 # delta worker, a sibling process, was the one that died. Pinning the
                 # canonical key keeps window switches from ever evicting the frame that
                 # every scanner needs, so 2 buys back more than the old 3 did.)
_CANON_KEY = (60, 3_000_000)   # the frame every scanner/Superchart/Ultra path asks for


def _evict_frames():
    """Trim _CACHE to _CACHE_MAX, never dropping the canonical frame.

    Callers must already hold _FRAME_LOCK. Plain LRU could evict (60, 3M) — the frame
    Superchart/Ultra/the edge chips all need — after two Replay window switches, and
    rebuilding it costs ~107s during which those views render empty.
    """
    while len(_CACHE) > _CACHE_MAX:
        for k in _CACHE:                       # oldest first
            if k != _CANON_KEY or len(_CACHE) == 1:
                _CACHE.pop(k)
                break
        else:                                  # only the canonical key is left
            break


_RS_REF: dict = {}



_VIX_REF = {}


def _load_vix_ref():
    """VIXY daily closes (date-str index) for the 🌡️ macro-VIX flag. Read from the analytics
    DB — VIXY sits below the frame's dollar-volume floor so it never appears in the frame
    itself. Cached for the process lifetime."""
    if "s" in _VIX_REF:
        return _VIX_REF["s"]
    try:
        import duckdb
        from studio.paths import ANALYTICS_DB
        c = duckdb.connect(ANALYTICS_DB, read_only=True)
        d = c.execute("""SELECT substr(CAST(date AS VARCHAR),1,10) d, any_value("close") c
                         FROM bars WHERE ticker='VIXY' GROUP BY date ORDER BY date""").fetchdf()
        c.close()
        _VIX_REF["s"] = pd.Series(d["c"].to_numpy(float), index=d["d"].to_numpy())
    except Exception:
        _VIX_REF["s"] = None
    return _VIX_REF["s"]


def _load_rs_ref():
    """(etf_price_df, sector_map) for the 🏆RS flag. Loads data/etf_px.parquet (SPY + 11 XL*
    sector ETFs, date-str index) + data/sector_map.json. If the parquet is >5 days stale and a
    Massive key is available, auto-extends it in place (best-effort; falls back to stale data).
    Cached for the process lifetime — frames are rebuilt often enough."""
    if "px" in _RS_REF:
        return _RS_REF["px"], _RS_REF["smap"]
    import json as _json
    _dd = os.path.join(os.path.dirname(__file__), "..", "data")
    px, smap = None, {}
    try:
        px = pd.read_parquet(os.path.join(_dd, "etf_px.parquet"))
        with open(os.path.join(_dd, "sector_map.json")) as f:
            smap = _json.load(f)
        last = str(px.index[-1])
        if (pd.Timestamp.now() - pd.Timestamp(last)).days > 5:
            try:                                  # stale — extend via Massive (never yfinance)
                from data import fetch_ohlcv
                fresh = {}
                for et in px.columns:
                    d = fetch_ohlcv(et, interval="1d", bars=60).reset_index()
                    dc = [c for c in d.columns if str(c).lower() in ("date", "datetime", "index", "timestamp")][0]
                    fresh[et] = pd.Series(d["close"].values,
                                          index=pd.to_datetime(d[dc]).dt.strftime("%Y-%m-%d"))
                fx = pd.DataFrame(fresh)
                px = pd.concat([px[~px.index.isin(fx.index)], fx]).sort_index()
                try:
                    px.to_parquet(os.path.join(_dd, "etf_px.parquet"))
                except Exception:
                    pass
            except Exception:
                pass                              # keep the stale parquet
    except Exception:
        px = None
    _RS_REF["px"], _RS_REF["smap"] = px, smap
    return px, smap


_OB_DAYS = {}


def _load_ob_days():
    """frozenset of 'TICKER|YYYY-MM-DD' where price retested a ≤8-bar-old bullish order block.
    Precomputed snapshot in data/ob_days.json (build_ob.py / nightly). Process-cached; returns
    empty set if missing (masks just stay empty). Recent days are causal (OB confirmed at +3 bars)."""
    if "s" in _OB_DAYS:
        return _OB_DAYS["s"]
    import json as _json
    try:
        p = os.path.join(os.path.dirname(__file__), "..", "data", "ob_days.json")
        with open(p) as f:
            _raw = _json.load(f)
        _OB_DAYS["s"] = frozenset(f"{tk}|{d}" for tk, days in _raw.items() for d in days)
    except Exception:
        _OB_DAYS["s"] = frozenset()
    return _OB_DAYS["s"]


def refresh_ob_days():
    """Recompute data/ob_days.json from the freshest bars (called by the nightly DB refresh AFTER
    the staging swap), then RE-WARM the hot frames in place so post-refresh scans hit warm caches
    instead of each triggering a cold-build storm (the 2026-07-16 thrash bug: the old version did
    _CACHE.clear() + rebuilt only ONE frame, so the 60mo warmup frames were evicted and every scan
    cold-built → 99% CPU / timeouts / a manual restart needed). Heavy (~2-3min) — nightly only.
    Mirror of build_ob.py; keep the OB rule in sync there."""
    import json as _json
    W = 8
    # 1. fresh 72mo frame (throwaway ob_retest — we only need OHLC to detect order blocks),
    #    built under the lock so it doesn't race concurrent scans.
    _OB_DAYS.pop("s", None)
    with _FRAME_LOCK:
        _df, _asof = _pull(72, 3_000_000)
        _df = _prep(_df)
        grp = {tk: g.reset_index(drop=True) for tk, g in _df.groupby("ticker", sort=False)}
    out = {}
    for tk, k in grp.items():
        o = k["open"].to_numpy(float); cl = k["close"].to_numpy(float); lo = k["low"].to_numpy(float)
        ds = k["date"].astype(str).str[:10].to_numpy(); n = len(k)
        ob = np.zeros(n, dtype=bool)
        for j in range(1, n - 3):
            if cl[j] < o[j] and (cl[j + 3] / cl[j] - 1) > 0.04:
                ob[j] = True
        recent = []; days = []
        for i in range(n):
            recent = [(hh, ll, a + 1) for (hh, ll, a) in recent if a + 1 <= W]
            if ob[i]:
                recent.append((max(o[i], cl[i]), min(o[i], cl[i]), 0))
            for (hh, ll, a) in recent:
                if lo[i] <= hh and cl[i] >= ll:
                    days.append(ds[i]); break
        if days:
            out[tk] = sorted(set(days))
    p = os.path.join(os.path.dirname(__file__), "..", "data", "ob_days.json")
    with open(p, "w") as f:
        _json.dump(out, f)
    _OB_DAYS.pop("s", None)                 # next frame build reads the fresh json
    # 2. re-warm the hot frames (warmup keys + replay default) so their ob_retest reflects the new
    #    dayset and the cache is never left empty — each replaces one entry under the lock.
    for _key in ((60, 3_000_000), (60, 5_000_000), (72, 3_000_000)):
        try:
            with _FRAME_LOCK:
                _d2, _as2 = _pull(*_key)
                _d2 = _prep(_d2)
                _CACHE[_key] = ({tk: g.reset_index(drop=True)
                                 for tk, g in _d2.groupby("ticker", sort=False)}, _as2)
                _CACHE.move_to_end(_key)
                _evict_frames()
        except Exception:
            pass
    return sum(len(v) for v in out.values())


_FRAME_LOCK = threading.Lock()   # 2026-07-13: serialize builds — see below


def _frame(months: int, dv_floor: float):
    key = (months, dv_floor)
    if key in _CACHE:
        _CACHE.move_to_end(key)              # mark as most-recently-used
        return _CACHE[key]
    # STAMPEDE FIX (2026-07-13): a fresh page load fires several scans that all want the SAME
    # (60, 3M) frame; without a lock each concurrent miss rebuilt it independently (3× the same
    # multi-minute build → CPU storm, RAM spike, OOM in DuckDB, "app looks dead"). One global
    # lock: first caller builds, the rest wait and hit the cache; different keys also serialize,
    # which caps peak memory during warmup.
    with _FRAME_LOCK:
        if key in _CACHE:                    # built while we waited for the lock
            _CACHE.move_to_end(key)
            return _CACHE[key]
        df, as_of = _pull(months, dv_floor)
        df = _prep(df)
        grp = {tk: g.reset_index(drop=True) for tk, g in df.groupby("ticker", sort=False)}
        _CACHE[key] = (grp, as_of)
        _evict_frames()                      # LRU, but the canonical frame is pinned
        return _CACHE[key]


def edge_replay(setup: str = "all", months: int = 36, dv_floor: float = 3_000_000,
                mode: str = "trail", stop: float = 0.10, target: float = 0.25,
                trail: float = 0.25, maxh: int = 60, with_trades: bool = False,
                slip: float = None, atr_k: float = 12.0) -> dict:
    # atr_k=12 is the BOOK DEFAULT since 2026-08-06 (law_exit_geometry: 49/49 setups
    # improved over fixed trail25, 2022 included). Pass atr_k=0/None for the legacy
    # fixed-trail view — the Replay UI exposes both.
    grp, as_of = _frame(int(months), float(dv_floor))
    want = [s for s in SETUPS if setup == "all" or s[0].lower() == setup.lower()]
    if not want:
        want = SETUPS
    out = []
    trades_out = None
    for name, col in want:
        tr = _pathsim(grp, col, mode, stop, target, trail, maxh, slip=slip, atr_k=(atr_k or None))
        out.append(_stats(name, tr))
        if with_trades and setup != "all":
            trades_out = [{"ticker": r["ticker"], "year": r["yr"], "ret_pct": round(r["ret"] * 100, 2)}
                          for _, r in tr.sort_values("ret", ascending=False).iterrows()]
    out.sort(key=lambda x: (x.get("pf") or 0), reverse=True)
    res = {"as_of": as_of, "months": int(months),
           "exit": {"mode": mode, "stop": stop, "target": target, "trail": trail, "maxh": maxh,
                    "slip": SLIP if slip is None else slip,
                    "atr_k": atr_k},
           "rows": out}
    if trades_out is not None:
        res["trades"] = trades_out[:300]
    return res


if __name__ == "__main__":
    import sys
    r = edge_replay(months=24)
    print(f"as_of {r['as_of']}  exit={r['exit']}")
    print(f"{'setup':14}{'n':>7}{'mean%':>7}{'win%':>7}{'PF':>6}{'yrs+':>7}{'conc%':>7}")
    for x in r["rows"]:
        if x["n"] == 0:
            print(f"{x['setup']:14}{'0':>7}"); continue
        print(f"{x['setup']:14}{x['n']:>7}{x['mean']:>7.2f}{x['win']:>7.1f}{x['pf'] or 0:>6.2f}"
              f"{str(x['pos_years'])+'/'+str(x['total_years']):>7}{x['conc_top10pct'] or 0:>7.0f}")
