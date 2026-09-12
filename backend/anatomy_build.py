"""▽△ BOTTOM-ANATOMY — the per-session verdict and its 0-8 score, for the WHOLE universe and the
WHOLE history, ported out of the per-ticker /api/day1h path.

WHY THIS EXISTS (2026-09-10, user: "orive gaakete" — build the history, then measure the ladder).
The ▽△ row on the Superchart is computed live, one ticker at a time, inside `/api/day1h`, and
`bottom_anatomy.latest_anatomy_map()` covers the whole universe but only its LAST bar. So the
verdict could be READ on a chart and never MEASURED across the book: there was no (ticker, session)
history to run a family on, and no column for the CSV. This builds it once, nightly.

DEFINITION — transcribed from `main.py::api_day1h`, unchanged. Three axes, each an integer, summed
into the score the label prints as `s/8`:

  a_loc 0-2   held  = the 25-bar range is tight (<= 35 %) AND today's low sits within 6 % of that
                      window's low  ·  key = >= 2 of the prior 25 lows within 1 % of the window low
  a_abs 0-3   (1H Z-absorption codes >= 1) + (>= 3) + (15m Z-absorption codes >= 4)
  a_rev 0-3   low_early (the day's 1H low lands in the first half) + close_upper (close in the top
              half of the day's range) + (an absorption Z in the first third AND a T in the last)

  at_floor = held OR (pos < 0.40 AND (key OR a_abs >= 2))     pos = close in the 25-bar range
  verdict  = rev    at_floor AND a_abs >= 1 AND a_rev >= 2
             shake  at_floor AND a down day AND a weak close (<= 40 % of range) AND a LATE hi-vol
                    T-reversal on 1H or 15m — the tell a weak daily bar hides
             cont   pos >= 0.6 AND the daily bar is up AND close_upper AND t1h >= z1h AND not held
             ''     none of the above
  rs       = close/SPY above its own EMA200 (alpha 2/201, >= 120 bars of warmup). 🔻 + rs = 🔻💪.

STATUS OF EVIDENCE. project_bottom_anatomy_mtf measured this as a DETECTOR, not a trade signal:
1.37x lift, 76 % recall, 33 % precision — it finds real lows and is wrong two times in three. That
test judged the VERDICT; the 0-8 SCORE ladder has never been measured, which is what
ANATOMY_LADDER_V1 is for. Nothing here is a ranking input.

OUTPUT
  data/anatomy_signals.parquet   one row per (ticker, session): v, s, loc, abs, rev, rs, plus the
                                 raw ingredients (z1h, t1h, z15, pos, held, key) for diagnosis.
  data/ANATOMY_SIGNALS_V1.json   spec + census.
RUN
  backend/.venv/bin/python backend/anatomy_build.py     # nightly, after the intraday derive
"""
from __future__ import annotations
import os
import sys
import json
import time

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from studio.paths import DATA_DIR                                        # noqa: E402
from studio.db import tf_db_path                                         # noqa: E402

OUT = os.path.join(DATA_DIR, "anatomy_signals.parquet")
SPEC = os.path.join(DATA_DIR, "ANATOMY_SIGNALS_V1.json")
ABSZ = ("Z1", "Z1G", "Z2", "Z2G", "Z5", "Z9", "Z10", "Z11")
ABSZ_SQL = "(" + ",".join(f"'{z}'" for z in ABSZ) + ")"
PX_MIN = 5.0                 # the day1h path's own floor
WIN = 25                     # daily location lookback
RS_LEN, RS_WARM = 201, 120


def location_axis(df: pd.DataFrame):
    """held / key / pos over the PRIOR `WIN` bars — the a_loc axis, and the one piece of this
    module that has already drifted from the chart once. `df` must be sorted by (ticker, date)
    with float high/low/close. Returns three arrays aligned to `df`.

    ⚠️ `key` counts the prior lows sitting within 1 % of ONE fixed threshold — the window low at
    bar i — exactly as main.py::api_day1h does:
        sum(1 for x in _dl[i-25:i] if x <= w_lo * 1.01) >= 2
    The first version wrote it as `near = lo_p <= wl * 1.01` and rolled the sum. That is
    elementwise: the low at row j gets compared against row j's OWN window low, a MOVING
    threshold. It agreed with the chart on only 54.7 % of days, bidirectionally (8,611 false
    positives, 9,061 false negatives), so it corrupted a_loc and the 0-8 score without biasing
    either. A sliding window makes the fixed threshold unavoidable; the test is
    backend/tests/test_anatomy_parity.py.

    Bars before the window is full stay blank — held False, key False, pos 0.5 — which is what
    api_day1h's `if i < 25` branch emits (it writes no "pos" key at all, and the reader defaults
    it to 0.5).
    """
    from numpy.lib.stride_tricks import sliding_window_view
    n = len(df)
    lowv = df["low"].to_numpy(float); highv = df["high"].to_numpy(float)
    closev = df["close"].to_numpy(float)
    held = np.zeros(n, bool); keyf = np.zeros(n, bool); pos = np.full(n, 0.5)
    start = 0
    for _tk, cnt in df.groupby("ticker", sort=False).size().items():
        a, b = start, start + cnt; start = b
        if cnt <= WIN:
            continue
        Lg, Hg, Cg = lowv[a:b], highv[a:b], closev[a:b]
        swl = sliding_window_view(Lg, WIN)[:cnt - WIN]        # swl[k] == Lg[k : k+WIN], k = i-WIN
        swh = sliding_window_view(Hg, WIN)[:cnt - WIN]
        w_lo, w_hi = swl.min(axis=1), swh.max(axis=1)
        e = a + WIN                                            # first evaluable bar of this ticker
        with np.errstate(invalid="ignore", divide="ignore"):
            rng = np.where(w_lo > 0, (w_hi - w_lo) / w_lo, 9.0)
            held[e:b] = (rng <= 0.35) & ((Lg[WIN:] - w_lo) / w_lo <= 0.06)
            pos[e:b] = np.where(w_hi > w_lo, (Cg[WIN:] - w_lo) / (w_hi - w_lo), 0.5)
            keyf[e:b] = (swl <= w_lo[:, None] * 1.01).sum(axis=1) >= 2
    return held, keyf, pos


def _daily(log=print) -> pd.DataFrame:
    """held / key / pos / up / rs per (ticker, session) — the LOCATION axis and the RS badge."""
    import duckdb
    con = duckdb.connect(tf_db_path("1d"), read_only=True)
    try:
        df = con.execute(f"""
            SELECT ticker, substr(CAST(date AS VARCHAR),1,10) AS session, open, high, low, close
            FROM bars WHERE close >= {PX_MIN} AND universe <> 'index'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY
              CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2
                            WHEN 'russell2k' THEN 3 ELSE 4 END) = 1
            ORDER BY ticker, date""").fetchdf()
        spy = con.execute(
            "SELECT substr(CAST(date AS VARCHAR),1,10) d, close FROM bars "
            "WHERE ticker = 'SPY' ORDER BY date").fetchdf()
    finally:
        con.close()
    log(f"  daily rows {len(df):,} · {df['ticker'].nunique():,} tickers · SPY {len(spy):,}")
    df["held"], df["key"], df["pos"] = location_axis(df)
    closev = df["close"].to_numpy(float)
    df["up"] = df["close"].to_numpy() >= df["open"].to_numpy()
    # RS: close/SPY vs its own EMA200, per ticker, warmup 120
    sp = dict(zip(spy["d"], spy["close"].astype(float)))
    df["_spy"] = df["session"].map(sp)
    a = 2.0 / RS_LEN
    rs_flag = np.zeros(len(df), bool)
    tk = df["ticker"].to_numpy()
    ratio = np.where((df["_spy"].to_numpy(float) > 0),
                     closev / np.where(df["_spy"].to_numpy(float) > 0, df["_spy"].to_numpy(float), 1), np.nan)
    prev = None; cnt = 0; cur = None
    for i in range(len(df)):
        if cur != tk[i]:
            cur, prev, cnt = tk[i], None, 0
        r = ratio[i]
        if not np.isnan(r):
            prev = r if prev is None else a * r + (1 - a) * prev
            cnt += 1
            if cnt >= RS_WARM and r > prev:
                rs_flag[i] = True
    df["rs"] = rs_flag
    return df[["ticker", "session", "held", "key", "pos", "up", "rs"]]


def _hourly(log=print) -> pd.DataFrame:
    """The 1H session shape: low_early, close_upper/weak, z_first & t_last, z1h, t1h, down_day,
    and the late hi-vol T-reversal. Volume class uses the Pine bands (basis +/- population sd)."""
    import duckdb
    con = duckdb.connect(tf_db_path("1h"), read_only=True)
    try:
        df = con.execute(f"""
            SELECT ticker, substr(CAST(date AS VARCHAR),1,10) AS session, date,
                   open, high, low, close, volume,
                   coalesce(t_sig,'') t, coalesce(z_sig,'') z
            FROM bars ORDER BY ticker, date""").fetchdf()
    finally:
        con.close()
    log(f"  1h rows {len(df):,}")
    v = df["volume"].astype(float)
    basis = v.groupby(df["ticker"], sort=False).rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    sd = v.groupby(df["ticker"], sort=False).rolling(20, min_periods=20).std(ddof=0).reset_index(level=0, drop=True)
    vb, vs = basis.to_numpy(float), sd.to_numpy(float)
    vol = v.to_numpy(float)
    # vclass >= 3 (B or VB) is what the late-reversal tell needs
    hv = np.where(np.isnan(vb), False, vol >= (vb + vs))
    df["_hv"] = hv
    df["_isT"] = df["t"].astype(str).str.startswith("T")
    df["_isZ"] = df["z"].isin(ABSZ)
    out = []
    for (tkr, ses), gg in df.groupby(["ticker", "session"], sort=False):
        m = len(gg)
        lows = gg["low"].to_numpy(float); highs = gg["high"].to_numpy(float)
        closes = gg["close"].to_numpy(float); opens = gg["open"].to_numpy(float)
        third = max(1, m // 3)
        d_hi, d_lo, d_c = float(highs.max()), float(lows.min()), float(closes[-1])
        rngd = d_hi - d_lo
        isT, isZ, hvv = gg["_isT"].to_numpy(), gg["_isZ"].to_numpy(), gg["_hv"].to_numpy()
        out.append((tkr, ses, m,
                    bool(int(lows.argmin()) <= (m - 1) / 2.0),
                    bool((d_c - d_lo) / rngd >= 0.5) if rngd > 0 else False,
                    bool((d_c - d_lo) / rngd <= 0.40) if rngd > 0 else False,
                    bool(isZ[:third].any() and isT[-third:].any()),
                    int(isZ.sum()), int(isT.sum()),
                    bool(d_c < float(opens[0])),
                    bool((isT[-third:] & hvv[-third:]).any())))
    return pd.DataFrame(out, columns=["ticker", "session", "m1h", "low_early", "close_upper",
                                      "close_weak", "z_first_t_last", "z1h", "t1h", "down_day",
                                      "t_close_hv_1h"])


def _m15(log=print) -> pd.DataFrame:
    """15m absorption count per session + the late hi-vol T tell (the AMD spring fired here)."""
    import duckdb
    con = duckdb.connect(tf_db_path("15m"), read_only=True)
    try:
        df = con.execute(f"""
            WITH r AS (
              SELECT ticker, substr(CAST(date AS VARCHAR),1,10) d, coalesce(t_sig,'') t,
                     coalesce(z_sig,'') z, volume,
                     row_number() OVER (PARTITION BY ticker, substr(CAST(date AS VARCHAR),1,10)
                                        ORDER BY date) rn,
                     count(*) OVER (PARTITION BY ticker, substr(CAST(date AS VARCHAR),1,10)) m,
                     avg(volume) OVER (PARTITION BY ticker ORDER BY date
                                       ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) vm,
                     stddev_pop(volume) OVER (PARTITION BY ticker ORDER BY date
                                              ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) vs
              FROM bars)
            SELECT ticker, d AS session,
                   sum(CASE WHEN z IN {ABSZ_SQL} THEN 1 ELSE 0 END) AS z15,
                   max(CASE WHEN rn > m - ceil(m/3.0) AND t LIKE 'T%' AND volume > vm + vs
                            THEN 1 ELSE 0 END) AS t15hv
            FROM r GROUP BY ticker, d""").fetchdf()
    finally:
        con.close()
    log(f"  15m sessions {len(df):,}")
    df["z15"] = df["z15"].fillna(0).astype(int)
    df["t15hv"] = df["t15hv"].fillna(0).astype(int).astype(bool)
    return df


def build(log=print) -> dict:
    t0 = time.time()
    d = _daily(log); log(f"  daily done ({time.time()-t0:.0f}s)")
    h = _hourly(log); log(f"  1h done ({time.time()-t0:.0f}s)")
    m = _m15(log); log(f"  15m done ({time.time()-t0:.0f}s)")
    X = d.merge(h, on=["ticker", "session"], how="inner").merge(m, on=["ticker", "session"], how="left")
    X["z15"] = X["z15"].fillna(0).astype(int)
    X["t15hv"] = X["t15hv"].fillna(False).astype(bool)
    log(f"  joined {len(X):,} (ticker, session) rows")

    X["a_loc"] = X["held"].astype(int) + X["key"].astype(int)
    X["a_abs"] = ((X["z1h"] >= 1).astype(int) + (X["z1h"] >= 3).astype(int)
                  + (X["z15"] >= 4).astype(int))
    X["a_rev"] = (X["low_early"].astype(int) + X["close_upper"].astype(int)
                  + X["z_first_t_last"].astype(int))
    X["s"] = X["a_loc"] + X["a_abs"] + X["a_rev"]
    at_floor = X["held"].to_numpy() | ((X["pos"].to_numpy() < 0.40)
                                       & (X["key"].to_numpy() | (X["a_abs"].to_numpy() >= 2)))
    t_close_hv = X["t_close_hv_1h"].to_numpy() | X["t15hv"].to_numpy()
    rev = at_floor & (X["a_abs"].to_numpy() >= 1) & (X["a_rev"].to_numpy() >= 2)
    shake = at_floor & X["down_day"].to_numpy() & X["close_weak"].to_numpy() & t_close_hv & ~rev
    cont = ((X["pos"].to_numpy() >= 0.6) & X["up"].to_numpy() & X["close_upper"].to_numpy()
            & (X["t1h"].to_numpy() >= X["z1h"].to_numpy()) & ~X["held"].to_numpy() & ~rev & ~shake)
    v = np.full(len(X), "", dtype=object)
    v[cont] = "cont"; v[shake] = "shake"; v[rev] = "rev"
    X["v"] = v
    X["at_floor"] = at_floor
    X["t_close_hv"] = t_close_hv

    out = X[["ticker", "session", "v", "s", "a_loc", "a_abs", "a_rev", "rs", "held", "key", "pos",
             "z1h", "t1h", "z15", "at_floor", "up", "down_day", "close_upper", "close_weak",
             "low_early", "z_first_t_last", "t_close_hv", "m1h"]].copy()
    out = out.rename(columns={"session": "date", "a_loc": "loc", "a_abs": "abs", "a_rev": "rev"})
    out["pos"] = out["pos"].astype("float32")
    out = out.sort_values(["date", "ticker"], kind="mergesort").reset_index(drop=True)

    census = dict(rows=int(len(out)), tickers=int(out["ticker"].nunique()),
                  dates=int(out["date"].nunique()),
                  first=str(out["date"].min()), last=str(out["date"].max()),
                  verdicts={str(k): int(vv) for k, vv in out["v"].value_counts().items()},
                  score_dist={int(k): int(vv) for k, vv in out["s"].value_counts().sort_index().items()},
                  axis_means=dict(loc=round(float(out["loc"].mean()), 3),
                                  abs=round(float(out["abs"].mean()), 3),
                                  rev=round(float(out["rev"].mean()), 3)),
                  rs_share=round(float(out["rs"].mean()), 4),
                  rev_with_rs=int((out["v"].eq("rev") & out["rs"]).sum()))
    spec = dict(spec_id="ANATOMY_SIGNALS_V1", built_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                source="ported from main.py::api_day1h — the same three axes the ▽△ row prints",
                status="DETECTOR, not a trade signal: project_bottom_anatomy_mtf measured 1.37x lift, "
                       "76% recall, 33% precision on the VERDICT. The 0-8 SCORE ladder is measured "
                       "separately by ANATOMY_LADDER_V1. Never a ranking input.",
                definition=dict(a_loc="held + key (25-bar window)", a_abs="z1h>=1 + z1h>=3 + z15>=4",
                                a_rev="low_early + close_upper + (z_first and t_last)",
                                score="a_loc + a_abs + a_rev, 0..8",
                                verdict="rev / shake / cont / '' — see the module docstring",
                                rs="close/SPY > EMA200(close/SPY), warmup 120"),
                corrections=[
                    "2026-09-11 key_fixed_threshold: `key` compared each prior low against its own "
                    "row's window low (a moving threshold) instead of the one window low at bar i. "
                    "Agreed with main.py::api_day1h on 54.7% of days, bidirectionally. Corrupted "
                    "a_loc and the 0-8 score; the verdict agreed 99.74% because `key` only reaches "
                    "at_floor when not held, pos<0.40 and a_abs<2. Any result measured on a parquet "
                    "built before this date is void for anything using `s`, `loc`, `key` or "
                    "`at_floor`. Guarded by backend/tests/test_anatomy_parity.py."],
                census=census, parquet=OUT)
    tmp = OUT + ".tmp"; out.to_parquet(tmp, index=False); os.replace(tmp, OUT)
    tmp_s = SPEC + ".tmp"; json.dump(spec, open(tmp_s, "w"), indent=1, default=str); os.replace(tmp_s, SPEC)
    log(f"  wrote {OUT} · {len(out):,} rows ({time.time()-t0:.0f}s)")
    log(f"  verdicts {census['verdicts']}")
    log(f"  score dist {census['score_dist']}")
    log(f"  axis means {census['axis_means']} · rs {census['rs_share']} · rev+rs {census['rev_with_rs']:,}")
    return spec


if __name__ == "__main__":
    build()
