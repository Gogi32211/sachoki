"""
mtf_ema_scan.py — Multi-TF EMA-stack signals (ported from the user's two Pine scripts:
1412_LOWER LOW SMX + 0912_RGTI Lower-Low Screener). 6 variants over 15m + 1H + 4H EMAs
(9/20/50/200) with Daily RSI/vol/price base filters.

Architecture: pure EMA/close/open features → NO signal engine needed. The same
per-tf condition builders serve BOTH the live scanner (tail windows, last completed bar)
and the historical validator (full series, EOD snapshot) — backtest == trade.

Data: studio_15m_base (OHLCV) · studio_1h · studio_4h · studio_analytics (1D).
Bars are session-anchored (TV-aligned) so EMAs match TradingView closely.
Evaluated on COMPLETED bars (EOD-style), like a bar-close TV screener.
"""
from __future__ import annotations
import os, json, time
import numpy as np
import pandas as pd

_DIR = os.path.dirname(__file__)
_CACHE_PATH = os.path.join(_DIR, "mtf_ema_scan.json")

# variant definitions live in _variant_masks() — keep names stable (UI + validation)
# K0 added 2026-07-06 (validate_mtf_mid): the EARLIEST recovery geometry — SMX's skeleton
# without the candle/RSI gates. Best of the family (+1.74/PF1.25/TR+0.71, 4× SMX's n);
# the SMX→ORANGE milestone ladder is monotone-declining, so earlier = better.
VARIANTS = ["K0", "SMX", "LL", "UP", "UPUP", "UPUPUP", "ORANGE"]


def _emas(close: pd.Series) -> dict:
    return {L: close.ewm(span=L, adjust=False).mean() for L in (9, 20, 50, 200)}


def tf_features(df: pd.DataFrame) -> pd.DataFrame:
    """df: one ticker's bars of ONE tf (sorted by date; cols open/close). Adds e9/e20/e50/e200."""
    e = _emas(df["close"])
    out = df.copy()
    for L, s in e.items():
        out[f"e{L}"] = s
    return out


def _variant_masks(f15, f1h, f4h, price_now, rsi_d):
    """All 6 variant conditions from the LAST rows of each tf frame (or aligned rows).
    Inputs are dict-like rows with e9/e20/e50/e200/close/open. price_now = latest price.
    Returns {variant: bool}. Base filters (price/vol) applied by the caller."""
    c15, c1, c4 = f15, f1h, f4h  # aliases

    smx = (
        # 4H: EMA200>EMA50>EMA20>EMA9 · close>EMA9 · bull candle
        c4["e200"] > c4["e50"] > c4["e20"] > c4["e9"]
        and c4["close"] > c4["e9"] and c4["close"] > c4["open"]
        # 1H: close>EMA9 · EMA9>EMA20 · bull candle
        and c1["close"] > c1["e9"] and c1["e9"] > c1["e20"] and c1["close"] > c1["open"]
        # 15m: EMA20<EMA9 · EMA200>EMA9 · EMA50<EMA20 · close>EMA50
        and c15["e20"] < c15["e9"] and c15["e200"] > c15["e9"]
        and c15["e50"] < c15["e20"] and c15["close"] > c15["e50"]
        # Daily RSI > 33 (SMX's own RSI gate)
        and (rsi_d is not None and rsi_d > 33)
    )
    ll = (
        c4["e50"] < c4["e20"] and c4["e20"] > c4["e9"]
        and price_now > c4["e50"] and price_now > c4["e20"]
        and price_now > c1["e9"]
        and c1["e9"] < c1["e50"] and c1["e50"] > c1["e20"] and c1["e200"] < c1["e50"]
        and c15["e200"] > c15["e50"]
    )
    up = (
        c4["e50"] < c4["e20"] and c4["e20"] < c4["e9"]
        and price_now > c4["e20"] and price_now > c4["e200"] and c4["e9"] > c4["e200"]
        and price_now > c1["e9"]
        and c1["e9"] > c1["e50"] and c1["e50"] < c1["e20"] and c1["e200"] < c1["e50"]
        and c15["e20"] > c15["e9"] and price_now > c15["e200"]
    )
    upup = (
        c4["e200"] > c4["e50"] and c4["e50"] < c4["e9"] and c4["e20"] < c4["e50"]
        and price_now > c4["e50"]
        and c15["e200"] < c15["e50"] and c15["e50"] > c15["e20"] and c15["e20"] > c15["e9"]
        and price_now < c15["e9"]
    )
    upupup = (
        c4["e200"] > c4["e50"] and c4["e50"] < c4["e9"] and c4["e20"] > c4["e50"]
        and price_now > c4["e50"] and price_now > c4["e9"]
        and c1["e9"] > c1["e200"] and c1["e200"] > c1["e50"]
        and c15["e200"] > c15["e20"] and c15["e9"] < c15["e20"]
    )
    orange = (
        c1["e200"] > c1["e9"] and c1["e9"] > c1["e20"] and c1["e20"] > c1["e50"]
        and c4["e200"] > c4["e50"] and c4["e50"] > c4["e20"] and c4["e9"] > c4["e20"]
        and c15["e9"] > c15["e20"] and c15["e20"] > c15["e50"] and c15["e50"] > c15["e200"]
    )
    k0 = (
        # 4H: full bear stack (no cross yet) · 1H: e9>e20 only (not ordered) ·
        # 15m: short stack up but still under its 200 — zero milestones done.
        c4["e200"] > c4["e50"] > c4["e20"] > c4["e9"]
        and c1["e9"] > c1["e20"] and c1["e20"] < c1["e50"]
        and c15["e9"] > c15["e20"] > c15["e50"] and c15["e50"] < c15["e200"]
    )
    return {"K0": bool(k0), "SMX": bool(smx), "LL": bool(ll), "UP": bool(up),
            "UPUP": bool(upup), "UPUPUP": bool(upupup), "ORANGE": bool(orange)}


# ── live scan ──────────────────────────────────────────────────────────────────
_TAIL = {"15m": 2200, "1h": 1400, "4h": 1000}   # EMA200 warmup ≥5×span everywhere
_AGE_DAYS = 12          # lookback for the age-aware map (Ultra N-bars max is 10)


def _eod_snap(df: pd.DataFrame, sfx: str, ndays: int) -> pd.DataFrame:
    """Last `ndays` EOD (last bar per ET-day) rows per ticker, with EMA cols suffixed."""
    day = (pd.to_datetime(df["date"]) - pd.Timedelta(hours=5)).dt.strftime("%Y-%m-%d")
    d = df.assign(day=day.values)
    snap = d.groupby(["ticker", "day"], sort=False).tail(1)
    snap = snap.groupby("ticker", sort=False).tail(ndays)
    cols = {"open": "open" + sfx, "close": "close" + sfx,
            **{f"e{L}": f"e{L}{sfx}" for L in (9, 20, 50, 200)}}
    return snap[["ticker", "day", "open", "close"]
                + [f"e{L}" for L in (9, 20, 50, 200)]].rename(columns=cols)


def _variant_ages(frames: dict, dhist: pd.DataFrame) -> dict:
    """{ticker: {variant: age_in_trading_days}} over the last _AGE_DAYS sessions, so the
    Ultra 'last N bars' selector can match a 📐 variant that fired within N days (not just
    today). Uses the SAME _masks_vec as the validated historical marks. Age 0 = latest day."""
    try:
        s15 = _eod_snap(frames["15m"], "_15", _AGE_DAYS)
        s1h = _eod_snap(frames["1h"], "_1h", _AGE_DAYS)
        s4h = _eod_snap(frames["4h"], "_4h", _AGE_DAYS)
        m = (dhist.rename(columns={"rsi_14": "rsi_14"})
             .merge(s15, on=["ticker", "day"], how="inner")
             .merge(s1h, on=["ticker", "day"], how="inner")
             .merge(s4h, on=["ticker", "day"], how="inner"))
        if m.empty:
            return {}
        alldays = sorted(dhist["day"].unique(), reverse=True)
        day_age = {d: i for i, d in enumerate(alldays)}          # 0 = most recent session
        masks = _masks_vec(m)
        ages: dict = {}
        for v, mk in masks.items():
            sub = m.loc[mk.values, ["ticker", "day"]].copy()
            if sub.empty:
                continue
            sub["age"] = sub["day"].map(day_age)
            for tk, a in sub.groupby("ticker")["age"].min().items():
                ages.setdefault(tk, {})[v] = int(a)
        return ages
    except Exception:
        return {}


def _pull_tail(db, tf, n):
    import duckdb
    from studio.paths import db_path
    name = "studio_15m_base.duckdb" if tf == "15m" else f"studio_{tf}.duckdb"
    c = duckdb.connect(db_path(name), read_only=True)
    try:
        df = c.execute(f"""
            WITH r AS (SELECT ticker, date, open, close,
                              row_number() OVER (PARTITION BY ticker ORDER BY date DESC) rn
                       FROM (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) u
                             FROM bars) WHERE u=1)
            SELECT ticker, date, open, close FROM r WHERE rn <= {n} ORDER BY ticker, date
        """).fetchdf()
        return df
    finally:
        c.close()


def scan(min_price: float = 3.0, min_avg_vol10: float = 30_000, use_cache_sec: int = 900) -> dict:
    # cache (the tail pulls + ewm over 3 DBs take ~1-2 min)
    try:
        st = os.stat(_CACHE_PATH)
        if time.time() - st.st_mtime < use_cache_sec:
            with open(_CACHE_PATH) as f:
                return json.load(f)
    except FileNotFoundError:
        pass

    import duckdb
    from studio.paths import ANALYTICS_DB
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        daily = a.execute("""
            WITH r AS (SELECT ticker, universe, date, close, volume, rsi_14,
                              row_number() OVER (PARTITION BY ticker ORDER BY date DESC) rn
                       FROM (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) u
                             FROM bars) WHERE u=1)
            SELECT ticker, any_value(universe) AS universe,
                   max(CASE WHEN rn=1 THEN close END) AS "close",
                   max(CASE WHEN rn=1 THEN rsi_14 END) AS rsi,
                   avg(CASE WHEN rn<=10 THEN volume END) AS avg_vol10
            FROM r WHERE rn<=10 GROUP BY ticker
        """).fetchdf().set_index("ticker")
        # daily history (last _AGE_DAYS sessions) → per-day close+rsi for age-aware masks
        dhist = a.execute(f"""
            WITH r AS (SELECT ticker, date, close, rsi_14,
                              row_number() OVER (PARTITION BY ticker ORDER BY date DESC) rn
                       FROM (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) u
                             FROM bars) WHERE u=1)
            SELECT ticker, CAST(date AS VARCHAR)[:10] AS day, close, rsi_14
            FROM r WHERE rn <= {_AGE_DAYS} ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()

    frames = {}
    for tf, n in _TAIL.items():
        frames[tf] = _pull_tail(None, tf, n)

    # last-row features per ticker per tf
    last = {}
    for tf, df in frames.items():
        g = df.groupby("ticker", sort=False)
        e = {L: g["close"].transform(lambda s, L=L: s.ewm(span=L, adjust=False).mean()) for L in (9, 20, 50, 200)}
        for L, s in e.items():
            df[f"e{L}"] = s
        last[tf] = df.groupby("ticker", sort=False).tail(1).set_index("ticker")

    rows = []
    common = set(last["15m"].index) & set(last["1h"].index) & set(last["4h"].index) & set(daily.index)
    for tk in sorted(common):
        d = daily.loc[tk]
        # priceNow == the Daily close (matches Pine: priceNow=close on the daily screener),
        # NOT the last 15m bar. Keeps the live scan identical to the historical marks engine.
        price = float(d["close"]) if pd.notna(d["close"]) else None
        if price is None or not (price > min_price and (d["avg_vol10"] or 0) > min_avg_vol10):
            continue
        v = _variant_masks(last["15m"].loc[tk], last["1h"].loc[tk], last["4h"].loc[tk],
                           price, float(d["rsi"]) if pd.notna(d["rsi"]) else None)
        fired = [k for k, ok in v.items() if ok]
        if fired:
            # score by 5yr path-sim rank (validate_mtf_ema 2026-07-03 + validate_mtf_mid
            # 2026-07-06): K0 best (+1.74/PF1.25/TR+0.71) > SMX (+1.52/PF1.22) > ORANGE; the
            # milestone ladder declines monotonically, so the earliest geometry outranks all.
            base = 80 if "K0" in fired else 78 if "SMX" in fired else 62 if "ORANGE" in fired else 50
            rows.append({"ticker": tk, "universe": str(d["universe"]),
                         "close": round(price, 2), "price": round(price, 2),
                         "rsi": round(float(d["rsi"]), 0) if pd.notna(d["rsi"]) else None,
                         "rsi_d": round(float(d["rsi"]), 0) if pd.notna(d["rsi"]) else None,
                         "variants": fired, "n_variants": len(fired),
                         "score": min(base + 6 * (len(fired) - 1), 100),
                         "atoms": fired, "l_sig": "", "signal_date": as_of, "age_days": 0})
    rows.sort(key=lambda r: (-r["score"], r["ticker"]))
    ages = _variant_ages(frames, dhist)      # {ticker: {variant: age_days}} last 12 sessions
    out = {"as_of": as_of, "count": len(rows), "rows": rows, "variants": VARIANTS,
           "ages": ages,
           "edge_note": ("Multi-TF EMA-stack signals (1412_SMX + 0912_RGTI Pine ports + K0): "
                         "15m+1H+4H EMA(9/20/50/200) geometry, Daily RSI/vol base, completed "
                         "session-anchored bars. K0 = earliest recovery geometry, family best "
                         "(+1.74/PF1.25; the SMX→ORANGE ladder declines — earlier is better; "
                         "confirmation-waiting costs). WATCH-TIER: medians negative raw; family "
                         "pays mainly Apr-Jun+Sep-Nov (K0×season med +3.25/PF1.96) and dies Dec-Mar.")}
    try:
        with open(_CACHE_PATH, "w") as f:
            json.dump(out, f)
    except Exception:
        pass
    return out


# ── historical marks (for chart overlay) — EOD daily snapshot, all history ──────
def _masks_vec(m: pd.DataFrame) -> dict:
    """Vectorized variant conditions on an aligned per-day frame (mirrors _variant_masks).
    Columns: e{9,20,50,200}_{15,1h,4h}, close_{4h,15}, open_{4h,1h}, close, rsi_14."""
    p = m["close"]
    r = {}
    r["SMX"] = ((m.e200_4h > m.e50_4h) & (m.e50_4h > m.e20_4h) & (m.e20_4h > m.e9_4h)
                & (m.close_4h > m.e9_4h) & (m.close_4h > m.open_4h)
                & (m.close_1h > m.e9_1h) & (m.e9_1h > m.e20_1h) & (m.close_1h > m.open_1h)
                & (m.e20_15 < m.e9_15) & (m.e200_15 > m.e9_15) & (m.e50_15 < m.e20_15)
                & (m.close_15 > m.e50_15) & (m.rsi_14 > 33))
    r["LL"] = ((m.e50_4h < m.e20_4h) & (m.e20_4h > m.e9_4h) & (p > m.e50_4h) & (p > m.e20_4h)
               & (p > m.e9_1h) & (m.e9_1h < m.e50_1h) & (m.e50_1h > m.e20_1h) & (m.e200_1h < m.e50_1h)
               & (m.e200_15 > m.e50_15))
    r["UP"] = ((m.e50_4h < m.e20_4h) & (m.e20_4h < m.e9_4h) & (p > m.e20_4h) & (p > m.e200_4h)
               & (m.e9_4h > m.e200_4h) & (p > m.e9_1h) & (m.e9_1h > m.e50_1h) & (m.e50_1h < m.e20_1h)
               & (m.e200_1h < m.e50_1h) & (m.e20_15 > m.e9_15) & (p > m.e200_15))
    r["UPUP"] = ((m.e200_4h > m.e50_4h) & (m.e50_4h < m.e9_4h) & (m.e20_4h < m.e50_4h) & (p > m.e50_4h)
                 & (m.e200_15 < m.e50_15) & (m.e50_15 > m.e20_15) & (m.e20_15 > m.e9_15) & (p < m.e9_15))
    r["UPUPUP"] = ((m.e200_4h > m.e50_4h) & (m.e50_4h < m.e9_4h) & (m.e20_4h > m.e50_4h)
                   & (p > m.e50_4h) & (p > m.e9_4h) & (m.e9_1h > m.e200_1h) & (m.e200_1h > m.e50_1h)
                   & (m.e200_15 > m.e20_15) & (m.e9_15 < m.e20_15))
    r["ORANGE"] = ((m.e200_1h > m.e9_1h) & (m.e9_1h > m.e20_1h) & (m.e20_1h > m.e50_1h)
                   & (m.e200_4h > m.e50_4h) & (m.e50_4h > m.e20_4h) & (m.e9_4h > m.e20_4h)
                   & (m.e9_15 > m.e20_15) & (m.e20_15 > m.e50_15) & (m.e50_15 > m.e200_15))
    r["K0"] = ((m.e200_4h > m.e50_4h) & (m.e50_4h > m.e20_4h) & (m.e20_4h > m.e9_4h)
               & (m.e9_1h > m.e20_1h) & (m.e20_1h < m.e50_1h)
               & (m.e9_15 > m.e20_15) & (m.e20_15 > m.e50_15) & (m.e50_15 < m.e200_15))
    return r


def _tf_daily_snapshot(ticker, tf):
    """EOD (last bar per ET day) EMA features for one tf, one ticker."""
    import duckdb
    from studio.paths import db_path
    name = "studio_15m_base.duckdb" if tf == "15m" else f"studio_{tf}.duckdb"
    c = duckdb.connect(db_path(name), read_only=True)
    try:
        df = c.execute("SELECT date, open, close FROM bars WHERE ticker=? ORDER BY date",
                       [ticker.upper()]).fetchdf()
    finally:
        c.close()
    if len(df) == 0:
        return pd.DataFrame()
    for L in (9, 20, 50, 200):
        df[f"e{L}"] = df["close"].ewm(span=L, adjust=False).mean()
    day = (pd.to_datetime(df["date"]) - pd.Timedelta(hours=5)).dt.strftime("%Y-%m-%d")
    df["day"] = day.values
    snap = df.groupby("day", sort=False).tail(1)
    sfx = {"15m": "_15", "1h": "_1h", "4h": "_4h"}[tf]
    cols = {"open": "open" + sfx, "close": "close" + sfx,
            **{f"e{L}": f"e{L}{sfx}" for L in (9, 20, 50, 200)}}
    return snap[["day", "open", "close"] + [f"e{L}" for L in (9, 20, 50, 200)]].rename(columns=cols)


def marks_for_ticker(ticker: str) -> dict:
    """Historical daily MTF-EMA variant fires for chart markers. Returns {marks:[{date,variant}]}."""
    import duckdb
    from studio.paths import ANALYTICS_DB
    tk = ticker.upper()
    s15 = _tf_daily_snapshot(tk, "15m"); s1h = _tf_daily_snapshot(tk, "1h"); s4h = _tf_daily_snapshot(tk, "4h")
    if len(s15) == 0 or len(s1h) == 0 or len(s4h) == 0:
        return {"ticker": tk, "marks": []}
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        d = a.execute("SELECT CAST(date AS VARCHAR) AS dstr, close, rsi_14 FROM bars WHERE ticker=? "
                      "QUALIFY row_number() OVER (PARTITION BY date ORDER BY universe)=1 ORDER BY date",
                      [tk]).fetchdf()
    finally:
        a.close()
    d["day"] = d["dstr"].str[:10]
    m = (d.merge(s15, on="day").merge(s1h, on="day").merge(s4h, on="day"))
    if len(m) == 0:
        return {"ticker": tk, "marks": []}
    masks = _masks_vec(m)
    marks = []
    for v, mask in masks.items():
        for day in m.loc[mask.fillna(False).values, "day"]:
            marks.append({"date": day, "variant": v})
    marks.sort(key=lambda x: x["date"])
    return {"ticker": tk, "marks": marks}


if __name__ == "__main__":
    r = scan(use_cache_sec=0)
    print("as_of", r["as_of"], "count", r["count"])
    for x in r["rows"][:15]:
        print(f"  {x['ticker']:6s} {x['price']:>8} rsi{x['rsi_d']} {x['variants']}")
