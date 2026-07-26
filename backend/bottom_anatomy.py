"""bottom_anatomy.py — latest-bar Bottom-Anatomy verdict for the WHOLE universe, cached,
for the Ultra screener column (2026-07-23, project_bottom_anatomy_mtf).

Same 3-axis DEFINITION as the /api/day1h detector (location + multi-TF absorption +
internal-reversal → 🔻 structural / 🔻💪 durable-RS / 🔺 continues), but computed batch
over every ticker's LAST daily bar so the screener can show it per row without a per-
ticker day1h call. DETECTOR, not a trade signal (1.37× lift / 76% recall / 33% precision).
"""
from __future__ import annotations
import time
import duckdb
import numpy as np
from collections import defaultdict
from datetime import timedelta

from studio.db import tf_db_path

_ABSZ = "('Z1','Z1G','Z2','Z2G','Z5','Z9','Z10','Z11')"
_CACHE: list = [0.0, {}]        # [built_ts, {ticker: {"v","s","rs"}}]


def latest_anatomy_map(ttl: int = 3600) -> dict:
    """{ticker: {'v':'rev'|'cont'|'', 's':int, 'rs':bool}} for each ticker's LAST bar. TTL 1h."""
    if _CACHE[1] and (time.time() - _CACHE[0]) < ttl:
        return _CACHE[1]
    try:
        out = _compute()
        _CACHE[0] = time.time(); _CACHE[1] = out
        return out
    except Exception:
        import logging
        logging.getLogger("uvicorn").debug("latest_anatomy_map failed", exc_info=True)
        return _CACHE[1] or {}


def _compute() -> dict:
    ana = tf_db_path("1d")
    con = duckdb.connect(ana, read_only=True)
    maxd = con.execute("SELECT max(date) FROM bars WHERE universe<>'index'").fetchone()[0]
    cut = str(maxd - timedelta(days=340))[:10]
    ddf = con.execute(
        """WITH d AS (SELECT * FROM bars WHERE close>=5 AND universe<>'index' AND date>=?
             QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker,date ORDER BY
               CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2
                             WHEN 'russell2k' THEN 3 ELSE 4 END)=1)
           SELECT ticker, substr(CAST(date AS VARCHAR),1,10) d, open, high, low, close, volume
           FROM d ORDER BY ticker, date""", [cut]).fetchdf()
    spy = con.execute(
        "SELECT substr(CAST(date AS VARCHAR),1,10) d, close FROM bars "
        "WHERE ticker='SPY' AND date>=? ORDER BY date", [cut]).fetchdf()
    con.close()
    spym = dict(zip(spy["d"], spy["close"].astype(float)))

    # 1H aggregate for each ticker's LAST day (within the window).
    h = duckdb.connect(tf_db_path("1h"), read_only=True)
    h1 = h.execute(
        f"""WITH r AS (SELECT ticker, substr(CAST(date AS VARCHAR),1,10) d,
              coalesce(t_sig,'') t, coalesce(z_sig,'') z, low, high, close, volume,
              row_number() OVER (PARTITION BY ticker,substr(CAST(date AS VARCHAR),1,10) ORDER BY date) rn,
              count(*) OVER (PARTITION BY ticker,substr(CAST(date AS VARCHAR),1,10)) m,
              avg(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) vm,
              stddev_pop(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) vs,
              max(date) OVER (PARTITION BY ticker) mxd
            FROM bars WHERE date>=?)
          SELECT ticker, d, any_value(m) m,
            sum(CASE WHEN z IN {_ABSZ} THEN 1 ELSE 0 END) z1h,
            min_by(rn,low) low_rn, min(low) dlo, max(high) dhi, arg_max(close,rn) dclose,
            max(CASE WHEN rn<=ceil(m/3.0) AND z IN {_ABSZ} THEN 1 ELSE 0 END) zf,
            max(CASE WHEN rn>m-ceil(m/3.0) AND t LIKE 'T%' THEN 1 ELSE 0 END) tl,
            max(CASE WHEN rn>m-ceil(m/3.0) AND t LIKE 'T%' AND volume>vm+vs THEN 1 ELSE 0 END) t_close_hv
          FROM r WHERE substr(CAST(mxd AS VARCHAR),1,10)=d GROUP BY ticker,d""", [cut]).fetchdf()
    h.close()
    h1m = {(r.ticker, r.d): (r.z1h, r.low_rn, r.m, r.dlo, r.dhi, r.dclose, r.zf, r.tl, r.t_close_hv)
           for r in h1.itertuples()}

    # 15m Z-absorption count per (ticker, last day).
    m = duckdb.connect(tf_db_path("15m"), read_only=True)
    m15 = m.execute(
        f"""WITH r AS (SELECT ticker, substr(CAST(date AS VARCHAR),1,10) d,
              coalesce(t_sig,'') t, coalesce(z_sig,'') z, volume,
              row_number() OVER (PARTITION BY ticker,substr(CAST(date AS VARCHAR),1,10) ORDER BY date) rn,
              count(*) OVER (PARTITION BY ticker,substr(CAST(date AS VARCHAR),1,10)) mm,
              avg(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) vm,
              stddev_pop(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) vs,
              max(date) OVER (PARTITION BY ticker) mxd FROM bars WHERE date>=?)
          SELECT ticker, d, sum(CASE WHEN z IN {_ABSZ} THEN 1 ELSE 0 END) z15,
            max(CASE WHEN rn>mm-ceil(mm/3.0) AND t LIKE 'T%' AND volume>vm+vs THEN 1 ELSE 0 END) t_close_hv
          FROM r WHERE substr(CAST(mxd AS VARCHAR),1,10)=d GROUP BY ticker,d""", [cut]).fetchdf()
    m.close()
    z15 = {(r.ticker, r.d): r.z15 for r in m15.itertuples()}
    t15hv = {(r.ticker, r.d): r.t_close_hv for r in m15.itertuples()}

    tk = ddf["ticker"].to_numpy(); dd = ddf["d"].to_numpy()
    O = ddf["open"].to_numpy(float)
    H = ddf["high"].to_numpy(float); L = ddf["low"].to_numpy(float); C = ddf["close"].to_numpy(float)
    idx = defaultdict(list)
    for i, t in enumerate(tk):
        idx[t].append(i)

    out: dict = {}
    for t, ii in idx.items():
        if len(ii) < 26:
            continue
        a = np.array(ii); Ll = L[a]; Hh = H[a]; Cc = C[a]; mlen = len(a)
        p = mlen - 1                                   # LAST bar
        wl = float(Ll[p - 25:p].min()); wh = float(Hh[p - 25:p].max())
        rng = (wh - wl) / wl if wl > 0 else 9.0
        held = rng <= 0.35 and (Ll[p] - wl) / wl <= 0.06
        key = int((Ll[p - 25:p] <= wl * 1.01).sum()) >= 2
        rpos = (Cc[p] - wl) / (wh - wl) if wh > wl else 0.5
        # RS vs SPY (EMA200 of close/spy)
        aa = 2.0 / 201.0; prev = None; cnt = 0; rs_ok = False
        for q in range(mlen):
            s = spym.get(dd[a[q]])
            if s and s > 0:
                rr = Cc[q] / s
                prev = rr if prev is None else aa * rr + (1 - aa) * prev
                cnt += 1
                rs_ok = cnt >= 120 and rr > prev
        d_last = dd[a[p]]
        r = h1m.get((t, d_last))
        if r is None:
            continue
        z1h, low_rn, mm, dlo, dhi, dclose, zf, tl, t_close_hv = r
        z15n = z15.get((t, d_last), 0)
        low_early = low_rn <= (mm + 1) / 2.0
        close_up = (dclose - dlo) / (dhi - dlo) >= 0.5 if dhi > dlo else False
        a_abs = (1 if z1h >= 1 else 0) + (1 if z1h >= 3 else 0) + (1 if z15n >= 4 else 0)
        a_rev = int(low_early) + int(close_up) + int(zf and tl)
        a_loc = int(held) + int(key)
        at_floor = held or (rpos < 0.40 and (key or a_abs >= 2))
        up = Cc[p] >= (Cc[p - 1] if p >= 1 else Cc[p])
        down_day = Cc[p] < O[a[p]]
        close_weak = (Cc[p] - Ll[p]) / (Hh[p] - Ll[p]) <= 0.40 if Hh[p] > Ll[p] else False
        if at_floor and a_abs >= 1 and a_rev >= 2:
            v = "rev"
        elif at_floor and down_day and close_weak and (t_close_hv or t15hv.get((t, d_last), 0)):
            v = "shake"
        elif rpos >= 0.6 and up and close_up and not held:
            v = "cont"
        else:
            continue                                   # only emit tickers with a verdict
        out[t] = {"v": v, "s": a_loc + a_abs + a_rev, "rs": bool(rs_ok)}
    return out
