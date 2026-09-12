"""Read side of data/lvx_signals.parquet — the "260906_WLNBB_L34_L46_VX_CHART" Pine script ported
(built by backend/lbal_build.py in the same nightly run as L-BAL; definitions in its docstring).

One row per (ticker, session) whose DAILY exact label is L34 or L46, graded:
  tier 0 plain · 1 V (daily volume > SMA20) · 2 VL (a lower TF echoes the plain label) ·
  3 VH (a lower TF echoes the V label) · 4 VX (both 15m and 60m echo).
DESCRIPTIVE ONLY — never a ranking input. Same access pattern as lbal_store: by_dates for the Ultra
enrichment, by_ticker for the chart line and the Superchart row. UI keys are prefixed `lvx_`.
"""
from __future__ import annotations
import os
import re
import time
import logging

from studio.paths import DATA_DIR

log = logging.getLogger(__name__)

PARQ = os.path.join(DATA_DIR, "lvx_signals.parquet")
COLS = ["ticker", "date", "fam", "colour", "v", "lv_15", "lv_1h", "n_15", "nv_15", "n_1h", "nv_1h",
        "bars_15", "bars_1h", "tier", "label", "text"]
FAMS = ("l34", "l46")
TIERS = (("v", 1), ("vl", 2), ("vh", 3), ("vx", 4))
# every UI field a MISS must still carry, so a chip filter reads `false`, never `undefined`
MISS = {"lvx": False, "lvx_fam": None, "lvx_tier": None, "lvx_label": "", "lvx_text": "", "lvx_v": False,
        "lvx_lv15": 0, "lvx_lv1h": 0}
MISS.update({f"lvx_{f}_{t}": False for f in FAMS for t, _ in TIERS})
MISS.update({f"lvx_{f}": False for f in FAMS})

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TICKER = re.compile(r"^[A-Z0-9.\-]{1,12}$")
_DATES_CACHE: dict = {"mtime": None, "key": None, "map": None}


def available() -> bool:
    return os.path.exists(PARQ)


def mtime():
    try:
        return os.path.getmtime(PARQ)
    except OSError:
        return None


def _py(v):
    return v.item() if hasattr(v, "item") else v


def _to_ui(rec: dict) -> dict:
    fam = str(rec["fam"]).lower()
    tier = int(_py(rec["tier"]))
    out = dict(MISS)
    out.update({
        "lvx": True, "lvx_fam": rec["fam"], "lvx_tier": tier, "lvx_label": rec["label"], "lvx_text": rec["text"],
        "lvx_v": bool(_py(rec["v"])), "lvx_lv15": int(_py(rec["lv_15"])), "lvx_lv1h": int(_py(rec["lv_1h"])),
        "lvx_n15": int(_py(rec["n_15"])), "lvx_nv15": int(_py(rec["nv_15"])), "lvx_n1h": int(_py(rec["n_1h"])),
        "lvx_nv1h": int(_py(rec["nv_1h"])), "lvx_bars15": int(_py(rec["bars_15"])), "lvx_bars1h": int(_py(rec["bars_1h"])),
        "lvx_colour": rec["colour"], f"lvx_{fam}": True,
    })
    for t, n in TIERS:                        # chips read "at least this tier"
        out[f"lvx_{fam}_{t}"] = tier >= n
    return out


def _query(sql: str, params: list) -> list[dict]:
    import duckdb
    con = duckdb.connect()
    try:
        cur = con.execute(sql, params)
        names = [d[0] for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]
    finally:
        con.close()


def by_dates(dates) -> dict:
    mt = mtime()
    if mt is None:
        return {}
    ds = sorted({str(d)[:10] for d in dates if _DATE.match(str(d)[:10] or "")})
    if not ds:
        return {}
    key = (mt, tuple(ds))
    if _DATES_CACHE["key"] == key:
        return _DATES_CACHE["map"]
    t0 = time.time()
    rows = _query(f"SELECT {', '.join(COLS)} FROM read_parquet(?) WHERE date IN ({', '.join('?' for _ in ds)})", [PARQ, *ds])
    m = {(r["ticker"], r["date"]): _to_ui(r) for r in rows}
    _DATES_CACHE.update(key=key, map=m, mtime=mt)
    log.info("lvx by_dates %s → %d rows (%.2fs)", ds, len(m), time.time() - t0)
    return m


def by_ticker(ticker: str, limit: int = 400) -> list[dict]:
    t = (ticker or "").upper()
    if not _TICKER.match(t) or mtime() is None:
        return []
    rows = _query(f"SELECT {', '.join(COLS)} FROM read_parquet(?) WHERE ticker = ? ORDER BY date DESC LIMIT ?",
                  [PARQ, t, int(max(1, min(limit, 5000)))])
    out = []
    for r in reversed(rows):
        d = _to_ui(r)
        d["date"] = r["date"]
        out.append(d)
    return out
