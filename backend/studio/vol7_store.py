"""Read side of data/vol7_signals.parquet — the "260829 • 7-Level Volume MR + Sigma" Pine script ported
for display (built by backend/lbal_build.py via vol7_build.py in the same nightly run).

Per (ticker, date): MR level M0..M6 (primary, volume / 20-day median), σ level σ0..σ6 (comparison),
consensus (= / MR+ / Σ+), the bar-to-bar level jump (▲+2 ▼−2 ◆+3 ◆−3), VB2 and SHIFT↑/↓.
DESCRIPTIVE ONLY — never a ranking input. M5/M6 are the script's B/VB and are NOT the app's WLNBB
B/VB bucket. UI keys are prefixed `vol7_`.
"""
from __future__ import annotations
import os
import re
import time
import logging

from studio.paths import DATA_DIR

log = logging.getLogger(__name__)

PARQ = os.path.join(DATA_DIR, "vol7_signals.parquet")
COLS = ["ticker", "date", "ratio", "mr", "sg", "diff", "trans", "vb2", "shift_up", "shift_dn", "prior_move",
        "cons", "jump", "label", "marks", "text"]
FLAG_KEYS = ("m0", "m5", "m6", "up2", "dn2", "up3", "dn3", "sigma_plus", "mr_plus", "eq", "vb2", "shift_up", "shift_dn")
MISS = {"vol7": False, "vol7_mr": None, "vol7_sg": None, "vol7_ratio": None, "vol7_cons": "", "vol7_jump": "",
        "vol7_label": "", "vol7_marks": "", "vol7_text": "", "vol7_trans": 0, "vol7_prior_move": None}
MISS.update({f"vol7_{k}": False for k in FLAG_KEYS})

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
    v = v.item() if hasattr(v, "item") else v
    return None if isinstance(v, float) and v != v else v


def _to_ui(rec: dict) -> dict:
    mr, sg, t = int(_py(rec["mr"])), int(_py(rec["sg"])), int(_py(rec["trans"]))
    diff = int(_py(rec["diff"]))
    out = dict(MISS)
    out.update({
        "vol7": True, "vol7_mr": mr, "vol7_sg": sg, "vol7_ratio": _py(rec["ratio"]), "vol7_cons": rec["cons"] or "",
        "vol7_jump": rec["jump"] or "", "vol7_label": rec["label"], "vol7_marks": rec["marks"] or "", "vol7_text": rec["text"],
        "vol7_trans": t, "vol7_prior_move": _py(rec["prior_move"]),
        "vol7_m0": mr == 0, "vol7_m5": mr == 5, "vol7_m6": mr == 6,
        "vol7_up2": t == 2, "vol7_dn2": t == -2, "vol7_up3": t >= 3, "vol7_dn3": t <= -3,
        "vol7_sigma_plus": diff <= -2, "vol7_mr_plus": diff >= 2, "vol7_eq": diff == 0,
        "vol7_vb2": bool(_py(rec["vb2"])), "vol7_shift_up": bool(_py(rec["shift_up"])), "vol7_shift_dn": bool(_py(rec["shift_dn"])),
    })
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
    log.info("vol7 by_dates %s → %d rows (%.2fs)", ds, len(m), time.time() - t0)
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
