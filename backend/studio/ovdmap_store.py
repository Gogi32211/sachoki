"""Read side of data/ovdmap_signals.parquet — the "260904_OVD_4_VOLUME_LOGICS_DAILY_MAP" Pine script
ported for display (built by backend/lbal_build.py via ovd_map_build.py in the same nightly run).

Tokens: OB·30/60 opening build · RC·30/60 prior-HV reclaim · CD·30/60 close dominance ·
HO·30/60 close→next-open handoff · NM? near-miss proxy. DESCRIPTIVE ONLY — the sealed OVD research
family closed 0 BUILD / 0 VETO; never a ranking input. UI keys are prefixed `ovdmap_`.
"""
from __future__ import annotations
import os
import re
import time
import logging

from studio.paths import DATA_DIR

log = logging.getLogger(__name__)

PARQ = os.path.join(DATA_DIR, "ovdmap_signals.parquet")
FLAGS = ("ob30", "ob60", "rc30", "rc60", "cd30", "cd60", "ho30", "ho60", "nm")
COLS = ["ticker", "date", *FLAGS, "tokens", "text", "rv_o60", "rv_c60", "reclaim60", "event_k", "handoff60", '"full"']   # full = reserved word
MISS = {"ovdmap": False, "ovdmap_tokens": "", "ovdmap_text": ""}
MISS.update({f"ovdmap_{f}": False for f in FLAGS})

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
    return None if isinstance(v, float) and v != v else v      # NaN → None (JSON-safe)


def _to_ui(rec: dict) -> dict:
    out = dict(MISS)
    out.update({"ovdmap": True, "ovdmap_tokens": rec["tokens"], "ovdmap_text": rec["text"],
                "ovdmap_rv_o60": _py(rec["rv_o60"]), "ovdmap_rv_c60": _py(rec["rv_c60"]),
                "ovdmap_reclaim60": _py(rec["reclaim60"]), "ovdmap_event_k": int(_py(rec["event_k"]) or 0),
                "ovdmap_handoff60": _py(rec["handoff60"]), "ovdmap_full": bool(_py(rec["full"]))})
    for f in FLAGS:
        out[f"ovdmap_{f}"] = bool(_py(rec[f]))
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
    log.info("ovdmap by_dates %s → %d rows (%.2fs)", ds, len(m), time.time() - t0)
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
