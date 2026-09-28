"""Read side of data/pv_multi_signals.parquet — the "260921_PV_MULTI" Pine script ported for
display (built by backend/pv_multi_build.py; definitions and evidence in its docstring).

One row per (ticker, session) on which EITHER price source fired one of the nine ordinal
price×volume shapes. Two sources, two UI rows, because they disagree on 47% of hits:
    pv_c   price = close        pv_o   price = ohlc4 (the Pine default)

DESCRIPTIVE ONLY. PV_MULTI_V1 sealed 2026-09-21, k = 9 → 0 BUILD / 4 VETO_CANDIDATE / 5 NULL, with
EVERY cell negative in MINE. The four VETO candidates (REV, RE2, UPP, RUP) are RECORDED, NOT
APPLIED — they cover ~13.6% of bars and applying them is an interaction question with its own k.
Nothing here is ever injected into EDGE / BUY / ULTRA / RANK scoring.

Same access pattern as lvx_store / shapectx_store, DuckDB `read_parquet` on a fresh connection per
call so the file never sits in the backend process:
  by_dates(dates)          → {(ticker, 'YYYY-MM-DD'): ui_row}   Ultra enrichment
  by_ticker(ticker, limit) → [ui_row, ...] oldest→newest        Superchart rows + CSV
UI keys are prefixed `pv_` so a row merges into an Ultra scan row without colliding.
"""
from __future__ import annotations
import os
import re
import json
import time
import logging

from studio.paths import DATA_DIR

log = logging.getLogger(__name__)

PARQ = os.path.join(DATA_DIR, "pv_multi_signals.parquet")
SPEC = os.path.join(DATA_DIR, "PV_MULTI_SIGNALS_V1.json")
COLS = ["ticker", "date", "pv_c", "pv_o", "pv_text"]
CODES = ("DIV", "UPP", "UPR", "REV", "RUP", "VUP", "TURN", "UP4", "RE2")
# The sealed family's own verdict, so a filter can select on evidence rather than on the label.
VETO_CODES = ("REV", "RE2", "UPP", "RUP")

# Every UI field a MISS must still carry, so a chip filter reads '' / false, never undefined.
MISS = {"pv": False, "pv_c": "", "pv_o": "", "pv_text": "", "pv_agree": False,
        "pv_veto_c": False, "pv_veto_o": False}
MISS.update({f"pv_c_{c.lower()}": False for c in CODES})
MISS.update({f"pv_o_{c.lower()}": False for c in CODES})

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


def spec() -> dict | None:
    try:
        return json.load(open(SPEC))
    except Exception:
        return None


def _to_ui(rec: dict) -> dict:
    c = str(rec.get("pv_c") or "")
    o = str(rec.get("pv_o") or "")
    out = dict(MISS)
    out.update({"pv": bool(c or o), "pv_c": c, "pv_o": o,
                "pv_text": str(rec.get("pv_text") or ""),
                "pv_agree": bool(c and c == o),
                "pv_veto_c": c in VETO_CODES, "pv_veto_o": o in VETO_CODES})
    if c:
        out[f"pv_c_{c.lower()}"] = True
    if o:
        out[f"pv_o_{o.lower()}"] = True
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
    rows = _query(
        f"SELECT {', '.join(COLS)} FROM read_parquet(?) WHERE date IN ({', '.join('?' for _ in ds)})",
        [PARQ, *ds])
    m = {(r["ticker"], r["date"]): _to_ui(r) for r in rows}
    _DATES_CACHE.update(key=key, map=m, mtime=mt)
    log.info("pv_multi by_dates %s → %d rows (%.2fs)", ds, len(m), time.time() - t0)
    return m


def by_ticker(ticker: str, limit: int = 400) -> list[dict]:
    t = (ticker or "").upper()
    if not _TICKER.match(t) or mtime() is None:
        return []
    rows = _query(
        f"SELECT {', '.join(COLS)} FROM read_parquet(?) WHERE ticker = ? ORDER BY date DESC LIMIT ?",
        [PARQ, t, int(max(1, min(limit, 5000)))])
    out = []
    for r in reversed(rows):
        d = _to_ui(r)
        d["date"] = r["date"]
        out.append(d)
    return out
