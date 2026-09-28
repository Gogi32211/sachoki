"""Read side of data/vol_echo_signals.parquet — the "260925_VOL_ECHO" Pine script ported for display
(built by backend/vol_echo_build.py; definitions and evidence in its docstring).

One row per (ticker, session) on which any VOL_ECHO mark fired: SPIKE, SPK, VE, Q, R, ▲/▼ release,
BO▲ BD▼ BOV▲ BDV▼, plus the QR_REL_V1 veto shape (yesterday Q∧R, today the first ▲ release).

DESCRIPTIVE ONLY. Every long study of these marks was NULL; the one confirmed result is a VETO
(QR_REL_V1: −1.40 pp vs a random buy in VERIFY, negative in all 6 years) and it is RECORDED, NOT
APPLIED. SPK is hindsight — an origin is only known to be "echoed" after the echo arrives.
Nothing here is ever injected into EDGE / BUY / ULTRA / RANK scoring.

Same access pattern as pv_multi_store / shapectx_store — DuckDB `read_parquet` on a fresh connection
per call so the file never sits in the backend process:
  by_dates(dates)          → {(ticker, 'YYYY-MM-DD'): ui_row}   Ultra enrichment
  by_ticker(ticker, limit) → [ui_row, ...] oldest→newest        Superchart row + CSV
UI keys are prefixed `ve_` so a row merges into an Ultra scan row without colliding.
"""
from __future__ import annotations
import os
import re
import json
import math
import time
import logging

from studio.paths import DATA_DIR

log = logging.getLogger(__name__)

PARQ = os.path.join(DATA_DIR, "vol_echo_signals.parquet")
SPEC = os.path.join(DATA_DIR, "VOL_ECHO_SIGNALS_V1.json")
BOOL = ("spike", "spk", "echo", "q", "r", "rel_up", "rel_dn", "bo", "bd", "bov", "bdv",
        "zone_armed", "qr_rel_veto")
INT = ("echo_gap", "echo_nmatch", "echo_hits", "qn", "rn", "rel_n", "rel_q", "bo_age", "zone_pos")
FLOAT = ("echo_vr", "bov_x", "zone_top", "zone_bot")
COLS = ["ticker", "date", *[f"ve_{k}" for k in BOOL + INT + FLOAT], "ve_echo_col", "ve_text"]

# Every UI field a MISS must still carry, so a chip filter reads false / '' — never undefined.
MISS = {"ve": False, "ve_text": "", "ve_echo_col": "", "ve_qr": False}
MISS.update({f"ve_{k}": False for k in BOOL})
MISS.update({f"ve_{k}": None for k in INT + FLOAT})

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


def _num(x):
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def _to_ui(rec: dict) -> dict:
    out = dict(MISS)
    for k in BOOL:
        out[f"ve_{k}"] = bool(rec.get(f"ve_{k}"))
    for k in INT:
        v = _num(rec.get(f"ve_{k}"))
        out[f"ve_{k}"] = None if v is None else int(v)
    for k in FLOAT:
        v = _num(rec.get(f"ve_{k}"))
        out[f"ve_{k}"] = None if v is None else round(v, 4)
    out["ve_echo_col"] = str(rec.get("ve_echo_col") or "")
    out["ve_text"] = str(rec.get("ve_text") or "")
    out["ve_qr"] = out["ve_q"] and out["ve_r"]
    out["ve"] = any(out[f"ve_{k}"] for k in ("echo", "q", "r", "rel_up", "rel_dn", "bo", "bd", "bov", "bdv"))
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
    log.info("vol_echo by_dates %s → %d rows (%.2fs)", ds, len(m), time.time() - t0)
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
