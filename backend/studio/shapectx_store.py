"""Read side of data/shapectx_signals.parquet (built by backend/shape_ctx_build.py — see its
docstring for the definitions and for the STATUS OF EVIDENCE).

DESCRIPTIVE ONLY. Four sealed families (MOTHER_V1, SHAPE_CLUSTER_V1, SHAPE_GATE_V1,
SWALLOW_DIR_V1), k = 21, 0 BUILD. The single cell with two-window evidence is LST|UP, and it is a
VETO — `shape_lstup_veto`. Nothing here is ever injected into EDGE / BUY / ULTRA / RANK scoring.

Two access paths, both through DuckDB `read_parquet` on a fresh connection per call, so the file is
never loaded into the backend process:
  by_dates(dates)          → {(ticker, 'YYYY-MM-DD'): ui_row}   Ultra enrichment (file sorted by
                                                                date → row-group pruning)
  by_ticker(ticker, limit) → [ui_row, ...] oldest→newest         chart overlay + Superchart row + CSV
`ui_row` keys are prefixed `shape_` so the dict merges into an Ultra scan row, a chart signal object
or a Superchart bar without colliding with anything already there.
"""
from __future__ import annotations
import os
import re
import json
import time
import logging

from studio.paths import DATA_DIR

log = logging.getLogger(__name__)

PARQ = os.path.join(DATA_DIR, "shapectx_signals.parquet")
SPEC = os.path.join(DATA_DIR, "SHAPECTX_SIGNALS_V1.json")

SHAPES = ("mid", "exp", "con", "last", "wrap", "coil", "moth")
# parquet column → UI key suffix (the UI key is "shape_" + suffix)
FIELDS = ("shape", "code", "arrow", "label", "grade", "vr", "absorb", "dry", "sweet",
          "pos20", "floor", "touches", "key", "rs", "rsi", "band", "knife", "veto",
          "lstup_veto", "dir_up", "dir_dn", "can_dir", "cl_fam", "cl_bars", "by_fam",
          "by_den", "mark", "legs")
COLS = ["ticker", "date"] + list(FIELDS) + [f"s_{k}" for k in SHAPES]

# Every UI field a MISS must still carry, so a chip filter reads false / 0 / '', never undefined.
MISS = {
    "shape": False, "shape_code": "", "shape_arrow": "", "shape_label": "",
    "shape_grade": None, "shape_vr": None, "shape_absorb": False, "shape_dry": False,
    "shape_sweet": False, "shape_pos20": None, "shape_floor": False, "shape_touches": 0,
    "shape_key": False, "shape_rs": False, "shape_rsi": None, "shape_band": "",
    "shape_knife": False, "shape_veto": False, "shape_lstup_veto": False,
    "shape_dir_up": False, "shape_dir_dn": False, "shape_can_dir": False,
    "shape_cl_fam": 0, "shape_cl_bars": 0, "shape_by_fam": False, "shape_by_den": False,
    "shape_mark": "", "shape_legs": "",
}
MISS.update({f"shape_s_{k}": False for k in SHAPES})

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TICKER = re.compile(r"^[A-Z0-9.\-]{1,12}$")
_SPEC_CACHE: dict = {"mtime": None, "spec": None}
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
        mt = os.path.getmtime(SPEC)
    except OSError:
        return None
    if _SPEC_CACHE["mtime"] != mt:
        with open(SPEC) as fh:
            _SPEC_CACHE["spec"] = json.load(fh)
        _SPEC_CACHE["mtime"] = mt
    return _SPEC_CACHE["spec"]


def _py(v):
    return v.item() if hasattr(v, "item") else v      # numpy scalar → python


def _to_ui(rec: dict) -> dict:
    out = dict(MISS)
    for f in FIELDS:
        if f in rec and rec[f] is not None:
            out["shape" if f == "shape" else f"shape_{f}"] = _py(rec[f])
    for k in SHAPES:
        if f"s_{k}" in rec:
            out[f"shape_s_{k}"] = bool(_py(rec[f"s_{k}"]))
    out["shape"] = bool(out.get("shape"))
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
    """{(ticker, date): ui_row} for every parquet row on the given dates. Cached per
    (mtime, date-set): the Ultra scan asks for the same one or two dates on every call."""
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
    ph = ", ".join("?" for _ in ds)
    rows = _query(f"SELECT {', '.join(COLS)} FROM read_parquet(?) WHERE date IN ({ph})", [PARQ, *ds])
    m = {(r["ticker"], r["date"]): _to_ui(r) for r in rows}
    _DATES_CACHE.update(key=key, map=m, mtime=mt)
    log.info("shapectx by_dates %s → %d rows (%.2fs)", ds, len(m), time.time() - t0)
    return m


def by_ticker(ticker: str, limit: int = 400) -> list[dict]:
    """The newest `limit` sessions of one ticker, oldest → newest, each carrying `date` + ui keys."""
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
