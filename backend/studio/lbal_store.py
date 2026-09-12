"""Read side of data/lbal_signals.parquet (built by backend/lbal_build.py — see its docstring for the
definitions and for the status of evidence: DESCRIPTIVE ONLY, never a ranking input).

Two access paths, both through DuckDB `read_parquet` on a fresh connection per call — the 3.7M-row
file is never loaded into the backend process:
  by_dates(dates)          → {(ticker, 'YYYY-MM-DD'): ui_row}   Ultra enrichment (file sorted by date →
                                                                  row-group pruning; one or a few dates)
  by_ticker(ticker, limit) → [ui_row, ...] oldest→newest         chart overlay + Superchart row
`ui_row` keys are prefixed `lbal_` so the same dict can be merged into an Ultra scan row, a chart
signal object or a Superchart bar without colliding with anything that already exists there.
"""
from __future__ import annotations
import os
import re
import json
import time
import logging

from studio.paths import DATA_DIR

log = logging.getLogger(__name__)

PARQ = os.path.join(DATA_DIR, "lbal_signals.parquet")
SPEC = os.path.join(DATA_DIR, "LBAL_SIGNALS_V1.json")

# Both counting modes are served side by side so the UI can switch without a rebuild (user,
# 2026-09-06: V as the default, the every-bar count as an additional switch):
#   lbal_*      = V mode   — only 15m bars with volume > SMA20 are counted (the TradingView setting)
#   lbal_all_*  = ALL mode — every labelled 15m bar (Pine default)
# The parquet's primary columns (udn … text, nv_* counts) are the builder's MODE; the *_all columns
# and n_* counts are the other set. `_to_ui` names them by MODE so the UI keys never flip meaning.
STATE = ("udn", "udn_c", "star", "conflict", "half_up", "half_dn", "nn", "marks", "text")
COUNTS = ("pos", "neg", "pos_c", "neg_c", "lab", "l34", "l3", "l46", "l12", "l25")
COMMON = {"n_bars": "lbal_n_bars", "nv_bars": "lbal_nv_bars", "colour": "lbal_colour", "colour_src": "lbal_colour_src"}
COLS = (["ticker", "date"] + list(STATE) + [s + "_all" for s in STATE]
        + [f"{p}_{c}" for p in ("n", "nv") for c in COUNTS] + list(COMMON))
# every UI field a MISS must still carry, so a chip filter reads `false`, never `undefined`
_EMPTY_STATE = {"udn": None, "udn_c": None, "star": False, "conflict": False, "half_up": False, "half_dn": False,
                "nn": False, "marks": "", "text": ""}
MISS = {"lbal": False, "lbal_mode": "V"}
MISS.update({f"lbal_{k}": v for k, v in _EMPTY_STATE.items()})
MISS.update({f"lbal_all_{k}": v for k, v in _EMPTY_STATE.items()})

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


def mode() -> str:
    """Counting mode of the parquet's primary state: 'V' (volume-confirmed bars) or 'ALL'."""
    sp = spec() or {}
    return str(sp.get("mode") or "V").upper()


def _py(v):
    return v.item() if hasattr(v, "item") else v      # numpy scalar → python


def _to_ui(rec: dict) -> dict:
    m = mode()
    out = {"lbal": True, "lbal_mode": m}
    # which parquet suffix / count prefix feeds which UI prefix
    if m == "V":
        sets = (("lbal_", "", "nv"), ("lbal_all_", "_all", "n"))
    else:
        sets = (("lbal_", "", "n"), ("lbal_all_", "_v", "nv"))   # builder MODE=ALL keeps the V set as *_v
    for ui_pre, sfx, cnt in sets:
        for s in STATE:
            if s + sfx in rec:
                out[ui_pre + s] = _py(rec[s + sfx])
        for c in COUNTS:
            if f"{cnt}_{c}" in rec:
                out[f"{ui_pre}n_{c}"] = _py(rec[f"{cnt}_{c}"])
    for col, key in COMMON.items():
        if col in rec:
            out[key] = _py(rec[col])
    # a session with no labelled bar in EITHER mode carries no state at all
    if not out.get("lbal_udn") and not out.get("lbal_all_udn"):
        out["lbal"] = False
        out.update({k: v for k, v in MISS.items() if k != "lbal"})
    else:
        for k, v in MISS.items():           # never leave a state key undefined in one of the modes
            out.setdefault(k, v)
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
    """{(ticker, date): ui_row} for every parquet row on the given dates. Cached per (mtime, date-set):
    the Ultra scan asks for the same one or two dates on every call."""
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
    placeholders = ", ".join("?" for _ in ds)
    rows = _query(f"SELECT {', '.join(COLS)} FROM read_parquet(?) WHERE date IN ({placeholders})", [PARQ, *ds])
    m = {(r["ticker"], r["date"]): _to_ui(r) for r in rows}
    _DATES_CACHE.update(key=key, map=m, mtime=mt)
    log.info("lbal by_dates %s → %d rows (%.2fs)", ds, len(m), time.time() - t0)
    return m


def by_ticker(ticker: str, limit: int = 400) -> list[dict]:
    """The newest `limit` sessions of one ticker, oldest → newest, each carrying `date` + ui keys."""
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
