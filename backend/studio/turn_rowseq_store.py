"""Read side of data/turn_rowseq_signals.parquet — TURN·58 count + ⟲ROW per-row turn tiers
(built nightly by backend/turn_rowseq_build.py with the Superchart's own JS; evidence in
research_out/TURN_SET_V1.md and research_out/ROWSEQ_V1.md).

DESCRIPTIVE ONLY — turn-zone identification. Both replicated as IDENTIFIERS out of sample, neither
picks a better trade (same-day Δ ≈ 0) and ≥ +30 % big-move prediction was NULL. Never a ranking
or score input.

Same access pattern as vol_echo_store — DuckDB `read_parquet` on a fresh connection per call:
  by_dates(dates) → {(ticker, 'YYYY-MM-DD'): ui_row}   Ultra enrichment
UI keys: turn58_* and rs_* (booleans for the filter chips, ints for display/CSV).
"""
from __future__ import annotations
import os
import re
import json
import logging

from studio.paths import DATA_DIR

log = logging.getLogger(__name__)

PARQ = os.path.join(DATA_DIR, "turn_rowseq_signals.parquet")
META = os.path.join(DATA_DIR, "TURN_ROWSEQ_SIGNALS_V1.json")
ROWS = ["fly", "gr", "mtf", "phys", "vol7", "pv", "break", "ovd", "delta"]
SHORT = {"fly": "FLY", "gr": "GR", "mtf": "MTF", "phys": "⚛", "vol7": "VOL7", "pv": "PV",
         "break": "BRK", "ovd": "OVD", "delta": "Δ"}
TURN_BANDS = (15, 20, 26, 30)

# Every UI field a MISS must still carry, so a chip filter reads false — never undefined.
MISS = {"turn58_cand": False, "turn58_n": None, "rs_pair": False, "rs_nconf": 0, "rs_nearly": 0, "rs_text": ""}
MISS.update({f"turn_ge{b}": False for b in TURN_BANDS})
for _r in ROWS:
    MISS.update({f"rs_{_r}": 0, f"rs_{_r}_c": False, f"rs_{_r}_e": False})
MISS.update({"rs_c2": False, "rs_c3": False})

# TOP·58 (research_out/TURN58_TOP_V1.md): the 10 best singles / 10 best pairs of the 58 turn keys, on in
# t-2..t. Labels mirror frontend/src/lib/topPairs.js. The 🕐DR pairs are the only items with a positive
# same-day return (descriptive) → top_dr_pair.
TOP_S = {"dr": "🕐DR", "fbo": "FBO↑", "rtv": "RTV", "c3": "🎯3", "gg3": "gG3", "hilo": "HILO↑",
         "zrt": "ZRT", "m4s6": "M4·σ6", "svs": "SVS", "g3": "G3"}
TOP_P = {"gg3_dr": "gG3+🕐DR", "rtv_gg3": "RTV+gG3", "flp_dr": "FLP↑+🕐DR", "fbo_p": "FBO↑+P",
         "gg3_fbo": "gG3+FBO↑", "v_fbo": "V+FBO↑", "svs_fbo": "SVS+FBO↑", "hilo_gg3": "HILO↑+gG3",
         "gg3_zrt": "gG3+ZRT", "g3_fbo": "G3+FBO↑"}
MISS.update({f"top_s_{c}": False for c in TOP_S})
MISS.update({f"top_p_{c}": False for c in TOP_P})
MISS.update({"top_pair_any": False, "top_dr_pair": False, "top_ns": 0, "top_text": ""})

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CACHE: dict = {"mtime": None, "key": None, "map": None}


def available() -> bool:
    return os.path.exists(PARQ)


def meta() -> dict | None:
    try:
        return json.load(open(META))
    except Exception:
        return None


def _to_ui(rec: dict) -> dict:
    out = dict(MISS)
    cand = bool(rec.get("turn_cand"))
    n = rec.get("turn_n")
    n = None if n is None or n != n else int(n)
    out["turn58_cand"] = cand
    out["turn58_n"] = n if cand else None
    for b in TURN_BANDS:
        out[f"turn_ge{b}"] = bool(cand and n is not None and n >= b)
    conf, early = [], []
    for r in ROWS:
        t = int(rec.get(f"rs_{r}") or 0)
        out[f"rs_{r}"] = t
        out[f"rs_{r}_c"] = t == 2
        out[f"rs_{r}_e"] = t >= 1
        (conf if t == 2 else early if t == 1 else []).append(SHORT[r])
    out["rs_pair"] = bool(rec.get("rs_pair"))
    out["rs_nconf"] = len(conf)
    out["rs_nearly"] = len(conf) + len(early)
    out["rs_c2"] = len(conf) >= 2
    out["rs_c3"] = len(conf) >= 3
    out["rs_text"] = " ".join((["◆V∧M"] if out["rs_pair"] else []) + [f"{s}●" for s in conf] + [f"{s}○" for s in early])
    ps = [c for c in TOP_P if bool(rec.get(f"top_p_{c}"))]
    ss = [c for c in TOP_S if bool(rec.get(f"top_s_{c}"))]
    for c in TOP_P:
        out[f"top_p_{c}"] = c in ps
    for c in TOP_S:
        out[f"top_s_{c}"] = c in ss
    out["top_pair_any"] = bool(ps)
    out["top_dr_pair"] = "gg3_dr" in ps or "flp_dr" in ps
    out["top_ns"] = len(ss)
    out["top_text"] = " ".join([TOP_P[c] for c in ps] + [TOP_S[c] for c in ss])
    return out


def by_dates(dates) -> dict:
    """{(ticker, date): ui_row} for the requested sessions (cached per file mtime + date set)."""
    ds = sorted({str(d)[:10] for d in dates if d and _DATE.match(str(d)[:10])})
    if not ds or not available():
        return {}
    mt = os.path.getmtime(PARQ)
    key = tuple(ds)
    if _CACHE["mtime"] == mt and _CACHE["key"] == key:
        return _CACHE["map"]
    import duckdb
    con = duckdb.connect()
    try:
        lst = ",".join(f"'{d}'" for d in ds)
        df = con.execute(f"SELECT * FROM read_parquet('{PARQ}') WHERE CAST(date AS VARCHAR) IN ({lst})").fetchdf()
    finally:
        con.close()
    m = {(r["ticker"], str(r["date"])[:10]): _to_ui(r) for r in df.to_dict("records")}
    _CACHE.update(mtime=mt, key=key, map=m)
    return m
