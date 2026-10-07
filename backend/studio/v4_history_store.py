"""Read side of data/v4_signals.parquet — the Superchart V4 for every ticker and bar (v4_history_build.py).

Per (ticker, date) the store holds the FIRED catalog keys. The score is NOT taken from the build: weights
change as the user edits frontend/src/lib/v4Weights.js, so `score()` re-sums the CURRENT weights over the
stored keys (parsed from that file, cached by mtime). `v4_score_build` stays available for reference only.
DESCRIPTIVE ONLY — V4 as a ranker was measured NULL (research_out/V4_HISTORY_V1.md).
"""
from __future__ import annotations
import os
import re
import logging

from studio.paths import DATA_DIR

log = logging.getLogger(__name__)

PARQ = os.path.join(DATA_DIR, "v4_signals.parquet")
WEIGHTS_JS = os.path.join(os.path.dirname(DATA_DIR), "frontend", "src", "lib", "v4Weights.js")
_TICKER = re.compile(r"^[A-Z0-9.\-]{1,12}$")
_W_CACHE: dict = {"mtime": None, "w": {}}


def available() -> bool:
    return os.path.exists(PARQ)


def weights() -> dict:
    """{key: points} from the frontend's own v4Weights.js (one source of truth), cached by mtime."""
    try:
        mt = os.path.getmtime(WEIGHTS_JS)
    except OSError:
        return _W_CACHE["w"]
    if _W_CACHE["mtime"] != mt:
        txt = open(WEIGHTS_JS, encoding="utf-8").read()
        body = txt[txt.find("V4_WEIGHTS"):]
        _W_CACHE["w"] = {m.group(1): int(m.group(2)) for m in
                         re.finditer(r"^\s*['\"]?([\w$]+)['\"]?\s*:\s*(-?\d+)\s*,?", body, re.M)}
        _W_CACHE["mtime"] = mt
    return _W_CACHE["w"]


def score(keys: str | list) -> int:
    w = weights()
    ks = keys.split() if isinstance(keys, str) else (keys or [])
    return int(sum(w.get(k, 0) for k in ks))


def by_ticker(ticker: str, limit: int = 400) -> list[dict]:
    """Newest `limit` bars of one ticker, oldest→newest: {date, v4_score, v4_n, v4_keys, v4_score_build}."""
    tk = str(ticker or "").upper().strip()
    if not _TICKER.match(tk) or not available():
        return []
    import pyarrow.parquet as pq
    df = pq.read_table(PARQ, filters=[("ticker", "==", tk)]).to_pandas()
    if df.empty:
        return []
    df = df.sort_values("date").tail(max(1, int(limit)))
    return [{"date": r.date, "v4_score": score(r.v4_keys), "v4_n": int(r.v4_n), "v4_keys": r.v4_keys.split(),
             "v4_score_build": int(r.v4_score_build)} for r in df.itertuples()]


_LATEST: dict = {"mtime": None, "map": None, "date": None}


def latest_map() -> dict:
    """{ticker: {"date", "v4_keys"}} for every ticker's LAST stored bar (Ultra's V4 column reads this so the
    screener, the Superchart and the CSV all score the same stored fired-keys). Cached by file mtime."""
    if not available():
        return {}
    mt = os.path.getmtime(PARQ)
    if _LATEST["mtime"] != mt:
        import pyarrow.parquet as pq
        df = pq.read_table(PARQ, columns=["ticker", "date", "v4_keys"]).to_pandas()
        last = df.sort_values(["ticker", "date"]).drop_duplicates("ticker", keep="last")
        _LATEST["map"] = {r.ticker: {"date": r.date, "v4_keys": r.v4_keys.split()} for r in last.itertuples()}
        _LATEST["date"] = str(df.date.max())
        _LATEST["mtime"] = mt
    return _LATEST["map"]

