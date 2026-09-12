"""🏅 RANK v1 — expected edge of an edge FIRE from the sealed RANK_V1 winner (variant A: the state-shrinkage table on
family × RSI band × conso × RS-intact × price band), served as a within-day percentile over the day's fires.

Source of truth: data/rank_v1_model.json (copied from /Users/sachoki/MASSIVE_DATA/RANK_V1/runs/OUT_20260907T174837Z with
provenance; user OK 2026-09-07 "ok gaakete"). The fires come from the SAME warm (60, 3M) edge frame and the SAME SETUPS
registry the model was trained on (build_opportunities.py), so display == backtest. A cold frame → {} (never blocks a
scan; the startup warmer fills it). TTL 1h, keyed on the frame's as_of.

RANK_V1 verdict to keep in mind: adding the display layers (B) or the score layers (C) LOWERED the top-10 head in both
windows — this column is the plain state table on purpose."""
from __future__ import annotations
import os, json, time, logging
import numpy as np
import pandas as pd

log = logging.getLogger(__name__)
HERE = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(os.path.dirname(HERE), "data", "rank_v1_model.json")
LOOKBACK_SESSIONS = 270                     # Superchart per-bar history + the live row
RSI_BANDS = ((-1e9, 35.0, "<35"), (35.0, 50.0, "35-50"), (50.0, 60.0, "50-60"), (60.0, 1e9, ">=60"))
PX_BANDS = ((0.0, 21.0, "<21"), (21.0, 89.0, "21-89"), (89.0, 377.0, "89-377"), (377.0, 1e9, ">=377"))
_MODEL: dict = {}
_MODEL_MTIME: list = [0.0]
_MAP: list = [0.0, "", {}]                  # [built_at, as_of, {(ticker, date): row}]
_FAM: dict = {}


def load_model() -> dict:
    """Cached; re-read when the file changes. {} when absent (the column simply stays empty)."""
    try:
        mt = os.path.getmtime(MODEL_PATH)
    except OSError:
        return {}
    if _MODEL and _MODEL_MTIME[0] == mt:
        return _MODEL
    try:
        m = json.load(open(MODEL_PATH))
        if m.get("winner") != "A" or "A" not in m:
            raise ValueError("rank_v1_model.json is not the A-table artifact")
        _MODEL.clear(); _MODEL.update(m); _MODEL_MTIME[0] = mt
    except Exception:
        log.warning("rank_v1 model load failed", exc_info=True)
        return {}
    return _MODEL


def _band(v: float, bands) -> str:
    try:
        v = float(v)
    except (TypeError, ValueError):
        return ""
    if v != v:
        return ""
    for lo, hi, lab in bands:
        if lo <= v < hi:
            return lab
    return ""


def expected_edge(model: dict, family: str, rsi, conso, rs, close) -> float | None:
    """The A table's shrunk expected edge (pp vs the same-day median) for one fire's state; None without a model."""
    A = (model or {}).get("A")
    if not A:
        return None
    rb, pb = _band(rsi, RSI_BANDS), _band(close, PX_BANDS)
    key = f"{family}|{rb}|{bool(conso)}|{bool(rs)}|{pb}"
    v = A["cell"].get(key)
    if v is None:
        v = A["p1"].get(f"{family}|{rb}")
        if v is None:
            v = A["fam"].get(family, A.get("g", 0.0))
    return float(v)


def _family(name: str) -> str:
    f = _FAM.get(name)
    if f is None:
        try:
            from ledger import family_of
            f = family_of(name)
        except Exception:
            f = str(name)
        _FAM[name] = f
    return f


def percentiles(best: pd.DataFrame) -> pd.DataFrame:
    """best: one row per (ticker, date) with 'edge'. Adds rank_pct (1..100, ties share the upper value) and rank_n."""
    if not len(best):
        return best.assign(rank_pct=pd.Series(dtype=int), rank_n=pd.Series(dtype=int))
    best = best.copy()
    best["rank_n"] = best.groupby("date")["edge"].transform("size").astype(int)
    best["rank_pct"] = (best.groupby("date")["edge"].rank(method="max", pct=True) * 100).round().astype(int).clip(1, 100)
    return best


def rank_map(build: bool = False) -> dict:
    """{(ticker, 'YYYY-MM-DD'): {rank_pct, rank_edge, rank_fam, rank_n}} over the last LOOKBACK_SESSIONS of the warm frame."""
    model = load_model()
    if not model:
        return {}
    try:
        import edge_replay as E
    except Exception:
        return {}
    if not build and (60, 3_000_000) not in E._CACHE:
        return _MAP[2] if (time.time() - _MAP[0]) < 3600 else {}
    try:
        grp, as_of = E._frame(60, 3_000_000)
    except Exception:
        log.warning("rank_map: frame unavailable", exc_info=True)
        return {}
    if _MAP[2] and _MAP[1] == str(as_of) and (time.time() - _MAP[0]) < 3600:
        return _MAP[2]
    t0 = time.time(); rows = []
    setups = [(name, col, _family(name)) for name, col in E.SETUPS]
    for tk, g in grp.items():
        n = len(g)
        if n == 0:
            continue
        lo = max(0, n - LOOKBACK_SESSIONS)
        ds = g["date"].astype(str).str[:10].to_numpy()[lo:]
        rsi = g["rsi_14"].to_numpy(float)[lo:] if "rsi_14" in g else np.full(n - lo, np.nan)
        cl = g["close"].to_numpy(float)[lo:]
        co = g["conso"].fillna(0).to_numpy().astype(bool)[lo:] if "conso" in g else np.zeros(n - lo, bool)
        rs = g["rs_intact"].fillna(False).to_numpy().astype(bool)[lo:] if "rs_intact" in g else np.zeros(n - lo, bool)
        best: dict = {}
        for name, col, fam in setups:
            if col not in g:
                continue
            m = g[col].to_numpy()[lo:]
            for i in np.flatnonzero(m.astype(bool)):
                e = expected_edge(model, fam, rsi[i], co[i], rs[i], cl[i])
                if e is None:
                    continue
                cur = best.get(i)
                if cur is None or e > cur[0]:
                    best[i] = (e, fam)
        for i, (e, fam) in best.items():
            rows.append((tk, ds[i], e, fam))
    if not rows:
        return {}
    df = percentiles(pd.DataFrame(rows, columns=["ticker", "date", "edge", "fam"]))
    out = {(t, d): dict(rank_pct=int(p), rank_edge=round(float(e), 2), rank_fam=f, rank_n=int(nn))
           for t, d, e, f, p, nn in zip(df["ticker"], df["date"], df["edge"], df["fam"], df["rank_pct"], df["rank_n"])}
    _MAP[0] = time.time(); _MAP[1] = str(as_of); _MAP[2] = out
    log.info("rank_map: %d fire-days over %d sessions (%.1fs)", len(out), LOOKBACK_SESSIONS, time.time() - t0)
    return out


def rank_for(rm: dict, ticker: str, *dates) -> dict | None:
    """First hit among candidate dates (the row's own date, then the frame as_of)."""
    tk = str(ticker or "").upper()
    for d in dates:
        if d:
            hit = rm.get((tk, str(d)[:10]))
            if hit:
                return hit
    return None
