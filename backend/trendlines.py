"""
trendlines.py — automatic pivot-based trendlines + sloped zones (2026-07-06).

Finds the lines a chartist would draw (like the user's TTE weekly chart):
    · swing pivots (k-bar fractals) on highs and lows
    · candidate lines through pivot pairs; kept only if the line is RESPECTED —
      extra touches within a PERCENT tolerance (scale-free across a 10× price
      range), near-zero penetration budget before the line counts as broken
    · deduped by (slope, current value); top-N per side by score
    · projected to today → a sloped ZONE (line ± tol) with a state:
      price BELOW / INSIDE / ABOVE  → "are we moving from below to above?"
    · only RELEVANT lines survive (value today within ~±35% of price)

Resistance lines from pivot HIGHS (descending = the reversal question), support
lines from pivot LOWS. Payload mirrors the gann-grid overlay format
(points: [{time, value}]) so the chart draws it identically. READ-ONLY.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import duckdb
from studio.paths import ANALYTICS_DB

K_PIVOT = 4            # fractal half-window (bars each side)
MAX_PEN = 2            # closes beyond line+pen tolerated before "broken"
MIN_SPAN = 25          # min bars between the two anchor pivots
MIN_TOUCH = 3          # 2 anchors + ≥1 extra respected touch
TOP_N = 4              # lines kept per side
REL_LO, REL_HI = 0.65, 1.55   # relevance: price/value_now must stay in this band


def _bars(ticker: str, tf: str = "1d", limit: int = 700) -> pd.DataFrame:
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        df = a.execute("""
            WITH r AS (SELECT ticker, date, open, high, low, close, volume,
                              row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                       FROM bars WHERE ticker = ?)
            SELECT CAST(date AS VARCHAR)[:10] AS dstr, open, high, low, close, volume
            FROM r WHERE rn=1 ORDER BY date
        """, [ticker.upper()]).fetchdf()
    finally:
        a.close()
    if df.empty:
        return df
    if tf == "1w":
        iso = pd.to_datetime(df.dstr)
        df["wk"] = iso.dt.strftime("%G-%V")
        df = df.groupby("wk", sort=False).agg(
            dstr=("dstr", "first"), open=("open", "first"), high=("high", "max"),
            low=("low", "min"), close=("close", "last"), volume=("volume", "sum")).reset_index(drop=True)
    return df.tail(limit).reset_index(drop=True)


def _pivots(v: np.ndarray, k: int, is_high: bool) -> list[int]:
    n = len(v)
    out = []
    for i in range(k, n - k):
        w = v[i - k:i + k + 1]
        if (is_high and v[i] >= w.max()) or (not is_high and v[i] <= w.min()):
            out.append(i)
    ded = []
    for i in out:                       # collapse runs of equal extremes
        if ded and i - ded[-1] <= k and v[i] == v[ded[-1]]:
            ded[-1] = i
        else:
            ded.append(i)
    return ded


def _atr_pct(df: pd.DataFrame, span: int = 14) -> float:
    h, l, c = df.high.to_numpy(float), df.low.to_numpy(float), df.close.to_numpy(float)
    pc = np.roll(c, 1); pc[0] = c[0]
    tr = np.maximum(h - l, np.maximum(abs(h - pc), abs(l - pc)))
    atr = pd.Series(tr / np.maximum(c, 1e-9)).ewm(span=span, adjust=False).mean()
    return float(np.clip(np.nanmedian(atr.tail(120)), 0.008, 0.10))


def _fit_lines(df: pd.DataFrame, side: str, tol_pct: float, pen_pct: float) -> list[dict]:
    """side='res' → pivot highs, price stays BELOW; 'sup' → lows, stays ABOVE.
    Tolerances are PERCENT of the line value (scale-free)."""
    is_res = side == "res"
    v = (df.high if is_res else df.low).to_numpy(float)
    cl = df.close.to_numpy(float)
    n = len(v)
    piv = _pivots(v, K_PIVOT, is_res)
    price = cl[-1]
    cand = []
    for a_i in range(len(piv)):
        for b_i in range(a_i + 1, len(piv)):
            i, j = piv[a_i], piv[b_i]
            if j - i < MIN_SPAN or v[i] <= 0 or v[j] <= 0:
                continue
            slope = (v[j] - v[i]) / (j - i)
            t = np.arange(i, n)
            line = v[i] + slope * (t - i)
            if line[-1] <= 0:
                continue
            seg_cl = cl[i:n]; seg_v = v[i:n]
            pen = line * pen_pct; tol = line * tol_pct
            beyond = (seg_cl > line + pen) if is_res else (seg_cl < line - pen)
            brk_rel = np.flatnonzero(beyond)
            brk_rel = brk_rel[brk_rel > (j - i)]          # anchor bars don't count
            broke = len(brk_rel) > MAX_PEN
            end = int(brk_rel[MAX_PEN]) if broke else n - 1 - i
            if i + end <= j:                              # broke before 2nd anchor
                continue
            touches = int((np.abs(seg_v[:end + 1] - line[:end + 1]) <= tol[:end + 1]).sum())
            if touches < MIN_TOUCH:
                continue
            val_now = float(line[-1])
            if not (REL_LO <= price / val_now <= REL_HI):  # ancient/far line → irrelevant
                continue
            end_abs = i + end
            score = (min(touches, 12) * 2.0 + (end_abs - i) / n * 4.0
                     + end_abs / (n - 1) * 4.0 + (0 if broke else 3.0))
            cand.append({"i": i, "j": j, "slope": float(slope), "y0": float(v[i]),
                         "end": int(end_abs), "touches": touches, "broke": bool(broke),
                         "val_now": val_now, "score": round(float(score), 2)})
    cand.sort(key=lambda x: -x["score"])
    kept = []
    for c in cand:                       # dedupe by (value_now, slope)
        dup = any(abs(c["val_now"] / k2["val_now"] - 1) < 1.2 * tol_pct
                  and abs(c["slope"] - k2["slope"]) * n < 2.0 * tol_pct * price
                  for k2 in kept)
        if not dup:
            kept.append(c)
        if len(kept) >= TOP_N:
            break
    return kept


def detect(ticker: str, tf: str = "1d", limit: int = 700) -> dict:
    df = _bars(ticker, tf, limit)
    if len(df) < 60:
        return {"ticker": ticker.upper(), "tf": tf, "n_bars": len(df), "price": None,
                "lines": [], "headline": None, "error": "not enough bars"}
    ap = _atr_pct(df)
    tol_pct = float(np.clip(0.55 * ap, 0.006, 0.030))
    pen_pct = 0.6 * tol_pct
    n = len(df)
    price = float(df.close.iloc[-1])
    dstr = df.dstr.tolist()
    out = []
    for side in ("res", "sup"):
        for c in _fit_lines(df, side, tol_pct, pen_pct):
            val_now = c["val_now"]
            tol = val_now * tol_pct
            state = ("above" if price > val_now + tol else
                     "below" if price < val_now - tol else "inside")
            out.append({
                "side": side, "slope_dir": "down" if c["slope"] < 0 else "up",
                "points": [{"time": dstr[c["i"]], "value": round(c["y0"], 4)},
                           {"time": dstr[-1], "value": round(val_now, 4)}],
                "touches": c["touches"], "broke": c["broke"], "score": c["score"],
                "anchor_from": dstr[c["i"]], "anchor_to": dstr[c["j"]],
                "value_now": round(val_now, 4), "tol": round(tol, 4), "state": state,
            })
    # headline = the user's question: where are we vs the best DESCENDING resistance?
    desc = [l for l in out if l["side"] == "res" and l["slope_dir"] == "down"]
    headline = None
    if desc:
        best = max(desc, key=lambda l: l["score"])
        headline = {"vs_desc_resistance": best["state"], "line_value": best["value_now"],
                    "touches": best["touches"], "broke": best["broke"]}
    return {"ticker": ticker.upper(), "tf": tf, "n_bars": n, "price": price,
            "atr_pct": round(ap, 4), "tol_pct": round(tol_pct, 4),
            "lines": out, "headline": headline}
