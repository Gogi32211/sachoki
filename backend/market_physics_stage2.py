"""Market Physics · stage 2 — invariance. Descriptive, no outcome, no k.

Stage 1 answered "are these independent" and said yes, more than I predicted. Independence is
not content: twenty-three independent noise columns would have scored the same. This stage asks
the question that separates a LAW from a DEFINITION.

A physical law has content because a coefficient is INVARIANT over some domain. `V = IR` means
something because R holds still while V and I move; if R were recomputed every instant from
V/I, it would be a ratio with a Greek letter. Every axis measured so far is recomputed per bar.
So: does any of them hold still, and over what?

    ICC              share of variance BETWEEN tickers. High → a property of the name.
    PERSISTENCE      autocorrelation of the axis within a ticker, at lags 1 · 5 · 20 · 60,
                     and the implied half-life.
    YoY STABILITY    the decisive one. Rank tickers by the axis within year y, rank them again
                     in year y+1, correlate the ranks. If a name's level this year predicts its
                     level next year, the axis is a property it HAS. If not, it is a reading.

WHY RANKS WITHIN YEAR. Volatility-like axes move together across the whole market, so raw
year-to-year correlation would be high for every one of them and would be measuring the market's
common factor, not any property of a name. Ranking inside each year removes that factor and asks
only whether names keep their ORDER.

WHAT THE ANSWER IS FOR. It decides how an axis may be used, which is a design decision and not a
preference:

    stable level      the signal is DEVIATION from the name's own level, never the level
    unstable level    the signal is the level itself; a "deviation from normal" is meaningless
                      because there is no normal
    neither           it goes no further; a token built from it would be a token of noise

Effective resolution rides beside every row. Stage 1 counted decl_H as a full independent
dimension while it carried about ten distinct values per thousand bars, and a confident
invariance statistic about a near-constant column deserves to be visibly that.
"""
from __future__ import annotations

import json
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import market_physics as MP                                           # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "MARKET_PHYSICS_STAGE2.json")

LAGS = (1, 5, 20, 60)
MIN_BARS_PER_TICKER = 300
MIN_TICKERS_PER_YEAR = 40


def _icc(d: pd.DataFrame, col: str) -> float:
    """Between-ticker share of total variance."""
    g = d.groupby("ticker")[col]
    between = g.mean().var()
    within = g.var().mean()
    tot = between + within
    return float(between / tot) if tot and np.isfinite(tot) else np.nan


def _persistence(d: pd.DataFrame, col: str) -> dict:
    """Autocorrelation within ticker, after removing each ticker's own mean."""
    x = d[col] - d.groupby("ticker")[col].transform("mean")
    out = {}
    for lag in LAGS:
        sh = x.groupby(d["ticker"]).shift(lag)
        ok = x.notna() & sh.notna()
        out[f"ac{lag}"] = round(float(x[ok].corr(sh[ok])), 3) if ok.sum() > 500 else None
    a1 = out.get("ac1")
    out["half_life_bars"] = (round(float(np.log(0.5) / np.log(a1)), 1)
                             if a1 and 0 < a1 < 1 else None)
    return out


def _yoy(d: pd.DataFrame, col: str) -> dict:
    """Do names keep their ORDER on this axis from one year to the next?

    Ranked inside each year first, so a market-wide move in the axis cannot masquerade as a
    per-name property.
    """
    med = (d.groupby(["ticker", "yr"])[col].median().reset_index())
    med["rk"] = med.groupby("yr")[col].rank(pct=True)
    years = sorted(med["yr"].unique())
    rhos, ns = [], []
    for y0, y1 in zip(years, years[1:]):
        a = med[med.yr == y0].set_index("ticker")["rk"]
        b = med[med.yr == y1].set_index("ticker")["rk"]
        common = a.index.intersection(b.index)
        if len(common) < MIN_TICKERS_PER_YEAR:
            continue
        rhos.append(float(a[common].corr(b[common], method="spearman")))
        ns.append(len(common))
    if not rhos:
        return {"yoy_rank_rho": None, "pairs": 0, "per_pair": []}
    return {"yoy_rank_rho": round(float(np.mean(rhos)), 3),
            "yoy_min": round(float(np.min(rhos)), 3),
            "yoy_max": round(float(np.max(rhos)), 3),
            "pairs": len(rhos), "tickers_median": int(np.median(ns)),
            "per_pair": [round(r, 3) for r in rhos]}


def classify(row: dict) -> str:
    """How this axis may be used. A verdict about USE, not about value."""
    res = row["resolution"]
    coarse = (res.get("distinct_per_1000") or 0) < 1.0
    yoy = row["yoy"].get("yoy_rank_rho")
    ac20 = row["persistence"].get("ac20")
    if coarse and (yoy is None or yoy < 0.3):
        return "NOISE_OR_TOO_COARSE — goes no further"
    if yoy is not None and yoy >= 0.5:
        return "STABLE_PROPERTY — use DEVIATION from the name's own level"
    if ac20 is not None and ac20 >= 0.3:
        return "SLOW_STATE — persistent within a name, level usable directly"
    return "FAST_STATE — the level itself, no meaningful 'normal' to deviate from"


def run(universe: str = "sp500") -> dict:
    t0 = time.time()
    phys = MP.load_and_compute(universe=universe, verbose=True)
    cols = MP.AXES_PROPOSED + MP.AXES_DECLARED
    d = phys[["ticker", "date"] + cols].replace([np.inf, -np.inf], np.nan)
    d["yr"] = pd.to_datetime(d["date"]).dt.year
    keep = d.groupby("ticker")["ticker"].transform("size") >= MIN_BARS_PER_TICKER
    d = d[keep]
    print(f"  {len(d):,} rows · {d.ticker.nunique():,} tickers · "
          f"{time.time() - t0:.0f}s", flush=True)

    res = MP.resolution(d, cols)
    axes = {}
    for c in cols:
        sub = d[["ticker", "yr", c]].dropna()
        if len(sub) < 5000:
            axes[c] = {"skipped": f"only {len(sub)} finite rows"}
            continue
        row = {"resolution": res[c], "icc_between_ticker": round(_icc(sub, c), 3),
               "persistence": _persistence(sub, c), "yoy": _yoy(sub, c),
               "finite_pct": round(100 * len(sub) / len(d), 1)}
        row["verdict"] = classify(row)
        axes[c] = row

    return {"stage": ("2 — INVARIANCE. Descriptive: no outcome column was read, no k charged. "
                      "Verdicts are about HOW an axis may be used, not about whether it works."),
            "universe": universe, "rows": int(len(d)), "tickers": int(d.ticker.nunique()),
            "spec": MP.spec(), "min_bars_per_ticker": MIN_BARS_PER_TICKER,
            "axes": axes, "seconds": round(time.time() - t0, 1)}


if __name__ == "__main__":
    r = run(sys.argv[1] if len(sys.argv) > 1 else "sp500")
    with open(REPORT, "w") as f:
        json.dump(r, f, indent=1, sort_keys=True)

    print("\n" + "=" * 108, flush=True)
    print("  STAGE 2 · INVARIANCE — does any coefficient hold still?", flush=True)
    print("=" * 108, flush=True)
    print(f"  {r['rows']:,} rows · {r['tickers']} tickers · {r['seconds']}s\n", flush=True)
    print(f"  {'axis':<24}{'res/1k':>8}{'ICC':>7}{'ac1':>7}{'ac20':>7}{'ac60':>7}"
          f"{'half-life':>11}{'YoY rank':>10}   verdict", flush=True)
    rows = [(k, v) for k, v in r["axes"].items() if "verdict" in v]
    rows.sort(key=lambda kv: -(kv[1]["yoy"].get("yoy_rank_rho") or -1))
    for c, v in rows:
        p, y = v["persistence"], v["yoy"]
        hl = p["half_life_bars"]
        print(f"  {c:<24}{v['resolution'].get('distinct_per_1000', 0):>8.2f}"
              f"{v['icc_between_ticker']:>7.2f}"
              f"{(p['ac1'] if p['ac1'] is not None else float('nan')):>7.2f}"
              f"{(p['ac20'] if p['ac20'] is not None else float('nan')):>7.2f}"
              f"{(p['ac60'] if p['ac60'] is not None else float('nan')):>7.2f}"
              f"{(f'{hl:.0f}' if hl else '—'):>11}"
              f"{(y['yoy_rank_rho'] if y['yoy_rank_rho'] is not None else float('nan')):>10.2f}"
              f"   {v['verdict']}", flush=True)
    print(f"\n  written to {os.path.basename(REPORT)}", flush=True)
