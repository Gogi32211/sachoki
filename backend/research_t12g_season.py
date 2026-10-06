"""T12G_CONTEXT_V1 · F10 re-estimated (user chose option 3, 2026-10-04).

F10 as registered (Dec-Mar vs Apr-Nov paired WITHIN a day) is not estimable — the two never share a day.
Estimand used instead (k = 2, T1G and T2G):
  per day, the signal's edge = n-weighted (signal-bar book return − mean of all OTHER liquid bars in the same
  day x ATR%-bin); Δ = mean daily edge on Dec-Mar days − mean daily edge on Apr-Nov days, bootstrap resampling
  days within each season. Same for the +10 %/20-bar/−7 % hit. Windows 2021-23 / 2024-26, per year, $21-89.
PASS = same sign + CI excludes 0 in both windows, >= 4/6 years same sign, worst year >= -2, $21-89 same sign.
"""
from __future__ import annotations
import json, os, sys
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import ATR_CUTS                # noqa: E402
from research_tzq_centre import book_returns_vec           # noqa: E402
import research_sbux_seq as S                              # noqa: E402
import research_t12g_context as T                          # noqa: E402


def daily_edge(e: pd.DataFrame, sig: str, col: str) -> pd.DataFrame:
    f = e[e[col].notna()]
    st = f.groupby(["d", "k"])[col].agg(["sum", "count"])
    s = f[f.tz == sig].groupby(["d", "k"])[col].agg(["sum", "count"]).rename(columns={"sum": "ss", "count": "sn"})
    j = s.join(st)
    j = j[j["count"] > j.sn]
    rest = (j["sum"] - j.ss) / (j["count"] - j.sn)
    j["x"] = (j.ss / j.sn - rest) * j.sn
    dd = j.groupby("d")[["x", "sn"]].sum()
    dd["edge"] = dd.x / dd.sn * 100
    dd["m"] = pd.to_datetime(dd.index).month
    dd["yr"] = pd.to_datetime(dd.index).year
    return dd


def season_delta(dd: pd.DataFrame, rng, B=3000):
    a = dd[dd.m.isin([12, 1, 2, 3])].edge.to_numpy(); b = dd[~dd.m.isin([12, 1, 2, 3])].edge.to_numpy()
    if len(a) < 10 or len(b) < 10:
        return {"note": "too few days", "days": [len(a), len(b)]}
    d = a.mean() - b.mean()
    bs = [a[rng.integers(0, len(a), len(a))].mean() - b[rng.integers(0, len(b), len(b))].mean() for _ in range(B)]
    lo, hi = np.percentile(bs, [2.5, 97.5])
    return {"delta": round(float(d), 2), "ci95": [round(lo, 2), round(hi, 2)],
            "edge_decmar": round(float(a.mean()), 2), "edge_aprnov": round(float(b.mean()), 2), "days": [len(a), len(b)]}


def main():
    df = T.load(); g = df.groupby("ticker", sort=False)
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    tk = df.ticker.to_numpy(); o, h, l, c = (df[x].to_numpy(float) for x in "ohlc"); A = df.atr.to_numpy(float)
    el = np.flatnonzero(((df.c >= 5) & (df.dv20 >= 5e6) & (df.atr > 0) & (df.tz != "")).to_numpy())
    e = df.iloc[el][["d", "c", "atr", "tz"]].copy()
    e["r"] = book_returns_vec(o, h, l, c, A, tk, el)
    e["hit"] = S.first_passage(o, h, l, tk, el)
    e["k"] = np.digitize(e.atr / e.c, ATR_CUTS); e["d"] = e.d.astype(str)
    rng = np.random.default_rng(20261004)
    out = {}
    for sig in T.SIGS:
        res = {}
        for col in ("r", "hit"):
            for wn, m in (("W1", e.d <= "2023-12-31"), ("W2", e.d > "2023-12-31"), ("px", e.c.between(21, 89))):
                res[f"{col}_{wn}"] = season_delta(daily_edge(e[m], sig, col), rng)
        dd = daily_edge(e, sig, "r")
        res["years"] = {int(y): season_delta(dd[dd.yr == y], rng, B=500).get("delta") for y in sorted(dd.yr.unique())}
        w1, w2 = res["r_W1"], res["r_W2"]
        ys = [v for v in res["years"].values() if v is not None]
        sgn = 1 if w1.get("delta", 0) > 0 else -1
        res["PASS"] = bool("ci95" in w1 and "ci95" in w2 and w1["ci95"][0] * w1["ci95"][1] > 0 and w2["ci95"][0] * w2["ci95"][1] > 0
                           and (w2["delta"] > 0) == (sgn > 0) and sum((y > 0) == (sgn > 0) for y in ys) >= 4
                           and min(sgn * y for y in ys) >= -2 and (res["r_px"].get("delta", 0) > 0) == (sgn > 0))
        out[sig] = res
        f = lambda x: f"{x['delta']:+.2f} [{x['ci95'][0]:+.2f},{x['ci95'][1]:+.2f}] (DecMar {x['edge_decmar']:+.2f} / AprNov {x['edge_aprnov']:+.2f})" if "ci95" in x else str(x)
        print(f"{sig}: BOOK W1 {f(w1)}\n      BOOK W2 {f(w2)}\n      px {f(res['r_px'])}\n      HIT W1 {f(res['hit_W1'])}\n      HIT W2 {f(res['hit_W2'])}"
              f"\n      years {' '.join(f'{y}:{v:+.1f}' for y, v in res['years'].items() if v is not None)} | PASS {res['PASS']}")
    json.dump(out, open(os.path.join(HERE, "..", "research_out", "T12G_SEASON_V1.json"), "w"), indent=1, default=str)


if __name__ == "__main__":
    main()
