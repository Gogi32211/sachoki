"""TZQ_CENTRE_V1 — does the Q body-centre split change the outcome INSIDE the same T/Z state?

Plan approved 2026-10-03 ("ki"), k = 9 pre-fixed contrasts (group g vs the rest of the same state):
  T4, T6, Z4, Z6    g = Q3 (engulf, centre up)  vs Q6 (centre down)
  T9, T10, Z9, Z10  g = Q4 (inside, centre up)  vs Q5 (centre down)
  Z11               g = Q1R (fully above)       vs Q2R (overlap up)
Estimand: book per-bar return (entry open[t+1], trail clip(12*ATR%,15,60), maxh 60, 15 bps — the
bottom_cluster_forward._bar_returns rule, vectorised here and checked against it) and 10-bar win
(close[t+10]/open[t+1]-1-30bps > 0); paired g vs rest within day x ATR%-bin (frozen ATR_CUTS),
n-weighted per day, day bootstrap (bottom_cluster_forward._paired).
Universe: close >= $5, 20d $volume >= $5M. Windows 2021-23 / 2024-26, per-year, $21-89.
PASS: same sign + CI excludes 0 in both windows, >= 4/6 years same sign, worst year (in that sign) >= -2,
$21-89 same sign. Bonferroni k=9 reported alongside.
"""
from __future__ import annotations
import json, os, sys
import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bottom_cluster_forward as BCF                                          # noqa: E402
from bottom_cluster_forward import ATR_CUTS, _paired, _bar_returns           # noqa: E402

# maxh: 60 = registered estimand; `python research_tzq_centre.py 21` = the user's 21-bar sensitivity run
HORIZON = int(sys.argv[1]) if __name__ == "__main__" and len(sys.argv) > 1 else 60
BCF.HORIZON = HORIZON          # the reference loop reads its module global — keep the parity check on the same horizon
from studio.paths import db_path                                             # noqa: E402
from studio.db import UNIVERSE_PRIORITY_SQL                                  # noqa: E402

S = 0.0015
CONTRASTS = {"T4": 3, "T6": 3, "Z4": 3, "Z6": 3, "T9": 4, "T10": 4, "Z9": 4, "Z10": 4, "Z11": 1}


def book_returns_vec(o, h, l, c, atr, tk, idx):
    """Vectorised _bar_returns: identical rule, all signal bars stepped together."""
    n = len(o); idx = np.asarray(idx)
    end = idx + 1 + HORIZON
    ok = end <= n
    e1 = np.minimum(end - 1, n - 1)
    ok &= (tk[e1] == tk[idx]) & (o[np.minimum(idx + 1, n - 1)] > 0) & (atr[idx] > 0) & (c[idx] > 0)
    out = np.full(len(idx), np.nan)
    t = idx[ok]
    tr = np.clip(12.0 * atr[t] / c[t], 0.15, 0.60)
    entry = o[t + 1] * (1 + S); pk = entry.copy()
    r = np.full(len(t), np.nan); live = np.ones(len(t), bool)
    for s in range(1, HORIZON + 1):
        j = t + s
        if s > 1:
            gap = live & (o[j] <= pk * (1 - tr))
            r[gap] = o[j][gap] / entry[gap] - 1 - S; live &= ~gap
        pk = np.where(live, np.maximum(pk, h[j]), pk)
        ts = pk * (1 - tr)
        hit = live & (l[j] <= ts)
        r[hit] = ts[hit] / entry[hit] - 1 - S; live &= ~hit
    r[live] = c[t[live] + HORIZON] / entry[live] - 1 - S
    out[ok] = r
    return out


def load():
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    df = con.execute(f"""SELECT ticker, CAST(date AS DATE) d, open o, high h, low l, close c, volume v,
            COALESCE(NULLIF(t_sig,''), NULLIF(z_sig,''), '') tz
        FROM bars WHERE universe <> 'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
        ORDER BY ticker, date""").fetchdf()
    assert not df.duplicated(["ticker", "d"]).any(), "data contract: duplicate ticker-date"
    return df.reset_index(drop=True)


def main():
    df = load()
    tk = df.ticker.to_numpy(); o, h, l, c = (df[x].to_numpy(float) for x in "ohlc")
    same1 = np.r_[False, tk[1:] == tk[:-1]]
    qt, qb = np.maximum(o, c), np.minimum(o, c); pqt, pqb = np.r_[np.nan, qt[:-1]], np.r_[np.nan, qb[:-1]]
    mu = (qt + qb) >= (pqt + pqb)
    eq = (qt == pqt) & (qb == pqb); ab = ~eq & (qb > pqt); be = ~eq & (qt < pqb)
    en = ~eq & ~ab & ~be & (qt >= pqt) & (qb <= pqb)
    ins = ~eq & ~ab & ~be & ~en & (qt <= pqt) & (qb >= pqb)
    ou = ~eq & ~ab & ~be & ~en & ~ins & (qt > pqt)
    df["qc"] = np.where(same1, np.select([eq, ab, ou, en & mu, ins & mu, ins, en, ~be], [0, 1, 2, 3, 4, 5, 6, 7], 8), -1)
    g = df.groupby("ticker", sort=False)
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    r10 = g.c.shift(-10) / g.o.shift(-1) - 1 - 2 * S
    df["win"] = np.where(r10.notna(), (r10 > 0).astype(float), np.nan)
    df["yr"] = pd.to_datetime(df.d).dt.year
    A = df.atr.to_numpy(float)
    sel = np.flatnonzero(df.tz.isin(CONTRASTS).to_numpy() & (df.c >= 5).to_numpy() & (df.dv20 >= 5e6).to_numpy()
                         & np.isfinite(A) & (A > 0) & (df.qc >= 0).to_numpy())
    rb = book_returns_vec(o, h, l, c, A, tk, sel)
    # parity check vs the reference loop on a random sample
    rng = np.random.default_rng(20261003)
    smp = rng.choice(len(sel), 3000, replace=False)
    ref = _bar_returns(o, h, l, c, A, tk, sel[smp])
    assert np.allclose(np.nan_to_num(ref, nan=-9), np.nan_to_num(rb[smp], nan=-9), atol=1e-12), "vectorised path-sim != reference"
    ev = df.iloc[sel].copy(); ev["r"] = rb
    ev["k"] = np.digitize(ev.atr / ev.c, ATR_CUTS)
    ev["g"] = ev.qc.to_numpy() == ev.tz.map(CONTRASTS).to_numpy()
    # Z11 rest must be Q2R only; engulf/inside states: rest = the other centre
    other = {3: 6, 4: 5, 1: 2}
    ev = ev[ev.g | (ev.qc.to_numpy() == ev.tz.map(CONTRASTS).map(other).to_numpy())]
    ev["d"] = ev.d.astype(str)
    out = {}
    for st in CONTRASTS:
        e = ev[ev.tz == st]
        res = {"n_g": int(e.g.sum()), "n_rest": int((~e.g).sum()),
               "raw_ret_g": round(e[e.g].r.mean() * 100, 2), "raw_ret_rest": round(e[~e.g].r.mean() * 100, 2),
               "raw_win_g": round(e[e.g].win.mean() * 100, 1), "raw_win_rest": round(e[~e.g].win.mean() * 100, 1)}
        for name, m in (("W1", e.yr <= 2023), ("W2", e.yr >= 2024), ("px2189", e.c.between(21, 89))):
            sub = e[m & e.r.notna()]
            res[name] = _paired(sub[["d", "k", "r", "g"]], rng)
            sw = e[m & e.win.notna()][["d", "k", "win", "g"]].rename(columns={"win": "r"})
            res[name + "_win"] = _paired(sw, rng)
        res["years"] = {int(y): _paired(e[(e.yr == y) & e.r.notna()][["d", "k", "r", "g"]], rng).get("paired_delta_pp")
                        for y in sorted(e.yr.unique())}
        out[st] = res
        print(st, json.dumps(res, default=str))
    json.dump(out, open(os.path.join(HERE, "..", "research_out", "TZQ_CENTRE_V1.json" if HORIZON == 60 else f"TZQ_CENTRE_V1_h{HORIZON}.json"), "w"), indent=1, default=str)


if __name__ == "__main__":
    main()
