"""TZQ_DIV_V1 — does a T/Z-vs-Q divergence bar in the 3 bars before a reinforced bull bar (T↑) add anything?

Plan approved 2026-10-04 ("ki"), from the user's RGTI / AMD / SNDK screenshots; k = 3 pre-fixed:
  bar polarity: T/Z sign (T = +, Z = -) x Q direction (Q1-Q4 up, Q5-Q8 down; Q0 neutral)
     TU = T + up (reinforced bull)   TD = T + down (bull divergence)
     ZU = Z + up (bear divergence / absorption)   ZD = Z + down (reinforced bear)
  bar 0 = TU in every test
  H1  >= 1 divergence bar (TD or ZU) in bars -3..-1      vs  TU with NO divergence in -3..-1
  H2  exactly TD, ZD, TU, TU (AMD-2 / SNDK-1)            vs  all other TU bars
  H3  >= 1 ZU in -3..-1                                  vs  TU with NO divergence in -3..-1
Estimand: book per-bar return (entry open[t+1], trail clip(12*ATR%,15,60), maxh 60, 15 bps); 10-bar win secondary.
Paired within day x ATR%-bin (frozen) x bar-0 T/Z state; day bootstrap. Universe close >= $5, 20d $vol >= $5M.
Windows 2021-23 / 2024-26, per year, $21-89. PASS = old rule; Bonferroni k=3 alongside.
"""
from __future__ import annotations
import json, os, sys
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import ATR_CUTS, _paired, _bar_returns   # noqa: E402
from research_tzq_centre import book_returns_vec, load               # noqa: E402

S = 0.0015


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
    qc = np.where(same1, np.select([eq, ab, ou, en & mu, ins & mu, ins, en, ~be], [0, 1, 2, 3, 4, 5, 6, 7], 8), -1)
    sign = np.where(df.tz.str.startswith("T"), "T", np.where(df.tz.str.startswith("Z"), "Z", ""))
    dirn = np.where((qc >= 1) & (qc <= 4), "U", np.where(qc >= 5, "D", ""))
    pol = np.where((sign != "") & (dirn != ""), np.char.add(sign.astype(str), dirn.astype(str)), "N")
    df["pol"] = pol
    g = df.groupby("ticker", sort=False)
    p1, p2, p3 = (g.pol.shift(k) for k in (1, 2, 3))
    have3 = p3.notna().to_numpy()
    P = np.vstack([p3.fillna("").to_numpy(), p2.fillna("").to_numpy(), p1.fillna("").to_numpy()])
    div_any = np.isin(P, ["TD", "ZU"]).any(axis=0)
    zu_any = (P == "ZU").any(axis=0)
    exact = (P[0] == "TD") & (P[1] == "ZD") & (P[2] == "TU")
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    r10 = g.c.shift(-10) / g.o.shift(-1) - 1 - 2 * S
    df["win"] = np.where(r10.notna(), (r10 > 0).astype(float), np.nan)
    A = df.atr.to_numpy(float)
    base = (df.pol == "TU").to_numpy() & have3 & (df.c >= 5).to_numpy() & (df.dv20 >= 5e6).to_numpy() & np.isfinite(A) & (A > 0)
    sel = np.flatnonzero(base)
    rb = book_returns_vec(o, h, l, c, A, tk, sel)
    rng = np.random.default_rng(20261004)
    smp = rng.choice(len(sel), 3000, replace=False)
    assert np.allclose(np.nan_to_num(_bar_returns(o, h, l, c, A, tk, sel[smp]), nan=-9), np.nan_to_num(rb[smp], nan=-9), atol=1e-12)
    ev = df.iloc[sel].copy(); ev["r"] = rb
    ev["k"] = pd.Series(np.digitize(ev.atr / ev.c, ATR_CUTS), index=ev.index).astype(str) + "|" + ev.tz
    ev["yr"] = pd.to_datetime(ev.d).dt.year; ev["d"] = ev.d.astype(str)
    dv, zu, ex = div_any[sel], zu_any[sel], exact[sel]
    T = {"H1 any divergence in -3..-1": (dv, ~dv),
         "H2 TD>ZD>TU>TU":              (ex, ~ex),
         "H3 any ZU in -3..-1":         (zu, ~dv)}
    print("bar-0 TU bars:", len(ev), "| with divergence:", int(dv.sum()), "| ZU:", int(zu.sum()), "| exact H2:", int(ex.sum()))
    out = {}
    for name, (gm, rm) in T.items():
        e = ev[gm | rm].copy(); e["g"] = gm[gm | rm]
        res = {"n_g": int(e.g.sum()), "n_rest": int((~e.g).sum()),
               "raw_ret_g": round(e[e.g].r.mean() * 100, 2), "raw_ret_rest": round(e[~e.g].r.mean() * 100, 2),
               "raw_win_g": round(e[e.g].win.mean() * 100, 1), "raw_win_rest": round(e[~e.g].win.mean() * 100, 1)}
        for wn, m in (("W1", e.yr <= 2023), ("W2", e.yr >= 2024), ("px2189", e.c.between(21, 89))):
            res[wn] = _paired(e[m & e.r.notna()][["d", "k", "r", "g"]], rng)
            res[wn + "_win"] = _paired(e[m & e.win.notna()][["d", "k", "win", "g"]].rename(columns={"win": "r"}), rng)
        res["years"] = {int(y): _paired(e[(e.yr == y) & e.r.notna()][["d", "k", "r", "g"]], rng).get("paired_delta_pp")
                        for y in sorted(e.yr.unique())}
        out[name] = res
    json.dump(out, open(os.path.join(HERE, "..", "research_out", "TZQ_DIV_V1.json"), "w"), indent=1, default=str, ensure_ascii=False)
    def f(x): return f"{x['paired_delta_pp']:+.2f} [{x['ci95'][0]:+.2f},{x['ci95'][1]:+.2f}]" if 'ci95' in x else str(x)
    for name, r in out.items():
        w1, w2 = r["W1"], r["W2"]; ys = [v for v in r["years"].values() if v is not None]
        s = 1 if w1["paired_delta_pp"] > 0 else -1
        ok = (w1["ci95"][0] * w1["ci95"][1] > 0 and w2["ci95"][0] * w2["ci95"][1] > 0 and (w2["paired_delta_pp"] > 0) == (s > 0)
              and sum((y > 0) == (s > 0) for y in ys) >= 4 and min(s * y for y in ys) >= -2 and (r["px2189"]["paired_delta_pp"] > 0) == (s > 0))
        print(f"| {name} | {r['n_g']}/{r['n_rest']} | raw {r['raw_ret_g']}/{r['raw_ret_rest']} win {r['raw_win_g']}/{r['raw_win_rest']} "
              f"| RET {f(w1)} {f(w2)} px {r['px2189']['paired_delta_pp']:+.2f} | WIN {f(r['W1_win'])} {f(r['W2_win'])} "
              f"| yrs {' '.join(f'{v:+.1f}' for v in ys)} | PASS {ok}")


if __name__ == "__main__":
    main()
