"""T12G_CONTEXT_V1 — when are T1G and T2G good? 10 book factors, pre-fixed, measured inside each signal.

Plan approved 2026-10-04 ("kki"); k = 10 factors x 2 signals = 20:
  F1  golden cross EMA50 > EMA200                      vs death cross
  F2  RS (close/SPY above its EMA200), inside GC        vs not RS, inside GC
  F3  RSI14 <= 35                                       vs 35-70
  F4  RSI14 >= 70                                       vs 35-70
  F5  volume / 20-bar median >= 1.5 (V>=4)              vs < 1.5 (V<=3)
  F6  gap class G3                                      vs G1   (bar_gap_range head)
  F7  close location in the bar's range < 50 % (weak)   vs >= 80 % (strong)
  F8  close / EMA20 - 1 <= +2 % (near the mean)         vs > +6 % (stretched)
  F9  10-bar return <= -5 % (after a fall)              vs >= +5 % (after a rise)
  F10 month Dec-Mar                                     vs Apr-Nov
Estimand: book per-bar return (entry open[t+1], trail clip(12*ATR%,15,60), maxh 60, 15 bps) and +10 % within 20 bars
before -7 %; paired group vs rest INSIDE the same signal, within day x ATR%-bin (frozen), day bootstrap.
Liquid universe (close >= $5, 20d $vol >= $5M). Windows 2021-23 / 2024-26, years, $21-89.
PASS: same sign + CI excludes 0 in both windows, >= 4/6 years same sign, worst year (that sign) >= -2, $21-89 same sign;
Bonferroni k=20 alongside. The combination of passing factors is shown DESCRIPTIVELY only (built from the passes).
"""
from __future__ import annotations
import json, os, sys
import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import ATR_CUTS, _paired            # noqa: E402
from research_tzq_centre import book_returns_vec                # noqa: E402
import research_sbux_seq as S                                   # noqa: E402
from studio.paths import db_path                                # noqa: E402
from studio.db import UNIVERSE_PRIORITY_SQL                     # noqa: E402

SIGS = ("T1G", "T2G")


def load() -> pd.DataFrame:
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    df = con.execute(f"""SELECT ticker, CAST(date AS DATE) d, open o, high h, low l, close c, volume v,
            COALESCE(NULLIF(t_sig,''), NULLIF(z_sig,''), '') tz, rsi_14 rsi, COALESCE(bar_gap_range,'') gr
        FROM bars WHERE universe <> 'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
        ORDER BY ticker, date""").fetchdf()
    spy = con.execute("""SELECT CAST(date AS DATE) d, any_value(close) spy FROM bars WHERE ticker = 'SPY' GROUP BY 1""").fetchdf()
    assert not df.duplicated(["ticker", "d"]).any(), "data contract: duplicate ticker-date"
    assert len(spy) > 1000, "SPY missing"
    return df.merge(spy, on="d", how="left").sort_values(["ticker", "d"]).reset_index(drop=True)


def main():
    df = load()
    g = df.groupby("ticker", sort=False)
    ew = lambda col, span: g[col].transform(lambda s: s.ewm(span=span, adjust=False, min_periods=span).mean())
    df["e20"], df["e50"], df["e200"] = ew("c", 20), ew("c", 50), ew("c", 200)
    df["ratio"] = df.c / df.spy
    df["rse"] = g["ratio"].transform(lambda s: s.ewm(span=200, adjust=False, min_periods=200).mean())
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["vmed"] = g.v.transform(lambda s: s.rolling(20, min_periods=20).median())
    df["ret10"] = df.c / g.c.shift(10) - 1
    tk = df.ticker.to_numpy(); o, h, l, c = (df[x].to_numpy(float) for x in "ohlc")
    A = df.atr.to_numpy(float)
    el = np.flatnonzero((df.tz.isin(SIGS) & (df.c >= 5) & (df.dv20 >= 5e6) & (df.atr > 0)).to_numpy())
    e = df.iloc[el].copy()
    e["r"] = book_returns_vec(o, h, l, c, A, tk, el)
    e["hit"] = S.first_passage(o, h, l, tk, el)
    e["k"] = np.digitize(e.atr / e.c, ATR_CUTS); e["yr"] = pd.to_datetime(e.d).dt.year; e["d"] = e.d.astype(str)
    gc = e.e50 > e.e200; dc = e.e50 < e.e200
    rs = e.ratio > e.rse; nrs = e.ratio < e.rse
    vr = e.v / e.vmed
    gcl = e.gr.str.split("-").str[0]
    loc = (e.c - e.l) / (e.h - e.l).replace(0, np.nan)
    dist = e.c / e.e20 - 1
    mon = pd.to_datetime(e.d).dt.month
    mid = e.rsi.between(35, 70, inclusive="neither")
    F = {"F1 GC vs DC":               (gc, dc),
         "F2 RS vs ¬RS (inside GC)":  (gc & rs, gc & nrs),
         "F3 RSI≤35 vs 35-70":        (e.rsi <= 35, mid),
         "F4 RSI≥70 vs 35-70":        (e.rsi >= 70, mid),
         "F5 vol≥1.5×med vs <1.5":    (vr >= 1.5, vr < 1.5),
         "F6 gap G3 vs G1":           (gcl == "G3", gcl == "G1"),
         "F7 close-loc <50% vs ≥80%": (loc < 0.5, loc >= 0.8),
         "F8 ≤+2% over EMA20 vs >+6%": (dist <= 0.02, dist > 0.06),
         "F9 ret10 ≤−5% vs ≥+5%":     (e.ret10 <= -0.05, e.ret10 >= 0.05),
         "F10 Dec-Mar vs Apr-Nov":    (mon.isin([12, 1, 2, 3]), ~mon.isin([12, 1, 2, 3]))}
    rng = np.random.default_rng(20261004)
    out, card = {}, {}
    for sig in SIGS:
        es = e[e.tz == sig]
        for name, (gm, rm) in F.items():
            gm_, rm_ = gm.loc[es.index].fillna(False).to_numpy(bool), rm.loc[es.index].fillna(False).to_numpy(bool)
            f = es[gm_ | rm_].assign(g=gm_[gm_ | rm_])
            res = {"n_g": int(f.g.sum()), "n_rest": int((~f.g).sum())}
            for col in ("r", "hit"):
                for wn, m in (("W1", f.d <= "2023-12-31"), ("W2", f.d > "2023-12-31"), ("px", f.c.between(21, 89))):
                    res[f"{col}_{wn}"] = _paired(f[m & f[col].notna()][["d", "k", col, "g"]].rename(columns={col: "r"}), rng)
            res["years"] = {int(y): _paired(f[(f.yr == y) & f.r.notna()][["d", "k", "r", "g"]], rng).get("paired_delta_pp")
                            for y in sorted(f.yr.unique())}
            w1, w2 = res["r_W1"], res["r_W2"]
            ys = [v for v in res["years"].values() if v is not None]
            ok = False
            if "ci95" in w1 and "ci95" in w2 and ys:
                sgn = 1 if w1["paired_delta_pp"] > 0 else -1
                ok = (w1["ci95"][0] * w1["ci95"][1] > 0 and w2["ci95"][0] * w2["ci95"][1] > 0
                      and (w2["paired_delta_pp"] > 0) == (sgn > 0) and sum((y > 0) == (sgn > 0) for y in ys) >= 4
                      and min(sgn * y for y in ys) >= -2 and (res["r_px"].get("paired_delta_pp", 0) > 0) == (sgn > 0))
                res["sign"] = sgn
            res["PASS"] = ok
            out[f"{sig} | {name}"] = res
    json.dump(out, open(os.path.join(HERE, "..", "research_out", "T12G_CONTEXT_V1.json"), "w"), indent=1, default=str, ensure_ascii=False)
    fmt = lambda x: f"{x['paired_delta_pp']:+.2f} [{x['ci95'][0]:+.2f},{x['ci95'][1]:+.2f}]" if "ci95" in x else "–"
    for key, r in out.items():
        print(f"| {key} | {r['n_g']}/{r['n_rest']} | {fmt(r['r_W1'])} | {fmt(r['r_W2'])} | {r['r_px'].get('paired_delta_pp', float('nan')):+.2f} | "
              f"hit {fmt(r['hit_W1'])} {fmt(r['hit_W2'])} | {' '.join(f'{v:+.1f}' for v in r['years'].values() if v is not None)} | {'PASS' + ('+' if r.get('sign', 0) > 0 else '−') if r['PASS'] else ''}")
    # live list of today's T1G / T2G with the factor flags (display only)
    last = df.d.max()
    lv = df[(df.d == last) & df.tz.isin(SIGS) & (df.c >= 5) & (df.dv20 >= 5e6)].copy()
    lv["GC"] = lv.e50 > lv.e200; lv["RS"] = lv.ratio > lv.rse
    lv["RSI"] = lv.rsi.round(0); lv["volx"] = (lv.v / lv.vmed).round(2)
    lv["gap"] = lv.gr.str.split("-").str[0]; lv["loc"] = ((lv.c - lv.l) / (lv.h - lv.l)).round(2)
    lv["ema20%"] = ((lv.c / lv.e20 - 1) * 100).round(1); lv["ret10%"] = (lv.ret10 * 100).round(1)
    lv[["ticker", "tz", "c", "GC", "RS", "RSI", "volx", "gap", "loc", "ema20%", "ret10%"]].to_csv(
        os.path.join(HERE, "..", "research_out", "T12G_CONTEXT_V1_live.csv"), index=False)
    print("live", str(last)[:10], len(lv))


if __name__ == "__main__":
    main()
