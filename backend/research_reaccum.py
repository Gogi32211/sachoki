"""REACCUM_V1 — the AMD Feb-Mar 2026 re-accumulation as a universe-wide STATE (not a sequence).

User: "ki gegma momwons gaakete" (2026-10-05). Spec frozen here BEFORE any outcome was computed.
STATE at day t (all must hold):
  S1 golden cross: EMA50 > EMA200
  S2 climax anchor: a VABS climax bar (sig_clm = 1) somewhere in [t-60, t-10]
  S3 floor tested: shapectx `touches` >= 4 at t
  S4 no knife: min RSI14 over [t-10, t] > 35
  S5 no-supply on dips: >= 2 VABS NS bars (sig_ns_vabs) in [t-20, t]
  S6 volume dried up: median(volume / 20-bar median volume) over [t-15, t-1] < 1.0
TRIGGERS (k = 3):
  RA-S  any day in the state
  RA-A  state ∧ edge cluster conf_n >= 4 for the first time (conf_n(t-1) < 4)   [AMD 2026-03-04]
  RA-B  state ∧ VABS BEST★ (sig_best = 1)                                        [AMD 2026-03-31]
Estimand: book per-bar return (entry open[t+1], trail clip(12*ATR%,15,60), maxh 60, 15 bps); +10 % within 20 bars
before -7 % secondary. Comparisons (same day x ATR%-bin, day bootstrap):
  primary  vs golden-cross bars only  (does the accumulation add anything beyond GC, which is already known)
  side     vs all liquid bars          (is it good at all)
Liquid universe (close >= $5, 20d $vol >= $5M). Windows 2022-23 / 2024-26 (GC needs 200 bars), years, $21-89.
PASS (primary): same sign + CI excludes 0 in both windows, >= 4/5 years same sign, worst year >= -2, $21-89 same
sign; Bonferroni k=3 alongside. Sanity: AMD 2026-03-04 must fire RA-A and AMD 2026-03-31 RA-B (logic, not evidence).
"""
from __future__ import annotations
import json, os, sys
import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import ATR_CUTS, _paired      # noqa: E402
from research_tzq_centre import book_returns_vec          # noqa: E402
import research_sbux_seq as S                             # noqa: E402
from studio.paths import db_path                          # noqa: E402
from studio.db import UNIVERSE_PRIORITY_SQL               # noqa: E402

OUT = os.path.join(HERE, "..", "research_out")
DATA = os.path.join(HERE, "..", "data")


def load() -> pd.DataFrame:
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    df = con.execute(f"""SELECT ticker, CAST(date AS DATE) d, open o, high h, low l, close c, volume v, rsi_14 rsi,
            COALESCE(sig_clm,0) clm, COALESCE(sig_ns_vabs,0) ns, COALESCE(sig_best,0) best
        FROM bars WHERE universe <> 'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
        ORDER BY ticker, date""").fetchdf()
    assert not df.duplicated(["ticker", "d"]).any(), "data contract: duplicate ticker-date"
    sh = pd.read_parquet(os.path.join(DATA, "shapectx_signals.parquet"), columns=["ticker", "date", "touches"])
    sh["d"] = pd.to_datetime(sh.date).dt.date; sh = sh.drop(columns="date").drop_duplicates(["ticker", "d"])
    ev = pd.read_parquet(os.path.join(DATA, "edge_votes_v1.parquet"), columns=["ticker", "session", "conf_n"])
    ev["d"] = pd.to_datetime(ev.session).dt.date; ev = ev.drop(columns="session").drop_duplicates(["ticker", "d"])
    df["d"] = pd.to_datetime(df.d).dt.date
    df = df.merge(sh, on=["ticker", "d"], how="left").merge(ev, on=["ticker", "d"], how="left")
    return df.sort_values(["ticker", "d"]).reset_index(drop=True)


def main():
    df = load(); g = df.groupby("ticker", sort=False)
    ew = lambda col, span: g[col].transform(lambda s: s.ewm(span=span, adjust=False, min_periods=span).mean())
    df["e50"], df["e200"] = ew("c", 50), ew("c", 200)
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["vmed"] = g.v.transform(lambda s: s.rolling(20, min_periods=20).median())
    df["vr"] = df.v / df.vmed
    roll = lambda col, win, fn: g[col].transform(lambda s: getattr(s.rolling(win, min_periods=win), fn)())
    # S2: climax in [t-60, t-10]  == rolling-51 max of clm, shifted by 10
    df["clm_win"] = g["clm"].transform(lambda s: s.shift(10).rolling(51, min_periods=1).max())
    df["rsi_min"] = roll("rsi", 11, "min")
    df["ns_cnt"] = roll("ns", 21, "sum")
    df["vr_med"] = g["vr"].transform(lambda s: s.shift(1).rolling(15, min_periods=15).median())
    df["conf_prev"] = g["conf_n"].shift(1)
    state = ((df.e50 > df.e200) & (df.clm_win >= 1) & (df.touches >= 4) & (df.rsi_min > 35)
             & (df.ns_cnt >= 2) & (df.vr_med < 1.0))
    df["RA_S"] = state
    df["RA_A"] = state & (df.conf_n >= 4) & (df.conf_prev.fillna(0) < 4)
    df["RA_B"] = state & (df.best == 1)
    for d, v in (("2026-03-04", "RA_A"), ("2026-03-31", "RA_B")):
        r = df[(df.ticker == "AMD") & (df.d.astype(str) == d)]
        print("SANITY AMD", d, v, r[["RA_S", "RA_A", "RA_B", "clm_win", "touches", "rsi_min", "ns_cnt", "vr_med", "conf_n"]].to_dict("records"))
    liq = ((df.c >= 5) & (df.dv20 >= 5e6) & (df.atr > 0) & df.e200.notna()).to_numpy()
    print("state counts (liquid):", {v: int((df[v] & liq).sum()) for v in ("RA_S", "RA_A", "RA_B")})
    tk = df.ticker.to_numpy(); o, h, l, c = (df[x].to_numpy(float) for x in "ohlc"); A = df.atr.to_numpy(float)
    el = np.flatnonzero(liq)
    e = df.iloc[el][["ticker", "d", "c", "atr", "e50", "e200", "RA_S", "RA_A", "RA_B"]].copy()
    e["r"] = book_returns_vec(o, h, l, c, A, tk, el)
    e["hit"] = S.first_passage(o, h, l, tk, el)
    e = e[e.r.notna()]
    e["k"] = np.digitize(e.atr / e.c, ATR_CUTS); e["d"] = e.d.astype(str); e["yr"] = pd.to_datetime(e.d).dt.year
    e = e[e.yr >= 2022]
    gcm = (e.e50 > e.e200)
    rng = np.random.default_rng(20261005)
    fmt = lambda x: f"{x['paired_delta_pp']:+.2f} [{x['ci95'][0]:+.2f},{x['ci95'][1]:+.2f}]" if "ci95" in x else str(x.get("note"))
    out = {}
    for v in ("RA_S", "RA_A", "RA_B"):
        P = e[e[v]]
        res = {"n": int(len(P)), "days": int(P.d.nunique()), "tickers": int(P.ticker.nunique()),
               "raw_ret": round(P.r.mean() * 100, 2), "raw_hit": round(P.hit.mean() * 100, 1)}
        for cmp_name, f in (("vs_GC", e[gcm]), ("vs_all", e)):
            f = f.assign(g=f[v])
            blk = {}
            for col in ("r", "hit"):
                for wn, m in (("W1", f.d <= "2023-12-31"), ("W2", f.d > "2023-12-31"), ("px", f.c.between(21, 89))):
                    blk[f"{col}_{wn}"] = _paired(f[m & f[col].notna()][["d", "k", col, "g"]].rename(columns={col: "r"}), rng)
            blk["years"] = {int(y): _paired(f[f.yr == y][["d", "k", "r", "g"]], rng).get("paired_delta_pp") for y in sorted(f.yr.unique())}
            w1, w2 = blk["r_W1"], blk["r_W2"]; ys = [x for x in blk["years"].values() if x is not None]
            ok = False
            if "ci95" in w1 and "ci95" in w2 and ys:
                s = 1 if w1["paired_delta_pp"] > 0 else -1
                ok = (w1["ci95"][0] * w1["ci95"][1] > 0 and w2["ci95"][0] * w2["ci95"][1] > 0 and (w2["paired_delta_pp"] > 0) == (s > 0)
                      and sum((y > 0) == (s > 0) for y in ys) >= 4 and min(s * y for y in ys) >= -2
                      and (blk["r_px"].get("paired_delta_pp", 0) > 0) == (s > 0))
            blk["PASS"] = bool(ok)
            res[cmp_name] = blk
        out[v] = res
        print(f"=== {v}: n {res['n']} days {res['days']} tickers {res['tickers']} raw ret {res['raw_ret']}% raw hit {res['raw_hit']}%")
        for cmp_name in ("vs_GC", "vs_all"):
            b = res[cmp_name]
            print(f"  {cmp_name}: BOOK W1 {fmt(b['r_W1'])} W2 {fmt(b['r_W2'])} px {fmt(b['r_px'])} | HIT W1 {fmt(b['hit_W1'])} W2 {fmt(b['hit_W2'])}"
                  f" | yrs {' '.join(f'{y}:{x:+.1f}' for y, x in b['years'].items() if x is not None)} | PASS {b['PASS']}")
    last = df.d.max()
    lv = df[(df.d == last) & df.RA_S & (df.c >= 5) & (df.dv20 >= 5e6)][["ticker", "c", "RA_A", "RA_B", "touches", "conf_n", "rsi"]]
    lv.to_csv(os.path.join(OUT, "REACCUM_V1_live.csv"), index=False)
    out["live"] = {"date": str(last), "n": int(len(lv))}
    json.dump(out, open(os.path.join(OUT, "REACCUM_V1.json"), "w"), indent=1, default=str)
    print("live", last, len(lv), lv.ticker.tolist()[:40])


if __name__ == "__main__":
    main()
