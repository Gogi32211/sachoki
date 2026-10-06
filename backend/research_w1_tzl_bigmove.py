"""W1_TZL_BIGMOVE_V1 — 3- and 4-bar weekly TZL sequences followed by a +20 % (and +40 %) move.

Plan approved 2026-10-04 ("ki"):
  space    1W DB; bars -3..-1 = T/Z state, bar 0 = T/Z + L (TZL); 3-bar and 4-bar; cells with n >= 100 and
           >= 30 distinct weeks in MINE; k reported
  outcome  primary: +20 % (high) within 13 weeks BEFORE a -15 % stop; entry next week's open; bar by bar, at each
           week the open is checked first (gap), then the stop BEFORE the target (conservative). secondary (reported,
           not selected on): +40 % within 26 weeks, same stop.
  contrast hit-rate lift vs the same week x weekly ATR%-bin (cut points frozen from MINE bars), week-clustered SE;
           universe close >= $5 and 10-week mean $volume >= $25M/week (~$5M/day); $21-89 read separately
  split    MINE 2021-06..2023-12 -> VERIFY 2024..; events whose horizon runs past the data are censored (NaN)
  PASS     VERIFY lift > 0 & CI lo > 0, >= 4 years positive, $21-89 > 0; Bonferroni over the verify k alongside
"""
from __future__ import annotations
import json, os, sys
import duckdb
import numpy as np
import pandas as pd
from scipy.stats import norm

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from studio.paths import db_path                       # noqa: E402
from studio.db import UNIVERSE_PRIORITY_SQL            # noqa: E402
from research_q3_seq import paired_all                 # noqa: E402

MIN_N, MIN_WEEKS, STOP = 100, 30, 0.15
OUT = os.path.join(HERE, "..", "research_out")


def load() -> pd.DataFrame:
    con = duckdb.connect(db_path("1w"), read_only=True)
    df = con.execute(f"""SELECT ticker, CAST(date AS DATE) d, open o, high h, low l, close c, volume v,
            COALESCE(NULLIF(t_sig,''), NULLIF(z_sig,''), '') tz, COALESCE(l_sig, '') ls
        FROM bars WHERE universe <> 'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
        ORDER BY ticker, date""").fetchdf()
    assert not df.duplicated(["ticker", "d"]).any(), "data contract: duplicate ticker-week"
    return df.reset_index(drop=True)


def first_passage(o, h, l, tk, idx, target, horizon):
    """1 = target before stop, 0 = stop first or neither within horizon, NaN = censored (data/ticker ends)."""
    n = len(o); idx = np.asarray(idx)
    out = np.full(len(idx), np.nan)
    end = idx + horizon
    ok = (end < n)
    ok[ok] &= tk[end[ok]] == tk[idx[ok]]
    ok &= np.r_[o[np.minimum(idx + 1, n - 1)]] > 0
    t = idx[ok]
    entry = o[t + 1]; tp = entry * (1 + target); sp = entry * (1 - STOP)
    res = np.full(len(t), np.nan); live = np.ones(len(t), bool)
    for s in range(1, horizon + 1):
        j = t + s
        og, ol, oh = o[j], l[j], h[j]
        if s > 1:
            gs = live & (og <= sp); res[gs] = 0; live &= ~gs
            gt = live & (og >= tp); res[gt] = 1; live &= ~gt
        st = live & (ol <= sp); res[st] = 0; live &= ~st
        hi = live & (oh >= tp); res[hi] = 1; live &= ~hi
    res[live] = 0
    out[ok] = res
    return out


def main():
    df = load()
    tk = df.ticker.to_numpy(); o, h, l, c = (df[x].to_numpy(float) for x in "ohlc")
    g = df.groupby("ticker", sort=False)
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv10"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(10, min_periods=10).mean())
    df["tzl"] = df.tz + "·" + df.ls
    t1, t2, t3 = (g.tz.shift(k) for k in (1, 2, 3))
    df["seq3"] = np.where(t2.notna() & t1.notna(), t2.fillna("") + ">" + t1.fillna("") + ">" + df.tzl, None)
    df["seq4"] = np.where(t3.notna() & t2.notna() & t1.notna(),
                          t3.fillna("") + ">" + t2.fillna("") + ">" + t1.fillna("") + ">" + df.tzl, None)
    elig = ((df.c >= 5) & (df.dv10 >= 25e6) & df.atr.gt(0) & (df.tz != "") & df.seq3.notna()).to_numpy()
    idx = np.flatnonzero(elig)
    df["hit20"] = np.nan; df["hit40"] = np.nan
    df.loc[idx, "hit20"] = first_passage(o, h, l, tk, idx, 0.20, 13)
    df.loc[idx, "hit40"] = first_passage(o, h, l, tk, idx, 0.40, 26)
    df["yr"] = pd.to_datetime(df.d).dt.year
    ev = df.iloc[idx].copy()
    ev["atrp"] = ev.atr / ev.c
    mine_m = ev.d.astype(str) <= "2023-12-31"
    cuts = list(np.quantile(ev.loc[mine_m, "atrp"], np.linspace(0, 1, 23)[1:-1]).round(6))   # frozen from MINE
    ev["k"] = np.digitize(ev.atrp, cuts)
    ev["d"] = ev.d.astype(str)
    summary = {"weekly_atr_cuts_frozen_from_mine": cuts,
               "eligible_bars": int(len(ev)),
               "base_hit20": {"mine": round(ev[mine_m].hit20.mean() * 100, 1), "verify": round(ev[~mine_m].hit20.mean() * 100, 1)},
               "base_hit40": {"mine": round(ev[mine_m].hit40.mean() * 100, 1), "verify": round(ev[~mine_m].hit40.mean() * 100, 1)}}
    print(json.dumps(summary))
    last_week = df.d.max()
    allres = {"summary": summary}
    for sq in ("seq3", "seq4"):
        e = ev[ev[sq].notna() & ev.hit20.notna()].copy()
        e["seq"] = e[sq]; e["win"] = e.hit20; e["r"] = e.hit20
        mine, ver = e[e.d <= "2023-12-31"], e[e.d > "2023-12-31"]
        M = paired_all(mine, ["seq"])
        cells = M[(M.n >= MIN_N) & (M.days >= MIN_WEEKS)]
        cand = cells[(cells.lift > 0) & (cells.lo > 0)].sort_values("lift", ascending=False)
        k_mine, k_ver = len(cells), len(cand)
        tag = lambda f: f.assign(seq=np.where(f.seq.isin(cand.index), f.seq, "~rest"))
        V = paired_all(tag(ver), ["seq"]).drop("~rest", errors="ignore")
        Y = paired_all(tag(e), ["seq", "yr"]).drop("~rest", level=0, errors="ignore")
        px = e[e.c.between(21, 89)]
        PZ = paired_all(tag(px), ["seq"]).drop("~rest", errors="ignore")
        e40 = e[e.hit40.notna()].assign(win=lambda f: f.hit40, r=lambda f: f.hit40)
        V40 = paired_all(tag(e40[e40.d > "2023-12-31"]), ["seq"]).drop("~rest", errors="ignore")
        zb = norm.ppf(1 - 0.025 / max(k_ver, 1))
        rows = []
        for s in cand.index:
            v = V.loc[s] if s in V.index else None
            yl = Y.loc[s].lift if s in Y.index.get_level_values(0) else pd.Series(dtype=float)
            pz = PZ.loc[s] if s in PZ.index else None
            r = {"seq": s, "mine_n": int(M.loc[s, "n"]), "mine_hit": round(M.loc[s, "win"], 1),
                 "mine_lift": round(M.loc[s, "lift"], 2), "mine_lo": round(M.loc[s, "lo"], 2),
                 "ver_n": int(v.n) if v is not None else 0,
                 "ver_hit": round(v.win, 1) if v is not None else None,
                 "ver_lift": round(v.lift, 2) if v is not None else None,
                 "ver_lo": round(v.lo, 2) if v is not None else None,
                 "ver_lo_bonf": round(v.lift - zb * v.se, 2) if v is not None else None,
                 "ver40_lift": round(V40.loc[s, "lift"], 2) if s in V40.index else None,
                 "years": {int(k_): round(x, 1) for k_, x in yl.items()},
                 "years_pos": int((yl > 0).sum()),
                 "px2189_lift": round(pz.lift, 2) if pz is not None else None, "px2189_n": int(pz.n) if pz is not None else 0}
            r["PASS"] = bool(v is not None and r["ver_lift"] > 0 and r["ver_lo"] > 0 and r["years_pos"] >= 4
                             and (r["px2189_lift"] or -1) > 0)
            r["PASS_bonf"] = bool(r["PASS"] and r["ver_lo_bonf"] > 0)
            rows.append(r)
        vlifts = [r["ver_lift"] for r in rows if r["ver_lift"] is not None]
        live = df[(df.d == last_week) & df[sq].isin([r["seq"] for r in rows if r["PASS"]]) & (df.c >= 5) & (df.dv10 >= 25e6)]
        res = {"k_mine_cells": k_mine, "k_verify": k_ver, "n_pass": sum(r["PASS"] for r in rows),
               "n_pass_bonf": sum(r["PASS_bonf"] for r in rows),
               "verify_positive": f"{sum(x > 0 for x in vlifts)}/{len(vlifts)}",
               "mean_lift_mine": round(float(cand.lift.mean()), 2) if k_ver else None,
               "mean_lift_verify": round(float(np.mean(vlifts)), 2) if vlifts else None,
               "rows": rows, "live_last_week": live[["ticker", "c", sq]].to_dict("records"),
               "last_week": str(last_week)}
        allres[sq] = res
        print(sq, json.dumps({k_: v_ for k_, v_ in res.items() if k_ not in ("rows", "live_last_week")}))
        for r in rows:
            if r["PASS"] or (r["ver_lift"] or -9) > 0:
                print("  ", r)
    json.dump(allres, open(os.path.join(OUT, "W1_TZL_BIGMOVE_V1.json"), "w"), indent=1, default=str, ensure_ascii=False)


if __name__ == "__main__":
    main()
