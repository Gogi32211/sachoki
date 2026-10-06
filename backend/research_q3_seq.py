"""Q3_SEQ_V1 — 3-bar Q-code sequences (Pine 260924 Q alphabet, line 1 only) with high 10-bar win rate.

Plan approved 2026-10-03 ("ki"):
  space   3-bar sequences of Q0-Q8 + G/R (line 1 only)
  metric  win = close[t+10] / open[t+1] - 1 - 30 bps > 0; lift = paired vs the rest of the SAME day x ATR%-bin
          (frozen ATR_CUTS), n-weighted per day, day-clustered CI
  split   MINE 2021-23 (n >= 300) -> VERIFY 2024-26 (right edge censored: last 11 sessions dropped)
  univ    liquid: close >= $5 and 20d $volume >= $5M; survivors also read on $21-89
  PASS    lift > 0 & CI lo > 0 in BOTH windows, >= 4/6 years positive, worst year >= -2 pp, $21-89 > 0;
          k reported; Bonferroni over the verify k shown alongside
"""
from __future__ import annotations
import json, os, sys
import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import ATR_CUTS          # noqa: E402
from studio.paths import db_path                     # noqa: E402
from studio.db import UNIVERSE_PRIORITY_SQL          # noqa: E402
from scipy.stats import norm                          # noqa: E402

H, COST, MIN_N = 10, 0.003, 300


def load() -> pd.DataFrame:
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    df = con.execute(f"""SELECT ticker, CAST(date AS DATE) d, open o, high h, low l, close c, volume v FROM bars
        WHERE universe <> 'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
        ORDER BY ticker, date""").fetchdf()
    assert not df.duplicated(["ticker", "d"]).any(), "data contract: duplicate ticker-date"
    return df


def build(df: pd.DataFrame) -> pd.DataFrame:
    tk = df.ticker.to_numpy()
    same1 = np.r_[False, tk[1:] == tk[:-1]]
    o, c, h, l = (df[x].to_numpy(float) for x in "ochl")
    qt, qb = np.maximum(o, c), np.minimum(o, c)
    pqt, pqb = np.r_[np.nan, qt[:-1]], np.r_[np.nan, qb[:-1]]
    mu = (qt + qb) >= (pqt + pqb)
    eq = (qt == pqt) & (qb == pqb); ab = ~eq & (qb > pqt); be = ~eq & (qt < pqb)
    en = ~eq & ~ab & ~be & (qt >= pqt) & (qb <= pqb)
    ins = ~eq & ~ab & ~be & ~en & (qt <= pqt) & (qb >= pqb)
    ou = ~eq & ~ab & ~be & ~en & ~ins & (qt > pqt)
    code = np.select([eq, ab, ou, en & mu, ins & mu, ins, en, ~be], [0, 1, 2, 3, 4, 5, 6, 7], 8)
    col = np.where(c > o, "G", np.where(c < o, "R", ""))
    q = np.array([f"Q{a}{b}" for a, b in zip(code, col)], dtype=object)
    q[~same1] = None
    df["q"] = q
    g = df.groupby("ticker", sort=False)
    pc = g.c.shift(1)
    tr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = tr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    q1, q2 = g.q.shift(1), g.q.shift(2)
    df["seq"] = np.where(q1.notna() & q2.notna() & df.q.notna(), q2.astype(str) + ">" + q1.astype(str) + ">" + df.q.astype(str), None)
    entry = g.o.shift(-1); exitc = g.c.shift(-H)
    df["r"] = exitc / entry - 1 - COST
    df["win"] = (df.r > 0).astype(float)
    df["k"] = np.digitize(df.atr / df.c, ATR_CUTS)
    df["yr"] = pd.to_datetime(df.d).dt.year
    last_ok = sorted(df.d.unique())[-(H + 2)]
    ok = (df.c >= 5) & (df.dv20 >= 5e6) & df.r.notna() & df.atr.gt(0) & (df.d <= last_ok) & df.seq.notna()
    return df[ok].copy()


def paired_all(fr: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    """Per cell (by): paired win-lift vs the rest of the same day x ATR-bin, n-weighted per day; day-clustered SE."""
    st = fr.groupby(["d", "k"]).agg(N=("win", "size"), W=("win", "sum"), R=("r", "sum"))
    cg = fr.groupby(by + ["d", "k"]).agg(n=("win", "size"), w=("win", "sum"), rs=("r", "sum")).reset_index()
    cg = cg.join(st, on=["d", "k"])
    cg = cg[cg.N > cg.n]
    rest_w = (cg.W - cg.w) / (cg.N - cg.n); rest_r = (cg.R - cg.rs) / (cg.N - cg.n)
    cg["xw"] = (cg.w / cg.n - rest_w) * cg.n
    cg["xr"] = (cg.rs / cg.n - rest_r) * cg.n
    dd = cg.groupby(by + ["d"]).agg(xw=("xw", "sum"), xr=("xr", "sum"), n=("n", "sum"), w=("w", "sum"), rs=("rs", "sum")).reset_index()
    dd["lw"] = dd.xw / dd.n * 100; dd["lr"] = dd.xr / dd.n * 100
    out = dd.groupby(by).agg(days=("lw", "size"), n=("n", "sum"), wins=("w", "sum"), rsum=("rs", "sum"),
                             lift=("lw", "mean"), sd=("lw", "std"), rlift=("lr", "mean"))
    out["se"] = out.sd / np.sqrt(out.days)
    out["lo"] = out.lift - 1.96 * out.se
    out["win"] = out.wins / out.n * 100
    out["avg_r"] = out.rsum / out.n * 100
    return out.drop(columns=["wins", "rsum"])


def main():
    df = build(load())
    base = {w: round(df[m].win.mean() * 100, 1) for w, m in
            (("mine", df.yr <= 2023), ("verify", df.yr >= 2024))}
    mine = df[df.yr.between(2021, 2023)]; ver = df[df.yr >= 2024]
    M = paired_all(mine, ["seq"])
    k_mine = int((M.n >= MIN_N).sum())
    cand = M[(M.n >= MIN_N) & (M.lift > 0) & (M.lo > 0)].sort_values("lift", ascending=False)
    k_ver = len(cand)
    # paired vs the WHOLE verify population, not just candidates: re-run with every bar present as "rest"
    V = paired_all(ver.assign(seq=np.where(ver.seq.isin(cand.index), ver.seq, "~rest")), ["seq"]).drop("~rest", errors="ignore")
    Y = paired_all(df.assign(seq=np.where(df.seq.isin(cand.index), df.seq, "~rest")), ["seq", "yr"]).drop("~rest", level=0, errors="ignore")
    px = df[(df.c >= 21) & (df.c <= 89)]
    PZ = paired_all(px.assign(seq=np.where(px.seq.isin(cand.index), px.seq, "~rest")), ["seq"]).drop("~rest", errors="ignore")
    zb = norm.ppf(1 - 0.025 / max(k_ver, 1))
    rows = []
    for s in cand.index:
        v = V.loc[s] if s in V.index else None
        yl = Y.loc[s].lift if s in Y.index.get_level_values(0) else pd.Series(dtype=float)
        pz = PZ.loc[s] if s in PZ.index else None
        r = {"seq": s,
             "mine_n": int(M.loc[s, "n"]), "mine_win": round(M.loc[s, "win"], 1), "mine_lift": round(M.loc[s, "lift"], 2),
             "mine_lo": round(M.loc[s, "lo"], 2),
             "ver_n": int(v.n) if v is not None else 0,
             "ver_win": round(v.win, 1) if v is not None else None,
             "ver_lift": round(v.lift, 2) if v is not None else None,
             "ver_lo": round(v.lo, 2) if v is not None else None,
             "ver_lo_bonf": round(v.lift - zb * v.se, 2) if v is not None else None,
             "ver_avg_r": round(v.avg_r, 2) if v is not None else None,
             "ver_rlift": round(v.rlift, 2) if v is not None else None,
             "years_pos": int((yl > 0).sum()), "years": int(len(yl)),
             "worst_yr": round(yl.min(), 2) if len(yl) else None,
             "yr_lifts": {int(k): round(x, 1) for k, x in yl.items()},
             "px2189_lift": round(pz.lift, 2) if pz is not None else None,
             "px2189_n": int(pz.n) if pz is not None else 0}
        r["PASS"] = bool(v is not None and r["ver_lift"] > 0 and r["ver_lo"] > 0 and r["years_pos"] >= 4
                         and r["worst_yr"] >= -2 and (r["px2189_lift"] or -1) > 0)
        r["PASS_bonf"] = bool(r["PASS"] and r["ver_lo_bonf"] > 0)
        rows.append(r)
    last = df.d.max()
    out = {"baseline_win": base, "k_mine_cells": k_mine, "k_verify": k_ver, "rows": rows,
           "mine_top_raw_win": M[M.n >= MIN_N].sort_values("win", ascending=False).head(10).round(2).reset_index().to_dict("records")}
    json.dump(out, open(os.path.join(HERE, "..", "research_out", "Q3_SEQ_V1.json"), "w"), default=str, indent=1)
    print(json.dumps({k: out[k] for k in ("baseline_win", "k_mine_cells", "k_verify")}))
    for r in rows:
        print(r)


if __name__ == "__main__":
    main()
