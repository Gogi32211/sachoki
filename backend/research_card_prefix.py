"""CARD_PREFIX_V1 — inside the "good T1G / T2G card", does the 2-bar pattern before the signal add anything?

Plan approved 2026-10-05 ("კი ორივე"):
  population  T1G / T2G with golden cross (EMA50 > EMA200) ∧ RS (close/SPY > its EMA200) ∧ volume < 1.5 x 20-bar
              median ∧ 35 < RSI14 < 70 ∧ 10-bar return <= 0; liquid (close >= $5, 20d $vol >= $5M)
  Part A (k=5, pre-fixed; prefix = bars -2, -1)
     P1 both prefix bars Z                                   vs the rest
     P2 a red L34 in the prefix (Z with L34)                 vs none
     P3 effort-down in the prefix (an L containing 6)        vs none
     P4 quiet: both prefix L codes only from digits {1,2,5}  vs the rest
     P5 inside pause: bar -1 in T9/T10/Z9/Z10                vs not
  Part B (search) every 2-bar T/Z prefix with n >= 100 in MINE 2022-23 (2021 = EMA200 warm-up); cells with a
     MINE CI excluding 0 (either sign) go to VERIFY 2024-26; k reported
  estimand  book per-bar return (entry open[t+1], trail clip(12*ATR%,15,60), maxh 60, 15 bps); +10 %/20 bars/-7 % hit
            secondary; paired group vs rest INSIDE the card, same day x ATR%-bin x signal; day-clustered
  PASS      same sign + CI excludes 0 in 2022-23 and 2024-26, >= 4/5 years same sign, worst year >= -2, $21-89 same
            sign; Bonferroni (A: k=5; B: its verify k) alongside
"""
from __future__ import annotations
import json, os, sys
import duckdb
import numpy as np
import pandas as pd
from scipy.stats import norm

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import ATR_CUTS, _paired             # noqa: E402
from research_tzq_centre import book_returns_vec                 # noqa: E402
from research_q3_seq import paired_all                           # noqa: E402
import research_sbux_seq as S                                    # noqa: E402
from studio.paths import db_path                                 # noqa: E402
from studio.db import UNIVERSE_PRIORITY_SQL                      # noqa: E402

OUT = os.path.join(HERE, "..", "research_out")
W1_END = "2023-12-31"


def load() -> pd.DataFrame:
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    df = con.execute(f"""SELECT ticker, CAST(date AS DATE) d, open o, high h, low l, close c, volume v,
            COALESCE(NULLIF(t_sig,''), NULLIF(z_sig,''), '') tz, COALESCE(l_sig,'') ls, rsi_14 rsi
        FROM bars WHERE universe <> 'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
        ORDER BY ticker, date""").fetchdf()
    spy = con.execute("SELECT CAST(date AS DATE) d, any_value(close) spy FROM bars WHERE ticker = 'SPY' GROUP BY 1").fetchdf()
    assert not df.duplicated(["ticker", "d"]).any(), "data contract: duplicate ticker-date"
    return df.merge(spy, on="d", how="left").sort_values(["ticker", "d"]).reset_index(drop=True)


def verdict(res, n_years_min=4):
    w1, w2 = res["W1"], res["W2"]
    ys = [v for v in res["years"].values() if v is not None]
    if not ("ci95" in w1 and "ci95" in w2 and ys):
        return False
    s = 1 if w1["paired_delta_pp"] > 0 else -1
    return bool(w1["ci95"][0] * w1["ci95"][1] > 0 and w2["ci95"][0] * w2["ci95"][1] > 0 and (w2["paired_delta_pp"] > 0) == (s > 0)
                and sum((y > 0) == (s > 0) for y in ys) >= n_years_min and min(s * y for y in ys) >= -2
                and (res["px"].get("paired_delta_pp", 0) > 0) == (s > 0))


def main():
    df = load(); g = df.groupby("ticker", sort=False)
    ew = lambda col, span: g[col].transform(lambda s: s.ewm(span=span, adjust=False, min_periods=span).mean())
    df["e50"], df["e200"] = ew("c", 50), ew("c", 200)
    df["ratio"] = df.c / df.spy
    df["rse"] = g["ratio"].transform(lambda s: s.ewm(span=200, adjust=False, min_periods=200).mean())
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["vmed"] = g.v.transform(lambda s: s.rolling(20, min_periods=20).median())
    df["ret10"] = df.c / g.c.shift(10) - 1
    for k in (1, 2):
        df[f"tz{k}"] = g.tz.shift(k).fillna(""); df[f"ls{k}"] = g.ls.shift(k).fillna("")
    card = (df.tz.isin(["T1G", "T2G"]) & (df.e50 > df.e200) & (df.ratio > df.rse) & (df.v / df.vmed < 1.5)
            & (df.rsi > 35) & (df.rsi < 70) & (df.ret10 <= 0) & (df.c >= 5) & (df.dv20 >= 5e6) & (df.atr > 0)
            & (df.tz2 != ""))
    tk = df.ticker.to_numpy(); o, h, l, c = (df[x].to_numpy(float) for x in "ohlc"); A = df.atr.to_numpy(float)
    el = np.flatnonzero(card.to_numpy())
    e = df.iloc[el].copy()
    e["r"] = book_returns_vec(o, h, l, c, A, tk, el)
    e["hit"] = S.first_passage(o, h, l, tk, el)
    e = e[e.r.notna()]
    e["k"] = pd.Series(np.digitize(e.atr / e.c, ATR_CUTS), index=e.index).astype(str) + "|" + e.tz
    e["d"] = e.d.astype(str); e["yr"] = pd.to_datetime(e.d).dt.year
    print("card bars:", len(e), "| by year:", e.yr.value_counts().sort_index().to_dict())
    z1, z2 = e.tz1.str.startswith("Z"), e.tz2.str.startswith("Z")
    quiet = lambda s: s.str.fullmatch(r"L[125]+")
    A_ = {"P1 two red (Z,Z)":          (z1 & z2, ~(z1 & z2)),
          "P2 red L34 in prefix":      ((z1 & (e.ls1 == "L34")) | (z2 & (e.ls2 == "L34")), ~((z1 & (e.ls1 == "L34")) | (z2 & (e.ls2 == "L34")))),
          "P3 effort-down (L has 6)":  (e.ls1.str.contains("6") | e.ls2.str.contains("6"), ~(e.ls1.str.contains("6") | e.ls2.str.contains("6"))),
          "P4 quiet (L from 1/2/5)":   (quiet(e.ls1) & quiet(e.ls2), ~(quiet(e.ls1) & quiet(e.ls2))),
          "P5 inside pause at -1":     (e.tz1.isin(["T9", "T10", "Z9", "Z10"]), ~e.tz1.isin(["T9", "T10", "Z9", "Z10"]))}
    rng = np.random.default_rng(20261005)
    out = {"card_bars": int(len(e)), "A": {}, "B": {}}

    def run(f):
        res = {"n_g": int(f.g.sum()), "n_rest": int((~f.g).sum()),
               "raw_ret_g": round(f[f.g].r.mean() * 100, 2), "raw_ret_rest": round(f[~f.g].r.mean() * 100, 2)}
        for wn, m in (("W1", f.d <= W1_END), ("W2", f.d > W1_END), ("px", f.c.between(21, 89))):
            res[wn] = _paired(f[m][["d", "k", "r", "g"]], rng)
            res[wn + "_hit"] = _paired(f[m & f.hit.notna()][["d", "k", "hit", "g"]].rename(columns={"hit": "r"}), rng)
        res["years"] = {int(y): _paired(f[f.yr == y][["d", "k", "r", "g"]], rng).get("paired_delta_pp") for y in sorted(f.yr.unique())}
        res["PASS"] = verdict(res)
        return res

    for name, (gm, rm) in A_.items():
        gm, rm = gm.fillna(False).to_numpy(bool), rm.fillna(False).to_numpy(bool)
        out["A"][name] = run(e[gm | rm].assign(g=gm[gm | rm]))
    # ── Part B ─────────────────────────────────────────────────────────────
    e["seq"] = e.tz2 + ">" + e.tz1
    mine = e[e.d <= W1_END].assign(win=lambda f: f.r)
    M = paired_all(mine, ["seq"])
    cells = M[M.n >= 100]
    cand = cells[(cells.lo > 0) | (cells.lift + 1.96 * cells.se < 0)]
    out["B"]["k_mine_cells"] = int(len(cells)); out["B"]["k_verify"] = int(len(cand))
    rows = []
    for s in cand.index:
        r = run(e.assign(g=e.seq == s))
        r.update({"seq": s, "mine_n": int(M.loc[s, "n"]), "mine_lift_pp": round(M.loc[s, "lift"], 2)})
        z = norm.ppf(1 - 0.025 / max(len(cand), 1))
        if "ci95" in r["W2"]:
            se = (r["W2"]["ci95"][1] - r["W2"]["ci95"][0]) / (2 * 1.96)
            r["W2_bonf"] = [round(r["W2"]["paired_delta_pp"] - z * se, 2), round(r["W2"]["paired_delta_pp"] + z * se, 2)]
        rows.append(r)
    out["B"]["rows"] = rows
    # live: today's card bars with their A flags
    last = df.d.max()
    lv = df[card & (df.d == last)].copy()
    lz1, lz2 = lv.tz1.str.startswith("Z"), lv.tz2.str.startswith("Z")
    lv["P1"] = lz1 & lz2; lv["P2"] = (lz1 & (lv.ls1 == "L34")) | (lz2 & (lv.ls2 == "L34"))
    lv["P3"] = lv.ls1.str.contains("6") | lv.ls2.str.contains("6")
    lv["P4"] = lv.ls1.str.fullmatch(r"L[125]+") & lv.ls2.str.fullmatch(r"L[125]+")
    lv["P5"] = lv.tz1.isin(["T9", "T10", "Z9", "Z10"])
    lv["prefix"] = lv.tz2 + "·" + lv.ls2 + " > " + lv.tz1 + "·" + lv.ls1
    lv[["ticker", "tz", "c", "prefix", "P1", "P2", "P3", "P4", "P5"]].to_csv(os.path.join(OUT, "CARD_PREFIX_V1_live.csv"), index=False)
    json.dump(out, open(os.path.join(OUT, "CARD_PREFIX_V1.json"), "w"), indent=1, default=str)
    f_ = lambda x: f"{x['paired_delta_pp']:+.2f} [{x['ci95'][0]:+.2f},{x['ci95'][1]:+.2f}]" if "ci95" in x else "–"
    print("PART A")
    for name, r in out["A"].items():
        print(f"| {name} | {r['n_g']}/{r['n_rest']} | raw {r['raw_ret_g']}/{r['raw_ret_rest']} | {f_(r['W1'])} | {f_(r['W2'])} | px {f_(r['px'])} | "
              f"hit {f_(r['W1_hit'])} {f_(r['W2_hit'])} | {' '.join(f'{v:+.1f}' for v in r['years'].values() if v is not None)} | {'PASS' if r['PASS'] else ''}")
    print(f"PART B: k_mine {out['B']['k_mine_cells']} -> verify {out['B']['k_verify']}")
    for r in rows:
        print(f"| {r['seq']} | mine n {r['mine_n']} lift {r['mine_lift_pp']:+.2f} | W1 {f_(r['W1'])} | W2 {f_(r['W2'])} bonf {r.get('W2_bonf')} | px {f_(r['px'])} | "
              f"{' '.join(f'{v:+.1f}' for v in r['years'].values() if v is not None)} | {'PASS' if r['PASS'] else ''}")
    print("live", str(last)[:10], len(lv))


if __name__ == "__main__":
    main()
