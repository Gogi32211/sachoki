"""SBUX_SEQ_V1 — which 3/4-bar TZL sequences preceded SBUX rises, and do they hold up?

Plan approved 2026-10-04 ("ki"):
  data     1D studio DB, SBUX; tokens: bars before 0 = T/Z state, bar 0 = T/Z + L
  rise     +10 % (high) within 20 bars BEFORE a -7 % stop; entry next open; open gap checked first, stop before
           target (conservative); censored if the horizon runs past the data
  describe every non-overlapping SBUX rise episode start -> the 3- and 4-bar sequence at that bar
  inside   sequences seen >= 3 times on SBUX: hit-rate after them vs SBUX's own base rate; k reported
  validate (a) SBUX 2021-23 vs 2024-26; (b) the same sequence across the liquid universe, hit-rate lift vs the
           same day x ATR%-bin (frozen ATR_CUTS), day-clustered — a SBUX-only pattern is that stock's history, not a rule
"""
from __future__ import annotations
import json, os, sys
import duckdb
import numpy as np
import pandas as pd
from scipy.stats import binomtest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import ATR_CUTS          # noqa: E402
from studio.paths import db_path                     # noqa: E402
from studio.db import UNIVERSE_PRIORITY_SQL          # noqa: E402
from research_q3_seq import paired_all               # noqa: E402

TICKER, TARGET, STOP, H, MIN_OCC = "SBUX", 0.10, 0.07, 20, 3


def load() -> pd.DataFrame:
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    df = con.execute(f"""SELECT ticker, CAST(date AS DATE) d, open o, high h, low l, close c, volume v,
            COALESCE(NULLIF(t_sig,''), NULLIF(z_sig,''), '') tz, COALESCE(l_sig, '') ls
        FROM bars WHERE universe <> 'index'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
        ORDER BY ticker, date""").fetchdf()
    assert not df.duplicated(["ticker", "d"]).any(), "data contract: duplicate ticker-date"
    return df.reset_index(drop=True)


def first_passage(o, h, l, tk, idx):
    n = len(o); idx = np.asarray(idx)
    out = np.full(len(idx), np.nan)
    end = idx + H
    ok = end < n
    ok[ok] &= tk[end[ok]] == tk[idx[ok]]
    t = idx[ok]
    entry = o[t + 1]; tp = entry * (1 + TARGET); sp = entry * (1 - STOP)
    res = np.full(len(t), np.nan); live = entry > 0
    res[~live] = np.nan
    for s in range(1, H + 1):
        j = t + s
        if s > 1:
            a = live & (o[j] <= sp); res[a] = 0; live &= ~a
            b = live & (o[j] >= tp); res[b] = 1; live &= ~b
        a = live & (l[j] <= sp); res[a] = 0; live &= ~a
        b = live & (h[j] >= tp); res[b] = 1; live &= ~b
    res[live] = 0
    out[ok] = res
    return out


def main():
    df = load()
    tk = df.ticker.to_numpy(); o, h, l, c = (df[x].to_numpy(float) for x in "ohlc")
    g = df.groupby("ticker", sort=False)
    t1, t2, t3 = (g.tz.shift(k) for k in (1, 2, 3))
    tzl = df.tz + "·" + df.ls
    df["seq3"] = np.where(t2.notna() & t1.notna(), t2.fillna("") + ">" + t1.fillna("") + ">" + tzl, None)
    df["seq4"] = np.where(t3.notna() & t2.notna() & t1.notna(),
                          t3.fillna("") + ">" + t2.fillna("") + ">" + t1.fillna("") + ">" + tzl, None)
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["yr"] = pd.to_datetime(df.d).dt.year

    # ── SBUX ───────────────────────────────────────────────────────────────
    si = np.flatnonzero((df.ticker == TICKER).to_numpy())
    sb = df.iloc[si].copy()
    sb["hit"] = first_passage(o, h, l, tk, si)
    sb = sb[sb.seq3.notna()]
    res = sb[sb.hit.notna()]
    base = float(res.hit.mean())
    # episode starts: a hit bar whose previous resolved bar was not a hit
    prev = res.hit.shift(1).fillna(0)
    starts = res[(res.hit == 1) & (prev == 0)]
    out = {"ticker": TICKER, "bars": int(len(sb)), "resolved": int(len(res)), "base_hit": round(base * 100, 1),
           "episodes": int(len(starts)),
           "episode_list": [{"date": str(r.d), "close": round(r.c, 2), "seq3": r.seq3, "seq4": r.seq4} for r in starts.itertuples()]}
    tested = []
    for sq in ("seq3", "seq4"):
        cnt = res.groupby(sq).hit.agg(["size", "sum"])
        cnt = cnt[cnt["size"] >= MIN_OCC]
        for s, row in cnt.iterrows():
            n_, k_ = int(row["size"]), int(row["sum"])
            p = binomtest(k_, n_, base, alternative="greater").pvalue
            tested.append({"len": sq, "seq": s, "n": n_, "hits": k_, "hit": round(k_ / n_ * 100, 1), "p_vs_base": round(p, 4)})
    tested.sort(key=lambda r: (r["p_vs_base"], -r["hit"]))
    out["k_tested"] = len(tested)
    out["bonferroni_alpha"] = round(0.05 / max(len(tested), 1), 5)
    top = [r for r in tested if r["hits"] >= 2 and r["hit"] >= base * 100 + 20][:15]

    # ── validation ─────────────────────────────────────────────────────────
    A = df.atr.to_numpy(float)
    el = np.flatnonzero(((df.c >= 5) & (df.dv20 >= 5e6) & df.atr.gt(0) & df.seq3.notna()).to_numpy())
    uni = df.iloc[el].copy()
    uni["hit"] = first_passage(o, h, l, tk, el)
    uni = uni[uni.hit.notna()]
    uni["k"] = np.digitize(uni.atr / uni.c, ATR_CUTS); uni["d"] = uni.d.astype(str)
    uni["win"] = uni.hit; uni["r"] = uni.hit
    for r in top:
        sq = r["len"]
        s_rows = res[res[sq] == r["seq"]]
        r["sbux_2021_23"] = f"{int(s_rows[s_rows.yr <= 2023].hit.sum())}/{int((s_rows.yr <= 2023).sum())}"
        r["sbux_2024_26"] = f"{int(s_rows[s_rows.yr >= 2024].hit.sum())}/{int((s_rows.yr >= 2024).sum())}"
        r["dates"] = [str(x) for x in s_rows.d]
        f = uni.assign(seq=np.where(uni[sq] == r["seq"], "P", "~rest"))
        for nm, m in (("uni_2021_23", f.d <= "2023-12-31"), ("uni_2024_26", f.d > "2023-12-31"), ("uni_all", f.d > "")):
            P = paired_all(f[m], ["seq"])
            r[nm] = (f"n {int(P.loc['P','n'])} hit {P.loc['P','win']:.1f}% lift {P.loc['P','lift']:+.1f} "
                     f"[{P.loc['P','lo']:+.1f}, {P.loc['P','lift'] + 1.96 * P.loc['P','se']:+.1f}]") if "P" in P.index else "n 0"
    out["top"] = top
    out["uni_base_hit"] = round(uni.hit.mean() * 100, 1)
    last = sb.tail(4)[["d", "tz", "ls", "seq3", "seq4"]].astype(str).to_dict("records")
    out["sbux_last_bars"] = last
    json.dump(out, open(os.path.join(HERE, "..", "research_out", "SBUX_SEQ_V1.json"), "w"), indent=1, default=str, ensure_ascii=False)
    print(json.dumps({k_: out[k_] for k_ in ("bars", "resolved", "base_hit", "episodes", "k_tested", "bonferroni_alpha", "uni_base_hit")}))
    print("EPISODES:")
    for e in out["episode_list"]:
        print("  ", e["date"], e["close"], "|", e["seq4"])
    print("TOP:")
    for r in top:
        print("  ", json.dumps({k_: v_ for k_, v_ in r.items() if k_ != "dates"}, ensure_ascii=False))
    print("LAST:", last)


if __name__ == "__main__":
    main()
