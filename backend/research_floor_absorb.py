"""FLOOR_ABSORB_V1 — the SBUX (2026-06) / AMD (2026-03/04) turn as a TZL phase grammar.

Plan approved 2026-10-04 ("ki"); k = 3, everything fixed before outcomes were computed:
  V-A core   Z4 -> T9·L34 -> (0-1 any bar) -> bar 0 = T1G or T2G (any L)
  V-B floor  V-A + the Z4/T9 low is within 0.5 x ATR of some low 3-15 bars before the Z4, and no close below
             that floor from the Z4 to bar 0 (tested twice and held)
  V-C grammar effort-down bar (any Z whose L has 5 or 6) 1-3 bars before an absorption bar (green non-breakout T:
             T3/T4/T5/T6/T9/T10/T11/T12 with L34, low within 0.5 x ATR of the 10-bar low), then within <= 2 bars
             bar 0 = T1/T1G/T2/T2G; no close below the 10-bar floor from the absorption bar to bar 0
  sanity     SBUX 2026-06-09 and AMD 2026-04-01 must fire in V-A and V-B (logic check, not evidence)
  AMENDMENT_1 (pre-outcome, forced by the sanity check): V-B floor test also passes when the T9 low is within
             0.5 x ATR of the Z4 low (SBUX's double test was Z4/T9 themselves)
Estimand: book per-bar return (entry open[t+1], trail clip(12*ATR%,15,60), maxh 60, 15 bps) and +10 % within 20 bars
before -7 %; paired vs the same day x ATR%-bin x bar-0 T/Z (other strength bars), plus vs all bars; liquid universe;
2021-23 / 2024-26, years, $21-89; PASS = old rule; Bonferroni k=3 alongside.

  python research_floor_absorb.py --sanity   -> detection only (no outcomes)
  python research_floor_absorb.py            -> full run
"""
from __future__ import annotations
import json, os, sys
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import ATR_CUTS, _paired      # noqa: E402
from research_tzq_centre import load, book_returns_vec    # noqa: E402
import research_sbux_seq as S                             # noqa: E402

NB_T = {"T3", "T4", "T5", "T6", "T9", "T10", "T11", "T12"}
SANITY = [("SBUX", "2026-06-09"), ("AMD", "2026-04-01")]


def detect(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("ticker", sort=False)
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["ls"] = df["ls"].fillna("")
    df["minlow10"] = g.l.transform(lambda s: s.rolling(10, min_periods=10).min())
    sh = lambda col, k: g[col].shift(k)
    tz0 = df.tz
    # ── V-A / V-B ────────────────────────────────────────────────────────
    va = np.zeros(len(df), bool); vb = np.zeros(len(df), bool)
    for t9o in (1, 2):                                   # T9 at bar -1 (no gap bar) or -2 (one bar between)
        zo = t9o + 1
        core = ((sh("tz", t9o) == "T9") & (sh("ls", t9o) == "L34") & (sh("tz", zo) == "Z4")
                & tz0.isin(["T1G", "T2G"])).to_numpy()
        va |= core
        fl = np.minimum(sh("l", zo), sh("l", t9o)).to_numpy()
        atr_z = sh("atr", zo).to_numpy()
        match_low = np.full(len(df), np.inf)
        for k in range(3, 16):
            lk = sh("l", zo + k).to_numpy()
            hit = np.abs(lk - fl) <= 0.5 * atr_z
            match_low = np.where(hit, np.minimum(match_low, lk), match_low)
        # AMENDMENT_1 (2026-10-04, before any outcome was computed; forced by the registered sanity check —
        # SBUX's double test was the Z4 low re-tested by the T9 itself, 93.81 / 93.64): the floor test also
        # passes when the T9 low is within 0.5 x ATR of the Z4 low.
        retest = (np.abs(sh("l", t9o) - sh("l", zo)) <= 0.5 * sh("atr", zo)).to_numpy()
        floor = np.where(np.isfinite(match_low), np.minimum(fl, match_low), fl)
        minc = np.min(np.vstack([sh("c", k).to_numpy() for k in range(0, zo + 1)]), axis=0)
        vb |= core & (np.isfinite(match_low) | retest) & (minc >= floor)
    # ── V-C ──────────────────────────────────────────────────────────────
    vc = np.zeros(len(df), bool)
    strong = tz0.isin(["T1", "T1G", "T2", "T2G"]).to_numpy()
    for ao in (1, 2, 3):                                 # absorption bar 1-3 bars before bar 0 (<= 2 between)
        a_ok = (sh("tz", ao).isin(NB_T) & (sh("ls", ao) == "L34")
                & (sh("l", ao) <= sh("minlow10", ao) + 0.5 * sh("atr", ao))).to_numpy()
        e_ok = np.zeros(len(df), bool)
        for eo in range(ao + 1, ao + 4):
            tze, lse = sh("tz", eo).fillna(""), sh("ls", eo).fillna("")
            e_ok |= (tze.str.startswith("Z") & (lse.str.contains("5") | lse.str.contains("6"))).to_numpy()
        minc = np.min(np.vstack([sh("c", k).to_numpy() for k in range(0, ao + 1)]), axis=0)
        held = minc >= sh("minlow10", ao).to_numpy()
        vc |= strong & a_ok & e_ok & held
    df["VA"], df["VB"], df["VC"] = va, vb, vc
    return df


def main(sanity_only: bool):
    df = load()
    df = df.rename(columns={})  # load() gives ticker,d,o,h,l,c,v,tz
    if "ls" not in df:
        import duckdb
        from studio.paths import db_path
        from studio.db import UNIVERSE_PRIORITY_SQL
        con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
        ls = con.execute(f"""SELECT ticker, CAST(date AS DATE) d, COALESCE(l_sig,'') ls FROM bars WHERE universe <> 'index'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1""").fetchdf()
        df = df.merge(ls, on=["ticker", "d"], how="left")
        df = df.sort_values(["ticker", "d"]).reset_index(drop=True)
    df = detect(df)
    for t, d in SANITY:
        r = df[(df.ticker == t) & (df.d.astype(str) == d)]
        print("SANITY", t, d, r[["tz", "ls", "VA", "VB", "VC"]].to_dict("records"))
    print("raw counts (all bars):", {v: int(df[v].sum()) for v in ("VA", "VB", "VC")})
    if sanity_only:
        return
    tk = df.ticker.to_numpy(); o, h, l, c = (df[x].to_numpy(float) for x in "ohlc")
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    A = df.atr.to_numpy(float)
    el = np.flatnonzero(((df.c >= 5) & (df.dv20 >= 5e6) & (A > 0) & np.isfinite(A) & (df.tz != "")).to_numpy())
    e = df.iloc[el].copy()
    e["r"] = book_returns_vec(o, h, l, c, A, tk, el)
    e["hit"] = S.first_passage(o, h, l, tk, el)
    e["atrk"] = np.digitize(e.atr / e.c, ATR_CUTS); e["d"] = e.d.astype(str); e["yr"] = pd.to_datetime(e.d).dt.year
    rng = np.random.default_rng(20261004)
    out = {}
    for v, strong in (("VA", ["T1G", "T2G"]), ("VB", ["T1G", "T2G"]), ("VC", ["T1", "T1G", "T2", "T2G"])):
        P = e[e[v]]
        res = {"n": int(len(P)), "days": int(P.d.nunique()),
               "raw_ret": round(P.r.mean() * 100, 2), "raw_hit": round(P.hit.mean() * 100, 1),
               "price": {"<21": int((P.c < 21).sum()), "21-89": int(P.c.between(21, 89).sum()), ">89": int((P.c > 89).sum())}}
        for cmp_name, f in (("vs_same_TZ", e[e.tz.isin(strong)]), ("vs_all", e)):
            # vs_same_TZ: strata carry the bar-0 T/Z; vs_all: day x ATR%-bin only
            f = f.assign(g=f[v], k=f.atrk.astype(str) + ("|" + f.tz if cmp_name == "vs_same_TZ" else ""))
            blk = {}
            for col in ("r", "hit"):
                for wn, m in (("W1", f.d <= "2023-12-31"), ("W2", f.d > "2023-12-31"), ("px2189", f.c.between(21, 89))):
                    blk[f"{col}_{wn}"] = _paired(f[m & f[col].notna()][["d", "k", col, "g"]].rename(columns={col: "r"}), rng)
            blk["years_r"] = {int(y): _paired(f[(f.yr == y) & f.r.notna()][["d", "k", "r", "g"]], rng).get("paired_delta_pp")
                              for y in sorted(f.yr.unique())}
            res[cmp_name] = blk
        out[v] = res
        live = df[(df.d == df.d.max()) & df[v] & (df.c >= 5)]
        res["live"] = live[["ticker", "c", "tz", "ls"]].values.tolist()
    json.dump(out, open(os.path.join(HERE, "..", "research_out", "FLOOR_ABSORB_V1.json"), "w"), indent=1, default=str)
    def f_(x): return f"{x['paired_delta_pp']:+.2f} [{x['ci95'][0]:+.2f},{x['ci95'][1]:+.2f}]" if "ci95" in x else str(x.get("note", x))
    for v, res in out.items():
        print(f"=== {v}: n {res['n']} days {res['days']} raw ret {res['raw_ret']}% raw hit {res['raw_hit']}% price {res['price']} live {res['live']}")
        for cmp_name in ("vs_same_TZ", "vs_all"):
            b = res[cmp_name]
            ys = [x for x in b["years_r"].values() if x is not None]
            s = 1 if b["r_W1"].get("paired_delta_pp", 0) > 0 else -1
            ok = ("ci95" in b["r_W1"] and "ci95" in b["r_W2"] and b["r_W1"]["ci95"][0] > 0 and b["r_W2"]["ci95"][0] > 0
                  and sum(y > 0 for y in ys) >= 4 and min(ys) >= -2 and b["r_px2189"].get("paired_delta_pp", -1) > 0)
            print(f"  {cmp_name}: BOOK W1 {f_(b['r_W1'])} W2 {f_(b['r_W2'])} px {f_(b['r_px2189'])} | HIT W1 {f_(b['hit_W1'])} W2 {f_(b['hit_W2'])}"
                  f" | yrs {' '.join(f'{y:+.1f}' for y in ys)} | PASS {ok}")


if __name__ == "__main__":
    main("--sanity" in sys.argv)
