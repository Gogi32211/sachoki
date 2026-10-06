"""QVOL_GAP_V1 — do the Q-tab's own volume marks and the TRUE-gap add anything ABOVE T/Z (and V level)?

Plan approved 2026-10-04 ("ki"); k = 7 pre-fixed contrasts, registered horizon only (maxh 60):
  V1  ▲ (median level >= sigma level + 2)   vs ● (levels agree)      strata + V level
  V2  ▼ (sigma level >= median level + 2)   vs ●                      strata + V level
  V3  ↑V = V>=4 on Q1-Q4                     vs Q1-Q4 with V<=3
  V4  ↓V = V>=4 on Q5-Q8                     vs Q5-Q8 with V<=3
  V5  ↑v (same level as prior bar, volume up) vs ↓v                   strata + V level
  G1  gap UP whose TRUE-gap class is smaller (e.g. G2→G1) vs same gap class with TRUE = class   strata + gap class
  G2  gap DOWN, same
Estimand: book per-bar return (entry open[t+1], trail clip(12*ATR%,15,60), maxh 60, 15 bps); 10-bar win secondary.
Paired g vs rest within day x ATR%-bin (frozen) x T/Z state [x V level | gap class], n-weighted per day, day bootstrap.
Universe close >= $5, 20d $volume >= $5M (index universe excluded). Windows 2021-23 / 2024-26, per year, $21-89.
PASS: same sign + CI excludes 0 in both windows, >= 4/6 years same sign, worst year (that sign) >= -2, $21-89 same sign.
Q / V / gap tokens come from studio.q_sequence._base_sql — the SQL the Q tab runs (100 % parity with the Pine).
"""
from __future__ import annotations
import json, os, sys
import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from bottom_cluster_forward import ATR_CUTS, _paired, _bar_returns   # noqa: E402
from research_tzq_centre import book_returns_vec                    # noqa: E402
from studio.paths import db_path                                     # noqa: E402
from studio.q_sequence import _base_sql                              # noqa: E402

S = 0.0015


def load() -> pd.DataFrame:
    con = duckdb.connect(db_path("studio_analytics.duckdb"), read_only=True)
    df = con.execute(f"""WITH {_base_sql(None).strip()},
        x AS (SELECT *, LAG(high) OVER (PARTITION BY ticker ORDER BY date) phigh,
                        LAG(low)  OVER (PARTITION BY ticker ORDER BY date) plow FROM base)
        SELECT ticker, CAST(date AS DATE) d, universe, open o, high h, low l, close c, volume v,
               COALESCE(NULLIF(t_sig,''), NULLIF(z_sig,''), '') tz, qcode, vlev, slev, pvlev, pvol,
               bar_gap_range, COALESCE(phys_gap_true, '') gtrue, phigh, plow
        FROM x ORDER BY ticker, date""").fetchdf()
    assert not df.duplicated(["ticker", "d"]).any(), "data contract: duplicate ticker-date"
    return df.reset_index(drop=True)


def main():
    df = load()
    tk = df.ticker.to_numpy(); o, h, l, c = (df[x].to_numpy(float) for x in "ohlc")
    g = df.groupby("ticker", sort=False)
    pc = g.c.shift(1)
    trr = np.maximum(df.h - df.l, np.maximum((df.h - pc).abs(), (df.l - pc).abs()))
    df["atr"] = trr.groupby(df.ticker).transform(lambda s: s.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    df["dv20"] = (df.c * df.v).groupby(df.ticker).transform(lambda s: s.rolling(20, min_periods=20).mean())
    r10 = g.c.shift(-10) / g.o.shift(-1) - 1 - 2 * S
    df["win"] = np.where(r10.notna(), (r10 > 0).astype(float), np.nan)
    A = df.atr.to_numpy(float)
    sel = np.flatnonzero((df.universe != "index").to_numpy() & (df.c >= 5).to_numpy() & (df.dv20 >= 5e6).to_numpy()
                         & np.isfinite(A) & (A > 0) & df.qcode.notna().to_numpy() & (df.tz != "").to_numpy())
    rb = book_returns_vec(o, h, l, c, A, tk, sel)
    rng = np.random.default_rng(20261004)
    smp = rng.choice(len(sel), 3000, replace=False)
    ref = _bar_returns(o, h, l, c, A, tk, sel[smp])
    assert np.allclose(np.nan_to_num(ref, nan=-9), np.nan_to_num(rb[smp], nan=-9), atol=1e-12)
    ev = df.iloc[sel].copy(); ev["r"] = rb
    for col in ("vlev", "slev", "pvlev", "qcode", "pvol", "v", "phigh", "plow"):
        ev[col] = pd.to_numeric(ev[col], errors="coerce").astype(float)     # nullable NA -> NaN (compares False)
    ev["atrk"] = np.digitize(ev.atr / ev.c, ATR_CUTS)
    ev["yr"] = pd.to_datetime(ev.d).dt.year
    ev["d"] = ev.d.astype(str)
    dl = ev.vlev - ev.slev
    gcls = np.where(ev.bar_gap_range.fillna("").str.startswith("G"), ev.bar_gap_range.fillna("").str.split("-").str[0], "")
    ev["gcls"] = gcls
    up_gap = (ev.o > ev.phigh).to_numpy(); dn_gap = (ev.o < ev.plow).to_numpy()
    has_g = (ev.gcls != "").to_numpy() & (ev.gtrue != "").to_numpy()
    corrected = has_g & (ev.gtrue.to_numpy() != ev.gcls.to_numpy())
    same_lvl = (ev.pvlev == ev.vlev).to_numpy()
    q = ev.qcode.to_numpy()
    lv = ev.vlev.to_numpy()
    T = {  # name: (group mask, rest mask, extra stratum column)
        "V1 ▲ vs ●":        ((dl >= 2).to_numpy(), (dl == 0).to_numpy(), "vlev"),
        "V2 ▼ vs ●":        ((dl <= -2).to_numpy(), (dl == 0).to_numpy(), "vlev"),
        "V3 ↑V vs Q1-4 V≤3": ((q >= 1) & (q <= 4) & (lv >= 4), (q >= 1) & (q <= 4) & (lv <= 3), None),
        "V4 ↓V vs Q5-8 V≤3": ((q >= 5) & (q <= 8) & (lv >= 4), (q >= 5) & (q <= 8) & (lv <= 3), None),
        "V5 ↑v vs ↓v":      (same_lvl & (ev.v > ev.pvol).to_numpy(), same_lvl & (ev.v < ev.pvol).to_numpy(), "vlev"),
        "G1 gap↑ TRUE<cls": (up_gap & corrected, up_gap & has_g & ~corrected, "gcls"),
        "G2 gap↓ TRUE<cls": (dn_gap & corrected, dn_gap & has_g & ~corrected, "gcls"),
    }
    out = {}
    for name, (gm, rm, extra) in T.items():
        e = ev[gm | rm].copy(); e["g"] = gm[gm | rm]
        key = e.atrk.astype(str) + "|" + e.tz + ("|" + e[extra].astype(str) if extra else "")
        e["k"] = key
        res = {"n_g": int(e.g.sum()), "n_rest": int((~e.g).sum()),
               "raw_ret_g": round(e[e.g].r.mean() * 100, 2), "raw_ret_rest": round(e[~e.g].r.mean() * 100, 2)}
        for wn, m in (("W1", e.yr <= 2023), ("W2", e.yr >= 2024), ("px2189", e.c.between(21, 89))):
            res[wn] = _paired(e[m & e.r.notna()][["d", "k", "r", "g"]], rng)
            res[wn + "_win"] = _paired(e[m & e.win.notna()][["d", "k", "win", "g"]].rename(columns={"win": "r"}), rng)
        res["years"] = {int(y): _paired(e[(e.yr == y) & e.r.notna()][["d", "k", "r", "g"]], rng).get("paired_delta_pp")
                        for y in sorted(e.yr.unique())}
        out[name] = res
        print(name, json.dumps(res, default=str, ensure_ascii=False))
    json.dump(out, open(os.path.join(HERE, "..", "research_out", "QVOL_GAP_V1.json"), "w"), indent=1, default=str, ensure_ascii=False)


if __name__ == "__main__":
    main()
