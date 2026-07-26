"""
validate_prebreakout_full.py — COMPREHENSIVE 5-day pre-breakout anatomy.

Event: +15% daily spike (dv>=2M, pc>=3). Window: 5 days ENDING at D; outcome:
spike at D+1 (ev_next) and within 5d (ev5). Controls = all other liquid days.
~40 features across 4 families:
  1. volume structure (1h day-aggs): rvol5, comp5, lbrel5, upvol5, vol-slope
  2. price structure: drift5, hl5, near 20d-high/low, RSI level & slope
  3. descriptor events in window: ND/NU/EU counts, vol=B, L5/L46/L34, R2X/R2L/
     VX/PS, T/Z/P counts, spring, gap-ups, SC-zone, sub200-B state
  4. Edge-setup fires in window on 1D and 4H (all edge_replay masks)
For each feature: presence%, lift of ev_next / ev5, then per-year lift for the
top movers. Multiple-testing guard: ~40 tests → only |lift|≥1.3 with year-
consistency counts as a finding.
"""
from __future__ import annotations
import gc, sys, os, time
import numpy as np, pandas as pd, duckdb

sys.path.insert(0, os.path.dirname(__file__))
from studio.paths import ANALYTICS_DB, db_path
import edge_replay as ER

DV = 2_000_000; PMIN = 3.0; SPIKE = 0.15
_PROJ = """universe, ticker, CAST(date AS VARCHAR) date, open, high, low, close, rsi_14, atr_14,
   coalesce(t_sig,'') t, coalesce(z_sig,'') z, coalesce(l_sig,'') l,
   coalesce(vol_bucket,'') vb, coalesce(bar_gap_class,'') gap,
   coalesce(close_suffix,'') csfx, coalesce(bar_line5,'') l5,
   coalesce(w2_spring,0) spring,
   coalesce(sig_t11,0) t11, coalesce(sig_t12,0) t12, coalesce(sig_eb_up,0) ebu,
   coalesce(sig_any_d,0) anyd, coalesce(sig_l1,0) l1,
   coalesce(sig_p55,0) p55, coalesce(sig_para_start,0) para,
   CASE WHEN sig_l6=1 AND sig_l4=1 AND close>=open THEN 1 ELSE 0 END l43,
   coalesce(wt_valid_tr,0) vtr, coalesce(wt_support,0) wt_sup,
   coalesce(wt_resistance,0) wt_res,
   CASE WHEN sig_bias_dn=1 OR sig_vol_5x=1 OR sig_vol_10x=1 OR sig_vol_20x=1
        THEN 1 ELSE 0 END supp"""


def daily_frame():
    """Full daily pull with descriptors + edge masks (via ER._prep) + events."""
    con = duckdb.connect(ANALYTICS_DB, read_only=True)
    df = con.execute(f"""WITH r AS (SELECT {_PROJ}, volume, full_suffix fsfx,
            coalesce(sig_any_p,0) anyp,
            row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
          FROM bars WHERE avg_vol_20d>0)
        SELECT * EXCLUDE (rn) FROM r WHERE rn=1 ORDER BY ticker, date""").fetchdf()
    con.close()
    df = ER._prep(df)                        # adds E_* setup masks (identical to board)
    df["day"] = df["date"].str[:10]
    return df


def h1_features():
    con = duckdb.connect(db_path("studio_1h.duckdb"), read_only=True)
    f = con.execute("""
    WITH b AS (SELECT ticker, CAST(date - INTERVAL 5 HOUR AS DATE) d0, date, open, high, low, close, volume FROM bars),
    a AS (SELECT ticker, CAST(d0 AS VARCHAR) dstr, count(*) nb, sum(volume) tv,
                 sum(CASE WHEN close>open THEN volume ELSE 0 END) uv,
                 arg_max(volume,date) lb, max(high) dh, min(low) dl, arg_max(close,date) lc
          FROM b GROUP BY ticker, d0)
    SELECT * FROM a WHERE nb>=5 ORDER BY ticker, dstr""").fetchdf()
    con.close()
    f = f.rename(columns={"dstr": "day"})
    f["upvol"] = f.uv / f.tv.replace(0, np.nan)
    f["lbsh"] = f.lb / f.tv.replace(0, np.nan)
    f["rng"] = (f.dh - f.dl) / f.lc.replace(0, np.nan)
    g = f.groupby("ticker", sort=False)
    f["v5"] = g["tv"].transform(lambda s: s.rolling(5).mean())
    f["v20"] = g["tv"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(5)
    f["rvol5"] = f.v5 / f.v20.replace(0, np.nan)
    f["r5"] = g["rng"].transform(lambda s: s.rolling(5).mean())
    f["r20"] = g["rng"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(5)
    f["comp5"] = f.r5 / f.r20.replace(0, np.nan)
    f["lb5"] = g["lbsh"].transform(lambda s: s.rolling(5).mean())
    f["lb20"] = g["lbsh"].transform(lambda s: s.rolling(20).mean()).groupby(f.ticker).shift(5)
    f["lbrel5"] = f.lb5 / f.lb20.replace(0, np.nan)
    f["upvol5"] = g["upvol"].transform(lambda s: s.rolling(5).mean())
    # volume slope: last-day vol vs 5d mean (rising into the event?)
    f["vslope"] = f.tv / f.v5.replace(0, np.nan)
    return f[["ticker", "day", "rvol5", "comp5", "lbrel5", "upvol5", "vslope"]]


def edge_fires_4h():
    con = duckdb.connect(db_path("studio_4h.duckdb"), read_only=True)
    df = con.execute(f"SELECT {_PROJ} FROM bars WHERE close>=5 AND avg_vol_20d>0 ORDER BY ticker, date").fetchdf()
    con.close()
    df = ER._prep(df)
    df["day"] = (pd.to_datetime(df["date"]) - pd.Timedelta(hours=5)).dt.strftime("%Y-%m-%d")
    cols = [c for c in df.columns if c.startswith("E_") and "SC" not in c]
    agg = df.groupby(["ticker", "day"], sort=False)[cols].max().reset_index()
    agg.columns = ["ticker", "day"] + [c + "_4h" for c in cols]
    del df; gc.collect()
    return agg


def main():
    t0 = time.time()
    d = daily_frame()
    print(f"daily+masks {len(d):,} ({time.time()-t0:.0f}s)", flush=True)
    g = d.groupby("ticker", sort=False)
    d["pc"] = g["close"].shift(1)
    d["ret"] = d.close / d.pc - 1
    d["dv"] = d.close * d["volume"]
    d["spikeF"] = (d.ret >= SPIKE).astype(float)
    d["ev_next"] = g["spikeF"].shift(-1)
    d["ev5"] = g["spikeF"].transform(lambda s: s[::-1].rolling(5, min_periods=1).max()[::-1])
    d["ev5"] = g["ev5"].shift(-1)

    # ── price structure (window ending at D) ─────────────────────────────────
    d["hi20"] = g["high"].transform(lambda s: s.rolling(20).max()).groupby(d.ticker).shift(1)
    d["lo20"] = g["low"].transform(lambda s: s.rolling(20).min()).groupby(d.ticker).shift(1)
    d["near_high"] = d.close >= d.hi20 * 0.97
    d["near_low"] = d.close <= d.lo20 * 1.03
    d["drift5"] = g["close"].transform(lambda s: s.pct_change(5))
    d["rsi_slope"] = d.rsi_14 - g["rsi_14"].shift(5)
    d["hlc"] = (d.low > g["low"].shift(1)).astype(float)
    d["hl5"] = g["hlc"].transform(lambda s: s.rolling(5).sum())
    # descriptor events in window (5d count ending at D)
    def cnt5(flag):
        return flag.astype(float).groupby(d.ticker).rolling(5).sum().reset_index(level=0, drop=True)
    d["nd5"] = cnt5(d.fsfx.str.startswith("ND"))       # failed breakdowns
    d["nu5"] = cnt5(d.fsfx.str.startswith("NU"))       # failed breakouts
    d["eu5"] = cnt5(d.fsfx.str.startswith("EU"))       # held breakouts
    d["volB5"] = cnt5(d.vb == "B")
    d["L55"] = cnt5(d.l == "L5"); d["L465"] = cnt5(d.l == "L46"); d["L345"] = cnt5(d.l == "L34")
    d["r2x5"] = cnt5(d.l5.str.contains("R2X", na=False))
    d["r2l5"] = cnt5(d.l5.str.contains("R2L", na=False))
    d["vx5"] = cnt5(d.l5.str.contains("VX", na=False))
    d["t5c"] = cnt5(d.t != ""); d["z5c"] = cnt5(d.z != "")
    d["p5c"] = cnt5(d.anyp == 1); d["spr5"] = cnt5(d.spring == 1)
    d["gap5"] = cnt5(d.gap.isin(["G2", "G3"]))
    d["sc_zone"] = (d.vtr == 1) & (d.wt_sup > 0) & ((d.close / d.wt_sup.replace(0, np.nan) - 1).abs() <= 0.05)
    # daily edge fires in window (any fire D-4..D)
    E1 = [c for c in d.columns if c.startswith("E_") and "SC" not in c]
    for c in E1:
        d[c + "_w"] = cnt5(d[c].astype(bool)) > 0

    liq = (d.pc >= PMIN) & (d.dv >= DV)
    print(f"daily features ready ({time.time()-t0:.0f}s)", flush=True)

    h1 = h1_features()
    print(f"1h features ({time.time()-t0:.0f}s)", flush=True)
    e4 = edge_fires_4h()
    print(f"4h edge fires ({time.time()-t0:.0f}s)", flush=True)
    m = d[liq].merge(h1, on=["ticker", "day"], how="left").merge(e4, on=["ticker", "day"], how="left")
    m["yr"] = m.day.str[:4]
    m = m.dropna(subset=["ev_next", "ev5"])
    del d; gc.collect()
    base1 = m.ev_next.mean() * 100; base5 = m.ev5.mean() * 100
    print(f"\nmerged {len(m):,} · base: next {base1:.2f}% · ≤5d {base5:.2f}%\n", flush=True)

    # ── boolean/count features → lift table ───────────────────────────────────
    BOOLS = {
        "near 20d-HIGH (coil@resistance)": m.near_high,
        "near 20d-LOW": m.near_low,
        "ND≥2 (failed breakdowns)": m.nd5 >= 2,
        "NU≥2 (failed breakouts)": m.nu5 >= 2,
        "EU≥2 (held breakouts)": m.eu5 >= 2,
        "vol=B ≥2 days": m.volB5 >= 2,
        "L5 in window": m.L55 >= 1,
        "L46 in window": m.L465 >= 1,
        "L34 in window": m.L345 >= 1,
        "R2X in window": m.r2x5 >= 1,
        "R2L≥2 (RSI2 oversold)": m.r2l5 >= 2,
        "VX in window (vol climax)": m.vx5 >= 1,
        "T≥3 days (bull cluster)": m.t5c >= 3,
        "Z≥3 days (bear cluster)": m.z5c >= 3,
        "P-signal in window": m.p5c >= 1,
        "spring in window": m.spr5 >= 1,
        "gap-up ≥2": m.gap5 >= 2,
        "SC-zone at D": m.sc_zone,
        "RSI<30 at D": m.rsi_14 < 30,
        "RSI>70 at D": m.rsi_14 > 70,
        "drift5 ≤ −10% (crashed)": m.drift5 <= -0.10,
        "drift5 ≥ +10% (running)": m.drift5 >= 0.10,
        "hl5 ≥ 4 (higher-lows grind)": m.hl5 >= 4,
        "rvol5 > 1.3": m.rvol5 > 1.3,
        "comp5 > 1.2": m.comp5 > 1.2,
        "lbrel5 < 0.8 (diffuse)": m.lbrel5 < 0.8,
        "vslope > 1.5 (vol rising NOW)": m.vslope > 1.5,
    }
    for c in [c for c in m.columns if c.endswith("_w")]:
        BOOLS[f"1D {c[2:-2]} fired≤5d"] = m[c] == True
    for c in [c for c in m.columns if c.endswith("_4h")]:
        BOOLS[f"4H {c[2:-3]} fired"] = m[c] == True

    rows = []
    for lab, q in BOOLS.items():
        q = q.fillna(False)
        s = m[q]
        if len(s) < 3000:
            continue
        rows.append({"feature": lab, "days%": len(s) / len(m) * 100,
                     "lift1": s.ev_next.mean() * 100 / base1,
                     "lift5": s.ev5.mean() * 100 / base5, "n": len(s), "mask": q})
    R = sorted(rows, key=lambda r: -abs(r["lift5"] - 1))
    print(f"{'feature':38s} {'days%':>6s} {'nx':>6s} {'≤5d':>6s}")
    for r in R:
        print(f"{r['feature']:38s} {r['days%']:5.1f}% {r['lift1']:5.2f}× {r['lift5']:5.2f}×")
    print("\nper-year ≤5d lift — top movers (|lift|≥1.25):")
    for r in R:
        if abs(r["lift5"] - 1) < 0.25:
            continue
        cells = []
        for y, sub in m.groupby("yr"):
            qq = r["mask"].reindex(sub.index, fill_value=False)
            s = sub[qq]
            b = sub.ev5.mean()
            cells.append(f"{y[2:]}:{s.ev5.mean()/b:.2f}" if len(s) > 1500 and b > 0 else f"{y[2:]}:·")
        print(f"  {r['feature']:38s} " + " ".join(cells))
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
