"""
validate_tradertom.py — test Tom Hougaard's Dow stats on our equity universe.

A) Intraday H/L timing (15m): by bar N, what % of days have printed their
   first extreme (the day's high-bar OR low-bar). Replicates Tom's curve
   (5min basis: bar7→50%, 13→70%, 18→90%) at 15m granularity (~26 bars/RTH day).
C) Daily gaps: fill rate (same-day / within-2d) AND a fade path-sim —
   fade = bet the gap fills (gap-up → short to prior close, gap-down → long),
   entry=open, target=prior close, stop=1×ATR beyond, 3-day stop-first
   pessimistic. Answers "does the 78.6% fill rate translate to profit?"
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb, time
from studio.paths import ANALYTICS_DB, db_path

DVMIN = 2_000_000; PMIN = 3.0


def test_A():
    con = duckdb.connect(db_path("studio_15m_base.duckdb"), read_only=True)
    df = con.execute("""SELECT ticker, date, high, low FROM (
      SELECT ticker,date,high,low,row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
      FROM bars) WHERE rn=1 ORDER BY ticker, date""").fetchdf()
    con.close()
    df["day"] = (pd.to_datetime(df["date"]) - pd.Timedelta(hours=5)).dt.strftime("%Y-%m-%d")
    first_ext = []          # bar index (0-based) of the FIRST extreme, and session length
    for (tk, day), g in df.groupby(["ticker", "day"], sort=False):
        if len(g) < 10:
            continue
        hb = int(np.argmax(g["high"].values)); lb = int(np.argmin(g["low"].values))
        first_ext.append((min(hb, lb), max(hb, lb), len(g)))
    F = pd.DataFrame(first_ext, columns=["first", "both", "nbars"])
    F = F[(F.nbars >= 24) & (F.nbars <= 27)]     # standard RTH days (~26 15m bars)
    n = len(F)
    print(f"\n=== A) intraday H/L timing — {n:,} standard RTH days (15m, ~26 bars) ===")
    print("  by 15m bar N → % of days that have printed their FIRST extreme (H or L):")
    for bar in [1, 2, 3, 4, 5, 7, 9, 13, 18, 22, 26]:
        pct = (F["first"] < bar).mean() * 100         # formed by END of bar (index<bar)
        frac = bar / 26.0
        print(f"    bar {bar:2d} (~{int(frac*390)}min, {frac*100:.0f}% of session): {pct:5.1f}%")
    print(f"  BOTH extremes formed by bar 13: {(F['both']<13).mean()*100:.1f}%  · by bar 22: {(F['both']<22).mean()*100:.1f}%")


def test_C():
    con = duckdb.connect(ANALYTICS_DB, read_only=True)
    d = con.execute("""SELECT ticker, CAST(date AS VARCHAR)[:10] AS day, open, high, low, close, volume FROM (
      SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn FROM bars)
      WHERE rn=1 ORDER BY ticker, date""").fetchdf()
    con.close()
    g = d.groupby("ticker", sort=False)
    pc = g["close"].shift(1)
    hi = g["high"].shift(1); lo = g["low"].shift(1)
    tr = np.maximum(d["high"] - d["low"], np.maximum((d["high"] - pc).abs(), (d["low"] - pc).abs()))
    d["atr"] = tr.groupby(d["ticker"]).transform(lambda s: s.ewm(alpha=1/14, adjust=False).mean())
    d["pc"] = pc; d["gap"] = (d["open"] / pc - 1) * 100
    d["dv"] = d["close"] * d["volume"]
    # next-2-day extremes for fill + 3-day path arrays
    for k in (1, 2, 3):
        d[f"h{k}"] = g["high"].shift(-k); d[f"l{k}"] = g["low"].shift(-k); d[f"o{k}"] = g["open"].shift(-k)
    m = d[(d.dv >= DVMIN) & (d.close >= PMIN) & d.pc.notna() & d.atr.notna() & (d.atr > 0)].copy()
    m["yr"] = m["day"].str[:4]

    def fade_sim(row):
        """Fade toward prior close. gap-up→short, gap-down→long. entry=open,
        target=prior close, stop=1*ATR beyond entry. 3-day, stop-first pessimistic.
        Returns R (reward/risk units)."""
        o = row["open"]; pc = row["pc"]; atr = row["atr"]
        up = row["gap"] > 0
        if up:
            tgt = pc; stop = o + atr; risk = stop - o; rew = o - tgt      # short
            if risk <= 0 or rew <= 0: return None
            for k in (0, 1, 2, 3):
                hh = row["high"] if k == 0 else row[f"h{k}"]
                ll = row["low"] if k == 0 else row[f"l{k}"]
                if pd.isna(hh): break
                if hh >= stop: return -1.0            # stopped (short: high hits stop above)
                if ll <= tgt: return rew / risk       # filled
            return 0.0
        else:
            tgt = pc; stop = o - atr; risk = o - stop; rew = tgt - o      # long
            if risk <= 0 or rew <= 0: return None
            for k in (0, 1, 2, 3):
                hh = row["high"] if k == 0 else row[f"h{k}"]
                ll = row["low"] if k == 0 else row[f"l{k}"]
                if pd.isna(ll): break
                if ll <= stop: return -1.0
                if hh >= tgt: return rew / risk
            return 0.0

    print("\n=== C) GAP fill + FADE path-sim (equity universe) ===")
    for lab, q in [("gap UP >+1%", m.gap > 1), ("gap UP >+3%", m.gap > 3),
                   ("gap DOWN <-1%", m.gap < -1), ("gap DOWN <-3%", m.gap < -3)]:
        s = m[q].copy()
        if len(s) < 200: continue
        up = s.gap.iloc[0] > 0
        # same-day fill
        if up:
            sameday = (s["low"] <= s["pc"]).mean() * 100
            d2 = ((s["low"] <= s["pc"]) | (s["l1"] <= s["pc"]) | (s["l2"] <= s["pc"])).mean() * 100
        else:
            sameday = (s["high"] >= s["pc"]).mean() * 100
            d2 = ((s["high"] >= s["pc"]) | (s["h1"] >= s["pc"]) | (s["h2"] >= s["pc"])).mean() * 100
        s["R"] = s.apply(fade_sim, axis=1)
        s = s.dropna(subset=["R"])
        R = s["R"].values
        winR = (R > 0).mean() * 100
        yr = s.groupby("yr")["R"].mean()
        ys = " ".join(f"{y[2:]}:{v:+.2f}" for y, v in yr.items())
        print(f"\n  {lab}  n={len(s):,}")
        print(f"    fill same-day {sameday:.0f}% · within-2d {d2:.0f}%")
        print(f"    FADE path-sim: meanR {R.mean():+.3f}  win {winR:.0f}%  (target=fill, stop=1ATR, 3d, stop-first)")
        print(f"    per-year meanR: {ys}")


def main():
    t0 = time.time()
    test_A()
    test_C()
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
