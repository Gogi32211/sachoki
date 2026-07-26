"""
validate_mtf_geometry.py — DISCOVER which multi-TF price/EMA geometry precedes growth.

Per tf (1h, 4h, 1D) at each day's LAST bar, classify into 4 states:
  A  mature bull   : close>e200 and e9>e20>e50
  B  RECOVERY      : close<e200 and e9>e20>e50   (short EMAs turned up under the long one)
  C  bull pullback : close>e200, stack not up
  D  bear          : close<e200, stack not up
64 combos (1h×4h×1D) → fwd 10d/20d return (entry next open, daily bars), universe-wide,
per-year + TRAIN(≤2023)/TEST(2024+). Report top/bottom vs baseline. Discovery only —
no path-sim yet; winners get the full battery afterwards.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import duckdb, time
from studio.paths import ANALYTICS_DB, db_path

DV_FLOOR = 2_000_000
PRICE_MIN = 3.0


def tf_states(db_file: str, label: str) -> pd.DataFrame:
    """Per (ticker, day): EOD EMA state A/B/C/D for one intraday tf DB."""
    con = duckdb.connect(db_path(db_file), read_only=True)
    try:
        df = con.execute("""
            SELECT ticker, date, close FROM (
              SELECT ticker, date, close,
                     row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
              FROM bars) WHERE rn = 1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        con.close()
    g = df.groupby("ticker", sort=False)["close"]
    for L in (9, 20, 50, 200):
        df[f"e{L}"] = g.transform(lambda s, L=L: s.ewm(span=L, adjust=False).mean())
    day = (pd.to_datetime(df["date"]) - pd.Timedelta(hours=5)).dt.strftime("%Y-%m-%d")
    df["day"] = day.values
    snap = df.groupby(["ticker", "day"], sort=False).tail(1)
    up = (snap["e9"] > snap["e20"]) & (snap["e20"] > snap["e50"])
    above = snap["close"] > snap["e200"]
    st = np.where(above & up, "A", np.where(~above & up, "B",
         np.where(above, "C", "D")))
    out = snap[["ticker", "day"]].copy(); out[label] = st
    return out


def main():
    t0 = time.time()
    # 1D states + forward returns from the analytics DB
    con = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        d = con.execute(f"""
            SELECT ticker, CAST(date AS VARCHAR)[:10] AS day, open, close, volume FROM (
              SELECT *, row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
              FROM bars) WHERE rn = 1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        con.close()
    g = d.groupby("ticker", sort=False)["close"]
    for L in (9, 20, 50, 200):
        d[f"e{L}"] = g.transform(lambda s, L=L: s.ewm(span=L, adjust=False).mean())
    up = (d["e9"] > d["e20"]) & (d["e20"] > d["e50"]); above = d["close"] > d["e200"]
    d["s1d"] = np.where(above & up, "A", np.where(~above & up, "B", np.where(above, "C", "D")))
    gg = d.groupby("ticker", sort=False)
    open_next = gg["open"].shift(-1)
    d["fwd10"] = (gg["close"].shift(-11) / open_next - 1) * 100
    d["fwd20"] = (gg["close"].shift(-21) / open_next - 1) * 100
    d["dv"] = d["close"] * d["volume"]
    d = d[(d["dv"] >= DV_FLOOR) & (d["close"] >= PRICE_MIN)]
    d = d[["ticker", "day", "close", "s1d", "fwd10", "fwd20"]]
    print(f"1D ready {len(d):,} rows ({time.time()-t0:.0f}s)")

    s4h = tf_states("studio_4h.duckdb", "s4h")
    print(f"4h ready {len(s4h):,} ({time.time()-t0:.0f}s)")
    s1h = tf_states("studio_1h.duckdb", "s1h")
    print(f"1h ready {len(s1h):,} ({time.time()-t0:.0f}s)")

    m = d.merge(s4h, on=["ticker", "day"]).merge(s1h, on=["ticker", "day"])
    m = m.dropna(subset=["fwd10"])
    m["yr"] = m["day"].str[:4]
    m["combo"] = m["s1h"] + m["s4h"] + m["s1d"]        # e.g. "BBD"
    print(f"joined {len(m):,} rows ({time.time()-t0:.0f}s)")
    base10, base20 = m["fwd10"].median(), m["fwd20"].dropna().median()
    print(f"BASELINE median fwd10 {base10:+.2f}%  fwd20 {base20:+.2f}%")

    rows = []
    for combo, sub in m.groupby("combo"):
        if len(sub) < 3000:
            continue
        yr = sub.groupby("yr")["fwd10"].median()
        TR = yr[yr.index <= "2023"].mean(); TE = yr[yr.index > "2023"].mean()
        pos_yrs = int((yr > base10).sum())
        rows.append({"combo": combo, "n": len(sub),
                     "med10": sub["fwd10"].median(), "med20": sub["fwd20"].median(),
                     "mean10": sub["fwd10"].mean(), "win10": (sub["fwd10"] > 0).mean() * 100,
                     "TR": TR, "TE": TE, "yrs_beat_base": pos_yrs, "n_yrs": len(yr)})
    R = pd.DataFrame(rows).sort_values("med10", ascending=False)
    pd.set_option("display.width", 200)
    fmt = lambda df: df.to_string(index=False, float_format=lambda x: f"{x:+.2f}"
                                  if abs(x) < 50 else f"{x:.0f}")
    print("\n=== TOP 12 combos by median fwd10 (combo = 1h·4h·1D; A bull/B RECOVERY/C pullback/D bear) ===")
    print(fmt(R.head(12)))
    print("\n=== BOTTOM 8 ===")
    print(fmt(R.tail(8)))
    # the specific hypothesis from the VPG chart: recovery on lower TFs under a bear/neutral 1D
    print("\n=== RECOVERY-focused slices ===")
    for q, lab in [((m.s1h == "B") & (m.s4h == "B"), "1h=B & 4h=B (double recovery)"),
                   ((m.s1h == "B") & (m.s4h == "B") & (m.s1d == "D"), "1h=B & 4h=B & 1D=D (early)"),
                   ((m.s1h == "B") & (m.s4h == "B") & (m.s1d == "B"), "1h=B & 4h=B & 1D=B (aligned rec)"),
                   ((m.s1h == "A") & (m.s4h == "B"), "1h=A & 4h=B"),
                   ((m.s1h == "A") & (m.s4h == "A") & (m.s1d == "A"), "ALL-A (mature bull, ref)")]:
        sub = m[q]
        if len(sub) < 500:
            print(f"  {lab:34s} n={len(sub)} (too few)"); continue
        yr = sub.groupby("yr")["fwd10"].median()
        ys = " ".join(f"{y[2:]}:{v:+.1f}" for y, v in yr.items())
        print(f"  {lab:34s} n={len(sub):7,d} med10 {sub['fwd10'].median():+.2f} med20 {sub['fwd20'].median():+.2f} "
              f"win {(sub['fwd10']>0).mean()*100:.0f}%  | {ys}")
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
