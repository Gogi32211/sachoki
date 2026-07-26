"""
validate_mtf_mid.py — intermediate stages between SMX (green) and ORANGE (orange).

SMX→ORANGE transition = 3 milestones on top of the shared recovery backdrop:
    M1  15m: e50>e200  (15m stack completes above its 200)
    M2  1H:  e20>e50   (1H ordering completes)
    M3  4H:  e9>e20    (first 4H bull cross — mutually exclusive with SMX)
Shared BASE (present in both): 4H e200>e50>e20 · 1H e9>e20 · 15m e9>e20>e50.

Tested (same harness as validate_mtf_ema: EOD snapshot → daily path-sim trail25/60,
gap-realistic, entry next open, dv≥3M, suppressor-clean):
    K0..K3        milestone-count ladder (monotonicity read)
    MID-A/B/C     specific intermediate geometries
    SMX / ORANGE  original variants for reference
    SEQ-*         same, gated by "SMX fired on this ticker within the prior 10 trading days"
READ-ONLY.
"""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim, _stats
from validate_mtf_ema import _daily_universe, _tf_daily_snapshot, _masks

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
TR = ["2021", "2022", "2023"]; TE = ["2024", "2025", "2026"]
YRS = ["2021", "2022", "2023", "2024", "2025", "2026"]
CHUNK = 400
SEQ_WIN = 10   # trading days after an SMX fire


def _mid_masks(m: pd.DataFrame) -> dict:
    base = ((m.e200_4h > m.e50_4h) & (m.e50_4h > m.e20_4h)          # 4H long-EMAs still bearish
            & (m.e9_1h > m.e20_1h)                                   # 1H turn under way
            & (m.e9_15 > m.e20_15) & (m.e20_15 > m.e50_15))          # 15m short stack up
    M1 = m.e50_15 > m.e200_15
    M2 = m.e20_1h > m.e50_1h
    M3 = m.e9_4h > m.e20_4h
    k = M1.astype(int) + M2.astype(int) + M3.astype(int)
    r = {}
    r["K0"] = base & (k == 0)
    r["K1"] = base & (k == 1)
    r["K2"] = base & (k == 2)
    r["K3"] = base & (k == 3)
    r["MID-A"] = base & M1 & ~M2 & ~M3      # 15m done, 1H/4H not yet
    r["MID-B"] = base & M1 & M2 & ~M3       # ORANGE minus the 4H cross (latest pre-orange)
    r["MID-C"] = base & M1 & ~M2 & M3       # 4H crossed before 1H ordered (alt order)
    return r


def run():
    print("pulling daily universe…", flush=True)
    daily = _daily_universe()
    daily["day"] = daily["date"].str[:10]
    tickers = daily["ticker"].unique().tolist()
    print(f"daily rows {len(daily):,} · tickers {len(tickers)}", flush=True)

    merged_parts = []
    t0 = time.time()
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i + CHUNK]
        s15 = _tf_daily_snapshot(chunk, "15m")
        s1h = _tf_daily_snapshot(chunk, "1h")
        s4h = _tf_daily_snapshot(chunk, "4h")
        if len(s15) == 0 or len(s1h) == 0 or len(s4h) == 0:
            continue
        d = daily[daily["ticker"].isin(chunk)]
        m = (d.merge(s15, on=["ticker", "day"], how="inner")
               .merge(s1h, on=["ticker", "day"], how="inner")
               .merge(s4h, on=["ticker", "day"], how="inner"))
        merged_parts.append(m)
        print(f"  chunk {i//CHUNK+1}/{(len(tickers)+CHUNK-1)//CHUNK} "
              f"rows={sum(len(x) for x in merged_parts):,} ({(time.time()-t0)/60:.1f}min)", flush=True)
    m = pd.concat(merged_parts, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)
    print(f"aligned rows {len(m):,}", flush=True)

    ref = _masks(m)                      # SMX / ORANGE originals
    mid = _mid_masks(m)
    clean = m["supp"] == 0
    for k in ("SMX", "ORANGE"):
        m[k] = (ref[k] & clean).values
    for k, v in mid.items():
        m[k] = (v & clean).values

    # SEQ gate: SMX fired on this ticker within the prior SEQ_WIN trading days (not today)
    g = m.groupby("ticker", sort=False)
    smx_recent = (g["SMX"].transform(lambda s: s.shift(1).rolling(SEQ_WIN, min_periods=1).max())
                  .fillna(0).astype(bool))
    for k in ("MID-A", "MID-B", "K2", "ORANGE"):
        m[f"SEQ-{k}"] = (m[k] & smx_recent).values

    grp = {tk: gg.reset_index(drop=True) for tk, gg in m.groupby("ticker", sort=False)}

    names = (["SMX", "K0", "K1", "K2", "K3", "MID-A", "MID-B", "MID-C", "ORANGE"]
             + [f"SEQ-{k}" for k in ("MID-A", "MID-B", "K2", "ORANGE")])
    print(f"\ntrail25/60 · gap-realistic · entry next daily open · dv≥3M · SEQ window {SEQ_WIN}d\n")
    for k in names:
        s = _stats(k, _pathsim(grp, k, **KW))
        if not s or s.get("n", 0) == 0:
            print(f"  {k:10s} n=0"); continue
        py = s["per_year"]
        tr = np.mean([py[y] for y in TR if y in py]); te = np.mean([py[y] for y in TE if y in py])
        yr = " ".join(f"{y[2:]}:{py.get(y, float('nan')):+4.1f}" for y in YRS)
        print(f"  {k:10s} n={s['n']:>6} m{s['mean']:+5.2f} md{s['median']:+5.2f} pf{str(s['pf']):>4} "
              f"y{s['pos_years']}/{s['total_years']} TR{tr:+5.2f} TE{te:+5.2f} | {yr}")
    print("\ndone.")


if __name__ == "__main__":
    run()
