"""The CLA<0 cell, opened up: how big did those 104 trades get, and what was the 1H tape doing?

The path-sim number (+6.86 median) is what the ⚡ATR trail actually HARVESTED. It says nothing
about how far the moves ran — a trailing exit gives back the tail by construction. So this
measures MFE (max favourable excursion) separately, and also MAE, because a big MFE with a big
MAE is not a tradeable edge, it is a wide swing you had to sit through.

Then the 1H question: the 1D read for this cell is "down bars close higher in their range than
up bars" — absorption. If that is real, the 1H tape in the base should show it too (demand
carrying volume, absorption bars present). If the 1H shows supply, the 1D reading is geometry,
not order flow — which is exactly what killed the WMT cluster yesterday.

Coverage caveat stated in the output: studio_1h carries 3,203 tickers vs 5,008 on daily, so
some signals have no 1H data at all. Those are counted, never silently dropped.
"""
import os, sys
import duckdb
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
W, MAXH = 20, 60
pd.set_option("display.width", 240)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}\n", flush=True)

# ── rebuild the exact cell ─────────────────────────────────────────────────────
sig = []
for tk, g in grp.items():
    h = g["high"].astype(float); l = g["low"].astype(float)
    c = g["close"].astype(float); o = g["open"].astype(float)
    rng = (h - l).replace(0, np.nan)
    loc = (c - l) / rng * 100.0
    up = c > c.shift(1)
    cla = (loc.where(up).rolling(W, min_periods=6).mean()
           - loc.where(~up).rolling(W, min_periods=6).mean()).shift(1)
    hi40 = c.rolling(40, min_periods=25).max()
    lo40 = c.rolling(40, min_periods=25).min()
    dd = ((lo40 / hi40 - 1.0) * 100.0).shift(1)
    pos = c.rolling(40, min_periods=25).apply(
        lambda s: float(np.argmin(s)) / (len(s) - 1), raw=True).shift(1)
    m = (cla < 0) & (dd <= -20) & (pos <= 0.6) & (g["rsi_14"] < 45)
    m = m.fillna(False).to_numpy()
    if not m.any():
        continue
    dts = pd.to_datetime(g["date"]).dt.strftime("%Y-%m-%d").to_numpy()
    hh = h.to_numpy(); ll = l.to_numpy(); cc = c.to_numpy(); oo = o.to_numpy()
    for i in np.where(m)[0]:
        if i + 1 >= len(g):
            continue
        entry = float(oo[i + 1])                       # entry on the NEXT bar's open
        if not np.isfinite(entry) or entry <= 0:
            continue
        j = min(i + 1 + MAXH, len(g))
        fwd_h = hh[i + 1:j]; fwd_l = ll[i + 1:j]
        if len(fwd_h) < 10:
            continue
        mfe = (fwd_h.max() / entry - 1) * 100
        mae = (fwd_l.min() / entry - 1) * 100
        peak = int(np.argmax(fwd_h)) + 1
        sig.append(dict(ticker=tk, date=dts[i], entry=entry, cla=float(cla.iloc[i]),
                        rsi=float(g["rsi_14"].iloc[i]), close=float(cc[i]),
                        mfe=mfe, mae=mae, bars_to_peak=peak,
                        mfe5=(fwd_h[:5].max() / entry - 1) * 100,
                        mfe10=(fwd_h[:10].max() / entry - 1) * 100,
                        mfe20=(fwd_h[:20].max() / entry - 1) * 100))

S = pd.DataFrame(sig).sort_values("date").reset_index(drop=True)
print(f"===== the cell: {len(S)} signals · {S.ticker.nunique()} tickers · "
      f"{S.date.min()} → {S.date.max()} =====\n", flush=True)

# ── 1. how big did they get ────────────────────────────────────────────────────
print("===== 1. MAX GAIN (MFE) — how far did they actually run =====", flush=True)
print(f"  MFE over 60 bars:", flush=True)
print("    " + "  ".join(f"p{int(p*100)} {S.mfe.quantile(p):>+7.1f}%"
                         for p in [.10, .25, .50, .75, .90]), flush=True)
print(f"    mean {S.mfe.mean():+.1f}%  ·  max {S.mfe.max():+.1f}%  "
      f"({S.loc[S.mfe.idxmax(),'ticker']} {S.loc[S.mfe.idxmax(),'date']})", flush=True)
print(f"\n  MFE by horizon (median):  5d {S.mfe5.median():+.1f}%  ·  "
      f"10d {S.mfe10.median():+.1f}%  ·  20d {S.mfe20.median():+.1f}%  ·  "
      f"60d {S.mfe.median():+.1f}%", flush=True)
print(f"\n  MAE (worst drawdown before the peak):", flush=True)
print("    " + "  ".join(f"p{int(p*100)} {S.mae.quantile(p):>+7.1f}%"
                         for p in [.10, .25, .50, .75, .90]), flush=True)
print(f"\n  median bars to the peak: {S.bars_to_peak.median():.0f}", flush=True)
for thr in [5, 10, 20, 50, 100]:
    print(f"    reached +{thr}%: {(S.mfe >= thr).mean()*100:>5.1f}%  "
          f"({int((S.mfe >= thr).sum())} of {len(S)})", flush=True)
print(f"\n  ⚠ MFE/MAE ratio (median): {(S.mfe / S.mae.abs().replace(0, np.nan)).median():.2f}"
      f"  — a big MFE only counts if MAE stayed small", flush=True)

print("\n  --- the 12 biggest ---", flush=True)
print(S.nlargest(12, "mfe")[["date", "ticker", "close", "rsi", "cla", "mfe", "mae",
                             "bars_to_peak"]]
      .to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)

print("\n  --- per year ---", flush=True)
S["yr"] = S.date.str[:4]
print(S.groupby("yr").agg(n=("mfe", "size"), mfe_med=("mfe", "median"),
                          mfe_max=("mfe", "max"), mae_med=("mae", "median"))
      .round(1).to_string(), flush=True)

# ── 2. what was the 1H tape doing in the base ─────────────────────────────────
print("\n\n===== 2. the 1H tape during the 1D base (10 sessions before the signal) =====",
      flush=True)
con = duckdb.connect(os.path.join(ROOT, "data", "studio_1h.duckdb"), read_only=True)
have1h = set(x[0] for x in con.execute("select distinct ticker from bars").fetchall())
miss = S[~S.ticker.isin(have1h)]
print(f"  1H coverage: {len(S)-len(miss)}/{len(S)} signals have 1H data "
      f"({len(miss)} missing — counted, not dropped)", flush=True)

rows = []
for _, r in S[S.ticker.isin(have1h)].iterrows():
    q = (f"select date, open, high, low, close, volume, t_sig, z_sig, l_sig "
         f"from bars where ticker='{r.ticker}' and date < '{r.date} 23:59' "
         f"and date >= (DATE '{r.date}' - INTERVAL 14 DAY) order by date")
    x = con.execute(q).fetchdf()
    if len(x) < 20:
        continue
    x["ret"] = x["close"].pct_change() * 100
    rg = (x["high"] - x["low"]).replace(0, np.nan)
    x["loc"] = (x["close"] - x["low"]) / rg * 100
    x["body"] = (x["close"] - x["open"]).abs() / rg * 100
    x["vx"] = x["volume"] / x["volume"].rolling(20, min_periods=8).mean()
    uv = x.loc[x["ret"] > 0, "volume"].mean()
    dv = x.loc[x["ret"] < 0, "volume"].mean()
    absb = int(((x["vx"] >= 1.8) & (x["body"] <= 40) & (x["loc"] >= 50)).sum())
    l34 = int(x["l_sig"].astype(str).str.contains("L34").sum())
    l46 = int(x["l_sig"].astype(str).str.contains("L46").sum())
    rows.append(dict(ticker=r.ticker, date=r.date, mfe=r.mfe,
                     updn=uv / dv if dv and dv == dv else np.nan,
                     absb=absb, bars=len(x),
                     loc_dn=x.loc[x["ret"] < 0, "loc"].mean(),
                     loc_up=x.loc[x["ret"] > 0, "loc"].mean(),
                     l34=l34, l46=l46))
con.close()
H = pd.DataFrame(rows)
print(f"  analysed {len(H)} signals with enough 1H history\n", flush=True)
if len(H):
    print(f"  1H up/down volume ratio:  median {H.updn.median():.2f}  ·  "
          f"share above 1.0 (DEMAND carries): {(H.updn > 1).mean()*100:.0f}%", flush=True)
    print(f"  1H absorption bars in the base: median {H.absb.median():.0f}  ·  "
          f"share with ≥1: {(H.absb >= 1).mean()*100:.0f}%  ·  "
          f"with ≥3: {(H.absb >= 3).mean()*100:.0f}%", flush=True)
    print(f"  1H close-location: down bars {H.loc_dn.mean():.1f}%  vs  "
          f"up bars {H.loc_up.mean():.1f}%  → Δ {H.loc_up.mean()-H.loc_dn.mean():+.1f}",
          flush=True)
    print(f"  1H VSA lines in the base: L34 (absorption) median {H.l34.median():.0f}  ·  "
          f"L46 (crowd) median {H.l46.median():.0f}", flush=True)

    print("\n  --- does the 1H tape SEPARATE winners from losers? ---", flush=True)
    H["big"] = H.mfe >= H.mfe.median()
    g2 = H.groupby("big").agg(n=("mfe", "size"), mfe=("mfe", "median"),
                              updn=("updn", "median"), absb=("absb", "median"),
                              l34=("l34", "median"), l46=("l46", "median")).round(2)
    g2.index = ["below-median MFE", "above-median MFE"]
    print(g2.to_string(), flush=True)
    for col, lab in [("updn", "1H up/dn volume"), ("absb", "1H absorption bars"),
                     ("l34", "1H L34 count"), ("l46", "1H L46 count")]:
        cr = H[col].corr(H["mfe"], method="spearman")
        print(f"    spearman({lab:20s}, MFE) = {cr:+.3f}", flush=True)

print("\nDONE", flush=True)
