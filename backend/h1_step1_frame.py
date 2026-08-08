"""1H independent research — STEP 1 ONLY (user instruction 2026-08-05: the later steps
of the original plan lean on 1D-derived priors, so they are NOT run).

This step does two things and nothing else:
  1. Build + cache the raw 1H frame (parquet) so later steps don't re-hit the 33GB DB.
  2. DESCRIBE the 1H language neutrally — code vocabulary frequencies, baseline forward
     return distributions at several horizons (measured in 1H bars, no 1D convention),
     and basic session anatomy (bars per day). No thresholds, no mining, no verdicts.
"""
import gc
import numpy as np
import pandas as pd
import duckdb
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import db_path

CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "h1_research_frame.parquet")

c = duckdb.connect(db_path("studio_1h.duckdb"), read_only=True)
df = c.execute("""
    SELECT ticker, CAST(date AS VARCHAR) dt, open, high, low, close, volume, rsi_14,
           coalesce(t_sig,'') t, coalesce(z_sig,'') z, coalesce(l_sig,'') l,
           coalesce(full_suffix,'') fsfx, coalesce(vol_bucket,'') vb
    FROM bars
    WHERE close >= 5 AND close * volume >= 400000
    ORDER BY ticker, date
""").fetchdf()
c.close()
df.to_parquet(CACHE, index=False)
print(f"frame cached -> {CACHE}", flush=True)
print(f"rows {len(df):,} · tickers {df['ticker'].nunique():,} · "
      f"range {df['dt'].min()[:10]} → {df['dt'].max()[:10]}", flush=True)

# ── session anatomy ──────────────────────────────────────────────────────────────
df["day"] = df["dt"].str[:10]
bpd = df.groupby(["ticker", "day"]).size()
print(f"\nbars per ticker-day: median {bpd.median():.0f} · p10 {bpd.quantile(.1):.0f} · "
      f"p90 {bpd.quantile(.9):.0f}", flush=True)
hrs = df["dt"].str[11:13].value_counts(normalize=True).sort_index() * 100
print("bar share by hour (ET, DB tz): " +
      " ".join(f"{h}:{v:.0f}%" for h, v in hrs.items()), flush=True)

# ── vocabulary: how often does each code fire on 1H? ─────────────────────────────
n = len(df)
tc = df.loc[df["t"] != "", "t"].value_counts()
zc = df.loc[df["z"] != "", "z"].value_counts()
lc = df.loc[df["l"] != "", "l"].value_counts()
print(f"\nT-codes: {tc.sum():,} bars ({100*tc.sum()/n:.1f}% of all)", flush=True)
print("  " + " ".join(f"{k}:{v:,}" for k, v in tc.items()), flush=True)
print(f"Z-codes: {zc.sum():,} bars ({100*zc.sum()/n:.1f}%)", flush=True)
print("  " + " ".join(f"{k}:{v:,}" for k, v in zc.items()), flush=True)
print(f"L-codes: {lc.sum():,} bars ({100*lc.sum()/n:.1f}%)", flush=True)
print("  " + " ".join(f"{k}:{v:,}" for k, v in lc.items()), flush=True)
sfx = df.loc[df["fsfx"] != "", "fsfx"].value_counts().head(12)
print(f"top suffixes: " + " ".join(f"{k}:{v:,}" for k, v in sfx.items()), flush=True)

# ── baseline forward-return distributions (several horizons, in 1H bars) ─────────
print("\nbaseline fwd returns (every 20th bar, per-horizon):", flush=True)
g = df.groupby("ticker", sort=False)
samp = (np.arange(len(df)) % 20 == 0)
for H in (7, 14, 35, 70, 140, 280):
    fwd = g["close"].shift(-H) / df["close"] - 1
    s = fwd[samp].dropna() * 100
    print(f"  H={H:>4} h-bars (~{H/7:.1f}d): med {s.median():>+6.2f} · mean {s.mean():>+6.2f}"
          f" · win {(s>0).mean()*100:.1f}% · p10 {s.quantile(.1):>+7.2f} · p90 {s.quantile(.9):>+7.2f}"
          f" · n {len(s):,}", flush=True)
    del fwd, s
    gc.collect()

# ── how clustered are signal bars? (do codes appear in runs on 1H?) ──────────────
code = np.where(df["t"] != "", 1, np.where(df["z"] != "", 1, 0))
prev = np.roll(code, 1); prev[0] = 0
same_tk = df["ticker"].eq(df["ticker"].shift(1)).to_numpy()
p_next = (code[1:][ (code[:-1]==1) & same_tk[1:] ]).mean()
print(f"\nP(signal bar | prev bar was signal) = {p_next:.2f}  vs unconditional {code.mean():.2f}",
      flush=True)
print("\nDONE", flush=True)
