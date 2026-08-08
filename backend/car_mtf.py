"""CAR 2026-02-19 → 2026-04-01: what built the base, read on W / D / 4H / 1H / 15m.

The user's question is about the ACCUMULATION, not the squeeze: what was happening in the
six weeks before the launch. Reads the five bar DBs directly (read-only, no writer risk).
"""
import duckdb
import numpy as np
import pandas as pd

TK = "CAR"
A, B = "2026-02-19", "2026-04-02"
pd.set_option("display.width", 240)
pd.set_option("display.max_rows", 400)

DBS = [("1W", "data/studio_1w.duckdb"), ("1D", "data/studio_analytics.duckdb"),
       ("4H", "data/studio_4h.duckdb"), ("1H", "data/studio_1h.duckdb"),
       ("15m", "data/studio_15m.duckdb")]

COLS = ("date, open, high, low, close, volume, rsi_14, cci_20, atr_14, "
        "t_sig, z_sig, l_sig, vol_bucket, full_suffix, composite_vol, wyc_phase")


def load(path, a=A, b=B):
    c = duckdb.connect(path, read_only=True)
    q = (f"select distinct {COLS} from bars where ticker='{TK}' "
         f"and date >= '{a}' and date < '{b}' order by date")
    df = c.execute(q).fetchdf()
    c.close()
    return df


def tag(df):
    """volume vs its own 20-bar average, bar geometry, close location in range."""
    df = df.copy()
    df["vx"] = df["volume"] / df["volume"].rolling(20, min_periods=5).mean()
    rng = (df["high"] - df["low"]).replace(0, np.nan)
    df["clo%"] = (df["close"] - df["low"]) / rng * 100          # close location in bar
    df["body%"] = (df["close"] - df["open"]).abs() / rng * 100
    df["ret%"] = df["close"].pct_change() * 100
    return df


print("=" * 100)
print(f"CAR · {A} → 2026-04-01 · the six weeks BEFORE the launch")
print("=" * 100, flush=True)

# ── context: the weekly picture, wider window ──────────────────────────────────
w = tag(load("data/studio_1w.duckdb", "2025-11-01", "2026-04-06"))
print("\n########## 1W — the structure the base sat in ##########")
print(w[["date", "open", "high", "low", "close", "ret%", "vx", "clo%", "rsi_14",
         "t_sig", "z_sig", "l_sig", "vol_bucket"]]
      .to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)

# ── 1D: the whole base, bar by bar ─────────────────────────────────────────────
d = tag(load("data/studio_analytics.duckdb"))
print(f"\n########## 1D — {len(d)} bars ##########")
print(d[["date", "open", "high", "low", "close", "ret%", "volume", "vx", "clo%", "body%",
         "rsi_14", "t_sig", "z_sig", "l_sig", "vol_bucket", "full_suffix", "wyc_phase"]]
      .to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)

# ── 1D summary: how the base was actually shaped ───────────────────────────────
print("\n########## 1D — base geometry ##########")
base = d[(d.date.astype(str) >= "2026-02-24") & (d.date.astype(str) <= "2026-03-20")]
print(f"  base window 2026-02-24 → 2026-03-20 ({len(base)} bars)")
print(f"    range  low {base.low.min():.2f}  high {base.high.max():.2f}  "
      f"= {(base.high.max()/base.low.min()-1)*100:.1f}% wide")
print(f"    closes low {base.close.min():.2f}  high {base.close.max():.2f}")
print(f"    avg vol {base.volume.mean():,.0f} vs pre-crash 20d "
      f"{d.volume.head(1).values[0]:,.0f} on the crash bar")
up = base[base["ret%"] > 0]; dn = base[base["ret%"] < 0]
print(f"    up bars {len(up)} avg vol {up.volume.mean():,.0f}  ·  "
      f"down bars {len(dn)} avg vol {dn.volume.mean():,.0f}  "
      f"→ ratio {up.volume.mean()/max(dn.volume.mean(),1):.2f}× "
      f"({'DEMAND' if up.volume.mean()>dn.volume.mean() else 'SUPPLY'} carries the volume)")
print(f"    avg close-location: up bars {up['clo%'].mean():.0f}%  down bars {dn['clo%'].mean():.0f}%")
# higher lows?
print("\n    swing lows through the base:")
lows = base.set_index(base.date.astype(str))["low"]
r = lows.rolling(3, center=True).min()
piv = lows[(lows == r) & (lows.shift(1) > lows) & (lows.shift(-1) > lows)]
for k, v in piv.items():
    print(f"      {k}  {v:.2f}")

# ── intraday: 4H / 1H / 15m ────────────────────────────────────────────────────
for lab, path in [("4H", "data/studio_4h.duckdb"), ("1H", "data/studio_1h.duckdb")]:
    x = tag(load(path))
    print(f"\n########## {lab} — {len(x)} bars ##########")
    print(f"  volume: up-bars {x.loc[x['ret%']>0,'volume'].mean():,.0f} vs "
          f"down-bars {x.loc[x['ret%']<0,'volume'].mean():,.0f}")
    big = x.nlargest(15, "volume")[["date", "close", "ret%", "volume", "vx", "clo%",
                                    "t_sig", "z_sig", "l_sig", "vol_bucket"]]
    print(f"  --- 15 highest-volume {lab} bars in the window ---")
    print(big.sort_values("date").to_string(index=False, float_format=lambda x: f"{x:.2f}"),
          flush=True)
    # absorption count: high volume, small body, close in upper half
    absb = x[(x["vx"] >= 1.8) & (x["body%"] <= 40) & (x["clo%"] >= 50)]
    print(f"  ABSORPTION bars (vol≥1.8× · body≤40% of range · close in upper half): {len(absb)}")
    if len(absb):
        print(absb[["date", "close", "volume", "vx", "clo%", "body%", "t_sig", "z_sig", "l_sig"]]
              .to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)

# ── 15m: daily aggregates (too many bars to print raw) ─────────────────────────
m = tag(load("data/studio_15m.duckdb"))
m["d"] = pd.to_datetime(m["date"]).dt.strftime("%Y-%m-%d")
print(f"\n########## 15m — {len(m)} bars, aggregated by day ##########")
agg = m.groupby("d").apply(lambda s: pd.Series({
    "bars": len(s), "vol": s.volume.sum(),
    "vmax/vavg": s.volume.max() / max(s.volume.mean(), 1),
    "up_vol": s.loc[s["ret%"] > 0, "volume"].sum(),
    "dn_vol": s.loc[s["ret%"] < 0, "volume"].sum(),
    "hi": s.high.max(), "lo": s.low.min(), "close": s.close.iloc[-1],
}), include_groups=False)
agg["up/dn"] = agg["up_vol"] / agg["dn_vol"].replace(0, np.nan)
print(agg[["bars", "vol", "vmax/vavg", "up/dn", "lo", "hi", "close"]]
      .to_string(float_format=lambda x: f"{x:,.2f}"), flush=True)
print("\n  🏆 15m max/avg ≥4× is our most universal intraday filter — days that cleared it:")
hot = agg[agg["vmax/vavg"] >= 4]
print("   " + (", ".join(f"{k} ({v:.1f}×)" for k, v in hot["vmax/vavg"].items()) or "none"),
      flush=True)

print("\nDONE", flush=True)
