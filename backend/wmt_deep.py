"""WMT deep check — the brain's own verdict, and the intraday layer the 1D read is blind to.

Two things the daily board cannot answer on its own:
  1. what would spine.decide() actually do — gates, size, stop, portfolio room
  2. what is the 1H/4H/15m tape doing under the 🎯Confluence cluster, since the MTF gate
     (our strongest veto: a 1D signal with ZERO intraday echo is 0/6 years) reads ❌
"""
import os, sys
import duckdb
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
from brain import spine, live, journal

TK = "WMT"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
pd.set_option("display.width", 240)

grp, as_of = er._frame(60, 3_000_000)
g = grp[TK].copy()
g["d"] = pd.to_datetime(g["date"]).dt.strftime("%Y-%m-%d")
last = g.iloc[-1]
px = float(last["close"])
print(f"{TK} · as_of {as_of} · last {last['d']} · ${px:.2f}\n", flush=True)

# ── 1. what the brain would actually decide ───────────────────────────────────
fired = [c for _, c in er.SETUPS if c in g and bool(g[c].fillna(False).iloc[-1])]
print(f"===== 1. BRAIN VERDICT =====", flush=True)
print(f"  fired on the last bar: {fired}", flush=True)

atr_pct = float(last["atr_14"]) / px
adv = float((g["close"] * g["volume"]).tail(20).mean())
swing_low = float(g["low"].tail(20).min())
try:
    book = journal.load() if hasattr(journal, "load") else {}
    openp = book.get("positions", []) if isinstance(book, dict) else []
except Exception:
    openp = []
print(f"  open positions in the paper book: {len(openp)}", flush=True)

try:
    bs = live._bar_states([TK]).get(TK, {})
except Exception as e:
    bs = {}
    print(f"  (bar_state unavailable: {e})", flush=True)

v = spine.decide(TK, fired, px, sector=str(last.get("sector", "?")),
                 atr_pct=atr_pct, adv_dollars=adv, swing_low=swing_low,
                 open_positions=openp, bar_state=bs)
print(f"\n  DECISION: {v.get('decision')}", flush=True)
for k in ("edge", "reason", "size_shares", "size_value", "size_pct", "stop", "target",
          "risk_pct", "mult", "tier", "conf"):
    if k in v:
        print(f"    {k:14s} {v[k]}", flush=True)
print("\n  --- full log ---", flush=True)
for line in v.get("log", []):
    print(f"    {line}", flush=True)
if "gates" in v or "checks" in v:
    for c in (v.get("gates") or v.get("checks") or []):
        if isinstance(c, dict):
            mark = "—" if not c.get("applicable", True) else ("⛔" if c.get("veto") else "·")
            print(f"    {mark} {c.get('title','?'):34s} ×{c.get('mult',1.0):.2f} "
                  f"{c.get('note','')}", flush=True)

# ── 2. the intraday tape under the cluster ────────────────────────────────────
print("\n\n===== 2. INTRADAY TAPE (the MTF gate reads ❌ — why?) =====", flush=True)
A, B = "2026-07-20", "2026-08-08"
COLS = ("date, open, high, low, close, volume, rsi_14, t_sig, z_sig, l_sig, vol_bucket")


def load(path):
    c = duckdb.connect(os.path.join(ROOT, "data", path), read_only=True)
    df = c.execute(f"select distinct {COLS} from bars where ticker='{TK}' "
                   f"and date >= '{A}' and date < '{B}' order by date").fetchdf()
    c.close()
    df["ret"] = df["close"].pct_change() * 100
    rng = (df["high"] - df["low"]).replace(0, np.nan)
    df["clo%"] = (df["close"] - df["low"]) / rng * 100
    df["body%"] = (df["close"] - df["open"]).abs() / rng * 100
    df["vx"] = df["volume"] / df["volume"].rolling(20, min_periods=5).mean()
    return df


for lab, path in [("4H", "studio_4h.duckdb"), ("1H", "studio_1h.duckdb")]:
    x = load(path)
    up = x.loc[x["ret"] > 0, "volume"].mean()
    dn = x.loc[x["ret"] < 0, "volume"].mean()
    print(f"\n  --- {lab} · {len(x)} bars ---", flush=True)
    print(f"    up-bar volume {up:,.0f}  vs  down-bar {dn:,.0f}  → {up/max(dn,1):.2f}× "
          f"({'DEMAND' if up > dn else 'SUPPLY'} carries it)", flush=True)
    absb = x[(x["vx"] >= 1.8) & (x["body%"] <= 40) & (x["clo%"] >= 50)]
    print(f"    absorption bars (vol≥1.8× · body≤40% · close upper half): {len(absb)}", flush=True)
    if len(absb):
        print(absb[["date", "close", "volume", "vx", "clo%", "body%", "t_sig", "z_sig",
                    "l_sig"]].to_string(index=False, float_format=lambda z: f"{z:.2f}"), flush=True)
    print(f"    5 highest-volume {lab} bars:", flush=True)
    print(x.nlargest(5, "volume")[["date", "close", "ret", "volume", "vx", "clo%",
                                   "t_sig", "z_sig", "l_sig"]]
          .sort_values("date").to_string(index=False, float_format=lambda z: f"{z:.2f}"), flush=True)

m = load("studio_15m.duckdb")
m["day"] = pd.to_datetime(m["date"]).dt.strftime("%Y-%m-%d")
agg = m.groupby("day").apply(lambda s: pd.Series({
    "vol": s.volume.sum(), "vmax/vavg": s.volume.max() / max(s.volume.mean(), 1),
    "up_vol": s.loc[s["ret"] > 0, "volume"].sum(),
    "dn_vol": s.loc[s["ret"] < 0, "volume"].sum(),
    "lo": s.low.min(), "hi": s.high.max(), "close": s.close.iloc[-1],
}), include_groups=False)
agg["up/dn"] = agg["up_vol"] / agg["dn_vol"].replace(0, np.nan)
print(f"\n  --- 15m by day ---", flush=True)
print(agg[["vol", "vmax/vavg", "up/dn", "lo", "hi", "close"]]
      .to_string(float_format=lambda z: f"{z:,.2f}"), flush=True)
hot = agg[agg["vmax/vavg"] >= 4]
print(f"\n  🏆 15m max/avg ≥4× (our most universal intraday filter): "
      f"{len(hot)}/{len(agg)} days", flush=True)
print("   " + (", ".join(f"{k} ({vv:.1f}×)" for k, vv in hot["vmax/vavg"].items()) or "none"),
      flush=True)

print("\nDONE", flush=True)
