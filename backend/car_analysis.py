"""CAR (Avis Budget) from 2026-02-13 — read the squeeze through the validated book.

n=1, and this is the single most survivorship-loaded shape there is (a 6x short squeeze).
So the question is NOT "does this pattern work" — it is: which of our BUILT edges actually
fired, at which bar, and what would the ⚡ATR x12 exit have taken out of it. Anything the
book missed is stated as missed.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er

TK, START = "CAR", "2026-02-13"
pd.set_option("display.width", 220)

grp, as_of = er._frame(60, 3_000_000)  # canonical pinned frame — CAR clears $3M easily
g = grp.get(TK)
print(f"frame as_of {as_of} · CAR bars {0 if g is None else len(g)}", flush=True)
g = g.copy()
g["d"] = pd.to_datetime(g["date"]).dt.strftime("%Y-%m-%d")
s = g[g["d"] >= START].reset_index(drop=True)

# ── 1. the path ────────────────────────────────────────────────────────────────
print("\n===== 1. what happened =====", flush=True)
print(f"  {s.d.iloc[0]} close {s.close.iloc[0]:.2f}  →  {s.d.iloc[-1]} close {s.close.iloc[-1]:.2f}"
      f"   ({(s.close.iloc[-1]/s.close.iloc[0]-1)*100:+.1f}%)", flush=True)
lo_i, hi_i = s.low.idxmin(), s.high.idxmax()
print(f"  LOW  {s.low[lo_i]:.2f} on {s.d[lo_i]}   ·   HIGH {s.high[hi_i]:.2f} on {s.d[hi_i]}"
      f"   = {s.high[hi_i]/s.low[lo_i]:.1f}x in {hi_i-lo_i} bars", flush=True)

# ── 2. which BUILT edges fired ─────────────────────────────────────────────────
print("\n===== 2. which validated edges fired on CAR since 2026-02-13 =====", flush=True)
fires = []
for name, col in er.SETUPS:
    if col not in s:
        continue
    m = s[col].fillna(False).astype(bool)
    for i in np.where(m.to_numpy())[0]:
        fires.append((s.d[i], name, float(s.close[i]), float(s.rsi_14[i]) if "rsi_14" in s else np.nan))
if fires:
    F = pd.DataFrame(fires, columns=["date", "edge", "close", "rsi"]).sort_values("date")
    for d_, sub in F.groupby("date"):
        print(f"  {d_}  close {sub.close.iloc[0]:>7.2f}  rsi {sub.rsi.iloc[0]:>5.1f}  "
              f"→ {' · '.join(sorted(sub.edge))}", flush=True)
    print(f"\n  total fires: {len(F)} on {F.date.nunique()} distinct bars", flush=True)
else:
    print("  NONE — the whole board stayed silent on this move", flush=True)

# ── 3. what would the ⚡ATR exit have made of each fire ─────────────────────────
print("\n===== 3. path-sim of those fires (⚡ATR×12 trail, real costs) =====", flush=True)
rows = []
for name, col in er.SETUPS:
    if col not in g:
        continue
    if not g[col].fillna(False).any():
        continue
    tr = er._pathsim({TK: g}, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) == 0:
        continue
    tr = tr[pd.to_datetime(tr["date_in"]).astype(str).str[:10] >= START]
    for _, r in tr.iterrows():
        rows.append((str(r["date_in"])[:10], name, r["ret"] * 100, r.get("bars", np.nan),
                     r.get("exit", "")))
if rows:
    R = pd.DataFrame(rows, columns=["entry", "edge", "ret%", "bars", "exit"]).sort_values("entry")
    print(R.to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)
    print(f"\n  median {R['ret%'].median():+.2f}%  ·  mean {R['ret%'].mean():+.2f}%  "
          f"·  best {R['ret%'].max():+.2f}%  ·  worst {R['ret%'].min():+.2f}%", flush=True)
else:
    print("  no completed trades in the window", flush=True)

# ── 4. full-descriptor read of the key bars ────────────────────────────────────
print("\n===== 4. full-descriptor read of the turning points =====", flush=True)
COLS = ["d", "close", "rsi_14", "t", "z", "l_sig", "full_suffix", "bar_gap_class",
        "vol_bucket", "conso", "rs_intact", "wyc_phase"]
COLS = [c for c in COLS if c in s]
key = pd.concat([
    s.iloc[max(0, lo_i - 2):lo_i + 3],                    # the Feb bottom
    s[(s.d >= "2026-03-02") & (s.d <= "2026-03-06")],     # the launch
    s.iloc[max(0, hi_i - 2):hi_i + 3],                    # the blow-off
    s.tail(5),                                            # now
]).drop_duplicates(subset="d")
print(key[COLS].to_string(index=False), flush=True)

# ── 5. where does it stand today ───────────────────────────────────────────────
print("\n===== 5. state on the last bar =====", flush=True)
last = s.iloc[-1]
for c in ["close", "rsi_14", "cci_20", "atr_14", "t", "z", "l_sig", "full_suffix",
          "vol_bucket", "conso", "rs_intact", "lead_in_lag", "adx", "adx_regime",
          "wyc_phase", "buy_score", "ultra_score_v3", "sector"]:
    if c in s:
        print(f"  {c:16s} {last[c]}", flush=True)
atrp = float(last["atr_14"]) / float(last["close"]) * 100
print(f"  ATR%             {atrp:.2f}   → ⚡ATR×12 trail would be "
      f"{min(max(12*atrp, 15), 60):.1f}%", flush=True)
print(f"  off the April high: {(float(last['close'])/float(s.high[hi_i])-1)*100:.1f}%", flush=True)

print("\nDONE", flush=True)
