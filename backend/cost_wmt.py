"""COST / WMT — what does the board actually say, and what does it NOT cover?

Same read as the CAR analysis: which validated edges fired, what the gates say on the last
bar, and the descriptor sequence into today. The extra question these two raise is price:
the whole book was validated in $-buckets and the real edge lives $21-89, so a mega-cap
retailer may sit outside every bucket the edges were built on. That has to be stated, not
buried.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er

TKS = ["COST", "WMT"]
pd.set_option("display.width", 240)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}\n", flush=True)

for TK in TKS:
    g = grp.get(TK)
    print("=" * 96, flush=True)
    if g is None:
        print(f"{TK}: NOT in the frame (below the $3M dollar-volume floor?)", flush=True)
        continue
    g = g.copy()
    g["d"] = pd.to_datetime(g["date"]).dt.strftime("%Y-%m-%d")
    last = g.iloc[-1]
    print(f"{TK}  ·  {len(g)} bars  ·  last {last['d']}  close ${last['close']:.2f}", flush=True)
    print("=" * 96, flush=True)

    # ── where does it sit vs the buckets the book was validated in ─────────────
    px = float(last["close"])
    bucket = ("$5-21" if px < 21 else "$21-89" if px < 89 else
              "$89-377" if px < 377 else "ABOVE $377 — outside every tested bucket")
    print(f"\n  price bucket: {bucket}", flush=True)
    hi = g["close"].max()
    print(f"  52w-ish: low ${g['close'].tail(252).min():.2f}  high ${g['close'].tail(252).max():.2f}"
          f"  ·  now {(px/g['close'].tail(252).max()-1)*100:+.1f}% off the high", flush=True)

    # ── which validated edges have EVER fired on this name ─────────────────────
    print("\n  --- edges that fired in the last 12 months ---", flush=True)
    recent = g[g["d"] >= "2025-08-08"]
    fires = []
    for name, col in er.SETUPS:
        if col not in recent:
            continue
        m = recent[col].fillna(False).astype(bool)
        for i in np.where(m.to_numpy())[0]:
            fires.append((recent["d"].iloc[i], name, float(recent["close"].iloc[i]),
                          float(recent["rsi_14"].iloc[i])))
    if fires:
        F = pd.DataFrame(fires, columns=["date", "edge", "close", "rsi"]).sort_values("date")
        for d_, sub in F.groupby("date"):
            print(f"    {d_}  ${sub.close.iloc[0]:>8.2f}  rsi {sub.rsi.iloc[0]:>5.1f}  "
                  f"→ {' · '.join(sorted(sub.edge))}", flush=True)
        print(f"    total: {len(F)} fires on {F.date.nunique()} bars", flush=True)
    else:
        print("    NONE — the board has been silent on this name for 12 months", flush=True)

    # ── 5-year fire count, to show whether the book covers this name at all ────
    tot = sum(int(g[c].fillna(False).sum()) for _, c in er.SETUPS if c in g)
    bars5 = len(g)
    print(f"\n  5-year total fires: {tot} across {bars5} bars "
          f"({tot/max(bars5,1)*100:.1f} per 100 bars)", flush=True)

    # ── the last bar's state ───────────────────────────────────────────────────
    print("\n  --- state on the last bar ---", flush=True)
    for c in ["rsi_14", "cci_20", "t", "z", "l_sig", "full_suffix", "vol_bucket",
              "conso", "rs_intact", "lead_in_lag", "adx_regime", "wyc_phase",
              "buy_score", "ultra_score_v3", "sector"]:
        if c in g:
            print(f"    {c:16s} {last[c]}", flush=True)
    atrp = float(last["atr_14"]) / px * 100
    print(f"    ATR%             {atrp:.2f}   → ⚡ATR×12 trail = "
          f"{min(max(12*atrp,15),60):.1f}%", flush=True)

    # ── gates ──────────────────────────────────────────────────────────────────
    print("\n  --- gates on the last bar ---", flush=True)
    for lab, c, want in [("🏆 RS", "rs_intact", True), ("❄️ CONSO", "conso", True),
                         ("🥇 lead-in-lag", "lead_in_lag", True),
                         ("🌡️ VIX-up", "macro_vix_up", False),
                         ("📐 ADX trend-up", "adx_trend_up", False),
                         ("🕐 h1 dual-reclaim", "h1_dr", True),
                         ("🕐 h1 quiet", "h1_quiet", True),
                         ("m15 Z-dom", "m15_zdom", True)]:
        if c in g:
            v = bool(last[c])
            print(f"    {'✅' if (v == want) else '❌'} {lab:22s} = {v}", flush=True)

    # ── the last 10 bars, full descriptor ──────────────────────────────────────
    cols = [c for c in ["d", "open", "high", "low", "close", "rsi_14", "t", "z", "l_sig",
                        "full_suffix", "vol_bucket"] if c in g]
    print("\n  --- last 10 bars ---", flush=True)
    print(g[cols].tail(10).to_string(index=False), flush=True)
    print(flush=True)

print("DONE", flush=True)
