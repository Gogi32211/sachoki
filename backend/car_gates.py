import os,sys,numpy as np,pandas as pd
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
grp,as_of=er._frame(60,3_000_000)
g=grp["CAR"].copy(); g["d"]=pd.to_datetime(g["date"]).dt.strftime("%Y-%m-%d")
last=g.iloc[-1]
print(f"as_of {as_of} · last bar {last['d']} close {last['close']:.2f}\n")
print("=== SUPPRESSORS / GATES on the last bar ===")
checks=[("🏆 RS gate (rs_intact)","rs_intact",True),
        ("❄️ CONSO (absence = −3.67)","conso",True),
        ("🥇 lead-in-lag","lead_in_lag",True),
        ("🌡️ macro VIX-up","macro_vix_up",False),
        ("📐 ADX trend-up","adx_trend_up",False),
        ("🔑 key level","key_level",True),
        ("🧱 OB","ob_ok",True),
        ("⛔ sub-200 rally","sub200_rally",False),
        ("📅 post-earnings ≤5d","earn_post",False),
        ("bias_dn","bias_dn",False),
        ("m15 zdom","m15_zdom",True),
        ("🕐 h1 quiet","h1_quiet",True),
        ("🕐 h1 dual-reclaim","h1_dr",True)]
for lab,c,want_true in checks:
    if c in g:
        v=last[c]
        ok = bool(v) if want_true else (not bool(v))
        print(f"  {'✅' if ok else '❌'} {lab:32s} = {v}")
    else:
        print(f"  ·  {lab:32s} (col absent)")
print("\n=== EMA / structure ===")
for c in ["ema_9","ema_20","ema_50","ema_200","e9","e20","e50","e200"]:
    if c in g: print(f"  {c:8s} {last[c]:.2f}")
print(f"  close   {last['close']:.2f}")
print("\n=== the Feb bottom, bar by bar ===")
cols=[c for c in ["d","open","high","low","close","rsi_14","t","z","l_sig","full_suffix","vol_bucket","bar_gap_class","wyc_phase","rs_intact"] if c in g]
print(g[(g.d>="2026-02-17")&(g.d<="2026-03-02")][cols].to_string(index=False))
print("\n=== last 12 bars ===")
print(g[cols].tail(12).to_string(index=False))
