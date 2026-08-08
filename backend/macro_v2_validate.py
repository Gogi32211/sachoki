"""Macro finding — validation + the user's two-level-RS question (2026-08-06).

Part A (the honesty checks the M2 finding needs before any gate is proposed):
  A1 overlap with the EXISTING gate_vspike — is "rising VIX" new information at all?
  A2 marginal contribution: does M2 still pay INSIDE and OUTSIDE the vspike state?
  A3 are M2 (VIX↑), M3 (small-caps lagging) and M5 (sector lagging) one factor or three?
     (pairwise co-occurrence + the 3-way cell)
  A4 DSR of the best gated cell against the 75 pre-specified macro trials

Part B (user's question): TWO-LEVEL RS. Our 🏆RS gate wants the STOCK strong vs its
sector; the M5 result wants the SECTOR weak vs SPY. Both at once = "leader inside a
laggard group" — a configuration the book has never tested. Four quadrants measured.
Also: ticker-vs-QQQ as an extra benchmark arm for the nasdaq names.
"""
import os, sys, json
import numpy as np
import pandas as pd
import duckdb
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB
import edge_replay as er
import overfit_stats as ofs

BASE = os.path.dirname(os.path.abspath(__file__))
SEC2ETF = {"Technology": "XLK", "Healthcare": "XLV", "Financials": "XLF",
           "Industrials": "XLI", "Materials": "XLB", "Consumer Discretionary": "XLY",
           "Consumer Staples": "XLP", "Utilities": "XLU", "Energy": "XLE",
           "Real Estate": "XLRE", "Communication Services": "XLC"}
SECMAP = {k: v for k, v in json.load(
    open("/Users/sachoki/Desktop/sachoki-desktop/data/sector_map.json")).items() if v}

con = duckdb.connect(ANALYTICS_DB, read_only=True)
ET = con.execute("""
    SELECT ticker, CAST(date AS VARCHAR) dt, any_value("close") AS cl
    FROM bars WHERE ticker IN ('SPY','QQQ','IWM','VIXY','XLK','XLV','XLF','XLI','XLB',
                               'XLY','XLP','XLU','XLE','XLRE','XLC')
    GROUP BY ticker, date ORDER BY ticker, date
""").fetchdf()
NASDAQ = set(r[0] for r in con.execute(
    "SELECT DISTINCT ticker FROM bars WHERE universe='nasdaq'").fetchall())
con.close()
P = ET.pivot(index="dt", columns="ticker", values="cl").sort_index()

M = pd.DataFrame(index=P.index)
M["vix_chg5"] = P["VIXY"].pct_change(5)
M["vix_pct"] = P["VIXY"].rolling(252).apply(lambda x: (x[-1] > x[:-1]).mean(), raw=True)
M["iwm_rel"] = (P["IWM"] / P["SPY"]).pct_change(20)
for sec, etf in SEC2ETF.items():
    M[f"sec_{etf}"] = (P[etf] / P["SPY"]).pct_change(20)
M["qqq_rel"] = (P["QQQ"] / P["SPY"]).pct_change(20)

RISING = set(M.index[(M["vix_chg5"] > 0.03).fillna(False)])
SMALL_LAG = set(M.index[(M["iwm_rel"] < -0.01).fillna(False)])
# the book's existing vspike definition: VIX-panic proxy — VIXY 1d jump or elevated pct
VSPIKE = set(M.index[((P["VIXY"].pct_change() > 0.05) | (M["vix_pct"] > 0.80)).fillna(False)])
print(f"days: rising-VIX {len(RISING)} · small-lag {len(SMALL_LAG)} · vspike {len(VSPIKE)}",
      flush=True)
print(f"A1 OVERLAP rising∩vspike = {100*len(RISING & VSPIKE)/max(len(RISING),1):.1f}% of rising days "
      f"({100*len(RISING & VSPIKE)/max(len(VSPIKE),1):.1f}% of vspike days)", flush=True)
print(f"A3 rising∩small-lag = {100*len(RISING & SMALL_LAG)/max(len(RISING),1):.1f}%", flush=True)

grp, as_of = er._frame(60, 3_000_000)
REV = ["E_qzcapit", "E_washout", "E_dl1", "E_g3", "E_zoneretest", "E_atomic",
       "E_spring", "E_g3abs"]
frames = []
for c in REV:
    tr = er._pathsim(grp, c, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    tr["edge"] = c
    frames.append(tr)
T = pd.concat(frames, ignore_index=True)
T["d"] = pd.to_datetime(T["date_in"]).astype(str)
print(f"REV trades {len(T):,}", flush=True)


def show(sub, label):
    if len(sub) < 150:
        print(f"  {label:44s} n={len(sub)} thin", flush=True); return None
    ym = sub.groupby("yr")["ret"].median() * 100
    w = sub["ret"] > 0
    den = -sub.loc[~w, "ret"].sum()
    pf = (sub.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    print(f"  {label:44s} n={len(sub):>6d} med{sub['ret'].median()*100:>+7.2f} "
          f"win{w.mean()*100:>5.1f} pf{pf:>5.2f} {int((ym>0).sum())}/{len(ym)}yr "
          f"worst{ym.min():>+6.2f}", flush=True)
    return sub


print("\n===== A2. marginal contribution of rising-VIX INSIDE/OUTSIDE vspike =====", flush=True)
inv = T["d"].isin(VSPIKE); inr = T["d"].isin(RISING)
show(T, "ALL")
show(T[inv], "vspike ON")
show(T[~inv], "vspike OFF")
show(T[inv & inr], "  vspike ON  + VIX rising")
show(T[inv & ~inr], "  vspike ON  + VIX not rising")
show(T[~inv & inr], "  vspike OFF + VIX rising")
show(T[~inv & ~inr], "  vspike OFF + VIX not rising")

print("\n===== A3. are the three macro dims one factor? =====", flush=True)
ins = T["d"].isin(SMALL_LAG)
show(T[inr & ins], "VIX rising AND small-caps lagging")
show(T[inr & ~ins], "VIX rising, small-caps NOT lagging")
show(T[~inr & ins], "VIX flat/down, small-caps lagging")

print("\n===== B. TWO-LEVEL RS: stock vs sector × sector vs SPY =====", flush=True)
T["etf"] = T["ticker"].map(lambda t: SEC2ETF.get(SECMAP.get(t, ""), None))
TB = T[T["etf"].notna()].copy()
secrs = []
for etf, sub in TB.groupby("etf"):
    s = M.get(f"sec_{etf}")
    secrs.append(sub.assign(sec_rs=sub["d"].map(s)))
TB = pd.concat(secrs).dropna(subset=["sec_rs"])
# stock-level RS: reuse the frame's own rs_intact at the signal bar
rs_lookup = {}
for tkr, gdf in grp.items():
    if "rs_intact" in gdf:
        dd = pd.to_datetime(gdf["date"]).astype(str).to_numpy()
        rs_lookup.update({(tkr, dd[i]): bool(v)
                          for i, v in enumerate(gdf["rs_intact"].to_numpy())})
TB["rs"] = [rs_lookup.get((t, d), None) for t, d in zip(TB["ticker"], TB["d"])]
TB = TB[TB["rs"].notna()]
print(f"  (n with both levels: {len(TB):,})", flush=True)
for rs_v, rs_l in [(True, "stock STRONG vs sector"), (False, "stock weak vs sector")]:
    for lag, lab in [(True, "sector LAGGING SPY"), (False, "sector leading SPY")]:
        sel = TB[(TB["rs"] == rs_v) & ((TB["sec_rs"] < -0.01) == lag)]
        show(sel, f"{rs_l} × {lab}")

print("\n===== B2. ticker vs QQQ (nasdaq names only) =====", flush=True)
TQ = T[T["ticker"].isin(NASDAQ)].copy()
TQ["qrel"] = TQ["d"].map(M["qqq_rel"])
TQ = TQ.dropna(subset=["qrel"])
show(TQ[TQ["qrel"] < -0.01], "nasdaq names · QQQ lagging SPY")
show(TQ[TQ["qrel"] > 0.01], "nasdaq names · QQQ leading SPY")

print("\n===== A4. DSR of the best gated cell (75 pre-specified macro trials) =====", flush=True)
fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
best = T[inr]
d = ofs.dsr(best["ret"].to_numpy(), fam, n_trials=75)
print(f"  REV × VIX-rising  SR {d['sr']:.4f} · sr* {d['sr_star']:.4f} · DSR {d['dsr']:.3f}",
      flush=True)
d0 = ofs.dsr(T["ret"].to_numpy(), fam, n_trials=75)
print(f"  REV ungated       SR {d0['sr']:.4f} · DSR {d0['dsr']:.3f}", flush=True)
print("\nDONE", flush=True)
