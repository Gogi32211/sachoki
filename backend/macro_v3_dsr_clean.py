"""The one bar left before any build: does the macro gate improve INDIVIDUAL edges?

The pooled-family DSR was the wrong instrument (a mixed 8-edge pool has a low Sharpe by
construction, so both gated and ungated scored 0.000 against a family of individually
built setups). The right test: take each strong board edge on its own, apply each gate,
and compare SR / DSR / year-stability gated vs ungated — same board family as the
deflation benchmark, 75 pre-specified macro trials.

Gates under test:
  G_vix   VIXY 5d change > +3%  AND NOT vspike   (the new territory, 68% of rising days)
  G_lead  rs_intact (stock strong vs its sector) AND sector 20d-RS vs SPY < -1%
  G_both  both at once
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
    FROM bars WHERE ticker IN ('SPY','VIXY','XLK','XLV','XLF','XLI','XLB','XLY','XLP',
                               'XLU','XLE','XLRE','XLC')
    GROUP BY ticker, date ORDER BY ticker, date
""").fetchdf()
con.close()
P = ET.pivot(index="dt", columns="ticker", values="cl").sort_index()
vix5 = P["VIXY"].pct_change(5)
vixpct = P["VIXY"].rolling(252).apply(lambda x: (x[-1] > x[:-1]).mean(), raw=True)
VSPIKE = set(P.index[((P["VIXY"].pct_change() > 0.05) | (vixpct > 0.80)).fillna(False)])
G_VIX_DAYS = set(P.index[(vix5 > 0.03).fillna(False)]) - VSPIKE
SEC_RS = {etf: (P[etf] / P["SPY"]).pct_change(20) for etf in SEC2ETF.values()}
print(f"G_vix days (rising, NOT vspike): {len(G_VIX_DAYS)}", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}", flush=True)

# per-(ticker,date) rs_intact lookup
rs_lookup = {}
for tkr, gdf in grp.items():
    if "rs_intact" in gdf:
        dd = pd.to_datetime(gdf["date"]).astype(str).to_numpy()
        rs = gdf["rs_intact"].to_numpy()
        for i in range(len(dd)):
            if rs[i]:
                rs_lookup[(tkr, dd[i])] = True
print(f"rs_intact true entries: {len(rs_lookup):,}", flush=True)

EDGES = [("QZC", "E_qzcapit"), ("G3", "E_g3"), ("WSH", "E_washout"),
         ("D+L1", "E_dl1"), ("SPR", "E_spring"), ("⚡G3A", "E_g3abs"),
         ("CAP", "E_t1capbounce"), ("L43", "E_l43triple")]

# board family Sharpes = the deflation benchmark
fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
print(f"board family: {len(fam)} setups\n", flush=True)


def describe(tr):
    if len(tr) < 60:
        return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    r = tr["ret"].to_numpy()
    d = ofs.dsr(r, fam, n_trials=75)
    return dict(n=len(tr), med=tr["ret"].median() * 100,
                pf=(tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf"),
                pos=int((ym > 0).sum()), ny=len(ym), worst=float(ym.min()),
                sr=d["sr"], dsr=d["dsr"], srstar=d["sr_star"])


def line(tag, d):
    if d is None:
        print(f"    {tag:22s} thin", flush=True); return
    print(f"    {tag:22s} n={d['n']:>5d} med{d['med']:>+7.2f} pf{d['pf']:>5.2f} "
          f"{d['pos']}/{d['ny']}yr worst{d['worst']:>+6.2f} SR{d['sr']:>7.4f} "
          f"DSR{d['dsr']:>6.3f}", flush=True)


print(f"{'':4s}{'':22s} (sr* is the same for all: deflation vs the board family)", flush=True)
summary = []
for nm, col in EDGES:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 100:
        continue
    tr["d"] = pd.to_datetime(tr["date_in"]).astype(str)
    tr["etf"] = tr["ticker"].map(lambda t: SEC2ETF.get(SECMAP.get(t, ""), None))
    parts = []
    for etf, sub in tr.groupby("etf"):
        s = SEC_RS.get(etf)
        parts.append(sub.assign(sec_rs=sub["d"].map(s) if s is not None else np.nan))
    tr = pd.concat(parts) if parts else tr.assign(sec_rs=np.nan)
    tr["rs"] = [bool(rs_lookup.get((t, d), False)) for t, d in zip(tr["ticker"], tr["d"])]
    m_vix = tr["d"].isin(G_VIX_DAYS)
    m_lead = tr["rs"] & (tr["sec_rs"] < -0.01)
    print(f"\n##### {nm}", flush=True)
    base = describe(tr); line("ungated", base)
    dv = describe(tr[m_vix]); line("+ G_vix", dv)
    dl = describe(tr[m_lead]); line("+ G_lead", dl)
    db = describe(tr[m_vix & m_lead]); line("+ G_both", db)
    for gname, d in [("G_vix", dv), ("G_lead", dl), ("G_both", db)]:
        if base and d:
            summary.append(dict(edge=nm, gate=gname, d_med=d["med"] - base["med"],
                                d_sr=d["sr"] - base["sr"], d_dsr=d["dsr"] - base["dsr"],
                                d_worst=d["worst"] - base["worst"],
                                yr_up=d["pos"] / d["ny"] - base["pos"] / base["ny"]))

S = pd.DataFrame(summary)
print("\n\n===== SUMMARY: does each gate lift its parent? =====", flush=True)
for gname, sub in S.groupby("gate"):
    print(f"\n  {gname}: tested on {len(sub)} edges", flush=True)
    print(f"    median lift  : {sub['d_med'].median():+.2f}  "
          f"(positive on {int((sub['d_med']>0).sum())}/{len(sub)})", flush=True)
    print(f"    SR lift      : {sub['d_sr'].median():+.4f} "
          f"({int((sub['d_sr']>0).sum())}/{len(sub)})", flush=True)
    print(f"    DSR lift     : {sub['d_dsr'].median():+.3f} "
          f"({int((sub['d_dsr']>0).sum())}/{len(sub)})", flush=True)
    print(f"    worst-yr lift: {sub['d_worst'].median():+.2f} "
          f"({int((sub['d_worst']>0).sum())}/{len(sub)})", flush=True)
S.round(4).to_csv(os.path.join(BASE, "macro_gate_summary.csv"), index=False)
print("\nDONE", flush=True)
