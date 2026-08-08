"""Macro / sector-rotation conditioning of the EXISTING edge book (2026-08-06).

New axis, not a new pattern. Every gate we own is either single-ticker internal (RSI,
RS, volume, location) or calendar (season, earnings). NONE is cross-asset. This asks a
question the book has never asked: WHEN, in macro terms, do our edges pay?

Five PRE-SPECIFIED dimensions, each cut into 3 point-in-time states (trailing windows
only — no full-sample percentiles, which would be lookahead):
  M1 VIX level      VIXY close vs its own trailing 252d percentile      low/mid/high
  M2 VIX direction  VIXY 5d change                                      falling/flat/rising
  M3 small-cap lead IWM/SPY 20d change                                  lagging/flat/leading
  M4 defensives     (XLU+XLP)/2 vs SPY, 20d change                      risk-on/flat/risk-off
  M5 sector RS      the stock's OWN sector ETF vs SPY, 20d change       lagging/flat/leading

Tested on the pooled reversal family and four individual flagship edges, on the book's
current default exit (⚡ATR×12). Trial count fixed in advance and printed.
"""
import os, sys, json
import numpy as np
import pandas as pd
import duckdb
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from studio.paths import ANALYTICS_DB
import edge_replay as er

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
    FROM bars WHERE ticker IN ('SPY','QQQ','IWM','VIXY','GLD','XLK','XLV','XLF','XLI',
                               'XLB','XLY','XLP','XLU','XLE','XLRE','XLC')
    GROUP BY ticker, date ORDER BY ticker, date
""").fetchdf()
con.close()
P = ET.pivot(index="dt", columns="ticker", values="cl").sort_index()
print(f"macro frame: {len(P)} days {P.index[0][:10]} → {P.index[-1][:10]}", flush=True)


def pct_rank_trailing(s, w=252):
    return s.rolling(w).apply(lambda x: (x[-1] > x[:-1]).mean(), raw=True)


M = pd.DataFrame(index=P.index)
M["vix_pct"] = pct_rank_trailing(P["VIXY"])
M["vix_chg5"] = P["VIXY"].pct_change(5)
M["iwm_rel"] = (P["IWM"] / P["SPY"]).pct_change(20)
M["def_rel"] = (((P["XLU"] + P["XLP"]) / 2) / P["SPY"]).pct_change(20)
for sec, etf in SEC2ETF.items():
    if etf in P:
        M[f"sec_{etf}"] = (P[etf] / P["SPY"]).pct_change(20)

STATES = {
    "M1 VIX level":     [("low",     lambda m: m["vix_pct"] < 0.33),
                         ("mid",     lambda m: m["vix_pct"].between(0.33, 0.67)),
                         ("high",    lambda m: m["vix_pct"] > 0.67)],
    "M2 VIX direction": [("falling", lambda m: m["vix_chg5"] < -0.03),
                         ("flat",    lambda m: m["vix_chg5"].between(-0.03, 0.03)),
                         ("rising",  lambda m: m["vix_chg5"] > 0.03)],
    "M3 small-cap":     [("lagging", lambda m: m["iwm_rel"] < -0.01),
                         ("flat",    lambda m: m["iwm_rel"].between(-0.01, 0.01)),
                         ("leading", lambda m: m["iwm_rel"] > 0.01)],
    "M4 defensives":    [("risk-on", lambda m: m["def_rel"] < -0.01),
                         ("flat",    lambda m: m["def_rel"].between(-0.01, 0.01)),
                         ("risk-off",lambda m: m["def_rel"] > 0.01)],
}
EDGES = [("REV family", ["E_qzcapit", "E_washout", "E_dl1", "E_g3", "E_zoneretest",
                         "E_atomic", "E_spring", "E_g3abs"]),
         ("QZC", ["E_qzcapit"]), ("WSH", ["E_washout"]),
         ("G3", ["E_g3"]), ("ATM", ["E_atomic"])]
N_TRIALS = sum(len(v) for v in STATES.values()) * len(EDGES) + 3 * len(EDGES)
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS}\n", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"replay frame as_of {as_of}", flush=True)

# per-day macro state lookup + per-ticker sector RS
day_state = {}
for dim, opts in STATES.items():
    for nm, fn in opts:
        day_state[(dim, nm)] = set(M.index[fn(M).fillna(False)])
SEC_RS = {}
for sec, etf in SEC2ETF.items():
    col = f"sec_{etf}"
    if col in M:
        SEC_RS[etf] = M[col]

trades = {}
for lab, cols in EDGES:
    frames = []
    for c in cols:
        tr = er._pathsim(grp, c, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
        frames.append(tr)
    T = pd.concat(frames, ignore_index=True)
    T["d"] = pd.to_datetime(T["date_in"]).astype(str)
    # entry-day macro state is knowable at the SIGNAL bar; use the prior session's row
    T["sig_d"] = T["d"]
    trades[lab] = T
    print(f"  {lab:12s} trades {len(T):,}", flush=True)


def block(T, label):
    ym0 = T.groupby("yr")["ret"].median() * 100
    base = T["ret"].median() * 100
    print(f"\n===== {label} · all n={len(T):,} med{base:+.2f} "
          f"{int((ym0>0).sum())}/{len(ym0)}yr =====", flush=True)
    for dim, opts in STATES.items():
        line = []
        for nm, _ in opts:
            days = day_state[(dim, nm)]
            sub = T[T["sig_d"].isin(days)]
            if len(sub) < 200:
                line.append(f"{nm}: thin"); continue
            ym = sub.groupby("yr")["ret"].median() * 100
            line.append(f"{nm}: n{len(sub):>6d} med{sub['ret'].median()*100:>+6.2f} "
                        f"({int((ym>0).sum())}/{len(ym)}, w{ym.min():>+5.1f})")
        print(f"  {dim:18s} " + " | ".join(line), flush=True)
    # sector RS
    T2 = T.copy()
    T2["etf"] = T2["ticker"].map(lambda t: SEC2ETF.get(SECMAP.get(t, ""), None))
    T2 = T2[T2["etf"].notna()]
    if len(T2) > 500:
        vals = []
        for etf, sub in T2.groupby("etf"):
            s = SEC_RS.get(etf)
            if s is None:
                continue
            vals.append(sub.assign(rs=sub["sig_d"].map(s)))
        if vals:
            V = pd.concat(vals).dropna(subset=["rs"])
            out = []
            for nm, msk in [("lagging", V["rs"] < -0.01), ("flat", V["rs"].between(-0.01, 0.01)),
                            ("leading", V["rs"] > 0.01)]:
                sub = V[msk]
                if len(sub) < 200:
                    out.append(f"{nm}: thin"); continue
                ym = sub.groupby("yr")["ret"].median() * 100
                out.append(f"{nm}: n{len(sub):>6d} med{sub['ret'].median()*100:>+6.2f} "
                           f"({int((ym>0).sum())}/{len(ym)}, w{ym.min():>+5.1f})")
            print(f"  {'M5 sector RS':18s} " + " | ".join(out), flush=True)


for lab, T in trades.items():
    block(T, lab)
print("\nDONE", flush=True)
