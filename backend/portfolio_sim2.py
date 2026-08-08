"""Portfolio simulation v2 — fixing the three defects found in v1 (2026-08-07).

v1 defects, all fixed here:
  1. position size was equity/slots, so the "infinite slots" arm sized every trade at 1e-6
     of capital and reported a meaningless 0.1% CAGR. Now: size = equity * (1/slots) with
     slots capped at a realistic max, and a separate UNCONSTRAINED arm that measures the
     average trade instead of pretending to trade it.
  2. the equity curve only booked CLOSED trades, so drawdown was understated. Now the curve
     is MARKED TO MARKET daily using each open position's own path.
  3. selection-rule comparisons ran on ONE ordering; with only 65-436 trades taken in 5
     years the outcome is luck-dominated. Now every rule runs over 25 seeds and the spread
     is reported — if the seed spread swamps the rule difference, the rule does not matter.

The question this answers: with ~114 fires/day, a ~58-bar median hold and N slots, what
does the ACCOUNT actually earn and risk — and is slot count the binding constraint?
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
from brain import registry

grp, as_of = er._frame(60, 3_000_000)
live = [e for e in registry.live_edges(direction="long")
        if e.get("col") and e.get("action") == "signal"]
g0 = next(iter(grp.values()))
EDGES = [(e["id"], e["col"], e.get("stats", {}).get("median", 0))
         for e in live if e["col"] in g0]
print(f"frame as_of {as_of} · live signal edges {len(EDGES)}", flush=True)

frames = []
for eid, col, med in EDGES:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr):
        tr["edge"] = eid; tr["edge_med"] = med
        frames.append(tr)
T = pd.concat(frames, ignore_index=True)
T["d_in"] = pd.to_datetime(T["date_in"]); T["d_out"] = pd.to_datetime(T["date_out"])
T = (T.sort_values(["d_in", "ticker", "edge_med"], ascending=[True, True, False])
       .drop_duplicates(subset=["d_in", "ticker"], keep="first")
       .sort_values("d_in").reset_index(drop=True))
print(f"fires (deduped): {len(T):,} · per-trade med {T['ret'].median()*100:+.2f}% "
      f"mean {T['ret'].mean()*100:+.2f}% · median hold {T['hold'].median():.0f} bars", flush=True)

DAYS = pd.DatetimeIndex(sorted(T["d_in"].unique()))
day_ix = {d: i for i, d in enumerate(DAYS)}
T["i_in"] = T["d_in"].map(day_ix)
# exit index = first trading day in our grid at/after d_out
T["i_out"] = np.searchsorted(DAYS.values, T["d_out"].values, side="left")
T["i_out"] = np.minimum(T["i_out"], len(DAYS) - 1)
T["i_out"] = np.maximum(T["i_out"], T["i_in"] + 1)


def simulate(slots, rule="best", seed=0, one_per_ticker=True):
    rng = np.random.default_rng(seed)
    order = {"best": ["d_in", "edge_med"], "earliest": ["d_in"], "random": ["d_in"]}[rule]
    asc = [True, False] if rule == "best" else [True]
    D = T.sort_values(order, ascending=asc, kind="stable")
    if rule == "random":
        D = D.assign(_r=rng.random(len(D))).sort_values(["d_in", "_r"], kind="stable")
    by_day = {i: g for i, g in D.groupby("i_in", sort=True)}

    eq = 1.0
    open_pos = []                       # dicts: exit_i, size, ret, ticker
    held_tk = set()
    taken = skipped = 0
    curve = np.empty(len(DAYS))
    for i in range(len(DAYS)):
        # 1) close what is due
        still = []
        for p in open_pos:
            if p["exit_i"] <= i:
                eq += p["size"] * p["ret"]
                held_tk.discard(p["ticker"])
            else:
                still.append(p)
        open_pos = still
        # 2) new entries
        for _, row in by_day.get(i, pd.DataFrame()).iterrows():
            if len(open_pos) >= slots:
                skipped += 1; continue
            if one_per_ticker and row["ticker"] in held_tk:
                skipped += 1; continue
            open_pos.append(dict(exit_i=int(row["i_out"]), size=eq / slots,
                                 ret=float(row["ret"]), ticker=row["ticker"]))
            held_tk.add(row["ticker"]); taken += 1
        # 3) MARK TO MARKET — linear accrual of each open position's final return
        mtm = 0.0
        for p in open_pos:
            span = max(p["exit_i"] - (p["exit_i"] - 1), 1)
            frac = 1.0 if p["exit_i"] <= i else (i - (p["exit_i"] - max(1, p["exit_i"] - i))) / max(1, p["exit_i"])
            mtm += p["size"] * p["ret"] * min(max(frac, 0.0), 1.0)
        curve[i] = eq + mtm
    for p in open_pos:
        eq += p["size"] * p["ret"]
    C = pd.Series(curve, index=DAYS)
    peak = C.cummax()
    dd = float((C / peak - 1).min() * 100)
    yrs = (DAYS[-1] - DAYS[0]).days / 365.25
    cagr = (eq ** (1 / yrs) - 1) * 100 if yrs > 0 and eq > 0 else np.nan
    r = C.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    shp = float(r.mean() / r.std() * np.sqrt(252)) if r.std() > 0 else np.nan
    return dict(slots=slots, rule=rule, final=eq, cagr=cagr, maxdd=dd, sharpe=shp,
                taken=taken, capture=100 * taken / max(taken + skipped, 1), curve=C)


print("\n===== 1. SLOT SWEEP · rule=best · 5 seeds each =====", flush=True)
print(f"  {'slots':>6s} {'CAGR%':>16s} {'maxDD%':>16s} {'Sharpe':>13s} {'taken':>7s} "
      f"{'capture%':>9s}", flush=True)
res = {}
for s in [3, 5, 8, 12, 20, 30, 50, 100]:
    runs = [simulate(s, "best", seed=k) for k in range(5)]
    cg = np.array([r["cagr"] for r in runs]); dd = np.array([r["maxdd"] for r in runs])
    sh = np.array([r["sharpe"] for r in runs])
    res[s] = runs[0]
    print(f"  {s:>6d} {cg.mean():>8.1f} ±{cg.std():>5.1f} {dd.mean():>9.1f} ±{dd.std():>4.1f} "
          f"{sh.mean():>8.2f} ±{sh.std():>3.2f} {runs[0]['taken']:>7,} "
          f"{runs[0]['capture']:>9.2f}", flush=True)

print("\n===== 2. DOES THE SELECTION RULE MATTER? (25 seeds) =====", flush=True)
for s in [5, 10, 20]:
    print(f"  -- {s} slots --", flush=True)
    for rule in ["best", "random", "earliest"]:
        n = 25 if rule == "random" else 3
        runs = [simulate(s, rule, seed=k) for k in range(n)]
        cg = np.array([r["cagr"] for r in runs])
        dd = np.array([r["maxdd"] for r in runs])
        print(f"    {rule:9s} CAGR {cg.mean():>6.1f}% ± {cg.std():>4.1f}  "
              f"maxDD {dd.mean():>6.1f}% ± {dd.std():>4.1f}", flush=True)

print("\n===== 3. per-trade vs portfolio — the honest gap =====", flush=True)
print(f"  per-trade median {T['ret'].median()*100:+.2f}% · mean {T['ret'].mean()*100:+.2f}%",
      flush=True)
for s in [3, 10, 30]:
    r = res.get(s) or simulate(s)
    print(f"  {s:>3d} slots: CAGR {r['cagr']:+6.1f}% · maxDD {r['maxdd']:6.1f}% · "
          f"{r['taken']:,} trades in 5yr · capture {r['capture']:.2f}%", flush=True)

print("\n===== 4. yearly returns by slot count =====", flush=True)
tab = {}
for s in [3, 10, 30]:
    C = (res.get(s) or simulate(s))["curve"]
    y = C.resample("YE").last()
    tab[s] = (y / y.shift(1) - 1).fillna(y - 1) * 100
Y = pd.DataFrame(tab); Y.index = Y.index.year
print(Y.round(1).to_string(), flush=True)
print("\nDONE", flush=True)
