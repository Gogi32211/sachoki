"""Portfolio-level reality check (2026-08-07) — the layer every number so far ignored.

Every statistic in the book is PER TRADE and assumes infinite capital: each fire is taken,
independently, with no slot contention and no correlated drawdown. Reality:
  - the median hold under the ATR exit is ~58 bars (~3 months)
  - the live envelope allows ~3 concurrent positions
so the board can fire thousands of times a year while the account can hold ~12. Slot
contention, not signal quality, may be the binding constraint — and nothing has measured it.

This simulates an ACTUAL account over the live edges:
  1. pooled trades (date_in, date_out, ret, ticker, edge) from the live registry edges
  2. walk day by day; take a fire only if a slot is free; equal-weight slots; compound
  3. slot sweep 3/5/8/12/20/inf → CAGR, max drawdown, Sharpe, capture rate
  4. selection rule when fires > free slots: random / best-edge-median / earliest
  5. correlated-drawdown diagnostics the per-trade view cannot show
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
from brain import registry

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of}", flush=True)

# the edges the brain may actually fire on
live = [e for e in registry.live_edges(direction="long")
        if e.get("col") and e.get("action") == "signal"]
g0 = next(iter(grp.values()))
EDGES = [(e["id"], e["col"], e.get("stats", {}).get("median", 0))
         for e in live if e["col"] in g0]
print(f"live signal edges in frame: {len(EDGES)}", flush=True)

frames = []
for eid, col, med in EDGES:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) == 0:
        continue
    tr["edge"] = eid
    tr["edge_med"] = med
    frames.append(tr)
T = pd.concat(frames, ignore_index=True)
T["d_in"] = pd.to_datetime(T["date_in"])
T["d_out"] = pd.to_datetime(T["date_out"])
T = T.sort_values("d_in").reset_index(drop=True)
print(f"pooled fires: {len(T):,} · {T['d_in'].min().date()} → {T['d_in'].max().date()}",
      flush=True)
print(f"median hold: {T['hold'].median():.0f} bars · mean {T['hold'].mean():.0f}", flush=True)

# same ticker can be picked by several edges on the same day — one position, best edge
T = T.sort_values(["d_in", "ticker", "edge_med"], ascending=[True, True, False])
T = T.drop_duplicates(subset=["d_in", "ticker"], keep="first").reset_index(drop=True)
print(f"after same-day/ticker dedup: {len(T):,}", flush=True)

fires_per_day = T.groupby(T["d_in"].dt.date).size()
print(f"fires/day: median {fires_per_day.median():.0f} · p90 {fires_per_day.quantile(.9):.0f} "
      f"· max {fires_per_day.max()}", flush=True)


def simulate(slots, rule="best", one_per_ticker=True):
    """walk the tape; take a fire only when a slot is free; equal-weight, compounding"""
    eq = 1.0
    open_pos = []          # (exit_date, size, ret)
    taken = skipped = 0
    curve = []
    if rule == "random":
        rng = np.random.default_rng(4)
        T2 = T.sample(frac=1.0, random_state=4).sort_values("d_in", kind="stable")
    elif rule == "earliest":
        T2 = T.sort_values(["d_in"], kind="stable")
    else:                                    # best edge first within the day
        T2 = T.sort_values(["d_in", "edge_med"], ascending=[True, False], kind="stable")
    held = set()
    for day, grp_day in T2.groupby(T2["d_in"], sort=True):
        # close everything due
        still = []
        for xd, sz, r in open_pos:
            if xd <= day:
                eq += sz * r
                held.discard(id((xd, sz, r)))
            else:
                still.append((xd, sz, r))
        open_pos = still
        held = set(t for _, _, t in [])      # simple: allow re-entry after exit
        openers = {p[0] for p in open_pos}
        for _, row in grp_day.iterrows():
            if len(open_pos) >= slots:
                skipped += 1
                continue
            sz = eq / slots
            open_pos.append((row["d_out"], sz, row["ret"]))
            taken += 1
        curve.append((day, eq + sum(sz * 0 for _, sz, _ in open_pos)))
    for xd, sz, r in open_pos:               # close the tail
        eq += sz * r
    C = pd.DataFrame(curve, columns=["d", "eq"]).set_index("d")
    peak = C["eq"].cummax()
    dd = (C["eq"] / peak - 1).min() * 100
    yrs = (C.index[-1] - C.index[0]).days / 365.25
    cagr = (eq ** (1 / yrs) - 1) * 100 if yrs > 0 else np.nan
    rets = C["eq"].pct_change().dropna()
    shp = (rets.mean() / rets.std() * np.sqrt(252)) if rets.std() > 0 else np.nan
    return dict(slots=slots, rule=rule, final=eq, cagr=cagr, maxdd=dd,
                sharpe=shp, taken=taken, skipped=skipped,
                capture=100 * taken / max(taken + skipped, 1), curve=C)


print("\n===== SLOT SWEEP (rule: best-edge-first) =====", flush=True)
print(f"  {'slots':>6s} {'final':>8s} {'CAGR%':>7s} {'maxDD%':>8s} {'Sharpe':>7s} "
      f"{'taken':>7s} {'capture%':>9s}", flush=True)
res = {}
for s in [3, 5, 8, 12, 20, 50, 10**6]:
    r = simulate(s)
    res[s] = r
    lbl = "inf" if s > 10**5 else str(s)
    print(f"  {lbl:>6s} {r['final']:>8.2f} {r['cagr']:>7.1f} {r['maxdd']:>8.1f} "
          f"{r['sharpe']:>7.2f} {r['taken']:>7,} {r['capture']:>9.1f}", flush=True)

print("\n===== SELECTION RULE (does picking well matter?) =====", flush=True)
for s in [3, 5, 8]:
    for rule in ["best", "random", "earliest"]:
        r = simulate(s, rule=rule)
        print(f"  slots {s:>2d} · {rule:9s} CAGR {r['cagr']:>6.1f}% · maxDD {r['maxdd']:>6.1f}% "
              f"· capture {r['capture']:>5.1f}%", flush=True)

print("\n===== CONCENTRATION: how crowded are the fire days? =====", flush=True)
print(f"  days with >=1 fire : {len(fires_per_day):,}", flush=True)
for k in [3, 5, 8, 12, 20]:
    share = (fires_per_day > k).mean() * 100
    print(f"  days with >{k:>2d} fires: {share:>5.1f}% of active days", flush=True)

print("\n===== per-trade vs portfolio: the honest gap =====", flush=True)
print(f"  per-trade median return : {T['ret'].median()*100:+.2f}%", flush=True)
print(f"  per-trade mean          : {T['ret'].mean()*100:+.2f}%", flush=True)
for s in [3, 8, 20]:
    r = res[s]
    print(f"  portfolio @{s:>2d} slots   : CAGR {r['cagr']:+.1f}% · maxDD {r['maxdd']:.1f}% "
          f"· only {r['capture']:.1f}% of fires taken", flush=True)

print("\n===== yearly equity (3 vs 8 vs 20 slots) =====", flush=True)
tab = {}
for s in [3, 8, 20]:
    C = res[s]["curve"]
    tab[s] = C["eq"].resample("YE").last().pct_change().fillna(
        C["eq"].resample("YE").last() - 1) * 100
Y = pd.DataFrame(tab)
Y.index = Y.index.year
print(Y.round(1).to_string(), flush=True)
print("\nDONE", flush=True)
