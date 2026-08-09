"""A5 — two survivorship channels, measured separately. (2026-08-09)

The first version of this script measured the wrong thing: it derived "dead" tickers from
the FRAME, where a name disappears the moment it falls below $5 or $3M daily turnover. That
is not delisting, it is filtering — and confusing the two produced 26% "deaths" against the
0.7% the raw database actually shows.

Correcting it exposed something more important than the original question. The frame filter
in edge_replay._pull is applied PER BAR:

    WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= dv_floor

So a company that keeps falling drops OUT of the frame as it falls. Our edges buy oversold,
beaten-down names; when such a name goes $6 → $4 → $2, the $4 and $2 bars are simply absent,
_pathsim runs out of bars, and the trade closes at the last one that survived the filter.

Losses get truncated. Gains never do — a name rising from $6 to $60 stays fully in view.

So this measures two channels:

  CHANNEL 1 · DELISTING     — from the RAW database, where filtering cannot confuse it.
  CHANNEL 2 · PATH TRUNCATION — how many simulated trades hit a gap or the end of their
                                ticker's series before their exit rule fired, and what that
                                does to the medians we quote.

Channel 2 is the one that touches every number in the book, because it operates inside every
study rather than at the edge of the dataset.
"""
import os
import sys

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er            # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "studio_analytics.duckdb")
FAIL_LOW, FAIL_HIGH = 0.075, 0.14        # stated assumption, not a measurement
pd.set_option("display.width", 210)

print(f"{'='*92}\nA5 — SURVIVORSHIP, TWO CHANNELS\n{'='*92}", flush=True)

# ══ CHANNEL 1 · delisting, from the RAW database ═════════════════════════════
print("\n### CHANNEL 1 — delisting (raw DB, no filters)\n", flush=True)
con = duckdb.connect(DB, read_only=True)
gmax = pd.Timestamp(con.execute("SELECT max(date) FROM bars").fetchone()[0])
life = con.execute("""
    SELECT ticker, max(date) last_bar, min(date) first_bar, count(*) n
    FROM bars GROUP BY ticker
""").fetchdf()
life["stale"] = (gmax - pd.to_datetime(life.last_bar)).dt.days
n_all = len(life)
dead = life[life.stale > 90]
print(f"  tickers in the raw DB: {n_all:,}", flush=True)
print(f"  ending >90d before {gmax.date()}: {len(dead):,} = {len(dead)/n_all:.2%}", flush=True)
print(f"  ASSUMED true failure-delisting over 5y: {FAIL_LOW:.1%}-{FAIL_HIGH:.1%} "
      f"= {int(n_all*FAIL_LOW):,}-{int(n_all*FAIL_HIGH):,} names", flush=True)
miss_lo = max(0, int(n_all * FAIL_LOW) - len(dead))
miss_hi = max(0, int(n_all * FAIL_HIGH) - len(dead))
print(f"  ⇒ MISSING ≈ {miss_lo:,}-{miss_hi:,} names "
      f"({miss_lo/n_all:.1%}-{miss_hi/n_all:.1%} of the universe)", flush=True)
con.close()

# ══ CHANNEL 2 · path truncation inside the frame ═════════════════════════════
print("\n### CHANNEL 2 — path truncation (the one that touches every study)\n", flush=True)
grp, as_of = er._frame(60, 3_000_000)
print(f"  frame as_of {as_of} · {len(grp):,} tickers", flush=True)

# per ticker: which bars are followed by a GAP (the next bar is far away) and where the
# series simply ends. A trade whose holding window runs into either was cut short.
gapinfo = {}
for tk, g in grp.items():
    d = pd.to_datetime(g["date"]).to_numpy()
    if len(d) < 3:
        continue
    step = np.diff(d).astype("timedelta64[D]").astype(int)
    gapinfo[tk] = (d, np.append(step, 0))
med_gap = np.median(np.concatenate([v[1][:-1] for v in gapinfo.values() if len(v[1]) > 1]))
print(f"  median spacing between consecutive frame bars: {med_gap:.0f} day(s)", flush=True)
GAP_THR = 10          # a hole of >10 calendar days = the name left the frame for a while

tot_gap_bars = sum(int((v[1] > GAP_THR).sum()) for v in gapinfo.values())
tot_bars = sum(len(v[0]) for v in gapinfo.values())
tk_with_gap = sum(1 for v in gapinfo.values() if (v[1] > GAP_THR).any())
print(f"  bars followed by a >{GAP_THR}d hole: {tot_gap_bars:,} of {tot_bars:,} "
      f"({tot_gap_bars/tot_bars:.3%}) on {tk_with_gap:,} tickers "
      f"({tk_with_gap/len(grp):.1%})", flush=True)

print(f"\n  {'setup':26s} {'n':>7s} {'trunc':>6s} {'%':>6s} {'med_all':>8s} "
      f"{'med_full':>9s} {'med_trunc':>10s} {'Δ':>7s}", flush=True)
rows = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 300:
        continue
    di = pd.to_datetime(tr["date_in"]).to_numpy()
    do = pd.to_datetime(tr["date_out"]).to_numpy()
    trunc = np.zeros(len(tr), bool)
    for i, tk in enumerate(tr["ticker"].to_numpy()):
        gi = gapinfo.get(tk)
        if gi is None:
            continue
        d, step = gi
        # a trade is truncated when its window contains a hole, or ends at the ticker's
        # very last frame bar (the series ran out before the exit rule fired)
        m = (d >= di[i]) & (d <= do[i])
        if m.any() and (step[m][:-1] > GAP_THR).any():
            trunc[i] = True
        elif do[i] >= d[-1]:
            trunc[i] = True
    if trunc.sum() < 10:
        continue
    m_all = tr["ret"].median() * 100
    m_full = tr.loc[~trunc, "ret"].median() * 100
    m_tr = tr.loc[trunc, "ret"].median() * 100
    rows.append(dict(setup=name, n=len(tr), trunc=int(trunc.sum()),
                     pct=trunc.mean() * 100, med=m_all, med_full=m_full, med_trunc=m_tr,
                     delta=m_tr - m_full))
    print(f"  {name:26s} {len(tr):>7,} {int(trunc.sum()):>6,} {trunc.mean()*100:>5.1f}% "
          f"{m_all:>+8.2f} {m_full:>+9.2f} {m_tr:>+10.2f} {m_tr-m_full:>+7.2f}", flush=True)

R = pd.DataFrame(rows)
if len(R):
    print(f"\n  setups measured: {len(R)} · median truncated share {R.pct.median():.1f}%",
          flush=True)
    print(f"  truncated trades vs complete ones: median Δ {R.delta.median():+.2f}pp  "
          f"(negative ⇒ truncation is REMOVING losses)", flush=True)
    worse = int((R.delta < 0).sum())
    print(f"  setups where truncated trades did WORSE: {worse} of {len(R)}", flush=True)
    R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "truncation_impact.csv"), index=False)
    print("  → truncation_impact.csv", flush=True)

print(f"\n{'='*92}\nHOW TO READ THIS", flush=True)
print("  Channel 1 is a property of the DATA and cannot be fixed without a delisted-security")
print("  feed. Channel 2 is a property of OUR FILTER and is fixable: a per-bar price/liquidity")
print("  floor lets a dying name leave the frame mid-trade, so the remaining path is unseen.")
print("  If truncated trades look BETTER than complete ones, the filter is removing losses —")
print("  every worst-year in the book is then flattered by an amount this table bounds.")
print("=" * 92, flush=True)
print("\nDONE", flush=True)
