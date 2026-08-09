"""A5b — separate the three causes of a short path, then measure the real one against truth.

The first pass conflated three different things under one "truncated" flag:

  (a) DROPOUT     the name fell through the frame's per-bar filter (close ≥ $5, turnover ≥
                  $3M) mid-trade and its remaining path is invisible to the simulator.
  (b) HALT        a hole in the series that later resumes — not survivorship.
  (c) OPEN_AT_END the trade was still open when the data ran out. Not survivorship either,
                  and it biases UPWARD, because those trades are all recent and 2026 rose.

Only (a) is the survivorship channel, and mixing (c) into it is what made QZ-Capit🧱OB look
+3.97 better when truncated.

Then the part that turns a bound into a measurement: the raw database still HOLDS the bars
that the frame filtered away. A name at $4 is missing from the frame but present in `bars`.
So for every DROPOUT trade we can look up what actually happened and re-run the exact same
trailing rule on the true path — and compare it with what the backtest recorded.

That answers the question directly: how much of the book's performance is the simulator
simply not seeing the rest of the fall?
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
MAXH, ATR_K, SLIP = 60, 12.0, er.SLIP
MAX_LOOKUPS = 4000                  # capped, and the cap is reported — never silent
pd.set_option("display.width", 210)

grp, as_of = er._frame(60, 3_000_000)
gl = max(pd.to_datetime(g["date"]).max() for g in grp.values())
print(f"frame as_of {as_of} · {len(grp):,} tickers · last bar {gl.date()}\n", flush=True)

# per-ticker frame geometry
geo = {}
for tk, g in grp.items():
    d = pd.to_datetime(g["date"]).to_numpy()
    geo[tk] = dict(d=d, last=d[-1] if len(d) else None,
                   close_last=float(g["close"].iloc[-1]) if len(g) else np.nan)
left = {tk: v for tk, v in geo.items()
        if v["last"] is not None and (gl - pd.Timestamp(v["last"])).days > 20}
print(f"tickers whose frame series ends >20d before the last bar: {len(left):,} "
      f"({len(left)/len(grp):.1%})", flush=True)
lowc = sum(1 for v in left.values() if v["close_last"] < 8)
print(f"  of those, {lowc:,} ({lowc/max(len(left),1):.0%}) last traded under $8 — "
      f"i.e. they fell through the $5 floor rather than delisting\n", flush=True)


def classify(tr: pd.DataFrame) -> np.ndarray:
    """→ array of 'COMPLETE' | 'DROPOUT' | 'HALT' | 'OPEN_END'."""
    out = np.array(["COMPLETE"] * len(tr), dtype=object)
    di = pd.to_datetime(tr["date_in"]).to_numpy()
    do = pd.to_datetime(tr["date_out"]).to_numpy()
    for i, tk in enumerate(tr["ticker"].to_numpy()):
        g = geo.get(tk)
        if g is None:
            continue
        if (gl - pd.Timestamp(do[i])).days <= 5:
            out[i] = "OPEN_END"                      # data ran out, not the name
            continue
        d = g["d"]
        w = d[(d >= di[i]) & (d <= do[i])]
        if len(w) > 1 and np.diff(w).astype("timedelta64[D]").astype(int).max() > 10:
            after = d[d > do[i]]
            out[i] = "HALT" if len(after) else "DROPOUT"
            continue
        if pd.Timestamp(do[i]) >= pd.Timestamp(g["last"]) and tk in left:
            out[i] = "DROPOUT"                        # series ends here and the name left
    return out


print("### 1. the three causes, separated\n", flush=True)
print(f"  {'setup':26s} {'n':>7s} {'DROP':>6s} {'HALT':>6s} {'OPEN':>6s} "
      f"{'med_cmpl':>9s} {'med_drop':>9s} {'Δ':>7s}", flush=True)
drops = []
rows = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, MAXH, atr_k=ATR_K)
    if len(tr) < 300:
        continue
    k = classify(tr)
    nd = int((k == "DROPOUT").sum())
    if nd < 10:
        continue
    mc = tr.loc[k == "COMPLETE", "ret"].median() * 100
    md = tr.loc[k == "DROPOUT", "ret"].median() * 100
    rows.append(dict(setup=name, n=len(tr), drop=nd, halt=int((k == "HALT").sum()),
                     open_end=int((k == "OPEN_END").sum()), med_cmpl=mc, med_drop=md,
                     delta=md - mc))
    print(f"  {name:26s} {len(tr):>7,} {nd:>6,} {int((k=='HALT').sum()):>6,} "
          f"{int((k=='OPEN_END').sum()):>6,} {mc:>+9.2f} {md:>+9.2f} {md-mc:>+7.2f}",
          flush=True)
    sub = tr.loc[k == "DROPOUT", ["ticker", "date_in", "date_out", "ret"]].copy()
    sub["setup"] = name
    drops.append(sub)

R = pd.DataFrame(rows)
D = pd.concat(drops, ignore_index=True) if drops else pd.DataFrame()
print(f"\n  setups with ≥10 dropout trades: {len(R)}", flush=True)
if len(R):
    print(f"  dropout vs complete: median Δ {R.delta.median():+.2f}pp · "
          f"worse in {int((R.delta < 0).sum())} of {len(R)} setups", flush=True)

# ── 2. what REALLY happened — the raw DB still has those bars ────────────────
print("\n### 2. re-running the dropout trades on the TRUE path from the raw DB\n", flush=True)
if len(D) == 0:
    print("  no dropout trades to check", flush=True); sys.exit(0)
U = D.drop_duplicates(subset=["ticker", "date_in"]).reset_index(drop=True)
print(f"  unique (ticker, entry) dropout trades: {len(U):,}", flush=True)
if len(U) > MAX_LOOKUPS:
    print(f"  ⚠ CAPPED at {MAX_LOOKUPS:,} (reported, not silent) — sampling uniformly",
          flush=True)
    U = U.sample(MAX_LOOKUPS, random_state=0).reset_index(drop=True)

con = duckdb.connect(DB, read_only=True)
res = []
for _, r in U.iterrows():
    q = con.execute("""SELECT DISTINCT date, open, high, low, close FROM bars
                       WHERE ticker=? AND date >= ? ORDER BY date LIMIT ?""",
                    [r.ticker, str(r.date_in)[:10], MAXH + 2]).fetchdf()
    if len(q) < 3:
        continue
    o = q.open.to_numpy(float); hi = q.high.to_numpy(float)
    lo = q.low.to_numpy(float); cl = q.close.to_numpy(float)
    entry = o[0] * (1 + SLIP)
    if not np.isfinite(entry) or entry <= 0:
        continue
    trail = 0.60                                   # the ATR trail saturates at the cap for
    pk, ret = entry, None                          # names this volatile
    for j in range(1, len(q)):
        ts_prev = pk * (1 - trail)
        if o[j] <= ts_prev:
            ret = o[j] / entry - 1 - SLIP; break
        pk = max(pk, hi[j])
        if lo[j] <= pk * (1 - trail):
            ret = pk * (1 - trail) / entry - 1 - SLIP; break
    if ret is None:
        ret = cl[-1] / entry - 1 - SLIP
    res.append((r.setup, r.ticker, str(r.date_in)[:10], float(r.ret) * 100, ret * 100,
                len(q)))
con.close()

T = pd.DataFrame(res, columns=["setup", "ticker", "date_in", "recorded", "true", "bars"])
T["gap"] = T["true"] - T["recorded"]
print(f"  resolved {len(T):,} of {len(U):,} against the raw database\n", flush=True)
print(f"  recorded median {T.recorded.median():+.2f}%   →   "
      f"TRUE median {T['true'].median():+.2f}%   ·   gap {T.gap.median():+.2f}pp", flush=True)
print(f"  recorded mean   {T.recorded.mean():+.2f}%   →   "
      f"TRUE mean   {T['true'].mean():+.2f}%   ·   gap {T.gap.mean():+.2f}pp", flush=True)
print(f"  trades whose true outcome was WORSE than recorded: "
      f"{(T.gap < -0.5).mean()*100:.1f}%", flush=True)
print(f"  median extra bars visible in the raw DB: "
      f"{T.bars.median():.0f} (frame had ≤ the trade's own length)", flush=True)
print("\n  worst under-statements:", flush=True)
print(T.nsmallest(10, "gap")[["setup", "ticker", "date_in", "recorded", "true", "gap"]]
      .to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)

# per-setup damage, weighted by how many of its trades dropped out
print("\n### 3. what this does to each setup's headline median\n", flush=True)
print(f"  {'setup':26s} {'n':>7s} {'drop%':>6s} {'med_now':>8s} {'med_fixed':>10s} {'Δ':>7s}",
      flush=True)
for _, r in R.iterrows():
    sub = T[T.setup == r["setup"]]
    if len(sub) < 10:
        continue
    share = r["drop"] / r["n"]   # r.drop is the DataFrame METHOD, not the column
    shift = (sub.gap.median()) * share
    print(f"  {r['setup']:26s} {r['n']:>7,} {share*100:>5.1f}% "
          f"{r['med_cmpl']:>+8.2f} {r['med_cmpl'] + shift:>+10.2f} {shift:>+7.2f}", flush=True)

T.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                      "truncation_true.csv"), index=False)
print("\n  → truncation_true.csv\nDONE", flush=True)
