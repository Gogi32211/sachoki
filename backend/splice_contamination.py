"""A3 — do uncorrected corporate actions contaminate the numbers we already trust?

The integrity gate found 134 of 240 checked candidates carrying a >4x level shift that never
reverts. Reading the examples (DFNS $0.17→$27.68, QMMM $2.81→$97.50, SDOT $0.49→$14.84) the
signature is not ticker REUSE, it is a REVERSE SPLIT: a sub-dollar stock doing 1:20 to keep
its listing. Which means prices are not adjusted for reverse splits.

That matters far more than ticker reuse, because it hits returns directly. A trade opened
before the split date sees the price multiply by the split ratio overnight and books it as
profit. A trailing stop does not save it — the price gaps up 20x, the trail sits 60% below
the new peak, and the trade still closes at an absurd gain.

This measures the actual exposure rather than assuming it:
  1. detect suspect bars across the WHOLE universe (not the old 336-row CSV, whose threshold
     is unknown and may be incomplete)
  2. count how many path-sim trades in every registry setup SPAN such a bar
  3. show each affected setup's median with and without them

If dv_floor=$3M and the price filters already exclude these names, the answer is "theoretical"
and we move on. If not, the affected setups need recomputing before anything is deflated.
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
JUMP = 4.0            # a bar whose close/prev_close leaves [1/4, 4]
REVERT = 2.0          # ... and whose level does NOT come back within 2x
pd.set_option("display.width", 200)

# ── 1. detect suspect corporate-action bars universe-wide ────────────────────
print("### 1. scanning the whole universe for unadjusted corporate actions\n", flush=True)
con = duckdb.connect(DB, read_only=True)
raw = con.execute(f"""
    WITH s AS (
      SELECT ticker, date, close,
             lag(close) OVER (PARTITION BY ticker ORDER BY date) pc
      FROM (SELECT DISTINCT ticker, date, close FROM bars)
    )
    SELECT ticker, date, pc AS prev_close, close,
           close / nullif(pc, 0) AS ratio
    FROM s
    WHERE pc > 0 AND (close / pc > {JUMP} OR close / pc < {1/JUMP})
    ORDER BY ticker, date
""").fetchdf()
print(f"  bars with a >{JUMP:.0f}x single-bar level change: {len(raw):,} "
      f"on {raw.ticker.nunique():,} tickers", flush=True)

# a crash reverts or continues; a split does not. Compare the 40-bar medians either side.
keep = []
for tk, grp_ in raw.groupby("ticker"):
    d = con.execute("SELECT DISTINCT date, close FROM bars WHERE ticker=? ORDER BY date",
                    [tk]).fetchdf()
    c = d.close.to_numpy(float)
    dt = d.date.astype(str).to_numpy()
    pos = {v: i for i, v in enumerate(dt)}
    for _, r in grp_.iterrows():
        i = pos.get(str(r.date)[:10])
        if i is None or i < 20 or i > len(c) - 20:
            continue
        pre = np.nanmedian(c[max(0, i - 40):i])
        post = np.nanmedian(c[i + 1:i + 41])
        if pre <= 0 or post <= 0:
            continue
        lvl = post / pre
        if lvl > REVERT or lvl < 1 / REVERT:
            keep.append((tk, str(r.date)[:10], float(pre), float(post), float(lvl),
                         float(r.ratio)))
con.close()
S = pd.DataFrame(keep, columns=["ticker", "date", "med_before", "med_after",
                                "level_shift", "bar_ratio"])
print(f"  of those, {len(S):,} do NOT revert → treated as unadjusted corporate actions "
      f"({S.ticker.nunique():,} tickers)", flush=True)
print(f"  direction: {int((S.level_shift > 1).sum()):,} up (reverse split) · "
      f"{int((S.level_shift < 1).sum()):,} down (forward split / spin-off)", flush=True)
print("\n  worst 10 by level shift:", flush=True)
print(S.reindex(S.level_shift.sub(1).abs().sort_values(ascending=False).index)
      .head(10).to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)
out_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "corporate_actions.csv")
S.to_csv(out_csv, index=False)
print(f"\n  → {out_csv}", flush=True)

# ── 2. how many traded bars are exposed? ─────────────────────────────────────
print("\n### 2. do our trades actually touch these bars?\n", flush=True)
grp, as_of = er._frame(60, 3_000_000)
bad = {}
for _, r in S.iterrows():
    bad.setdefault(r.ticker, []).append(pd.Timestamp(r.date))
in_frame = sum(1 for t in bad if t in grp)
print(f"  frame as_of {as_of} · {len(grp):,} tickers · of {len(bad):,} affected tickers, "
      f"{in_frame:,} survive the $3M dollar-volume floor", flush=True)

if in_frame == 0:
    print("\n  ✅ NOT EXPOSED — every affected name is filtered out by dv_floor before any "
          "study sees it. The problem is real in the raw DB but cannot reach a result.",
          flush=True)
    print("\nDONE", flush=True)
    sys.exit(0)

# mark, per ticker, the bars whose forward window would span a corporate action
for tk, g in grp.items():
    dts = pd.to_datetime(g["date"])
    flag = np.zeros(len(g), bool)
    for bd in bad.get(tk, []):
        # a trade opened up to MAXH bars before the action would carry it
        span = (dts >= bd - pd.Timedelta(days=130)) & (dts <= bd + pd.Timedelta(days=5))
        flag |= span.to_numpy()
    g["_ca"] = flag
n_bars = sum(int(g["_ca"].sum()) for g in grp.values())
tot_bars = sum(len(g) for g in grp.values())
print(f"  bars inside a corporate-action window: {n_bars:,} of {tot_bars:,} "
      f"({n_bars/tot_bars*100:.3f}%)", flush=True)

# ── 3. per-setup damage ──────────────────────────────────────────────────────
print("\n### 3. per-setup: median with and without the exposed trades\n", flush=True)
print(f"  {'setup':30s} {'n':>7s} {'n_exp':>6s} {'med':>8s} {'med_clean':>10s} "
      f"{'Δ':>7s} {'max_ret':>9s}", flush=True)
rows = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 100:
        continue
    key = set()
    for tk in bad:
        for bd in bad[tk]:
            key.add((tk, bd))
    d_in = pd.to_datetime(tr["date_in"])
    exp = np.zeros(len(tr), bool)
    for i, (tk, di) in enumerate(zip(tr["ticker"], d_in)):
        for bd in bad.get(tk, []):
            if di - pd.Timedelta(days=130) <= bd <= di + pd.Timedelta(days=130):
                exp[i] = True
                break
    med = tr["ret"].median() * 100
    clean = tr.loc[~exp, "ret"].median() * 100 if (~exp).sum() > 30 else float("nan")
    rows.append(dict(setup=name, n=len(tr), n_exp=int(exp.sum()), med=med,
                     med_clean=clean, delta=clean - med,
                     max_ret=tr["ret"].max() * 100))
    if exp.sum() > 0:
        print(f"  {name:30s} {len(tr):>7,} {int(exp.sum()):>6,} {med:>+8.2f} "
              f"{clean:>+10.2f} {clean-med:>+7.2f} {tr['ret'].max()*100:>+9.1f}", flush=True)

R = pd.DataFrame(rows)
aff = R[R.n_exp > 0]
print(f"\n  setups with ≥1 exposed trade: {len(aff)} of {len(R)}", flush=True)
if len(aff):
    print(f"  median |Δ| across affected setups: {aff.delta.abs().median():.3f}pp  ·  "
          f"worst {aff.delta.abs().max():.2f}pp ({aff.loc[aff.delta.abs().idxmax(),'setup']})",
          flush=True)
print(f"\n  trades returning >200%: "
      f"{int((R.max_ret > 200).sum())} setups have at least one", flush=True)
print(R.nlargest(8, "max_ret")[["setup", "n", "n_exp", "med", "max_ret"]]
      .to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)

print("\nDONE", flush=True)
