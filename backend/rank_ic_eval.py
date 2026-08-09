"""C2b — Rank IC: does any score actually ORDER the outcomes? (2026-08-09)

The allocation test was the wrong first question and I ran it first. It sees 233 trades out of
1,140,344 opportunities, because ten slots over five years is all an account touches. At that
sample size a real but modest edge is undetectable — the walk-forward result reads "no better",
not "worse".

Rank IC asks the prior question on the whole set: WITHIN EACH DAY, does the score put the
better outcomes higher? That is the property an allocator needs, and it is measurable on a
million rows instead of two hundred.

    IC_t = spearman(score, outcome) across the opportunities available on day t
    IC   = mean over days · ICIR = IC / sd(IC) · hit = share of days with IC > 0

Three disciplines this run keeps:

  CROSS-SECTIONAL, NOT POOLED. Correlating score against outcome over the whole pooled table
  would mostly measure that some months were good for everything. The question is whether the
  score ranks the names available on the SAME day against each other.

  CLUSTERED INFERENCE. The IC series is one number per day; its confidence interval comes
  from resampling DAYS, using the machinery built in A6. A pooled t-test over 1.1M rows would
  claim significance that 1,300 days cannot support.

  WALK-FORWARD SCORES. The state table is fitted strictly before each test year, purged by the
  holding period. A score fitted on the whole history would rank its own training data.

Candidate scores, cheapest first — if the setup's own historical mean already ranks, nothing
more elaborate is justified.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from analysis_kit import bootstrap_ci_clustered, effective_n     # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPP = os.path.join(ROOT, "data", "opportunities.parquet")
PURGE, MIN_PER_DAY = 60, 8
pd.set_option("display.width", 220)

O = pd.read_parquet(OPP)
O["d"] = pd.to_datetime(O["date_in"].astype(str).str[:10])
O["y"] = O["d"].dt.year
O["ret_pct"] = O["ret"].astype(float) * 100
O = O.dropna(subset=["ret_pct", "sig_rsi_14", "sig_close"]).reset_index(drop=True)
print(f"opportunities {len(O):,} · days {O.d.nunique():,} · "
      f"{O.d.min().date()} → {O.d.max().date()}\n", flush=True)

rsi = O["sig_rsi_14"].astype(float)
O["k_full"] = (O.family.astype(str) + "|"
               + pd.cut(rsi, [-1, 30, 40, 50, 60, 101],
                        labels=["a", "b", "c", "d", "e"]).astype(str) + "|"
               + O.sig_conso.fillna(False).astype(bool).map({True: "C", False: "X"}) + "|"
               + O.sig_rs_intact.fillna(False).astype(bool).map({True: "R", False: "n"}) + "|"
               + pd.cut(O.sig_close.astype(float), [0, 21, 89, 377, 1e9],
                        labels=["p1", "p2", "p3", "p4"]).astype(str))
O["k_mid"] = (O.family.astype(str) + "|"
              + pd.cut(rsi, [-1, 30, 40, 50, 60, 101],
                       labels=["a", "b", "c", "d", "e"]).astype(str))


def walk_forward_scores() -> pd.DataFrame:
    """Every score, fitted strictly before its test year and purged by the hold."""
    out = []
    for y in sorted(O.y.unique())[1:]:
        te = O[O.y == y]
        tr = O[O.d < te.d.min() - pd.Timedelta(days=PURGE + 5)]
        if len(tr) < 5000 or te.empty:
            continue
        fam = tr.groupby("family")["ret_pct"].agg(["mean", "size"])
        fam = fam[fam["size"] >= 200]["mean"]
        full = tr.groupby("k_full")["ret_pct"].agg(["mean", "size"])
        full = full[full["size"] >= 60]["mean"]
        mid = tr.groupby("k_mid")["ret_pct"].agg(["mean", "size"])
        mid = mid[mid["size"] >= 60]["mean"]
        glob = float(tr["ret_pct"].mean())
        t = te.copy()
        t["s_family"] = t.family.map(fam).fillna(glob)
        t["s_state"] = (t.k_full.map(full).fillna(t.k_mid.map(mid))
                        .fillna(t.family.map(fam)).fillna(glob))
        # cheap, already-validated single features — is anything simpler enough?
        t["s_rsi_low"] = -t.sig_rsi_14.astype(float)
        t["s_rs"] = t.sig_rs_intact.fillna(False).astype(float)
        t["s_conso"] = t.sig_conso.fillna(False).astype(float)
        out.append(t)
    return pd.concat(out, ignore_index=True)


W = walk_forward_scores()
print(f"walk-forward rows: {len(W):,} · years {sorted(W.y.unique())}\n", flush=True)

SCORES = [("setup family mean", "s_family"), ("★ state-conditioned", "s_state"),
          ("RSI low (single feature)", "s_rsi_low"), ("🏆RS intact", "s_rs"),
          ("❄️CONSO", "s_conso")]

print(f"  {'score':28s} {'IC':>8s} {'CI(days)':>18s} {'ICIR':>7s} {'hit%':>6s} "
      f"{'days':>6s} | {'2022':>7s}{'2023':>7s}{'2024':>7s}{'2025':>7s}{'2026':>7s}",
      flush=True)
rows = []
for label, col in SCORES:
    ics, yrs = [], {}
    for (day, y), dd in W.groupby(["d", "y"], sort=True):
        if len(dd) < MIN_PER_DAY or dd[col].nunique() < 2:
            continue
        ic = dd[col].corr(dd["ret_pct"], method="spearman")
        if np.isfinite(ic):
            ics.append({"d": day, "y": y, "ic": ic})
    if len(ics) < 100:
        print(f"  {label:28s} too few days"); continue
    I = pd.DataFrame(ics)
    lo, hi = bootstrap_ci_clustered(I.ic, I.d.astype(str), stat="mean")
    icir = I.ic.mean() / I.ic.std() if I.ic.std() > 0 else np.nan
    per = I.groupby("y")["ic"].mean()
    ys = "".join(f"{per.get(y, float('nan')):>7.3f}" for y in range(2022, 2027))
    sig = "" if (lo <= 0 <= hi) else "  ✅"
    print(f"  {label:28s} {I.ic.mean():>+8.4f} [{lo:>+7.4f},{hi:>+7.4f}] {icir:>7.3f} "
          f"{(I.ic>0).mean()*100:>5.1f} {len(I):>6,} |{ys}{sig}", flush=True)
    rows.append(dict(score=label, ic=I.ic.mean(), lo=lo, hi=hi, icir=icir,
                     hit=(I.ic > 0).mean() * 100, days=len(I),
                     ic2022=per.get(2022, np.nan)))

R = pd.DataFrame(rows)
print(f"\n{'='*118}", flush=True)
print("HOW TO READ THIS", flush=True)
print("  IC is a per-day cross-sectional rank correlation. In equities a real IC of 0.02-0.05")
print("  is normal and useful; the interval is what decides whether it exists at all.")
print("  ✅ marks scores whose day-clustered interval excludes zero.")
print("\n  If NO score clears zero, the allocator has nothing to rank with and the 233-trade")
print("  result was not underpowered — there was simply no signal to detect.", flush=True)
if len(R):
    best = R.iloc[R.ic.idxmax()]
    print(f"\n  best: {best.score} · IC {best.ic:+.4f} [{best.lo:+.4f},{best.hi:+.4f}] "
          f"· 2022 IC {best.ic2022:+.4f}", flush=True)
print("=" * 118, flush=True)
R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "rank_ic_eval.csv"),
         index=False)
print("\n  → rank_ic_eval.csv\nDONE", flush=True)
