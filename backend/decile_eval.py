"""C2c — the top of the ranking, which is the only part an allocator ever reads.

The contradiction this resolves: the state score has its BEST IC in 2022 (+0.021) and the
allocator lost −8.18 that year. Both can be true, because IC scores the whole cross-section
while ten slots live entirely in the extreme top. A score can order the bulk correctly and
still have a bad head.

So this measures the head directly, per day, per year:

    top-10   what the allocator actually funds
    top 10%  the decile, less noisy than ten names
    bottom   the other end, to show whether the spread is a ranking or a level effect

If a score's top-10 pays in 2022 while its overall IC there is flat, the fix is not a better
model — it is that the allocator should read a different part of the distribution. If the head
is bad wherever the IC is bad, the regime dependence is real and no amount of head-reading
saves it.

Scores are walk-forward, fitted strictly before each test year and purged by the hold.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.expanduser("~/.claude/skills/quant-study/scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from analysis_kit import bootstrap_ci_clustered      # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPP = os.path.join(ROOT, "data", "opportunities.parquet")
PURGE, MIN_PER_DAY, TOPN = 60, 20, 10
pd.set_option("display.width", 230)

O = pd.read_parquet(OPP)
O["d"] = pd.to_datetime(O["date_in"].astype(str).str[:10])
O["y"] = O["d"].dt.year
O["ret_pct"] = O["ret"].astype(float) * 100
O = O.dropna(subset=["ret_pct", "sig_rsi_14", "sig_close"]).reset_index(drop=True)

rsi = O["sig_rsi_14"].astype(float)
rb = pd.cut(rsi, [-1, 30, 40, 50, 60, 101], labels=["a", "b", "c", "d", "e"]).astype(str)
O["k_full"] = (O.family.astype(str) + "|" + rb + "|"
               + O.sig_conso.fillna(False).astype(bool).map({True: "C", False: "X"}) + "|"
               + O.sig_rs_intact.fillna(False).astype(bool).map({True: "R", False: "n"}) + "|"
               + pd.cut(O.sig_close.astype(float), [0, 21, 89, 377, 1e9],
                        labels=["p1", "p2", "p3", "p4"]).astype(str))
O["k_mid"] = O.family.astype(str) + "|" + rb

parts = []
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
    t["s_rs"] = t.sig_rs_intact.fillna(False).astype(float)
    t["s_rs_state"] = t["s_rs"] * 1000 + t["s_state"]     # RS first, state to break ties
    parts.append(t)
W = pd.concat(parts, ignore_index=True)
print(f"walk-forward rows {len(W):,} · days {W.d.nunique():,}\n", flush=True)

SCORES = [("setup family mean", "s_family"), ("state-conditioned", "s_state"),
          ("🏆RS intact", "s_rs"), ("🏆RS → then state", "s_rs_state")]
YEARS = list(range(2022, 2027))


def heads(col: str) -> pd.DataFrame:
    out = []
    for (day, y), dd in W.groupby(["d", "y"], sort=True):
        if len(dd) < MIN_PER_DAY:
            continue
        d = dd.sort_values(col, ascending=False).drop_duplicates("dup_group")
        if len(d) < MIN_PER_DAY:
            continue
        k = max(1, len(d) // 10)
        out.append({"d": day, "y": y,
                    "top10": d.head(TOPN)["ret_pct"].mean(),
                    "topdec": d.head(k)["ret_pct"].mean(),
                    "botdec": d.tail(k)["ret_pct"].mean(),
                    "all": d["ret_pct"].mean()})
    return pd.DataFrame(out)


print(f"  {'score':22s} {'slice':9s} {'mean':>8s} {'CI (day-clustered)':>20s} | "
      + "".join(f"{y:>8d}" for y in YEARS), flush=True)
rows = []
for label, col in SCORES:
    H = heads(col)
    if H.empty:
        continue
    for slc in ("top10", "topdec", "botdec", "all"):
        lo, hi = bootstrap_ci_clustered(H[slc], H.d.astype(str), stat="mean")
        per = H.groupby("y")[slc].mean()
        ys = "".join(f"{per.get(y, float('nan')):>8.2f}" for y in YEARS)
        star = "" if (lo <= 0 <= hi) else " ✅"
        mark = " ←funded" if slc == "top10" else ""
        print(f"  {label:22s} {slc:9s} {H[slc].mean():>+8.2f} [{lo:>+7.2f},{hi:>+7.2f}] |{ys}"
              f"{star}{mark}", flush=True)
        rows.append(dict(score=label, slice=slc, mean=H[slc].mean(), lo=lo, hi=hi,
                         **{f"y{y}": per.get(y, np.nan) for y in YEARS}))
    sp = H["topdec"] - H["botdec"]
    lo, hi = bootstrap_ci_clustered(sp, H.d.astype(str), stat="mean")
    per = (H.groupby("y")["topdec"].mean() - H.groupby("y")["botdec"].mean())
    ys = "".join(f"{per.get(y, float('nan')):>8.2f}" for y in YEARS)
    print(f"  {label:22s} {'SPREAD':9s} {sp.mean():>+8.2f} [{lo:>+7.2f},{hi:>+7.2f}] |{ys}"
          f"{'' if lo <= 0 <= hi else ' ✅'}\n", flush=True)
    rows.append(dict(score=label, slice="spread", mean=sp.mean(), lo=lo, hi=hi,
                     **{f"y{y}": per.get(y, np.nan) for y in YEARS}))

R = pd.DataFrame(rows)
print("=" * 120, flush=True)
print("READING IT", flush=True)
print("  `top10` is what ten slots actually fund; `all` is the day's average opportunity.")
print("  A score earns its keep only if top10 > all, and if that holds in 2022 as well as")
print("  in the bull years. A positive SPREAD with a flat top10 means the score is sorting")
print("  the bulk while telling the allocator nothing.", flush=True)
R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "decile_eval.csv"),
         index=False)
print("\n  → decile_eval.csv\nDONE", flush=True)
