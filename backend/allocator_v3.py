"""C2f — the allocator, priced honestly. Third attempt, and the first two were both wrong.

v1  ranked by the state score, lost −8.18 in 2022, and I called the score a failure. The
    decile test then showed the score's top-10 is positive in every year. The verdict was
    about the allocator.

v2  swept the hold and added replacement, and BOTH were mispriced the same way: a position
    locked the slot for `hold_bars` but was booked at its FULL 60-bar outcome. So a shorter
    hold appeared to give the same returns more often, and replacement gave extra positions
    for free. Of that whole table only the two hold-60 rows were valid.

v3  prices every close at what the position was actually worth on the day it closed, using
    the mark-to-market grid. Two rules, and they are the whole point:

      natural expiry   → mtm at `hold_bars`, not the 60-bar figure
      replaced early   → mtm at the bars actually held, and if the exit rule had already
                         fired, that realised loss stands (mtm carries it forward)

    A shorter hold now costs what it costs: less time in the trade. Replacement costs what
    it costs: you sell at the price on the day, not at the outcome you would have had.

Everything else is unchanged from v2 — same walk-forward fit, same purge, same dedup, same
two-sided acceptance with 2022 in the window.
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
SLOTS, PURGE, BAR2CAL = 10, 60, 1.45
GRID = np.array([1, 2, 3, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60])
pd.set_option("display.width", 235)

O = pd.read_parquet(OPP)
MTM = [f"mtm_{g}" for g in GRID]
if not all(c in O.columns for c in MTM):
    sys.exit("mark-to-market columns missing — run add_mtm.py first")
O["d"] = pd.to_datetime(O["date_in"].astype(str).str[:10])
O["y"] = O["d"].dt.year
O["ret_pct"] = O["ret"].astype(float) * 100
O = O.dropna(subset=["ret_pct", "sig_rsi_14", "sig_close", "mtm_60"]).reset_index(drop=True)

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
    t["s_base"] = t.family.map(fam).fillna(glob)
    t["s_state"] = (t.k_full.map(full).fillna(t.k_mid.map(mid))
                    .fillna(t.family.map(fam)).fillna(glob))
    parts.append(t)
W = pd.concat(parts, ignore_index=True).sort_values("d").reset_index(drop=True)
MTM_ARR = W[MTM].to_numpy(float) * 100
print(f"walk-forward rows {len(W):,} · days {W.d.nunique():,}\n", flush=True)


def price(idx: int, bars_held: float) -> float:
    """What the position was worth on the day it closed."""
    j = int(np.abs(GRID - max(1.0, bars_held)).argmin())
    v = MTM_ARR[idx, j]
    return float(v) if np.isfinite(v) else float("nan")


def simulate(score_col: str, hold_bars: int, replace: bool, repl_edge: float = 1.0):
    open_pos: list[dict] = []
    closed = []

    def close(p, day, reason):
        bars = (day - p["opened"]).days / BAR2CAL
        r = price(p["idx"], bars if reason == "replaced" else hold_bars)
        if np.isfinite(r):
            closed.append({"y": p["y"], "d": p["opened"], "ret": r, "why": reason,
                           "bars": bars})

    for day, dd in W.groupby("d", sort=True):
        for p in [q for q in open_pos if q["until"] <= day]:
            close(p, day, "expiry"); open_pos.remove(p)
        cand = (dd.sort_values(score_col, ascending=False)
                  .drop_duplicates("dup_group"))
        held = {q["dup"] for q in open_pos}
        cand = cand[~cand.dup_group.isin(held)]
        free = SLOTS - len(open_pos)
        if free > 0 and len(cand):
            for pos, (_, r) in enumerate(cand.head(free).iterrows()):
                open_pos.append({"idx": r.name, "dup": r.dup_group, "score": r[score_col],
                                 "opened": day, "y": r.y,
                                 "until": day + pd.Timedelta(days=hold_bars * BAR2CAL)})
            cand = cand.iloc[free:]
        if replace and len(cand) and open_pos:
            worst = min(open_pos, key=lambda q: q["score"])
            best = cand.iloc[0]
            if best[score_col] > worst["score"] + repl_edge:
                close(worst, day, "replaced"); open_pos.remove(worst)
                open_pos.append({"idx": best.name, "dup": best.dup_group,
                                 "score": best[score_col], "opened": day, "y": best.y,
                                 "until": day + pd.Timedelta(days=hold_bars * BAR2CAL)})
    for p in open_pos:                                  # mark the book to the last day
        close(p, W.d.max(), "expiry")
    return pd.DataFrame(closed)


def report(P: pd.DataFrame, label: str) -> dict:
    if P.empty or len(P) < 30:
        print(f"  {label:46s} n={len(P)} too thin"); return {}
    yr = P.groupby("y")["ret"].mean()
    lo, hi = bootstrap_ci_clustered(P.ret, P.d.astype(str), stat="mean")
    ys = "".join(f"{yr.get(y, float('nan')):>7.2f}" for y in range(2022, 2027))
    rep = (P.why == "replaced").mean() * 100 if "why" in P else 0.0
    print(f"  {label:46s} n={len(P):>5,} mean{P.ret.mean():>+7.2f} "
          f"[{lo:>+6.2f},{hi:>+6.2f}] |{ys} | {int((yr>0).sum())}/{len(yr)}yr "
          f"worst{yr.min():>+7.2f} swap{rep:>5.0f}%", flush=True)
    return dict(label=label, n=len(P), mean=P.ret.mean(), lo=lo, hi=hi,
                worst=yr.min(), yrs=int((yr > 0).sum()))


print("=" * 132)
print(f"ALLOCATOR v3 · {SLOTS} slots · every close priced at its mark-to-market")
print("=" * 132)
print(f"  {'configuration':46s} {'n':>7s} {'mean':>11s} {'CI(days)':>16s} |"
      f"{'2022':>7s}{'2023':>7s}{'2024':>7s}{'2025':>7s}{'2026':>7s} {'yrs':>7s} "
      f"{'worst':>11s} {'swap':>9s}", flush=True)

rows = []
print("\n── hold sweep, honestly priced ──", flush=True)
for h in (20, 30, 45, 60):
    rows.append(report(simulate("s_base", h, False), f"BASELINE (tier,median) · hold {h}"))
    rows.append(report(simulate("s_state", h, False), f"  state · hold {h}"))

print("\n── replacement, honestly priced ──", flush=True)
for h in (20, 60):
    for e in (1.0, 3.0):
        rows.append(report(simulate("s_state", h, True, e),
                           f"state · hold {h} · REPLACE edge {e:.0f}"))

R = pd.DataFrame([r for r in rows if r])
ref = R[R.label == "BASELINE (tier,median) · hold 60"]
print("\n" + "=" * 132, flush=True)
if len(ref):
    b = ref.iloc[0]
    print(f"  reference (today): mean {b['mean']:+.2f} · worst {b.worst:+.2f} · {b.yrs}/5yr\n",
          flush=True)
    ok = R[(R["mean"] > b["mean"]) & (R.worst >= b.worst)]
    if len(ok):
        print("  ✅ PASS (better mean AND not worse in the worst year):", flush=True)
        print(ok.sort_values("mean", ascending=False)
              [["label", "n", "mean", "lo", "hi", "worst", "yrs"]]
              .to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)
    else:
        print("  ⛔ NOTHING PASSES", flush=True)
print("=" * 132, flush=True)
R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "allocator_v3.csv"),
         index=False)
print("\n  → allocator_v3.csv\nDONE", flush=True)
