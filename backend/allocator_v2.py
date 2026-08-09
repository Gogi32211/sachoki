"""C2d — the allocator rebuilt, after the first one buried a working score.

WHAT WENT WRONG THE FIRST TIME

v1 ranked by the state score and lost −8.18 in 2022, so I called the ranker a failure. The
decile test then showed the score's top-10 returns +5.71 against a pool average of +2.32, and
is positive in EVERY year including 2022 (+0.70) — while the current `(tier, median)` rule is
−0.92 there.

The score was fine. The allocator was not. With a 60-bar hold each slot locks for roughly
three months, so ten slots make about forty decisions a year and WHICH forty is decided by
when a slot happens to free, not by what is worth owning. Selection was drowned by timing.

THREE THINGS THIS VERSION CHANGES

  hold        swept 20 / 30 / 45 / 60 bars. Short holds are not primarily about return per
              slot-day — they are about how often the ranking gets to speak. At 20 bars the
              account makes roughly three times as many choices.
  replacement optionally swap an open position for a materially better candidate. Never
              tested here before; without it, a slot taken on a quiet day is dead capital
              while the best opportunity of the month goes unfunded.
  min score   a floor, so empty slots stay empty rather than funding whatever is least bad.

Everything else is held identical to v1 so the comparison is about these three levers only:
same walk-forward fit, same purge, same dedup by dup_group, same acceptance — beat
`(tier, median)` on the mean AND do not worsen the worst year, with 2022 in the window.
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
pd.set_option("display.width", 230)

O = pd.read_parquet(OPP)
O["d"] = pd.to_datetime(O["date_in"].astype(str).str[:10])
O["y"] = O["d"].dt.year
O["ret_pct"] = O["ret"].astype(float) * 100
O["mfe_pct"] = O["mfe"].astype(float) * 100
O = O.dropna(subset=["ret_pct", "sig_rsi_14", "sig_close"]).reset_index(drop=True)

rsi = O["sig_rsi_14"].astype(float)
rb = pd.cut(rsi, [-1, 30, 40, 50, 60, 101], labels=["a", "b", "c", "d", "e"]).astype(str)
O["k_full"] = (O.family.astype(str) + "|" + rb + "|"
               + O.sig_conso.fillna(False).astype(bool).map({True: "C", False: "X"}) + "|"
               + O.sig_rs_intact.fillna(False).astype(bool).map({True: "R", False: "n"}) + "|"
               + pd.cut(O.sig_close.astype(float), [0, 21, 89, 377, 1e9],
                        labels=["p1", "p2", "p3", "p4"]).astype(str))
O["k_mid"] = O.family.astype(str) + "|" + rb

# ── walk-forward scores, fitted strictly before each test year ───────────────
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
print(f"walk-forward rows {len(W):,} · days {W.d.nunique():,} · "
      f"years {sorted(W.y.unique())}\n", flush=True)


def simulate(score_col: str, hold_bars: int, replace: bool, min_score: float = -1e9):
    """Day-by-day allocation with real slot contention.

    A held position exposes its own score; a candidate must beat it by REPL_EDGE before the
    swap is allowed, otherwise the book churns on noise."""
    REPL_EDGE = 1.0
    open_pos: list[dict] = []
    taken = []
    for day, dd in W.groupby("d", sort=True):
        open_pos = [p for p in open_pos if p["until"] > day]
        cand = (dd[dd[score_col] >= min_score]
                .sort_values(score_col, ascending=False)
                .drop_duplicates("dup_group"))
        held_names = {p["dup"] for p in open_pos}
        cand = cand[~cand.dup_group.isin(held_names)]
        free = SLOTS - len(open_pos)
        if free > 0 and len(cand):
            for _, r in cand.head(free).iterrows():
                open_pos.append({"dup": r.dup_group, "score": r[score_col],
                                 "until": day + pd.Timedelta(days=hold_bars * BAR2CAL)})
                taken.append({"y": r.y, "d": day, "ret": r.ret_pct, "setup": r.setup})
            cand = cand.iloc[free:]
        if replace and len(cand) and open_pos:
            worst = min(open_pos, key=lambda p: p["score"])
            best = cand.iloc[0]
            if best[score_col] > worst["score"] + REPL_EDGE:
                open_pos.remove(worst)
                open_pos.append({"dup": best.dup_group, "score": best[score_col],
                                 "until": day + pd.Timedelta(days=hold_bars * BAR2CAL)})
                taken.append({"y": best.y, "d": day, "ret": best.ret_pct,
                              "setup": best.setup})
    return pd.DataFrame(taken)


def report(P: pd.DataFrame, label: str) -> dict:
    if P.empty or len(P) < 30:
        print(f"  {label:44s} n={len(P)} too thin"); return {}
    yr = P.groupby("y")["ret"].mean()
    lo, hi = bootstrap_ci_clustered(P.ret, P.d.astype(str), stat="mean")
    ys = "".join(f"{yr.get(y, float('nan')):>7.2f}" for y in range(2022, 2027))
    print(f"  {label:44s} n={len(P):>5,} mean{P.ret.mean():>+7.2f} "
          f"[{lo:>+6.2f},{hi:>+6.2f}] |{ys} | {int((yr>0).sum())}/{len(yr)}yr "
          f"worst{yr.min():>+7.2f}", flush=True)
    return dict(label=label, n=len(P), mean=P.ret.mean(), lo=lo, hi=hi,
                worst=yr.min(), yrs=int((yr > 0).sum()))


print(f"{'='*126}")
print(f"ALLOCATOR v2 · {SLOTS} slots · walk-forward · purge {PURGE}d")
print(f"{'='*126}")
print(f"  {'configuration':44s} {'n':>7s} {'mean':>11s} {'CI(days)':>16s} |"
      f"{'2022':>7s}{'2023':>7s}{'2024':>7s}{'2025':>7s}{'2026':>7s} {'yrs':>7s} {'worst':>11s}",
      flush=True)

rows = []
print("\n── reference: today's rule, today's hold ──", flush=True)
rows.append(report(simulate("s_base", 60, False), "BASELINE (tier,median) · hold 60"))

print("\n── the score, holding it constant ──", flush=True)
rows.append(report(simulate("s_state", 60, False), "state · hold 60"))

print("\n── hold sweep: how often the ranking gets to speak ──", flush=True)
for h in (20, 30, 45):
    rows.append(report(simulate("s_state", h, False), f"state · hold {h}"))
    rows.append(report(simulate("s_base", h, False), f"  baseline · hold {h} (control)"))

print("\n── replacement ──", flush=True)
for h in (20, 30, 60):
    rows.append(report(simulate("s_state", h, True), f"state · hold {h} · REPLACE"))

R = pd.DataFrame([r for r in rows if r])
base = R[R.label == "BASELINE (tier,median) · hold 60"]
print(f"\n{'='*126}", flush=True)
if len(base):
    b = base.iloc[0]
    print(f"  reference: mean {b['mean']:+.2f} · worst {b.worst:+.2f} · {b.yrs}/5yr\n", flush=True)
    cand = R[(R["mean"] > b["mean"]) & (R.worst >= b.worst)]
    if len(cand):
        print("  ✅ CONFIGURATIONS THAT PASS (better mean AND not worse in 2022):", flush=True)
        print(cand.sort_values("mean", ascending=False)
              [["label", "n", "mean", "lo", "hi", "worst", "yrs"]]
              .to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)
    else:
        print("  ⛔ NOTHING PASSES — no configuration beats the current rule on both axes",
              flush=True)
print("=" * 126, flush=True)
R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "allocator_v2.csv"),
         index=False)
print("\n  → allocator_v2.csv\nDONE", flush=True)
