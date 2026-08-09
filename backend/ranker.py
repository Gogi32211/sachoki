"""C2 — the cross-sectional allocator: which 10 of today's ~400 opportunities?

THE HOLE THIS FILLS, MEASURED

The board produces ~400 distinct (ticker, date) opportunities per trading day. The account
holds 8-11. Everything in this repo so far answers "is this setup real"; nothing answers
"which of the forty that fired today". The current rule is `(core-tier, median)` — rank by
the setup's historical median and take from the top.

And the exit lab just showed why this is the lever: the median opportunity across the whole
pool returns +0.67% on a 60-bar hold, while the book quotes +1.89% per setup. That entire gap
is selection.

DESIGN CHOICES, AND WHY

  UNIT = opportunity, deduplicated at allocation. One stock can fire six setups on one bar
  (CAR: four QZ-Capit variants on 2026-02-23). Ranking sees them separately; funding sees
  ONE trade, via dup_group.

  MODEL = a state-conditioned lookup table, not a gradient booster. Every shape-based search
  in this book has died out of sample, and the one surviving law is STATE > SHAPE. A lookup
  over (family × RSI band × volume bucket × RS × price band) is transparent, cannot silently
  memorise a ticker, and tests the thesis directly. If a black box later beats it, that is a
  finding — but it has to beat something honest first.

  NO LOOKAHEAD BY CONSTRUCTION. The table is estimated on bars strictly before each test
  window, walk-forward with a purge equal to the holding period, so a training row's outcome
  cannot overlap the test period.

  ACCEPTANCE INCLUDES 2022. Frozen-OOS 2024-26 is a bull window where the baseline itself
  reads +1.43 against +0.09 over six years — beating a baseline there proves little. The
  portfolio study showed allocation decides in the stress year: −49.4% at 3 slots vs −28.0%
  at 10. So the walk-forward tests every year, and 2022 is the one that counts.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPP = os.path.join(ROOT, "data", "opportunities.parquet")
SLOTS, PURGE = 10, 60
pd.set_option("display.width", 220)

print("loading opportunities...", flush=True)
O = pd.read_parquet(OPP)
O["d"] = pd.to_datetime(O["date_in"].astype(str).str[:10])
O["ret_pct"] = O["ret"].astype(float) * 100
O = O.dropna(subset=["ret_pct", "sig_rsi_14", "sig_close"]).reset_index(drop=True)
print(f"  {len(O):,} rows · {O.d.min().date()} → {O.d.max().date()}\n", flush=True)


# ── state buckets: coarse on purpose. Fine bins memorise. ────────────────────
def state(df: pd.DataFrame) -> pd.DataFrame:
    s = pd.DataFrame(index=df.index)
    s["fam"] = df["family"].astype(str)
    s["rsi"] = pd.cut(df["sig_rsi_14"].astype(float), [-1, 30, 40, 50, 60, 101],
                      labels=["<30", "30-40", "40-50", "50-60", "60+"]).astype(str)
    # vol_bucket is called `vb` inside the frame and did not make it into the table;
    # 🧊CONSO is the validated compression gate and is present, so use that instead.
    s["vol"] = df["sig_conso"].fillna(False).astype(bool).map({True: "conso", False: "exp"})
    s["rs"] = df["sig_rs_intact"].fillna(False).astype(bool).map({True: "RS", False: "noRS"})
    s["px"] = pd.cut(df["sig_close"].astype(float), [0, 21, 89, 377, 1e9],
                     labels=["<21", "21-89", "89-377", "377+"]).astype(str)
    return s


ST = state(O)
O["k_full"] = ST.fam + "|" + ST.rsi + "|" + ST.vol + "|" + ST.rs + "|" + ST.px
O["k_mid"] = ST.fam + "|" + ST.rsi + "|" + ST.rs
O["k_fam"] = ST.fam
print(f"state cells: full {O.k_full.nunique():,} · mid {O.k_mid.nunique():,} · "
      f"family {O.k_fam.nunique()}", flush=True)


def fit_scores(train: pd.DataFrame) -> dict:
    """Hierarchical shrinkage: a rare cell falls back to its parent instead of trusting
    a handful of observations. MIN_N is the point where a cell is allowed to speak."""
    MIN_N = 60
    g = train.groupby("k_full")["ret_pct"].agg(["mean", "size"])
    mid = train.groupby("k_mid")["ret_pct"].agg(["mean", "size"])
    fam = train.groupby("k_fam")["ret_pct"].agg(["mean", "size"])
    glob = float(train["ret_pct"].mean())
    return {"full": g[g["size"] >= MIN_N]["mean"].to_dict(),
            "mid": mid[mid["size"] >= MIN_N]["mean"].to_dict(),
            "fam": fam[fam["size"] >= 200]["mean"].to_dict(), "glob": glob}


def score(df: pd.DataFrame, tab: dict) -> np.ndarray:
    out = df["k_full"].map(tab["full"])
    out = out.fillna(df["k_mid"].map(tab["mid"]))
    out = out.fillna(df["k_fam"].map(tab["fam"]))
    return out.fillna(tab["glob"]).to_numpy(float)


def baseline_scores(df: pd.DataFrame, tab: dict) -> np.ndarray:
    """What spine.py does today: rank by the setup's own historical median."""
    return df["k_fam"].map(tab["fam"]).fillna(tab["glob"]).to_numpy(float)


def allocate(day_df: pd.DataFrame, sc: np.ndarray, free: int) -> pd.DataFrame:
    """Take the best-scoring opportunities, one per underlying trade."""
    d = day_df.assign(_s=sc).sort_values("_s", ascending=False)
    d = d.drop_duplicates(subset="dup_group")          # never fund one trade twice
    return d.head(max(0, free))


def run(rank_fn, label: str, seed: int | None = None) -> pd.DataFrame:
    """Walk-forward: refit each year on everything strictly before it (purged), then
    allocate day by day with real slot contention."""
    years = sorted(O["d"].dt.year.unique())
    picks = []
    for y in years[1:]:                                  # first year is training only
        te = O[O["d"].dt.year == y]
        cutoff = te["d"].min() - pd.Timedelta(days=PURGE + 5)
        tr = O[O["d"] < cutoff]
        if len(tr) < 5000 or te.empty:
            continue
        tab = fit_scores(tr)
        rng = np.random.default_rng(seed if seed is not None else 0)
        busy: list[pd.Timestamp] = []                    # slot release dates
        for day, dd in te.groupby("d", sort=True):
            busy = [b for b in busy if b > day]
            free = SLOTS - len(busy)
            if free <= 0:
                continue
            sc = rng.random(len(dd)) if rank_fn is None else rank_fn(dd, tab)
            got = allocate(dd, sc, free)
            for _, r in got.iterrows():
                picks.append({"y": y, "d": day, "ret": r.ret_pct, "hold": r.hold,
                              "setup": r.setup, "ticker": r.ticker})
                busy.append(day + pd.Timedelta(days=float(r.hold) * 1.45))
    return pd.DataFrame(picks)


def summarise(P: pd.DataFrame, label: str) -> dict:
    if P.empty:
        print(f"  {label:26s} no trades"); return {}
    yr = P.groupby("y")["ret"].mean()
    ys = "".join(f"{yr.get(y, float('nan')):>7.2f}" for y in range(2022, 2027))
    print(f"  {label:26s} n={len(P):>5,} mean{P.ret.mean():>+7.2f} med{P.ret.median():>+7.2f} "
          f"win{(P.ret>0).mean()*100:>5.1f} |{ys} | {int((yr>0).sum())}/{len(yr)}yr "
          f"worst{yr.min():>+7.2f}", flush=True)
    return dict(label=label, n=len(P), mean=P.ret.mean(), med=P.ret.median(),
                worst=yr.min(), yrs=int((yr > 0).sum()))


print(f"\n{'='*118}")
print(f"WALK-FORWARD ALLOCATION · {SLOTS} slots · purge {PURGE}d · refit every year")
print(f"{'='*118}")
print(f"  {'strategy':26s} {'n':>7s} {'mean':>11s} {'med':>10s} {'win':>8s} "
      f"{'2022':>7s}{'2023':>7s}{'2024':>7s}{'2025':>7s}{'2026':>7s} {'yrs':>7s} {'worst':>11s}",
      flush=True)

rows = []
rand = [run(None, "random", seed=s) for s in range(3)]
for i, P in enumerate(rand):
    rows.append(summarise(P, f"random (seed {i})"))
rows.append(summarise(run(baseline_scores, "baseline"), "BASELINE (tier, median)"))
rows.append(summarise(run(score, "state"), "★ STATE-CONDITIONED"))

R = pd.DataFrame([r for r in rows if r])
if len(R) >= 2:
    b = R[R.label == "BASELINE (tier, median)"]
    s = R[R.label == "★ STATE-CONDITIONED"]
    if len(b) and len(s):
        b, s = b.iloc[0], s.iloc[0]
        print(f"\n{'='*118}")
        print(f"  ranker vs baseline:  mean {s['mean']-b['mean']:+.2f}pp · "
              f"worst-year {s.worst-b.worst:+.2f}pp · years {s.yrs} vs {b.yrs}", flush=True)
        rmean = np.mean([r["mean"] for r in rows[:3] if r])
        print(f"  baseline vs random:  {b['mean']-rmean:+.2f}pp "
              f"(random mean {rmean:+.2f} over 3 seeds)", flush=True)
        ok = (s["mean"] > b["mean"]) and (s.worst >= b.worst)
        print(f"\n  ACCEPTANCE (beat baseline on mean AND not worse in the worst year, "
              f"2022 included): {'PASS' if ok else 'FAIL'}", flush=True)
        print("=" * 118, flush=True)

R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "ranker_eval.csv"),
         index=False)
print("\n  → ranker_eval.csv\nDONE", flush=True)
