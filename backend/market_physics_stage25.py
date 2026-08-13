"""Market Physics · stage 2.5 — is any of this new? Descriptive, no outcome, no k.

The question I should have asked first, before independence and before invariance: does a
physics axis carry anything the NINE LINES ALREADY ENCODE. This project has described every bar
with t_sig · z_sig · l_sig · full_suffix · bar_body_wick · bar_gap_range · bar_line5 · vol_sig
for years. If `land_barrier_up` is predictable from those tokens, it is a re-encoding of the
incumbent with a physics name on it, and no amount of downstream rigour makes it new.

The test is the one stage 1 ran against my own parameterisation, pointed at the incumbent
instead: regress each physics axis on the one-hot alphabet and read the R².

    R² ≥ 0.7    RE-ENCODED — the alphabet already knows this
    0.4-0.7     overlaps
    R² < 0.4    genuinely new description

OUT OF SAMPLE, because it has to be. The alphabet expands to about 120 dummies, and 120 free
parameters will fit some of any target. A train/test split by DATE — not at random — answers the
question that was meant, and the date split matters: random rows from the same day leak the
day's market-wide state across the boundary.

WHY THIS CAN KILL THE WHOLE IDEA AND SHOULD BE ALLOWED TO. Stages 1 and 2 tested coherence and
invariance; both are properties of the vocabulary talking to itself. This is the first test where
the incumbent gets a vote, and it is cheap, so it comes before anything expensive.
"""
from __future__ import annotations

import json
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import market_physics as MP                                           # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPORT = os.path.join(HERE, "MARKET_PHYSICS_STAGE25.json")

# the incumbent description of a bar — the nine lines, as stored
ALPHABET = ["t_sig", "z_sig", "l_sig", "full_suffix", "bar_body_wick", "bar_gap_range",
            "bar_line5", "vol_sig"]
SPLIT_DATE = "2025-02-01"        # ~80/20 by date; declared before the numbers exist
MAX_LEVELS = 40                  # per column, rarest folded into "(other)"


def _load_alphabet(universe: str) -> pd.DataFrame:
    from studio.signal_stats import get_conn                          # noqa: PLC0415
    con = get_conn(read_only=True)
    cols = ", ".join(ALPHABET)
    q = (f"SELECT ticker, CAST(date AS VARCHAR) AS date, {cols} FROM bars "
         f"WHERE universe = '{universe}' "
         f"QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1")
    return con.execute(q).fetchdf()


def _design(d: pd.DataFrame) -> tuple:
    """One-hot the alphabet, rare levels folded together so a level seen twice cannot fit."""
    frames, names = [], []
    for c in ALPHABET:
        s = d[c].fillna("(empty)").astype(str)
        top = s.value_counts().head(MAX_LEVELS).index
        s = s.where(s.isin(top), "(other)")
        dm = pd.get_dummies(s, prefix=c, drop_first=True, dtype=float)
        frames.append(dm)
        names.extend(dm.columns)
    X = pd.concat(frames, axis=1)
    return X.to_numpy(float), names


def _oos_r2(Xtr, ytr, Xte, yte) -> float:
    """Ridge-free OLS with a pseudo-inverse; R² measured on the held-out block only."""
    A = np.c_[np.ones(len(Xtr)), Xtr]
    B = np.c_[np.ones(len(Xte)), Xte]
    beta, *_ = np.linalg.lstsq(A, ytr, rcond=None)
    pred = B @ beta
    ss_res = float(((yte - pred) ** 2).sum())
    ss_tot = float(((yte - yte.mean()) ** 2).sum())
    return 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan


def run(universe: str = "sp500") -> dict:
    t0 = time.time()
    phys = MP.load_and_compute(universe=universe, verbose=False)
    alpha = _load_alphabet(universe)
    phys["date"] = phys["date"].astype(str)
    d = phys.merge(alpha, on=["ticker", "date"], how="inner")
    print(f"  {len(d):,} rows joined ({time.time() - t0:.0f}s)", flush=True)

    cols = [c for c in MP.AXES_PROPOSED + MP.AXES_DECLARED]
    d = d.replace([np.inf, -np.inf], np.nan)
    X, names = _design(d)
    tr = (d["date"] < SPLIT_DATE).to_numpy()
    te = ~tr

    axes = {}
    for c in cols:
        y = d[c].to_numpy(float)
        ok = np.isfinite(y)
        a, b = tr & ok, te & ok
        if a.sum() < 5000 or b.sum() < 1000:
            axes[c] = {"skipped": f"train {int(a.sum())} test {int(b.sum())}"}
            continue
        # Rank WITHIN each block, never globally.
        #
        # The first version ranked over the whole sample and then split, which put a level shift
        # between train and test whenever the axis's distribution moved across the boundary —
        # and for the volatility-percentile family it moved a lot. The result was OOS R² of
        # −1.098, which reads as "worse than the mean" and was really "the intercept is fitted
        # to a different period's scale". Ranking inside each block asks the question that was
        # meant: can the alphabet place a bar within ITS OWN period's distribution.
        ytr = pd.Series(y[a]).rank(pct=True).to_numpy()
        yte = pd.Series(y[b]).rank(pct=True).to_numpy()
        r2 = _oos_r2(X[a], ytr, X[b], yte)
        axes[c] = {"oos_r2": round(float(r2), 3),
                   "n_train": int(a.sum()), "n_test": int(b.sum()),
                   "verdict": ("RE-ENCODED — the alphabet already knows this" if r2 >= 0.7
                               else "overlaps the alphabet" if r2 >= 0.4
                               else "new description")}
    return {"stage": ("2.5 — REDUNDANCY AGAINST THE INCUMBENT. Descriptive: no outcome column "
                      "was read, no k charged."),
            "universe": universe, "rows": int(len(d)), "split_date": SPLIT_DATE,
            "alphabet": ALPHABET, "n_dummies": len(names),
            "axes": axes, "seconds": round(time.time() - t0, 1)}


if __name__ == "__main__":
    r = run(sys.argv[1] if len(sys.argv) > 1 else "sp500")
    with open(REPORT, "w") as f:
        json.dump(r, f, indent=1, sort_keys=True)
    print("\n" + "=" * 96, flush=True)
    print("  STAGE 2.5 · DOES PHYSICS ADD ANYTHING THE NINE LINES DO NOT?", flush=True)
    print("=" * 96, flush=True)
    print(f"  {r['rows']:,} rows · {r['n_dummies']} alphabet dummies · "
          f"train < {r['split_date']} ≤ test · {r['seconds']}s\n", flush=True)
    rows = [(k, v) for k, v in r["axes"].items() if "oos_r2" in v]
    rows.sort(key=lambda kv: -kv[1]["oos_r2"])
    print(f"  {'axis':<26}{'OOS R²':>9}   verdict", flush=True)
    for c, v in rows:
        print(f"  {c:<26}{v['oos_r2']:>9.3f}   {v['verdict']}", flush=True)
    print(f"\n  written to {os.path.basename(REPORT)}", flush=True)
