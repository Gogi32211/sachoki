"""
edge_overfit.py — DSR + PBO over the Edge Replay setups.

Runs the SAME path-sim as the Edge Replay tab (trail25/60, gap-realistic, 15bps,
cooldown-5) over the full window, then:
  · per setup: trade-level Sharpe → PSR vs 0 → DSR deflated against the family
    (n_trials defaults to the family size; --trials lets us be honest about the
    many variants tried-and-discarded during research, e.g. 100).
  · family PBO via CSCV on the month × setup return matrix (sum of trade returns
    per calendar month; empty months = 0 exposure).

Writes edge_overfit.json next to this file (served by /api/edge-overfit).
Usage:  python edge_overfit.py [--months 62] [--trials 100]
"""
from __future__ import annotations
import argparse, json, os, time
import numpy as np
import pandas as pd

import edge_replay as ER
from overfit_stats import sharpe, psr, dsr, pbo_cscv

OUT = os.path.join(os.path.dirname(__file__), "edge_overfit.json")


def run(months: int = 62, dv_floor: float = 3_000_000, n_trials: int = None) -> dict:
    t0 = time.time()
    grp, as_of = ER._frame(int(months), float(dv_floor))
    per_setup: dict[str, pd.DataFrame] = {}
    for name, col in ER.SETUPS:
        # ATR-adaptive exit = the book default since 2026-08-06 (law_exit_geometry)
        tr = ER._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
        if len(tr):
            per_setup[name] = tr

    # family of trial Sharpes (trade-level) — the deflation benchmark
    fam_srs = {n: sharpe(tr["ret"].to_numpy()) for n, tr in per_setup.items()}
    trials = int(n_trials or len(fam_srs))

    rows = []
    for name, tr in per_setup.items():
        r = tr["ret"].to_numpy(float)
        d = dsr(r, list(fam_srs.values()), n_trials=trials)
        rows.append({
            "setup": name, "n": int(len(r)),
            "mean_pct": round(float(r.mean()) * 100, 2),
            "sr_trade": d["sr"],
            "psr0": round(psr(r, 0.0), 4),      # P(true edge > 0), no deflation
            "sr_star": d["sr_star"],            # what max-SR pure luck would show
            "dsr": d["dsr"],                    # P(edge > luck-of-N-trials)
        })
    rows.sort(key=lambda x: x["dsr"], reverse=True)

    # month × setup matrix for CSCV-PBO (sum of trade rets per calendar month)
    names = list(per_setup.keys())
    mon_all = sorted({m for tr in per_setup.values() for m in tr["date_in"].str[:7].unique()})
    M = np.zeros((len(mon_all), len(names)))
    mi = {m: i for i, m in enumerate(mon_all)}
    for j, n_ in enumerate(names):
        g = per_setup[n_].groupby(per_setup[n_]["date_in"].str[:7])["ret"].sum()
        for m, v in g.items():
            M[mi[m], j] = v
    pbo = pbo_cscv(M, S=8)

    out = {"as_of": as_of, "months": int(months), "n_trials_assumed": trials,
           "elapsed_s": round(time.time() - t0, 1),
           "note": ("DSR = P(true edge beats the luck-of-N-trials benchmark SR*); "
                    ">=0.95 strong, >=0.75 decent, <0.5 indistinguishable from selection luck. "
                    "PBO = P(the in-sample-best setup ranks below median out-of-sample); "
                    "<=0.2 healthy family, ~0.5 pure noise."),
           "pbo": pbo, "rows": rows}
    with open(OUT, "w") as f:
        json.dump(out, f)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=62)
    ap.add_argument("--trials", type=int, default=None,
                    help="assumed total variants tested (deflation N); default = family size")
    a = ap.parse_args()
    r = run(months=a.months, n_trials=a.trials)
    print(f"as_of {r['as_of']} · {r['months']}mo · N_trials={r['n_trials_assumed']} · {r['elapsed_s']}s")
    p = r["pbo"]
    print(f"FAMILY PBO = {p['pbo']}  (splits {p['n_splits']}, IS→OOS SR {p['is_best_sr_mean']}→{p['oos_of_is_best_sr_mean']}, ratio {p['oos_is_ratio']})\n")
    print(f"  {'setup':18s} {'n':>6s} {'mean%':>7s} {'SR':>7s} {'PSR0':>6s} {'SR*':>6s} {'DSR':>6s}")
    for x in r["rows"]:
        print(f"  {x['setup']:18s} {x['n']:6d} {x['mean_pct']:7.2f} {x['sr_trade']:7.3f} "
              f"{x['psr0']:6.3f} {x['sr_star']:6.3f} {x['dsr']:6.3f}")
