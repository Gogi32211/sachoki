"""L2 / L3 gates for the 1D T-signal x MTF confirmation study.

L1 already ran in mtf_t_signal_study.py. This adds only what L1 cannot answer:

  L2  baseline + 1pp   -- two controls, both scored by the SAME exit engine:
                          (a) unconditional: every bar in the $21-89 population
                          (b) pooled: every 1D T fire, confirm-blind
  L3  n per bucket     -- from L1
      plateau          -- does the confirm ladder hold inside each price bucket,
                          and does it hold when the confirm is required on only
                          one specific lower timeframe?
      DSR              -- deflated Sharpe against the FULL search: 12 T x 4 groups

Reuses the cached mask frame. Does not reimplement the exit engine.
"""
from __future__ import annotations
import os, sys, json                                                    # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

CACHE = "/Users/sachoki/MASSIVE_DATA/_mtf_masks.parquet"
OUT = "/Users/sachoki/MASSIVE_DATA/MTF_T_SIGNAL_L23.json"
T_SIGNALS = ["T1", "T1G", "T2", "T2G", "T3", "T4", "T5", "T6", "T9", "T10", "T11", "T12"]
ATR_K, MAXH, TRAIL = 12.0, 60, 0.25


def _sim(grp, col):
    from edge_replay import _pathsim, _stats
    tr = _pathsim(grp, col, "trail", 0.10, 0.25, TRAIL, MAXH, atr_k=ATR_K)
    s = _stats(col, tr)
    return s, tr


def main():
    import numpy as np, pandas as pd
    from overfit_stats import sharpe, dsr

    df = pd.read_parquet(CACHE)
    df["yr"] = pd.to_datetime(df.date).dt.year
    price_ok = (df.close >= 21) & (df.close <= 89)
    out = {}

    # ---- L2 controls -----------------------------------------------------
    # (a) unconditional. Every 40th bar in the population, so the control has the
    #     same universe, price gate and exit rule and differs ONLY in entry.
    #     Thinning is deterministic (row order is ticker,date) -- not a random draw.
    df["CTRL_uncond"] = False
    idx = np.where(price_ok.to_numpy())[0][::40]
    df.iloc[idx, df.columns.get_loc("CTRL_uncond")] = True
    # (b) pooled T: any T fire regardless of confirmation
    df["CTRL_anyT"] = np.logical_or.reduce([df[f"{T}_any"].to_numpy() for T in T_SIGNALS])

    grp = {tk: g.reset_index(drop=True) for tk, g in df.groupby("ticker", sort=False)}
    print(f"{len(grp):,} ticker frames", flush=True)

    for c in ("CTRL_uncond", "CTRL_anyT"):
        s, _ = _sim(grp, c)
        out[c] = s
        print(f"  {c:12s} n={s['n']:>7,d} med={s['median']:+.2f} win={s['win']:.1f} "
              f"pf={s['pf']:.2f} yrs={s['pos_years']}/{s['total_years']}", flush=True)

    # ---- L3 plateau: price buckets --------------------------------------
    # A real effect survives inside sub-ranges. A price artefact does not.
    BUCKETS = [(21, 40), (40, 60), (60, 89)]
    plat = {}
    for T in ("T5", "T1G", "T12"):
        for lo, hi in BUCKETS:
            pb = (df.close >= lo) & (df.close < hi)
            for g in ("c0", "c1", "c2"):
                col = f"PB_{T}_{lo}_{g}"
                df[col] = df[f"{T}_{g}"].to_numpy() & pb.to_numpy()
    grp = {tk: g.reset_index(drop=True) for tk, g in df.groupby("ticker", sort=False)}
    for T in ("T5", "T1G", "T12"):
        for lo, hi in BUCKETS:
            row = []
            for g in ("c0", "c1", "c2"):
                s, _ = _sim(grp, f"PB_{T}_{lo}_{g}")
                row.append((s.get("n", 0), s.get("median", None)))
            plat[f"{T}_${lo}-{hi}"] = row
            print(f"  plateau {T:4s} ${lo}-{hi}: " + "  ".join(
                f"c{i} n={n:>6,d} med={m if m is not None else '--'}"
                for i, (n, m) in enumerate(row)), flush=True)
    out["plateau_price"] = {k: [[n, m] for n, m in v] for k, v in plat.items()}

    # ---- L3 DSR ----------------------------------------------------------
    # The search that produced the winner was 12 signals x 4 groups = 48 cells.
    # DSR must be charged the full 48, not the 1 that survived.
    res = json.load(open("/Users/sachoki/MASSIVE_DATA/MTF_T_SIGNAL_STUDY_RESULT.json"))
    trial_srs = []
    for r in res["rows"]:
        if r["n"] and r.get("mean") is not None and r.get("sortino") is not None:
            trial_srs.append(r["exp_r"])
    out["n_trials"] = len(trial_srs)
    for T, g in (("T5", "c2"), ("T1G", "c2"), ("T12", "c2")):
        _, tr = _sim(grp, f"{T}_{g}") if f"{T}_{g}" in df.columns else (None, None)
        if tr is None or not len(tr):
            continue
        rr = (tr["ret"].to_numpy() / 100.0)
        d = dsr(rr, trial_srs, n_trials=len(trial_srs))
        out[f"dsr_{T}_{g}"] = d
        print(f"  DSR {T}_{g}: sr={d.get('sr'):.4f} dsr={d.get('dsr'):.3f} "
              f"k={len(trial_srs)}", flush=True)

    json.dump(out, open(OUT, "w"), indent=1, default=str)
    print(f"\nwritten -> {OUT}", flush=True)


if __name__ == "__main__":
    main()
