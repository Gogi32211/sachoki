"""What T5 x MTF confirmation actually does BELOW $21.

The price-open run showed $5-8 with the highest medians of any bucket (+4.08 / +4.58 /
+6.61). That number cannot be read on its own: the exit rule is ATR-scaled
(trail = clip(12*ATR%, 15, 60)), so a volatile $6 stock is traded with a far wider stop
than a $60 one and BOTH tails grow. A bigger median there may be more risk taken, not more
edge.

So this run reports, per sub-$21 bucket, the numbers that separate those two readings:
  median / mean  -- level
  win / pf       -- is the middle trade actually winning
  med_mae        -- heat taken to get it   <-- the deciding column
  med_mfe        -- room given
  conc_top10pct  -- how much of total profit sits in the top 10% of trades
  per-year       -- does it survive 2022

Same exposed history as everything else in this family. Diagnostic, not promotion.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from mtf_t_signal_study import pull_1d, pull_confirms                   # noqa: E402
from mtf_t5_price_open import build_nogate                              # noqa: E402

STUDY = "/Users/sachoki/MASSIVE_DATA/MTF_1D_X_LTF_STUDY"
SUB = [(5, 8), (8, 12), (12, 16), (16, 21), (21, 89)]


def main():
    import pandas as pd, numpy as np
    from edge_replay import _pathsim, _stats
    t0 = time.time()
    out_p = os.path.join(STUDY, "MTF_T5_SUB21.json")
    if os.path.exists(out_p):
        os.remove(out_p)

    df, as_of = pull_1d()
    df, nconf = build_nogate(df, pull_confirms("studio_1h.duckdb", as_of),
                             pull_confirms("studio_15m.duckdb", as_of))
    cl = df.close.to_numpy()
    fire = (df.t.to_numpy() == "T5")
    for lo, hi in SUB:
        for g in (0, 1, 2):
            df[f"S_{lo}_{g}"] = fire & (nconf == g) & (cl >= lo) & (cl < hi)
    grp = {tk: g.reset_index(drop=True) for tk, g in df.groupby("ticker", sort=False)}
    print(f"joined, {len(grp):,} frames ({time.time()-t0:.0f}s)\n", flush=True)

    rows = {}
    hdr = ("  bucket    conf       n     med    mean   win     pf   med_MAE  med_MFE  "
           "top10%  yrs   worst")
    print(hdr); print("  " + "-" * (len(hdr) - 2))
    for lo, hi in SUB:
        for g in (0, 1, 2):
            tr = _pathsim(grp, f"S_{lo}_{g}", "trail", .10, .25, .25, 60, atr_k=12.0)
            s = _stats(f"T5_${lo}-{hi}_c{g}", tr)
            rows[f"{lo}|{g}"] = s
            if not s["n"]:
                print(f"  ${lo:>3}-{hi:<3}   c{g}      n=0"); continue
            print(f"  ${lo:>3}-{hi:<3}   c{g}  {s['n']:>6,d}  {s['median']:+6.2f}  "
                  f"{s['mean']:+6.2f}  {s['win']:4.1f}  {s['pf']:5.2f}   "
                  f"{s['med_mae']:+6.2f}  {s['med_mfe']:+6.2f}   {s['conc_top10pct']:4.0f}  "
                  f"{s['pos_years']}/6  {s['worst_year']:+6.2f}", flush=True)
        print()

    # 2022 is the one bear year in the window -- the honest stress test for a cheap-stock
    # result that leans on 2021 and 2023-2026.
    print("  per-year medians, c2 only")
    for lo, hi in SUB:
        s = rows[f"{lo}|2"]
        if s["n"]:
            py = s["per_year"]
            print(f"  ${lo:>3}-{hi:<3}  " + "  ".join(
                f"{y}:{py[y]:+6.2f}" for y in sorted(py)), flush=True)

    json.dump({"as_of": as_of, "buckets": SUB, "rows": rows,
               "run_id": time.strftime("SUB21_%Y%m%dT%H%M%SZ", time.gmtime())},
              open(out_p, "w"), indent=1, default=str)
    print(f"\nwritten -> {out_p} ({time.time()-t0:.0f}s)", flush=True)


if __name__ == "__main__":
    main()
