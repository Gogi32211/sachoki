"""T5 x MTF confirmation with the $21-89 restriction REMOVED, segmented by price.

The standing rule is "segment every test by price" -- not "restrict to $21-89". This run
drops the restriction and reports every price bucket separately, so the question "does the
2-confirm effect exist outside $21-89" gets an answer instead of an assumption.

Note the universe filter already carries close >= 5, so the lowest bucket is $5-8, not $0-8.

This is a NEW pass over the SAME already-exposed history. It cannot upgrade the candidate's
status; it can only relocate where the effect appears to live.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/backend")
from mtf_t_signal_study import pull_1d, pull_confirms, T_SIGNALS        # noqa: E402

STUDY = "/Users/sachoki/MASSIVE_DATA/MTF_1D_X_LTF_STUDY"
BUCKETS = [(5, 8), (8, 21), (21, 89), (89, 200), (200, 10_000)]


def build_nogate(df, c1h, c15m):
    """Same join as the study, with the price predicate removed."""
    import duckdb, pandas as pd, numpy as np
    con = duckdb.connect(); con.execute("pragma threads=8")
    con.register("d1", df); con.register("h1", c1h); con.register("h15", c15m)
    j = con.execute("""
        SELECT d1.ticker, d1.date,
               CASE WHEN h1.ticker  IS NOT NULL THEN 1 ELSE 0 END AS n1,
               CASE WHEN h15.ticker IS NOT NULL THEN 1 ELSE 0 END AS n15
        FROM d1
        LEFT JOIN (SELECT DISTINCT ticker, d, t FROM h1) h1
               ON h1.ticker=d1.ticker AND h1.d=CAST(d1.date AS DATE) AND h1.t=d1.t
        LEFT JOIN (SELECT DISTINCT ticker, d, t FROM h15) h15
               ON h15.ticker=d1.ticker AND h15.d=CAST(d1.date AS DATE) AND h15.t=d1.t
        WHERE d1.t <> ''""").df()                       # <-- no price gate
    con.close()
    df = df.copy()
    k = pd.MultiIndex.from_arrays([df.ticker, pd.to_datetime(df.date)])
    jk = pd.MultiIndex.from_arrays([j.ticker, pd.to_datetime(j.date)])
    nc = pd.Series(0, index=k); nc.loc[jk] = (j.n1 + j.n15).to_numpy()
    return df, nc.to_numpy()


def main():
    import pandas as pd, numpy as np
    from edge_replay import _pathsim, _stats
    t0 = time.time()
    out_p = os.path.join(STUDY, "MTF_T5_PRICE_OPEN.json")
    if os.path.exists(out_p):
        os.remove(out_p)                       # never leave a stale result readable

    df, as_of = pull_1d()
    c1h = pull_confirms("studio_1h.duckdb", as_of)
    c15m = pull_confirms("studio_15m.duckdb", as_of)
    df, nconf = build_nogate(df, c1h, c15m)
    print(f"joined ({time.time()-t0:.0f}s)", flush=True)

    cl = df.close.to_numpy()
    for T in ("T5", "T1G", "T12"):
        fire = (df.t.to_numpy() == T)
        for g in (0, 1, 2):
            df[f"{T}_ALLP_c{g}"] = fire & (nconf == g)          # every price
            for lo, hi in BUCKETS:
                df[f"{T}_{lo}_c{g}"] = fire & (nconf == g) & (cl >= lo) & (cl < hi)
    grp = {tk: g.reset_index(drop=True) for tk, g in df.groupby("ticker", sort=False)}
    print(f"{len(grp):,} frames ({time.time()-t0:.0f}s)", flush=True)

    rows = {}
    for T in ("T5", "T1G", "T12"):
        print(f"\n{T}")
        for tag in ["ALLP"] + [str(lo) for lo, _ in BUCKETS]:
            line = []
            for g in (0, 1, 2):
                s = _stats("x", _pathsim(grp, f"{T}_{tag}_c{g}", "trail",
                                         .10, .25, .25, 60, atr_k=12.0))
                rows[f"{T}|{tag}|{g}"] = s
                line.append(f"c{g} n={s['n']:>6,d} med={s['median']:+5.2f} "
                            f"{s['pos_years']}/6 w={s['worst_year']:+6.2f}"
                            if s["n"] else f"c{g} n=0")
            lab = "ALL PRICES" if tag == "ALLP" else f"${tag}+"
            print(f"  {lab:>11}  " + " | ".join(line), flush=True)

    json.dump({"as_of": as_of, "buckets": BUCKETS,
               "run_id": time.strftime("PRICEOPEN_%Y%m%dT%H%M%SZ", time.gmtime()),
               "rows": rows}, open(out_p, "w"), indent=1, default=str)
    print(f"\nwritten -> {out_p} ({time.time()-t0:.0f}s)", flush=True)


if __name__ == "__main__":
    main()
