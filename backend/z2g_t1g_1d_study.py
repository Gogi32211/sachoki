"""Z2G[1] -> T1G, 1D-NATIVE — mine / verify / full.

SCOPE, decided by the user 2026-09-02: 1D ONLY. The cross-timeframe echo leg was DROPPED,
not deferred with a workaround: `_pathsim`'s trail is `clip(atr_k*ATR%, 0.15, 0.60)` with
those bounds hardcoded (edge_replay.py:1994-1996). On 15m the median ATR% is 0.462, so
12*ATR% = 5.5% is clipped UP to 15% -- a trail ~3x wider than the timeframe, which would
never trigger inside maxh=60 bars and would silently turn a path-sim into a fixed-horizon
forward-return test. Making it correct needs new clip bounds inside `_pathsim`, and that
file is not to be modified. So the intraday claim is simply NOT MADE.

THE CLAIM UNDER TEST is not "the pattern is profitable". Mining already showed it is not,
in absolute terms (PF 0.93). It is the CONDITIONAL effect, measured inside the T1G
population so composition cannot explain it:

    does a Z2G on the PRIOR bar change what a T1G is worth?
    mine: T1G without Z2G -1.11  ->  T1G after Z2G +0.05   (+1.16)

OOS discipline: 2024-01-01 .. 2026-09-01 was reserved before mining and is opened here.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DATA = os.path.join(os.path.dirname(HERE), "data")
STUDY = "/Users/sachoki/MASSIVE_DATA/Z2G_T1G_ECHO"
MINE = ("2021-05-26", "2023-12-31")
VERIFY = ("2024-01-01", "2026-09-01")
FULL = ("2021-05-26", "2026-09-01")
BUCKETS = [(5, 8), (8, 21), (21, 89), (89, 200), (200, 10_000)]


def pull(start, end):
    import duckdb
    a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
    a.execute("pragma threads=8")
    df = a.execute(f"""
        WITH r AS (
          SELECT ticker, date, open, high, low, close, atr_14, z_sig, t_sig,
                 row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
          FROM bars
          WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
            AND universe<>'index'
            AND date >= DATE '{start}' AND date <= DATE '{end}')
        SELECT ticker, date, open, high, low, close, atr_14,
               coalesce(z_sig,'') z, coalesce(t_sig,'') t
        FROM r WHERE rn=1 ORDER BY ticker, date""").df()
    a.close()
    return df


def masks(df):
    import numpy as np
    df = df.reset_index(drop=True)
    # groupby-shift, so the prior bar never leaks across a ticker boundary
    pz = df.groupby("ticker", sort=False)["z"].shift(1).fillna("").to_numpy()
    cl = df.close.to_numpy()
    t1g = df.t.to_numpy() == "T1G"
    p21 = cl >= 21
    df["PAT"] = t1g & (pz == "Z2G") & p21
    df["T1G_ONLY"] = t1g & (pz != "Z2G") & p21
    df["T1G_ALL"] = t1g & p21
    df["Z2G_ALL"] = (df.z.to_numpy() == "Z2G") & p21
    df["CTRL"] = False
    df.iloc[np.where(p21)[0][::40], df.columns.get_loc("CTRL")] = True
    for blo, bhi in BUCKETS:
        df[f"PAT_{blo}"] = t1g & (pz == "Z2G") & (cl >= blo) & (cl < bhi)
        df[f"ONLY_{blo}"] = t1g & (pz != "Z2G") & (cl >= blo) & (cl < bhi)
    return df


def phase(label, win, out):
    from edge_replay import _pathsim, _stats
    df = masks(pull(*win))
    grp = {t: g.reset_index(drop=True) for t, g in df.groupby("ticker", sort=False)}
    print(f"\n=== {label}  {str(df.date.min())[:10]} .. {str(df.date.max())[:10]} "
          f"· {df.ticker.nunique():,} tickers ===")
    print("  cell               n      med    mean   win     pf   yrs    worst")
    print("  " + "-" * 66)

    def line(tag, col):
        s = _stats(tag, _pathsim(grp, col, "trail", .10, .25, .25, 60, atr_k=12.0))
        out[f"{label}|{tag}"] = s
        if not s["n"]:
            print(f"  {tag:16s}  n=0"); return None
        print(f"  {tag:16s} {s['n']:>7,d}  {s['median']:+6.2f}  {s['mean']:+6.2f}  "
              f"{s['win']:4.1f}  {s['pf']:5.2f}  {s['pos_years']}/{s['total_years']}  "
              f"{s['worst_year']:+6.2f}", flush=True)
        return s

    line("BASELINE", "CTRL")
    line("Z2G (all)", "Z2G_ALL")
    line("T1G (all)", "T1G_ALL")
    a = line("T1G w/o Z2G", "T1G_ONLY")
    b = line("PATTERN", "PAT")
    if a and b:
        print(f"  {'CONDITIONAL LIFT':16s} {b['median'] - a['median']:+6.2f}  "
              f"(pattern minus T1G-without-Z2G, measured inside the T1G population)")
    print()
    for blo, bhi in BUCKETS:
        x = line(f"  PAT ${blo}-{bhi}", f"PAT_{blo}")
        y = line(f"  only ${blo}-{bhi}", f"ONLY_{blo}")
        if x and y:
            print(f"       -> lift {x['median'] - y['median']:+6.2f}")
    return out


def main():
    t0 = time.time()
    os.makedirs(STUDY, exist_ok=True)
    p = os.path.join(STUDY, "RESULT_1D.json")
    if os.path.exists(p):
        os.remove(p)
    out = {}
    phase("MINE", MINE, out)
    phase("VERIFY", VERIFY, out)
    phase("FULL", FULL, out)
    json.dump(dict(scope="1D only — intraday echo leg dropped, _pathsim not modified",
                   claim="conditional: does a prior-bar Z2G change what a T1G is worth",
                   windows=dict(mine=MINE, verify=VERIFY, full=FULL),
                   run_id=time.strftime("Z2GT1G_%Y%m%dT%H%M%SZ", time.gmtime()),
                   rows=out), open(p, "w"), indent=1, default=str)
    print(f"\nwritten -> {p} ({time.time()-t0:.0f}s)")


if __name__ == "__main__":
    main()
