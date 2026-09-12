"""Z2G x lower-TF reversal — MINING PHASE ONLY (2020-06 .. 2023-12-31).

PLAN: /Users/sachoki/MASSIVE_DATA/Z2G_REVERSAL_STUDY/PLAN_APPROVED.md

The 2024-01-01 .. 2026-09-01 verification window is RESERVED. This file refuses to read a
bar dated on or after MINE_END, enforced by an assertion on the pulled frame -- not by
discipline. The verify script is separate and is written only after the mining verdict is
frozen.

CAUSALITY

Z2G fires on D. The reversal may arrive up to D+3, so entering at D+1 would use bars that
had not happened. Instead the mask is set on the DECISION bar:

    confirmed   d* = first day in [D, D+3] with a bullish T on 1h and/or 15m
                -> mask on d*, path-sim enters d*+1 open, group = #TFs firing on d*
    unconfirmed the window is known empty only at the close of D+3
                -> mask on D+3, entry D+4 open

so every entry uses only information available before it.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DATA = os.path.join(os.path.dirname(HERE), "data")
STUDY = "/Users/sachoki/MASSIVE_DATA/Z2G_REVERSAL_STUDY"
MINE_START, MINE_END = "2020-06-01", "2023-12-31"
WINDOW = 3                     # sessions after D, inclusive of D
BUCKETS = [(5, 8), (8, 21), (21, 89), (89, 200), (200, 10_000)]
ATR_K, MAXH, TRAIL = 12.0, 60, 0.25


class OOSLeak(RuntimeError):
    pass


def pull():
    """1D frame for the MINING window only, plus the lower-TF bullish-T session sets."""
    import duckdb, pandas as pd
    a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
    a.execute("pragma threads=8")
    df = a.execute(f"""
        WITH r AS (
          SELECT ticker, date, open, high, low, close, atr_14, z_sig,
                 row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
          FROM bars
          WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= 3000000
            AND universe <> 'index'
            AND date >= DATE '{MINE_START}' AND date <= DATE '{MINE_END}'
        )
        SELECT ticker, date, open, high, low, close, atr_14,
               coalesce(z_sig,'') z FROM r WHERE rn = 1
        ORDER BY ticker, date""").df()
    a.close()
    if str(df.date.max())[:10] > MINE_END:
        raise OOSLeak(f"a bar dated {df.date.max()} passed the mining boundary {MINE_END}")

    def bullT(fname):
        c = duckdb.connect(os.path.join(DATA, fname), read_only=True)
        c.execute("pragma threads=8")
        t = c.execute(f"""SELECT DISTINCT ticker, CAST(date AS DATE) d FROM bars
                          WHERE t_sig IS NOT NULL AND t_sig <> ''
                            AND CAST(date AS DATE) >= DATE '{MINE_START}'
                            AND CAST(date AS DATE) <= DATE '{MINE_END}'""").df()
        c.close()
        return t
    return df, bullT("studio_1h.duckdb"), bullT("studio_15m.duckdb")


def build(df, h1, m15, any_mode=False):
    """Causal masks. any_mode=True builds the ACTIVITY CONFOUND control instead: the echo
    is 'the stock had any lower-TF T that day' regardless of it being a reversal after a
    Z2G -- which for the T family is the same predicate, so in this study the confound
    control is the SIGNAL-BLIND version: any lower-TF bar at all present that session."""
    import numpy as np, pandas as pd
    df = df.copy().reset_index(drop=True)
    d = pd.to_datetime(df.date).dt.date.to_numpy()
    tk = df.ticker.to_numpy()
    h1s = set(zip(h1.ticker, pd.to_datetime(h1.d).dt.date))
    m15s = set(zip(m15.ticker, pd.to_datetime(m15.d).dt.date))

    n = len(df)
    grp_start = np.r_[0, 1 + np.where(tk[1:] != tk[:-1])[0], n]
    fire = (df.z.to_numpy() == "Z2G")
    conf = np.zeros(n, dtype=np.int8)          # group at the decision bar
    dec = np.zeros(n, dtype=bool)              # is this bar a decision bar
    grp_id = np.zeros(n, dtype=np.int8)

    for gi in range(len(grp_start) - 1):
        lo, hi = grp_start[gi], grp_start[gi + 1]
        for i in range(lo, hi):
            if not fire[i]:
                continue
            hit = -1
            for j in range(i, min(i + WINDOW + 1, hi)):
                a = (tk[j], d[j]) in h1s
                b = (tk[j], d[j]) in m15s
                if a or b:
                    hit = j
                    grp_id[j] = int(a) + int(b)
                    break
            k = hit if hit >= 0 else min(i + WINDOW, hi - 1)
            if hit < 0:
                grp_id[k] = 0
            dec[k] = True
            conf[k] = grp_id[k]

    cl = df.close.to_numpy()
    for g in (0, 1, 2):
        df[f"Z2G_c{g}"] = dec & (conf == g) & (cl >= 21)
        for blo, bhi in BUCKETS:
            df[f"Z2G_{blo}_c{g}"] = dec & (conf == g) & (cl >= blo) & (cl < bhi)
    df["Z2G_any"] = dec & (cl >= 21)
    # baseline: every 40th bar in the same universe, same exit, entry is the only difference
    df["CTRL_base"] = False
    idx = np.where(cl >= 21)[0][::40]
    df.iloc[idx, df.columns.get_loc("CTRL_base")] = True
    return df


def main():
    import pandas as pd, numpy as np
    from edge_replay import _pathsim, _stats
    t0 = time.time()
    os.makedirs(STUDY, exist_ok=True)
    out_p = os.path.join(STUDY, "MINE_RESULT.json")
    if os.path.exists(out_p):
        os.remove(out_p)

    df, h1, m15 = pull()
    print(f"[1/3] mine window {df.date.min()} .. {df.date.max()} · {len(df):,} bars · "
          f"{df.ticker.nunique():,} tickers ({time.time()-t0:.0f}s)", flush=True)
    df = build(df, h1, m15)
    grp = {t: g.reset_index(drop=True) for t, g in df.groupby("ticker", sort=False)}
    tot = sum(int(df[f"Z2G_c{g}"].sum()) for g in (0, 1, 2))
    if int(df.Z2G_any.sum()) != tot:
        raise SystemExit(f"HARD STOP: groups {tot} != any {int(df.Z2G_any.sum())}")
    print(f"[2/3] masks built · {tot:,} Z2G decisions >=$21 ({time.time()-t0:.0f}s)",
          flush=True)

    rows = {}
    print("\n[3/3] MINING RESULT — 2020-06 .. 2023-12 ONLY\n")
    print("  cell             n      med    mean   win     pf   med_MAE  yrs    worst")
    print("  " + "-" * 74)

    def line(tag, col):
        s = _stats(tag, _pathsim(grp, col, "trail", .10, .25, TRAIL, MAXH, atr_k=ATR_K))
        rows[tag] = s
        if not s["n"]:
            print(f"  {tag:14s}  n=0"); return
        print(f"  {tag:14s} {s['n']:>6,d}  {s['median']:+6.2f}  {s['mean']:+6.2f}  "
              f"{s['win']:4.1f}  {s['pf']:5.2f}   {s['med_mae']:+6.2f}  "
              f"{s['pos_years']}/{s['total_years']}  {s['worst_year']:+6.2f}", flush=True)

    line("BASELINE", "CTRL_base")
    print()
    for g in (0, 1, 2):
        line(f"Z2G {g} conf", f"Z2G_c{g}")
    print()
    for blo, bhi in BUCKETS:
        for g in (0, 1, 2):
            line(f"${blo}-{bhi} c{g}", f"Z2G_{blo}_c{g}")
        print()

    json.dump(dict(phase="MINE", window=[MINE_START, MINE_END], echo_window_days=WINDOW,
                   run_id=time.strftime("Z2GMINE_%Y%m%dT%H%M%SZ", time.gmtime()),
                   rows=rows), open(out_p, "w"), indent=1, default=str)
    print(f"written -> {out_p} ({time.time()-t0:.0f}s)", flush=True)


if __name__ == "__main__":
    main()
