"""Z2G x lower-TF reversal — VERIFY PHASE (2024-01-01 .. 2026-09-01) + combined window.

PLAN: /Users/sachoki/MASSIVE_DATA/Z2G_REVERSAL_STUDY/PLAN_APPROVED.md
MINE: MINE_RESULT.json (2021-05 .. 2023-12), frozen before this file was written.

WHAT IS BEING VERIFIED — user's choice, 2026-09-02

The mining phase showed the LONG hypothesis is null (c2 median +0.05, PF 1.00, win 50.1%).
The same already-registered cell carried a very strong NEGATIVE result instead:

    Z2G with NO bullish T on any lower timeframe within D..D+3
      median -11.08 · win 30.0% · PF 0.28 · 0/3 years · worst -21.00
      and it repeated in every price bucket

So the claim under test flips from "buy it" to "it is a suppressor". This is the SAME test
with a changed interpretation, not a new search: no mask, window, entry rule or exit rule
is altered. `build()` is imported verbatim from the mining module so the two phases cannot
drift.

GATE, DECLARED BEFORE THIS RAN

A veto's L1 is the mirror of the long gate: >= 4/6 NEGATIVE years AND best year <= +2.
L2: the veto must be worse than the matched baseline by at least 1.0pp.
L3: n >= 80 · holds across price buckets · DSR is not applicable to a single
    pre-registered cell carried over from mining, and is not claimed.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from z2g_reversal_mine import build, BUCKETS, ATR_K, MAXH, TRAIL, STUDY  # noqa: E402

DATA = os.path.join(os.path.dirname(HERE), "data")
VERIFY_START, VERIFY_END = "2024-01-01", "2026-09-01"
FULL_START = "2021-05-26"


def pull(start, end):
    import duckdb
    a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
    a.execute("pragma threads=8")
    df = a.execute(f"""
        WITH r AS (
          SELECT ticker, date, open, high, low, close, atr_14, z_sig,
                 row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
          FROM bars
          WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= 3000000
            AND universe <> 'index'
            AND date >= DATE '{start}' AND date <= DATE '{end}'
        )
        SELECT ticker, date, open, high, low, close, atr_14,
               coalesce(z_sig,'') z FROM r WHERE rn = 1
        ORDER BY ticker, date""").df()
    a.close()

    def bullT(fname):
        c = duckdb.connect(os.path.join(DATA, fname), read_only=True)
        c.execute("pragma threads=8")
        t = c.execute(f"""SELECT DISTINCT ticker, CAST(date AS DATE) d FROM bars
                          WHERE t_sig IS NOT NULL AND t_sig <> ''
                            AND CAST(date AS DATE) >= DATE '{start}'
                            AND CAST(date AS DATE) <= DATE '{end}'""").df()
        c.close()
        return t
    return df, bullT("studio_1h.duckdb"), bullT("studio_15m.duckdb")


def phase(label, start, end, out):
    import pandas as pd
    from edge_replay import _pathsim, _stats
    df, h1, m15 = pull(start, end)
    df = build(df, h1, m15)
    grp = {t: g.reset_index(drop=True) for t, g in df.groupby("ticker", sort=False)}
    print(f"\n=== {label}  ({df.date.min()} .. {df.date.max()}, "
          f"{df.ticker.nunique():,} tickers) ===")
    print("  cell             n      med    mean   win     pf   med_MAE  yrs    worst   best")
    print("  " + "-" * 82)

    def line(tag, col):
        s = _stats(tag, _pathsim(grp, col, "trail", .10, .25, TRAIL, MAXH, atr_k=ATR_K))
        out[f"{label}|{tag}"] = s
        if not s["n"]:
            print(f"  {tag:14s}  n=0"); return
        neg = s["total_years"] - s["pos_years"]
        print(f"  {tag:14s} {s['n']:>6,d}  {s['median']:+6.2f}  {s['mean']:+6.2f}  "
              f"{s['win']:4.1f}  {s['pf']:5.2f}   {s['med_mae']:+6.2f}  "
              f"{neg}/{s['total_years']}n {s['worst_year']:+6.2f} {s['best_year']:+6.2f}",
              flush=True)

    line("BASELINE", "CTRL_base")
    for g in (0, 1, 2):
        line(f"Z2G {g} conf", f"Z2G_c{g}")
    for blo, bhi in BUCKETS:
        line(f"${blo}-{bhi} c0", f"Z2G_{blo}_c0")
    return out


def main():
    t0 = time.time()
    out_p = os.path.join(STUDY, "VERIFY_RESULT.json")
    if os.path.exists(out_p):
        os.remove(out_p)
    out = {}
    phase("VERIFY", VERIFY_START, VERIFY_END, out)      # the reserved window, opened now
    phase("FULL", FULL_START, VERIFY_END, out)          # combined, for the 6-year gate
    json.dump(dict(phase="VERIFY", verify=[VERIFY_START, VERIFY_END],
                   claim="Z2G with zero lower-TF bullish T in D..D+3 is a SUPPRESSOR",
                   gate="veto L1 = >=4/6 NEGATIVE years AND best year <= +2 "
                        "(declared before the run)",
                   run_id=time.strftime("Z2GVER_%Y%m%dT%H%M%SZ", time.gmtime()),
                   rows=out), open(out_p, "w"), indent=1, default=str)
    print(f"\nwritten -> {out_p} ({time.time()-t0:.0f}s)")


if __name__ == "__main__":
    main()
