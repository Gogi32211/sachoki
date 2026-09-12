"""MT5+ — 1D T5 with SAME-SESSION T5 echo on BOTH 1H and 15m.

    MT5 IS NOT PT5. They share a letter and nothing else.

    PT5    membership in FROZEN token-sequence families (BUY→L5, VOL_W→L46x, L43→T2,
           BUY→Z2) and frozen opening-hour 15m cluster medoids. X-only, cutoff-gated,
           sealed. Untouched by this file.
    MT5    literally: t_sig == 'T5' on the 1H and 15m bars of the same calendar session.

    Nothing here reads, writes or validates a PT artifact. Nothing here is sealed into
    the frozen T5 chain.

STATUS — read this before trusting a marker

MT5 comes from an EXPOSED HISTORICAL SEARCH (12 T signals x 4 confirmation groups over one
history, with no window reserved first). It is a SEARCH-EXPOSED CANDIDATE:
NOT independent confirmation, NOT forward-validated, NOT a book edge. The chip exists so the
setup can be looked at on a chart, not because it has been proven.

WHAT IT IS NOT — corrected 2026-09-02 by a horizon sweep

The word "confirmation" implies an event AT the bar. There is none. Measuring the same masks
at maxh 3 / 5 / 10 / 20 / 60 gives a c2-minus-c0 lift of +0.09 / +0.20 / +0.40 / +0.70 /
+1.78 -- i.e. per bar 0.030 / 0.040 / 0.040 / 0.035 / 0.030. FLAT. The lift is a constant
drift-rate difference that simply accumulates with holding time; 3->60 bars is 20x and the
lift grows 19.8x. At three bars the entire effect is +0.09.

The state is still real: over 60 bars median +2.13, win 55.7%, PF 1.48, and med_MAE lower
than baseline at every horizon (-2.27 vs -2.30 at h=3; -9.78 vs -10.54 at h=60). But it is a
SLOW SELECTION criterion, not entry timing, and the UI hint says so.

Why this was not visible before: every study in this arc used maxh=60 with a 15-60% trailing
stop, which closes 94-97% of trades on the TIMER and produces near-identical med_MAE/med_MFE
(~ -10 / +11) in every cell including the baseline. That instrument cannot distinguish an
event from a drift. `maxh` is a parameter, so the sweep needed no change to `_pathsim`.

Study: /Users/sachoki/MASSIVE_DATA/MTF_1D_X_LTF_STUDY/ (family MTF_1D_X_LTF_STUDY_V1)

DEFINITION (frozen here, one place)

    universe   the book's standard: close>=5, avg_vol_20d>0, close*volume>=3M,
               universe<>'index'   -- identical to the study
    price      close >= 21 at the signal bar, NO upper bound   [user's choice 2026-09-02]
    signal     1D t_sig == 'T5'
    confirm    t_sig == 'T5' on 1H and/or 15m, SAME calendar session
    MT5+       both lower timeframes echo (the only class the UI exposes)

Why >= 21 and no cap: the confirmation ladder replicates in every price bucket, but below
$21 it does not. $8-12 inverts (c1 +1.81 > c2 +0.95) and $12-21 is near zero (+0.69). $5-8
looks best on paper only because EVERYTHING wins there (c0 already +4.08, PF 3.28, top10%
carries 49% of profit, mean 16.57 vs median 6.61) -- that is the lottery tail, not the
signal. Above $21 the ladder holds through $200+ (weakest, worst-year -3.10) so no cap is
imposed; the user chose to keep the whole upper range.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

ROOT = os.path.dirname(HERE)
DATA = os.path.join(ROOT, "data")
OUT = os.path.join(DATA, "mt5_signals.parquet")
SPEC = os.path.join(HERE, "MT5_SIGNALS_V1.json")
PRICE_MIN = 21
DV_FLOOR = 3_000_000

# MT5 is a signal view. An outcome column reaching this table would let the chart show a
# result the study never claimed, so the same guard PT5 uses is kept.
FORBIDDEN = {"ret", "mfe", "mae", "ret_10d", "mfe_10d", "mae_10d", "median", "win", "pf"}


class MT5GateFailure(RuntimeError):
    pass


def build():
    import duckdb, pandas as pd

    a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
    a.execute("pragma threads=8")
    as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
    d1 = a.execute(f"""
        WITH r AS (
          SELECT ticker, date, close, t_sig,
                 row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
          FROM bars
          WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= {DV_FLOOR}
            AND universe <> 'index' AND t_sig = 'T5'
        )
        SELECT ticker, CAST(date AS DATE) d, close FROM r
        WHERE rn = 1 AND close >= {PRICE_MIN}""").df()
    a.close()

    def echo(fname):
        c = duckdb.connect(os.path.join(DATA, fname), read_only=True)
        c.execute("pragma threads=8")
        t = c.execute("""SELECT DISTINCT ticker, CAST(date AS DATE) d FROM bars
                         WHERE t_sig = 'T5'""").df()
        c.close()
        return t

    h1, m15 = echo("studio_1h.duckdb"), echo("studio_15m.duckdb")
    con = duckdb.connect(); con.execute("pragma threads=8")
    con.register("d1", d1); con.register("h1", h1); con.register("h15", m15)
    df = con.execute("""
        SELECT d1.ticker, d1.d, d1.close,
               CASE WHEN h1.ticker  IS NOT NULL THEN TRUE ELSE FALSE END AS mt5_h1,
               CASE WHEN h15.ticker IS NOT NULL THEN TRUE ELSE FALSE END AS mt5_m15
        FROM d1
        LEFT JOIN h1  ON h1.ticker = d1.ticker  AND h1.d  = d1.d
        LEFT JOIN h15 ON h15.ticker = d1.ticker AND h15.d = d1.d
        ORDER BY d1.ticker, d1.d""").df()
    con.close()

    df["date"] = pd.to_datetime(df.d).dt.strftime("%Y-%m-%d")   # str, like pt*_signals
    df = df.drop(columns=["d"])
    df["mt5_conf"] = df.mt5_h1.astype(int) + df.mt5_m15.astype(int)
    df["mt5_strong"] = df.mt5_conf == 2
    df["mt5_class"] = df.mt5_conf.map({0: "MT5_BASE", 1: "MT5_1", 2: "MT5_STRONG"})
    df = df[["ticker", "date", "close", "mt5_class", "mt5_h1", "mt5_m15",
             "mt5_conf", "mt5_strong"]]

    bad = set(df.columns) & FORBIDDEN
    if bad:
        raise MT5GateFailure(f"outcome column reached MT5: {bad}")
    if df.duplicated(["ticker", "date"]).any():
        raise MT5GateFailure("duplicate (ticker, date) grain")
    if (df.close < PRICE_MIN).any():
        raise MT5GateFailure(f"row below the ${PRICE_MIN} floor")
    n0, n1, n2 = [int((df.mt5_conf == g).sum()) for g in (0, 1, 2)]
    if n0 + n1 + n2 != len(df):
        raise MT5GateFailure("confirmation classes do not partition the table")
    if n2 == 0:
        raise MT5GateFailure("zero MT5+ rows — that is a join defect, not a finding")
    return df, as_of, (n0, n1, n2)


def main():
    import pandas as pd
    t0 = time.time()
    for p in (OUT, SPEC):
        if os.path.exists(p):
            os.remove(p)                 # never leave a stale build readable
    df, as_of, (n0, n1, n2) = build()
    df.to_parquet(OUT, index=False)
    spec = dict(
        spec_id="MT5_SIGNALS_V1", status="BUILT", as_of=as_of,
        built_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        status_of_evidence=dict(
            claim="SEARCH-EXPOSED CANDIDATE",
            is_forward_validated=False, is_independent_confirmation=False,
            is_book_edge=False,
            is_entry_timing_signal=False,
            what_it_actually_is="a slow SELECTION criterion: a constant drift-rate "
                                "difference of about +0.035%/bar, not an event at the bar",
            horizon_sweep_lift_c2_minus_c0={"3": 0.09, "5": 0.20, "10": 0.40,
                                            "20": 0.70, "60": 1.78},
            horizon_sweep_lift_per_bar={"3": 0.030, "5": 0.040, "10": 0.040,
                                        "20": 0.035, "60": 0.030},
            at_60_bars=dict(median=2.13, win=55.7, pf=1.48, med_mae=-9.78,
                            baseline_med_mae=-10.54),
            search="12 T signals x 4 confirmation groups over one history, "
                   "no window reserved before the search",
            study_family="MTF_1D_X_LTF_STUDY_V1"),
        definition=dict(universe="close>=5, avg_vol_20d>0, close*volume>=3M, "
                                 "universe<>'index'",
                        price=f"close >= {PRICE_MIN}, no upper bound",
                        signal="1D t_sig == 'T5'",
                        confirm="t_sig == 'T5' on 1h / 15m, same calendar session",
                        exposed_class="MT5_STRONG (both) — the only class in the UI"),
        not_pt5="PT5 is frozen token-family membership. MT5 is same-signal echo. "
                "No PT artifact is read or written by mt5_build.py.",
        counts=dict(rows=len(df), MT5_BASE=n0, MT5_1=n1, MT5_STRONG=n2,
                    tickers=int(df.ticker.nunique())),
        outputs=dict(parquet=os.path.relpath(OUT, ROOT)))
    json.dump(spec, open(SPEC, "w"), indent=1)
    print(f"MT5 build · as_of {as_of} · {len(df):,} T5 rows >= ${PRICE_MIN}")
    print(f"  MT5_BASE {n0:,}   MT5_1 {n1:,}   MT5_STRONG {n2:,}   "
          f"({df.ticker.nunique():,} tickers, {time.time()-t0:.0f}s)")
    print(f"  -> {OUT}\n  -> {SPEC}")


if __name__ == "__main__":
    main()
