"""1D T-signal x MTF confirmation study.

APPROVED PLAN: /Users/sachoki/MASSIVE_DATA/MTF_T_SIGNAL_STUDY_PLAN_APPROVED.md

Question: for any T signal on 1D, which one is stronger when the SAME T signal also fires
on 1H and on 15m in the SAME session.

Frozen parameters (approved before any data was read):
  population    1D t_sig in T1 T1G T2 T2G T3 T4 T5 T6 T9 T10 T11 T12
  price gate    close between 21 and 89 AT THE SIGNAL BAR
  universe      the book's standard: close>=5, avg_vol_20d>0, close*volume>=3M,
                universe<>'index'
  confirm       SAME t_sig on the lower timeframe, SAME calendar session
  groups        0 / 1 / 2 confirmations
  outcome       edge_replay._pathsim — TRUE bar-by-bar, stop-first, gap-realistic
                mode=trail, atr_k=12 (clip 15..60%), maxh=60, slip=15bps
                NOT an MFE proxy

Nothing here reimplements the exit engine; it builds entry masks and calls the sacred one.
"""
from __future__ import annotations
import os, sys, json, time                                             # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DATA = "/Users/sachoki/Desktop/sachoki-desktop/data"
T_SIGNALS = ["T1", "T1G", "T2", "T2G", "T3", "T4", "T5", "T6", "T9", "T10", "T11", "T12"]
PRICE_LO, PRICE_HI = 21, 89
DV_FLOOR = 3_000_000
MONTHS = 72
ATR_K = 12.0
MAXH = 60
TRAIL = 0.25


def pull_1d(months=MONTHS, dv_floor=DV_FLOOR):
    import duckdb, pandas as pd
    a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
    a.execute("pragma threads=8")
    as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
    df = a.execute(f"""
        WITH r AS (
          SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
          FROM bars
          WHERE close >= 5 AND avg_vol_20d > 0 AND close*volume >= {dv_floor}
            AND universe <> 'index'
            AND date >= DATE '{as_of}' - INTERVAL {months*31 + 40} DAY
        )
        SELECT ticker, date, open, high, low, close, atr_14,
               coalesce(t_sig,'') t
        FROM r WHERE rn = 1
        ORDER BY ticker, date""").df()
    a.close()
    return df, as_of


def pull_confirms(tf_file, as_of, months=MONTHS):
    """distinct (ticker, session date, t_sig) on a lower timeframe."""
    import duckdb
    c = duckdb.connect(os.path.join(DATA, tf_file), read_only=True)
    c.execute("pragma threads=8")
    q = f"""
        SELECT DISTINCT ticker, CAST(date AS DATE) d, t_sig t
        FROM bars
        WHERE t_sig IS NOT NULL AND t_sig <> ''
          AND CAST(date AS DATE) >= DATE '{as_of}' - INTERVAL {months*31 + 40} DAY"""
    df = c.execute(q).df()
    c.close()
    return df


def build(df, c1h, c15m):
    """Adds, for each T signal, three boolean entry columns: _c0 _c1 _c2.

    The join runs in DuckDB, not through Python tuple keys. A first version built
    `set(zip(ticker, date, t))` on both sides — but the 1D side produced
    `datetime.date` and the lower-timeframe side `pandas.Timestamp`, so no key ever
    matched and EVERY fire was scored 0-confirm. The result looked like a finding and
    was not one. SQL joins on the column type, which removes the failure mode.
    """
    import duckdb, pandas as pd, numpy as np
    con = duckdb.connect(); con.execute("pragma threads=8")
    con.register("d1", df)
    con.register("h1", c1h)
    con.register("h15", c15m)
    j = con.execute(f"""
        SELECT d1.ticker, d1.date,
               CASE WHEN h1.ticker  IS NOT NULL THEN 1 ELSE 0 END AS n1,
               CASE WHEN h15.ticker IS NOT NULL THEN 1 ELSE 0 END AS n15
        FROM d1
        LEFT JOIN (SELECT DISTINCT ticker, d, t FROM h1) h1
               ON h1.ticker = d1.ticker
              AND h1.d = CAST(d1.date AS DATE)
              AND h1.t = d1.t
        LEFT JOIN (SELECT DISTINCT ticker, d, t FROM h15) h15
               ON h15.ticker = d1.ticker
              AND h15.d = CAST(d1.date AS DATE)
              AND h15.t = d1.t
        WHERE d1.t <> '' AND d1.close BETWEEN {PRICE_LO} AND {PRICE_HI}
    """).df()
    con.close()
    df = df.copy()
    key = pd.MultiIndex.from_arrays([df.ticker, pd.to_datetime(df.date)])
    jkey = pd.MultiIndex.from_arrays([j.ticker, pd.to_datetime(j.date)])
    nconf = pd.Series(0, index=key)
    nconf.loc[jkey] = (j.n1 + j.n15).to_numpy()
    nconf = nconf.to_numpy()
    price_ok = ((df.close >= PRICE_LO) & (df.close <= PRICE_HI)).to_numpy()
    tt = df.t.to_numpy()
    for T in T_SIGNALS:
        fire = (tt == T) & price_ok
        df[f"{T}_any"] = fire
        df[f"{T}_c0"] = fire & (nconf == 0)
        df[f"{T}_c1"] = fire & (nconf == 1)
        df[f"{T}_c2"] = fire & (nconf == 2)
    return df


def run():
    import pandas as pd, numpy as np
    from edge_replay import _pathsim, _stats
    t0 = time.time()
    RESULT = "/Users/sachoki/MASSIVE_DATA/MTF_T_SIGNAL_STUDY_RESULT.json"
    # STALE RESULT FILE READ AS CURRENT — the exact failure this run already hit once.
    # The old JSON is removed BEFORE any work, so a crash leaves no readable result at all
    # rather than a plausible-looking one from a previous, buggy run.
    if os.path.exists(RESULT):
        os.remove(RESULT)
    print("[1/4] pulling 1D universe ...", flush=True)
    df, as_of = pull_1d()
    print(f"      {len(df):,} bars · {df.ticker.nunique():,} tickers · as_of {as_of} "
          f"({time.time()-t0:.0f}s)", flush=True)
    print("[2/4] pulling 1H / 15m confirmations ...", flush=True)
    c1h = pull_confirms("studio_1h.duckdb", as_of)
    c15m = pull_confirms("studio_15m.duckdb", as_of)
    print(f"      1H {len(c1h):,} (ticker,date,T) · 15m {len(c15m):,} "
          f"({time.time()-t0:.0f}s)", flush=True)
    print("[3/4] building confirmation masks ...", flush=True)
    cache = "/Users/sachoki/MASSIVE_DATA/_mtf_masks.parquet"
    if os.path.exists(cache):
        df = pd.read_parquet(cache); print("      (reused cached masks)", flush=True)
    else:
        df = build(df, c1h, c15m)
        df.to_parquet(cache, index=False)
    if "d" in df.columns:
        df = df.drop(columns=["d"])
    grp = {tk: g.reset_index(drop=True) for tk, g in df.groupby("ticker", sort=False)}
    print(f"      {len(grp):,} ticker frames ({time.time()-t0:.0f}s)", flush=True)
    tot_c = 0
    for T in T_SIGNALS:
        a = int(df[f"{T}_any"].sum())
        p012 = int(df[f"{T}_c0"].sum()) + int(df[f"{T}_c1"].sum()) + int(df[f"{T}_c2"].sum())
        if a != p012:
            raise SystemExit(f"HARD STOP: {T} partition {p012} != any {a}")
        tot_c += int(df[f"{T}_c1"].sum()) + int(df[f"{T}_c2"].sum())
    if tot_c == 0:
        raise SystemExit("HARD STOP: zero confirmations across every T signal — that is a "
                         "join defect, not a finding")
    print(f"      partition OK · {tot_c:,} confirmed fires", flush=True)
    print("[4/4] path-sim ...", flush=True)
    rows = []
    for T in T_SIGNALS:
        for g in ("any", "c0", "c1", "c2"):
            col = f"{T}_{g}"
            tr = _pathsim(grp, col, "trail", 0.10, 0.25, TRAIL, MAXH, atr_k=ATR_K)
            s = _stats(col, tr)
            s["T"] = T
            s["group"] = {"any": "ALL", "c0": "0 confirm", "c1": "1 confirm",
                          "c2": "2 confirm"}[g]
            rows.append(s)
        done = [r for r in rows if r["T"] == T]
        print(f"      {T:4s} " + "  ".join(
            f"{r['group']:>9} n={r['n']:>5} med={r.get('median','--'):>6}" for r in done), flush=True)
    out = dict(as_of=as_of, months=MONTHS, price_gate=[PRICE_LO, PRICE_HI],
               dv_floor=DV_FLOOR,
               exit=dict(mode="trail", atr_k=ATR_K, maxh=MAXH, trail=TRAIL, slip=0.0015),
               confirm=dict(window="same session", type="same T signal on the lower TF",
                            lower_tfs=["1h", "15m"]),
               rows=rows, elapsed_s=round(time.time() - t0))
    out["run_id"] = time.strftime("MTF_%Y%m%dT%H%M%SZ", time.gmtime())
    out["confirm_partition_check"] = "per T: c0 + c1 + c2 == any (asserted below)"
    p = RESULT
    json.dump(out, open(p, "w"), indent=1, default=str)
    print(f"\nwritten -> {p}   ({time.time()-t0:.0f}s)", flush=True)
    return out


if __name__ == "__main__":
    run()
