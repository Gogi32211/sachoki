"""T9 x L34/L12 x lower-TF (T9+L34) echo — MINING PHASE (2021-05 .. 2023-12).

2024-01-01 .. 2026-09-01 is RESERVED and is not read here.

T9 on 1D carries exactly one of two volume lines and nothing else (L12 63% / L34 37%), so
L12 is not an arbitrary control -- it is L34's exact complement inside the T9 population,
which means composition cannot explain a difference between them.

ECHO (user's choice): the lower timeframe must show t_sig='T9' AND l_sig='L34' together in
the same session. Applied identically to the 1D-L34 and 1D-L12 groups, so the question
"does a lower-TF T9L34 help only an L34 day, or any T9 day" is answerable.

HORIZON SWEEP IS BUILT IN, not an afterthought. Every result measured today at maxh=60
alone turned out to be a constant drift-rate difference rather than an event at the bar;
94-97% of trades closed on the timer and med_MAE/med_MFE were ~ -10/+11 in every cell
including the baseline. So the lift is reported PER BAR at maxh 3/5/10/20/60:
  flat per-bar lift -> drift, no moment, not entry timing
  a hump at short horizons -> a real event
`maxh` is a parameter; `_pathsim` is not modified.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DATA = os.path.join(os.path.dirname(HERE), "data")
STUDY = "/Users/sachoki/MASSIVE_DATA/T9_L34_STUDY"
MINE = ("2021-05-26", "2023-12-31")
HOR = [3, 5, 10, 20, 60]
BUCKETS = [(21, 89), (89, 200), (200, 10_000)]


def pull(start, end):
    import duckdb
    a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
    a.execute("pragma threads=8")
    df = a.execute(f"""
        WITH r AS (
          SELECT ticker,date,open,high,low,close,atr_14,t_sig,l_sig,
                 row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
          FROM bars
          WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000 AND universe<>'index'
            AND date >= DATE '{start}' AND date <= DATE '{end}')
        SELECT ticker,date,open,high,low,close,atr_14,
               coalesce(t_sig,'') t, coalesce(l_sig,'') l
        FROM r WHERE rn=1 ORDER BY ticker,date""").df()
    a.close()
    if str(df.date.max())[:10] > end:
        raise SystemExit(f"OOS LEAK: {df.date.max()} > {end}")

    def echo(fname):
        c = duckdb.connect(os.path.join(DATA, fname), read_only=True)
        c.execute("pragma threads=8")
        e = c.execute(f"""SELECT DISTINCT ticker, CAST(date AS DATE) d FROM bars
                          WHERE t_sig='T9' AND l_sig='L34'
                            AND CAST(date AS DATE) >= DATE '{start}'
                            AND CAST(date AS DATE) <= DATE '{end}'""").df()
        c.close()
        return e
    return df, echo("studio_1h.duckdb"), echo("studio_15m.duckdb")


def build(df, h1, m15):
    import numpy as np, pandas as pd
    df = df.reset_index(drop=True)
    d = pd.to_datetime(df.date).dt.date
    key = pd.MultiIndex.from_arrays([df.ticker, d])
    n = np.zeros(len(df), dtype=np.int8)
    for e in (h1, m15):
        ek = pd.MultiIndex.from_arrays([e.ticker, pd.to_datetime(e.d).dt.date])
        n += key.isin(ek).astype(np.int8)
    cl = df.close.to_numpy()
    t9 = df.t.to_numpy() == "T9"
    p21 = cl >= 21
    for lab, L in (("L34", "L34"), ("L12", "L12")):
        base = t9 & (df.l.to_numpy() == L) & p21
        df[f"{lab}_any"] = base
        for g in (0, 1, 2):
            df[f"{lab}_c{g}"] = base & (n == g)
        for lo, hi in BUCKETS:
            df[f"{lab}_{lo}_c2"] = base & (n == 2) & (cl >= lo) & (cl < hi)
            df[f"{lab}_{lo}_c0"] = base & (n == 0) & (cl >= lo) & (cl < hi)
    df["CTRL"] = False
    df.iloc[np.where(p21)[0][::40], df.columns.get_loc("CTRL")] = True
    return df


def main():
    import pandas as pd
    from edge_replay import _pathsim, _stats
    t0 = time.time()
    os.makedirs(STUDY, exist_ok=True)
    p = os.path.join(STUDY, "MINE_HORIZONS.json")
    if os.path.exists(p):
        os.remove(p)

    df, h1, m15 = pull(*MINE)
    df = build(df, h1, m15)
    for lab in ("L34", "L12"):
        a = int(df[f"{lab}_any"].sum())
        s = sum(int(df[f"{lab}_c{g}"].sum()) for g in (0, 1, 2))
        if a != s:
            raise SystemExit(f"HARD STOP: {lab} groups {s} != any {a}")
    grp = {t: g.reset_index(drop=True) for t, g in df.groupby("ticker", sort=False)}
    print(f"mine {str(df.date.min())[:10]} .. {str(df.date.max())[:10]} · "
          f"{len(df):,} bars · L34 {int(df.L34_any.sum()):,} · L12 {int(df.L12_any.sum()):,}"
          f"  ({time.time()-t0:.0f}s)\n", flush=True)

    out = {}
    cells = [("BASELINE", "CTRL")]
    for lab in ("L34", "L12"):
        cells += [(f"{lab} {g} conf", f"{lab}_c{g}") for g in (0, 1, 2)]
    print("  maxh  cell             n       med    win     pf     MAE   "
          "lift/bar vs baseline")
    print("  " + "-" * 76)
    for H in HOR:
        st = {}
        for tag, col in cells:
            st[tag] = _stats(tag, _pathsim(grp, col, "trail", .10, .25, .25, H, atr_k=12.0))
            out[f"{H}|{tag}"] = st[tag]
        b = st["BASELINE"]["median"]
        for tag, _ in cells:
            s = st[tag]
            if not s["n"]:
                print(f"  {H:>4}  {tag:14s} n=0"); continue
            per = "" if tag == "BASELINE" else f"   {(s['median']-b)/H:+7.3f}"
            print(f"  {H:>4}  {tag:14s} {s['n']:>6,d}  {s['median']:+6.2f}  {s['win']:4.1f}  "
                  f"{s['pf'] if s['pf'] is not None else float('nan'):5.2f}  "
                  f"{s['med_mae']:+6.2f}{per}", flush=True)
        print()

    json.dump(dict(phase="MINE", window=MINE, horizons=HOR,
                   echo="lower-TF t_sig=T9 AND l_sig=L34, same session",
                   run_id=time.strftime("T9L34_%Y%m%dT%H%M%SZ", time.gmtime()),
                   rows=out), open(p, "w"), indent=1, default=str)
    print(f"written -> {p} ({time.time()-t0:.0f}s)")


if __name__ == "__main__":
    main()
