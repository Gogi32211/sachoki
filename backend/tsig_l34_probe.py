"""<T> x L34/L12 x lower-TF echo — horizon sweep + 1H anatomy, for any bullish T signal.

usage:  tsig_l34_probe.py T3 [T9 ...]

Generalised from t9_l34_mine.py (which stays as the record of the T9 run). Same frozen
choices, so results are comparable across signals:

  window   MINE 2021-05..2023-12 only; 2024-01..2026-09 stays reserved
  split    L34 vs L12 -- on these signals a bullish T carries exactly one of the two and
           nothing else, so L12 is L34's exact complement and composition cannot explain a
           difference between them
  echo     lower TF must show the SAME t_sig AND l_sig='L34' in the same session
  measure  maxh 3/5/10/20/60, lift reported PER BAR
           flat per-bar lift -> constant drift, no moment
           front-loaded      -> a real event at the bar

MULTIPLICITY WARNING, to be carried into any verdict: this is the third bullish signal
probed on the same history (T5, then T9, now T3), each with an L-split, three echo groups
and five horizons. The looks accumulate even though each individual run is cheap. Nothing
here is promotable on its own.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402
from collections import Counter                                         # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
DATA = os.path.join(os.path.dirname(HERE), "data")
STUDY = "/Users/sachoki/MASSIVE_DATA/TSIG_L34_PROBE"
MINE = ("2021-05-26", "2023-12-31")
HOR = [3, 5, 10, 20, 60]
ET = ["09:30", "10:30", "11:30", "12:30", "13:30", "14:30", "15:30"]


def pull(T, start, end):
    import duckdb
    a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
    a.execute("pragma threads=8")
    df = a.execute(f"""
        WITH r AS (SELECT ticker,date,open,high,low,close,atr_14,t_sig,l_sig,
                     row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                   FROM bars
                   WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
                     AND universe<>'index'
                     AND date >= DATE '{start}' AND date <= DATE '{end}')
        SELECT ticker,date,open,high,low,close,atr_14,
               coalesce(t_sig,'') t, coalesce(l_sig,'') l
        FROM r WHERE rn=1 ORDER BY ticker,date""").df()
    a.close()
    if str(df.date.max())[:10] > end:
        raise SystemExit(f"OOS LEAK: {df.date.max()} > {end}")

    def echo(f):
        c = duckdb.connect(os.path.join(DATA, f), read_only=True)
        c.execute("pragma threads=8")
        e = c.execute(f"""SELECT DISTINCT ticker, CAST(date AS DATE) d FROM bars
                          WHERE t_sig='{T}' AND l_sig='L34'
                            AND CAST(date AS DATE) >= DATE '{start}'
                            AND CAST(date AS DATE) <= DATE '{end}'""").df()
        c.close()
        return e
    return df, echo("studio_1h.duckdb"), echo("studio_15m.duckdb")


def sweep(T, out):
    import numpy as np, pandas as pd
    from edge_replay import _pathsim, _stats
    df, h1, m15 = pull(T, *MINE)
    df = df.reset_index(drop=True)
    key = pd.MultiIndex.from_arrays([df.ticker, pd.to_datetime(df.date).dt.date])
    n = np.zeros(len(df), dtype=np.int8)
    for e in (h1, m15):
        n += key.isin(pd.MultiIndex.from_arrays(
            [e.ticker, pd.to_datetime(e.d).dt.date])).astype(np.int8)
    cl = df.close.to_numpy()
    hit = (df.t.to_numpy() == T) & (cl >= 21)
    for lab in ("L34", "L12"):
        base = hit & (df.l.to_numpy() == lab)
        for g in (0, 1, 2):
            df[f"{lab}_c{g}"] = base & (n == g)
    df["CTRL"] = False
    df.iloc[np.where(cl >= 21)[0][::40], df.columns.get_loc("CTRL")] = True
    grp = {t: g.reset_index(drop=True) for t, g in df.groupby("ticker", sort=False)}

    cells = [("BASELINE", "CTRL")] + [(f"{L} {g} conf", f"{L}_c{g}")
                                      for L in ("L34", "L12") for g in (0, 1, 2)]
    print(f"\n{'='*78}\n{T} x L34/L12 — MINE horizon sweep\n{'='*78}")
    print("  maxh  cell             n       med    win     pf     MAE    lift/bar")
    for H in HOR:
        st = {}
        for tag, col in cells:
            st[tag] = _stats(tag, _pathsim(grp, col, "trail", .10, .25, .25, H, atr_k=12.0))
            out[f"{T}|{H}|{tag}"] = st[tag]
        b = st["BASELINE"]["median"]
        for tag, _ in cells:
            s = st[tag]
            if not s["n"]:
                print(f"  {H:>4}  {tag:14s} n=0"); continue
            per = "" if tag == "BASELINE" else f"   {(s['median']-b)/H:+7.3f}"
            pf = s["pf"] if s["pf"] is not None else float("nan")
            print(f"  {H:>4}  {tag:14s} {s['n']:>6,d}  {s['median']:+6.2f}  {s['win']:4.1f}  "
                  f"{pf:5.2f}  {s['med_mae']:+6.2f}{per}", flush=True)
        print()


def anatomy(T, out):
    import duckdb, pandas as pd
    a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
    a.execute("pragma threads=8")
    days = a.execute(f"""
        WITH r AS (SELECT ticker,date,close,t_sig,l_sig,
                     row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                   FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
                     AND universe<>'index')
        SELECT ticker, CAST(date AS DATE) d FROM r
        WHERE rn=1 AND t_sig='{T}' AND l_sig='L34' AND close>=21""").df()
    a.close()
    c = duckdb.connect(os.path.join(DATA, "studio_1h.duckdb"), read_only=True)
    c.execute("pragma threads=8"); c.register("d", days)
    h = c.execute("""SELECT b.ticker, CAST(b.date AS DATE) d, b.date ts,
           coalesce(nullif(b.t_sig,''), nullif(b.z_sig,''), '·') s,
           CASE WHEN b.t_sig<>'' AND b.t_sig IS NOT NULL THEN 'T' ELSE 'Z' END c
        FROM bars b JOIN d ON d.ticker=b.ticker AND d.d=CAST(b.date AS DATE)
        ORDER BY b.ticker, b.date""").df()
    c.close()

    fine, coarse = [], []
    for _, x in h.groupby(["ticker", "d"], sort=False):
        if len(x) == 7:
            fine.append(tuple(x.s)); coarse.append(tuple(x.c))
    N = len(fine)
    F, C = Counter(fine), Counter(coarse)
    print(f"\n{'='*78}\n{T}+L34 — 1H anatomy   ({len(days):,} days, {N:,} standard 7-bar "
          f"sessions)\n{'='*78}")
    print(f"  full sequences : {len(F):,} distinct of {N:,}  ·  most common "
          f"{F.most_common(1)[0][1]} times ({100*F.most_common(1)[0][1]/N:.2f}%)")
    print(f"  T/Z patterns   : {len(C)} of 128  ·  top "
          f"{''.join(C.most_common(1)[0][0])} {100*C.most_common(1)[0][1]/N:.2f}% "
          f"(uniform = 0.78%)")
    df = pd.DataFrame(fine); dc = pd.DataFrame(coarse)
    print("\n  slot    bullish%   top signals")
    for i in range(7):
        vc = df[i].value_counts().head(3)
        top = "  ".join(f"{k} {100*v/N:4.1f}%" for k, v in vc.items())
        print(f"  {ET[i]}   {100*(dc[i]=='T').mean():5.1f}%    {top}")
    nT = dc.apply(lambda r: sum(v == "T" for v in r), axis=1)
    print(f"\n  bullish 1H bars/day: mean {nT.mean():.2f}  ·  "
          + "  ".join(f"{k}:{100*v/N:.1f}%" for k, v in nT.value_counts().sort_index().items()))
    self_echo = df.apply(lambda r: any(v == T for v in r), axis=1)
    print(f"  days where 1H also prints {T}: {100*self_echo.mean():.1f}%")
    out[f"{T}|anatomy"] = dict(sessions=N, distinct_full=len(F), distinct_tz=len(C),
                              top_tz={"".join(k): v for k, v in C.most_common(10)})


if __name__ == "__main__":
    os.makedirs(STUDY, exist_ok=True)
    res = {}
    for T in (sys.argv[1:] or ["T3"]):
        sweep(T, res)
        anatomy(T, res)
    p = os.path.join(STUDY, "PROBE.json")
    json.dump(res, open(p, "w"), indent=1, default=str)
    print(f"\nwritten -> {p}")
