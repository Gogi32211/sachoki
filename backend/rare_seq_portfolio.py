"""Does "collect rare high-win-rate sequences" work? A walk-forward test.

The user's idea, tested the only way it can be: SELECT in one window, TRADE in another,
and compare against what random selection of the same size would have produced.

  MINE   2021-05-26 .. 2023-12-31   sequences are found and picked here
  VERIFY 2024-01-01 .. 2026-09-02   the picked list is traded here, unchanged

Two precedents in the book point opposite ways, and the difference is method, not idea:
  project_robust_seq_system  mined 2021-23, verified 2024-26, 2371 -> 969 survived
  project_tz_package_audit   66,989 rules picked and concluded on ONE window = noise
                             (8 survivors where chance gives 11)
The control that separates them is the random benchmark, so it is mandatory here.

PRE-REGISTERED, before any number was produced:
  token       signal||l_sig per bar; sequence = 3 consecutive tokens
  universe    close>=5, avg_vol_20d>0, close*volume>=3M, universe<>'index', close>=$21
  candidates  n in [30, 300] INSIDE THE MINE WINDOW
  selection   win >= 60% AND median > 0, measured on MINE only
  horizon     maxh = 5. Chosen because today's sweeps showed long horizons measure drift,
              not events; a rare sequence claim is an event claim. Not fitted.
  exit        _pathsim, trail, atr_k=12, slip 15bps -- unmodified
  success     the picked list must beat, on VERIFY:
                (a) the matched baseline by >= 1.0pp, AND
                (b) the random-selection benchmark of the same size (200 draws)
              both judged with DAY-CLUSTERED accounting, because 44% of the exact
              4-bar pattern's occurrences landed on just two market-wide days

Per-sequence scoring uses a per-sequence mini universe (only the tickers where that
sequence occurs). Pooling every candidate into one mask makes sequences compete for
non-overlapping trade slots and thins them unequally -- an artifact hit earlier today.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
DATA = os.path.join(os.path.dirname(HERE), "data")
OUT = "/Users/sachoki/MASSIVE_DATA/RARE_SEQ_PORTFOLIO"
MINE_END = "2023-12-31"
VER_START = "2024-01-01"
N_LO, N_HI = 30, 300
WIN_MIN, MED_MIN = 60.0, 0.0
H = 5
SEED = 20260903


def load():
    import duckdb, numpy as np, pandas as pd
    a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
    a.execute("pragma threads=8")
    df = a.execute("""
        WITH r AS (SELECT ticker,date,open,high,low,close,atr_14,
            coalesce(nullif(t_sig,''), nullif(z_sig,''),'')||coalesce(l_sig,'') k,
            row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
          FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
            AND universe<>'index')
        SELECT ticker,date,open,high,low,close,atr_14,k FROM r WHERE rn=1
        ORDER BY ticker,date""").df()
    a.close()
    df = df.reset_index(drop=True)
    g = df.groupby("ticker", sort=False)["k"]
    df["seq"] = (g.shift(2).fillna("") + ">" + g.shift(1).fillna("") + ">" + df.k)
    df.loc[df.close < 21, "seq"] = ""
    df["d"] = pd.to_datetime(df.date)
    return df


def score(df, seqs, lo, hi, tag):
    """median/win/n per sequence inside [lo,hi], each on its own mini universe."""
    import numpy as np, pandas as pd
    from edge_replay import _pathsim
    w = df[(df.d >= lo) & (df.d <= hi)]
    by_seq = {s: g for s, g in w[w.seq != ""].groupby("seq", sort=False)}
    frames = {t: x.reset_index(drop=True) for t, x in w.groupby("ticker", sort=False)}
    res, trades = {}, {}
    t0 = time.time()
    for i, s in enumerate(seqs):
        sub = by_seq.get(s)
        if sub is None:
            continue
        tks = sub.ticker.unique()
        mini = {}
        for t in tks:
            f = frames[t].copy()
            f["M"] = (f.seq == s).to_numpy()
            mini[t] = f
        tr = _pathsim(mini, "M", "trail", .10, .25, .25, H, atr_k=12.0)
        if not len(tr):
            continue
        r = tr["ret"].to_numpy() * 100
        res[s] = dict(n=len(r), med=float(np.median(r)), win=float((r > 0).mean() * 100))
        trades[s] = tr[["ticker", "date_in", "ret"]]
        if i and i % 400 == 0:
            print(f"    [{tag}] {i:,}/{len(seqs):,}  ({time.time()-t0:.0f}s)", flush=True)
    return res, trades


def agg(trades, keys):
    """Pooled stats over the picked sequences, plus DAY-clustered accounting."""
    import numpy as np, pandas as pd
    fr = [trades[k] for k in keys if k in trades]
    if not fr:
        return None
    T = pd.concat(fr)
    r = T["ret"].to_numpy() * 100
    day = pd.to_datetime(T.date_in).dt.date
    per_day = T.assign(day=day).groupby("day")["ret"].mean().to_numpy() * 100
    return dict(trades=len(r), med=float(np.median(r)),
                win=float((r > 0).mean() * 100), mean=float(r.mean()),
                days=len(per_day), day_med=float(np.median(per_day)),
                day_mean=float(per_day.mean()),
                day_win=float((per_day > 0).mean() * 100))


def main():
    import numpy as np, pandas as pd
    os.makedirs(OUT, exist_ok=True)
    p = os.path.join(OUT, "RESULT.json")
    if os.path.exists(p):
        os.remove(p)
    rng = np.random.default_rng(SEED)
    df = load()
    print(f"loaded {len(df):,} bars\n", flush=True)

    m_lo, m_hi = pd.Timestamp("2021-05-26"), pd.Timestamp(MINE_END)
    v_lo, v_hi = pd.Timestamp(VER_START), df.d.max()

    mine = df[(df.d >= m_lo) & (df.d <= m_hi)]
    vc = mine[mine.seq != ""].seq.value_counts()
    cand = vc[(vc >= N_LO) & (vc <= N_HI)].index.tolist()
    print(f"candidates (mine n in [{N_LO},{N_HI}]): {len(cand):,}", flush=True)

    print("\n[1/2] scoring candidates on MINE ...", flush=True)
    ms, _ = score(df, cand, m_lo, m_hi, "mine")
    picked = [s for s, r in ms.items()
              if r["win"] >= WIN_MIN and r["med"] > MED_MIN and r["n"] >= N_LO]
    print(f"      scored {len(ms):,}  ·  PICKED {len(picked):,} "
          f"(win>={WIN_MIN}, med>{MED_MIN})", flush=True)

    print("\n[2/2] scoring ALL candidates on VERIFY ...", flush=True)
    vs, vt = score(df, cand, v_lo, v_hi, "verify")
    print(f"      scored {len(vs):,} on verify", flush=True)

    got = [s for s in picked if s in vt]
    P = agg(vt, got)
    ALL = agg(vt, list(vt.keys()))
    print(f"\n{'='*72}\nVERIFY {VER_START} .. {str(v_hi)[:10]}\n{'='*72}")
    print(f"  PICKED list   : {len(got):,} sequences of {len(picked):,} recurred")
    for lab, a in (("PICKED", P), ("ALL candidates", ALL)):
        if a:
            print(f"  {lab:14s} trades {a['trades']:>6,d}  med {a['med']:+6.2f}  "
                  f"win {a['win']:5.1f}   |  days {a['days']:>5,d}  day-med "
                  f"{a['day_med']:+6.2f}  day-win {a['day_win']:5.1f}")

    print(f"\n  RANDOM benchmark: 200 draws of {len(got):,} sequences from the same pool",
          flush=True)
    pool = list(vt.keys())
    meds, dmeds = [], []
    for _ in range(200):
        k = list(rng.choice(pool, size=min(len(got), len(pool)), replace=False))
        a = agg(vt, k)
        if a:
            meds.append(a["med"]); dmeds.append(a["day_med"])
    meds, dmeds = np.array(meds), np.array(dmeds)
    print(f"    trade-median : mean {meds.mean():+.3f}  p5 {np.percentile(meds,5):+.3f}  "
          f"p95 {np.percentile(meds,95):+.3f}")
    print(f"    day-median   : mean {dmeds.mean():+.3f}  p5 {np.percentile(dmeds,5):+.3f}  "
          f"p95 {np.percentile(dmeds,95):+.3f}")
    if P:
        print(f"\n  PICKED beats random on trade-median in "
              f"{100*(P['med'] > meds).mean():.1f}% of draws")
        print(f"  PICKED beats random on DAY-median   in "
              f"{100*(P['day_med'] > dmeds).mean():.1f}% of draws")

    json.dump(dict(candidates=len(cand), picked=len(picked), recurred=len(got),
                   horizon=H, selection=dict(win_min=WIN_MIN, med_min=MED_MIN),
                   verify=P, all_candidates=ALL,
                   random=dict(trade_med_mean=float(meds.mean()),
                               trade_med_p95=float(np.percentile(meds, 95)),
                               day_med_mean=float(dmeds.mean()),
                               day_med_p95=float(np.percentile(dmeds, 95))),
                   run_id=time.strftime("RARESEQ_%Y%m%dT%H%M%SZ", time.gmtime())),
              open(p, "w"), indent=1, default=str)
    print(f"\nwritten -> {p}")


if __name__ == "__main__":
    main()
