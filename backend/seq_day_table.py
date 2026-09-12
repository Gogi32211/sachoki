"""STANDARD SEQUENCE REPORT — one row per signal DAY, with a same-day control.

    date │ n │ min │ median │ mean │ max │ ctrl_median │ EDGE

User-approved 2026-09-03 (memory: feedback-day-clustered-accounting). Why each column:
  n            max/min scale with how many tickers fired; without n they cannot be compared
  median+mean  their gap is the tail (one +139% name moves a mean, not a median)
  ctrl_median  what a random same-universe basket did THAT DAY -- a day's outcome is mostly
               the market; only EDGE = median - ctrl_median speaks about the signal
  verdict      computed over DAYS from EDGE, never over trades

usage:
  seq_day_table.py Z2GL46 Z2GL46 Z2L46 T1L3            exact tokens (signal+volume line)
  seq_day_table.py Z2G Z2G Z2 T1                       signal-only tokens (any volume line)
  seq_day_table.py --H 60 ...                          horizon (default 5)

A token matches exactly when it contains an L-code; otherwise it matches the bar's signal
alone. Tokens are consecutive daily bars; the LAST token is the signal bar; entry is the
next open via edge_replay._pathsim (unmodified).
"""
from __future__ import annotations
import os, sys, json, re                                                # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
DATA = os.path.join(os.path.dirname(HERE), "data")
OUT = "/Users/sachoki/MASSIVE_DATA/SEQ_DAY_TABLES"
CTRL_EVERY = 20            # every 20th universe ticker on each signal day ≈ 200 controls/day


def load():
    import duckdb, pandas as pd
    a = duckdb.connect(os.path.join(DATA, "studio_analytics.duckdb"), read_only=True)
    a.execute("pragma threads=8")
    df = a.execute("""
        WITH r AS (SELECT ticker,date,open,high,low,close,atr_14,
            coalesce(nullif(t_sig,''), nullif(z_sig,''),'') s, coalesce(l_sig,'') l,
            row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
          FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000
            AND universe<>'index')
        SELECT ticker,date,open,high,low,close,atr_14,s,l FROM r WHERE rn=1
        ORDER BY ticker,date""").df()
    a.close()
    df = df.reset_index(drop=True)
    df["d"] = pd.to_datetime(df.date).dt.date
    return df


def match(df, tokens):
    """boolean mask on the LAST bar of each occurrence of the token sequence."""
    import numpy as np
    G = df.groupby("ticker", sort=False)
    m = (df.close.to_numpy() >= 21)
    k = len(tokens)
    for i, tok in enumerate(tokens):
        lag = k - 1 - i
        s = G["s"].shift(lag) if lag else df["s"]
        l = G["l"].shift(lag) if lag else df["l"]
        mm = re.match(r"^([TZ]\d+G?)(L\d+)?$", tok)
        if not mm:
            raise SystemExit(f"bad token {tok!r}")
        sig, lc = mm.group(1), mm.group(2)
        m &= (s == sig).to_numpy()
        if lc:
            m &= (l == lc).to_numpy()
    return m


def run(df, tokens, H):
    import numpy as np, pandas as pd
    from edge_replay import _pathsim
    sig = match(df, tokens)
    n_sig = int(sig.sum())
    if n_sig == 0:
        print("no occurrences"); return None
    sig_days = set(df.d.to_numpy()[sig])
    sig_pairs = set(zip(df.ticker.to_numpy()[sig], df.d.to_numpy()[sig]))

    # same-day control: every CTRL_EVERY-th universe ticker on each signal day, excluding
    # the tickers that fired (so the control is "the rest of the market that day")
    ctrl = np.zeros(len(df), dtype=bool)
    on_day = df.d.isin(sig_days).to_numpy() & (df.close.to_numpy() >= 21)
    idx = np.where(on_day)[0]
    for d, grp_idx in pd.Series(idx).groupby(df.d.to_numpy()[idx]):
        gi = grp_idx.to_numpy()
        keep = [i for i in gi if (df.ticker.iat[i], d) not in sig_pairs][::CTRL_EVERY]
        ctrl[keep] = True

    df = df.copy()
    df["SIG"], df["CTRL"] = sig, ctrl
    grp = {t: x.reset_index(drop=True) for t, x in df.groupby("ticker", sort=False)}

    # signal bar = the bar before date_in; map back by position within the ticker
    pos = {}
    for t, x in grp.items():
        dd = x.d.to_numpy()
        for i in range(1, len(dd)):
            pos[(t, dd[i])] = dd[i - 1]

    def trades(col):
        tr = _pathsim(grp, col, "trail", .10, .25, .25, H, atr_k=12.0)
        if not len(tr):
            return tr
        din = pd.to_datetime(tr.date_in).dt.date
        tr["sig_day"] = [pos.get((t, d), d) for t, d in zip(tr.ticker, din)]
        tr["r"] = tr.ret * 100
        return tr

    S, C = trades("SIG"), trades("CTRL")
    cm = C.groupby("sig_day")["r"].median() if len(C) else pd.Series(dtype=float)
    rows = []
    for d, g in S.groupby("sig_day"):
        r = g.r.to_numpy()
        c = float(cm.get(d, np.nan))
        rows.append(dict(date=d, n=len(r), min=r.min(), median=float(np.median(r)),
                         mean=r.mean(), max=r.max(), ctrl=c,
                         edge=float(np.median(r)) - c,
                         tickers=",".join(sorted(g.ticker))))
    T = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)

    e = T.edge.dropna().to_numpy()
    pos_e = e[e > 0].sum()
    top2 = np.sort(e)[-2:].sum() if len(e) >= 2 else e.sum()
    agg = dict(occurrences=n_sig, trades=len(S), days=len(T),
               day_median_edge=float(np.median(e)) if len(e) else None,
               day_win_edge=float((e > 0).mean() * 100) if len(e) else None,
               day_median_raw=float(T["median"].median()),
               day_median_ctrl=float(T.ctrl.median()),
               top2_share_of_positive_edge=float(top2 / pos_e * 100) if pos_e > 0 else None,
               trade_median_raw=float(np.median(S.r)),
               trade_win_raw=float((S.r > 0).mean() * 100))
    return T, agg


def show(tokens, H, T, agg):
    print(f"\n{'='*96}")
    print(f"{' → '.join(tokens)}   ·   maxh={H}   ·   {agg['occurrences']} occurrences on "
          f"{agg['days']} days")
    print("=" * 96)
    print(f"  {'date':10s} {'n':>3} {'min':>8} {'median':>8} {'mean':>8} {'max':>8} "
          f"{'ctrl_med':>9} {'EDGE':>8}   tickers")
    print("  " + "-" * 92)
    for _, r in T.iterrows():
        tk = r.tickers if len(r.tickers) <= 28 else r.tickers[:25] + "…"
        print(f"  {str(r.date):10s} {r.n:>3d} {r['min']:+8.2f} {r['median']:+8.2f} "
              f"{r['mean']:+8.2f} {r['max']:+8.2f} {r.ctrl:+9.2f} {r.edge:+8.2f}   {tk}")
    print("  " + "-" * 92)
    print(f"  DAYS {agg['days']:>4}   day-median EDGE {agg['day_median_edge']:+.2f}   "
          f"day-win EDGE {agg['day_win_edge']:.0f}%   "
          f"(raw day-median {agg['day_median_raw']:+.2f} vs ctrl {agg['day_median_ctrl']:+.2f})")
    print(f"  top-2 days carry {agg['top2_share_of_positive_edge']:.0f}% of all positive edge"
          if agg['top2_share_of_positive_edge'] is not None else "  no positive edge")
    print(f"  [for contrast, trade-level: median {agg['trade_median_raw']:+.2f}  "
          f"win {agg['trade_win_raw']:.1f}%  over {agg['trades']} trades]")


if __name__ == "__main__":
    args = sys.argv[1:]
    H = 5
    if "--H" in args:
        i = args.index("--H"); H = int(args[i + 1]); del args[i:i + 2]
    tokens = args or ["Z2GL46", "Z2GL46", "Z2L46", "T1L3"]
    os.makedirs(OUT, exist_ok=True)
    df = load()
    res = run(df, tokens, H)
    if res:
        T, agg = res
        show(tokens, H, T, agg)
        tag = "_".join(tokens) + f"_H{H}"
        T.to_csv(os.path.join(OUT, f"{tag}.csv"), index=False)
        json.dump(agg, open(os.path.join(OUT, f"{tag}.json"), "w"), indent=1, default=str)
