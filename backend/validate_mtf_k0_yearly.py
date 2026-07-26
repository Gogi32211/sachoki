"""K0 per-year deep slice (n/mean/median/win/PF by year, ±2022 aggregates). READ-ONLY."""
import os, sys, time
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
from edge_replay import _pathsim
from validate_mtf_ema import _daily_universe, _tf_daily_snapshot
from validate_mtf_mid import _mid_masks, CHUNK, KW


def run():
    daily = _daily_universe()
    daily["day"] = daily["date"].str[:10]
    tickers = daily["ticker"].unique().tolist()
    parts = []
    for i in range(0, len(tickers), CHUNK):
        chunk = tickers[i:i + CHUNK]
        s15 = _tf_daily_snapshot(chunk, "15m"); s1h = _tf_daily_snapshot(chunk, "1h")
        s4h = _tf_daily_snapshot(chunk, "4h")
        if len(s15) == 0 or len(s1h) == 0 or len(s4h) == 0:
            continue
        d = daily[daily["ticker"].isin(chunk)]
        parts.append(d.merge(s15, on=["ticker", "day"], how="inner")
                      .merge(s1h, on=["ticker", "day"], how="inner")
                      .merge(s4h, on=["ticker", "day"], how="inner"))
    m = pd.concat(parts, ignore_index=True).sort_values(["ticker", "date"]).reset_index(drop=True)
    m["K0"] = (_mid_masks(m)["K0"] & (m["supp"] == 0)).values
    grp = {tk: g.reset_index(drop=True) for tk, g in m.groupby("ticker", sort=False)}
    tr = _pathsim(grp, "K0", **KW)
    print(f"K0 trades {len(tr):,}\n")
    print(f"{'yr':>4} {'n':>7} {'mean':>6} {'med':>6} {'win%':>5} {'PF':>5}")
    for y, g in tr.groupby("yr"):
        w = g.ret > 0
        pf = g.loc[w, "ret"].sum() / max(-g.loc[~w, "ret"].sum(), 1e-9)
        print(f"{y:>4} {len(g):>7,} {g.ret.mean()*100:>+6.2f} {g.ret.median()*100:>+6.2f} "
              f"{w.mean()*100:>5.1f} {pf:>5.2f}")
    for label, sub in (("ALL", tr), ("ex-2022", tr[tr.yr != "2022"]), ("ex-21+22", tr[~tr.yr.isin(["2021", "2022"])])):
        w = sub.ret > 0
        pf = sub.loc[w, "ret"].sum() / max(-sub.loc[~w, "ret"].sum(), 1e-9)
        print(f"\n{label:>8}: n={len(sub):,} mean{sub.ret.mean()*100:+.2f} med{sub.ret.median()*100:+.2f} "
              f"win{w.mean()*100:.1f}% PF{pf:.2f}")


if __name__ == "__main__":
    run()
