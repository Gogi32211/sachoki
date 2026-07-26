"""K0 seasonality slice: per-calendar-month stats + year×month matrix. READ-ONLY."""
import os, sys
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
    tr["mo"] = tr["date_in"].str[5:7]
    print(f"K0 trades {len(tr):,}\n")

    print("── calendar month, ALL years pooled ──")
    print(f"{'mo':>3} {'n':>7} {'mean':>6} {'med':>6} {'win%':>5} {'PF':>5}  {'pos-yrs'}")
    ym = tr.groupby(["yr", "mo"])["ret"].median()
    for mo, g in tr.groupby("mo"):
        w = g.ret > 0
        pf = g.loc[w, "ret"].sum() / max(-g.loc[~w, "ret"].sum(), 1e-9)
        yrs = ym.xs(mo, level="mo")
        print(f"{mo:>3} {len(g):>7,} {g.ret.mean()*100:>+6.2f} {g.ret.median()*100:>+6.2f} "
              f"{w.mean()*100:>5.1f} {pf:>5.2f}  {int((yrs > 0).sum())}/{len(yrs)}")

    print("\n── calendar month, ex-2022 ──")
    t2 = tr[tr.yr != "2022"]
    ym2 = t2.groupby(["yr", "mo"])["ret"].median()
    for mo, g in t2.groupby("mo"):
        w = g.ret > 0
        pf = g.loc[w, "ret"].sum() / max(-g.loc[~w, "ret"].sum(), 1e-9)
        yrs = ym2.xs(mo, level="mo")
        print(f"{mo:>3} {len(g):>7,} {g.ret.mean()*100:>+6.2f} {g.ret.median()*100:>+6.2f} "
              f"{w.mean()*100:>5.1f} {pf:>5.2f}  {int((yrs > 0).sum())}/{len(yrs)}")

    print("\n── year × month MEDIAN ret% matrix ──")
    mat = tr.pivot_table(index="yr", columns="mo", values="ret", aggfunc="median") * 100
    cnt = tr.pivot_table(index="yr", columns="mo", values="ret", aggfunc="size")
    hdr = "    " + " ".join(f"{mo:>6}" for mo in mat.columns)
    print(hdr)
    for yr in mat.index:
        print(yr + " " + " ".join(
            f"{mat.loc[yr, mo]:>+6.1f}" if pd.notna(mat.loc[yr, mo]) else "     ·"
            for mo in mat.columns))
    print("\n(n per cell, min/med/max):",
          int(np.nanmin(cnt.values)), int(np.nanmedian(cnt.values)), int(np.nanmax(cnt.values)))


if __name__ == "__main__":
    run()
