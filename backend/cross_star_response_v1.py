"""CROSS_STAR — post-signal price response. PURELY DESCRIPTIVE.

Not an edge study: no controls, no matching, no incremental comparison, no veto, no thresholds, no
_pathsim and no cooldown. One question only — after the two signals coincided on a ticker-day, where
did price go and by how much?

Prices: the canonical 1D authority (ovd_build.canonical_current), the same basis every sealed family
uses. Offsets are TRADING SESSIONS inside each ticker's own canonical series, never calendar days;
a ticker without N further real sessions is UNRESOLVED (NaN), never carried forward.

⚠️ 2026: no SIGNAL DATE is from 2026. Forward prices may reach into 2026 to resolve a late-2025
signal — that is resolution, not evaluation of the locked period — and the number of observations
that depend on it is reported separately so the effect of excluding them can be seen.
"""
from __future__ import annotations
import os, sys
import numpy as np, pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ovd_build as B

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "data") + os.sep
HOR_TABLE = [1, 3, 5, 10, 20]
PATH_K = 20
HOR = list(range(1, PATH_K + 1))
L = lambda c="═", n=96: print(c * n)


def load():
    E = pd.read_parquet(R + "cross_star_arms.parquet")
    E["date"] = pd.to_datetime(E["date"])
    assert (E.date < "2026-01-01").all(), "a 2026 signal date is present"
    cur = B.canonical_current()
    P = pd.read_parquet(cur["derived_parquet"], columns=["ticker", "session_date", "open", "close"])
    P = P.rename(columns={"session_date": "date"})
    P["date"] = pd.to_datetime(P["date"])
    P = P.sort_values(["ticker", "date"]).reset_index(drop=True)
    P["i"] = P.groupby("ticker").cumcount()
    P["n"] = P.groupby("ticker")["i"].transform("size")
    return E, P, cur


def responses(events: pd.DataFrame, P: pd.DataFrame, log=print):
    """close→close and next-open→close returns at every horizon, on trading-session offsets."""
    m = events[["ticker", "date"]].drop_duplicates()
    dup = len(events) - len(m)
    j = m.merge(P[["ticker", "date", "i", "n", "close", "open"]], on=["ticker", "date"], how="left")
    no_price = int(j["i"].isna().sum())
    j = j[j["i"].notna()].copy()
    j["i"] = j["i"].astype(int)
    idx = P.set_index(["ticker", "i"])
    out = {"n_events": len(m), "dup_dropped": dup, "no_canonical_price": no_price,
           "scored": len(j), "rows": j[["ticker", "date"]].copy()}
    # next open (t+1) — the tradable anchor, since a daily signal is only known at that day's close
    nxt = idx.reindex(pd.MultiIndex.from_arrays([j.ticker, j.i + 1]))
    j["open1"] = np.where((j.i + 1) < j.n, nxt["open"].to_numpy(), np.nan)
    for h in HOR:
        fut = idx.reindex(pd.MultiIndex.from_arrays([j.ticker, j.i + h]))
        c = np.where((j.i + h) < j.n, fut["close"].to_numpy(), np.nan)
        out[f"c2c_{h}"] = c / j.close.to_numpy() - 1.0
        out[f"o2c_{h}"] = c / j.open1.to_numpy() - 1.0
        out[f"fut_i_{h}"] = np.where((j.i + h) < j.n, (j.i + h).to_numpy(), -1)
    out["i"], out["n"], out["ticker"], out["date"] = j.i.to_numpy(), j.n.to_numpy(), j.ticker.to_numpy(), j.date.to_numpy()
    return out


def table(res, kind, label, mask=None):
    print(f"\n  {label}")
    print(f"  {'Horizon':>8s} {'N':>8s} {'Mean':>9s} {'Median':>9s} {'% Up':>7s} {'% Down':>7s}"
          f" {'P10':>9s} {'P25':>9s} {'P75':>9s} {'P90':>9s}")
    for h in HOR_TABLE:
        v = res[f"{kind}_{h}"]
        if mask is not None:
            v = v[mask]
        v = v[np.isfinite(v)]
        if not len(v):
            print(f"  {str(h)+'d':>8s} {'—':>8s}"); continue
        q = np.percentile(v, [10, 25, 75, 90])
        print(f"  {str(h)+'d':>8s} {len(v):>8,} {100*v.mean():>8.2f}% {100*np.median(v):>8.2f}%"
              f" {100*(v>0).mean():>6.1f}% {100*(v<0).mean():>6.1f}%"
              f" {100*q[0]:>8.2f}% {100*q[1]:>8.2f}% {100*q[2]:>8.2f}% {100*q[3]:>8.2f}%")


def path(res, mask=None, label=""):
    print(f"\n  average / median normalised path — {label}   (Day 0 = 0 %)")
    print(f"  {'day':>4s} {'mean':>9s} {'median':>9s} {'% up':>7s} {'n':>8s}")
    for k in range(0, PATH_K + 1):
        if k == 0:
            print(f"  {0:>4d} {0.0:>8.2f}% {0.0:>8.2f}% {'—':>7s} {'—':>8s}"); continue
        v = res.get(f"c2c_{k}")
        if v is None:
            continue
        if mask is not None:
            v = v[mask]
        v = v[np.isfinite(v)]
        print(f"  {k:>4d} {100*v.mean():>8.2f}% {100*np.median(v):>8.2f}% {100*(v>0).mean():>6.1f}% {len(v):>8,}")


def main():
    E, P, cur = load()
    print(f"canonical authority {cur['derived_sha256_16']} · {P.date.min().date()} … {P.date.max().date()}")
    groups = {
        "A) TOP_STAR × BOTTOM_STAR": E[E.same_day_cross],
        "B) CD_BOTH × BOTTOM_STAR": E[E.coverage_all_required & E.cd30 & E.cd60 & E.bottom_star_any],
    }
    last2025 = P[P.date <= "2025-12-31"].groupby("ticker")["i"].max()

    for name, ev in groups.items():
        L("═"); print(name); L("═")
        res = responses(ev, P)
        print(f"  events {res['n_events']:,} · duplicate ticker-days removed {res['dup_dropped']:,}"
              f" · no canonical price {res['no_canonical_price']:,}"
              f" · scored {res['scored']:,}")
        d = pd.Series(res["date"])
        need26 = int((pd.Series(res["i"]) + 20 > pd.Series(res["ticker"]).map(last2025).fillna(-1)).sum())
        print(f"  20d horizons reaching into 2026 to resolve: {need26:,} "
              f"({100*need26/max(res['scored'],1):.1f} %) — every SIGNAL date is pre-2026")
        unres = {h: int(np.isnan(res[f"c2c_{h}"]).sum()) for h in HOR_TABLE}
        print("  unresolved (no N further real sessions): " + " · ".join(f"{h}d:{v:,}" for h, v in unres.items()))

        wn = pd.Series(np.where(d <= "2023-12-31", "MINE", "VERIFY"))
        for wlab in ("FULL", "MINE", "VERIFY"):
            m = None if wlab == "FULL" else (wn == wlab).to_numpy()
            table(res, "c2c", f"{wlab} · close(t) -> close(t+N)", m)
        table(res, "o2c", "FULL · open(t+1) -> close(t+N)   [tradable anchor]")
        path(res, None, name)

        print("\n  year by year (close -> close)")
        print(f"  {'year':>5s} {'N':>7s} | {'5d mean':>8s} {'5d med':>8s} {'5d %up':>7s} |"
              f" {'10d mean':>9s} {'10d med':>8s} {'10d %up':>7s} |"
              f" {'20d mean':>9s} {'20d med':>8s} {'20d %up':>7s}")
        for y in sorted(d.dt.year.unique()):
            ym = (d.dt.year == y).to_numpy()
            row = f"  {y:>5d} {int(ym.sum()):>7,} |"
            for h in (5, 10, 20):
                v = res[f"c2c_{h}"][ym]; v = v[np.isfinite(v)]
                row += (f" {100*v.mean():>8.2f}% {100*np.median(v):>7.2f}% {100*(v>0).mean():>6.1f}% |"
                        if len(v) else f" {'-':>26s} |")
            print(row)


if __name__ == "__main__":
    main()
