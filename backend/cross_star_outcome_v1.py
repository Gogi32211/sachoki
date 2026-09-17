"""CROSS_STAR_CONCURRENCE_V1 — OUTCOME RUN. Executes the spec frozen in commit 786b76f.

Nothing here is chosen at run time. Every definition — arms, buckets, distance, k, windows, horizons,
the cooldown neutralisation — comes from research_out/CROSS_STAR_CONCURRENCE_V1.md §11.

⛔ COOLDOWN. _pathsim carries a stateful 5-bar same-ticker cooldown that thins the arms unevenly
(TOP-only 89.3 %, CROSS 25.9 %). Per feedback-pathsim-cooldown-estimand the per-observation estimand
and its neutralisation were REGISTERED PRE-OUTCOME: the UNMODIFIED engine is called over five
disjoint bar-index-mod-5 masks, so consecutive taken signals in one mask are >= 5 bars apart.
"""
from __future__ import annotations
import os, sys, json
import numpy as np, pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ovd_build as B, ovd_outcome_v1 as O, ovd_outcome_direct_v1 as OD

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FEAT = os.path.join(ROOT, "data", "cross_star_features.parquet")
OUT = os.path.join(ROOT, "data", "cross_star_outcomes.parquet")

# ── FROZEN (786b76f) ──────────────────────────────────────────────────────────────────────────
MINE, VERIFY = ("2021-01-01", "2023-12-31"), ("2024-01-01", "2025-12-31")
LOCKED_FROM = "2026-01-01"                 # never accessed
K = 5
DV_EDGES = [0, 1e5, 1e6, 3e6, 1e7, 3e7, 1e8, np.inf]      # declared pre-outcome
PX_EDGES = [-1, 2, 8, 21, 89, 377, np.inf]
MAXH_PRIMARY, MAXH_SECONDARY = 60, 20
SPEC_COMMIT = "786b76f"
L = lambda c="─", n=86: print(c * n)


def arms():
    X = pd.read_parquet(FEAT)
    X["date"] = pd.to_datetime(X["date"])
    if (X.date >= LOCKED_FROM).any():
        X = X[X.date < LOCKED_FROM]                       # 2026 never enters the frame at all
    E = X[X.both_covered].copy()
    E = E[(E.date >= MINE[0]) & (E.date <= VERIFY[1])]
    E["dvb"] = pd.cut(E.dollar_vol, DV_EDGES, labels=False)
    E["pxb"] = pd.cut(E.close, PX_EDGES, labels=False)
    E["cross"] = E.same_day_cross
    E["toponly"] = E.top_star_any & ~E.bottom_star_any
    E["botonly"] = E.bottom_star_any & ~E.top_star_any
    E["cd_arm"] = E.coverage_all_required & E.bottom_star_any & E.cd_both
    E["cdonly"] = E.coverage_all_required & E.cd_both & ~E.bottom_star_any
    return E


def match(E, treated_col, control_col, tag):
    """k = K nearest inside (date, universe, price bucket, $vol bucket) by
    |Δlog dvol| + 0.5·|Δlog price|. Deterministic — no sampling variance."""
    key = ["date", "universe", "pxb", "dvb"]
    t = E.loc[E[treated_col], key + ["ticker", "dollar_vol", "close"]].rename(
        columns={"ticker": "t_ticker", "dollar_vol": "t_dv", "close": "t_px"})
    c = E.loc[E[control_col], key + ["ticker", "dollar_vol", "close"]].rename(
        columns={"ticker": "c_ticker", "dollar_vol": "c_dv", "close": "c_px"})
    m = t.merge(c, on=key, how="inner")
    m = m[m.t_ticker != m.c_ticker]
    lg = lambda v: np.log(np.maximum(v.to_numpy(float), 1e-9))
    m["dist"] = np.abs(lg(m.t_dv) - lg(m.c_dv)) + 0.5 * np.abs(lg(m.t_px) - lg(m.c_px))
    m = m.sort_values("dist").groupby(key + ["t_ticker"], observed=True, sort=False).head(K)
    m["arm"] = tag
    return m[key + ["t_ticker", "c_ticker", "dist", "arm"]]


def run_pathsim(frames, keys: pd.DataFrame, maxh: int, log=print):
    """Five disjoint bar-index-mod-5 masks over the UNMODIFIED engine — the registered
    per-observation construction. Returns trades and the engine's own conservation per mask."""
    ps, _ = O.sacred_pathsim()
    O.assert_no_local_pathsim(open(__file__).read(), "cross_star")
    pos = {tk: {d: i for i, d in enumerate(g["date"].to_numpy())} for tk, g in frames.items()}
    keys = keys.copy()
    keys["bi"] = [pos.get(tk, {}).get(s, -1) for tk, s in zip(keys.ticker, keys.session)]
    dropped_no_frame = int((keys.bi < 0).sum())
    keys = keys[keys.bi >= 0]
    out, cons = [], []
    for r in range(5):
        k = keys[keys.bi % 5 == r][["ticker", "session"]]
        if not len(k):
            continue
        tr, cn = OD.direct_trades(ps, frames, k, maxh=maxh)
        cn["mask"] = r; cn["keys_in"] = int(len(k)); cons.append(cn)
        out.append(tr)
    T = pd.concat(out, ignore_index=True) if out else pd.DataFrame()
    return T, cons, dropped_no_frame


def main():
    L("═"); print(f"CROSS_STAR_CONCURRENCE_V1 · OUTCOME RUN · spec {SPEC_COMMIT}"); L("═")
    ps, _ = O.sacred_pathsim()
    cur = B.canonical_current()
    E = arms()

    # ── preflight, fail closed ────────────────────────────────────────────────────────────
    assert O.PATHSIM_SRC_SHA == "0e74668f554910de", "pathsim digest mismatch"
    assert int((E.date >= LOCKED_FROM).sum()) == 0, "2026 rows present"
    assert MAXH_PRIMARY == 60 and MAXH_SECONDARY == 20
    print(f"  pathsim digest      {O.PATHSIM_SRC_SHA}  OK")
    print(f"  canonical derived   {cur['derived_sha256_16']}")
    print(f"  2026 rows in frame  {int((E.date >= LOCKED_FROM).sum())}  (locked)")

    mt = match(E, "cross", "toponly", "TOP")
    mb = match(E, "cross", "botonly", "BOTTOM")
    frames = OD.load_frames(cur["derived_parquet"], E.ticker.unique())

    need = pd.concat([
        E.loc[E.cross, ["ticker", "date"]],
        mt.rename(columns={"c_ticker": "ticker"})[["ticker", "date"]],
        mb.rename(columns={"c_ticker": "ticker"})[["ticker", "date"]],
        E.loc[E.cd_arm | E.cdonly | E.botonly, ["ticker", "date"]],
    ]).drop_duplicates()
    need["session"] = need["date"].dt.strftime("%Y-%m-%d")
    print(f"  unique keys needing an outcome: {len(need):,}")

    res = {}
    for maxh in (MAXH_PRIMARY, MAXH_SECONDARY):
        T, cons, nofr = run_pathsim(frames, need[["ticker", "session"]], maxh)
        res[maxh] = dict(trades=T, cons=cons, no_frame=nofr)
        print(f"  maxh={maxh:<3d} trades {len(T):>9,} · keys without a canonical frame {nofr:,}")

    json.dump({str(h): [{k: int(v) if isinstance(v, (int, np.integer)) else v for k, v in c.items()}
                        for c in res[h]["cons"]] for h in res},
              open(os.path.join(ROOT, "data", "cross_star_conservation.json"), "w"), indent=1)
    for h in res:
        res[h]["trades"].assign(maxh=h).to_parquet(
            OUT.replace(".parquet", f"_maxh{h}.parquet"), index=False)
    mt.to_parquet(os.path.join(ROOT, "data", "cross_star_match_TOP.parquet"), index=False)
    mb.to_parquet(os.path.join(ROOT, "data", "cross_star_match_BOTTOM.parquet"), index=False)
    E.to_parquet(os.path.join(ROOT, "data", "cross_star_arms.parquet"), index=False)
    print("\n  artifacts written: arms · match_TOP · match_BOTTOM · outcomes_maxh60 · outcomes_maxh20 · conservation")


if __name__ == "__main__":
    main()
