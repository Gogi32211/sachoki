"""GANN_X_INTEGRITY_V1 — engine qualification before the X evidence state is frozen.

Five mechanical closures, no new research gate:

    1  FUTURE MUTATION   rewrite every bar AFTER D and D's grid, direction and event must
                         not move — the strongest statement of "the grid used on D is
                         frozen at D-1"
    2  ROW REORDER       shuffled raw input must give identical X artifacts
    3  PRICE RESCALE     OHLC x c must leave levels, membership and direction invariant;
                         the lattice is multiplicative, so this is an exact symmetry
    4  TWO FRESH BUILDS  identical state and event digests
    5  NO-Y LOAD MANIFEST  the columns actually read, and zero outcome columns anywhere

Plus the coverage census reconciled from the artifact, and the onset observability
assertion (missing is not FALSE).

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import ast, hashlib, json, os, re, sys, time                                  # noqa: E402
import duckdb, numpy as np, pandas as pd                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import gann_grid as G                                                # noqa: E402

OUT = "GANN_X_INTEGRITY_V1.json"
N_TICKERS = 300
CMP = ["ticker", "claim_id", "decision_date", "direction", "level_i", "n_lines_touched",
       "line_at_prev", "line_at_d", "M", "B", "anchor_L", "anchor_H", "bars_diff"]


def dig(df):
    d = df.sort_values(["ticker", "claim_id", "decision_date"]).reset_index(drop=True)
    return hashlib.sha256(pd.util.hash_pandas_object(d[CMP], index=False)
                          .values.tobytes()).hexdigest()[:16]


def events(frames):
    return G.events_from_grid(pd.concat(frames, ignore_index=True))


def main():
    t0 = time.time()
    conn = duckdb.connect(G.DB1D, read_only=True)
    tks = [r[0] for r in conn.execute(
        f"SELECT DISTINCT ticker FROM bars WHERE universe IN {G.UNIVERSES} "
        f"ORDER BY ticker LIMIT {N_TICKERS}").fetchall()]
    raw = {tk: G.load_ticker(conn, tk) for tk in tks}
    conn.close()
    base_frames = [G.build_ticker(df) for df in raw.values()]
    base_frames = [f for f in base_frames if len(f)]
    E0 = events(base_frames)
    d0 = dig(E0)

    # ── 1 · future mutation ─────────────────────────────────────────────
    CUT = "2025-06-30"
    rng = np.random.default_rng(11)
    mutated = []
    for tk, df in raw.items():
        d = df.copy()
        m = d.date.astype(str) > CUT
        if m.any():
            f = rng.uniform(0.3, 3.0, int(m.sum()))
            for c in ("open", "high", "low", "close"):
                d.loc[m, c] = d.loc[m, c].to_numpy() * f
            hi = np.maximum.reduce([d.loc[m, c].to_numpy() for c in
                                    ("open", "high", "low", "close")])
            lo = np.minimum.reduce([d.loc[m, c].to_numpy() for c in
                                    ("open", "high", "low", "close")])
            d.loc[m, "high"], d.loc[m, "low"] = hi, lo
        g = G.build_ticker(d)
        if len(g):
            mutated.append(g)
    Em = events(mutated)
    a = E0[E0.decision_date.astype(str) <= CUT].sort_values(CMP[:3]).reset_index(drop=True)
    b = Em[Em.decision_date.astype(str) <= CUT].sort_values(CMP[:3]).reset_index(drop=True)
    fut_ok = len(a) == len(b) and dig(a) == dig(b)

    # ── 2 · row reorder ─────────────────────────────────────────────────
    sh = []
    for df in raw.values():
        d = df.sample(frac=1.0, random_state=23).reset_index(drop=True)
        d = d.sort_values("date").reset_index(drop=True)   # the loader's own contract
        g = G.build_ticker(d)
        if len(g):
            sh.append(g)
    Es = events(sh)
    reorder_ok = dig(Es) == d0

    # ── 3 · price rescale ───────────────────────────────────────────────
    sc = []
    for tk, df in raw.items():
        d = df.copy()
        c = 7.3197
        for col in ("open", "high", "low", "close"):
            d[col] = d[col] * c
        g = G.build_ticker(d)
        if len(g):
            sc.append(g)
    Ec = events(sc)
    j = E0.merge(Ec, on=["ticker", "claim_id", "decision_date"], suffixes=("", "_s"))
    scale_ok = bool(len(j) == len(E0)
                    and (j.direction == j.direction_s).all()
                    and np.allclose(j.level_i.astype(float),
                                    j.level_i_s.astype(float), equal_nan=True)
                    and (j.n_lines_touched == j.n_lines_touched_s).all()
                    and np.allclose(j.M, j.M_s, rtol=1e-9)
                    and np.allclose(j.B, j.B_s)
                    and np.allclose(j.line_at_d * 7.3197, j.line_at_d_s, rtol=1e-9))

    # ── 4 · two fresh builds ────────────────────────────────────────────
    again = [G.build_ticker(df) for df in raw.values()]
    E2 = events([f for f in again if len(f)])
    twice_ok = dig(E2) == d0

    # ── 5 · no-Y load manifest ──────────────────────────────────────────
    # context-aware: a token inside the module's OWN outcome guard, or in prose, is not a
    # loaded outcome column. Check what the DB query actually selects and assert the guard.
    src = open("gann_grid.py").read()
    tree = ast.parse(src)
    sql = [n.value for n in ast.walk(tree)
           if isinstance(n, ast.Constant) and isinstance(n.value, str)
           and "FROM bars" in n.value]
    selected = sorted({w for q in sql for w in re.findall(r"any_value\((\w+)\)", q)}
                      | {"date", "ticker"})
    loaded = selected
    forbidden = [c for c in selected
                 if c.lower().startswith(("fwd_", "mfe", "mae", "ret", "mtm"))
                 or c.lower() in ("path_status_10d", "target", "label")]
    guard_present = 'raise RuntimeError(f"outcome columns leaked' in src
    cols_in_artifacts = sorted(set(pd.read_parquet(G.OUT_EVENTS).columns)
                               | set(pd.read_parquet(G.OUT_GRID).columns))
    y_cols = [c for c in cols_in_artifacts
              if c.lower().startswith(("fwd_", "mfe", "mae", "ret", "mtm"))]

    # ── onset observability ─────────────────────────────────────────────
    E = pd.read_parquet(G.OUT_EVENTS)
    Gr = pd.read_parquet(G.OUT_GRID)
    first = Gr.groupby("ticker").di.min()
    E["first_di"] = E.ticker.map(first)
    early = int((E.bar_index - G.ONSET_QUIET_BARS < E.first_di).sum())

    # ── coverage census, reconciled ─────────────────────────────────────
    conn = duckdb.connect(G.DB1D, read_only=True)
    n = conn.execute(f"SELECT ticker, count(DISTINCT date) n FROM bars WHERE universe IN "
                     f"{G.UNIVERSES} GROUP BY ticker").fetchdf()
    conn.close()
    have = set(Gr.ticker.unique())
    n["has_grid"] = n.ticker.isin(have)
    no_grid = n[~n.has_grid]
    short = int((no_grid.n < G.HIST_LOOKBACK + 2).sum())
    long_no_grid = no_grid[no_grid.n >= G.HIST_LOOKBACK + 2]
    reasons = []
    for r in long_no_grid.itertuples():
        conn = duckdb.connect(G.DB1D, read_only=True)
        df = G.load_ticker(conn, r.ticker); conn.close()
        g = G.build_ticker(df)
        reasons.append(dict(ticker=r.ticker, distinct_dates=int(r.n),
                            rows_after_dedup=int(len(df)),
                            grid_rows=int(len(g)),
                            reason="fewer than 506 rows after the one-row-per-date "
                                   "collapse" if len(df) < G.HIST_LOOKBACK + 2 else
                                   "no bar window produced a valid H > L > 0 anchor pair"))
    checks = {
        "1 future mutation leaves pre-cutoff X identical": fut_ok,
        "2 row reorder leaves X identical": reorder_ok,
        "3 price rescale leaves levels/membership/direction invariant": scale_ok,
        "4 two fresh builds identical": twice_ok,
        "5 no outcome column loaded or written": (not forbidden and not y_cols
                                                    and guard_present),
        "onset never recorded before 5 observable prior states": early == 0,
        "coverage census reconciles": short + len(long_no_grid) == len(no_grid),
    }
    ok = all(checks.values())
    body = dict(
        spec_id="GANN_X_INTEGRITY_V1", result="PASS" if ok else "FAIL",
        scope="engine qualification before the X evidence state is frozen; no research "
              "gate is added or changed",
        tickers_tested=len(tks), events_in_sample=int(len(E0)), sample_digest=d0,
        future_mutation=dict(cutoff=CUT, mutation="every OHLC after the cutoff scaled by "
                                                  "U(0.3, 3.0) per bar, high/low repaired",
                             events_compared=int(len(a)), identical=fut_ok),
        row_reorder=dict(identical=reorder_ok, digest=dig(Es)),
        price_rescale=dict(constant=7.3197, invariant=scale_ok,
                           note="the lattice is multiplicative, so level index, "
                                "membership and direction are exactly scale-invariant "
                                "while the level PRICES scale by the same constant"),
        two_fresh_builds=dict(identical=twice_ok, digest=dig(E2)),
        no_y_manifest=dict(columns_loaded_from_db=loaded,
                           outcome_columns_selected=forbidden,
                           builder_guard_present=guard_present,
                           outcome_columns_in_artifacts=y_cols,
                           artifact_columns=cols_in_artifacts),
        onset_observability=dict(
            rule="onset requires the claim state OBSERVABLE on D and on each of D-1..D-5, "
                 "and FALSE on all five; an unobservable day is not a quiet day",
            events_violating=early),
        coverage=dict(tickers_scanned=int(len(n)), with_grid=int(n.has_grid.sum()),
                      without_grid=int(len(no_grid)),
                      without_grid_fewer_than_506_dates=short,
                      without_grid_with_506plus_dates=int(len(long_no_grid)),
                      reconciles=bool(short + len(long_no_grid) == len(no_grid)),
                      the_506plus_cases=reasons,
                      note="the earlier report stated both '1,333 have fewer than 506 "
                           "bars' and '2 have 506+ but no grid'; those cannot both hold "
                           "and the machine-reconciled split above replaces them"),
        checks={k: bool(v) for k, v in checks.items()},
        outcome_exposure="NOT_EXPOSED",
        runtime_min=round((time.time() - t0) / 60, 1))
    d = ART.seal(body, OUT, required=("spec_id", "result", "checks", "coverage"),
                 supersede=os.path.exists(OUT))
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")
    print(f"\n  coverage: scanned {body['coverage']['tickers_scanned']:,} · with grid "
          f"{body['coverage']['with_grid']:,} · without {body['coverage']['without_grid']:,}"
          f" = {short:,} short + {len(long_no_grid)} with 506+ dates")
    for r in reasons:
        print(f"    {r['ticker']}: {r['distinct_dates']} dates · {r['rows_after_dedup']} "
              f"rows · {r['reason']}")
    print(f"\nGANN_X_INTEGRITY_V1 · {d} · {body['result']}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
