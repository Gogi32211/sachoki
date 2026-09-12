"""INTRADAY_EFFORT_BALANCE_V1 — a separate research family (user-approved 5-line plan, 2026-09-06).

Question: inside a 1D bar, does the DIRECTION of intraday effort (15m bars with rising volume on up-closes vs on
down-closes — the WLNBB L-labels) carry information beyond the daily candle itself? Only one coherent hypothesis
from the book is registered: a RED daily close whose intraday effort is UP (absorption under the candle) and its
mirror. Everything else in the L-composition is DESCRIPTIVE.

  EFFORT_BALANCE(D) = (n_L34 + n_L3 − n_L46) / (n_L34 + n_L3 + n_L46)   over the 15m bars of session D
  (exact-label counting: a 15m bar counts as its exact WLNBB label — "L34" = digits {3,4}, "L3" = {3} alone,
   "L46" = {4,6}; L12/L25/L5/L2/L4 are the low-volume side and are descriptive only)
  UNAVAILABLE when the denominator < 3.   V variant: only 15m bars with volume > SMA20(15m volume) × 1.0 are counted.
  Bins (frozen): B- <= -0.33 · B0 (-0.33, +0.33) · B+ >= +0.33.  Daily colour: RED close<open · GREEN close>open (doji outside).

  k = 16 cells:  class A (non-V): 3 bins × 2 colours = 6  +  C1 [RED ∧ B+ ∧ RSI14 < 40]  +  C2 [GREEN ∧ B- ∧ RSI14 >= 60]
                 class B (V):     the same 8 on EFFORT_BALANCE_V.
  1H = replication of the 8 non-V cells (fractal echo), NOT in k.
  Estimand: direct sacred _pathsim per registered cell mask (5-bar cooldown included — never partitioned), entry
  D+1 open, book config (trail atr_k 12, maxh 60, slip 0.0015). Control: median of OTHER direct trades of the same
  class on the same entry date (ticker-exclusive), >= 20 others. Day-clustered median. DSR at k = 16.
  OOS reserved BEFORE any outcome: MINE 2021-09-07..2024-12-31 · VERIFY 2025-01-01..2026-09-03.
  Gates (registered): MINE median>0 · day-win>50% · >=3 positive years (years with >=5 days) · worst >= -2 pp ·
  DSR >= 0.95 · not THIN (n_days>=30, n_obs>=100); VERIFY (mine survivors only) median>0 · day-win>50% · worst >= -2.
  BUILD_CANDIDATE = both; VETO mirror; mine-pass/verify-fail = MINE_ONLY_NOT_REPLICATED; else NULL.
  Limitations: canonical 1D = split-adjusted as of fetch, DIVIDENDS_NOT_ADJUSTED -> price-return paths.
"""
from __future__ import annotations
import os, sys, json, time, hashlib, glob, inspect                       # noqa: E402
import numpy as np                                                       # noqa: E402
import pandas as pd                                                      # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import HardStop, RunSpace, DATA as STORE_DATA, ny_local   # noqa: E402  (V1 guards reused, never edited)
import ovd_build as B                                                    # noqa: E402
import ovd_outcome_v1 as O                                               # noqa: E402  (loo/day/trade stats, sacred engine, guards)
import ovd_outcome_direct_v1 as OD                                       # noqa: E402  (direct_trades, load_frames)

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/INTRADAY_EFFORT_BALANCE_V1"
FAMILY = "INTRADAY_EFFORT_BALANCE_V1"
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
WLNBB = dict(ma_period=20, vol_avg_len=20, vol_avg_mult=1.0, stdev="population (Pine ta.stdev default)")
BAL_BINS = {"B-": (-np.inf, -0.33), "B0": (-0.33, 0.33), "B+": (0.33, np.inf)}   # B-: x <= -0.33 · B0: -0.33 < x < 0.33 · B+: x >= 0.33
MIN_EFFORT_BARS = 3
RSI_LOW, RSI_HIGH = 40.0, 60.0
K_EXPECTED = 16
GATES = dict(mine=dict(median_gt=0.0, day_win_gt=50.0, pos_years_ge=3, worst_year_ge=-2.0, dsr_ge=0.95, min_days_per_year=5),
             verify=dict(median_gt=0.0, day_win_gt=50.0, worst_year_ge=-2.0),
             thin=dict(n_days_lt=30, n_obs_lt=100), k=K_EXPECTED,
             source="book analysis standard (yrs >= 2/3 of window, worst >= -2 pp, DSR at family k); registered before any outcome")
CODE_OF = {"L12": 3, "L25": 18, "L34": 12, "L46": 40, "L2": 2, "L3": 4, "L4": 8, "L5": 16}   # exact digit sets (bit0 L1 .. bit5 L6)


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


# ── WLNBB L-code on an intraday store (exact Pine semantics, continuous per ticker in time order) ──
def lcode_sql(src: str) -> str:
    ny = ny_local("date")
    return f"""
    WITH b AS (
      SELECT ticker, date, open, high, low, close, COALESCE(volume, 0)::DOUBLE AS v,
             CAST({ny} AS DATE) AS session
      FROM {src}),
    w AS (
      SELECT *, avg(v) OVER w20 AS mid, stddev_pop(v) OVER w20 AS sd, count(*) OVER w20 AS n20,
             lag(v) OVER w1 AS v1, lag(close) OVER w1 AS c1, lag(high) OVER w1 AS h1, lag(low) OVER w1 AS l1
      FROM b WINDOW w20 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                    w1  AS (PARTITION BY ticker ORDER BY date)),
    k AS (
      SELECT *, CASE WHEN n20 < 20 THEN NULL
                     WHEN v < mid - sd THEN 0 WHEN v < mid THEN 1 WHEN v < mid + sd THEN 2 WHEN v < (mid + sd) + mid THEN 3 ELSE 4 END AS bkt
      FROM w),
    p AS (SELECT *, lag(bkt) OVER (PARTITION BY ticker ORDER BY date) AS bkt1 FROM k),
    f AS (
      SELECT *,
        (bkt > bkt1) OR (bkt = bkt1 AND v > v1) AS vol_up,
        (bkt < bkt1) OR (bkt = bkt1 AND v < v1) AS vol_dn,
        close > c1 AS up_close, close < c1 AS dn_close, close >= l1 AS no_new_low, close <= h1 AS no_new_high,
        v > mid * {WLNBB['vol_avg_mult']} AS v_above
      FROM p WHERE bkt IS NOT NULL AND bkt1 IS NOT NULL)
    SELECT ticker, session, date, v_above,
      (CASE WHEN vol_dn AND up_close THEN 1 ELSE 0 END) + (CASE WHEN vol_dn AND no_new_low THEN 2 ELSE 0 END)
      + (CASE WHEN vol_up AND up_close THEN 4 ELSE 0 END) + (CASE WHEN vol_up AND no_new_high THEN 8 ELSE 0 END)
      + (CASE WHEN vol_dn AND dn_close THEN 16 ELSE 0 END) + (CASE WHEN vol_up AND dn_close THEN 32 ELSE 0 END) AS code
    FROM f"""


def session_counts_sql(codes_tbl: str) -> str:
    cnt = ", ".join(f"count(*) FILTER (WHERE code = {c}) AS n_{n.lower()}, count(*) FILTER (WHERE code = {c} AND v_above) AS nv_{n.lower()}"
                    for n, c in CODE_OF.items())
    return f"""
    SELECT ticker, session, count(*) AS n_bars, count(*) FILTER (WHERE v_above) AS n_bars_v, {cnt}
    FROM {codes_tbl} GROUP BY 1, 2"""


def balance(n34, n3, n46, min_bars: int = MIN_EFFORT_BARS):
    d = n34 + n3 + n46
    return None if d < min_bars else (n34 + n3 - n46) / d


def bin_of(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    if x <= -0.33:
        return "B-"
    if x >= 0.33:
        return "B+"
    return "B0"


def rsi_wilder(close: pd.Series, n: int = 14) -> pd.Series:
    d = close.diff()
    up = d.clip(lower=0.0); dn = (-d).clip(lower=0.0)
    au = up.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
    ad = dn.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
    rs = au / ad.replace(0.0, np.nan)
    out = 100.0 - 100.0 / (1.0 + rs)
    return out.where(ad > 0, 100.0).where(au.notna())


def cells() -> list[dict]:
    out = []
    for cls, col in (("A", "bal"), ("B", "bal_v")):
        for colour in ("RED", "GREEN"):
            for b in ("B-", "B0", "B+"):
                out.append(dict(claim_id=f"{cls}|{colour}|{b}", cls=cls, column=col, colour=colour, bin=b, rsi=None, kind="grid"))
        out.append(dict(claim_id=f"{cls}|RED|B+|RSI<40", cls=cls, column=col, colour="RED", bin="B+", rsi="<40", kind="conditional",
                        hypothesis="absorption under a red candle in an oversold context -> buy-side"))
        out.append(dict(claim_id=f"{cls}|GREEN|B-|RSI>=60", cls=cls, column=col, colour="GREEN", bin="B-", rsi=">=60", kind="conditional",
                        hypothesis="distribution under a green candle in an overbought context -> veto-side"))
    return out


def cell_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    m = (X["colour"].to_numpy(object) == c["colour"]) & (X[c["column"] + "_bin"].to_numpy(object) == c["bin"])
    if c["rsi"] == "<40":
        m &= X["rsi14"].to_numpy(float) < RSI_LOW
    elif c["rsi"] == ">=60":
        m &= X["rsi14"].to_numpy(float) >= RSI_HIGH
    return m & X["eligible"].to_numpy(bool)


def class_mask(X: pd.DataFrame, cls: str) -> np.ndarray:
    col = "bal" if cls == "A" else "bal_v"
    return X["eligible"].to_numpy(bool) & X[col].notna().to_numpy() & X["colour"].isin(["RED", "GREEN"]).to_numpy()


# ── X-only build ───────────────────────────────────────────────────────────────────────────
def build(log=print) -> dict:
    import duckdb
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("IEB_%Y%m%dT%H%M%SZ", time.gmtime()))
    con = duckdb.connect(); con.execute("pragma threads=8")
    con.execute(f"ATTACH '{os.path.join(STORE_DATA, 'studio_15m.duckdb')}' AS m15 (READ_ONLY)")
    con.execute(f"ATTACH '{os.path.join(STORE_DATA, 'studio_1h.duckdb')}' AS h1 (READ_ONLY)")
    rep = dict(run_id=run.run_id, family=FAMILY, canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]), oos=OOS, wlnbb=WLNBB)
    t0 = time.time()
    for tf, src in (("15m", "m15.bars"), ("1h", "h1.bars")):
        con.execute(f"CREATE OR REPLACE TEMP TABLE codes_{tf} AS {lcode_sql(src)}")
        B.assert_unique(con, f"SELECT * FROM codes_{tf}", "ticker, date")
        con.execute(f"CREATE OR REPLACE TEMP TABLE sc_{tf} AS {session_counts_sql(f'codes_{tf}')}")
        p = os.path.join(run.dir, f"session_counts_{tf}.parquet")
        con.execute(f"COPY (SELECT * FROM sc_{tf} ORDER BY ticker, session) TO '{p}' (FORMAT PARQUET)")
        n, nt, s0, s1 = con.execute(f"SELECT count(*), count(DISTINCT ticker), min(session), max(session) FROM sc_{tf}").fetchone()
        rep[f"source_{tf}"] = dict(store=os.path.realpath(os.path.join(STORE_DATA, f"studio_{tf}.duckdb")), sessions_rows=n, tickers=nt,
                                   range=[str(s0), str(s1)], parquet=os.path.basename(p), sha256_16=_dig(p))
        log(f"  {tf}: {n:,} ticker-sessions ({time.time() - t0:.0f}s)")
    # daily frame from the canonical authority (never the Studio 1D store)
    d = con.execute(f"""SELECT ticker, session_date AS session, open, high, low, close, volume, avg_vol_20d, atr_14
                        FROM read_parquet('{cur["derived_parquet"]}') ORDER BY ticker, session_date""").df()
    d["session"] = d["session"].astype(str)
    d["close_prev"] = d.groupby("ticker")["close"].shift(1)
    d["ret_same_day"] = (d["close"] / d["close_prev"] - 1.0) * 100.0
    d["rsi14"] = d.groupby("ticker")["close"].transform(rsi_wilder)
    d["eligible"] = (d["close"] >= 5) & (d["avg_vol_20d"] > 0) & (d["close"] * d["volume"] >= 3_000_000)
    d["colour"] = np.where(d["close"] < d["open"], "RED", np.where(d["close"] > d["open"], "GREEN", "DOJI"))
    sc = pd.read_parquet(os.path.join(run.dir, "session_counts_15m.parquet")); sc["session"] = sc["session"].astype(str)
    sh = pd.read_parquet(os.path.join(run.dir, "session_counts_1h.parquet")); sh["session"] = sh["session"].astype(str)
    sh = sh.rename(columns={c: c + "_1h" for c in sh.columns if c not in ("ticker", "session")})
    X = d.merge(sc, on=["ticker", "session"], how="left").merge(sh, on=["ticker", "session"], how="left")
    rep["joins"] = dict(d1_x_15m=B.typed_join_report(con, "SELECT ticker, session_date AS session FROM read_parquet('" + cur["derived_parquet"] + "')",
                                                     "SELECT ticker, session FROM sc_15m", "session"))
    for col, (a, b3, c46) in {"bal": ("n_l34", "n_l3", "n_l46"), "bal_v": ("nv_l34", "nv_l3", "nv_l46"), "bal_1h": ("n_l34_1h", "n_l3_1h", "n_l46_1h")}.items():
        den = X[a].fillna(0) + X[b3].fillna(0) + X[c46].fillna(0)
        X[col] = np.where(den >= MIN_EFFORT_BARS, (X[a].fillna(0) + X[b3].fillna(0) - X[c46].fillna(0)) / den.replace(0, np.nan), np.nan)
        X[col + "_bin"] = [bin_of(v) if not np.isnan(v) else None for v in X[col].to_numpy(float)]
    X = X[X["eligible"]].reset_index(drop=True)
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    if (X["in_mine"] & X["in_verify"]).any():
        raise HardStop("OOS windows overlap")
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    # X-only census + the free pre-check (same-day return is the day's OWN return, not an outcome)
    cen = dict(rows=int(len(X)), tickers=int(X["ticker"].nunique()), sessions=int(X["session"].nunique()),
               available=dict(bal=int(X["bal"].notna().sum()), bal_v=int(X["bal_v"].notna().sum()), bal_1h=int(X["bal_1h"].notna().sum())),
               colour=X["colour"].value_counts().to_dict(),
               bal_quantiles=[round(float(q), 3) for q in np.nanquantile(X["bal"].to_numpy(float), [0.05, 0.25, 0.5, 0.75, 0.95])],
               effort_bars_per_day_median=float(np.nanmedian((X["n_l34"] + X["n_l3"] + X["n_l46"]).to_numpy(float))),
               bars_per_day_median=float(np.nanmedian(X["n_bars"].to_numpy(float))))
    ok = X["bal"].notna() & X["ret_same_day"].notna()
    r = float(np.corrcoef(X.loc[ok, "bal"], X.loc[ok, "ret_same_day"])[0, 1])
    # partial information: residual of bal after the daily return AND the colour (what the candle already says)
    Z = X.loc[ok, ["ret_same_day"]].copy(); Z["green"] = (X.loc[ok, "colour"] == "GREEN").astype(float); Z["const"] = 1.0
    beta, *_ = np.linalg.lstsq(Z.to_numpy(float), X.loc[ok, "bal"].to_numpy(float), rcond=None)
    resid = X.loc[ok, "bal"].to_numpy(float) - Z.to_numpy(float) @ beta
    cen["precheck"] = dict(corr_bal_vs_same_day_return=round(r, 4), r2_on_return_and_colour=round(1 - resid.var() / X.loc[ok, "bal"].var(), 4),
                           residual_std=round(float(resid.std()), 4), total_std=round(float(X.loc[ok, "bal"].std()), 4),
                           corr_bal_vs_bal_1h=round(float(X.loc[X["bal"].notna() & X["bal_1h"].notna(), ["bal", "bal_1h"]].corr().iloc[0, 1]), 4),
                           note="same-day return is X-side (the bar's own return); no forward outcome touched")
    cs = {}
    for c in cells():
        m = cell_mask(X, c)
        cs[c["claim_id"]] = dict(TRUE=int(m.sum()), TRUE_mine=int((m & X["in_mine"]).sum()), TRUE_verify=int((m & X["in_verify"]).sum()),
                                 class_n=int(class_mask(X, c["cls"]).sum()))
    cen["cells"] = cs
    # 15m label partition: every coded 15m bar is exactly one label
    tot = con.execute("SELECT count(*) FROM codes_15m").fetchone()[0]
    lab = con.execute("SELECT count(*) FROM codes_15m WHERE code IN (3,18,12,40,2,4,8,16)").fetchone()[0]
    other = con.execute("SELECT code, count(*) FROM codes_15m WHERE code NOT IN (3,18,12,40,2,4,8,16) GROUP BY 1 ORDER BY 2 DESC LIMIT 8").fetchall()
    cen["label_partition_15m"] = dict(coded_bars=tot, exact_label_bars=lab, other_codes=other,
                                      note="code 0 = no L (vol neither up nor down adapted, or flat volume); L1-alone (1) and L6-alone (32) cannot occur")
    rep.update(census=cen, x_sha256_16=_dig(xp), elapsed_s=round(time.time() - t0))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    con.close(); log(f"X {len(X):,} rows; corr(bal, same-day ret) {r:+.3f}; R² on return+colour {cen['precheck']['r2_on_return_and_colour']}")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "IEB_*")), reverse=True):
        if RunSpace.is_current(r, "X.parquet"):
            return r
    return None


# ── registry + seal ────────────────────────────────────────────────────────────────────────
def registry(rep: dict) -> dict:
    cs = cells()
    if len(cs) != K_EXPECTED or len({c["claim_id"] for c in cs}) != K_EXPECTED:
        raise HardStop("k != 16")
    return dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="DRAFT_PRE_SEAL",
                provenance=["USER_APPROVED_5_LINE_PLAN_2026-09-06", "PRE_OUTCOME", "PRE_PATHSIM", "SEPARATE_FAMILY_FROM_OVD_AND_MTF"],
                feature=dict(EFFORT_BALANCE="(n_L34 + n_L3 - n_L46) / (n_L34 + n_L3 + n_L46) over the 15m bars of session D; exact-label counting; UNAVAILABLE if denominator < 3",
                             EFFORT_BALANCE_V="same, counting only 15m bars with volume > SMA20(15m volume) x 1.0",
                             wlnbb=WLNBB, bins="B- <= -0.33 · B0 (-0.33, +0.33) · B+ >= +0.33", colour="RED close<open · GREEN close>open (DOJI outside)",
                             rsi="Wilder RSI14 on canonical closes; conditional cells RSI14 < 40 / >= 60",
                             known_at="16:00 NY of session D", entry="D+1 open", descriptive_only=["n_L12", "n_L25", "n_L5", "n_L2", "n_L4", "1H replication (bal_1h)"]),
                samples=dict(A="eligible rows with EFFORT_BALANCE evaluable and colour in {RED, GREEN}", B="same with EFFORT_BALANCE_V"),
                same_day_control="median of OTHER direct trades of the same class (union of the class's 6 grid cells' direct trades) on the same entry date, "
                                 "ticker-exclusive, >= 20 others else UNAVAILABLE; edge = ret - control; day median; cell = median over days",
                estimand="direct sacred edge_replay._pathsim per registered cell mask, 5-bar same-ticker cooldown INCLUDED (never partitioned); "
                         "mode trail, atr_k 12, maxh 60, slip 0.0015; PRICE-RETURN paths (DIVIDENDS_NOT_ADJUSTED)",
                oos=OOS, oos_rule="MINE decides survivors with the mine gates; VERIFY is evaluated for mine survivors only (all cells reported descriptively); "
                                  "no cell is selected on VERIFY", gates=GATES,
                multiplicity=dict(k=K_EXPECTED, rule="DSR (overfit_stats.dsr) with the cell's MINE day-edge series; trial family = the 16 cells' MINE day-edge Sharpes; n_trials = 16"),
                replication=dict(tf="1H", cells="the 8 class-A cells on bal_1h", status="DESCRIPTIVE echo test, not in k"),
                cells=cs, x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
                sources=dict(m15=rep["source_15m"], h1=rep["source_1h"]),
                stop_rule="if no cell passes the mine gates AND replicates, the family is NULL and closes; no threshold / L-combination search follows")


def seal() -> dict:
    if os.path.exists(os.path.join(FAMILY_DIR, "SEAL.json")):
        raise HardStop("SEAL.json exists")
    if any(k in f.lower() for _, _, fs in os.walk(FAMILY_DIR) for f in fs for k in ("outcome", "result", "pathsim")):
        raise HardStop("outcome-bearing file present")
    run = latest_run()
    if not run:
        raise HardStop("no COMPLETED X build")
    rep = json.load(open(os.path.join(run, "build_report.json")))
    if _dig(os.path.join(run, "X.parquet")) != rep["x_sha256_16"]:
        raise HardStop("X digest mismatch")
    ps, slip = O.sacred_pathsim()
    add_p = os.path.join(run, "PRECHECK_ADDENDUM.json")
    if os.path.exists(add_p):
        rep["census"]["precheck"]["robust"] = json.load(open(add_p)); rep["census"]["precheck"]["addendum_sha256_16"] = _dig(add_p)
    reg = registry(rep)
    reg["status"] = "SEALED"; reg["sealed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED, x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"],
             build_report_sha256_16=_dig(os.path.join(run, "build_report.json")), canonical_1d=rep["canonical_1d"], sources=reg["sources"],
             oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip, code=dict(ieb_family=_dig(__file__), ovd_outcome_v1=_dig(O.__file__),
             ovd_outcome_direct_v1=_dig(OD.__file__), edge_replay=_dig(os.path.join(HERE, "edge_replay.py"))),
             precheck=rep["census"]["precheck"], outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


# ── outcome (direct sacred _pathsim) ──────────────────────────────────────────────────────
def _stats_window(obs: pd.DataFrame, lo: str, hi: str) -> dict:
    w = obs[(obs["date_in"] >= lo) & (obs["date_in"] <= hi)]
    e = w.dropna(subset=["edge"])
    ds = O.day_stats(e); ts = O.trade_stats(w) if len(w) else dict(n_obs=0)
    yrs = {y: v for y, v in ds["per_year"].items() if ds["days_per_year"].get(y, 0) >= GATES["mine"]["min_days_per_year"]}
    return dict(n_obs=ts.get("n_obs", 0), n_days=ds["n_days"], median_edge=ds["median_edge"], day_win=ds["day_win"], top2_share=ds["top2_share"],
                per_year=ds["per_year"], days_per_year=ds["days_per_year"], positive_years=sum(1 for v in yrs.values() if v > 0), years_counted=len(yrs),
                worst_year=(min(yrs.values()) if yrs else None), best_year=(max(yrs.values()) if yrs else None),
                raw_median=ts.get("raw_median"), win_rate=ts.get("win_rate"), profit_factor=ts.get("profit_factor"), avg_hold=ts.get("avg_hold"),
                day_series=ds.get("day_series", pd.Series(dtype=float)))


def classify(mine: dict, ver: dict, dsr_pos, dsr_neg) -> str:
    g = GATES
    if mine["n_days"] < g["thin"]["n_days_lt"] or mine["n_obs"] < g["thin"]["n_obs_lt"]:
        return "DESCRIPTIVE_ONLY"
    m, v = g["mine"], g["verify"]
    me, dw, py, wy, by = mine["median_edge"] or 0.0, mine["day_win"] or 0.0, mine["positive_years"], mine["worst_year"], mine["best_year"]
    neg_years = mine["years_counted"] - py
    build_mine = me > m["median_gt"] and dw > m["day_win_gt"] and py >= m["pos_years_ge"] and wy is not None and wy >= m["worst_year_ge"] and (dsr_pos or 0) >= m["dsr_ge"]
    veto_mine = me < 0 and dw < 50 and neg_years >= m["pos_years_ge"] and by is not None and by <= 2.0 and (dsr_neg or 0) >= m["dsr_ge"]
    if build_mine:
        ok = (ver["median_edge"] or 0) > v["median_gt"] and (ver["day_win"] or 0) > v["day_win_gt"] and ver["worst_year"] is not None and ver["worst_year"] >= v["worst_year_ge"]
        return "BUILD_CANDIDATE" if ok else "MINE_ONLY_NOT_REPLICATED"
    if veto_mine:
        ok = (ver["median_edge"] or 0) < 0 and (ver["day_win"] or 0) < 50 and ver["best_year"] is not None and ver["best_year"] <= 2.0
        return "VETO_CANDIDATE" if ok else "MINE_ONLY_NOT_REPLICATED"
    return "NULL"


def _loo_by_ticker(pool: pd.DataFrame, obs: pd.DataFrame):
    """control_i = median of pool trades on obs.date_in with ticker != obs.ticker; n_others."""
    ctrl = np.full(len(obs), np.nan); noth = np.zeros(len(obs), int)
    groups = {d: g for d, g in pool.groupby("date_in")}
    for i, (d, tk) in enumerate(zip(obs["date_in"].to_numpy(), obs["ticker"].to_numpy())):
        g = groups.get(d)
        if g is None:
            continue
        v = g.loc[g["ticker"] != tk, "ret"].to_numpy(float)
        noth[i] = len(v)
        if len(v):
            ctrl[i] = np.median(v)
    return ctrl, noth


def outcome(log=print) -> dict:
    sp = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(sp):
        raise HardStop("not sealed")
    s = json.load(open(sp)); reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")))
    if _dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")) != s["registry_sha256_16"] or len(reg["cells"]) != s["k"]:
        raise HardStop("registry digest / k mismatch")
    run_x = os.path.join(FAMILY_DIR, "runs", s["x_run"])
    if _dig(os.path.join(run_x, "X.parquet")) != s["x_sha256_16"] or not RunSpace.is_current(run_x, "X.parquet"):
        raise HardStop("X mismatch")
    cur = B.canonical_current()
    if cur["derived_sha256_16"] != s["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical changed")
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "ieb")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), seal_registry=s["registry_sha256_16"]))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")
    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    frames = OD.load_frames(cur["derived_parquet"], X["ticker"].unique())
    cs = reg["cells"]; trades = {}; cons = {}
    for c in cs + [dict(claim_id=f"REPL1H|{c['claim_id']}", cls="R", column="bal_1h", colour=c["colour"], bin=c["bin"], rsi=c["rsi"], kind="replication")
                   for c in cs if c["cls"] == "A"]:
        m = cell_mask(X, c)
        tr, cn = OD.direct_trades(ps, frames, X.loc[m, ["ticker", "session"]])
        tr["cell_id"] = c["claim_id"]; trades[c["claim_id"]] = tr; cons[c["claim_id"]] = cn
    log(f"  direct _pathsim: {len(trades)} masks")
    run.write_atomic("selection_conservation.json", cons)
    pd.concat([t for t in trades.values() if len(t)], ignore_index=True).to_parquet(os.path.join(run.dir, "direct_trades.parquet"), index=False)
    pools = {}
    for cls in ("A", "B", "R"):
        parts = [trades[k] for k in trades if k.startswith("REPL1H|" if cls == "R" else f"{cls}|") and "|RSI" not in k.split("|", 1)[-1].replace("REPL1H|", "")]
        pools[cls] = pd.concat([p for p in parts if len(p)], ignore_index=True) if parts else pd.DataFrame()
    results, mine_series = [], {}
    for cid, tr in trades.items():
        cls = "R" if cid.startswith("REPL1H|") else cid.split("|")[0]
        obs = tr[tr["ret"].notna()].copy()
        if len(obs):
            ctrl, noth = _loo_by_ticker(pools[cls], obs)
            obs["control"] = ctrl; obs["n_others"] = noth
            obs["edge"] = np.where(obs["n_others"] >= O.MIN_CONTROL, obs["ret"] - obs["control"], np.nan)
        mine = _stats_window(obs, *OOS["MINE"]); ver = _stats_window(obs, *OOS["VERIFY"])
        results.append(dict(claim_id=cid, cls=cls, in_k=not cid.startswith("REPL1H|"), **{k: v for k, v in cons[cid].items()},
                            mine={k: v for k, v in mine.items() if k != "day_series"}, verify={k: v for k, v in ver.items() if k != "day_series"}))
        mine_series[cid] = mine["day_series"]
    from overfit_stats import dsr, sharpe
    kids = [r["claim_id"] for r in results if r["in_k"]]
    trial = [sharpe(mine_series[k].to_numpy()) if len(mine_series[k]) >= 2 else 0.0 for k in kids]
    for r in results:
        srs = mine_series[r["claim_id"]].to_numpy(float)
        if len(srs) >= 3:
            dp = dsr(srs, trial, n_trials=K_EXPECTED); dn = dsr(-srs, [-x for x in trial], n_trials=K_EXPECTED)
            r.update(dsr=dp["dsr"], sr=dp["sr"], sr_star=dp["sr_star"], dsr_neg=dn["dsr"])
        else:
            r.update(dsr=None, sr=None, sr_star=None, dsr_neg=None)
        r["classification"] = classify(r["mine"], r["verify"], r["dsr"], r["dsr_neg"]) if r["in_k"] else "REPLICATION_DESCRIPTIVE"
    if sorted(kids) != sorted(c["claim_id"] for c in cs):
        raise HardStop("result table != registry")
    run.write_atomic("results.json", results)
    cls_census = {}
    for r in results:
        if r["in_k"]:
            cls_census[r["classification"]] = cls_census.get(r["classification"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED, oos=OOS, gates=GATES,
                pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, outcome_access_count=led["outcome_access_count"], classification_census=cls_census,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id, seal_registry_sha256_16=s["registry_sha256_16"],
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")), outcome_access_count=led["outcome_access_count"],
                   pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, classification_census=cls_census, status="COMPLETE — family SEARCH-EXPOSED",
                   written_at=summ["completed_at"]), open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"COMPLETED {run.run_id}: {cls_census}")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    if cmd == "build":
        print(json.dumps(build(), indent=1, default=str))
    elif cmd == "seal":
        print(json.dumps(seal(), indent=1, default=str))
    elif cmd == "outcome":
        print(json.dumps(outcome(), indent=1, default=str))
