"""INTRADAY_BREADTH_TRANSITION_V1 — a separate research family (user-approved 5-line plan, 2026-09-07).

Question: the user observed that moves start when MORE of the day's 15m / 1H bars trade above average
volume — not only the first and last bars. Does same-slot volume BREADTH (the share of a session's 26
15m bars that are heavier than the same time slot's median over the previous 20 sessions) carry
information about the next move, and does it depend on where it happens and how big it is?

  BREADTH(D) = 100 × #{15m bars of session D with volume > median of the same slot over the previous 20
               sessions} / 26.  Defined only for full 26-bar sessions whose 26 slots all have 20-session
               history. (Same-slot benchmark: the lower-TF SMA measured the time of day, not the day —
               74 % "above" at 09:30, 6 % at midday, 100 % at 15:45.)
  Events (registered, both from the user's observation):
      T  transition — BREADTH(D−1) ≤ 50 and BREADTH(D) ≥ 75   (the FIRST broad day after a quiet one)
      F  full       — BREADTH(D) = 100                         (every slot heavier than its norm)
  Context (where):  NEWLOW  close(D) < min(close, previous 20 sessions) · NEWHIGH close(D) > max(...) · MID otherwise
  Magnitude (how big): RVOL = volume(D) / median(volume, previous 20 sessions):  RV<2 · RV≥2
  k = 2 events × 3 contexts × 2 magnitudes = 12 cells. No interactions beyond these, no threshold sweep.
  Known at 16:00 NY of session D → entry D+1 open. Eligibility as the other families: close ≥ 5,
  avg_vol_20d > 0, close × volume ≥ 3M.
  Estimand: direct sacred _pathsim per registered cell mask (5-bar cooldown INCLUDED, never partitioned),
  book config (trail atr_k 12, maxh 60, slip 0.0015). Control: median of OTHER direct trades of the same
  EVENT class (union of that event's 6 cells, deduplicated per trade) on the same entry date, ticker-
  exclusive, ≥ 20 others. Day-clustered median. DSR at k = 12 on MINE.
  OOS reserved BEFORE any outcome: MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  Gates (registered): MINE median>0 · day-win>50 % · ≥3 positive years (years with ≥5 days) · worst ≥ −2 pp ·
  DSR ≥ 0.95 · not THIN (n_days ≥ 30, n_obs ≥ 100); VERIFY (mine survivors only) median>0 · day-win>50 % · worst ≥ −2.
  BUILD_CANDIDATE = both; VETO mirror; mine-pass / verify-fail = MINE_ONLY_NOT_REPLICATED; else NULL.
  Stop rule: nothing passes AND replicates → family NULL, closes; no threshold / window search follows.
  Limitations: canonical 1D = split-adjusted as of fetch, DIVIDENDS_NOT_ADJUSTED → price-return paths;
  15m volume = Massive aggregates (the 16:00 closing print is in no 15m bar).
"""
from __future__ import annotations
import os, sys, json, time, hashlib, glob                                # noqa: E402
import numpy as np                                                       # noqa: E402
import pandas as pd                                                      # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import HardStop, RunSpace, DATA as STORE_DATA             # noqa: E402  (V1 guards reused, never edited)
import ovd_build as B                                                    # noqa: E402
import ovd_outcome_v1 as O                                               # noqa: E402  (day/trade stats, sacred engine, guards)
import ovd_outcome_direct_v1 as OD                                       # noqa: E402  (direct_trades, load_frames)

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/INTRADAY_BREADTH_TRANSITION_V1"
FAMILY = "INTRADAY_BREADTH_TRANSITION_V1"
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
SLOT_HIST = 20                 # sessions of same-slot history behind the median
BARS_PER_SESSION = 26
T_FROM, T_TO = 50.0, 75.0      # transition: prev ≤ 50 → today ≥ 75
RVOL_SPLIT = 2.0
K_EXPECTED = 12
GATES = dict(mine=dict(median_gt=0.0, day_win_gt=50.0, pos_years_ge=3, worst_year_ge=-2.0, dsr_ge=0.95, min_days_per_year=5),
             verify=dict(median_gt=0.0, day_win_gt=50.0, worst_year_ge=-2.0),
             thin=dict(n_days_lt=30, n_obs_lt=100), k=K_EXPECTED,
             source="book analysis standard (yrs >= 2/3 of window, worst >= -2 pp, DSR at family k); registered before any outcome")


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


# ── same-slot breadth on the 15m store ────────────────────────────────────────────────────
def breadth_sql(src: str) -> str:
    return f"""
    WITH b AS (
      SELECT ticker, date, COALESCE(volume, 0)::DOUBLE AS v, close > open AS grn,
             strftime(ny, '%H:%M') AS hm, CAST(ny AS DATE) AS session
      FROM (SELECT *, timezone('America/New_York', timezone('UTC', date)) AS ny FROM {src})),
    w AS (
      SELECT *, median(v) OVER (PARTITION BY ticker, hm ORDER BY session ROWS BETWEEN {SLOT_HIST} PRECEDING AND 1 PRECEDING) AS slot_med,
                count(*)  OVER (PARTITION BY ticker, hm ORDER BY session ROWS BETWEEN {SLOT_HIST} PRECEDING AND 1 PRECEDING) AS slot_n
      FROM b)
    SELECT ticker, session, count(*) AS n_bars, count(*) FILTER (WHERE slot_n >= {SLOT_HIST}) AS n_def,
           count(*) FILTER (WHERE slot_n >= {SLOT_HIST} AND v > slot_med) AS n_above,
           count(*) FILTER (WHERE slot_n >= {SLOT_HIST} AND v > slot_med AND grn) AS heavy_g,
           count(*) FILTER (WHERE slot_n >= {SLOT_HIST} AND v > slot_med AND NOT grn) AS heavy_r
    FROM w GROUP BY 1, 2"""


def context_of(close, lo20, hi20):
    if not (np.isfinite(lo20) and np.isfinite(hi20)):
        return None
    return "NEWLOW" if close < lo20 else ("NEWHIGH" if close > hi20 else "MID")


def cells() -> list[dict]:
    out = []
    for ev, evn in (("T", "transition: breadth(D-1) <= 50 and breadth(D) >= 75"), ("F", "full: breadth(D) = 100")):
        for ctx in ("NEWLOW", "NEWHIGH", "MID"):
            for rv in ("RV<2", "RV>=2"):
                out.append(dict(claim_id=f"{ev}|{ctx}|{rv}", cls=ev, event=evn, context=ctx, rvol=rv, kind="grid"))
    return out


def cell_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    ev = X["ev_T"].to_numpy(bool) if c["cls"] == "T" else X["ev_F"].to_numpy(bool)
    ctx = X["context"].to_numpy(object) == c["context"]
    rv = X["rvol"].to_numpy(float)
    rvm = (rv < RVOL_SPLIT) if c["rvol"] == "RV<2" else (rv >= RVOL_SPLIT)
    return ev & ctx & np.isfinite(rv) & rvm & X["eligible"].to_numpy(bool)


def class_mask(X: pd.DataFrame, cls: str) -> np.ndarray:
    ev = X["ev_T"].to_numpy(bool) if cls == "T" else X["ev_F"].to_numpy(bool)
    return ev & X["eligible"].to_numpy(bool) & X["context"].notna().to_numpy() & np.isfinite(X["rvol"].to_numpy(float))


# ── X-only build ───────────────────────────────────────────────────────────────────────────
def build(log=print) -> dict:
    import duckdb
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("IBT_%Y%m%dT%H%M%SZ", time.gmtime()))
    con = duckdb.connect(); con.execute("pragma threads=8")
    con.execute(f"ATTACH '{os.path.join(STORE_DATA, 'studio_15m.duckdb')}' AS m15 (READ_ONLY)")
    rep = dict(run_id=run.run_id, family=FAMILY, canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]), oos=OOS,
               feature=dict(slot_hist=SLOT_HIST, bars_per_session=BARS_PER_SESSION, transition=[T_FROM, T_TO], rvol_split=RVOL_SPLIT))
    t0 = time.time()
    con.execute(f"CREATE TEMP TABLE br AS {breadth_sql('m15.bars')}")
    B.assert_unique(con, "SELECT * FROM br", "ticker, session")
    p = os.path.join(run.dir, "breadth_15m.parquet")
    con.execute(f"COPY (SELECT * FROM br ORDER BY ticker, session) TO '{p}' (FORMAT PARQUET)")
    n, nt, s0, s1 = con.execute("SELECT count(*), count(DISTINCT ticker), min(session), max(session) FROM br").fetchone()
    rep["source_15m"] = dict(store=os.path.realpath(os.path.join(STORE_DATA, "studio_15m.duckdb")), sessions_rows=n, tickers=nt, range=[str(s0), str(s1)],
                             parquet=os.path.basename(p), sha256_16=_dig(p))
    log(f"  15m breadth: {n:,} ticker-sessions ({time.time() - t0:.0f}s)")
    # daily frame from the canonical authority (never the Studio 1D store)
    d = con.execute(f"""SELECT ticker, session_date AS session, open, high, low, close, volume, avg_vol_20d, atr_14
                        FROM read_parquet('{cur["derived_parquet"]}') ORDER BY ticker, session_date""").df()
    d["session"] = d["session"].astype(str)
    g = d.groupby("ticker", sort=False)
    d["close_prev"] = g["close"].shift(1)
    d["ret_same_day"] = (d["close"] / d["close_prev"] - 1.0) * 100.0
    d["vol_med20_prev"] = g["volume"].transform(lambda s: s.shift(1).rolling(SLOT_HIST).median())
    d["hi20_prev"] = g["close"].transform(lambda s: s.shift(1).rolling(SLOT_HIST).max())
    d["lo20_prev"] = g["close"].transform(lambda s: s.shift(1).rolling(SLOT_HIST).min())
    d["rvol"] = np.where(d["vol_med20_prev"] > 0, d["volume"] / d["vol_med20_prev"], np.nan)
    d["context"] = [context_of(c, lo, hi) for c, lo, hi in zip(d["close"], d["lo20_prev"], d["hi20_prev"])]
    d["eligible"] = (d["close"] >= 5) & (d["avg_vol_20d"] > 0) & (d["close"] * d["volume"] >= 3_000_000)
    br = pd.read_parquet(p); br["session"] = br["session"].astype(str)
    full = (br["n_bars"] == BARS_PER_SESSION) & (br["n_def"] == BARS_PER_SESSION)
    br["breadth"] = np.where(full, 100.0 * br["n_above"] / BARS_PER_SESSION, np.nan)
    X = d.merge(br[["ticker", "session", "n_bars", "n_def", "breadth", "heavy_g", "heavy_r"]], on=["ticker", "session"], how="left")
    rep["joins"] = dict(d1_x_15m=B.typed_join_report(con, "SELECT ticker, session_date AS session FROM read_parquet('" + cur["derived_parquet"] + "')",
                                                     "SELECT ticker, session FROM br", "session"))
    X["breadth_prev"] = X.groupby("ticker", sort=False)["breadth"].shift(1)      # previous CANONICAL session
    X["ev_T"] = X["breadth_prev"].notna() & X["breadth"].notna() & (X["breadth_prev"] <= T_FROM) & (X["breadth"] >= T_TO)
    X["ev_F"] = X["breadth"].notna() & (X["breadth"] >= 100.0 - 1e-9)
    X = X[X["eligible"]].reset_index(drop=True)
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    if (X["in_mine"] & X["in_verify"]).any():
        raise HardStop("OOS windows overlap")
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    # X-only census + free pre-checks (same-day return is the day's OWN return; no forward outcome touched)
    okb = X["breadth"].notna()
    cen = dict(rows=int(len(X)), tickers=int(X["ticker"].nunique()), sessions=int(X["session"].nunique()),
               breadth_available=int(okb.sum()), breadth_quantiles=[round(float(q), 1) for q in np.nanquantile(X["breadth"].to_numpy(float), [0.1, 0.25, 0.5, 0.75, 0.9])],
               event_days=dict(T=int(X["ev_T"].sum()), F=int(X["ev_F"].sum()), T_and_F=int((X["ev_T"] & X["ev_F"]).sum())),
               context=X.loc[okb, "context"].value_counts(dropna=False).to_dict(),
               rvol_quantiles=[round(float(q), 2) for q in np.nanquantile(X["rvol"].to_numpy(float), [0.1, 0.5, 0.9])])
    ok = okb & X["ret_same_day"].notna() & X["rvol"].notna()
    from scipy.stats import spearmanr
    cen["precheck"] = dict(spearman_breadth_vs_abs_same_day_return=round(float(spearmanr(X.loc[ok, "breadth"], X.loc[ok, "ret_same_day"].abs())[0]), 4),
                           spearman_breadth_vs_rvol=round(float(spearmanr(X.loc[ok, "breadth"], X.loc[ok, "rvol"])[0]), 4),
                           spearman_breadth_vs_signed_same_day_return=round(float(spearmanr(X.loc[ok, "breadth"], X.loc[ok, "ret_same_day"])[0]), 4),
                           note="X-side only: breadth carries magnitude (|return|, RVOL), not sign — the reason the cells split by context and RVOL")
    cs = {}
    for c in cells():
        m = cell_mask(X, c)
        cs[c["claim_id"]] = dict(TRUE=int(m.sum()), TRUE_mine=int((m & X["in_mine"]).sum()), TRUE_verify=int((m & X["in_verify"]).sum()),
                                 class_n=int(class_mask(X, c["cls"]).sum()))
    cen["cells"] = cs
    rep.update(census=cen, x_sha256_16=_dig(xp), elapsed_s=round(time.time() - t0))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    con.close(); log(f"X {len(X):,} rows; T days {cen['event_days']['T']:,} · F days {cen['event_days']['F']:,}; "
                     f"spearman(breadth, |ret|) {cen['precheck']['spearman_breadth_vs_abs_same_day_return']:+.3f}, signed {cen['precheck']['spearman_breadth_vs_signed_same_day_return']:+.3f}")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "IBT_*")), reverse=True):
        if RunSpace.is_current(r, "X.parquet"):
            return r
    return None


# ── registry + seal ────────────────────────────────────────────────────────────────────────
def registry(rep: dict) -> dict:
    cs = cells()
    if len(cs) != K_EXPECTED or len({c["claim_id"] for c in cs}) != K_EXPECTED:
        raise HardStop("k != 12")
    return dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="DRAFT_PRE_SEAL",
                provenance=["USER_APPROVED_5_LINE_PLAN_2026-09-07", "PRE_OUTCOME", "PRE_PATHSIM", "SEPARATE_FAMILY_FROM_OVD_IEB_MTF"],
                feature=dict(BREADTH="100 x #{15m bars of session D with volume > median of the same slot over the previous 20 sessions} / 26; "
                                     "defined only for full 26-bar sessions whose 26 slots all have 20-session history",
                             events=dict(T="breadth(D-1) <= 50 and breadth(D) >= 75 (previous CANONICAL session)", F="breadth(D) = 100"),
                             context="NEWLOW close < min(close, prev 20) · NEWHIGH close > max(close, prev 20) · MID",
                             rvol="volume(D) / median(volume, prev 20 sessions); split at 2.0",
                             known_at="16:00 NY of session D", entry="D+1 open", descriptive_only=["heavy_g", "heavy_r", "breadth_prev"]),
                samples=dict(T="eligible rows with event T, context and RVOL defined", F="eligible rows with event F, context and RVOL defined"),
                same_day_control="median of OTHER direct trades of the same EVENT class (union of that event's 6 cells' direct trades, deduplicated per "
                                 "(ticker, entry date)) on the same entry date, ticker-exclusive, >= 20 others else UNAVAILABLE; edge = ret - control; "
                                 "day median; cell = median over days",
                estimand="direct sacred edge_replay._pathsim per registered cell mask, 5-bar same-ticker cooldown INCLUDED (never partitioned); "
                         "mode trail, atr_k 12, maxh 60, slip 0.0015; PRICE-RETURN paths (DIVIDENDS_NOT_ADJUSTED)",
                oos=OOS, oos_rule="MINE decides survivors with the mine gates; VERIFY is evaluated for mine survivors only (all cells reported descriptively); "
                                  "no cell is selected on VERIFY", gates=GATES,
                multiplicity=dict(k=K_EXPECTED, rule="DSR (overfit_stats.dsr) with the cell's MINE day-edge series; trial family = the 12 cells' MINE day-edge Sharpes; n_trials = 12"),
                cells=cs, x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
                sources=dict(m15=rep["source_15m"]),
                stop_rule="if no cell passes the mine gates AND replicates, the family is NULL and closes; no threshold / window / TF search follows")


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
    reg = registry(rep)
    reg["status"] = "SEALED"; reg["sealed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED, x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"],
             build_report_sha256_16=_dig(os.path.join(run, "build_report.json")), canonical_1d=rep["canonical_1d"], sources=reg["sources"],
             oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(breadth_family=_dig(__file__), ovd_outcome_v1=_dig(O.__file__), ovd_outcome_direct_v1=_dig(OD.__file__),
                       edge_replay=_dig(os.path.join(HERE, "edge_replay.py"))),
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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "ibt")
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
    for c in cs:
        m = cell_mask(X, c)
        tr, cn = OD.direct_trades(ps, frames, X.loc[m, ["ticker", "session"]])
        tr["cell_id"] = c["claim_id"]; trades[c["claim_id"]] = tr; cons[c["claim_id"]] = cn
        log(f"  {c['claim_id']}: signal {cn.get('signal_true_n', '?')} → trades {len(tr)}")
    run.write_atomic("selection_conservation.json", cons)
    pd.concat([t for t in trades.values() if len(t)], ignore_index=True).to_parquet(os.path.join(run.dir, "direct_trades.parquet"), index=False)
    # event-class pools: union of the class's 6 cells, one row per (ticker, entry date)
    pools = {}
    for cls in ("T", "F"):
        parts = [trades[k] for k in trades if k.startswith(f"{cls}|") and len(trades[k])]
        pools[cls] = (pd.concat(parts, ignore_index=True).drop_duplicates(["ticker", "date_in"]) if parts else pd.DataFrame(columns=["ticker", "date_in", "ret"]))
    results, mine_series = [], {}
    for cid, tr in trades.items():
        cls = cid.split("|")[0]
        obs = tr[tr["ret"].notna()].copy()
        if len(obs):
            ctrl, noth = _loo_by_ticker(pools[cls], obs)
            obs["control"] = ctrl; obs["n_others"] = noth
            obs["edge"] = np.where(obs["n_others"] >= O.MIN_CONTROL, obs["ret"] - obs["control"], np.nan)
        mine = _stats_window(obs, *OOS["MINE"]); ver = _stats_window(obs, *OOS["VERIFY"])
        results.append(dict(claim_id=cid, cls=cls, in_k=True, **{k: v for k, v in cons[cid].items()},
                            mine={k: v for k, v in mine.items() if k != "day_series"}, verify={k: v for k, v in ver.items() if k != "day_series"}))
        mine_series[cid] = mine["day_series"]
    from overfit_stats import dsr, sharpe
    kids = [r["claim_id"] for r in results]
    trial = [sharpe(mine_series[k].to_numpy()) if len(mine_series[k]) >= 2 else 0.0 for k in kids]
    for r in results:
        srs = mine_series[r["claim_id"]].to_numpy(float)
        if len(srs) >= 3:
            dp = dsr(srs, trial, n_trials=K_EXPECTED); dn = dsr(-srs, [-x for x in trial], n_trials=K_EXPECTED)
            r.update(dsr=dp["dsr"], sr=dp["sr"], sr_star=dp["sr_star"], dsr_neg=dn["dsr"])
        else:
            r.update(dsr=None, sr=None, sr_star=None, dsr_neg=None)
        r["classification"] = classify(r["mine"], r["verify"], r["dsr"], r["dsr_neg"])
    if sorted(kids) != sorted(c["claim_id"] for c in cs):
        raise HardStop("result table != registry")
    run.write_atomic("results.json", results)
    cls_census = {}
    for r in results:
        cls_census[r["classification"]] = cls_census.get(r["classification"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED, oos=OOS, gates=GATES,
                pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, outcome_access_count=led["outcome_access_count"], classification_census=cls_census,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id, seal_registry_sha256_16=s["registry_sha256_16"],
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")), outcome_access_count=led["outcome_access_count"],
                   pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, classification_census=cls_census, status="COMPLETE — family SEARCH-EXPOSED",
                   rule="every later idea born from these outcomes is POST_EXPOSURE_HYPOTHESIS; no threshold / window / TF search follows"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"classification: {cls_census}")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    if cmd == "build":
        build()
    elif cmd == "seal":
        s = seal(); print(json.dumps({k: s[k] for k in ("sealed_at", "registry_sha256_16", "k", "x_run", "x_sha256_16")}, indent=1))
    elif cmd == "outcome":
        outcome()
    else:
        raise SystemExit("build | seal | outcome")
