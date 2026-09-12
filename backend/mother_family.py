"""MOTHER_V1 — does the 4-bar MOTHER structure pay, and is it conditional on the RSI band?
User-approved 5-line plan 2026-09-10 ("gaushvi marto mother"). k = 5.

  Signal   MOTHER, from the user's own chart observation (Pine `260910_NEST`), a-priori — it was NOT
           derived by looking at any outcome. On four consecutive sessions q1..q4 (q4 = the signal bar):
               body(q2) inside body(q1)  AND  body(q3) inside body(q1)  AND  close(q4) > max(open(q1), close(q1))
           Bodies are |open..close| extents; bar colour is ignored. The closing leg makes it directional.
  Why      the user's hypothesis, with a mechanism: at LOW rsi MOTHER = compression at a low then a
           reclaim (the book's validated reversal family); at HIGH rsi it is a continuation breakout
           (the family the book keeps refuting). So the RSI band should split it.
  Cells    MOTHER|ALL · MOTHER|RSI<35 · MOTHER|RSI35-50 · MOTHER|RSI50-60 · MOTHER|RSI>=60. k = 5.
           The bands are the book's standard cuts, fixed here, never searched.
  Prices   MOTHER is computed on the CANONICAL 1D open/close — the same authority the trades run on, so
           the pattern simulated is exactly the pattern detected. It is evaluated on the FULL consecutive
           session sequence per ticker; the liquidity filter is applied to the SIGNAL bar only (filtering
           first would splice non-consecutive sessions into a "4-bar" window). rsi_14 is joined from the
           Studio bars store for the band, as every other family in the book does.
  Universe close >= 21, avg_vol_20d > 0, close*volume >= 3M, universe <> index, one row per (ticker, date).
  Control  CONTROL_KEYS v2 (de-phased; >= 20 trades on 100 % of sessions in every year). Per entry date the
           control-day median; edge = trade - that median; day-clustered.
  Estimand direct sacred `edge_replay._pathsim` over the registered mask, engine cooldown included.
  OOS      MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  Stop     nothing passes AND replicates -> NULL. One outcome run; no cell re-cut, no band search.
           The overlap census against the book's own signals/edges is X-only and DESCRIPTIVE, never a cell.
"""
from __future__ import annotations
import os, sys, json, time, hashlib, glob, gc
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import HardStop, RunSpace                                 # noqa: E402
import ovd_build as B                                                    # noqa: E402
import ovd_outcome_v1 as O                                               # noqa: E402
import ovd_outcome_direct_v1 as OD                                       # noqa: E402
import control_keys as CK                                                # noqa: E402
from breadth_family import GATES, classify, _stats_window                # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/MOTHER_V1"
FAMILY = "MOTHER_V1"
DB = os.path.join(os.path.dirname(HERE), "data", "studio_analytics.duckdb")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED = 5
RSI_BANDS = ((-1e9, 35.0, "<35"), (35.0, 50.0, "35-50"), (50.0, 60.0, "50-60"), (60.0, 1e9, ">=60"))
# X-only overlap census — never cells
OVL_MASKS = ["E_washout", "E_qzcapit", "E_t1capbounce", "E_zoneretest", "E_coilfloor", "E_spring", "E_engulfabs"]
OVL_COLS = ["svs", "rtv", "hilo_buy", "l34", "sig_abs", "sig_conso", "vbo_up", "be_up"]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def cells():
    c = [dict(claim_id="MOTHER|ALL", band=None, rule="every MOTHER bar in the liquid universe")]
    for lo, hi, lab in RSI_BANDS:
        c.append(dict(claim_id=f"MOTHER|RSI{lab}", band=lab, rule=f"MOTHER & rsi_14 in [{lo}, {hi})"))
    return c


def cell_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    if c["band"] is None:
        return np.ones(len(X), bool)
    return (X["rsi_band"] == c["band"]).to_numpy()


def _band(v: np.ndarray) -> np.ndarray:
    out = np.full(len(v), "", dtype=object)
    for lo, hi, lab in RSI_BANDS:
        out[(v >= lo) & (v < hi)] = lab
    return out


# ── X build ───────────────────────────────────────────────────────────────────────────────
def mother_flags(px: pd.DataFrame) -> pd.DataFrame:
    """MOTHER on the FULL consecutive session sequence per ticker. px must be sorted (ticker, session)."""
    o, c = px["open"], px["close"]
    top = np.maximum(o, c); bot = np.minimum(o, c)
    px = px.assign(_top=top, _bot=bot)
    g = px.groupby("ticker", sort=False)
    # q1 = 3 bars back, q2 = 2 back, q3 = 1 back, q4 = this bar
    q1t, q1b = g["_top"].shift(3), g["_bot"].shift(3)
    q2t, q2b = g["_top"].shift(2), g["_bot"].shift(2)
    q3t, q3b = g["_top"].shift(1), g["_bot"].shift(1)
    in2 = (q2t <= q1t) & (q2b >= q1b)          # bar2 body inside bar1 body (inclusive, as the Pine default)
    in3 = (q3t <= q1t) & (q3b >= q1b)          # bar3 body inside bar1 body
    brk = px["close"] > q1t                    # bar4 closes above BOTH of bar1's open and close
    px["mother"] = (in2 & in3 & brk).fillna(False).to_numpy()
    px["m_in2"] = in2.fillna(False).to_numpy()
    px["m_in3"] = in3.fillna(False).to_numpy()
    px["m_brk"] = brk.fillna(False).to_numpy()
    return px.drop(columns=["_top", "_bot"])


def _engine_state(log=print):
    import edge_replay as E
    t0 = time.time(); grp, as_of = E._frame(60, float(DV_FLOOR))
    keep = [m for m in OVL_MASKS]
    parts = []
    for tk, g in grp.items():
        cols = [x for x in keep if x in g.columns]
        p = g[["date"] + cols].copy(); p["ticker"] = tk; parts.append(p)
    st = pd.concat(parts, ignore_index=True)
    st["session"] = st["date"].astype(str).str[:10]; st = st.drop(columns=["date"])
    present = [x for x in keep if x in st.columns]
    for x in keep:
        if x not in st.columns:
            st[x] = False
        st[x] = st[x].fillna(False).astype(bool)
    log(f"  engine frame: {len(grp):,} tickers · as_of {as_of} · masks {present} ({time.time()-t0:.0f}s)")
    try:
        E._CACHE.clear()
    except Exception:
        pass
    del grp; gc.collect()
    return st, str(as_of)


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("MO_%Y%m%dT%H%M%SZ", time.gmtime()))

    px = pd.read_parquet(cur["derived_parquet"], columns=["ticker", "session_date", "open", "close", "volume", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    px = px.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    px = mother_flags(px)
    log(f"  canonical rows {len(px):,} · MOTHER raw fires {int(px['mother'].sum()):,}")

    sig = px[px["mother"]
             & (px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
             & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1])][
        ["ticker", "session", "close", "m_in2", "m_in3", "m_brk"]].reset_index(drop=True)
    log(f"  after liquidity + window: {len(sig):,} signal rows")
    del px; gc.collect()

    import duckdb
    con = duckdb.connect(DB, read_only=True)
    try:
        con.register("keys", sig[["ticker", "session"]])
        cols = ", ".join(f"b.{c}" for c in ["rsi_14"] + OVL_COLS)
        st = con.execute(f"""
            WITH r AS (SELECT b.ticker, CAST(b.date AS VARCHAR) AS session, {cols},
                              row_number() OVER (PARTITION BY b.ticker, b.date ORDER BY b.universe) rn
                       FROM bars b JOIN keys k ON k.ticker = b.ticker AND CAST(b.date AS VARCHAR) = k.session
                       WHERE b.universe <> 'index')
            SELECT * EXCLUDE (rn) FROM r WHERE rn = 1""").fetchdf()
    finally:
        con.close()
    st["session"] = st["session"].str[:10]
    X = sig.merge(st, on=["ticker", "session"], how="left")
    eng, as_of = _engine_state(log)
    X = X.merge(eng, on=["ticker", "session"], how="left")
    X["in_frame"] = X["E_washout"].notna()
    for m in OVL_MASKS:
        X[m] = X[m].fillna(False).astype(bool)
    for c in OVL_COLS:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0).astype(int).astype(bool)
    X["rsi_band"] = _band(pd.to_numeric(X["rsi_14"], errors="coerce").to_numpy(float))
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)

    cs = {c["claim_id"]: dict(TRUE=int(cell_mask(X, c).sum()),
                              TRUE_mine=int((cell_mask(X, c) & X["in_mine"]).sum()),
                              TRUE_verify=int((cell_mask(X, c) & X["in_verify"]).sum())) for c in cells()}
    ovl = {m: dict(share=round(float(X[m].mean()), 4), n=int(X[m].sum())) for m in OVL_MASKS}
    ovl.update({c: dict(share=round(float(X[c].mean()), 4), n=int(X[c].sum())) for c in OVL_COLS})
    ovl["ANY_BOOK_EDGE"] = dict(share=round(float(X[OVL_MASKS].any(axis=1).mean()), 4), n=int(X[OVL_MASKS].any(axis=1).sum()))
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, px_min=PX_MIN, dv_floor=DV_FLOOR,
               signal=dict(name="MOTHER", origin="user chart observation (Pine 260910_NEST), a-priori — not derived from any outcome",
                           definition="body(q2) in body(q1) AND body(q3) in body(q1) AND close(q4) > max(open(q1), close(q1)); bodies = |open..close|; colour ignored",
                           prices="canonical 1D open/close, full consecutive session sequence; liquidity filter on the signal bar only"),
               bands=[b[2] for b in RSI_BANDS],
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               sources=dict(studio_db=DB, engine_frame_as_of=as_of),
               census=dict(rows=int(len(X)), tickers=int(X["ticker"].nunique()), days=int(X["session"].nunique()),
                           rows_per_year={int(y): int(n) for y, n in X.groupby(X["session"].str[:4]).size().items()},
                           in_engine_frame=round(float(X["in_frame"].mean()), 4),
                           rsi_band_shares=X["rsi_band"].value_counts(normalize=True).round(4).to_dict(),
                           cells=cs, overlap=ovl),
               x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    log(f"X {len(X):,} MOTHER bars · " + " · ".join(f"{k.split('|')[1]} {v['TRUE_mine']}/{v['TRUE_verify']}" for k, v in cs.items()))
    log("  overlap: " + " · ".join(f"{k} {v['share']:.3f}" for k, v in list(ovl.items())[:10]))
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "MO_*")), reverse=True):
        if RunSpace.is_current(r, "X.parquet"):
            return r
    return None


# ── seal ──────────────────────────────────────────────────────────────────────────────────
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
    # ── declared BEFORE the outcome, straight from the X census ──────────────────────────
    # MOTHER's closing leg (close > bar1's body top) is a breakout close, so it lands on HIGH rsi
    # by construction: >=60 36.6% · 50-60 34.2% · 35-50 23.3% · <35 only 1.7% (253 MINE rows).
    # The user's hypothesis targets exactly the rarest cell, so RSI<35 carries the least evidence
    # and will be the first to fail the THIN rule / carry the weakest DSR. Recorded, not trimmed.
    bands = rep["census"]["rsi_band_shares"]
    lim = dict(field="rsi_band", band_shares=bands,
               rsi_lt35_mine=rep["census"]["cells"]["MOTHER|RSI<35"]["TRUE_mine"],
               note="MOTHER is structurally a high-RSI pattern (its last leg is a breakout close); the "
                    "hypothesis cell RSI<35 is the rarest by construction and may be THIN. Declared, not trimmed.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", known_limitations=lim,
               sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-10 (\"gaushvi marto mother\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "A_PRIORI_HYPOTHESIS — pattern came from chart observation, never from an outcome"],
               question="does MOTHER pay, and does the RSI band split it (user hypothesis: low RSI good, high RSI bad)?",
               signal=rep["signal"], bands=rep["bands"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}, universe <> index, one row per (ticker, date)",
               control=f"CONTROL_KEYS v2 de-phased ({rep['control']['run_id']}); per entry date the control-day median (>= {O.MIN_CONTROL}); "
                       "edge = trade - control median; day-clustered",
               estimand="direct sacred edge_replay._pathsim over the registered mask, engine cooldown INCLUDED; registered exit law; price-return paths",
               oos=OOS, gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="DSR with the 5 cells' MINE day-edge Sharpes; n_trials = 5"),
               cells=cells(), descriptives="X-only overlap census vs the book's signals and edge masks — never a cell",
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], sources=rep["sources"], census=rep["census"],
               stop_rule="nothing passes AND replicates -> NULL; one outcome run; no cell re-cut; no band search; BUILD only on a second user OK")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(mother_family=_dig(__file__), edge_replay=_dig(os.path.join(HERE, "edge_replay.py")),
                       control_keys=_dig(os.path.join(HERE, "control_keys.py")), ovd_outcome_v1=_dig(O.__file__),
                       ovd_outcome_direct_v1=_dig(OD.__file__)),
             outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


# ── outcome ───────────────────────────────────────────────────────────────────────────────
def _day_control(ctrl: pd.DataFrame, obs: pd.DataFrame):
    med = ctrl.groupby("date_in")["ret"].median(); cnt = ctrl.groupby("date_in")["ret"].size()
    return obs["date_in"].map(med).to_numpy(float), obs["date_in"].map(cnt).fillna(0).to_numpy(int)


def outcome(log=print) -> dict:
    sp = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(sp):
        raise HardStop("not sealed")
    s = json.load(open(sp)); reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")))
    if _dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")) != s["registry_sha256_16"] or len(reg["cells"]) != s["k"]:
        raise HardStop("registry digest / k mismatch")
    run_x = os.path.join(FAMILY_DIR, "runs", s["x_run"])
    if _dig(os.path.join(run_x, "X.parquet")) != s["x_sha256_16"]:
        raise HardStop("X mismatch")
    cur = B.canonical_current()
    if cur["derived_sha256_16"] != s["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical changed")
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    if man["sha256_16"] != s["control"]["sha256_16"]:
        raise HardStop("control artifact changed since sealing")
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "mother")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), seal_registry=s["registry_sha256_16"]))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")

    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    ctl = CK.load(man); ctl["session"] = ctl["session"].astype(str).str[:10]
    ctl = ctl[(ctl["session"] >= OOS["MINE"][0]) & (ctl["session"] <= OOS["VERIFY"][1])].reset_index(drop=True)
    frames = OD.load_frames(cur["derived_parquet"], pd.concat([X["ticker"], ctl["ticker"]]).unique())
    trades, cons = {}, {}
    for c in reg["cells"]:
        m = cell_mask(X, c)
        tr, cn = OD.direct_trades(ps, frames, X.loc[m, ["ticker", "session"]])
        tr["cell_id"] = c["claim_id"]; trades[c["claim_id"]] = tr; cons[c["claim_id"]] = cn
        log(f"  {c['claim_id']}: signal {cn.get('signal_true_n', '?')} -> trades {len(tr)}")
    ctr, ccn = OD.direct_trades(ps, frames, ctl[["ticker", "session"]])
    cons["CONTROL"] = ccn; log(f"  control: {ccn.get('signal_true_n', '?')} -> trades {len(ctr)}")
    run.write_atomic("selection_conservation.json", cons)
    pd.concat([t for t in trades.values() if len(t)] + [ctr.assign(cell_id="CONTROL")], ignore_index=True) \
      .to_parquet(os.path.join(run.dir, "direct_trades.parquet"), index=False)
    ctr = ctr[ctr["ret"].notna()]
    cn = ctr.groupby("date_in")["ret"].size()
    cov = {}
    for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
        sl = cn[(cn.index >= lo) & (cn.index <= hi)]
        cov[w] = dict(sessions=int(len(sl)), median_per_day=float(sl.median()) if len(sl) else None,
                      share_ge_min=round(float((sl >= O.MIN_CONTROL).mean() * 100), 1) if len(sl) else None)
    log(f"  control coverage: {cov}")

    results, mine_series = [], {}
    for cid, tr in trades.items():
        obs = tr[tr["ret"].notna()].copy()
        if len(obs):
            cv, n = _day_control(ctr, obs)
            obs["control"] = cv; obs["n_others"] = n
            obs["edge"] = np.where(obs["n_others"] >= O.MIN_CONTROL, obs["ret"] - obs["control"], np.nan)
        mine = _stats_window(obs, *OOS["MINE"]); ver = _stats_window(obs, *OOS["VERIFY"])
        results.append(dict(claim_id=cid, **{k: v for k, v in cons[cid].items()},
                            mine={k: v for k, v in mine.items() if k != "day_series"},
                            verify={k: v for k, v in ver.items() if k != "day_series"}))
        mine_series[cid] = mine["day_series"]
    from overfit_stats import dsr, sharpe, psr
    kids = [r["claim_id"] for r in results]
    if len(kids) != K_EXPECTED:
        raise HardStop(f"k mismatch: {len(kids)} != {K_EXPECTED}")
    trial = [sharpe(mine_series[k].to_numpy()) if len(mine_series[k]) >= 2 else 0.0 for k in kids]
    for r in results:
        srs = mine_series[r["claim_id"]].to_numpy(float)
        if len(srs) >= 3:
            dp = dsr(srs, trial, n_trials=K_EXPECTED); dn = dsr(-srs, [-x for x in trial], n_trials=K_EXPECTED)
            r.update(dsr=dp["dsr"], sr=dp["sr"], sr_star=dp["sr_star"], dsr_neg=dn["dsr"], psr0=round(float(psr(srs, 0.0)), 4))
        else:
            r.update(dsr=None, sr=None, sr_star=None, dsr_neg=None, psr0=None)
        r["classification"] = classify(r["mine"], r["verify"], r["dsr"], r["dsr_neg"])
        m, v = r["mine"], r["verify"]
        log(f"  {r['claim_id']:18s} MINE med {m['median_edge']} win {m['day_win']} yrs {m['positive_years']}/{m['years_counted']} "
            f"worst {m['worst_year']} raw {m.get('raw_median')} | VERIFY med {v['median_edge']} win {v['day_win']} worst {v['worst_year']} "
            f"raw {v.get('raw_median')} | DSR {r['dsr']} | {r['classification']}")
    run.write_atomic("results.json", results)
    cls = {}
    for r in results:
        cls[r["classification"]] = cls.get(r["classification"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED, oos=OOS,
                gates=GATES, control=s["control"], control_coverage=cov, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                outcome_access_count=led["outcome_access_count"], classification_census=cls,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id, seal_registry_sha256_16=s["registry_sha256_16"],
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")), outcome_access_count=led["outcome_access_count"],
                   pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, classification_census=cls, status="COMPLETE — family SEARCH-EXPOSED"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"classification: {cls}")
    return summ


def report(write: bool = True) -> str:
    runs = [r for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OUT_*")), reverse=True) if RunSpace.is_current(r, "results.json")]
    if not runs:
        raise HardStop("no completed outcome run")
    run = runs[0]; res = json.load(open(os.path.join(run, "results.json"))); summ = json.load(open(os.path.join(run, "summary.json")))
    reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"))); cen = reg["census"]
    f = lambda v, d=2: "—" if v is None else f"{float(v):+.{d}f}"
    L = [f"# MOTHER_V1 — first outcome ({summ['run_id']})\n",
         f"Seal `{summ['registry_sha256_16']}` · X `{reg['x_run']}` · k = {summ['k']} · control `{summ['control']['run_id']}` "
         f"· path-sim `{summ['pathsim_src_sha256_16']}` · access {summ['outcome_access_count']}\n",
         f"MOTHER: {reg['signal']['definition']}\n",
         f"{cen['rows']:,} bars · {cen['tickers']:,} tickers · {cen['days']:,} days · per year {cen['rows_per_year']}\n",
         f"Control coverage: {summ['control_coverage']}\n",
         "## Deciding table\n",
         "| cell | trades | MINE edge | win% | yrs+ | worst | raw | DSR | VERIFY edge | win% | worst | raw | class |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for r in res:
        m, v = r["mine"], r["verify"]
        L.append(f"| {r['claim_id']} | {r.get('direct_selected_n', 0):,} | {f(m['median_edge'])} | {f(m['day_win'],1)} | "
                 f"{m['positive_years']}/{m['years_counted']} | {f(m['worst_year'])} | {f(m.get('raw_median'))} | {f(r.get('dsr'))} | "
                 f"{f(v['median_edge'])} | {f(v['day_win'],1)} | {f(v['worst_year'])} | {f(v.get('raw_median'))} | {r['classification']} |")
    L.append("\n## X-only overlap with the book's own signals and edges (share of MOTHER bars)\n")
    L.append("| item | share | n |"); L.append("|---|---:|---:|")
    for k, v in cen["overlap"].items():
        L.append(f"| {k} | {v['share']:.3f} | {v['n']:,} |")
    L.append(f"\nRSI band shares: {cen['rsi_band_shares']}\n")
    L.append(f"Gates: {json.dumps(reg['gates'])}\n")
    txt = "\n".join(L)
    if write:
        open(os.path.join(FAMILY_DIR, "CHECKPOINT_FIRST_OUTCOME.md"), "w").write(txt)
    return txt


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items() if k in ("sealed_at", "registry_sha256_16", "k", "x_run", "x_sha256_16")}, indent=1)),
     "outcome": outcome,
     "report": lambda: print(report(write="--dry" not in sys.argv))}[cmd]()
