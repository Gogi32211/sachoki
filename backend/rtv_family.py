"""RTV_V1 — does the RTV reversal pay on its own, and is it anything the book does not already own?
Separate research family; user-approved 5-line plan 2026-09-09 ("ki"). k = 4.

  Signal   RTV = the app's `rtv` bars column — a LINE-FOR-LINE port of the user's Pine
           (`combo_engine.py`): RSI(2) crossing up through 20 · a bearish close in the prior 2 bars ·
           a green reversal bar · Williams-VIX-Fix confirm (wvf >= BB upper OR >= 0.85 x its 50-bar
           high, this bar or the one before). Never re-implemented here; read as a stored column.
  Cells    RTV|ALL · RTV|RS (rs_intact) · RTV|RSI35 (rsi_14 < 35) · RTV|NOT_WSH (no E_washout on the
           same bar). k = 4, all in k. The gates come from the book, not from a search.
  Why      RTV sits in the family the book already validated (Washout +1.73/6yr, Absorption-reversal
           +1.70/6yr) but has never been measured on its own; the NOT_WSH cell asks whether it is a
           new signal or a re-labelling of Washout.
  Universe close >= 21, avg_vol_20d > 0, close*volume >= 3M, universe <> index, one row per
           (ticker, date) — the book's liquid frame.
  Control  the engine's own book control (feedback-day-clustered-accounting): every 40th bar with
           close >= $21 per ticker, reused verbatim from GATE_QUIET_GEM1_V1 so the reference is the
           same one the other families were judged against. Per entry date the control-day median
           (>= 20 control trades); edge = trade - that median; day-clustered.
  Estimand direct sacred `edge_replay._pathsim` over the registered mask, the engine's own cooldown
           included; no local exit engine, no partitioning of the mask.
  OOS      MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  Stop     nothing passes AND replicates -> NULL. One outcome run; no cell re-cut, no threshold
           search (the 35 RSI cut and the RS gate are book constants). BUILD only on a second user OK.
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
from breadth_family import GATES, classify, _stats_window                # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/RTV_V1"
FAMILY = "RTV_V1"
DB = os.path.join(os.path.dirname(HERE), "data", "studio_analytics.duckdb")
CONTROL_SRC = "/Users/sachoki/MASSIVE_DATA/GATE_QUIET_GEM1_V1/engine_control_keys.parquet"
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
RSI_CUT = 35.0
PX_MIN = 21.0
DV_FLOOR = 3_000_000
K_EXPECTED = 4
# X-only overlap census — which book edges land on the same bar (descriptive, never a cell)
OVERLAP = ["E_washout", "E_qzcapit", "E_t1capbounce", "E_engulf_absorb_rev", "E_dl1", "E_spring", "E_zoneretest"]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def cells():
    return [dict(claim_id="RTV|ALL",     rule="every RTV bar in the liquid universe"),
            dict(claim_id="RTV|RS",      rule="RTV & rs_intact (close/benchmark > EMA200) — the book's worst-year rescuer"),
            dict(claim_id="RTV|RSI35",   rule=f"RTV & rsi_14 < {RSI_CUT:.0f} — the only all-era-positive RSI zone"),
            dict(claim_id="RTV|NOT_WSH", rule="RTV & NOT E_washout on the same bar — is it new, or Washout re-labelled?")]


def cell_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    cid = c["claim_id"]
    if cid == "RTV|ALL":
        return np.ones(len(X), bool)
    if cid == "RTV|RS":
        return X["rs_intact"].to_numpy(bool)
    if cid == "RTV|RSI35":
        return (X["rsi_14"].to_numpy(float) < RSI_CUT)
    if cid == "RTV|NOT_WSH":
        return ~X["E_washout"].to_numpy(bool)
    raise HardStop(f"unknown cell {cid}")


# ── X build ───────────────────────────────────────────────────────────────────────────────
def _engine_state(log=print):
    """X-only: rs_intact + the overlap masks per (ticker, session), from the engine's own frame."""
    import edge_replay as E
    t0 = time.time()
    grp, as_of = E._frame(60, float(DV_FLOOR))
    keep = ["rs_intact"] + OVERLAP
    parts = []
    for tk, g in grp.items():
        cols = [c for c in keep if c in g.columns]
        p = g[["date"] + cols].copy(); p["ticker"] = tk; parts.append(p)
    st = pd.concat(parts, ignore_index=True)
    st["session"] = st["date"].astype(str).str[:10]; st = st.drop(columns=["date"])
    present = [c for c in keep if c in st.columns]
    for c in keep:
        if c not in st.columns:
            st[c] = False
        st[c] = st[c].fillna(False).astype(bool)
    log(f"  engine frame: {len(grp):,} tickers · {len(st):,} rows · as_of {as_of} · masks present {present} ({time.time() - t0:.0f}s)")
    try:
        E._CACHE.clear()
    except Exception:
        pass
    del grp; gc.collect()
    return st, str(as_of)


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("RV_%Y%m%dT%H%M%SZ", time.gmtime()))
    import duckdb
    con = duckdb.connect(DB, read_only=True)
    try:
        sig = con.execute(f"""
            WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn FROM bars
                       WHERE close >= {PX_MIN} AND avg_vol_20d > 0 AND close*volume >= {DV_FLOOR}
                         AND universe <> 'index' AND rtv = 1
                         AND date BETWEEN '{OOS["MINE"][0]}' AND '{OOS["VERIFY"][1]}')
            SELECT ticker, CAST(date AS VARCHAR) AS session, close, rsi_14, hilo_buy, svs
            FROM r WHERE rn = 1 ORDER BY ticker, session""").fetchdf()
    finally:
        con.close()
    sig["session"] = sig["session"].str[:10]
    log(f"  RTV signal rows {len(sig):,} · tickers {sig['ticker'].nunique():,} · days {sig['session'].nunique():,}")
    st, as_of = _engine_state(log)
    X = sig.merge(st, on=["ticker", "session"], how="left")
    X["in_frame"] = X["rs_intact"].notna()
    for c in ["rs_intact"] + OVERLAP:
        X[c] = X[c].fillna(False).astype(bool)
    # a signal bar that the engine frame never saw cannot carry the book's state — drop it, and say so
    n_all = len(X); X = X[X["in_frame"]].reset_index(drop=True)
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    ctl = pd.read_parquet(CONTROL_SRC)
    ctl["session"] = ctl["session"].astype(str).str[:10]
    ctl = ctl[(ctl["session"] >= OOS["MINE"][0]) & (ctl["session"] <= OOS["VERIFY"][1])].reset_index(drop=True)
    cp = os.path.join(run.dir, "control_keys.parquet"); ctl.to_parquet(cp, index=False)
    cs = {c["claim_id"]: dict(TRUE=int(cell_mask(X, c).sum()),
                              TRUE_mine=int((cell_mask(X, c) & X["in_mine"]).sum()),
                              TRUE_verify=int((cell_mask(X, c) & X["in_verify"]).sum())) for c in cells()}
    ov = {m: dict(share=round(float(X[m].mean()), 4), n=int(X[m].sum())) for m in OVERLAP}
    ov["ANY_BOOK_EDGE"] = dict(share=round(float(X[OVERLAP].any(axis=1).mean()), 4), n=int(X[OVERLAP].any(axis=1).sum()))
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, rsi_cut=RSI_CUT, px_min=PX_MIN, dv_floor=DV_FLOOR,
               signal=dict(column="rtv", producer="combo_engine.py (line-for-line port of the user's Pine RTV)",
                           definition="RSI(2) crossover 20 & bearish close in prior 2 bars & green bar & Williams-VIX-Fix confirm"),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               sources=dict(studio_db=DB, engine_frame_as_of=as_of, control_keys=dict(path=CONTROL_SRC, sha256_16=_dig(CONTROL_SRC), rows=int(len(ctl)))),
               census=dict(rtv_rows_raw=int(n_all), rtv_rows=int(len(X)), dropped_not_in_engine_frame=int(n_all - len(X)),
                           tickers=int(X["ticker"].nunique()), days=int(X["session"].nunique()),
                           rows_per_year={int(y): int(n) for y, n in X.groupby(X["session"].str[:4]).size().items()},
                           cells=cs, overlap_with_book_edges=ov,
                           hilo_buy_share=round(float(X["hilo_buy"].fillna(0).astype(bool).mean()), 4),
                           svs_same_bar_share=round(float(X["svs"].fillna(0).astype(bool).mean()), 4)),
               x_sha256_16=_dig(xp), control_sha256_16=_dig(cp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    log(f"X {len(X):,} RTV bars · cells " + " · ".join(f"{k} {v['TRUE_mine']}/{v['TRUE_verify']}" for k, v in cs.items()))
    log(f"  overlap with book edges: " + " · ".join(f"{k} {v['share']:.3f}" for k, v in ov.items()))
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "RV_*")), reverse=True):
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
    # ── declared BEFORE the outcome, from the X census ──────────────────────────────────
    # The engine frame starts ~60 months back, so the RS reference's EMA200 has no warm-up
    # data at the head of MINE: rs_intact is identically FALSE for every 2021 bar. The RS cell
    # therefore cannot have a 2021 year, and the gates' min_days_per_year rule will simply not
    # count it (the same thing happened to SCORE_AUDIT_V1's C|v3_rs, which scored 3/3 not 4/4).
    # Recorded here rather than trimming the window per cell — trimming would make the four
    # cells non-comparable and is exactly the kind of tailoring the stop rule forbids.
    Xs = pd.read_parquet(os.path.join(run, "X.parquet"))
    rs_by_year = {str(y): round(float(v), 4) for y, v in Xs.groupby(Xs["session"].str[:4])["rs_intact"].mean().items()}
    warmup = dict(field="rs_intact", rs_true_share_by_year=rs_by_year,
                  note="2021 is identically 0.0 — RS benchmark EMA200 warm-up at the head of the engine frame; "
                       "the RS cell loses that year to the min_days_per_year gate, it is NOT trimmed away")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", known_limitations=warmup,
               sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-09 (\"ki\" on the 5-line plan)", "PRE_OUTCOME", "PRE_PATHSIM",
                           "SIGNAL_FROM_A_USER_PINE_SCRIPT_ALREADY_PORTED_AS_A_BARS_COLUMN"],
               question="does RTV pay on its own, and does it survive removing the bars Washout already owns?",
               signal=rep["signal"], universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}, universe <> index, one row per (ticker, date)",
               control="engine book control: every 40th bar with close >= 21 per ticker (reused verbatim from GATE_QUIET_GEM1_V1); "
                       "per entry date the control-day median (>= 20); edge = trade - control median; day-clustered",
               estimand="direct sacred edge_replay._pathsim over the registered mask, engine cooldown INCLUDED; registered exit law; price-return paths",
               oos=OOS, gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="DSR with the 4 cells' MINE day-edge Sharpes; n_trials = 4"),
               cells=cells(), descriptives="overlap census with the book's own edges (X-only, never a cell); HILO/SVS co-fire shares",
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], control_sha256_16=rep["control_sha256_16"],
               canonical_1d=rep["canonical_1d"], sources=rep["sources"], census=rep["census"],
               stop_rule="nothing passes AND replicates -> NULL; one outcome run; no cell re-cut; no threshold search "
                         "(RSI 35 and the RS gate are book constants); BUILD only on a second user OK")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED, x_run=rep["run_id"],
             x_sha256_16=rep["x_sha256_16"], control_sha256_16=rep["control_sha256_16"], canonical_1d=rep["canonical_1d"],
             oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(rtv_family=_dig(__file__), edge_replay=_dig(os.path.join(HERE, "edge_replay.py")),
                       combo_engine=_dig(os.path.join(HERE, "combo_engine.py")), ovd_outcome_v1=_dig(O.__file__),
                       ovd_outcome_direct_v1=_dig(OD.__file__)),
             outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


# ── outcome ───────────────────────────────────────────────────────────────────────────────
def _day_control(ctrl: pd.DataFrame, obs: pd.DataFrame):
    med = ctrl.groupby("date_in")["ret"].median(); cnt = ctrl.groupby("date_in")["ret"].size()
    c = obs["date_in"].map(med).to_numpy(float); n = obs["date_in"].map(cnt).fillna(0).to_numpy(int)
    return c, n


def outcome(log=print, control: str = "sealed", amendment: str | None = None) -> dict:
    """First run: control='sealed' (the phase-0 keys the SEAL binds). AMENDMENT_1 (post-exposure, declared in
    AMENDMENT_1.md): control='v2' swaps in the repaired de-phased CONTROL_KEYS artifact — same cells, same HIGH
    rules, same gates, same k, same OOS; only the mis-sampled instrument changes. Run 1 stays immutable."""
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
    if control == "sealed":
        if _dig(os.path.join(run_x, "control_keys.parquet")) != s["control_sha256_16"]:
            raise HardStop("control mismatch")
        ctl_path, ctl_prov = os.path.join(run_x, "control_keys.parquet"), dict(source="SEALED_PHASE0", sha256_16=s["control_sha256_16"])
    elif control == "v2":
        import control_keys as CK
        _man = CK.current(); CK.assert_matches_canonical(_man, cur)
        ctl_path = _man["parquet"]
        ctl_prov = dict(source="CONTROL_KEYS_V2_DEPHASED", run_id=_man["run_id"], sha256_16=_man["sha256_16"],
                        worst_year_share_ge_min=_man["worst_year_share_ge_min"])
    else:
        raise HardStop(f"unknown control '{control}'")
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "rtv")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    if amendment and not os.path.exists(os.path.join(FAMILY_DIR, f"{amendment}.md")):
        raise HardStop(f"{amendment}.md must be written (declared) before the amended run")
    if amendment and led["outcome_access_count"] < 1:
        raise HardStop("an amendment is a POST-exposure correction only")
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()) + (f"_{amendment}" if amendment else ""))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                              seal_registry=s["registry_sha256_16"], control=ctl_prov, amendment=amendment))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']} (control {ctl_prov['source']}{', ' + amendment if amendment else ''})")
    X = pd.read_parquet(os.path.join(run_x, "X.parquet")); ctl = pd.read_parquet(ctl_path)
    ctl["session"] = ctl["session"].astype(str).str[:10]
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
        log(f"  {r['claim_id']:14s} MINE med {r['mine']['median_edge']} win {r['mine']['day_win']} "
            f"yrs {r['mine']['positive_years']}/{r['mine']['years_counted']} worst {r['mine']['worst_year']} raw {r['mine'].get('raw_median')} "
            f"| VERIFY med {r['verify']['median_edge']} win {r['verify']['day_win']} worst {r['verify']['worst_year']} raw {r['verify'].get('raw_median')} "
            f"| DSR {r['dsr']} | {r['classification']}")
    run.write_atomic("results.json", results)
    # control day-coverage per window — print it BEFORE reading any verdict (feedback-control-density)
    cn = ctr.groupby("date_in")["ret"].size()
    cov = {}
    for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
        sl = cn[(cn.index >= lo) & (cn.index <= hi)]
        cov[w] = dict(sessions=int(len(sl)), median_per_day=float(sl.median()) if len(sl) else None,
                      share_ge_min=round(float((sl >= O.MIN_CONTROL).mean() * 100), 1) if len(sl) else None)
    log(f"  control coverage: {cov}")
    # POST_EXPOSURE_HYPOTHESIS rider — RTV|RS split by same-bar Zone-Retest. Reported, never gated.
    desc = {}
    try:
        rs = trades["RTV|RS"]; rs = rs[rs["ret"].notna()].merge(X[["ticker", "session", "E_zoneretest"]], on=["ticker", "session"], how="left")
        rs["yr"] = rs["date_in"].str[:4]
        desc["RTV|RS_by_zoneretest"] = {
            str(z): {str(y): dict(n=int(len(g)), raw_median=round(float(g["ret"].median()), 3),
                                  win=round(float((g["ret"] > 0).mean() * 100), 1))
                     for y, g in gz.groupby("yr")}
            for z, gz in rs.groupby(rs["E_zoneretest"].fillna(False).astype(bool).map({True: "ZRT", False: "no_ZRT"}))}
        desc["status"] = "POST_EXPOSURE_HYPOTHESIS — derived from the exposed run; descriptive only, cannot support a BUILD"
    except Exception:
        desc["status"] = "unavailable"
    run.write_atomic("descriptives.json", desc)
    cls_census = {}
    for r in results:
        cls_census[r["classification"]] = cls_census.get(r["classification"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED, oos=OOS, gates=GATES,
                control=ctl_prov, amendment=amendment, control_coverage=cov,
                pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, outcome_access_count=led["outcome_access_count"],
                classification_census=cls_census, completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    rec = f"{amendment}_EXECUTION.json" if amendment else "FIRST_OUTCOME_EXECUTION.json"
    if os.path.exists(os.path.join(FAMILY_DIR, rec)):
        raise HardStop(f"{rec} exists — execution records are immutable")
    json.dump(dict(record_id=f"{FAMILY}_{amendment or 'FIRST_OUTCOME'}_EXECUTION", run_id=run.run_id,
                   seal_registry_sha256_16=s["registry_sha256_16"], control=ctl_prov, amendment=amendment,
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")), outcome_access_count=led["outcome_access_count"],
                   pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, classification_census=cls_census,
                   status="COMPLETE — family SEARCH-EXPOSED" + (" — POST_EXPOSURE_CORRECTION" if amendment else "")),
              open(os.path.join(FAMILY_DIR, rec), "w"), indent=1)
    log(f"classification: {cls_census}")
    return summ


def report(write: bool = True) -> str:
    runs = [r for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OUT_*")), reverse=True) if RunSpace.is_current(r, "results.json")]
    if not runs:
        raise HardStop("no completed outcome run")
    run = runs[0]; res = json.load(open(os.path.join(run, "results.json"))); summ = json.load(open(os.path.join(run, "summary.json")))
    reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"))); cen = reg["census"]
    f = lambda v, d=2: "—" if v is None else f"{float(v):+.{d}f}"
    L = [f"# RTV_V1 — first outcome ({summ['run_id']})\n",
         f"Seal `{summ['registry_sha256_16']}` · X `{reg['x_run']}` · k = {summ['k']} · path-sim `{summ['pathsim_src_sha256_16']}` · outcome access {summ['outcome_access_count']}\n",
         f"Signal: `rtv` — {reg['signal']['definition']}\n",
         f"RTV bars {cen['rtv_rows']:,} · {cen['tickers']:,} tickers · {cen['days']:,} days · per year {cen['rows_per_year']}\n",
         "## Deciding table — day-clustered edge vs the book control\n",
         "| cell | trades | MINE edge | win% | yrs+ | worst | raw med | DSR | VERIFY edge | win% | worst | raw med | class |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for r in res:
        m, v = r["mine"], r["verify"]
        L.append(f"| {r['claim_id']} | {r.get('direct_selected_n', 0):,} | {f(m['median_edge'])} | {f(m['day_win'],1)} | "
                 f"{m['positive_years']}/{m['years_counted']} | {f(m['worst_year'])} | {f(m.get('raw_median'))} | {f(r.get('dsr'))} | "
                 f"{f(v['median_edge'])} | {f(v['day_win'],1)} | {f(v['worst_year'])} | {f(v.get('raw_median'))} | {r['classification']} |")
    L.append("\n## Is RTV new? — X-only overlap with the book's own edges (share of RTV bars)\n")
    L.append("| book edge on the same bar | share | n |"); L.append("|---|---:|---:|")
    for k, v in cen["overlap_with_book_edges"].items():
        L.append(f"| {k} | {v['share']:.3f} | {v['n']:,} |")
    L.append(f"\nHILO-buy co-fires on {cen['hilo_buy_share']:.3f} of RTV bars (RTV's own first leg) · SVS on {cen['svs_same_bar_share']:.3f}.\n")
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
     "amend1": lambda: outcome(control="v2", amendment="AMENDMENT_1"),
     "report": lambda: print(report(write="--dry" not in sys.argv))}[cmd]()
