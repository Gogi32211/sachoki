"""SWALLOW_DIR_V1 — does DIRECTION split the swallow shapes (LST / WRP / EXP)?
User-approved 2026-09-10 ("ki"), after the user noticed the gap. k = 6.

  WHY THIS EXISTS — it corrects MY omission, and the user caught it.
  SHAPE_CLUSTER_V1 and SHAPE_GATE_V1 both declared "bodies are |open..close| extents; bar
  colour is ignored", so LST-up and LST-down sat in ONE cell, and likewise WRP and EXP. If the
  two directions carry OPPOSITE signs, pooling them cancels the effect and a NULL is produced
  BY CONSTRUCTION. That risk is concentrated exactly where it hurts: the swallow shapes are
  most of the population (WRAP 33.6% + LAST 27.7% + EXP 5.0% of X).

  It matters because direction here is not decoration. As the user's own Pine notes, on a
  swallow shape containment FORCES the meaning: green = the wrapping bar closed AT/ABOVE the
  engulfed body, red = AT/BELOW it — absorption versus distribution. The book's single most
  replicated law is on exactly this axis: red-L34 +2.05 vs green-L34 -1.01
  (project_l34_red_triple, project_what_actually_works).

  NOT affected, and therefore NOT re-run: MOTHER and COIL4 already carry a directional leg
  (close > q1_top), so they were never pooled — MOTHER's 4 NULL + 1 VETO stands. MID and CON
  are geometrically symmetric and the Pine assigns them no direction either (`canDir` covers
  only EXP / LAST / WRAP).

  Cells    LST|UP · LST|DN · WRP|UP · WRP|DN · EXP|UP · EXP|DN.  k = 6.
           Shapes are taken from the Pine's DISPLAY PRIORITY CHAIN (dispExp / dispLast /
           dispWrap), not the raw flags, so the cells are mutually exclusive, exclude
           MOTHER/COIL/MID/CON, and measure exactly the label the chart prints.
           Direction = `close > open` on the signal bar (Pine `dirUp`). Doji bars
           (close == open) are in NEITHER cell — declared, not silently bucketed.

  PRE-REGISTERED PREDICTION, taken from the BOOK and not from this data:
           DN > UP on all three shapes ("fade strength, buy absorbed weakness").
           UP > DN refutes the law on this family; a zero gap means the earlier pooling was
           harmless and those NULLs stand as measured.

  Prices   canonical 1D open/close, full consecutive session sequence per ticker; identical
           `shape_flags` code path as SHAPE_CLUSTER_V1 — imported, NOT re-implemented, and that
           file is not modified so the two sealed families keep their recorded code digests.
  Universe close >= 21, avg_vol_20d > 0, close*volume >= 3M, universe <> index.
  Control  CONTROL_KEYS v2 (de-phased); per entry date the control-day median; day-clustered.
  Estimand direct sacred `edge_replay._pathsim` over the registered mask, engine cooldown included.
  OOS      MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  Stop     One outcome run. The DN-minus-UP gap must carry the SAME SIGN in both windows for a
           direction claim; otherwise NULL. No cell re-cut, no threshold search.
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
from shape_cluster_family import shape_flags                             # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/SWALLOW_DIR_V1"
FAMILY = "SWALLOW_DIR_V1"
DB = os.path.join(os.path.dirname(HERE), "data", "studio_analytics.duckdb")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED = 6
SHAPE_KEYS = [("LST", "disp_last"), ("WRP", "disp_wrap"), ("EXP", "disp_exp")]
RSI_BANDS = ((-1e9, 35.0, "<35"), (35.0, 50.0, "35-50"), (50.0, 60.0, "50-60"), (60.0, 1e9, ">=60"))


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def cells():
    out = []
    for lab, col in SHAPE_KEYS:
        for d, dl in (("UP", "close > open"), ("DN", "close < open")):
            out.append(dict(claim_id=f"{lab}|{d}", col=col, dir=d,
                            rule=f"Pine display-priority {col} AND {dl} on the signal bar"))
    return out


def cell_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    d = X["dir_up"].to_numpy(bool) if c["dir"] == "UP" else X["dir_dn"].to_numpy(bool)
    return X[c["col"]].to_numpy(bool) & d


def _band(v: np.ndarray) -> np.ndarray:
    out = np.full(len(v), "", dtype=object)
    for lo, hi, lab in RSI_BANDS:
        out[(v >= lo) & (v < hi)] = lab
    return out


def display_chain(px: pd.DataFrame) -> pd.DataFrame:
    """Pine FIX C — the priority chain that decides which shape the LABEL shows, plus the
    direction the arrow uses. Derived here so shape_cluster_family.py stays byte-identical
    to what the two earlier seals recorded."""
    m, c_, mi = px["s_moth"].to_numpy(), px["s_coil"].to_numpy(), px["s_mid"].to_numpy()
    e, cn, la, wr = (px["s_exp"].to_numpy(), px["s_con"].to_numpy(),
                     px["s_last"].to_numpy(), px["s_wrap"].to_numpy())
    not_hi = ~m & ~c_ & ~mi
    px["disp_exp"] = not_hi & e
    px["disp_con"] = not_hi & ~e & cn
    px["disp_last"] = not_hi & ~e & ~cn & la
    px["disp_wrap"] = not_hi & ~e & ~cn & ~la & wr
    px["dir_up"] = (px["close"] > px["open"]).to_numpy()
    px["dir_dn"] = (px["close"] < px["open"]).to_numpy()
    px["can_dir"] = px["disp_exp"] | px["disp_last"] | px["disp_wrap"]
    return px


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("SD_%Y%m%dT%H%M%SZ", time.gmtime()))

    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "open", "close", "volume", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    px = px.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    px = shape_flags(px)
    px = display_chain(px)
    log(f"  canonical rows {len(px):,} · can_dir {int(px['can_dir'].sum()):,} "
        f"(exp {int(px['disp_exp'].sum()):,} · last {int(px['disp_last'].sum()):,} · wrap {int(px['disp_wrap'].sum()):,})")
    doji_all = int((px["can_dir"] & ~px["dir_up"] & ~px["dir_dn"]).sum())

    keep = ["ticker", "session", "close", "disp_exp", "disp_last", "disp_wrap",
            "dir_up", "dir_dn", "can_dir"]
    sig = px[px["can_dir"]
             & (px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
             & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1])][keep].reset_index(drop=True)
    log(f"  after liquidity + window: {len(sig):,} swallow bars")
    del px; gc.collect()

    import duckdb
    con = duckdb.connect(DB, read_only=True)
    try:
        con.register("keys", sig[["ticker", "session"]])
        st = con.execute("""
            WITH r AS (SELECT b.ticker, CAST(b.date AS VARCHAR) AS session, b.rsi_14,
                              row_number() OVER (PARTITION BY b.ticker, b.date ORDER BY b.universe) rn
                       FROM bars b JOIN keys k ON k.ticker = b.ticker AND CAST(b.date AS VARCHAR) = k.session
                       WHERE b.universe <> 'index')
            SELECT * EXCLUDE (rn) FROM r WHERE rn = 1""").fetchdf()
    finally:
        con.close()
    st["session"] = st["session"].str[:10]
    X = sig.merge(st, on=["ticker", "session"], how="left")
    X["rsi_band"] = _band(pd.to_numeric(X["rsi_14"], errors="coerce").to_numpy(float))
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)

    cs = {}
    for c in cells():
        m = cell_mask(X, c)
        cs[c["claim_id"]] = dict(TRUE=int(m.sum()), TRUE_mine=int((m & X["in_mine"]).sum()),
                                 TRUE_verify=int((m & X["in_verify"]).sum()))
    doji = int((~X["dir_up"] & ~X["dir_dn"]).sum())
    # sanity: the 6 cells plus doji must exhaust the population, and the shapes must be disjoint
    covered = np.zeros(len(X), bool)
    for c in cells():
        mm = cell_mask(X, c)
        if (covered & mm).any():
            raise HardStop(f"cells overlap at {c['claim_id']} — display chain is not exclusive")
        covered |= mm
    if int((~covered).sum()) != doji:
        raise HardStop(f"uncovered rows {int((~covered).sum())} != doji {doji}")
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, px_min=PX_MIN, dv_floor=DV_FLOOR,
               signal=dict(name="SWALLOW_DIR",
                           question="does direction (close > open) split the swallow shapes LST / WRP / EXP?",
                           origin="user caught that SHAPE_CLUSTER_V1 and SHAPE_GATE_V1 pooled UP and DN",
                           shapes="Pine display-priority chain: dispExp / dispLast / dispWrap "
                                  "(MOTHER, COIL4, MID, CON take precedence and are excluded)",
                           direction="close > open on the signal bar (Pine dirUp); doji bars in neither cell",
                           semantics="containment forces green = the wrapper closed AT/ABOVE the engulfed "
                                     "body, red = AT/BELOW it — absorption vs distribution",
                           prediction="BOOK-derived: DN > UP on all three (red-L34 +2.05 vs green-L34 -1.01). "
                                      "UP > DN refutes the law here; a zero gap means the earlier pooling was "
                                      "harmless and those NULLs stand.",
                           prices="canonical 1D open/close, full consecutive session sequence; shape_flags "
                                  "imported unmodified from shape_cluster_family"),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               sources=dict(studio_db=DB),
               census=dict(rows=int(len(X)), tickers=int(X["ticker"].nunique()), days=int(X["session"].nunique()),
                           rows_per_year={int(y): int(n) for y, n in X.groupby(X["session"].str[:4]).size().items()},
                           doji_in_X=doji, doji_all_canonical=doji_all,
                           rsi_band_shares=X["rsi_band"].value_counts(normalize=True).round(4).to_dict(),
                           rsi_by_cell={c["claim_id"]: X.loc[cell_mask(X, c), "rsi_band"]
                                        .value_counts(normalize=True).round(3).to_dict() for c in cells()},
                           cells=cs),
               x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    log(f"X {len(X):,} swallow bars · " + " · ".join(f"{k} {v['TRUE_mine']}/{v['TRUE_verify']}" for k, v in cs.items()))
    log(f"  doji excluded: {doji:,} in X")
    log(f"  rsi bands {rep['census']['rsi_band_shares']}")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "SD_*")), reverse=True):
        if RunSpace.is_current(r, "X.parquet"):
            return r
    return None


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
    cen = rep["census"]
    lim = dict(cell_counts={k: v["TRUE_mine"] for k, v in cen["cells"].items()},
               doji_excluded=cen["doji_in_X"], rsi_by_cell=cen["rsi_by_cell"],
               declared_prior="STRONGER than the two earlier shape families. The book has a replicated law on "
                              "exactly this axis (red-L34 +2.05 vs green-L34 -1.01) and the direction is "
                              "mechanically meaningful on a swallow shape. Against it: EXP is rare relative to "
                              "LAST and WRAP, and this is a 3rd look at the same shape population.",
               note="This family exists to test whether the pooling in SHAPE_CLUSTER_V1 / SHAPE_GATE_V1 "
                    "produced a false NULL. A DN>UP result does NOT resurrect those families' verdicts by "
                    "itself — it would mean the swallow shapes need re-measuring WITH direction, which is a "
                    "separate sealed run.",
               multiplicity_carried="SHAPE family looks so far: MOTHER k=5, SHAPE_CLUSTER k=5, SHAPE_GATE k=5, "
                                    "this k=6 -> 21 cells on overlapping populations. Carry into any verdict.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", known_limitations=lim,
               sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-10 (\"ki\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "USER-IDENTIFIED GAP — the user noticed the earlier families pooled UP and DN"],
               question=rep["signal"]["question"], signal=rep["signal"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}, universe <> index",
               control=f"CONTROL_KEYS v2 de-phased ({rep['control']['run_id']}); per entry date the control-day "
                       f"median (>= {O.MIN_CONTROL}); edge = trade - control median; day-clustered",
               estimand="direct sacred edge_replay._pathsim over the registered mask, engine cooldown INCLUDED",
               oos=OOS, gates=GATES,
               multiplicity=dict(k=K_EXPECTED, rule="DSR with the 6 cells' MINE day-edge Sharpes; n_trials = 6"),
               cells=cells(), descriptives="per-cell RSI composition — never a cell",
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], sources=rep["sources"], census=cen,
               stop_rule="the DN-minus-UP gap must carry the SAME SIGN in MINE and VERIFY for a direction "
                         "claim; otherwise NULL. One outcome run; no cell re-cut; BUILD only on a second user OK")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(swallow_dir_family=_dig(__file__),
                       shape_cluster_family=_dig(os.path.join(HERE, "shape_cluster_family.py")),
                       edge_replay=_dig(os.path.join(HERE, "edge_replay.py")),
                       control_keys=_dig(os.path.join(HERE, "control_keys.py")), ovd_outcome_v1=_dig(O.__file__),
                       ovd_outcome_direct_v1=_dig(OD.__file__)),
             outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "swallow")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                              seal_registry=s["registry_sha256_16"]))
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
        log(f"  {r['claim_id']:8s} MINE med {m['median_edge']} win {m['day_win']} yrs {m['positive_years']}/{m['years_counted']} "
            f"worst {m['worst_year']} raw {m.get('raw_median')} | VERIFY med {v['median_edge']} win {v['day_win']} "
            f"worst {v['worst_year']} raw {v.get('raw_median')} | DSR {r['dsr']} | {r['classification']}")

    # the registered test: DN - UP per shape, and whether the sign holds in both windows
    def _m(cid, w):
        return next((r[w]["median_edge"] for r in results if r["claim_id"] == cid), None)
    gaps = {}
    for lab, _ in SHAPE_KEYS:
        g = {}
        for w in ("mine", "verify"):
            u, d = _m(f"{lab}|UP", w), _m(f"{lab}|DN", w)
            g[w] = None if (u is None or d is None) else round(d - u, 3)
            g[f"{w}_up"], g[f"{w}_dn"] = u, d
        g["same_sign"] = (g["mine"] is not None and g["verify"] is not None
                          and g["mine"] * g["verify"] > 0)
        g["direction"] = ("DN>UP" if (g["mine"] or 0) > 0 else "UP>DN") if g["same_sign"] else "unstable"
        gaps[lab] = g
        log(f"  GAP {lab}: MINE dn-up {g['mine']} (up {g['mine_up']} dn {g['mine_dn']}) · "
            f"VERIFY {g['verify']} (up {g['verify_up']} dn {g['verify_dn']}) · same_sign={g['same_sign']}")
    stable = [k for k, v in gaps.items() if v["same_sign"]]
    verdict = ("DIRECTION_MATTERS:" + ",".join(f"{k}={gaps[k]['direction']}" for k in stable)) if stable \
        else "DIRECTION_DOES_NOT_SPLIT"
    log(f"  VERDICT: {verdict}")

    run.write_atomic("results.json", results)
    cls = {}
    for r in results:
        cls[r["classification"]] = cls.get(r["classification"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED, oos=OOS,
                gates=GATES, control=s["control"], control_coverage=cov, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                outcome_access_count=led["outcome_access_count"], classification_census=cls,
                direction_gaps=gaps, verdict=verdict,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id,
                   seal_registry_sha256_16=s["registry_sha256_16"],
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")),
                   outcome_access_count=led["outcome_access_count"], pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                   classification_census=cls, verdict=verdict, status="COMPLETE — family SEARCH-EXPOSED"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"classification: {cls} · {verdict}")
    return summ


def report(write: bool = True) -> str:
    runs = [r for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OUT_*")), reverse=True)
            if RunSpace.is_current(r, "results.json")]
    if not runs:
        raise HardStop("no completed outcome run")
    run = runs[0]; res = json.load(open(os.path.join(run, "results.json")))
    summ = json.load(open(os.path.join(run, "summary.json")))
    reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"))); cen = reg["census"]
    f = lambda v, d=2: "—" if v is None else f"{float(v):+.{d}f}"
    L = [f"# SWALLOW_DIR_V1 — first outcome ({summ['run_id']})\n",
         f"Seal `{summ['registry_sha256_16']}` · X `{reg['x_run']}` · k = {summ['k']} · control "
         f"`{summ['control']['run_id']}` · path-sim `{summ['pathsim_src_sha256_16']}` · access "
         f"{summ['outcome_access_count']}\n",
         f"Prediction: {reg['signal']['prediction']}\n",
         f"{cen['rows']:,} swallow bars · {cen['tickers']:,} tickers · {cen['days']:,} days · "
         f"per year {cen['rows_per_year']} · doji excluded {cen['doji_in_X']:,}\n",
         f"Control coverage: {summ['control_coverage']}\n",
         "## Deciding table\n",
         "| cell | trades | MINE edge | win% | yrs+ | worst | raw | DSR | VERIFY edge | win% | worst | raw | class |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for r in res:
        m, v = r["mine"], r["verify"]
        L.append(f"| {r['claim_id']} | {r.get('direct_selected_n', 0):,} | {f(m['median_edge'])} | {f(m['day_win'],1)} | "
                 f"{m['positive_years']}/{m['years_counted']} | {f(m['worst_year'])} | {f(m.get('raw_median'))} | "
                 f"{f(r.get('dsr'))} | {f(v['median_edge'])} | {f(v['day_win'],1)} | {f(v['worst_year'])} | "
                 f"{f(v.get('raw_median'))} | {r['classification']} |")
    L.append("\n## The registered test — DN minus UP\n")
    L.append("| shape | MINE up | MINE dn | MINE dn−up | VERIFY up | VERIFY dn | VERIFY dn−up | same sign |")
    L.append("|---|---:|---:|---:|---:|---:|---:|---|")
    for lab, g in summ["direction_gaps"].items():
        L.append(f"| {lab} | {f(g['mine_up'])} | {f(g['mine_dn'])} | **{f(g['mine'])}** | {f(g['verify_up'])} | "
                 f"{f(g['verify_dn'])} | **{f(g['verify'])}** | {g['same_sign']} |")
    L.append(f"\n**verdict: {summ['verdict']}**\n")
    L.append(f"RSI by cell: {json.dumps(cen['rsi_by_cell'])}\n")
    L.append(f"Gates: {json.dumps(reg['gates'])}\n")
    txt = "\n".join(L)
    if write:
        open(os.path.join(FAMILY_DIR, "CHECKPOINT_FIRST_OUTCOME.md"), "w").write(txt)
    return txt


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items()
                                       if k in ("sealed_at", "registry_sha256_16", "k", "x_run", "x_sha256_16")}, indent=1)),
     "outcome": outcome,
     "report": lambda: print(report())}[cmd]()
