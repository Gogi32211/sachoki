"""SHAPE_GATE_V1 — does a shape CLUSTER in the trailing window damage a book edge?
User-approved 2026-09-10 ("es gavzomot"). k = 5.

  Question SHAPE_CLUSTER_V1 showed the shapes are worthless as an entry and that piling them
           up makes things monotonically WORSE (NO_CLUSTER -0.13 -> BOTH -0.68 in MINE). That
           result was about the shape bars themselves. This asks the DIFFERENT question the
           user actually cares about: when one of the book's own validated edges fires, does a
           shape cluster in the bars leading up to it damage that edge? If compression and
           thinning are what the shapes measure, a good edge fired into a compressed tape
           should pay less — which would make this a SUPPRESSOR, the way `sig_conso`'s absence
           became one. That is a real, buildable thing; an entry signal it is not.

  Population The union of SEVEN book edge masks, FIXED IN ADVANCE and NOT chosen for this study
           — this is exactly the OVL_MASKS list registered in MOTHER_V1's sealed registry on
           2026-09-10, before any of these results existed:
               E_t1capbounce · E_qzcapit · E_washout · E_zoneretest · E_coilfloor · E_spring
               · E_engulfabs
           All are BUILT reversal/absorption edges, so the mechanism claim ("compression hurts
           a reversal") applies to every one of them. POOLED: the claim is universal, so the
           test is pooled and per-edge numbers are descriptive only, never cells.

  Cells    A DOSE LADDER over the same 10-bar trailing window the user's Pine uses, ending ON
           the edge-fire bar (causal — only bars that had already closed):
               EDGE|ALL         the reference population
               EDGE|CLEAN       zero shape bars in the window
               EDGE|SHAPE_LOW   1-3 shape bars, neither cluster mark
               EDGE|CLUSTER     exactly one of the marks (fam >= 3 XOR bars >= 4)
               EDGE|BOTH        both marks (the 🎯🔁 case)
           CLEAN / SHAPE_LOW / CLUSTER / BOTH are an EXACT PARTITION of ALL.  k = 5.
           PRE-REGISTERED PREDICTION, so this can fail: if compression damages edges, the
           ladder must DECAY -- CLEAN > SHAPE_LOW > CLUSTER > BOTH. A flat ladder refutes it;
           an increasing ladder refutes it in the opposite direction.

  Prices   Shapes and the window on the CANONICAL 1D open/close over the full consecutive
           session sequence per ticker (identical code path to SHAPE_CLUSTER_V1); edge masks
           joined from the engine frame at (ticker, session). Liquidity filter on the fire bar.
  Control  CONTROL_KEYS v2 (de-phased). Per entry date the control-day median; edge = trade -
           that median; day-clustered.
  Estimand direct sacred `edge_replay._pathsim` over the registered mask, engine cooldown included.
  OOS      MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  Stop     One outcome run. No cell re-cut, no window search (10 / 3 / 4 are the user's shipped
           Pine values, fixed). A SUPPRESSOR is claimed only if the ladder decays in MINE AND
           the CLEAN-minus-BOTH gap replicates in VERIFY; otherwise NULL.

  DECLARED PRIOR (recorded before the outcome): better than the last one, but not strong. The
  monotone decay in SHAPE_CLUSTER_V1 is a real structure and the mechanism is coherent. Against
  it: that decay did NOT replicate in VERIFY (BOTH went +0.09 there), and the shape overlap with
  the capitulation family measured ON the fire bar was ~0, so the trailing-window overlap may
  still leave thin cells. Base rates are declared before sealing and a thin cell is reported,
  never trimmed.
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
from shape_cluster_family import shape_flags, SHAPES, CL_LOOK, CL_MIN_FAM, CL_MIN_BARS  # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/SHAPE_GATE_V1"
FAMILY = "SHAPE_GATE_V1"
DB = os.path.join(os.path.dirname(HERE), "data", "studio_analytics.duckdb")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED = 5
# FIXED IN ADVANCE — this is MOTHER_V1's registered OVL_MASKS list, sealed 2026-09-10 before
# any shape result existed. Not selected for this study, not re-cut here.
EDGE_MASKS = ["E_t1capbounce", "E_qzcapit", "E_washout", "E_zoneretest", "E_coilfloor",
              "E_spring", "E_engulfabs"]
RSI_BANDS = ((-1e9, 35.0, "<35"), (35.0, 50.0, "35-50"), (50.0, 60.0, "50-60"), (60.0, 1e9, ">=60"))


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def cells():
    return [
        dict(claim_id="EDGE|ALL", kind="all", rule="any of the 7 registered book edges fires (reference)"),
        dict(claim_id="EDGE|CLEAN", kind="clean", rule=f"0 shape bars in the trailing {CL_LOOK}-bar window"),
        dict(claim_id="EDGE|SHAPE_LOW", kind="low",
             rule=f"1-3 shape bars and fam < {CL_MIN_FAM} (neither cluster mark)"),
        dict(claim_id="EDGE|CLUSTER", kind="one",
             rule=f"exactly one mark: (fam >= {CL_MIN_FAM}) XOR (bars >= {CL_MIN_BARS})"),
        dict(claim_id="EDGE|BOTH", kind="both",
             rule=f"both marks: fam >= {CL_MIN_FAM} AND bars >= {CL_MIN_BARS} (the 🎯🔁 case)"),
    ]


def cell_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    d = X["cl_by_fam"].to_numpy(bool)
    b = X["cl_by_den"].to_numpy(bool)
    n = X["cl_bars"].to_numpy(float)
    k = c["kind"]
    if k == "all":
        return np.ones(len(X), bool)
    if k == "clean":
        return (n == 0) & ~d & ~b
    if k == "low":
        return (n >= 1) & (n <= 3) & ~d & ~b
    if k == "one":
        return (d ^ b)
    return d & b


def _band(v: np.ndarray) -> np.ndarray:
    out = np.full(len(v), "", dtype=object)
    for lo, hi, lab in RSI_BANDS:
        out[(v >= lo) & (v < hi)] = lab
    return out


def _engine_edges(log=print):
    """Every (ticker, session) where one of the registered edge masks fires."""
    import edge_replay as E
    t0 = time.time(); grp, as_of = E._frame(60, float(DV_FLOOR))
    parts = []
    for tk, g in grp.items():
        cols = [x for x in EDGE_MASKS if x in g.columns]
        if not cols:
            continue
        sub = g[cols].fillna(False).astype(bool)
        hit = sub.any(axis=1).to_numpy()
        if not hit.any():
            continue
        p = g.loc[hit, ["date"]].copy()
        for c in EDGE_MASKS:
            p[c] = sub[c].to_numpy()[hit] if c in cols else False
        p["ticker"] = tk
        parts.append(p)
    st = pd.concat(parts, ignore_index=True)
    st["session"] = st["date"].astype(str).str[:10]; st = st.drop(columns=["date"])
    missing = [x for x in EDGE_MASKS if x not in set().union(*[set(p.columns) for p in parts])]
    log(f"  engine frame: {len(grp):,} tickers · as_of {as_of} · edge fires {len(st):,}"
        + (f" · MISSING MASKS {missing}" if missing else ""))
    if missing:
        raise HardStop(f"registered edge masks absent from the frame: {missing}")
    try:
        E._CACHE.clear()
    except Exception:
        pass
    del grp, parts; gc.collect()
    return st, str(as_of)


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("SG_%Y%m%dT%H%M%SZ", time.gmtime()))

    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "open", "close", "volume", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    px = px.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    px = shape_flags(px)          # identical code path to SHAPE_CLUSTER_V1
    log(f"  canonical rows {len(px):,} · any_shape {int(px['any_shape'].sum()):,}")

    # keep the WHOLE frame's cluster state — an edge fire needs it whether or not a shape fired
    ctx = px.loc[px["win_full"] & (px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR)
                 & (px["avg_vol_20d"] > 0)
                 & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1]),
                 ["ticker", "session", "close", "any_shape", "cl_bars", "cl_fam",
                  "cl_by_fam", "cl_by_den"]].reset_index(drop=True)
    log(f"  liquid bars with a full {CL_LOOK}-bar window: {len(ctx):,}")
    del px; gc.collect()

    eng, as_of = _engine_edges(log)
    X = eng.merge(ctx, on=["ticker", "session"], how="inner").reset_index(drop=True)
    log(f"  edge fires with a usable window: {len(X):,} of {len(eng):,}")
    del eng, ctx; gc.collect()

    import duckdb
    con = duckdb.connect(DB, read_only=True)
    try:
        con.register("keys", X[["ticker", "session"]])
        st = con.execute("""
            WITH r AS (SELECT b.ticker, CAST(b.date AS VARCHAR) AS session, b.rsi_14,
                              row_number() OVER (PARTITION BY b.ticker, b.date ORDER BY b.universe) rn
                       FROM bars b JOIN keys k ON k.ticker = b.ticker AND CAST(b.date AS VARCHAR) = k.session
                       WHERE b.universe <> 'index')
            SELECT * EXCLUDE (rn) FROM r WHERE rn = 1""").fetchdf()
    finally:
        con.close()
    st["session"] = st["session"].str[:10]
    X = X.merge(st, on=["ticker", "session"], how="left")
    X["rsi_band"] = _band(pd.to_numeric(X["rsi_14"], errors="coerce").to_numpy(float))
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)

    cs, covered = {}, np.zeros(len(X), bool)
    for c in cells():
        m = cell_mask(X, c)
        if c["kind"] != "all":
            covered |= m
        cs[c["claim_id"]] = dict(TRUE=int(m.sum()), TRUE_mine=int((m & X["in_mine"]).sum()),
                                 TRUE_verify=int((m & X["in_verify"]).sum()))
    if not covered.all():
        raise HardStop(f"cells do not partition the population: {int((~covered).sum())} rows uncovered")
    # descriptive only, never cells
    per_edge = {e: dict(n=int(X[e].sum()),
                        both=int((X[e] & X["cl_by_fam"] & X["cl_by_den"]).sum()),
                        clean=int((X[e] & (X["cl_bars"] == 0)).sum())) for e in EDGE_MASKS}
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, px_min=PX_MIN, dv_floor=DV_FLOOR,
               signal=dict(name="SHAPE_GATE",
                           question="does a shape cluster in the trailing window damage a book edge?",
                           edges=EDGE_MASKS,
                           edge_set_provenance="MOTHER_V1 sealed registry OVL_MASKS, fixed 2026-09-10 before "
                                               "any shape outcome existed; not selected for this study",
                           window=dict(lookback=CL_LOOK, min_families=CL_MIN_FAM, min_bars=CL_MIN_BARS,
                                       ends_on="the edge-fire bar (causal)"),
                           prediction="if compression damages edges the ladder DECAYS: CLEAN > SHAPE_LOW > "
                                      "CLUSTER > BOTH; flat or increasing refutes it",
                           prices="canonical 1D open/close, full consecutive session sequence; identical "
                                  "shape_flags code path as SHAPE_CLUSTER_V1"),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               sources=dict(studio_db=DB, engine_frame_as_of=as_of),
               census=dict(rows=int(len(X)), tickers=int(X["ticker"].nunique()), days=int(X["session"].nunique()),
                           rows_per_year={int(y): int(n) for y, n in X.groupby(X["session"].str[:4]).size().items()},
                           cl_bars_dist={int(k): int(v) for k, v in X["cl_bars"].value_counts().sort_index().items()},
                           cl_fam_dist={int(k): int(v) for k, v in X["cl_fam"].value_counts().sort_index().items()},
                           any_shape_share=round(float(X["any_shape"].mean()), 4),
                           rsi_band_shares=X["rsi_band"].value_counts(normalize=True).round(4).to_dict(),
                           cells=cs, per_edge=per_edge),
               x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    log(f"X {len(X):,} edge fires · " + " · ".join(f"{k.split('|')[1]} {v['TRUE_mine']}/{v['TRUE_verify']}"
                                                   for k, v in cs.items()))
    log(f"  cl_bars dist {rep['census']['cl_bars_dist']}")
    log("  per edge (n · both · clean): " + " · ".join(
        f"{e[2:]} {v['n']:,}/{v['both']:,}/{v['clean']:,}" for e, v in per_edge.items()))
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "SG_*")), reverse=True):
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
               cl_bars_dist=cen["cl_bars_dist"], per_edge=cen["per_edge"],
               declared_prior="Better than SHAPE_CLUSTER_V1's but not strong. The monotone decay found there "
                              "is real and the mechanism is coherent, but that decay did NOT replicate in "
                              "VERIFY (BOTH went +0.09), and same-bar shape overlap with the capitulation "
                              "family was ~0, so trailing-window cells may still be thin.",
               note="CLEAN / SHAPE_LOW / CLUSTER / BOTH are an exact partition of ALL, so the cells are not "
                    "independent by construction; DSR over the 5 MINE day-edge Sharpes as declared. Thresholds "
                    "10 / 3 / 4 are the user's shipped Pine values, FIXED. Per-edge numbers are descriptive.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", known_limitations=lim,
               sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-10 (\"es gavzomot\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "EDGE SET FIXED IN ADVANCE — MOTHER_V1 registry OVL_MASKS, sealed before any shape result"],
               question=rep["signal"]["question"], signal=rep["signal"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}, universe <> index",
               control=f"CONTROL_KEYS v2 de-phased ({rep['control']['run_id']}); per entry date the control-day "
                       f"median (>= {O.MIN_CONTROL}); edge = trade - control median; day-clustered",
               estimand="direct sacred edge_replay._pathsim over the registered mask, engine cooldown INCLUDED",
               oos=OOS, gates=GATES,
               multiplicity=dict(k=K_EXPECTED, rule="DSR with the 5 cells' MINE day-edge Sharpes; n_trials = 5"),
               cells=cells(), descriptives="per-edge counts — never cells",
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], sources=rep["sources"], census=cen,
               stop_rule="SUPPRESSOR only if the ladder DECAYS in MINE and the CLEAN-minus-BOTH gap replicates "
                         "in VERIFY; otherwise NULL. One outcome run; no cell re-cut; no window search; BUILD "
                         "only on a second user OK")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(shape_gate_family=_dig(__file__),
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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "shape")
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
        log(f"  {r['claim_id']:18s} MINE med {m['median_edge']} win {m['day_win']} yrs {m['positive_years']}/{m['years_counted']} "
            f"worst {m['worst_year']} raw {m.get('raw_median')} | VERIFY med {v['median_edge']} win {v['day_win']} "
            f"worst {v['worst_year']} raw {v.get('raw_median')} | DSR {r['dsr']} | {r['classification']}")
    # the LADDER is the registered test — report it explicitly, computed from the same results
    order = ["EDGE|CLEAN", "EDGE|SHAPE_LOW", "EDGE|CLUSTER", "EDGE|BOTH"]
    lad = {w: [next((r[w]["median_edge"] for r in results if r["claim_id"] == c), None) for c in order]
           for w in ("mine", "verify")}
    def _dec(v):
        vv = [x for x in v if x is not None]
        return len(vv) == len(order) and all(vv[i] >= vv[i + 1] for i in range(len(vv) - 1))
    gap = {w: (None if lad[w][0] is None or lad[w][-1] is None else round(lad[w][0] - lad[w][-1], 3))
           for w in ("mine", "verify")}
    ladder = dict(order=order, mine=lad["mine"], verify=lad["verify"],
                  monotone_decay_mine=_dec(lad["mine"]), monotone_decay_verify=_dec(lad["verify"]),
                  clean_minus_both=gap,
                  verdict=("SUPPRESSOR_CANDIDATE" if _dec(lad["mine"]) and (gap["verify"] or 0) > 0
                           else "NOT_A_SUPPRESSOR"))
    log(f"  LADDER mine   {lad['mine']}  monotone={ladder['monotone_decay_mine']}")
    log(f"  LADDER verify {lad['verify']}  monotone={ladder['monotone_decay_verify']}")
    log(f"  CLEAN - BOTH  mine {gap['mine']} · verify {gap['verify']} -> {ladder['verdict']}")
    run.write_atomic("results.json", results)
    cls = {}
    for r in results:
        cls[r["classification"]] = cls.get(r["classification"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED, oos=OOS,
                gates=GATES, control=s["control"], control_coverage=cov, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                outcome_access_count=led["outcome_access_count"], classification_census=cls, ladder=ladder,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id,
                   seal_registry_sha256_16=s["registry_sha256_16"],
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")),
                   outcome_access_count=led["outcome_access_count"], pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                   classification_census=cls, ladder_verdict=ladder["verdict"],
                   status="COMPLETE — family SEARCH-EXPOSED"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"classification: {cls} · ladder: {ladder['verdict']}")
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
    L = [f"# SHAPE_GATE_V1 — first outcome ({summ['run_id']})\n",
         f"Seal `{summ['registry_sha256_16']}` · X `{reg['x_run']}` · k = {summ['k']} · control "
         f"`{summ['control']['run_id']}` · path-sim `{summ['pathsim_src_sha256_16']}` · access "
         f"{summ['outcome_access_count']}\n",
         f"Edges (fixed in advance): {', '.join(reg['signal']['edges'])}\n",
         f"Window: {json.dumps(reg['signal']['window'])}\n",
         f"Prediction: {reg['signal']['prediction']}\n",
         f"{cen['rows']:,} edge fires · {cen['tickers']:,} tickers · {cen['days']:,} days · per year {cen['rows_per_year']}\n",
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
    lad = summ["ladder"]
    L.append(f"\n## The registered ladder test\n")
    L.append(f"order: {' > '.join(lad['order'])}\n")
    L.append(f"- MINE   {lad['mine']} · monotone decay: **{lad['monotone_decay_mine']}**")
    L.append(f"- VERIFY {lad['verify']} · monotone decay: **{lad['monotone_decay_verify']}**")
    L.append(f"- CLEAN − BOTH: MINE {lad['clean_minus_both']['mine']} · VERIFY {lad['clean_minus_both']['verify']}")
    L.append(f"- **verdict: {lad['verdict']}**\n")
    L.append(f"Per-edge counts (n · both · clean), descriptive only: {json.dumps(cen['per_edge'])}\n")
    L.append(f"cl_bars distribution: {json.dumps(cen['cl_bars_dist'])}\n")
    L.append(f"RSI band shares: {json.dumps(cen['rsi_band_shares'])}\n")
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
