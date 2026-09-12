"""LSTUP_GATE_V1 — does a ⛔LST↑ in the trailing window damage a BOOK edge?
User-approved 2026-09-10 ("ki"). k = 4.

  THE QUESTION. SWALLOW_DIR_V1 found the one shape cell with two-window evidence: LST|UP —
  a GREEN bar whose body swallows both prior bodies — is -0.63 MINE / -0.47 VERIFY, 0 of 4
  positive years, worst -3.41, DSR_neg 0.998 over 29,911 trades, and the only shape cell that
  does not flip sign between windows. That says do not BUY it. It does NOT say anything about
  what happens to somebody ELSE's edge when one appeared in the run-up. This asks that.

  WHY IT IS NOT SHAPE_GATE_V1 AGAIN. That family asked whether ANY shape cluster damages an
  edge and answered NOT_A_SUPPRESSOR. The third cell here is what separates the two questions:
  SHAPE_no_LSTUP is "some other shape fired, but not a green swallow". If LSTUP and
  SHAPE_no_LSTUP come out the same, this is the already-refuted result wearing a new name and
  must be reported as such, not as a finding.

  Population The SAME seven book edges as SHAPE_GATE_V1 — MOTHER_V1's registered OVL_MASKS,
           fixed on 2026-09-10 before any shape outcome existed. The list is not re-picked.
  Cells    EDGE|ALL (reference) · EDGE|LSTUP (a LST-up in the trailing 10 bars) ·
           EDGE|SHAPE_no_LSTUP (a shape, but no LST-up) · EDGE|CLEAN (no shape at all). k = 4.
           The last three are an EXACT PARTITION of ALL, asserted in code.
  PRE-REGISTERED PREDICTION: CLEAN ~ SHAPE_no_LSTUP > LSTUP. Equal thirds refute the veto.
           LSTUP ~ SHAPE_no_LSTUP < CLEAN means it is shapes in general, which contradicts
           SHAPE_GATE_V1 and would need explaining rather than claiming.
  Control  CONTROL_KEYS v2 (de-phased); per entry date the control-day median; day-clustered.
  Estimand direct sacred `edge_replay._pathsim`, engine cooldown included.
  OOS      MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  Stop     the CLEAN-minus-LSTUP gap must carry the SAME SIGN in both windows; else NULL.

  DECLARED PRIOR: weak. SHAPE_GATE_V1 already answered the general form NOT_A_SUPPRESSOR, and
  in that family GEM1 was unmeasurable (256 fires, 3 with a cluster). What is new is only that
  LST-up is the one cell with real evidence about itself. Recorded before the outcome.
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

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/LSTUP_GATE_V1"
FAMILY = "LSTUP_GATE_V1"
DB = os.path.join(os.path.dirname(HERE), "data", "studio_analytics.duckdb")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED = 4
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
        dict(claim_id="EDGE|LSTUP", kind="lstup",
             rule=f"a display-priority LASTNEST with close > open in the trailing {CL_LOOK}-bar window"),
        dict(claim_id="EDGE|SHAPE_no_LSTUP", kind="shape_no",
             rule=f"some shape in the window but NO LST-up — separates 'a green swallow hurts' from "
                  f"'any shape hurts', which SHAPE_GATE_V1 already refuted"),
        dict(claim_id="EDGE|CLEAN", kind="clean", rule=f"no shape at all in the trailing {CL_LOOK} bars"),
    ]


def cell_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    lu = X["lstup_win"].to_numpy(bool)
    n = X["cl_bars"].to_numpy(float)
    k = c["kind"]
    if k == "all":
        return np.ones(len(X), bool)
    if k == "lstup":
        return lu
    if k == "shape_no":
        return (n > 0) & ~lu
    return (n == 0) & ~lu


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
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("LG_%Y%m%dT%H%M%SZ", time.gmtime()))

    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "open", "close", "volume", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    px = px.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    px = shape_flags(px)          # identical code path to SHAPE_CLUSTER_V1 and SWALLOW_DIR_V1
    # LST|UP is the DISPLAY-PRIORITY LASTNEST (MOTHER / COIL4 / MID / EXPAND / CONTRACT outrank it)
    # with close > open — exactly the cell SWALLOW_DIR_V1 measured. Reproduced here rather than
    # imported so that family's sealed code digest stays untouched; the parity test pins the chain.
    _hi = px["s_moth"] | px["s_coil"] | px["s_mid"] | px["s_exp"] | px["s_con"]
    px["lstup"] = (~_hi) & px["s_last"] & (px["close"] > px["open"])
    g = px.groupby("ticker", sort=False)
    px["lstup_win"] = (g["lstup"].rolling(CL_LOOK, min_periods=1).sum()
                       .reset_index(level=0, drop=True) > 0).fillna(False).to_numpy()
    log(f"  canonical rows {len(px):,} · any_shape {int(px['any_shape'].sum()):,} "
        f"· LST-up bars {int(px['lstup'].sum()):,} · bars with one in the last {CL_LOOK}: "
        f"{int(px['lstup_win'].sum()):,}")

    # keep the WHOLE frame's window state — an edge fire needs it whether or not a shape fired
    ctx = px.loc[px["win_full"] & (px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR)
                 & (px["avg_vol_20d"] > 0)
                 & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1]),
                 ["ticker", "session", "close", "any_shape", "cl_bars", "cl_fam",
                  "cl_by_fam", "cl_by_den", "lstup", "lstup_win"]].reset_index(drop=True)
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
                           question="does a LST-up (green swallow) in the trailing window damage a book edge, "
                                    "and is that different from any shape at all?",
                           edges=EDGE_MASKS,
                           edge_set_provenance="MOTHER_V1 sealed registry OVL_MASKS, fixed 2026-09-10 before "
                                               "any shape outcome existed; not selected for this study",
                           window=dict(lookback=CL_LOOK, min_families=CL_MIN_FAM, min_bars=CL_MIN_BARS,
                                       ends_on="the edge-fire bar (causal)"),
                           prediction="CLEAN ~ SHAPE_no_LSTUP > LSTUP. Equal thirds refute the veto. "
                                      "LSTUP ~ SHAPE_no_LSTUP < CLEAN would mean shapes in general, which "
                                      "contradicts SHAPE_GATE_V1 and needs explaining, not claiming.",
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
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "LG_*")), reverse=True):
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
               declared_prior="WEAK. SHAPE_GATE_V1 already answered the general form NOT_A_SUPPRESSOR, and "
                              "GEM1 was unmeasurable there (256 fires, 3 with a cluster). What is new is only "
                              "that LST-up is the ONE shape cell with two-window evidence about itself "
                              "(-0.63 MINE / -0.47 VERIFY, 0/4 years, DSR_neg 0.998; SWALLOW_DIR_V1).",
               note="LSTUP / SHAPE_no_LSTUP / CLEAN are an exact partition of ALL (asserted in code), so the "
                    "cells are not independent by construction; DSR over the 4 MINE day-edge Sharpes as "
                    "declared. SHAPE_no_LSTUP is the control that separates this question from SHAPE_GATE_V1 - "
                    "if it matches LSTUP, this is the already-refuted result under a new name and must be "
                    "reported as that. The 10-bar window is the user's shipped Pine value, FIXED.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", known_limitations=lim,
               sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-10 (\"ki\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "EDGE SET FIXED IN ADVANCE — MOTHER_V1 registry OVL_MASKS, sealed before any shape result"],
               question=rep["signal"]["question"], signal=rep["signal"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}, universe <> index",
               control=f"CONTROL_KEYS v2 de-phased ({rep['control']['run_id']}); per entry date the control-day "
                       f"median (>= {O.MIN_CONTROL}); edge = trade - control median; day-clustered",
               estimand="direct sacred edge_replay._pathsim over the registered mask, engine cooldown INCLUDED",
               oos=OOS, gates=GATES,
               multiplicity=dict(k=K_EXPECTED, rule="DSR with the 4 cells' MINE day-edge Sharpes; n_trials = 4"),
               cells=cells(), descriptives="per-edge counts — never cells",
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], sources=rep["sources"], census=cen,
               stop_rule="the CLEAN-minus-LSTUP gap must carry the SAME SIGN in both windows AND "
                         "SHAPE_no_LSTUP must NOT show the same gap; otherwise NULL. One outcome run; no cell "
                         "re-cut; no window search; BUILD only on a second user OK")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(lstup_gate_family=_dig(__file__),
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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "lstup")
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
    # the registered test: each shape cell against CLEAN, both windows. The SHAPE_no_LSTUP
    # column is the one that decides whether this is about LST-up or just about shapes.
    def _m(cid, w):
        return next((r[w]["median_edge"] for r in results if r["claim_id"] == cid), None)
    gaps = {}
    for cid in ("EDGE|LSTUP", "EDGE|SHAPE_no_LSTUP"):
        g = {}
        for w in ("mine", "verify"):
            x, c0 = _m(cid, w), _m("EDGE|CLEAN", w)
            g[w] = None if (x is None or c0 is None) else round(c0 - x, 3)   # CLEAN minus cell
            g[f"{w}_cell"], g[f"{w}_clean"] = x, c0
        g["same_sign"] = (g["mine"] is not None and g["verify"] is not None and g["mine"] * g["verify"] > 0)
        gaps[cid] = g
        log(f"  CLEAN - {cid}: MINE {g['mine']} · VERIFY {g['verify']} · same_sign={g['same_sign']}")
    lu, sn = gaps["EDGE|LSTUP"], gaps["EDGE|SHAPE_no_LSTUP"]
    if lu["same_sign"] and (lu["mine"] or 0) > 0 and not (sn["same_sign"] and (sn["mine"] or 0) > 0):
        verdict = "LSTUP_SUPPRESSOR_CANDIDATE"
    elif lu["same_sign"] and sn["same_sign"] and (lu["mine"] or 0) > 0 and (sn["mine"] or 0) > 0:
        verdict = "SHAPES_IN_GENERAL — contradicts SHAPE_GATE_V1, explain before claiming"
    else:
        verdict = "NOT_A_SUPPRESSOR"
    log(f"  VERDICT: {verdict}")

    run.write_atomic("results.json", results)
    cls = {}
    for r in results:
        cls[r["classification"]] = cls.get(r["classification"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED, oos=OOS,
                gates=GATES, control=s["control"], control_coverage=cov, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                outcome_access_count=led["outcome_access_count"], classification_census=cls, gaps_vs_clean=gaps, verdict=verdict,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id,
                   seal_registry_sha256_16=s["registry_sha256_16"],
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")),
                   outcome_access_count=led["outcome_access_count"], pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                   classification_census=cls, verdict=verdict,
                   status="COMPLETE — family SEARCH-EXPOSED"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"classification: {cls} · {verdict}")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items()
                                       if k in ("sealed_at", "registry_sha256_16", "k", "x_run", "x_sha256_16")}, indent=1)),
     "outcome": outcome}[cmd]()
