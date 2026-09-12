"""GATE_QUIET_GEM1_V1 — does a QUIET signal day (same-slot 15m breadth ≤ 35 %) improve the validated GEM1 edge?
Separate research family (user: "Seamowme", 2026-09-07). k = 2.

  Base edge   GEM1 = T1 capitulation-bounce, the engine's own registered mask `E_t1capbounce` (edge_replay._frame,
              60 months, dv floor 3M) — NEVER re-implemented here; signal keys exported X-only by the frame job.
  Gate        breadth(D) = share of the signal day's 26 15m bars above their same-slot 20-session median (IBT X build).
              QUIET = breadth ≤ 35 % · BROAD = breadth > 35 %. (RVOL ≤ 1 is NOT part of this gate: GEM1 requires the
              WLNBB B volume bucket, so a low-RVOL condition would empty the cell by construction.)
  Cells       GEM1|QUIET · GEM1|BROAD (k = 2). Reference row (not in k): GEM1|ALL (breadth defined).
  Control     the engine's own book control (feedback-day-clustered-accounting): every 40th bar with close ≥ $21 per
              ticker, same direct _pathsim; per entry date the control-day median; edge = trade − that median; need
              ≥ 20 control trades on the date. Day-clustered median; DSR at k = 2 on MINE.
  OOS         MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates (reserved pre-outcome).
  Gates       the book standard (breadth_family.GATES). Stop rule: nothing passes AND replicates → NULL; no threshold
              search on the 35 % gate; T6 buy-dip is not a registered engine mask and is out of scope.
"""
from __future__ import annotations
import os, sys, json, time, hashlib, glob
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from ovd_build import HardStop, RunSpace                                 # noqa: E402
import ovd_build as B                                                    # noqa: E402
import ovd_outcome_v1 as O                                               # noqa: E402
import ovd_outcome_direct_v1 as OD                                       # noqa: E402
from breadth_family import GATES, classify, _stats_window                # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/GATE_QUIET_GEM1_V1"
FAMILY = "GATE_QUIET_GEM1_V1"
IBT_DIR = "/Users/sachoki/MASSIVE_DATA/INTRADAY_BREADTH_TRANSITION_V1"
QUIET_MAX = 35.0
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
K_EXPECTED = 2


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def cells():
    return [dict(claim_id="GEM1|QUIET", cls="GEM1", gate="QUIET", rule=f"breadth <= {QUIET_MAX}"),
            dict(claim_id="GEM1|BROAD", cls="GEM1", gate="BROAD", rule=f"breadth > {QUIET_MAX}")]


def cell_mask(X, c):
    b = X["breadth"].to_numpy(float)
    return np.isfinite(b) & ((b <= QUIET_MAX) if c["gate"] == "QUIET" else (b > QUIET_MAX))


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("GQ_%Y%m%dT%H%M%SZ", time.gmtime()))
    sigp = os.path.join(FAMILY_DIR, "gem1_signals_engine.parquet"); ctlp = os.path.join(FAMILY_DIR, "engine_control_keys.parquet")
    sig = pd.read_parquet(sigp); ctl = pd.read_parquet(ctlp)
    seal = json.load(open(os.path.join(IBT_DIR, "SEAL.json")))
    brp = os.path.join(IBT_DIR, "runs", seal["x_run"], "breadth_15m.parquet")
    br = pd.read_parquet(brp); br["session"] = br["session"].astype(str)
    full = (br["n_bars"] == 26) & (br["n_def"] == 26)
    br["breadth"] = np.where(full, 100.0 * br["n_above"] / 26.0, np.nan)
    X = sig.merge(br[["ticker", "session", "breadth"]], on=["ticker", "session"], how="left")
    # restrict to the canonical window (the engine frame may reach past the canonical authority's last day)
    X = X[(X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["VERIFY"][1])].reset_index(drop=True)
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    ctl = ctl[(ctl["session"] >= OOS["MINE"][0]) & (ctl["session"] <= OOS["VERIFY"][1])].reset_index(drop=True)
    cp = os.path.join(run.dir, "control_keys.parquet"); ctl.to_parquet(cp, index=False)
    cs = {c["claim_id"]: dict(TRUE=int(cell_mask(X, c).sum()), TRUE_mine=int((cell_mask(X, c) & X["in_mine"]).sum()),
                              TRUE_verify=int((cell_mask(X, c) & X["in_verify"]).sum())) for c in cells()}
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, quiet_max=QUIET_MAX, canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               sources=dict(gem1_signals=dict(path=sigp, sha256_16=_dig(sigp), rows=int(len(sig))), control_keys=dict(path=ctlp, sha256_16=_dig(ctlp), rows=int(len(ctl))),
                            breadth=dict(ibt_x_run=seal["x_run"], parquet=brp, sha256_16=_dig(brp))),
               census=dict(gem1_rows=int(len(X)), breadth_defined=int(X["breadth"].notna().sum()), distinct_dates=int(X["session"].nunique()),
                           breadth_quantiles=[round(float(q), 1) for q in np.nanquantile(X["breadth"].to_numpy(float), [0.1, 0.25, 0.5, 0.75, 0.9])], cells=cs),
               x_sha256_16=_dig(xp), control_sha256_16=_dig(cp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    log(f"GEM1 rows {len(X):,} (breadth defined {rep['census']['breadth_defined']:,}); cells " + " · ".join(f"{k} {v['TRUE_mine']}/{v['TRUE_verify']}" for k, v in cs.items()))
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "GQ_*")), reverse=True):
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
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-07 (\"Seamowme\")", "PRE_OUTCOME", "PRE_PATHSIM", "GATE_ON_A_VALIDATED_EDGE"],
               base_edge="GEM1 = engine mask E_t1capbounce (edge_replay._frame(60, 3M)); not re-implemented",
               gate=dict(feature="same-slot 15m breadth of the signal day (IBT X build)", quiet=f"<= {QUIET_MAX}", broad=f"> {QUIET_MAX}",
                         note="RVOL not in the gate: GEM1 requires the WLNBB B bucket"),
               control="engine book control: every 40th bar with close >= 21 per ticker, same direct _pathsim; per entry date the control-day median (>= 20); "
                       "edge = trade - control median; day-clustered median",
               estimand="direct sacred edge_replay._pathsim per registered mask, 5-bar cooldown INCLUDED; trail atr_k 12, maxh 60, slip 0.0015; price-return paths",
               oos=OOS, gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="DSR with the 2 cells' MINE day-edge Sharpes; n_trials = 2"),
               cells=cells(), x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], control_sha256_16=rep["control_sha256_16"],
               canonical_1d=rep["canonical_1d"], sources=rep["sources"],
               stop_rule="nothing passes AND replicates -> NULL; no threshold search on the gate; T6 buy-dip out of scope (not a registered mask)")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED, x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"],
             control_sha256_16=rep["control_sha256_16"], canonical_1d=rep["canonical_1d"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(gate_quiet_family=_dig(__file__), edge_replay=_dig(os.path.join(HERE, "edge_replay.py")), ovd_outcome_v1=_dig(O.__file__),
                       ovd_outcome_direct_v1=_dig(OD.__file__)), outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


def _day_control(ctrl: pd.DataFrame, obs: pd.DataFrame):
    med = ctrl.groupby("date_in")["ret"].median(); cnt = ctrl.groupby("date_in")["ret"].size()
    c = obs["date_in"].map(med).to_numpy(float); n = obs["date_in"].map(cnt).fillna(0).to_numpy(int)
    return c, n


def outcome(log=print) -> dict:
    sp = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(sp):
        raise HardStop("not sealed")
    s = json.load(open(sp)); reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")))
    if _dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")) != s["registry_sha256_16"] or len(reg["cells"]) != s["k"]:
        raise HardStop("registry digest / k mismatch")
    run_x = os.path.join(FAMILY_DIR, "runs", s["x_run"])
    if _dig(os.path.join(run_x, "X.parquet")) != s["x_sha256_16"] or _dig(os.path.join(run_x, "control_keys.parquet")) != s["control_sha256_16"]:
        raise HardStop("X / control mismatch")
    cur = B.canonical_current()
    if cur["derived_sha256_16"] != s["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical changed")
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "gq")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), seal_registry=s["registry_sha256_16"]))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")
    X = pd.read_parquet(os.path.join(run_x, "X.parquet")); ctl = pd.read_parquet(os.path.join(run_x, "control_keys.parquet"))
    frames = OD.load_frames(cur["derived_parquet"], pd.concat([X["ticker"], ctl["ticker"]]).unique())
    trades, cons = {}, {}
    for c in reg["cells"] + [dict(claim_id="GEM1|ALL", gate="ALL")]:
        m = cell_mask(X, c) if c["gate"] != "ALL" else X["breadth"].notna().to_numpy()
        tr, cn = OD.direct_trades(ps, frames, X.loc[m, ["ticker", "session"]])
        tr["cell_id"] = c["claim_id"]; trades[c["claim_id"]] = tr; cons[c["claim_id"]] = cn
        log(f"  {c['claim_id']}: signal {cn.get('signal_true_n', '?')} → trades {len(tr)}")
    ctr, ccn = OD.direct_trades(ps, frames, ctl[["ticker", "session"]])
    cons["CONTROL"] = ccn; log(f"  control: {ccn.get('signal_true_n', '?')} → trades {len(ctr)}")
    run.write_atomic("selection_conservation.json", cons)
    pd.concat([t for t in trades.values() if len(t)] + [ctr.assign(cell_id="CONTROL")], ignore_index=True).to_parquet(os.path.join(run.dir, "direct_trades.parquet"), index=False)
    ctr = ctr[ctr["ret"].notna()]
    results, mine_series = [], {}
    for cid, tr in trades.items():
        obs = tr[tr["ret"].notna()].copy()
        if len(obs):
            c, n = _day_control(ctr, obs)
            obs["control"] = c; obs["n_others"] = n
            obs["edge"] = np.where(obs["n_others"] >= O.MIN_CONTROL, obs["ret"] - obs["control"], np.nan)
        mine = _stats_window(obs, *OOS["MINE"]); ver = _stats_window(obs, *OOS["VERIFY"])
        results.append(dict(claim_id=cid, in_k=cid != "GEM1|ALL", **{k: v for k, v in cons[cid].items()},
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
        r["classification"] = classify(r["mine"], r["verify"], r["dsr"], r["dsr_neg"]) if r["in_k"] else "REFERENCE"
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
                   pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, classification_census=cls_census, status="COMPLETE — family SEARCH-EXPOSED"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"classification: {cls_census}")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build, "seal": lambda: print(json.dumps({k: v for k, v in seal().items() if k in ("sealed_at", "registry_sha256_16", "k", "x_run", "x_sha256_16")}, indent=1)),
     "outcome": outcome}[cmd]()
