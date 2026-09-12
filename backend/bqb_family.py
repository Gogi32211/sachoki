"""BQB_V1 — Burst → Quiet pullback (no supply) → Burst.  Separate research family (user-approved 5-line plan, 2026-09-07).

The pattern read off the 260907 breadth labels on AMD / RGTI, fixed BEFORE the census so nothing could tune it
(thresholds identical to the 260907_BQB Pine script):
  B1  burst           T day (close > open), close > close[1], daily RVOL ≥ 2.0
  Q   quiet pullback  a later day inside the window with breadth ≤ 35 %, RVOL ≤ 1.0, close > B1 low;
                      the FIRST such day is the Q-entry
  B2  new burst       T day, breadth ≥ 50 %, RVOL ≥ 1.5, close > B1 high, after ≥ 1 quiet day
  cancel              supply day (Z day with breadth ≥ 50 %) · close ≤ B1 low · window > 8 bars · a new B1
                      overrides the armed pattern; a B2 becomes the next B1 (chain)
  breadth = same-slot 15m breadth from the INTRADAY_BREADTH_TRANSITION_V1 X build (26-bar sessions with
  20-session slot history); RVOL = volume / median of the previous 20 daily volumes (canonical 1D).
  Eligibility on the event day: close ≥ 5, avg_vol_20d > 0, close × volume ≥ 3M. Entry D+1 open.

  Registry (k = 6): three entries × two contexts of the ORIGINATING B1 (closed above the previous 20 closes = NH,
  else OTHER):  B1-only (reference: buy the burst) · Q-entry (buy the first quiet day) · B2-entry (buy the confirmation).
  Control: OTHER B1-only direct trades entered on the same date (both contexts, one row per trade), ticker-exclusive,
  ≥ 20 — "does the quiet pullback beat simply buying the burst?". Day-clustered median; DSR at k = 6 on MINE.
  OOS reserved before any outcome: MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  Gates: the book standard (median>0 · day-win>50 · ≥3 positive years · worst ≥ −2 · DSR ≥ 0.95 · not THIN); stop rule:
  nothing passes AND replicates → NULL, closes; no threshold / window search; the Z-climax variant is a separate family.
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
from breadth_family import GATES, classify, _stats_window, _loo_by_ticker   # noqa: E402  (identical registered gates)

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/BQB_V1"
FAMILY = "BQB_V1"
IBT_DIR = "/Users/sachoki/MASSIVE_DATA/INTRADAY_BREADTH_TRANSITION_V1"
P = dict(b1_rvol=2.0, q_breadth=35.0, q_rvol=1.0, sup_breadth=50.0, b2_breadth=50.0, b2_rvol=1.5, window=8, hold="close_above_b1_low", chain=True)
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
K_EXPECTED = 6


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def state_machine(close, open_, high, low, rvol, breadth, newhigh):
    """One ticker, chronological arrays. Returns per-day event codes, ages and the originating B1's context."""
    n = len(close)
    ev = np.zeros(n, dtype="U8")          # B1 · Q · Qn · B2 · X_SUP · X_HOLD · X_WIN
    age_q = np.full(n, -1); age_b2 = np.full(n, -1); b1nh = np.zeros(n, bool)
    state = 0; b1 = -1; b1h = b1l = np.nan; qc = 0; nh = False
    for i in range(n):
        r, b = rvol[i], breadth[i]
        isT = close[i] > open_[i]; isZ = close[i] < open_[i]
        isB1 = isT and i > 0 and close[i] > close[i - 1] and np.isfinite(r) and r >= P["b1_rvol"]
        fired_b2 = False
        if state >= 1:
            age = i - b1
            holds = close[i] > b1l
            supply = isZ and np.isfinite(b) and b >= P["sup_breadth"]
            quiet = np.isfinite(b) and np.isfinite(r) and b <= P["q_breadth"] and r <= P["q_rvol"] and holds
            b2 = state == 2 and isT and np.isfinite(b) and np.isfinite(r) and b >= P["b2_breadth"] and r >= P["b2_rvol"] and close[i] > b1h
            if b2:
                ev[i] = "B2"; age_b2[i] = age; b1nh[i] = nh; fired_b2 = True
                if P["chain"]:
                    state = 1; b1 = i; b1h = high[i]; b1l = low[i]; qc = 0; nh = bool(newhigh[i])
                else:
                    state = 0
            elif supply:
                ev[i] = "X_SUP"; state = 0
            elif not holds:
                ev[i] = "X_HOLD"; state = 0
            elif age > P["window"]:
                ev[i] = "X_WIN"; state = 0
            elif quiet:
                qc += 1
                if qc == 1:
                    ev[i] = "Q"; age_q[i] = age; b1nh[i] = nh; state = 2
                else:
                    ev[i] = "Qn"
        if isB1 and not fired_b2:
            ev[i] = "B1"; b1nh[i] = bool(newhigh[i])
            state = 1; b1 = i; b1h = high[i]; b1l = low[i]; qc = 0; nh = bool(newhigh[i])
    return ev, age_q, age_b2, b1nh


def frame(log=print):
    cur = B.canonical_current()
    d = pd.read_parquet(cur["derived_parquet"], columns=["ticker", "session_date", "open", "high", "low", "close", "volume", "avg_vol_20d"])
    d = d.rename(columns={"session_date": "session"}); d["session"] = d["session"].astype(str)
    seal = json.load(open(os.path.join(IBT_DIR, "SEAL.json")))
    brp = os.path.join(IBT_DIR, "runs", seal["x_run"], "breadth_15m.parquet")
    br = pd.read_parquet(brp); br["session"] = br["session"].astype(str)
    full = (br["n_bars"] == 26) & (br["n_def"] == 26)
    br["breadth"] = np.where(full, 100.0 * br["n_above"] / 26.0, np.nan)
    d = d.merge(br[["ticker", "session", "breadth"]], on=["ticker", "session"], how="left").sort_values(["ticker", "session"]).reset_index(drop=True)
    g = d.groupby("ticker", sort=False)
    d["rvol"] = d["volume"] / g["volume"].transform(lambda s: s.shift(1).rolling(20).median())
    d["hi20_prev"] = g["close"].transform(lambda s: s.shift(1).rolling(20).max())
    d["newhigh"] = d["close"] > d["hi20_prev"]
    d["eligible"] = (d["close"] >= 5) & (d["avg_vol_20d"] > 0) & (d["close"] * d["volume"] >= 3_000_000)
    evs, aq, ab, nh = [], [], [], []
    for tk, gg in d.groupby("ticker", sort=False):
        e, q, b2, n = state_machine(gg["close"].to_numpy(float), gg["open"].to_numpy(float), gg["high"].to_numpy(float), gg["low"].to_numpy(float),
                                    gg["rvol"].to_numpy(float), gg["breadth"].to_numpy(float), gg["newhigh"].to_numpy(bool))
        evs.append(e); aq.append(q); ab.append(b2); nh.append(n)
    d["ev"] = np.concatenate(evs); d["age_q"] = np.concatenate(aq); d["age_b2"] = np.concatenate(ab); d["b1_nh"] = np.concatenate(nh)
    return d, cur, dict(ibt_x_run=seal["x_run"], parquet=brp, sha256_16=_dig(brp))


def cells() -> list[dict]:
    out = []
    for ent, desc in (("B1", "reference: D+1 open after the burst"), ("Q", "D+1 open after the first quiet day"), ("B2", "D+1 open after the confirmation burst")):
        for ctx in ("NH", "OTHER"):
            out.append(dict(claim_id=f"{ent}|{ctx}", cls=ent, entry=desc, context=ctx, kind="reference" if ent == "B1" else "candidate"))
    return out


def cell_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    m = (X["ev"].to_numpy(object) == c["cls"]) & X["eligible"].to_numpy(bool)
    nh = X["b1_nh"].to_numpy(bool)
    return m & (nh if c["context"] == "NH" else ~nh)


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("BQB_%Y%m%dT%H%M%SZ", time.gmtime()))
    t0 = time.time()
    d, cur, src = frame(log)
    X = d[d["eligible"] & d["ev"].isin(["B1", "Q", "B2"])].reset_index(drop=True)
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    cs = {c["claim_id"]: dict(TRUE=int(cell_mask(X, c).sum()), TRUE_mine=int((cell_mask(X, c) & X["in_mine"]).sum()),
                              TRUE_verify=int((cell_mask(X, c) & X["in_verify"]).sum())) for c in cells()}
    e_all = d[d["eligible"] & (d["ev"] != "")]
    rep = dict(run_id=run.run_id, family=FAMILY, params=P, oos=OOS, canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               breadth_source=src, census=dict(rows=int(len(d)), eligible_event_days=e_all["ev"].value_counts().to_dict(), cells=cs,
                                               median_age_Q=float(X.loc[X.ev == "Q", "age_q"].median()), median_age_B2=float(X.loc[X.ev == "B2", "age_b2"].median())),
               x_sha256_16=_dig(xp), elapsed_s=round(time.time() - t0))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    log(f"X {len(X):,} event rows; cells " + " · ".join(f"{k} {v['TRUE_mine']}/{v['TRUE_verify']}" for k, v in cs.items()))
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "BQB_*")), reverse=True):
        if RunSpace.is_current(r, "X.parquet"):
            return r
    return None


def registry(rep: dict) -> dict:
    cs = cells()
    if len(cs) != K_EXPECTED:
        raise HardStop("k != 6")
    return dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="DRAFT_PRE_SEAL",
                provenance=["USER_APPROVED_5_LINE_PLAN_2026-09-07", "PRE_OUTCOME", "PRE_PATHSIM", "SEPARATE_FAMILY_FROM_IBT_OVD_IEB"],
                pattern=P, feature=dict(breadth="same-slot 15m breadth (IBT X build)", rvol="volume / median(prev 20 daily volumes)",
                                        context="NH = the originating B1 closed above the previous 20 closes; OTHER otherwise",
                                        known_at="16:00 NY of the event day", entry="D+1 open"),
                same_day_control="median of OTHER B1-only direct trades entered on the same date (both contexts, one row per (ticker, entry date)), "
                                 "ticker-exclusive, >= 20 others else UNAVAILABLE; edge = ret - control; day median; cell = median over days",
                estimand="direct sacred edge_replay._pathsim per registered cell mask, 5-bar same-ticker cooldown INCLUDED (never partitioned); "
                         "mode trail, atr_k 12, maxh 60, slip 0.0015; PRICE-RETURN paths (DIVIDENDS_NOT_ADJUSTED)",
                oos=OOS, oos_rule="MINE decides survivors with the mine gates; VERIFY is evaluated for mine survivors only; no cell is selected on VERIFY",
                gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="DSR with the cell's MINE day-edge series; trial family = the 6 cells' MINE day-edge Sharpes; n_trials = 6"),
                cells=cs, x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"], breadth_source=rep["breadth_source"],
                stop_rule="if no candidate cell passes the mine gates AND replicates, the family is NULL and closes; no threshold / window search follows; "
                          "the Z-climax variant is a separate family")


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
    reg = registry(rep); reg["status"] = "SEALED"; reg["sealed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED, x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"],
             build_report_sha256_16=_dig(os.path.join(run, "build_report.json")), canonical_1d=rep["canonical_1d"], breadth_source=rep["breadth_source"],
             oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(bqb_family=_dig(__file__), breadth_family=_dig(os.path.join(HERE, "breadth_family.py")), ovd_outcome_v1=_dig(O.__file__),
                       ovd_outcome_direct_v1=_dig(OD.__file__), edge_replay=_dig(os.path.join(HERE, "edge_replay.py"))),
             outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "bqb")
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
    parts = [trades[k] for k in trades if k.startswith("B1|") and len(trades[k])]
    pool = pd.concat(parts, ignore_index=True).drop_duplicates(["ticker", "date_in"]) if parts else pd.DataFrame(columns=["ticker", "date_in", "ret"])
    results, mine_series = [], {}
    for cid, tr in trades.items():
        obs = tr[tr["ret"].notna()].copy()
        if len(obs):
            ctrl, noth = _loo_by_ticker(pool, obs)
            obs["control"] = ctrl; obs["n_others"] = noth
            obs["edge"] = np.where(obs["n_others"] >= O.MIN_CONTROL, obs["ret"] - obs["control"], np.nan)
        mine = _stats_window(obs, *OOS["MINE"]); ver = _stats_window(obs, *OOS["VERIFY"])
        results.append(dict(claim_id=cid, cls=cid.split("|")[0], in_k=True, **{k: v for k, v in cons[cid].items()},
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
        r["classification"] = classify(r["mine"], r["verify"], r["dsr"], r["dsr_neg"]) if r["cls"] != "B1" else "REFERENCE"
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
                   rule="every later idea born from these outcomes is POST_EXPOSURE_HYPOTHESIS; no threshold / window search follows"),
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
