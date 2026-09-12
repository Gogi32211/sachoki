"""ADJACENT_TURN_V1 — one question, asked properly this time. User-approved 2026-09-11. k = 1.

  THE QUESTION. TURN_V1 failed its registered test, but one shape was the same in BOTH windows:
  a 🔺 markup bar sitting IMMEDIATELY after a bottom run beat a 🔺 with no bottom before it, while
  DELAYED turns (2-10 bars later) were negative in both.

      rung            no bottom   adjacent   2-3 bars   4-5 bars   6-10 bars
      MINE              -0.497     +0.437     -0.211     -0.389     -0.167
      VERIFY            +0.417     +0.765     -1.288     -1.389     -0.839

  That contrast was NOT the statistic TURN_V1 registered (it registered span = last rung minus
  first, which cannot see a middle peak), so it could not be claimed there — choosing a statistic
  after seeing the result is the thing sealing exists to prevent. It is claimed here instead, as
  its own sealed family with its own k, which is the honest route.

  Cells (k = 1 — the registered object is the CONTRAST, not the two levels):
    TURN|ADJACENT    a `cont` bar whose immediately preceding bar ended a bottom run of >= 2
                     bars of 🔻rev / 🌀shake
    TURN|NO_BOTTOM   a `cont` bar with NO such run anywhere in the previous 10 bars
  Registered statistic: ADJACENT minus NO_BOTTOM, day-clustered, in each window.
  PASS = the contrast keeps its SIGN in MINE and VERIFY *and* sits outside a size-matched placebo
  (p < 0.05) in BOTH. The placebo is not optional here: the two cells differ ~14x in size, which is
  exactly the configuration that manufactured a false "directional" verdict in UDN_CONFLICT_V1.

  NO SAMPLING. Earlier families thinned 1-in-12 to make whole-universe ladders runnable; this one
  is a single contrast, ADJACENT is the scarce side, and throwing away 11 of every 12 of its events
  would be self-defeating. Both cells use every qualifying bar.

  DIRECTION IS REGISTERED THIS TIME: ADJACENT > NO_BOTTOM. It was observed in both windows before
  this family existed, so pretending to be agnostic would be false. The honesty that matters here is
  different — this is a SECOND look at data whose shape I have already seen, so the placebo and the
  two-window requirement carry the whole burden, and a pass is weaker evidence than a first look
  would have been. That is stated in the registry, not buried.

  Instrument: CONTROL_KEYS v2 restricted to the family's tickers · day-clustered statistic ·
  `population_vs_control` reported · MINE 2021-09-07..2024-12-31, VERIFY 2025-01-01..2026-09-03 ·
  one outcome run, no re-cut of "adjacent", no widening to gap <= 2 after the fact.
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
from breadth_family import GATES, _stats_window                          # noqa: E402
from turn_family import _features, MAX_GAP, MIN_RUN, ANAT               # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/ADJACENT_TURN_V1"
FAMILY = "ADJACENT_TURN_V1"
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED, NSHUF, RNG_SEED = 1, 500, 20260911


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("AT_%Y%m%dT%H%M%SZ", time.gmtime()))
    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "close", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    liq = px[(px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
             & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1])][["ticker", "session"]]
    del px; gc.collect()
    feat = _features(log).rename(columns={"date": "session"})
    X0 = liq.merge(feat, on=["ticker", "session"], how="inner")
    g = X0["gap"].to_numpy()
    adj = (g == 1)
    nob = (g < 1) | (g > MAX_GAP)          # no qualifying run inside the window (gap -1 = none ever)
    X = pd.concat([
        X0.loc[adj, ["ticker", "session"]].assign(cell="TURN|ADJACENT"),
        X0.loc[nob, ["ticker", "session"]].assign(cell="TURN|NO_BOTTOM"),
    ], ignore_index=True).sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    cs = {c: dict(n=int((X["cell"] == c).sum()),
                  tickers=int(X.loc[X["cell"] == c, "ticker"].nunique()),
                  mine=int(((X["cell"] == c) & (X["session"] <= OOS["MINE"][1])).sum()),
                  verify=int(((X["cell"] == c) & (X["session"] >= OOS["VERIFY"][0])).sum()))
          for c in X["cell"].unique()}
    log(f"  liquid cont bars {len(X0):,} · " + " · ".join(
        f"{c} {v['n']:,} (mine {v['mine']:,} / verify {v['verify']:,})" for c, v in cs.items()))
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, n_shuffles=NSHUF, rng_seed=RNG_SEED,
               max_gap=MAX_GAP, min_run=MIN_RUN, sampling="NONE — every qualifying bar is used",
               signal=dict(name="ADJACENT_TURN",
                           question="is a markup bar immediately after a bottom run better than a "
                                    "markup bar with no bottom run before it?",
                           registered_statistic="ADJACENT minus NO_BOTTOM, day-clustered, per window",
                           registered_direction="ADJACENT > NO_BOTTOM",
                           second_look="YES — the shape was seen in TURN_V1's rung table before this "
                                       "family existed. The placebo and the two-window requirement "
                                       "carry the whole burden; a pass is weaker evidence than a "
                                       "first look would have been.",
                           source=ANAT, source_sha256_16=_dig(ANAT)),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               census=cs, x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "AT_*")), reverse=True):
        if RunSpace.is_current(r, "X.parquet"):
            return r
    return None


def seal() -> dict:
    if os.path.exists(os.path.join(FAMILY_DIR, "SEAL.json")):
        raise HardStop("SEAL.json exists")
    if any(k in f.lower() for _, _, fs in os.walk(FAMILY_DIR) for f in fs
           for k in ("outcome", "result", "pathsim")):
        raise HardStop("outcome-bearing file present")
    run = latest_run()
    if not run:
        raise HardStop("no COMPLETED X build")
    rep = json.load(open(os.path.join(run, "build_report.json")))
    if _dig(os.path.join(run, "X.parquet")) != rep["x_sha256_16"]:
        raise HardStop("X digest mismatch")
    ps, slip = O.sacred_pathsim()
    lim = dict(cell_counts=rep["census"],
               second_look_warning="This tests a shape ALREADY SEEN in TURN_V1's rung table. It is "
                                   "a second look at the same data, so a pass is weaker evidence "
                                   "than a first look. The alternative — claiming it inside TURN_V1 "
                                   "by recomputing a statistic after seeing the result — is worse, "
                                   "which is why it is here with its own k.",
               why_placebo_is_mandatory="The two cells differ roughly 14x in size. That exact "
                                        "configuration produced a false 'directional' verdict in "
                                        "UDN_CONFLICT_V1, where size-matched placebos spanned "
                                        "-0.07..+0.44 pp while the claim rested on -0.07.",
               population_caveat="`cont` is a MARKUP bar, so the population sits slightly below its "
                                 "own control (project_entry_timing: strength-chasing loses). Levels "
                                 "are read RELATIVE to each other, not absolutely.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED",
               known_limitations=lim, sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-11 (\"ki\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "SECOND LOOK — shape first seen in TURN_V1, declared as such"],
               question=rep["signal"]["question"], signal=rep["signal"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}",
               control=f"CONTROL_KEYS v2 ({rep['control']['run_id']}) restricted to this family's "
                       f"tickers; control-day median needs >= {O.MIN_CONTROL} trades",
               estimand="direct sacred edge_replay._pathsim, engine cooldown INCLUDED",
               oos=OOS, gates=GATES,
               multiplicity=dict(k=K_EXPECTED, rule="one registered contrast"),
               cells=[dict(claim_id="TURN|ADJACENT",
                           rule=f"cont bar whose previous bar ended a bottom run of >= {MIN_RUN}"),
                      dict(claim_id="TURN|NO_BOTTOM",
                           rule=f"cont bar with no such run in the previous {MAX_GAP} bars")],
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], census=rep["census"],
               stop_rule="PASS only if the ADJACENT-minus-NO_BOTTOM contrast keeps its sign in MINE "
                         "and VERIFY AND sits outside the size-matched placebo (p < 0.05) in BOTH. "
                         "One outcome run; no re-cut of 'adjacent'; no widening to gap <= 2.")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")
    json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
             slip=slip, n_shuffles=NSHUF, rng_seed=RNG_SEED,
             code=dict(adjacent_turn_family=_dig(__file__),
                       turn_family=_dig(os.path.join(HERE, "turn_family.py")),
                       anatomy_build=_dig(os.path.join(HERE, "anatomy_build.py")),
                       edge_replay=_dig(os.path.join(HERE, "edge_replay.py"))),
             outcome_access_count=0, rule="registry frozen; outcome is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


def _cell_day_edge(obs, cell, lo, hi):
    g = obs[(obs["cell"] == cell) & (obs["date_in"] >= lo) & (obs["date_in"] <= hi)]
    if not len(g):
        return np.nan, 0
    byday = g.groupby("date_in").apply(lambda d: d["ret"].median() - d["cmed"].iloc[0],
                                       include_groups=False)
    return (float(np.median(byday)) if len(byday) else np.nan), int(len(byday))


def outcome(log=print) -> dict:
    sp = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(sp):
        raise HardStop("not sealed")
    s = json.load(open(sp)); reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")))
    if _dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")) != s["registry_sha256_16"]:
        raise HardStop("registry digest mismatch")
    run_x = os.path.join(FAMILY_DIR, "runs", s["x_run"])
    if _dig(os.path.join(run_x, "X.parquet")) != s["x_sha256_16"]:
        raise HardStop("X mismatch")
    cur = B.canonical_current()
    if cur["derived_sha256_16"] != s["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical changed")
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "adjacent")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")

    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    ctl = CK.load(man); ctl["session"] = ctl["session"].astype(str).str[:10]
    ctl = ctl[(ctl["session"] >= OOS["MINE"][0]) & (ctl["session"] <= OOS["VERIFY"][1])]
    ctl = ctl[ctl["ticker"].isin(set(X["ticker"].unique()))].reset_index(drop=True)
    frames = OD.load_frames(cur["derived_parquet"], pd.concat([X["ticker"], ctl["ticker"]]).unique())
    ctr, _ = OD.direct_trades(ps, frames, ctl[["ticker", "session"]]); ctr = ctr[ctr["ret"].notna()]
    med = ctr.groupby("date_in")["ret"].median(); cnt = ctr.groupby("date_in")["ret"].size()
    tr, cons = OD.direct_trades(ps, frames, X[["ticker", "session"]])
    tr = tr.merge(X, on=["ticker", "session"], how="left")
    obs = tr[tr["ret"].notna()].copy()
    obs["cmed"] = obs["date_in"].map(med).to_numpy(float)
    obs["cn"] = obs["date_in"].map(cnt).fillna(0).to_numpy(int)
    obs = obs[obs["cn"] >= O.MIN_CONTROL].copy()
    del frames; gc.collect()
    log(f"  trades {len(tr):,} · usable {len(obs):,} · control {len(ctr):,}")

    real, days = {}, {}
    for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
        a, da = _cell_day_edge(obs, "TURN|ADJACENT", lo, hi)
        n, dn = _cell_day_edge(obs, "TURN|NO_BOTTOM", lo, hi)
        real[w] = dict(adjacent=None if np.isnan(a) else round(a, 3),
                       no_bottom=None if np.isnan(n) else round(n, 3),
                       contrast=None if (np.isnan(a) or np.isnan(n)) else round(a - n, 3))
        days[w] = dict(adjacent_days=da, no_bottom_days=dn)
    rng = np.random.default_rng(RNG_SEED)
    base = obs["cell"].to_numpy().copy()
    pl = {w: [] for w in ("mine", "verify")}
    for _ in range(NSHUF):
        obs["cell"] = rng.permutation(base)
        for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
            a, _ = _cell_day_edge(obs, "TURN|ADJACENT", lo, hi)
            n, _ = _cell_day_edge(obs, "TURN|NO_BOTTOM", lo, hi)
            pl[w].append(np.nan if (np.isnan(a) or np.isnan(n)) else a - n)
    obs["cell"] = base
    pv = {}
    for w in ("mine", "verify"):
        arr = np.asarray(pl[w], float); arr = arr[~np.isnan(arr)]
        r0 = real[w]["contrast"]
        pv[f"{w}_p"] = (None if (r0 is None or not len(arr))
                        else round(float((np.abs(arr) >= abs(r0)).mean()), 4))
        pv[f"{w}_null_sd"] = None if not len(arr) else round(float(arr.std()), 3)
    popd = {w: _stats_window(obs.assign(edge=obs["ret"] - obs["cmed"]), lo, hi)["median_edge"]
            for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"]))}
    cm, cv = real["mine"]["contrast"], real["verify"]["contrast"]
    pm, pvf = pv["mine_p"], pv["verify_p"]
    same = cm is not None and cv is not None and cm * cv > 0
    positive = same and cm > 0                     # the registered direction
    passes = bool(positive and pm is not None and pvf is not None and pm < 0.05 and pvf < 0.05)
    for w in ("mine", "verify"):
        log(f"  {w.upper():6s} adjacent {real[w]['adjacent']} · no_bottom {real[w]['no_bottom']} "
            f"· contrast {real[w]['contrast']} (p {pv[w+'_p']}, null sd {pv[w+'_null_sd']}) "
            f"· days {days[w]}")
    log(f"  pop_vs_control {popd} · same_sign {same} · registered direction held {positive} "
        f"· PASSES {passes}")
    res = dict(real=real, days=days, placebo=pv, population_vs_control=popd,
               same_sign=same, direction_held=positive, PASSES=passes,
               conservation={k: v for k, v in cons.items()})
    run.write_atomic("results.json", res)
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"],
                k=K_EXPECTED, oos=OOS, outcome_access_count=led["outcome_access_count"],
                PASSES=passes, completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items()
                                       if k in ("sealed_at", "registry_sha256_16", "k", "x_run")}, indent=1)),
     "outcome": outcome}[cmd]()
