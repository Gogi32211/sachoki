"""ANATOMY_LADDER_V1 — does the ▽△ row's own 0-8 score, and its verdict order, trend with the
day-clustered edge? User-approved 2026-09-10 ("orive gaakete"). k = 3.

  The ▽△ row already carries two intrinsic orderings, so it is a ladder question, not a cell one:
    SCORE        a_loc + a_abs + a_rev, 0..8 — what the label prints as `s/8`
    VERDICT      🔺cont < 🌀shake < 🔻rev < 🔻💪rev+RS — the app's OWN sort order (anatSortVal)
    SCORE_IN_REV the same score restricted to the `rev` verdict: given the detector has fired,
                 does its score separate the good calls from the bad? That is the practically
                 useful question, because `rev` fires on 28 % of ALL sessions and is right about a
                 third of the time (1.37x lift / 76 % recall / 33 % precision,
                 project_bottom_anatomy_mtf).

  DECLARED BEFORE THE OUTCOME, from the build census (3,177,077 sessions):
    · The score is bell-shaped around 5, not pinned low, so the ladder has real spread. The sparse
      tail 0-2 (78k of 3.18M) is merged into a `<=3` rung; every rung then holds >= 67k.
    · a_abs averages 2.348 of a possible 3 — the absorption axis is nearly SATURATED and therefore
      cannot discriminate much. Before the build I predicted the strength would come from a_abs
      ([[project_what_actually_works]]: absorption is the one coherent confluence). That prediction
      is now UNLIKELY on its own census, and it is recorded here rather than quietly dropped. The
      variance lives in a_loc (1.002/2) and a_rev (1.629/3).
    · `rev` covers 28 % of sessions and rev+RS 6.2 % — the verdict is common, not selective.
  Registered shapes: SCORE and SCORE_IN_REV expected to INCREASE (a higher score should mean a
  better bottom, which is what the label asserts); VERDICT expected to INCREASE along the app's own
  order. A flat ladder refutes the score's meaning; a DECREASING one refutes it harder.

  Two defects from LADDER_V1 are fixed here rather than inherited:
    · the pass condition used `(p or 1) < 0.05`, and a perfect p of 0.0 is falsy, so it turned into
      1 and failed. Compared explicitly against None now.
    · the rung statistic took the median over TRADES while the registry said day-edge. Here the
      rung statistic IS day-clustered: per entry day, the rung's median trade minus the control's
      day-median, then the median over DAYS.

  Instrument: 1-in-12 de-phased per-ticker sampling (stride > the 5-bar cooldown); CONTROL_KEYS v2
  restricted to each ladder's own tickers; `population_vs_control` reported and required to be ~0;
  200 rung-label shuffles at fixed rung sizes give the null for span and rho.
  MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  A ladder passes only if span keeps its sign in BOTH windows AND sits outside the placebo (p<0.05)
  in BOTH. One outcome run; no rung re-cut; no re-reading a failed ladder as a different shape.
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

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/ANATOMY_LADDER_V1"
FAMILY = "ANATOMY_LADDER_V1"
ANAT = os.path.join(os.path.dirname(HERE), "data", "anatomy_signals.parquet")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED, STRIDE, NSHUF, RNG_SEED = 3, 12, 200, 20260910

LADDERS = [
    dict(id="SCORE", keep="TRUE", expr="greatest(3, s)", rungs=[3, 4, 5, 6, 7, 8],
         labels=["<=3", "4", "5", "6", "7", "8"], expect="increase"),
    dict(id="VERDICT", keep="v <> ''",
         expr="case when v = 'cont' then 0 when v = 'shake' then 1 "
              "when v = 'rev' and not rs then 2 else 3 end",
         rungs=[0, 1, 2, 3], labels=["cont", "shake", "rev", "rev+RS"], expect="increase"),
    dict(id="SCORE_IN_REV", keep="v = 'rev'", expr="greatest(3, s)", rungs=[3, 4, 5, 6, 7, 8],
         labels=["<=3", "4", "5", "6", "7", "8"], expect="increase"),
]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    if not os.path.exists(ANAT):
        raise HardStop("anatomy_signals.parquet missing — run backend/anatomy_build.py")
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("AL_%Y%m%dT%H%M%SZ", time.gmtime()))
    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "close", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    liq = px[(px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
             & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1])][["ticker", "session"]]
    liq = liq.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    del px; gc.collect()
    log(f"  liquid sessions {len(liq):,} · {liq['ticker'].nunique():,} tickers")

    import duckdb
    con = duckdb.connect(); con.register("liq", liq)
    census, out = {}, []
    for L in LADDERS:
        df = con.execute(f"""
            SELECT l.ticker, l.session, ({L['expr']}) AS rung
            FROM liq l JOIN read_parquet('{ANAT}') a
              ON a.ticker = l.ticker AND CAST(a.date AS VARCHAR) = l.session
            WHERE {L['keep']}""").fetchdf()
        df = df[df["rung"].isin(L["rungs"])].sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
        pos = df.groupby("ticker", sort=False).cumcount().to_numpy()
        off = df["ticker"].map(lambda t: CK.phase(t, STRIDE)).to_numpy()
        df = df[(pos % STRIDE) == off].reset_index(drop=True)
        df["rung"] = df["rung"].astype(int)
        out.append(df.assign(ladder=L["id"]))
        dist = {int(k): int(v) for k, v in df["rung"].value_counts().sort_index().items()}
        census[L["id"]] = dict(rows=int(len(df)), tickers=int(df["ticker"].nunique()),
                               rung_counts=dist, expect=L["expect"])
        log(f"  {L['id']:12s} {len(df):>7,} sampled · rungs {dist}")
    con.close()
    X = pd.concat(out, ignore_index=True)
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, stride=STRIDE, n_shuffles=NSHUF,
               rng_seed=RNG_SEED,
               signal=dict(name="ANATOMY_LADDER",
                           question="does the ▽△ score (and the verdict order) trend with the "
                                    "day-clustered edge?",
                           ladders=[{k: v for k, v in L.items()} for L in LADDERS],
                           statistic="DAY-CLUSTERED per rung: per entry day the rung's median trade "
                                     "minus the control day-median, then the median over DAYS",
                           source=ANAT, source_sha256_16=_dig(ANAT)),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               census=census, x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "AL_*")), reverse=True):
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
    lim = dict(rung_counts={k: v["rung_counts"] for k, v in rep["census"].items()},
               registered_shapes={L["id"]: L["expect"] for L in LADDERS},
               declared_prior="Mixed, and one half is already weakened by the build census. The "
                              "verdict was measured before as a DETECTOR (1.37x lift, 76% recall, "
                              "33% precision) and `rev` covers 28% of ALL sessions, so it is common "
                              "rather than selective. I predicted the score's strength would come "
                              "from a_abs; the census shows a_abs averages 2.348 of 3 — nearly "
                              "saturated, so it cannot discriminate much. Recorded before the "
                              "outcome rather than dropped afterwards.",
               fixed_from_LADDER_V1="(1) the pass condition no longer uses `(p or 1)`, which turned "
                                    "a perfect p of 0.0 into 1 and failed it; (2) the rung statistic "
                                    "is day-clustered, matching the registry, instead of a median "
                                    "over trades.",
               note="Rungs are not independent cells: the registered object is the TREND across "
                    "them, one test per ladder, hence k = 3.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED",
               known_limitations=lim, sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-10 (\"orive gaakete\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "USER-CHOSEN ROW — the ▽△ row the user asked to analyse"],
               question=rep["signal"]["question"], signal=rep["signal"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}",
               control=f"CONTROL_KEYS v2 ({rep['control']['run_id']}) restricted per ladder to that "
                       f"ladder's tickers; control-day median needs >= {O.MIN_CONTROL} trades",
               estimand="direct sacred edge_replay._pathsim, engine cooldown INCLUDED",
               oos=OOS, gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="one trend test per ladder"),
               cells=[dict(claim_id=L["id"], rule=f"rungs {L['labels']}", expect=L["expect"])
                      for L in LADDERS],
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], census=rep["census"],
               stop_rule="a ladder passes only if span keeps its sign in MINE and VERIFY AND sits "
                         "outside the 200-shuffle placebo (p < 0.05) in BOTH. One outcome run.")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")
    json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
             slip=slip, stride=STRIDE, n_shuffles=NSHUF, rng_seed=RNG_SEED,
             code=dict(anatomy_ladder_family=_dig(__file__),
                       anatomy_build=_dig(os.path.join(HERE, "anatomy_build.py")),
                       edge_replay=_dig(os.path.join(HERE, "edge_replay.py")),
                       control_keys=_dig(os.path.join(HERE, "control_keys.py"))),
             outcome_access_count=0, rule="registry frozen; outcome is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


def _spearman(x, y):
    x, y = np.asarray(x, float), np.asarray(y, float)
    ok = ~np.isnan(y)
    if ok.sum() < 3:
        return np.nan
    rx = pd.Series(x[ok]).rank().to_numpy(); ry = pd.Series(y[ok]).rank().to_numpy()
    if rx.std() == 0 or ry.std() == 0:
        return np.nan
    return float(np.corrcoef(rx, ry)[0, 1])


def _rung_day_edges(obs, rungs, lo, hi):
    """DAY-CLUSTERED per rung: per entry day, the rung's median trade minus that day's control
    median, then the median over DAYS. This is the statistic the registry names."""
    w = obs[(obs["date_in"] >= lo) & (obs["date_in"] <= hi)]
    out = []
    for r in rungs:
        g = w[w["rung"] == r]
        if not len(g):
            out.append(np.nan); continue
        byday = g.groupby("date_in").apply(lambda d: d["ret"].median() - d["cmed"].iloc[0])
        out.append(float(np.median(byday)) if len(byday) else np.nan)
    return out


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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "anat")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")

    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    ctl_all = CK.load(man); ctl_all["session"] = ctl_all["session"].astype(str).str[:10]
    ctl_all = ctl_all[(ctl_all["session"] >= OOS["MINE"][0]) & (ctl_all["session"] <= OOS["VERIFY"][1])]
    rng = np.random.default_rng(RNG_SEED)
    results = []
    for L in LADDERS:
        df = X[X["ladder"] == L["id"]].reset_index(drop=True)
        ctl = ctl_all[ctl_all["ticker"].isin(set(df["ticker"].unique()))].reset_index(drop=True)
        frames = OD.load_frames(cur["derived_parquet"], pd.concat([df["ticker"], ctl["ticker"]]).unique())
        ctr, _ = OD.direct_trades(ps, frames, ctl[["ticker", "session"]])
        ctr = ctr[ctr["ret"].notna()]
        med = ctr.groupby("date_in")["ret"].median(); cnt = ctr.groupby("date_in")["ret"].size()
        tr, _ = OD.direct_trades(ps, frames, df[["ticker", "session"]])
        tr = tr.merge(df, on=["ticker", "session"], how="left")
        obs = tr[tr["ret"].notna()].copy()
        obs["cmed"] = obs["date_in"].map(med).to_numpy(float)
        obs["cn"] = obs["date_in"].map(cnt).fillna(0).to_numpy(int)
        obs = obs[obs["cn"] >= O.MIN_CONTROL].copy()
        obs["edge"] = obs["ret"] - obs["cmed"]
        del frames; gc.collect()

        rungs = L["rungs"]
        real = {}
        for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
            v = _rung_day_edges(obs, rungs, lo, hi)
            real[w] = dict(rung_edge=[None if np.isnan(x) else round(x, 3) for x in v],
                           span=None if (np.isnan(v[0]) or np.isnan(v[-1])) else round(v[-1] - v[0], 3),
                           rho=None if np.isnan(_spearman(rungs, v)) else round(_spearman(rungs, v), 3),
                           n_days=int(obs[(obs["date_in"] >= lo) & (obs["date_in"] <= hi)]["date_in"].nunique()))
        pl = {w: [] for w in ("mine", "verify")}
        base = obs["rung"].to_numpy().copy()
        for _ in range(NSHUF):
            obs["rung"] = rng.permutation(base)
            for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
                v = _rung_day_edges(obs, rungs, lo, hi)
                pl[w].append(np.nan if (np.isnan(v[0]) or np.isnan(v[-1])) else v[-1] - v[0])
        obs["rung"] = base
        pv = {}
        for w in ("mine", "verify"):
            arr = np.asarray(pl[w], float); arr = arr[~np.isnan(arr)]
            r0 = real[w]["span"]
            pv[f"{w}_p"] = (None if (r0 is None or not len(arr))
                            else round(float((np.abs(arr) >= abs(r0)).mean()), 4))
            pv[f"{w}_null_sd"] = None if not len(arr) else round(float(arr.std()), 3)
        popd = {w: _stats_window(obs, lo, hi)["median_edge"]
                for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"]))}
        same = (real["mine"]["span"] is not None and real["verify"]["span"] is not None
                and real["mine"]["span"] * real["verify"]["span"] > 0)
        # explicit None checks: a perfect p of 0.0 is falsy and `(p or 1)` would fail it
        pm, pvf = pv.get("mine_p"), pv.get("verify_p")
        passes = bool(same and pm is not None and pvf is not None and pm < 0.05 and pvf < 0.05)
        results.append(dict(ladder=L["id"], labels=L["labels"], expect=L["expect"],
                            n_trades=int(len(tr)), population_vs_control=popd,
                            mine=real["mine"], verify=real["verify"], placebo=pv,
                            span_same_sign=same, PASSES=passes))
        log(f"  {L['id']:12s} n {len(tr):>7,} | pop_vs_ctrl m {popd['mine']} v {popd['verify']}")
        log(f"     MINE   {real['mine']['rung_edge']} span {real['mine']['span']} rho {real['mine']['rho']} "
            f"(p {pm}, null sd {pv.get('mine_null_sd')}, days {real['mine']['n_days']})")
        log(f"     VERIFY {real['verify']['rung_edge']} span {real['verify']['span']} rho {real['verify']['rho']} "
            f"(p {pvf}, null sd {pv.get('verify_null_sd')}, days {real['verify']['n_days']})")
        log(f"     expect {L['expect']} · same_sign {same} · PASSES {passes}\n", flush=True)

    run.write_atomic("results.json", results)
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"],
                k=K_EXPECTED, oos=OOS, outcome_access_count=led["outcome_access_count"],
                passed=[r["ladder"] for r in results if r["PASSES"]],
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    log(f"PASSED: {summ['passed'] or 'none'}")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items()
                                       if k in ("sealed_at", "registry_sha256_16", "k", "x_run")}, indent=1)),
     "outcome": outcome}[cmd]()
