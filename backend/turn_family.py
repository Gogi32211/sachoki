"""TURN_V1 — "first it stops at the bottom, then it goes up." Does the TRANSITION pay?
User-approved 2026-09-11. k = 3.

  THE QUESTION IS THE USER'S, and it is a better one than anything measured before it. Every family
  so far tested a STATE — this bar is a MOTHER, this bar scores 6/8, this bar's UDN disagrees — and
  every one of them failed. What the user actually reads off the ▽△ row is a SEQUENCE: a run of
  🔻/🌀 bottom bars, then 🔺 markup. That is a state CHANGE, and almost everything the book has ever
  validated is a change rather than a state — 🌀Spring (shakeout then reclaim), 🔄dual-reclaim,
  RTB build→turn, the G3→G3→RL chain, T6 at the SC floor.

  WHAT THE DATA SAID BEFORE THE DESIGN WAS FIXED (this is why the definition allows a gap):
  a DIRECT 🔻→🔺 step is rare — of 543,497 `cont` bars only 36,726 (7 %) sit immediately after a
  bottom bar. `rev → rev` (450,278) and `rev → ''` (247,523) dominate: the bottom clusters, goes
  quiet, and only then turns. Allowing a gap makes the event common and well spread:
  run >= 2 then 🔺 within 10 bars = 197,815 events, 3,080 tickers, 17-40k per year.
  The gap is itself an intrinsic ladder: adjacent 20,956 · 2-3 64,848 · 4-5 106,135 · 6-10 197,815.

  Ladders (3), all intrinsic, all on `cont` bars:
    TURN_GAP     0 = NO bottom run in the last 10 bars · 1 · 2-3 · 4-5 · 6-10
                 The bottom rung is a 🔺 with no bottom before it at all, so this ladder answers the
                 user's question directly: does "it stopped at the bottom first" add anything?
    TURN_RUN     among turns only: how long it sat at the bottom — 2 · 3-4 · 5+ bars
                 (no 1-bar rung: MIN_RUN is 2, so it could never fill)
    TURN_QUALITY among turns only: the BEST anatomy score reached during that run — <=5 · 6 · 7-8
                 (<=4 alone was 106 rows: rev/shake already demand a minimum score)

  NO DIRECTION IS REGISTERED, deliberately. Two book laws point opposite ways here: transitions are
  what usually works, but 🔺 is markup and project_entry_timing found strength-chasing loses while
  pullbacks win. So a fast turn could be the good rung or the bad one. What IS registered: a FLAT
  ladder means "bottom first" adds nothing to an ordinary 🔺.

  Instrument (the version that survived 2026-09-10's audit): 1-in-12 de-phased per-ticker sampling;
  CONTROL_KEYS v2 restricted to each ladder's own tickers; `population_vs_control` reported and
  required to be ~0; DAY-CLUSTERED rung statistic; 200 rung-label shuffles at fixed rung sizes as
  the null. Pass = span keeps its sign in BOTH windows AND sits outside the placebo (p<0.05) in BOTH.
  MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates. One outcome run.
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

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/TURN_V1"
FAMILY = "TURN_V1"
ANAT = os.path.join(os.path.dirname(HERE), "data", "anatomy_signals.parquet")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED, STRIDE, NSHUF, RNG_SEED = 3, 12, 200, 20260911
MAX_GAP = 10          # how far after a bottom run a 🔺 still counts as its turn
MIN_RUN = 2           # a "stop at the bottom" is at least this many bottom bars

LADDERS = [
    dict(id="TURN_GAP", field="rung_gap", rungs=[0, 1, 2, 3, 4],
         labels=["no bottom", "adjacent", "2-3 bars", "4-5 bars", "6-10 bars"], expect=None,
         note="rung 0 = a 🔺 with NO bottom run in the last 10 bars — the baseline the question needs"),
    # No rung for a 1-bar run: MIN_RUN is 2, so a run of 1 is not a "stop at the bottom" by
    # definition and that rung could never fill (the same mistake the OVD ladder made).
    dict(id="TURN_RUN", field="rung_run", rungs=[2, 3, 4], labels=["2", "3-4", "5+"],
         expect=None, note="turns only: how long it sat at the bottom"),
    # <=4 was 106 rows: `rev`/`shake` already require a_abs>=1 and a_rev>=2, so a low-scoring
    # bottom run barely exists. Merged into the bottom rung rather than left to distort the span.
    dict(id="TURN_QUALITY", field="rung_qual", rungs=[0, 1, 2],
         labels=["<=5", "6", "7-8"], expect=None,
         note="turns only: the best anatomy score reached during the bottom run"),
]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def _features(log=print) -> pd.DataFrame:
    """Per (ticker, session): for every `cont` bar, how long ago the last qualifying bottom run
    ended, how long that run was, and the best score it reached."""
    d = pd.read_parquet(ANAT, columns=["ticker", "date", "v", "s"])
    d = d.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    tk = d["ticker"].to_numpy(); v = d["v"].to_numpy(); sc = d["s"].to_numpy(int)
    bot = np.isin(v, ["rev", "shake"])
    n = len(d)
    gap = np.full(n, -1, int); rlen = np.zeros(n, int); rmax = np.zeros(n, int)
    prev = None; run = 0; run_max = 0
    last_end = -10**9; last_len = 0; last_max = 0
    for i in range(n):
        if tk[i] != prev:
            prev = tk[i]; run = 0; run_max = 0
            last_end = -10**9; last_len = 0; last_max = 0
        if bot[i]:
            run += 1; run_max = max(run_max, int(sc[i]))
        else:
            # CLOSE the run first, THEN read the gap — otherwise the bar immediately after a run
            # still sees the PREVIOUS run and `gap == 1` (adjacent) can never occur. The first
            # build produced rungs {0,2,3,4} with no 1 at all; the census caught it.
            if run >= MIN_RUN:
                last_end = i - 1; last_len = run; last_max = run_max
            run = 0; run_max = 0
        # everything used here ended on or before bar i-1, so the read stays strictly causal
        gap[i] = (i - last_end) if last_end > -10**8 else -1
        rlen[i] = last_len; rmax[i] = last_max
    d["gap"] = gap; d["run_len"] = rlen; d["run_max"] = rmax
    out = d[d["v"].to_numpy() == "cont"].copy()
    g = out["gap"].to_numpy()
    has = (g >= 1) & (g <= MAX_GAP)
    rg = np.zeros(len(out), int)
    rg[has & (g == 1)] = 1
    rg[has & (g >= 2) & (g <= 3)] = 2
    rg[has & (g >= 4) & (g <= 5)] = 3
    rg[has & (g >= 6) & (g <= MAX_GAP)] = 4
    out["rung_gap"] = rg
    rl = out["run_len"].to_numpy()
    rr = np.where(rl >= 5, 4, np.where(rl >= 3, 3, rl))
    out["rung_run"] = np.where(has, rr, -1)
    rm = out["run_max"].to_numpy()
    rq = np.where(rm >= 7, 2, np.where(rm == 6, 1, 0))
    out["rung_qual"] = np.where(has, rq, -1)
    out["is_turn"] = has
    log(f"  cont bars {len(out):,} · of them turns (run>={MIN_RUN}, gap<={MAX_GAP}) {int(has.sum()):,}")
    return out[["ticker", "date", "rung_gap", "rung_run", "rung_qual", "is_turn", "gap",
                "run_len", "run_max"]]


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    if not os.path.exists(ANAT):
        raise HardStop("anatomy_signals.parquet missing — run backend/anatomy_build.py")
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("TU_%Y%m%dT%H%M%SZ", time.gmtime()))
    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "close", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    liq = px[(px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
             & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1])][["ticker", "session"]]
    del px; gc.collect()
    feat = _features(log).rename(columns={"date": "session"})
    X0 = liq.merge(feat, on=["ticker", "session"], how="inner")
    X0 = X0.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    log(f"  liquid cont bars {len(X0):,} · turns {int(X0['is_turn'].sum()):,}")

    census, parts = {}, []
    for L in LADDERS:
        sub = X0 if L["id"] == "TURN_GAP" else X0[X0["is_turn"]]
        sub = sub[sub[L["field"]].isin(L["rungs"])].reset_index(drop=True)
        pos = sub.groupby("ticker", sort=False).cumcount().to_numpy()
        off = sub["ticker"].map(lambda t: CK.phase(t, STRIDE)).to_numpy()
        sub = sub[(pos % STRIDE) == off].reset_index(drop=True)
        parts.append(sub.assign(ladder=L["id"], rung=sub[L["field"]].astype(int)))
        dist = {int(k): int(vv) for k, vv in sub[L["field"]].value_counts().sort_index().items()}
        census[L["id"]] = dict(rows=int(len(sub)), tickers=int(sub["ticker"].nunique()),
                               rung_counts=dist, note=L["note"])
        log(f"  {L['id']:13s} {len(sub):>7,} sampled · rungs {dist}")
    X = pd.concat([p[["ticker", "session", "ladder", "rung"]] for p in parts], ignore_index=True)
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, stride=STRIDE, n_shuffles=NSHUF,
               rng_seed=RNG_SEED, max_gap=MAX_GAP, min_run=MIN_RUN,
               signal=dict(name="TURN",
                           question="does 'it stopped at the bottom first, then went up' beat an "
                                    "ordinary markup bar, and does how it got there matter?",
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
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "TU_*")), reverse=True):
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
               no_direction_registered="Deliberate. Two book laws point opposite ways: transitions "
                                       "are what usually works, but 🔺 is markup and "
                                       "project_entry_timing found strength-chasing loses while "
                                       "pullbacks win. A fast turn could be the best rung or the "
                                       "worst. What IS registered: a FLAT ladder means 'bottom "
                                       "first' adds nothing to an ordinary markup bar.",
               why_a_gap_is_allowed="Measured before the design was fixed: a direct 🔻→🔺 step covers "
                                    "only 7% of cont bars (36,726 of 543,497); rev→rev and rev→'' "
                                    "dominate. The bottom clusters, goes quiet, then turns.",
               declared_prior="Stronger than the five families that preceded it, because this is a "
                              "state CHANGE and every one of those tested a static state and failed. "
                              "Against it: 🔺 is by construction a strength bar.",
               note="Rungs are not independent cells; the registered object is the TREND, one test "
                    "per ladder, hence k = 3. TURN_RUN and TURN_QUALITY are conditional on a turn "
                    "having happened, so they cannot answer 'does the bottom matter' — only "
                    "TURN_GAP's rung 0 can, and that is why it exists.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED",
               known_limitations=lim, sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-11 (\"ki\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "USER-POSED QUESTION — the alternation they actually read off the ▽△ row"],
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
                         "outside the 200-shuffle placebo (p < 0.05) in BOTH. One outcome run; no "
                         "rung re-cut; no gap/run threshold search after the fact.")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")
    json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
             slip=slip, stride=STRIDE, n_shuffles=NSHUF, rng_seed=RNG_SEED,
             code=dict(turn_family=_dig(__file__),
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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "turn")
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

        rungs = L["rungs"]; real = {}
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
        pm, pvf = pv.get("mine_p"), pv.get("verify_p")
        passes = bool(same and pm is not None and pvf is not None and pm < 0.05 and pvf < 0.05)
        results.append(dict(ladder=L["id"], labels=L["labels"], n_trades=int(len(tr)),
                            population_vs_control=popd, mine=real["mine"], verify=real["verify"],
                            placebo=pv, span_same_sign=same, PASSES=passes))
        log(f"  {L['id']:13s} n {len(tr):>7,} | pop_vs_ctrl m {popd['mine']} v {popd['verify']}")
        log(f"     labels {L['labels']}")
        log(f"     MINE   {real['mine']['rung_edge']} span {real['mine']['span']} rho {real['mine']['rho']} "
            f"(p {pm}, null sd {pv.get('mine_null_sd')}, days {real['mine']['n_days']})")
        log(f"     VERIFY {real['verify']['rung_edge']} span {real['verify']['span']} rho {real['verify']['rho']} "
            f"(p {pvf}, null sd {pv.get('verify_null_sd')}, days {real['verify']['n_days']})")
        log(f"     same_sign {same} · PASSES {passes}\n", flush=True)

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
