"""RS_TURN_V1 — the two transitions the user pointed at on the chart. 2026-09-11. k = 2.

  The user named two specific steps off the ▽△ row, not a statistic off a results table:
      A   🌀 shake  →  🔻💪 rev+RS       a shakeout, then a DURABLE (RS-intact) bottom
      B   🔻💪 rev+RS  →  🔺 cont        a durable bottom, then markup
  Both are finer than anything measured before: TURN_V1 pooled `rev` and `shake` into one "bottom"
  state and never used `rs` at all, on any ladder.

  ⚠ MULTIPLICITY, STATED PLAINLY — the two questions are NOT equal on this axis.
    A is a genuinely new question. RS never entered TURN_V1, so this is an untested axis rather
      than a re-cut of a failed result. The book already holds RS to be a real discriminator
      (project_rs_gate: the universal worst-year rescuer).
    B is a SUBGROUP OF A FAILED CELL. ADJACENT_TURN_V1's ADJACENT cell was "any bottom run → 🔺"
      and it failed VERIFY (-0.084, p 0.82). Testing a subset of it in the hope that the subset
      works is subgroup search. Its comparator is chosen to limit the damage: `rev → cont` WITHOUT
      RS, so the contrast isolates RS ALONE and does not re-ask "was there a bottom", which is
      already answered and negative. Even so, a pass on B is a CANDIDATE needing a clean forward
      window, never a BUILD.
    The user pointed from the chart, which is a-priori on their side; I have seen the outcomes,
    which is not. This paragraph exists so that cannot be quietly forgotten later.

  Contrasts (each is ONE registered claim; k = 2):
    A  revRS bars WITH a shake immediately before  minus  revRS bars WITHOUT one
    B  cont bars after revRS                       minus  cont bars after rev-without-RS
  Registered direction: A and B both POSITIVE (a shakeout first, and RS, each help). Registered
  because the book already leans that way — pretending otherwise would be false.

  Instrument: no sampling, every qualifying bar · 500 size-matched label shuffles as the null ·
  CONTROL_KEYS v2 restricted to the family's tickers · DAY-CLUSTERED statistic ·
  `population_vs_control` reported · MINE 2021-09-07..2024-12-31 decides, VERIFY
  2025-01-01..2026-09-03 replicates · PASS = sign holds in BOTH windows AND p < 0.05 in BOTH ·
  one outcome run, no re-cut, no swapping a comparator afterwards.
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

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/RS_TURN_V1"
FAMILY = "RS_TURN_V1"
ANAT = os.path.join(os.path.dirname(HERE), "data", "anatomy_signals.parquet")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED, NSHUF, RNG_SEED = 2, 500, 20260911

CONTRASTS = [
    dict(id="A_SHAKE_TO_RSBOTTOM", treat="A_with_shake", ctrl="A_no_shake",
         question="does a 🌀 shakeout immediately before a 🔻💪 durable bottom improve it?",
         status="NEW AXIS — RS never entered TURN_V1"),
    dict(id="B_RSBOTTOM_TO_MARKUP", treat="B_after_revRS", ctrl="B_after_rev",
         question="on a 🔺 markup bar, does the preceding bottom having RS matter?",
         status="SUBGROUP OF A FAILED CELL — ADJACENT_TURN_V1's ADJACENT failed VERIFY; the "
                "comparator isolates RS alone so this does not re-ask a settled question"),
]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def _cells(log=print) -> pd.DataFrame:
    d = pd.read_parquet(ANAT, columns=["ticker", "date", "v", "rs"])
    d = d.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    tk = d["ticker"].to_numpy(); v = d["v"].to_numpy(); rs = d["rs"].to_numpy(bool)
    st = np.where((v == "rev") & rs, "revRS",
                  np.where(v == "rev", "rev", np.where(v == "shake", "shake",
                           np.where(v == "cont", "cont", ""))))
    same = np.r_[False, tk[1:] == tk[:-1]]
    prev = np.concatenate(([""], st[:-1]))
    prev = np.where(same, prev, "")
    cell = np.full(len(d), "", dtype=object)
    cell[(st == "revRS") & (prev == "shake")] = "A_with_shake"
    cell[(st == "revRS") & (prev != "shake") & (prev != "")] = "A_no_shake"
    cell[(st == "cont") & (prev == "revRS")] = "B_after_revRS"
    cell[(st == "cont") & (prev == "rev")] = "B_after_rev"
    d["cell"] = cell
    out = d[d["cell"] != ""][["ticker", "date", "cell"]].reset_index(drop=True)
    log(f"  anatomy rows {len(d):,} · cell rows {len(out):,}")
    return out


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("RT_%Y%m%dT%H%M%SZ", time.gmtime()))
    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "close", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    liq = px[(px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
             & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1])][["ticker", "session"]]
    del px; gc.collect()
    X = _cells(log).rename(columns={"date": "session"}).merge(liq, on=["ticker", "session"], how="inner")
    X = X.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    cs = {c: dict(n=int((X["cell"] == c).sum()),
                  tickers=int(X.loc[X["cell"] == c, "ticker"].nunique()),
                  mine=int(((X["cell"] == c) & (X["session"] <= OOS["MINE"][1])).sum()),
                  verify=int(((X["cell"] == c) & (X["session"] >= OOS["VERIFY"][0])).sum()))
          for c in sorted(X["cell"].unique())}
    for c, val in cs.items():
        log(f"  {c:15s} {val['n']:>8,}  (MINE {val['mine']:,} / VERIFY {val['verify']:,})  "
            f"tickers {val['tickers']:,}")
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, n_shuffles=NSHUF, rng_seed=RNG_SEED,
               sampling="NONE — every qualifying bar",
               signal=dict(name="RS_TURN", contrasts=CONTRASTS,
                           registered_direction="both POSITIVE",
                           multiplicity_note="third look at the ▽△ transition data; contrast B is a "
                                             "subgroup of ADJACENT_TURN_V1's failed ADJACENT cell",
                           source=ANAT, source_sha256_16=_dig(ANAT)),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               census=cs, x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "RT_*")), reverse=True):
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
               multiplicity="THIRD look at the ▽△ transition data (TURN_V1 k=3 failed, "
                            "ADJACENT_TURN_V1 k=1 failed). Contrast A is a new axis — RS never "
                            "entered TURN_V1. Contrast B is a SUBGROUP of ADJACENT_TURN_V1's failed "
                            "ADJACENT cell, so a pass on B is a CANDIDATE needing a clean forward "
                            "window, never a BUILD. The user pointed from the chart (a-priori on "
                            "their side); I had seen the outcomes (not).",
               why_this_comparator="B compares `cont after revRS` with `cont after rev-without-RS`, "
                                   "not with `cont after nothing`. That isolates RS alone and avoids "
                                   "re-asking 'was there a bottom', which ADJACENT_TURN_V1 already "
                                   "answered and answered negatively.",
               population_caveat="`cont` is a markup bar and sits slightly below its own control "
                                 "(project_entry_timing: strength-chasing loses); levels are read "
                                 "relative to the comparator, not absolutely.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED",
               known_limitations=lim, sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-11 (\"ki\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "USER POINTED AT TWO SPECIFIC CHART TRANSITIONS"],
               signal=rep["signal"], contrasts=CONTRASTS,
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}",
               control=f"CONTROL_KEYS v2 ({rep['control']['run_id']}) restricted to this family's "
                       f"tickers; control-day median needs >= {O.MIN_CONTROL} trades",
               estimand="direct sacred edge_replay._pathsim, engine cooldown INCLUDED",
               oos=OOS, gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="two registered contrasts"),
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], census=rep["census"],
               stop_rule="PASS only if the contrast keeps its sign in MINE and VERIFY AND sits "
                         "outside the size-matched placebo (p < 0.05) in BOTH. One outcome run; no "
                         "re-cut; no swapping a comparator afterwards.")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")
    json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
             slip=slip, n_shuffles=NSHUF, rng_seed=RNG_SEED,
             code=dict(rs_turn_family=_dig(__file__),
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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "rsturn")
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
    tr, _ = OD.direct_trades(ps, frames, X[["ticker", "session"]])
    tr = tr.merge(X, on=["ticker", "session"], how="left")
    obs = tr[tr["ret"].notna()].copy()
    obs["cmed"] = obs["date_in"].map(med).to_numpy(float)
    obs["cn"] = obs["date_in"].map(cnt).fillna(0).to_numpy(int)
    obs = obs[obs["cn"] >= O.MIN_CONTROL].copy()
    del frames; gc.collect()
    log(f"  trades {len(tr):,} · usable {len(obs):,} · control {len(ctr):,}\n")

    rng = np.random.default_rng(RNG_SEED)
    results = []
    for C in CONTRASTS:
        pair = [C["treat"], C["ctrl"]]
        sub = obs[obs["cell"].isin(pair)].copy()
        real, days = {}, {}
        for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
            t, dt = _cell_day_edge(sub, C["treat"], lo, hi)
            c, dc = _cell_day_edge(sub, C["ctrl"], lo, hi)
            real[w] = dict(treat=None if np.isnan(t) else round(t, 3),
                           ctrl=None if np.isnan(c) else round(c, 3),
                           contrast=None if (np.isnan(t) or np.isnan(c)) else round(t - c, 3))
            days[w] = dict(treat_days=dt, ctrl_days=dc)
        base = sub["cell"].to_numpy().copy()
        pl = {w: [] for w in ("mine", "verify")}
        for _ in range(NSHUF):
            sub["cell"] = rng.permutation(base)
            for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
                t, _ = _cell_day_edge(sub, C["treat"], lo, hi)
                c, _ = _cell_day_edge(sub, C["ctrl"], lo, hi)
                pl[w].append(np.nan if (np.isnan(t) or np.isnan(c)) else t - c)
        sub["cell"] = base
        pv = {}
        for w in ("mine", "verify"):
            arr = np.asarray(pl[w], float); arr = arr[~np.isnan(arr)]
            r0 = real[w]["contrast"]
            pv[f"{w}_p"] = (None if (r0 is None or not len(arr))
                            else round(float((np.abs(arr) >= abs(r0)).mean()), 4))
            pv[f"{w}_null_sd"] = None if not len(arr) else round(float(arr.std()), 3)
        popd = {w: _stats_window(sub.assign(edge=sub["ret"] - sub["cmed"]), lo, hi)["median_edge"]
                for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"]))}
        cm, cv = real["mine"]["contrast"], real["verify"]["contrast"]
        pm, pvf = pv["mine_p"], pv["verify_p"]
        same = cm is not None and cv is not None and cm * cv > 0
        positive = bool(same and cm > 0)
        passes = bool(positive and pm is not None and pvf is not None and pm < 0.05 and pvf < 0.05)
        results.append(dict(contrast=C["id"], status=C["status"], real=real, days=days,
                            placebo=pv, population_vs_control=popd, same_sign=same,
                            direction_held=positive, PASSES=passes))
        log(f"  {C['id']}   ({C['status'][:38]}…)")
        for w in ("mine", "verify"):
            log(f"    {w.upper():6s} treat {real[w]['treat']} · ctrl {real[w]['ctrl']} "
                f"· contrast {real[w]['contrast']} (p {pv[w+'_p']}, null sd {pv[w+'_null_sd']}) "
                f"· days {days[w]}")
        log(f"    pop_vs_control {popd} · same_sign {same} · direction held {positive} "
            f"· PASSES {passes}\n", flush=True)

    run.write_atomic("results.json", results)
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"],
                k=K_EXPECTED, oos=OOS, outcome_access_count=led["outcome_access_count"],
                passed=[r["contrast"] for r in results if r["PASSES"]],
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
