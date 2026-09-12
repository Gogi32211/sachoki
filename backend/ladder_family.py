"""LADDER_V1 — every Superchart row already carries a STRONG↔WEAK ordering. Test the LADDER,
not the flat marks. User-approved 2026-09-10 ("a" = all rows together). k = 7.

  WHY THIS DESIGN (user's proposal, and it is better than what preceded it). Until now each mark was
  tested as an isolated binary cell, which produced three problems this family removes:
    · thin cells — 🥇L43·LEAD-in-LAG rested on 67 entry-days;
    · unequal cell sizes — UDN_CONFLICT_V1's "directional" verdict died when size-matched placebos
      showed ±0.25 pp of pure noise at 18k trades, wider than the -0.07 pp being claimed;
    · k inflation — 5-10 cells per row instead of one question per row.
  A ladder uses the WHOLE row, and a monotone trend across 3-7 ordered rungs is far harder for noise
  to fake than one pairwise gap. The book's two clearest results are already ladders:
  🎯Cluster-Bottom (x3 -> x4 -> x6+ = +12.93) and the volume inverted-U (2-3x best).

  Ladders (7). Every ordering is INTRINSIC to the data — none is a judgement of mine:
    LVX     tier 1..4          V < VL < VH < VX, the script's own grade
    VOL7    mr 0..6            volume vs the 20-day median, the script's own level
    SHAPE   grade 0..2         ⛔dry < normal < 💨absorbed (the only axis that Pine scores)
    DIVERS  cl_fam 1..4        distinct shape families in the 10-bar window
    DENS    cl_bars 1..4+      bars carrying a shape in that window
    UDN     |pos-neg|/lab      the IMBALANCE magnitude, banded — a number, not an opinion about
                               which mark "means more". V-mode counts (the parquet's primary).
    OVD     token count 0..3+  how many daily-map tokens fired
  PRE-REGISTERED SHAPES, so a result cannot be reinterpreted afterwards: VOL7 is expected to be an
  INVERTED-U, not monotone (project_volume_magnitude found 2-3x best), so rho ~ 0 there CONFIRMS the
  book rather than failing; DIVERS and DENS are expected to DECREASE (SHAPE_CLUSTER_V1 measured
  exactly that); LVX and SHAPE are expected to INCREASE; UDN and OVD have no registered direction.

  Statistics per ladder — placebo-calibrated, which is the point:
    span  = day-edge(top rung) - day-edge(bottom rung)      the primary number
    rho   = Spearman(rung index, per-rung day-edge)          monotonicity
    Path-sim runs ONCE per ladder population; the rung is only a COLUMN, so shuffling the rung
    labels (preserving rung sizes) is free. 200 shuffles give the null distribution of span and rho
    under "no relationship, same sizes". The real value is reported as a percentile of that.
    A ladder passes ONLY if the sign agrees in MINE and VERIFY *and* the real value sits outside the
    placebo distribution (p < 0.05) in BOTH windows.

  Control  CONTROL_KEYS v2 (de-phased), RESTRICTED TO THE TICKERS THE LADDER ACTUALLY COVERS.
           UDN_CONFLICT_V1 exposed why: its control drew on 3,176 tickers while the cells needed 15m
           coverage and had 2,696, and "buy every bar" came out a BUILD_CANDIDATE. Each ladder also
           reports `population_vs_control` — the whole population's day-edge, which MUST be ~0. If it
           is not, the control is not comparable and that ladder's levels are not interpretable.
  Sampling deterministic 1-in-12 per ticker at a sha256 offset, identical within a ladder. 12 > the
           engine's 5-bar cooldown, so nothing is suppressed.
  Estimand direct sacred `edge_replay._pathsim`; MINE 2021-09-07..2024-12-31 decides,
           VERIFY 2025-01-01..2026-09-03 replicates.
  Stop     one outcome run; no rung re-cut, no stride search, no re-reading a failed ladder as a
           different shape than the one registered above.
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

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/LADDER_V1"
FAMILY = "LADDER_V1"
DATA = os.path.join(os.path.dirname(HERE), "data")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED = 7
STRIDE = 12
NSHUF = 200
RNG_SEED = 20260910

LADDERS = [
    dict(id="LVX", parquet="lvx_signals.parquet", expr="tier", keep="tier >= 1",
         rungs=[1, 2, 3, 4], labels=["V", "VL", "VH", "VX"], expect="increase"),
    dict(id="VOL7", parquet="vol7_signals.parquet", expr="mr", keep="mr IS NOT NULL",
         rungs=[0, 1, 2, 3, 4, 5, 6], labels=["M0", "M1", "M2", "M3", "M4", "M5", "M6"],
         expect="inverted_U"),
    dict(id="SHAPE", parquet="shapectx_signals.parquet", expr="grade", keep="shape",
         rungs=[0, 1, 2], labels=["dry", "normal", "absorbed"], expect="increase"),
    dict(id="DIVERS", parquet="shapectx_signals.parquet", expr="cl_fam", keep="cl_fam >= 1",
         rungs=[1, 2, 3, 4], labels=["1", "2", "3", "4"], expect="decrease"),
    dict(id="DENS", parquet="shapectx_signals.parquet", expr="least(cl_bars, 4)", keep="cl_bars >= 1",
         rungs=[1, 2, 3, 4], labels=["1", "2", "3", "4+"], expect="decrease"),
    dict(id="UDN", parquet="lbal_signals.parquet",
         expr="least(3, cast(floor(4.0 * abs(nv_pos - nv_neg) / nv_lab) as integer))",
         keep="nv_lab > 0", rungs=[0, 1, 2, 3], labels=["<25%", "25-50%", "50-75%", ">=75%"],
         expect=None),
    # There is NO zero-token rung: ovdmap_signals.parquet holds only bars where a token fired
    # (0 empty strings in 1,344,962 rows), so "no OVD signal" is an ABSENT row, not a stored 0.
    # The ladder therefore asks "given at least one token, does more matter". The first seal
    # registered a rung 0 that could never fill; it was replaced before any outcome was read —
    # see SEAL_SUPERSEDED_PRE_OUTCOME.json.
    dict(id="OVD", parquet="ovdmap_signals.parquet",
         expr="least(3, cast(length(trim(tokens)) - length(replace(trim(tokens), ' ', '')) + 1 as integer))",
         keep="trim(tokens) <> ''", rungs=[1, 2, 3], labels=["1", "2", "3+"], expect=None),
]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def _liquid(cur) -> pd.DataFrame:
    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "close", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    px = px[(px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
            & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1])]
    return px[["ticker", "session"]].sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("LD_%Y%m%dT%H%M%SZ", time.gmtime()))
    liq = _liquid(cur)
    log(f"  liquid sessions {len(liq):,} · {liq['ticker'].nunique():,} tickers")

    import duckdb
    con = duckdb.connect()
    con.register("liq", liq)
    census, frames_out = {}, {}
    for L in LADDERS:
        p = os.path.join(DATA, L["parquet"])
        if not os.path.exists(p):
            raise HardStop(f"{L['id']}: {L['parquet']} missing")
        df = con.execute(f"""
            SELECT l.ticker, l.session, ({L['expr']}) AS rung
            FROM liq l JOIN read_parquet('{p}') s
              ON s.ticker = l.ticker AND CAST(s.date AS VARCHAR) = l.session
            WHERE {L['keep']}""").fetchdf()
        df = df[df["rung"].isin(L["rungs"])].copy()
        df = df.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
        pos = df.groupby("ticker", sort=False).cumcount().to_numpy()
        off = df["ticker"].map(lambda t: CK.phase(t, STRIDE)).to_numpy()
        df = df[(pos % STRIDE) == off].reset_index(drop=True)
        df["rung"] = df["rung"].astype(int)
        frames_out[L["id"]] = df
        dist = {int(k): int(v) for k, v in df["rung"].value_counts().sort_index().items()}
        census[L["id"]] = dict(rows=int(len(df)), tickers=int(df["ticker"].nunique()),
                               rung_counts=dist, expect=L["expect"],
                               parquet_sha256_16=_dig(p))
        log(f"  {L['id']:7s} {len(df):>7,} sampled · rungs {dist}")
    con.close()
    del liq; gc.collect()

    xp = os.path.join(run.dir, "X.parquet")
    allf = pd.concat([d.assign(ladder=k) for k, d in frames_out.items()], ignore_index=True)
    allf.to_parquet(xp, index=False)
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, px_min=PX_MIN, dv_floor=DV_FLOOR,
               stride=STRIDE, n_shuffles=NSHUF, rng_seed=RNG_SEED,
               signal=dict(name="LADDER",
                           question="does each Superchart row's own strong-to-weak ordering trend "
                                    "with the day-clustered edge?",
                           ladders=[{k: v for k, v in L.items() if k != "expr"} | {"expr": L["expr"]}
                                    for L in LADDERS],
                           statistics="span = day-edge(top rung) - day-edge(bottom rung); "
                                      "rho = Spearman(rung, per-rung day-edge); both calibrated "
                                      "against 200 shuffles of the rung labels at fixed rung sizes",
                           control_scope="CONTROL_KEYS v2 RESTRICTED to the tickers each ladder covers"),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               census=census, x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "LD_*")), reverse=True):
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
    lim = dict(rung_counts={k: v["rung_counts"] for k, v in rep["census"].items()},
               registered_shapes={L["id"]: L["expect"] for L in LADDERS},
               why_placebo="UDN_CONFLICT_V1 (2026-09-10) produced a 'directional' verdict that "
                           "size-matched placebos destroyed: four random subsamples of AGREE, with no "
                           "signal by construction, spanned -0.07 to +0.44 in VERIFY while the claim "
                           "rested on -0.07. Any contrast between unequally sized cells must be "
                           "calibrated the same way, so it is built into this family rather than bolted on.",
               why_control_restricted="In that same family 'buy every bar' scored +0.34 with DSR 0.9987 "
                                      "because the control drew on 3,176 tickers and the cells on 2,696. "
                                      "Here the control is intersected with each ladder's own tickers and "
                                      "`population_vs_control` is reported as a sanity number that must be ~0.",
               note="Rungs within a ladder are NOT independent cells: the registered object is the TREND "
                    "across them, one test per row, which is why k = 7 and not the ~30 rungs.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", known_limitations=lim,
               sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-10 (\"ki\" to the 5-line plan, \"a\" = all rows)",
                           "PRE_OUTCOME", "PRE_PATHSIM",
                           "USER-PROPOSED DESIGN — the strong/weak ordering inside each row"],
               question=rep["signal"]["question"], signal=rep["signal"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}",
               control=f"CONTROL_KEYS v2 ({rep['control']['run_id']}) restricted per ladder to that "
                       f"ladder's tickers; per entry date the control-day median (>= {O.MIN_CONTROL})",
               estimand="direct sacred edge_replay._pathsim, engine cooldown INCLUDED",
               oos=OOS, gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="one registered trend test per row"),
               cells=[dict(claim_id=L["id"], rule=f"rungs {L['labels']} by {L['expr']}", expect=L["expect"])
                      for L in LADDERS],
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], census=rep["census"],
               stop_rule="a ladder passes only if the sign of span agrees in MINE and VERIFY AND the real "
                         "span sits outside the 200-shuffle placebo distribution (p < 0.05) in BOTH windows. "
                         "One outcome run; no rung re-cut; no re-reading a failed ladder as a different shape.")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             stride=STRIDE, n_shuffles=NSHUF, rng_seed=RNG_SEED,
             code=dict(ladder_family=_dig(__file__), edge_replay=_dig(os.path.join(HERE, "edge_replay.py")),
                       control_keys=_dig(os.path.join(HERE, "control_keys.py")), ovd_outcome_v1=_dig(O.__file__),
                       ovd_outcome_direct_v1=_dig(OD.__file__)),
             outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
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


def _rung_edges(obs: pd.DataFrame, rungs, lo, hi):
    """per-rung day-edge median inside a window, using the day-clustered statistic."""
    w = obs[(obs["date_in"] >= lo) & (obs["date_in"] <= hi)]
    out = []
    for r in rungs:
        e = w.loc[w["rung"] == r, "edge"].to_numpy()
        e = e[~np.isnan(e)]
        out.append(float(np.median(e)) if len(e) else np.nan)
    return out


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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "ladder")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                              seal_registry=s["registry_sha256_16"]))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")

    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    ctl_all = CK.load(man); ctl_all["session"] = ctl_all["session"].astype(str).str[:10]
    ctl_all = ctl_all[(ctl_all["session"] >= OOS["MINE"][0]) & (ctl_all["session"] <= OOS["VERIFY"][1])]
    rng = np.random.default_rng(RNG_SEED)
    results = []
    for L in LADDERS:
        df = X[X["ladder"] == L["id"]].reset_index(drop=True)
        tick = set(df["ticker"].unique())
        ctl = ctl_all[ctl_all["ticker"].isin(tick)].reset_index(drop=True)
        frames = OD.load_frames(cur["derived_parquet"], pd.concat([df["ticker"], ctl["ticker"]]).unique())
        ctr, _ = OD.direct_trades(ps, frames, ctl[["ticker", "session"]])
        ctr = ctr[ctr["ret"].notna()]
        med = ctr.groupby("date_in")["ret"].median(); cnt = ctr.groupby("date_in")["ret"].size()
        tr, cons = OD.direct_trades(ps, frames, df[["ticker", "session"]])
        tr = tr.merge(df, on=["ticker", "session"], how="left")
        obs = tr[tr["ret"].notna()].copy()
        obs["control"] = obs["date_in"].map(med).to_numpy(float)
        obs["n_others"] = obs["date_in"].map(cnt).fillna(0).to_numpy(int)
        obs["edge"] = np.where(obs["n_others"] >= O.MIN_CONTROL, obs["ret"] - obs["control"], np.nan)
        del frames; gc.collect()

        rungs = L["rungs"]
        real = {}
        for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
            v = _rung_edges(obs, rungs, lo, hi)
            real[w] = dict(rung_edge=[None if np.isnan(x) else round(x, 3) for x in v],
                           span=None if (np.isnan(v[0]) or np.isnan(v[-1])) else round(v[-1] - v[0], 3),
                           rho=None if np.isnan(_spearman(rungs, v)) else round(_spearman(rungs, v), 3))
        # placebo: shuffle the rung labels, rung SIZES preserved, trades untouched
        pl = {w: dict(span=[], rho=[]) for w in ("mine", "verify")}
        base = obs["rung"].to_numpy().copy()
        for _ in range(NSHUF):
            obs["rung"] = rng.permutation(base)
            for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
                v = _rung_edges(obs, rungs, lo, hi)
                pl[w]["span"].append(np.nan if (np.isnan(v[0]) or np.isnan(v[-1])) else v[-1] - v[0])
                pl[w]["rho"].append(_spearman(rungs, v))
        obs["rung"] = base
        pv = {}
        for w in ("mine", "verify"):
            for st in ("span", "rho"):
                arr = np.asarray(pl[w][st], float); arr = arr[~np.isnan(arr)]
                r0 = real[w][st]
                pv[f"{w}_{st}_p"] = (None if (r0 is None or not len(arr))
                                     else round(float((np.abs(arr) >= abs(r0)).mean()), 4))
                pv[f"{w}_{st}_null_sd"] = None if not len(arr) else round(float(arr.std()), 3)
        # the whole population against its own control — must be ~0 or the control is not comparable
        popd = {}
        for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
            st = _stats_window(obs, lo, hi)
            popd[w] = st["median_edge"]
        same = (real["mine"]["span"] is not None and real["verify"]["span"] is not None
                and real["mine"]["span"] * real["verify"]["span"] > 0)
        passes = bool(same and (pv.get("mine_span_p") or 1) < 0.05 and (pv.get("verify_span_p") or 1) < 0.05)
        rec = dict(ladder=L["id"], labels=L["labels"], expect=L["expect"], n_trades=int(len(tr)),
                   population_vs_control=popd, mine=real["mine"], verify=real["verify"],
                   placebo=pv, span_same_sign=same, PASSES=passes)
        results.append(rec)
        log(f"  {L['id']:7s} n {len(tr):>7,} | pop_vs_ctrl m {popd['mine']} v {popd['verify']}", flush=True)
        log(f"          MINE   rungs {real['mine']['rung_edge']} span {real['mine']['span']} rho {real['mine']['rho']} "
            f"(p_span {pv.get('mine_span_p')}, null sd {pv.get('mine_span_null_sd')})")
        log(f"          VERIFY rungs {real['verify']['rung_edge']} span {real['verify']['span']} rho {real['verify']['rho']} "
            f"(p_span {pv.get('verify_span_p')}, null sd {pv.get('verify_span_null_sd')})")
        log(f"          expect {L['expect']} · same_sign {same} · PASSES {passes}\n", flush=True)

    run.write_atomic("results.json", results)
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED,
                oos=OOS, control=s["control"], pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                outcome_access_count=led["outcome_access_count"],
                passed=[r["ladder"] for r in results if r["PASSES"]],
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id,
                   seal_registry_sha256_16=s["registry_sha256_16"],
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")),
                   outcome_access_count=led["outcome_access_count"], passed=summ["passed"],
                   status="COMPLETE — family SEARCH-EXPOSED"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"PASSED: {summ['passed'] or 'none'}")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items()
                                       if k in ("sealed_at", "registry_sha256_16", "k", "x_run")}, indent=1)),
     "outcome": outcome}[cmd]()
