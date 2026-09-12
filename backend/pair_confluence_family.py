"""PAIR_CONFLUENCE_V1 — do two signals firing together pay MORE than the sum of their parts?
User-approved 2026-09-11 ("a"), from the user's own three pairs. k = 3.

  The user named them: L34 × 💪 · L46 × 💪 · 🔺 × P50. This is the outcome question that
  [[project_signal_cooccurrence_map]] deliberately did not ask — that map measured whether marks
  fire TOGETHER (they are independent: 99.2 % of cross-layer pairs within ±10 % of lift 1.0) and
  made no claim about what the joint cell earns.

  ⚠ THE STATISTIC IS THE INTERACTION, NOT "BOTH minus A". The user's first formulation was
  `BOTH − A_ONLY` — does adding the second signal to the first help? That cannot answer the
  confluence question, because [[project_rs_gate]] already holds RS to be a universal worst-year
  rescuer: it lifts EVERYTHING, so `BOTH − A_ONLY` would come out positive on L34 even if L34 has
  nothing to do with it. The registered object is therefore the 2×2 difference-in-differences

      INTERACTION = [edge(BOTH) − edge(A_ONLY)] − [edge(B_ONLY) − edge(NEITHER)]

  which asks whether B helps MORE on A-bars than it helps elsewhere. That is what "confluence"
  means. `BOTH − A_ONLY` is still computed and printed, clearly marked DESCRIPTIVE, so the practical
  reading the user asked for is not lost.
  Registered direction: POSITIVE for all three (confluence adds). Registered honestly — the book
  leans that way for RS, and [[project_confluence_laws]] leans the OTHER way for confirmation
  triggers ("confirmation costs"), so pair 3 is a genuine coin-flip and is registered positive only
  because that is the hypothesis being tested.

  KNOWN BEFORE SEALING, from the census (2,166,628 liquid sessions):
    · L34 × 💪 co-occurrence lift 1.042 / 1.025 and L46 × 💪 0.945 / 0.905 — INDEPENDENT. The pair
      is not a coincidence to explain, only a cell to price.
    · 🔺 × P50 lift 2.065 / 1.755 — these two genuinely travel together. That is a REDUNDANCY risk,
      not a bonus: P50 is an EMA cross and 🔺 is markup continuation, so they may be measuring the
      same thing twice. Recorded here so a positive result on pair 3 is read with that in mind.
    · 💪 exists in TWO places and they are not the same flag: anatomy_signals.rs is True on 40.71 %
      of sessions, shapectx_signals.rs on 37.22 %, agreeing 96.451 %. This family uses
      anatomy_signals.rs because the user wrote 💪, which is the ▽△ row's own badge, and because
      🔺 comes from the same file — one source, not two.
    · Colours are POOLED, by the user's choice ("a"). [[project_l34_red_triple]] found red-L34 good
      and green-L34 a trap, but in the 🟢REV context specifically; generalising it here would import
      a prior past its scope. L46 is 86 % RED anyway, so the split is nearly a no-op there; on L34
      (24 % RED) it is a real second question and belongs to its own family, not to this k.

  Instrument: NO sampling — every qualifying session, the registered mask run directly with the
  path-sim's own cooldown left in place ([[feedback-pathsim-cooldown-estimand]]); one universe for
  all four cells (liquid AND anatomy present); CONTROL_KEYS v2 restricted to the family's tickers;
  DAY-CLUSTERED statistic; NSHUF size-matched label shuffles as the null; MINE decides, VERIFY
  replicates; PASS = sign holds in BOTH windows AND p < 0.05 in BOTH. One outcome run, no re-cut.
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

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/PAIR_CONFLUENCE_V1"
FAMILY = "PAIR_CONFLUENCE_V1"
DATA = os.path.join(os.path.dirname(HERE), "data")
ANAT = os.path.join(DATA, "anatomy_signals.parquet")
LVX = os.path.join(DATA, "lvx_signals.parquet")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED, NSHUF, RNG_SEED = 3, 500, 20260911
# NO SAMPLING. The first draft capped each cell with a de-phased stride, but the cooldown floor
# (stride must exceed the path-sim's 5 bars) forced a 77k cell down to 12.8k — three times below the
# cap, for nothing. Thinning has already erased an apparent replication once
# ([[project_turn_transition]]), and [[feedback-pathsim-cooldown-estimand]] says to run the
# registered mask DIRECTLY and let the cooldown suppress what it suppresses, reporting conservation.
# ADJACENT_TURN_V1 and RS_TURN_V1 both did exactly that. So does this.

PAIRS = [
    dict(id="L34_x_RS", A="L34", B="rs",
         question="does 💪RS help MORE on an L34 bar than it helps elsewhere?",
         cooccurrence="INDEPENDENT (lift 1.042 MINE / 1.025 VERIFY)"),
    dict(id="L46_x_RS", A="L46", B="rs",
         question="does 💪RS help MORE on an L46 bar than it helps elsewhere?",
         cooccurrence="INDEPENDENT (lift 0.945 MINE / 0.905 VERIFY)"),
    dict(id="CONT_x_P50", A="cont", B="p50",
         question="does a P50 cross help MORE on a 🔺 markup bar than it helps elsewhere?",
         cooccurrence="REDUNDANCY RISK — lift 2.065 MINE / 1.755 VERIFY; both may measure momentum"),
]
CELLS = ["BOTH", "A_ONLY", "B_ONLY", "NEITHER"]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def _flags(liq: pd.DataFrame, log=print) -> pd.DataFrame:
    """One row per liquid (ticker, session) that anatomy covers, with the five raw flags."""
    import duckdb
    from studio.db import tf_db_path
    ana = pd.read_parquet(ANAT, columns=["ticker", "date", "v", "rs"]).rename(columns={"date": "session"})
    X = liq.merge(ana, on=["ticker", "session"], how="inner")          # anatomy present = the universe
    X["rs"] = X["rs"].astype(bool)
    X["cont"] = X["v"].eq("cont")
    X = X.drop(columns=["v"])

    lvx = pd.read_parquet(LVX, columns=["ticker", "date", "fam"]).rename(columns={"date": "session"})
    for fam in ("L34", "L46"):
        f = lvx.loc[lvx["fam"] == fam, ["ticker", "session"]].drop_duplicates()
        f[fam] = True
        X = X.merge(f, on=["ticker", "session"], how="left")
        X[fam] = X[fam].fillna(False).infer_objects(copy=False).astype(bool)

    con = duckdb.connect(tf_db_path("1d"), read_only=True)
    p50 = con.execute("""
        SELECT ticker, substr(CAST(date AS VARCHAR),1,10) AS session,
               max(CASE WHEN coalesce(sig_p50,false) THEN 1 ELSE 0 END) AS p50
        FROM bars WHERE universe <> 'index' AND date >= ? AND date <= ?
        GROUP BY 1,2""", [OOS["MINE"][0], OOS["VERIFY"][1]]).fetchdf()
    con.close()
    X = X.merge(p50, on=["ticker", "session"], how="left")
    X["p50"] = X["p50"].fillna(0).astype(bool)
    log(f"  universe (liquid AND anatomy present) {len(X):,} · {X['ticker'].nunique():,} tickers")
    log("  base rates " + " · ".join(f"{c} {X[c].mean()*100:.2f}%" for c in ("L34", "L46", "rs", "cont", "p50")))
    return X.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    for p in (ANAT, LVX):
        if not os.path.exists(p):
            raise HardStop(f"missing {p}")
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("PC_%Y%m%dT%H%M%SZ", time.gmtime()))

    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "close", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    liq = px[(px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
             & (px["session"] >= OOS["MINE"][0])
             & (px["session"] <= OOS["VERIFY"][1])][["ticker", "session"]].drop_duplicates()
    del px; gc.collect()
    F = _flags(liq, log)

    X = F[["ticker", "session"]].copy()
    census = {}
    for P in PAIRS:
        a, b = F[P["A"]].to_numpy(bool), F[P["B"]].to_numpy(bool)
        X[P["id"]] = np.where(a & b, "BOTH", np.where(a & ~b, "A_ONLY",
                              np.where(~a & b, "B_ONLY", "NEITHER")))
        cs = {}
        for c in CELLS:
            m = X[P["id"]].to_numpy() == c
            cs[c] = dict(n=int(m.sum()), tickers=int(X.loc[m, "ticker"].nunique()),
                         mine=int((m & (X["session"] <= OOS["MINE"][1]).to_numpy()).sum()),
                         verify=int((m & (X["session"] >= OOS["VERIFY"][0]).to_numpy()).sum()))
            log(f"  {P['id']:12s} {c:8s} {cs[c]['n']:>9,}  "
                f"(MINE {cs[c]['mine']:,} / VERIFY {cs[c]['verify']:,}) tickers {cs[c]['tickers']:,}")
        census[P["id"]] = cs

    X = X.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    log(f"  X rows {len(X):,} (one per session; three label columns)")

    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, n_shuffles=NSHUF, rng_seed=RNG_SEED,
               sampling="NONE — every qualifying session; the path-sim cooldown suppresses what it "
                        "suppresses and conservation is reported",
               signal=dict(name="PAIR_CONFLUENCE", pairs=PAIRS, cells=CELLS,
                           registered_statistic="INTERACTION = [edge(BOTH) - edge(A_ONLY)] - "
                                                "[edge(B_ONLY) - edge(NEITHER)], day-clustered",
                           also_reported_not_registered="BOTH - A_ONLY (the user's first "
                                                        "formulation; confounded by B's own main "
                                                        "effect, which for RS is known to be real)",
                           registered_direction="POSITIVE for all three",
                           definitions=dict(L34="lvx_signals.fam == 'L34', colours POOLED",
                                            L46="lvx_signals.fam == 'L46', colours POOLED",
                                            rs="anatomy_signals.rs — the ▽△ row's own 💪 badge, NOT "
                                               "shapectx_signals.rs (they agree 96.451%)",
                                            cont="anatomy_signals.v == 'cont' (🔺)",
                                            p50="studio_analytics bars.sig_p50"),
                           sources={p: _dig(p) for p in (ANAT, LVX)}),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               census=census, x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "PC_*")), reverse=True):
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
    lim = dict(
        cell_counts=rep["census"],
        why_interaction="`BOTH - A_ONLY` cannot separate confluence from B's own main effect, and "
                        "for 💪RS that main effect is known to be real (project_rs_gate: universal "
                        "worst-year rescuer). The registered object is the 2x2 interaction. The "
                        "user's original formulation is computed and printed as DESCRIPTIVE.",
        pair3_redundancy="🔺 and P50 co-occur at 2.065x / 1.755x. They may be measuring momentum "
                         "twice rather than confirming each other. A positive interaction here is a "
                         "CANDIDATE needing a decomposition, not a BUILD.",
        colours_pooled="User's choice ('a'). project_l34_red_triple's red/green law was established "
                       "in the 🟢REV context; L46 is 86% RED so the split is nearly a no-op, but on "
                       "L34 (24% RED) it is a real untested second question, deliberately NOT in this k.",
        rs_source="anatomy_signals.rs (40.71% of sessions), not shapectx_signals.rs (37.22%); they "
                  "agree on 96.451% of shared rows. One flag, declared, never switched afterwards.",
        sampling="NONE. A first draft capped each cell with a de-phased stride, but the cooldown "
                 "floor (stride must exceed the path-sim's 5 bars) cut a 77k cell to 12.8k for "
                 "nothing. Thinning erased an apparent replication once (project_turn_transition), "
                 "so every qualifying session is run and conservation is reported instead.",
        universe_caveat="Restricted to sessions anatomy covers (a 1H session must exist), which is "
                        "91.1%% of liquid sessions. All four cells share that one universe.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED",
               known_limitations=lim, sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-11 (\"a\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "USER-CHOSEN PAIRS — L34×💪, L46×💪, 🔺×P50"],
               signal=rep["signal"], pairs=PAIRS,
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}, "
                        f"anatomy_signals present",
               control=f"CONTROL_KEYS v2 ({rep['control']['run_id']}) restricted to this family's "
                       f"tickers; control-day median needs >= {O.MIN_CONTROL} trades",
               estimand="direct sacred edge_replay._pathsim, engine cooldown INCLUDED",
               oos=OOS, gates=GATES,
               multiplicity=dict(k=K_EXPECTED, rule="one interaction test per pair"),
               cells=[dict(claim_id=P["id"], rule=f"2x2 on ({P['A']}, {P['B']})", expect="increase")
                      for P in PAIRS],
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], census=rep["census"],
               stop_rule="PASS only if the INTERACTION keeps its registered positive sign in MINE "
                         "and VERIFY AND sits outside the size-matched placebo (p < 0.05) in BOTH. "
                         "One outcome run; no re-cut; no promotion of the descriptive BOTH - A_ONLY "
                         "to a claim afterwards.")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")
    json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
             slip=slip, n_shuffles=NSHUF, rng_seed=RNG_SEED, sampling="NONE",
             code=dict(pair_confluence_family=_dig(__file__),
                       anatomy_build=_dig(os.path.join(HERE, "anatomy_build.py")),
                       lbal_build=_dig(os.path.join(HERE, "lbal_build.py")),
                       edge_replay=_dig(os.path.join(HERE, "edge_replay.py"))),
             outcome_access_count=0, rule="registry frozen; outcome is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


def _cell_edges(sub: pd.DataFrame, lo: str, hi: str) -> dict:
    """DAY-CLUSTERED per cell: per entry day the cell's median trade minus that day's control
    median, then the median over DAYS. Returns {cell: (edge, n_days)}."""
    w = sub[(sub["date_in"] >= lo) & (sub["date_in"] <= hi)]
    if not len(w):
        return {c: (np.nan, 0) for c in CELLS}
    byday = (w.groupby(["cell", "date_in"], sort=False)
               .agg(m=("ret", "median"), c=("cmed", "first")))
    byday["e"] = byday["m"] - byday["c"]
    out = {}
    for c in CELLS:
        if c in byday.index.get_level_values(0):
            e = byday.loc[c, "e"].to_numpy(float)
            out[c] = (float(np.median(e)) if len(e) else np.nan, int(len(e)))
        else:
            out[c] = (np.nan, 0)
    return out


def _interaction(ed: dict):
    v = [ed[c][0] for c in CELLS]
    if any(np.isnan(x) for x in v):
        return None
    return (v[0] - v[1]) - (v[2] - v[3])


def outcome(log=print) -> dict:
    sp = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(sp):
        raise HardStop("not sealed")
    s = json.load(open(sp))
    if _dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")) != s["registry_sha256_16"]:
        raise HardStop("registry digest mismatch")
    run_x = os.path.join(FAMILY_DIR, "runs", s["x_run"])
    if _dig(os.path.join(run_x, "X.parquet")) != s["x_sha256_16"]:
        raise HardStop("X mismatch")
    cur = B.canonical_current()
    if cur["derived_sha256_16"] != s["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical changed")
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "paircon")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")

    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    keys = X[["ticker", "session"]].reset_index(drop=True)
    ctl = CK.load(man); ctl["session"] = ctl["session"].astype(str).str[:10]
    ctl = ctl[(ctl["session"] >= OOS["MINE"][0]) & (ctl["session"] <= OOS["VERIFY"][1])]
    ctl = ctl[ctl["ticker"].isin(set(X["ticker"].unique()))].reset_index(drop=True)
    frames = OD.load_frames(cur["derived_parquet"], pd.concat([keys["ticker"], ctl["ticker"]]).unique())
    ctr, _ = OD.direct_trades(ps, frames, ctl[["ticker", "session"]]); ctr = ctr[ctr["ret"].notna()]
    med = ctr.groupby("date_in")["ret"].median(); cnt = ctr.groupby("date_in")["ret"].size()
    tr, _ = OD.direct_trades(ps, frames, keys)                     # path-sim ONCE per session
    tr = tr[tr["ret"].notna()].copy()
    tr["cmed"] = tr["date_in"].map(med).to_numpy(float)
    tr["cn"] = tr["date_in"].map(cnt).fillna(0).to_numpy(int)
    tr = tr[tr["cn"] >= O.MIN_CONTROL].copy()
    del frames; gc.collect()
    log(f"  distinct sessions {len(keys):,} · usable trades {len(tr):,} · control {len(ctr):,}\n")

    rng = np.random.default_rng(RNG_SEED)
    results = []
    for P in PAIRS:
        lab = X[["ticker", "session", P["id"]]].rename(columns={P["id"]: "cell"})
        sub = tr.merge(lab, on=["ticker", "session"], how="inner")
        real, days, desc = {}, {}, {}
        for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
            ed = _cell_edges(sub, lo, hi)
            it = _interaction(ed)
            real[w] = dict(**{c.lower(): (None if np.isnan(ed[c][0]) else round(ed[c][0], 3))
                              for c in CELLS},
                           interaction=None if it is None else round(it, 3))
            days[w] = {c.lower() + "_days": ed[c][1] for c in CELLS}
            bo, ao = ed["BOTH"][0], ed["A_ONLY"][0]
            desc[w] = None if (np.isnan(bo) or np.isnan(ao)) else round(bo - ao, 3)

        base = sub["cell"].to_numpy().copy()
        pl = {w: [] for w in ("mine", "verify")}
        for _ in range(NSHUF):
            sub["cell"] = rng.permutation(base)
            for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
                pl[w].append(_interaction(_cell_edges(sub, lo, hi)))
        sub["cell"] = base
        pv = {}
        for w in ("mine", "verify"):
            arr = np.asarray([x for x in pl[w] if x is not None], float)
            r0 = real[w]["interaction"]
            pv[f"{w}_p"] = (None if (r0 is None or not len(arr))
                            else round(float((np.abs(arr) >= abs(r0)).mean()), 4))
            pv[f"{w}_null_sd"] = None if not len(arr) else round(float(arr.std()), 3)
        popd = {w: _stats_window(sub.assign(edge=sub["ret"] - sub["cmed"]), lo, hi)["median_edge"]
                for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"]))}

        im, iv = real["mine"]["interaction"], real["verify"]["interaction"]
        pm, pvf = pv["mine_p"], pv["verify_p"]
        same = im is not None and iv is not None and im * iv > 0
        positive = bool(same and im > 0)
        passes = bool(positive and pm is not None and pvf is not None and pm < 0.05 and pvf < 0.05)
        results.append(dict(pair=P["id"], question=P["question"], cooccurrence=P["cooccurrence"],
                            real=real, days=days, placebo=pv,
                            descriptive_both_minus_a_only=desc,
                            population_vs_control=popd, same_sign=same,
                            direction_held=positive, PASSES=passes))
        log(f"  {P['id']}   {P['question']}")
        log(f"    co-occurrence {P['cooccurrence']}")
        for w in ("mine", "verify"):
            r = real[w]
            log(f"    {w.upper():6s} BOTH {r['both']} · A_ONLY {r['a_only']} · B_ONLY {r['b_only']} "
                f"· NEITHER {r['neither']}")
            log(f"           INTERACTION {r['interaction']} (p {pv[w+'_p']}, null sd "
                f"{pv[w+'_null_sd']}) · days {days[w]}")
            log(f"           [descriptive] BOTH - A_ONLY = {desc[w]}")
        log(f"    pop_vs_control {popd} · same_sign {same} · direction held {positive} "
            f"· PASSES {passes}\n", flush=True)

    run.write_atomic("results.json", results)
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"],
                k=K_EXPECTED, oos=OOS, outcome_access_count=led["outcome_access_count"],
                passed=[r["pair"] for r in results if r["PASSES"]],
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
