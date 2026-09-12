"""JOINT_STACK_V1 — the user's correction: individually-null signals can still be JOINTLY
informative, so count how many independent layers are lit at once. 2026-09-11, user-approved. k = 2.

  WHY THIS EXISTS. I listed each component of the AMD 2026-03-31 bar and its measured null — T9,
  L34, ▽△rev+💪, 🎯🔁, cont×P50 — and implied the stack was therefore null. The user rejected that
  and was right: "ცალკე შეიძლება ნული იყოს, მაგრამ ჩვენ გვაინტერესებს ერთობლიობა". The book itself
  is the counter-example — [[project_confluence_cluster_bottom]]: no single edge family is strong,
  yet the COUNT of distinct families in a 10-bar window rises monotonically with forward edge, is
  the only confluence that survives 2022, and survives cluster-dedup (x4 = +4.47 / 6-6yr).

  So this generalises that count from the 8 edge families to the WHOLE chart — every row the
  Superchart draws — and asks the one question the pairwise families could not:

      does the NUMBER of independent layers lit on a bar rank the forward day-clustered edge?

  TWO LADDERS, and the second exists because the first has a flaw:
    A  LIT_COUNT   how many of 9 mark-bearing rows carry ANY mark. ZERO researcher degrees of
                   freedom — nothing is judged, marks are counted. Its flaw: "lit" may just track
                   activity/volatility rather than direction.
    B  BULL_COUNT  how many of 12 layers carry a mark the BOOK already classes as bottom- or
                   absorption-flavoured. The vote table was written out and approved by the user
                   BEFORE sealing ("vetanxmebi"), and is frozen verbatim in VOTES_BULL below.
  Both registered to INCREASE.

  THE EDGE ROW IS ONE VOTE, NOT EIGHT. edge_replay's 8 de-duplicated families collapse to a single
  boolean (conf_anyfam). Otherwise the one already-validated confluence would dominate a count whose
  whole purpose is to test whether the OTHER layers add anything. conf_n is carried as DESCRIPTIVE
  so the two can be compared afterwards without being confounded in the registered statistic.

  SCORES ARE EXCLUDED — SCORE / ULTRA / UV3 / 🏅 / β / V3. [[project_score_audit_v1]] measured all
  twenty at k=20 and found 0 RANKER / 1 ANTI / 19 CONTEXT. Adding them would add noise to a count,
  and that exclusion is a sealed prior, not a choice made here.

  WHAT WOULD MAKE THIS A REAL RESULT, declared in advance. [[project_signal_cooccurrence_map]]
  measured these layers as INDEPENDENT (99.2 % of cross-layer pairs within +/-10 % of lift 1.0), so a
  high simultaneous count must be genuinely rare. If the census shows the top rung is NOT rare, the
  layers are dependent, the count is measuring one thing several times, and the ladder means little
  whichever way it comes out. The census is printed before sealing for exactly that check.

  Instrument: NO sampling — every qualifying session, path-sim cooldown left in place
  ([[feedback-pathsim-cooldown-estimand]]); CONTROL_KEYS v2 restricted to the family's tickers;
  DAY-CLUSTERED rung statistic; registered statistic is the SPAN (top rung minus bottom rung) with
  Spearman rho reported but NOT registered — three-to-five rungs make a perfect rho cheap
  ([[project_turn_transition]]); 500 rung-label shuffles at fixed rung sizes give the null;
  MINE 2021-09-07..2024-12-31 decides, VERIFY 2025-01-01..2026-09-03 replicates; PASS = span keeps
  its sign in BOTH windows AND sits outside the placebo (p < 0.05) in BOTH. One outcome run.
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

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/JOINT_STACK_V1"
FAMILY = "JOINT_STACK_V1"
DATA = os.path.join(os.path.dirname(HERE), "data")
SRC = {k: os.path.join(DATA, f"{k}.parquet") for k in
       ("anatomy_signals", "shapectx_signals", "lvx_signals", "ovdmap_signals",
        "vol7_signals", "lbal_signals", "edge_votes_v1")}
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED, NSHUF, RNG_SEED = 2, 500, 20260911

# V1 ran on every DB-present session and the diagnostic then showed the count was confounded with
# DATA COVERAGE: the bottom rung had anatomy on only 73.4 % of its bars and lbal on 77.7 %, against
# 99.7 % / 100 % at the top, because four of the nine layers are derived from intraday data. A bar
# whose 1h/15m history is missing scores low for a reason that has nothing to do with the market.
# V2 sets this flag: a session survives only if every COVERAGE source has a row for it, so a low
# count means quiet rather than absent. OVD (38.1 %) and L-VX (32.4 %) are EVENT sources — their
# absence is a real state, not a gap — so they are not required.
REQUIRE_FULL_COVERAGE = False
COVERAGE_SOURCES = ("anatomy_signals", "lbal_signals", "vol7_signals", "shapectx_signals",
                    "edge_votes_v1")

# The vote tables, frozen. BULL is the table the user read and approved before any outcome existed.
VOTES_LIT = {
    "TD":    "t_sig or z_sig present",
    "L":     "l_sig present",
    "ANAT":  "▽△ verdict present (rev / shake / cont)",
    "SHAPE": "a body-nest shape fired",
    "LVX":   "an L34/L46 graded row exists",
    "OVD":   "any OVD token",
    "VOL7":  "any VOL7 mark",
    "UDN":   "any UDN★ mark (V or all)",
    "EDGE":  "any of the 8 de-duplicated edge families fired",
}
VOTES_BULL = {
    "T":     "a T-code fired (demand bar)",
    "L":     "l_sig is L34 or L3 (the absorption lines)",
    "ANAT":  "▽△ verdict is 🔻rev or 🌀shake",
    "SHAPE": "a shape fired with dir_up",
    "LVX":   "L34 graded VH or VX",
    "OVD":   "token HO or RC (handoff / reclaim)",
    "VOL7":  "Σ+ consolidation or M >= 4",
    "UDN":   "★ mark (V or all)",
    "WYCK":  "spring or accumulation",
    "EDGE":  "any of the 8 de-duplicated edge families fired",
    "RS":    "🏆 close/benchmark above its own EMA200",
    "RSI":   "rsi_14 < 50",
}
LADDERS = [
    dict(id="LIT_COUNT", col="lit", votes=list(VOTES_LIT), expect="increase"),
    dict(id="BULL_COUNT", col="bull", votes=list(VOTES_BULL), expect="increase"),
]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def _votes(liq: pd.DataFrame, log=print) -> pd.DataFrame:
    """One row per liquid (ticker, session) with the two counts and every individual vote."""
    import duckdb
    from studio.db import tf_db_path
    X = liq.copy()

    con = duckdb.connect(tf_db_path("1d"), read_only=True)
    db = con.execute("""
        SELECT ticker, substr(CAST(date AS VARCHAR),1,10) AS session,
               max(CASE WHEN coalesce(t_sig,'') <> '' THEN 1 ELSE 0 END) AS t_on,
               max(CASE WHEN coalesce(z_sig,'') <> '' THEN 1 ELSE 0 END) AS z_on,
               max(CASE WHEN coalesce(l_sig,'') <> '' THEN 1 ELSE 0 END) AS l_on,
               max(CASE WHEN coalesce(l_sig,'') IN ('L34','L3') THEN 1 ELSE 0 END) AS l_abs,
               max(CASE WHEN coalesce(wyc_spring,0)=1 OR coalesce(w2_spring,0)=1
                          OR coalesce(w2_accum,0)=1 THEN 1 ELSE 0 END) AS wyck,
               max(CASE WHEN rsi_14 < 50 THEN 1 ELSE 0 END) AS rsi_lo
        FROM bars WHERE universe <> 'index' AND date >= ? AND date <= ?
        GROUP BY 1,2""", [OOS["MINE"][0], OOS["VERIFY"][1]]).fetchdf()
    con.close()
    X = X.merge(db, on=["ticker", "session"], how="left")
    # ⚠ THE CENSUS CAUGHT THIS. 91,341 liquid sessions (4.22 %, 226 tickers) have NO row in the
    # analytics DB at all, so every vote reads 0 — including `rsi_14 < 50` and `rs`, which cannot
    # both be false on a real bar. Left in, the bottom rung would measure DATA AVAILABILITY rather
    # than quietness and the span would be meaningless. They are dropped from the universe for ALL
    # rungs equally, before sealing, and the count is recorded in the registry.
    X["db_present"] = X["t_on"].notna()

    a = pd.read_parquet(SRC["anatomy_signals"], columns=["ticker", "date", "v", "rs"]) \
          .rename(columns={"date": "session"})
    X = X.merge(a, on=["ticker", "session"], how="left")

    s = pd.read_parquet(SRC["shapectx_signals"], columns=["ticker", "date", "shape", "dir_up"]) \
          .rename(columns={"date": "session"})
    X = X.merge(s, on=["ticker", "session"], how="left")

    lv = pd.read_parquet(SRC["lvx_signals"], columns=["ticker", "date", "fam", "label"]) \
           .rename(columns={"date": "session"})
    lv_any = lv[["ticker", "session"]].drop_duplicates(); lv_any["lvx_any"] = True
    lv_b = lv[(lv["fam"] == "L34") & lv["label"].astype(str).str.contains("VH|VX", regex=True)]
    lv_b = lv_b[["ticker", "session"]].drop_duplicates(); lv_b["lvx_bull"] = True
    X = X.merge(lv_any, on=["ticker", "session"], how="left").merge(lv_b, on=["ticker", "session"], how="left")

    ov = pd.read_parquet(SRC["ovdmap_signals"],
                         columns=["ticker", "date", "tokens", "ho30", "ho60", "rc30", "rc60"]) \
           .rename(columns={"date": "session"})
    ov["ovd_any"] = ov["tokens"].astype(str).str.strip().ne("") & ov["tokens"].notna()
    ov["ovd_bull"] = ov[["ho30", "ho60", "rc30", "rc60"]].fillna(False).astype(bool).any(axis=1)
    X = X.merge(ov[["ticker", "session", "ovd_any", "ovd_bull"]], on=["ticker", "session"], how="left")

    v7 = pd.read_parquet(SRC["vol7_signals"], columns=["ticker", "date", "marks", "cons", "mr"]) \
           .rename(columns={"date": "session"})
    v7["v7_any"] = v7["marks"].astype(str).str.strip().ne("") & v7["marks"].notna()
    v7["v7_bull"] = v7["cons"].astype(str).str.contains("Σ\\+", regex=True) | (v7["mr"].fillna(0) >= 4)
    X = X.merge(v7[["ticker", "session", "v7_any", "v7_bull"]], on=["ticker", "session"], how="left")

    lb = pd.read_parquet(SRC["lbal_signals"],
                         columns=["ticker", "date", "marks", "marks_all", "star", "star_all"]) \
           .rename(columns={"date": "session"})
    lb["udn_any"] = (lb["marks"].astype(str).str.strip().ne("")
                     | lb["marks_all"].astype(str).str.strip().ne(""))
    lb["udn_bull"] = lb[["star", "star_all"]].fillna(False).astype(bool).any(axis=1)
    X = X.merge(lb[["ticker", "session", "udn_any", "udn_bull"]], on=["ticker", "session"], how="left")

    ev = pd.read_parquet(SRC["edge_votes_v1"])
    X = X.merge(ev, on=["ticker", "session"], how="left")

    # coverage: does each SOURCE have a row at all for this session?
    X["cov_anatomy_signals"] = X["v"].notna() | X["rs"].notna()
    X["cov_shapectx_signals"] = X["shape"].notna()
    X["cov_vol7_signals"] = X["v7_any"].notna()
    X["cov_edge_votes_v1"] = X["conf_anyfam"].notna()
    X["cov_lbal_signals"] = X["udn_any"].notna()

    def _b(c):
        return X[c].fillna(False).infer_objects(copy=False).astype(bool).to_numpy()

    lit = {
        "TD":    (_b("t_on") | _b("z_on")),
        "L":     _b("l_on"),
        "ANAT":  X["v"].notna().to_numpy() & X["v"].astype(str).str.strip().ne("").to_numpy(),
        "SHAPE": _b("shape"),
        "LVX":   _b("lvx_any"),
        "OVD":   _b("ovd_any"),
        "VOL7":  _b("v7_any"),
        "UDN":   _b("udn_any"),
        "EDGE":  _b("conf_anyfam"),
    }
    bull = {
        "T":     _b("t_on"),
        "L":     _b("l_abs"),
        "ANAT":  X["v"].isin(["rev", "shake"]).to_numpy(),
        "SHAPE": (_b("shape") & _b("dir_up")),
        "LVX":   _b("lvx_bull"),
        "OVD":   _b("ovd_bull"),
        "VOL7":  _b("v7_bull"),
        "UDN":   _b("udn_bull"),
        "WYCK":  _b("wyck"),
        "EDGE":  _b("conf_anyfam"),
        "RS":    _b("rs"),
        "RSI":   _b("rsi_lo"),
    }
    for k, v in lit.items():
        X["lit_" + k] = v
    for k, v in bull.items():
        X["bull_" + k] = v
    X["lit"] = np.sum(list(lit.values()), axis=0).astype(int)
    X["bull"] = np.sum(list(bull.values()), axis=0).astype(int)
    X["conf_n"] = X["conf_n"].fillna(0).astype(int)

    log("  per-layer vote rates:")
    for nm, tab in (("LIT", lit), ("BULL", bull)):
        log(f"    {nm}: " + " · ".join(f"{k} {v.mean()*100:.1f}%" for k, v in tab.items()))
    return X


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    for k, p in SRC.items():
        if not os.path.exists(p):
            raise HardStop(f"missing {k}: {p}")
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("JS_%Y%m%dT%H%M%SZ", time.gmtime()))

    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "close", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    liq = px[(px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
             & (px["session"] >= OOS["MINE"][0])
             & (px["session"] <= OOS["VERIFY"][1])][["ticker", "session"]].drop_duplicates()
    del px; gc.collect()
    log(f"  liquid sessions {len(liq):,} · {liq['ticker'].nunique():,} tickers")

    X = _votes(liq, log)
    n_all = len(X)
    X = X[X["db_present"]].reset_index(drop=True)
    log(f"  DB-present universe {len(X):,} (dropped {n_all - len(X):,} = "
        f"{(n_all - len(X)) / n_all * 100:.2f}% with no analytics row)")
    cov_report = {c: round(float(X["cov_" + c].mean()) * 100, 2) for c in COVERAGE_SOURCES}
    log(f"  source coverage {cov_report}")
    if REQUIRE_FULL_COVERAGE:
        n_db = len(X)
        m = np.ones(len(X), bool)
        for c in COVERAGE_SOURCES:
            m &= X["cov_" + c].to_numpy(bool)
        X = X[m].reset_index(drop=True)
        log(f"  FULL-COVERAGE universe {len(X):,} (dropped {n_db - len(X):,} = "
            f"{(n_db - len(X)) / n_db * 100:.2f}% missing at least one coverage source)")
    keep = (["ticker", "session", "lit", "bull", "conf_n"]
            + [c for c in X.columns if c.startswith(("lit_", "bull_"))])
    X = X[keep].sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)

    census = {}
    for L in LADDERS:
        d = X[L["col"]].value_counts().sort_index()
        census[L["id"]] = dict(
            n_layers=len(L["votes"]),
            counts={int(k): int(v) for k, v in d.items()},
            share={int(k): round(float(v) / len(X) * 100, 3) for k, v in d.items()},
            mean=round(float(X[L["col"]].mean()), 3))
        log(f"  {L['id']:11s} ({len(L['votes'])} layers) mean {census[L['id']]['mean']}  "
            + " ".join(f"{k}:{v:,}" for k, v in census[L["id"]]["counts"].items()))
    census["conf_n"] = {int(k): int(v) for k, v in X["conf_n"].value_counts().sort_index().items()}
    census["coverage_pct"] = cov_report
    census["require_full_coverage"] = bool(REQUIRE_FULL_COVERAGE)
    # the liquidity profile per count — the second confound the V1 diagnostic found
    _px = pd.read_parquet(cur["derived_parquet"],
                          columns=["ticker", "session_date", "close", "dollar_vol"])
    _px["session"] = _px["session_date"].astype(str).str[:10]
    _j = X[["ticker", "session", "lit", "bull"]].merge(
        _px[["ticker", "session", "close", "dollar_vol"]], on=["ticker", "session"], how="left")
    del _px; gc.collect()
    for _c in ("lit", "bull"):
        prof = _j.groupby(_c).agg(dv_med=("dollar_vol", "median"), px_med=("close", "median"))
        census[f"liquidity_by_{_c}"] = {int(k): dict(dv_med=round(float(r.dv_med), 0),
                                                     px_med=round(float(r.px_med), 2))
                                        for k, r in prof.iterrows()}
        log(f"  dollar-vol median by {_c}: "
            + " ".join(f"{int(k)}:{r.dv_med/1e6:.0f}M" for k, r in prof.iterrows()))
    log(f"  conf_n (descriptive) {census['conf_n']}")

    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    log(f"  X rows {len(X):,}")
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, n_shuffles=NSHUF, rng_seed=RNG_SEED,
               sampling="NONE — every qualifying session; path-sim cooldown left in place",
               universe_restriction=dict(
                   rule="the session must exist in the analytics DB (bars)",
                   why="91,341 liquid sessions (4.22%, 226 tickers) have no analytics row, so every "
                       "vote reads 0 — including rsi_14<50 and rs, which cannot both be false on a "
                       "real bar. The bottom rung would have measured data availability.",
                   dropped=int(n_all - len(X)), kept=int(len(X))),
               signal=dict(name="JOINT_STACK", ladders=LADDERS,
                           votes_lit=VOTES_LIT, votes_bull=VOTES_BULL,
                           edge_is_one_vote="edge_replay's 8 de-duplicated families collapse to a "
                                            "single boolean so the already-validated Cluster-Bottom "
                                            "cannot dominate a count meant to test the other layers",
                           scores_excluded="SCORE/ULTRA/UV3/🏅/β/V3 — project_score_audit_v1 found "
                                           "0 RANKER / 1 ANTI / 19 CONTEXT at k=20",
                           statistic="DAY-CLUSTERED per rung; registered object is the SPAN "
                                     "(top rung minus bottom rung); Spearman rho reported, NOT "
                                     "registered",
                           registered_direction="both ladders INCREASE",
                           provenance="the user's correction — individually-null signals can be "
                                      "jointly informative; generalises project_confluence_cluster_"
                                      "bottom from 8 edge families to the whole chart",
                           sources={k: _dig(p) for k, p in SRC.items()}),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               census=census, x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "JS_*")), reverse=True):
        if RunSpace.is_current(r, "X.parquet"):
            return r
    return None


def _rungs_from_census(counts: dict, min_n: int = 40_000):
    """Merge the sparse tails so no rung is too thin to carry a day-clustered median. Deterministic,
    run BEFORE sealing, and the resulting edges are frozen into the registry."""
    counts = {int(k): int(v) for k, v in counts.items()}   # a JSON round-trip stringifies int keys
    ks = sorted(counts)
    lo, acc, edges = ks[0], 0, []
    for k in ks:
        acc += counts[k]
        if acc >= min_n:
            edges.append((lo, k)); lo, acc = k + 1, 0
    if acc and edges:
        edges[-1] = (edges[-1][0], ks[-1])
    elif acc:
        edges.append((lo, ks[-1]))
    return edges


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

    bins = {}
    for L in LADDERS:
        e = _rungs_from_census(rep["census"][L["id"]]["counts"])
        bins[L["id"]] = dict(edges=e, labels=[f"{a}" if a == b else f"{a}-{b}" for a, b in e])
        if len(e) < 3:
            raise HardStop(f"{L['id']}: only {len(e)} rungs — the ladder cannot show a trend")

    lim = dict(
        census=rep["census"], rungs=bins,
        rarity_measured="Observed count sd vs the sd a sum of INDEPENDENT Bernoullis with the same "
                        "per-layer rates would have: LIT 1.476 vs 1.190 (ratio 1.240), BULL 1.743 "
                        "vs 1.444 (ratio 1.207). So the layers are WEAKLY positively dependent, not "
                        "redundant — a high simultaneous count is genuinely rarer than chance, and "
                        "the count is not one thing measured twelve times. Measured BEFORE the "
                        "outcome, recorded here so the reading cannot be adjusted afterwards.",
        degenerate_votes="LIT_COUNT's TD and L votes are on for 95.8% of bars, so they act as a "
                         "near-constant +2 offset. Harmless for the ORDERING the span tests, but it "
                         "means LIT_COUNT is effectively a 7-layer count, not a 9-layer one.",
        rarity_check="project_signal_cooccurrence_map measured these layers as INDEPENDENT, so a "
                     "high simultaneous count must be rare. If the top rung is common, the layers "
                     "are dependent, the count measures one thing repeatedly, and the ladder means "
                     "little whichever way it falls. The census above is the check; it is recorded "
                     "here BEFORE the outcome so the reading cannot be adjusted afterwards.",
        why_span_not_rho="With 3-5 rungs a perfect Spearman rho is cheap — TURN_QUALITY scored "
                         "rho 1.0 in BOTH windows and the placebo still said p 0.19 / 0.465 "
                         "(project_turn_transition). rho is printed, the SPAN is registered.",
        edge_one_vote="The EDGE row contributes ONE vote, not eight, so the validated Cluster-Bottom "
                      "cannot carry the ladder. conf_n is kept as a descriptive column so the two "
                      "can be compared after the outcome without confounding the claim.",
        activity_confound="LIT_COUNT counts marks without judging them, so it may track activity or "
                          "volatility rather than direction. BULL_COUNT exists because of that, and "
                          "its vote table was approved by the user before sealing. Neither ladder "
                          "can separate the two on its own; that is a limit, stated, not solved.",
        multiplicity="k = 2. Running k for this arc: PAIR_CONFLUENCE 3 + L34_GREEN 1 + this 2 = 6. "
                     "The question is NEW (a count, not a pair), not a re-cut of a failed cell.",
        level_caveat="population_vs_control ran -0.407 in MINE and +0.389 in VERIFY on this same "
                     "universe (PAIR_CONFLUENCE_V1). Rung LEVELS are not comparable across windows; "
                     "the span within a window is.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED",
               known_limitations=lim, sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-11 (\"vetanxmebi gaushvi\")", "PRE_OUTCOME",
                           "PRE_PATHSIM", "USER'S OWN CORRECTION — the joint, not the parts",
                           "VOTE TABLE SHOWN TO AND APPROVED BY THE USER BEFORE SEALING"],
               signal=rep["signal"], ladders=LADDERS, rungs=bins,
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}",
               control=f"CONTROL_KEYS v2 ({rep['control']['run_id']}) restricted to this family's "
                       f"tickers; control-day median needs >= {O.MIN_CONTROL} trades",
               estimand="direct sacred edge_replay._pathsim, engine cooldown INCLUDED",
               oos=OOS, gates=GATES,
               multiplicity=dict(k=K_EXPECTED, rule="one trend test per ladder"),
               cells=[dict(claim_id=L["id"], rule=f"rungs {bins[L['id']]['labels']}",
                           expect=L["expect"]) for L in LADDERS],
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], census=rep["census"],
               stop_rule="PASS only if the SPAN keeps its registered positive sign in MINE and "
                         "VERIFY AND sits outside the 500-shuffle placebo (p < 0.05) in BOTH. One "
                         "outcome run; no re-cut of the rungs; rho is not a fallback statistic.")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")
    json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
             slip=slip, n_shuffles=NSHUF, rng_seed=RNG_SEED, rungs=bins,
             code=dict(joint_stack_family=_dig(__file__),
                       anatomy_build=_dig(os.path.join(HERE, "anatomy_build.py")),
                       lbal_build=_dig(os.path.join(HERE, "lbal_build.py")),
                       shape_ctx_build=_dig(os.path.join(HERE, "shape_ctx_build.py")),
                       edge_replay=_dig(os.path.join(HERE, "edge_replay.py"))),
             outcome_access_count=0, rule="registry frozen; outcome is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


def _spearman(x, y):
    x, y = np.asarray(x, float), np.asarray(y, float)
    ok = ~np.isnan(y)
    if ok.sum() < 3:
        return None
    rx = pd.Series(x[ok]).rank().to_numpy(); ry = pd.Series(y[ok]).rank().to_numpy()
    if rx.std() == 0 or ry.std() == 0:
        return None
    return float(np.corrcoef(rx, ry)[0, 1])


def _rung_edges(sub: pd.DataFrame, n_rungs: int, lo: str, hi: str):
    w = sub[(sub["date_in"] >= lo) & (sub["date_in"] <= hi)]
    if not len(w):
        return [np.nan] * n_rungs, [0] * n_rungs
    byday = (w.groupby(["rung", "date_in"], sort=False)
               .agg(m=("ret", "median"), c=("cmed", "first")))
    byday["e"] = byday["m"] - byday["c"]
    lvl0 = byday.index.get_level_values(0)
    out, days = [], []
    for r in range(n_rungs):
        if r in lvl0:
            e = byday.loc[r, "e"].to_numpy(float)
            out.append(float(np.median(e)) if len(e) else np.nan); days.append(int(len(e)))
        else:
            out.append(np.nan); days.append(0)
    return out, days


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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "jstack")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")

    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    keys = X[["ticker", "session"]]
    ctl = CK.load(man); ctl["session"] = ctl["session"].astype(str).str[:10]
    ctl = ctl[(ctl["session"] >= OOS["MINE"][0]) & (ctl["session"] <= OOS["VERIFY"][1])]
    ctl = ctl[ctl["ticker"].isin(set(X["ticker"].unique()))].reset_index(drop=True)
    frames = OD.load_frames(cur["derived_parquet"], pd.concat([keys["ticker"], ctl["ticker"]]).unique())
    ctr, _ = OD.direct_trades(ps, frames, ctl[["ticker", "session"]]); ctr = ctr[ctr["ret"].notna()]
    med = ctr.groupby("date_in")["ret"].median(); cnt = ctr.groupby("date_in")["ret"].size()
    tr, _ = OD.direct_trades(ps, frames, keys)
    tr = tr[tr["ret"].notna()].copy()
    tr["cmed"] = tr["date_in"].map(med).to_numpy(float)
    tr["cn"] = tr["date_in"].map(cnt).fillna(0).to_numpy(int)
    tr = tr[tr["cn"] >= O.MIN_CONTROL].copy()
    del frames; gc.collect()
    log(f"  sessions {len(X):,} · usable trades {len(tr):,} · control {len(ctr):,}\n")

    rng = np.random.default_rng(RNG_SEED)
    results = []
    for L in LADDERS:
        edges = [tuple(e) for e in s["rungs"][L["id"]]["edges"]]
        labels = s["rungs"][L["id"]]["labels"]
        lab = X[["ticker", "session", L["col"]]].copy()
        v = lab[L["col"]].to_numpy()
        rung = np.full(len(lab), -1)
        for i, (a, b) in enumerate(edges):
            rung[(v >= a) & (v <= b)] = i
        lab["rung"] = rung
        lab = lab[lab["rung"] >= 0][["ticker", "session", "rung"]]
        sub = tr.merge(lab, on=["ticker", "session"], how="inner")
        nr = len(edges)

        real, days, rho = {}, {}, {}
        for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
            e, d = _rung_edges(sub, nr, lo, hi)
            real[w] = dict(rung_edge=[None if np.isnan(x) else round(x, 3) for x in e],
                           span=None if (np.isnan(e[-1]) or np.isnan(e[0])) else round(e[-1] - e[0], 3))
            days[w] = d
            rho[w] = _spearman(np.arange(nr), e)

        base = sub["rung"].to_numpy().copy()
        pl = {w: [] for w in ("mine", "verify")}
        for _ in range(NSHUF):
            sub["rung"] = rng.permutation(base)
            for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
                e, _d = _rung_edges(sub, nr, lo, hi)
                pl[w].append(np.nan if (np.isnan(e[-1]) or np.isnan(e[0])) else e[-1] - e[0])
        sub["rung"] = base
        pv = {}
        for w in ("mine", "verify"):
            arr = np.asarray(pl[w], float); arr = arr[~np.isnan(arr)]
            r0 = real[w]["span"]
            pv[f"{w}_p"] = (None if (r0 is None or not len(arr))
                            else round(float((np.abs(arr) >= abs(r0)).mean()), 4))
            pv[f"{w}_null_sd"] = None if not len(arr) else round(float(arr.std()), 3)
        popd = {w: _stats_window(sub.assign(edge=sub["ret"] - sub["cmed"]), lo, hi)["median_edge"]
                for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"]))}

        sm, sv = real["mine"]["span"], real["verify"]["span"]
        same = sm is not None and sv is not None and sm * sv > 0
        held = bool(same and sm > 0)
        pm, pvf = pv["mine_p"], pv["verify_p"]
        passes = bool(held and pm is not None and pvf is not None and pm < 0.05 and pvf < 0.05)
        results.append(dict(ladder=L["id"], labels=labels, expect=L["expect"], n_trades=int(len(sub)),
                            real=real, days=days, rho=rho, placebo=pv,
                            population_vs_control=popd, span_same_sign=same,
                            direction_held=held, PASSES=passes))
        log(f"  {L['id']}   n {len(sub):,} · rungs {labels}")
        for w in ("mine", "verify"):
            log(f"    {w.upper():6s} {real[w]['rung_edge']}  span {real[w]['span']} "
                f"rho {None if rho[w] is None else round(rho[w], 3)} "
                f"(p {pv[w+'_p']}, null sd {pv[w+'_null_sd']}) days {days[w]}")
        log(f"    pop_vs_control {popd} · same_sign {same} · held {held} · PASSES {passes}\n",
            flush=True)

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
                                       if k in ("sealed_at", "registry_sha256_16", "k", "x_run", "rungs")},
                                      indent=1)),
     "outcome": outcome}[cmd]()
