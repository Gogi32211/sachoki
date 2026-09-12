"""SHAPE_CLUSTER_V1 — does CLUSTERING of the user's body-nest shapes pay, and is DIVERSITY
different from DENSITY? User-approved 5-line plan 2026-09-10 ("ki gaushvi"). k = 5.

  Signal   The user's own 7 body-nest patterns from Pine `SHAPE_CTX` (a-priori — chart
           observation, never derived from an outcome), transcribed EXACTLY from the source
           with its shipped defaults (inclusive=true, wrapExcl=true, clLook=10, clMin=3,
           clMinB=4). Pine indices: b1=t-2, b2=t-1, b3=t (the signal bar), q1=t-3.
               MID   b1 ⊂ b2 ⊃ b3            EXP   b1 ⊂ b2 ⊂ b3
               CON   b2 ⊂ b1 · b3 ⊂ b2       LAST  b1 ⊂ b3 · b2 ⊂ b3
               WRAP  b1 ⊂ b3, minus LAST and EXP (wrapExcl)
               COIL4 b1 ⊂ b2 · (open or close of b2 inside q1's body) · close > q1_top
               MOTHER b1 ⊂ q1 · b2 ⊂ q1 · close > q1_top
           Bodies are |open..close| extents; bar colour ignored. Containment is INCLUSIVE.
  Cluster  The user's own two axes, over a 10-bar window ENDING ON the signal bar:
               DENSITY   n bars in the window carrying any shape        >= 4
               DIVERSITY n distinct FAMILIES present                    >= 3 of 4
           Families exactly as the Pine groups them: fMid · fCon · fSwal(EXP|LAST|WRAP) ·
           fDir(COIL|MOTHER). This is the question — not the individual shapes.
  Cells    SHAPE|ANY (the reference) and its EXACT PARTITION into
           |DIVERSITY_ONLY · |DENSITY_ONLY · |BOTH · |NO_CLUSTER.  k = 5.
           The partition is what makes the claim falsifiable: if clustering carries the
           signal, NO_CLUSTER must be the worst cell. No RSI cut (it would quadruple k);
           RSI composition is reported descriptively only.
  Prices   Shapes and the cluster window are computed on the CANONICAL 1D open/close — the
           same authority the trades run on — over the FULL consecutive session sequence per
           ticker. The liquidity filter is applied to the SIGNAL bar only (filtering first
           would splice non-consecutive sessions into a "10-bar window"). A bar needs 10
           prior sessions before its window is evaluated.
  Universe close >= 21, avg_vol_20d > 0, close*volume >= 3M, universe <> index, one row per
           (ticker, date).
  Control  CONTROL_KEYS v2 (de-phased). Per entry date the control-day median (>= MIN_CONTROL);
           edge = trade - that median; day-clustered.
  Estimand direct sacred `edge_replay._pathsim` over the registered mask, engine cooldown included.
  OOS      MINE 2021-09-07..2024-12-31 decides · VERIFY 2025-01-01..2026-09-03 replicates.
  Stop     nothing passes AND replicates -> NULL. One outcome run; no cell re-cut, no threshold
           search (3 / 4 / 10 are the user's shipped values and are FIXED here).

  DECLARED PRIOR (before any outcome, recorded so it cannot be revised afterwards): weak.
  MOTHER — one of these seven — was measured on 2026-09-10 as 4 NULL + 1 VETO_CANDIDATE,
  negative in BOTH windows, and the family is 71% sig_conso. The book's validated cluster
  edge (confluence_cluster_bottom) works because its constituents are individually POSITIVE
  and sit at a bottom; here the constituents are individually negative. Clustering is
  nevertheless a genuinely different question and the measurement is cheap, so it is worth
  one sealed run to get a definitive answer.
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

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/SHAPE_CLUSTER_V1"
FAMILY = "SHAPE_CLUSTER_V1"
DB = os.path.join(os.path.dirname(HERE), "data", "studio_analytics.duckdb")
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
PX_MIN, DV_FLOOR = 21.0, 3_000_000
K_EXPECTED = 5
# the user's shipped Pine defaults — FIXED, never searched
CL_LOOK, CL_MIN_FAM, CL_MIN_BARS = 10, 3, 4
SHAPES = ["s_mid", "s_exp", "s_con", "s_last", "s_wrap", "s_coil", "s_moth"]
RSI_BANDS = ((-1e9, 35.0, "<35"), (35.0, 50.0, "35-50"), (50.0, 60.0, "50-60"), (60.0, 1e9, ">=60"))
OVL_MASKS = ["E_washout", "E_qzcapit", "E_t1capbounce", "E_zoneretest", "E_coilfloor", "E_spring", "E_engulfabs"]
OVL_COLS = ["svs", "rtv", "hilo_buy", "l34", "sig_abs", "sig_conso", "vbo_up", "be_up"]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


# ── cells ─────────────────────────────────────────────────────────────────────────────────
def cells():
    return [
        dict(claim_id="SHAPE|ANY", kind="any",
             rule="any of the 7 shapes fires on the bar (the reference population)"),
        dict(claim_id="SHAPE|DIVERSITY_ONLY", kind="div",
             rule=f"fam >= {CL_MIN_FAM} AND bars < {CL_MIN_BARS} in the {CL_LOOK}-bar window (the 🎯 mark alone)"),
        dict(claim_id="SHAPE|DENSITY_ONLY", kind="den",
             rule=f"bars >= {CL_MIN_BARS} AND fam < {CL_MIN_FAM} in the {CL_LOOK}-bar window (the 🔁 mark alone)"),
        dict(claim_id="SHAPE|BOTH", kind="both",
             rule=f"fam >= {CL_MIN_FAM} AND bars >= {CL_MIN_BARS} (the 🎯🔁 mark)"),
        dict(claim_id="SHAPE|NO_CLUSTER", kind="none",
             rule=f"fam < {CL_MIN_FAM} AND bars < {CL_MIN_BARS} (a lone shape)"),
    ]


def cell_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    d = X["cl_by_fam"].to_numpy(bool)
    b = X["cl_by_den"].to_numpy(bool)
    k = c["kind"]
    if k == "any":
        return np.ones(len(X), bool)
    if k == "div":
        return d & ~b
    if k == "den":
        return b & ~d
    if k == "both":
        return d & b
    return ~d & ~b


def _band(v: np.ndarray) -> np.ndarray:
    out = np.full(len(v), "", dtype=object)
    for lo, hi, lab in RSI_BANDS:
        out[(v >= lo) & (v < hi)] = lab
    return out


# ── X build ───────────────────────────────────────────────────────────────────────────────
def shape_flags(px: pd.DataFrame) -> pd.DataFrame:
    """The 7 shapes + the two cluster axes, on the FULL consecutive session sequence per
    ticker. px must be sorted (ticker, session). Transcribed from Pine SHAPE_CTX with
    inclusive containment and wrapExcl=true."""
    o, c = px["open"].to_numpy(float), px["close"].to_numpy(float)
    px = px.assign(_T=np.maximum(o, c), _B=np.minimum(o, c))
    g = px.groupby("ticker", sort=False)

    def sh(col, n):
        return g[col].shift(n)

    # Pine: b1 = [2], b2 = [1], b3 = [0] (this bar), q1 = [3]
    b1T, b1B = sh("_T", 2), sh("_B", 2)
    b2T, b2B = sh("_T", 1), sh("_B", 1)
    b3T, b3B = px["_T"], px["_B"]
    q1T, q1B = sh("_T", 3), sh("_B", 3)

    def inside(aT, aB, bT, bB):                      # inclusive=true
        return (aT <= bT) & (aB >= bB)

    s_mid = inside(b1T, b1B, b2T, b2B) & inside(b3T, b3B, b2T, b2B)
    s_exp = inside(b1T, b1B, b2T, b2B) & inside(b2T, b2B, b3T, b3B)
    s_con = inside(b2T, b2B, b1T, b1B) & inside(b3T, b3B, b2T, b2B)
    s_last = inside(b1T, b1B, b3T, b3B) & inside(b2T, b2B, b3T, b3B)
    s_wrap_raw = inside(b1T, b1B, b3T, b3B)
    s_wrap = s_wrap_raw & ~s_last & ~s_exp           # wrapExcl=true
    # q3InQ1: the Pine reads open[1]/close[1] — that is b2 — against q1's body, inclusive
    o1, c1 = g["open"].shift(1), g["close"].shift(1)
    q3InQ1 = ((o1 >= q1B) & (o1 <= q1T)) | ((c1 >= q1B) & (c1 <= q1T))
    s_coil = inside(b1T, b1B, b2T, b2B) & q3InQ1 & (px["close"] > q1T)
    s_moth = inside(b1T, b1B, q1T, q1B) & inside(b2T, b2B, q1T, q1B) & (px["close"] > q1T)

    for name, v in zip(SHAPES, [s_mid, s_exp, s_con, s_last, s_wrap, s_coil, s_moth]):
        px[name] = v.fillna(False).to_numpy()
    # Pine guards anyShape with bar_index >= 3 (fresh groupby: px gained columns above)
    enough = px.groupby("ticker", sort=False).cumcount().to_numpy() >= 3
    px["any_shape"] = px[SHAPES].any(axis=1).to_numpy() & enough

    # ── cluster axes over the CL_LOOK window ENDING ON this bar (Pine math.sum includes it)
    px["f_mid"] = px["s_mid"]
    px["f_con"] = px["s_con"]
    px["f_swal"] = px[["s_exp", "s_last", "s_wrap"]].any(axis=1)
    px["f_dir"] = px[["s_coil", "s_moth"]].any(axis=1)
    g2 = px.groupby("ticker", sort=False)
    roll = lambda col: (g2[col].rolling(CL_LOOK, min_periods=CL_LOOK).sum()
                        .reset_index(level=0, drop=True))
    px["cl_bars"] = roll("any_shape")
    fam = None
    for f in ("f_mid", "f_con", "f_swal", "f_dir"):
        present = (roll(f) > 0).astype(float)
        fam = present if fam is None else fam + present
    px["cl_fam"] = fam
    px["cl_by_fam"] = (px["cl_fam"] >= CL_MIN_FAM).fillna(False).to_numpy()
    px["cl_by_den"] = (px["cl_bars"] >= CL_MIN_BARS).fillna(False).to_numpy()
    # a bar whose window is not yet full carries no cluster claim
    px["win_full"] = px["cl_bars"].notna().to_numpy()
    return px.drop(columns=["_T", "_B", "f_mid", "f_con", "f_swal", "f_dir"])


def _engine_state(log=print):
    import edge_replay as E
    t0 = time.time(); grp, as_of = E._frame(60, float(DV_FLOOR))
    parts = []
    for tk, g in grp.items():
        cols = [x for x in OVL_MASKS if x in g.columns]
        p = g[["date"] + cols].copy(); p["ticker"] = tk; parts.append(p)
    st = pd.concat(parts, ignore_index=True)
    st["session"] = st["date"].astype(str).str[:10]; st = st.drop(columns=["date"])
    present = [x for x in OVL_MASKS if x in st.columns]
    for x in OVL_MASKS:
        if x not in st.columns:
            st[x] = False
        st[x] = st[x].fillna(False).astype(bool)
    log(f"  engine frame: {len(grp):,} tickers · as_of {as_of} · masks {present} ({time.time()-t0:.0f}s)")
    try:
        E._CACHE.clear()
    except Exception:
        pass
    del grp; gc.collect()
    return st, str(as_of)


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    man = CK.current(); CK.assert_matches_canonical(man, cur)
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("SC_%Y%m%dT%H%M%SZ", time.gmtime()))

    px = pd.read_parquet(cur["derived_parquet"],
                         columns=["ticker", "session_date", "open", "close", "volume", "avg_vol_20d", "dollar_vol"])
    px["session"] = px["session_date"].astype(str).str[:10]
    px = px.sort_values(["ticker", "session"], kind="mergesort").reset_index(drop=True)
    px = shape_flags(px)
    shape_raw = {s: int(px[s].sum()) for s in SHAPES}
    log(f"  canonical rows {len(px):,} · any_shape {int(px['any_shape'].sum()):,} "
        f"({100*px['any_shape'].mean():.1f}% of bars)")
    log("  raw shape fires: " + " · ".join(f"{k[2:]} {v:,}" for k, v in shape_raw.items()))

    keep = ["ticker", "session", "close", "cl_bars", "cl_fam", "cl_by_fam", "cl_by_den"] + SHAPES
    sig = px[px["any_shape"] & px["win_full"]
             & (px["close"] >= PX_MIN) & (px["dollar_vol"] >= DV_FLOOR) & (px["avg_vol_20d"] > 0)
             & (px["session"] >= OOS["MINE"][0]) & (px["session"] <= OOS["VERIFY"][1])][keep].reset_index(drop=True)
    log(f"  after liquidity + window + full-lookback: {len(sig):,} signal rows")
    del px; gc.collect()

    import duckdb
    con = duckdb.connect(DB, read_only=True)
    try:
        con.register("keys", sig[["ticker", "session"]])
        cols = ", ".join(f"b.{c}" for c in ["rsi_14"] + OVL_COLS)
        st = con.execute(f"""
            WITH r AS (SELECT b.ticker, CAST(b.date AS VARCHAR) AS session, {cols},
                              row_number() OVER (PARTITION BY b.ticker, b.date ORDER BY b.universe) rn
                       FROM bars b JOIN keys k ON k.ticker = b.ticker AND CAST(b.date AS VARCHAR) = k.session
                       WHERE b.universe <> 'index')
            SELECT * EXCLUDE (rn) FROM r WHERE rn = 1""").fetchdf()
    finally:
        con.close()
    st["session"] = st["session"].str[:10]
    X = sig.merge(st, on=["ticker", "session"], how="left")
    eng, as_of = _engine_state(log)
    X = X.merge(eng, on=["ticker", "session"], how="left")
    X["in_frame"] = X["E_washout"].notna()
    for m in OVL_MASKS:
        X[m] = X[m].fillna(False).astype(bool)
    for c in OVL_COLS:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0).astype(int).astype(bool)
    X["rsi_band"] = _band(pd.to_numeric(X["rsi_14"], errors="coerce").to_numpy(float))
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)

    cs = {}
    for c in cells():
        m = cell_mask(X, c)
        cs[c["claim_id"]] = dict(TRUE=int(m.sum()), TRUE_mine=int((m & X["in_mine"]).sum()),
                                 TRUE_verify=int((m & X["in_verify"]).sum()))
    ovl = {m: dict(share=round(float(X[m].mean()), 4), n=int(X[m].sum())) for m in OVL_MASKS}
    ovl.update({c: dict(share=round(float(X[c].mean()), 4), n=int(X[c].sum())) for c in OVL_COLS})
    ovl["ANY_BOOK_EDGE"] = dict(share=round(float(X[OVL_MASKS].any(axis=1).mean()), 4),
                                n=int(X[OVL_MASKS].any(axis=1).sum()))
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, px_min=PX_MIN, dv_floor=DV_FLOOR,
               signal=dict(name="SHAPE_CLUSTER",
                           origin="user's Pine SHAPE_CTX, a-priori chart observation — not derived from any outcome",
                           shapes=dict(MID="b1 in b2 and b3 in b2", EXP="b1 in b2 and b2 in b3",
                                       CON="b2 in b1 and b3 in b2", LAST="b1 in b3 and b2 in b3",
                                       WRAP="b1 in b3, minus LAST and EXP",
                                       COIL4="b1 in b2 and (open/close of b2 inside q1 body) and close > q1_top",
                                       MOTHER="b1 in q1 and b2 in q1 and close > q1_top"),
                           indices="b1=t-2, b2=t-1, b3=t (signal bar), q1=t-3; bodies = |open..close|; inclusive containment",
                           cluster=dict(lookback=CL_LOOK, min_families=CL_MIN_FAM, min_bars=CL_MIN_BARS,
                                        families="fMid · fCon · fSwal(EXP|LAST|WRAP) · fDir(COIL|MOTHER)",
                                        note="the user's shipped Pine defaults; FIXED, never searched"),
                           prices="canonical 1D open/close, full consecutive session sequence; liquidity filter on the signal bar only"),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]),
               control=dict(run_id=man["run_id"], sha256_16=man["sha256_16"], version=man["version"]),
               sources=dict(studio_db=DB, engine_frame_as_of=as_of),
               census=dict(rows=int(len(X)), tickers=int(X["ticker"].nunique()), days=int(X["session"].nunique()),
                           rows_per_year={int(y): int(n) for y, n in X.groupby(X["session"].str[:4]).size().items()},
                           in_engine_frame=round(float(X["in_frame"].mean()), 4),
                           shape_raw_fires=shape_raw,
                           shape_shares_in_X={s: round(float(X[s].mean()), 4) for s in SHAPES},
                           cl_fam_dist={int(k): int(v) for k, v in X["cl_fam"].value_counts().sort_index().items()},
                           cl_bars_dist={int(k): int(v) for k, v in X["cl_bars"].value_counts().sort_index().items()},
                           rsi_band_shares=X["rsi_band"].value_counts(normalize=True).round(4).to_dict(),
                           cells=cs, overlap=ovl),
               x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    log(f"X {len(X):,} shape bars · " + " · ".join(f"{k.split('|')[1]} {v['TRUE_mine']}/{v['TRUE_verify']}" for k, v in cs.items()))
    log(f"  cl_fam dist {rep['census']['cl_fam_dist']}")
    log(f"  cl_bars dist {rep['census']['cl_bars_dist']}")
    log(f"  rsi bands {rep['census']['rsi_band_shares']}")
    log("  overlap: " + " · ".join(f"{k} {v['share']:.3f}" for k, v in list(ovl.items())[:10]))
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "SC_*")), reverse=True):
        if RunSpace.is_current(r, "X.parquet"):
            return r
    return None


# ── seal ──────────────────────────────────────────────────────────────────────────────────
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
               cl_fam_dist=cen["cl_fam_dist"], cl_bars_dist=cen["cl_bars_dist"],
               rsi_band_shares=cen["rsi_band_shares"], shape_shares=cen["shape_shares_in_X"],
               declared_prior="WEAK. MOTHER — one of these seven — was measured 2026-09-10 as 4 NULL + 1 "
                              "VETO_CANDIDATE, negative in BOTH windows; the family is ~71% sig_conso. The "
                              "book's validated cluster edge works because its constituents are individually "
                              "POSITIVE and at a bottom; here they are individually negative. Recorded before "
                              "the outcome so it cannot be revised afterwards.",
               note="The 5 cells are ANY plus its EXACT 4-way partition, so the cells are not independent by "
                    "construction; DSR is computed over the 5 MINE day-edge Sharpes as declared. The cluster "
                    "thresholds (10 / 3 / 4) are the user's shipped Pine values and are FIXED, not searched.")
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", known_limitations=lim,
               sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-10 (\"ki gaushvi\")", "PRE_OUTCOME", "PRE_PATHSIM",
                           "A_PRIORI — the shapes and both cluster axes come from the user's own chart work"],
               question="does clustering of the body-nest shapes pay, and is DIVERSITY (distinct families) "
                        "different from DENSITY (bars with a fire)?",
               signal=rep["signal"],
               universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}, universe <> index, one row per (ticker, date)",
               control=f"CONTROL_KEYS v2 de-phased ({rep['control']['run_id']}); per entry date the control-day "
                       f"median (>= {O.MIN_CONTROL}); edge = trade - control median; day-clustered",
               estimand="direct sacred edge_replay._pathsim over the registered mask, engine cooldown INCLUDED; "
                        "registered exit law; price-return paths",
               oos=OOS, gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="DSR with the 5 cells' MINE day-edge Sharpes; n_trials = 5"),
               cells=cells(), descriptives="X-only overlap census vs the book's signals and edge masks — never a cell",
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
               control_artifact=rep["control"], sources=rep["sources"], census=cen,
               stop_rule="nothing passes AND replicates -> NULL; one outcome run; no cell re-cut; no threshold "
                         "search; BUILD only on a second user OK")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED,
             x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"],
             control=rep["control"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(shape_cluster_family=_dig(__file__), edge_replay=_dig(os.path.join(HERE, "edge_replay.py")),
                       control_keys=_dig(os.path.join(HERE, "control_keys.py")), ovd_outcome_v1=_dig(O.__file__),
                       ovd_outcome_direct_v1=_dig(OD.__file__)),
             outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


# ── outcome ───────────────────────────────────────────────────────────────────────────────
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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "shape")
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
        log(f"  {r['claim_id']:22s} MINE med {m['median_edge']} win {m['day_win']} yrs {m['positive_years']}/{m['years_counted']} "
            f"worst {m['worst_year']} raw {m.get('raw_median')} | VERIFY med {v['median_edge']} win {v['day_win']} "
            f"worst {v['worst_year']} raw {v.get('raw_median')} | DSR {r['dsr']} | {r['classification']}")
    run.write_atomic("results.json", results)
    cls = {}
    for r in results:
        cls[r["classification"]] = cls.get(r["classification"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED, oos=OOS,
                gates=GATES, control=s["control"], control_coverage=cov, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                outcome_access_count=led["outcome_access_count"], classification_census=cls,
                completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id,
                   seal_registry_sha256_16=s["registry_sha256_16"],
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")),
                   outcome_access_count=led["outcome_access_count"], pathsim_src_sha256_16=O.PATHSIM_SRC_SHA,
                   classification_census=cls, status="COMPLETE — family SEARCH-EXPOSED"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"classification: {cls}")
    return summ


def report(write: bool = True) -> str:
    runs = [r for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OUT_*")), reverse=True)
            if RunSpace.is_current(r, "results.json")]
    if not runs:
        raise HardStop("no completed outcome run")
    run = runs[0]; res = json.load(open(os.path.join(run, "results.json")))
    summ = json.load(open(os.path.join(run, "summary.json")))
    reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"))); cen = reg["census"]
    f = lambda v, d=2: "—" if v is None else f"{float(v):+.{d}f}"
    L = [f"# SHAPE_CLUSTER_V1 — first outcome ({summ['run_id']})\n",
         f"Seal `{summ['registry_sha256_16']}` · X `{reg['x_run']}` · k = {summ['k']} · control `{summ['control']['run_id']}` "
         f"· path-sim `{summ['pathsim_src_sha256_16']}` · access {summ['outcome_access_count']}\n",
         f"Cluster: {json.dumps(reg['signal']['cluster'])}\n",
         f"{cen['rows']:,} shape bars · {cen['tickers']:,} tickers · {cen['days']:,} days · per year {cen['rows_per_year']}\n",
         f"Control coverage: {summ['control_coverage']}\n",
         "## Deciding table\n",
         "| cell | trades | MINE edge | win% | yrs+ | worst | raw | DSR | VERIFY edge | win% | worst | raw | class |",
         "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for r in res:
        m, v = r["mine"], r["verify"]
        L.append(f"| {r['claim_id']} | {r.get('direct_selected_n', 0):,} | {f(m['median_edge'])} | {f(m['day_win'],1)} | "
                 f"{m['positive_years']}/{m['years_counted']} | {f(m['worst_year'])} | {f(m.get('raw_median'))} | {f(r.get('dsr'))} | "
                 f"{f(v['median_edge'])} | {f(v['day_win'],1)} | {f(v['worst_year'])} | {f(v.get('raw_median'))} | {r['classification']} |")
    L.append(f"\nShape shares inside X: {json.dumps(cen['shape_shares_in_X'])}\n")
    L.append(f"cl_fam distribution: {json.dumps(cen['cl_fam_dist'])}\n")
    L.append(f"cl_bars distribution: {json.dumps(cen['cl_bars_dist'])}\n")
    L.append(f"RSI band shares: {json.dumps(cen['rsi_band_shares'])}\n")
    L.append("## X-only overlap with the book's own signals and edges (share of shape bars)\n")
    L.append("| item | share | n |"); L.append("|---|---:|---:|")
    for k, v in cen["overlap"].items():
        L.append(f"| {k} | {v['share']:.3f} | {v['n']:,} |")
    L.append(f"\nGates: {json.dumps(reg['gates'])}\n")
    txt = "\n".join(L)
    if write:
        open(os.path.join(FAMILY_DIR, "CHECKPOINT_FIRST_OUTCOME.md"), "w").write(txt)
    return txt


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items()
                                       if k in ("sealed_at", "registry_sha256_16", "k", "x_run", "x_sha256_16")}, indent=1)),
     "outcome": outcome,
     "report": lambda: print(report())}[cmd]()
