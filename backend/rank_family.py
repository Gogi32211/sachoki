"""RANK_V1 — does adding the app's newer display/score layers to the PROVEN state-conditioned rank improve the HEAD
(top-10 edge fires per day) in VERIFY, day-clustered?  User-approved plan (2026-09-07, "gidastureb gaakete"). k = 3.

  Unit       an edge FIRE from data/opportunities.parquet (ticker, sig_date, setup, family); several fires can share one
             (ticker, sig_date) — the HEAD is evaluated on distinct tickers per day (max prediction over a ticker's fires).
  A          the incumbent: hierarchical-shrinkage table on (family x RSI band x conso x RS x price band), K = 60 to the
             (family x RSI band) parent, then to family, then to the train mean (allocation study: top10 +5.71 vs +3.41).
  B          A + ridge (lambda 100, fixed) on the residual over the display layers: bodywick, physics, CISD, L-BAL (V),
             L-VX, OVD, VOL7 (one-hot / 0-1 scaled; levels with < 300 fires pooled to OTHER; missing -> indicator).
  C          A + ridge over the B layers plus score features: 🎲 hits (core), UV3-core, conf_n, beta_score, beta_zone.
  Y          the opportunities' own sacred-pathsim mark-to-market at 60 bars (mtm_60, the current rule's horizon; no new
             path-sim); edge = mtm_60 − same-day median over the day's distinct opportunities (>= 20 that day).
  Walk-fwd   score year t is predicted by a fit on sig_date < Jan 1 of t. MINE = 2022-01-01..2024-12-31 (2021 cannot be
             scored walk-forward and is train-only — a declared deviation from the family-standard 2021-09-07 start);
             VERIFY = 2025-01-01..2026-08-06 (the snapshot's last day). Reserved before any outcome.
  HEAD       per day: top-10 distinct tickers by prediction (ties: ticker asc) -> mean edge; day series per variant.
  Gates      B/C BEATS_A iff MINE: mean(head_v − head_A) > 0, day-bootstrap 90 % CI > 0 (2000 draws, seed 7), >= 2/3 years
             positive, DSR(head_v) >= 0.95 at k = 3; and VERIFY: mean delta > 0 with both years positive.
             Winner = C if BEATS_A and delta_C(VERIFY) >= delta_B(VERIFY), else B if BEATS_A, else A.
  Output     RANK = winner's expected edge, served as a within-day percentile; the fitted production model (all data) is
             written to the run dir. No feature search, no lambda/K search, one outcome run. UI wiring needs a second OK.
"""
from __future__ import annotations
import os, sys, json, time, hashlib, glob, gc
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
from ovd_build import HardStop, RunSpace                                 # noqa: E402
import ovd_build as B                                                    # noqa: E402
import ovd_outcome_v1 as O                                               # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/RANK_V1"
FAMILY = "RANK_V1"
DB = os.path.join(ROOT, "data", "studio_analytics.duckdb")
OPP = os.path.join(ROOT, "data", "opportunities.parquet")
LAYERS = {k: os.path.join(ROOT, "data", f"{k}_signals.parquet") for k in ("lbal", "lvx", "ovdmap", "vol7")}
OOS = dict(TRAIN_FROM="2021-05-27", MINE=["2022-01-01", "2024-12-31"], VERIFY=["2025-01-01", "2026-08-06"])
SCORE_YEARS = [2022, 2023, 2024, 2025, 2026]
K_SHRINK = 60.0
LAMBDA = 100.0
MIN_LEVEL = 300
MIN_DAY = 20
HEAD_N = 10
BOOT = 2000
SEED = 7
K_EXPECTED = 3
RSI_BANDS = ((-1e9, 35.0, "<35"), (35.0, 50.0, "35-50"), (50.0, 60.0, "50-60"), (60.0, 1e9, ">=60"))
PX_BANDS = ((0.0, 21.0, "<21"), (21.0, 89.0, "21-89"), (89.0, 377.0, "89-377"), (377.0, 1e9, ">=377"))

# ── feature registry (fixed) ──────────────────────────────────────────────────────────────
BARS_CAT = ["bar_body_wick", "phys_r", "phys_regime", "phys_m", "phys_e", "phys_k", "phys_c", "phys_h", "phys_gap_true",
            "phys_wyc", "phys_s", "phys_ad"]
BARS_BOOL = ["sig_cisd_plus_struct", "sig_cisd_minus_struct", "sig_cisd_cplus", "sig_cisd_seq"]
LBAL_CAT = ["lbal_udn", "lbal_udn_c", "lbal_marks", "lbal_colour"]
LBAL_BOOL = ["lbal_star", "lbal_conflict", "lbal_half_up", "lbal_half_dn", "lbal_nn"]
LBAL_NUM = {"lbal_nv_pos": 26.0, "lbal_nv_neg": 26.0, "lbal_nv_l34": 26.0, "lbal_nv_l46": 26.0}
LVX_CAT = ["lvx_fam"]; LVX_BOOL = ["lvx_v"]; LVX_NUM = {"lvx_tier": 4.0}
OVD_BOOL = ["ovd_ob30", "ovd_ob60", "ovd_rc30", "ovd_rc60", "ovd_cd30", "ovd_cd60", "ovd_ho30", "ovd_ho60", "ovd_nm", "ovd_hv_event"]
VOL7_CAT = ["vol7_jump", "vol7_cons"]; VOL7_BOOL = ["vol7_vb2", "vol7_shift_up", "vol7_shift_dn"]; VOL7_NUM = {"vol7_mr": 6.0, "vol7_sg": 6.0}
MISSING = ["miss_bars", "miss_lbal", "miss_lvx", "miss_ovd", "miss_vol7"]
SCORE_CAT = ["beta_zone"]; SCORE_NUM = {"hits_core": 6.0, "uv3_core": 55.0, "conf_n": 6.0, "beta_score": 100.0}
SET_B = dict(cat=BARS_CAT + LBAL_CAT + LVX_CAT + VOL7_CAT, bool=BARS_BOOL + LBAL_BOOL + LVX_BOOL + OVD_BOOL + VOL7_BOOL + MISSING,
             num={**LBAL_NUM, **LVX_NUM, **VOL7_NUM})
SET_C = dict(cat=SET_B["cat"] + SCORE_CAT, bool=SET_B["bool"], num={**SET_B["num"], **SCORE_NUM})
STATE = ["family", "rsi_band", "conso", "rs", "px_band"]


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


def _band(v: np.ndarray, bands) -> np.ndarray:
    out = np.full(len(v), "", dtype=object)
    for lo, hi, lab in bands:
        out[(v >= lo) & (v < hi)] = lab
    return out


# ── X build ───────────────────────────────────────────────────────────────────────────────
def _axes_from_engine(log=print):
    import edge_replay as E
    t0 = time.time(); grp, as_of = E._frame(60, 3_000_000.0); parts = []
    for tk, g in grp.items():
        cols = [c for c in ("conf_n", "tls_bar", "iv_vspike", "iv_dry", "rs_intact") if c in g.columns]
        p = g[["date"] + cols].copy(); p["ticker"] = tk; parts.append(p)
    ax = pd.concat(parts, ignore_index=True); ax["sig_date"] = ax["date"].astype(str).str[:10]; ax = ax.drop(columns=["date"])
    ax["conf_n"] = ax["conf_n"].fillna(0).astype(int)
    for c in ("tls_bar", "iv_vspike", "iv_dry", "rs_intact"):
        ax[c] = ax[c].fillna(False).astype(bool)
    log(f"  engine frame axes: {len(ax):,} rows ({time.time() - t0:.0f}s)")
    try:
        E._CACHE.clear()
    except Exception:
        pass
    del grp; gc.collect()
    return ax, str(as_of)


def _bars_features(keys: pd.DataFrame, ax: pd.DataFrame, log=print) -> pd.DataFrame:
    """bars columns for the fire days + recomputed ULTRA / UV3-core / BUY / 🎲-core (chunked, pure fns)."""
    import duckdb
    from ultra_score import compute_ultra_score, compute_ultra_score_v3, compute_score_hits
    from buy_score import compute_buy_score
    con = duckdb.connect(DB, read_only=True)
    con.register("keys", keys[["ticker", "sig_date"]])
    out = []; t0 = time.time()
    for h in range(10):
        df = con.execute(f"""
            WITH r AS (SELECT b.*, row_number() OVER (PARTITION BY b.ticker, b.date ORDER BY b.universe) rn
                       FROM bars b JOIN keys k ON k.ticker = b.ticker AND CAST(b.date AS VARCHAR) = k.sig_date
                       WHERE hash(b.ticker) % 10 = {h})
            SELECT * EXCLUDE (rn) FROM r WHERE rn = 1""").fetchdf()
        if not len(df):
            continue
        df["sig_date"] = df["date"].astype(str).str[:10]
        df = df.merge(ax, on=["ticker", "sig_date"], how="left")
        df["conf_n"] = df["conf_n"].fillna(0).astype(int)
        us = np.zeros(len(df)); uc = np.zeros(len(df)); bs = np.zeros(len(df)); hc = np.zeros(len(df))
        for i, r in enumerate(df.to_dict("records")):
            base = dict(r, rsi=r.get("rsi_14"), last_price=r.get("close"), rs_intact=False, conf_n=0, tls_bar=False, iv_vspike=False, no_vol_event=False)
            u = compute_ultra_score(r)["ultra_score"]; v = compute_ultra_score_v3(base)["ultra_score_v3"]
            b = compute_buy_score(r.get("prebreak_v2"), r.get("rsi_14"), r.get("vol_bucket"))["buy_score"]
            hcore = compute_score_hits(dict(ultra_score_v3=v, ultra_score_v3_core=v, ultra_score=u, buy_score=b, prebreak_v2=r.get("prebreak_v2"),
                                            prebreak_v3=r.get("prebreak_v3"), conf_n=int(r.get("conf_n") or 0)))["score_hits"]
            us[i] = u; uc[i] = v; bs[i] = b; hc[i] = hcore
        keep = df[["ticker", "sig_date"] + BARS_CAT + BARS_BOOL + ["beta_score", "beta_zone", "profile_category", "conf_n", "tls_bar", "iv_vspike", "iv_dry"]].copy()
        keep["ultra_score"] = us; keep["uv3_core"] = uc; keep["buy_score"] = bs; keep["hits_core"] = hc
        out.append(keep); log(f"  bars chunk {h}: {len(df):,} rows ({time.time() - t0:.0f}s)")
        del df; gc.collect()
    con.close()
    return pd.concat(out, ignore_index=True)


def _layer_features(keys: pd.DataFrame) -> pd.DataFrame:
    import duckdb
    con = duckdb.connect()
    con.register("keys", keys[["ticker", "sig_date"]])
    q = lambda p, cols: con.execute(f"""SELECT k.ticker, k.sig_date, {cols} FROM keys k
                                        JOIN read_parquet('{p}') s ON s.ticker = k.ticker AND CAST(s.date AS VARCHAR) = k.sig_date""").fetchdf()
    lb = q(LAYERS["lbal"], "udn AS lbal_udn, udn_c AS lbal_udn_c, marks AS lbal_marks, colour AS lbal_colour, star AS lbal_star, conflict AS lbal_conflict, "
                          "half_up AS lbal_half_up, half_dn AS lbal_half_dn, nn AS lbal_nn, nv_pos AS lbal_nv_pos, nv_neg AS lbal_nv_neg, nv_l34 AS lbal_nv_l34, nv_l46 AS lbal_nv_l46")
    lv = q(LAYERS["lvx"], "fam AS lvx_fam, v AS lvx_v, tier AS lvx_tier")
    ov = q(LAYERS["ovdmap"], "ob30 AS ovd_ob30, ob60 AS ovd_ob60, rc30 AS ovd_rc30, rc60 AS ovd_rc60, cd30 AS ovd_cd30, cd60 AS ovd_cd60, ho30 AS ovd_ho30, ho60 AS ovd_ho60, nm AS ovd_nm, hv_event AS ovd_hv_event")
    v7 = q(LAYERS["vol7"], "jump AS vol7_jump, cons AS vol7_cons, vb2 AS vol7_vb2, shift_up AS vol7_shift_up, shift_dn AS vol7_shift_dn, mr AS vol7_mr, sg AS vol7_sg")
    con.close()
    # L-VX carries one row per (ticker, date, fam); keep one per day deterministically (L34 sorts before L46)
    lv = lv.sort_values(["ticker", "sig_date", "lvx_fam"]).drop_duplicates(["ticker", "sig_date"]).reset_index(drop=True)
    return lb, lv, ov, v7


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("RK_%Y%m%dT%H%M%SZ", time.gmtime()))
    opp = pd.read_parquet(OPP, columns=["ticker", "sig_date", "date_in", "setup", "family", "sig_rsi_14", "sig_close", "sig_conso", "sig_rs_intact"])
    opp["sig_date"] = opp["sig_date"].astype(str).str[:10]; opp["date_in"] = opp["date_in"].astype(str).str[:10]
    opp = opp[(opp["sig_date"] >= OOS["TRAIN_FROM"]) & (opp["sig_date"] <= OOS["VERIFY"][1]) & opp["family"].notna()].reset_index(drop=True)
    keys = opp[["ticker", "sig_date"]].drop_duplicates().reset_index(drop=True)
    log(f"fires {len(opp):,} · distinct (ticker, day) {len(keys):,} · families {opp['family'].nunique()} · {opp['sig_date'].min()}..{opp['sig_date'].max()}")
    ax, as_of = _axes_from_engine(log)
    bf = _bars_features(keys, ax, log)
    lb, lv, ov, v7 = _layer_features(keys)
    F = keys.merge(bf, on=["ticker", "sig_date"], how="left")
    F["miss_bars"] = F["bar_body_wick"].isna()
    for name, d in (("lbal", lb), ("lvx", lv), ("ovd", ov), ("vol7", v7)):
        F = F.merge(d, on=["ticker", "sig_date"], how="left")
        F[f"miss_{name}"] = F[d.columns[2]].isna()
    X = opp.merge(F, on=["ticker", "sig_date"], how="left")
    X["rsi_band"] = _band(X["sig_rsi_14"].to_numpy(float), RSI_BANDS)
    X["px_band"] = _band(X["sig_close"].to_numpy(float), PX_BANDS)
    X["conso"] = X["sig_conso"].fillna(0).astype(int).astype(bool); X["rs"] = X["sig_rs_intact"].fillna(0).astype(int).astype(bool)
    for c in SET_C["cat"]:
        X[c] = X[c].fillna("").astype(str).replace({"None": "", "nan": ""})
    for c in SET_C["bool"]:
        X[c] = X[c].fillna(False).astype(bool)
    for c in SET_C["num"]:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0.0).astype(float)
    X["in_mine"] = (X["sig_date"] >= OOS["MINE"][0]) & (X["sig_date"] <= OOS["MINE"][1])
    X["in_verify"] = (X["sig_date"] >= OOS["VERIFY"][0]) & (X["sig_date"] <= OOS["VERIFY"][1])
    # fixed vocabularies (X-only): levels with >= MIN_LEVEL fires, else OTHER
    vocab = {}
    for c in SET_C["cat"]:
        vc = X[c].value_counts(); vocab[c] = sorted([str(k) for k, n in vc.items() if n >= MIN_LEVEL])
    keep = list(dict.fromkeys(["ticker", "sig_date", "date_in", "setup", "sig_rsi_14", "sig_close"] + STATE + SET_C["cat"] + SET_C["bool"] + list(SET_C["num"]) + ["in_mine", "in_verify"]))
    X = X[keep].copy()
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    cov = {n: round(float(1 - X[f"miss_{n}"].mean()), 4) for n in ("bars", "lbal", "lvx", "ovd", "vol7")}
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, k_shrink=K_SHRINK, lam=LAMBDA, min_level=MIN_LEVEL, head_n=HEAD_N, min_day=MIN_DAY,
               sources=dict(opportunities=dict(path=OPP, sha256_16=_dig(OPP)), layers={k: dict(path=v, sha256_16=_dig(v)) for k, v in LAYERS.items()},
                            engine_frame_as_of=as_of, studio_db=DB),
               census=dict(fires=int(len(X)), keys=int(len(keys)), families=int(X["family"].nunique()), days=int(X["sig_date"].nunique()),
                           fires_per_year={int(y): int(n) for y, n in X.groupby(X["sig_date"].str[:4]).size().items()},
                           layer_coverage=cov, vocab_sizes={c: len(v) for c, v in vocab.items()},
                           state_cells=int(X.groupby(STATE).ngroups)),
               vocab=vocab, feature_sets=dict(A=STATE, B=SET_B, C=SET_C), x_sha256_16=_dig(xp))
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    log(f"X {len(X):,} fires · coverage {cov} · vocab {rep['census']['vocab_sizes']}")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "RK_*")), reverse=True):
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
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-07 (\"gidastureb gaakete\" on the 5-line plan)", "PRE_OUTCOME", "RANK_OVER_EDGE_FIRES"],
               question="do the newer display/score layers improve the proven state-conditioned rank on the HEAD (top-10 fires/day) in VERIFY?",
               unit="edge fire (ticker, sig_date, setup, family); HEAD on distinct tickers per day (max prediction over a ticker's fires)",
               variants=dict(A="hierarchical shrinkage table on " + " x ".join(STATE) + f" (K={K_SHRINK:.0f} to (family x rsi_band), then family, then train mean)",
                             B=f"A + ridge(lambda={LAMBDA:.0f}) on residual over display layers", C="B's layers + score features (hits_core, uv3_core, conf_n, beta_score, beta_zone)"),
               feature_sets=rep["feature_sets"], vocab=rep["vocab"],
               y="opportunities.parquet mtm_60 (sacred path-sim mark-to-market at 60 bars); edge = mtm_60 − same-day median over distinct opportunities (>= 20/day)",
               walk_forward=dict(score_years=SCORE_YEARS, rule="fit on sig_date < Jan 1 of the score year", train_from=OOS["TRAIN_FROM"]),
               oos=dict(MINE=OOS["MINE"], VERIFY=OOS["VERIFY"], note="MINE starts 2022-01-01: 2021 cannot be scored walk-forward (train-only)"),
               head=dict(n=HEAD_N, ties="ticker asc", min_day=MIN_DAY), bootstrap=dict(draws=BOOT, seed=SEED, ci=90),
               gates=dict(mine="mean(delta) > 0, CI90 lower > 0, >= 2/3 years delta > 0, DSR(head_v) >= 0.95 at k=3", verify="mean(delta) > 0 and both years delta > 0",
                          winner="C if BEATS_A and delta_C(VERIFY) >= delta_B(VERIFY); else B if BEATS_A; else A"),
               multiplicity=dict(k=K_EXPECTED, rule="DSR with the 3 variants' MINE head-series Sharpes"),
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], sources=rep["sources"], census=rep["census"],
               stop_rule="one outcome run; no feature / lambda / K search; RANK ships with the winner; UI wiring needs a second OK")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED, x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"],
             opportunities_sha256_16=rep["sources"]["opportunities"]["sha256_16"], oos=OOS,
             code=dict(rank_family=_dig(__file__), ultra_score=_dig(os.path.join(HERE, "ultra_score.py")), buy_score=_dig(os.path.join(HERE, "buy_score.py")),
                       edge_replay=_dig(os.path.join(HERE, "edge_replay.py"))), outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


# ── models ────────────────────────────────────────────────────────────────────────────────
def _fit_A(tr: pd.DataFrame, y: np.ndarray) -> dict:
    """Hierarchical shrinkage: cell -> (family, rsi_band) -> family -> global. Returns lookup dicts."""
    g = float(np.mean(y)) if len(y) else 0.0
    d = tr[STATE].copy(); d["y"] = y
    fam = d.groupby("family")["y"].agg(["sum", "count"])
    m_fam = {k: (r["sum"] + K_SHRINK * g) / (r["count"] + K_SHRINK) for k, r in fam.iterrows()}
    p1 = d.groupby(["family", "rsi_band"])["y"].agg(["sum", "count"])
    m_p1 = {k: (r["sum"] + K_SHRINK * m_fam.get(k[0], g)) / (r["count"] + K_SHRINK) for k, r in p1.iterrows()}
    cell = d.groupby(STATE)["y"].agg(["sum", "count"])
    m_cell = {k: (r["sum"] + K_SHRINK * m_p1.get((k[0], k[1]), m_fam.get(k[0], g))) / (r["count"] + K_SHRINK) for k, r in cell.iterrows()}
    return dict(g=g, fam=m_fam, p1=m_p1, cell=m_cell)


def _pred_A(model: dict, df: pd.DataFrame) -> np.ndarray:
    keys = list(zip(*[df[c].to_numpy() for c in STATE]))
    out = np.empty(len(df))
    for i, k in enumerate(keys):
        v = model["cell"].get(k)
        if v is None:
            v = model["p1"].get((k[0], k[1]))
            if v is None:
                v = model["fam"].get(k[0], model["g"])
        out[i] = v
    return out


def _design(df: pd.DataFrame, fs: dict, vocab: dict) -> tuple[np.ndarray, list[str]]:
    cols, names = [], []
    for c in fs["cat"]:
        v = df[c].astype(str).to_numpy(); levels = vocab.get(c, [])
        known = set(levels)
        for L in levels:
            cols.append((v == L).astype(np.float32)); names.append(f"{c}={L}")
        cols.append(np.array([x not in known and x != "" for x in v], dtype=np.float32)); names.append(f"{c}=OTHER")
    for c in fs["bool"]:
        cols.append(df[c].to_numpy(bool).astype(np.float32)); names.append(c)
    for c, sc in fs["num"].items():
        cols.append((df[c].to_numpy(float) / sc).astype(np.float32)); names.append(c)
    return np.column_stack(cols) if cols else np.zeros((len(df), 0), np.float32), names


def _fit_ridge(Xm: np.ndarray, r: np.ndarray) -> tuple[np.ndarray, float]:
    mu = r.mean(); Xc = Xm.astype(np.float64); xm = Xc.mean(axis=0); Xc = Xc - xm
    A = Xc.T @ Xc + LAMBDA * np.eye(Xc.shape[1]); b = Xc.T @ (r - mu)
    w = np.linalg.solve(A, b)
    return w, float(mu - xm @ w)


# ── outcome ───────────────────────────────────────────────────────────────────────────────
def _head_series(df: pd.DataFrame, pred_col: str) -> pd.Series:
    """per day: max prediction per ticker, top-HEAD_N tickers, mean edge."""
    d = df.groupby(["sig_date", "ticker"], sort=False).agg(p=(pred_col, "max"), e=("edge", "first"), n=("edge", "size")).reset_index()
    d = d.sort_values(["sig_date", "p", "ticker"], ascending=[True, False, True])
    d["rk"] = d.groupby("sig_date").cumcount()
    cnt = d.groupby("sig_date")["ticker"].transform("size")
    top = d[(d["rk"] < HEAD_N) & (cnt >= MIN_DAY)]
    return top.groupby("sig_date")["e"].mean()


def _daily_ic(df: pd.DataFrame, pred_col: str) -> pd.Series:
    d = df.groupby(["sig_date", "ticker"], sort=False).agg(p=(pred_col, "max"), e=("edge", "first")).reset_index()
    d["rp"] = d.groupby("sig_date")["p"].rank(); d["re"] = d.groupby("sig_date")["e"].rank()
    d["rp"] -= d.groupby("sig_date")["rp"].transform("mean"); d["re"] -= d.groupby("sig_date")["re"].transform("mean")
    d["ab"] = d["rp"] * d["re"]; d["aa"] = d["rp"] ** 2; d["bb"] = d["re"] ** 2
    s = d.groupby("sig_date").agg(ab=("ab", "sum"), aa=("aa", "sum"), bb=("bb", "sum"), n=("ab", "size"))
    s = s[s["n"] >= MIN_DAY]
    return (s["ab"] / np.sqrt(s["aa"] * s["bb"]).replace(0, np.nan)).dropna()


def _wstats(s: pd.Series, lo: str, hi: str) -> dict:
    w = s[(s.index >= lo) & (s.index <= hi)]
    if not len(w):
        return dict(n_days=0)
    yrs = pd.Index(w.index).str[:4]
    per_year = {str(y): round(float(w[yrs == y].mean()), 3) for y in sorted(set(yrs))}
    return dict(n_days=int(len(w)), mean=round(float(w.mean()), 3), median=round(float(w.median()), 3), day_win=round(float((w > 0).mean() * 100), 1),
                per_year=per_year, worst_year=min(per_year.values()), positive_years=sum(1 for v in per_year.values() if v > 0), years=len(per_year))


def _boot_ci(delta: pd.Series) -> tuple[float, float]:
    rng = np.random.default_rng(SEED); x = delta.to_numpy(float); n = len(x)
    if n < 5:
        return (float("nan"), float("nan"))
    m = np.array([x[rng.integers(0, n, n)].mean() for _ in range(BOOT)])
    return float(np.quantile(m, 0.05)), float(np.quantile(m, 0.95))


def outcome(log=print) -> dict:
    sp = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(sp):
        raise HardStop("not sealed")
    s = json.load(open(sp)); reg = json.load(open(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")))
    if _dig(os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json")) != s["registry_sha256_16"]:
        raise HardStop("registry digest mismatch")
    run_x = os.path.join(FAMILY_DIR, "runs", s["x_run"])
    if _dig(os.path.join(run_x, "X.parquet")) != s["x_sha256_16"] or _dig(OPP) != s["opportunities_sha256_16"]:
        raise HardStop("X / opportunities digest mismatch")
    O.assert_no_local_pathsim(open(__file__).read(), "rank")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), seal_registry=s["registry_sha256_16"]))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']}")
    X = pd.read_parquet(os.path.join(run_x, "X.parquet")); vocab = reg["vocab"]
    # ── Y: the opportunities' own sacred-pathsim mark-to-market; one value per (ticker, sig_date) ──
    Y = pd.read_parquet(OPP, columns=["ticker", "sig_date", "mtm_60"])
    Y["sig_date"] = Y["sig_date"].astype(str).str[:10]
    spread = Y.groupby(["ticker", "sig_date"])["mtm_60"].agg(["min", "max"])
    if float((spread["max"] - spread["min"]).abs().max()) > 1e-9:
        raise HardStop("mtm_60 differs across fires of one (ticker, sig_date) — the unit assumption is broken")
    Y = Y.groupby(["ticker", "sig_date"], as_index=False)["mtm_60"].first()
    Y = Y[Y["mtm_60"].notna()]
    Y["mtm_60"] = Y["mtm_60"].astype(float) * (100.0 if Y["mtm_60"].abs().median() < 1.0 else 1.0)
    dn = Y.groupby("sig_date")["mtm_60"].transform("size"); dm = Y.groupby("sig_date")["mtm_60"].transform("median")
    Y["edge"] = np.where(dn >= MIN_DAY, Y["mtm_60"] - dm, np.nan)
    df = X.merge(Y[["ticker", "sig_date", "edge", "mtm_60"]], on=["ticker", "sig_date"], how="inner")
    df = df[df["edge"].notna()].reset_index(drop=True)
    log(f"  fires with defined edge {len(df):,} · distinct keys {df.groupby(['ticker', 'sig_date']).ngroups:,} · days {df['sig_date'].nunique():,}")
    # ── walk-forward ──
    XB, nB = _design(df, SET_B, vocab); XC, nC = _design(df, SET_C, vocab)
    for v in ("A", "B", "C"):
        df[f"pred_{v}"] = np.nan
    yrs = df["sig_date"].str[:4].astype(int).to_numpy(); e = df["edge"].to_numpy(float)
    coefs = {}
    for t in SCORE_YEARS:
        tr = yrs < t; te = yrs == t
        if tr.sum() < 1000 or te.sum() == 0:
            continue
        mA = _fit_A(df[tr], e[tr]); pA_tr = _pred_A(mA, df[tr]); pA_te = _pred_A(mA, df[te])
        df.loc[te, "pred_A"] = pA_te
        wB, bB = _fit_ridge(XB[tr], e[tr] - pA_tr); df.loc[te, "pred_B"] = pA_te + XB[te] @ wB + bB
        wC, bC = _fit_ridge(XC[tr], e[tr] - pA_tr); df.loc[te, "pred_C"] = pA_te + XC[te] @ wC + bC
        coefs[str(t)] = dict(B=dict(zip(nB, [round(float(x), 4) for x in wB])), C=dict(zip(nC, [round(float(x), 4) for x in wC])))
        log(f"  year {t}: train {int(tr.sum()):,} → score {int(te.sum()):,}")
    sc = df[df["pred_A"].notna()].copy()
    heads = {v: _head_series(sc, f"pred_{v}") for v in ("A", "B", "C")}
    ics = {v: _daily_ic(sc, f"pred_{v}") for v in ("A", "B", "C")}
    pool = sc.groupby(["sig_date", "ticker"])["edge"].first().groupby("sig_date").mean()
    from overfit_stats import dsr, sharpe, psr
    trial = [sharpe(heads[v][(heads[v].index >= OOS["MINE"][0]) & (heads[v].index <= OOS["MINE"][1])].to_numpy()) for v in ("A", "B", "C")]
    results = {}
    for v in ("A", "B", "C"):
        h = heads[v]; hm = h[(h.index >= OOS["MINE"][0]) & (h.index <= OOS["MINE"][1])].to_numpy(float)
        r = dict(variant=v, head=dict(mine=_wstats(h, *OOS["MINE"]), verify=_wstats(h, *OOS["VERIFY"])),
                 ic=dict(mine=_wstats(ics[v], *OOS["MINE"]), verify=_wstats(ics[v], *OOS["VERIFY"])),
                 pool=dict(mine=_wstats(pool, *OOS["MINE"]), verify=_wstats(pool, *OOS["VERIFY"])))
        d = dsr(hm, trial, n_trials=K_EXPECTED) if len(hm) >= 3 else dict(dsr=None, sr=None, sr_star=None)
        r.update(dsr=d["dsr"], sr=d["sr"], sr_star=d["sr_star"], psr0=round(float(psr(hm, 0.0)), 4) if len(hm) >= 3 else None)
        if v != "A":
            delta = (heads[v] - heads["A"]).dropna()
            dm_ = delta[(delta.index >= OOS["MINE"][0]) & (delta.index <= OOS["MINE"][1])]; dv_ = delta[(delta.index >= OOS["VERIFY"][0]) & (delta.index <= OOS["VERIFY"][1])]
            lo_m, hi_m = _boot_ci(dm_); lo_v, hi_v = _boot_ci(dv_)
            sm = _wstats(dm_, *OOS["MINE"]); sv = _wstats(dv_, *OOS["VERIFY"])
            mine_ok = sm.get("mean", 0) > 0 and lo_m > 0 and sm.get("positive_years", 0) >= 2 and (r["dsr"] or 0) >= 0.95
            ver_ok = sv.get("mean", 0) > 0 and sv.get("positive_years", 0) == sv.get("years", 0) and sv.get("years", 0) >= 1
            r["delta_vs_A"] = dict(mine=dict(**sm, ci90=[round(lo_m, 3), round(hi_m, 3)]), verify=dict(**sv, ci90=[round(lo_v, 3), round(hi_v, 3)]),
                                   mine_pass=bool(mine_ok), verify_pass=bool(ver_ok))
            r["classification"] = "BEATS_A" if (mine_ok and ver_ok) else ("MINE_ONLY_NOT_REPLICATED" if mine_ok else "NULL")
        else:
            r["classification"] = "INCUMBENT"
        results[v] = r
        log(f"  {v}: HEAD MINE {r['head']['mine'].get('mean')} (pool {r['pool']['mine'].get('mean')}) win {r['head']['mine'].get('day_win')} | VERIFY {r['head']['verify'].get('mean')} (pool {r['pool']['verify'].get('mean')}) win {r['head']['verify'].get('day_win')} | IC {r['ic']['mine'].get('mean')} / {r['ic']['verify'].get('mean')} | DSR {r['dsr']} | {r['classification']}" +
            (f" | Δ MINE {r['delta_vs_A']['mine'].get('mean')} CI {r['delta_vs_A']['mine']['ci90']} · VERIFY {r['delta_vs_A']['verify'].get('mean')} CI {r['delta_vs_A']['verify']['ci90']}" if v != "A" else ""))
    dB = results["B"].get("delta_vs_A", {}).get("verify", {}).get("mean", -1e9); dC = results["C"].get("delta_vs_A", {}).get("verify", {}).get("mean", -1e9)
    winner = "C" if (results["C"]["classification"] == "BEATS_A" and dC >= dB) else ("B" if results["B"]["classification"] == "BEATS_A" else "A")
    # ── production model: fit the winner's components on ALL scored data (through the snapshot) ──
    mA = _fit_A(df, e); prodA = dict(g=mA["g"], fam={k: v for k, v in mA["fam"].items()}, p1={"|".join(map(str, k)): v for k, v in mA["p1"].items()},
                                     cell={"|".join(map(str, k)): v for k, v in mA["cell"].items()})
    pA_all = _pred_A(mA, df); prod = dict(winner=winner, A=prodA, state=STATE, k_shrink=K_SHRINK, lam=LAMBDA, vocab=vocab, fitted_on=dict(rows=int(len(df)), through=df["sig_date"].max()))
    if winner in ("B", "C"):
        fs = SET_B if winner == "B" else SET_C; Xw, nw = _design(df, fs, vocab); w, b = _fit_ridge(Xw, e - pA_all)
        prod["ridge"] = dict(feature_set=fs, names=nw, weights=[float(x) for x in w], bias=b)
    run.write_atomic("results.json", results); run.write_atomic("coefficients_walkforward.json", coefs); run.write_atomic("rank_v1_model.json", prod)
    sc[["ticker", "sig_date", "family", "setup", "edge", "pred_A", "pred_B", "pred_C"]].to_parquet(os.path.join(run.dir, "predictions.parquet"), index=False)
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED, winner=winner,
                classification={v: results[v]["classification"] for v in results}, fires=int(len(df)), scored_fires=int(len(sc)),
                outcome_access_count=led["outcome_access_count"], completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    json.dump(dict(record_id=f"{FAMILY}_FIRST_OUTCOME_EXECUTION", run_id=run.run_id, seal_registry_sha256_16=s["registry_sha256_16"], winner=winner,
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")), outcome_access_count=led["outcome_access_count"],
                   classification=summ["classification"], status="COMPLETE — family SEARCH-EXPOSED"),
              open(os.path.join(FAMILY_DIR, "FIRST_OUTCOME_EXECUTION.json"), "w"), indent=1)
    log(f"winner: {winner} · {summ['classification']}")
    return summ


def report(write: bool = True) -> str:
    runs = [r for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "OUT_*")), reverse=True) if RunSpace.is_current(r, "results.json")]
    if not runs:
        raise HardStop("no completed outcome run")
    run = runs[0]; res = json.load(open(os.path.join(run, "results.json"))); summ = json.load(open(os.path.join(run, "summary.json")))
    coefs = json.load(open(os.path.join(run, "coefficients_walkforward.json")))
    f = lambda v, d=2: "—" if v is None else f"{float(v):+.{d}f}"
    L = [f"# RANK_V1 — first outcome ({summ['run_id']})\n", f"Winner **{summ['winner']}** · classification {summ['classification']} · scored fires {summ['scored_fires']:,} · outcome access {summ['outcome_access_count']}\n",
         "## HEAD (top-10 distinct tickers per day, mean edge vs same-day median) and pool\n",
         "| variant | MINE head | pool | win% | yrs+ | worst | SR/day | SR* | DSR | VERIFY head | pool | win% | yrs+ | worst | IC MINE | IC VERIFY | class |", "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for v in ("A", "B", "C"):
        r = res[v]; hm, hv = r["head"]["mine"], r["head"]["verify"]
        L.append(f"| {v} | {f(hm.get('mean'))} | {f(r['pool']['mine'].get('mean'))} | {hm.get('day_win')} | {hm.get('positive_years')}/{hm.get('years')} | {f(hm.get('worst_year'))} | {f(r.get('sr'),3)} | {f(r.get('sr_star'),3)} | {f(r.get('dsr'))} | "
                 f"{f(hv.get('mean'))} | {f(r['pool']['verify'].get('mean'))} | {hv.get('day_win')} | {hv.get('positive_years')}/{hv.get('years')} | {f(hv.get('worst_year'))} | {f(r['ic']['mine'].get('mean'),4)} | {f(r['ic']['verify'].get('mean'),4)} | {r['classification']} |")
    L.append("\n## Δ vs A (paired day series of head edge)\n")
    L.append("| variant | MINE Δ mean | CI90 | yrs+ | VERIFY Δ mean | CI90 | yrs+ | MINE pass | VERIFY pass |"); L.append("|---|---:|---|---:|---:|---|---:|---|---|")
    for v in ("B", "C"):
        d = res[v]["delta_vs_A"]
        L.append(f"| {v} | {f(d['mine'].get('mean'))} | {d['mine']['ci90']} | {d['mine'].get('positive_years')}/{d['mine'].get('years')} | {f(d['verify'].get('mean'))} | {d['verify']['ci90']} | {d['verify'].get('positive_years')}/{d['verify'].get('years')} | {d['mine_pass']} | {d['verify_pass']} |")
    last = coefs[max(coefs)]
    for v in ("B", "C"):
        w = sorted(last[v].items(), key=lambda kv: -abs(kv[1]))[:20]
        L.append(f"\n## Largest ridge weights, variant {v} (last walk-forward fit, year {max(coefs)}) — descriptive\n")
        L.append("| feature | weight |"); L.append("|---|---:|")
        L += [f"| {k} | {x:+.3f} |" for k, x in w]
    txt = "\n".join(L)
    if write:
        open(os.path.join(FAMILY_DIR, "CHECKPOINT_FIRST_OUTCOME.md"), "w").write(txt)
    return txt


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items() if k in ("sealed_at", "registry_sha256_16", "k", "x_run", "x_sha256_16")}, indent=1)),
     "outcome": outcome,
     "report": lambda: print(report(write="--dry" not in sys.argv))}[cmd]()
