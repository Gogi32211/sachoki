"""SCORE_AUDIT_V1 — which of the app's scores carry RANK information beyond RSI band and price band?
Separate research family (user: "gaakete", 2026-09-07, on the 5-line plan). k = 20.

  Question   Of the 12 stored/served scores, UV3's 7 components and the 🎲 ensemble, which ones' HIGH group earns a
             positive day-clustered edge AFTER removing the (day × RSI band × price band) cell mean — in MINE — and
             replicates in VERIFY? Verdict per cell: RANKER / ANTI / CONTEXT / MINE_ONLY / THIN.
  X          Studio 1D bars, close >= 21, avg_vol_20d > 0, close*volume >= 3M, universe <> index, one row per
             (ticker, date). STRIDE-10 sample per ticker on the liquid row sequence (offset = hash(ticker) mod 10),
             declared here before any outcome: sampled rows are >= 10 sessions apart, so the engine's 5-bar cooldown
             never binds (feedback-pathsim-cooldown-estimand: the sample IS the mask, nothing is partitioned around it).
             Scores: turbo/prebreak_v2/prebreak_v3/beta/rtb_total/profile/aes/prebreak_score/gog as STORED (mixed-formula
             history — a declared limitation); ultra_score, ultra_score_v3 and buy_score RECOMPUTED with the pure
             functions (current formula on history); UV3 axes (rs_intact/conf_n/tls_bar/iv_vspike/iv_dry) from the
             engine frame edge_replay._frame(60, 3M) — the same source production injects live.
  Y          direct sacred edge_replay._pathsim (registered exit law, ATR x12, maxh 60), one call over the whole
             sample; control = the sample itself (the book's every-40th-bar control at 4x density): edge_raw = ret −
             same-day median (>= 20 trades/day); the DECIDING series is the cell-demeaned edge (ret − mean of the same
             (day, RSI band, price band) cell, >= 5 rows, thin cells fall back to (day, RSI band)), median per day over
             the cell's HIGH rows (every entry day with >= 1 HIGH row counts once). Bands fixed by the book, never
             searched: RSI 35/50/60, price 89/377. The first X build (SA_20260907T162007Z) is STALE: its "core" UV3
             still carried the axes; caught by the X census, fixed and rebuilt before anything was sealed.
  Gates      breadth_family.GATES (median > 0, day-win > 50, >= 3 positive years, worst >= −2, DSR >= 0.95 at k = 20)
             on MINE 2021-09-07..2024-12-31; VERIFY 2025-01-01..2026-09-03 replicates. Stop rule: no cell re-cut, no
             second HIGH rule, no band search. The 🎲 zone check (live axes-UV3 vs core-UV3) is DESCRIPTIVE only.
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
from breadth_family import GATES, classify, _stats_window                # noqa: E402

FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/SCORE_AUDIT_V1"
FAMILY = "SCORE_AUDIT_V1"
DB = "/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb"
OOS = dict(MINE=["2021-09-07", "2024-12-31"], VERIFY=["2025-01-01", "2026-09-03"])
STRIDE = 10
PX_MIN = 21.0
DV_FLOOR = 3_000_000
RSI_BANDS = ((-1e9, 35.0, "<35"), (35.0, 50.0, "35-50"), (50.0, 60.0, "50-60"), (60.0, 1e9, ">=60"))
PX_BANDS = ((21.0, 89.0, "21-89"), (89.0, 377.0, "89-377"), (377.0, 1e9, ">=377"))
MIN_DAY = 20          # trades on the day for the raw same-day median (O.MIN_CONTROL)
MIN_CELL = 5          # rows in a (day, band) cell for demeaning
MIN_HIGH = 1          # HIGH rows on a day for that day to enter the series — book practice: every entry day with >= 1 trade
                      # counts once (a >= 3 floor would starve the rare terms: TLS fires on ~1 % of rows, ~2 rows/day)
TOP_PCT = 0.8         # within-cell percentile rank >= 0.8 = "top quintile"
K_EXPECTED = 20
SCORES_STORED = ["turbo_score", "prebreak_v2", "prebreak_v3", "beta_score", "rtb_total", "profile_score",
                 "aes_score", "prebreak_score", "gog_score"]
SCORES_RECOMPUTED = ["ultra_score", "ultra_score_v3", "buy_score"]
SCORE_ORDER = ["turbo_score", "ultra_score", "ultra_score_v3", "buy_score", "prebreak_v2", "prebreak_v3",
               "beta_score", "rtb_total", "profile_score", "aes_score", "prebreak_score", "gog_score"]
# v3_pzv: the universe is close >= 21, so the term's only content is the −6 penalty on >= $377 (4.4 % of rows). Its
# HIGH group is that minority (the quality zone would be 96 % of rows and a cell-demeaned test of a 96 % group is
# attenuated x0.04 by construction — fixed BEFORE sealing from the X census). Expected direction: ANTI.
COMPONENTS = [("v3_earn", "gt0", "rsi_px"), ("v3_osv", "ge15", "px"), ("v3_pzv", "lt10", "rsi"),
              ("v3_rs", "gt0", "rsi_px"), ("v3_cluster", "gt0", "rsi_px"), ("v3_tls", "gt0", "rsi_px"),
              ("v3_vol", "gt0", "rsi_px")]
AXES = ["rs_intact", "conf_n", "tls_bar", "iv_vspike", "iv_dry"]
ROLE = {"BUILD_CANDIDATE": "RANKER", "VETO_CANDIDATE": "ANTI", "NULL": "CONTEXT",
        "MINE_ONLY_NOT_REPLICATED": "MINE_ONLY", "DESCRIPTIVE_ONLY": "THIN"}


def _dig(p, n=16):
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:n]


# ── registry ──────────────────────────────────────────────────────────────────────────────
def cells() -> list[dict]:
    c = []
    for s in SCORE_ORDER:
        c.append(dict(claim_id=f"S|{s}", kind="score", col=s, high=("gt0" if s == "gog_score" else "top_quintile"),
                      cond="rsi_px", in_k=True,
                      source=("recomputed (pure fn, current formula)" if s in SCORES_RECOMPUTED else "stored (bars column, mixed-formula history)")))
    for col, high, cond in COMPONENTS:
        c.append(dict(claim_id=f"C|{col}", kind="component", col=col, high=high, cond=cond, in_k=True,
                      source="term of compute_ultra_score_v3, asserted equal to the pure function on every row"))
    c.append(dict(claim_id="H|score_hits_live", kind="ensemble", col="score_hits_live", high="ge4", cond="rsi_px", in_k=True,
                  source="compute_score_hits on the LIVE-shaped row (axes-UV3), as the DB-instant scan serves it"))
    return c


def reference_cells() -> list[dict]:
    return [dict(claim_id="R|rsi_low", kind="reference", col="rsi_14", high="lt35", cond="px", in_k=False, source="the yardstick"),
            dict(claim_id="R|ultra_score_v3_core", kind="reference", col="ultra_score_v3_core", high="top_quintile", cond="rsi_px", in_k=False,
                 source="UV3 without axes = the stored column's formula = the Superchart UV3 row"),
            dict(claim_id="R|score_hits_core", kind="ensemble_reference", col="score_hits_core", high="ge4", cond="rsi_px", in_k=False,
                 source="compute_score_hits with core-UV3 — the 🎲 zone check reference"),
            dict(claim_id="R|v3_vol_veto", kind="reference", col="v3_dry", high="gt0", cond="rsi_px", in_k=False,
                 source="the −25 veto side of the volume-event axis (expected ANTI)")]


HIGH_RULES = {"top_quintile": f"within-(day x cond cell) percentile rank >= {TOP_PCT}", "gt0": "value > 0", "ge15": "value >= 15 (RSI < 35 tier)",
              "lt10": "value < 10 (the >= $377 penalty group; expected ANTI)", "ge4": "value >= 4", "lt35": "value < 35"}


def high_mask(X: pd.DataFrame, c: dict) -> np.ndarray:
    v = X[c["col"]].to_numpy(float)
    r = c["high"]
    if r == "top_quintile":
        return X[f"pct_{c['col']}"].to_numpy(float) >= TOP_PCT
    if r == "gt0":
        return v > 0
    if r == "ge15":
        return v >= 15
    if r == "lt10":
        return v < 10
    if r == "ge4":
        return v >= 4
    if r == "lt35":
        return v < 35
    raise HardStop(f"unknown HIGH rule {r}")


def _band(v: np.ndarray, bands) -> np.ndarray:
    out = np.full(len(v), "", dtype=object)
    for lo, hi, lab in bands:
        out[(v >= lo) & (v < hi)] = lab
    return out


# ── X build ───────────────────────────────────────────────────────────────────────────────
def _sample_sql() -> str:
    return f"""
    WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn FROM bars
               WHERE close >= {PX_MIN} AND avg_vol_20d > 0 AND close*volume >= {DV_FLOOR} AND universe <> 'index'
                 AND date BETWEEN '{OOS["MINE"][0]}' AND '{OOS["VERIFY"][1]}'),
         u AS (SELECT * EXCLUDE (rn) FROM r WHERE rn = 1),
         s AS (SELECT *, row_number() OVER (PARTITION BY ticker ORDER BY date) - 1 AS pos, hash(ticker) % {STRIDE} AS off FROM u)
    SELECT * EXCLUDE (pos, off) FROM s WHERE pos % {STRIDE} = off ORDER BY ticker, date"""


def _axes_from_engine(log=print) -> pd.DataFrame:
    """X-only: the engine frame's UV3 axes per (ticker, session). Same source production injects live."""
    import edge_replay as E
    t0 = time.time()
    grp, as_of = E._frame(60, float(DV_FLOOR))
    parts = []
    for tk, g in grp.items():
        cols = [c for c in AXES + ["clean"] if c in g.columns]
        p = g[["date"] + cols].copy(); p["ticker"] = tk; parts.append(p)
    ax = pd.concat(parts, ignore_index=True)
    ax["session"] = ax["date"].astype(str).str[:10]; ax = ax.drop(columns=["date"])
    for c in AXES + ["clean"]:
        if c not in ax.columns:
            ax[c] = 0
    ax["rs_intact"] = ax["rs_intact"].astype(bool); ax["tls_bar"] = ax["tls_bar"].astype(bool)
    ax["iv_vspike"] = ax["iv_vspike"].astype(bool); ax["iv_dry"] = ax["iv_dry"].astype(bool)
    ax["conf_n"] = ax["conf_n"].fillna(0).astype(int); ax["clean"] = ax["clean"].astype(bool)
    log(f"  engine frame: {len(grp):,} tickers, {len(ax):,} rows, as_of {as_of}, {time.time() - t0:.0f}s")
    try:
        E._CACHE.clear()
    except Exception:
        pass
    del grp; gc.collect()
    return ax, str(as_of)


def _v3_terms(row: dict):
    """The seven UV3 terms, mirrored from compute_ultra_score_v3 and asserted equal to it on every row (build-time)."""
    from ultra_score import _signal_set, _truthy, _safe_float, _V3_OSV, _V3_PXV, _V3_PX_DEFAULT, _V3_VOL_EVENT_BONUS, _V3_NO_VOL_VETO
    sigs = _signal_set(row); earn = 0
    if "BX_UP" in sigs:
        earn += 12
    if "STR" in sigs:
        earn += 8
    if _truthy(row.get("d_absorb_bull")):
        earn += 15
    earn = min(earn, 25)
    rsi = _safe_float(row.get("rsi"), default=50.0); osv = -18
    for cut, pts in _V3_OSV:
        if rsi < cut:
            osv = pts; break
    px = _safe_float(row.get("last_price") or row.get("price") or row.get("close"), 0.0); pzv = _V3_PX_DEFAULT
    for cut, pts in _V3_PXV:
        if px < cut:
            pzv = pts; break
    rs = 12 if _truthy(row.get("rs_intact")) else 0
    cn = int(_safe_float(row.get("conf_n"), 0.0)); cl = min(cn, 6) * 4 if cn >= 3 else 0
    tls = 10 if _truthy(row.get("tls_bar")) else 0
    vol = -_V3_NO_VOL_VETO if _truthy(row.get("no_vol_event")) else (_V3_VOL_EVENT_BONUS if _truthy(row.get("iv_vspike")) else 0)
    return earn, osv, pzv, rs, cl, tls, vol


def _recompute(df: pd.DataFrame, log=print, chunk: int = 4000) -> pd.DataFrame:
    from ultra_score import compute_ultra_score, compute_ultra_score_v3, compute_score_hits
    from buy_score import compute_buy_score
    n = len(df); out = {k: np.full(n, np.nan) for k in ["ultra_score", "ultra_score_v3", "ultra_score_v3_core", "buy_score",
                                                          "v3_earn", "v3_osv", "v3_pzv", "v3_rs", "v3_cluster", "v3_tls", "v3_vol", "v3_dry",
                                                          "score_hits_live", "score_hits_core"]}
    mism = 0; t0 = time.time()
    for s in range(0, n, chunk):
        rows = df.iloc[s:s + chunk].to_dict("records")
        for i, r in enumerate(rows):
            j = s + i
            # CORE = the pure function WITHOUT axes (the stored column's formula). The merged row already carries the
            # engine axes, so they must be stripped explicitly — the first X build (SA_20260907T162007Z) missed this and
            # its "core" was live-minus-veto; caught by the X census (live-only rows = 0) and rebuilt before sealing.
            base = dict(r, rsi=r.get("rsi_14"), last_price=r.get("close"), rs_intact=False, conf_n=0, tls_bar=False, iv_vspike=False, no_vol_event=False)
            live = dict(base, rs_intact=bool(r.get("rs_intact")), conf_n=int(r.get("conf_n") or 0), tls_bar=bool(r.get("tls_bar")),
                        iv_vspike=bool(r.get("iv_vspike")), no_vol_event=bool(r.get("iv_dry")))
            us = compute_ultra_score(r)["ultra_score"]
            v3l = compute_ultra_score_v3(live)["ultra_score_v3"]
            v3c = compute_ultra_score_v3(base)["ultra_score_v3"]
            terms = _v3_terms(live)
            if max(0, min(100, int(round(sum(terms))))) != v3l:
                mism += 1
            bs = compute_buy_score(r.get("prebreak_v2"), r.get("rsi_14"), r.get("vol_bucket"))["buy_score"]
            hl = compute_score_hits(dict(ultra_score_v3=v3l, ultra_score=us, buy_score=bs, prebreak_v2=r.get("prebreak_v2"),
                                         prebreak_v3=r.get("prebreak_v3"), conf_n=(int(r.get("conf_n") or 0) if r.get("axes_defined") else None)))["score_hits"]
            hc = compute_score_hits(dict(ultra_score_v3=v3c, ultra_score=us, buy_score=bs, prebreak_v2=r.get("prebreak_v2"),
                                         prebreak_v3=r.get("prebreak_v3"), conf_n=(int(r.get("conf_n") or 0) if r.get("axes_defined") else None)))["score_hits"]
            out["ultra_score"][j] = us; out["ultra_score_v3"][j] = v3l; out["ultra_score_v3_core"][j] = v3c; out["buy_score"][j] = bs
            for k, v in zip(["v3_earn", "v3_osv", "v3_pzv", "v3_rs", "v3_cluster", "v3_tls", "v3_vol"], terms):
                out[k][j] = v
            out["v3_dry"][j] = 1.0 if live["no_vol_event"] else 0.0
            out["score_hits_live"][j] = hl; out["score_hits_core"][j] = hc
        if (s // chunk) % 10 == 0:
            log(f"  recompute {min(s + chunk, n):,}/{n:,} ({time.time() - t0:.0f}s)")
    if mism:
        raise HardStop(f"UV3 term decomposition != compute_ultra_score_v3 on {mism} rows")
    for k, v in out.items():
        df[k] = v
    return df


def _pct_within(X: pd.DataFrame, col: str, keys: list[str]) -> np.ndarray:
    v = X[col]
    return v.groupby([X[k] for k in keys]).rank(method="average", pct=True).to_numpy(float)


def build(log=print) -> dict:
    B.assert_no_pathsim_copy(__file__)
    cur = B.canonical_current()
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("SA_%Y%m%dT%H%M%SZ", time.gmtime()))
    ax, as_of = _axes_from_engine(log)
    import duckdb
    con = duckdb.connect(DB, read_only=True)
    try:
        t0 = time.time(); df = con.execute(_sample_sql()).fetchdf(); log(f"  sample pulled: {len(df):,} rows x {df.shape[1]} cols ({time.time() - t0:.0f}s)")
    finally:
        con.close()
    df["session"] = df["date"].astype(str).str[:10]
    n0 = len(df)
    df = df[df["rsi_14"].notna()].reset_index(drop=True)
    df = df.merge(ax, on=["ticker", "session"], how="left")
    df["axes_defined"] = df["rs_intact"].notna()
    for c in ["rs_intact", "tls_bar", "iv_vspike", "iv_dry", "clean"]:
        df[c] = df[c].fillna(False).astype(bool)
    df["conf_n"] = df["conf_n"].fillna(0).astype(int)
    df = _recompute(df, log)
    df["rsi_band"] = _band(df["rsi_14"].to_numpy(float), RSI_BANDS)
    df["px_band"] = _band(df["close"].to_numpy(float), PX_BANDS)
    keep = ["ticker", "session", "universe", "close", "volume", "rsi_14", "vol_bucket", "rsi_band", "px_band", "axes_defined", "clean"] + AXES + \
           SCORE_ORDER + ["ultra_score_v3_core", "v3_earn", "v3_osv", "v3_pzv", "v3_rs", "v3_cluster", "v3_tls", "v3_vol", "v3_dry",
                          "score_hits_live", "score_hits_core"]
    X = df[keep].copy(); del df; gc.collect()
    for s in SCORES_STORED:
        X[s] = X[s].astype(float)
    # within-(day x RSI band x price band) percentile ranks for the top-quintile HIGH rule (X-only)
    for col in SCORE_ORDER + ["ultra_score_v3_core"]:
        X[f"pct_{col}"] = _pct_within(X, col, ["session", "rsi_band", "px_band"])
    X["in_mine"] = (X["session"] >= OOS["MINE"][0]) & (X["session"] <= OOS["MINE"][1])
    X["in_verify"] = (X["session"] >= OOS["VERIFY"][0]) & (X["session"] <= OOS["VERIFY"][1])
    xp = os.path.join(run.dir, "X.parquet"); X.to_parquet(xp, index=False)
    per_day = X.groupby("session").size()
    prev = {}
    for c in cells() + reference_cells():
        m = high_mask(X, c)
        prev[c["claim_id"]] = dict(high_mine=int((m & X["in_mine"]).sum()), high_verify=int((m & X["in_verify"]).sum()),
                                   share_mine=round(float(m[X["in_mine"].to_numpy()].mean()), 4))
    census = dict(rows_pulled=int(n0), rows=int(len(X)), tickers=int(X["ticker"].nunique()), dates=int(X["session"].nunique()),
                  rows_per_day=dict(min=int(per_day.min()), median=float(per_day.median()), max=int(per_day.max())),
                  rows_per_year={int(y): int(n) for y, n in X.groupby(X["session"].str[:4]).size().items()},
                  axes_defined_share=round(float(X["axes_defined"].mean()), 4), engine_frame_as_of=as_of,
                  score_nonnull={s: round(float(X[s].notna().mean()), 4) for s in SCORE_ORDER},
                  score_nonzero={s: round(float((X[s].fillna(0) != 0).mean()), 4) for s in SCORE_ORDER},
                  band_shares=dict(rsi=X["rsi_band"].value_counts(normalize=True).round(4).to_dict(), px=X["px_band"].value_counts(normalize=True).round(4).to_dict()),
                  high_prevalence=prev,
                  uv3_zone=dict(live_gt25_share=round(float((X["ultra_score_v3"] > 25).mean()), 4), core_gt25_share=round(float((X["ultra_score_v3_core"] > 25).mean()), 4),
                                hits_live=X["score_hits_live"].value_counts().sort_index().to_dict(), hits_core=X["score_hits_core"].value_counts().sort_index().to_dict()))
    rep = dict(run_id=run.run_id, family=FAMILY, oos=OOS, stride=STRIDE, px_min=PX_MIN, dv_floor=DV_FLOOR, bands=dict(rsi=[b[2] for b in RSI_BANDS], px=[b[2] for b in PX_BANDS]),
               canonical_1d=dict(run_id=cur["run_id"], derived_sha256_16=cur["derived_sha256_16"]), studio_db=dict(path=DB, sha256_16=None),
               census=census, x_sha256_16=_dig(xp), self_check="UV3 term decomposition == compute_ultra_score_v3 on every row")
    run.write_atomic("build_report.json", rep); run.complete("X.parquet")
    log(f"X {len(X):,} rows · {census['tickers']:,} tickers · {census['dates']:,} days · axes {census['axes_defined_share']:.3f} · "
        f"UV3>25 live {census['uv3_zone']['live_gt25_share']:.3f} vs core {census['uv3_zone']['core_gt25_share']:.3f}")
    return rep


def latest_run():
    for r in sorted(glob.glob(os.path.join(FAMILY_DIR, "runs", "SA_*")), reverse=True):
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
    reg = dict(registry_id=f"{FAMILY}_SEARCH_REGISTRY", family=FAMILY, status="SEALED", sealed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               provenance=["USER_APPROVED_2026-09-07 (\"gaakete\" on the 5-line plan)", "PRE_OUTCOME", "PRE_PATHSIM", "AUDIT_OF_EXISTING_SCORES"],
               question="which scores / UV3 terms / the 🎲 ensemble carry rank information beyond RSI band x price band, day-clustered, MINE -> VERIFY",
               sample=dict(rule=f"stride-{STRIDE} per ticker on the liquid row sequence, offset hash(ticker) mod {STRIDE}", universe=f"close >= {PX_MIN}, avg_vol_20d > 0, close*volume >= {DV_FLOOR}, universe <> index",
                           cooldown_note="sampled rows are >= 10 sessions apart; the engine's 5-bar cooldown cannot bind; nothing is partitioned around it"),
               scores=dict(stored=SCORES_STORED, recomputed=SCORES_RECOMPUTED, axes_source="edge_replay._frame(60, 3M) — the production axes source",
                           limitation="stored columns are mixed-formula history; prebreak_v3 stored SQL still carries SVS +5 / CCI0R +3 that the live python dropped"),
               control="the sample itself: edge_raw = ret − same-day median (>= 20 trades that day)",
               deciding_series=dict(edge="ret − mean over the same (day, RSI band, price band) cell (>= 5 rows; thin cell -> (day, RSI band))",
                                    cond_variants=dict(rsi_px="day x RSI band x price band", px="day x price band", rsi="day x RSI band"),
                                    aggregation="median per day over the cell's HIGH rows (>= 3 HIGH rows/day) -> breadth_family._stats_window (O.day_stats order)"),
               bands=dict(rsi=[b[2] for b in RSI_BANDS], px=[b[2] for b in PX_BANDS], rule="book bands, fixed, never searched"),
               high_rules=HIGH_RULES, top_pct=TOP_PCT, min_day=MIN_DAY, min_cell=MIN_CELL, min_high=MIN_HIGH,
               estimand="direct sacred edge_replay._pathsim, registered exit law (ATR x12, maxh 60, slip 0.0015), 5-bar cooldown INCLUDED; price-return paths",
               oos=OOS, gates=GATES, multiplicity=dict(k=K_EXPECTED, rule="DSR with the 20 in-k cells' MINE day-series Sharpes; n_trials = 20; reference rows not in k"),
               cells=cells(), reference_cells=reference_cells(), role_map=ROLE,
               descriptives="raw same-day-median ladders by within-day quintile, daily rank-IC (raw and conditional), 🎲 ladders live vs core, UV3>25 hit rates — DESCRIPTIVE, not gated",
               x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"], canonical_1d=rep["canonical_1d"], census=rep["census"],
               stop_rule="one outcome run; no cell re-cut, no second HIGH rule, no band search; formula or UI change only with a second user OK")
    rp = os.path.join(FAMILY_DIR, "SEARCH_REGISTRY.json"); json.dump(reg, open(rp, "w"), indent=1, default=str)
    s = dict(family=FAMILY, sealed_at=reg["sealed_at"], registry_sha256_16=_dig(rp), k=K_EXPECTED, x_run=rep["run_id"], x_sha256_16=rep["x_sha256_16"],
             canonical_1d=rep["canonical_1d"], oos=OOS, gates=GATES, pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, slip=slip,
             code=dict(score_audit_family=_dig(__file__), edge_replay=_dig(os.path.join(HERE, "edge_replay.py")), ovd_outcome_v1=_dig(O.__file__),
                       ovd_outcome_direct_v1=_dig(OD.__file__), ultra_score=_dig(os.path.join(HERE, "ultra_score.py")), buy_score=_dig(os.path.join(HERE, "buy_score.py")),
                       breadth_family=_dig(os.path.join(HERE, "breadth_family.py"))),
             outcome_access_count=0, rule="registry frozen; outcome phase is a SEPARATE command")
    json.dump(s, open(os.path.join(FAMILY_DIR, "SEAL.json"), "w"), indent=1, default=str)
    return s


# ── outcome ───────────────────────────────────────────────────────────────────────────────
def _demean(df: pd.DataFrame, cond: str, stat: str = "mean") -> np.ndarray:
    """ret − cell centre. stat='mean' was the sealed first run; AMENDMENT_1 uses stat='median' (the book's control is a
    same-day MEDIAN): with right-skewed exit-law returns the median of mean-demeaned returns is biased negative for EVERY
    subgroup (VERIFY: −1.10 over all rows, 23 % of days > 0), which made the replication column unreadable in run 1."""
    keys = {"rsi_px": ["date_in", "rsi_band", "px_band"], "px": ["date_in", "px_band"], "rsi": ["date_in", "rsi_band"]}[cond]
    g = df.groupby(keys)["ret"]; n = g.transform("size").to_numpy(); m = g.transform(stat).to_numpy()
    e = np.where(n >= MIN_CELL, df["ret"].to_numpy(float) - m, np.nan)
    if cond == "rsi_px":
        g2 = df.groupby(["date_in", "rsi_band"])["ret"]; n2 = g2.transform("size").to_numpy(); m2 = g2.transform(stat).to_numpy()
        e = np.where(n >= MIN_CELL, e, np.where(n2 >= MIN_CELL, df["ret"].to_numpy(float) - m2, np.nan))
    return e


def _daily_ic(df: pd.DataFrame, col: str, ycol: str) -> dict:
    d = df[[col, ycol, "date_in", "in_mine", "in_verify"]].dropna()
    r1 = d.groupby("date_in")[col].rank(); r2 = d.groupby("date_in")[ycol].rank()
    t = pd.DataFrame(dict(a=r1, b=r2, d=d["date_in"], m=d["in_mine"], v=d["in_verify"]))
    t["a"] -= t.groupby("d")["a"].transform("mean"); t["b"] -= t.groupby("d")["b"].transform("mean")
    t["ab"] = t["a"] * t["b"]; t["aa"] = t["a"] ** 2; t["bb"] = t["b"] ** 2
    s = t.groupby("d").agg(ab=("ab", "sum"), aa=("aa", "sum"), bb=("bb", "sum"), n=("ab", "size"), m=("m", "first"), v=("v", "first"))
    s = s[s["n"] >= MIN_DAY]; ic = s["ab"] / np.sqrt(s["aa"] * s["bb"]).replace(0, np.nan)
    out = {}
    for w, mask in (("mine", s["m"]), ("verify", s["v"])):
        x = ic[mask.to_numpy(bool)].dropna().to_numpy(float)
        out[w] = dict(n_days=int(len(x)), mean_ic=round(float(x.mean()), 4) if len(x) else None,
                      t_stat=round(float(x.mean() / x.std(ddof=1) * np.sqrt(len(x))), 2) if len(x) > 2 and x.std(ddof=1) > 0 else None,
                      share_pos=round(float((x > 0).mean() * 100), 1) if len(x) else None)
    return out


def _ladder(df: pd.DataFrame, col: str, bins: list, ycol: str = "edge_raw") -> dict:
    out = {}
    for w in ("mine", "verify"):
        m = df["in_mine"] if w == "mine" else df["in_verify"]
        d = df[m.to_numpy(bool) & df[ycol].notna()]
        lad = []
        for lo, hi, lab in bins:
            x = d[(d[col] >= lo) & (d[col] < hi)]
            lad.append(dict(bin=lab, n=int(len(x)), n_days=int(x["date_in"].nunique()), median=round(float(x[ycol].median()), 3) if len(x) else None,
                            mean=round(float(x[ycol].mean()), 3) if len(x) else None, win=round(float((x["ret"] > 0).mean() * 100), 1) if len(x) else None))
        out[w] = lad
    return out


def outcome(log=print, demean_stat: str = "mean", amendment: str | None = None) -> dict:
    """First run: demean_stat='mean' (as sealed). AMENDMENT_1 (post-exposure, declared in AMENDMENT_1.md): the same
    cells / HIGH rules / bands / gates / k, only the cell centre becomes the MEDIAN. The first run stays immutable."""
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
    ps, _ = O.sacred_pathsim(); O.assert_no_local_pathsim(open(__file__).read(), "sa")
    ledger_p = os.path.join(FAMILY_DIR, "OUTCOME_ACCESS_LEDGER.json")
    led = json.load(open(ledger_p)) if os.path.exists(ledger_p) else dict(outcome_access_count=0, events=[])
    if amendment and not os.path.exists(os.path.join(FAMILY_DIR, f"{amendment}.md")):
        raise HardStop(f"{amendment}.md must be written (declared) before the amended run")
    if amendment and led["outcome_access_count"] < 1:
        raise HardStop("an amendment is a POST-exposure correction only")
    run = RunSpace(family_dir=FAMILY_DIR, run_id=time.strftime("OUT_%Y%m%dT%H%M%SZ", time.gmtime()) + (f"_{amendment}" if amendment else ""))
    led["outcome_access_count"] += 1
    led["events"].append(dict(run_id=run.run_id, at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), seal_registry=s["registry_sha256_16"],
                              demean_stat=demean_stat, amendment=amendment))
    json.dump(led, open(ledger_p, "w"), indent=1)
    log(f"{run.run_id}: outcome access -> {led['outcome_access_count']} (demean {demean_stat}{', ' + amendment if amendment else ''})")
    X = pd.read_parquet(os.path.join(run_x, "X.parquet"))
    t0 = time.time(); frames = OD.load_frames(cur["derived_parquet"], X["ticker"].unique())
    in_canon = np.zeros(len(X), bool)
    for tk, g in X.groupby("ticker").groups.items():
        f = frames.get(tk)
        if f is not None:
            in_canon[np.asarray(g)] = X.loc[g, "session"].isin(set(f["date"])).to_numpy()
    n_not = int((~in_canon).sum()); X = X[in_canon].reset_index(drop=True)
    log(f"  canonical frames {len(frames):,} tickers ({time.time() - t0:.0f}s); NOT_IN_CANONICAL {n_not:,} rows dropped; {len(X):,} signal rows")
    t0 = time.time(); tr, cons = OD.direct_trades(ps, frames, X[["ticker", "session"]])
    cons["NOT_IN_CANONICAL"] = n_not; log(f"  direct trades {len(tr):,} ({time.time() - t0:.0f}s): {cons}")
    run.write_atomic("selection_conservation.json", cons)
    tr.to_parquet(os.path.join(run.dir, "direct_trades.parquet"), index=False)
    df = X.merge(tr, on=["ticker", "session"], how="inner")
    df = df[df["ret"].notna()].reset_index(drop=True)
    day_n = df.groupby("date_in")["ret"].transform("size").to_numpy(); day_med = df.groupby("date_in")["ret"].transform("median").to_numpy()
    df["edge_raw"] = np.where(day_n >= MIN_DAY, df["ret"].to_numpy(float) - day_med, np.nan)
    df["in_mine"] = (df["date_in"] >= OOS["MINE"][0]) & (df["date_in"] <= OOS["MINE"][1])
    df["in_verify"] = (df["date_in"] >= OOS["VERIFY"][0]) & (df["date_in"] <= OOS["VERIFY"][1])
    edge_c = {cond: _demean(df, cond, demean_stat) for cond in ("rsi_px", "px", "rsi")}
    # centring check (all rows): the day-median of the demeaned edge must sit at ~0 in BOTH windows for the series to be readable
    centre = {}
    for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
        m = ((df["date_in"] >= lo) & (df["date_in"] <= hi)).to_numpy() & np.isfinite(edge_c["rsi_px"])
        d = pd.Series(edge_c["rsi_px"][m]).groupby(df.loc[m, "date_in"].to_numpy()).median()
        centre[w] = dict(all_rows_day_median=round(float(d.median()), 3), days_pos=round(float((d > 0).mean() * 100), 1))
    log(f"  centring (all rows, {demean_stat}): {centre}")
    results, mine_series = [], {}
    for c in reg["cells"] + reg["reference_cells"]:
        hm = high_mask(df, c); e = edge_c[c["cond"]]
        sel = hm & np.isfinite(e) & np.isfinite(df["edge_raw"].to_numpy(float))
        obs = df[sel].copy(); obs["edge"] = e[sel]
        cnt = obs.groupby("date_in")["edge"].transform("size"); obs = obs[cnt >= MIN_HIGH]
        mine = _stats_window(obs, *OOS["MINE"]); ver = _stats_window(obs, *OOS["VERIFY"])
        raw = {}
        for w, lo, hi in (("mine", *OOS["MINE"]), ("verify", *OOS["VERIFY"])):
            o = obs[(obs["date_in"] >= lo) & (obs["date_in"] <= hi)]
            d = o.groupby("date_in")["edge_raw"].median()
            raw[w] = dict(n_days=int(len(d)), median_day_edge_raw=round(float(d.median()), 3) if len(d) else None, day_win_raw=round(float((d > 0).mean() * 100), 1) if len(d) else None)
        results.append(dict(claim_id=c["claim_id"], kind=c["kind"], col=c["col"], high=c["high"], cond=c["cond"], in_k=bool(c["in_k"]),
                            high_rows=int(hm.sum()), mine={k: v for k, v in mine.items() if k != "day_series"}, verify={k: v for k, v in ver.items() if k != "day_series"},
                            raw=raw))
        mine_series[c["claim_id"]] = mine["day_series"]
        log(f"  {c['claim_id']:24s} HIGH {int(hm.sum()):7,d}  MINE med {mine['median_edge']} win {mine['day_win']} yrs {mine['positive_years']}/{mine['years_counted']} worst {mine['worst_year']} | VERIFY med {ver['median_edge']} win {ver['day_win']} worst {ver['worst_year']}")
    from overfit_stats import dsr, sharpe, psr
    kids = [r["claim_id"] for r in results if r["in_k"]]
    if len(kids) != K_EXPECTED:
        raise HardStop(f"k mismatch: {len(kids)} != {K_EXPECTED}")
    trial = [sharpe(mine_series[k].to_numpy()) if len(mine_series[k]) >= 2 else 0.0 for k in kids]
    for r in results:
        srs = mine_series[r["claim_id"]].to_numpy(float)
        if len(srs) >= 3:
            dp = dsr(srs, trial, n_trials=K_EXPECTED); dn = dsr(-srs, [-x for x in trial], n_trials=K_EXPECTED)
            r.update(dsr=dp["dsr"], sr=dp["sr"], sr_star=dp["sr_star"], dsr_neg=dn["dsr"], psr0=round(float(psr(srs, 0.0)), 4), n_days_mine=int(len(srs)))
        else:
            r.update(dsr=None, sr=None, sr_star=None, dsr_neg=None, psr0=None, n_days_mine=int(len(srs)))
        r["demean_stat"] = demean_stat; r["amendment"] = amendment
        r["classification"] = classify(r["mine"], r["verify"], r["dsr"], r["dsr_neg"])
        r["role"] = ROLE.get(r["classification"], r["classification"]) if r["in_k"] else f"REFERENCE:{ROLE.get(r['classification'], r['classification'])}"
    run.write_atomic("results.json", results)
    # ── descriptives (not gated) ──
    q5 = [(0.0, 0.2, "Q1"), (0.2, 0.4, "Q2"), (0.4, 0.6, "Q3"), (0.6, 0.8, "Q4"), (0.8, 1.01, "Q5")]
    desc = dict(ladders={}, ic={}, hits={}, uv3_zone={})
    df["edge_rsi_px"] = edge_c["rsi_px"]
    for col in SCORE_ORDER + ["ultra_score_v3_core"]:
        df[f"dpct_{col}"] = df[col].groupby(df["date_in"]).rank(method="average", pct=True)
        desc["ladders"][col] = _ladder(df, f"dpct_{col}", q5)
        desc["ic"][col] = dict(raw=_daily_ic(df, col, "edge_raw"), conditional=_daily_ic(df, f"pct_{col}", "edge_rsi_px"))
    hb = [(i, i + 1, str(i)) for i in range(0, 7)]
    desc["hits"] = dict(live=_ladder(df, "score_hits_live", hb), core=_ladder(df, "score_hits_core", hb))
    for w in ("mine", "verify"):
        m = (df["in_mine"] if w == "mine" else df["in_verify"]).to_numpy(bool)
        d = df[m & df["edge_raw"].notna()]
        desc["uv3_zone"][w] = dict(live_gt25_share=round(float((d["ultra_score_v3"] > 25).mean()), 4), core_gt25_share=round(float((d["ultra_score_v3_core"] > 25).mean()), 4),
                                   live_gt25_median=round(float(d.loc[d["ultra_score_v3"] > 25, "edge_raw"].median()), 3) if (d["ultra_score_v3"] > 25).any() else None,
                                   core_gt25_median=round(float(d.loc[d["ultra_score_v3_core"] > 25, "edge_raw"].median()), 3) if (d["ultra_score_v3_core"] > 25).any() else None,
                                   live_only_median=round(float(d.loc[(d["ultra_score_v3"] > 25) & ~(d["ultra_score_v3_core"] > 25), "edge_raw"].median()), 3)
                                   if ((d["ultra_score_v3"] > 25) & ~(d["ultra_score_v3_core"] > 25)).any() else None,
                                   axes_defined_share=round(float(d["axes_defined"].mean()), 4))
    run.write_atomic("descriptives.json", desc)
    cls_census, role_census = {}, {}
    for r in results:
        if r["in_k"]:
            cls_census[r["classification"]] = cls_census.get(r["classification"], 0) + 1
            role_census[r["role"]] = role_census.get(r["role"], 0) + 1
    summ = dict(run_id=run.run_id, family=FAMILY, registry_sha256_16=s["registry_sha256_16"], k=K_EXPECTED, oos=OOS, gates=GATES, demean_stat=demean_stat,
                amendment=amendment, centring=centre, trial_sharpes={k: round(float(v), 4) for k, v in zip(kids, trial)},
                pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, outcome_access_count=led["outcome_access_count"], classification_census=cls_census, role_census=role_census,
                trades=int(len(df)), days=int(df["date_in"].nunique()), completed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    run.write_atomic("summary.json", summ); run.complete("results.json")
    rec_name = f"{FAMILY}_{amendment}_EXECUTION" if amendment else f"{FAMILY}_FIRST_OUTCOME_EXECUTION"
    rec_file = f"{amendment}_EXECUTION.json" if amendment else "FIRST_OUTCOME_EXECUTION.json"
    if os.path.exists(os.path.join(FAMILY_DIR, rec_file)):
        raise HardStop(f"{rec_file} exists — execution records are immutable")
    json.dump(dict(record_id=rec_name, run_id=run.run_id, seal_registry_sha256_16=s["registry_sha256_16"], demean_stat=demean_stat, amendment=amendment,
                   results_sha256_16=_dig(os.path.join(run.dir, "results.json")), outcome_access_count=led["outcome_access_count"],
                   pathsim_src_sha256_16=O.PATHSIM_SRC_SHA, classification_census=cls_census, role_census=role_census,
                   status="COMPLETE — family SEARCH-EXPOSED" + (" — POST_EXPOSURE_CORRECTION" if amendment else "")),
              open(os.path.join(FAMILY_DIR, rec_file), "w"), indent=1)
    log(f"roles: {role_census}")
    return summ


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "build"
    {"build": build,
     "seal": lambda: print(json.dumps({k: v for k, v in seal().items() if k in ("sealed_at", "registry_sha256_16", "k", "x_run", "x_sha256_16")}, indent=1)),
     "outcome": outcome,
     "amend1": lambda: outcome(demean_stat="median", amendment="AMENDMENT_1")}[cmd]()
