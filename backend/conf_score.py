"""
conf_score.py — CONF: the all-vs-all confluence score (2026-07-21).

Aggregates the 812 dual-gate-qualified signal PAIRS (data/allpairs_qualified.csv,
built by analysis/260721_all_vs_all.py) into one per-bar number:
  score = Σ best-cell ps-contribution per BULL feature − Σ per BEAR feature
(each FEATURE counts once per side — its strongest active cell — so 30 correlated
vol_20x cells can't stack).

VALIDATED (260721_conf_score_validation.py): strictly monotone decile ladder on
per-bar path-sim, D0 −3.07% → D9 +3.63%, medians −9.30 → +1.13; TOP-5% positive
ALL 6 years (2022 +0.5), BOT-5% negative ALL 6 years — both tails era-robust.

Regenerate cells: rerun analysis/260721_all_vs_all.py (writes the CSV).
"""
from __future__ import annotations
import os
import numpy as np
import pandas as pd

_CELLS: list = [None]

RAW_BIN_PREFIXES = ("sig_",)
EXTRA_BIN = ("bo_up", "bx_up", "be_up", "vbo_up", "fbo_bull", "fbo_bear", "load", "bf_buy",
             "bf_sell", "rocket", "hilo_buy", "best_long", "tz_bull", "w2_sc", "w2_ar", "w2_st",
             "w2_spring", "w2_sos", "w2_jac", "w2_lps", "w2_evr", "w2_accum", "w2_break",
             "wt_sos", "wt_spring", "wt_lps", "wt_evr", "ad_fresh", "ad_cluster",
             "prebreak_prime", "prebreak_ready", "prebreak_watch", "pb_lvbo", "pb_wvf_confirm",
             "pb_pp_rtv", "pb_fly_cd_c", "pb_follow_confirm", "wyc_spring", "wyc_sos",
             "wyc_in_tr", "wyc_sow")


def _norm_feat(f: str) -> str:
    """Collapse duplicate namings: the T=/Z= one-hots are identical to the raw sig_t*/z_*
    flags (e.g. 'Z=Z3' == 'z3') — without this a qualified cell counts twice."""
    if f.startswith("T=") or f.startswith("Z="):
        return f[2:].lower()
    return f


def cells() -> pd.DataFrame:
    if _CELLS[0] is None:
        p = os.path.join(os.path.dirname(__file__), "data", "allpairs_qualified.csv")
        q = pd.read_csv(p)
        q["a"] = q["a"].map(_norm_feat)
        q["b"] = q["b"].map(_norm_feat)
        q["_key"] = q.apply(lambda r: (r["dir"],) + tuple(sorted((r["a"], r["b"]))), axis=1)
        q = q.drop_duplicates("_key").drop(columns="_key").reset_index(drop=True)
        _CELLS[0] = q
    return _CELLS[0]


_EXT: list = [None]


def ext_cells() -> pd.DataFrame:
    """GRAY info-only tier (2026-07-21, user: "meti ujra iyos ubralod nacrisferebi"):
    ALL pair cells from data/allpairs_all.csv (no dual gates, n>=300) that are NOT in
    the validated core, with |ps| >= 0.3. Direction = sign of ps (the label dir came
    from up%-lift and can disagree). NOT validated — display gray, never colored."""
    if _EXT[0] is None:
        p = os.path.join(os.path.dirname(__file__), "data", "allpairs_all.csv")
        if not os.path.exists(p):
            _EXT[0] = pd.DataFrame(columns=["dir", "a", "b", "ps"])
            return _EXT[0]
        q = pd.read_csv(p)
        q["a"] = q["a"].map(_norm_feat)
        q["b"] = q["b"].map(_norm_feat)
        q["dir"] = np.where(q["ps"] > 0, "BULL", "BEAR")
        q["_key"] = q.apply(lambda r: tuple(sorted((r["a"], r["b"]))), axis=1)
        q = q.drop_duplicates("_key")
        core = cells()
        core_keys = {tuple(sorted((a, b))) for a, b in zip(core.a, core.b)}
        q = q[~q["_key"].isin(core_keys) & (q["ps"].abs() >= 0.3)]
        _EXT[0] = q.drop(columns="_key").reset_index(drop=True)
    return _EXT[0]


def needed_raw_columns(include_ext: bool = True) -> list:
    """Raw binary DB columns referenced by the cells (for SQL SELECT). Only returns
    names that actually exist in the DB (ext features may reference derived-only ones)."""
    feats = set(cells().a) | set(cells().b)
    if include_ext:
        feats |= set(ext_cells().a) | set(ext_cells().b)
    raw = []
    known = _known_sig()
    for f in feats:
        if f.startswith(("T=", "Z=", "L=", "gap=", "vol=", "wyc=", "RSI", "CCI", "px",
                         "c>", "c<", "stack", "dip", "L34red")):
            continue
        if ("sig_" + f) in known:
            raw.append("sig_" + f)
        elif f in known:
            raw.append(f)
    return sorted(set(raw))


_KNOWN_SIG: list = [None]


def _known_sig():
    if _KNOWN_SIG[0] is None:
        import duckdb
        db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "data", "studio_analytics.duckdb")
        c = duckdb.connect(db, read_only=True)
        _KNOWN_SIG[0] = {r[1] for r in c.execute("PRAGMA table_info(bars)").fetchall()}
        c.close()
    return _KNOWN_SIG[0]


def build_features(df: pd.DataFrame, feats=None) -> dict:
    """df: per-ticker-sorted frame with ticker/open/close/rsi_14/cci_20/tt/zz/ll/gap/vb/wp
    + the raw binary columns (already coalesced to 0/1). Returns {feature: bool ndarray}."""
    if feats is None:
        feats = set(cells().a) | set(cells().b)
    F = {}
    c = df["close"].to_numpy(float)
    o = df["open"].to_numpy(float)
    tta = df["tt"].to_numpy(); zza = df["zz"].to_numpy(); lla = df["ll"].to_numpy()
    rs = df["rsi_14"].to_numpy(float)
    cc = df["cci_20"].to_numpy(float)
    if "e20" in df.columns:            # precomputed (screener last-bar path)
        e20 = df["e20"].to_numpy(float)
        e50 = df["e50"].to_numpy(float)
        e200 = df["e200"].to_numpy(float)
    else:
        g = df.groupby("ticker")["close"]
        e20 = g.transform(lambda s: s.ewm(span=20, adjust=False).mean()).to_numpy()
        e50 = g.transform(lambda s: s.ewm(span=50, adjust=False).mean()).to_numpy()
        e200 = g.transform(lambda s: s.ewm(span=200, adjust=False).mean()).to_numpy()
    for f in feats:
        if f.startswith("T="):
            F[f] = tta == f[2:]
        elif f.startswith("Z="):
            F[f] = zza == f[2:]
        elif f.startswith("L="):
            F[f] = lla == f[2:]
        elif f == "L34red":
            F[f] = (lla == "L34") & (c < o)
        elif f.startswith("gap="):
            F[f] = df["gap"].to_numpy() == f[4:]
        elif f.startswith("vol="):
            F[f] = df["vb"].to_numpy() == f[4:]
        elif f.startswith("wyc="):
            F[f] = df["wp"].to_numpy() == f[4:]
        elif f.startswith("su="):
            # setup token (A/SM/N/MX) — persisted 2026-07-21, space-padded membership
            tok = f[3:]
            if "sut" in df.columns:
                pad = (" " + df["sut"].fillna("").astype(str) + " ").to_numpy()
                F[f] = np.char.find(pad.astype(str), f" {tok} ") >= 0
            else:
                F[f] = np.zeros(len(df), bool)
        elif f.startswith("cx="):
            # context token (LD/WRC/SVS/LRC/LDS/SQB/LDC/F8C/BCT/LDP/LRP)
            tok = f[3:]
            if "cxt" in df.columns:
                pad = (" " + df["cxt"].fillna("").astype(str) + " ").to_numpy()
                F[f] = np.char.find(pad.astype(str), f" {tok} ") >= 0
            else:
                F[f] = np.zeros(len(df), bool)
        elif f == "RSI<30":
            F[f] = rs < 30
        elif f == "RSI30-40":
            F[f] = (rs >= 30) & (rs < 40)
        elif f == "RSI40-50":
            F[f] = (rs >= 40) & (rs < 50)
        elif f == "RSI50-60":
            F[f] = (rs >= 50) & (rs < 60)
        elif f == "RSI60+":
            F[f] = rs >= 60
        elif f == "CCI<-100":
            F[f] = cc < -100
        elif f == "CCI-100..0":
            F[f] = (cc >= -100) & (cc < 0)
        elif f == "CCI0..100":
            F[f] = (cc >= 0) & (cc < 100)
        elif f == "CCI>100":
            F[f] = cc >= 100
        elif f == "c>e200" or f == "c>e200 near-hi":
            F[f] = c > e200
        elif f == "c<e200":
            F[f] = c <= e200
        elif f == "stack e20>e50>e200":
            F[f] = (e20 > e50) & (e50 > e200)
        elif f == "dip c<e20 up":
            F[f] = (c < e20) & (e50 > e200)
        elif f == "px5-21":
            F[f] = (c >= 5) & (c < 21)
        elif f == "px21-89":
            F[f] = (c >= 21) & (c < 89)
        elif f == "px89+":
            F[f] = c >= 89
        else:
            col = "sig_" + f if ("sig_" + f) in df.columns else f
            F[f] = (df[col].to_numpy() == 1) if col in df.columns else np.zeros(len(df), bool)
    return F


def _aggregate(Q: pd.DataFrame, F: dict, n: int, top_k: int):
    """Per-feature-best-per-side aggregation over a cell table → (score, detail)."""
    bull = np.zeros(n)
    bear = np.zeros(n)
    side_best: dict = {"BULL": {}, "BEAR": {}}
    active_cells = []      # (contribution ndarray, label) for tooltips
    for _, r in Q.iterrows():
        if r.a not in F or r.b not in F:
            continue
        m = F[r.a] & F[r.b]
        if not m.any():
            continue
        w = float(r.ps)
        cand = np.where(m, w, 0.0)
        active_cells.append((cand, f"{r.a}+{r.b}"))
        for feat in (r.a, r.b):
            prev = side_best[r.dir].get(feat)
            side_best[r.dir][feat] = cand if prev is None else np.where(np.abs(cand) > np.abs(prev), cand, prev)
    for feat, arr in side_best["BULL"].items():
        bull += np.maximum(arr, 0)
    for feat, arr in side_best["BEAR"].items():
        bear += np.minimum(arr, 0)
    score = bull + bear
    # top cells per bar (only where score nonzero) — keep it cheap: top_k by |contribution|
    detail = [None] * n
    if active_cells:
        idx_nz = np.nonzero(score != 0)[0]
        mat = np.stack([a for a, _ in active_cells])          # cells × bars
        labels = [l for _, l in active_cells]
        for i in idx_nz:
            col = mat[:, i]
            nz = np.nonzero(col != 0)[0]
            if len(nz) == 0:
                continue
            top = nz[np.argsort(-np.abs(col[nz]))][:top_k]
            detail[i] = " · ".join(f"{labels[j]}({col[j]:+.1f})" for j in top)
    return score, detail


def compute(df: pd.DataFrame, top_k: int = 3, with_ext: bool = False):
    """Returns (score, detail) — or (score, detail, ext_score, ext_detail) with with_ext.
    ext = the gray info-only tier (unvalidated sub-threshold cells), meant for display
    ONLY where the core score is 0."""
    Q = cells()
    n = len(df)
    if not with_ext:
        F = build_features(df)
        return _aggregate(Q, F, n, top_k)
    E = ext_cells()
    feats = set(Q.a) | set(Q.b) | set(E.a) | set(E.b)
    F = build_features(df, feats)
    score, detail = _aggregate(Q, F, n, top_k)
    ext_score, ext_detail = _aggregate(E, F, n, top_k)
    return score, detail, ext_score, ext_detail
