"""v3a_discovery.py — Phase 1/2/3 under a frozen search budget. 2026-09-12.

⛔ PROGRAMME CLOSED 2026-09-12 — VERDICT **NULL / NO IMPLEMENTATION**. See V3A_NULL_REPORT.md.
   Discovery completed under the two-layer maturity contract (v3a_warmup_contract). The single
   pair that cleared the frozen gate, `phys_e_raw>p90 & bar_gap_range=G3-C` (+8.37 pp vs BL3),
   is a liquidity artefact: 90 % of its 680 events sit below $1M/day, median $14k, and above a
   tradeable floor it retains 79 events. On names trading >= $1M/day the best conjunction beats
   max(BL3, BL4) by +2.68 pp against a +5.00 pp gate.
   VALIDATION 2024-2025 was never opened. HOLDOUT 2026 was never opened. No registry was frozen.
   Adding a liquidity floor here would be changing a rule after seeing the result — that is a new
   study (V4 — Liquid Major Breakouts) with its own pre-registration, not a continuation.

TARGET, frozen before any feature was looked at:
    MFE20_ATR = (max(high[t+1:t+20]) − close[t]) / ATR14[t]        Y8 := MFE20_ATR >= 8
    DISCOVERY base rate 8.63 %

SEARCH BUDGET, frozen (tightened at the user's instruction):
    Phase 1   ~180 single-primitive screens, DISCOVERY 2021-2023 ONLY
    Phase 2   <= 6 families  x  <= 2 representatives  =  <= 12
    Phase 3   <= 20 registered cross-family conjunctions
    registry  <= 5 candidates opened on 2026
No candidate may be created after VALIDATION results are seen.

BASELINES — joint primary comparators, recomputed on each candidate's OWN eligible universe:
    BL3 = RSI_14 > 50
    BL4 = close > max(high[t-20:t-1])  AND  volume > 1.5 * avg_vol_20d
    BEST_BASELINE_PRECISION = max(BL3, BL4) on that universe.

GATE (2026, applied by v3a_holdout.py):
    candidate - BEST >= +5.00 pp  AND  candidate / BEST >= 1.10
    same direction already on 2024-2025 · ticker-clustered CI on the difference excluding zero
    n_events >= 300 · top-5 ticker share <= 15 %

2026 IS PHYSICALLY UNREACHABLE FROM THIS MODULE. It loads through
v3a_frame_build.load(max_date="2025-12-31"), which asserts the cut.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
from collections import OrderedDict

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import v3a_frame_build as F                                              # noqa: E402
import para_target_v1 as T                                              # noqa: E402
import v3a_warmup_contract as W                                         # noqa: E402

OUTDIR = os.path.join(os.path.dirname(HERE), "research_out")
REGISTRY = os.path.join(os.path.dirname(HERE), "data", "v3a_registry.json")

DISCOVERY = ("2021-01-01", "2023-12-31")
VALIDATION = ("2024-01-01", "2025-12-31")
COOLDOWN = 10
RACE_FWD = 20

# ⚠️ NO PHASE-1 CAP. The first draft truncated 313 generated primitives to 200 by dataframe
# column order and capped each family at 24 the same way. That is research selection — not
# outcome-based, but still arbitrary: whether a feature is screened would depend on where its
# column happens to sit in the schema. Every generated primitive is now screened. The search
# burden is controlled downstream, where it belongs: Phase 2 <=6x2, Phase 3 <=20, registry <=5.
MAX_FAMILIES = 6
MAX_PER_FAMILY = 2
MAX_CONJUNCTIONS = 20
MAX_REGISTRY = 5

# Phase 1 -> Phase 2 retention, pre-specified
MIN_EVENTS, MIN_DELTA_PP, MAX_TOP5_SHARE, MIN_POS_YEARS = 1000, 1.0, 15.0, 2

# ⚠️ ELIGIBILITY NEVER LOOKS AT THE OUTCOME. The first draft restricted a primitive's universe to
# its source-available rows only when |Y8_available - Y8_missing| >= 2 pp. That let the outcome
# decide which universe a feature is judged in — sometimes global, sometimes coverage-matched,
# depending on what discovery happened to show. A feature sourced from a partially-covered layer
# is ALWAYS evaluated inside that layer's available rows, whatever the gap turns out to be.
# Missing source data is UNKNOWN, never False. The Y8 gap is kept, as a DIAGNOSTIC only.

FAMILY_OF = [
    ("TZ", ("t_sig", "z_sig", "sig_t", "sig_z", "sig_tz", "sig_bias", "sig_wk", "tz_")),
    ("L_VSA", ("l_sig", "sig_l", "l34", "l43", "l22", "sig_fri", "sig_blue", "sig_pp")),
    ("LEVEL_BREAK", ("bo_up", "bo_dn", "bx_up", "bx_dn", "be_up", "be_dn", "vbo_up", "sig_vbo")),
    ("F", ("sig_f", "f8")),
    ("FLY", ("sig_fly", "fly_")),
    ("GOG_GAP", ("sig_g", "gog", "g1p", "g2p", "g3p", "g1l", "g2l", "g3l", "g1c", "g2c", "g3c",
                 "setup_tokens", "context_tokens")),
    ("B_BREAK", ("sig_b",)),
    ("VOL_ABSORB", ("sig_va", "sig_svs", "svs", "sig_abs", "sig_clm", "sig_sc", "sig_bc",
                    "sig_ns", "sig_nd", "sig_vol_", "sq", "load", "cons", "vol_bucket", "rtv")),
    ("PHYS", ("phys_",)),
    ("WYCKOFF", ("w2_", "wt_", "wyc_")),
    ("CISD", ("sig_cisd",)),
    ("PARA", ("sig_para", "para_")),
    ("EMA_CROSS", ("sig_p", "sig_d", "price_gt", "price_lt")),
    ("OSCILLATOR", ("rsi", "cci", "psar", "wvf", "vix_")),
    ("BAR_TAXONOMY", ("bar_", "ne_suffix", "wick_suffix", "penetration_suffix", "close_suffix",
                      "full_suffix", "composite_")),
    ("RTB", ("rtb_", "dbg_")),
    ("DISPLAY", ("LBAL", "LVX", "VOL7", "SHAPE", "OVD")),
    ("PRICE_DERIVED", ("pct_change", "pct_from", "dist_ema", "range_pct", "body_pct",
                       "upper_wick_pct", "lower_wick_pct", "atr_pct", "vol_ratio",
                       "dollar_volume", "gap_pct", "consec_up", "hi20_age", "close_pos")),
]
SKIP_COLS = {"ticker", "date", "universe", "sector", "open", "high", "low", "close", "volume",
             "avg_vol_20d", "atr_14"}


def family_of(col: str) -> str | None:
    for fam, prefixes in FAMILY_OF:
        for p in prefixes:
            if col == p or col.startswith(p):
                return fam
    return None


def bounds(tk: np.ndarray) -> np.ndarray:
    return np.r_[np.flatnonzero(np.r_[True, tk[1:] != tk[:-1]]), len(tk)]


def to_events(fire: np.ndarray, bnd: np.ndarray, cooldown: int = COOLDOWN) -> np.ndarray:
    n = len(fire); idx = np.arange(n, dtype=np.int64); out = np.zeros(n, bool)
    for a, b in zip(bnd[:-1], bnd[1:]):
        if b <= a:
            continue
        f = fire[a:b]
        last = np.maximum.accumulate(np.where(f, idx[a:b], -1))
        prev = np.r_[-1, last[:-1]]
        gap = np.where(prev >= 0, idx[a:b] - prev, 10 ** 6)
        out[a:b] = f & (gap > cooldown)
    return out


def labels(df: pd.DataFrame, log=print) -> pd.DataFrame:
    L = T.label(df[["ticker", "date", "open", "high", "low", "close", "atr_14"]].copy(),
                atr_col="atr_14")
    out = pd.DataFrame(index=df.index)
    out["MFE20_ATR"] = L["mfe_atr"].to_numpy()
    out["Y8"] = (out["MFE20_ATR"] >= 8).where(out["MFE20_ATR"].notna())
    log(f"  labels · Y8 base {out['Y8'].mean()*100:.2f}% over {int(out['Y8'].notna().sum()):,} rows")
    return out


# ───────────────────────────────────────────────────────────────────────────
# primitive generation — no hypothesis in the selection
# ───────────────────────────────────────────────────────────────────────────
def make_primitives(df: pd.DataFrame, disc: np.ndarray, log=print) -> "OrderedDict[str, dict]":
    """Boolean flags, categorical first-entries and continuous decile events.
    Thresholds (deciles, level sets) are fixed on DISCOVERY rows only."""
    prims: "OrderedDict[str, dict]" = OrderedDict()
    per_family: dict = {}
    for col in df.columns:
        if col in SKIP_COLS:
            continue
        fam = family_of(col)
        if fam is None:
            continue
        s = df[col]
        cnt = per_family.get(fam, 0)
        # Thresholds and level frequencies are fixed on the rows where this column is BOTH
        # source-available and mature. Fixing a decile on rows where the producer's EMA is still
        # seed-dominated would put the immaturity straight into the threshold.
        disc_col = disc & feature_eligible(df, col)[0]
        if s.dtype == bool or (pd.api.types.is_numeric_dtype(s)
                               and s.dropna().isin([0, 1]).all() and s.notna().any()):
            v = s.fillna(0).to_numpy(float) > 0
            r = v[disc_col].mean() if disc_col.any() else 0.0
            if 0.001 <= r <= 0.50:
                prims[f"{col}"] = dict(family=fam, kind="flag", mask=v, source=col)
                per_family[fam] = cnt + 1
        elif s.dtype == object:
            vc = s[disc_col].fillna("").astype(str).value_counts(normalize=True)
            for lvl, rate in vc.items():
                if lvl == "" or not (0.005 <= rate <= 0.40):
                    continue
                v = (s.fillna("").astype(str).to_numpy() == lvl)
                prims[f"{col}={lvl}"] = dict(family=fam, kind="level", mask=v, source=col)
                per_family[fam] = per_family.get(fam, 0) + 1
        elif pd.api.types.is_numeric_dtype(s):
            x = s.to_numpy(float)
            d = x[disc_col]
            d = d[np.isfinite(d)]
            if len(d) < 10000:
                continue
            hi, lo = np.percentile(d, 90), np.percentile(d, 10)
            if not np.isfinite(hi) or hi == lo:
                continue
            prims[f"{col}>p90"] = dict(family=fam, kind="cont_hi", mask=x > hi, source=col,
                                       threshold=float(hi))
            prims[f"{col}<p10"] = dict(family=fam, kind="cont_lo", mask=x < lo, source=col,
                                       threshold=float(lo))
            per_family[fam] = per_family.get(fam, 0) + 2
    log(f"  primitives generated {len(prims)} across {len(set(p['family'] for p in prims.values()))}"
        f" families")
    return prims


# ───────────────────────────────────────────────────────────────────────────
# scoring
# ───────────────────────────────────────────────────────────────────────────
_AVAIL_CACHE: dict = {}


def source_available(df, source: str | None) -> np.ndarray:
    """Rows where this primitive's SOURCE actually exists. An empty string counts as missing for
    object columns (a blank phys_k is warm-up, not a state). Outcome-free by construction —
    this is the only thing that may decide a primitive's eligible universe."""
    if source is None or source not in df.columns:
        return np.ones(len(df), bool)
    if source in _AVAIL_CACHE:
        return _AVAIL_CACHE[source]
    s = df[source]
    if s.dtype == object:
        v = s.notna().to_numpy() & (s.fillna("").astype(str).str.strip() != "").to_numpy()
    else:
        v = s.notna().to_numpy()
    _AVAIL_CACHE[source] = v
    return v


# ───────────────────────────────────────────────────────────────────────────
# TWO-LAYER MATURITY (v3a_warmup_contract). Added after the first run's top two primitives
# turned out to be series-start artefacts: `sig_l2` +3.43 -> +0.21 and `sig_t` +3.01 -> +0.22
# once the first 20 bars of each ticker were removed. Layer A is the global target warm-up
# (the TARGET's own ATR14 is immature there); layer B is each feature's own producer-declared
# history. Neither is derived from Y8.
# ───────────────────────────────────────────────────────────────────────────
_BI_CACHE: dict = {}


def bar_index(df: pd.DataFrame) -> np.ndarray:
    """Position of each row inside its own ticker's series, within this frame. The frame starts
    before the DB's first bar for essentially every ticker (measured: 3,741 of 3,748), so this is
    a LOWER BOUND on true series age — requiring bar_index >= n therefore guarantees >= n bars of
    real history, never less."""
    key = ("bi", len(df))
    if key in _BI_CACHE:
        return _BI_CACHE[key]
    tk = df["ticker"].to_numpy()
    bnd = bounds(tk)
    bi = np.zeros(len(df), np.int64)
    for a, b in zip(bnd[:-1], bnd[1:]):
        bi[a:b] = np.arange(b - a)
    _BI_CACHE[key] = bi
    return bi


def feature_eligible(df: pd.DataFrame, source: str | None) -> tuple[np.ndarray, int]:
    """(mask, required_history). Source data present AND both maturity layers satisfied."""
    req = W.required(source)
    key = ("fe", source, req, len(df))
    if key in _BI_CACHE:
        return _BI_CACHE[key], req
    m = source_available(df, source) & (bar_index(df) >= req)
    _BI_CACHE[key] = m
    return m, req


def coverage_diagnostics(df, Lb, ok, has) -> dict:
    """DIAGNOSTIC ONLY. Reported so a coverage/liquidity marker is visible, never used to pick
    a universe — that is decided by source_available() alone."""
    out = dict(cov_pct=round(float(has[ok].mean()) * 100, 2))
    if bool(has[ok].all()):
        return out
    a, b = ok & has, ok & ~has
    if a.sum() < 1000 or b.sum() < 1000:
        return out
    y = Lb["Y8"].to_numpy(float)
    dv = df["close"].to_numpy(float) * df["volume"].to_numpy(float)
    ya, yb = float(np.nanmean(y[a])), float(np.nanmean(y[b]))
    out.update(y8_when_avail=round(ya * 100, 2), y8_when_missing=round(yb * 100, 2),
               y8_gap_pp=round((ya - yb) * 100, 2),
               dvol_avail_musd=round(float(np.nanmedian(dv[a])) / 1e6, 2),
               dvol_missing_musd=round(float(np.nanmedian(dv[b])) / 1e6, 2),
               tickers_avail=int(df.loc[a, "ticker"].nunique()),
               tickers_missing=int(df.loc[b, "ticker"].nunique()))
    return out


_CELL_CACHE: dict = {}


def _cell_tables(df, Lb, elig_key: str, elig: np.ndarray):
    """(cell_sum, cell_cnt) of Y8 per (ticker, year) cell, over one eligibility mask.

    Cached per eligibility key so a primitive costs O(hit cells) instead of O(rows). The matched
    baseline is the whole point of this pipeline — it is what caught the B2b coverage artefact —
    so it has to be cheap enough to run on EVERY primitive rather than a chosen few.
    """
    if elig_key in _CELL_CACHE:
        return _CELL_CACHE[elig_key]
    if "cell_code" not in _CELL_CACHE:
        cell = (df["ticker"].astype(str) + "|" + df["date"].str[:4])
        codes, _ = pd.factorize(cell)
        _CELL_CACHE["cell_code"] = codes.astype(np.int32)
        _CELL_CACHE["n_cells"] = int(codes.max()) + 1
    codes = _CELL_CACHE["cell_code"]; nc = _CELL_CACHE["n_cells"]
    y = Lb["Y8"].to_numpy(float)
    m = elig & np.isfinite(y)
    csum = np.bincount(codes[m], weights=y[m], minlength=nc)
    ccnt = np.bincount(codes[m], minlength=nc).astype(float)
    _CELL_CACHE[elig_key] = (csum, ccnt)
    return csum, ccnt


def matched_base(fire_ev, df, Lb, elig_key, elig, sub=None, sub_key="") -> float:
    """Base rate of the (ticker, year) cells the events occupy. `sub` restricts to one year."""
    if not fire_ev.any():
        return np.nan
    key = elig_key if sub is None else f"{elig_key}|{sub_key}"
    csum, ccnt = _cell_tables(df, Lb, key, elig if sub is None else (elig & sub))
    hit = np.unique(_CELL_CACHE["cell_code"][fire_ev])
    tot = ccnt[hit].sum()
    return float(csum[hit].sum() / tot) if tot > 0 else np.nan


def score(mask, df, Lb, bnd, ok, name, family, source=None) -> dict:
    """Event-level, matched-baseline scoring.

    ELIGIBILITY = ok AND the primitive's source actually exists. ALWAYS — the outcome plays no
    part in choosing the universe, only in scoring inside it.
    """
    avail = source_available(df, source)                 # coverage only — for the diagnostics
    mature, req = feature_eligible(df, source)           # coverage AND both maturity layers
    elig = ok & mature
    partial = not bool(avail[ok].all())
    elig_key = f"req{req}" if not partial else f"{source}|req{req}"
    row = dict(primitive=name, family=family, source=source, partial_coverage=partial,
               min_history=req, min_history_why=W.min_history(source)[1])
    row.update(coverage_diagnostics(df, Lb, ok, avail))

    fb = mask & elig
    fe = to_events(fb, bnd) & elig
    n = int(fe.sum())
    row["eligible_rows"] = int(elig.sum())
    row["n_events"] = n
    row["tickers"] = int(df.loc[fe, "ticker"].nunique()) if n else 0
    # DIAGNOSTIC for the second defect, which this module does NOT fix: a state true on a large
    # share of bars collapses under the 10-bar cooldown to roughly one event per ticker, i.e. to
    # "the first eligible bar of the series". `sig_t` was true on 47.1 % of bars and produced 1.64
    # events per ticker. Such a primitive is not being tested as a signal. Reported, not acted on.
    row["raw_rate"] = round(float((mask & elig).sum()) / max(int(elig.sum()), 1) * 100, 2)
    row["events_per_ticker"] = round(n / row["tickers"], 2) if row["tickers"] else None
    if n < 200:
        row["verdict"] = "INSUFFICIENT"
        return row

    y = Lb["Y8"].to_numpy(float)
    p = float(np.nanmean(y[fe]))
    bm = matched_base(fe, df, Lb, elig_key, elig)
    ref = bm if np.isfinite(bm) else float(np.nanmean(y[elig]))
    yr = df["date"].str[:4].to_numpy()
    pos_years = 0
    for yy in np.unique(yr[fe]):
        sub = (yr == yy)
        s_ev = fe & sub
        if s_ev.sum() < 100:
            row[f"y{yy}_delta"] = None
            continue
        by = matched_base(s_ev, df, Lb, elig_key, elig, sub=sub, sub_key=str(yy))
        py = float(np.nanmean(y[s_ev]))
        row[f"y{yy}_delta"] = round((py - by) * 100, 2) if np.isfinite(by) else None
        row[f"y{yy}_n"] = int(s_ev.sum())
        if np.isfinite(by) and py > by:
            pos_years += 1
    vc = df.loc[fe, "ticker"].value_counts()
    row.update(prec=round(p * 100, 2), base_matched=round(ref * 100, 2),
               delta_pp=round((p - ref) * 100, 2), lift=round(p / ref, 3) if ref else None,
               top5_share=round(float(vc.head(5).sum() / n) * 100, 2), pos_years=pos_years,
               med_mfe=round(float(np.nanmedian(Lb["MFE20_ATR"].to_numpy(float)[fe])), 3))
    # FROZEN SURVIVOR RULE — unchanged after the primitive count went 313 -> 549. Adjusting a
    # threshold because the search got wider would be a reaction to multiplicity, not a rule.
    # The fifth criterion ("no coverage/liquidity confound") is satisfied BY CONSTRUCTION: every
    # primitive is scored inside its own source-available universe, so a coverage gap cannot
    # inflate it. The diagnostics stay in the table so that is auditable rather than asserted.
    row["survivor_flag"] = bool(n >= MIN_EVENTS and row["delta_pp"] >= MIN_DELTA_PP
                                and row["top5_share"] <= MAX_TOP5_SHARE
                                and pos_years >= MIN_POS_YEARS)
    row["verdict"] = "RETAIN" if row["survivor_flag"] else "DROP"
    return row


def baselines(df, bnd) -> dict:
    c = df["close"].to_numpy(float); h = df["high"].to_numpy(float)
    v = df["volume"].to_numpy(float); av = df["avg_vol_20d"].to_numpy(float)
    n = len(df); prior = np.full(n, np.nan)
    for a, b in zip(bnd[:-1], bnd[1:]):
        prior[a:b] = pd.Series(h[a:b]).shift(1).rolling(20, min_periods=20).max().to_numpy()
    # (mask, source) — the source drives the baseline's OWN maturity requirement, so a comparator
    # is never credited on rows where its own inputs are still seed-dominated.
    return {"BL3_rsi_gt50": (df["rsi_14"].to_numpy(float) > 50, "__BL3_rsi14__"),
            "BL4_breakout_volexp": ((c > prior) & (v > 1.5 * av), "__BL4_brk20vol__")}


CLOSED_NULL = True          # set by the closure above; unset only under a NEW pre-registration


def freeze_registry(candidates: list, log=print) -> str:
    if CLOSED_NULL:
        raise RuntimeError(
            "V3-A is CLOSED with verdict NULL / NO IMPLEMENTATION (V3A_NULL_REPORT.md). "
            "No candidate may be frozen and neither 2024-2025 nor 2026 may be opened from this "
            "module. A liquidity-floored search is a NEW programme with its own pre-registration.")
    payload = dict(target="MFE20_ATR>=8", cooldown=COOLDOWN,
                   budget=dict(phase1="ALL", families=MAX_FAMILIES,
                               per_family=MAX_PER_FAMILY, conjunctions=MAX_CONJUNCTIONS,
                               registry=MAX_REGISTRY),
                   retention=dict(min_events=MIN_EVENTS, min_delta_pp=MIN_DELTA_PP,
                                  max_top5=MAX_TOP5_SHARE, min_pos_years=MIN_POS_YEARS),
                   gate=dict(abs_pp=5.0, rel=1.10, min_events=300, max_top5=15.0,
                             baselines=["BL3_rsi_gt50", "BL4_breakout_volexp"],
                             rule="candidate must beat max(BL3, BL4) on its own eligible universe"),
                   candidates=candidates)
    blob = json.dumps(payload, sort_keys=True, default=str).encode()
    payload["sha256"] = hashlib.sha256(blob).hexdigest()
    json.dump(payload, open(REGISTRY, "w"), indent=1, default=str)
    log(f"  registry FROZEN · {len(candidates)} candidates · sha256 {payload['sha256'][:16]}")
    return payload["sha256"]
