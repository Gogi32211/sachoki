"""breakout_eval_v2.py — the eleven methodological changes approved 2026-09-12.

The engine (loader, primitives, state machines) stays in 260911_breakout_systems_research.py.
This file replaces its evaluation, because the first pass showed the evaluation was the part that
was wrong — not the state machines.

WHAT CHANGED AND WHY

 §1 COVERAGE-MATCHED BASELINES. B2b looked like the only system beating the best baseline
    (43.94 % vs 42.29 %). It was measuring data availability: bars where LBAL exists have a
    43.61 % base rate against 32.79 % where it does not, so B2b's true conditional lift is 1.008.
    Every system is now scored against THREE baselines and the global one is never the primary:
      FULL      every labelled row
      ELIGIBLE  only rows where every layer that system needs actually exists
      MATCHED   only the (ticker, year) cells the system's own events occupy
 §2 Every partially-covered layer is audited for exactly that failure mode.
 §3 B1_ORDERED dropped — 40.83 % vs 40.82 % on 307k/310k fires, i.e. no order effect.
    B1_PARTIAL is the hypothesis; the TURN/PART ordering is reported descriptively only.
 §4 RSI<35 removed from B3's specification — it is ANTI-predictive alone (36.37 %, lift 0.924)
    and the conjunction adds nothing over RTB alone (41.64 % vs 41.62 %). No new cutoff is
    searched: that would be threshold fishing on a dead hypothesis.
 §6 States vs transitions, reported as pairs, because most PHYS fields are price-derived
    descriptions and the question is whether the EVENT adds anything over the STATE.
 §7 K_ACT vs K_REACT — the headline test of the two-stage theory.
 §8 Participation families measured one at a time, and incrementally over REGIME_UP and K_ACT,
    before any recombination. No exhaustive combinatorial search.
 §9 EVENT-LEVEL IS PRIMARY. A state true for five days is one event, not five successes.
    Bar-level is still reported, and the gap between them is itself a result.
 §10 Failure paths: for every event that misses the target, what the state machine did next.
 §11 Every hypothesis is labelled SURVIVED / NULL / ANTI / COVERAGE_CONFOUND /
    INSUFFICIENT_DATA / UNSTABLE_ACROSS_FOLDS. The goal of this pass is elimination.

Missingness stays three-state everywhere: TRUE / FALSE / UNKNOWN. An absent layer is never False.
"""
from __future__ import annotations

import importlib.util
import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
_spec = importlib.util.spec_from_file_location(
    "bsr", os.path.join(HERE, "260911_breakout_systems_research.py"))
R = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(R)

OUTDIR = R.OUTDIR
EVENT_COOLDOWN = 10        # bars; a fire inside this window of the previous one is the same event

# Which layers each system actually requires. Used for the ELIGIBLE baseline.
REQUIRES = {
    "B1_PARTIAL": [], "B2a": [], "B2c": [], "B3": [], "B4": [], "R1": [],
    "B2b": ["LBAL_PRESENT"],
}


# ───────────────────────────────────────────────────────────────────────────
# §9  event collapse
# ───────────────────────────────────────────────────────────────────────────
def to_events(fire: np.ndarray, bnd: np.ndarray, cooldown: int = EVENT_COOLDOWN) -> np.ndarray:
    """First qualifying bar of each run is the anchor; later fires inside `cooldown` are the
    same event. Returns a boolean mask of anchors.

    ⚠️ The first implementation used `_recent(fire, cooldown, include_now=False)`, which is wrong:
    `_bars_since` counts the CURRENT bar, so a firing bar has bs = 0 and the `bs >= 1` test made
    every fire its own anchor — n_events == n_bars for every system, and the cooldown-sensitivity
    table read identically at 5 / 10 / 15. That table is what exposed it. The gap must be measured
    to the last fire STRICTLY BEFORE this bar.
    """
    n = len(fire)
    idx = np.arange(n, dtype=np.int64)
    out = np.zeros(n, bool)
    for a, b in zip(bnd[:-1], bnd[1:]):
        if b <= a:
            continue
        f = fire[a:b]
        last = np.maximum.accumulate(np.where(f, idx[a:b], -1))
        prev_last = np.r_[-1, last[:-1]]                 # last fire strictly before each bar
        gap = np.where(prev_last >= 0, idx[a:b] - prev_last, 10 ** 6)
        out[a:b] = f & (gap > cooldown)
    return out


# ───────────────────────────────────────────────────────────────────────────
# §1  three baselines
# ───────────────────────────────────────────────────────────────────────────
def eligible_mask(system: str, P: pd.DataFrame, ok: np.ndarray) -> np.ndarray:
    m = ok.copy()
    for c in REQUIRES.get(system, []):
        m &= P[c].to_numpy()
    return m


def matched_mask(fire: np.ndarray, df: pd.DataFrame, elig: np.ndarray) -> np.ndarray:
    """Rows in the same (ticker, year) cells the events occupy — the coverage-matched control."""
    if not fire.any():
        return np.zeros(len(df), bool)
    yr = df["date"].str[:4].to_numpy()
    cell = pd.Series(df["ticker"].to_numpy().astype(str)) + "|" + pd.Series(yr)
    hit = set(cell[fire].unique())
    return elig & cell.isin(hit).to_numpy()


def three_baselines(fire, P, df, Lb, system) -> dict:
    ok = Lb["Y3"].notna().to_numpy()
    elig = eligible_mask(system, P, ok)
    matched = matched_mask(fire & elig, df, elig)
    out = {}
    for nm, m in (("full", ok), ("eligible", elig), ("matched", matched)):
        v = Lb.loc[m, "Y3"]
        out[nm] = float(v.mean()) if len(v) else np.nan
        out[nm + "_n"] = int(m.sum())
    return out


# ───────────────────────────────────────────────────────────────────────────
# core scorer — event-level primary, bar-level reported
# ───────────────────────────────────────────────────────────────────────────
def score(fire_bar, P, df, Lb, bnd, system, label="") -> dict:
    ok = Lb["Y3"].notna().to_numpy()
    elig = eligible_mask(system, P, ok)
    fb = fire_bar & elig
    fe = to_events(fb, bnd) & elig
    bl = three_baselines(fb, P, df, Lb, system)
    base = bl["matched"] if not np.isnan(bl["matched"]) else bl["eligible"]
    row = dict(system=label or system, n_bars=int(fb.sum()), n_events=int(fe.sum()),
               tickers=int(df.loc[fe, "ticker"].nunique()),
               base_full=round(bl["full"] * 100, 2),
               base_eligible=round(bl["eligible"] * 100, 2),
               base_matched=round(bl["matched"] * 100, 2) if not np.isnan(bl["matched"]) else None)
    if fe.sum() == 0:
        row["verdict"] = "INSUFFICIENT_DATA"
        return row
    for k, col in (("3", "Y3"), ("5", "Y5"), ("8", "Y8")):
        pe = float(Lb.loc[fe, col].mean())
        be = float(Lb.loc[elig, col].mean())
        bm = float(Lb.loc[matched_mask(fb, df, elig), col].mean())
        ref = bm if not np.isnan(bm) else be
        row[f"ev_prec_{k}"] = round(pe * 100, 2)
        row[f"ev_lift_{k}"] = round(pe / ref, 3) if ref else None
    row["bar_prec_3"] = round(float(Lb.loc[fb, "Y3"].mean()) * 100, 2)
    row["bar_lift_3"] = round(float(Lb.loc[fb, "Y3"].mean()) / base, 3) if base else None
    row["med_mfe20"] = round(float(Lb.loc[fe, "MFE20_ATR"].median()), 3)
    row["med_mae20"] = round(float(Lb.loc[fe, "MAE_20_ATR"].median()), 3)
    row["med_fwd20"] = round(float(Lb.loc[fe, "FWD_20"].median()), 3)
    return row


def verdict(row, min_events=300, gate_pp=5.0) -> str:
    if not row.get("n_events") or row["n_events"] < min_events:
        return "INSUFFICIENT_DATA"
    l3 = row.get("ev_lift_3")
    p3 = row.get("ev_prec_3")
    ref = row.get("base_matched") or row.get("base_eligible")
    if l3 is None or ref is None:
        return "INSUFFICIENT_DATA"
    if l3 < 0.97:
        return "ANTI"
    if p3 - ref >= gate_pp:
        return "SURVIVED"
    return "NULL"


# ───────────────────────────────────────────────────────────────────────────
# §2  layer audit
# ───────────────────────────────────────────────────────────────────────────
def layer_audit(df, P, Lb) -> pd.DataFrame:
    ok = Lb["Y3"].notna().to_numpy()
    dv = (df["close"].to_numpy(float) * df["volume"].to_numpy(float))
    yr = df["date"].str[:4].to_numpy()
    layers = {
        "LBAL": P["LBAL_PRESENT"].to_numpy(),
        "LVX": df["LVX_TIER"].notna().to_numpy(),
        "VOL7": df["VOL7_MR"].notna().to_numpy(),
        "SHAPE": df["SHAPE_LSTUP_VETO"].notna().to_numpy(),
        "ANATOMY": df["ANAT_V"].notna().to_numpy() if "ANAT_V" in df.columns
        else np.zeros(len(df), bool),
    }
    rows = []
    for nm, has in layers.items():
        a, b = ok & has, ok & ~has
        ydist = pd.Series(yr[a]).value_counts(normalize=True).round(3).to_dict() if a.any() else {}
        rows.append(dict(
            layer=nm, coverage_pct=round(float(has[ok].mean()) * 100, 2),
            n_available=int(a.sum()), n_missing=int(b.sum()),
            y3_when_available=round(float(Lb.loc[a, "Y3"].mean()) * 100, 2) if a.any() else None,
            y3_when_missing=round(float(Lb.loc[b, "Y3"].mean()) * 100, 2) if b.any() else None,
            tickers_available=int(df.loc[a, "ticker"].nunique()),
            tickers_missing=int(df.loc[b, "ticker"].nunique()),
            med_dollar_vol_available=round(float(np.nanmedian(dv[a])) / 1e6, 1) if a.any() else None,
            med_dollar_vol_missing=round(float(np.nanmedian(dv[b])) / 1e6, 1) if b.any() else None,
            year_dist_available=str(ydist)))
    return pd.DataFrame(rows)


# ───────────────────────────────────────────────────────────────────────────
# §6 §7 §8  state-vs-transition, K_ACT vs K_REACT, participation families
# ───────────────────────────────────────────────────────────────────────────
def k_react_flag(P, bnd, window: int) -> np.ndarray:
    """K reactivation: a bullish K, then a reset to K0, then a new activation within `window`."""
    reset = P["K_RESET_FLAG"].to_numpy()
    return P["K_ACT_FLAG"].to_numpy() & R._recent(reset, window, bnd, include_now=False)


def contrasts(df, P, Lb, bnd, log=print) -> pd.DataFrame:
    ok = Lb["Y3"].notna().to_numpy()
    a = lambda c: P[c].to_numpy()                                        # noqa: E731
    part5 = R._recent(a("PART_ANY"), 5, bnd)
    tests = []

    # §6 state vs transition
    tests += [("STATE  REGIME_UP", a("REGIME_UP")),
              ("EVENT  REGIME_D_TO_U", a("REGIME_D_TO_U")),
              ("STATE  K_BULL", a("K_BULL")),
              ("EVENT  K_ACT", a("K_ACT_FLAG")),
              ("STATE  M1/M2", a("TM_M")),
              ("EVENT  M_PROGRESS", a("M_PROGRESS")),
              ("STATE  S2U/S3U", a("TM_S")),
              ("EVENT  S_PROGRESS", a("S_PROGRESS"))]

    # §7 K_ACT vs K_REACT — the headline
    for w in (3, 6, 10):
        kr = k_react_flag(P, bnd, w)
        tests += [(f"K_REACT w{w}", kr),
                  (f"K_REACT w{w} + part5", kr & part5),
                  (f"K_REACT w{w} + RF", kr & a("RF")),
                  (f"K_REACT w{w} → K1U", kr & a("K_ACT_TO_K1U")),
                  (f"K_REACT w{w} → K2U", kr & a("K_ACT_TO_K2U"))]
    tests += [("K_ACT alone", a("K_ACT_FLAG")),
              ("K_ACT + part5", a("K_ACT_FLAG") & part5),
              ("K_ACT → K1U", a("K_ACT_TO_K1U")),
              ("K_ACT → K2U", a("K_ACT_TO_K2U"))]

    # §8 participation families, alone and incremental over REGIME_UP / K_ACT
    for fam in ("PART_T2G", "PART_F", "PART_FLY", "PART_VBO", "PART_GOG"):
        tests += [(f"{fam} alone", a(fam)),
                  (f"{fam} + REGIME_UP", a(fam) & a("REGIME_UP")),
                  (f"{fam} + K_ACT", a(fam) & a("K_ACT_FLAG"))]
    tests += [("PART_FAMILY_COUNT>=2", a("PART_FAMILY_COUNT") >= 2),
              ("PART_FAMILY_COUNT>=3", a("PART_FAMILY_COUNT") >= 3)]

    rows = []
    for nm, m in tests:
        r = score(m & ok, P, df, Lb, bnd, system="B1_PARTIAL", label=nm)   # no extra layer needed
        r["verdict"] = verdict(r)
        rows.append(r)
    return pd.DataFrame(rows)


# ───────────────────────────────────────────────────────────────────────────
# §10  failure paths
# ───────────────────────────────────────────────────────────────────────────
def failure_paths(df, P, Lb, bnd, fire_events, horizon=10) -> pd.DataFrame:
    ok = Lb["Y3"].notna().to_numpy()
    ev = fire_events & ok
    win = ev & (Lb["Y3"].to_numpy() == 1)
    lose = ev & (Lb["Y3"].to_numpy() == 0)
    k = df["phys_k"].fillna("").astype(str).to_numpy()
    reg = df["phys_regime"].fillna("").astype(str).to_numpy()
    n = len(df)

    def _fwd_any(flag):
        """flag true on any of the next `horizon` bars, per ticker."""
        out = np.zeros(n, bool)
        for s, e in zip(bnd[:-1], bnd[1:]):
            seg = flag[s:e]
            if len(seg) == 0:
                continue
            rev = pd.Series(seg[::-1]).rolling(horizon, min_periods=1).max().to_numpy()[::-1]
            sh = np.r_[rev[1:], 0]
            out[s:e] = sh > 0
        return out

    paths = {
        "K_reset_to_K0": _fwd_any(k == "K0"),
        "regime_went_D": _fwd_any(reg == "D"),
        "participation_vanished": ~_fwd_any(P["PART_ANY"].to_numpy()),
        "K_reactivated": _fwd_any(P["K_ACT_FLAG"].to_numpy()),
        "expanded_RF": _fwd_any(P["RF"].to_numpy()),
        "veto_appeared": _fwd_any(P["VETO_ANY"].to_numpy()),
    }
    rows = []
    for nm, m in paths.items():
        pw = float(m[win].mean()) * 100 if win.any() else np.nan
        pl = float(m[lose].mean()) * 100 if lose.any() else np.nan
        rows.append(dict(path=nm, pct_of_winners=round(pw, 2), pct_of_losers=round(pl, 2),
                         diff_pp=round(pl - pw, 2)))
    return pd.DataFrame(rows).sort_values("diff_pp", ascending=False)


# ═══════════════════════════════════════════════════════════════════════════
# v2 FINAL CONSTRAINTS — approved 2026-09-12, frozen before the run
#
# §1 FROZEN: event cooldown 10 · verdict thresholds · matched-baseline construction ·
#    K_REACT windows 3/6/10 · system definitions · veto handling · target. Any idea that
#    arrives from these results belongs to v3.
# §3 ⚠️ EVENT COLLAPSE IS PER STAGE, NEVER ACROSS STAGES. A REIGN that lands within the
#    cooldown of an IGN1 is a DIFFERENT semantic event and keeps its own anchor. The cooldown
#    suppresses repeated fires of the SAME stage only. `stage_events()` is the only sanctioned
#    entry point and it enforces this by construction.
# ═══════════════════════════════════════════════════════════════════════════
COOLDOWN_SENSITIVITY = (5, 10, 15)     # §2 reported, never selected on
LIQ_BUCKETS = (0, 3e6, 2e7, 1e8, 5e8, np.inf)


def stage_events(stage_masks: dict, bnd, cooldown: int = EVENT_COOLDOWN) -> dict:
    """§3 — collapse EACH stage independently. Cross-stage transitions are never merged."""
    return {name: to_events(m, bnd, cooldown) for name, m in stage_masks.items()}


def liquidity_bucket(df) -> np.ndarray:
    dv = df["close"].to_numpy(float) * df["volume"].to_numpy(float)
    return np.digitize(dv, LIQ_BUCKETS[1:-1]).astype(np.int8)


def matched_mask_liq(fire, df, elig, liq) -> np.ndarray:
    """§4 robustness baseline: matched by (year, liquidity bucket) instead of exact ticker."""
    if not fire.any():
        return np.zeros(len(df), bool)
    yr = df["date"].str[:4].to_numpy()
    cell = pd.Series(yr) + "|" + pd.Series(liq.astype(str))
    return elig & cell.isin(set(cell[fire].unique())).to_numpy()


def boot_ci(fire_events, Lb, df, col="Y3", n_boot=400, seed=20260912):
    """§7 — cluster bootstrap BY TICKER. Correlated events inside a name are not independent."""
    m = fire_events & Lb[col].notna().to_numpy()
    if m.sum() < 30:
        return (None, None)
    y = Lb.loc[m, col].to_numpy(float)
    tk = df.loc[m, "ticker"].to_numpy()
    codes, uniq = pd.factorize(tk)
    by = [y[codes == i] for i in range(len(uniq))]
    rng = np.random.default_rng(seed)
    out = np.empty(n_boot)
    for i in range(n_boot):
        pick = rng.integers(0, len(by), len(by))
        out[i] = np.concatenate([by[j] for j in pick]).mean()
    return (round(float(np.percentile(out, 2.5)) * 100, 2),
            round(float(np.percentile(out, 97.5)) * 100, 2))


def time_to_target(df, bnd, atr_mult=3.0, horizon=20) -> np.ndarray:
    """§5 — bars until the high first reaches close_t + atr_mult*ATR_t. NaN if never in horizon."""
    c = df["close"].to_numpy(float); h = df["high"].to_numpy(float)
    atr = df["atr_14"].to_numpy(float)
    n = len(df); out = np.full(n, np.nan)
    for s, e in zip(bnd[:-1], bnd[1:]):
        hh = h[s:e]; cc = c[s:e]; aa = atr[s:e]; m = e - s
        for i in range(m):
            if not np.isfinite(aa[i]) or aa[i] <= 0:
                continue
            tgt = cc[i] + atr_mult * aa[i]
            j_end = min(i + horizon, m - 1)
            if j_end <= i:
                continue
            w = hh[i + 1:j_end + 1]
            k = np.flatnonzero(w >= tgt)
            if len(k):
                out[s + i] = k[0] + 1
    return out


def score_v2(fire_bar, P, df, Lb, bnd, system, label="", liq=None, ttt=None,
             cooldown=EVENT_COOLDOWN) -> dict:
    """The frozen v2 scorer: event-level primary, three baselines, bootstrap CI, tail diagnostics."""
    ok = Lb["Y3"].notna().to_numpy()
    elig = eligible_mask(system, P, ok)
    fb = fire_bar & elig
    fe = to_events(fb, bnd, cooldown) & elig
    mm = matched_mask(fb, df, elig)
    ml = matched_mask_liq(fb, df, elig, liq) if liq is not None else np.zeros(len(df), bool)
    row = dict(system=label or system, n_bars=int(fb.sum()), n_events=int(fe.sum()),
               tickers=int(df.loc[fe, "ticker"].nunique()))
    if fe.sum() == 0:
        row["PRIMARY_VERDICT"] = "INSUFFICIENT_DATA"
        return row
    for k, col in (("3", "Y3"), ("5", "Y5"), ("8", "Y8")):
        pe = float(Lb.loc[fe, col].mean())
        bf = float(Lb.loc[ok, col].mean())
        be = float(Lb.loc[elig, col].mean())
        bm = float(Lb.loc[mm, col].mean()) if mm.any() else np.nan
        bl2 = float(Lb.loc[ml, col].mean()) if ml.any() else np.nan
        ref = bm if np.isfinite(bm) else be
        row[f"prec_{k}"] = round(pe * 100, 2)
        row[f"base_full_{k}"] = round(bf * 100, 2)
        row[f"base_elig_{k}"] = round(be * 100, 2)
        row[f"base_matched_{k}"] = round(bm * 100, 2) if np.isfinite(bm) else None
        row[f"base_liq_{k}"] = round(bl2 * 100, 2) if np.isfinite(bl2) else None
        row[f"lift_{k}"] = round(pe / ref, 3) if ref else None
        row[f"delta_pp_{k}"] = round((pe - ref) * 100, 2) if ref else None
    lo, hi = boot_ci(fe, Lb, df, "Y3")
    row["prec3_ci95"] = f"[{lo}, {hi}]" if lo is not None else None
    row["bar_prec_3"] = round(float(Lb.loc[fb, "Y3"].mean()) * 100, 2)
    row["med_mfe20"] = round(float(Lb.loc[fe, "MFE20_ATR"].median()), 3)
    row["med_mae20"] = round(float(Lb.loc[fe, "MAE_20_ATR"].median()), 3)
    row["med_fwd20"] = round(float(Lb.loc[fe, "FWD_20"].median()), 3)
    if ttt is not None:
        t = ttt[fe]
        row["med_bars_to_3atr"] = round(float(np.nanmedian(t)), 2) if np.isfinite(t).any() else None
        row["pct_reach_3atr"] = round(float(np.isfinite(t).mean()) * 100, 2)
    # §6 — the primary verdict uses the 3-ATR target only; tail effects are kept but never promote
    row["PRIMARY_VERDICT"] = verdict(dict(n_events=row["n_events"], ev_lift_3=row["lift_3"],
                                          ev_prec_3=row["prec_3"],
                                          base_matched=row["base_matched_3"],
                                          base_eligible=row["base_elig_3"]))
    row["tail_note"] = ("8ATR lift " + str(row.get("lift_8"))) if row.get("lift_8") else None
    return row


def cooldown_sensitivity(fire_bar, P, df, Lb, bnd, system, label, liq) -> pd.DataFrame:
    """§2 — reported, never used to choose. Does the conclusion move with the cooldown?"""
    rows = []
    for cd in COOLDOWN_SENSITIVITY:
        r = score_v2(fire_bar, P, df, Lb, bnd, system, label=f"{label} cd{cd}", liq=liq, cooldown=cd)
        rows.append(dict(system=label, cooldown=cd, n_events=r.get("n_events"),
                         prec_3=r.get("prec_3"), base_matched_3=r.get("base_matched_3"),
                         delta_pp_3=r.get("delta_pp_3"), verdict=r.get("PRIMARY_VERDICT")))
    return pd.DataFrame(rows)


def event_table(stage_ev: dict, df, Lb, system, bnd) -> pd.DataFrame:
    """§3 — one row per event, with everything needed for manual bar-by-bar inspection."""
    frames = []
    for stage, m in stage_ev.items():
        if not m.any():
            continue
        idx = np.flatnonzero(m)
        frames.append(pd.DataFrame(dict(
            system=system, stage=stage,
            ticker=df["ticker"].to_numpy()[idx], event_anchor_date=df["date"].to_numpy()[idx],
            first_fire_date=df["date"].to_numpy()[idx],
            year=pd.Series(df["date"].to_numpy()[idx]).str[:4].to_numpy(),
            eligible_universe=system,
            close=df["close"].to_numpy()[idx], atr=df["atr_14"].to_numpy()[idx],
            phys_k=df["phys_k"].to_numpy()[idx], phys_regime=df["phys_regime"].to_numpy()[idx],
            phys_r=df["phys_r"].to_numpy()[idx], rtb_phase=df["rtb_phase"].to_numpy()[idx],
            t_sig=df["t_sig"].to_numpy()[idx], gog_tier=df["gog_tier"].to_numpy()[idx],
            mfe20_atr=Lb["MFE20_ATR"].to_numpy()[idx], y3=Lb["Y3"].to_numpy()[idx],
            y5=Lb["Y5"].to_numpy()[idx], y8=Lb["Y8"].to_numpy()[idx])))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def k_failure_paths(df, P, Lb, bnd, horizon=10) -> pd.DataFrame:
    """§8 — what the machine did next, for K_ACT and K_REACT, winners vs losers."""
    ok = Lb["Y3"].notna().to_numpy()
    k = df["phys_k"].fillna("").astype(str).to_numpy()
    reg = df["phys_regime"].fillna("").astype(str).to_numpy()
    n = len(df)

    def _fwd_any(flag):
        out = np.zeros(n, bool)
        for s, e in zip(bnd[:-1], bnd[1:]):
            seg = flag[s:e]
            if not len(seg):
                continue
            rev = pd.Series(seg[::-1]).rolling(horizon, min_periods=1).max().to_numpy()[::-1]
            out[s:e] = np.r_[rev[1:], 0] > 0
        return out

    paths = {
        "K_reset_to_K0": _fwd_any(k == "K0"),
        "regime_went_D": _fwd_any(reg == "D"),
        "new_participation": _fwd_any(P["PART_ANY"].to_numpy()),
        "RF_expansion": _fwd_any(P["RF"].to_numpy()),
        "reached_K2U": _fwd_any(k == "K2U"),
        "M_PROGRESS": _fwd_any(P["M_PROGRESS"].to_numpy()),
        "S_PROGRESS": _fwd_any(P["S_PROGRESS"].to_numpy()),
        "second_ignition": _fwd_any(P["K_ACT_FLAG"].to_numpy()),
    }
    kact = to_events(P["K_ACT_FLAG"].to_numpy() & ok, bnd)
    krea = to_events(k_react_flag(P, bnd, 6) & ok, bnd)
    rows = []
    for nm, ev in (("K_ACT", kact), ("K_REACT_w6", krea)):
        w = ev & (Lb["Y3"].to_numpy() == 1)
        l = ev & (Lb["Y3"].to_numpy() == 0)
        for p, m in paths.items():
            rows.append(dict(cohort=nm, path=p, n_win=int(w.sum()), n_lose=int(l.sum()),
                             pct_winners=round(float(m[w].mean()) * 100, 2) if w.any() else None,
                             pct_losers=round(float(m[l].mean()) * 100, 2) if l.any() else None,
                             diff_pp=round((float(m[w].mean()) - float(m[l].mean())) * 100, 2)
                             if w.any() and l.any() else None))
    return pd.DataFrame(rows)
