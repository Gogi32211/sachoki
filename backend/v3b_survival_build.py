"""v3b_survival_build.py — anchors, decision bars and CAUSAL primitives. 2026-09-12.

THE QUESTION. A breakout has already happened. At bar t0+h, using ONLY what was knowable then,
could a better hold/exit decision have been made?

WHY THE CONSTRUCTION MATTERS MORE THAN THE FEATURES. The v2 failure-path table showed a −32.6 pp
gap between winners and losers on "regime went D in the next 10 bars". That is not a predictive
signal: the path window and the outcome window were the SAME 20 bars, so "the regime died" and
"it never made +3 ATR" are two measurements of one event. This module exists to make that
impossible:

        t0          ANCHOR         a causal breakout fired
        t0+h        DECISION BAR   every feature reads bars <= t0+h and nothing else
        t0+h+1 ...  OUTCOME        the race, scored by v3b_survival_eval, strictly afterwards

FROZEN BEFORE THE RUN (approved 2026-09-12)
    anchor           BL1 = close > max(high[t-20 .. t-1]); episode-deduplicated, cooldown 10
                     K_ACT as a SECONDARY robustness anchor, never the primary
    horizons         h = 1, 2, 3, 5 — FOUR SEPARATE research questions. The best h is never
                     chosen; each is reported in full on its own.
    reference        price = close[t_d], ATR = ATR_14[t_d] (the decision bar's, not the anchor's)
    race horizon     20 bars after the decision bar
    splits           2021-2023 discovery · 2024-2025 internal validation · 2026 UNTOUCHED

RAW DESCRIPTORS, NOT RULES. `REGIME_HELD` and `K_HELD` are deliberately left as raw causal
descriptors. "Regime U on every bar of the window" may well be too strict to be a rule; whether
it has incremental value is for discovery to decide on the training split, not for me to decide
here by writing it into the state machine.
"""
from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

DATA = os.path.join(os.path.dirname(HERE), "data")
FRAME = os.path.join(DATA, "breakout_frame_v1.parquet")
OUT_EVENTS = os.path.join(DATA, "v3b_events.parquet")

ANCHOR_COOLDOWN = 10                 # episode dedup; 5/15 are sensitivity only
HORIZONS = (1, 2, 3, 5)
RACE_H = 20                          # bars after the decision bar — frozen
BREAKOUT_LOOKBACK = 20

SPLITS = {                           # by ANCHOR date; the outcome window may not cross the edge
    "DISCOVERY":  ("2021-01-01", "2023-12-31"),
    "VALIDATION": ("2024-01-01", "2025-12-31"),
    "HOLDOUT":    ("2026-01-01", "2099-12-31"),
}

FORBIDDEN_PREFIX = ("fwd_", "mfe_", "mae_", "hit_", "drop_", "bars_to_", "pct_to_",
                    "ret_to_next_", "max_high_", "tt_up", "tt_dn", "vbo_w", "gog_w", "rank_",
                    "is_pivot", "next_pivot", "fwd_swing", "swing_ret_from")
FORBIDDEN_EXACT = {"swing_type", "seq34_win", "edge_gold", "turbo_score", "final_bull_score",
                   "ultra_score", "ultra_score_v3", "buy_score", "prebreak_v2", "prebreak_v3",
                   "beta_score", "gog_score", "profile_score", "rtb_total"}

_M_RANK = {"M0": 0, "M1": 1, "M2": 2}


def _bounds(tk: np.ndarray) -> np.ndarray:
    chg = np.r_[True, tk[1:] != tk[:-1]]
    return np.r_[np.flatnonzero(chg), len(tk)]


def load_frame(log=print) -> pd.DataFrame:
    df = pd.read_parquet(FRAME)
    bad = [c for c in df.columns
           if c.lower() in FORBIDDEN_EXACT or c.lower().startswith(FORBIDDEN_PREFIX)]
    assert not bad, f"FORBIDDEN COLUMNS IN THE FRAME: {bad}"
    df = df.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    log(f"  frame {len(df):,} rows · {df['ticker'].nunique():,} tickers "
        f"· {df['date'].min()}..{df['date'].max()}")
    return df


def derive_series(df: pd.DataFrame, bnd: np.ndarray) -> dict:
    """Per-bar series the anchors and policies need. All backward-looking."""
    c = df["close"].to_numpy(float)
    h = df["high"].to_numpy(float)
    n = len(df)
    prior_hi = np.full(n, np.nan)
    ema20 = np.full(n, np.nan)
    for a, b in zip(bnd[:-1], bnd[1:]):
        s = pd.Series(h[a:b])
        prior_hi[a:b] = s.shift(1).rolling(BREAKOUT_LOOKBACK,
                                           min_periods=BREAKOUT_LOOKBACK).max().to_numpy()
        ema20[a:b] = pd.Series(c[a:b]).ewm(span=20, adjust=False).mean().to_numpy()
    return dict(prior_hi=prior_hi, ema20=ema20)


def anchors(df: pd.DataFrame, bnd: np.ndarray, ser: dict, kind: str = "BL1",
            cooldown: int = ANCHOR_COOLDOWN) -> np.ndarray:
    """Episode-deduplicated anchor indices. The FIRST bar of a breakout episode only."""
    c = df["close"].to_numpy(float)
    if kind == "BL1":
        fire = c > ser["prior_hi"]
        fire = np.where(np.isfinite(ser["prior_hi"]), fire, False)
    elif kind == "K_ACT":
        k = df["phys_k"].fillna("").astype(str).to_numpy()
        prev = np.empty_like(k)
        for a, b in zip(bnd[:-1], bnd[1:]):
            prev[a] = ""
            if b > a + 1:
                prev[a + 1:b] = k[a:b - 1]
        fire = np.isin(k, ["K1U", "K2U"]) & np.isin(prev, ["K0", "K1D", "K2D"])
    else:
        raise ValueError(kind)

    idx = np.arange(len(df), dtype=np.int64)
    keep = np.zeros(len(df), bool)
    for a, b in zip(bnd[:-1], bnd[1:]):
        f = fire[a:b]
        if not f.any():
            continue
        last = np.maximum.accumulate(np.where(f, idx[a:b], -1))
        prev_last = np.r_[-1, last[:-1]]
        gap = np.where(prev_last >= 0, idx[a:b] - prev_last, 10 ** 6)
        keep[a:b] = f & (gap > cooldown)
    return np.flatnonzero(keep)


def _block_end(i: np.ndarray, bnd: np.ndarray) -> np.ndarray:
    """Exclusive end index of each row's ticker block."""
    return bnd[np.searchsorted(bnd, i, side="right")]


def build_events(df: pd.DataFrame, bnd: np.ndarray, ser: dict, kind: str = "BL1",
                 log=print) -> pd.DataFrame:
    """One row per (anchor, horizon) with every causal primitive. No outcome of any kind."""
    a_idx = anchors(df, bnd, ser, kind)
    log(f"  {kind}: {len(a_idx):,} deduplicated anchors (cooldown {ANCHOR_COOLDOWN})")

    close = df["close"].to_numpy(float); high = df["high"].to_numpy(float)
    atr = df["atr_14"].to_numpy(float)
    reg = df["phys_regime"].fillna("").astype(str).to_numpy()
    kcl = df["phys_k"].fillna("").astype(str).to_numpy()
    mcl = df["phys_m"].fillna("").astype(str).to_numpy()
    scl = df["phys_s"].fillna("").astype(str).to_numpy()
    part = (df["t_sig"].fillna("").astype(str).to_numpy() != "")
    for cflag in ("sig_f1", "sig_f3", "sig_f6", "sig_f7", "sig_fly_abcd", "sig_fly_cd", "vbo_up"):
        if cflag in df.columns:
            part = part | (df[cflag].fillna(0).to_numpy(float) > 0)
    part = part | (df["gog_tier"].fillna("").astype(str).to_numpy() != "")
    vol7 = df["VOL7_MR"].to_numpy(float)
    tk = df["ticker"].to_numpy(); date = df["date"].to_numpy().astype(str)
    m_rank = pd.Series(mcl).map(_M_RANK).to_numpy(float)
    s_num = pd.Series(scl).str.extract(r"S(\d)", expand=False).astype(float).to_numpy()
    s_rank = np.where(np.char.endswith(scl.astype(str), "U"), s_num, -s_num)

    rows = []
    for h in HORIZONS:
        i0 = a_idx
        i_d = i0 + h
        end = _block_end(i0, bnd)
        # the decision bar AND the whole race window must live inside the same ticker block
        ok = (i_d + RACE_H) < end
        i0, i_d = i0[ok], i_d[ok]
        if len(i0) == 0:
            continue
        ok2 = np.isfinite(atr[i_d]) & (atr[i_d] > 0) & np.isfinite(close[i_d])
        i0, i_d = i0[ok2], i_d[ok2]

        # ── window descriptors: bars i0 .. i_d only ──────────────────────────
        offs = np.arange(h + 1)
        reg_win = np.stack([reg[i0 + o] for o in offs])           # (h+1, n_events)
        k_win = np.stack([kcl[i0 + o] for o in offs])
        part_win = np.stack([part[i0 + o] for o in offs])
        high_win = np.stack([high[i0 + o] for o in offs])

        regime_held = (reg_win == "U").all(axis=0)
        regime_lost = (reg_win[1:] == "D").any(axis=0) if h >= 1 else np.zeros(len(i0), bool)
        k_bull_win = np.isin(k_win, ["K1U", "K2U"])
        k_reset = (k_win[1:] == "K0").any(axis=0) if h >= 1 else np.zeros(len(i0), bool)
        k_held = k_bull_win.all(axis=0)
        krank = np.vectorize({"K2D": -2, "K1D": -1, "K0": 0, "K1U": 1, "K2U": 2}.get,
                             otypes=[float])(k_win)
        k_advanced = krank[-1] > krank[0]
        new_part = part_win[1:].any(axis=0) if h >= 1 else np.zeros(len(i0), bool)
        max_hi_win = high_win.max(axis=0)
        mfe_so_far = (np.stack([high[i0 + o] for o in offs[1:]]).max(axis=0) - close[i0]) / atr[i0] \
            if h >= 1 else np.zeros(len(i0))
        giveback = (close[i_d] - max_hi_win) / atr[i_d]

        rows.append(pd.DataFrame(dict(
            anchor_kind=kind, h=h, i0=i0, i_d=i_d,
            ticker=tk[i0], anchor_date=date[i0], decision_date=date[i_d],
            year=pd.Series(date[i0]).str[:4].to_numpy(),
            ref_close=close[i_d], ref_atr=atr[i_d],
            # state AT the decision bar
            REGIME_D=reg[i_d], K_D=kcl[i_d], M_D=mcl[i_d], S_D=scl[i_d],
            R_D=df["phys_r"].fillna("").astype(str).to_numpy()[i_d],
            E_D=df["phys_e"].fillna("").astype(str).to_numpy()[i_d],
            H_D=df["phys_h"].fillna("").astype(str).to_numpy()[i_d],
            RSI_D=df["rsi_14"].to_numpy(float)[i_d],
            VOL7_D=vol7[i_d],
            DOLLAR_VOL_D=close[i_d] * df["volume"].to_numpy(float)[i_d],
            K_AT_DECISION_IS_K2U=(kcl[i_d] == "K2U"),
            REGIME_UP_AT_D=(reg[i_d] == "U"),
            # what happened between the anchor and the decision bar
            REGIME_HELD=regime_held, REGIME_LOST=regime_lost,
            K_HELD=k_held, K_RESET=k_reset, K_ADVANCED=k_advanced,
            M_PROGRESS=(m_rank[i_d] > m_rank[i0]), S_PROGRESS=(s_rank[i_d] > s_rank[i0]),
            NEW_PART=new_part,
            FOLLOW_THROUGH=(close[i_d] > close[i0]),
            MFE_SO_FAR=mfe_so_far, GIVEBACK=giveback,
            VOL_EXPANSION=(vol7[i_d] - vol7[i0]),
        )))
        log(f"    h={h}: {len(i0):,} events")

    E = pd.concat(rows, ignore_index=True)
    E["split"] = ""
    for name, (lo, hi) in SPLITS.items():
        E.loc[(E["anchor_date"] >= lo) & (E["anchor_date"] <= hi), "split"] = name
    # an event may not have its outcome window cross a split edge
    end_date = date[np.minimum(E["i_d"].to_numpy() + RACE_H, len(date) - 1)]
    E["outcome_end_date"] = end_date
    for name, (lo, hi) in SPLITS.items():
        if name == "HOLDOUT":
            continue
        bad = (E["split"] == name) & (E["outcome_end_date"] > hi)
        E.loc[bad, "split"] = "EDGE_DROPPED"
    log(f"  events {len(E):,} · split "
        f"{E['split'].value_counts().to_dict()}")
    return E


def causality_audit(df, bnd, ser, E: pd.DataFrame, n_sample=500, seed=20260912, log=print) -> dict:
    """THE TEST THAT WOULD HAVE CAUGHT THE v2 TAUTOLOGY.

    Recompute every primitive for a sample of events on a frame TRUNCATED EXACTLY at the decision
    bar. If any value moves, a feature is reading the future and the run must not continue.
    """
    rng = np.random.default_rng(seed)
    samp = E.sample(min(n_sample, len(E)), random_state=seed)
    feat_cols = [c for c in E.columns if c not in
                 ("anchor_kind", "h", "i0", "i_d", "ticker", "anchor_date", "decision_date",
                  "year", "split", "outcome_end_date")]
    diffs = []
    for _, ev in samp.iterrows():
        i_d = int(ev["i_d"])
        blk_start = bnd[np.searchsorted(bnd, i_d, side="right") - 1]
        sub = df.iloc[blk_start:i_d + 1].reset_index(drop=True)      # nothing after the decision bar
        sb = _bounds(sub["ticker"].to_numpy())
        ss = derive_series(sub, sb)
        # rebuild the same event inside the truncated frame: it is the last bar
        i0_local = int(ev["i0"]) - blk_start
        i_d_local = i_d - blk_start
        assert i_d_local == len(sub) - 1
        got = _single_event_primitives(sub, ss, i0_local, i_d_local, int(ev["h"]))
        for c in feat_cols:
            if c not in got:
                continue
            a, b = ev[c], got[c]
            same = (a == b) if not isinstance(a, float) else (
                (np.isnan(a) and np.isnan(b)) or np.isclose(float(a), float(b), rtol=1e-9,
                                                            atol=1e-12))
            if not same:
                diffs.append((ev["ticker"], ev["decision_date"], c, a, b))
    log(f"  causality audit: {len(samp)} events × {len(feat_cols)} primitives "
        f"= {len(samp)*len(feat_cols):,} comparisons · differences {len(diffs)}")
    if diffs:
        log(f"    FIRST DIFFS: {diffs[:5]}")
    return dict(sampled=int(len(samp)), primitives=len(feat_cols),
                comparisons=int(len(samp) * len(feat_cols)), differences=len(diffs),
                examples=diffs[:10])


def _single_event_primitives(sub: pd.DataFrame, ser: dict, i0: int, i_d: int, h: int) -> dict:
    """The same primitive definitions, computed for ONE event on a truncated frame."""
    close = sub["close"].to_numpy(float); high = sub["high"].to_numpy(float)
    atr = sub["atr_14"].to_numpy(float)
    reg = sub["phys_regime"].fillna("").astype(str).to_numpy()
    kcl = sub["phys_k"].fillna("").astype(str).to_numpy()
    mcl = sub["phys_m"].fillna("").astype(str).to_numpy()
    scl = sub["phys_s"].fillna("").astype(str).to_numpy()
    part = (sub["t_sig"].fillna("").astype(str).to_numpy() != "")
    for cflag in ("sig_f1", "sig_f3", "sig_f6", "sig_f7", "sig_fly_abcd", "sig_fly_cd", "vbo_up"):
        if cflag in sub.columns:
            part = part | (sub[cflag].fillna(0).to_numpy(float) > 0)
    part = part | (sub["gog_tier"].fillna("").astype(str).to_numpy() != "")
    vol7 = sub["VOL7_MR"].to_numpy(float)
    kmap = {"K2D": -2, "K1D": -1, "K0": 0, "K1U": 1, "K2U": 2}
    w = slice(i0, i_d + 1)
    m_rank = pd.Series(mcl).map(_M_RANK).to_numpy(float)
    s_num = pd.Series(scl).str.extract(r"S(\d)", expand=False).astype(float).to_numpy()
    s_rank = np.where(np.char.endswith(scl.astype(str), "U"), s_num, -s_num)
    return dict(
        ref_close=close[i_d], ref_atr=atr[i_d],
        REGIME_D=reg[i_d], K_D=kcl[i_d], M_D=mcl[i_d], S_D=scl[i_d],
        R_D=sub["phys_r"].fillna("").astype(str).to_numpy()[i_d],
        E_D=sub["phys_e"].fillna("").astype(str).to_numpy()[i_d],
        H_D=sub["phys_h"].fillna("").astype(str).to_numpy()[i_d],
        RSI_D=sub["rsi_14"].to_numpy(float)[i_d], VOL7_D=vol7[i_d],
        DOLLAR_VOL_D=close[i_d] * sub["volume"].to_numpy(float)[i_d],
        K_AT_DECISION_IS_K2U=(kcl[i_d] == "K2U"), REGIME_UP_AT_D=(reg[i_d] == "U"),
        REGIME_HELD=bool((reg[w] == "U").all()),
        REGIME_LOST=bool((reg[i0 + 1:i_d + 1] == "D").any()) if h >= 1 else False,
        K_HELD=bool(np.isin(kcl[w], ["K1U", "K2U"]).all()),
        K_RESET=bool((kcl[i0 + 1:i_d + 1] == "K0").any()) if h >= 1 else False,
        K_ADVANCED=bool(kmap.get(kcl[i_d], 0) > kmap.get(kcl[i0], 0)),
        M_PROGRESS=bool(m_rank[i_d] > m_rank[i0]), S_PROGRESS=bool(s_rank[i_d] > s_rank[i0]),
        NEW_PART=bool(part[i0 + 1:i_d + 1].any()) if h >= 1 else False,
        FOLLOW_THROUGH=bool(close[i_d] > close[i0]),
        MFE_SO_FAR=float((high[i0 + 1:i_d + 1].max() - close[i0]) / atr[i0]) if h >= 1 else 0.0,
        GIVEBACK=float((close[i_d] - high[w].max()) / atr[i_d]),
        VOL_EXPANSION=float(vol7[i_d] - vol7[i0]),
    )


def build(log=print):
    df = load_frame(log)
    bnd = _bounds(df["ticker"].to_numpy())
    ser = derive_series(df, bnd)
    frames = [build_events(df, bnd, ser, k, log) for k in ("BL1", "K_ACT")]
    E = pd.concat(frames, ignore_index=True)
    audit = causality_audit(df, bnd, ser, E[E["anchor_kind"] == "BL1"], log=log)
    assert audit["differences"] == 0, "CAUSALITY AUDIT FAILED — a primitive reads the future"
    E.to_parquet(OUT_EVENTS, index=False)
    log(f"  wrote {OUT_EVENTS} · {len(E):,} events")
    import json
    json.dump(audit, open(OUT_EVENTS.replace(".parquet", "_audit.json"), "w"),
              indent=1, default=str)
    return E, audit


if __name__ == "__main__":
    build()
