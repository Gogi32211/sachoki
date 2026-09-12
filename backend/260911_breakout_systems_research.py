"""260911_breakout_systems_research.py — five breakout-path state machines, measured.

APPROVED PLAN, 2026-09-12. Broad universe (SP500+Nasdaq, 3,865,401 stock-days, 4,167 tickers,
one schema, volume present) built by breakout_frame_build.py. The 28 chart-export CSVs are
inspection cases only and are not read here.

WHAT IS FORBIDDEN AS INPUT, asserted in the loader rather than remembered:
    LOOKAHEAD  swing_type · FWD_* · MAX_HIGH_* · HIT_* · BARS_TO_* · VBO_W* · GOG_W* ·
               RET_TO_NEXT_* · SEQ34_WIN · EDGE_GOLD · RANK_*
    FITTED     turbo_score · FINAL_BULL_SCORE + every sub-score · ultra_score* · prebreak_* ·
               beta_* · GOG_SCORE · BUY_SCORE · PROFILE_SCORE · rtb_total and its numeric parts
    DEAD       the 114 measured columns
Labels are built in a separate pass, from raw OHLC, AFTER every feature and stage column exists.

THE APPROVAL'S TWO CORRECTIONS TO MY FIRST DRAFT, both implemented here:
  §9  B1 is a WINDOWED PARTIAL ORDER. TURN and PARTICIPATION may arrive in either order inside a
      local window; whether TURN-before-PART helps is MEASURED (B1_ORDERED vs B1_PARTIAL), not
      assumed. The same principle is applied anywhere sequencing is not yet proven.
  §10 `LBAL_IMPROVING OR true` is gone — that clause made LBAL meaningless. B2 is split into
      B2a / B2b / B2c and the three are compared out-of-sample.

FAMILIES ARE NEVER COLLAPSED (§5, §6). TURN keeps six subfamilies and PARTICIPATION five, so a
report can say WHICH turn path and WHICH participation mix preceded a winner. The aggregate flags
exist only for state-machine convenience.

NOT EVALUATED THIS PASS: mtf_echo, and therefore the MTF_ZERO veto. It needs a 4H/1H intraday
REV-day join and is gated on `rev_buy`. Reported as not-evaluated; no substitute is invented.
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import OrderedDict

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import para_target_v1 as T                                               # noqa: E402

DATA = os.path.join(os.path.dirname(HERE), "data")
FRAME = os.path.join(DATA, "breakout_frame_v1.parquet")
OUTDIR = os.path.join(os.path.dirname(HERE), "research_out")

FORBIDDEN_PREFIX = ("fwd_", "mfe_", "mae_", "hit_", "drop_", "bars_to_", "pct_to_",
                    "ret_to_next_", "max_high_", "max_low_", "min_low_", "tt_up", "tt_dn",
                    "vbo_w", "gog_w", "rank_", "is_pivot", "next_pivot", "fwd_swing",
                    "swing_ret_from")
FORBIDDEN_EXACT = {"swing_type", "swing_type_3", "swing_type_5", "seq34_win", "edge_gold",
                   "turbo_score", "final_bull_score", "rocket_score", "clean_entry_score",
                   "shakeout_absorb_score", "extra_bull_score", "experimental_score",
                   "rebound_squeeze_score", "hard_bear_score", "volatility_risk_score",
                   "final_regime", "final_score_bucket", "ultra_score", "ultra_score_v3",
                   "buy_score", "prebreak_v2", "prebreak_v3", "prebreak_score", "profile_score",
                   "gog_score", "rtb_total", "rtb_build", "rtb_turn", "rtb_ready",
                   "rtb_bonus3", "rtb_late", "beta_score", "beta_raw"}

# Walk-forward folds: train is everything up to and including the year named, test is the next.
FOLDS = [("2023-12-31", "2024-01-01", "2024-12-31"),
         ("2024-12-31", "2025-01-01", "2025-12-31"),
         ("2025-12-31", "2026-01-01", "2026-12-31")]
# The whole declared parameter grid. Small, and every choice is made on TRAIN only.
GRID_EXPIRY = [5, 10, 15]
GRID_REACT = [3, 6, 10]
MIN_FIRES_TRAIN = 200          # a variant must have support on train to be selectable


# ───────────────────────────────────────────────────────────────────────────
# group-aware helpers — the frame is sorted by (ticker, date)
# ───────────────────────────────────────────────────────────────────────────
def _bounds(tk: np.ndarray):
    """Start index of every ticker block."""
    chg = np.r_[True, tk[1:] != tk[:-1]]
    starts = np.flatnonzero(chg)
    return np.r_[starts, len(tk)]


def _bars_since(flag: np.ndarray, bnd: np.ndarray) -> np.ndarray:
    """Bars since `flag` was last True, per ticker. 10**6 when never. Vectorised per block."""
    n = len(flag)
    out = np.full(n, 10 ** 6, np.int32)
    idx = np.arange(n, dtype=np.int64)
    for a, b in zip(bnd[:-1], bnd[1:]):
        if b <= a:
            continue
        last = np.where(flag[a:b], idx[a:b], -1)
        last = np.maximum.accumulate(last)
        seen = last >= 0
        d = np.full(b - a, 10 ** 6, np.int32)
        d[seen] = (idx[a:b][seen] - last[seen]).astype(np.int32)
        out[a:b] = d
    return out


def _recent(flag: np.ndarray, w: int, bnd: np.ndarray, include_now=True) -> np.ndarray:
    """flag fired within the last w bars (inclusive of this bar when include_now)."""
    bs = _bars_since(flag, bnd)
    return (bs <= w) if include_now else ((bs <= w) & (bs >= 1))


def _run_len(flag: np.ndarray, bnd: np.ndarray) -> np.ndarray:
    """Length of the current consecutive True run, per ticker."""
    n = len(flag)
    out = np.zeros(n, np.int32)
    for a, b in zip(bnd[:-1], bnd[1:]):
        c = 0
        seg = flag[a:b]
        o = np.zeros(b - a, np.int32)
        for i in range(b - a):
            c = c + 1 if seg[i] else 0
            o[i] = c
        out[a:b] = o
    return out


def _shift1(a: np.ndarray, bnd: np.ndarray, fill):
    out = np.empty_like(a)
    for s, e in zip(bnd[:-1], bnd[1:]):
        out[s] = fill
        if e > s + 1:
            out[s + 1:e] = a[s:e - 1]
    return out


def _b(df, c):
    """A stored flag as a clean boolean. Missing column -> all False, stated once by the caller."""
    if c not in df.columns:
        return np.zeros(len(df), bool)
    s = df[c]
    if s.dtype == object:
        return s.astype(str).str.strip().str.lower().isin(["1", "true", "t", "yes"]).to_numpy()
    return s.fillna(0).to_numpy(float) > 0


# ───────────────────────────────────────────────────────────────────────────
# §1  load
# ───────────────────────────────────────────────────────────────────────────
def load(log=print) -> pd.DataFrame:
    df = pd.read_parquet(FRAME)
    bad = [c for c in df.columns
           if c.lower() in FORBIDDEN_EXACT or c.lower().startswith(FORBIDDEN_PREFIX)]
    assert not bad, f"FORBIDDEN COLUMNS REACHED THE FRAME: {bad}"
    df = df.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    log(f"  frame {len(df):,} rows × {df.shape[1]} cols · {df['ticker'].nunique():,} tickers "
        f"· {df['date'].min()}..{df['date'].max()}")
    log(f"  forbidden-column assertion PASSED ({len(FORBIDDEN_EXACT)} exact + "
        f"{len(FORBIDDEN_PREFIX)} prefixes checked)")
    return df


# ───────────────────────────────────────────────────────────────────────────
# §2  primitives — families preserved
# ───────────────────────────────────────────────────────────────────────────
def primitives(df: pd.DataFrame, log=print) -> pd.DataFrame:
    tk = df["ticker"].to_numpy()
    bnd = _bounds(tk)
    P = pd.DataFrame(index=df.index)

    # ── regime ──
    reg = df["phys_regime"].fillna("").astype(str).to_numpy()
    P["REGIME_UP"] = reg == "U"
    prev_reg = _shift1(reg, bnd, "")
    P["REGIME_D_TO_U"] = (reg == "U") & (prev_reg == "D")
    rl = _run_len(P["REGIME_UP"].to_numpy(), bnd)
    for n in (3, 5, 8):
        P[f"REGIME_PERSIST_{n}"] = rl >= n

    # ── K: the backbone. PHYS_K ∈ {K0, K1U, K1D, K2U, K2D} ──
    k = df["phys_k"].fillna("").astype(str).to_numpy()
    prev_k = _shift1(k, bnd, "")
    P["K_BULL"] = np.isin(k, ["K1U", "K2U"])
    P["K_ACT_FLAG"] = P["K_BULL"].to_numpy() & np.isin(prev_k, ["K0", "K1D", "K2D"])
    P["K_ACT_TO_K1U"] = P["K_ACT_FLAG"].to_numpy() & (k == "K1U")   # kept separate (§7)
    P["K_ACT_TO_K2U"] = P["K_ACT_FLAG"].to_numpy() & (k == "K2U")
    for r in (3, 5, 8):
        P[f"K_RESET_FLAG_{r}"] = (k == "K0") & _recent(P["K_BULL"].to_numpy(), r, bnd,
                                                       include_now=False)
    P["K_RESET_FLAG"] = P["K_RESET_FLAG_5"]

    # ── TURN, six subfamilies kept separate (§5) ──
    rtbt = df["RTB_TRANSITION"].fillna("").astype(str).to_numpy()
    P["TURN_HILO"] = _b(df, "hilo_buy")
    P["TURN_RTV"] = _b(df, "rtv")
    P["TURN_REGIME"] = P["REGIME_D_TO_U"].to_numpy()
    P["TURN_LRECLAIM"] = _b(df, "l34") & _recent(_b(df, "l43") | _b(df, "l22") | _b(df, "l34"),
                                                 3, bnd, include_now=False)
    P["TURN_RTBA_B"] = rtbt == "A_TO_B"
    P["TURN_BO"] = _b(df, "bo_up")
    TURN_FAMS = ["TURN_HILO", "TURN_RTV", "TURN_REGIME", "TURN_LRECLAIM", "TURN_RTBA_B", "TURN_BO"]
    P["TURN_ANY"] = P[TURN_FAMS].any(axis=1)
    P["TURN_ATTEMPT_ONLY"] = P[["TURN_HILO", "TURN_RTV", "TURN_LRECLAIM", "TURN_REGIME",
                                "TURN_RTBA_B"]].any(axis=1)          # VARIANT_A: drops BO
    P["TURN_N"] = P[TURN_FAMS].sum(axis=1).astype(np.int8)

    # ── PARTICIPATION, five families kept separate (§6) ──
    t_sig = df["t_sig"].fillna("").astype(str).to_numpy()
    P["PART_T"] = t_sig != ""
    P["PART_T2G"] = t_sig == "T2G"
    P["PART_F"] = (_b(df, "sig_f1") | _b(df, "sig_f3") | _b(df, "sig_f6") | _b(df, "sig_f7"))
    P["PART_FLY"] = (_b(df, "sig_fly_abcd") | _b(df, "sig_fly_cd") |
                     _b(df, "sig_fly_bd") | _b(df, "sig_fly_ad"))
    P["PART_VBO"] = _b(df, "vbo_up")
    P["PART_GOG"] = df["gog_tier"].fillna("").astype(str).to_numpy() != ""
    PART_FAMS = ["PART_T2G", "PART_F", "PART_FLY", "PART_VBO", "PART_GOG"]
    P["PART_ANY"] = P[PART_FAMS].any(axis=1)
    P["PART_FAMILY_COUNT"] = P[PART_FAMS].sum(axis=1).astype(np.int8)

    # ── LBAL, as raw candidates only (§4) — absence is NO INFORMATION, not False ──
    udn = df["LBAL_UDN"].fillna("").astype(str).to_numpy()
    P["LBAL_PRESENT"] = udn != ""
    P["LBAL_U"] = udn == "U"
    net = (df["LBAL_NPOS"].fillna(0).to_numpy(float) - df["LBAL_NNEG"].fillna(0).to_numpy(float))
    P["LBAL_NET"] = net
    P["LBAL_NET_RISING"] = P["LBAL_PRESENT"].to_numpy() & (net > _shift1(net, bnd, np.nan))
    lu = _run_len(P["LBAL_U"].to_numpy(), bnd)
    P["LBAL_U_PERSIST_2"] = lu >= 2
    P["LBAL_U_PERSIST_3"] = lu >= 3

    # ── load / release, tested separately, never mandatory ──
    e = df["phys_e"].fillna("").astype(str).to_numpy()
    P["E_LOAD"] = np.char.startswith(e.astype(str), "E2")
    P["E_RELEASE"] = _b(df, "phys_e_release")

    # ── trend memory: four ingredients kept separate, only 2-of-4 / 3-of-4 (§8) ──
    m = df["phys_m"].fillna("").astype(str).to_numpy()
    s = df["phys_s"].fillna("").astype(str).to_numpy()
    P["TM_K"] = _recent(P["K_BULL"].to_numpy(), 20, bnd)
    P["TM_M"] = np.isin(m, ["M1", "M2"])
    P["TM_S"] = np.isin(s, ["S2U", "S3U"])
    P["TM_REGIME"] = P["REGIME_PERSIST_5"].to_numpy()
    tmn = P[["TM_K", "TM_M", "TM_S", "TM_REGIME"]].sum(axis=1).astype(np.int8)
    P["TM_N"] = tmn
    P["TREND_MEMORY_2"] = tmn >= 2
    P["TREND_MEMORY_3"] = tmn >= 3

    # ── expansion: RF is never sufficient on its own ──
    r = df["phys_r"].fillna("").astype(str).to_numpy()
    P["RF"] = r == "RF"
    m_rank = pd.Series(m).map({"M0": 0, "M1": 1, "M2": 2}).to_numpy(float)
    s_rank = pd.Series(s).str.extract(r"S(\d)", expand=False).astype(float).to_numpy()
    s_up = np.char.endswith(s.astype(str), "U")
    s_rank = np.where(s_up, s_rank, -s_rank)
    P["M_PROGRESS"] = m_rank > _shift1(m_rank, bnd, np.nan)
    P["S_PROGRESS"] = s_rank > _shift1(s_rank, bnd, np.nan)
    P["EXPANSION_FLAG"] = P["RF"].to_numpy() & (
        _recent(P["PART_ANY"].to_numpy(), 3, bnd) | (k == "K2U")
        | P["M_PROGRESS"].to_numpy() | P["S_PROGRESS"].to_numpy())

    # ── RTB / RSI, for B3 ──
    ph = df["rtb_phase"].fillna("0").astype(str).to_numpy()
    P["RTB_AB"] = np.isin(ph, ["A", "B"])
    P["RTB_A"] = ph == "A"
    rsi = df["rsi_14"].to_numpy(float)
    P["RSI_LT35"] = rsi < 35
    P["EXHAUST"] = P["RTB_AB"].to_numpy() & P["RSI_LT35"].to_numpy()

    # ── GOG context tokens, from the stored token strings ──
    ctx = df["context_tokens"].fillna("").astype(str)
    P["CTX_COMPRESSION"] = ctx.str.contains(r"\bSQB\b|\bBCT\b|\bWRC\b|\bF8C\b", regex=True).to_numpy()

    # ── vetoes: separate modifiers (§14). Absence of the layer is NOT a veto ──
    P["VETO_VOL"] = df["VOL7_MR"].fillna(-1).to_numpy(float) == 6
    P["VETO_LVX"] = df["LVX_TIER"].fillna(-1).to_numpy(float) == 0
    P["VETO_SHAPE"] = _b(df, "SHAPE_LSTUP_VETO")
    P["VETO_ANY"] = P[["VETO_VOL", "VETO_LVX", "VETO_SHAPE"]].any(axis=1)

    log(f"  primitives {P.shape[1]} columns")
    return P


def primitive_frequencies(P: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for c in P.columns:
        s = P[c]
        if s.dtype == bool:
            rows.append((c, round(float(s.mean()) * 100, 3)))
        elif np.issubdtype(s.dtype, np.integer):
            rows.append((c, round(float(s.mean()), 3)))
    return pd.DataFrame(rows, columns=["primitive", "rate_pct_or_mean"]).sort_values("primitive")


# ───────────────────────────────────────────────────────────────────────────
# §3  state machines
# ───────────────────────────────────────────────────────────────────────────
def _stage_machine(steps, bnd, n, expiry):
    """Generic forward-only stage machine.

    `steps` is an ordered list of (stage_name, condition_array). A bar advances to stage i+1 when
    its condition holds AND stage i was reached within `expiry` bars. Stage 0's condition is the
    entry. Returns (stage_label array, dict of per-stage boolean arrays, expiry-death count).
    """
    reached = {}
    prev_name, prev_arr = None, None
    deaths = 0
    for i, (name, cond) in enumerate(steps):
        if i == 0:
            arr = cond.copy()
        else:
            ok = _recent(prev_arr, expiry, bnd, include_now=True)
            arr = cond & ok
            deaths += int((cond & ~ok).sum())
        reached[name] = arr
        prev_name, prev_arr = name, arr
    label = np.full(n, "NONE", dtype=object)
    for name, arr in reached.items():          # later stages overwrite earlier ones
        label[arr] = name
    return label, reached, deaths


def build_systems(df, P, bnd, expiry_by_system, react_window, log=print):
    n = len(df)
    S, meta = {}, {}
    a = lambda c: P[c].to_numpy()                                        # noqa: E731

    # ── B1 LOADED — windowed PARTIAL ORDER (§9). Both orderings are built and compared. ──
    watch = a("E_LOAD") | a("RTB_A") | a("CTX_COMPRESSION")
    e1 = expiry_by_system["B1"]
    w_recent = _recent(watch, 10, bnd)
    turn_in_ctx = a("TURN_ANY") & w_recent
    part_in_ctx = a("PART_ANY") & w_recent
    # PARTIAL: both TURN and PART present inside the same local window, either order
    both = (_recent(turn_in_ctx, e1, bnd) & _recent(part_in_ctx, e1, bnd)
            & (turn_in_ctx | part_in_ctx))
    b1_steps_partial = [("WATCH", watch), ("EARLY", turn_in_ctx | part_in_ctx),
                        ("IGN", both), ("CONF", a("K_ACT_FLAG")), ("EXP", a("EXPANSION_FLAG"))]
    b1_steps_ordered = [("WATCH", watch), ("EARLY", turn_in_ctx),
                        ("IGN", part_in_ctx), ("CONF", a("K_ACT_FLAG")),
                        ("EXP", a("EXPANSION_FLAG"))]
    S["B1_PARTIAL"], r1p, d1p = _stage_machine(b1_steps_partial, bnd, n, e1)
    S["B1_ORDERED"], r1o, d1o = _stage_machine(b1_steps_ordered, bnd, n, e1)
    meta["B1_PARTIAL"] = dict(terminal="EXP", reached=r1p, expiry_deaths=d1p, expiry=e1)
    meta["B1_ORDERED"] = dict(terminal="EXP", reached=r1o, expiry_deaths=d1o, expiry=e1)

    # ── B2 MOMENTUM — three explicit variants (§10). No `OR true` anywhere. ──
    e2 = expiry_by_system["B2"]
    ign2 = a("PART_VBO") | a("PART_FLY") | a("PART_T2G")
    exp2 = (_recent(a("PART_ANY"), 3, bnd, include_now=False) | a("RF")
            | (df["phys_k"].fillna("").astype(str).to_numpy() == "K2U") | a("M_PROGRESS"))
    variants = {
        "B2a": a("REGIME_UP"),
        "B2b": a("REGIME_UP") & a("LBAL_U"),
        "B2c": a("REGIME_PERSIST_5"),
    }
    for nm, early in variants.items():
        steps = [("EARLY", early), ("IGN", ign2), ("CONF", a("K_ACT_FLAG")), ("EXP", exp2)]
        S[nm], rr, dd = _stage_machine(steps, bnd, n, e2)
        meta[nm] = dict(terminal="EXP", reached=rr, expiry_deaths=dd, expiry=e2)

    # ── B3 REVERSAL — K is the LAST step, never an early gate ──
    e3 = expiry_by_system["B3"]
    steps3 = [("EXHAUST", a("EXHAUST")), ("TURN", a("TURN_ANY")), ("EARLY", a("PART_ANY")),
              ("VALID", (a("REGIME_D_TO_U") | a("REGIME_UP")) & a("K_ACT_FLAG"))]
    S["B3"], r3, d3 = _stage_machine(steps3, bnd, n, e3)
    meta["B3"] = dict(terminal="VALID", reached=r3, expiry_deaths=d3, expiry=e3)
    # §11 benchmarks: each component alone, and the conjunction
    S["B3_RSI_ONLY"] = np.where(a("RSI_LT35"), "VALID", "NONE").astype(object)
    S["B3_RTB_ONLY"] = np.where(a("RTB_AB"), "VALID", "NONE").astype(object)
    S["B3_CONJ_ONLY"] = np.where(a("EXHAUST"), "VALID", "NONE").astype(object)
    for nm in ("B3_RSI_ONLY", "B3_RTB_ONLY", "B3_CONJ_ONLY"):
        meta[nm] = dict(terminal="VALID", reached={"VALID": S[nm] == "VALID"},
                        expiry_deaths=0, expiry=None)

    # ── B4 RE-IGNITION ──
    e4 = expiry_by_system["B4"]
    ign1 = a("K_ACT_FLAG") & _recent(a("PART_ANY"), 5, bnd)
    kk = df["phys_k"].fillna("").astype(str).to_numpy()
    reset = (_recent(ign1, e4, bnd, include_now=False)
             & ((kk == "K0") | (df["phys_regime"].fillna("").astype(str).to_numpy() == "D")))
    reign = (_recent(reset, react_window, bnd, include_now=False) & a("K_ACT_FLAG")
             & (a("PART_ANY") | a("RF")) & a("TREND_MEMORY_2"))
    reign_plus = reign & (a("K_ACT_TO_K2U") | (a("PART_FAMILY_COUNT") >= 3))
    lab4 = np.full(n, "NONE", dtype=object)
    lab4[ign1] = "IGN1"; lab4[reset] = "RESET"; lab4[reign] = "REIGN"
    lab4[reign_plus] = "REIGN_PLUS"
    S["B4"] = lab4
    meta["B4"] = dict(terminal="REIGN", reached=dict(IGN1=ign1, RESET=reset, REIGN=reign,
                                                     REIGN_PLUS=reign_plus),
                      expiry_deaths=0, expiry=e4, react_window=react_window)

    # ── R1 TREND RELOAD ──
    er = expiry_by_system["R1"]
    trend = a("REGIME_PERSIST_5") & a("K_BULL")
    pull = (_recent(trend, er, bnd, include_now=False)
            & (np.isin(kk, ["K0", "K1D"]) | (df["phys_regime"].fillna("").astype(str).to_numpy() == "D"))
            & a("TREND_MEMORY_2"))
    reload_ = (_recent(pull, 15, bnd, include_now=False) & (a("TURN_ANY") | a("PART_ANY"))
               & a("K_ACT_FLAG") & (a("RF") | a("M_PROGRESS")))
    labr = np.full(n, "NONE", dtype=object)
    labr[trend] = "TREND"; labr[pull] = "PULLBACK"; labr[reload_] = "RELOAD"
    S["R1"] = labr
    meta["R1"] = dict(terminal="RELOAD", reached=dict(TREND=trend, PULLBACK=pull, RELOAD=reload_),
                      expiry_deaths=0, expiry=er)

    log(f"  systems built: {', '.join(S)}")
    return S, meta


# ───────────────────────────────────────────────────────────────────────────
# §4  baselines (§17 adds REGIME D→U + recent participation)
# ───────────────────────────────────────────────────────────────────────────
def baselines(df, P, bnd) -> dict:
    c = df["close"].to_numpy(float)
    h = df["high"].to_numpy(float)
    n = len(df)
    prior20 = np.full(n, np.nan)
    ema20 = np.full(n, np.nan)
    for a_, b_ in zip(bnd[:-1], bnd[1:]):
        s = pd.Series(h[a_:b_])
        prior20[a_:b_] = s.shift(1).rolling(20, min_periods=20).max().to_numpy()
        ema20[a_:b_] = pd.Series(c[a_:b_]).ewm(span=20, adjust=False).mean().to_numpy()
    ema_rising = ema20 > _shift1(ema20, bnd, np.nan)
    vol = df["volume"].to_numpy(float)
    avg = df["avg_vol_20d"].to_numpy(float)
    return {
        "BL1_20d_breakout": c > prior20,
        "BL2_ema20_rising": (c > ema20) & ema_rising,
        "BL3_rsi_gt50": df["rsi_14"].to_numpy(float) > 50,
        "BL4_breakout_volexp": (c > prior20) & (vol > 1.5 * avg),
        "BL5_regime_kact": P["REGIME_UP"].to_numpy() & P["K_ACT_FLAG"].to_numpy(),
        "BL6_dtou_participation": (P["REGIME_D_TO_U"].to_numpy()
                                   & _recent(P["PART_ANY"].to_numpy(), 5, bnd)),
    }


# ───────────────────────────────────────────────────────────────────────────
# §5  labels — built LAST, from raw OHLC only
# ───────────────────────────────────────────────────────────────────────────
def labels(df: pd.DataFrame, log=print) -> pd.DataFrame:
    L = T.label(df[["ticker", "date", "open", "high", "low", "close", "atr_14"]].copy(),
                atr_col="atr_14")
    out = pd.DataFrame(index=df.index)
    out["MFE20_ATR"] = L["mfe_atr"].to_numpy()
    out["Y3"] = (out["MFE20_ATR"] >= 3).where(out["MFE20_ATR"].notna())
    out["Y5"] = (out["MFE20_ATR"] >= 5).where(out["MFE20_ATR"].notna())
    out["Y8"] = (out["MFE20_ATR"] >= 8).where(out["MFE20_ATR"].notna())
    tk = df["ticker"].to_numpy(); bnd = _bounds(tk)
    c = df["close"].to_numpy(float); h = df["high"].to_numpy(float); lo = df["low"].to_numpy(float)
    atr = df["atr_14"].to_numpy(float)
    for H in (5, 10, 20):
        fw = np.full(len(df), np.nan); mx = np.full(len(df), np.nan); mn = np.full(len(df), np.nan)
        for a_, b_ in zip(bnd[:-1], bnd[1:]):
            m = b_ - a_
            if m <= H:
                continue
            fw[a_:b_ - H] = c[a_ + H:b_] / c[a_:b_ - H] - 1
            hh = pd.Series(h[a_:b_][::-1]).rolling(H, min_periods=H).max().to_numpy()[::-1]
            ll = pd.Series(lo[a_:b_][::-1]).rolling(H, min_periods=H).min().to_numpy()[::-1]
            mx[a_:b_ - H] = hh[H:]
            mn[a_:b_ - H] = ll[H:]
        out[f"FWD_{H}"] = fw * 100
        out[f"MFE_{H}_ATR"] = (mx - c) / atr
        out[f"MAE_{H}_ATR"] = (mn - c) / atr
    log(f"  labels built · Y3 base {out['Y3'].mean()*100:.2f}% · "
        f"Y5 {out['Y5'].mean()*100:.2f}% · Y8 {out['Y8'].mean()*100:.2f}%")
    return out


# ───────────────────────────────────────────────────────────────────────────
# §6  evaluation
# ───────────────────────────────────────────────────────────────────────────
def evaluate(mask, Lb, df, tag) -> dict:
    m = mask & Lb["Y3"].notna().to_numpy()
    n = int(m.sum())
    if n == 0:
        return dict(system=tag, n=0)
    y3 = Lb.loc[m, "Y3"].mean(); y5 = Lb.loc[m, "Y5"].mean(); y8 = Lb.loc[m, "Y8"].mean()
    base3 = Lb["Y3"].mean(); base5 = Lb["Y5"].mean(); base8 = Lb["Y8"].mean()
    sub = df.loc[m]
    vc = sub["ticker"].value_counts()
    yr = sub["date"].str[:4].value_counts(normalize=True)
    return dict(system=tag, n=n, tickers=int(sub["ticker"].nunique()),
                prec_3atr=round(float(y3) * 100, 2), base_3atr=round(float(base3) * 100, 2),
                lift_3atr=round(float(y3 / base3), 3),
                prec_5atr=round(float(y5) * 100, 2), lift_5atr=round(float(y5 / base5), 3),
                prec_8atr=round(float(y8) * 100, 2), lift_8atr=round(float(y8 / base8), 3),
                med_mfe20_atr=round(float(Lb.loc[m, "MFE20_ATR"].median()), 3),
                med_mae20_atr=round(float(Lb.loc[m, "MAE_20_ATR"].median()), 3),
                med_fwd20_pct=round(float(Lb.loc[m, "FWD_20"].median()), 3),
                top5_ticker_share=round(float(vc.head(5).sum() / n) * 100, 2),
                max_year_share=round(float(yr.max()) * 100, 2) if len(yr) else None)


def main(log=print):
    t0 = time.time()
    os.makedirs(OUTDIR, exist_ok=True)
    log("=" * 78); log("BREAKOUT SYSTEMS RESEARCH — first walk-forward"); log("=" * 78)

    log("\n§1 LOAD")
    df = load(log)
    tk = df["ticker"].to_numpy(); bnd = _bounds(tk)

    log("\n§2 PRIMITIVES")
    P = primitives(df, log)
    freq = primitive_frequencies(P)
    freq.to_csv(os.path.join(OUTDIR, "primitive_frequencies.csv"), index=False)
    log("  top rates: " + " · ".join(
        f"{r.primitive} {r.rate_pct_or_mean}" for r in freq.head(0).itertuples()))

    log("\n§3 LABELS (built after features, from raw OHLC)")
    Lb = labels(df, log)

    log("\n§4 BASELINES")
    BL = baselines(df, P, bnd)
    rows = [evaluate(v, Lb, df, k) for k, v in BL.items()]
    log("  " + " | ".join(f"{r['system']} n={r['n']:,} p3={r['prec_3atr']} lift={r['lift_3atr']}"
                          for r in rows if r.get("n")))

    log("\n§5 SYSTEMS (defaults; train-selection follows)")
    exp_def = {k: 10 for k in ("B1", "B2", "B3", "B4", "R1")}
    S, meta = build_systems(df, P, bnd, exp_def, 6, log)
    sys_rows = []
    for name, lab in S.items():
        term = meta[name]["terminal"]
        sys_rows.append(evaluate(lab == term, Lb, df, f"{name}:{term}"))
    for r in sys_rows:
        if r.get("n"):
            log(f"  {r['system']:22s} n={r['n']:>7,} tk={r['tickers']:>4} "
                f"p3={r['prec_3atr']:>6}% lift={r['lift_3atr']:>5} "
                f"p8={r['prec_8atr']:>5}% lift8={r['lift_8atr']:>5}")
        else:
            log(f"  {r['system']:22s} NO FIRES")

    pd.DataFrame(rows + sys_rows).to_csv(
        os.path.join(OUTDIR, "breakout_system_summary_defaults.csv"), index=False)
    log(f"\n({time.time()-t0:.0f}s) first pass written to {OUTDIR}")
    return df, P, S, meta, Lb, BL


if __name__ == "__main__":
    main()


# ═══════════════════════════════════════════════════════════════════════════
# §7  WALK-FORWARD, HOLDOUTS, VETOES, OVERLAP, FAILURES  — the approved §16
# ═══════════════════════════════════════════════════════════════════════════
SYSTEMS_MAIN = ["B1_PARTIAL", "B1_ORDERED", "B2a", "B2b", "B2c", "B3",
                "B3_RSI_ONLY", "B3_RTB_ONLY", "B3_CONJ_ONLY", "B4", "R1"]
# A system whose fires need a layer that is not everywhere must be scored against the base rate
# of the universe it can actually fire in. Otherwise coverage becomes the "edge" — measured:
# LBAL-present bars have a 43.61 % base rate vs 32.79 % where it is absent.
UNIVERSE_OF = {"B2b": "LBAL_PRESENT"}


def _uni_mask(nm, P, ok):
    u = UNIVERSE_OF.get(nm)
    return ok & P[u].to_numpy() if u else ok


def _prec(mask, Lb, col="Y3"):
    m = mask & Lb[col].notna().to_numpy()
    n = int(m.sum())
    return (float(Lb.loc[m, col].mean()) if n else np.nan), n


def walk_forward(df, P, bnd, Lb, log=print) -> pd.DataFrame:
    """Parameters chosen on TRAIN only, frozen, evaluated once on TEST."""
    date = df["date"].to_numpy().astype(str)
    ok = Lb["Y3"].notna().to_numpy()
    rows = []
    for tr_end, te_lo, te_hi in FOLDS:
        tr = ok & (date <= tr_end)
        te = ok & (date >= te_lo) & (date <= te_hi)
        if te.sum() < 10_000:
            continue
        best = {}
        for e in GRID_EXPIRY:
            for rw in (GRID_REACT if True else [6]):
                S, meta = build_systems(df, P, bnd, {k: e for k in ("B1", "B2", "B3", "B4", "R1")},
                                        rw, log=lambda *a: None)
                for nm in SYSTEMS_MAIN:
                    term = meta[nm]["terminal"]
                    fire = (S[nm] == term)
                    p, n = _prec(fire & tr & _uni_mask(nm, P, ok), Lb)
                    if n < MIN_FIRES_TRAIN or np.isnan(p):
                        continue
                    if nm not in best or p > best[nm][0]:
                        best[nm] = (p, e, rw, n)
        # freeze and score once on test
        for nm, (p_tr, e, rw, n_tr) in best.items():
            S, meta = build_systems(df, P, bnd, {k: e for k in ("B1", "B2", "B3", "B4", "R1")},
                                    rw, log=lambda *a: None)
            term = meta[nm]["terminal"]
            fire = (S[nm] == term)
            uni_te = _uni_mask(nm, P, ok) & te
            p_te, n_te = _prec(fire & uni_te, Lb)
            base_te, _ = _prec(uni_te, Lb)
            rows.append(dict(fold=f"<= {tr_end} → {te_lo[:4]}", system=nm,
                             expiry=e, react=rw, n_train=n_tr,
                             prec_train=round(p_tr * 100, 2), n_test=n_te,
                             prec_test=round(p_te * 100, 2) if n_te else None,
                             base_test=round(base_te * 100, 2),
                             lift_test=round(p_te / base_te, 3) if n_te and base_te else None))
    return pd.DataFrame(rows)


def baseline_walk_forward(df, P, bnd, Lb) -> pd.DataFrame:
    date = df["date"].to_numpy().astype(str)
    ok = Lb["Y3"].notna().to_numpy()
    BL = baselines(df, P, bnd)
    rows = []
    for tr_end, te_lo, te_hi in FOLDS:
        te = ok & (date >= te_lo) & (date <= te_hi)
        if te.sum() < 10_000:
            continue
        base_te, _ = _prec(te, Lb)
        for nm, m in BL.items():
            p, n = _prec(m & te, Lb)
            rows.append(dict(fold=f"<= {tr_end} → {te_lo[:4]}", system=nm, n_test=n,
                             prec_test=round(p * 100, 2) if n else None,
                             base_test=round(base_te * 100, 2),
                             lift_test=round(p / base_te, 3) if n else None))
    return pd.DataFrame(rows)


def veto_modes(df, P, S, meta, Lb, log=print) -> pd.DataFrame:
    """Four modes per §14: none · each veto alone · any veto · hard exclusion."""
    ok = Lb["Y3"].notna().to_numpy()
    rows = []
    for nm in SYSTEMS_MAIN:
        term = meta[nm]["terminal"]
        fire = (S[nm] == term) & _uni_mask(nm, P, ok)
        base, _ = _prec(_uni_mask(nm, P, ok), Lb)
        p0, n0 = _prec(fire, Lb)
        rows.append(dict(system=nm, mode="no_veto", n=n0, prec=round(p0 * 100, 2),
                         lift=round(p0 / base, 3)))
        for v in ("VETO_VOL", "VETO_LVX", "VETO_SHAPE"):
            vm = P[v].to_numpy()
            p1, n1 = _prec(fire & vm, Lb)
            p2, n2 = _prec(fire & ~vm, Lb)
            rows.append(dict(system=nm, mode=f"{v}_present", n=n1,
                             prec=round(p1 * 100, 2) if n1 else None,
                             lift=round(p1 / base, 3) if n1 else None))
            rows.append(dict(system=nm, mode=f"{v}_excluded", n=n2,
                             prec=round(p2 * 100, 2) if n2 else None,
                             lift=round(p2 / base, 3) if n2 else None))
        va = P["VETO_ANY"].to_numpy()
        p3, n3 = _prec(fire & ~va, Lb)
        rows.append(dict(system=nm, mode="hard_exclude_any", n=n3,
                         prec=round(p3 * 100, 2) if n3 else None,
                         lift=round(p3 / base, 3) if n3 else None))
    return pd.DataFrame(rows)


def overlap_matrix(S, meta, Lb) -> pd.DataFrame:
    ok = Lb["Y3"].notna().to_numpy()
    fires = {nm: (S[nm] == meta[nm]["terminal"]) & ok for nm in
             ["B1_PARTIAL", "B2a", "B2b", "B2c", "B3", "B4", "R1"]}
    rows = []
    for a in fires:
        for b in fires:
            inter = int((fires[a] & fires[b]).sum())
            rows.append(dict(a=a, b=b, both=inter,
                             pct_of_a=round(inter / max(int(fires[a].sum()), 1) * 100, 2)))
    return pd.DataFrame(rows).pivot(index="a", columns="b", values="pct_of_a")


def b4_diagnostics(df, P, S, meta, bnd, Lb, log=print) -> pd.DataFrame:
    """§12 — is the SECOND ignition systematically better than the first?"""
    ok = Lb["Y3"].notna().to_numpy()
    ign1 = meta["B4"]["reached"]["IGN1"] & ok
    reign = meta["B4"]["reached"]["REIGN"] & ok
    rplus = meta["B4"]["reached"]["REIGN_PLUS"] & ok
    base, _ = _prec(ok, Lb)
    rows = []
    for nm, m in (("IGN1 (first ignition)", ign1), ("REIGN (second)", reign),
                  ("REIGN_PLUS", rplus)):
        p, n = _prec(m, Lb)
        rows.append(dict(stage=nm, n=n, prec=round(p * 100, 2), lift=round(p / base, 3),
                         med_mfe20=round(float(Lb.loc[m, "MFE20_ATR"].median()), 3)))
    # K flavour of the reactivation
    for nm, m in (("REIGN → K1U", reign & P["K_ACT_TO_K1U"].to_numpy()),
                  ("REIGN → K2U", reign & P["K_ACT_TO_K2U"].to_numpy())):
        p, n = _prec(m, Lb)
        rows.append(dict(stage=nm, n=n, prec=round(p * 100, 2) if n else None,
                         lift=round(p / base, 3) if n else None,
                         med_mfe20=round(float(Lb.loc[m, "MFE20_ATR"].median()), 3) if n else None))
    return pd.DataFrame(rows)


def stability(df, P, S, meta, Lb) -> pd.DataFrame:
    """Year-by-year and per-sector, each against the base rate of its own slice."""
    ok = Lb["Y3"].notna().to_numpy()
    yr = df["date"].str[:4].to_numpy()
    sec = df["sector"].fillna("?").astype(str).to_numpy()
    rows = []
    for nm in ["B1_PARTIAL", "B2a", "B2c", "B3", "B4", "R1"]:
        fire = (S[nm] == meta[nm]["terminal"]) & _uni_mask(nm, P, ok)
        uni = _uni_mask(nm, P, ok)
        for key, arr in (("year", yr), ("sector", sec)):
            for v in pd.unique(arr):
                m = arr == v
                p, n = _prec(fire & m, Lb)
                b, nb = _prec(uni & m, Lb)
                if n < 200 or not b:
                    continue
                rows.append(dict(system=nm, split=key, value=str(v), n=n,
                                 prec=round(p * 100, 2), base=round(b * 100, 2),
                                 lift=round(p / b, 3)))
    return pd.DataFrame(rows)
