"""studio/bar_physics.py — the physics fields, reimplemented from Pine, bar for bar.

Two TradingView studies describe every bar with fields the DB does not carry yet:

    260814_TZ_RC     R (Ohm) · regime · C (Coulomb) · H (entropy) · AD-FRESH ·
                     Wyckoff phase · TRUE-gap
    260814_PHYS      M (momentum) · E (spring energy) · K (Hooke) · S (resonance)

FAITHFUL FIRST, IMPROVED SECOND. Every threshold, window and formula here is the Pine default,
unchanged, so a bar's class in the app and on the chart are the same class. Where I think a
definition is weak I say so in a comment and measure it later — silently "improving" a
classifier during a port makes the two disagree and neither can be checked against the other.

PINE EQUIVALENCE, the parts that are easy to get wrong:

    ta.ema(x, n)        ewm(span=n, adjust=False)
    ta.sma / ta.median  rolling(n), no min_periods relaxation
    ta.atr(14)          ewm(alpha=1/14, adjust=False) of true range — this is exactly the
                        atr_14 already stored, so it is READ rather than recomputed
    ta.pivothigh(h,3,3) confirmed three bars AFTER the pivot. The level joins the charge array
                        at that later bar, carrying volume[pivot]. Replicated with a shift, so
                        no level is visible before Pine would have seen it.

NO LOOKAHEAD ANYWHERE. Every window is backward-looking and every groupby is per ticker. The
pivot rule above is the one place where a naive port leaks the future, and it is handled
explicitly rather than assumed.

RAW VALUES ARE STORED BESIDE THE CLASSES. Thresholds are opinions; the underlying quantities are
measurements. Keeping r_raw/c_raw/h_raw/m_raw/e_raw/k_x means a threshold can be re-cut later
without re-running 132M bars, which is the difference between a decision that costs an hour and
one that costs a night.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# ── Pine defaults, named once ───────────────────────────────────────────────
R_VOL_LEN, R_MED_LEN = 20, 30
R_FREE_FRAC, R_ABS_MULT, R_EPS_ATR = 0.6, 2.0, 0.05
RG_EMA_LEN = 20

C_PIV_LR, C_MAX_LVLS, C_MED_LEN = 3, 12, 50
C_HI_MULT, C_LO_FRAC, C_MIN_R_ATR = 2.0, 0.5, 0.25

H_ENT_LEN, H_LO_THR, H_HI_THR = 20, 0.60, 0.95

M_SLOPE_BARS, M_VOL_FAST, M_VOL_SLOW, M_MED_LEN = 10, 20, 100, 50
M_HI_MULT, M_LO_FRAC = 1.8, 0.4

E_AVG_LEN, E_WIN, E_MED_LEN, E_HI_MULT, E_REL_MULT = 50, 15, 50, 1.6, 1.5

K_EMA_LEN, K_STRETCH, K_BREAK = 20, 1.0, 3.0

GAP_SMALL_ATR, GAP_MEDIUM_ATR = 0.2, 0.5

AD_LOOKBACK, AD_CLUSTER_WIN, AD_FRESH_POS_THR = 12, 8, 0.50
WYC_COMP_MULT = 0.70

# what this module adds to `bars`
VARCHAR_COLS = ["phys_r", "phys_regime", "phys_c", "phys_h", "phys_line6",
                "phys_m", "phys_e", "phys_k", "phys_s",
                "phys_gap_true", "phys_ad", "phys_wyc"]
DOUBLE_COLS = ["phys_r_raw", "phys_c_raw", "phys_h_raw", "phys_m_raw",
               "phys_e_raw", "phys_k_x"]
SMALLINT_COLS = ["phys_e_release", "phys_s_net"]
ALL_COLS = VARCHAR_COLS + DOUBLE_COLS + SMALLINT_COLS

PHYSICS_VERSION = "bar_physics_v1_pine260814"


def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def _med(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).median()


def _coulomb(close: np.ndarray, high: np.ndarray, low: np.ndarray, vol: np.ndarray,
             atr: np.ndarray, vol_ma: np.ndarray) -> np.ndarray:
    """Σ q/r² over the most recent pivot levels, r measured in ATRs.

    THE CAUSALITY DETAIL. `ta.pivothigh(high, 3, 3)` is confirmed three bars after the pivot,
    and Pine pushes the level at THAT bar with `volume[3]` — the pivot bar's volume. A port that
    pushes the level at the pivot bar itself would let every bar see levels three bars before
    Pine did, which is a small, invisible, permanent lookahead.

    THE DEFINITION I WOULD CHANGE LATER, and am not changing now. A pivot exists BECAUSE price
    reversed there, so "price reacts at pivots" is partly circular; charge defined by
    volume-at-price would not be. Ported faithfully so the app and the chart agree, and the
    comparison is a measurement for another day.
    """
    n = len(close)
    lr = C_PIV_LR
    pot = np.zeros(n)
    px: list[float] = []
    q: list[float] = []
    for i in range(n):
        # a pivot centred at i-lr becomes visible now, exactly as Pine confirms it
        j = i - lr
        if j - lr >= 0 and j + lr < n:
            seg_h = high[j - lr:j + lr + 1]
            seg_l = low[j - lr:j + lr + 1]
            if high[j] == seg_h.max() and (seg_h == high[j]).sum() == 1:
                px.insert(0, high[j]); q.insert(0, vol[j])
                if len(px) > C_MAX_LVLS:
                    px.pop(); q.pop()
            if low[j] == seg_l.min() and (seg_l == low[j]).sum() == 1:
                px.insert(0, low[j]); q.insert(0, vol[j])
                if len(px) > C_MAX_LVLS:
                    px.pop(); q.pop()
        if not px or not np.isfinite(atr[i]) or atr[i] <= 0:
            continue
        a = atr[i]
        vm = vol_ma[i] if np.isfinite(vol_ma[i]) and vol_ma[i] > 0 else 1.0
        r = np.maximum(np.abs(close[i] - np.asarray(px)) / a, C_MIN_R_ATR)
        pot[i] = float((np.asarray(q) / vm / (r * r)).sum())
    return pot


def compute(df: pd.DataFrame) -> pd.DataFrame:
    """All physics fields for ONE ticker's frame, ordered by date ascending.

    Requires: open, high, low, close, volume, atr_14. Uses sig_l3/sig_l4 and wvf_spike when
    present (Wyckoff needs them); degrades to a partial Wyckoff rather than failing if absent.
    """
    out = pd.DataFrame(index=df.index)
    n = len(df)
    if n < 2:
        for c in VARCHAR_COLS:
            out[c] = ""
        for c in DOUBLE_COLS:
            out[c] = np.nan
        for c in SMALLINT_COLS:
            out[c] = 0
        return out

    o = df["open"].astype(float)
    h = df["high"].astype(float)
    lo = df["low"].astype(float)
    c = df["close"].astype(float)
    v = df["volume"].astype(float).fillna(0.0)
    atr = df["atr_14"].astype(float)
    atr_safe = atr.where(atr > 0, np.nan)

    ema9, ema20, ema50, ema200 = (_ema(c, k) for k in (9, 20, 50, 200))

    # ── R (Ohm) — effort over result ────────────────────────────────────────
    # NORMALISED ON BOTH SIDES, which is why this is not the size proxy a raw
    # volume/|Δp| would be: the numerator is volume against its own mean and the
    # denominator is displacement in ATRs, so a $400 mega-cap and an $8 name are comparable.
    vol_ma = v.rolling(R_VOL_LEN, min_periods=R_VOL_LEN).mean()
    effort = (v / vol_ma).where(vol_ma > 0, 1.0)
    result = (c.diff().abs() / atr_safe).fillna(0.0)
    r_raw = effort / np.maximum(result, R_EPS_ATR)
    r_med = _med(r_raw, R_MED_LEN)
    r_cls = pd.Series(np.select([r_raw > r_med * R_ABS_MULT, r_raw < r_med * R_FREE_FRAC],
                                ["RA", "RF"], default="RN"), index=df.index)
    r_cls = r_cls.where(r_med.notna(), "")

    rg_ema = _ema(c, RG_EMA_LEN)
    regime = pd.Series(np.where(c >= rg_ema, "U", "D"), index=df.index)

    # ── C (Coulomb) ─────────────────────────────────────────────────────────
    c_raw = pd.Series(_coulomb(c.to_numpy(), h.to_numpy(), lo.to_numpy(), v.to_numpy(),
                               atr_safe.to_numpy(), vol_ma.to_numpy()), index=df.index)
    c_med = _med(c_raw, C_MED_LEN)
    c_cls = pd.Series(np.select([c_raw > c_med * C_HI_MULT, c_raw < c_med * C_LO_FRAC],
                                ["C2", "C0"], default="C1"), index=df.index)
    c_cls = c_cls.where(c_med.notna(), "")

    # ── H (entropy) ─────────────────────────────────────────────────────────
    # Binary Shannon entropy of "which side of the EMA", ported as specified. I measured this
    # family and found it close to saturated on daily bars; the thresholds here cut the tails
    # (0.60 / 0.95), so H0 and H2 are rare states rather than a spread — which is a different
    # and possibly better use of the same quantity. Kept as written, to be measured.
    h_side = (c >= rg_ema).astype(float)
    p = h_side.rolling(H_ENT_LEN, min_periods=H_ENT_LEN).mean()
    pc = p.clip(1e-9, 1 - 1e-9)
    h_raw = (-(pc * np.log(pc) + (1 - pc) * np.log(1 - pc)) / np.log(2)).where(
        (p > 1e-4) & (p < 1 - 1e-4), 0.0)
    h_raw = h_raw.where(p.notna())
    h_cls = pd.Series(np.select([h_raw < H_LO_THR, h_raw > H_HI_THR], ["H0", "H2"],
                                default="H1"), index=df.index)
    h_cls = h_cls.where(h_raw.notna(), "")

    line6 = r_cls.where(r_cls == "", r_cls + "·" + regime + "·" + c_cls + "·" + h_cls)

    # ── M (momentum × mass) ─────────────────────────────────────────────────
    slope = (ema20 - ema20.shift(M_SLOPE_BARS)).abs() / (atr_safe * M_SLOPE_BARS)
    dvol = v * c
    mass = dvol.rolling(M_VOL_FAST, min_periods=M_VOL_FAST).mean() / \
        dvol.rolling(M_VOL_SLOW, min_periods=M_VOL_SLOW).mean().clip(lower=1.0)
    m_raw = slope * mass
    m_med = _med(m_raw, M_MED_LEN)
    m_cls = pd.Series(np.select([m_raw > m_med * M_HI_MULT, m_raw < m_med * M_LO_FRAC],
                                ["M2", "M0"], default="M1"), index=df.index)
    m_cls = m_cls.where(m_med.notna(), "")

    # ── E (spring energy) + release ─────────────────────────────────────────
    atr_base = _ema(atr, E_AVG_LEN)
    comp = ((atr_base - atr).clip(lower=0.0) / atr_base.clip(lower=1e-12))
    e_raw = comp.rolling(E_WIN, min_periods=E_WIN).mean() * E_WIN
    e_med = _med(e_raw, E_MED_LEN)
    loaded = e_raw > e_med * E_HI_MULT
    e_cls = pd.Series(np.select([loaded, e_raw < e_med * 0.5], ["E2", "E0"], default="E1"),
                      index=df.index)
    release = loaded.shift(1).astype("boolean").fillna(False).astype(bool) & ((h - lo) > atr_base * E_REL_MULT)
    e_cls = e_cls.where(e_med.notna(), "")
    e_cls = e_cls.mask(release & (e_cls != ""), e_cls + "★")

    # ── K (Hooke stretch) ───────────────────────────────────────────────────
    k_x = (c - _ema(c, K_EMA_LEN)) / atr_safe
    k_abs = k_x.abs()
    k_cls = pd.Series(np.select([k_abs >= K_BREAK, k_abs >= K_STRETCH], ["K2", "K1"],
                                default="K0"), index=df.index)
    k_cls = k_cls.where(k_cls == "K0", k_cls + np.where(k_x > 0, "U", "D"))
    k_cls = k_cls.where(k_x.notna(), "")

    # ── S (resonance) ───────────────────────────────────────────────────────
    up_align = ((ema9 > ema20).astype(int) + (ema20 > ema50).astype(int)
                + (ema50 > ema200).astype(int))
    dn_align = ((ema9 < ema20).astype(int) + (ema20 < ema50).astype(int)
                + (ema50 < ema200).astype(int))
    s_up = up_align >= dn_align
    s_cls = pd.Series(np.where(s_up,
                               "S" + up_align.astype(str) + np.where(up_align > 0, "U", ""),
                               "S" + dn_align.astype(str) + "D"), index=df.index)

    # ── TRUE-gap ────────────────────────────────────────────────────────────
    # The correction the Pine comment flags: measuring a gap from the previous CLOSE reports
    # overnight displacement and inflates the class (~2x, never under). The empty-space edge —
    # the previous high or low — is the consistent measure, because it is what the detection
    # rule already used.
    ph, pl = h.shift(1), lo.shift(1)
    has_gap = (o > ph) | (o < pl)
    gap_true = np.where(o > ph, o - ph, np.where(o < pl, pl - o, 0.0))
    gt_ratio = pd.Series(gap_true, index=df.index) / atr_safe
    gap_cls = pd.Series(np.select([~has_gap, gt_ratio < GAP_SMALL_ATR,
                                   gt_ratio < GAP_MEDIUM_ATR], ["", "G1", "G2"],
                                  default="G3"), index=df.index)

    # ── AD-FRESH ────────────────────────────────────────────────────────────
    # Raw patterns recomputed rather than read from t_sig/z_sig: the stored codes are
    # priority-RESOLVED, so a bar that is Z1G_raw and also Z4_raw stores Z4 and would be missed.
    # Pine's AD rule reads the raw booleans, so the port does too.
    prev_bull = c.shift(1) > o.shift(1)
    prev_bear = (c.shift(1) < o.shift(1)) | (c.shift(1) == o.shift(1))
    is_bull, is_bear = c > o, c < o
    body_prev = (c.shift(1) - o.shift(1)).abs()
    prev_top = pd.concat([o.shift(1), c.shift(1)], axis=1).max(axis=1)
    prev_bot = pd.concat([o.shift(1), c.shift(1)], axis=1).min(axis=1)
    cur_top = pd.concat([o, c], axis=1).max(axis=1)
    cur_bot = pd.concat([o, c], axis=1).min(axis=1)
    engulf = (cur_top >= prev_top) & (prev_bot >= cur_bot) & \
             ((c - o).abs() / body_prev.clip(lower=1e-12) >= 1.0)

    z1g = prev_bull & (o < c.shift(1)) & (o < o.shift(1)) & (c < o.shift(1)) & is_bear
    z2g = prev_bear & (o <= o.shift(1)) & (o < c.shift(1)) & (c < c.shift(1)) & is_bear
    t4 = prev_bear & is_bull & engulf
    t6 = prev_bull & is_bull & engulf
    t2g = prev_bull & (o >= o.shift(1)) & (o > c.shift(1)) & (c > c.shift(1)) & is_bull
    t2 = prev_bull & (o >= o.shift(1)) & (o <= c.shift(1)) & (c > c.shift(1)) & is_bull

    a_sig = (z1g | z2g).to_numpy()
    d_sig = (t4 | t6 | t2g | t2).to_numpy()
    since_a = np.full(n, 10**6)
    last = -10**6
    for i in range(n):
        if a_sig[i]:
            last = i
        since_a[i] = i - last
    a_recent = since_a <= AD_LOOKBACK

    h20 = h.rolling(20, min_periods=20).max()
    l20 = lo.rolling(20, min_periods=20).min()
    pos = (c - l20) / (h20 - l20).clip(lower=1e-12)
    ad_fresh = pd.Series(d_sig & a_recent, index=df.index) & (pos < AD_FRESH_POS_THR)

    af = ad_fresh.to_numpy()
    since_ad = np.full(n, 10**6)
    last = -10**6
    cluster = np.zeros(n, bool)
    for i in range(n):
        if af[i] and 0 < (i - last) <= AD_CLUSTER_WIN:
            cluster[i] = True
        if af[i]:
            last = i
    # ★A — the flip confirmed by absorption at the exhaustion bar
    r_at_a = pd.Series(np.where(a_sig, r_raw.to_numpy(), np.nan), index=df.index).ffill()
    absorbed = r_at_a > r_med * R_ABS_MULT
    ad_cls = pd.Series(np.select([cluster, ad_fresh & absorbed, ad_fresh],
                                 ["★★", "★A", "★"], default=""), index=df.index)

    # ── Wyckoff phase ───────────────────────────────────────────────────────
    vol_up = (df["sig_l3"].fillna(0).astype(bool) | df["sig_l4"].fillna(0).astype(bool)) \
        if "sig_l3" in df.columns and "sig_l4" in df.columns else pd.Series(False, index=df.index)
    vix_spike = df["wvf_spike"].fillna(0).astype(bool) if "wvf_spike" in df.columns \
        else pd.Series(False, index=df.index)
    macro_up, macro_dn = ema50 > ema200, ema50 < ema200
    compress = atr < _ema(atr, 50) * WYC_COMP_MULT
    sup20 = lo.rolling(20, min_periods=20).min().shift(1)
    res20 = h.rolling(20, min_periods=20).max().shift(1)
    spring = (lo < sup20) & (c > sup20) & is_bull & macro_dn & vol_up
    utad = (h > res20) & (c < res20) & is_bear & macro_up & vol_up
    sos = ad_fresh & macro_dn & vix_spike
    wyc = pd.Series(np.select(
        [spring & (r_raw > r_med * R_ABS_MULT), spring, utad, sos,
         macro_dn & compress, macro_up & compress, macro_up, macro_dn],
        ["SPRING★", "SPRING", "UTAD", "SOS★", "ACC-TR", "DIST-TR", "MARKUP", "MKDN"],
        default=""), index=df.index)

    out["phys_r"] = r_cls.fillna("")
    out["phys_regime"] = regime
    out["phys_c"] = c_cls.fillna("")
    out["phys_h"] = h_cls.fillna("")
    out["phys_line6"] = line6.fillna("")
    out["phys_m"] = m_cls.fillna("")
    out["phys_e"] = e_cls.fillna("")
    out["phys_k"] = k_cls.fillna("")
    out["phys_s"] = s_cls
    out["phys_gap_true"] = gap_cls
    out["phys_ad"] = ad_cls
    out["phys_wyc"] = wyc
    out["phys_r_raw"] = r_raw
    out["phys_c_raw"] = c_raw
    out["phys_h_raw"] = h_raw
    out["phys_m_raw"] = m_raw
    out["phys_e_raw"] = e_raw
    out["phys_k_x"] = k_x
    out["phys_e_release"] = release.astype(int)
    out["phys_s_net"] = (up_align - dn_align).astype(int)
    return out
