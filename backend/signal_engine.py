"""
signal_engine.py — faithful Pine Script → pandas translation.

Each bar gets exactly ONE signal id (0–25).
All operations are vectorized; no row-level Python loops.

Signal IDs
----------
Bullish (T):  0=NONE 1=T1G 2=T1 3=T2G 4=T2 5=T3 6=T4 7=T5 8=T6 9=T9 10=T10 11=T11
Bearish (Z):  12=Z1G 13=Z1 14=Z2G 15=Z2 16=Z3 17=Z4 18=Z5 19=Z6
              20=Z7(doji) 21=Z8 22=Z9 23=Z10 24=Z11 25=Z12

Priority bullish (highest wins):  T4 > T6 > T1G > T2G > T1 > T2 > T9 > T10 > T3 > T11 > T5
Priority bearish (highest wins):  Z4 > Z6 > Z1G > Z2G > Z1 > Z2 > Z8 > Z9 > Z10 > Z3 > Z11 > Z5 > Z12 > Z7

Rules:
  - If any bullish fires → bearish codes are 0 for that bar
  - Z8 fires only if NO other Z signal on same bar
  - Z7 (doji) fires only if NO bullish AND NO other bearish signal
"""

from __future__ import annotations

import numpy as np
import pandas as pd

pd.set_option("future.no_silent_downcasting", True)

# ---------------------------------------------------------------------------
# Signal ID constants
# ---------------------------------------------------------------------------
NONE = 0
T1G, T1, T2G, T2, T3, T4, T5, T6, T9, T10, T11 = 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
Z1G, Z1, Z2G, Z2, Z3, Z4, Z5, Z6 = 12, 13, 14, 15, 16, 17, 18, 19
Z7, Z8, Z9, Z10, Z11, Z12 = 20, 21, 22, 23, 24, 25

SIG_NAMES: dict[int, str] = {
    0: "NONE",
    1: "T1G", 2: "T1", 3: "T2G", 4: "T2", 5: "T3", 6: "T4", 7: "T5", 8: "T6",
    9: "T9", 10: "T10", 11: "T11",
    12: "Z1G", 13: "Z1", 14: "Z2G", 15: "Z2", 16: "Z3", 17: "Z4", 18: "Z5", 19: "Z6",
    20: "Z7", 21: "Z8", 22: "Z9", 23: "Z10", 24: "Z11", 25: "Z12",
}

BULLISH_SIGS = frozenset(range(1, 12))
BEARISH_SIGS = frozenset(range(12, 26))

# bc priority code -> signal id
_BC_TO_SID = {1: 6, 2: 8, 3: 1, 4: 3, 5: 2, 6: 4, 7: 9, 8: 10, 9: 5, 10: 11, 11: 7}
# zc priority code -> signal id
_ZC_TO_SID = {1: 17, 2: 19, 3: 12, 4: 14, 5: 13, 6: 15, 7: 21, 8: 22,
              9: 23, 10: 16, 11: 24, 12: 18, 13: 25, 14: 20}

_BC_SID_MAP = np.zeros(12, dtype=np.int8)
for _k, _v in _BC_TO_SID.items():
    _BC_SID_MAP[_k] = _v

_ZC_SID_MAP = np.zeros(15, dtype=np.int8)
for _k, _v in _ZC_TO_SID.items():
    _ZC_SID_MAP[_k] = _v


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_signals(
    df: pd.DataFrame,
    use_wick: bool = False,
    min_body_ratio: float = 1.0,
    doji_thresh: float = 0.05,
) -> pd.DataFrame:
    """
    Translate Pine Script T/Z logic into signal codes for every bar.

    Parameters
    ----------
    df : DataFrame with columns open, high, low, close (case-insensitive).

    Returns
    -------
    DataFrame with columns:
        bc (int8)     - bullish priority code 0-11
        zc (int8)     - bearish priority code 0-14
        sig_id (int8) - final signal 0-25
        sig_name (str)
        is_bull (bool)
        is_bear (bool)
    """
    df = _norm(df)
    o = df["open"]
    h = df["high"]
    l = df["low"]
    c = df["close"]

    # ── Pine Script bar components ────────────────────────────────────────
    rng = h - l
    bdy = (c - o).abs()
    mintick = 1e-10

    isDoji = (rng > 0) & ((bdy / rng.replace(0, np.nan)) <= doji_thresh)
    isDoji = isDoji.fillna(False)

    isBull = c > o
    isBear = c < o

    p1Bull = (c.shift(1) > o.shift(1))
    p1Bear = (c.shift(1) < o.shift(1)) | isDoji.shift(1).fillna(False).astype(bool)

    pBody = (c.shift(1) - o.shift(1)).abs()
    pTop  = np.maximum(o.shift(1), c.shift(1))
    pBot  = np.minimum(o.shift(1), c.shift(1))

    cBody = bdy
    cTop  = np.maximum(o, c)
    cBot  = np.minimum(o, c)

    # use_wick=False: eH=cTop, eL=cBot
    eH  = cTop
    eL  = cBot
    eP  = pTop
    ePl = pBot

    safe  = np.maximum(pBody, mintick)
    engOk = (cBody / safe >= min_body_ratio) & (eH >= eP) & (ePl >= eL)
    insOk = (cTop <= pTop) & (cBot >= pBot)

    # ── Bullish patterns (direct Pine translation) ────────────────────────
    cT1G = p1Bear & (o > c.shift(1)) & (o > o.shift(1)) & (c > o.shift(1)) & isBull
    cT1  = p1Bear & (o >= c.shift(1)) & (o.shift(1) >= o) & (c > o.shift(1)) & isBull
    cT2G = p1Bull & (o >= o.shift(1)) & (o > c.shift(1)) & (c > c.shift(1)) & isBull
    cT2  = p1Bull & (o >= o.shift(1)) & (o <= c.shift(1)) & (c > c.shift(1)) & isBull
    cT3  = (p1Bear & isBull & (o < o.shift(1)) & (o < c.shift(1))
            & (c < o.shift(1)) & (c > c.shift(1)))
    cT4  = p1Bear & isBull & engOk
    cT5  = (p1Bear & isBull & (o < o.shift(1)) & (o < c.shift(1))
            & (c < o.shift(1)) & (c.shift(1) >= c))
    cT6  = p1Bull & isBull & engOk
    cT9  = p1Bear & isBull & insOk
    cT10 = p1Bull & isBull & insOk
    cT11 = p1Bull & (o < o.shift(1)) & ((c < c.shift(1)) | (c < o.shift(1))) & isBull

    # ── Bearish patterns (direct Pine translation) ────────────────────────
    cZ1G = p1Bull & (o < c.shift(1)) & (o < o.shift(1)) & (c < o.shift(1)) & isBear
    cZ1  = p1Bull & (o <= c.shift(1)) & (o > o.shift(1)) & (c < o.shift(1)) & isBear
    cZ2G = p1Bear & (o <= o.shift(1)) & (o < c.shift(1)) & (c < c.shift(1)) & isBear
    cZ2  = p1Bear & (o <= o.shift(1)) & (o >= c.shift(1)) & (c < c.shift(1)) & isBear
    cZ3  = (p1Bull & isBear & (o > o.shift(1)) & (o > c.shift(1))
            & (c > o.shift(1)) & (c < c.shift(1)))
    cZ4  = p1Bull & isBear & engOk
    cZ5  = (p1Bull & isBear & (o > o.shift(1)) & (o > c.shift(1))
            & (c > o.shift(1)) & (c >= c.shift(1)))
    cZ6  = p1Bear & isBear & engOk
    cZ9  = p1Bull & isBear & insOk
    cZ10 = p1Bear & isBear & insOk
    cZ11 = p1Bear & (o > o.shift(1)) & isBear & ((c > c.shift(1)) | (c > o.shift(1)))
    cZ12 = p1Bull & (o <= o.shift(1)) & isBear

    cZ8b = p1Bull & (o > c.shift(1)) & isBear & (c >= o.shift(1))
    anyZ = (cZ1G | cZ1 | cZ2G | cZ2 | cZ3 | cZ4 | cZ5 | cZ6
            | cZ9 | cZ10 | cZ11 | cZ12)
    cZ8  = cZ8b & ~anyZ

    anyB = cT1G | cT1 | cT2G | cT2 | cT3 | cT4 | cT5 | cT6 | cT9 | cT10 | cT11
    cZ7c = isDoji & ~anyB & ~anyZ

    # ── bc priority code ──────────────────────────────────────────────────
    # T4?1 : T6?2 : T1G?3 : T2G?4 : T1?5 : T2?6 : T9?7 : T10?8 : T3?9 : T11?10 : T5?11 : 0
    bc_arr = np.zeros(len(df), dtype=np.int8)
    for code, cond in [
        (1, cT4), (2, cT6), (3, cT1G), (4, cT2G), (5, cT1),
        (6, cT2), (7, cT9), (8, cT10), (9, cT3), (10, cT11), (11, cT5),
    ]:
        mask = cond.fillna(False).to_numpy() & (bc_arr == 0)
        bc_arr[mask] = code

    # ── zc priority code ──────────────────────────────────────────────────
    # Z4?1 : Z6?2 : Z1G?3 : Z2G?4 : Z1?5 : Z2?6 : Z8?7 : Z9?8 : Z10?9 :
    # Z3?10 : Z11?11 : Z5?12 : Z12?13 : Z7c?14 : 0
    zc_arr = np.zeros(len(df), dtype=np.int8)
    for code, cond in [
        (1, cZ4), (2, cZ6), (3, cZ1G), (4, cZ2G), (5, cZ1),
        (6, cZ2), (7, cZ8), (8, cZ9), (9, cZ10), (10, cZ3),
        (11, cZ11), (12, cZ5), (13, cZ12), (14, cZ7c),
    ]:
        mask = cond.fillna(False).to_numpy() & (zc_arr == 0)
        zc_arr[mask] = code

    # if any bullish fired, zero out zc for that bar
    zc_arr = np.where(bc_arr > 0, np.int8(0), zc_arr).astype(np.int8)

    # ── map bc/zc -> sig_id ───────────────────────────────────────────────
    sid = np.where(bc_arr > 0, _BC_SID_MAP[bc_arr], _ZC_SID_MAP[zc_arr])

    bc       = pd.Series(bc_arr, index=df.index, name="bc")
    zc       = pd.Series(zc_arr, index=df.index, name="zc")
    sig_id   = pd.Series(sid.astype(np.int8), index=df.index, name="sig_id")
    sig_name = sig_id.map(SIG_NAMES).fillna("NONE")
    is_bull  = sig_id.isin(BULLISH_SIGS)
    is_bear  = sig_id.isin(BEARISH_SIGS)

    return pd.DataFrame(
        {"bc": bc, "zc": zc, "sig_id": sig_id, "sig_name": sig_name,
         "is_bull": is_bull, "is_bear": is_bear},
        index=df.index,
    )


def _norm(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    missing = {"open", "high", "low", "close"} - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    return df


# ---------------------------------------------------------------------------
# ok3 helper  (used by scanner)
# ---------------------------------------------------------------------------

def ok3(sig_series: pd.Series) -> pd.Series:
    """True where any of the last 3 bars has a non-NONE signal."""
    return (sig_series != NONE).astype(int).rolling(3, min_periods=1).sum() > 0


# ---------------------------------------------------------------------------
# G signals  (260410_G_BUILDER)
# ---------------------------------------------------------------------------
# G1  = first T1  after Z10 / Z11 / Z12
# G2  = first T1G after Z10 / Z11 / Z12
# G4  = first T4  after Z10 / Z11 / Z12
# G6  = first T6  after Z10 / Z11 / Z12
# G11 = first T1  after T10 or T11
# No RSI filter applied here.

def compute_g_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stateful G-signal state machine matching 260410_G_BUILDER Pine logic.
    Returns DataFrame with boolean columns: g1, g2, g4, g6, g11.
    """
    sig = compute_signals(df)
    sid = sig["sig_id"].values.astype(int)
    n   = len(sid)

    g1  = np.zeros(n, dtype=bool)
    g2  = np.zeros(n, dtype=bool)
    g4  = np.zeros(n, dtype=bool)
    g6  = np.zeros(n, dtype=bool)
    g11 = np.zeros(n, dtype=bool)

    # sig_id constants (from the ID table above)
    # T1G=1, T1=2, T4=6, T6=8, T10=10, T11=11
    # Z10=23, Z11=24, Z12=25

    g_armed   = False  # armed by Z10/Z11/Z12, fires on first T1/T1G/T4/T6
    g11_armed = False  # armed by T10/T11, fires on first T1

    for i in range(n):
        s = sid[i]

        # G1/G2/G4/G6 ---------------------------------------------------
        trigger_z = s in (23, 24, 25)   # Z10, Z11, Z12
        g1_raw  = g_armed and s == 2    # T1
        g2_raw  = g_armed and s == 1    # T1G
        g4_raw  = g_armed and s == 6    # T4
        g6_raw  = g_armed and s == 8    # T6
        any_g   = g1_raw or g2_raw or g4_raw or g6_raw

        g1[i]  = g1_raw
        g2[i]  = g2_raw
        g4[i]  = g4_raw
        g6[i]  = g6_raw

        g_armed = (g_armed or trigger_z) and not any_g

        # G11 ------------------------------------------------------------
        g11_trigger = s in (10, 11)     # T10, T11
        g11_raw = g11_armed and s == 2  # T1
        g11[i]  = g11_raw
        g11_armed = (g11_armed or g11_trigger) and not g11_raw

    return pd.DataFrame(
        {"g1": g1, "g2": g2, "g4": g4, "g6": g6, "g11": g11},
        index=df.index,
    )


# ---------------------------------------------------------------------------
# B1–B11  (260321_B_BUILDER / 260410_COMBO)
# ---------------------------------------------------------------------------
# Pine bc priority codes: T4=1,T6=2,T1G=3,T2G=4,T1=5,T2=6,T9=7,T10=8,T3=9,T11=10,T5=11
# Pine zc priority codes: Z4=1,Z6=2,Z1G=3,Z2G=4,Z1=5,Z2=6,Z8=7,Z9=8,Z10=9,Z3=10,
#                         Z11=11,Z5=12,Z12=13,Z7=14
# These match bc_arr/zc_arr values from compute_signals() above.

def compute_b_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute B1–B11 buy patterns using T/Z signal combinations.
    Returns DataFrame with boolean columns b1..b11.
    """
    sig  = compute_signals(df)
    bc   = sig["bc"].fillna(0).astype(int)
    zc   = sig["zc"].fillna(0).astype(int)
    cls  = df["close"] if "close" in df.columns else df[df.columns[3]]
    opn  = df["open"]  if "open"  in df.columns else df[df.columns[0]]

    fv = 0  # fill_value for shifted series
    bc1 = bc.shift(1, fill_value=fv).astype(int)
    bc2 = bc.shift(2, fill_value=fv).astype(int)
    bc3 = bc.shift(3, fill_value=fv).astype(int)
    bc6 = bc.shift(6, fill_value=fv).astype(int)
    zc1 = zc.shift(1, fill_value=fv).astype(int)
    zc2 = zc.shift(2, fill_value=fv).astype(int)
    zc3 = zc.shift(3, fill_value=fv).astype(int)
    c1  = cls.shift(1)

    # ── B1 ────────────────────────────────────────────────────────────────────
    B1 = (
        ((bc1==11) & (bc==6)) |              # T5[1]  T2
        ((bc1==10) & (bc==4)) |              # T11[1] T2G
        ((zc2==11) & bc.isin([9, 5, 3])) |  # Z11[2] (T3/T1/T1G)
        ((zc2==2)  & (zc1==6) & (bc==5)) |  # Z6[2]  Z2[1]  T1
        ((bc1==5)  & (bc==4)) |              # T1[1]  T2G
        ((bc1==7)  & (bc==6)) |              # T9[1]  T2
        ((zc3==3)  & (bc==4))               # Z1G[3] T2G
    )

    # ── B2 ────────────────────────────────────────────────────────────────────
    B2 = (
        (bc2.isin([5,3,6,4,7,9,11,2,1,10,8]) & (bc==1)) |  # (any T)[2] T4
        (bc1.isin([1,4,3,9,7,5,8,10,2]) & (bc==2)) |        # (+T1G/T6)[1] T6
        ((bc2==10) & (bc==2)) |                              # T11[2] T6
        ((bc3==9)  & (bc==1)) |                              # T3[3]  T4
        ((bc6==9)  & (bc==1)) |                              # T3[6]  T4
        ((zc1==4)  & (bc==1)) |                              # Z2G[1] T4
        (((zc1==2) | (zc2==2)) & (bc==1))                   # (Z6[1] or Z6[2]) T4
    )

    # ── B3 ────────────────────────────────────────────────────────────────────
    _B3_strong2 = bc2.isin([3, 4, 1, 7, 11])  # T1G/T2G/T4/T9/T5 at bar[-2]
    B3 = (
        ((bc2==2)  & (bc==5)) |              # T6[2]  T1
        ((zc2==10) & (zc1==6)  & (bc==9)) | # Z3[2]  Z2[1] T3
        ((zc2==10) & (bc1==9)  & (bc==6)) | # Z3[2]  T3[1] T2
        (_B3_strong2 & (zc1==1) & bc.isin([5, 9])) | # (T1G/T2G/T4/T9/T5)[2] Z4[1] (T1/T3)
        ((bc1==8)  & bc.isin([3, 4, 6]))    # T10[1] (T1G/T2G/T2)
    )

    # ── B4 ────────────────────────────────────────────────────────────────────
    B4 = ((zc1==1) & (bc==3))              # Z4[1] T1G

    # ── B5 ────────────────────────────────────────────────────────────────────
    B5 = (
        ((zc2==1)  & (bc1==11) & ((bc==6) | (bc==4))) | # Z4[2]  T5[1] (T2/T2G)
        ((zc2==12) & (bc1==11) & (bc==4)) |              # Z5[2]  T5[1] T2G
        ((zc2==4)  & (bc1==11) & (bc==4)) |              # Z2G[2] T5[1] T2G
        ((zc2==6)  & (bc1==11) & (bc==2))                # Z2[2]  T5[1] T6
    )

    # ── B6 ────────────────────────────────────────────────────────────────────
    B6 = (
        ((bc2==1)  & (bc1==8) & (bc==4)) |  # T4[2]  T10[1] T2G
        ((zc2==6)  & (zc1==4) & (bc==7))    # Z2[2]  Z2G[1] T9
    )

    # ── B7 ────────────────────────────────────────────────────────────────────
    B7 = (
        ((zc2==12) & (zc1==6) & (bc==5)) | # Z5[2]  Z2[1] T1
        ((bc2==7)  & (bc1==6) & (bc==6)) | # T9[2]  T2[1] T2
        ((bc1==5)  & (bc==6)) |             # T1[1]  T2
        ((bc1==6)  & (bc==4)) |             # T2[1]  T2G
        ((bc1==1)  & (bc==4)) |             # T4[1]  T2G
        ((bc1==3)  & (bc==4))               # T1G[1] T2G
    )

    # ── B8 ────────────────────────────────────────────────────────────────────
    B8 = (
        ((bc2==3)  & (bc==3)) |              # T1G[2] T1G
        (zc2.isin([4,3]) & bc.isin([3,7])) | # (Z2G/Z1G)[2] (T1G/T9)
        (((zc1==4) | (zc1==3)) & (bc==3)) |  # (Z2G/Z1G)[1] T1G
        ((zc2==3)  & (bc==3)) |              # Z1G[2] T1G
        ((zc2==2)  & (bc==3)) |              # Z6[2]  T1G
        ((bc2==7)  & (zc1==8) & (bc==9)) |  # T9[2]  Z9[1]  T3
        ((zc2==10) & (zc1==4) & (bc==3))    # Z3[2]  Z2G[1] T1G
    )

    # ── B9 ────────────────────────────────────────────────────────────────────
    B9 = (
        (zc3==9) & (zc2==4) & (bc1==7) & (cls > opn)  # Z10[3] Z2G[2] T9[1] close>open
    )

    # ── B10 ───────────────────────────────────────────────────────────────────
    B10_s1 = (
        ((zc2==9)  & (zc1==4)  & (bc==5)) |  # Z10[2] Z2G[1] T1
        ((zc1==9)  & ((bc==5)  | (bc==1))) |  # Z10[1] (T1/T4)
        ((zc2==9)  & (zc1==11) & (zc==14)) |  # Z10[2] Z11[1] Z7
        ((zc2==9)  & (zc1==4)  & (bc==3))     # Z10[2] Z2G[1] T1G
    )
    B10_s2 = ((zc2==9) & ((zc1==2) | (bc1==7)) & ((bc==5) | (bc==2)))
    B10_s3 = (((zc2==2) | (zc2==4)) & (zc1==9) & ((bc==3) | (bc==2)))
    B10 = B10_s1 | B10_s2 | B10_s3

    # ── B11 ───────────────────────────────────────────────────────────────────
    B11 = (
        ((zc1==11) & (bc==5)) |              # Z11[1] T1
        ((bc2==9)  & (bc1==10) & (bc==6)) |  # T3[2]  T11[1] T2
        ((zc2==10) & (zc1==9)  & (zc==11)) | # Z3[2]  Z10[1] Z11
        ((zc2==8)  & (zc1==9)  & (cls > c1)) | # Z9[2] Z10[1] close>close[1]
        ((zc2==2)  & (zc1==9)  & (cls > c1)) | # Z6[2] Z10[1] close>close[1]
        (((bc3==9) | (bc3==1)) & (zc2==1) & (zc1==6) & bc.isin([7, 3])) | # (T3/T4)[3] Z4[2] Z2[1] (T9/T1G)
        ((zc2==6)  & (bc1==3)  & (bc==6)) |  # Z2[2]  T1G[1] T2
        ((bc2==9)  & (bc1==4)  & (bc==6)) |  # T3[2]  T2G[1] T2
        ((zc2==6)  & (bc1==9)  & (bc==6))   # Z2[2]  T3[1]  T2
    )

    return pd.DataFrame(
        {"b1": B1, "b2": B2, "b3": B3, "b4": B4, "b5": B5,
         "b6": B6, "b7": B7, "b8": B8, "b9": B9, "b10": B10, "b11": B11},
        index=df.index,
    ).fillna(False)
