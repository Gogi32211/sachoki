"""VOL7 — the "260829 • 7-Level Volume MR + Sigma Consensus/Divergence + B/VB + Shift" Pine v6 script
(and its 260828 sigma-only sibling) ported for DISPLAY. Daily bars only.

STATUS — DESCRIPTIVE ONLY. Nothing here is an edge, a score input or a filter with claimed lift. The
book's B/VB (WLNBB bucket, VB = vol ≥ 2·mid + σ) stays what it is; these levels are named M0..M6 /
σ0..σ6 so they never masquerade as it. SHIFT is a compound SETUP marker; any claim about it needs a
PLAN-FIRST study, not this file.

DEFINITIONS (verbatim from the scripts; per ticker over the deduplicated daily bar sequence)
  MR level  ratio = volume / median(volume, 20) (1.0 when the median is 0):
            M0 < 0.40 · M1 < 0.70 · M2 < 1.00 · M3 < 1.50 · M4 < 2.50 · M5 < 5.00 (script "B") · M6 ≥ 5.00 (script "VB")
  σ level   [mid, up, low] = ta.bb(volume, 20, 1) (population stdev): σ0 < mid−1.5σ · σ1 < mid−σ · σ2 < mid−0.5σ ·
            σ3 < mid+0.5σ · σ4 < mid+σ · σ5 < mid+2σ · σ6 otherwise
  consensus diff = MR − σ:  "=" when 0 · "MR+" when ≥ 2 · "Σ+" when ≤ −2 · ±1 unmarked
  PRIMARY state = MR (the 260829 script: "these seven levels are the ACTUAL volume state")
  transition  MR(D) − MR(D−1) when both defined, else 0:  ▲+2 / ▼−2 = skip · ◆+3 / ◆−3 = big jump (|Δ| ≥ 3)
  VB cluster  VB = M6; a VB within 2 bars of the previous VB is the same event; VB2 = a new VB event that
              comes ≥ 6 bars after the previous VB bar (script: clusterGapBars 2, minQuietBars 6)
  prior move  (close[1] / close[11] − 1) × 100, needs bar_index > 11: up ≥ +3 % · down ≤ −3 %
  SHIFT       at VB2: direction +1 if prior move down, −1 if up, else no candidate; range = the VB2 bar's
              high/low, expanded by VB bars within 2 bars; within the next 5 bars close > range high with a
              green candle → SHIFT↑, close < range low with a red candle → SHIFT↓; expires after 5 bars; a
              new VB2 restarts the candidate
  Levels are undefined for the first 19 bars of a ticker (rolling windows), exactly as in Pine.
  Note: ta.median's even-length convention is not documented; the true median (mean of the two middle
  values) is used here — a boundary-only difference.

OUTPUT  data/vol7_signals.parquet — one row per (ticker, date) with defined levels: mr, sg, ratio, cons,
        trans, jump, flags, prior move, marks (notable tokens), label ("M4·σ5"), text (tooltip).
Built by lbal_build.py in the same nightly step (atomic replace).
"""
from __future__ import annotations
import os
import sys
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from studio.paths import DATA_DIR                                      # noqa: E402

OUT = os.path.join(DATA_DIR, "vol7_signals.parquet")
VOL_LEN = 20
MR_EDGES = (0.40, 0.70, 1.00, 1.50, 2.50, 5.00)
CLUSTER_GAP, MIN_QUIET, TREND_LOOKBACK, MIN_PRIOR_MOVE, CONFIRM_BARS = 2, 6, 10, 3.0, 5
JUMP = {2: "▲+2", -2: "▼−2"}
MARK_MEANING = {
    "M0":     "extreme low volume — below 0.40× the 20-day median",
    "M5":     "script B — 2.5..5× the 20-day median",
    "M6":     "script VB — ≥ 5× the 20-day median",
    "=":      "exact consensus — median-ratio and sigma models give the same level",
    "MR+":    "median-ratio level ≥ 2 above the sigma level (spiky window under-rates the bar in σ terms)",
    "Σ+":     "sigma level ≥ 2 above the median-ratio level (quiet, uniform window over-rates the bar in σ terms)",
    "▲+2":    "volume regime skipped UP two levels vs yesterday",
    "▼−2":    "volume regime skipped DOWN two levels vs yesterday",
    "◆+3":    "BIG volume jump UP, three or more levels vs yesterday",
    "◆−3":    "BIG volume jump DOWN, three or more levels vs yesterday",
    "VB2":    "new extreme-volume event ≥ 6 bars after the previous VB (VBs within 2 bars = one event)",
    "SHIFT↑": "bullish regime shift — VB2 after a ≥ 3 % 10-day decline, then a green close above the VB range within 5 bars",
    "SHIFT↓": "bearish regime shift — VB2 after a ≥ 3 % 10-day rise, then a red close below the VB range within 5 bars",
}


def mr_level(ratio: np.ndarray) -> np.ndarray:
    return np.select([ratio < e for e in MR_EDGES], [0, 1, 2, 3, 4, 5], 6)


def sigma_level(v: np.ndarray, mid: np.ndarray, sd: np.ndarray) -> np.ndarray:
    return np.select([v < mid - 1.5 * sd, v < mid - sd, v < mid - 0.5 * sd, v < mid + 0.5 * sd, v < mid + sd, v < mid + 2 * sd],
                     [0, 1, 2, 3, 4, 5], 6)


def compute_ticker(d: pd.DataFrame) -> pd.DataFrame:
    """d: one ticker's daily bars sorted by date: date, open, high, low, close, volume."""
    v = np.nan_to_num(d["volume"].to_numpy(float), nan=0.0)
    o, h, l, c = (d[k].to_numpy(float) for k in ("open", "high", "low", "close"))
    n = len(v)
    s = pd.Series(v)
    med = s.rolling(VOL_LEN).median().to_numpy()
    mid = s.rolling(VOL_LEN).mean().to_numpy()
    sd = s.rolling(VOL_LEN).std(ddof=0).to_numpy()
    defined = np.isfinite(med)
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.where(med > 0, v / med, 1.0)
    mr = np.where(defined, mr_level(ratio), -1)
    sg = np.where(defined, sigma_level(v, mid, sd), -1)
    diff = np.where(defined, mr - sg, 0)
    prev = np.r_[-1, mr[:-1]]
    trans = np.where(defined & (prev >= 0), mr - prev, 0)
    # VB cluster / VB2 / prior move / SHIFT — a bar-by-bar state machine, exactly the script's var logic
    is_vb = mr == 6
    vb2 = np.zeros(n, bool); up = np.zeros(n, bool); dn = np.zeros(n, bool); pmove = np.full(n, np.nan)
    last = -1; cand = False; cdir = 0; cstart = -1; ch = cl = np.nan
    for i in range(n):
        since = i - last if last >= 0 else -1
        newc = is_vb[i] and (last < 0 or since > CLUSTER_GAP)
        vb2[i] = bool(newc and last >= 0 and since >= MIN_QUIET)
        if is_vb[i]:
            last = i
        if i > TREND_LOOKBACK + 1:
            pmove[i] = (c[i - 1] / c[i - TREND_LOOKBACK - 1] - 1.0) * 100.0
        if vb2[i]:
            pm = pmove[i]
            cdir = 1 if (np.isfinite(pm) and pm <= -MIN_PRIOR_MOVE) else (-1 if (np.isfinite(pm) and pm >= MIN_PRIOR_MOVE) else 0)
            cand = cdir != 0; cstart = i; ch = h[i]; cl = l[i]
        if cand and is_vb[i] and cstart >= 0 and (i - cstart) <= CLUSTER_GAP:
            ch = max(ch, h[i]); cl = min(cl, l[i])
        if cand and cstart >= 0:
            age = i - cstart
            inside = 0 < age <= CONFIRM_BARS
            if inside and cdir == 1 and c[i] > ch and c[i] > o[i]:
                up[i] = True; cand = False
            elif inside and cdir == -1 and c[i] < cl and c[i] < o[i]:
                dn[i] = True; cand = False
            if age > CONFIRM_BARS:
                cand = False
    return pd.DataFrame(dict(date=d["date"].to_numpy(), ratio=ratio, mr=mr, sg=sg, diff=diff, trans=trans,
                             vb2=vb2, shift_up=up, shift_dn=dn, prior_move=pmove, defined=defined))


def marks_of(row) -> str:
    out = []
    if row["mr"] == 0:
        out.append("M0")
    if row["diff"] >= 2:
        out.append("MR+")
    elif row["diff"] <= -2:
        out.append("Σ+")
    t = int(row["trans"])
    if t >= 3:
        out.append("◆+3")
    elif t <= -3:
        out.append("◆−3")
    elif t in JUMP:
        out.append(JUMP[t])
    if row["vb2"]:
        out.append("VB2")
    if row["shift_up"]:
        out.append("SHIFT↑")
    if row["shift_dn"]:
        out.append("SHIFT↓")
    return " ".join(out)


def build(daily: pd.DataFrame, log=print) -> pd.DataFrame:
    """daily: deduplicated 1D bars (ticker, date 'YYYY-MM-DD', open, high, low, close, volume), any order."""
    daily = daily.sort_values(["ticker", "date"])
    parts = []
    for tk, g in daily.groupby("ticker", sort=False):
        r = compute_ticker(g.reset_index(drop=True))
        r = r[r["defined"]].drop(columns=["defined"]).copy()
        if len(r):
            r.insert(0, "ticker", tk); parts.append(r)
    out = pd.concat(parts, ignore_index=True)
    for col in ("mr", "sg", "diff", "trans"):
        out[col] = out[col].astype("int8")
    out["cons"] = np.where(out["diff"] == 0, "=", np.where(out["diff"] >= 2, "MR+", np.where(out["diff"] <= -2, "Σ+", "")))
    out["jump"] = [("◆+3" if t >= 3 else "◆−3" if t <= -3 else JUMP.get(int(t), "")) for t in out["trans"]]
    out["label"] = [f"M{a}·σ{b}" for a, b in zip(out["mr"], out["sg"])]
    out["marks"] = [marks_of(r) for _, r in out.iterrows()]
    pm = out["prior_move"].to_numpy()
    out["text"] = [f"{lab} · {rt:.2f}× median{(' ' + mk) if mk else ''}{(f' · prior 10d {p:+.1f}%' if np.isfinite(p) else '')}"
                   for lab, rt, mk, p in zip(out["label"], out["ratio"], out["marks"], pm)]
    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)
    if out.duplicated(["ticker", "date"]).any():
        raise RuntimeError("VOL7: duplicate (ticker, date) grain")
    if ((out["shift_up"] | out["shift_dn"]) & ~out["vb2"].shift(1, fill_value=False) & False).any():
        raise RuntimeError("unreachable")
    if int(out["vb2"].sum()) == 0 or int(out["shift_up"].sum()) == 0:
        raise RuntimeError("VOL7: zero VB2 / SHIFT rows — a defect, not a finding")
    log(f"  VOL7: {len(out):,} bars · M6 {int((out.mr == 6).sum()):,} · VB2 {int(out.vb2.sum()):,} · "
        f"SHIFT↑ {int(out.shift_up.sum()):,} · SHIFT↓ {int(out.shift_dn.sum()):,}")
    return out
