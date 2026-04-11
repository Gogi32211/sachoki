"""
predictor.py — 3-bar and 2-bar next-bar signal prediction with regime split.
"""
from __future__ import annotations
from collections import Counter

import numpy as np
import pandas as pd

from signal_engine import SIG_NAMES, BULLISH_SIGS, BEARISH_SIGS


def predict_next(df: pd.DataFrame, lookback: int = 5000) -> dict:
    """
    Given df with sig_id + OHLCV columns, find the last 3-bar and 2-bar patterns
    then count what signal appeared on the next bar historically.
    Also splits outcomes by market regime (EMA20 > EMA50 = bull).

    Returns
    -------
    {
      "current_regime": "bull" | "bear",
      "tz_3bar": {
        "pattern": "ZTN",
        "signals": "Z4 → T1G → NONE",
        "total_matches": 23,
        "bull_matches": 15,  "bear_matches": 8,
        "bull_bull_pct": 73, "bear_bull_pct": 50,
        "top_outcomes": [...]
      },
      "tz_2bar": { same structure }
    }
    """
    df = df.tail(lookback).reset_index(drop=True)

    if "sig_id" not in df.columns:
        return _empty()

    sigs = df["sig_id"].to_numpy(dtype=np.int8)
    n = len(sigs)

    if n < 4:
        return _empty()

    # ── Regime: bull = EMA20 > EMA50 ────────────────────────────────────────
    if "close" in df.columns:
        ema20 = df["close"].ewm(span=20, adjust=False).mean().to_numpy()
        ema50 = df["close"].ewm(span=50, adjust=False).mean().to_numpy()
        is_bull_regime = ema20 > ema50
    else:
        is_bull_regime = np.ones(n, dtype=bool)

    current_regime = "bull" if is_bull_regime[-1] else "bear"

    last3 = tuple(sigs[-3:])
    last2 = tuple(sigs[-2:])

    sig3_label = " → ".join(SIG_NAMES.get(int(s), "NONE") for s in last3)
    sig2_label = " → ".join(SIG_NAMES.get(int(s), "NONE") for s in last2)

    pat3 = "".join(_pchar(s) for s in last3)
    pat2 = "".join(_pchar(s) for s in last2)

    out3: list[int] = []; out3_bull: list[int] = []; out3_bear: list[int] = []
    out2: list[int] = []; out2_bull: list[int] = []; out2_bear: list[int] = []

    for i in range(n - 3):
        if sigs[i] == last3[0] and sigs[i+1] == last3[1] and sigs[i+2] == last3[2]:
            outcome = int(sigs[i + 3])
            out3.append(outcome)
            if is_bull_regime[i + 2]:
                out3_bull.append(outcome)
            else:
                out3_bear.append(outcome)

    for i in range(n - 2):
        if sigs[i] == last2[0] and sigs[i+1] == last2[1]:
            outcome = int(sigs[i + 2])
            out2.append(outcome)
            if is_bull_regime[i + 1]:
                out2_bull.append(outcome)
            else:
                out2_bear.append(outcome)

    return {
        "current_regime": current_regime,
        "tz_3bar": _summarize(pat3, sig3_label, out3, out3_bull, out3_bear),
        "tz_2bar": _summarize(pat2, sig2_label, out2, out2_bull, out2_bear),
    }


def _pchar(s: int) -> str:
    if s in BULLISH_SIGS:
        return "T"
    if s in BEARISH_SIGS:
        return "Z"
    return "N"


def _bull_pct(outcomes: list[int]) -> int:
    if not outcomes:
        return 0
    return round(sum(1 for s in outcomes if s in BULLISH_SIGS) / len(outcomes) * 100)


def _summarize(pattern: str, signals: str, outcomes: list[int],
               bull_out: list[int], bear_out: list[int]) -> dict:
    total = len(outcomes)
    if total == 0:
        return {"pattern": pattern, "signals": signals,
                "total_matches": 0, "top_outcomes": [],
                "bull_matches": 0, "bear_matches": 0,
                "bull_bull_pct": 0, "bear_bull_pct": 0}

    counts = Counter(outcomes)
    top = [
        {
            "sig_id": sid,
            "sig_name": SIG_NAMES.get(sid, "NONE"),
            "count": cnt,
            "pct": round(cnt / total * 100),
            "is_bull": sid in BULLISH_SIGS,
            "is_bear": sid in BEARISH_SIGS,
        }
        for sid, cnt in counts.most_common(10)
    ]
    return {
        "pattern": pattern, "signals": signals,
        "total_matches": total, "top_outcomes": top,
        "bull_matches": len(bull_out),
        "bear_matches": len(bear_out),
        "bull_bull_pct": _bull_pct(bull_out),
        "bear_bull_pct": _bull_pct(bear_out),
    }


def _empty() -> dict:
    empty = {"pattern": "", "signals": "", "total_matches": 0, "top_outcomes": [],
             "bull_matches": 0, "bear_matches": 0, "bull_bull_pct": 0, "bear_bull_pct": 0}
    return {"current_regime": "unknown", "tz_3bar": empty, "tz_2bar": empty}
