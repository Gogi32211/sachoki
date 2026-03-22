"""
predictor.py — 3-bar and 2-bar next-bar signal prediction.
"""
from __future__ import annotations
from collections import Counter

import numpy as np
import pandas as pd

from signal_engine import SIG_NAMES, BULLISH_SIGS, BEARISH_SIGS


def predict_next(df: pd.DataFrame, lookback: int = 5000) -> dict:
    """
    Given df with sig_id column, find the last 3-bar and 2-bar patterns
    then count what signal appeared on the next bar historically.

    Returns
    -------
    {
      "3bar": {
        "pattern": "ZTN",
        "signals": "Z4 → T1G → NONE",
        "total_matches": 23,
        "top_outcomes": [
          {"sig_id": 4, "sig_name": "T2", "count": 8, "pct": 35,
           "is_bull": True, "is_bear": False},
          ...
        ]
      },
      "2bar": { same structure }
    }
    """
    df = df.tail(lookback).reset_index(drop=True)

    if "sig_id" not in df.columns:
        return _empty()

    sigs = df["sig_id"].to_numpy(dtype=np.int8)
    n = len(sigs)

    if n < 4:
        return _empty()

    last3 = tuple(sigs[-3:])
    last2 = tuple(sigs[-2:])

    sig3_label = " → ".join(SIG_NAMES.get(int(s), "NONE") for s in last3)
    sig2_label = " → ".join(SIG_NAMES.get(int(s), "NONE") for s in last2)

    pat3 = "".join(_pchar(s) for s in last3)
    pat2 = "".join(_pchar(s) for s in last2)

    outcomes3: list[int] = []
    outcomes2: list[int] = []

    for i in range(n - 3):
        if sigs[i] == last3[0] and sigs[i+1] == last3[1] and sigs[i+2] == last3[2]:
            outcomes3.append(int(sigs[i + 3]))

    for i in range(n - 2):
        if sigs[i] == last2[0] and sigs[i+1] == last2[1]:
            outcomes2.append(int(sigs[i + 2]))

    return {
        "tz_3bar": _summarize(pat3, sig3_label, outcomes3),
        "tz_2bar": _summarize(pat2, sig2_label, outcomes2),
    }


def _pchar(s: int) -> str:
    if s in BULLISH_SIGS:
        return "T"
    if s in BEARISH_SIGS:
        return "Z"
    return "N"


def _summarize(pattern: str, signals: str, outcomes: list[int]) -> dict:
    total = len(outcomes)
    if total == 0:
        return {"pattern": pattern, "signals": signals,
                "total_matches": 0, "top_outcomes": []}

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
    return {"pattern": pattern, "signals": signals,
            "total_matches": total, "top_outcomes": top}


def _empty() -> dict:
    empty = {"pattern": "", "signals": "", "total_matches": 0, "top_outcomes": []}
    return {"tz_3bar": empty, "tz_2bar": empty}
