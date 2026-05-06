"""Load and index the TZ Signal Intelligence master matrix CSV."""
from __future__ import annotations
import csv
import os
from functools import lru_cache
from typing import Dict, List

def _find_csv() -> str:
    """Locate the matrix CSV in Docker (/app/...) or local dev (repo root)."""
    base = os.path.dirname(__file__)
    candidates = [
        os.path.join(base, "..", "tz_intelligence_package",
                     "TZ_SIGNAL_INTELLIGENCE_master_matrix_seed.csv"),
        os.path.join(base, "..", "..", "tz_intelligence_package",
                     "TZ_SIGNAL_INTELLIGENCE_master_matrix_seed.csv"),
    ]
    for p in candidates:
        p = os.path.normpath(p)
        if os.path.exists(p):
            return p
    return os.path.normpath(candidates[0])

_CSV_PATH = _find_csv()


@lru_cache(maxsize=1)
def load_matrix() -> "MatrixIndex":
    rows: List[dict] = []
    with open(_CSV_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return MatrixIndex(rows)


class MatrixIndex:
    def __init__(self, rows: List[dict]):
        self.rows = rows

        # composite pattern → rules  (e.g. "T1L12NP" → [...])
        self.composite: Dict[str, List[dict]] = {}
        # reject composite pattern → rules
        self.reject_composite: Dict[str, List[dict]] = {}
        # seq4 pipe-pattern → rules  (e.g. "Z2G|T1|Z5|T1" → [...])
        self.seq4: Dict[str, List[dict]] = {}
        # reject seq4
        self.reject_seq4: Dict[str, List[dict]] = {}
        # baseline signal → rule
        self.baseline: Dict[str, dict] = {}
        # meta rules (EMA, PRICE_POSITION, SHORT_CONFIRMATION, ARCHITECTURE, ROLE_GROUP)
        self.meta: List[dict] = []

        for r in rows:
            rt = r["rule_type"]
            pat = r["pattern"].strip()
            if rt == "BASELINE":
                self.baseline.setdefault(r["signal"], r)
            elif rt == "COMPOSITE":
                self.composite.setdefault(pat, []).append(r)
            elif rt == "REJECT_COMPOSITE":
                self.reject_composite.setdefault(pat, []).append(r)
            elif rt == "SEQ4":
                self.seq4.setdefault(pat, []).append(r)
            elif rt == "REJECT_SEQ4":
                self.reject_seq4.setdefault(pat, []).append(r)
            else:
                self.meta.append(r)

    def get_ema_bonus(self) -> int:
        for r in self.meta:
            if r["pattern"] == "EMA50_RECLAIM":
                return int(r["score_base"] or 0)
        return 10

    def get_price_position_bonus(self) -> int:
        for r in self.meta:
            if r["pattern"] == "FINAL_CLOSE_TOP_75PCT_4BAR":
                return int(r["score_base"] or 0)
        return 10

    def get_short_go_bonus(self) -> int:
        for r in self.meta:
            if r["pattern"] == "BREAK_4BAR_LOW_AFTER_REJECT":
                return int(r["score_base"] or 0)
        return 35
