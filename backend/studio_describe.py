"""Counting the alphabet, in the app instead of in a chat window.

Everything this serves, I had been running by hand as one-off SQL and pasting the table back —
which means the answer existed only in a transcript, could not be re-run, and stopped being
available the moment the conversation moved on. This is the same query behind a button.

STRICTLY DESCRIPTIVE, AND THAT IS A BOUNDARY RATHER THAN A LIMITATION. It counts how often
values occur and co-occur. It never touches an outcome column — no forward return, no win rate,
no verdict. That is why it does NOT charge k: counting how many bars carry `L34` exposes no
claim about what `L34` does, and charging for it would make the counter meaningless by inflating
it with bookkeeping. The moment an outcome column becomes selectable here, this becomes a search
surface and the k-charge that the sequence builder now has applies to it too.

THE ALLOWLIST IS THE ONE IN `sources`, NOT A NEW ONE. `BAR_PRIMITIVES` and `BAR_FORBIDDEN`
already encode which columns are facts about a bar and which are conclusions we fitted. A second
list here would drift from the first, and the drift would be discovered by someone grouping by
`ultra_score_v3` and reading it as data.

NO SILENT TRUNCATION. A crosstab of two high-cardinality columns can be thousands of rows; the
response caps it and says so in `truncated`, with the number dropped. A capped table that does
not admit it reads as a complete one.
"""
from __future__ import annotations

import os
from typing import Optional

from pydantic import BaseModel

from sources import BAR_FORBIDDEN, BAR_PRIMITIVES


class DescribeRequest(BaseModel):
    """Module level, and that is load-bearing rather than tidy.

    Defined inside `build_router()` with `from __future__ import annotations` in effect, FastAPI
    cannot resolve the annotation at route-registration time and silently degrades the body to a
    query parameter — the request then fails with "field required (query, req)". This project
    already has an integration-integrity incident for exactly that pair; I reintroduced it here
    while writing the route, which is the second time the same shape has cost an hour.
    """
    col_a: str
    col_b: str = ""
    universe: str = "sp500"
    years: Optional[list] = None
    months: Optional[list] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None

HERE = os.path.dirname(os.path.abspath(__file__))

MAX_ROWS = 400

# Columns worth offering, in the order a person thinks about a bar. Only those that exist in the
# studio DB are returned — the list is intersected with DESCRIBE at request time.
OFFERED = [
    ("t_sig", "L1 · T signal"),
    ("z_sig", "L1 · Z signal"),
    ("l_sig", "L2 · L (WLNBB)"),
    ("full_suffix", "L3 · suffix"),
    ("bar_body_wick", "L4 · body/wick"),
    ("bar_gap_range", "L5 · gap/range"),
    ("bar_line5", "L6 · VIX/PSAR/RSI2"),
    ("vol_sig", "L7 · volume bucket"),
    ("g_sig", "gap signal"),
    ("b_sig", "b signal"),
    ("fly_sig", "fly signal"),
    ("combo_sig", "combo signal"),
    ("swing_type", "swing type"),
    ("swing_type_3", "swing type (3)"),
    ("swing_type_5", "swing type (5)"),
    ("ne_suffix", "ne suffix"),
    ("wick_suffix", "wick suffix"),
    ("penetration_suffix", "penetration suffix"),
    ("close_suffix", "close suffix"),
    ("setup_tokens", "setup tokens"),
    ("context_tokens", "context tokens"),
    ("sector", "sector"),
    ("universe", "universe"),
]

UNIVERSES = ["sp500", "nasdaq", "russell2k"]


class DescribeError(RuntimeError):
    """A column that may not be grouped by, or a request that cannot be answered."""


def _conn():
    from studio.signal_stats import get_conn                          # noqa: PLC0415
    return get_conn(read_only=True)


def _available(conn) -> set:
    return set(conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())


def assert_groupable(col: str, available: set) -> None:
    """The refusal carries the reason from the contract, not a generic 'not allowed'."""
    if col in BAR_FORBIDDEN:
        raise DescribeError(
            f"{col!r} cannot be grouped by here: {BAR_FORBIDDEN[col]}. Counting how often our own "
            f"score takes a value describes our past decisions, not the market.")
    if col not in BAR_PRIMITIVES:
        raise DescribeError(
            f"{col!r} is not in the bar-primitive allowlist. The allowlist is shared with "
            f"`sources.bars()` on purpose — a second one here would drift from the first.")
    if col not in available:
        raise DescribeError(f"{col!r} is not a column of the studio bars table")


def columns() -> dict:
    """What the dropdowns are built from — served, never hardcoded in the frontend."""
    conn = _conn()
    available = _available(conn)
    out = []
    for col, label in OFFERED:
        if col in available and col in BAR_PRIMITIVES:
            out.append({"column": col, "label": label})
    return {"columns": out, "universes": UNIVERSES,
            "excluded_note": ("scores, tiers and our forward labels are excluded by the shared "
                              "allowlist in sources.py — grouping by them would describe our own "
                              "past conclusions"),
            "descriptive_only": ("counts only. No outcome column is reachable from here, which "
                                 "is why this surface does not charge k")}


def _filters(universe, years, months, min_price, max_price) -> str:
    w = []
    if universe:
        u = str(universe).lower()
        if u not in UNIVERSES:
            raise DescribeError(f"unknown universe {universe!r}")
        w.append(f"universe = '{u}'")
    if years:
        w.append("YEAR(date) IN (" + ",".join(str(int(y)) for y in years) + ")")
    if months:
        w.append("MONTH(date) IN (" + ",".join(str(int(m)) for m in months) + ")")
    if min_price is not None:
        w.append(f"close >= {float(min_price)}")
    if max_price is not None:
        w.append(f"close <= {float(max_price)}")
    return ("WHERE " + " AND ".join(w)) if w else ""


def describe(*, col_a: str, col_b: str = "", universe: str = "sp500", years=None, months=None,
             min_price=None, max_price=None) -> dict:
    """Counts for one column, or a crosstab of two. One row per (ticker, date)."""
    conn = _conn()
    available = _available(conn)
    assert_groupable(col_a, available)
    if col_b:
        assert_groupable(col_b, available)

    where = _filters(universe, years, months, min_price, max_price)
    # QUALIFY: without a universe filter the same ticker-date lives in several universe rows,
    # and counting them all would silently multiply every cell.
    dedup = ("QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1")
    a = f"COALESCE(NULLIF(CAST({col_a} AS VARCHAR), ''), '(empty)')"
    total = conn.execute(f"SELECT COUNT(*) FROM (SELECT 1 FROM bars {where} {dedup})").fetchone()[0]

    if not col_b:
        df = conn.execute(
            f"SELECT {a} AS value, COUNT(*) AS n FROM (SELECT * FROM bars {where} {dedup}) "
            f"GROUP BY 1 ORDER BY n DESC").fetchdf()
        rows = [{"value": r.value, "n": int(r.n),
                 "pct": round(100.0 * r.n / total, 2) if total else 0.0}
                for r in df.itertuples()]
        return {"mode": "single", "col_a": col_a, "total": int(total),
                "distinct": len(rows), "rows": rows[:MAX_ROWS],
                "truncated": max(0, len(rows) - MAX_ROWS),
                "descriptive_only": True}

    b = f"COALESCE(NULLIF(CAST({col_b} AS VARCHAR), ''), '(empty)')"
    df = conn.execute(
        f"SELECT {a} AS a, {b} AS b, COUNT(*) AS n FROM (SELECT * FROM bars {where} {dedup}) "
        f"GROUP BY 1,2 ORDER BY n DESC").fetchdf()
    rows = [{"a": r.a, "b": r.b, "n": int(r.n),
             "pct": round(100.0 * r.n / total, 2) if total else 0.0} for r in df.itertuples()]
    n_a = df["a"].nunique()
    n_b = df["b"].nunique()
    return {"mode": "cross", "col_a": col_a, "col_b": col_b, "total": int(total),
            "distinct_a": int(n_a), "distinct_b": int(n_b),
            # the number worth seeing: how much of the grid actually exists. A builder offering
            # n_a x n_b combinations is offering mostly empty ones.
            "grid": int(n_a * n_b), "realised": len(rows),
            "realised_100": int((df["n"] >= 100).sum()),
            "realised_1000": int((df["n"] >= 1000).sum()),
            "rows": rows[:MAX_ROWS], "truncated": max(0, len(rows) - MAX_ROWS),
            "descriptive_only": True}


def build_router():
    from fastapi import APIRouter, HTTPException                      # noqa: PLC0415

    router = APIRouter(prefix="/api/studio/describe", tags=["studio-describe"])

    @router.get("/columns")
    def _columns():
        try:
            return columns()
        except Exception as e:                                        # noqa: BLE001
            raise HTTPException(500, detail=str(e))

    @router.post("")
    def _describe(req: DescribeRequest):
        try:
            return describe(col_a=req.col_a, col_b=req.col_b, universe=req.universe,
                            years=req.years, months=req.months,
                            min_price=req.min_price, max_price=req.max_price)
        except DescribeError as e:
            raise HTTPException(400, detail=str(e))
        except Exception as e:                                        # noqa: BLE001
            raise HTTPException(500, detail=str(e))

    return router
