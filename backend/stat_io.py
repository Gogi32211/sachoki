"""
stat_io.py — shared I/O helpers for the TZ/WLNBB stock_stat data surface.

ULTRA Stage 2 writes a subset file once and feeds it to four readers
(tz_wlnbb, tz_intelligence, pullback, rare_reversal). Historically every
reader called `csv.DictReader` independently, which meant:

  • 4× re-reading of a 100+MB file from disk
  • CSV's text-parse cost paid four times
  • 4× memory duplication of the same row-dict structure

This module provides:

  read_stat_as_df(path)            — autodetect .parquet vs .csv
  df_to_string_rows(df)            — CSV-compatible string rows preserving
                                     downstream `row.get("close") or 0`
                                     semantics (NaN → "")
  group_rows_by_ticker(rows, …)    — the common grouping the 4 readers do
"""
from __future__ import annotations

import csv as _csv
import os as _os
from typing import Any, Iterable

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Path / format helpers
# ─────────────────────────────────────────────────────────────────────────────

def is_parquet_path(path: str) -> bool:
    return bool(path) and path.lower().endswith(".parquet")


def read_stat_as_df(path: str) -> pd.DataFrame:
    """Load a stock_stat file into a DataFrame. Autodetects parquet vs CSV
    by extension. Raises FileNotFoundError if the file is missing."""
    if not _os.path.exists(path):
        raise FileNotFoundError(path)
    if is_parquet_path(path):
        return pd.read_parquet(path)
    # CSV: keep dtype=object so we preserve CSV string semantics for callers
    # that still rely on `float(row.get("x") or 0)` lenient parsing.
    return pd.read_csv(path, dtype=object, keep_default_na=False, na_values=[])


# ─────────────────────────────────────────────────────────────────────────────
# Row materialisation
# ─────────────────────────────────────────────────────────────────────────────

def df_to_string_rows(df: pd.DataFrame) -> list[dict[str, str]]:
    """Materialise a DataFrame as list-of-string-dicts so downstream readers
    (which originally consumed `csv.DictReader`) keep their string-coerce
    semantics: NaN → "", True → "True", 1.5 → "1.5", etc.

    The string-coerce keeps `float(row.get("x") or 0)` working: "" is
    falsy → falls through to 0; "0.5" parses fine.
    """
    if df is None or df.empty:
        return []

    # Build a string view column-by-column — cheaper than per-cell Python loops.
    out_cols: dict[str, list[str]] = {}
    for col in df.columns:
        s = df[col]
        kind = s.dtype.kind
        if kind == "b":
            out_cols[col] = s.map({True: "True", False: "False"}).fillna("").tolist()
        elif kind in ("f", "i", "u"):
            # Cast to object, replace NaN with "", then stringify
            obj = s.astype(object).where(s.notna(), "")
            out_cols[col] = [("" if v == "" else str(v)) for v in obj]
        elif kind == "M":  # datetime64
            out_cols[col] = s.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("").tolist()
        else:
            out_cols[col] = s.fillna("").astype(str).tolist()

    cols = list(out_cols.keys())
    n = len(df)
    rows: list[dict[str, str]] = [
        {c: out_cols[c][i] for c in cols} for i in range(n)
    ]
    return rows


def group_rows_by_ticker(
    rows: Iterable[dict[str, Any]],
    universe_filter: str | None = None,
    sort_by_bar: bool = True,
) -> dict[str, list[dict[str, Any]]]:
    """Group rows by ticker. Optionally drop rows whose `universe` field
    doesn't match (used by tz_wlnbb latest extraction). Sorts each group
    by `bar_datetime` (or `date` fallback) ascending."""
    by_ticker: dict[str, list[dict]] = {}
    for row in rows:
        t = (row.get("ticker") or "").strip()
        if not t:
            continue
        if universe_filter is not None:
            u = row.get("universe", "")
            if u and u != universe_filter:
                continue
        by_ticker.setdefault(t, []).append(row)
    if sort_by_bar:
        for lst in by_ticker.values():
            lst.sort(key=lambda r: r.get("bar_datetime") or r.get("date", ""))
    return by_ticker


# ─────────────────────────────────────────────────────────────────────────────
# CSV → parquet conversion (used by ULTRA orchestrator after subset gen)
# ─────────────────────────────────────────────────────────────────────────────

def convert_csv_to_parquet(csv_path: str, parquet_path: str) -> int:
    """Read a CSV file, write parquet, return row count. Caller decides
    whether to delete the source CSV afterwards."""
    df = pd.read_csv(csv_path, dtype=object, keep_default_na=False, na_values=[])
    df.to_parquet(parquet_path, index=False, compression="snappy")
    return len(df)


def count_rows(path: str) -> int:
    """Cheap row count for either CSV or parquet."""
    if is_parquet_path(path):
        return pd.read_parquet(path, columns=[]).shape[0] if _os.path.exists(path) else 0
    n = 0
    if not _os.path.exists(path):
        return 0
    with open(path, newline="", encoding="utf-8") as f:
        for _ in _csv.DictReader(f):
            n += 1
    return n
