"""
studio/event_detector.py — Detect outcome events from the bars table.

An "event" is a bar after which a specific price outcome occurred
(e.g. stock grew 2× in 60 days, or dropped -20% in 10 days, or
our turbo signal fired but price fell).

Events are written to the `events` table in DuckDB.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional

import pandas as pd

from studio.db import get_conn

log = logging.getLogger(__name__)


# ── Preset event definitions ───────────────────────────────────────────────────
PRESET_EVENTS: dict[str, dict] = {
    "BULL_20PCT_5D":  {"mfe_col": "mfe_5d",  "threshold": 20.0,  "direction": "up",   "label": "+20% in 5d"},
    "BULL_30PCT_10D": {"mfe_col": "mfe_10d", "threshold": 30.0,  "direction": "up",   "label": "+30% in 10d"},
    "BULL_50PCT_20D": {"mfe_col": "mfe_20d", "threshold": 50.0,  "direction": "up",   "label": "+50% in 20d"},
    "BULL_2X_60D":    {"mfe_col": "mfe_60d", "threshold": 100.0, "direction": "up",   "label": "×2 in 60d"},
    "BULL_3X_90D":    {"mfe_col": "mfe_60d", "threshold": 200.0, "direction": "up",   "label": "×3 in 90d"},
    "BEAR_DROP_20D":  {"mae_col": "mae_10d", "threshold": -20.0, "direction": "down", "label": "-20% in 10d"},
    "BEAR_DROP_30D":  {"mae_col": "mae_20d", "threshold": -30.0, "direction": "down", "label": "-30% in 20d"},
    # Signal-based
    "SIGNAL_CATCH":   {"turbo_min": 50, "fwd_col": "fwd_5d", "fwd_min": 8.0,   "label": "turbo≥50 → +8% in 5d"},
    "FALSE_POS":      {"turbo_min": 50, "fwd_col": "fwd_10d","fwd_max": -10.0,  "label": "turbo≥50 → -10% in 10d"},
    "MISS":           {"turbo_max": 15, "mfe_col": "mfe_20d", "threshold": 40.0,"label": "no signal → +40% in 20d"},
}


@dataclass
class EventFilter:
    """Parameters for event detection."""
    event_type: str                          # preset key or 'custom'
    universes: list[str] = field(default_factory=lambda: ["sp500", "nasdaq"])
    date_from: Optional[str] = None
    date_to:   Optional[str] = None
    # Custom overrides (only used if event_type == 'custom')
    custom_name: Optional[str] = None
    mfe_col:     Optional[str] = None
    mfe_min:     Optional[float] = None
    mae_col:     Optional[str] = None
    mae_max:     Optional[float] = None
    fwd_col:     Optional[str] = None
    fwd_min:     Optional[float] = None
    fwd_max:     Optional[float] = None
    turbo_min:   Optional[float] = None
    turbo_max:   Optional[float] = None
    price_min:   Optional[float] = None
    price_max:   Optional[float] = None
    volume_min:  Optional[int]   = None


def _build_where(f: EventFilter, preset: dict | None) -> tuple[str, list]:
    """Build SQL WHERE clause + params from EventFilter."""
    clauses: list[str] = []
    params:  list       = []

    # Universe filter
    if f.universes:
        placeholders = ", ".join("?" * len(f.universes))
        clauses.append(f"universe IN ({placeholders})")
        params.extend(f.universes)

    # Date range
    if f.date_from:
        clauses.append("date >= ?"); params.append(f.date_from)
    if f.date_to:
        clauses.append("date <= ?"); params.append(f.date_to)

    # Price / volume filters
    if f.price_min is not None:
        clauses.append("close >= ?"); params.append(f.price_min)
    if f.price_max is not None:
        clauses.append("close <= ?"); params.append(f.price_max)
    if f.volume_min is not None:
        clauses.append("volume >= ?"); params.append(f.volume_min)

    # Resolve MFE/MAE/FWD/TURBO from preset or from filter
    p = preset or {}
    mfe_col   = f.mfe_col   or p.get("mfe_col")
    mfe_min   = f.mfe_min   if f.mfe_min   is not None else p.get("threshold") if p.get("direction") == "up"   else None
    mae_col   = f.mae_col   or p.get("mae_col")
    mae_max   = f.mae_max   if f.mae_max   is not None else p.get("threshold") if p.get("direction") == "down" else None
    fwd_col   = f.fwd_col   or p.get("fwd_col")
    fwd_min   = f.fwd_min   if f.fwd_min   is not None else p.get("fwd_min")
    fwd_max   = f.fwd_max   if f.fwd_max   is not None else p.get("fwd_max")
    turbo_min = f.turbo_min if f.turbo_min is not None else p.get("turbo_min")
    turbo_max = f.turbo_max if f.turbo_max is not None else p.get("turbo_max")

    if mfe_col and mfe_min is not None:
        clauses.append(f"{mfe_col} >= ?"); params.append(mfe_min)
    if mae_col and mae_max is not None:
        clauses.append(f"{mae_col} <= ?"); params.append(mae_max)
    if fwd_col and fwd_min is not None:
        clauses.append(f"{fwd_col} >= ?"); params.append(fwd_min)
    if fwd_col and fwd_max is not None:
        clauses.append(f"{fwd_col} <= ?"); params.append(fwd_max)
    if turbo_min is not None:
        clauses.append("turbo_score >= ?"); params.append(turbo_min)
    if turbo_max is not None:
        clauses.append("turbo_score <= ?"); params.append(turbo_max)

    # Must have non-null MFE/MAE values
    for col in [mfe_col, mae_col]:
        if col:
            clauses.append(f"{col} IS NOT NULL")

    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params


def detect_events(f: EventFilter, clear_existing: bool = True) -> dict:
    """
    Detect events matching the filter, write to `events` table.
    Returns summary dict.
    """
    preset = PRESET_EVENTS.get(f.event_type) if f.event_type != "custom" else None
    event_type = f.custom_name if f.event_type == "custom" else f.event_type

    where, params = _build_where(f, preset)

    sql = f"""
        SELECT
            ticker, date, close, universe, turbo_score,
            mfe_60d, fwd_30d, fwd_60d
        FROM bars
        WHERE {where}
        ORDER BY ticker, date
    """

    conn = get_conn()
    try:
        rows = conn.execute(sql, params).fetchdf()

        if clear_existing:
            conn.execute("DELETE FROM events WHERE event_type = ?", [event_type])

        if len(rows) == 0:
            conn.commit()
            return {"event_type": event_type, "total_events": 0}

        rows["event_type"]  = event_type
        rows["tags"]        = "[]"
        rows["notes"]       = ""

        # Rename to match events table
        rows = rows.rename(columns={
            "date":         "event_date",
            "close":        "close_price",
            "turbo_score":  "turbo_at_event",
        })

        # Insert
        conn.register("_ev_df", rows)
        conn.execute("""
            INSERT INTO events
              (ticker, event_date, event_type, close_price, mfe_60d,
               fwd_30d, fwd_60d, universe, turbo_at_event, tags, notes)
            SELECT
              ticker, event_date, event_type, close_price, mfe_60d,
              fwd_30d, fwd_60d, universe, turbo_at_event, tags, notes
            FROM _ev_df
        """)
        conn.commit()

        # Summary stats
        import math
        total = len(rows)
        by_universe = rows.groupby("universe").size().to_dict()
        avg_mfe60 = rows["mfe_60d"].mean() if "mfe_60d" in rows else None

        def _safe(v):
            """Convert value to JSON-safe scalar."""
            if v is None:
                return None
            try:
                fv = float(v)
                return None if (math.isnan(fv) or math.isinf(fv)) else round(fv, 2)
            except (TypeError, ValueError):
                return str(v)[:20] if hasattr(v, '__str__') else None

        top10 = []
        if "mfe_60d" in rows:
            for _, r in rows.nlargest(10, "mfe_60d")[["ticker","event_date","mfe_60d","fwd_30d"]].iterrows():
                top10.append({
                    "ticker":     str(r["ticker"]),
                    "event_date": str(r["event_date"])[:10],
                    "mfe_60d":    _safe(r["mfe_60d"]),
                    "fwd_30d":    _safe(r["fwd_30d"]),
                })

        return {
            "event_type":   event_type,
            "total_events": total,
            "by_universe":  {str(k): int(v) for k, v in by_universe.items()},
            "avg_mfe_60d":  round(float(avg_mfe60), 1) if avg_mfe60 and not math.isnan(float(avg_mfe60)) else None,
            "top_movers":   top10,
        }
    finally:
        conn.close()


def get_events_summary() -> dict:
    """Return aggregate stats about all events in the table."""
    conn = get_conn(read_only=True)
    try:
        by_type = conn.execute(
            "SELECT event_type, COUNT(*) as n FROM events GROUP BY event_type ORDER BY n DESC"
        ).fetchdf().to_dict("records")
        total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        return {"total": total, "by_type": by_type}
    finally:
        conn.close()


def list_events(
    event_type: str | None = None,
    universe:   str | None = None,
    limit:      int = 200,
    offset:     int = 0,
) -> list[dict]:
    """Paginated event listing."""
    clauses, params = [], []
    if event_type:
        clauses.append("event_type = ?"); params.append(event_type)
    if universe:
        clauses.append("universe = ?");   params.append(universe)
    where = " AND ".join(clauses) if clauses else "1=1"
    sql = f"""
        SELECT id, ticker, event_date, event_type, close_price,
               mfe_60d, fwd_30d, universe, turbo_at_event
        FROM events
        WHERE {where}
        ORDER BY mfe_60d DESC NULLS LAST
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    import math
    conn = get_conn(read_only=True)
    try:
        df = conn.execute(sql, params).fetchdf()
        records = []
        for rec in df.to_dict("records"):
            clean = {}
            for k, v in rec.items():
                if hasattr(v, 'isoformat'):          # Timestamp → string
                    clean[k] = str(v)[:10]
                elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    clean[k] = None
                else:
                    clean[k] = v
            records.append(clean)
        return records
    finally:
        conn.close()
