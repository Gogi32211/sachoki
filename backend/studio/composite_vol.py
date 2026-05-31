"""
studio/composite_vol.py — stored "composite + volume bucket" string.

bars.composite_vol = composite ('·' volume bucket), e.g.  Z2L46NBO·VB

where composite = (t_sig|z_sig) + l_sig + composite_full_suffix  (the same code
the Sequence Builder matches) and the volume bucket (W/L/N/B/VB) is appended
after a '·' separator so the whole thing — candle pattern + volume magnitude —
is one searchable string. Empty when the bar has no composite at all.

The volume bucket already lives in its own `vol_bucket` column (untouched); this
is a convenience/searchable combined field. Computed by one vectorised SQL pass,
recomputed per universe after enrichment in the daily incremental.
"""
from __future__ import annotations

_COMP = ("(COALESCE(NULLIF(t_sig,''), NULLIF(z_sig,'')) || COALESCE(l_sig,'') "
         "|| COALESCE(composite_full_suffix,''))")


def composite_vol_sql() -> str:
    """DuckDB expression for composite_vol (empty if no composite)."""
    return (
        f"CASE WHEN {_COMP} = '' THEN '' ELSE {_COMP} || "
        f"CASE WHEN COALESCE(vol_bucket,'') <> '' THEN '·' || vol_bucket ELSE '' END END"
    )


def apply_composite_vol(universe: str | None = None) -> int:
    """Compute + persist composite_vol for bars (one SQL pass). Needs exclusive
    writer for a full backfill (stop uvicorn); safe per-universe in the pipeline."""
    from studio.db import get_conn
    where = ""
    if universe:
        u = str(universe).strip().lower()
        if u not in ("sp500", "nasdaq", "russell2k"):
            raise ValueError(f"bad universe {universe!r}")
        where = f"WHERE universe = '{u}'"
    conn = get_conn(read_only=False)
    try:
        conn.execute(f"UPDATE bars SET composite_vol = {composite_vol_sql()} {where}")
        n = conn.execute(f"SELECT COUNT(*) FROM bars {where}").fetchone()[0]
        conn.commit()
        return int(n)
    finally:
        conn.close()
