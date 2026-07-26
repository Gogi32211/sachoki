"""
qlib_lab/columns.py — selectable feature columns for the QLIB tab.

Reads the `bars` schema (read-only) and returns the columns that may be used as
model features, grouped by family for the UI picker. Forward-return / outcome
columns are excluded here so they can never reach a model.
"""

from __future__ import annotations

import re
from functools import lru_cache

from studio.db import get_conn

# ── The one rule that matters ────────────────────────────────────────────────
# Any column matching these prefixes is an OUTCOME label (look-ahead). It must
# never appear in the feature picker. The label the model learns is built from
# price separately (see data.py).
FORBIDDEN_RE = re.compile(r"^(fwd_|mfe_|mae_|hit_|drop_|fwd_swing_)")

# Identity / price columns — not features, handled separately.
IDENTITY = {"id", "ticker", "date", "universe", "open", "high", "low", "close", "volume"}

# DuckDB types we treat as numeric (usable directly by LightGBM).
_NUMERIC_TYPES = (
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
    "FLOAT", "DOUBLE", "DECIMAL", "REAL", "BOOLEAN",
)


def _is_numeric(duck_type: str) -> bool:
    t = duck_type.upper()
    return any(t.startswith(n) for n in _NUMERIC_TYPES)


def _family(col: str) -> str:
    """Bucket a column into a UI family. Mirrors how the signals are organised
    in the rest of the app (TZ / Volume / Scores / Wyckoff / Regime / Signals)."""
    c = col.lower()
    if c in ("t_sig", "z_sig", "tz_bull") or c.startswith(("sig_t", "sig_z", "sig_tz")):
        return "TZ"
    if c.startswith(("sig_l", "sig_vol")) or "vol" in c:
        return "Volume"
    if c.endswith("_score") or "score" in c:
        return "Scores"
    if c.startswith("wyc"):
        return "Wyckoff"
    if c.startswith(("rtb_", "beta_", "gog_")) or "regime" in c or "phase" in c or c.endswith("_tier") or c.endswith("_zone"):
        return "Regime / Phase"
    return "Signals"


# Display order for the families in the picker.
FAMILY_ORDER = ["TZ", "Volume", "Scores", "Wyckoff", "Regime / Phase", "Signals"]


@lru_cache(maxsize=8)
def list_columns(universe: str) -> dict:
    """Return selectable feature columns for a universe, grouped by family.

    Shape:
        {
          "universe": "sp500",
          "families": [
            {"family": "TZ", "columns": [
                {"name": "tz_bull", "type": "SMALLINT", "kind": "numeric"},
                {"name": "t_sig",   "type": "VARCHAR",  "kind": "categorical"},
                ...]},
            ...
          ],
          "forbidden": ["fwd_1d", ...],   # surfaced so the UI can explain the rule
        }
    The `universe` arg is only used as a cache key — the column set is identical
    across universes (one shared `bars` schema) but we keep the signature honest.
    """
    con = get_conn(read_only=True)
    try:
        info = con.execute("PRAGMA table_info('bars')").fetchdf()
    finally:
        con.close()

    families: dict[str, list[dict]] = {}
    forbidden: list[str] = []
    for _, row in info.iterrows():
        name = row["name"]
        dtype = str(row["type"])
        if name in IDENTITY:
            continue
        if FORBIDDEN_RE.match(name):
            forbidden.append(name)
            continue
        kind = "numeric" if _is_numeric(dtype) else "categorical"
        families.setdefault(_family(name), []).append(
            {"name": name, "type": dtype, "kind": kind}
        )

    ordered = []
    for fam in FAMILY_ORDER:
        cols = sorted(families.pop(fam, []), key=lambda d: d["name"])
        if cols:
            ordered.append({"family": fam, "columns": cols})
    # any family not in the explicit order (defensive) appended at the end
    for fam in sorted(families):
        ordered.append({"family": fam, "columns": sorted(families[fam], key=lambda d: d["name"])})

    return {
        "universe": universe,
        "families": ordered,
        "forbidden": sorted(forbidden),
    }


def validate_features(features: list[str]) -> list[str]:
    """Raise if any requested feature is a forbidden outcome column or identity
    column. Returns the cleaned list (deduped, order preserved)."""
    bad = [f for f in features if FORBIDDEN_RE.match(f)]
    if bad:
        raise ValueError(
            f"Outcome columns can never be features (look-ahead leak): {bad}"
        )
    ident = [f for f in features if f in IDENTITY]
    if ident:
        raise ValueError(f"Identity/price columns are not features: {ident}")
    seen, out = set(), []
    for f in features:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out
