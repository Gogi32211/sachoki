"""
studio/paths.py — single source of truth for on-disk data locations.

Everything the app persists (DuckDB analytics DBs) lives under ONE data directory
so the whole project is self-contained and portable: copy the sachoki-desktop
folder to another machine and it just works.

  DATA_DIR resolves to (first that is set):
    1. env  SACHOKI_DATA_DIR        (override — e.g. point at an external disk)
    2. <project-root>/data          (default — keeps data inside the repo folder)

Lightweight on purpose (only stdlib) so any script can import it cheaply.
"""
from __future__ import annotations
import os

# backend/studio/paths.py  →  <project-root>
_HERE = os.path.dirname(os.path.abspath(__file__))          # .../backend/studio
BACKEND_DIR = os.path.dirname(_HERE)                        # .../backend
PROJECT_ROOT = os.path.dirname(BACKEND_DIR)                 # .../sachoki-desktop

DATA_DIR = os.environ.get("SACHOKI_DATA_DIR") or os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)


def db_path(name: str) -> str:
    """Absolute path to a DuckDB file in DATA_DIR.
    Accepts a full filename ('studio_1w.duckdb') or a timeframe shorthand
    ('4h' → 'studio_4h.duckdb', '15m_base' → 'studio_15m_base.duckdb')."""
    if not name.endswith(".duckdb"):
        name = f"studio_{name}.duckdb"
    return os.path.join(DATA_DIR, name)


# canonical databases (import these instead of hardcoding ~/Downloads paths)
ANALYTICS_DB = db_path("studio_analytics.duckdb")   # 1D native — the research source of truth
BASE_15M_DB  = db_path("studio_15m_base.duckdb")     # lean 15m OHLCV base → intraday tf
WEEKLY_DB    = db_path("studio_1w.duckdb")           # 1W native

# generated CSV outputs (bulk_export, track exports) and import seed CSVs
EXPORTS_DIR = os.path.join(PROJECT_ROOT, "exports")   # things the user pulls out
SEEDS_DIR   = os.path.join(DATA_DIR, "seeds")          # one-time DB-rebuild import CSVs


def export_path(name: str) -> str:
    """Absolute path for a generated CSV export; ensures EXPORTS_DIR exists."""
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    return os.path.join(EXPORTS_DIR, name)


def seed_path(name: str) -> str:
    """Absolute path for an import seed CSV in the data dir."""
    return os.path.join(SEEDS_DIR, name)
