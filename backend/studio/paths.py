"""
studio/paths.py — single source of truth for on-disk data locations.

Everything the app persists (DuckDB analytics DBs) lives under ONE data directory
so the whole project is self-contained and portable: copy the sachoki-desktop
folder to another machine and it just works.

  DATA_DIR resolves to (first that is set):
    1. env  SACHOKI_DATA_DIR        (override — e.g. point at an external disk)
    2. <project-root>/data          (default — keeps data inside the repo folder)

Lightweight on purpose (only stdlib) so any script can import it cheaply.

THIS MODULE IS NON-MUTATING. It defines paths and nothing else: no mkdir, no touch, no
database open, no file creation — at import time or otherwise (export_path is the one
exception, and it creates only the exports directory, never DATA_DIR, and only when
called). This is a rule, not an accident, and it is enforced by fixture G in
mount_guard_conformance.py.

It used to call os.makedirs(DATA_DIR, exist_ok=True) here at import. That line was
removed. DATA_DIR is now a symlink to an external volume, and a mkdir on the canonical
path is the one thing that can MANUFACTURE the failure it looks like it is preventing:
with the volume absent but /Volumes/QUANT_RESEARCH present as an ordinary folder, that
call would cheerfully create the tree on the internal disk and every subsequent write
would land in a phantom that looks exactly like the real thing.

Creating canonical directories is a writer's job, and a writer must clear the mount guard
first. Importing a path constant is not a writer, and must stay free to happen anywhere —
including in read-only modules and tests running with no external disk attached.
"""
from __future__ import annotations
import os

# backend/studio/paths.py  →  <project-root>
_HERE = os.path.dirname(os.path.abspath(__file__))          # .../backend/studio
BACKEND_DIR = os.path.dirname(_HERE)                        # .../backend
PROJECT_ROOT = os.path.dirname(BACKEND_DIR)                 # .../sachoki-desktop

DATA_DIR = os.environ.get("SACHOKI_DATA_DIR") or os.path.join(PROJECT_ROOT, "data")
# NO mkdir here — see the module docstring. Writers create directories, after the guard.


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
