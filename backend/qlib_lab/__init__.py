"""
qlib_lab — lean "qlib-style" factor research over the Studio DuckDB.

Trains ML models (LightGBM by default) on the precomputed point-in-time signal
columns in `bars` and measures whether they predict the next-bar forward return
(IC / Rank IC / ICIR + feature importance), with a strict time-ordered split.

This is the DEFAULT engine: DuckDB -> pandas -> LightGBM, no Microsoft qlib
dependency. The `models.py` registry is pluggable so a real qlib `LGBModel`
backend can be slotted in later without restructuring the API or the UI.

HARD RULE (enforced in columns.py): forward-return columns (fwd_*, mfe_*, mae_*,
hit_*, drop_*, fwd_swing_*) are OUTCOME labels and can never be features. The
label is built from price only, one bar AFTER the signal (no look-ahead).
"""
