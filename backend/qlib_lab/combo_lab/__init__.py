"""
qlib_lab/combo_lab — systematic combo discovery with walk-forward + multiple-
testing discipline. Output: a combo_catalog with statistically supported setups,
best entry/exit per setup, ready for AI Journal to use as anchored evidence.

Phases:
  1. enumerate.py — singles / pairs / triples of validated predicates (+ context
     slices: RSI/mcap/phase) → one DuckDB conditional-aggregate pass.
  2. backtest.py  — walk-forward: train (2021-2024) → OOS (2025-2026). Honest
     baseline + Bonferroni correction. Pass if OOS confirms train edge.
  3. exits.py     — for each passing combo, grid-search ATR-stop/target/time-stop
     on train, validate on OOS, store best in combo_catalog.
"""
