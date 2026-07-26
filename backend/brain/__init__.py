"""brain/ — the decision brain (in construction, 2026-07-23).

The 9-layer decision model. We are STRONG at layers 0-4 (data, universe, context, candidate
detection, scoring) and MISSING layers 5, 8 (sizing, portfolio-risk) — which is what actually
decides results. This module builds the missing decision-spine on top of the existing detectors.

Current bricks:
  registry.py   — living FINDINGS REGISTRY (knowledge core). Every validated discovery
                  registers here; the decision-spine reads it. This is how new research
                  auto-updates the brain (Layer-9 feedback made concrete).

Planned bricks (order):
  1. sizing.py    — Layer 5: risk-per-trade -> shares via stop distance, R:R gate, coeffs
  2. portfolio.py — Layer 8: exposure, sector/correlation limits, drawdown stops
  3. regime.py    — Layer 2: collapse the many gates into one permission verdict
  4. spine.py     — the orchestrator: regime -> candidate -> disqualify -> size -> portfolio -> DECISION + log
  5. agents/      — narrow LLM agents on judgment nodes only (regime-synth, adversarial critic, calibration)
"""
from . import registry  # noqa: F401
