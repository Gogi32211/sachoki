# QVOL_GAP_V1: Q-tab volume marks and TRUE-gap above T/Z (2026-10-04)

## Set-up

- **Plan:** approved ("ki"). Script `backend/research_qvol_gap.py`, raw output `QVOL_GAP_V1.json`.
- **Contrasts:** k = 7, fixed in advance.
- **Tokens:** come from `studio.q_sequence._base_sql`, the same SQL the 🔷 Q tab runs (100 % parity with the Pine).
- **Return:** registered book return (entry open[t+1], trail clip(12·ATR%, 15, 60), maxh 60, 15 bps). The vectorised path-sim was re-checked against `_bar_returns` on 3,000 bars.
- **Pairing:** group vs rest within day × ATR%-bin × **T/Z state**, plus V level (V1, V2, V5) or gap class (G1, G2). Day bootstrap.
- **Universe:** close ≥ $5, 20-day dollar volume ≥ $5M.

## Result

Δ = group − rest, book return in pp, with 95 % CI.

| # | contrast | n g / rest | 2021-23 | 2024-26 | $21-89 | years | PASS |
|---|---|---|---|---|---|---|---|
| V1 | ▲ vs ● | 15k / 1.46M | −0.59 [−2.77, +1.56] | +0.07 [−3.02, +3.49] | −1.51 | −0.5 −2.9 +1.8 +1.7 −2.0 +1.0 | ✗ |
| V2 | ▼ vs ● | 200k / 1.46M | **+0.33 [+0.08, +0.59]** | −0.03 [−0.40, +0.35] | +0.23 | +0.6 +0.2 +0.3 −0.1 +0.0 +0.1 | ✗ |
| **V3** | **↑V (V ≥ 4 on Q1-Q4) vs Q1-Q4 V ≤ 3** | 226k / 1.18M | **−0.47 [−0.79, −0.15]** | **−0.45 [−0.82, −0.08]** | −0.26 | −0.6 −0.3 −0.6 −0.3 −0.5 −0.7 | **✓ (negative)** |
| V4 | ↓V (V ≥ 4 on Q5-Q8) vs Q5-Q8 V ≤ 3 | 223k / 1.14M | +0.08 [−0.34, +0.45] | **−0.79 [−1.13, −0.45]** | −0.48 | +0.4 +0.6 −0.6 −0.3 −1.6 −0.1 | ✗ |
| V5 | ↑v vs ↓v (same level) | 522k / 572k | +0.01 [−0.20, +0.21] | −0.18 [−0.43, +0.05] | −0.09 | ≈ 0 | ✗ |
| G1 | gap ↑ with TRUE class < class vs TRUE = class | 180k / 208k | +0.03 [−0.91, +0.93] | +0.39 [−0.41, +1.11] | +0.25 | | ✗ |
| G2 | gap ↓, same | 165k / 189k | +0.62 [−0.52, +1.84] | +0.76 [−0.28, +1.90] | +0.66 | +0.6 +0.6 +0.7 +0.7 +1.4 −0.2 | ✗ |

The secondary 10-bar win rate shows no replicated cell either. V1 is −4.18 in 2021-23 but +0.92 in 2024-26; V2 is −0.06 then +0.82.

## Reading

**V3 is the only PASS, in the negative direction.**

- **Size:** an up-arrangement bar (Q1-Q4) on high volume (V ≥ 4, ≥ 1.5× the 20-bar median) does about −0.45 pp per trade worse than the same T/Z state on the same day and ATR%-bin with normal volume.
- **Robustness:** negative in 6 / 6 years and in $21-89.
- **Bonferroni (k = 7):** survives 2021-23 (upper ≈ −0.03) but not 2024-26 (upper ≈ +0.06).
- **What it really is:** by construction ↑V is the V level restricted to up bars. This is a confirmation, inside T/Z, of the known law in `project_volume_magnitude` and `project_confluence_laws` (effort on strength fails; extreme volume ↓), not new information from the mark itself. The effect is small, suitable as a context or veto hint and not as a signal.

**Everything else is NULL:**

- the median-vs-σ disagreement marks (▲ ▼);
- same-level volume up or down (↑v ↓v);
- the TRUE-gap correction (G2 gap-down leans positive in 5 / 6 years, but both CIs include 0);
- ↓V on down bars (significant only in 2024-26).

## Conclusion

The Q tab's extra lines add no new information beyond T/Z plus V level. The single surviving effect, "high volume on an up bar = slightly worse", is an already-known law.
