# BDZ5L12_V1 — clean test of BD▼ × Z5·L12 (the one recurring VOL_ECHO cell)

**Date** 2026-09-25 · the user asked for this test ("Seamowme") · plan frozen to `bdz5_plan.txt` before any outcome · k = 1 new family.
**Verdict: NULL — the cell does not replicate. The VOL_ECHO long family is closed.**

## Why this cell
BD▼ × Z5·L12 was the top cell in both VOL_ECHO_LONG_V1 (+1.33, MINE 2021-23) and VOL_ECHO_LONG_2326 (+1.02, MINE 2023-24). Both failed DSR. It was chosen as the best of 274 + 286 cells, on data that both contain 2023. The only clean data for it is 2025-01 → 2026-08, which had never been opened for it.

The bar is a red candle that opens and closes below the echo zone. It is higher than the previous bar (Z5), and its volume is lower than the previous bar's (L12).

## Registered bar
- Median ≥ +0.5 pp, CI lower > 0.
- 2025 and 2026 both positive.
- Beats the rest of BD▼ (Δ CI lower > 0).
- Method identical to the family.

## Result (≥ $8, 2025-01-02 → 2026-08-26)

| | value |
|---|---|
| n / days | 409 / 203 |
| median 20-day excess | **+0.15** [−0.42, +1.26] |
| 2025 / 2026 | +0.48 / −0.03 |
| Δ vs rest of BD▼ | +0.04 [−0.64, +1.19] |
| mean · win % · 5-day median | +0.30 · 51.1 % · +0.26 |

**It fails on every gate.** It is no different from any other BD▼ bar, and the edge seen in 2021-23 and 2023-24 is gone.

## Descriptive only (not gates)
- **$21-89:** n 186, median +1.05.
- **<$8:** n 156, median +2.50.

Both are sub-segments chosen after the fact, and <$8 is the lottery zone. Chasing them would repeat the selection that produced this cell. They are recorded, not pursued.

## Reading
- **This is a textbook selection effect:** the best of about 560 cells, strong in-sample, gone out of sample.
- **All 260925_VOL_ECHO signals are closed as long entries** in both windows. They remain a descriptive chart tool.

Artifacts: session scratchpad `vecho/bdz5_plan.txt`, `bdz5.py`, `bdz5.log`. No UI, score, or memory change.
