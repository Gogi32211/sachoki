# QR_RSI_V1 — buying after a Q∧R bar with RSI14 > 50

**Date** 2026-09-27 · plan approved ("ki") · frozen to `qr_rsi_plan.txt` before any outcome · k = 2.
**Verdict: long NULL (stopped at the deciding test). Veto mirror not reached.** RSI > 50 makes the Q∧R bar *worse*, not better, but by less than the veto bar.

## Setup
- **Entry.** Open after the Q∧R bar (dry return into a broken echo zone).
- **Comparison.** A random buy (control_keys) on the same day with the same exit.
- **Exit.** Book path-sim (ATR×12 trail, maxh 60, slip 0.15 %); the 5/15 bracket is shown descriptively.
- **Universe.** ≥ $1, ≥ $2M.
- **Windows.** MINE 2021-23. VERIFY was not opened.

## MINE 2021-2023

| cell | trades / days | mean · median · win | Δ vs random buy (pp) | by year | 5/15 Δ |
|---|---|---|---|---|---|
| **Q∧R & RSI > 50** | 10,367 / 600 | −3.04 % · −2.78 % · 44 % | **−0.88** [−1.62, −0.16] | −0.64 / −1.61 / −0.28 | +0.15 |
| Q∧R & RSI ≤ 50 | 12,387 / 618 | −1.07 % · −1.33 % · 47 % | −0.41 [−1.07, +0.25] | −1.35 / +0.07 / −0.43 | −0.01 |
| *reference* Q∧R all | 21,621 / 621 | −1.96 % · −1.98 % · 45 % | −0.34 [−0.86, +0.20] | | +0.11 |

The random buy returned −1.69 % mean and 45 % wins over MINE, which includes the 2022 bear.

- **Long:** no cell reaches +0.5, so the study stops.
- **Veto mirror:** RSI > 50 is negative in 3 of 3 years with a CI below zero, but −0.88 does not reach the −1.0 bar. **Not a veto.**
- **VERIFY 2024-26 was not opened.**

## Reading
- **Adding RSI > 50 turns a flat bar (−0.34) into a mildly harmful one (−0.88).** Buying a dry return into a broken zone when momentum is already in the upper half is buying strength into supply.
- **This matches the book's two laws:** "fade strength / buy absorbed weakness", and "RSI dominates". RSI ≤ 50 is the less bad half, but still not a buy.
- **The Q∧R family is now well covered.**
  - Q∧R alone: ≈ 0.
  - Q∧R → ▲1: VETO confirmed (QR_REL_V1).
  - Q∧R + RSI > 50: mildly negative, below the veto bar.
  - **None is a long entry.**

Artifacts: session scratchpad `vecho/qr_rsi_plan.txt`, `qr_rsi.py`, `qr_rsi.log`. No UI, score, or memory change.
