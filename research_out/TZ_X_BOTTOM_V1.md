# TZ_X_BOTTOM_V1 — T/Z state × bottom signal (🔻💪, 🔻, 🌀, 🕐DR) on the same bar

**Date** 2026-09-28 · user: "check other TZ signals combined with 🔻💪, and other signals of this kind" · plan frozen to `tzxbot_plan.txt` before any outcome.
- **Search.** 101 cells on MINE; 63 have ≥ 300 trades.
- **Selection.** 11 passed the MINE filter, which requires beating the anchor by ≥ 0.5 pp with CI > 0. All 11 went to VERIFY once.
- **Verdict: 2/11 PASS.** 🔻💪 + T9, and 🕐DR + Z2G (marginal).

## Anchors alone (same-day Δ vs control, pp)

| anchor | MINE | VERIFY |
|---|---|---|
| 🔻💪 | +1.70 | +0.92 |
| 🔻 | +0.90 | +0.23 |
| 🌀 | +1.09 | +0.19 |
| **🕐DR** | **+2.37** | **+2.35** |

## MINE-selected cells → VERIFY

| cell | MINE Δ [CI] · n | VERIFY Δ [CI] · n | vs anchor in VERIFY | verdict |
|---|---|---|---|---|
| **🔻💪 + T9** | +2.62 [+0.74, +4.49] · 2,232 | **+1.65 [+0.32, +3.03]** · 2,786 | +0.73 above 🔻💪 | **PASS** |
| **🕐DR + Z2G** | +3.32 [+0.81, +6.80] · 1,180 | **+2.41 [+0.66, +4.35]** · 1,177 | +0.06 above 🕐DR | **PASS (marginal)** |
| 🔻💪 + T6 | +3.63 · 873 | +0.16 [−1.70, +2.13] | | fail |
| 🔻💪 + Z5 | +3.40 · 476 | +0.38 | | fail |
| 🔻💪 + T10 | +3.37 · 359 | +1.59 [−2.32, +6.01] | | fail (n small) |
| 🔻💪 + Z2 | +2.71 · 1,147 | +0.24 | | fail |
| 🌀 + Z11 | +2.71 · 1,085 | +0.97 [−0.97, +3.28] | | fail |
| 🌀 + Z5 | +2.02 · 2,575 | +0.27 | | fail |
| 🔻 + T5 / T12 / Z1G | +1.4…+1.7 | +0.13…+0.28 | | fail |

**Multiplicity.** 2 passes from 11 VERIFY tests, at about 0.3 expected false passes: modest evidence.
- **🔻💪 + T9** is the more meaningful result. It adds +0.7 pp over 🔻💪 alone in the unseen window.
  - AMD's turn bar 2026-03-31 was exactly this: T9 · L34 FRI34 BL with 🔻💪.
- **🕐DR + Z2G** mostly rides 🕐DR, which is already +2.35.

**Note on Z2G_BOTTOM_V1.** There, 🔻💪+Z2G on 10-bar lows was +2.6 / +2.15. Here, over all 🔻💪 bars, Z2G is below the anchor (+0.8 in MINE). The earlier slice was specific to low bars, which is why it was post-hoc.

## Next (needs user OK)
Forward-register 🔻💪 + T9 and 🕐DR + Z2G next to BOTTOM_CLUSTER B1/B2, read on or after 2027-03-01.

Artifacts (session scratchpad `v4hist/`): `tzxbot_plan.txt`, `tzxbot.py`, `tzxbot.log`. No UI, score or memory change.

## ADDENDUM_1 — triangle-up anchors (🔺, ▲4H REV, △1H REV), same design, frozen before outcome
**Verdict: 0/8 PASS.**

**Anchors alone (same-day Δ, pp):**

| anchor | MINE | VERIFY |
|---|---|---|
| 🔺 continuation | +0.62 | +0.25 |
| ▲4H REV | +1.07 | +0.30 |
| △1H REV | +0.93 | +0.36 |

**MINE-selected → VERIFY:**

| cell | MINE | VERIFY | note |
|---|---|---|---|
| △1H + T12 | +2.34 | +2.14 [−0.02, +4.26] | nearest miss |
| ▲4H + Z10 | +2.13 | +1.46 [−0.24, +3.49] | |
| 🔺 + T5 | +2.04 | +1.16 [−0.15, +2.40] | |
| 🔺 + T9 | +1.84 | +0.18 | |
| ▲4H + T6 / T1 | | +0.38 / +0.17 | |
| ▲4H + T3 | | −0.73 | |
| **▲4H + Z11** | +1.95 | **−2.61 [−4.11, −1.24]** | sign flip |

**Reading.** The triangle-up signals are weak entries on their own out of sample: +0.25…+0.36 pp. No T/Z state makes them reliably better.
- ▲4H on a Z11 bar is a clear **negative** in 2024-26. This matches "Z11 = abort" again.
- The near misses (△1H+T12, ▲4H+Z10, 🔺+T5) are leads only.
- The search burden across the whole family is now ≈ 170 cells.
