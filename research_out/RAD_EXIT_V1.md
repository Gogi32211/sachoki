# RAD_EXIT_V1 — what follows the end of an RA·D streak

**Date** 2026-09-28 · user: "after RA·D or its repetition, does RN·U or another signal cause lift?" · plan frozen to `radexit_plan.txt` before any outcome · k = 2 + 10.
**Verdict:**
- **H1 and H2 (the regime flip to ·U / RN·U) FAIL.**
- **Of the mined signals, 5/10 pass.** All five are signals the book already knows: 🧊CF, 🏆rs, 🕐DR, G3², G3L46. Nothing specific to "after RA·D" was found.

## Setup
- **EXIT bar.** RA·D on t-1 and not on t: 228k bars, 5,739 tickers.
- **Primary outcome.** Same-day return Δ vs control, ATR×12 trail. The pooled comparison is against the rest of the EXIT bars.

## H1 / H2

| | MINE same-day · vs rest of EXIT | VERIFY same-day · vs rest of EXIT |
|---|---|---|
| EXIT (all) | −0.01 | −0.35 |
| H1 EXIT → ·U regime (RA·U / RN·U / RF·U) | −0.06 · **−1.55** [−2.68, −0.57] | −0.50 · −0.63 |
| H2 streak ≥ 2 → RN·U | −0.02 · −1.05 | +0.44 · −0.28 |

**Mean return by the prior streak length L:**

| window | L = 1 | L = 2 | L = 3 | L = 4+ |
|---|---|---|---|---|
| MINE | −0.73 % | −0.48 % | −1.08 % | **−2.40 %** |
| VERIFY | +2.51 % | +2.68 % | +2.61 % | **−0.28 %** |

After a 4+ bar RA·D run, even the exit bar is a poor entry. This agrees with RAD_STREAK_V1.

## Mining: the key on the EXIT bar, EXIT∧key − EXIT∧¬key
- **Search.** 444 keys evaluated; 84 passed the MINE filter; the top 10 went to VERIFY.

| key on the EXIT bar | MINE | VERIFY diff [CI] · same-day | |
|---|---|---|---|
| 🧊CF coil-floor absorption | +4.49 | **+2.58 [+0.95, +4.26]** · +1.14 | PASS |
| 🏆rs | +4.29 | **+1.96 [+1.26, +2.70]** · +1.05 | PASS |
| 🕐DR | +4.17 | **+1.45 [+0.09, +2.79]** · +1.82 | PASS |
| G3² gap-chain | +3.95 | **+5.21 [+0.66, +9.21]** · +0.73 (n 423) | PASS |
| G3L46 chain | +3.78 | **+5.97 [+1.02, +10.74]** · +0.36 (n 601) | PASS |
| P<200, ⚛K2D, EDGES any, FRI64 | +3.6…+4.2 | −0.8…+1.2, same-day ≤ 0 | fail |

## Reading
- **The regime flip itself (RA·D → RN·U or → ·U) is not a lift signal.** H1 is even worse than the other exits in 2021-23.
- **What lifts a bar after RA·D is what lifts any bar:** the validated edges (🧊CF, 🕐DR, G3², G3L46) and 🏆 relative strength. These are the same ones the book already uses.
- This study did not show that RA·D-exit context makes them *better than they are elsewhere*. That would be a separate comparison.
- **The consistent new fact is negative.** A long RA·D run (4+) spoils the next bars, including the exit bar. It supports the RA·D≥3/4 veto candidate from RAD_STREAK_V1.

Artifacts (session scratchpad `v4hist/`): `radexit_plan.txt`, `radexit.py`, `radexit.log`. No UI, score or memory change.
