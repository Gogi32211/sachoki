# RS_DECOMP_V1 — is the 🔻💪 / 🕐DR gain on T1 / T3 / T6 just RS?

**Date** 2026-09-29 · user: "rsi testi gaakete samiveze t1, t6, t3". Plan frozen to `rsdecomp_plan.txt` before any outcome.
**Setup**
- **RS** = 🏆rs (`shape_rs`: close/SPY > EMA200 of the ratio).
- **Primary k = 9:** 3 states × 3 contrasts. All are paired within day × ATR% on the book per-bar return; the PASS rule is as in T3_BOTTOM_V1.

**Sanity check.** Only 70-76 % of 🔻💪 bars carry 🏆rs.
- The anatomy RS uses a 120-bar warm-up; `shape_rs` uses 200 bars.
- So C1 is "🔻💪 vs RS-without-any-bottom", not an exact RS-matched pair.

## VERDICT: 1 / 9 PASS — 🕐DR adds to RS on T6. The 🔻💪 gain is mostly RS: the bottom structure adds a little, not significantly. RS itself works only from 2023 on.

| state | C1 🔻💪 vs 🏆rs without bottom | C2 🏆rs vs no RS | C3 🕐DR vs 🏆rs without 🕐DR |
|---|---|---|---|
| T1 | +1.32 [−0.51, 3.07] / +0.62 [−1.28, 2.41] · 4/5 yrs | −1.53 / **+1.96 [0.81, 3.21]** · 2022 −5.10 | +1.33 / +1.38 (CIs cross 0) |
| T3 | +0.81 [−0.96, 2.55] / +0.74 [−1.17, 2.58] · 3/5 yrs | +0.25 / **+2.42 [1.31, 3.57]** · 2022 −1.73 | +0.25 / **+3.93 [1.20, 6.86]** · 2025 +8.66 carries it |
| T6 | +0.54 / **+3.00 [0.77, 5.24]** · 5/5 yrs | −0.48 / **+2.20 [0.86, 3.60]** · 2022 −1.12 | **+5.99 [2.93, 9.00] / +5.42 [2.09, 9.06] · 5/5 yrs → PASS** |

Each cell reads MINE / VERIFY, in pp.

**Descriptive: C4, 🔻 without RS vs no RS and no bottom**

| state | MINE | VERIFY |
|---|---|---|
| T1 | +1.04 | +0.27 |
| T3 | +1.50 | +0.33 |
| T6 | +1.97 | −1.18 |

A bottom without RS was fine in 2021-23 and has faded or turned negative since 2024.

## Reading
1. **RS carries most of it, and RS is regime-dependent.**
   - Inside T1, T3 and T6, 🏆rs is worth +2.0…+2.4 pp in 2024-26.
   - It was negative in 2022, the bear year: T1 −5.10, T3 −1.73, T6 −1.12.
   - The book's "RS rescues" law is therefore a 2023-26 phenomenon on these bars.
2. **The bottom structure adds only a small, unproven increment over RS.**
   - C1 is positive in all 6 point estimates (+0.5…+3.0).
   - Only T6 VERIFY clears 0.
   - T3 + 🔻💪 (the T3_BOTTOM_V1 pass) is therefore mostly RS plus a small bottom increment.
3. **🕐DR is more than RS.**
   - On T6 it adds +5.4…+6.0 over RS-only T6 in both windows, 5/5 years.
   - On T3 it adds +3.93 in 2024-26 only.
   - The 1H dual-reclaim confirmation is the informative part.

**Caveats**
- 🕐DR on T6 is small: 276 / 280 trades on 85 / 108 days.
- Its $21-89 VERIFY value is only +0.24.
- 2021 is excluded (warm-up).

No build, no memory entry. Artifacts (scratchpad `v4hist/`): `rsdecomp_plan.txt`, `rsdecomp.py`, `rsdecomp.log`, `rsdecomp_result.json`.
