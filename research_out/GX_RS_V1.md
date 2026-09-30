# GX_RS_V1 — golden cross vs 🏆RS: which one carries the T/Z modifier?

**Date** 2026-09-30 · user-approved ("ki"). Plan frozen to `gxrs_plan.txt` before any outcome.
**Disclosure.** Both measures were seen on this history, so this is a DECOMPOSITION, not a confirmation.
**Pool.** All 24 T/Z states, 2,211,560 bars in ⚛ MARKUP (GC = EMA50 > EMA200) or ⚛ MKDN (DC).
- **Cell shares:** GC∧RS 22.7 %, GC∧¬RS 29.5 %, DC∧RS 3.3 %, DC∧¬RS 44.5 %.
- **Estimand:** book per-bar long return, paired within day × ATR% (22) × state × the other factor, day-clustered.

## VERDICT: C1 PASS · C2 PASS (as pre-registered). The GOLDEN CROSS is the main axis; RS adds a smaller increment, only from 2024 on, and only inside a golden cross.

| test | MINE 2021-23 | VERIFY 2024-26 | years | $21-89 VERIFY |
|---|---|---|---|---|
| **C1 GC vs DC, RS held equal** | **+0.75 [0.49, 1.03]** | **+1.35 [1.10, 1.61]** | 2021 −0.16 · 2022 +0.16 · 2023 +1.84 · 2024 +1.72 · 2025 +1.70 · 2026 −0.08 | +2.07 |
| **C2 RS vs ¬RS, cross held equal** | +0.04 [−0.16, 0.24] | **+0.83 [0.64, 1.02]** | 2022 −0.11 · 2023 +0.14 · 2024 +0.35 · 2025 +1.34 · 2026 +0.76 (no 2021: RS warm-up) | +0.57 |

C2 passes the letter of the rule, but its MINE value is ≈ 0: the RS increment is a 2024-26 phenomenon.

**Descriptive: each cell vs DC∧¬RS** (same day, same ATR, same state)

| cell | MINE | VERIFY | years |
|---|---|---|---|
| **GC∧RS** | +0.92 | **+2.07 [1.72, 2.42]** | 2022 −0.66, 2023-25 +2.0…+2.9, 2026 +0.14 |
| GC∧¬RS | +0.80 | +1.51 [1.25, 1.75] | 2022 +0.43, 2023-25 +1.7…+2.2, 2026 −0.41 |
| DC∧RS | −0.14 | +0.22 [−1.37, 1.76] | mixed (−1.51 … +2.62) |

## Reading
1. **The golden cross does most of the work,** about +1.35 pp in 2024-26 at equal RS. That is larger than RS at equal cross (+0.83) and present in 2021-23 as well.
2. **RS helps only inside a golden cross.** GC∧RS (+2.07) beats GC∧¬RS (+1.51). In a death-cross name RS does **not** rescue: DC∧RS vs DC∧¬RS ≈ 0, n small (3.3 % of bars).
3. **Both are regime-dependent.** Neither helps in 2021-22; both are strong in 2023-25; 2026 is weak so far (C1 −0.08).
4. **Practical ordering for a T/Z long:** GC∧RS > GC∧¬RS > DC (either).

## Next (not done)
- Forward registration of C1 (GC vs DC inside T/Z, ATR-matched) and of the GC∧RS cell is the only clean confirmation.
- **Display:** ⚛ MARKUP/MKDN are already visible as physics tokens, but under a misleading "Wyckoff" name. A "GC/DC" relabel or tooltip is a UI change and needs the user's OK.

No build, no memory entry. Artifacts (scratchpad `v4hist/`): `gxrs_plan.txt`, `gxrs.py`, `gxrs.log`, `gxrs_result.json`.

## AMENDMENT_4 — forward registration (2026-09-30, user: "samive")
**Rules added** to `backend/bottom_cluster_forward.py` before any forward data (entries ≥ 2026-09-29; read ONCE on/after 2027-03-01):
- **B8:** GC vs DC inside any T/Z, strata = day × ATR%-bin × state × RS.
- **B9:** GC∧RS vs DC∧¬RS inside any T/Z, strata = day × ATR%-bin × state.

**Estimand.** Per-bar book return (no cooldown), matching the research; ATR%-bin cut points are frozen in `ATR_CUTS` from the MINE window.
**PASS.** Mean day Δ > 0 with 95 % CI lo > 0. Forward k = 9.

**Seen-window check** (`--check 2024-01-01 2024-12-31`, no verdict):
- **B8:** +1.48 [1.22, 1.75] (research 2024 value +1.72).
- **B9:** +1.90 [1.54, 2.27] (research +2.17).
- **B1-B4** reproduce the earlier checks.

**UI.** The Ultra chips MARKUP⚛ / MKDN⚛ gained hints naming them the golden- / death-cross regime. The label and filter are unchanged; the Superchart "523" row MARKUP/MKDN is a different engine (`wy523`) and was not touched.
