# ALL_TZ_V1 — census + path study for the remaining 17 T/Z states (T5, T10-T12, Z1-Z12, Z1G, Z2G)

**Date** 2026-09-30 · user: "yvela gaanalize". Plan frozen to `alltz_plan.txt` before any outcome.
**Design.** ONE uniform template for all 17 states:
- **A1** RUN (t-2 colour = t-1 colour) vs TURN;
- **A2** LARGE t-1 (body ≥ 0.5 ATR) vs small;
- **A3** FULL break in the bar's own direction (T: close > t-1 high · Z: close < t-1 low).

**Estimand and reading**
- Long-trade book return, paired within day × ATR%.
- For **Z states a negative Δ means the long is worse**, i.e. the bearish reading is stronger.

## VERDICT
**Part A: 0 / 42 testable (9 A3 cells are N/A by definition).** The path into a T/Z bar never changes what it pays. Across all 24 states studied (with T1 / T1G / T2G / T3 / T4 / T6 / T9) that is **0 of 63** path contrasts.

**Part B: 15 / 303 VERIFY passes, ≈ 2× the ≈ 6-8 expected by chance.** Every pass belongs to an already-known family:

| state · path | signal | MINE → VERIFY (pp) | years | family |
|---|---|---|---|---|
| T5 · RUN | **🕐DR ↑** | +3.72 → +3.65 [0.66, 7.12] | 5/6 | 🕐DR (also T5 / T6 in TZ_BOOSTERS) |
| Z2 · RUN | **MKDN⚛ ↓** | −1.44 → −2.84 [−4.53, −1.17] | 4/6 | ⚛ Wyckoff phase |
| Z1G · RUN | **MKDN⚛ ↓** | −1.99 → −2.72 [−4.50, −0.89] | 5/6 | ⚛ Wyckoff phase |
| Z2G · RUN | **MKDN⚛ ↓** | −1.78 → −1.69 [−2.95, −0.51] | 5/6 | ⚛ Wyckoff phase |
| Z6 · RUN | **RSI ≤ 35 ↓** | −3.32 → −3.33 [−6.01, −0.91] | 6/6 | knife law |
| Z2 · RUN | 4BF↓ ↓ | −2.07 → −2.73 [−4.50, −1.10] | 6/6 | sell-side fire |
| Z2 · RUN | F ↓ | −1.76 → −1.51 | 6/6 | wick descriptor |
| Z3 · RUN | M4·σ4 ↓ | −2.10 → −2.38 | 6/6 | volume extreme |
| Z1G · RUN | D50 ↓ | −2.96 → −2.20 | 6/6 | EMA-cross down (also Z1G in TZ_BOOSTERS) |
| Z2 · TURN | ★V ↑ | +2.12 → +2.58 [1.57, 3.62] | 6/6 | L-BAL (new here) |
| Z6 · TURN | ⚛m:M1 ↑ | +2.78 → +2.15 | 5/6 | ⚛ phys |
| Z1G · RUN | σ1 ↑ | +2.57 → +2.37 | 5/6 | VOL7 |
| Z2G · RUN | CD·30 / CD·60 ↑ | +2.44 / +2.32 → +1.24 / +1.18 | 5-6/6 | OVD (shrank ~50 %) |
| T12 · RUN | ATM Atomic ↑ | +2.62 → +2.91 [0.09, 5.87] | 4/6 | Atomic edge (definitionally close to T12 / T5) |

## Census highlights (DESCRIPTIVE; the full per-state lists are in `alltz_result.json`)

| state | share | t-1 | what sits on it (×) | bottom marks | RSI |
|---|---|---|---|---|---|
| T5 | 3.6 % | red, non-selective, 0.36 ATR | **ATM Atomic 52 % (22×)**, L43 46 % (15×), ⚡G3-Abs 21×, G3-reclaim 14× | **🕐DR 2.4×**, 🔻💪 1.6× | ≤35 1.6× |
| T10 | 2.0 % | green, big (T4 1.45×) | CON↑ 18×, MID↑ 12×, L43 11× | 🕐DR 0.8× | ≥70 1.35× |
| T11 | 0.7 % | green, small | FRI43 20×, ZRT🟢 16×, L43 15× | 🔻💪 1.65× | — |
| T12 | 0.9 % | green, tiny 0.13 (T5 / T10 2.2×) | L43🔇 78×, ⚡G3-Abs 27×, ATM 22× | **🕐DR 2.3×** | — |
| Z1 | 4.1 % | green | WRP↓ 12×, R2D, BO↓ | 🌀 2.2× | ≥70 0.3× |
| Z2 | 7.8 % | red (Z4 / Z1 1.2×) | R2L 67 %, 4BF↓ 50 %, DIV 3.4× | 🌀 2.6× | **≤35 2.0×** |
| Z3 | 4.7 % | green, big (T4 1.4×) | FBO↓ 6.5×, UTAD 5.6× | 🌀 1.6× | ≥70 1.1× |
| Z4 | 4.5 % | green, small (T5 / T9) | EXP↓ 18×, LST↓ 15×, BE↓ 8× | 🌀 2.5× | — |
| Z5 | 3.8 % | green | red L34 10×, D+L1 12×, UTAD 5× | — | **≥70 1.65×** |
| Z6 | 2.4 % | red, tiny 0.10 (Z5 / Z10 2×) | LST↓ 13×, EXP↓ 9× | 🌀 2.6× | ≤35 1.5× |
| Z7 | 0.8 % | either colour | doji (MJ 100 %), DIST-TR 6× | — | MKDN 1.1× |
| Z9 | 4.3 % | green, big (T4 1.7×) | MID↓ 15×, CON↓ 15× | 🕐DR 0.1× | — |
| Z10 | 2.1 % | red, big (Z4 1.45×) | CON↓ 17×, MID↓ 16×, **red L34 16×**, D+L1 12× | 🌀 1.3× | ≤35 1.3× |
| Z11 | 1.8 % | red, small (Z10 2×) | **red L34 17×, D+L1 17×** | — | — |
| Z12 | 0.16 % | green, tiny | WRP↓ 6×, BO↓ | 🌀 1.9× | — |
| Z1G | 3.2 % | green, tiny (T5 / T10 2×) | BX↓ 5.5×, BO↓, QZC 4×, BDV▼ 3.6× | 🌀 2.2× | ≤35 1.6× |
| Z2G | 11.2 % | red, non-selective | BD▼ / BDV▼ 4.2×, DIV 3.9×, 4BF↓ 65 %, QZC 3.6× | 🌀 2.3× | **≤35 2.2×** |

**Structure seen in all 24 states**
- **Mirror pairs:** T3↔Z3, T9↔Z9 (harami after a big bar of the opposite colour), T10↔Z10 (inside after a big bar of the same colour), T4↔Z4, T6↔Z6 (engulf of a small bar).
- **Bottom-type states** (🕐DR ≥ 2×): T3, T9, T5, T12. **🌀 shakeout** sits on the bearish engulf / gap-down Z states: Z1, Z2, Z4, Z6, Z1G, Z2G.

## Caveats
- Z12 (n 3,997) has too few bars for Part B.
- A3 is definitionally empty for T5 / T10-T12 / Z3 / Z5 / Z9-Z11.
- Part B selects at family level (k = 10,020 MINE cells, 303 VERIFY). Treat individual passes as WATCH, except where they repeat a known family.

Artifacts (scratchpad `v4hist/`): `alltz_plan.txt`, `alltz.py`, `alltz.log`, `alltz_result.json`. No build, no memory entry.
