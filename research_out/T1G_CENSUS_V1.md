# T1G_CENSUS_V1 — what sits on a T1G bar and what comes before it (DESCRIPTIVE ONLY, no outcomes)

**Date** 2026-09-29 · user: "t1g gaanalize" (same format as T6_CENSUS_V1).
**Definition** (`signal_engine.py`):
- The previous bar is red (or a doji).
- T1G is green, opens **above the previous bar's open and close** (a gap over its body), and closes above its open.
- So T1G jumps over a red bar; it does not engulf it.

**Data.** **88,129 T1G bars = 3.61 %** of all bars, 2021-05…2026-09, all US tickers.
**Method.** Same rules as the T6 census: always-on keys removed, near-definitional pairs removed, × = % on T1G ÷ % on all bars.

## 1. The bar itself

| signal | % of T1G | % all | × |
|---|---|---|---|
| wick U (`_wk_u`) | 86.9 | 39.3 | 2.2 |
| NE: E | 78.1 | 50.7 | 1.5 |
| body X (expanded) | 70.2 | 37.8 | 1.9 |
| ⚡×2+ / ⚡×3+ | 76.0 / 39.0 | 45.0 / 18.5 | 1.7 / 2.1 |
| RF (⚛ phys) / RF·U | 50.7 / 38.3 | 25.5 / 13.3 | 2.0 / 2.9 |
| 4BF | 50.4 | 17.6 | 2.9 |
| R2H / R2X | 50.3 / 28.0 | 23.7 / 10.8 | 2.1 / 2.6 |
| 🔺 continuation (markup) | 36.9 | 16.8 | 2.2 |

**WLNBB**
- **L-signal:** L3 49.7 % (3.4×), L12 41.0 % (1.6×), L34 9.4 %.
- **Suffix:** **EU 57.6 % (3.2×)**, EUR 16.9 % (3.2×). All others are under-represented.

**Context.** ·U 69.5 %, P>50 62.6 %, 🔻 25.9 %, 🔻💪 8.8 %, 🏆rs 29.0 %, 🕐DR 2.0 %, gG3 5.6 % (1.9×).

## 2. Most characteristic (≥ 2 % of T1G)

| signal | % T1G | × |
|---|---|---|
| G2 (`g2`) | 26.4 | **27.7** |
| MTH↑ | 5.0 | 5.0 |
| 260308 | 8.2 | 4.8 |
| −S | 5.8 | 4.8 |
| BB↑ | 4.1 | 4.3 |
| BO▲ / BOV▲ (VOL ECHO) | 11.3 / 7.4 | 3.5 |
| XJ | 5.8 | 3.4 |
| UPR / REV / RE2 (PV) | 14.0 / 5.3 / 6.3 | 3.4 |
| RETEST | 4.9 | 3.3 |
| G1L | 15.5 | 3.3 |
| ΔΔ↑ | 16.5 | 3.2 |
| VBO↑ | 22.2 | 3.2 |
| BX↑ | 14.8 | 3.1 |
| SVS | 12.7 | 3.0 |
| FLP↑ | 13.0 | 2.9 |
| BO↑ | 20.5 | 2.9 |
| RTV | 11.2 | 2.8 |
| [E] | 11.5 | 2.7 |
| P55 refined | 3.5 | 2.7 |

## 3. Rare on T1G

| signal | % of T1G | % all bars |
|---|---|---|
| FLY BD / CD / AD / ABCD | ≈ 0 | 22-24 |
| R2L | 0.06 | 23 |
| 🌀 shakeout | 0.03 | 11.7 |
| wick D | 2.7 | 38 |
| RA / RA·D / RA·U | 3.5 / 1.5 / 1.9 | 25 / 13 / 13 |
| RSI ≤ 35 | 3.3 | 10.4 |

- FLY is the mirror image of T6, where FLY was on ≈ 95 %.
- RSI ≤ 35 is 0.32×.

## 4. Most frequent pairs of T1G-characteristic signals

| pair | % T1G | % all | × |
|---|---|---|---|
| E + ⚡×2+ | 62.0 | 23.8 | 2.6 |
| E + X | 59.7 | 26.7 | 2.2 |
| ⚡×2+ + X | 56.2 | 19.1 | 2.9 |
| ⚡×2+ + L3 | 51.4 | 19.5 | 2.6 |
| E + 4BF | 50.4 | 17.6 | 2.9 |
| E + L3 | 49.6 | 14.6 | 3.4 |
| E + RF | 46.5 | 20.7 | 2.2 |
| X + L3 | 43.1 | 11.0 | 3.9 |
| X + 4BF | 40.8 | 9.1 | **4.5** |
| ⚡×2+ + 4BF | 42.4 | 13.2 | 3.2 |

## 5. What T1G jumps over, and the bars before it

**The t-1 bar**
- **Colour:** red on 94.6 %, doji on 5.4 %. Z7 is the doji state: 5.3 % before T1G vs ≈ 0 % of red bars.
- **Body:** usually small, median **0.12 ATR** (q75 0.23).

| t-1 state | % before T1G | % of all red bars | × |
|---|---|---|---|
| Z2G | 21.8 | 22.4 | 1.0 |
| **Z5** | 13.9 | 7.5 | 1.9 |
| **Z9** | 12.4 | 8.5 | 1.5 |
| Z2 | 9.4 | 15.6 | 0.6 |
| **Z10** | 9.2 | 4.3 | 2.1 |
| Z3 | 7.3 | 9.4 | 0.8 |
| Z1G | 6.6 | 6.3 | 1.0 |
| **Z11** | 5.6 | 3.5 | 1.6 |
| Z1 | 3.8 | 8.1 | 0.5 |
| Z4 | 2.5 | 9.1 | 0.3 |
| Z6 | 1.7 | 4.8 | 0.35 |

**Reading.** As with T6, the jumped-over bar is typically a small "inside / hesitant" red (Z5, Z9, Z10, Z11). Large engulfing reds (Z4, Z6, Z1) are rare.
- **L-signal on t-1:** L46 30.6 %, L25 25.6 %, L12 17.1 %, L5 12.0 %, L34 9.2 %.

**The gap** (T1G open over the t-1 body top, in ATR)
- **Median:** 0.15 ATR.
- **Size bands:** <0.1 ATR 36 %, 0.1-0.25 ATR 34 %, 0.25-0.5 ATR 20 %, 0.5-1 ATR 7 %, ≥1 ATR 3 %.
- **Relation to the t-1 high:**
  - opens above it (full gap) on 34.1 %;
  - leaves the gap unfilled (T1G low > t-1 high) on 15.1 %;
  - closes above it on 78.1 %.
- **Location:**
  - T1G closes above the prior 10-bar high on 25.0 %;
  - t-1 is the 10-bar low on 17.0 %.

**Bars before**
- **Consecutive red bars before T1G:** 1 = 47.6 %, 2 = 23.9 %, 3 = 11.6 %, 4+ = 11.5 % (doji-only 5.4 %).
- **Colours t-3, t-2, t-1:** GRR 23.6 %, RGR 23.6 %, RRR 23.1 %, GGR 22.9 %. t-2 and t-3 are ≈ 50/50.
- **Two paths (t-2→t-1):**
  - **(a) pause in an up-move** (green, then a small red): T2G→Z5 3.3 %, T2G→Z9 3.0 %, T2→Z9 2.0 %, T2→Z5 1.7 %, T2G→Z3 1.7 %, T1→Z9 1.6 %, T4→Z9 1.6 %.
  - **(b) after a red run:** Z2G→Z2G 4.9 %, Z2→Z2G 3.4 %, Z2G→Z2 2.1 %, Z3→Z2G 2.1 %, Z2G→Z10 2.0 %.
- **3-bar sequences** are dispersed; the top one, Z2G→Z2G→Z2G, is 1.0 %.

Full lists (scratchpad `v4hist/`): `t1g_singles.csv`, `t1g_pairs_clean.csv`, `t1gcensus.log`, `t1gprev.log`. No outcomes measured.
