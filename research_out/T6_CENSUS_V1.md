# T6_CENSUS_V1 — which signals sit on a T6 bar, and which pairs (DESCRIPTIVE ONLY, no outcomes)

**Date** 2026-09-29 · user: "marto T6 gamomikvlie. chamowere yvela signalebi romlebic am T6 ze xvdeba xolme procentulad, aseve signalebis tanxvedrebi".
**Data** 5,739 US tickers, eligible bars, 2021-05…2026-09.
- **T6 bars:** 47,121 = **1.93 %** of all bars.
- **Enrich** = % of T6 bars carrying the signal ÷ % of all bars carrying it.
- **Removed:**
  - T/Z keys;
  - identical duplicate columns;
  - 7 keys that are ~100 % on every bar in this frame (P<20, !EXT, ANY L, L5∗ …) and carry no information;
  - near-definitional pairs, i.e. pairs whose all-bar lift is ≥ 5.

## 1. What a T6 bar almost always carries (structure of the bar itself)

| signal | % of T6 | % of all bars | × |
|---|---|---|---|
| FLY BD / CD / AD / ABCD | 96 / 94 / 89 / 89 | 22-24 | ≈ 4.0 |
| body X (expanded) | 94.6 | 37.8 | 2.5 |
| ⚡×2+ / ⚡×3+ / ⚡×4+ | 90 / 64 / 33 | 45 / 18.5 / 6.2 | 2.0 / 3.5 / 5.3 |
| close A | 66.7 | 14.8 | 4.5 |
| body F / XF | 54.5 / 53.0 | 33.3 / 21.0 | 1.6 / 2.5 |
| R2H (line5) | 51.0 | 23.7 | 2.2 |
| pen R | 42.7 | 15.9 | 2.7 |
| wick B | 31.7 | 9.5 | 3.3 |

**WLNBB on T6**
- **L-signal:** L12 46.4 % (1.8×), L3 30.2 % (2.1×), L34 20.9 % (2.2×).
- **Suffix:** EUR 30.8 % (**5.8×**), EB 20.1 % (3.9×), NDP 18.5 % (2.7×), NUR 11.9 %, NB 11.6 %.

## 2. Most CHARACTERISTIC of T6 (≥ 2 % of T6, ranked by ×)

| signal | % of T6 | % all | × |
|---|---|---|---|
| G6 | 10.8 | 0.2 | **51.8** |
| SBC | 49.0 | 2.3 | **21.6** |
| ⛔LST↑ | 20.2 | 1.4 | 14.3 |
| BE↑ | 32.9 | 4.0 | 8.2 |
| CA | 40.5 | 6.0 | 6.7 |
| LST | 20.2 | 3.2 | 6.3 |
| L88 | 2.7 | 0.4 | 6.1 |
| ↑ Building | 13.9 | 2.5 | 5.7 |
| CW | 10.3 | 2.0 | 5.1 |
| ⚛ AD ★ / ★★ / any | 13.4 / 21.3 / 35.6 | 2.7 / 4.4 / 7.3 | ≈ 5 |
| ⭐ Sweet Spot | 7.6 | 1.6 | 4.7 |
| P3 / P2 / P55 / P66 | 5.6 / 11.2 / 6.3 / 3.8 | 1.3 / 2.9 / 1.8 / 1.1 | 3.5-4.3 |
| EB↑ | 17.5 | 4.3 | 4.0 |
| FRI34 | 3.1 | 0.8 | 3.7 |
| 260308 | 6.3 | 1.7 | 3.7 |
| ΔΔ↑ / Δ↑ | 18.0 / 33.8 | 5.2 / 12.0 | 3.5 / 2.8 |
| ANY P | 26.0 | 8.0 | 3.3 |
| NS | 26.1 | 9.1 | 2.9 |
| green L34 | 20.9 | 7.4 | 2.8 |
| FLP↑ | 11.4 | 4.4 | 2.6 |
| ZRT | 22.8 | 9.3 | 2.5 |
| 🔺 continuation (markup) | 37.8 | 16.8 | 2.3 |
| FBO↑ | 8.5 | 4.1 | 2.1 |

**Bottom / edge marks on T6**

| mark | % of T6 | × |
|---|---|---|
| 🔻 structural bottom | 30.9 | 1.5 |
| 🔻💪 | 11.4 | 1.8 |
| 🏆rs | 30.9 | 1.2 |
| 🎯3 | 2.8 | 1.8 |
| 🕐DR | **2.0** | 1.3 |

- **Context:** ·U 64.8 %; P>50 59.3 %; ⚛ MARKUP 52.2 % vs MKDN 44.1 % (≈ base).

## 3. RARE on T6 (base ≥ 5 %)
Each signal below is shown as % of T6 bars vs % of all bars.
- **R2L** 1.5 vs 23.0 %.
- **close I** 2.5 vs 23.0 %.
- **body J** 2.6 vs 21.6 %.
- **body S** 5.4 vs 31.7 %.
- **RF·D** 3.8 vs 12.2 %.
- **△ 1H REV only** 7.5 vs 20.2 %.
- **RSI ≤ 35** 5.0 vs 10.4 %.
- **wick D** 18.5 vs 38.3 %.
- **K1D** 10.8 vs 21.4 %.
- **gG3** 1.3 vs 3.0 %.

**Reading.** T6 is rarely an oversold or knife bar. It is an expanded, strong-close bar (X · A · R2H), usually above EMA50.

## 4. Pairs of T6-characteristic signals (both ≥ 3 % of T6 and ≥ 1.3× base), most frequent

| pair | % of T6 | % all bars | × |
|---|---|---|---|
| F + XF | 53.0 | 21.0 | 2.5 |
| A + ⚡×3+ | 43.8 | 5.0 | 8.7 |
| ⚡×3+ + L3 | 40.6 | 11.6 | 3.5 |
| ⚡×3+ + F | 40.0 | 8.8 | 4.6 |
| **⚡×3+ + SBC** | 39.3 | 1.9 | **21.2** |
| ⚡×3+ + XF | 39.1 | 6.7 | 5.8 |
| A + L∗ | 38.9 | 5.0 | 7.7 |
| ⚡×3+ + R2H | 35.1 | 8.2 | 4.3 |
| A + L3 | 34.5 | 6.7 | 5.2 |
| L∗ + BE | 32.9 | 4.0 | 8.2 |
| **A + SBC** | 32.8 | 1.5 | **21.5** |
| A + L1 | 32.2 | 8.1 | 4.0 |
| A + B | 31.3 | 3.8 | 8.3 |
| A + XF | 30.1 | 2.7 | 11.2 |
| A + CA | 28.7 | 2.6 | 11.2 |
| ⚡×3+ + Δ↑ | 29.0 | 8.2 | 3.6 |
| R2H + 4BF | 27.5 | 15.2 | 1.8 |
| R2H + 🔺 continuation | 26.5 | 10.4 | 2.5 |
| L1 + NS | 25.6 | 7.7 | 3.3 |
| 🔺 continuation + K1U | 25.2 | 10.8 | 2.3 |

The full lists are in the scratchpad (`v4hist/`): `t6_singles.csv` (380 signals) and `t6_pairs_clean.csv` (12,898 pairs). Also there: `t6census.py`, `t6pairs.py` and the logs.

**Scope.** Descriptive only. How often a signal sits on T6 says nothing about whether T6 then pays. Earlier return results, all within T6, same day, ATR-matched (TZ_BOOSTERS_V1 AMENDMENT_1):
- 🕐DR **+8.13 pp** (VERIFY);
- ⚛ MARKUP +2.57;
- ⚛ MKDN −2.58.

---
## 5. What T6 swallows and what comes before it (DESCRIPTIVE, 47,121 T6 bars)

**Definition.** Per `signal_engine.py`, T6 = the previous bar is **green**, the current bar is green, and the T6 body covers the previous body (body ratio ≥ 1, top ≥ previous top, bottom ≤ previous bottom). So T6 always swallows a green body. It swallows the body, not the wicks: its body covers the whole high-low of t-1 on only 10 % of T6s.

**The swallowed bar (t-1)**
- **Size.** It is usually tiny.
  - Body < 0.1 ATR on 48.4 %, 0.1-0.25 ATR on 35.4 %, 0.25-0.5 ATR on 14.1 %, ≥ 0.5 ATR on 2.1 %.
  - The T6 body is a median **5.1×** the swallowed body (quartiles 2.8× / 11×).
- **T/Z state vs any green bar:**

| t-1 state | % of T6 | % of all green bars | × |
|---|---|---|---|
| T2G | 23.7 | 23.7 | 1.0 |
| **T9** | 15.5 | 9.3 | 1.7 |
| **T5** | 15.3 | 7.3 | 2.1 |
| **T10** | 10.2 | 4.0 | 2.6 |
| T2 | 9.4 | 14.3 | 0.7 |
| T1G | 6.9 | 7.3 | 1.0 |
| T3 | 6.1 | 8.7 | 0.7 |
| T12 | 3.9 | 1.9 | 2.1 |
| T1 | 3.7 | 9.4 | 0.4 |
| T4 | 2.1 | 8.6 | 0.2 |
| T11 | 1.9 | 1.4 | 1.4 |
| T6 | 1.3 | 3.9 | 0.3 |

- **Reading.** The small "inside" / hesitant green bars (T9, T10, T5, T12) are over-represented. The large engulfing greens (T4, T6, T1) are rarely swallowed again.
- **L-signal on t-1:** L12 40 %, L34 17 %, L25 14 %, L46 13 %, L3 12 %.

**Engulf depth (consecutive prior bodies inside the T6 body)**

| bodies | 1 | 2 | 3 | 4 | 5 | 6+ |
|---|---|---|---|---|---|---|
| % of T6 | 74.8 | 15.5 | 5.1 | 2.2 | 1.0 | 1.4 |

**Location.** T6 closes above the prior 10-bar high on 13.8 %; it opens at or below the prior 10-bar low on 3.0 %.

**Bars before**
- **Colours t-3, t-2, t-1 (t-1 always green):** RGG 25.8 %, RRG 24.5 %, GGG 24.1 %, GRG 23.7 %.
- **Colours t-2 and t-3 alone:** each ≈ 50/50.
- **Consecutive green bars before T6:** 1 = 49.6 %, 2 = 26.2 %, 3 = 12.3 %, 4 = 5.9 %, 5+ = 6 %.
- **Two families of path:**
  - **(a) Continuation** — green run: T2G→T2G 5.4 %, T2→T2G 3.3 %, T2G→T10 2.4 %, T9/T1/T3/T4→T2G ≈ 2 % each.
  - **(b) Turn** — red bar, then a small green, then T6: Z2G→T5 3.4 %, Z2G→T9 3.4 %, Z2→T9 3.2 %, Z2→T5 2.2 %, Z4→T9 2.0 %, Z1→T9 1.7 %, Z3→T9 1.5 %.
- **3-bar sequences** are very dispersed; the top one, T2G→T2G→T2G, is only 1.2 %.

Scratchpad: `t6prev.py`, `t6prev.log`. No outcomes measured.
