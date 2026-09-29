# T2G_V1 — census + path study (census DESCRIPTIVE; paths plan frozen to `t2gpaths_plan.txt` before any outcome)

**Date** 2026-09-29 · user: "t2g". Census and paths were run together, as agreed.
**Definition** (`signal_engine.py`). The previous bar is green; T2G opens at or above its open and **above its close** (a gap), is green and closes higher. It is a gap continuation.
**Data** **285,274 T2G bars = 11.7 %** of all bars, the most common T/Z state. 269,629 are usable for the path study.

## Census (DESCRIPTIVE)

**The bar itself**

| signal | % of T2G | % all | × |
|---|---|---|---|
| FLY BD / CD / AD / ABCD | 88-95 | 22-24 | 4.0 |
| wick U | 92.6 | 39.3 | 2.4 |
| NE: E | 84.7 | 50.7 | 1.7 |
| R2H | 74.8 | 23.7 | 3.2 |
| 4BF | 65.5 | 17.6 | 3.7 |
| 🔺 continuation | 43.2 | 16.8 | 2.6 |
| RSI ≥ 70 | 15.0 | 6.3 | 2.4 |

- **WLNBB:** L3 48.1 % (3.3×), L12 46.5 %, L34 only 5.4 %. Suffix **EU 75.8 % (4.3×)**; all N* suffixes are under-represented.
- **Most characteristic:**

| signal | % of T2G | × |
|---|---|---|
| CD (`cd`) | 16.3 | 6.1 |
| [E] | 19.5 | 4.6 |
| BB↑ | 4.0 | 4.2 |
| ⭐ Sweet Spot | 6.7 | 4.1 |
| BO▲ / BOV▲ | 13.0 / 8.4 | 4.1 / 4.0 |
| UPP / UP4 | 18.7 / 3.6 | 4.0 / 3.9 |
| CA | 23.0 | 3.8 |
| RETEST | 5.5 | 3.7 |
| ★★ | 16.0 | 3.6 |
| B/S↑ | 7.5 | 3.6 |

- **Rare:**

| signal | % of T2G | % all bars |
|---|---|---|
| close I | 0 | 23 |
| R2L | 0.1 | 23 |
| 🌀 shakeout | 0.05 | 11.7 |
| wick D | 1.1 | 38 |
| △1H REV | 2.4 | 20 |
| RSI ≤ 35 | 2.2 | 10.4 (0.21×) |

- **Bottom marks:** 🔻💪 6.9 % (1.1×), 🕐DR 1.3 % (0.85×).
- **Reading.** T2G is a strength / continuation bar, not a bottom bar.

**What T2G gaps over (t-1)**
- **Its T/Z mix equals that of all green bars** (every × is 0.97-1.05). T2G does not select its t-1, unlike T6, T4 and T1G, which prefer small hesitant bars.
- **Body of t-1:** median 0.36 ATR.
- **Gap:** median 0.17 ATR.
  - Opens above the t-1 high (full gap) on 45.3 %.
  - Leaves the gap unfilled on 19.7 %.
  - Closes above the t-1 high on 84.7 %.
  - Closes above the prior 10-bar high on 36.6 %.
- **Green bars before T2G:** 1 = 51.4 %, 2 = 25.1 %, 3 = 12.0 %, 4+ = 11.5 %.
- **Colours t-3, t-2, t-1:** ≈ 25 % each.

## Paths (k = 3), day × ATR% matched return Δ in pp

| contrast | share | MINE | VERIFY | years | verdict |
|---|---|---|---|---|---|
| A1 TURN (t-2 red) vs RUN (t-2 green) | 50.4 % | +0.18 | +0.35 | 3+ / 3− | FAIL |
| A2 t-1 T9/T5/T10/T12 vs other | 22.8 % | +0.07 | +0.77 [−0.06, 1.64] | 4+ / 2− | FAIL |
| A3 FULL gap vs partial | 45.2 % | −0.43 | **+0.81 [0.17, 1.47]** | 3+ / 3− | FAIL (sign flips) |

- REV lift is 0.96-1.05 everywhere.
- **Full gap goes the opposite way from T1G** (T1G VERIFY −1.35). Neither is stable, so "full gap" is not a usable rule.

## Part B — signals on any of t-2..t, within each path
- **MINE:** k = 813; |t| ≥ 1.96 in 43 (TURN) + 36 (RUN), against about 21 + 19 by chance.
- **VERIFY:** k = 20. **PASS 4:**

| path | signal | VERIFY Δ | years | family |
|---|---|---|---|---|
| TURN | **⚛ MARKUP ↑** | +1.26 [0.20, 2.28] | 5/6 | ⚛ Wyckoff phase (also T2/T6/Z1) |
| RUN | **⚛ MKDN ↓** | −1.77 [−2.88, −0.71] | 5/6 | ⚛ Wyckoff phase (also T6/T2G) |
| RUN | **V×5 ↓** | −6.62 [−11.33, −1.99] | 5/6 | extreme volume |
| TURN | **WSH washout ↓** | −7.18 [−12.55, −1.92], n 84 VERIFY | 6/6 | extreme volatility, small n |

**Reading.** All 4 passes belong to families already found in TZ_BOOSTERS_V1: the ⚛ MARKUP/MKDN axis and extreme volume/volatility. This is a replication of known families inside T2G, not a new, path-specific finding.

Artifacts (scratchpad `v4hist/`): `t2gpaths_plan.txt`, `t2gcensus.py`, `t2gpaths.py`, `t2gprev.py`, logs, `t2g_singles.csv`, `t2g_pairs_clean.csv`, `t2gpaths_B.json`. No build, no memory entry.
