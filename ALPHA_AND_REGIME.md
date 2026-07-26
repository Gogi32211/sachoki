# Alpha-vs-beta + causal regime gate — acc_tr(TEST)

_Fixed strategy: russell2k+nasdaq · acc_tr(TEST) · entry-day $-vol≥$500k · next-open · −15% stop / +100% target · gap-aware. OOS (ex PAVS/WNW/GLOO). Percent units. No production code._

---

# FINAL VERDICT (read first)

**The two tests split: acc_tr(TEST) is GENUINE selection ALPHA (not beta) — but the causal regime gate FAILED, so the good years cannot be timed mechanically. → A validated discretionary SCANNER/WATCHLIST flag for surfacing explosive candidates; NOT a mechanical all-weather buy.**

### TEST 1 — Alpha or beta? → **ALPHA, decisively.**
Week-matched against BOTH controls (random microcaps **and** other same-week T-signal triggers), acc_tr(TEST) sits at the **100th percentile** on every tail metric, in both universes, all-years and risk-on:
- **mean winner ≈ 3×** control (russell2k 25.3 vs ~8; nasdaq 26.4 vs ~9–10), **100th pct**.
- **P(+100%) ≈ 10×** control (3.7–5.8% vs 0.3–0.7%), **100th pct**. P(+50%) likewise 100th.
- **EXPECTANCY** 100th pct vs both controls everywhere — *except* **nasdaq vs CONTROL-B = 85th (in-band)**, the one soft spot (nasdaq's mean edge over other triggers is thinner; russell2k is the cleaner alpha).
- **win% is at the 0th percentile** (34–36% vs 47–51% control) — *not a flaw*: it is the positive-skew signature. acc_tr(TEST) wins **less often but ~3× bigger and ~10× more tail-hits.** Holding the regime AND the "it's a long trigger" constant (CONTROL-B), it still concentrates explosive moves far beyond the control band. **This is selection, not just risk-on beta.**

### TEST 2 — Is "risk-on only" causally deployable? → **NO. The gate fails the stand-down test.**
A causal, self-calibrating gate (breadth50 + pumpiness vs their expanding-medians, data < i) does **not** separate the good years from the bad:
- **2022 (bear):** gate ON **39–42%** of days, and **gated expectancy stays NEGATIVE (−1.01 to −1.13) — worse than ungated.** It does **not** stand down.
- **2024 (worst failure):** gate ON **75% of days — the highest of any year — while the year loses (−1.5).** The "risk-on" reading was simply wrong in 2024.
- **2025–26:** gate captures the gains (2.7–6.9) but those years were **already positive ungated** — the gate adds ~nothing.
- **Leakage:** results survive a 5-day lag **except** the 2023 russell2k cell (gated +8.81 → lag5 **+0.07**) — that one big number is fragile/leakage-sensitive and should be discounted.
- **Conclusion:** the per-year P&L swing (negative 2021/2022/2024, positive 2023/2025/2026) is **real but not separable by any causal regime proxy buildable from this data.** Automated regime timing is hindsight here.

### Where this leaves the promotion trail
- **Alpha is real** (TEST 1) — acc_tr(TEST) genuinely *selects* explosive candidates; it is the strongest validated piece of the whole trail.
- **Tradeability is not** — unconditionally the strategy is only marginally positive (+1.25 r2k / +0.52 nas per trade over all years) and **loses in 3 of 6 years**, and TEST 2 shows you **cannot mechanically avoid the bad years**.

### Recommendation — exact rule
- **SHIP as a discretionary SCANNER / WATCHLIST flag** (its real value is the **~10× tail concentration** — ranking/surfacing, not mechanical entry):
  - russell2k / nasdaq · **acc_tr fires AND close_pos<0.5 AND <50% above 20-day low** · entry-day **$-vol ≥ $500k** · glitch-screened.
  - Label: *"high explosive-tail selection (P(+100%) ~10× base); wins ~35% of the time, big when it does."*
- **For actual trading** (if at all): **−15% stop / +100% fixed target**, next-open entry, **small fixed-fraction size**, and **DISCRETIONARY** risk-on regime judgment by the operator — **the automated gate does not work**, so do not promise mechanical timing.
- **Do NOT** ship as a standalone mechanical buy, and **do NOT** fold into turbo/ultra score (anti-correlated with win-rate; loses in bear/neutral years).
- `range_exp`, `acc_tr×range_exp` remain **watchlist-only / drop** (from EXIT_BACKTEST: negative net of fills).

_No production scoring changed._

---

## TEST 1 — ALPHA vs BETA (week-matched bootstrap controls)

CONTROL-A = random microcap bars (no signal), matched (universe, ISO-week, $-vol≥$500k). CONTROL-B = candidate T-signal bars that are NOT acc_tr(TEST), same matching. 600 bootstrap baskets; treatment percentile within each control distribution.


### russell2k · all years  (acc_tr(TEST) n=1827)

**vs CONTROL-A (random microcap, same week)**

| metric | acc_tr(TEST) | control mean | control 5–95% | **percentile** |
|---|---|---|---|---|
| EXPECTANCY | 1.25 | 0.33 | -0.12 … 0.78 | **100.0th** ✅>90th |
| P(+50%) | 4.38 | 0.53 | 0.27 … 0.82 | **100.0th** ✅>90th |
| P(+100%) | 3.67 | 0.27 | 0.11 … 0.49 | **100.0th** ✅>90th |
| win% | 35.63 | 49.17 | 47.29 … 51.07 | **0.0th** ❌<10th |
| mean winner | 25.26 | 7.95 | 7.33 … 8.65 | **100.0th** ✅>90th |

**vs CONTROL-B (candidate non-acc_tr, same week)**

| metric | acc_tr(TEST) | control mean | control 5–95% | **percentile** |
|---|---|---|---|---|
| EXPECTANCY | 1.25 | 0.31 | -0.15 … 0.76 | **100.0th** ✅>90th |
| P(+50%) | 4.38 | 0.71 | 0.38 … 1.04 | **100.0th** ✅>90th |
| P(+100%) | 3.67 | 0.44 | 0.22 … 0.71 | **100.0th** ✅>90th |
| win% | 35.63 | 48.36 | 46.52 … 50.14 | **0.0th** ❌<10th |
| mean winner | 25.26 | 8.22 | 7.56 … 8.93 | **100.0th** ✅>90th |

### russell2k · risk-on 2025–26  (acc_tr(TEST) n=759)

**vs CONTROL-A (random microcap, same week)**

| metric | acc_tr(TEST) | control mean | control 5–95% | **percentile** |
|---|---|---|---|---|
| EXPECTANCY | 2.47 | 0.81 | 0.06 … 1.63 | **100.0th** ✅>90th |
| P(+50%) | 6.06 | 0.69 | 0.26 … 1.19 | **100.0th** ✅>90th |
| P(+100%) | 5.27 | 0.38 | 0.0 … 0.79 | **100.0th** ✅>90th |
| win% | 36.1 | 50.73 | 47.83 … 53.49 | **0.0th** ❌<10th |
| mean winner | 28.66 | 8.62 | 7.52 … 9.91 | **100.0th** ✅>90th |

**vs CONTROL-B (candidate non-acc_tr, same week)**

| metric | acc_tr(TEST) | control mean | control 5–95% | **percentile** |
|---|---|---|---|---|
| EXPECTANCY | 2.47 | 0.95 | 0.16 … 1.77 | **99.7th** ✅>90th |
| P(+50%) | 6.06 | 1.12 | 0.53 … 1.71 | **100.0th** ✅>90th |
| P(+100%) | 5.27 | 0.7 | 0.26 … 1.19 | **100.0th** ✅>90th |
| win% | 36.1 | 49.32 | 46.77 … 52.17 | **0.0th** ❌<10th |
| mean winner | 28.66 | 9.31 | 8.07 … 10.55 | **100.0th** ✅>90th |

### nasdaq · all years  (acc_tr(TEST) n=1776)

**vs CONTROL-A (random microcap, same week)**

| metric | acc_tr(TEST) | control mean | control 5–95% | **percentile** |
|---|---|---|---|---|
| EXPECTANCY | 0.52 | -0.02 | -0.48 … 0.46 | **97.0th** ✅>90th |
| P(+50%) | 4.95 | 0.71 | 0.39 … 1.07 | **100.0th** ✅>90th |
| P(+100%) | 4.0 | 0.32 | 0.11 … 0.56 | **100.0th** ✅>90th |
| win% | 33.9 | 46.38 | 44.59 … 47.97 | **0.0th** ❌<10th |
| mean winner | 26.39 | 9.28 | 8.56 … 10.09 | **100.0th** ✅>90th |

**vs CONTROL-B (candidate non-acc_tr, same week)**

| metric | acc_tr(TEST) | control mean | control 5–95% | **percentile** |
|---|---|---|---|---|
| EXPECTANCY | 0.52 | 0.17 | -0.34 … 0.69 | **85.3th** ⚠ in-band |
| P(+50%) | 4.95 | 0.97 | 0.62 … 1.35 | **100.0th** ✅>90th |
| P(+100%) | 4.0 | 0.47 | 0.23 … 0.73 | **100.0th** ✅>90th |
| win% | 33.9 | 46.71 | 44.87 … 48.43 | **0.0th** ❌<10th |
| mean winner | 26.39 | 9.73 | 8.93 … 10.57 | **100.0th** ✅>90th |

### nasdaq · risk-on 2025–26  (acc_tr(TEST) n=744)

**vs CONTROL-A (random microcap, same week)**

| metric | acc_tr(TEST) | control mean | control 5–95% | **percentile** |
|---|---|---|---|---|
| EXPECTANCY | 2.43 | 0.46 | -0.32 … 1.22 | **100.0th** ✅>90th |
| P(+50%) | 6.72 | 0.99 | 0.54 … 1.61 | **100.0th** ✅>90th |
| P(+100%) | 5.78 | 0.4 | 0.13 … 0.81 | **100.0th** ✅>90th |
| win% | 33.6 | 47.4 | 44.62 … 50.27 | **0.0th** ❌<10th |
| mean winner | 32.63 | 10.12 | 9.04 … 11.36 | **100.0th** ✅>90th |

**vs CONTROL-B (candidate non-acc_tr, same week)**

| metric | acc_tr(TEST) | control mean | control 5–95% | **percentile** |
|---|---|---|---|---|
| EXPECTANCY | 2.43 | 0.78 | -0.05 … 1.7 | **99.7th** ✅>90th |
| P(+50%) | 6.72 | 1.37 | 0.67 … 2.15 | **100.0th** ✅>90th |
| P(+100%) | 5.78 | 0.71 | 0.27 … 1.21 | **100.0th** ✅>90th |
| win% | 33.6 | 47.52 | 44.49 … 50.28 | **0.0th** ❌<10th |
| mean winner | 32.63 | 10.79 | 9.43 … 12.3 | **100.0th** ✅>90th |


## TEST 2 — CAUSAL regime gate (universe-internal, data ≤ i)

Gate inputs at date i (all backward-looking): **breadth50** = % of active universe with close>MA50; **pump** = % of active universe with trailing-10d return ≥+50%. Gate **ON** if breadth50 AND pump each exceed their own **expanding-median** (computed only from dates < i → self-calibrating, no future data). A 5-day-lagged variant tests leakage.

### Per-year: gate ON-fraction (days) + strategy expectancy

| year | uni | gate ON% days | ungated EXP (n) | **gated EXP (n)** | gated-lag5 EXP (n) |
|---|---|---|---|---|---|
| 2021 | russell2k | 30.0% | -0.9 (n165) | **-3.26 (n67)** | -3.49 (n70) |
| 2021 | nasdaq | 31.0% | -1.27 (n145) | **-2.59 (n60)** | -3.24 (n71) |
| 2022 | russell2k | 39.0% | -0.56 (n349) | **-1.13 (n120)** | -1.12 (n154) |
| 2022 | nasdaq | 42.0% | -0.85 (n324) | **-1.01 (n117)** | -0.54 (n131) |
| 2023 | russell2k | 51.0% | 3.65 (n264) | **8.81 (n134)** | 0.07 (n112) |
| 2023 | nasdaq | 54.0% | -0.62 (n253) | **2.0 (n123)** | 1.25 (n118) |
| 2024 | russell2k | 75.0% | -0.74 (n290) | **-1.55 (n222)** | -1.45 (n218) |
| 2024 | nasdaq | 75.0% | -0.87 (n310) | **-1.48 (n235)** | -0.98 (n240) |
| 2025 | russell2k | 57.0% | 2.35 (n494) | **2.72 (n303)** | 2.77 (n308) |
| 2025 | nasdaq | 60.0% | 1.64 (n523) | **1.66 (n336)** | 1.16 (n342) |
| 2026 | russell2k | 66.0% | 2.7 (n265) | **2.7 (n163)** | 3.02 (n147) |
| 2026 | nasdaq | 59.0% | 4.3 (n221) | **6.89 (n122)** | 2.83 (n111) |

### Stand-down / capture summary

- **russell2k**: 2022 ungated -0.56 (n349) → gated -1.13 (n120, 34.0% of bear entries taken). 2025–26 ungated 2.47 (n759) → gated 2.71 (n466, 61.0% taken).
- **nasdaq**: 2022 ungated -0.85 (n324) → gated -1.01 (n117, 36.0% of bear entries taken). 2025–26 ungated 2.43 (n744) → gated 3.05 (n458, 62.0% taken).

### Leakage check

- Gate uses only MA/trailing-return/expanding-median of dates **< i** (shift(1) on the threshold). The **gated-lag5** column delays the gate a further 5 trading days; if results survive that lag, the edge is not riding an i-coincident peek.

