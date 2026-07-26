# Path-dependent exit backtest — discriminator promotion gate

_TEST/absorption subset (close_pos<0.5 & <50% above 20d-low). Entry = NEXT bar open (close-entry shown for the best config). Gap-aware fills; same-bar stop+target = STOP first. russell2k+nasdaq, OOS (ex PAVS/WNW/GLOO). Percent units. No production code touched._

---

# FINAL VERDICT (read first)

**An asymmetric stop+target exit DOES flip the close-negative edge positive — but ONLY on the `acc_tr(TEST)` gate, and the edge is REGIME-DEPENDENT (it loses in the 2022 bear and in 2024). → Conditional, regime-gated, small-size flag — NOT an all-weather tradeable strategy, and NOT a standalone scanner buy.**

### 1. Does a defined-risk exit turn expectancy positive? — YES, on acc_tr(TEST), robustly
- `acc_tr(TEST)` + a **fixed stop/target** is positive in **12 / 12** grid cells in **both** universes (n≈5.8–6.1k each). Best: **s15/t100 → +1.07%/trade russell2k, +0.76% nasdaq.** (close-entry was negative in the prior report; the next-open + 15%-stop/100%-target exit flips it.)
- **Trailing-20% is the worst config** (−2.27 / −2.56) — these names whipsaw; a trailing stop bleeds. **Fixed target beats trailing.**
- `range_exp(TEST)` (n=130/154): essentially **not** tradeable — only russell2k s15/t100 scrapes +0.27, nasdaq negative throughout.
- `acc_tr×range_exp(TEST)` (n=28/31): **negative everywhere** — the intersection is too small and gets chopped.

### 2. Survives realistic fills + a liquidity floor? — acc_tr(TEST) YES, the others NO
- De-rated (**≥$500k entry $-volume + glitch-screen**): `acc_tr(TEST)` stays **+1.22 (n1821) russell2k / +0.50 (n1769) nasdaq**; at **≥$1M it rises to +2.4 / +1.31.** The edge **improves** with a liquidity floor → it is *not* a sub-penny illiquidity artifact.
- `range_exp(TEST)` **goes negative** under the floor (−0.44 / −2.47) → its earlier P(+50%) shine was partly illiquid/fantasy fills. `acc_tr×range_exp` worse. **Only acc_tr(TEST) survives.**
- Fill realism is benign: gap-through-stop tax **7.8%** of stops (modest), glitch-flagged only **11–14** episodes (negligible). ~2/3 of the ≥+100% tail winners are sub-$500k, yet expectancy stays positive after removing them.

### 3. The kill-shot — per-year, the edge is REGIME-DEPENDENT (fails 2022 & 2024)
acc_tr(TEST), s15/t100, ≥$500k+glitch, mean %/trade:

| year | russell2k | nasdaq | tape |
|---|---|---|---|
| 2021 | −0.9 | −1.27 | ❌ |
| **2022** | **−0.94** | **−1.26** | ❌ **bear — fails** |
| 2023 | +3.79 | −0.5 | ~ |
| 2024 | −0.69 | −0.78 | ❌ |
| 2025 | +2.42 | +1.71 | ✅ |
| 2026 | +2.70 | +4.30 | ✅ |

The whole positive headline is carried by **2025–2026 (and 2023 russell2k)** — a risk-on microcap regime. In the **2022 bear it loses, and 2021/2024 are negative too.** A tail-long strategy that needs a risk-on tape is a **beta play, not an all-weather edge.** Combined with **win% only 33–42%, negative median trade, and a −100% naive-full-size equity drawdown** (path risk — requires small fractional sizing), it cannot be shipped as an unconditional buy.

### 4. Recommendation — exact rule
- **`range_exp`, `acc_tr×range_exp` → WATCHLIST-ONLY / drop.** Negative net of realistic fills. Do not trade.
- **`acc_tr(TEST)` → a CONDITIONAL, regime-gated scanner flag**, tradeable only with all of:
  - **Universe:** russell2k / nasdaq. **Signal:** acc_tr fires AND close_pos<0.5 AND <50% above 20-day low (the TEST/absorption bar).
  - **Liquidity:** entry-day $-volume **≥ $500k** (≥$1M is cleaner). Glitch-screen on.
  - **Entry:** next-bar **open**. **Stop:** −15% (gap-aware). **Target:** +100% fixed (not trailing). Horizon 10–20d (insensitive: +1.22 vs +1.24).
  - **Sizing:** small fixed-fraction (you lose on ~60% of trades; max single loss −50%+).
  - **REGIME GATE (mandatory):** risk-on microcap tape only. **Stand down in bear/neutral regimes** — it was negative in 2021/2022/2024.
- Without the regime gate it is a **watchlist tag**, because unconditionally it does not survive 2022.

_Caveats: maxDD −100% is a sequential-equity (one-at-a-time, full-compounding) figure — flags path risk, not a literal account wipe under fractional sizing. Recent-signal trades are right-censored at the data edge. Close-entry de-rated was actually higher (+2.81/+2.2) than next-open; next-open is reported as the conservative headline. No production scoring changed._

---

### TEST-subset episode counts (OOS)

| gate | russell2k | nasdaq |
|---|---|---|
| acc_tr(TEST) | 5804 | 6056 |
| range_exp(TEST) | 130 | 154 |
| acc_tr x range_exp(TEST) | 28 | 31 |

## PART A — stop/target/trailing expectancy grid (entry=next open, horizon=10d, OOS)


**acc_tr(TEST) · russell2k**  (TEST n=5804)

| config | n | EXPECTANCY | med | win% | P(+50%) | max loss | std | exp/std | maxDD |
|---|---|---|---|---|---|---|---|---|---|
| trail20 | 5804 | **-2.27** | -4.38 | 32.8 | 1.3 | -52.3 | 22.1 | -0.103 | -100.0 |
| s8/t25 | 5804 | **0.33** | -8.0 | 29.6 | 0.7 | -34.4 | 16.8 | 0.02 | -100.0 |
| s8/t50 | 5804 | **0.68** | -8.0 | 27.3 | 6.0 | -44.4 | 21.4 | 0.032 | -100.0 |
| s8/t100 | 5804 | **0.7** | -8.0 | 26.8 | 3.2 | -45.1 | 23.6 | 0.03 | -100.0 |
| s12/t25 | 5804 | **0.72** | -7.27 | 36.2 | 1.1 | -39.7 | 23.8 | 0.03 | -100.0 |
| s12/t50 | 5804 | **1.0** | -9.31 | 33.3 | 7.6 | -44.4 | 27.7 | 0.036 | -100.0 |
| s12/t100 | 5804 | **1.04** | -9.96 | 32.6 | 4.0 | -45.1 | 30.0 | 0.035 | -100.0 |
| s15/t25 | 5804 | **0.77** | -4.21 | 39.4 | 1.1 | -52.3 | 24.6 | 0.031 | -100.0 |
| s15/t50 | 5804 | **1.01** | -5.29 | 36.3 | 8.2 | -52.3 | 28.5 | 0.035 | -100.0 |
| s15/t100 | 5804 | **1.07** | -5.51 | 35.5 | 4.4 | -52.3 | 31.0 | 0.034 | -100.0 |
| s20/t25 | 5804 | **0.85** | -2.64 | 42.2 | 1.2 | -54.5 | 25.4 | 0.033 | -100.0 |
| s20/t50 | 5804 | **1.03** | -3.68 | 38.8 | 8.8 | -54.5 | 29.5 | 0.035 | -100.0 |
| s20/t100 | 5804 | **1.06** | -3.97 | 37.9 | 4.6 | -54.5 | 32.0 | 0.033 | -100.0 |

**acc_tr(TEST) · nasdaq**  (TEST n=6056)

| config | n | EXPECTANCY | med | win% | P(+50%) | max loss | std | exp/std | maxDD |
|---|---|---|---|---|---|---|---|---|---|
| trail20 | 6056 | **-2.56** | -4.6 | 32.7 | 1.4 | -68.2 | 18.1 | -0.141 | -100.0 |
| s8/t25 | 6056 | **0.4** | -8.0 | 29.5 | 0.8 | -68.2 | 17.6 | 0.023 | -100.0 |
| s8/t50 | 6056 | **0.58** | -8.0 | 27.1 | 6.3 | -68.2 | 20.5 | 0.028 | -100.0 |
| s8/t100 | 6056 | **0.58** | -8.0 | 26.4 | 3.3 | -68.2 | 23.7 | 0.025 | -100.0 |
| s12/t25 | 6056 | **0.69** | -8.1 | 36.5 | 1.1 | -68.2 | 20.2 | 0.034 | -100.0 |
| s12/t50 | 6056 | **0.77** | -11.76 | 33.4 | 7.9 | -68.2 | 23.5 | 0.033 | -100.0 |
| s12/t100 | 6056 | **0.81** | -12.0 | 32.5 | 4.2 | -68.2 | 26.8 | 0.03 | -100.0 |
| s15/t25 | 6056 | **0.69** | -4.47 | 39.6 | 1.2 | -68.2 | 21.2 | 0.032 | -100.0 |
| s15/t50 | 6056 | **0.72** | -5.87 | 36.3 | 8.6 | -68.2 | 24.5 | 0.029 | -100.0 |
| s15/t100 | 6056 | **0.76** | -6.24 | 35.4 | 4.5 | -68.2 | 28.0 | 0.027 | -100.0 |
| s20/t25 | 6056 | **0.8** | -2.73 | 42.6 | 1.3 | -68.2 | 22.3 | 0.036 | -100.0 |
| s20/t50 | 6056 | **0.79** | -3.93 | 39.0 | 9.3 | -68.2 | 25.7 | 0.031 | -100.0 |
| s20/t100 | 6056 | **0.81** | -4.21 | 38.0 | 4.8 | -68.2 | 29.3 | 0.028 | -100.0 |

**range_exp(TEST) · russell2k**  (TEST n=130)

| config | n | EXPECTANCY | med | win% | P(+50%) | max loss | std | exp/std | maxDD |
|---|---|---|---|---|---|---|---|---|---|
| trail20 | 130 | **-5.27** | -9.33 | 18.5 | 2.3 | -20.7 | 22.4 | -0.235 | -100.0 |
| s8/t25 | 130 | **-1.05** | -8.0 | 20.0 | 0.8 | -11.8 | 15.9 | -0.066 | -97.3 |
| s8/t50 | 130 | **-1.18** | -8.0 | 14.6 | 6.9 | -11.8 | 19.1 | -0.062 | -98.0 |
| s8/t100 | 130 | **-0.86** | -8.0 | 12.3 | 3.1 | -12.9 | 34.7 | -0.025 | -99.2 |
| s12/t25 | 130 | **-0.69** | -12.0 | 25.4 | 1.5 | -20.7 | 25.1 | -0.027 | -99.2 |
| s12/t50 | 130 | **-1.21** | -12.0 | 20.0 | 9.2 | -20.7 | 27.6 | -0.044 | -99.5 |
| s12/t100 | 130 | **-0.9** | -12.0 | 17.7 | 4.6 | -20.7 | 40.5 | -0.022 | -99.8 |
| s15/t25 | 130 | **-0.48** | -15.0 | 30.0 | 2.3 | -20.7 | 27.2 | -0.018 | -99.6 |
| s15/t50 | 130 | **-0.89** | -15.0 | 24.6 | 11.5 | -20.7 | 29.9 | -0.03 | -99.8 |
| s15/t100 | 130 | **0.27** | -15.0 | 22.3 | 6.9 | -20.7 | 43.8 | 0.006 | -99.9 |
| s20/t25 | 130 | **-1.13** | -17.71 | 34.6 | 2.3 | -23.7 | 28.8 | -0.039 | -99.9 |
| s20/t50 | 130 | **-2.28** | -20.0 | 27.7 | 12.3 | -23.7 | 31.4 | -0.072 | -100.0 |
| s20/t100 | 130 | **-0.82** | -20.0 | 25.4 | 7.7 | -23.7 | 45.6 | -0.018 | -100.0 |

**range_exp(TEST) · nasdaq**  (TEST n=154)

| config | n | EXPECTANCY | med | win% | P(+50%) | max loss | std | exp/std | maxDD |
|---|---|---|---|---|---|---|---|---|---|
| trail20 | 154 | **-6.3** | -10.22 | 14.9 | 1.9 | -25.3 | 20.9 | -0.301 | -100.0 |
| s8/t25 | 154 | **-2.06** | -8.0 | 16.9 | 0.6 | -8.5 | 14.8 | -0.139 | -99.5 |
| s8/t50 | 154 | **-3.0** | -8.0 | 11.0 | 4.5 | -8.5 | 16.4 | -0.183 | -99.9 |
| s8/t100 | 154 | **-2.13** | -8.0 | 9.1 | 2.6 | -12.9 | 32.0 | -0.067 | -99.9 |
| s12/t25 | 154 | **-2.05** | -12.0 | 22.1 | 1.3 | -20.7 | 23.5 | -0.087 | -99.9 |
| s12/t50 | 154 | **-3.13** | -12.0 | 16.2 | 7.1 | -20.7 | 25.1 | -0.125 | -100.0 |
| s12/t100 | 154 | **-2.39** | -12.0 | 14.3 | 3.9 | -20.7 | 37.5 | -0.064 | -100.0 |
| s15/t25 | 154 | **-2.25** | -15.0 | 25.3 | 1.9 | -20.7 | 25.5 | -0.088 | -100.0 |
| s15/t50 | 154 | **-3.26** | -15.0 | 19.5 | 9.1 | -20.7 | 27.4 | -0.119 | -100.0 |
| s15/t100 | 154 | **-1.8** | -15.0 | 17.5 | 5.8 | -20.7 | 40.7 | -0.044 | -100.0 |
| s20/t25 | 154 | **-3.07** | -20.0 | 29.9 | 1.9 | -25.3 | 27.3 | -0.112 | -100.0 |
| s20/t50 | 154 | **-4.59** | -20.0 | 22.7 | 9.7 | -25.3 | 29.3 | -0.157 | -100.0 |
| s20/t100 | 154 | **-2.88** | -20.0 | 20.8 | 6.5 | -25.3 | 42.6 | -0.068 | -100.0 |

**acc_tr x range_exp(TEST) · russell2k**  (TEST n=28)

| config | n | EXPECTANCY | med | win% | P(+50%) | max loss | std | exp/std | maxDD |
|---|---|---|---|---|---|---|---|---|---|
| trail20 | 28 | **-8.39** | -10.61 | 28.6 | 0.0 | -20.7 | 10.6 | -0.793 | -92.5 |
| s8/t25 | 28 | **-2.82** | -8.0 | 17.9 | 0.0 | -8.0 | 11.6 | -0.242 | -73.8 |
| s8/t50 | 28 | **-5.19** | -8.0 | 14.3 | 0.0 | -8.0 | 7.2 | -0.722 | -79.5 |
| s8/t100 | 28 | **-5.19** | -8.0 | 14.3 | 0.0 | -8.0 | 7.2 | -0.722 | -79.5 |
| s12/t25 | 28 | **-3.77** | -12.0 | 25.0 | 0.0 | -20.7 | 15.5 | -0.244 | -85.6 |
| s12/t50 | 28 | **-6.72** | -12.0 | 17.9 | 3.6 | -20.7 | 14.0 | -0.479 | -91.4 |
| s12/t100 | 28 | **-4.93** | -12.0 | 17.9 | 3.6 | -20.7 | 22.0 | -0.224 | -91.4 |
| s15/t25 | 28 | **-5.93** | -15.0 | 25.0 | 0.0 | -20.7 | 16.7 | -0.356 | -92.6 |
| s15/t50 | 28 | **-9.09** | -15.0 | 17.9 | 3.6 | -20.7 | 15.0 | -0.607 | -95.8 |
| s15/t100 | 28 | **-7.31** | -15.0 | 17.9 | 3.6 | -20.7 | 22.8 | -0.32 | -95.8 |
| s20/t25 | 28 | **-5.2** | -20.0 | 32.1 | 0.0 | -23.7 | 21.1 | -0.246 | -94.6 |
| s20/t50 | 28 | **-9.64** | -20.0 | 21.4 | 7.1 | -23.7 | 20.0 | -0.482 | -97.9 |
| s20/t100 | 28 | **-6.07** | -20.0 | 21.4 | 7.1 | -23.7 | 31.5 | -0.193 | -97.9 |

**acc_tr x range_exp(TEST) · nasdaq**  (TEST n=31)

| config | n | EXPECTANCY | med | win% | P(+50%) | max loss | std | exp/std | maxDD |
|---|---|---|---|---|---|---|---|---|---|
| trail20 | 31 | **-8.75** | -10.32 | 25.8 | 0.0 | -25.3 | 10.5 | -0.831 | -95.0 |
| s8/t25 | 31 | **-3.33** | -8.0 | 16.1 | 0.0 | -8.5 | 11.2 | -0.299 | -79.7 |
| s8/t50 | 31 | **-5.48** | -8.0 | 12.9 | 0.0 | -8.5 | 6.9 | -0.796 | -84.2 |
| s8/t100 | 31 | **-5.48** | -8.0 | 12.9 | 0.0 | -8.5 | 6.9 | -0.796 | -84.2 |
| s12/t25 | 31 | **-4.23** | -12.0 | 22.6 | 0.0 | -20.7 | 14.8 | -0.285 | -89.1 |
| s12/t50 | 31 | **-6.9** | -12.0 | 16.1 | 3.2 | -20.7 | 13.4 | -0.514 | -93.5 |
| s12/t100 | 31 | **-5.28** | -12.0 | 16.1 | 3.2 | -20.7 | 21.0 | -0.251 | -93.5 |
| s15/t25 | 31 | **-6.38** | -15.0 | 22.6 | 0.0 | -20.7 | 16.0 | -0.398 | -94.8 |
| s15/t50 | 31 | **-9.24** | -15.0 | 16.1 | 3.2 | -20.7 | 14.4 | -0.642 | -97.0 |
| s15/t100 | 31 | **-7.62** | -15.0 | 16.1 | 3.2 | -20.7 | 21.8 | -0.349 | -97.0 |
| s20/t25 | 31 | **-6.21** | -20.0 | 29.0 | 0.0 | -25.3 | 20.6 | -0.302 | -96.8 |
| s20/t50 | 31 | **-10.23** | -20.0 | 19.4 | 6.5 | -25.3 | 19.3 | -0.529 | -98.8 |
| s20/t100 | 31 | **-7.0** | -20.0 | 19.4 | 6.5 | -25.3 | 30.2 | -0.232 | -98.8 |

## PART A — best config per gate×universe (n≥30, by OOS expectancy)

| gate · universe | best config | n | EXPECTANCY | positive? |
|---|---|---|---|---|
| acc_tr x range_exp(TEST) · nasdaq | s8/t25 | 31 | **-3.33** | ❌ no |
| acc_tr(TEST) · nasdaq | s12/t100 | 6056 | **0.81** | ✅ YES |
| acc_tr(TEST) · russell2k | s15/t100 | 5804 | **1.07** | ✅ YES |
| range_exp(TEST) · nasdaq | s15/t100 | 154 | **-1.8** | ❌ no |
| range_exp(TEST) · russell2k | s15/t100 | 130 | **0.27** | ✅ YES |

### Does any config give POSITIVE OOS expectancy on a high-n gate?

- trail20 · russell2k:-2.27(n5804) · nasdaq:-2.56(n6056)
- s8/t25 · russell2k:0.33(n5804) · nasdaq:0.4(n6056) ✅BOTH+
- s8/t50 · russell2k:0.68(n5804) · nasdaq:0.58(n6056) ✅BOTH+
- s8/t100 · russell2k:0.7(n5804) · nasdaq:0.58(n6056) ✅BOTH+
- s12/t25 · russell2k:0.72(n5804) · nasdaq:0.69(n6056) ✅BOTH+
- s12/t50 · russell2k:1.0(n5804) · nasdaq:0.77(n6056) ✅BOTH+
- s12/t100 · russell2k:1.04(n5804) · nasdaq:0.81(n6056) ✅BOTH+
- s15/t25 · russell2k:0.77(n5804) · nasdaq:0.69(n6056) ✅BOTH+
- s15/t50 · russell2k:1.01(n5804) · nasdaq:0.72(n6056) ✅BOTH+
- s15/t100 · russell2k:1.07(n5804) · nasdaq:0.76(n6056) ✅BOTH+
- s20/t25 · russell2k:0.85(n5804) · nasdaq:0.8(n6056) ✅BOTH+
- s20/t50 · russell2k:1.03(n5804) · nasdaq:0.79(n6056) ✅BOTH+
- s20/t100 · russell2k:1.06(n5804) · nasdaq:0.81(n6056) ✅BOTH+

**Configs positive in BOTH universes (acc_tr TEST, n≥30):** ['s8/t25', 's8/t50', 's8/t100', 's12/t25', 's12/t50', 's12/t100', 's15/t25', 's15/t50', 's15/t100', 's20/t25', 's20/t50', 's20/t100']


## PART B — tail capturability / fill realism

Reference config = **s15/t100** (best acc_tr(TEST) config).

### Liquidity floor — entry-day $-volume

| gate · uni | floor | n kept | n dropped | EXPECTANCY kept |
|---|---|---|---|---|
| acc_tr(TEST) · russell2k | $100k | 3556 | 2248 | 1.32 |
| acc_tr(TEST) · russell2k | $500k | 1827 | 3977 | 1.25 |
| acc_tr(TEST) · russell2k | $1000k | 1284 | 4520 | 2.4 |
| acc_tr(TEST) · nasdaq | $100k | 3642 | 2414 | 0.74 |
| acc_tr(TEST) · nasdaq | $500k | 1776 | 4280 | 0.52 |
| acc_tr(TEST) · nasdaq | $1000k | 1208 | 4848 | 1.31 |
| range_exp(TEST) · russell2k | $100k | 123 | 7 | 0.52 |
| range_exp(TEST) · russell2k | $500k | 85 | 45 | -0.44 |
| range_exp(TEST) · russell2k | $1000k | 60 | 70 | 3.43 |
| range_exp(TEST) · nasdaq | $100k | 146 | 8 | -1.65 |
| range_exp(TEST) · nasdaq | $500k | 100 | 54 | -2.47 |
| range_exp(TEST) · nasdaq | $1000k | 73 | 81 | 0.13 |

### Tail winners (realized ≥+100%) vs liquidity floor

| gate · uni | tail winners | <$100k | <$500k | <$1M |
|---|---|---|---|---|
| acc_tr(TEST) · russell2k | 202 | 70 | 135 | 147 |
| acc_tr(TEST) · nasdaq | 212 | 74 | 141 | 153 |
| range_exp(TEST) · russell2k | 9 | 0 | 4 | 4 |
| range_exp(TEST) · nasdaq | 9 | 0 | 4 | 4 |

### Gap-through tax + glitch screen (acc_tr(TEST), per uni)

| uni | n | stop exits | gapped-through-stop% | target exits | gapped-thru-target% | glitch flagged |
|---|---|---|---|---|---|---|
| russell2k | 5804 | 2294 | 7.8 | 202 | 25.2 | 11 |
| nasdaq | 6056 | 2485 | 7.9 | 212 | 25.5 | 14 |

### De-rated best config (≥$500k entry $-vol + glitch-dropped)

| gate · uni | raw EXP (n) | de-rated EXP (n) |
|---|---|---|
| acc_tr(TEST) · russell2k | 1.07 (n5804) | 1.22 (n1821) |
| acc_tr(TEST) · nasdaq | 0.76 (n6056) | 0.5 (n1769) |
| range_exp(TEST) · russell2k | 0.27 (n130) | -1.48 (n83) |
| range_exp(TEST) · nasdaq | -1.8 (n154) | -3.39 (n98) |
| acc_tr x range_exp(TEST) · russell2k | -7.31 (n28) | -10.53 (n18) |
| acc_tr x range_exp(TEST) · nasdaq | -7.62 (n31) | -11.17 (n21) |

## Per-year stability — acc_tr(TEST), ref config, ≥$500k + glitch-dropped

| year | russell2k EXP (n) | nasdaq EXP (n) |
|---|---|---|
| 2021 | -0.9 (n165) | -1.27 (n145) |
| 2022 | -0.94 (n348) | -1.26 (n323) |
| 2023 | 3.79 (n262) | -0.5 (n251) |
| 2024 | -0.69 (n289) | -0.78 (n308) |
| 2025 | 2.42 (n492) | 1.71 (n521) |
| 2026 | 2.7 (n265) | 4.3 (n221) |

## Horizon & entry sensitivity — acc_tr(TEST), ref config (≥$500k+glitch)

| variant | russell2k EXP (n) | nasdaq EXP (n) |
|---|---|---|
| h10 next-open | 1.22 (n1821) | 0.5 (n1769) |
| h20 next-open | 1.24 (n1819) | 0.32 (n1766) |
| h10 close-entry | 2.81 (n1821) | 2.2 (n1769) |
