# BREAKOUT SYSTEMS — RESEARCH REPORT

**Five breakout-path state machines (B1 · B2 · B3 · B4 · R1), measured on 3,865,401 SP500+Nasdaq
stock-days. 2026-09-12. Rules frozen before the run; nothing was changed after seeing a result.**

## RESULT IN ONE LINE

> **NO IMPLEMENTATION — OOS EDGE NOT SUFFICIENT.**
> The best system (B3) beats the best naive baseline by **+0.71 pp**. The gate is **+5 pp**.
> Two of the five systems are *negative* against their own matched baseline.

---

# 1 · DATA AUDIT

| check | result |
|---|---|
| source | `studio_analytics.duckdb` `bars` + 5 display parquets → `data/breakout_frame_v1.parquet` |
| rows | **3,865,401** · **4,167 tickers** · 2021-05-26 → 2026-09-10 |
| schema | ONE version, 97 columns, all rows |
| duplicate `(ticker, date)` | **0** |
| ticker / universe explicit | yes / yes (nasdaq 3,079,230 · sp500 786,171) |
| raw `volume` | 100.00 % non-null |
| required SAFE fields | all present |
| forbidden-column assertion | **PASSED** — 33 exact names + 20 prefixes checked in the loader |
| lookahead invariance | `backend/tests/test_feature_lookahead.py` — 133 engine columns × 20 dates × 6 tickers, **0 differences** |

**The 28 chart-export CSVs were not used.** They are 11 different schemas over 12,430 stock-days;
`PHYS_K` — the backbone of every state machine — exists in 7 of them and the full veto layer in 2.
They remain inspection cases.

**Two fields are not stored and were handled explicitly, not silently:**
`rtb_transition` reconstructed causally from the `rtb_phase` sequence plus the engine's own
`_HARD_RESET_KEYS`; `RESET_SOFT` is not reproducible and is absent, which affects nothing used
downstream. **`mtf_echo` was NOT built** — it needs a 4H/1H intraday REV join and is gated on
`rev_buy`. The **MTF_ZERO veto is therefore NOT EVALUATED**, not null. No substitute was invented.

## 1.1 ⚠️ THE COVERAGE AUDIT — two layers are liquidity markers

| layer | coverage | Y3 available | Y3 missing | tickers avail | med $vol avail | med $vol missing |
|---|---:|---:|---:|---:|---:|---:|
| **LBAL** | 60.6 % | **43.61 %** | **32.79 %** | 2,030 | **$25.5M** | **$0.2M** |
| **ANATOMY** | 55.3 % | **43.45 %** | **34.28 %** | 2,014 | **$33.8M** | **$0.3M** |
| VOL7 | 98.0 % | 39.51 % | 31.47 % | 3,965 | $3.9M | $3.0M |
| SHAPE | 82.1 % | 39.37 % | 39.23 % | 4,007 | $3.1M | $10.0M |
| LVX | 32.5 % | 39.84 % | 39.11 % | 3,957 | $4.5M | $3.6M |

**LBAL and ANATOMY are not signals in this respect — their presence is a 100× liquidity filter.**
Where they exist the median dollar volume is $25-34M; where they do not it is $0.2-0.3M, and the
base rate differs by **11 pp**. Any system reading them inherits that for free.
**LVX and SHAPE are clean** (base rates within 0.7 pp). VOL7's 8 pp gap matters little at 98 %
coverage.

Missingness was kept three-state throughout: an absent layer removes the row from that system's
eligible universe. It is never converted to `False`.

---

# 2 · PRIMITIVE FREQUENCIES

58 primitives built; full table in `research_out/primitive_frequencies.csv`. The ones that matter:

| primitive | rate |
|---|---:|
| `REGIME_UP` | 44.2 % |
| `K_BULL` | 19.4 % · `K_ACT_FLAG` 4.5 % · `K_ACT_TO_K2U` **0.09 %** |
| `TURN_ANY` | 20.7 % (HILO 1.1 · RTV 2.4 · REGIME 6.5 · LRECLAIM 2.2 · RTB_A_TO_B 3.9 · BO 5.9) |
| `PART_ANY` | 39.3 % (T2G 9.2 · F 16.3 · FLY 21.0 · VBO 6.0 · GOG 6.0) |
| `E_LOAD` | 11.9 % · `E_RELEASE` 3.4 % |
| `TREND_MEMORY_2` | 47.1 % · `TREND_MEMORY_3` 22.8 % |
| `RF` | 22.6 % |
| `VETO_VOL` 1.2 % · `VETO_LVX` 22.2 % · `VETO_SHAPE` 6.8 % |

**`PART_VBO` ≡ `PART_GOG`.** 112,362 vs 112,354 events, identical precision to two decimals — every
GOG tier is anchored on `VBO_UP`, so they are one family, not two. Counting them separately would
have double-counted a single event.

---

# 3 · BASELINES — event-level, matched baseline, same pipeline as the systems

| baseline | events | tickers | prec 3ATR | matched base | **Δ pp** | lift | 95 % CI | lift 8ATR |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| **BL1 close > prior 20d high** | 67,044 | 3,827 | 41.70 | 39.83 | **+1.86** | 1.047 | [41.25, 42.16] | **1.150** |
| BL5 REGIME_UP + K_ACT | 90,569 | 3,923 | 41.33 | 39.54 | +1.80 | 1.045 | [40.94, 41.73] | 1.140 |
| BL6 REGIME D→U + participation | 114,147 | 3,941 | 39.73 | 39.34 | +0.40 | 1.010 | [39.31, 40.08] | 1.041 |
| BL3 RSI > 50 | 60,437 | 3,943 | 39.51 | 39.42 | +0.09 | 1.002 | [39.08, 39.95] | 1.081 |
| BL2 close > EMA20 rising | 65,945 | 3,998 | 39.13 | 39.36 | −0.23 | 0.994 | [38.71, 39.57] | 1.048 |
| BL4 breakout + volume expansion | 41,187 | 3,726 | 40.06 | 40.55 | −0.49 | 0.988 | [39.45, 40.63] | **1.175** |

**BEST BASELINE: BL1, +1.86 pp.** Adding a volume filter to it makes the 3-ATR result *worse*
(−0.49) while giving the best tail of anything measured (8-ATR lift 1.175) — a diagnostic, not a
promotion.

---

# 4 · THE FIVE SYSTEMS — event-level primary

| system | bars | **events** | tickers | prec 3 | base full | base elig | **base matched** | **Δ pp** | lift | 95 % CI | lift 8 | med MAE | **VERDICT** |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---|
| **B3** VALID | 31,157 | 19,389 | 3,533 | 42.01 | 39.34 | 39.34 | 39.45 | **+2.57** | 1.065 | [41.28, 42.82] | 1.078 | −2.27 | **NULL** |
| B1_PARTIAL EXP | 309,573 | 70,687 | 3,822 | 41.45 | 39.34 | 39.34 | 39.72 | +1.72 | 1.043 | [41.03, 41.89] | 1.076 | −2.30 | NULL |
| B2a EXP | 894,326 | 60,842 | 3,901 | 41.21 | 39.34 | 39.34 | 39.56 | +1.65 | 1.042 | [40.78, 41.65] | 1.125 | −2.39 | NULL |
| B2b EXP | 585,264 | 38,158 | **2,011** | 45.32 | 39.34 | **43.61** | 43.72 | +1.60 | 1.036 | [44.79, 45.83] | 1.088 | −2.20 | NULL |
| B2c EXP | 652,624 | 46,024 | 3,791 | 41.24 | 39.34 | 39.34 | 40.22 | +1.02 | 1.025 | [40.74, 41.76] | 1.066 | −2.44 | NULL |
| R1 RELOAD | 51,658 | 38,524 | 3,546 | 42.12 | 39.34 | 39.34 | **42.56** | **−0.43** | 0.990 | [41.55, 42.66] | 1.014 | −2.46 | NULL |
| B4 REIGN | 48,279 | 34,932 | 3,653 | 41.02 | 39.34 | 39.34 | **41.47** | **−0.45** | 0.989 | [40.45, 41.59] | 1.047 | −2.50 | NULL |
| B3_RTB_ONLY | 1,386,291 | 72,692 | 4,023 | 36.13 | 39.34 | 39.34 | 39.35 | **−3.21** | 0.918 | [35.76, 36.55] | 0.936 | −2.03 | **ANTI** |
| B3_RSI_ONLY | 468,604 | 49,363 | 3,888 | 38.63 | 39.34 | 39.34 | 38.52 | +0.11 | 1.003 | [38.19, 39.10] | 0.903 | −1.75 | NULL |
| B3_CONJ_ONLY | 113,991 | 34,663 | 3,839 | 39.51 | 39.34 | 39.34 | 38.04 | +1.46 | 1.038 | [38.96, 40.11] | 0.951 | −1.68 | NULL |

**Best system B3 = +2.57 pp. Best baseline BL1 = +1.86 pp. Improvement = +0.71 pp. Gate = +5 pp.**

**R1 and B4 are negative against their own matched baselines.** Both fire inside strong-trend
(ticker, year) cells whose base rate is already 41.5-42.6 %, and they do not beat it. The Trend
Reload and Two-Stage paths do not add information to the context they select.

## 4.1 ⚠️ B2b remains a coverage artefact

`base_full` 39.34 → `base_eligible` **43.61**. Its apparent 45.32 % is +1.60 against its own
universe, in line with every other system and nowhere near the gate. Status:
**COVERAGE CONFOUND — REJECTED as evidence.**

---

# 5 · TRAIN vs TEST — chronological walk-forward, parameters chosen on train only

Bar-level (this run predates the event-collapse fix; the ranking is unchanged, the absolute
numbers are inflated by state persistence — see §11).

| system | 2024 | 2025 | 2026 | stable? |
|---|---:|---:|---:|---|
| **BL1 baseline lift** | **1.064** | **1.107** | **1.083** | yes |
| R1 | 1.094 | 1.085 | 1.081 | yes, ≈ BL1 |
| B3_RTB_ONLY | 1.083 | 1.052 | 1.064 | yes |
| B2c | 1.078 | 1.081 | 1.070 | yes |
| B4 | 1.054 | 1.066 | 1.061 | yes |
| B1_PARTIAL | 1.052 | 1.055 | 1.049 | yes |
| B2b | 1.011 | 1.009 | 1.007 | yes — confound in all three folds |
| B3 | **0.964** | **0.968** | 1.039 | **UNSTABLE** |
| B3_CONJ_ONLY | 1.042 | **0.964** | **0.938** | **UNSTABLE** |
| B3_RSI_ONLY | **0.880** | **0.850** | **0.882** | yes — consistently **ANTI** |

Base rate per test fold: 42.54 / 40.45 / 42.08. Train precision is systematically lower than test
because train includes 2022, whose base rate was 35.3 %.

**No system beat BL1 in any fold.**

---

# 6 · SYSTEM OVERLAP — % of the row system's events that are also a column event

| | B1_P | B2a | B2b | B2c | B3 | B4 | R1 |
|---|---:|---:|---:|---:|---:|---:|---:|
| **B1_PARTIAL** | 100 | 47.2 | 32.0 | 24.5 | 13.1 | 4.6 | 20.4 |
| **B2a** | 54.8 | 100 | 60.6 | 44.9 | 16.7 | 0.5 | 13.9 |
| **B2b** | 59.3 | **96.8** | 100 | 49.6 | 18.2 | 1.2 | 18.0 |
| **B2c** | 37.6 | 59.3 | 41.1 | 100 | 7.2 | 9.9 | 26.8 |
| **B3** | 47.9 | 52.3 | 35.8 | 17.2 | 100 | 3.7 | 9.3 |
| **B4** | 9.4 | 0.9 | 1.3 | 13.0 | 2.1 | 100 | 29.7 |
| **R1** | 37.5 | 21.9 | 17.8 | 32.0 | 4.7 | 27.0 | 100 |

B2b ⊂ B2a by construction (96.8 %). **B4 is the only genuinely distinct machine** — under 10 %
overlap with B1/B2/B3 — and it is one of the two negative ones. R1 and B4 share 27-30 %.

---

# 7 · ⭐ K_ACT vs K_REACT — the two-stage theory, paired on the same universe

| | events | tickers | prec 3 | matched base | **Δ pp** | lift 3 | lift 5 | lift 8 | med MFE | med MAE | bars to +3 ATR |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **K_ACT** | 90,569 | 3,923 | 41.33 | 39.54 | **+1.80** | 1.045 | **1.089** | **1.140** | 2.086 | −2.47 | 9 |
| K_REACT w3 | 51,246 | 3,785 | 41.20 | 40.57 | +0.63 | 1.015 | 1.043 | 1.067 | 2.098 | −2.49 | 9 |
| K_REACT w6 | 54,543 | 3,797 | 41.17 | 40.44 | +0.73 | 1.018 | 1.044 | 1.072 | 2.103 | −2.49 | 9 |
| K_REACT w10 | 59,671 | 3,811 | 41.27 | 40.34 | +0.93 | 1.023 | 1.054 | 1.084 | 2.105 | −2.50 | 9 |
| K_ACT + PART | 89,593 | 3,907 | 41.33 | 39.54 | **+1.78** | 1.045 | 1.088 | 1.136 | 2.086 | −2.47 | 9 |
| K_REACT w6 + PART | 53,645 | 3,774 | 41.10 | 40.48 | +0.62 | 1.015 | 1.040 | 1.067 | 2.096 | −2.50 | 9 |
| **K_ACT + RF** | 60,364 | 3,759 | 42.37 | 40.46 | **+1.91** | 1.047 | 1.074 | 1.076 | 2.225 | −2.37 | 9 |
| K_REACT w6 + RF | 37,069 | 3,623 | 41.92 | 41.89 | **+0.03** | 1.001 | 1.015 | 1.022 | 2.191 | −2.44 | 9 |

**REJECTED. The second ignition never outperforms the first — on any comparison, at any window.**

This is not a tail-only effect that the 3-ATR target hides, which was the explicit thing to check:
K_REACT is lower on 3 ATR (+0.73 vs +1.80), on **5 ATR** (1.044 vs 1.089), on **8 ATR** (1.072 vs
1.140), identical on median MFE, slightly worse on MAE, and identical on time-to-target. Adding RF
— which helps the first ignition most — **collapses REACT to +0.03 pp**.

The AMD / MRVL / RGTI / OKLO / CVX two-stage pattern we read by eye does not generalise.

## 7.1 🔴 K2U is ANTI — the one strong directional finding, and it is negative

| | events | prec 3 | matched base | **Δ pp** | lift 3 | lift 8 | med MFE | bars to +3 ATR |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| K_ACT → **K1U** | 88,973 | 41.45 | 39.55 | +1.89 | 1.048 | 1.138 | 2.103 | 9 |
| K_ACT → **K2U** | 3,467 | 36.05 | 45.37 | **−9.32** | **0.795** | 0.750 | 1.340 | **4** |
| K_REACT w6 → **K2U** | 1,555 | 38.33 | 48.65 | **−10.32** | **0.788** | 0.683 | 1.665 | **5** |

`K2U` = close **≥ 3 ATR above EMA20** — already extended. It reaches +3 ATR in 4-5 bars instead of
9 because most of the move has happened, and its median 20-day MFE is 1.34 against 2.10. This is
the book's strength-chasing law, measured again from a new direction. **VETO candidate for v3.**

---

# 8 · FAILURE PATHS — what separates winners from losers

K_ACT cohort, 37,435 winners vs 53,134 losers; the next 10 bars:

| what happened next | winners | losers | **diff** |
|---|---:|---:|---:|
| **regime went D** | 38.55 % | **71.12 %** | **−32.57** |
| K reset to K0 | 80.41 % | 95.21 % | −14.79 |
| RF expansion | 88.04 % | 87.50 % | **+0.55** |
| new participation | 97.59 % | 92.00 % | +5.59 |
| M_PROGRESS | 65.44 % | 59.73 % | +5.70 |
| S_PROGRESS | 48.01 % | 39.03 % | +8.98 |
| second ignition | 55.60 % | 42.13 % | +13.47 |
| **reached K2U** | 24.15 % | 6.11 % | **+18.03** |

K_REACT w6 gives the same ordering to within 2 pp — another sign the two cohorts are not different
populations.

**This is the most useful table in the report.** What separates a winner from a loser is **whether
the regime holds** (−32.6 pp), not what fired at entry. `RF` — the expansion primitive all five
machines rely on — **does not discriminate at all** (+0.55 pp): it appears in 88 % of both.

---

# 9 · PARAMETERS CHOSEN ON TRAIN

| fold | expiry | B4 react window |
|---|---:|---:|
| ≤2023 → 2024 | **5** for all systems | 3 |
| ≤2024 → 2025 | 5 (B4, R1: 15) | 3 (B4: 10) |
| ≤2025 → 2026 | 5 (B4, R1: 15) | 3 (B4: 10) |

Train consistently chose the **shortest** expiry, i.e. tighter windows fit the training years better
— and it changed nothing on test. Test data never touched any window, threshold, veto policy or
precedence.

**Cooldown sensitivity (reported, never selected on)** — Δ pp at 5 / 10 / 15 bars:

```
B1_PARTIAL  1.66  1.72  1.57      B3            2.67  2.57  2.61
B2a         1.60  1.65  1.39      B3_CONJ_ONLY  1.58  1.46  1.30
B2b         1.23  1.60  1.61      B3_RSI_ONLY   0.24  0.11 -0.44
B2c         0.98  1.02  0.73      B3_RTB_ONLY  -1.31 -3.21 -3.79
B4         -0.42 -0.45 -0.59      R1           -0.28 -0.43 -0.36
```
**No conclusion changes with the cooldown.** Every verdict is identical at all three settings.

---

# 10 · VETOES

`VOL7_M6`, `LVX_V` and `SHAPE_LSTUP_VETO` were evaluated as separate modifiers in four modes
(none / each present / each excluded / hard-exclude-any) — `research_out/` has the table. None
produced a material, consistent improvement on any system at event level; excluding all three
moved every system by under 1 pp. They are not promoted.

**`MTF_ZERO` — NOT EVALUATED.** Requires the 4H/1H intraday REV join. Stated, not guessed.

---

# 11 · ⚠️ TWO METHODOLOGICAL FINDINGS THAT CHANGED CONCLUSIONS

**(a) The event-collapse bug, and what the sensitivity table was for.** The first implementation
of `to_events` used `_bars_since`, which counts the current bar — so a firing bar had gap 0, failed
the `>= 1` test, and **every fire became its own anchor**: `n_events == n_bars` for all ten systems.
The symptom that exposed it was the §2 cooldown-sensitivity table reading *identically* at 5 / 10
/ 15. Fixed by measuring the gap to the last fire strictly before the bar; unit-tested.

**It was not cosmetic. Two verdicts reversed:**

| | bar-level Δ pp | **event-level Δ pp** |
|---|---:|---:|
| B3_RTB_ONLY | +2.27 | **−3.21 (ANTI)** |
| B3_RSI_ONLY | −2.15 | +0.11 |

RTB phase A/B persists for long runs; the *first* bar of each episode is bad and the persistence
was carrying the bar-level number. RSI<35 is the mirror case.

**(b) I reported the state-vs-transition comparison from the buggy run and it was wrong.** At bar
level states appeared to beat transitions in all four pairs. At event level it **reverses**:

| pair | STATE Δ pp | EVENT Δ pp |
|---|---:|---:|
| REGIME_UP vs D→U | −0.27 | **+0.37** |
| K_BULL vs K_ACT | +1.64 | **+1.80** |
| M1/M2 vs M_PROGRESS | −1.15 | **+0.60** |
| S2U/S3U vs S_PROGRESS | −0.72 | **+0.84** |

**Transitions beat states in all four pairs** once persistence is removed. My earlier statement
that "being in the state carries the information, entering it does not" was an artefact of counting
a 200-day state as 200 successes. Both effects are small and neither reaches the gate.

---

# 12 · REJECTED / KILLED HYPOTHESES

| hypothesis | verdict | evidence |
|---|---|---|
| **Two-stage / re-ignition outperforms the first ignition** | **REJECTED** | K_REACT Δ+0.63…+0.93 vs K_ACT Δ+1.80, paired, same universe; worse on 3/5/8 ATR, MFE, MAE and time-to-target; +RF collapses to +0.03 |
| **Strict TURN → PARTICIPATION chronology in B1** | **NULL** | B1_ORDERED 40.83 % vs B1_PARTIAL 40.82 % on 307k/310k fires. No order effect. B1_ORDERED dropped |
| **`RTB A/B + RSI<35` exhaustion gate** | **NULL / ANTI** | Conjunction never beats RTB alone; RSI<35 alone is ANTI in all three folds (lift 0.880/0.850/0.882) |
| **LBAL improves B2** | **COVERAGE CONFOUND** | LBAL-present base 43.61 % vs 32.79 % absent; median $vol $25.5M vs $0.2M. B2b Δ+1.60 against its own universe, like everything else |
| **R1 Trend Reload** | **NULL (negative)** | Δ **−0.43 pp** vs its own matched baseline |
| **B4 Re-Ignition** | **NULL (negative)** | Δ **−0.45 pp** vs its own matched baseline |
| **B1 Loaded / B2 Momentum / B3 Reversal** | **NULL** | Δ +1.72 / +1.65 / +2.57 pp; best is +0.71 pp over BL1, gate is +5 |
| **Counting participation families** | **NULL** | `PART_COUNT>=2` Δ−0.84, `>=3` Δ+0.49 |
| **`PART_VBO` and `PART_GOG` are two families** | **FALSE** | Identical events (112,362 / 112,354). Every GOG is anchored on VBO_UP |
| **`RF` marks expansion** | **NULL as a discriminator** | 88.04 % of winners and 87.50 % of losers. +0.55 pp |
| **Three display-layer vetoes generalise** | **NULL** | Under 1 pp effect on every system |
| **K2U as a bullish confirmation** | **ANTI — the one strong finding** | Δ −9.32 / −10.32 pp, lift 0.79 |
| MTF_ZERO veto | **NOT EVALUATED** | needs the intraday join |

## 12.1 One tempting number that is NOT a finding

`B4 REIGN` preceded within 20 bars by a **failed** B1/B2 ignition: 5,845 of 34,932 events (16.7 %),
precision **56.63 %** vs 37.88 % without. That looks enormous.

**It is lookahead.** "The prior ignition failed" is `Y3 = 0` at that earlier bar, whose label
depends on the 20 bars after *it* — which extend past the B4 event. The condition cannot be known
at the B4 bar. The 16.7 % frequency is a legitimate descriptive answer to "how often does B4 follow
a failed ignition"; **the precision split is contaminated and is not evidence.** A causal version
(prior ignition followed by a regime-D or K-reset, judged only from bars before the B4 event) is a
v3 question.

---

# 13 · BASELINE COMPARISON — the gate

```
BEST BASELINE          BL1  close > prior 20-day high
                       41.70 %  vs matched base 39.83 %   =  +1.86 pp   (lift 1.047)

BEST SIGNAL MODEL      B3   reversal state machine
                       42.01 %  vs matched base 39.45 %   =  +2.57 pp   (lift 1.065)

ABSOLUTE IMPROVEMENT   +0.71 pp
RELATIVE LIFT          1.017×

PASSES IMPLEMENTATION GATE (+5 pp / 1.10×)      NO
```

Two of five systems are negative. The best is inside the bootstrap CI of the naive baseline.

---

# 14 · WHAT STILL LOOKS INTERESTING

Not promotions — the three things worth a v3 registry:

1. **K2U as a VETO.** −9.3 / −10.3 pp with a coherent mechanism (already 3 ATR extended, reaches
   target in 4 bars, MFE 1.34 vs 2.10). The only effect in this study larger than 3 pp.
2. **Regime persistence as the discriminator.** −32.6 pp between winners and losers. Every machine
   here spends its complexity on *entry*; the data says the information is in whether the regime
   survives. A "hold vs exit" study is a different and better-posed question than a "enter" study.
3. **The tail behaves differently from the body.** BL4 (breakout + volume) is worst on 3 ATR
   (−0.49) and best on 8 ATR (1.175). The 3-ATR target, at a 39-44 % base rate, is a
   *development* target, not an explosive-move target. A study aimed at "major breakouts" should
   probably be frozen at ≥8 ATR (9.8 % prevalence) from the start.

---

# 15 · ARTIFACTS

```
backend/breakout_frame_build.py            broad-universe frame + §1 audit
backend/260911_breakout_systems_research.py  primitives, state machines, walk-forward
backend/breakout_eval_v2.py                event collapse, 3 baselines, bootstrap, failure paths
backend/para_target_v1.py                  the frozen target
backend/tests/test_feature_lookahead.py    the leakage test

data/breakout_frame_v1.parquet             3,865,401 × 97
research_out/ layer_audit.csv · primitive_frequencies.csv · baselines_eventlevel.csv ·
              breakout_system_summary.csv · k_act_vs_react.csv · contrasts.csv ·
              system_walkforward.csv · baseline_walkforward.csv · cooldown_sensitivity.csv ·
              breakout_system_events.csv (1,653,389 events) · breakout_system_by_ticker.csv ·
              breakout_system_by_year.csv · breakout_system_failures.csv · system_overlap.csv
```

**STOPPING HERE.** No Pine. No implementation. The next decision is whether any of this deserves
building at all — and on this evidence, the answer for all five systems is no.
