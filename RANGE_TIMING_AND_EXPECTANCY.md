# range_exp fire-timing + expectancy

_Follow-up to DISCRIMINATOR_VALIDATION.md. PART A recomputes bar-level timing for the range_exp subset; PART B uses the cached 1.1M-episode parquet. Units = percent. Headline numbers exclude PAVS/WNW/GLOO (OOS). No production code touched._

---

# VERDICT (read first)

### 1. Is range_exp predictive or lagging? → **PREDICTIVE, but only the TEST/absorption variant — and not on close-entry.**
range_exp is **not merely a launch-bar artifact.** Splitting the OOS subset by fire-timing:

| variant | what it is | n (r2k / nas) | close_pos | same-bar | ext vs 20d-low | **P(+50%)** | mean fwd% |
|---|---|---|---|---|---|---|---|
| **TEST** | wide range, spike **rejected**, closed low, **not extended** | 161 / 190 | 0.33 | ~2% | ~21% | **21.1 / 16.8** | −4.1 / −9.1 |
| **LAUNCH** | closed near high, move **already underway**, extended | 43 / 41 | 0.80 | ~34% | ~73% | 25.6 / 26.8 | −15.9 / −21.4 |

- The **TEST** bar — the genuinely *pre*-breakout one — **still carries ~17–21% P(+50%)** (≈8–10× the universe base of 1.8–2.6%) on n≥30. So the wide-range *absorption* bar precedes explosive moves: range_exp has real forward signal, it is **not** purely coincident.
- **LAUNCH** bars have even higher P(+50%) but are already **~73% above their 20-day low**, so entering on their close gives back far more (mean fwd −16 to −21% vs −4 to −9% for TEST). That portion is momentum-continuation, not anticipation.
- **32% of all range_exp bars are already >50% above the 20d-low, 16% >100%, 8% >200%** — a third are extended-at-trigger and should not be bought on close.
- **Caveat that dominates everything:** even TEST has a **negative mean fwd_10d on close-entry.** The signal ranks the tail; the close is a bad entry.

### 2. Does any gate have POSITIVE expectancy with a tail fat enough to trade? → **NO — not on a buy-the-close / hold-10d basis.**
Mean fwd_10d (the decision number) is **negative for every explosive gate** in small/micro, and gets *more* negative as conviction rises:

| gate (OOS) | russell2k EXP | nasdaq EXP | P(loss) | mean loss | mean win | payoff | cap-gap |
|---|---|---|---|---|---|---|---|
| HIGH (≥2) | **−0.40** | **−0.88** | 57–59% | −14 to −16 | +21 to +23 | 1.47 | 11–13 |
| acc_tr | **−0.48** | **−0.88** | 60% | −15 | +23 | 1.5 | 13–14 |
| range_exp | **−5.78** | **−7.48** | 67–72% | −25 to −26 | **+69 to +76** | 2.7–3.0 | **25–27** |
| acc_tr × range_exp | **−11.72** | **−14.0** | 75–78% | −29 | **+142 to +147** | **5.0–5.1** | **34–36** |

- The right tail is **genuinely fat** — range_exp mean *winner* = +69–76%, acc_tr×range_exp = **+142–147%**, payoff ratio up to **5.1**, P(+200%) 1.6–3.0%. But it is **swamped** by a 67–78% loss rate and −25 to −29% mean losses → **net mean stays negative**, and **exp/std is negative** too (−0.11 to −0.26). The fatter the tail, the fatter the losses; on close-entry they do not net out.
- Boring bars win this comparison: **LOW(<2) / score 0–1 are the only mildly-positive segments** (EXP +0.1 to +0.3, market drift). The discriminator deliberately selects **negative-mean, fat-tail lottery bars.**
- **sp500** is positive everywhere (acc_tr EXP +1.96) but P(+50%)≈0 — wrong regime, no explosive move to capture. Ignore.

### 3. What exit is required, and is it tradeable? → **Close-exit = negative. The 25–36pp capture-gap means an MFE-capturing exit is mandatory; only then is it plausibly tradeable — and only on TEST/non-extended entries.**
- The **capture gap** (median MFE − median fwd) is **26–36pp** for the range_exp gates: holding to the 10-day **close throws away 26–36 points of favorable excursion.** That single fact explains most of the negative close-expectancy — the move happens and is given back.
- Therefore the edge can be realized **only** with: (a) entry filtered to **TEST-type, <50%-above-20d-low** bars (better mean, less given back), **and** (b) a **target / trailing exit** (scale out at +25/+50/+100, trail the remainder) with a **tight initial stop** sized to the −15 to −29% typical adverse move. Buy-and-hold-to-close is structurally negative.
- This is a **hypothesis the data motivates but does not prove** — it requires a stop/target backtest, not assumed here.

### Recommendation
**Keep range_exp / the high-score discriminator as a WATCHLIST TAG, not a tradeable buy-the-close scanner flag.** Surface explosive candidates (russell2k/nasdaq only), **prefer TEST-type, non-extended bars**, and label it "high tail-potential, negative close-expectancy — needs target/trailing exit." Promote to a tradeable flag **only after** a follow-up backtest proves an MFE-capturing exit on the TEST/non-extended subset turns expectancy positive. As-is, close-to-close, it is **not** tradeable. No production scoring changed.

---

## PART A — is range_exp PREDICTIVE or LAGGING?

range_exp episodes with timing fields (OOS, ex-PAVS/WNW/GLOO): **1152** (nasdaq 598, russell2k 554, sp500 0).

- 32% of range_exp bars are already **>50% above their 20-day low** (extended).
- 16% of range_exp bars are already **>100% above their 20-day low** (extended).
- 8% of range_exp bars are already **>200% above their 20-day low** (extended).
- median close_pos = 0.62, median same-bar return = 3.7%, median %-from-20d-low = 29%.

**TEST/absorption** = close_pos<0.5 & same-bar<20% (spike rejected). **LAUNCH** = close_pos≥0.5 & same-bar≥20% (move underway). MIXED = the rest.

| universe · bucket | n | close_pos | same-bar% | ext%(20dlow) | mean fwd% | med fwd% | P(+50%) | P(+100%) | win% |
|---|---|---|---|---|---|---|---|---|---|
| russell2k · TEST | 161 | 0.33 | 1.8 | 22.2 | -4.09 | -10.51 | 21.1 | 9.9 | 27.3 |
| russell2k · LAUNCH | 43 | 0.8 | 34.2 | 76.5 | -15.92 | -18.81 | 25.6 | 7.0 | 25.6 |
| russell2k · MIXED | 350 | 0.71 | 3.9 | 29.2 | -5.31 | -10.62 | 20.9 | 7.1 | 33.7 |
| russell2k · ALL range_exp | 554 | 0.61 | 3.8 | 30.3 | -5.78 | -11.09 | 21.3 | 7.9 | 31.2 |
| nasdaq · TEST | 190 | 0.33 | 1.9 | 20.6 | -9.07 | -12.27 | 16.8 | 6.8 | 21.1 |
| nasdaq · LAUNCH | 41 | 0.79 | 33.9 | 69.6 | -21.37 | -27.06 | 26.8 | 9.8 | 17.1 |
| nasdaq · MIXED | 367 | 0.71 | 3.9 | 27.5 | -5.11 | -11.0 | 20.2 | 7.4 | 30.8 |
| nasdaq · ALL range_exp | 598 | 0.59 | 3.7 | 27.9 | -7.48 | -12.23 | 19.6 | 7.4 | 26.8 |
| | | | | | | | | | |
| [supp] r2k+nasdaq · TEST | 351 | 0.33 | 1.9 | 20.8 | -6.79 | -11.43 | 18.8 | 8.3 | 23.9 |
| [supp] r2k+nasdaq · LAUNCH | 84 | 0.79 | 34.0 | 73.1 | -18.58 | -26.88 | 26.2 | 8.3 | 21.4 |
| [supp] r2k+nasdaq · MIXED | 717 | 0.71 | 3.9 | 28.1 | -5.21 | -10.82 | 20.5 | 7.3 | 32.2 |


## PART B — expectancy + payoff distribution

EXPECTANCY = mean fwd_10d per trade (clipped [-100,500]). cap-gap = median(MFE)−median(fwd) = tail given back if you exit on close. payoff = mean_win/|mean_loss|.


### russell2k  (n=598,561, OOS)

#### Distribution & risk-adjusted

| segment | n | EXPECTANCY | p5 | p25 | p50 | p75 | p90 | p95 | p99 | std | exp/std | cap-gap |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| score 0 | 423576 | **0.12** | -16.2 | -5.0 | -0.2 | 4.5 | 10.6 | 16.4 | 36.0 | 12.9 | 0.009 | 4.9 |
| score 1 | 153645 | **0.32** | -21.7 | -6.8 | -0.5 | 5.3 | 13.9 | 22.6 | 56.3 | 19.9 | 0.016 | 6.6 |
| score 2 | 19834 | **-0.27** | -32.3 | -11.7 | -1.9 | 6.4 | 19.4 | 34.1 | 99.9 | 29.4 | -0.009 | 10.8 |
| score 3 | 1496 | **-2.04** | -46.2 | -20.6 | -6.8 | 6.2 | 27.1 | 53.9 | 155.8 | 44.9 | -0.045 | 20.7 |
| score 4 | 10 | **-27.07** | -57.7 | -37.8 | -26.4 | -17.0 | -11.1 | 1.9 | 12.3 | 22.0 | -1.229 | 42.2 |
| LOW (<2) | 577221 | **0.17** | -17.7 | -5.4 | -0.2 | 4.7 | 11.4 | 17.9 | 41.8 | 15.1 | 0.011 | 5.3 |
| HIGH (>=2) | 21340 | **-0.4** | -33.6 | -12.3 | -2.2 | 6.4 | 19.9 | 34.8 | 102.8 | 30.8 | -0.013 | 11.4 |
| gate: acc_tr | 26849 | **-0.48** | -33.8 | -13.7 | -3.3 | 6.2 | 21.4 | 37.9 | 112.2 | 33.2 | -0.014 | 13.3 |
| gate: range_exp | 554 | **-5.78** | -55.7 | -27.5 | -11.1 | 4.8 | 27.7 | 55.9 | 210.1 | 51.2 | -0.113 | 26.5 |
| gate: acc_tr x range_exp | 111 | **-11.72** | -62.0 | -36.4 | -18.8 | 0.2 | 17.0 | 43.3 | 72.5 | 57.0 | -0.206 | 35.9 |

#### Payoff decomposition

| segment | n | EXPECTANCY | P(loss)% | mean loss | mean MAE | P(win)% | mean win | payoff | P(+50%) | P(+100%) | P(+200%) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| score 0 | 423576 | **0.12** | 50.9 | -7.1 | -7.1 | 48.4 | 7.9 | 1.1 | 1.2 | 0.3 | 0.1 |
| score 1 | 153645 | **0.32** | 52.1 | -9.4 | -9.3 | 47.1 | 11.5 | 1.22 | 3.4 | 1.1 | 0.3 |
| score 2 | 19834 | **-0.27** | 56.2 | -14.0 | -13.9 | 42.7 | 20.2 | 1.45 | 8.7 | 3.5 | 1.1 |
| score 3 | 1496 | **-2.04** | 63.8 | -20.3 | -20.9 | 35.3 | 37.4 | 1.85 | 16.6 | 7.0 | 2.5 |
| score 4 | 10 | **-27.07** | 90.0 | -31.7 | -34.7 | 10.0 | 14.9 | 0.47 | 10.0 | 10.0 | 0.0 |
| LOW (<2) | 577221 | **0.17** | 51.2 | -7.8 | -7.7 | 48.0 | 8.8 | 1.14 | 1.8 | 0.5 | 0.2 |
| HIGH (>=2) | 21340 | **-0.4** | 56.8 | -14.5 | -14.4 | 42.2 | 21.3 | 1.47 | 9.2 | 3.7 | 1.2 |
| gate: acc_tr | 26849 | **-0.48** | 59.7 | -14.7 | -15.3 | 38.9 | 23.6 | 1.6 | 10.4 | 4.2 | 1.4 |
| gate: range_exp | 554 | **-5.78** | 67.3 | -25.2 | -26.6 | 31.2 | 69.2 | 2.74 | 21.3 | 7.9 | 2.9 |
| gate: acc_tr x range_exp | 111 | **-11.72** | 74.8 | -28.7 | -30.6 | 25.2 | 142.1 | 4.96 | 18.9 | 5.4 | 1.8 |

### nasdaq  (n=415,403, OOS)

#### Distribution & risk-adjusted

| segment | n | EXPECTANCY | p5 | p25 | p50 | p75 | p90 | p95 | p99 | std | exp/std | cap-gap |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| score 0 | 275313 | **0.01** | -18.5 | -5.9 | -0.4 | 4.7 | 12.0 | 19.2 | 43.1 | 14.9 | 0.0 | 5.7 |
| score 1 | 119961 | **0.13** | -24.2 | -8.1 | -0.9 | 5.6 | 15.8 | 25.8 | 63.7 | 22.0 | 0.006 | 7.8 |
| score 2 | 18467 | **-0.68** | -34.6 | -13.3 | -2.8 | 6.6 | 20.7 | 36.4 | 106.8 | 31.2 | -0.022 | 12.6 |
| score 3 | 1649 | **-2.97** | -48.7 | -21.2 | -7.6 | 5.2 | 26.1 | 52.0 | 151.5 | 44.2 | -0.067 | 21.3 |
| score 4 | 13 | **-25.05** | -53.8 | -30.2 | -22.4 | -16.2 | -14.1 | -2.4 | 11.5 | 19.5 | -1.283 | 33.7 |
| LOW (<2) | 395274 | **0.04** | -20.3 | -6.5 | -0.5 | 4.9 | 13.1 | 21.1 | 49.4 | 17.4 | 0.003 | 6.3 |
| HIGH (>=2) | 20129 | **-0.88** | -35.8 | -13.9 | -3.1 | 6.5 | 21.1 | 37.5 | 108.9 | 32.5 | -0.027 | 13.2 |
| gate: acc_tr | 27461 | **-0.88** | -35.2 | -14.4 | -3.6 | 6.2 | 21.7 | 38.9 | 113.6 | 33.4 | -0.026 | 14.1 |
| gate: range_exp | 598 | **-7.48** | -58.5 | -30.0 | -12.2 | 1.7 | 20.2 | 51.9 | 222.1 | 53.1 | -0.141 | 25.2 |
| gate: acc_tr x range_exp | 127 | **-14.0** | -64.3 | -36.3 | -20.5 | -1.6 | 14.9 | 27.8 | 72.1 | 53.7 | -0.261 | 33.3 |

#### Payoff decomposition

| segment | n | EXPECTANCY | P(loss)% | mean loss | mean MAE | P(win)% | mean win | payoff | P(+50%) | P(+100%) | P(+200%) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| score 0 | 275313 | **0.01** | 52.4 | -8.0 | -8.1 | 46.8 | 9.2 | 1.15 | 1.8 | 0.5 | 0.1 |
| score 1 | 119961 | **0.13** | 53.8 | -10.5 | -10.6 | 45.2 | 13.0 | 1.24 | 4.4 | 1.4 | 0.5 |
| score 2 | 18467 | **-0.68** | 58.0 | -15.0 | -15.2 | 40.9 | 21.7 | 1.45 | 9.9 | 3.9 | 1.3 |
| score 3 | 1649 | **-2.97** | 65.3 | -20.4 | -21.3 | 33.7 | 37.0 | 1.81 | 16.1 | 6.9 | 2.4 |
| score 4 | 13 | **-25.05** | 92.3 | -28.4 | -33.0 | 7.7 | 14.9 | 0.53 | 15.4 | 7.7 | 0.0 |
| LOW (<2) | 395274 | **0.04** | 52.8 | -8.8 | -8.8 | 46.3 | 10.4 | 1.18 | 2.6 | 0.7 | 0.2 |
| HIGH (>=2) | 20129 | **-0.88** | 58.6 | -15.5 | -15.7 | 40.3 | 22.8 | 1.47 | 10.4 | 4.2 | 1.4 |
| gate: acc_tr | 27461 | **-0.88** | 60.1 | -15.4 | -15.9 | 38.7 | 22.5 | 1.46 | 11.1 | 4.4 | 1.5 |
| gate: range_exp | 598 | **-7.48** | 71.6 | -25.6 | -27.7 | 26.8 | 76.3 | 2.99 | 19.6 | 7.4 | 3.0 |
| gate: acc_tr x range_exp | 127 | **-14.0** | 78.0 | -28.7 | -30.9 | 21.3 | 146.9 | 5.11 | 18.1 | 4.7 | 1.6 |

### sp500  (n=104,847, OOS)

#### Distribution & risk-adjusted

| segment | n | EXPECTANCY | p5 | p25 | p50 | p75 | p90 | p95 | p99 | std | exp/std | cap-gap |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| score 0 | 85826 | **0.32** | -10.9 | -3.4 | 0.3 | 3.9 | 7.9 | 11.3 | 20.6 | 7.6 | 0.042 | 3.4 |
| score 1 | 18059 | **0.62** | -11.9 | -3.4 | 0.5 | 4.3 | 8.9 | 12.7 | 25.0 | 8.2 | 0.075 | 3.6 |
| score 2 | 959 | **1.06** | -13.6 | -3.4 | 0.6 | 5.1 | 10.5 | 16.6 | 31.9 | 10.3 | 0.103 | 4.1 |
| score 3 | 3 | **5.04** | -9.4 | -8.4 | -7.2 | 12.4 | 24.1 | 28.1 | 31.2 | 23.4 | 0.216 | 9.1 |
| score 4 | 0 | — | — | — | — | — | — | — | — | — | — | — | — | — |
| LOW (<2) | 103885 | **0.37** | -11.1 | -3.4 | 0.4 | 4.0 | 8.1 | 11.6 | 21.3 | 7.7 | 0.048 | 3.4 |
| HIGH (>=2) | 962 | **1.08** | -13.6 | -3.4 | 0.6 | 5.1 | 10.5 | 17.0 | 32.0 | 10.3 | 0.104 | 4.1 |
| gate: acc_tr | 218 | **1.96** | -25.5 | -7.2 | 0.7 | 6.8 | 20.5 | 40.0 | 68.6 | 19.3 | 0.101 | 5.7 |
| gate: range_exp | 0 | — | — | — | — | — | — | — | — | — | — | — | — | — |
| gate: acc_tr x range_exp | 0 | — | — | — | — | — | — | — | — | — | — | — | — | — |

#### Payoff decomposition

| segment | n | EXPECTANCY | P(loss)% | mean loss | mean MAE | P(win)% | mean win | payoff | P(+50%) | P(+100%) | P(+200%) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| score 0 | 85826 | **0.32** | 47.4 | -5.0 | -4.8 | 52.5 | 5.2 | 1.03 | 0.1 | 0.0 | 0.0 |
| score 1 | 18059 | **0.62** | 46.0 | -5.4 | -5.2 | 53.9 | 5.8 | 1.07 | 0.3 | 0.0 | 0.0 |
| score 2 | 959 | **1.06** | 46.0 | -6.1 | -6.0 | 53.9 | 7.2 | 1.18 | 0.5 | 0.1 | 0.0 |
| score 3 | 3 | **5.04** | 66.7 | -8.4 | -5.5 | 33.3 | 32.0 | 3.79 | 0.0 | 0.0 | 0.0 |
| score 4 | 0 | — | — | — | — | — | — | — | — | — | — |
| LOW (<2) | 103885 | **0.37** | 47.2 | -5.1 | -4.9 | 52.7 | 5.3 | 1.04 | 0.1 | 0.0 | 0.0 |
| HIGH (>=2) | 962 | **1.08** | 46.0 | -6.1 | -6.0 | 53.8 | 7.2 | 1.18 | 0.5 | 0.1 | 0.0 |
| gate: acc_tr | 218 | **1.96** | 48.6 | -10.8 | -9.1 | 51.4 | 14.1 | 1.3 | 5.5 | 1.4 | 0.0 |
| gate: range_exp | 0 | — | — | — | — | — | — | — | — | — | — |
| gate: acc_tr x range_exp | 0 | — | — | — | — | — | — | — | — | — | — |
