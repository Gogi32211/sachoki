# V3-B — BREAKOUT SURVIVAL / EXIT · FIRST COMPLETE RUN

**2026-09-12. Rules frozen before the run. BL1 anchors on 3,865,401 SP500+Nasdaq stock-days;
K_ACT as the secondary robustness anchor. h = 1, 2, 3, 5 reported separately — the best horizon
is never selected.**

## RESULT IN ONE LINE

> **NO POLICY PASSES.** 36 candidate rule × horizon combinations; the largest improvement anywhere
> is **+0.029 ATR** against a **+0.10 ATR** gate. And the headline of the previous report —
> the −32.6 pp regime gap — **dissolves under the causal construction**: acting on it at the
> decision bar *loses* money.

---

# 1 · CAUSALITY AUDIT — the test this study exists for

| check | result |
|---|---|
| primitives recomputed on a frame truncated **exactly at the decision bar** | 500 events × 26 primitives = **13,000 comparisons** |
| **differences** | **0** |
| counterfactual causality assertion | **PASSED** — `premature-exit` is unchanged when every bar up to and including the exit bar is multiplied by 10 |
| forbidden-column assertion | PASSED |
| split-edge rule | 28,183 events dropped because their 20-bar outcome window crossed a split boundary |

The v2 report's −32.6 pp came from a path window and an outcome window that were the same 20 bars.
This construction makes that impossible by moving the measurement point.

---

# 2 · EVENT COUNTS

| anchor | h | DISCOVERY 2021-23 | VALIDATION 2024-25 | HOLDOUT 2026 |
|---|---:|---:|---:|---:|
| **BL1** | 1 | 27,153 | 27,264 | 11,067 |
| | 2 | 27,104 | 27,202 | 10,973 |
| | 3 | 27,063 | 27,145 | 10,884 |
| | 5 | 26,958 | 26,870 | 10,662 |
| K_ACT | 1 | 37,745 | 36,698 | 14,474 |
| | 5 | 37,537 | 36,094 | 13,991 |

69,872 deduplicated BL1 anchors · 94,229 K_ACT anchors · 613,946 usable events.

---

# 3 · RACE-CLASS DISTRIBUTION — `+3 ATR before −1.5 ATR`

BL1, primary race, from the decision-bar close:

| h | split | WIN | LOSE | OPEN | AMBIGUOUS | n |
|---:|---|---:|---:|---:|---:|---:|
| 1 | DISCOVERY | 26.17 | 61.02 | 12.71 | **0.11** | 27,153 |
| 1 | VALIDATION | 27.75 | 58.15 | 13.89 | **0.21** | 27,264 |
| 1 | HOLDOUT | 29.05 | 58.00 | 12.65 | **0.30** | 11,067 |
| 2 | DISCOVERY | 25.97 | 60.23 | 13.71 | 0.10 | 27,104 |
| 5 | HOLDOUT | 28.00 | 55.50 | 16.33 | 0.18 | 10,662 |

**AMBIGUOUS is immaterial — 0.10 to 0.30 %.** The concern about unknowable intrabar order was
worth raising and turns out not to matter; best-case and worst-case bounds differ by under 0.3 pp
and change no conclusion. `OPEN` is 12.6-16.3 % and is kept separate everywhere.

**The descriptive fact underneath all of this:**
```
0.29 × (+3 ATR)  −  0.58 × (−1.5 ATR)  =  +0.87 − 0.87  ≈  0
```
**A fresh 20-day breakout, held at 2:1, is a coin flip in expectancy terms.** That is the honest
starting point for any survival rule: there is very little to protect.

---

# 4 · REFERENCE POLICIES — BL1, HOLDOUT 2026

| h | policy | mean | **median** | captured MFE | med exit bar | premature % |
|---:|---|---:|---:|---:|---:|---:|
| 1 | **P0 hold** | **+0.4425** | −0.1341 | 0.005 | 20 | 0.00 |
| 1 | P1 ATR×12 trail | +0.4334 | −0.1385 | 0.003 | 20 | 0.21 |
| 1 | P2 EMA20 exit | +0.0217 | −0.7254 | −0.280 | 10 | 12.98 |
| 1 | P3 fixed −1.5 ATR | +0.1280 | −1.5000 | −0.467 | 11 | 9.32 |
| 1 | P4 time stop 10 | +0.2096 | −0.0230 | 0.019 | 10 | **32.85** |
| 5 | **P0 hold** | **+0.4185** | −0.1039 | 0.000 | 20 | 0.00 |
| 5 | P2 EMA20 exit | +0.0368 | −0.4583 | −0.189 | 7 | 16.60 |

**On the holdout, doing nothing beats every exit rule at every horizon.** The validated ATR×12
trail is indistinguishable from holding (premature 0.21 %) — in a 20-bar window a 15-60 % trail
almost never triggers, so this is not a contradiction of `project_atr_exit_law`, it is that rule
being asked to work outside the horizon it was built for.

**Every policy has a positive mean and a negative median.** The expectancy is carried by a minority
of large winners. A rule that improves the typical trade will usually destroy the average one.

---

# 5 · ⚠️ THE EXIT RULES WORK — ON THE MEDIAN — AND DESTROY THE MEAN

This is the most useful pattern in the run. BL1, HOLDOUT, h = 1:

| rule | **Δ mean** | **Δ median** | Δ premature |
|---|---:|---:|---:|
| `exit_if_no_FOLLOW` | **−0.0094** | **+0.7254** | **+11.67 pp** |
| `exit_if_GIVEBACK < −1` | −0.0034 | +0.7254 | +6.18 pp |
| `exit_if_K_RESET` | −0.0243 | +0.6200 | +3.87 pp |
| `exit_if_K_not_bull` | −0.0238 | +0.6353 | +3.87 pp |
| `exit_if_RSI > 70` | +0.0019 | +0.3569 | +6.06 pp |
| `exit_if_K2U` | +0.0147 | +0.1543 | +3.10 pp |
| `exit_if_VOL7 == 6` | +0.0221 | +0.1723 | +2.42 pp |

Every rule does exactly what it was designed to do — the median trade improves by up to **+0.73 ATR**
— and the mean does not move, or falls. The mechanism is visible in the last column: the same rule
cuts 4-12 % of positions that would later have reached +3 ATR. **The exits remove the left tail and
the right tail together**, converting a skewed positive-expectancy distribution into a symmetric
one of no greater value.

---

# 6 · ⭐ THE −32.6 pp FINDING DISSOLVES

The v2 report's largest number was: `regime went D` in 38.6 % of winners and 71.1 % of losers.

Tested causally — the regime state **at the decision bar**, acted on:

| h | `exit_if_REGIME_D` Δ mean (HOLDOUT) | ticker-clustered CI |
|---:|---:|---|
| 1 | **−0.0055** | [−0.0109, −0.0005] |
| 2 | −0.0042 | [−0.0104, +0.0017] |
| 3 | **−0.0812** | [−0.1718, −0.0230] |
| 5 | **−0.0189** | [−0.0352, −0.0059] |

`exit_if_not_REGIME_HELD` gives the same picture (−0.006 / −0.007 / −0.081 / −0.036).

**Negative at every horizon, and the CI excludes zero at h = 1, 3 and 5 — on the wrong side.**
Knowing the regime broke and acting on it is worse than ignoring it. The v2 gap measured
"the move died" against "the move died"; the causal version says the information has no decision
value and slightly negative action value.

**This is the clearest single result of the whole programme, and it is the retirement of its own
most promising lead.**

---

# 7 · CANDIDATE RULES — VALIDATION vs HOLDOUT, and the gate

36 combinations. **PASSING: NONE.**

| h | best rule by Δ mean | Δ mean HOLDOUT | Δ mean VALIDATION | verdict |
|---:|---|---:|---:|---|
| 1 | `exit_if_VOL7_6` | +0.0221 | +0.0137 | FAIL — mean < +0.10 |
| 2 | `exit_if_RSI > 70` | **+0.0285** | **−0.0289** | FAIL — sign flips, and < +0.10 |
| 3 | `exit_if_RSI > 70` | +0.0094 | −0.0060 | FAIL |
| 5 | `exit_if_RSI > 70` | +0.0033 | +0.0028 | FAIL |

The largest improvement in the entire study is **+0.029 ATR**, a third of the gate, and it is the
one rule whose sign **reverses** between validation and holdout. `exit_if_VOL7_6` is the only rule
with the same positive sign in both splits, at +0.014 / +0.022 — a quarter of the gate, with a CI
of [−0.005, +0.045] that straddles zero.

**A note on the reference these are measured against.** The best reference policy was selected on
DISCOVERY, which is correct protocol — and DISCOVERY chose `P2_ema20_exit`. On the holdout, the
best reference was actually `P0_hold` at +0.4425. So every candidate sits roughly **0.4 ATR below
simply holding**. The gate result is if anything understated.

---

# 8 · ⚠️ EXIT VALUE IS REGIME-DEPENDENT — and one holdout year cannot settle it

BL1, h = 1, mean expectancy in ATR:

| policy | DISCOVERY 2021-23 | VALIDATION 2024-25 | HOLDOUT 2026 |
|---|---:|---:|---:|
| **P0 hold** | **−0.0483** | **+0.2814** | **+0.4425** |
| P1 ATR×12 trail | −0.0451 | +0.2737 | +0.4334 |
| **P2 EMA20 exit** | **−0.0107** | +0.0824 | +0.0217 |
| P3 fixed stop | −0.0154 | +0.1487 | +0.1280 |
| P4 time stop 10 | −0.0553 | +0.1333 | +0.2096 |

**In 2021-2023 — which contains the 2022 bear — holding a breakout was NEGATIVE (−0.048) and the
EMA20 exit was the least-bad policy.** In 2024-2026 holding is the best by a wide margin.

So the correct statement is **not** "exit rules are useless". It is: *the value of an exit rule is
a function of the regime, and this holdout is one favourable year.* Any future exit study must
stratify by market regime rather than pool across it — the single-holdout design used here cannot
answer the question it was built for once that dependence is visible.

**K2U in the hold context:** Δ mean +0.015 / +0.015 / −0.006 / −0.013 across horizons, all CIs
straddling zero. K2U is bad for a **new entry** (v2: −9.3 pp) and **neutral for an exit decision**.
The two contexts genuinely differ, exactly as you framed it.

---

# 9 · EARLY-EXIT DIAGNOSTICS

| policy | med exit bar | premature % | what it means |
|---|---:|---:|---|
| P4 time stop 10 | 10 | **32.85** | one exit in three would have reached +3 ATR later |
| P2 EMA20 exit | 10 | 12.98 | |
| P3 fixed −1.5 ATR | 11 | 9.32 | |
| P1 ATR×12 trail | 20 | 0.21 | effectively never fires in 20 bars |
| P0 hold | 20 | 0.00 | by construction |

`captured MFE` (median) is ≈ 0 for hold and **negative** for every stop-based policy — the median
trade ends below the decision-bar close while the window's high went higher. The distribution is
the story, not the average.

---

# 10 · VERDICTS

| hypothesis | verdict |
|---|---|
| A causal survival/exit rule beats the best reference policy by +0.10 ATR | **REJECTED** — 0 of 36 |
| Regime break at the decision bar is actionable | **REJECTED — NEGATIVE**, CI excludes zero on the wrong side at h=1,3,5 |
| K reset / no-follow-through / giveback are actionable exits | **NULL on the mean**, positive on the median, at a 4-12 pp premature-exit cost |
| K2U is a hold/exit signal | **NULL** — neutral here, unlike its −9.3 pp as an entry |
| Holding a fresh breakout has positive expectancy | **REGIME-DEPENDENT** — −0.048 in 2021-23, +0.44 in 2026 |
| AMBIGUOUS intrabar cases matter | **NO** — 0.1-0.3 % of events |
| ATR×12 trail adds value inside 20 bars | **NO** — fires on 0.2 %; out of its designed horizon |

---

# 11 · WHAT THIS CHANGES FOR THE PROGRAMME

1. **The survival lead is retired.** It was the strongest remaining idea and the causal test says
   it is worth −0.006 to −0.081 ATR. That is the value of building the construction properly.
2. **The real finding is distributional, not directional.** Breakout returns are right-skewed with a
   negative median; every exit rule tested trades mean for median. Whether that trade is worth
   making is a **position-sizing and portfolio** question, not a signal question —
   `project_portfolio_layer` already found slot count is the binding constraint.
3. **Regime stratification is now mandatory** for anything in this area, and it is a different
   study from the one just run.
4. **V3-A is unaffected** and remains the open question: major breakouts at `MFE20_ATR ≥ 8`, with
   2026 untouched. Nothing in this run has been allowed to inform it.

## ARTIFACTS
```
backend/v3b_survival_build.py   anchors · decision bars · causal primitives · causality audit
backend/v3b_survival_eval.py    race labels · policies · counterfactuals · gate
data/v3b_events.parquet         642,129 events · v3b_events_audit.json
research_out/ v3b_race_classes.csv · v3b_policies.csv · v3b_candidates.csv · v3b_gate.csv
```

**STOPPING HERE.** No Pine, no implementation, no new rules invented from these results.
