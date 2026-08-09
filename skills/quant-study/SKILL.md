---
name: quant-study
description: Run a quantitative study on data honestly — testing whether a pattern, signal, feature or hypothesis is real. Use whenever asked to analyse data, check if something predicts/pays/works, compare groups, validate a feature, or measure an effect — in finance or anywhere else. Enforces matched baselines, auto-generated controls, population context before interpretation, multiplicity budgets, leak-free time-series validation, and a verdict that cannot overstate. For evaluating an already-built trading strategy from its metrics, use backtest-expert instead.
---

# Quant Study

A procedure for finding out whether an effect is **real**, and refusing to say it is when it isn't.

## Why this exists

Prose does not prevent mechanical errors. Every guard below was added after a real failure in
which the analyst knew the rule and broke it anyway:

- A feature read as an anomaly (82 vs 46) that was **below** its population median (38).
- A cell from a 2024-26 window compared to a 2021-26 baseline → **+1.3pp of free fake lift**.
- A chain A→B→C reported as an edge; the control "C plus the same filters, no chain" gave
  **87% of the result**.

So the guards live in `scripts/analysis_kit.py` and they **refuse**, they do not remind.

## Workflow

### 1. Plan first — five lines, before any code

Write and get agreement on:

1. **Question** — one sentence.
2. **Deciding first test** — the single check that can kill the idea immediately. State the
   threshold now (e.g. "if agreement with the feature we already own is >80%, stop").
3. **Main hypothesis** — measured in bands, so a plateau is visible instead of a peak.
4. **Controls** — each component alone, and the hypothesis minus each component.
5. **Bar** — the thresholds for the verdict, chosen before seeing results.

Never skip to step 3. The deciding test exists to save the other four.

### 2. Set the trial budget before running

```python
from analysis_kit import Study
st = Study("does X pay?", n_trials=12, outcome="ret", time_col="date")
```

`n_trials` is a pre-registration. Exceeding it raises — because a multiplicity correction
computed after the fact is not a correction.

### 3. Describe before interpreting

```python
st.describe(df, "feature", value=36)   # → "p47 → UNREMARKABLE"
```

Any raw number is meaningless without its population. `cell(..., requires=["feature"])`
refuses to score a threshold on a column that was never described.

### 4. Matched baseline

```python
df = df[df.date >= "2024-02-01"]    # whatever restriction the data forces
st.baseline(df)                     # baseline computed on THESE rows
st.cell(df, "X >= 20", df.X >= 20)  # raises if `df` is no longer the same universe
```

If new data covers a shorter window, the baseline **must be recomputed on that window**.
This is the single most productive source of fake findings.

### 5. Controls, generated not remembered

```python
st.controls(df, {"chain": chain_mask, "RS": df.rs_ok, "oversold": df.rsi < 45})
```

Measures each part alone and every leave-one-out. **If the full combination does not beat its
own pieces, the extra conditions are decoration.** Compare against the *gated* control, not
the naive baseline — "the same filters without the pattern" is usually the real competitor.

### 6. Time-series validation without leakage

```python
from analysis_kit import purged_splits
for tr, te in purged_splits(df.date, n_splits=5, horizon=60):
    ...
```

Plain k-fold is banned on data whose outcome spans forward periods. Overlapping labels leak
across the boundary even chronologically; purging removes the overlap.

### 7. Ranking questions use rank IC

```python
from analysis_kit import rank_ic
rank_ic(df.score, df.fwd_ret, by=df.date)   # ic, icir, hit rate per period
```

"Does my score order things correctly?" is answered by rank IC per cross-section, never by
the mean outcome of the top bucket.

### 8. Verdict

```python
st.verdict(dsr=0.72)
```

Returns exactly one of **BUILD · BOOSTER · WATCH · NULL · VETO**, names the deciding gate,
and **will not return BUILD** unless L1/L2/L3 actually passed:

| Gate | Requirement |
|---|---|
| **L1** | ≥⅔ of periods positive **and** worst period ≥ −2 (in outcome units) |
| **L2** | ≥ +1 unit over the **matched** baseline |
| **L3** | n ≥ 80 per reported cell · plateau in neighbouring parameters · DSR ≥ 0.6 |

## Non-negotiables

- **Point estimates never travel alone** — every number carries a bootstrap interval.
- **No silent truncation.** If the study caps coverage (top-N, sampling, a dropped subgroup),
  print what was dropped. Silent caps read as "we covered everything".
- **Declare the lag of every feature.** Join on *when it became knowable*, not when it
  happened. Published-with-delay data joined on its event date is a lookahead bug that
  produces beautiful, false results.
- **Segment by the natural scale of the domain** (price band, size, sector, cohort). Pooling
  hides an effect that lives in one segment and an artefact that lives in another.
- **A plateau, not a peak.** A winner whose neighbouring parameters disagree is noise.
- **State what would change your mind** before you look at the answer.

## Reading the result

- **Monotone across bands** → a real dose-response. **Single spiking cell** → noise.
- **Effect survives removing the top few units** (tickers, users, sites) → not concentration.
- **Mean ≫ median** → tail-driven; say so, and report both.
- **Big MFE with big MAE** → not an edge, a wide swing someone had to sit through.

## References

- `references/guards.md` — each guard, the failure that created it, and how it refuses.
- `scripts/analysis_kit.py` — the implementation. Import it; do not re-implement the checks.
- For validating an already-built **trading strategy** from its metrics (robustness sweeps,
  slippage stress, regime splits), use the **backtest-expert** skill — it starts where this
  one ends.
