# ABSP_BD_GATE_V1 — does a BD▼ anchor improve Absorption→P?

**Date** 2026-09-25 · the user approved variant A ("ki") · plan frozen to `absp_gate_plan.txt` before any outcome · k = 1.
**Verdict: NULL. A BD▼ anchor does not help Absorption→P; on path-sim it makes it worse.** The sample is THIN, so this is not a veto either.

## Why
The user asked to combine BD▼ × Z5·L12 (a failed VOL_ECHO cell) with the book's Absorption→P reversal. The exact conjunction (anchor Z5·L12·PS-R2L that is also BD▼) happened **6 times in 5 years**, which cannot be tested. The approved variant asks a broader question: when any valid Absorption→P anchor is also BD▼ (it closed below a volume-echo zone), is the trade better than the rest of the same edge?

## Setup
- **Events.** The historical Absorption→P events use the same logic as `absorption_p_scan.py`: a valid anchor combo, RSI14 30-45, any P within 3 bars, close ≥ $5, dollar volume ≥ $2M, no suppressor. One event is kept per (ticker, anchor).
- **Split.** 67 events have a BD▼ anchor (59 days) and 1,456 do not (630 days), all with a 20-day outcome. The census said 82; deduplicating anchors and requiring an outcome gave 67.
- **Outcome.** Entry at the open after the P bar, 20-day excess vs the same-day, same-price-bucket median, day-clustered CI. Path-sim uses the book config: trail 25 % with ATR×12, maxh 60, slip 0.15 %.
- **Bar.** Δ ≥ +1.0 pp, CI lower > 0, Δ positive in ≥ 4 of 6 years. Even a pass would have been a THIN hint.

## Result

| | BD▼ anchor | rest of Absorption→P |
|---|---|---|
| median 20-day excess | +0.91 | +1.27 |
| **Δ (BD − rest)** | **−0.36** [−3.94, +2.07] | |
| Δ positive years | 2 of 6 (2021 −3.4 · 2022 −6.0 · 2023 −4.4 · 2024 +4.4 · 2025 +3.5 · 2026 −3.4) | |
| path-sim mean / median | **−1.05 % / −2.67 %** | **+1.69 % / +0.72 %** |
| path-sim win % · positive years | 45 % · 2/6 | 51 % · 4/6 |

**It fails every gate.** The BD▼ subset is worse than the parent edge on path-sim (mean −1.05 % vs +1.69 %), negative in 4 of 6 years, and the CI is very wide (n 67).

## Reading
- **Absorption→P itself holds up** in this independent re-computation: +1.69 % mean path-sim, 4/6 years, consistent with the book's +1.70.
- **Adding "closed below the echo zone" does not sharpen it.** If anything it selects the worse trades. That fits the book: an absorption anchor that is breaking *down* through a prior volume zone is closer to a knife than to absorbed weakness.
- **No veto is claimed.** With 67 events the CI spans −3.9 to +2.1 pp. This goes on record only as a THIN negative lean.
- **The BD▼ × Z5·L12 line is closed.** It failed alone (BDZ5L12_V1), and its broader gate on the built edge does not help.

Artifacts: session scratchpad `vecho/absp_gate_plan.txt`, `absp_census.py`, `absp_gate.py`, `absp_gate.log`, `absp_events.parquet`. No UI, score, or memory change.
