# NASDAQ_TURN_RESELECT_V1 — re-select the turn signals on NASDAQ, with repetition

**Date** 2026-09-27 · requested by the user ("nasdaqistvis tavidan gadaarchie signalebi … miaqcie yuradReba rodesac signalebi meordebian") · plan frozen to `nq_resel_plan.txt` before any outcome of the new features.
**Verdict: STOP at the deciding test.** A NASDAQ-specific list, including repetition forms, beats the frozen S&P-58 list by only **+0.07 lift in-sample** at equal coverage (bar: +0.10). VERIFY was not run.

## Setup
- **History.** Full-field NASDAQ history: 3,372,197 ticker-days, 3,992 tickers. It was built in chunks by the app's own catalog code, with the same EDGE semantics as Ultra.
- **Candidates and label.** As in NASDAQ_TURN_V1 (10-bar low, ≥ $1, ≥ $1M; turn = no lower low in 10 bars and +3 ATR within 20).
  - MINE ≤ 2023: 163,251 candidates, base 8.0 %.
  - VERIFY ≥ 2024: 185,394 candidates, base 10.1 %.
- **Features.** Every usable key in two forms:
  - **ANY3** — fired on any of t-2..t.
  - **REP** — fired on ≥ 2 of t-4..t (repetition across consecutive bars).
  - That gives 779 usable features from 452 keys.
- **Selection rule.** Unchanged from the S&P study: lift ≥ 1.1 and ≥ 10 % of turns, best form per key → **76 features, 12 of them in REP form**.

## The NASDAQ list (MINE)

**New relative to S&P-58 (examples):**

| group | keys |
|---|---|
| T4 and the L34 grades | T4 1.35 · L34VL 1.40 · L34VH 1.38 · green L34 1.27 |
| context and filters | 🏆rs 1.21 · 4H REV-today 1.13 · 🔻 structural bottom 1.14 · RTB 1.10 |
| volume and candle | VB volume 1.22 · BB hammer 1.21 · FLY BD/CD/AD 1.17 |
| OVD | HO·60 1.17 · OB·30/60 1.11 |

**Repetition helps for:**

| key (REP form) | lift |
|---|---|
| L∗ | **1.33** |
| HILO↑ | 1.30 |
| R2X (RSI2 reclaim) | 1.30 |
| G2 | 1.15 |
| RN·U | 1.14 |
| B-volume | 1.14 |
| ★a | 1.12 |
| L1 | 1.12 |
| ★★a | 1.11 |
| OB·60 | 1.11 |
| σ5 | 1.10 |
| ★V | 1.10 |

For these, "fired twice in five bars" beat "fired once in three".

**Strongest overall:** FLP↑ 1.40 · SVS 1.40 · L34VL 1.40 · ZRT 1.36 · G3 1.34 · ΔΔ↑ 1.34 · FBO↑ 1.34.

## Deciding test (MINE, coverage-matched)

| rule | coverage | lift |
|---|---|---|
| S&P-58 ≥ 20 | 11.6 % | 1.47 |
| NEW ≥ 24 of 76 | 11.1 % | 1.55 (+0.07) |
| S&P-58 ≥ 26 | 3.1 % | 1.56 |
| NEW ≥ 31 of 76 | 2.9 % | 1.63 (+0.06) |

The new list was chosen on these same MINE data, so even this +0.07 is an in-sample advantage, and it would be expected to shrink out of sample.

## Reading
- **The turn information is saturated, not list-specific.** Dozens of overlapping signals (volume, L-lines, delta, candle, edges) all read one underlying *turn state*. Swapping which 58-76 of them are counted barely moves the result. The S&P-mined list already captures about as much as a NASDAQ-mined list with repetition.
- **Repetition is real for a few signals** (L∗, HILO↑, R2X, G2, L-BAL marks): a signal that recurs over consecutive bars carries more than one that fires once. It is not enough to change the count's power.
- **The ceiling for this approach** is a turn lift of about 1.5-1.6 at 3-11 % of 10-bar lows. Earlier studies showed that identification at this level does not become a same-day trade edge.
- **Nothing changes in the app.** The Superchart TURN·58 row keeps the frozen S&P list, which transfers to NASDAQ (NASDAQ_TURN_V1: lift 1.42 / 1.56 out of sample).

Artifacts (session scratchpad `v4hist/`): `nq_resel_plan.txt`, `build_rows_nqf.py`, `build_nqf.log`, `fired_nqf.ndjson`, `nq_resel.py`, `nq_resel.log`, `nq_resel_S.json`, `nq_resel_mine.json`. No UI, score or memory change.
