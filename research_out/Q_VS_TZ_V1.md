# Q_VS_TZ_V1 — colour-blind Q (+G/R) vs TZ

**Date** 2026-09-24 · pre-registered k = 11 (3 system comparisons + 8 centre-split cells), plan approved by the user before running.
**Verdict: NULL.** Q does not beat TZ, and the one thing Q adds (engulf/inside centre up vs down) carries no signal (0/8).

## Setup
- S&P 500, 620 tickers, 778,760 ticker-days, 2021-05-27 → 2026-08-25, deduplicated on (ticker, date).
- Entry is the next bar's open and exit is close[t+20], so there is no close-entry lookahead. Rows without a full forward window are dropped, so there is no right-edge carry-forward.
- Outcome = forward return minus the same-day universe median (pp).
- MINE 2021-2023 fits a per-cell median score (cells with n < 200 get 0). VERIFY 2024-2026 scores it with a daily cross-sectional rank IC. CIs use a 20-day block bootstrap.
- Encodings:
  - TZ: 26 cells.
  - Q+colour: 23 cells.
  - Q+colour+previous colour: 55 cells.
  - TZ + centre split on T4/T6/Z4/Z6/T9/T10/Z9/Z10: 34 cells.

## Structure (descriptive, no k)
- TZ = Q + current colour + previous colour. That triple predicts the TZ code on **99.6 %** of bars (0.03 bits left). Q+colour alone predicts 72 %, Q alone 49 %.
- Q's only addition is the centre direction inside engulf/inside bars, about 25 % of bars.

## Results — VERIFY 2024-26, daily rank IC

| encoding | IC | CI | ICIR |
|---|---|---|---|
| TZ | +0.0088 | [+0.0018, +0.0142] | 0.11 |
| Q+colour | +0.0070 | [+0.0007, +0.0126] | 0.09 |
| Q+colour+prev | +0.0052 | [−0.0007, +0.0100] | 0.07 |
| TZ+centre | +0.0066 | [+0.0009, +0.0114] | 0.09 |

Pre-registered comparisons against TZ:

| comparison | Δ IC | CI | result |
|---|---|---|---|
| Q+colour − TZ | −0.0018 | [−0.0055, +0.0022] | no difference |
| Q+colour+prev − TZ | −0.0036 | [−0.0058, −0.0013] | **worse** (55 cells overfit MINE) |
| TZ+centre − TZ | −0.0022 | [−0.0043, +0.0004] | no difference |

$21-89 (secondary) gives the same picture: all three Δ include 0.

**All ICs are below 0.01.** Neither alphabet ranks stocks on its own. Every cell's VERIFY median excess sits within ±0.25 pp, and cells flip sign between windows (e.g. T6 −0.14 → +0.16).

## Centre split (up − down, median excess pp) — 0/8 pass
The bar was same sign in both windows and |Δ| ≥ 0.5 pp in both. Largest MINE effect: Z9 +0.30 [+0.02, +0.59], which fell to +0.01 in VERIFY. Every VERIFY CI includes 0.

## Reading
- **Q+G/R and TZ are the same information in two alphabets.** Choose by readability, not by expected edge.
- **The extra Q3/Q6 and Q4/Q5 distinction is noise.** It is a display detail, not a signal.
- **Consistent with the book.** Plain T/Z carry weight 0 in the V4 book-verdict map. Their value appears only with context: oversold, absorption, RS, volume, price zone. This study says nothing against those conditioned edges, which were validated on TZ.

## Not changed
No UI, score, or memory change. Artifacts: session scratchpad `qtz/study.py`, `study.log`, `result.json`.
