# Q3_SEQ_V1: 3-bar Q sequences with a high 10-bar win rate (2026-10-03)

## Study set-up

- **Plan:** approved by the user ("ki").
- **Script:** `backend/research_q3_seq.py`. Full output is in `Q3_SEQ_V1.json`.
- **Alphabet:** Q0-Q8 + G/R, line 1 only. Q is computed exactly as in the Pine script; parity with the Q tab is 100 %.
- **Win:** `close[t+10] / open[t+1] − 1 − 30 bps > 0`.
- **Lift:** paired against the other bars of the same day and the same ATR%-bin (frozen ATR_CUTS). It is n-weighted per day, and the CI is day-clustered.
- **Universe:** liquid stocks (close ≥ $5, 20-day dollar volume ≥ $5M).

## Search accounting

| step | count |
|---|---|
| MINE 2021-23: cells with n ≥ 300 | **k = 981** |
| passed the MINE filter (lift > 0, CI lo > 0) | 24 |
| sent to VERIFY 2024-26 | 24 |
| positive in VERIFY | **12 / 24** (a coin flip) |

- Mean lift of the 24 candidates: **MINE +5.72 pp → VERIFY +0.11 pp**. They regressed fully to zero, so the MINE "winners" are search noise.
- Baseline win rate: MINE 47.2 %, VERIFY 49.0 %.

## Raw win rate misleads

The highest raw win rate in MINE is `Q2R>Q4G>Q1R`: **67.1 % win, but lift −5.0 pp**. Those events fell on days when everything went up. A high win rate alone is not an edge, and the lift is what measures one.

## The only PASS (not Bonferroni)

**`Q2G > Q7R > Q7R`**: a rally bar (overlap up, green), then two red overlap-down bars, i.e. a shallow pullback after strength.

| | MINE 2021-23 | VERIFY 2024-26 |
|---|---|---|
| n | 2,605 | 2,757 |
| win | 48.5 % | 49.5 % |
| lift | +3.80 (lo +0.65) | **+3.03 (lo +0.22)** |
| Bonferroni lo (k_ver = 24) | | −1.38 ✗ |

- **By year:** 2021 +8.5, 2022 +2.2, 2023 +2.0, 2024 +0.6, 2025 +5.5, 2026 +2.7. That is 6/6 positive, worst +0.6.
- **$21-89 zone:** +1.96 pp.
- **Return lift:** only +0.31 pp. The win rate improves, but the average return barely does.

**Verdict: SEARCH-EXPOSED CANDIDATE, not a BUILD.** With k = 24, about 1.2 false PASSes are expected at 5 %, and this one dies under Bonferroni. Its shape (pullback after strength) matches the known law in `project_entry_timing` (PULLBACK wins, chasing strength loses). It is a confirmation of that law rather than a new edge.

- **On 2026-10-02 (liquid):** IMTX $7.59, RAPP $32.17, LBRX $36.99, CGON $67.38. These are not trades, only a watchlist.
- **To view in the Q tab:** use 3b and LINE1 only, with bar −2 `Q2G`, bar −1 `Q7R`, bar 0 `Q7R`.

## Conclusion

The Q alphabet does not contain a 3-bar sequence with a robust high win rate at line 1. This matches the earlier finding for T/Z, where the path into a bar does not change its return.
