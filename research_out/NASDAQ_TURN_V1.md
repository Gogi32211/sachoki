# NASDAQ_TURN_V1 — the frozen S&P TURN·58 rule on unseen NASDAQ

**Date** 2026-09-27 · the user delegated the choice ("tu ginda nasdaq ze gaakete … ar shegzgudav") · plan frozen to `nq_turn_plan.txt` before any NASDAQ outcome · k = 2.
Nothing was re-selected on NASDAQ: the 58 keys, the candidate rule, the turn label and the thresholds all come from TURN_SET_V1 (S&P MINE 2021-23). **All NASDAQ data 2021-05 … 2026-09 is out of sample.**

## Data
- **Universe.** 3,372,197 NASDAQ ticker-days (3,992 tickers).
- **Candidates.** 348,645 10-bar lows with close ≥ $1 and dollar volume ≥ $1M.
- **Turn base rate.** 9.1 %.

## (1) Identification — PASS

| rule | n | days | turn rate | lift vs all lows (day-clustered CI) |
|---|---|---|---|---|
| TURN·58 ≥ 20 | 40,861 | 1,305 | 13.0 % | **1.42 [1.35, 1.50]** |
| TURN·58 ≥ 26 | 10,979 | 1,214 | 14.3 % | **1.56 [1.45, 1.68]** |

It holds in every price bucket (≥ 20 lift): <$8 **1.52** · $8-21 **1.35** · >$21 **1.42**.

## (2) Selection — FAIL
Book ATR×12 trail, entry open[t+1], compared with other lows on the same entry day:

| rule | trades | mean · win | same-day Δ (CI) | by year 2021 … 2026 |
|---|---|---|---|---|
| ≥ 20 | 30,497 | +1.02 % · 48 % | +0.43 [−0.18, +1.06] | +3.1 / −0.5 / +0.7 / −0.8 / +1.0 / +0.1 |
| ≥ 26 | 9,173 | +1.17 % · 48 % | +0.28 [−0.69, +1.24] | +3.3 / +0.3 / −0.0 / −1.4 / −0.2 / +1.7 |

## (3) Timing — mixed
- **Pooled** (not same-day matched), vs all lows at +0.12 %:
  - ≥ 20: **+0.90 pp**, day-clustered CI [+0.06, +1.81], 4/6 years positive.
  - ≥ 26: +1.05 pp, CI [−0.11, +2.23], 5/6 years positive.
  - Both are carried mainly by **2025** (+3.85 / +2.89).
  - The first print of this line used a trade-level bootstrap. It was replaced by the day-clustered one above.
- **Market timing:** the daily share of lows with TURN·58 ≥ 20 against the next 20-day equal-weight NASDAQ return gives Spearman +0.022 [−0.077, +0.097]. **FAIL.** The quintiles are not monotone.

## Reading
- **TURN·58 is a real, portable turn identifier.** Mined on S&P large caps, it identifies turns on unseen NASDAQ names 1.4-1.6× better than an average 10-bar low, at every price level. That is the strongest out-of-sample result of this session.
- **It still does not pick the better trade on the day.** Same-day Δ is positive on NASDAQ (+0.3…+0.4 pp, unlike S&P's −0.35), but it is small, year-unstable and inside the noise.
- **The pooled advantage is real but episodic.** It concentrates in 2025 (the April sell-off and rebound is the obvious candidate). The simple market-timing version (the day's share of high-count lows) carries no signal, so it is not a clean timing tool either.
- **Where the gap between "turns more" and "earns more" seems to come from.** The high-count lows win about as often under a trail (48 %) and just earn a slightly larger average. The information is real but mostly already priced into the typical rebound size.

## What changed from the S&P conclusion
- Identification: S&P lift ~1.35 → **NASDAQ 1.42-1.56**.
- Same-day selection: S&P −0.35 → **NASDAQ +0.3…+0.4** (not significant).

The direction is better in the user's own market, but it is not yet a trade edge.

Artifacts (session scratchpad `v4hist/`): `nq_turn_plan.txt`, `build_rows_nq.py`, `rows_nq.ndjson`, `fired_nq.ndjson`, `nq_turn.py`, `nq_turn.log`, `nq_pooled.py`, `nq_pooled.log`. No UI, score or memory change.
