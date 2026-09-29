# BD_RECLAIM_V1 — a breakdown (BD▼/BDV▼) that is closed back above within 3 bars

**Date** 2026-09-29 · user: "moxda garRveva, magram mesame bari garRvevis barze zemoT daixura" · plan frozen to `bdrec_plan.txt` before any outcome.
**Universe** all 5,739 US tickers, eligible bars, MINE 2021-23 / VERIFY 2024-26.

## Setup
- **Event bar.** The event fires on the first bar r in t+1..t+3 after BD▼/BDV▼ that meets the reclaim rule.
  - **H1:** close[r] > the BD bar's **high**.
  - **H2:** close[r] > the BD bar's **open** (top of the body).
- **Entry.** open[r+1].
- **k = 2.**

## Results

| rule | n (MINE / VERIFY) | REV lift | CLEAN lift | return Δ vs control, MINE | return Δ, VERIFY | verdict |
|---|---|---|---|---|---|---|
| H1 close > BD high | 13,775 / 17,682 | 1.04 / 1.02 | 1.05 / 1.01 | −0.80 [−1.52, −0.03] | +0.41 [−0.36, +1.21] | NULL |
| H2 close > BD open | 18,008 / 22,973 | 1.01 / 1.02 | 1.04 / 1.01 | −0.87 | +0.05 | NULL |
| · H1, r = t+2 (3rd bar) | 4,575 / 5,714 | 1.02 / 1.01 | 1.06 / 0.98 | −0.76 | +0.10 | — |
| · H1, BDV▼ base | 6,852 / 8,276 | 1.05 / 1.03 | 1.04 / 1.02 | −0.36 | +0.78 [−0.32, +1.80] | — |

- **Years (H1):** 2021 −2.85, 2022 +0.30, 2023 −0.87, 2024 −0.23, 2025 +0.44, 2026 +1.23. That is 3 years each sign.
- **Price buckets:** no bucket has CI lo > 0.

## Comparators (descriptive, not pre-registered as hypotheses)

| comparator | return Δ, MINE | return Δ, VERIFY | years |
|---|---|---|---|
| Generic reclaim: a red bar at a fresh 10-bar low, then a close above its high, no echo zone | −0.60 | **−1.70 [−2.35, −1.06]** | 5 of 6 negative; $89+ negative in both windows |
| BD with NO reclaim in 3 bars, measured at t+3 | **−0.70 [−1.30, −0.11]** | **−0.93 [−1.48, −0.33]** | 5 of 6 negative |

**Reading.** A reclaim after a VOL_ECHO breakdown is **less bad** than a generic reclaim of a fresh low, and less bad than an unreclaimed breakdown. But it is not a buy: its reversal likelihood equals a location-matched bar, and its return is ≈ 0 and unstable.
- The "no reclaim → negative" and "generic reclaim → negative" comparators are hypotheses only. They were not pre-registered, so they cannot be a VETO without a fresh sealed test.

**Verdict: NULL.** No build, no memory entry.
Artifacts (scratchpad `v4hist/`): `bdrec_plan.txt`, `bdrec.py`, `bdrec.log`.
