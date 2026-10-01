# INSIDER_BUY_V1 — do officer/director open-market purchases (SEC Form 4) predict the stock?

**Date** 2026-09-30 · user-approved ("ki"); prompted by LTRX (CEO bought 15,000 sh on 2026-09-09 at the bottom). Plan frozen to `insider_plan.txt` before any outcome.

**Data**
- **Source:** SEC Insider Transactions Data Sets 2021Q1-2026Q1, 21 archives, 227 MB, downloaded with the user's permission.
- **Purchases:** 132,681 open-market purchases (code P) → `MASSIVE_DATA/INSIDER_FORM345/insider_purchases.parquet`.
- **Filters:** officer/director buys only (10 %-owner-only excluded), 44,747 ticker-filing days.
- **Event = the FILING date** (information public), entry at the next open. Median filing lag is 2 days.
- **Note:** the chart ★ sits on the transaction date, which is 1-2 days before the public could know.

## VERDICT: 0 / 4 PASS. Strong in 2021-23, gone and slightly NEGATIVE in 2024-26. A textbook decaying anomaly.

Primary test: per-bar book return vs all eligible bars on the same day, same ATR% bin.

| variant | n (eligible) | MINE 2021-23 | VERIFY 2024-26Q1 | years | $21-89 V | verdict |
|---|---|---|---|---|---|---|
| V1 any officer/director ≥ $10k | 8,012 | **+1.36 [0.47, 2.28]** | −0.68 [−1.76, 0.44] | 2021 +2.1 · 2022 +1.7 · 2023 +0.6 · 2024 +0.8 · **2025 −1.6 · 2026 −2.5** | −1.25 | FAIL |
| V2 cluster ≥ 2 insiders / 7 d | 1,285 | **+3.60 [1.29, 5.82]** | −1.63 [−4.17, 0.88] | +2.6 / +5.8 / +1.2 / +2.1 / **−3.7 / −4.8** | −1.06 | FAIL |
| V3 CEO/CFO ≥ $50k | 2,011 | **+2.02 [0.64, 3.44]** | **−2.36 [−4.48, −0.34]** | +1.6 / +2.1 / +2.2 / **−1.9 / −2.0 / −5.0** | −3.28 | FAIL |
| V4 V1 near the 60-bar low (LTRX type) | 3,906 | +1.79 [0.56, 2.95] | −0.49 [−1.82, 0.80] | +4.7 / +3.0 / −0.7 / +0.7 / −1.1 / −2.7 | −0.90 | FAIL |

Adding a location stratum (position in the 20-bar range) changes little (V4: MINE +1.19, VERIFY +0.32). The effect was not "buying the dip".

**Simple table** (entry next open; % that rose / median)

| variant | 5 bars | 10 bars | 20 bars | 10 b max up / down | 10 b touched +10 % / −10 % |
|---|---|---|---|---|---|
| V1 | 51 % / +0.1 | 52 % / +0.2 | 49 % / −0.1 | +4.9 / −4.5 | 22 % / 20 % |
| V2 | 51 % / +0.2 | 52 % / +0.3 | 50 % / +0.1 | +5.5 / −4.8 | 24 % / 23 % |
| V3 | 51 % / +0.1 | 54 % / +0.7 | 52 % / +0.5 | +5.5 / −4.6 | 25 % / 22 % |
| V4 | 53 % / +0.3 | 53 % / +0.4 | 52 % / +0.5 | +4.5 / −4.0 | 19 % / 16 % |
| any bar | 50 % / 0.0 | 50 % / 0.0 | 50 % / 0.0 | +4.7 / −4.6 | 21 % / 21 % |

**$5-8 (LTRX zone), 20 bars:**

| variant | rose | median | n |
|---|---|---|---|
| V1 | 46 % | −1.4 | 605 |
| V2 | 43 % | −2.4 | 118 |
| V3 | 47 % | −1.0 | 193 |
| any bar | 47 % | −1.2 | — |

## Reading
1. **Insider buying WAS a real signal in 2021-23:** +1.4 to +3.6 pp, clusters and CEO/CFO strongest. That matches the academic literature.
2. **It has not worked since 2024.** It turned negative in 2025 and so far in 2026 (V3 −2.36 [−4.48, −0.34] VERIFY). Likely reasons are crowding (insider feeds are now everywhere) or a regime change. A forward read is the only way to know whether it returns.
3. **At the LTRX price level ($5-8)** insider buys did not beat any bar.
4. **Coverage caveat.** Book eligibility (≥ $5, $vol ≥ $5M) keeps only ~37 % of insider-buy days; insider buying is concentrated in micro-caps below $5. That segment is untested here, by design of the book's universe.

No build, no memory entry. Artifacts (scratchpad `v4hist/`): `insider_plan.txt`, `insider.py`, `insider.log`, `insider_table.py/.log`.

---
## Sub-$5 test (INSIDER_BUY_SUB5_V1, frozen `insider_sub5_plan.txt` before outcome; 2026-10-01)

**Universe and method**
- **Universe:** close $1-5, 20-day $volume ≥ $1M; 297,635 bars, 1,821 tickers.
- **Strata:** day × ATR% bin (bins re-cut for this universe).
- **Costs:** optimistic, since spreads are wider than 15 bps.

| variant | n | MINE | VERIFY | years | verdict |
|---|---|---|---|---|---|
| **V1 any ≥ $10k** | 1,696 | +4.17 [−0.83, 10.18] | +3.31 [−0.79, 7.91] | **6/6 positive** (+2.4 … +6.4) | FAIL (CIs cross 0), WATCH |
| V2 cluster | 326 | +1.74 | +6.10 [−2.49, 15.96] | mixed | FAIL |
| V3 CEO/CFO ≥ $50k | 529 | +0.68 | −0.24 | mixed | FAIL |
| V4 near 60-bar low | 443 | −1.93 | −1.48 | mixed | FAIL |

**Simple table, V1**
- % that rose: 51 % at 5 and 10 bars, 45 % at 20 bars.
- Within 10 bars, touched +10 % on 52 % and −10 % on 40 %.
- **Any sub-$5 bar:** 45 % rose; touched +10 % / −10 % on 48 % / 47 %. Sub-$5 names drift down; insider buys sit about 6 pp better on the downside.

## Forward registration (2026-10-01, user: "samive gaakete")
**Script.** `backend/insider_forward.py`. Filings 2026-04-01 … 2027-02-28 (unseen); read ONCE on or after **2027-06-01**.

**Rules**
- **F1:** cluster ≥ 2 insiders / 7 d, book universe.
- **F2:** CEO/CFO ≥ $50k, book universe.
- **F3:** sub-$5 V1.

**Method and PASS.** Per-bar returns vs all bars of the universe, same day × frozen ATR%-bins. PASS = Δ > 0 with CI lo > 0. Forward k = 3.

**Seen-window check, 2023**

| rule | evaluator | research yearly mean |
|---|---|---|
| F3 | +3.37 | +3.30 |
| F1 | +0.14 | +1.24 |
| F2 | +0.66 | +2.15 |

F1 and F2 sit within one-year noise (CI ± 3).

**Data.** The SEC archives 2026q2 … 2027q1 must be downloaded into `MASSIVE_DATA/INSIDER_FORM345/raw` before the read, each with the user's OK.
