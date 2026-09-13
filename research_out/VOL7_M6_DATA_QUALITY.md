# VOL7 `M6` — data-quality audit of the −11.97 pp volume-spike rung

**Audit only. No setup, no veto, no build, no second outcome access.** Sealed 2026-09-13.

`M6` is the top rung of the VOL7 ladder: `ratio = volume / rolling_median(volume, 20) ≥ 5.00`
(`backend/vol7_build.py`, `MR_EDGES`). LADDER_V1 measured it at **−11.97 MINE / −12.11 VERIFY** day-clustered over 585 / 322 days — larger
than anything else in the book — and its own caveat said a number that size has to be shown to be
market behaviour and not a split / corporate-action / bad-volume artifact before anything is built on
it. This pass characterises the population only.

## 1 · Which VOL7 numbers are authoritative

`/Users/sachoki/MASSIVE_DATA/LADDER_V1/runs/OUT_20260910T143238Z/results.json` stores
`M6 rung_edge −8.399 / −8.487`, `span_p 0.0 / 0.0`, `rho_p 0.71 / 0.38` and `PASSES: false`.
**Two of those stored fields are superseded and must not be quoted as the result.**

* **`PASSES: false` is a bug, not a verdict.** `ladder_family.py:334` reads
  `(pv.get("mine_span_p") or 1) < 0.05`. A perfect p of `0.0` is falsy, so `(0.0 or 1)` becomes `1`
  and the test fails. With `span_p = 0.0` in both windows and `span_same_sign: true`, VOL7 **does**
  satisfy the registered pass condition. This is the `(p or 1)` defect already recorded in
  [[feedback-placebo-and-cell-size]]; it was found and corrected in-session, and fixed in the
  successor family.
* **`rho` is not part of the pass condition at all** — line 334 uses the span p-values only. A weak
  rho was *pre-registered as expected*: VOL7's declared shape is an inverted-U, so ρ ≈ 0 confirms the
  shape rather than failing it.
* **`rung_edge −8.399 / −8.487` is the superseded statistic.** It is the median over TRADES; the
  registry named the day-clustered day-edge (`ladder_family.py:245`). Recomputed correctly the
  rung is **−11.97 MINE / −12.11 VERIFY over 585 / 322 days**, and the corrected figure is larger,
  not smaller.

So the authoritative reading is: **M6 = −11.97 / −12.11 day-clustered, span significant in both
windows (p = 0.0), shape as pre-registered, ladder passes.** The stored `results.json` predates both
corrections. Those corrections are recorded in [[project-ladder-v1]] and are the reason this audit
exists: its own caveat 2 said the magnitude demanded a data check before anything is built.

## 2 · 18.75 % of the M6 population carries a data-quality flag

140,430 M6 rows, joined back to the exact frame `vol7_build` was fed (`bars`, deduped by universe,
no index).

| flag | rows | share |
|---|---|---|
| price discontinuity — `close/prev_close` outside 0.67–1.5 | 5,695 | 4.06 % |
| 20-day median under 1,000 shares | 15,451 | 11.00 % |
| `ratio` > 100× | 9,899 | 7.05 % |
| a zero-volume bar inside the 20-day window | 1,138 | 0.81 % |
| **any of the above** | **26,328** | **18.75 %** |

`ratio` reaches **334,999×**; p99 = 1,332×; 1.31 % of M6 exceeds 1000×. The denominator is what
manufactures it — 1.48 % of M6 rows have a 20-day median **under 10 shares**.

Worked examples from the extreme tail:

| ticker | date | ratio | volume | 20-day median | prev vol | close | prev close |
|---|---|---|---|---|---|---|---|
| SPCX | 2026-06-12 | 335,000 | 511,544,394 | 1,527 | 6,555 | 160.95 | 21.98 |
| QH | 2026-07-22 | 152,696 | 152,696 | **1.0** | 43,067 | 5.31 | 5.53 |
| BFLX | 2026-06-17 | 146,574 | 27,189,538 | 185.5 | **10** | 25.66 | 25.70 |
| MCHB | 2025-09-03 | 135,017 | 472,559 | **3.5** | 176,485 | 13.54 | 12.79 |

SPCX is a 7.3× same-day price jump — a corporate action, not a volume spike. QH's and MCHB's 20-day
medians are 1 and 3.5 shares; "five times the median" has no meaning there.

## 3 · What the audit RULED OUT

* **Warm-up is clean.** 100 % of M6 rows sit on a full 20-bar window. No series-start artifact.
* **`nan_to_num(volume, 0)` is not the main driver.** `vol7_build.py:75` turns missing volume into
  zero, which would drag the median down and inflate later ratios — but only 0.81 % of M6 rows have
  any zero-volume bar in their window.
* **No ticker or day concentration.** 5,322 tickers over 1,311 sessions; top ticker 0.13 %, top-10
  1.2 %.

## 4 · Where M6 actually lives

| price band | share of M6 | | dollar volume on the M6 bar | share |
|---|---|---|---|---|
| < $2 | 17.96 % | | < $100k | 6.90 % |
| $2–8 | 27.28 % | | < $1M | 30.81 % |
| $8–21 | 27.25 % | | < $3M | 17.64 % |
| $21–89 | 17.41 % | | < $10M | 15.21 % |
| $89–377 | 5.47 % | | ≥ $10M | 29.43 % |
| ≥ $377 | 4.63 % | | | |

**45 % of M6 is under $8** and **37.7 % of M6 bars trade under $1M on the day itself.** The most
frequent M6 tickers (SPRC, PLAG, BAOS, IPDN, FACT, CLBR, AUUD, RETO) are nano-caps with 20-day median
volumes of 167–8,770 shares. That is the zone [[project-fib-price-zones]] already calls a lottery and
[[feedback-price-bucket-always]] says never to pool.

## 5 · Verdict

**DATA-QUALITY-BOUNDED FINDING — not a production veto, and not a clean behavioural result.**

M6 is a real, significant span — it passes the registered condition once the `(p or 1)` defect is
accounted for — but roughly a fifth of its population is not a volume spike in any useful sense, and
half of it sits below $8 and below $1M a day. Under the pre-registered three-way rule (clean → real
veto candidate · dissolves → artifact · partly survives → finding only), this is **the third case**.
It is a finding, not a production rule, and the reason is the contamination, not the ladder's
statistics.

## 6 · Explicitly not done

**No AMENDMENT_1 and no second outcome access.** The natural follow-up — re-measuring −8.4 with the
flagged rows excluded — would need a second outcome run on a sealed family whose own rule is
*"Stop: one outcome run"*, whose `OUTCOME_ACCESS_LEDGER.json` stands at 1, and which did not persist
`direct_trades.parquet` (so `_pathsim` would have to re-run). It was judged not worth it: M6 has no setup or veto status and nothing is built on it, so the
refinement could not change a production decision today. It remains the obvious next step if M6 is
ever proposed as a real veto. Nothing in production was touched. No rung was re-cut, no threshold moved, no filter added.

## ⭐ The takeaway worth more than the finding

**A very large apparent edge or veto magnitude must pass a raw-data plausibility audit BEFORE any
outcome rerun or production rule is considered — especially in volume-ratio families**, where the
statistic is a quotient and a near-zero denominator manufactures arbitrarily large values. Here the
denominator reached 1 share and the ratio reached 335,000×. The magnitude was the reason to look, not
evidence in itself.

Code: `backend/vol7_m6_audit.py`.
