# TZL_IN_ZONE_V1 — are the TZL bottom sequences stronger inside a ⟲ROW turn zone (like the EDGEs)?

**Date** 2026-09-28 · user hypothesis: "as edges are stronger in the zone, TZL should be too" · plan frozen to `tzlzone_plan.txt` before any outcome · k = 1.
**Verdict: MINE PASS → VERIFY FAIL.** The zone effect does not replicate for TZL, unlike EDGE_IN_TURNZONE_V1.

## Setup
- **Events.** The 30 TZL sequences frozen in TZL_BOTTOM_SEQ_V1: 43,043 fires, of which 11.8 % fall inside ⟲ROW≥2.
- **Exit.** Book ATR×12 trail, maxh 60.
- **Universe.** All 5,739 DB tickers.
- **Primary measure.** Pooled mean return of fires inside ⟲ROW≥2 minus fires outside it, with a day-clustered CI.

## Results

| | MINE 2021-23 | VERIFY 2024-26 |
|---|---|---|
| TZL all: mean · PF · same-day Δ | −1.81 % · 0.79 · +0.19 [−0.57, +0.95] | +1.65 % · 1.22 · −0.36 [−1.08, +0.29] |
| TZL in ⟲ROW≥2 | +0.92 % · PF 1.12 | +0.91 % · PF 1.12 |
| TZL outside | −2.18 % · PF 0.75 | +1.73 % · PF 1.23 |
| **in − out (S2)** | **+3.10 [+1.69, +4.44]** · +2.4 / +4.2 / +2.5 | **−0.82 [−2.17, +0.53]** · +0.45 / −2.16 / −0.71 |
| in − out (S3 TOP pair, descriptive) | +4.13 [+2.36, +5.61] | −0.72 [−2.33, +0.89] |

## Reading
- **The TZL bottom sequences are not trades.** The same-day Δ is ≈ 0 in both windows.
- **The zone helped them in 2021-23 and not in 2024-26.** The MINE effect fits the 2022 bear market, when only the zone's market-wide turning days paid. In the 2024-26 up-market, fires outside the zone did as well or better.
- **The EDGE result (EDGE_IN_TURNZONE_V1) replicated because the edges themselves carry value.** A timing filter can lift a positive-expectancy signal, but it cannot turn a zero-expectancy one into a trade.
- **Per-sequence splits are small (n 100-400 inside the zone) and mixed**, e.g. Z2G·L46→Z2G·L46→T1·L12 in +7.2 % vs out +2.3 %, but Z5·L12→T1G·L3 in −2.0 % vs out 0.0 %. None is actionable without its own sealed test.

Artifacts (session scratchpad `v4hist/`): `tzlzone_plan.txt`, `tzlzone.py`, `tzlzone.log`. No UI, score or memory change.
