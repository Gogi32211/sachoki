# DR_PAIRS_V1 + EDGE_IN_TURNZONE_V1

**Date** 2026-09-28 · user: "gaakete orive" · both plans frozen to `drpairs_edgezone_plan.txt` before any outcome · k = 2 + 3.
**Universe.** All 5,739 US tickers in the DB. Book ATR×12 trail, maxh 60, entry open[t+1].

## Study 1 — do FLP↑ / gG3 improve 🕐DR? → **FAIL (both)**
The pairs were selected on MINE for identification, and their VERIFY returns were already seen, so MINE trade returns are the deciding window.

| MINE 2021-23 | n | mean | PF | same-day Δ [CI] |
|---|---|---|---|---|
| **🕐DR (all)** | 10,007 | **+2.82 %** | 1.58 | **+2.37 [+1.21, +3.96]** |
| FLP↑ + 🕐DR | 2,724 | +2.83 % | 1.59 | +1.99 [+0.34, +4.06] |
| gG3 + 🕐DR | 1,934 | +2.10 % | 1.42 | +1.30 [+0.08, +2.43] |
| pair − 🕐DR without that partner | | FLP↑ −0.01 [−1.01, +1.04] · gG3 −0.76 [−2.96, +1.20] | | |

**VERIFY 2024-26 (seen, descriptive):**
- 🕐DR +4.97 % mean, same-day +2.35.
- gG3+🕐DR +7.18 % (−DR-without-gG3 +2.80 [−0.12, +5.34]).
- FLP↑+🕐DR −0.35 vs DR alone.

**Reading.** **🕐DR itself is the edge**: same-day +2.37 and +2.35 in the two windows, PF 1.6-2.0. Neither partner improves it in the unseen window. gG3's VERIFY lift is not supported by MINE (−0.76), so it stays a lead, not a finding.

## Study 2 — do the existing EDGEs work better inside a turn zone? → **S2 ⟲ROW●≥2 PASS; S1, S3 FAIL**
The measure is the pooled mean return of all per-code EDGE fires in the state minus those not in the state, with day-clustered CI.

| state (coverage of bars) | MINE Δ [CI] · years | VERIFY Δ [CI] · years | verdict |
|---|---|---|---|
| S1 TURN·58 ≥ 20 (2.2 %) | +0.72 [−0.48, +1.84] · −0.34/+1.59/+0.57 | +1.68 [−0.03, +3.55] | FAIL |
| **S2 ⟲ROW ≥ 2 rows confirmed (11 %)** | **+1.69 [+0.83, +2.37] · +0.63/+2.04/+1.83** | **+0.97 [+0.09, +1.96] · +0.67/+1.66/+0.42** | **PASS** |
| S3 TOP pair any (9 %) | +0.87 [−0.20, +1.73] · 3/3 + | +1.19 [+0.15, +2.25] · 3/3 + | FAIL (MINE CI) |

**Post-hoc audit (disclosed; it changes the interpretation).** Compare the same-day Δ vs control:

| | MINE, in-state | MINE, out-of-state | VERIFY, in-state | VERIFY, out-of-state |
|---|---|---|---|---|
| S2 | +1.08 | +0.81 | 0.00 | +0.25 |

So **on the same day**, an edge inside a ⟲ROW turn zone does *not* beat an edge outside one. The pooled advantage comes from **timing**: ⟲ROW≥2 states cluster on days when many names are turning at once, and those days pay. For a trader that is still money per trade, since you take fewer, better-timed edge fires. It is **not** cross-sectional selection, and it depends on those market-wide days continuing to pay. Every year is positive in both windows, which argues against a single-episode artefact.

**Per-code (descriptive, both windows pooled, in- vs out-of-state mean return):** most codes are higher inside a TOP-pair or ⟲ROW state. Examples:

| code | in-state | out-of-state |
|---|---|---|
| G3² | 6.9 % | 4.0 % |
| ATM | 3.4 % | 2.0 % |
| QZC | 4.0 % | 2.0 % |
| G3L46 | 5.9 % | 3.3 % |
| WSH🔎 | 4.6 % | 1.2 % |
| ENG | 8.4 % | 1.0 % |

## What to do with it (needs user OK)
- **Use 🕐DR as is.** Do not add the FLP↑ or gG3 filter.
- **As a timing filter, take EDGE fires when ⟲ROW ≥ 2 rows are confirmed.** Ultra already has both chips: `ROW●≥2` plus any EDGE. A combined "EDGE in ⟲ROW zone" chip or a forward registration would be the next step.

Artifacts (session scratchpad `v4hist/`): `drpairs_edgezone_plan.txt`, `drpairs_edgezone.py`, `drpairs_edgezone.log`. No UI, score or memory change.
