# V4_BOOK_WEIGHTS_V1 — V4 re-scored with the book-verdict weight map

**Date** 2026-09-24 · **trial 2 of the V4 idea** (V4_HISTORY_V1 = trial 1, NULL) · pre-registered before scoring, n_trials=12 (8 used), DSR family carries the 28 prior variants.
**Kit verdict: BUILD (V4 90+)** — with the honesty caveats below, this is a *forward-frozen candidate*, not a finished edge.

## The map (frontend/src/lib/v4Weights.js, 94 non-zero of 614)
Weights come from **already-sealed per-family verdicts** in the book, not from V4_HISTORY_V1's table:
15 = validated 5-6yr edges (34: EDGE-board BUILD codes, 💠TRIPLE, DIV deep) · 10 = validated intraday confirmations, backtested combos, 🏆RS, ABSORB (26) · 5 = ⚛ physics states that kept their sign in both windows, VABS STR/BEST, red L34 (18) · 0 = everything sealed descriptive/NULL (L-BAL, L-VX, OVD, SHAPE, PV, VOL7 mid, PT, plain T/Z/L, MTF-EMA alone, anatomy ladder) · −10/−5 = measured VETOes (16). Previous all-5 map: `research_out/v4Weights.user_all5_2026-09-24.js`.
Coverage: 87.2 % of positive points reproducible (110/860 not — turn_echo, mtf_conf, REV+echo, div_buy, rs_strong: the intraday-echo families, live-only). **Live V4 therefore runs higher than this history for the same setup.**

## Same universe, same outcome as trial 1
S&P 500, 620 tickers, 749,474 sessions, 2021-08-02 → 2026-08-25, fwd_20d close-to-close, day-clustered CIs. Matched baseline **+0.49 % [+0.25, +0.70]**, 5/6 yr, worst −0.73.

## Deciding test — rank IC: still killed (and why that is not the whole story)
V4 IC −0.0008, ICIR −0.009 across the full cross-section. 62 % of rows have no validated signal at all (V4 ≤ 10), so a whole-population rank is dominated by ties and vetoes. **V4 is not a ranker of the market; it is a sparse stack-of-validated-edges score.** The band question is the right one.

## Bands — a staircase, not a peak
| V4 | n | median fwd20 | CI | yrs pos | worst | Δ base |
|---|---|---|---|---|---|---|
| <0 (veto) | 142,762 | +0.35 | [+0.12, +0.57] | 4/6 | −1.22 | −0.13 |
| 0-10 | 318,575 | +0.42 | [+0.20, +0.65] | 5/6 | −0.91 | −0.06 |
| 10-25 | 180,365 | +0.47 | [+0.22, +0.72] | 5/6 | −0.74 | −0.01 |
| 25-40 | 69,397 | +0.65 | [+0.33, +0.97] | 5/6 | −0.26 | +0.16 |
| **40-60** | 28,288 | **+1.10** | [+0.73, +1.47] | **6/6** | **+0.58** | +0.61 |
| **60-90** | 7,473 | **+1.70** | [+1.07, +2.31] | 5/6 | −0.07 | **+1.22** |
| **90+** | 2,614 | **+2.60** | [+1.46, +3.75] | 5/6 | −0.12 | **+2.11** |
| ≥45 (screen green) | 26,868 | +1.47 | [+0.93, +1.90] | 6/6 | +0.36 | +0.98 |

Every step up; win % 51.8 → 61.5. Per year, 90+ is +2.21 / **+2.17 (2022)** / +1.96 / +2.88 / +4.79 for 2021-25; the only soft year is the partial 2026 (−0.12, and 60-90 −0.07) — both ends near zero, small n, and it is the most recent window. $21-89 beats "other" in every band up to 60-90 (price law still active underneath).

## Controls
- **Placebo (same weight multiset, random keys) ×20**: top-band lift mean **+0.24**, p95 +0.95 — V4 90+ = +2.11 → **beats 100 % of placebos.** The *selection* of keys carries it, not the shape of the weights.
- **Plain count** of fired weighted keys: ≥4 → +0.76 only. Counting is not enough; which keys, is.
- Leave-one-family-out on IC is uninformative here (IC ≈ 0 by construction of a sparse score) — not pursued, no extra trials.
- DSR of 90+ against 56 variants (this + prior study) = **0.924**.

## What "BUILD" means here — read this before acting
1. **Not clean out-of-sample.** The Tier-1 keys are the EDGE-board setups, validated on this same 2021-26 DB. V4 90+ ≈ "≥6 book-validated signals on one bar" — it re-measures that those setups stack, which is the 🎯 Cluster-Bottom finding in a new coat, not a new discovery. The honest test is **forward**: freeze this map today and score sessions after 2026-09-24.
2. **Live ≠ history by +10…+30.** The intraday-echo families (12.8 % of points) fire live but were not in this history, so a live 45 is not this study's 45. If a threshold is chosen from this table, expect the live equivalent to sit higher.
3. The rise **starts around 25-40** (+0.16), is **solid from 40** (6/6 yr, worst +0.58), **strong from 60** (+1.2 over baseline), and **best at 90+** (+2.1, ~500 sessions/yr universe-wide ≈ 2 per day).
4. The <0 (veto) band is the worst band (4/6 yr, worst −1.22) — the negative weights are doing their job.

## Not changed
Screen threshold (green ≥45) left as the user set it — on this history it is 6/6 yr, +0.98 over baseline, p97 of the score. Any second tier (≥60 / ≥90) is a UI decision for the user.

Artifacts (session scratchpad `v4hist/`): `deps2.json`, `scores2.ndjson`, `study2.log`, `study2_result.json`; runner `runner.mjs`, `build_rows.py`, `study2.py`.
