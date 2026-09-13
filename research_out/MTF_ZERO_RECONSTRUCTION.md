# `MTF_ZERO` — feature reconstruction · BUILT, PARITY GATE PASSED

**Feature build only. No outcome was opened. `MTF_ZERO` remains NOT EVALUATED.** 2026-09-13.
Spec: `MTF_ZERO_RECONSTRUCTION_SPEC.md` (`c88f4e4`). Audit: `MTF_ZERO_ALIGNMENT_AUDIT.md` (`a2faf01`).

Output `data/mtf_rev_signals.parquet` — **3,614,324 ticker-days**, 2021-07 … 2026-09, tri-state.
Code `backend/mtf_rev_build.py` (build) and `backend/mtf_rev_census.py` (§5–§8).

## §7 first — the gate, because it decides whether anything else counts

| window | TF | comparable rows | agree | disagree | rate |
|---|---|---:|---:|---:|---:|
| 2024-03-01..20 | 4H | 36,126 | 36,126 | 0 | **100.0000 %** |
| 2024-03-01..20 | 1H | 36,354 | 36,354 | 0 | **100.0000 %** |
| 2025-10-01..20 | 4H | 38,839 | 38,839 | 0 | **100.0000 %** |
| 2025-10-01..20 | 1H | 39,135 | 39,135 | 0 | **100.0000 %** |
| 2026-05-01..20 | 4H | 40,068 | 40,068 | 0 | **100.0000 %** |
| 2026-05-01..20 | 1H | 40,344 | 40,344 | 0 | **100.0000 %** |

**GATE: PASS** on 231,866 comparable rows across three years. The frozen acceptance was 100 %, and
it is met exactly — as it must be for a deterministic feature.

### The gate caught a real port error — mine

The first build scored **78.19 % (4H) / 84.70 % (1H)**. Two separate causes, and only one of them
was the producer's.

**(a) My port was wrong.** In the production query `close >= 5` sits **inside the CTE's WHERE**, so
it filters bars *before* the window functions run: `m5`, `LAG(close)` and `LAG(rsi_14)` reference the
previous bar **above $5**, not the previous bar. My first build applied the floor as a per-bar
predicate and computed the windows over all bars. Worked example — **BBBY 4H, 2026-05-01**, after a
three-session dip below $5 (4.97 / 4.73 / 4.83 / 4.82 / 4.96 / 4.91):

* my version: `m5` = min over the actual preceding bars = **35.9** < 38 → REV **True**
* production: those bars are gone; `m5` = min over the preceding *above-$5* bars = **49.4** → **False**

Every residual disagreement on the clean window was this, one-directional (`mine=True/live=False`,
24 of 40,068 on 4H). Fixed by filtering before the windows, exactly as production does. **This is
what the gate was for**: a feature that looked right and quietly wasn't.

**(b) The benchmark itself is broken on recent dates** — see §8. The live helper's own 14-day window
sits entirely inside that period, which is why the gate is run on clean windows instead.

## §8 · Producer anomaly — reported, NOT resolved here

From about **2026-06-30** the stored intraday `rsi_14` stops following a Wilder-14 over the stored
`close` series. On ~26 sessions it diverges across essentially every ticker (59 of 60 sampled).

| store | median bar-to-bar \|Δ\| | p99 | max | bars jumping > 20 pts |
|---|---:|---:|---:|---:|
| 1D | 0.80 | 14.10 | 100.0 | 0.378 % |
| 4H | 2.50 | 17.20 | 100.0 | 0.614 % |
| 1H | 2.40 | 17.20 | 100.0 | 0.555 % |

A Wilder-14 cannot move 20 points in one bar. **AAPL 4H, 2026-08-19: stored `rsi_14` = 84.1**, while
the previous stored bar is 23.9 and *every* reconstruction gives 51.5–59.5 — full series 51.5,
trailing-window reseeds from 15 to 800 bars 51.5–59.5, SMA-14 variant 46.6. **No RSI over that close
series reproduces the stored value.**

Ruled out: retroactive backfill (4H is a clean 2 bars per ticker-day in every recent month, and
off-grid bars are *falling* — 89–100/month recently against 1,000–1,700 in 2025-12 … 2026-04).

⚠️ **This is a live production concern, not just a research one.** `studio/ultra_db_scan.py` reads
this column to compute the served `mtf_echo` and the `⚠️REV` veto. On affected sessions those are
computed from values that do not follow from the price series. **Cause unknown; needs its own
investigation.** Nothing was changed here.

## §5 · Eligible 1D universe — attrition, liquidity visible

| step | ticker-days | kept | tickers | median $vol |
|---|---:|---:|---:|---:|
| all 1D ticker-days | 5,380,039 | | 5,714 | $6,111,355 |
| `close >= 5` | 4,391,244 | 81.6 % | 5,419 | $12,005,257 |
| + 4H covered | 3,162,936 | 58.8 % | 3,172 | $26,990,956 |
| + 1H covered | 3,162,710 | 58.8 % | 3,172 | $26,991,892 |
| + 4H mature | 3,036,612 | 56.4 % | 3,134 | $27,548,212 |
| **+ 1H mature = FINAL** | **3,036,612** | **56.4 %** | **3,134** | **$27,548,212** |

**The eligible universe is 56.4 % of 1D ticker-days and 4.5× more liquid than the whole.** That is
the declared universe for *both* arms of any future comparison — never a filter on one side.

## §6 · Feature prevalence — no outcome involved

| cell | rows | share |
|---|---:|---:|
| 4H only | 243,035 | 6.72 % |
| 1H only | 657,128 | 18.18 % |
| both TRUE | 340,803 | 9.43 % |
| **both FALSE — candidate `MTF_ZERO`** | **2,212,873** | **61.23 %** |
| one known / one UNKNOWN | 117,740 | 3.26 % |
| both UNKNOWN | 42,745 | 1.18 % |

`rev_4h` TRUE on 583,867 of 3,454,105 decidable (16.90 %); `rev_1h` on 1,028,528 of 3,571,313
(28.80 %).

**`MTF_ZERO` is not a rare event — it is the majority state**, 61.23 % of ticker-days and a median
**65.0 %** of each session's names (range 0–100 % across 1,250 sessions). Concentration is nil:
3,171 tickers over 1,247 sessions, top ticker 0.056 %, top-10 0.56 %. A veto that fires on two
thirds of the field is a very different object from a selective one, and any evaluation has to
reckon with that.

## Status and what comes next

**`MTF_ZERO` is still NOT EVALUATED.** The feature now exists, honestly and verifiably, over the
full span. Nothing is known about predictive value.

The outcome spec is a separate freeze. Two things must go into it that this build surfaced:
1. the eligible universe is liquidity-selected (4.5×) and must be declared for both arms;
2. the candidate population is the **majority** state, so the comparator design matters more than it
   would for a rare signal.

Production untouched: `mtf_echo` still serves from `ultra_db_scan.py`. This wrote a research
artifact only.
