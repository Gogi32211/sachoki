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

## §8 · ⚠️ CORRECTION — the cause is an intraday OUTAGE, not an RSI producer bug

**The first version of this section was wrong about the cause and is corrected here rather than
silently replaced.** It reported that "the stored intraday `rsi_14` stops following a Wilder-14 over
the stored close series". That statement is literally true, and the diagnosis drawn from it — an RSI
producer anomaly — was not.

**What actually happened: the 1H and 4H stores lost whole sessions.**

| store | damaged sessions (since 2026-06-01) | first | lost ticker-days |
|---|---:|---|---:|
| **15m** | **0 / 72** | — | — |
| **1D** | **0 / 72** | — | — |
| **1H** | **20 / 72** | 2026-07-20 | **62,612** |
| **4H** | **27 / 72** | 2026-06-29 | **84,321** |

A damaged session holds **0–5 tickers instead of ~3,100**. The affected interval is
**2026-06-29 … 2026-09-01**; every session from **2026-09-02** onward is complete again. Every one of
1H's 20 damaged dates is inside 4H's 27 — a shared upstream failure, not independent per-timeframe
corruption. Two independent methods agree on those counts: a comparison against the healthy 15m
calendar, and the build's own intrinsic health rule.

**Why the RSI looked broken.** `build_intraday_db._build_one` fetches 30-minute bars, resamples, and
runs `enrich_ticker_df` over the **complete fetched series** — then only the rows for sessions that
survive the write reach the database. So the stored `rsi_14` is a full-series value while the stored
`close` series has holes. AAPL simply has no 1H/4H bars on 2026-08-14, 08-17 or 08-18, which is why
its stored RSI reads 23.9 then 84.1, and why no reconstruction over the stored closes — full series,
trailing reseeds from 15 to 800 bars, or an SMA-14 variant — could reproduce 84.1. The RSI was the
symptom; the missing bars are the disease.

**Production impact, unchanged by the correction.** On a damaged session `studio/ultra_db_scan.py`
finds an empty `_rev_sets`, so `mtf_echo` is False and the `⚠️REV` veto fires. Through July and
August that veto was measuring **absence of data**, not absence of echo. Cause of the outage not yet
established; nothing was changed here.

### What this forced in the build — the session-health guard

A damaged session is **not "no REV"; it is NO DATA**, and must read UNKNOWN. The build now judges
health at **date level, per timeframe, independently** (the 1H ⊂ 4H relationship is an observation,
never hard-coded), and a damaged date sets `covered_* = False` → `rev_* = NULL`.

Two things this cost, both worth recording:

* **The calendar must come from outside the store.** A session that failed so badly it wrote no rows
  is invisible in that store's own date list, yet it still tears a hole in every ticker's series.
  Judging health on the store's own dates missed 14 of the 27 damaged 4H sessions. The trading
  calendar now comes from the 1D store.
* **The health window must be much longer than the outage it detects.** At a 21-session rolling
  median the median itself collapsed to 2–5 in the middle of the outage, so a session holding one
  ticker passed as healthy against its equally-broken neighbours — the rule silently inverted. At
  251 sessions the median stays pinned to the healthy majority. `HEALTH_WIN = 251`.

**Contamination reaches forward.** A gap does not damage only its own session: the Wilder recursion
and the `m5` / `LAG` windows keep referencing pre-gap bars. A bar is decidable only when the whole
`REV_MIN_BARS` span behind it is healthy — 44 sessions on 4H, 13 on 1H. That moves **188,549** 4H and
**88,867** 1H ticker-days to UNKNOWN.

The three parity windows are all pre-outage, so **§7 is unaffected and still passes at 100 %**.

## §5 · Eligible 1D universe — attrition, liquidity visible

| step | ticker-days | kept | tickers | median $vol |
|---|---:|---:|---:|---:|
| all 1D ticker-days | 5,380,039 | | 5,714 | $6,111,355 |
| `close >= 5` | 4,391,244 | 81.6 % | 5,419 | $12,005,257 |
| + 4H covered | 2,990,175 | 55.6 % | 3,169 | $26,932,642 |
| + 1H covered | 2,989,949 | 55.6 % | 3,169 | $26,933,756 |
| + 4H mature | 2,964,028 | 55.1 % | 3,104 | $27,204,646 |
| **+ 1H mature = FINAL** | **2,964,028** | **55.1 %** | **3,104** | **$27,204,646** |

**The eligible universe is 55.1 % of 1D ticker-days and 4.5× more liquid than the whole.** That is
the declared universe for *both* arms of any future comparison — never a filter on one side.

## §6 · Feature prevalence — no outcome involved

| cell | rows | share |
|---|---:|---:|
| 4H only | 237,779 | 6.58 % |
| 1H only | 639,021 | 17.68 % |
| both TRUE | 334,202 | 9.25 % |
| **both FALSE — candidate `MTF_ZERO`** | **2,162,687** | **59.84 %** |
| one known / one UNKNOWN | 138,737 | 3.84 % |
| both UNKNOWN | 101,898 | 2.82 % |

`rev_4h` TRUE on 572,010 of 3,373,955 decidable (16.95 %); `rev_1h` on 1,009,557 of 3,512,160
(28.74 %).

**`MTF_ZERO` is not a rare event — it is the majority state**, 59.84 % of ticker-days and a median
**65.0 %** of each session's names (range 9.7–94.8 % across 1,208 sessions — the old 0 % and 100 %
extremes were the damaged sessions, and their disappearance is a sanity check on the guard).
Concentration is nil: 3,137 tickers over 1,208 sessions, top ticker 0.056 %, top-10 0.56 %. A veto that fires on two
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
