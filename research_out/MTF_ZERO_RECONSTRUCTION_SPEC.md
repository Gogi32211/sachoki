# `MTF_ZERO` — feature reconstruction spec · FROZEN 2026-09-13

**Nothing here may be changed once the build runs.** This is a FEATURE BUILD, not a study. **No
outcome is opened.** `MTF_ZERO` remains NOT EVALUATED throughout.

The risk this spec exists to control is no longer outcome leakage — the alignment audit
(`MTF_ZERO_ALIGNMENT_AUDIT.md`, `a2faf01`) cleared that. **The risk is feature-definition drift**: a
historical port that quietly computes something other than what production computes. So the
acceptance test is fixed *before* the build.

## 1 · Output contract — `data/mtf_rev_signals.parquet`

One row per `(ticker, date)` over the full intraday span.

| column | type | meaning |
|---|---|---|
| `ticker` | VARCHAR | |
| `date` | DATE | the session's calendar day (unambiguous — the audit showed bars never cross midnight UTC) |
| `rev_4h` | BOOLEAN, **nullable** | the intraday REV condition on 4H — `NULL` unless decidable |
| `rev_1h` | BOOLEAN, **nullable** | the same on 1H |
| `covered_4h` | BOOLEAN | a 4H bar exists for this ticker-day |
| `covered_1h` | BOOLEAN | a 1H bar exists for this ticker-day |
| `mature_4h` | BOOLEAN | the 4H series has enough history at this bar for a valid RSI-based REV |
| `mature_1h` | BOOLEAN | the same on 1H |

**`covered` and `mature` are stored separately on purpose.** Both collapse to UNKNOWN in an
evaluation, but they are different failures and provenance must distinguish "we have no bar" from
"we have a bar too early in the series to trust".

## 2 · Tri-state semantics

`rev_4h` / `rev_1h` are `TRUE` or `FALSE` **only** when all three hold for that source:

1. `covered_*` — a bar exists for the ticker-day;
2. `mature_*` — the maturity contract below is satisfied;
3. the session is valid — the bar sits on the regular-hours grid the audit established.

Otherwise the value is **`NULL`. Never `False`.** Coding UNKNOWN as `False` is the single failure
mode that would turn `MTF_ZERO` into a liquidity proxy (audit §2: coverage 64.3 %, missing
population 82× less liquid).

## 3 · RSI reconstruction

**The stored intraday `rsi_14` is not used as a feature value anywhere.** The audit showed it is
populated from each ticker's *second* bar — built on fewer than 14 observations, and the whole REV
condition is RSI-based.

The build recomputes a causal Wilder RMA-14 from the same raw `close` series, and the maturity
threshold is **imported, not written as a literal**:

```python
from v3a_warmup_contract import RSI14        # = rma_hist(14) = 81 bars, SEED_DECADES = 6.0
REV_MIN_BARS = RSI14 + 5 + 1                 # 87 — rsi must also be mature across the m5 window
                                             # (5 strictly-preceding bars) and its LAG
```

`mature_* := bar_index >= REV_MIN_BARS − 1` (0-indexed), i.e. the 87th bar of that ticker's series
on that timeframe — about **13 sessions on 1H** (7 bars/session) and **44 sessions on 4H**
(2 bars/session).

## 4 · The REV condition — semantics only

Ported verbatim from `studio/ultra_db_scan.py:807-819`:

```
close >= 5
AND m5 = MIN(rsi_14) over the 5 STRICTLY PRECEDING bars < 38
AND rsi_14 BETWEEN 30 AND 55
AND close > LAG(close)
AND rsi_14 > LAG(rsi_14)
```

REV on a ticker-day = **EXISTS** such a bar that day.

⚠️ **The live query's `date >= max(date) - INTERVAL 14 DAY` is an operational lookback optimisation
and MUST NOT be carried into the historical feature definition.** Only the semantics port. Likewise
the live `ticker IN (...)` restriction is a scan-scope detail, not part of the feature.

## 5 · Eligible-1D-universe census — attrition, with liquidity visible

Reported as a ladder, each step with its row count and **median dollar volume**, so any liquidity
selection is visible rather than implied:

```
all 1D ticker-days  →  close >= 5  →  4H covered  →  1H covered
                    →  4H mature   →  1H mature   →  FINAL ELIGIBLE
```

## 6 · Feature prevalence — before any outcome

| cell | |
|---|---|
| `rev_4h = TRUE` | |
| `rev_1h = TRUE` | |
| both TRUE | |
| **both FALSE** | the candidate `MTF_ZERO` population |
| one known / one UNKNOWN | |
| both UNKNOWN | |

Plus **day and ticker concentration** of the both-FALSE cell, reported separately.

## 7 · Parity with the live `mtf_echo` — the gate

Checked **only on comparable rows**, all three required:

1. the live helper had both sources for that ticker-day;
2. the historical reconstruction is `covered` **and** `mature` for both;
3. the timestamp / session boundary matches exactly.

**Frozen acceptance: 100 % agreement on comparable rows.** The feature is deterministic; there is no
tolerance band to negotiate afterwards. Reported: exact agreement rate · disagreement count ·
a disagreement sample trace.

* Any mismatch must be **explained** by a producer-version or date-window difference, named
  explicitly.
* **Unexplained mismatch → STOP.** The build is not accepted, and no outcome evaluation begins.

## 8 · Explicitly not done in this build

No outcome access. No evaluation of `MTF_ZERO`'s predictive value. No threshold search, no filter,
no production change. `mtf_echo` in production is untouched — this writes a new research artifact
only. The outcome spec is a **separate** freeze, and it may only be written after §5, §6 and §7 pass.
