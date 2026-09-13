# `MTF_ZERO` — causal-alignment audit

**Audit only. No outcome access. `MTF_ZERO` remains NOT EVALUATED.** Sealed 2026-09-13.

The question this pass answers is **not** "does the veto work?" but the one that has to come first:
**can `mtf_echo` be reconstructed honestly on the 1D fire bar, over history, without intraday
leakage?** The answer is yes on alignment and conditional on three contracts.

## The production definition being reconstructed

`backend/studio/ultra_db_scan.py:807-860`:

```sql
-- intraday REV on (ticker, calendar day) := EXISTS a bar that day with
close >= 5
AND m5 = MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date
                           ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) < 38
AND rsi_14 BETWEEN 30 AND 55
AND close > LAG(close) AND rsi_14 > LAG(rsi_14)
```
```python
mtf_echo := rev_buy AND (REV_4h OR REV_1h) on d0 (the 1D bar's own date) OR d1 (the prior one)
```

`mtf_echo = False` is the "validated hard veto (−1.07 %, 0/6yr)" that gates `edge_rev`. Its inputs
are exactly two intraday primitives: `close` and `rsi_14`, on 4H and 1H.

---

# The four headlines

## 1 · Alignment — **PASS**

| check | result |
|---|---|
| timestamps | **UTC**, and the grid tracks US DST correctly: Jan/Feb/Dec → 14:30, Apr–Oct → 13:30, Mar/Nov transitional |
| midnight crossing | **never** — every bar falls in 13:30–20:45 UTC, so calendar-day attribution is unambiguous |
| extended hours | **excluded** — 98.92 % of 4H ticker-sessions carry exactly **2** bars, 96.56 % of 1H carry exactly **7**: the regular 6.5-hour session |
| `m5` window | `ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING` — strictly prior bars, no self-inclusion |
| same-day `d0` | admissible — every intraday bar stamped `d0` closes at or before that session's 1D close, so it is known when the 1D signal is known |
| off-grid bars | 2.30 % (4H) / 2.41 % (1H) sit off the `hh:30` boundary — small, but they need a declared policy |

**No mechanical leakage was found.** What the audit did find is a different class of problem.

## 2 · Coverage — **BIAS RISK**

```
1D ticker-days              5,380,039
  covered by 4H             3,457,750   64.27 %    median $vol  $22,123,016
  covered by 1H             3,479,375   64.67 %
  MISSING                   1,922,289   35.73 %    median $vol      $269,969
```

**An 82× liquidity gap.** 3,203 tickers are covered; 5,690 appear in the missing set. Join
cardinality: 63.57 % of 1D ticker-days have exactly two 4H bars, 0.70 % have one, **35.73 % have
none**. A further 134,624 intraday ticker-days (3.75 %) are **orphans** with no 1D bar at all.

**Therefore `missing ≠ False`.** Assigning `mtf_echo = False` to an uncovered ticker-day would make
`MTF_ZERO` a liquidity proxy — precisely the pathology `BREAKOUT_SYSTEMS_REPORT.md` §1.1 already
named for LBAL and ANATOMY ("their presence is a 100× liquidity filter"). Uncovered rows are
**UNKNOWN** and must leave both arms.

## 3 · Warm-up — **INVALID AS STORED**

Across each ticker's first 13 intraday bars: n = 44,828 with **3,203 NULLs — exactly the ticker
count**, i.e. only the very first bar is NULL. **Every bar from the second onward already carries an
`rsi_14`**, computed from fewer than 14 observations.

The whole REV condition is RSI-based (`m5 < 38`, `30 ≤ rsi_14 ≤ 55`, `rsi_14 > prev`), so the early
series is not merely thin — it is **silently wrong**. Under the maturity contract
([[feedback-warmup-maturity-contract]]) a Wilder RMA of length 14 needs roughly **84 bars** for the
seed to decay: about **12 sessions on 1H and 42 sessions on 4H**.

**A historical reconstruction must compute its own mature RSI from raw bars — the stored `rsi_14`
cannot be used as-is.**

## 4 · The historical field does not exist — **RECONSTRUCTION REQUIRED**

`ultra_db_scan.py:813` caps the REV query at `date >= (SELECT max(date) FROM bars) - INTERVAL 14 DAY`.
The served `mtf_echo` is a **live helper over the last 14 days only**. There is no stored history and
no back-fill. Any study must recompute the condition across the full span — 7,145,816 4H bars and
24,990,532 1H bars, 2021-07-02 to 2026-09-11, 3,203 tickers.

This is why the breakout pass reported `MTF_ZERO` as **NOT EVALUATED rather than null**, and
invented no substitute. That call was right.

---

## Status

**`MTF_ZERO` is still NOT EVALUATED.** This audit establishes only that a causal reconstruction is
*possible under the right contract*. **No outcome was opened and nothing is known about predictive
value.**

## The contract the evaluation spec must carry (agreed, to be frozen separately)

1. **Universe** — only ticker-days where the 1D bar exists, `close ≥ 5`, **both** required intraday
   sources have coverage, and the required warm-up is mature.
2. **UNKNOWN** — any coverage or maturity failure removes the row from **both** the `MTF_ZERO` arm
   and the comparator. Never coded as `False`.
3. **Feature reconstruction** — causal REV recomputed from raw 1H/4H bars, **not** the stored
   `rsi_14`.
4. **Primary estimand** — `MTF_ZERO` versus matched non-zero echo inside that same eligible universe.
5. **No outcome access** until a coverage census, feature-prevalence census and day/ticker
   concentration check have passed their sanity checks.

Order of work: **this audit → feature-only historical reconstruction and census → only then an
outcome spec.**

Note on the `close ≥ 5` floor: it already removes **18.72 %** of 1D bars, which can never produce a
REV on any timeframe. Combined with coverage, the evaluable population is a liquid, ≥ $5 subset. That
subset is the **declared universe for both arms**, not a filter applied to one side.

Code: `backend/mtf_zero_alignment_audit.py`.
