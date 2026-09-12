# V3-A — MAJOR BREAKOUT DISCOVERY (MFE20_ATR ≥ 8)

## VERDICT: **NULL / NO IMPLEMENTATION**

Closed 2026-09-12 at the discovery stage. **2024-2025 was never opened. 2026 was never opened.**
No candidate registry was frozen (`data/v3a_registry.json` does not exist). No Pine was written.
No production code was changed.

---

## 1. What was asked

Find, on DISCOVERY 2021-2023 only, a state or a two-way conjunction that predicts a **major**
breakout — `(max(high[t+1..t+20]) − close[t]) / ATR14[t] ≥ 8` — well enough to beat the better of
two trivial comparators by a margin that would matter in practice.

Frozen before any result was read:

| item | value |
|---|---|
| target | `Y8 = MFE20_ATR ≥ 8`, base rate **8.57 %** on DISCOVERY |
| splits | DISCOVERY 2021-2023 · VALIDATION 2024-2025 · HOLDOUT 2026 (physically locked) |
| event definition | event-level collapse, cooldown 10, sensitivity reported at 5/10/15 |
| baseline | matched (ticker, year) cells; never the global base rate |
| comparators | BL3 `RSI14 > 50` · BL4 `close > max(high[t-20:t-1]) AND volume > 1.5 × avg_vol_20d` |
| **gate** | candidate − max(BL3, BL4) **≥ +5.00 pp AND ≥ 1.10×**, n ≥ 300, top-5 ticker share ≤ 15 % |
| search budget | Phase 1 all primitives · Phase 2 ≤ 6 families × ≤ 2 · Phase 3 ≤ 15 pairs · registry ≤ 5 |
| survivor rule | n ≥ 1000 · Δ ≥ +1.0 pp · top5 ≤ 15 % · positive in ≥ 2 of 3 years |

BL3 scored **10.45 %** on DISCOVERY, so the gate was an absolute precision of **15.45 %**.

---

## 2. Two defects found and fixed before the result was accepted

### 2.1 The target was contaminated at the start of every series

`Y8 = MFE20 / ATR14[t]`. ATR14 is a Wilder RMA seeded on the first bar, so at the start of a
series it is roughly 20 % too small and the ratio is correspondingly too large.

| bar index in series | rows | ATR14/close | **Y8** |
|---|---:|---:|---:|
| 0-3 | 8,559 | 3.46 % | **13.13 %** |
| 6-10 | 11,449 | 3.64 % | 9.76 % |
| 20-40 | 56,819 | 3.99 % | 8.24 % |
| 120+ | 1,331,611 | 4.30 % | **8.47 %** |

The DB's 1d history begins 2021-05-26 for 3,741 of 3,748 tickers, so this zone sits inside
DISCOVERY for essentially the whole universe.

### 2.2 The event collapse degenerates for dense states

`sig_t` is true on **47.1 %** of bars. Under a 10-bar cooldown that produces **1.64 events per
ticker** over ~650 bars — in effect "the first bar of the series", which is exactly the
contaminated zone. 61.5 % of `sig_t`'s events and 71.3 % of `sig_l2`'s sat on 3.43 % of rows.

The mechanism for L_VSA is worse than immaturity. `compute_wlnbb` returns NaN for the first 19
bars and `signal_extraction` substitutes **0.0**, so every volume-Bollinger comparison becomes
`v < 0.0` = False and the volume bucket collapses to one category for the whole warm-up. The
value is not noisy — it is wrong.

**Effect on the first run's headline results:** `sig_l2` **+3.43 → +0.11**, `sig_t` +3.01 → out.
These were the #1 and #2 primitives of the entire screen and both Phase-3 anchors. The first
Phase-3 NULL was therefore not a valid pre-registered test, and the whole programme was re-run.

### 2.3 The fix — `backend/v3a_warmup_contract.py`

Two layers, neither derived from Y8:

- **A. Global target maturity** — `bar_index ≥ 20` for every observation.
- **B. Feature-specific maturity** — `bar_index ≥ min_history(source)`, read off the producer's
  own definition. 282 columns, complete coverage enforced by assertion.

Precedence: a producer-declared contract wins → a non-recursive n-window requires n → a recursive
filter with no declared contract requires its seed weight to fall below e⁻⁶ (EMA span N ≈ 3N,
Wilder RMA length L ≈ 6L) → a stateful tracker with neither gets the global floor, labelled.

`EMA20 → 60 · EMA50 → 150 · EMA89 → 267 · EMA200 → 600 · Wilder RMA14 → 81`

Reading the producers also corrected two assumptions: T/Z raws are pure two-bar candle
comparisons and need **2** bars, not 200; and `phys_k_x` is literally `(close − ema20)/atr_14`,
the same formula as `dist_ema20_atr`, so both carry the ATR14 requirement (81).

**Known and deliberate incoherence:** ATR14 requires 81 bars when it feeds a *feature* and 20 when
it feeds the *target*, because the global floor was frozen at 20. Raising it to 81 is one line.

---

## 3. Result

| | pre-fix | post-fix |
|---|---:|---:|
| primitives screened | 549 | **564** |
| raw survivors | 39 | **30** |
| unique survivors | 36 | **29** |
| duplicate groups | 14 | 11 |

Phase 2 selected 10 representatives across 6 families. TZ and L_VSA, previously #1 and #2, fell
out of the top six entirely. Phase 3 tested all **C(6,2) = 15** cross-family conjunctions;
8 were evaluable, and **synergy was positive in all 8** — the clean primitives do combine.

### The one pair that cleared the frozen gate — and why it was rejected

`phys_e_raw>p90 & bar_gap_range=G3-C` — a true gap of ≥ 0.5 ATR followed by a bar whose range is
< 0.5 ATR, with top-decile effort. n = 680, precision **18.82 %**, matched base 9.87 %,
**Δ +8.95 pp**, synergy +7.02, vs BL3 **+8.37 pp / 1.801×**.

Everything held: 383 distinct dates, 2022 +9.28 (n=312) and 2023 +9.13 (n=333), cooldown
5/10/15 → +9.96/+8.95/+7.65, ticker-clustered bootstrap +8.41 pp with a 95 % CI of
[+5.38, +11.54] that still excludes zero under Holm k=15, and 0 of 1000 size-matched placebo
draws reached it.

**It is a liquidity artefact.**

| | candidate | universe | BL3 |
|---|---:|---:|---:|
| median daily dollar volume | **$0.014 M** | $4.69 M | $5.46 M |
| events below $1 M/day | **90.0 %** | — | — |
| `vol_ratio_20d` | **0.38×** | 0.85× | — |
| `range_pct` | **0.49 %** | 3.68 % | — |

"Top-decile effort" here does not mean heavy volume — volume is 38 % of its own 20-day average.
It means the range is near zero, so `volume / range` explodes. This is a stock that does not
trade: it prints an opening gap, then sits. The 8-ATR move that follows is an illiquid book
filling in. Above a $1 M/day floor the pair retains **79 events** and is no longer measurable.

`bar_gap_range=G3-C` alone shows the same: median $66 k, 68 % sub-$1M, and on tradeable names
**Δ +1.69 → −0.10**.

### On tradeable names (≥ $1 M/day, 66 % of DISCOVERY) nothing reaches the gate

| pair | n | Δpp | **vs BL3** |
|---|---:|---:|---:|
| `hi20_age>p90 & phys_e_raw>p90` | 1338 | +4.56 | **+2.68** |
| `phys_e_raw>p90 & sig_cisd_cplus_minus` | 669 | +4.61 | +1.04 |
| `hi20_age>p90 & sig_cisd_cplus_minus` | 1002 | +4.34 | +0.20 |
| `hi20_age>p90 & w2_spring` | 285 | +2.29 | −1.60 |
| `phys_e_raw>p90 & bar_gap_range=G3-C` | **79** | — | not measurable |

Required **+5.00 pp**. Best observed **+2.68 pp**.

---

## 4. The negative result, stated precisely

> In the ~500-primitive SAFE stack of this application, there is no standalone or two-way causal
> state that predicts a **major (≥ 8 ATR in 20 bars) breakout in liquid names** with practically
> useful precision. The best clean two-way conjunction reaches 13.0 % precision against a 10.3 %
> RSI>50 comparator on the same rows — a +2.7 pp edge, against a +5.0 pp requirement.

This is a boundary, not a failure. It says where this kind of feature mining has stopped paying.

**A caution on reading the by-product tables.** The rerun shows `dist_ema200_atr>p90` at −20.88 pp
and `price_gt_200` at −9.46 pp. These are **not** anti-predictive findings. EMA200 requires 600
bars and the DB starts 2021-05-26, so those primitives retain ~50 bars per ticker at the very end
of 2023, about 1.1 events per ticker. The EMA200 family is simply **not testable in this data
window**.

---

## 5. Carried forward as a HYPOTHESIS — not an implementation

The quiet / compressed / aged-high context is the one line that **strengthened** under the
liquidity filter rather than dying:

| primitive | Δ all | Δ ≥ $1M/day |
|---|---:|---:|
| `hi20_age>p90` — 20-day high is unusually stale | +2.52 | **+4.02** |
| `phys_e_raw>p90` — top-decile effort | +1.93 | **+3.45** |
| `atr_pct<p10` — compressed volatility | +1.72 | +2.45 |
| `w2_evr` | +1.64 | +2.10 |

That is suggestive of real structure and is **not** a discovery: it was measured on discovery
data only, never validated, and never reached the gate. It must not be built, displayed as an
edge, or used to rank anything.

Adding a liquidity floor to *this* programme would be changing a rule after seeing the result.
If it is pursued, it is a new study — **V4 — Liquid Major Breakouts** — with a universe frozen
before the target is looked at (`median dollar volume ≥ $1M/day`, or more strictly $5M/day), and
with the 8-ATR threshold itself re-examined for realism in liquid large/mid caps.

---

## 6. Artifacts

`research_out/v3a_phase1.csv` (564 rows, 20-column audit schema plus `min_history`, `raw_rate`,
`events_per_ticker`, duplicate annotations) · `v3a_phase2.csv` · `v3a_phase3.csv` ·
`v3a_phase3_detail.csv` · `v3a_phase3_audit_secondary.csv` · `v3a_duplicate_groups.csv` ·
`v3a_family_survivors.csv` · `v3a_baselines_discovery.csv` · `v3a_prefix_vs_postfix.csv` ·
`research_out/pre_warmup_fix/` (the 10 contaminated pre-fix tables, preserved as audit artifacts)

`backend/v3a_warmup_contract.py` · `backend/v3a_discovery.py` · `backend/v3a_frame_build.py`
