# TZ Signal Logic — Pine ↔ Python Verification Report

**Scope:** VERIFY ONLY. No production code, Pine, or scoring engine was modified.
**Method:** A faithful Python replica of `260523_TZ_F_WLNBB_CMB.pine` (raw T/Z +
priority engine) was built and compared, bar-by-bar, against the repo's two Python
implementations: `analyzers/tz_wlnbb/signal_logic.py` (per-bar reference) and
`signal_engine.py` (vectorized, feeds the DB).
**Sample:** 10 tickers × 300 daily bars = ~3,000 bars (RGTI, RKLB + NVDA, SOFI,
HOFT, OKLO, AAPL, TSLA, PLTR, AMD). Body-mode engulf (`useWick=false`,
`minBodyRatio=1.0`), exactly as the Pine inputs default.
**Note:** the named v3 parquets / `tz_verify.py` are NOT in this repo; this is a
code-faithfulness audit (Pine vs the live Python), not the full SP/NQ dataset
census. Returns use the DB `fwd_10d` column.

---

## 1. Verdicts per hypothesis

| # | Hypothesis | Verdict | Evidence |
|---|---|---|---|
| 1 | **T12 dead in Pine** (raw>0, final=0) | ✅ **CONFIRMED** | T12 raw_n=33, Pine final_n=**0** (100% lost to priority) |
| 1b | **Z12 dead in Pine** | ⚠️ **MOSTLY** (not strict) | Z12 raw_n=113, final_n=**1** — 112 lost to Z1G, but **1 survives** on the `open==open[1]` boundary → Z12_raw ⊄ Z1G_raw |
| 2 | **Pine ↔ Python divergence** | ✅ **CONFIRMED & ISOLATED** | The ONLY Pine-vs-Python mismatch is **T11/T12**: 33 bars are `T11` in Pine but `T12` in the dataset. All 22 other signals match byte-for-byte |
| 3 | **T11 swallows T12 in Pine** | ✅ **CONFIRMED** | Pine `T11_raw` lacks the `not T12_raw` guard → whenever `close<open[1]` (T12), T11 (prio 10) also fires and outranks T12 (prio 12) |
| 4 | **T1/Z1 boundary asymmetry** | ✅ **CONFIRMED (code), RARE (effect)** | T1 uses `open[1]>=open` (inclusive), Z1 uses `open>open[1]` (strict). `open==open[1]` prev-bull bear bars fall through to Z12. Only **6/2990 bars (0.20%)** sit on this edge |

---

## 2. The divergence in plain language

The Pine chart and the Python dataset **disagree on exactly one thing: T11 vs T12.**

- **Pine** `T11_raw = prev1IsBull and open<open[1] and (close<close[1] or close<open[1]) and isBull`
  — the `or close<open[1]` makes T11 fire on **every T12 bar too**. Since T11 (priority 10)
  outranks T12 (priority 12), **T12 can never win → the chart never shows T12.**
- **Python** (`signal_logic.py`) adds `and not T12_raw`; (`signal_engine.py`) uses
  `close >= open[1]` — both **carve T12 out of T11**. So the dataset reports T12 as a
  live signal, and its T11 is the narrower "closed back inside the body" case.

> Net: 33 bars that the **chart labels T11**, the **dataset labels T12**. Everything else
> is identical. This is why "my chart never shows T12 but my dataset reports T12."

**Which one is right?** The documented intent of T11 = *"opened below prev open, closed
back inside body"* = `close>=open[1] and close<close[1]` = **the Python version**. So the
**Python matches the intent; the Pine code has the swallowing bug.** The dataset is the
faithful-to-intent one; the chart is the outlier.

---

## 3. Per-signal raw / final / lost (10 tickers, ~3000 bars)

| sig | raw_n | Pine final | Python final | Pine lost-to-prio |
|---|---|---|---|---|
| T4 | 122 | 122 | 122 | 0 |
| T6 | 60 | 60 | 60 | 0 |
| T1G | 128 | 128 | 128 | 0 |
| T2G | 372 | 372 | 372 | 0 |
| T1 | 164 | 154 | 154 | 10 |
| T2 | 223 | 221 | 221 | 2 |
| T9 | 118 | 117 | 117 | 1 |
| T10 | 84 | 84 | 84 | 0 |
| T3 | 124 | 124 | 124 | 0 |
| **T11** | 54 | **54** | **21** | 0 |
| T5 | 110 | 110 | 110 | 0 |
| **T12** | 33 | **0** ☠️ | **33** | **33** |
| Z4 | 128 | 128 | 128 | 0 |
| Z6 | 67 | 67 | 67 | 0 |
| Z1G | 112 | 112 | 112 | 0 |
| Z2G | 286 | 286 | 286 | 0 |
| Z1 | 110 | 109 | 109 | 1 |
| Z2 | 196 | 195 | 195 | 1 |
| Z9 | 109 | 109 | 109 | 0 |
| Z10 | 79 | 79 | 79 | 0 |
| Z3 | 143 | 143 | 143 | 0 |
| Z11 | 60 | 60 | 60 | 0 |
| Z5 | 144 | 144 | 144 | 0 |
| **Z12** | 113 | **1** | **1** | **112** |
| Z7 | 11 | 11 | 11 | 0 |

- **T12**: dead in Pine (0), alive in Python (33). T11 absorbs the 33 (Pine 54 = Python 21 + 33).
- **Z12**: nearly dead in BOTH (final=1) — no Pine↔Python divergence here; the 1 survivor is the
  `open==open[1]` boundary bar.
- `signal_logic.py` vs `signal_engine.py`: **0 mismatches** (the two Python engines agree
  perfectly after the Z1 fix; both diverge from Pine only on T11/T12).

**Pine vs `signal_logic` total mismatches: 33 — all `Pine T11 → Python T12`.** Nothing else.

---

## 4. Boundary census (tick = exact equality, raw OHLC)

| edge | bars | % |
|---|---|---|
| open == open[1] | 6 | 0.20% |
| open == close[1] | 43 | 1.44% |
| close == close[1] | 13 | 0.43% |
| close == open[1] | 7 | 0.23% |

The T1(inclusive)/Z1(strict) asymmetry only bites on `open==open[1]` = **0.20%** of bars.
Real but negligible — the strict/inclusive question is a rounding-edge curiosity, not a
material relabelling driver. The T11/T12 issue (1.1% of bars, 33/3000) dominates.

---

## 5. Edge-vs-core: does splitting T12 out carry signal? (fwd 10d, prev-bull bull, open<open[1])

| label | n | median fwd_10d | win% |
|---|---|---|---|
| T11-core (`close≥open[1]`) | 20 | **+0.84%** | 65% |
| T12 (`close<open[1]`) | 30 | **+1.76%** | 60% |

T12 bars carry a **2× higher median forward return** than the T11-core. So the Python's
revival of T12 is **informative, not noise** — the "deeper close below prev open" subset
behaves differently. (n=20/30 — directional, below the n=30 bar on T11-core, so suggestive
not conclusive; the full SP/NQ dataset would settle it.)

---

## 6. Recommendation (no code changed — proposals only)

1. **The dataset (Python) is the intent-faithful one; the Pine chart has the T11 bug.**
   To reconcile, **fix the Pine** so the chart matches the dataset & intent:
   `T11_raw := prev1IsBull and open<open[1] and close>=open[1] and close<close[1] and isBull`
   (or append `and not T12_raw`). This revives T12 on the chart and narrows T11 to its
   documented meaning. **Do not change the Python** — it is already correct.
2. **Z12** is effectively a single-bar curiosity (the `open==open[1]` fall-through). Either
   accept it or, if desired, mirror T1's inclusive boundary on Z1 (`open>=open[1]`) — but
   this only moves ~0.2% of bars and the forward-edge case (§5-style) should decide.
3. **Z1 needs no action** — Pine and Python now agree (the earlier `signal_engine` Z1 bug,
   `open<=open[1]` instead of `open>open[1]`, was already fixed; it now matches Pine's strict `>`).
4. Treat the **dataset as canonical for analytics**; the chart is a visualization that
   currently lags the intent on T11/T12 only.

---

*Generated by an automated VERIFY-ONLY audit. Pine replica validated against the live
`signal_logic.py` priority engine; 33/3000 divergent bars, all T11↔T12, root-caused to the
missing `not T12_raw` guard in the Pine `T11_raw` definition.*
