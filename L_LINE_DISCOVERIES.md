# L-line (l_sig) research — L34/L46 → capitulation-bounce edge

_The full thread: from "L34/L46 are always there" → to a validated tradeable edge. 5-yr,
8.3M bars, universe-drift removed (median forward EXCESS), per-year + IS/OOS + clip25 +
per-ticker concentration. ANALYSIS ONLY — no production logic changed by the research._

Scripts: `analysis/l46_l34_prebreakout.py`, `l34_l46_cooccur.py`, `l34_cluster_validate.py`,
`l46_rsi_cci.py`, `l_rsi_extremes.py`, `capitulation_plus_signals.py`, `cap_bluefri_deep.py`

> `l_sig` = which raw VSA volume-lines fire (L34=L3&L4 demand bar, L46=L4&L6 supply bar…).
> NOT `bar_line5` (PSAR/RSI2/VIX) — see [[reference_l_sig_vs_line5]].

---

## 0. The observation & the honest first answer
**"L46/L34 are always present before breakouts."** — TRUE, but it's **base rate**, not signal:
- L46 fires on **23.7%** of ALL bars, L34 on **9.7%**. They're everywhere.
- Pre-breakout (20-bar window): L46 **19.2%** (−4.5pp, LESS), L34 **10.0%** (+0.3pp, = base).
- The L46→L34 "sequence" (79% of breakouts) is a base-rate artifact of two common signals.
- L34 in the last 5 bars before a breakout → **no** bigger move (−0.20 vs −0.15).

**A signal on 24% of bars can't discriminate breakouts.** Ubiquity ≠ predictive power.

## 1. The PATTERN is in what L34 ATTRACTS, not L34 alone
Co-occurrence scan: L34 alone median-excess **−0.08 (2/6 yr — noise)**. But L34 genuinely attracts
an accumulation cluster (freq-lift vs base): **LOAD +9.8pp · FRI34/BLUE +7.4pp · BEST +6.5pp ·
squeeze +4.1pp**, and that COMBO turns it positive & robust:

| composite | medL | per-year |
|---|---|---|
| L34 alone | −0.08 | 2/6 ❌ |
| L34 + LOAD | +0.18 | 5/6 ✅ |
| **L34 + LOAD + squeeze** | **+0.38** | 5/6 ✅ |
| L46 + squeeze | +0.38 | 5/6 ✅ |

→ matches SMX. But magnitude is small (+0.2…+0.4 median).

## 2. RSI/CCI conditioning — half the user's thesis held
"Low RSI/CCI = bullish, high = bearish":
- ✅ **Low = bullish** confirmed. But the sweet spot is **RSI 20-35**, not deep <20.
- 🔪 **RSI<20 ALONE = falling knife** (L34 −0.41, clip25 −1.20) — deep oversold without confirmation bleeds.
- ❗ **CCI is U-shaped** — BOTH extremes bullish (≤−100 AND ≥100); high CCI = momentum continuation, NOT bearish.
- ❌ **High = short** NOT supported at 65; only raw L46 at **RSI>80** turns mildly negative (−0.33), and even
  that flips positive with momentum (CCI>100 / vol-climax). Blind overbought-fade loses.

## 3. The key: RSI and CCI must ALIGN (capitulation)
| context | medL | per-year |
|---|---|---|
| RSI<20 **alone** | −0.41 🔪 | knife |
| **RSI<20 + CCI<−100** (capitulation) | **+0.60** | 6/6 ✅ |
| RSI 20-35 (sweet spot) | +0.45 | 6/6 ✅ |

Deep oversold works ONLY when RSI **and** CCI are both extreme = real capitulation, not a lone knife.

## 4. 🥇 The top edge: capitulation + volume-coil
On the capitulation anchor (+0.60), what stacks (robust, big-n, 6/6):

| + signal | n | medL | per-year |
|---|---|---|---|
| **BLUE** (vol z-spike + RSI coil) | 3474 | **+1.27** | 6/6 |
| **FRI64** (vol-coil engulf↓) | 3109 | **+1.32** | 6/6 |
| d_absorb_bear | 950 | +1.16 | 6/6 |

The additions are **bearish down-bars with a volume coil/absorption** = the classic **Wyckoff selling
climax** (final flush, supply absorbed) → bounce.

## 5. DEEP validation — `L34/L46 + RSI<20 + CCI<−100 + BLUE/FRI64`
| metric | value | read |
|---|---|---|
| n / win% | 3474 / 56.7% | |
| median excess | **+1.27** | |
| **clip25-mean** | **+1.40** | clip25 **> median** → NOT tail-driven; whole distribution shifts up |
| fwd_20d excess | +1.00 | bounce holds at 20d |
| per-year | **6/6** (incl 2022 bear) | 21:+0.7 22:+0.5 23:+2.5 24:+1.6 25:+2.5 26:+0.6 |
| IS / OOS | +1.2 / **+1.5** | OOS ≥ IS — not overfit |
| path | MFE +9.0 / MAE −6.8 | favorable (sit through ~−7% heat) |
| **concentration** | **1286 tickers · top1 0.3% · top5 1.7%** | broad — NOT a few pump-names (kills survivorship doubt) |
| universe | sp500 +1.06 / nasdaq +1.37 / russell2k +1.27 | works in all 3 |

⚠️ Capitulation BOUNCE play (10–20d mean-reversion). Positive-skew: ~10% >+25%, ~7% <−25%
(falling-knife minority, worst −78%) → **diversify** (the 1286-ticker breadth provides it); size for the MAE.

## 📌 Conclusion
The observation pointed the right way; the magic was never one signal — it was the **STACK**:
**L-bar (L34/L46) + aligned RSI/CCI capitulation (RSI<20, CCI<−100) + volume-coil (BLUE/FRI64).**
This is the strongest, best-diversified, most regime-robust edge in the L-line thread (+1.27 median,
clip25 +1.40, 6/6 yr, 1286 tickers, IS≤OOS, all universes) — a statistically validated Wyckoff
selling-climax spring. Productized as the **💥 Capit** Ultra filter (`_enrich_capitulation`).

Same physics as the session's core finding (oversold + absorption, [[project_what_actually_works]]):
**buy absorbed weakness** — here via the L-line + capitulation + coil axis.
