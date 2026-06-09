# Engulf Signals — Full Bar-by-Bar Analysis (1 → 4 bars)

How the edge of the engulf signals (T4/T6 bull, Z4/Z6 bear) evolves as we add bars,
measured on the full 8.3M-bar DB. Metric: **median fwd_10d** and **win%** (median
over mean, n≥30). IS = before 2025-01-01, OOS = 2025-01-01 onward.

---

## STEP 1 — 1-bar (the raw engulf signal)

Raw T4/T6/Z4/Z6, pooled, is **flat-to-negative** (median ≈ −0.4%). The decisive
split is **the market**, not the pattern:

| signal | sp500 | nasdaq | russell2k |
|---|---|---|---|
| T4/T6 (bull) | **+0.25%** ✅ | −0.7% ❌ | −0.4% |
| Z4/Z6 (bear) | **+0.4..0.5%** ❌ (price UP) | −0.4% ✅ | ~0 |

**Finding:** in sp500 *everything drifts up* (bull engulf works, bear engulf
reverses up); in nasdaq *everything drifts down*. The bar's bull/bear look is NOT
the signal — `universe` is. → **sp500 = LONG home, nasdaq = SHORT home.**

---

## STEP 2 — 2-bar (engulf + volume class + L-code)

In sp500, add `vol_bucket`: monotonic **B (controlled) best, VB (climactic) worst**.

| sp500 | vol=B | vol=N | vol=VB (trap) |
|---|---|---|---|
| T4 | +0.46% | +0.29% | +0.11% |
| Z4/Z6 + L (buy-weakness) | up to **+2.1%** | | |

Best 2-bar combos (sp500, vol=B + L-code):
- **Z6·L5·B +2.13% / 65% win** (n=155) · **Z4·L46·B +0.75% / 56%** (n=3538, robust)

`base sp500 engulf+B = +0.627% / 55.0% (n=11,779)`. The bearish-engulf variants are
**contrarian** — a scary big-volume down-bar in sp500 is a shakeout that bounces.

---

## STEP 3 — 3-bar (add the following bar)

After engulf+B, what does bar+1 do? **Continued weakness strengthens** (buy the 2nd
down bar), a bullish follow-through weakens:

| bar+1 | n | median (from bar+1) | win |
|---|---|---|---|
| **bear** (deeper shakeout) | 5932 | **+0.869%** | 56.3% |
| bull (follow-through) | 5795 | +0.330% | 53.0% |
| + next Z9 (bear inside) | 998 | **+1.792%** | 60.3% |

(Backward branch: an engulf PRECEDED by a reversal-TZ — T5/T2G/Z3/Z5/Z2G on bar−1
— also lifts: +0.63% → +0.81% / 56.5%, n=4851. Not used in the journal.)

---

## STEP 4 — 4-bar (add the turn)

At bar+2 the optimal action **flips**: now you want the **bullish TURN**, not a 3rd
down bar.

| bar+2 | n | median (from bar+2) | win |
|---|---|---|---|
| bullish T (T9/T3/T4) + vol | 1600 | **+1.168%** | 57.8% |
| └ bull-engulf trigger only (T4/T6 @ bar−2) | **800** | **+1.898%** | **63.1%** |
| trap: bar+2 bear + vol=W (knife keeps falling) | 120 | −0.406% | 45.0% |

**The 4-bar is a momentum *pullback-resume*, not buy-weakness:** a strong bar →
healthy pullback → resume.

---

## THE LADDER + OVERFIT CHECK (sp500 LONG, IS vs OOS)

| level | n | IS median | IS win | OOS median | OOS win | holds? |
|---|---|---|---|---|---|---|
| **1** engulf only | 92,118 | +0.309% | 52.3% | +0.472% | 53.7% | ✅ |
| **2** + vol=B | 11,779 | +0.611% | 54.9% | +0.697% | 55.1% | ✅ |
| **3** + bar+1 bearish | 5,932 | +0.656% | 55.0% | **+1.381%** | 59.0% | ✅ |
| **4** + bar+2 turn+vol | 1,600 | +0.720% | 54.6% | **+2.010%** | **63.4%** | ✅ |

### Verdict: 4-bar is NOT overfit — but read it carefully
1. **OOS ≥ IS at every level** — the edge HOLDS and even GROWS out-of-sample. The
   opposite of overfit (overfit = OOS collapses). The bar-stacking is real.
2. **The edge rises monotonically 1→4 in BOTH IS and OOS.** Each added bar adds
   genuine median.
3. **BUT the giant OOS jump (3-bar +1.38%, 4-bar +2.01%) is regime-amplified** —
   2025-26 sp500 was a strong dip-buy regime. Don't bank on +2% everywhere; the
   **steadier IS number (+0.7% / 55%) is the conservative expectation**, with the
   OOS +2% as favorable-regime upside.
4. **n thins** 11.8k → 5.9k → 1.6k → 800. Level 4 (576 OOS) is still robust, not
   dangerously thin.

### 3-bar vs 4-bar
- **4-bar** = higher quality / lower frequency. Cleaner entry (you buy the confirmed
  turn, not a falling knife): IS +0.72% / OOS +2.01% / win OOS 63%. n=1600.
- **3-bar** = ~3.7× more signals (n=5932), still strong OOS (+1.38% / 59%), but you
  enter while price is still falling (psychologically hard).
- **Neither is overfit.** 4-bar is the better-conviction setup; 3-bar is the
  higher-throughput fallback. Both belong.

---

## CONCRETE bar-by-bar (modal codes from real instances)

### Setup A — 2-bar buy-weakness (sp500) · 56.3% win · +0.78% med (n=6,076)
```
bar −1 :  (the engulfed bar) usually BULL — modal TZ: T2G / Z2G / T9 / T5
bar  0 :  Z4 (66%) or Z6 (34)  +  L46 / L5 / L25  +  vol=B   ← ENTER
```

### Setup B — 4-bar pullback-resume (sp500) · 63.1% win · +1.90% med (n=800)
```
bar −2 :  T4 (74%) or T6 (26)  bull engulf  +  L3 / L34 / L12  +  vol=B
bar −1 :  bearish pullback — modal TZ: Z9 / Z3 / Z1 / Z5
bar  0 :  T9 / T3 / T4  bull turn  +  L12 / L34 / L3  +  vol≠W   ← ENTER
```

Both are sp500-LONG. nasdaq mirrors are SHORT (weaker: 4-bar short median −0.35%);
not paper-traded (journal is long-only).
