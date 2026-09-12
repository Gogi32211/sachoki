# SIGNAL REFERENCE

**The single authoritative description of every signal the app computes: what it is, how it is
calculated, whether it is safe to use live, and what the research says about it.**

Written 2026-09-11. Replaces nothing — before this, the signal logic lived only in code and in
thirty-odd dated session notes (`T1_*_ANALIZI.md`, `L_LINE_DISCOVERIES.md`, `SESSION_NOTES_*`),
which record *studies*, not *definitions*. This file records definitions.

## How to read it

Every signal carries three tags. They answer different questions and must not be confused.

| tag | meaning |
|---|---|
| **LIVE-SAFE** | the value at bar *t* uses only bars ≤ *t*. Verified by `backend/tests/test_feature_lookahead.py`, which recomputes each engine on a frame truncated at *t* and demands an identical row. |
| **LOOKAHEAD** | the value at *t* depends on bars after *t*. Usable for describing history, never for a decision. |
| **FITTED** | no future bar is in the row, but the formula's constants were fitted on outcomes over the same history. Not lookahead; still contaminated for any backtest of that history. |

And a research status: **BUILT** (validated, in use) · **CONTEXT** (describes, does not rank) ·
**VETO** (predicts badly enough to avoid) · **NULL** (measured, nothing there) · **UNMEASURED**.

> **The rule this file exists to protect.** A name tells you nothing. On 2026-09-11 two fields that
> pass every name-based filter turned out to leak: `acc_exit_class` stores the distance to a
> *future* breakout bar, and `aes_score` weights each signal by a lift measured on that ticker's
> *own future*. Both were found by reading the code, not the column list.

---

# 1 · LAYER MAP

The Superchart draws ~25 rows. Each comes from one producer.

| row | producer | stored as |
|---|---|---|
| `Z` / `T/D` | `signal_engine.compute_signals` | `t_sig`, `z_sig`, `sig_t*`, `sig_z*` |
| `L` | `wlnbb_engine.compute_wlnbb` | `l_sig`, `sig_l1..l6`, `l34`, `l43`, `l22` |
| `▭` | bar taxonomy (suffix decomposition) | `bar_body_wick`, `*_suffix`, `bar_gap_class` |
| `⟂` | `cisd_engine.compute_cisd` | `sig_cisd_*` |
| `⚛` | `studio/bar_physics.py` | `phys_r`, `phys_regime`, `phys_c`, `phys_h`, `phys_m`, `phys_e`, `phys_k`, `phys_s` |
| `EDGE` | `edge_replay.py` masks | not stored — computed on demand |
| `G` / `I` / `ULT` / `VOL` / `VABS` / `WICK` | `signal_engine.compute_g_signals`, `compute_b_signals`, `ultra_engine`, `vabs_engine`, `wick_engine` | `sig_g*`, `sig_b*`, `sig_va`, `sig_svs` … |
| `WYCK` | `wyckoff_v2_engine` | `w2_*`, `wt_*`, `wyc_*` |
| `UDN★` | `lbal_build.py` | `data/lbal_signals.parquet` |
| `SHAPE` | `shape_ctx_build.py` | `data/shapectx_signals.parquet` |
| `L-VX` | `lbal_build.py` | `data/lvx_signals.parquet` |
| `OVD` | `lbal_build.py` | `data/ovdmap_signals.parquet` |
| `VOL7` | `lbal_build.py` | `data/vol7_signals.parquet` |
| `▽△` | `anatomy_build.py` / `main.api_day1h` | `data/anatomy_signals.parquet` |
| `SCORE` / `ULTRA` / `UV3` / `🏅` / `β` / `V3` | scoring engines | **FITTED — see §7** |

`main.compute_all_signals(df, ticker, tf)` is the single entry that runs the twelve per-bar engines
together. It is the port authority: if two places disagree about a signal, that function is right.

---

# 2 · BAR GEOMETRY AND THE T/Z CODES

**Producer** `backend/signal_engine.py::compute_signals` · **LIVE-SAFE** (verified: 133 engine
columns × 20 dates × 6 tickers, zero differences).

Everything in this system starts here. Each bar gets **exactly one** code out of 26, from the
relationship between this bar's body and the previous bar's body. No indicator, no volume, no
lookback beyond one bar.

## 2.1 Primitives

```
isBull  = close > open                 isBear  = close < open
isDoji  = close == open
p1Bull  = prev close > prev open       p1Bear  = prev close < prev open OR prev bar was a doji

pTop / pBot = max / min of the PREVIOUS bar's open and close   ← the body, not the wick
cTop / cBot = max / min of THIS bar's open and close

engOk  = |body| / |prev body| >= 1  AND  cTop >= pTop  AND  pBot >= cBot     (engulfing)
insOk  = cTop <= pTop  AND  cBot >= pBot                                     (inside / nested)
```

Note `engOk` and `insOk` compare **bodies**, not ranges. A bar that engulfs the previous *range*
but not its *body* is not T4. This is the single most misread rule in the system.

## 2.2 The twelve bullish codes

| code | definition |
|---|---|
| **T1G** | prev bear · open > prev close · open > prev open · close > prev open · bull — **gap-down reclaim** |
| **T1** | prev bear · open ≥ prev close · prev open ≥ open · close > prev open · bull — **reclaim without a gap** |
| **T2G** | prev bull · open ≥ prev open · open > prev close · close > prev close — **gap-up continuation** |
| **T2** | prev bull · open ≥ prev open · open ≤ prev close · close > prev close — **continuation, no gap** |
| **T3** | prev bear · bull · opens below both · closes between prev close and prev open — **partial recovery** |
| **T4** | prev bear · bull · `engOk` — **bullish engulfing** |
| **T5** | prev bear · bull · opens below both · closes below prev open · prev close ≥ close — **failed recovery** |
| **T6** | prev bull · bull · `engOk` — **continuation engulfing** |
| **T9** | prev bear · bull · `insOk` — **inside bar after a down bar** |
| **T10** | prev bull · bull · `insOk` — **inside bar after an up bar** |
| **T11** | prev bull · bull · opens below prev open · closes ≥ prev open but < prev close |
| **T12** | prev bull · bull · opens below prev open · closes below prev open |

## 2.3 The bearish codes

Z1G · Z1 · Z2G · Z2 · Z3 · Z4 · Z5 · Z6 · Z9 · Z10 · Z11 · Z12 mirror the T codes with the
directions reversed. **Z7** is special: a doji that fires **only when no bullish and no other
bearish code fired**.

```
cZ11 = prev bear AND open > prev open AND bear AND (close > prev close OR close > prev open)
```
Z11 is the odd one — a red bar that still closed above something. It is the code the book flags
as an abort condition (`project_z11l12_sequences`).

## 2.4 Priority — why only one code per bar

Several conditions fire at once; the engine resolves them by a **fixed priority**, then a bullish
code **erases** any bearish code on the same bar.

```
bull:  T4 > T6 > T1G > T2G > T1 > T2 > T9 > T10 > T3 > T11 > T5 > T12
bear:  Z4 > Z6 > Z1G > Z2G > Z1 > Z2 > Z9 > Z10 > Z3 > Z11 > Z5 > Z12 > Z7
```

**Consequence that matters for research:** the stored `t_sig` / `z_sig` are *priority-resolved*.
A bar that is raw-Z1G *and* raw-Z4 stores only Z4. Any study that needs the raw pattern must
recompute it — `studio/bar_physics.py` does exactly this for its AD-FRESH field and says so.

**Research status.** The T-family is the most studied object in the book. **BUILT**: T1
capitulation-bounce (`project_capitulation_bounce`, the most robust setup found, 6/6 years),
T6 at a Wyckoff SC floor with RSI<40, T9 body-inside reversal, T3/T5 inside oversold sequences.
**VETO**: a Z12 prefix is universal poison (`project_t1g_nb_suffix`).

---

# 3 · THE L-LINES — VOLUME AGAINST RESULT

**Producer** `backend/wlnbb_engine.py::compute_wlnbb` · **LIVE-SAFE**.

This is the Volume-Spread-Analysis layer and the second pillar of the system. It asks one question
per bar: **did effort produce result?**

## 3.1 Volume class W / L / N / B / VB

A Bollinger band on volume itself, 20 periods, 1 **population** standard deviation (Pine's
`ta.stdev` is population — using the sample version silently shifts every bucket):

```
mid = SMA(volume, 20)      sd = population stdev(volume, 20)
upper = mid + sd           lower = max(mid − sd, 0)

W  volume < lower                      wide / dried up
L  lower <= volume < mid               low
N  mid   <= volume < upper             normal
B  upper <= volume < upper + mid       big
VB volume >= upper + mid               very big
```

## 3.2 "Adapted" volume direction

Raw `volume > volume[1]` is too noisy. The engine uses the **bucket level** first:

```
vol_up   = bucket rose   OR (bucket unchanged AND raw volume rose)
vol_down = bucket fell   OR (bucket unchanged AND raw volume fell)
```

A naive `v > v[1]` here "broke every L-signal" — the comment in the source records it.

## 3.3 The six primitives

```
L1 = vol_down AND close > prev close        volume drying up, price still rising
L2 = vol_down AND close >= prev low         volume drying up, no new low
L3 = vol_up   AND close > prev close        effort up, price up
L4 = vol_up   AND close <= prev high        effort up, NO new high      ← the key one
L5 = vol_down AND close < prev close        volume drying up, price falling
L6 = vol_up   AND close < prev close        effort up, price down
```

`L4` is the definition of *absorption*: volume rose and the bar still could not make a new high.
Someone is selling into it.

## 3.4 The composites — what the chart actually prints

```
L34 = L3 AND L4 AND close >= open     effort UP, close UP, NO new high, green bar
L22 = L3 AND L4 AND close <  open     the same, on a red bar
L43 = L6 AND L4 AND close >  open     effort UP, close DOWN vs prev, no new high, green bar
L64 = L6 AND L4 AND close <  open     the same, on a red bar
L555 = three consecutive L5           volume dying while price falls — exhaustion
ONLY_L2L4 = L2 AND L4 AND none of L1, L3, L5, L6
```

**L34 is the system's signature bar.** Effort went in, the bar closed up, and it still made no new
high. On a green bar that is buying absorbed; on a red bar (`L22`) it is the same effort failing.
The book's validated edges lean on it heavily: L34→L34 continuity (`project_l34_continuity`,
+2.60/5-6yr **BUILT**), red-L34 on a reversal bar (`project_l34_red_triple`, +2.05), the
L43-TRIPLE (`project_l43_triple`, **BUILT**).

## 3.5 BLUE, FRI and the pre-pump scan

```
vol_z      = (volume − mid) / sd
rsi_range3 = max(RSI14, 3 bars) − min(RSI14, 3 bars)
BLUE  = vol_z >= 1.1 AND rsi_range3 <= 5.0      big volume, RSI going nowhere
FRI34 = BLUE AND L34     FRI43 = BLUE AND L43     FRI64 = BLUE AND L64
UI    = at least 2 BLUE bars in the last 10
```

BLUE is effort-without-result stated a second way, through the oscillator instead of the price.

Classical VSA primitives, used for the pre-pump counter:

```
avg_rng = SMA(high−low, 20)     avg_vol = SMA(volume, 20)     mid_px = (high+low)/2

squat     = range < 0.7 × avg_rng AND volume > 1.5 × avg_vol      high effort, no spread
nosupply  = range < 0.7 × avg_rng AND volume < 0.7 × avg_vol AND close >  mid_px
nod       = range < 0.7 × avg_rng AND volume < 0.7 × avg_vol AND close <= mid_px
climax    = range > 1.5 × avg_rng AND volume > 2.0 × avg_vol

PRE_PUMP = cooldown(  count(squat|nosupply|nod|climax over 20 bars) >= 2,  6 bars )
```

## 3.6 Breakouts of an absorption level

```
l34_hi / l34_lo = the high / low of the MOST RECENT L34 bar, forward-filled
BO_UP = close > l34_hi, was not above it yesterday, and this bar is not itself an L34
BO_DN = close < l34_lo, mirror
BX_UP / BX_DN = the same against the most recent L43 bar
```

So `BO_UP` means *"price has left the level where supply was absorbed"* — not a generic breakout.

---

# 4 · PATTERN ENGINES

All **LIVE-SAFE** (covered by the truncation test).

### F1–F11 — `f_engine.py`
Buy patterns built on the **priority codes** (`bc` / `zc`), not on the code names. Encodes
sequences such as "a strong bear code followed within N bars by a strong bull code". Output
`sig_f1..sig_f11`, `sig_any_f`.

### FLY ABCD — `fly_engine.py`
An A→B→C→D role sequence with an EMA-cross context:
```
A = zc in {3,4}                 Z1G, Z2G          strong bearish
B = zc in {9,1,2,5,10,8,12,7}                     ordinary bearish
C = bc in {9,10,12,7,5}          T3,T11,T12,T9,T1  moderate bull
D = bc in {1,2,4,6}              T4,T6,T2G,T2      strong bull — fires on the CURRENT bar
```
`fly_abcd` needs the full ordered A→B→C in the last 30 bars plus an EMA sequence at A;
`fly_cd` / `fly_bd` / `fly_ad` need only that one prior role within 20 bars.

### VABS — `vabs_engine.py`
Volume absorption and breakout, with explicit gates: 20-bar MA, ≥2 bucket jump, 10-bar break
window, spike ≥ 1.30 × MA, z-score ≥ 0.70, delta ≥ 0.40, spread ≤ 1.50 ATR, move ≤ 12 %,
close-location value ≥ 0.30. Base ratios differ by volume tier (3.0 low / 1.6 mid / 1.20 high).

### WICK — `wick_engine.py`
Two-candle opposite-wick reversal: each bar's dominant wick ≥ 40 % of its range, bodies ≤ 55 %,
wick ≥ 0.10 ATR, confirmed within 3 bars by a breakout.

### ULTRA — `ultra_engine.py`
`260308+L88`: volume ≥ 2× the previous bar with a delta multiple of 1.5 on a bullish candle.
Plus `compute_ultra_v2` (`260315`).

### CISD — `cisd_engine.py`
Change-in-State-of-Delivery. Tracks ±CISD structure shifts and names four sequences:
`++--` (SEQ) · `++-` (PPM) · `-+-` (MPM) · `+--` (PMM).

### PARA — `para_engine.py`
A stateful parabola detector (`260420 v3.6`): base compression → seed → `PARA` → `PARA+` →
`RETEST`, with a campaign lock / rearm / reset cycle. **Do not read `para_start` as a buy**:
`project_p_parabola_ride` found the edge is in the tail with a 25 % trailing stop, not at entry.

### G and B families — `signal_engine.compute_g_signals` / `compute_b_signals`
Gap and breakout variants (`sig_g1…g11`, `sig_b1…b11`). `project_g3_gap_reclaim` **BUILT**:
a large gap-up on an oversold bar with a T code and non-VB volume, +2.15/6yr.

---

# 5 · STRUCTURE LAYERS

## 5.1 Wyckoff V2 — `wyckoff_v2_engine.py` · LIVE-SAFE

A state machine anchored to a real Selling Climax:
```
0 idle → 1 SC → 2 AR → 3 ST → 4 SPRING → 5 SOS/JAC → 6 LPS → reset
```
Outputs `w2_sc w2_ar w2_st w2_spring w2_sos w2_jac w2_lps w2_evr`, plus `w2_accum` (states 1-4),
`w2_break` (5-6), `w2_state` (0-6) and `w2_tr_quality` (0..1).

**The pivot subtlety, handled correctly:** SC/AR/ST/LPS are pivot-based and therefore fire on the
**confirmation bar**, `pivotLen` bars after the pivot they describe — exactly as the Pine does.
That is what makes them screen-safe. (Contrast `swing_type`, §8, which stores the label on the
pivot bar itself and therefore leaks.)

**BUILT**: `project_wyckoff_spring` — buy the shakeout, not the breakout, +1.06/6yr.
`project_wyckoff_range_super` — reversal edges below the SC, momentum edges between SC and AR.

## 5.2 Physics — `studio/bar_physics.py` · LIVE-SAFE

Eight quantities per bar, each stored as a **class** and as a **raw value** (so a threshold can be
re-cut later without recomputing 132M bars). Ported faithfully from the Pine `260814_TZ_RC` and
`260814_PHYS` studies.

| field | quantity | classes |
|---|---|---|
| `phys_r` | **R, resistance (Ohm)** = (volume / SMA₂₀ volume) ÷ max(\|Δclose\| / ATR, 0.05). Normalised on both sides, so a $400 mega-cap and an $8 name are comparable. | `RA` > 2× its 30-bar median · `RF` < 0.6× · else `RN` |
| `phys_regime` | close vs EMA20 | `U` / `D` |
| `phys_c` | **C, Coulomb** — charge accumulated at confirmed pivot levels (pivot 3-3, ≤12 levels, volume of the pivot bar) | `C2` > 2× median · `C0` < 0.5× · else `C1` |
| `phys_h` | **H, entropy** — binary Shannon entropy of "which side of EMA20", 20 bars | `H0` < 0.60 · `H2` > 0.95 · else `H1` |
| `phys_m` | **M, momentum × mass** = \|EMA20 − EMA20[10]\| / (ATR×10) × (fast/slow dollar-volume) | `M2` > 1.8× median · `M0` < 0.4× · else `M1` |
| `phys_e` | **E, spring energy** — mean ATR compression over 15 bars | `E2` loaded · `E0` slack · else `E1`; a **★** is appended on release (yesterday loaded and today's range > 1.6 × base ATR) |
| `phys_k` | **K, Hooke stretch** = (close − EMA20) / ATR | `K0` < 1 · `K1U/K1D` ≥ 1 · `K2U/K2D` ≥ 3 |
| `phys_s` | **S, resonance** — how many of EMA9>20, 20>50, 50>200 agree | `S0`…`S3U` up-aligned, `S…D` down-aligned |

`phys_line6` concatenates `R·regime·C·H`.

**TRUE-gap** (`phys_gap_true`) fixes a real measurement error: a gap measured from the previous
*close* reports overnight displacement and inflates the class roughly 2× and never under. This
measures from the previous **high or low** — the empty space — and classes it `G1` < 0.2 ATR ·
`G2` < 0.5 ATR · `G3` above.

**Research status: CONTEXT, not ranker.** `project_score_audit_v1` measured twenty scores and
states at k=20 and found 0 RANKER / 1 ANTI / 19 CONTEXT. These letters describe a bar well and
do not order forward returns.

---

# 6 · RESEARCH / DISPLAY LAYERS

Built nightly by `update_all.sh` into `data/*.parquet`. **All are DESCRIPTIVE. None is a ranking
input.** Each has been measured; the verdicts are below and they are not encouraging, which is the
point of recording them.

| layer | file | what it is | verdict |
|---|---|---|---|
| **UDN★ / L-BAL** | `lbal_signals.parquet` | 15-minute L-label balance per session; `★` = the two lines disagree with the daily candle colour (`U`+RED or `D`+GREEN) | **CONTEXT** — fires on 40 %+ of bars |
| **L-VX** | `lvx_signals.parquet` | daily L34/L46 graded `V · VL · VH · VX` by how much intraday support it has, plus the candle colour | **VETO** at the weakest grade: `LVX V` = −14.88 / −11.25 day-clustered (`project_ladder_v1`) |
| **OVD** | `ovdmap_signals.parquet` | opening-vs-close volume displacement tokens `OB / RC / CD / HO / NM?` | **CONTEXT** |
| **VOL7** | `vol7_signals.parquet` | seven-level volume regime, `M·σ` grid, jumps, Σ+ consolidation | **VETO** at the extreme: `M6` (≥5× median) = −11.97 / −12.11 over 585/322 days |
| **SHAPE** | `shapectx_signals.parquet` | seven body-nest shapes under the Pine display priority MTH > CL4 > MID > EXP > CON > LST > WRP, plus `📍floor 🧱key 🏆RS` legs and the 🎯/🔁 cluster axes | **NULL / inverted** — `project_shape_cluster_v1`: NO_CLUSTER is best, 🎯🔁 worst, monotonically. The one keeper is `SWALLOW_DIR LST↑` = −0.63 / −0.47, a **VETO** |
| **▽△** | `anatomy_signals.parquet` | bottom anatomy: `a_loc` (held+key, 0-2) + `a_abs` (1H/15m Z-absorption, 0-3) + `a_rev` (low early + close upper + Z→T flip, 0-3) → score 0-8, verdict `rev / shake / cont`, and `rs` = close/SPY above its own EMA200 | **DETECTOR ONLY** — 1.37× lift, 76 % recall, 33 % precision. Tested nine ways (score ladder, verdict order, turn timing, run quality, two RS transitions); none ranks or trades. Never sort or filter by it |
| **EDGE** | computed by `edge_replay.py` | the validated setup masks and, above them, the **Cluster-Bottom** count | **the one confluence that works** — see below |

## 6.1 🎯 Cluster-Bottom — the only validated confluence

`edge_replay.py` de-duplicates the setups into **eight families** so variants cannot double-count:

```
capit  = QZ-Capit, Washout, D+L1, T1-CapBounce, H1-Bottom      retest = Zone-Retest
spring = Wyckoff Spring                                        gap    = G3
atomic = Atomic                                                oseq   = Z11/T11
l43    = L43-TRIPLE                                            engulf = Engulf-Absorption
```

`conf_n` = how many **distinct families** fired in the trailing 10 bars. Forward edge rises
**monotonically** with it, and — unlike any single family — it survives 2022 and survives
cluster-dedup:

```
>=2 families   +2.51   median +1.02   win 53 %   PF 1.45   6 of 6 years
>=3 families   +3.28   median +1.66   win 54 %              5 of 6 years
>=4 families   +4.47   median +3.05   win 58 %   PF 1.92   6 of 6 years
```

Entry only on a family-event bar (`conf_anyfam`) with trailing density ≥ the tier. Price gate
`close >= 21`; the `$89+` bucket was added 2026-07-13 after it excluded AMD's Feb-Mar 2026 ×4 at
$195 → +50 %.

**This is the one place where counting signals works.** The generalisation does **not**: counting
the nine *display* rows instead of the eight edge *families* was tested on 2026-09-11
(`JOINT_STACK_V2`, 1.54M sessions) and failed — span +1.44 in MINE, **−0.22** in VERIFY. The
difference appears to be that an edge family is a complete validated *setup*, while a display row
is a *marker*, and counting markers measures activity rather than evidence.

---

# 7 · THE SCORES — AND WHY THEY ARE NOT FEATURES

`turbo_score` · `ultra_score` · `ultra_score_v3` · `buy_score` · `prebreak_v2/v3/v4` ·
`beta_score` · `gog_score` · `profile_score` · `rtb_total` · `aes_score` · `final_bull_score`

These are **FITTED**. No future bar appears in the row, but the constants were fitted on outcomes
drawn from the same history. `prebreak_v2` is the clearest example — a frozen logistic regression,
`_BIAS = −1.07019` and 28 hard-coded weights, whose training target was
`mfe_20d >= 20 % AND fwd_10d >= 0`.

Consequences, both of which matter:

1. **They are fine to display and to screen on.** Nothing about them is dishonest live.
2. **They must not be inputs to a model validated on 2021-2026**, because the feature already saw
   those outcomes. In the current research programme they are excluded from modelling and may
   appear only as a *comparator baseline*.

And `project_score_audit_v1` (k=20) is the empirical footnote: **0 RANKER, 1 ANTI, 19 CONTEXT**.
Only the live UV3 ranks, and it does so through its RS and cluster axes — its core is ≈ 0.

---

# 8 · LEAKAGE REGISTER

| field | producer | calculation | future bars? | live-safe? |
|---|---|---|---|---|
| `fwd_*`, `mfe_*`, `mae_*`, `hit_*`, `drop_*` | enricher | forward returns and excursions | **YES** | no — these are labels |
| `is_pivot_*`, `next_pivot_*`, `swing_type`, `swing_type_3/5` | enricher (Williams pivots) | a pivot at *t* is confirmed by bars *t+1…t+n* but stored on row *t* | **YES** | no — `prebreak_v2.py:24` records that they "topped the raw-lift charts precisely because they are look-ahead" |
| `fwd_swing_*`, `swing_ret_from_prev_*`, `pct_to_*`, `bars_to_*` | enricher | distance and return to the next pivot | **YES** | no |
| **`acc_exit_class`, `acc_exit_in_n`** | `studio/enricher.py:239` | a loop running `for i in range(n-1, -1, -1)` stores, on bar *i*, the distance to a **future** markup bar. `BO_1` means "the breakout is tomorrow" | **YES** | **no** — found 2026-09-11 |
| **`aes_score`, `aes_leading`, `aes_trend_5d`, `aes_stage`** | `studio/enricher.py:960` | weights each signal by `lift_2_3`, a lift measured from future outcomes and looked up **per ticker** | no future bar in the row, but the weight was fitted on that ticker's own future | **no** — found 2026-09-11 |
| all scores in §7 | scoring engines | constants fitted on historical outcomes | no | display yes, modelling no |
| `universe` | universe loader | membership **as of today**, not point-in-time | no | survivorship — a ticker is tagged `sp500` because it is in the index *now* |
| T/Z, L, F, FLY, VABS, WICK, ULTRA, CISD, PARA, G, B, physics, Wyckoff V2 | per-bar engines | bar *t* and earlier only | no | **yes — verified by truncation test** |

`backend/tests/test_feature_lookahead.py` runs this as an automated test and carries a canary that
fails if `acc_exit_class` ever stops carrying a forward distance, so it cannot be quietly re-admitted.

---

# 9 · WHAT IS ACTUALLY VALIDATED

The honest summary, because a reference that lists 400 signals without saying which ones work is
worse than useless.

**BUILT — validated, day-clustered, survives its worst year**
T1 Capitulation-Bounce · Engulf-Absorption · Z-Absorb-Turn · G3 Gap-Reclaim · Wyckoff Spring ·
QZ-Capit-Reversal · L43-TRIPLE · L34→L34 continuity · Coil-floor absorption · 1H-confirmed bottom ·
**🎯 Cluster-Bottom** (the only confluence) · 🏆 RS gate (universal worst-year rescuer) ·
⚡ ATR×12 trailing exit.

**VETO — avoid, do not trade the opposite**
Z12 prefix · sub-200 rally (close<EMA200 with EMA9>20>50) · Dec-Mar season · post-earnings ≤5 days ·
VOL7 `M6` · L-VX `V` · SHAPE `LST↑`.

**CONTEXT — describes a bar, does not order forward returns**
All physics letters · all scores except live UV3 · UDN★ · OVD · ▽△ · SHAPE marks.

**NULL — measured, nothing there**
Fractal / cross-TF shape matching · harmonic patterns · Dan Zanger flag breakouts · RSI-50 cross ·
L2/L4 alone · intraday effort balance (k=16) · opening-volume dynamics (k=300) · ▽△ in all nine
directions · signal-layer co-occurrence pairs · the joint display-layer stack.

---

*Keep this file honest: when an engine changes, change the section. When a study settles a
question, record the verdict here, not only in a session note.*
