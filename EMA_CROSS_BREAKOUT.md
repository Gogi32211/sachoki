> ⚠️ **CORRECTION (verified by true bar-by-bar path simulation).** The expectancy
> numbers below (e.g. "+3.4%, 6/6 years") were computed with an **MFE-based proxy**
> that counted any bar reaching +50% within 20 bars as a winner — **even if it first
> dropped through the −15% stop.** On these volatile names most eventual-winners dip
> −15% first, so a real −15% intraday stop gets whipsawed out. Under a true
> day-by-day path sim (stop-first), the edge is **NEGATIVE at every volume level**
> (−1.5% to −2.7% cost-adjusted, 0/6 years; higher volume = worse). The only exit
> that reaches break-even is a **CLOSE-based −15% stop ≈ +0.15% net** — not a
> tradeable edge. **The P/EMA-cross + volume breakout does NOT have a robust
> deployable edge.** The signal marks breakout-*starts* descriptively, but positive
> expectancy can't be extracted with a stop. Treat everything below as signal-anatomy
> notes, NOT a validated strategy. (Consistent with the rest of the system: the real
> edge is capitulation / absorbed-weakness mean-reversion, not breakout/momentum
> chase.)
>
> **SHORT/FADE inversion also tested & rejected.** Fading the high-volume breakout
> looked like +1.4% with an optimistic stop-fill, but a **gap-aware** short sim
> (stop-buy fills at the OPEN when the name gaps up past the stop — the real squeeze)
> plus borrow cost flips it to **−2.2% cost-adjusted, 1/6 years, worst trade −626%
> to −1700%** (a single RGTI/SDEV-type runner squeezes the short into oblivion). Only
> 2022 (bear) was positive. So **neither direction is an edge** — the volatility
> whipsaws longs and squeezes shorts symmetrically. Lesson (twice over): an
> optimistic fill assumption (stop fills exactly at the stop level) inflates BOTH
> long and short expectancy; always gap-fill in the path sim.

# EMA-Cross Breakout — full study (P-signals, volume, candle, D→P, 3G)

Trigger idea (user): *"a breakout often starts with an EMA crossover + volume."* The
P-signals already encode exactly this. Studied on the full 5yr / ~8M-bar DB with an
**asymmetric breakout exit** (−15% stop / +50% target / 20-bar), measured by
expectancy per trade `E = P(+50%)·50 + P(stop −15%)·(−15)` and per-year stability.

A buy-and-hold (median fwd) view makes every breakout look −EV — breakouts are
right-tailed: most fail (you get stopped), a few moon (SDEV-type +200%). The
asymmetric exit is the correct lens: the tail pays for the stops.

## The signals
`P = a bar that OPENS below an EMA and CLOSES above it` (an EMA crossover ON that bar):
- **P2** = crossed EMA9 & EMA20 (the FAST cross) — earliest
- **P3** = crossed 9 & 20 & 50 together
- **P50 / P89** = crossed EMA50 / EMA89
- **P55 = EMA89-reclaim, P66 = EMA200-reclaim** — the slow/late crosses
- **3G (`sig_3g`)** = GAPPED above all 3 fast EMAs at once (open already above)
- **D2/D3/D55/D66/D89** = the bearish mirror (open above EMA, close below = down-cross)

## Core finding #1 — VOLUME MAGNITUDE is the edge, not the cross
Every P variant is ~flat to −EV alone; the edge scales monotonically with the volume
of the cross bar (`volume / avg_vol_20d`):

| vol on the cross bar | E/trade (any-P) | +yr |
|---|---|---|
| ≥3× | +0.7% | 4/6 |
| ≥7× | +3.4% | 6/6 |
| ≥10× | +4.9% | 6/6 |

Volume confirms the cross is real, not a whipsaw. **No volume → no trade** (any-P
alone = −1.6%, 0/6).

## Core finding #2 — P2 (fast cross) >> P3/P66/3G (late cross / gap)
Earlier you catch the cross, the better. **P2 (9&20) is the best; 3G (gap) is the WORST.**

| signal + vol≥10x | E/trade | +yr | note |
|---|---|---|---|
| **P2** | **+5.5%** | 6/6 | fast cross — best |
| P89 | +4.8% | 6/6 | |
| any-P | +4.9% | 6/6 | dominated by fast crosses |
| P3 (9+20+50) | +4.5% | 5/6 | later |
| P66 (EMA200) | +3.7% | 5/6 | late |
| **3G (gap)** | **+0.4%** | 3/6 | gap already ate the move |

⚠️ **The scoring engine ranks 3G HIGHEST** (`br_engine` prebreak bonus +12, vs P3 +10,
P89 +8). The data says the opposite — **3G is the weakest breakout entry.** A bar that
gaps over all the EMAs has already made the move inside the gap; little follow-through.

## Core finding #3 — the refinements that STACK (all stay 6/6)
| add-on | what | lift |
|---|---|---|
| **clean** | no D (down-cross) in the prior 20 bars | **+1.6** |
| up-wick + escape | `wick_suffix=U` & `ne_suffix=E` candle | +0.4 |
| washout lead-in | selling-climax / vbo_dn in prior 5 (alt to clean) | +1.4 |
| squeeze same-bar | `sq` on the cross bar | +1.1 |

### Final recipe (validated, 6/6 years)
```
P2  +  volume ≥ 10×  +  clean (no D-cross in prior 20)  +  up-wick & escape candle
→  E/trade +7.5%,  P(+50%) 32.9%,  exit −15% / +50% / 20-bar
```
Practical higher-n variants: `P2 + vol≥7× + clean` = +6.2% (n=973); `any-P + vol≥7× +
clean + upwick-escape` = +4.8% (n=1936). Base `any-P + vol≥7×` = +3.4% (n=7538).

MFE/MAE of the base trigger: stop-out ~66%, but P(+50%) 23%, P(+100%) 11%, P(+200%) 4%
— the moonshot tail (SDEV-type) carries it. Discipline required: ~2/3 stop out.

## What DOESN'T work (tested, honest)
- **3G gap** — worst variant (despite the engine over-scoring it).
- **D→P "reversal"** — a P preceded by a recent D (down-cross) trades WORSE (+2.6 vs
  +3.4); the best breakouts have NO recent D. A *gradual* breakdown→reclaim is choppy.
  (A *sharp* selling-climax washout before the breakout IS good — see below.)
- **Tight base / consolidation** — REDUCES the edge (−0.9). Low energy. Explosive
  breakouts come from looser setups, not tight coils.
- **L34/L46 lead-in** — present before ~91% of these breakouts, so descriptively true
  but adds NO selectivity. Same-bar L34/L46 slightly HURTS.
- **BE/EB same-bar** — BE neutral, EB hurts (−1.0); big-body candle = already extended.
- **FBO same-bar** — hurts a lot (−2.7); it's a failed-breakout marker.
- **'O' (weak-close) suffix** — rare on breakout bars; 'O' is the *Atomic fade* edge,
  not breakout. Breakouts want a STRONG close (`close_suffix=A`).

## Connection to the broader thesis
The one lead-in that HELPS is a prior **selling-climax washout** (sc / vbo_dn, +1.4 to
+2.6) — i.e. **capitulation → reclaim**. That is the same "buy absorbed weakness"
edge found across the rest of the system, now expressed as: *washout, then a
high-volume fast-EMA reclaim.* Distinguish from the choppy D→P (gradual breakdown),
which does NOT work.

## Caveat
SDEV's own Dec-2025 breakout is NOT in our DB (SDEV only present from 2026-04-06), so
the pattern was validated across the whole universe rather than that single name —
more robust anyway. +7.5%/trade is strong and 6/6-year stable, but n is smaller at the
tightest stack (441); the +4.8% / n=1936 variant is the robust workhorse.
