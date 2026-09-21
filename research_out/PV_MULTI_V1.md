# PV_MULTI_V1 — nine price × volume ordinal shapes

**Source** user Pine `260921 PRICE × VOLUME — Multi Signal` · **k = 9** · sealed `a8251b0e18915f90`
· one outcome run, access count 1 · **0 BUILD · 4 VETO_CANDIDATE · 5 NULL**

## Why it was worth a k

Every volume feature the app stores is a **level** — `vol_bucket` W/L/N/B/VB against a 20-bar band,
VOL7 `M0…M6` against the 20-day median, `sig_vol_5x/10x/20x` against the average. Across 439 columns
of the 1D store there is **no ordinal volume-direction feature at all**, and the single `rising3` in
the whole codebase ([ovd_map_build.py:120](../backend/ovd_map_build.py:120)) runs on same-slot
opening RVOL, not on raw daily volume. The shapes were therefore genuinely untested — while their
neighbourhood was uniformly NULL: OPENING_VOLUME_DYNAMICS k=300 → 283 NULL, BQB_V1 k=6 → NULL,
TURN_V1 k=3 → 3/3 fail, ADJACENT_TURN_V1 → VERIFY −0.08, INTRADAY_EFFORT_BALANCE_V1 → 16/16 NULL.

## Census, read before sealing

2.95M liquid ticker-sessions, contract clean, adjacency 99.69%:

| | DIV | UPP | UPR | REV | RUP | VUP | TURN | UP4 | RE2 |
|---|---|---|---|---|---|---|---|---|---|
| fire rate | 5.95% | 5.97% | 3.58% | 1.64% | 3.83% | **10.55%** | 0.74% | 1.42% | 2.19% |
| median RVOL | 1.35 | 1.38 | 1.33 | 1.04 | 1.04 | **0.81** | 1.10 | 1.43 | 1.14 |
| in B/VB | 12.1% | 14.5% | 14.2% | 5.7% | 4.6% | 1.0% | 5.5% | 14.4% | 7.0% |

⭐ **"Volume rising three days" is not "high volume".** Median RVOL on a firing bar is 1.33–1.43 and
only 12–15% land in the app's B/VB buckets; VUP's median RVOL is 0.81 with 76% *below* the 20-day
median. These shapes select the **middle** of the volume distribution — exactly the region the
level vocabulary says nothing about, which is what made them a distinct object rather than a rename.

**The nine are mutually exclusive by construction** (every pair contradicts on the price or the
volume leg; the build hard-stops if any bar carries two). 35.9% of liquid bars carry exactly one.

**Succession was measured descriptively and carries nothing.** Every A→B lift sits at lag 1–2 and
collapses to 1.00 from lag 3 onward — `DIV→RE2` is **0.00** at lag 1 and `TURN→TURN` **0.00** at lags
1–3. Those are impossibility constraints from shared bar windows, i.e. arithmetic. No sequence cell
was registered, because there was nothing to register.

## Result — all nine negative in MINE, eight of nine in VERIFY

```
 cell  trades  days |  MINE med    dw  neg yrs   best |  VER med    dw   best | dsr_neg  verdict
  REV  33,584   807 |    -0.909  42.8    4/4    -0.57 |   -0.686  44.3  -0.57 |   1.000  VETO_CANDIDATE
  RE2  45,398   805 |    -0.689  44.8    3/4     0.06 |   -0.320  47.7  -0.02 |   1.000  VETO_CANDIDATE
  UPP  95,686   832 |    -0.618  44.7    4/4    -0.30 |   -0.573  43.0  -0.15 |   1.000  VETO_CANDIDATE
 TURN  15,692   715 |    -0.396  47.7    4/4    -0.13 |   -0.508  47.2  -0.23 |   0.493  NULL
  RUP  78,162   821 |    -0.350  46.0    4/4    -0.04 |   -0.316  46.3  -0.13 |   0.996  VETO_CANDIDATE
  VUP 155,445   831 |    -0.331  45.6    3/4     0.03 |    0.029  51.1   0.07 |   0.928  NULL
  UPR  66,614   831 |    -0.303  46.6    3/4     0.05 |   -0.265  46.3  -0.10 |   0.929  NULL
  DIV  92,420   828 |    -0.253  47.3    3/4     0.14 |   -0.473  45.4  -0.47 |   0.673  NULL
  UP4  28,542   806 |    -0.177  48.4    3/4     0.51 |   -0.709  45.4  -0.71 |   0.890  NULL
```

Every cell cleared its size-matched placebo band in both windows (p ≤ 0.035; most 0.000 — at 30k–155k
trades the day-clustered median of a signal-free draw is very stable, band sd 0.000–0.156). TURN and
UP4 are NULL on `dsr_neg`, not on the placebo.

## The control was audited before the result was believed

Nine mutually exclusive shapes covering a third of all bars cannot all be worse than an ordinary bar
unless the benchmark is offset — so a **disjoint second 1-in-40 de-phased grid** (same rule, offset
+20, zero overlap with the control keys) was path-simmed against the same control:

```
ORDINARY BAR  MINE    n 34,476 · 834 days · median_edge  +0.009 · day_win 50.1
ORDINARY BAR  VERIFY  n 19,632 · 419 days · median_edge  −0.179 · day_win 47.7
```

**MINE is clean** — an arbitrary eligible bar is worth exactly zero against this control, so the MINE
column is interpretable as written. **VERIFY carries a −0.18 offset of its own**, so every VERIFY
number above is that much too generous to the negative side; REV at −0.686 is really ≈ −0.51 beyond
an ordinary bar. The MINE window is the one that decides, and it is the clean one.

This diagnostic was **not in the registry** — omitting it was my error (LADDER_V1 registers
`population_vs_control` precisely for this). It is reported as a control audit, not as a cell.

## What it means

**Four VETOes.** REV is the cleanest negative in the set: −0.909 / −0.686, **negative in every one of
the four MINE years**, best year −0.57, `dsr_neg` 1.000. UPP — price up three days on rising volume,
the textbook "healthy accumulation" read — is negative in all four years too, which is the book's own
`project_entry_timing` law (strength-chasing loses) arriving from a new direction.

**VUP is the one that is not negative in VERIFY** (+0.029, day-win 51.1) — the quiet, volume-drying
shape, matching BQB_V1's "quiet pullback beats chasing but raw ≈ 0".

⭐ **My stated prior was wrong, and in the opposite direction.** Before the run I singled out REV and
RUP as the two most likely to work, because volume contracting and then price and volume turning up
together is the grammar of Wyckoff Spring and coil-floor absorption. REV came back the **worst cell
in the family**. The shape those book edges share is not "both turn up together" — it is *absorption*,
which is a volume **level** at a price **location**, and none of that survives being reduced to three
ordinal comparisons.

**POST_EXPOSURE, not a result:** the failure ordering runs from confirmation (UPP, REV, RE2 worst)
toward quiet (VUP least bad), which is the book's "confirmation costs" law. The family is now
SEARCH-EXPOSED; that observation is a hypothesis for a separate family, never a claim from this one.

## Status

Family **CLOSED**. One outcome run, ledger access count 1. No threshold, window, price-bucket or
timeframe search follows, and no failed cell may be re-read as a different shape.

**Nothing has been wired into production.** The four VETO candidates are recorded, not applied: they
cover ~13.6% of bars between them, so using them as suppressors is an interaction question about the
existing setups and would need its own family and its own k.

### Declared deviations from the approved 5-line plan

1. Price floor **close ≥ 21, not ≥ 5** — CONTROL_KEYS v2 is built at ≥ 21, and a cell measured on a
   universe its control does not cover is what made "buy every bar" a BUILD_CANDIDATE in
   UDN_CONFLICT_V1. The family follows the control.
2. Window **2021-09-07 … 2026-09-03** — the control artifact's own window, not 2021-05.
3. **No 1-in-N sampling**; the registered masks ran directly (ADJACENT_TURN_V1's thin sample
   manufactured a replication that 12× more data erased, and the cooldown estimand forbids mod-N
   partitions around the engine's 5-bar rule).

### A port defect caught before sealing

The first draft required **four prior bars for all nine**, which silently suppressed the shallow
shapes (DIV/UPP/UPR/VUP, legal on a ticker's 3rd bar in Pine) at every series start. No downstream
statistic could have revealed it — it would have produced a perfectly well-formed NULL.
`backend/tests/test_pv_multi_port.py` (22 hermetic tests) pins every shape against a hand-built
frame, including the user's own worked RE2 example (price 20→18→17→19, volume 5→8→4→9), mutual
exclusivity, per-shape warm-up depth, cross-ticker isolation, and that ties fire nothing.
