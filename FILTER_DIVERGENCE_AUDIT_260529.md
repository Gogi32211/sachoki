# Filter Divergence Audit — TradingView feed vs Studio DB (Massive)
2026-05-29

## Why filters diverge
Empirically established (JOBY bar-by-bar): **volume is identical** between the two feeds,
**OHLC prices differ slightly**. So a filter's divergence risk = how sensitive its signal is
to small price differences.

- **Path-dependent / cumulative** indicators (EMA, RSI, CCI, ATR, PSAR, Bollinger, WVF/VIX-Fix)
  compound tiny price diffs over many bars → flip easily. HIGH risk.
- **Penny-sensitive relative** reads (wick vs prev high/low, penetration, body ratios, gap, exact
  EMA crosses) flip on cents. HIGH risk.
- **Gross candle direction / engulfing** is robust to cents → mostly agrees. MEDIUM/LOW.
- **Pure volume** signals use the identical volume series → SAFE. LOW.

Empirical anchor (JOBY 7 bars): L (volume buckets) matched 7/7; TZ 6/7; gap/range 6/7;
body/wick 4/7; suffix 1/7; line5 1/7.

---

## TIER 1 — HIGH divergence risk (verify against DB chart before trusting)
Cumulative indicators + penny-sensitive geometry.

| filter group | depends on | why risky |
|---|---|---|
| **line5: L5★ / VX / VR / PB / PS / R2L / R2H / R2X** | WVF, PSAR, RSI2 | path-dependent over 20-50 bars; confirmed worst (1/7) |
| **suffix: NE / E / WK↑↓↕** | wick vs prev high/low | cents flip U↔P↔R; confirmed 1/7 |
| **body/wick: X / M / S / J / TB / BB / F / XF / MF** | body & wick fractions | penny-sensitive; 4/7 |
| **gap/range: G1 / G2 / G3 / V / C / N** | gap size, range vs ATR | ATR + exact gap detection |
| **CCI / CCI0R / CCIB** | CCI(20) | cumulative oscillator |
| **RSI≤35 / RSI≥70** | RSI(14) | path-dependent |
| **BB↑ / BO↑ / BO↓ / BX↑ / BX↓ / BE↑ / BE↓ / RH / RL / PP** | Bollinger / RSI | bands + crosses |
| **ATR↑ / VA (partly)** | ATR | cumulative volatility |
| **PREUP/PREDN: P66 P55 P89 P3 P2 P50 / D66 D55 D89 D3 D2 D50 / ANY P / ANY D** | EMA crosses | EMA cumulative + cross flips on cents |
| **P>200 / P>89 / P>50 / P>20 / P<...** | price vs EMA | EMA cumulative |
| **3G** | EMA9/20/50 gap | EMA-relative |
| **RTV** | RSI2 + WVF | both path-dependent |
| **HILO↑** | RSI2 | path-dependent |
| **↑BIAS** | MACD + EMA + RSI | multi-cumulative |
| **PREP / PARA / PARA+ / RETEST** | PSAR | path-dependent |
| **WVF (prebreak), PREBREAK: PRIME★/READY/WATCH/LVBO/W-PHASE** | WVF + EMA + ATR + score | composite cumulative |
| **260523: AD-FRESH★ / AD-CLUSTER★★ / WYC: SPRING/UTAD/SOS/ACC_TR/DIST_TR/MARKUP/MKDN / In TR / SOW** | Fourier bandpass + ATR + EMA | heavily cumulative |
| **RTB Phase: A-Build / B-Turn / C-Ready / D-Late** | regime/EMA | cumulative |
| **X2G / X2 / X1G / X1 / X3** | wick-extension geometry | penny-sensitive |

---

## TIER 2 — MEDIUM divergence risk (usually agree, can flip on close calls)
Candle-direction patterns + delta-from-wick + volume-spread mixes.

| filter group | depends on | note |
|---|---|---|
| **TZ: T1..T12 / Z1..Z12 / T1G/T2G/Z1G/Z2G / ANY T / ANY Z / TZ→3 / TZ→2 / W** | candle direction + engulfing vs prev | confirmed 6/7; occasional flip (saw T10↔Z9) |
| **Delta: ΔΔ↑ Δ↑ B/S↑ Ab↑ dSPR cd↑ FLP↑ ORG↑ ΔΔ↑R Δ↑R Δ↓G ΔΔ↓G T↓ NS ND** | buy/sell-vol from wick geometry | delta = volume × wick-position → wick diffs flip it |
| **ULTRA v2: BEST↑ FBO↑/↓ EB↑/↓ 4BF/4BF↓ 3↑** | break-fail + delta | wick/delta sensitive |
| **VABS: BEST★ STRONG ABS CLB LD / NS SQ SC ND** | volume + ATR spread + CLV | volume solid, spread/CLV price-sensitive |
| **Combo 2809: BUY 🚀 UM SVS CON** | ROC + vol + EMA + ATR | mixed |
| **F/G: CD CA CW G1 G2 G4 G6 G11 SBC** | candle patterns + EMA context | mixed |
| **FLY: ABCD CD BD AD** | swing/pivot geometry | pivot-based |
| **260308 / L88** | volume jump + delta + L34/L43 | volume core + delta |
| **Swing: HL (bounce LL) / HH (top) / LH / Any pivot** | Williams 3-3 pivots on price | local extremes fairly robust; a pivot can shift ±1 bar |

---

## TIER 3 — LOW divergence risk (volume-based → SAFE, matches TradingView)
These use the volume series, which is identical across feeds.

| filter group | depends on | note |
|---|---|---|
| **L family: ANY L / L1..L6 / L22 / L34 / L43 / L64 / FRI34 / FRI43 / L555 / L1L2** | WLNBB volume Bollinger buckets | confirmed 7/7 match |
| **Vol bucket: VB / B / N** | same volume buckets | safe |
| **VABS volume multiples: V×20 / V×10 / V×5** | volume / prev-bar volume | volume identical → safe |

---

## NOT IN DB (separate issue, not feed divergence)
These never compute in DB-instant mode regardless of feed:
- **RGTI: LL / UP / ↑↑ / ↑↑↑ / ORG / GRN / GC** and **SMX** — disabled in LIVE too
- **GOG / CTX: A / SM / N / MX / GOG / G1P..G3C / ★GOG+ / !EXT / LDS / LDC / LDP / LRC / LRP / WRC / SQB / BCT** — gog_engine multi-input, not ported to DB
- **Sector: XLC..XLU** — DB `sector` is NULL, lazy-fetched
- **Cross-engine: ⚡×2+ / ×3+ / ×4+** — depends on the above

---

## ADDENDUM — remaining filters (verified in code)

### TIER 1 — HIGH
| filter | depends on | note |
|---|---|---|
| **RS+ / RS** | Relative Strength vs SPY+IWM (turbo_engine) | price ratio + needs index feed; double-feed-sensitive |
| **Z7** | doji = close==open (exact equality) | one cent removes the doji → fragile |
| **CON** (consolidation 2809) | ATR / range / BB | cumulative volatility |
| **Macro: No penalty / Penalty** | pb_macro_penalty = EMA20 falling + below EMA50 + drawdown | EMA cumulative |
| **Profile: Sweet Spot / Building / Watch** | composite turbo score (many cumulative inputs) | composite |
| **BE / BL** | WLNBB Bollinger-extended | bands |

### TIER 2 — MEDIUM
| filter | depends on | note |
|---|---|---|
| **VBO↑** | volume breakout above level (vabs_engine) | volume core + price level |
| **ABS / CLB / LD** | VABS absorption / climax / load (volume + spread + CLV) | volume solid, spread price-sensitive |
| **CW / SBC** | F/G candle patterns | direction patterns |
| **B/S↑ / T↓ / Δ↑R / Δ↓G / ΔΔ↑R / ΔΔ↓G** | delta red/green variants | delta-from-wick (same as delta family) |

### TIER 3 — LOW (volume-based → SAFE)
| filter | depends on | note |
|---|---|---|
| **VA** | volume-ratio crossover (vol > 2× avg) — VERIFIED volume-only | safe |
| **FRI64 / L2L4 / L\*** | WLNBB volume buckets | safe |

### METADATA / not price (no feed divergence per se)
- **[E]** = earnings marker — event date from metadata (needs earnings data in DB, but not price-feed-sensitive)
- **yf** = could not locate definition in engines — treat as unknown / verify if used

### NOT IN DB (re-confirmed)
- RGTI: LL / UP / ↑↑ / ↑↑↑ / ORG / GRN / GC ; SMX
- GOG/CTX: A / SM / N / MX / GOG / G1P..G3C / ★GOG+ / !EXT / LDS / LDC / LDP / LRC / LRP / WRC / SQB / BCT
- ⚡×2+ / ×3+ / ×4+ (cross-engine count of the above)
- Sector XLC..XLU (sector NULL, lazy-fetched)

## Practical rule
- **Trust on any chart:** L-codes, vol buckets, V×N (volume-based).
- **Cross-check against the DB Chart before acting:** everything in TIER 1 (especially line5,
  suffix, RSI/CCI/PSAR/EMA/WVF, PREUP/PREDN, WYC, PREBREAK) — these can read differently on
  TradingView vs the DB the scanner uses.
- TIER 2 usually agrees but verify on borderline candles.
- The DB Chart tab shows the DB's own values — that's ground truth for what the scanner/Sequence
  Builder use.
