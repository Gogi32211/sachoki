# SUPERCHART CSV — COLUMN REFERENCE

**Every one of the 476 columns in a `<TICKER>_1d_signals.csv` export: what produces it, exactly how
it is calculated, and whether it carries information at all.**

Written 2026-09-11 against `OKLO_1d_signals.csv` (585 rows, 2024-05-10 → 2026-09-10). The deep
engine logic lives in [`SIGNAL_REFERENCE.md`](SIGNAL_REFERENCE.md); this file is the column-by-column
map. Producer of the file: `backend/bulk_export.py` → `main.api_bar_signals()`, mirroring
`SuperchartPanel.jsx :: exportCsv()`.

## Status tags

| tag | meaning |
|---|---|
| **SAFE** | uses bar *t* and earlier only. Verified by `backend/tests/test_feature_lookahead.py` |
| **FITTED** | no future bar in the row, but the constants were fitted on historical outcomes — display yes, modelling no |
| **LABEL** | a forward outcome. Never an input |
| **DEAD** | no producer writes it, or it has never fired on 8.9M bars. Measured, not guessed |
| **SPARSE** | fires elsewhere, just never for this ticker |

## ⚠️ Headline finding

**114 of 476 columns (24 %) are empty or zero in this export.** They split into two very different
groups and the difference matters:

**Structurally DEAD — no producer exists.** All 20 `MDL_*` model columns, `HAS_ELITE_MODEL`,
`HAS_BEAR_MODEL`, ten GOG sub-score columns, and the basic derived-stat block
(`PCT_CHANGE_3D/5D/10D`, `PCT_FROM_20D_HIGH/LOW`, `DIST_20D_HIGH`, `VOL_RATIO_20D`,
`DOLLAR_VOLUME`, `GAP_PCT`). `bulk_export.py:243` reads `b.get("mdl_um_gog1", 0)` and nothing in
the codebase ever sets `mdl_um_gog1` — the default is the only value these columns have ever had.

**DEAD at the engine level** — present in `bars`, never fires on any of 8.9M rows:
`SIG_X1` · `SIG_X2` · `SIG_X1G` · `SIG_X3` · `SIG_L2L4` · `SIG_NS_DELTA` · `SIG_ND_DELTA` ·
`wyc_sow` · `ALREADY_EXTENDED_FLAG` · `ATR_BREAKOUT`. And `prebreak_prime` fires on 0.0087 % —
777 bars out of 8.9M, which is dead for any practical purpose.

**SPARSE, not dead** — real signals that simply did not fire for OKLO:
`SIG_PARA_*` (1.1-3.4 % universe-wide) · `wyc_spring` 0.045 % · `wyc_sos` 0.085 % ·
`SIG_3UP` 0.27 % · `SIG_BEST_UP` 0.14 % · `SIG_FRI43` 0.32 % · `SIG_B4` 0.081 % · `SIG_B9` 0.020 % ·
`SIG_VOL_10X` 1.10 % · `SIG_VOL_20X` 0.57 % · `pb_lvbo` 7.34 % · `ROCKET` 0.19 %.

---

# BLOCK 1 — BAR (cols 1-6)

| column | logic | status |
|---|---|---|
| `date`, `open`, `high`, `low`, `close` | raw daily OHLC, de-duplicated across universe tags by `sp500 > nasdaq > russell2k` | SAFE |
| `vol_bucket` | WLNBB volume class. BB(20) on volume with **population** σ: `W` < mid−σ · `L` < mid · `N` < mid+σ · `B` < (mid+σ)+mid · `VB` above | SAFE |

---

# BLOCK 2 — RTB, Reversal-To-Breakout (cols 7-22)

`backend/rtb_engine.py` — **RTB v4**. Ranks a name moving *downtrend → dead base → accumulation →
first reversal → breakout-ready*, aiming to flag the **1-3 bars before** the breakout.

| column | logic | status |
|---|---|---|
| `turbo_score` | see BLOCK 5 | FITTED |
| `rtb_build` | **A-phase** score, cap 12 — base / compression evidence | SAFE |
| `rtb_turn` | **B-phase** score, cap 14 — the first reversal | SAFE |
| `rtb_ready` | **C-phase** score, cap 12 — breakout-ready | SAFE |
| `rtb_bonus3` | 3-bar context bonus, cap 8 | SAFE |
| `rtb_late` | **D-phase** penalty, cap 12 — already gone | SAFE |
| `rtb_total` | `max(0, build + turn + ready + bonus3 − late)` | SAFE |
| `rtb_phase` | `"0" \| A \| B \| C \| D` | SAFE |
| `rtb_transition` | `A_START · A_HOLD · A_TO_B · B_HOLD · B_TO_C · C_HOLD · C_TO_D · RESET_HARD · RESET_SOFT` | SAFE |
| `dbg_*` (7 cols) | engine debug: context-ready flag, T4/T6 context, activation bonus, launch-cluster count, pending phase and its age | SAFE |

RTB consumes the turbo row under its own names: `conso_2809→CONSO`, `l64→L64`, `l22→L22`,
`l34→L34`, `fri34→FRI34`, `l43→L43`, `cci_ready→CCI_READY`, `abs_sig→ABS`, `climb_sig→CLM`,
`load_sig→LD`, `ns→NS`, `sq→SQ`, `d_spring→dSPR`, `sig_l88→L88`, `sig_260308→260308`,
`tz_bull_flip→TZ→3`, `tz_attempt→TZ→2`.

> **Research:** `project_rtb_score` — the *score* is **ANTI-predictive**, it ranks backwards. The
> only thing that survived path-sim is the **A/B build-and-turn phase with RSI < 35**. Use the
> phase, not the number.

---

# BLOCK 3 — THE SIGNAL TOKEN ROWS (cols 23-38)

One string per row, exactly what the chart prints.

| column | content | status |
|---|---|---|
| `Z` / `T` | the bar's single priority-resolved TZ code — see SIGNAL_REFERENCE §2 for all 26 rules | SAFE |
| `L` | the L-line composite (`L34 · L43 · L22 · L12 · L25 · L46 · L5 …`) | SAFE |
| `F` | F1-F11 pattern names | SAFE |
| `FLY` | `FLY` when an ABCD/CD/BD/AD role sequence completed on this bar | SAFE |
| `G` | G-family gap signals | SAFE |
| `B` | B-family breakout signals | SAFE |
| `Combo` | combo tokens (`CONSO`, `SVS`, `HILO↑/↓`, `UM`, `CA` …) | SAFE |
| `ULT` | ULTRA tokens (`EB↑`, `4BF` …) | SAFE |
| `VOL` / `VABS` / `WICK` | volume, absorption and wick-reversal tokens | SAFE |
| `SETUP` / `CONTEXT` | `setup_tokens` / `context_tokens` — the GOG engine's own token strings | SAFE |
| `GOG_TIER` | the winning GOG tier (below) | SAFE |
| `ALL_SIGNALS` | everything concatenated, for eyeballing | SAFE |

---

# BLOCK 4 — GOG PRIORITY ENGINE (cols 39, 87-114)

`backend/gog_engine.py` — port of Pine *260501 GOG Priority Engine — FULL + Internal F8*.
Parameters: lookback 5 · load 10 · LDP 10 · WRC 10 · context cooldown 2 · bottom 10 · hard-bottom
14 · support 18 · absorption 12 · ignition 6 · sequence 24 · cooldown 4 · RSI 14 · late-break ×2.80.

## 4.1 The four setup tokens

Each is raw-detected then passed through a **4-bar cooldown** so one event cannot re-fire:

| token | what it marks |
|---|---|
| `A` | *akan* — the deep-bottom setup |
| `SM` | the SMX setup |
| `N` | the nnn setup |
| `MX` | the MX setup |

Their building blocks are named explicitly in the engine:
```
hardBottomZ  = Z4 | Z6 | Z9 | Z10 | Z11 | Z12
lateBottomZ  = Z10 | Z11 | Z12                 bottomT = T10 | T11 | T12
recentCompression   = hardBottomZ within 14 bars  OR  (lateBottomZ|bottomT|F8) within 10
absorptionContext   = recentSupport AND recentAbs
softAbsorptionContext = recentAbs | recentSupport | (SQ|NS|LOAD|ABS) within 12
firstIgnitionNow = T3|T2G|T2|T6|F3|F6|F4|F8|BO_UP|BE_UP|VBO_UP|BUY_HERE|SIG_260308|L88
earlyIgnitionNow = (T3|T2G|T2|T6|F3|F6|F8) AND softAbsorptionContext
```

## 4.2 The context tokens (cols 103-112)

All cooled down by 2 bars:
```
SQB = SQ AND (L64 | L34)                      squat on an absorption bar
BCT = SQB AND SVS                             the same, confirmed by SVS
LD  = LOAD                                    the VABS load signal
LDS = LOAD AND strBstContext                  load inside a strong-best context
LDC = LOAD AND SQ AND (L64 | L34)             load + squat + absorption
LDP = LDC AND strBstContext                   the premium version
LRC = l_prev3 AND L34                         an L34 today with an L-event in the last 3 bars
LRP = LRC AND SQ AND LOAD                     the premium reclaim
WRC = isW AND (recentCompression | supportStep | absStep | NS | SQ)   a W-volume bar in context
F8C = F8                                      cooled-down F8
```
`l_prev3` = any of `L64 · L43 · L22` on bars *t−1, t−2, t−3*.

## 4.3 The GOG tiers (cols 91-102) and `GOG_SCORE`

```
asSetup = A | SM        nmSetup = N | MX        (each "recent" = within 5 bars)

GOG1 = VBO_UP AND asRecent AND nmRecent        both setup families present
GOG2 = VBO_UP AND nmRecent AND NOT asRecent
GOG3 = VBO_UP AND asRecent AND NOT nmRecent

recentPremium = LDP or LRP within 10 bars
recentLoad    = LOAD within 10 bars
recentCompCtx = WRC or F8C within 10 bars

G1P/G2P/G3P = GOGn AND recentPremium
G1L/G2L/G3L = GOGn AND recentLoad    AND NOT recentPremium
G1C/G2C/G3C = GOGn AND recentCompCtx AND NOT recentLoad AND NOT recentPremium
```

`GOG_TIER` / `GOG_SCORE` take the **first match** down a fixed priority ladder — every GOG is
anchored on a `VBO_UP`, and the suffix says what context preceded it:

```
G1P 100 · G2P 92 · G3P 88 · G1L 82 · G2L 76 · G3L 72
G1C  66 · G2C 60 · G3C 56 · GOG1 50 · GOG2 46 · GOG3 42
```

> **Research:** `project_g3_gapchain` — `G3→G3→RL` PF 3.07 over 6 of 6 years, **BUILT**.
> `project_lead_in_lag` found 🥇G3 among the two families that die under search-burden deflation.

---

# BLOCK 5 — THE SCORE CASCADE (cols 40-53, 76-86)

`backend/canonical_scoring_engine.py` — *"single source of truth… no other module may recompute
these values independently."* All **FITTED**: the weights below were chosen from historical
average-forward-return tables, so they are fine to display and must not be model inputs.

`FINAL_BULL_SCORE` **is** `turbo_score` — the same number under two names, both from
`turbo_engine._calc_turbo_score(sig_row, profile)`. The sub-scores are a **breakdown for reading**,
not addends: the total is not their sum.

| sub-score | cap | components (exact weights) |
|---|---:|---|
| `ROCKET_SCORE` | 40 | `rocket` +25, else `buy_2809` +8 · `seq_bcont` +8 · `vol_spike_10x` +6 |
| `CLEAN_ENTRY_SCORE` | 40 | `f8`+12 `f6`+10 `f3`+5 `f4`+5 `f11`+5 (NQ +2) · `b8`+5 `b6`+5 · `ultra_3up`+8 · `blue`+6 · `fly_abcd`+8 `fly_cd`+5 `fly_bd`+4 · **SP500 only:** `f9`+6 `f5`+5 `svs`+8 `buy`+8 `g4`+5 `g1`+5 `strong`+5 `eb_bull`+5 `be_up`+5 |
| `SHAKEOUT_ABSORB_SCORE` | 30 | `be_up`+18 · `eb_bull`+10 · `fbo_bull`+8 |
| `EXTRA_BULL_SCORE` | 20 | `l34`+6 · `fri43`+5 (NQ 0) · `fuchsia_rl`+1 · `cci_ready`+4 · `bx_up`+3 · `l43`+3 · NQ: `b9`+5 `fri64`+2 · SP: `l555`+6 `fri64`+5 `best_long`+8 |
| `EXPERIMENTAL_SCORE` | 15 | `fly_abcd`+8 `fly_cd`+5 `fly_bd`+4 `fly_ad`+3 |
| `REBOUND_SQUEEZE_SCORE` | 20 | `tz_bull_flip`+8 · `ca`+5 · `tz_attempt`+4 · `tz_weak_bull`+3 (NQ 2) |
| `HARD_BEAR_SCORE` | 40 | `fbo_bear`+20 (NQ 0) · `fuchsia_rh`+10 (NQ 3) · `bo_dn`+12 · `bx_dn`+8 · `eb_bear`+8 · SP `bias_down`+6 |
| `VOLATILITY_RISK_SCORE` | 20 | `vol_spike_10x`+10 — **DEAD in this export** |

**The exchange split is not cosmetic.** `f11` is +5 on SP500 and +2 on Nasdaq because `SIG_F11`
averaged −0.41 % over 10 days on NQ; `fri43` drops to 0 on NQ (−0.79 %); `best_long` is +8 on SP
and absent on NQ (−2.96 %). The same signal genuinely reverses sign by exchange.

One deletion is worth reading, because it is the house style: `cci_0_retest` used to add +5 to
`HARD_BEAR_SCORE` and was removed 2026-07-28 — path-sim gave median −0.66 vs a −0.69 baseline,
*and* it was **inverted**: 88 % of fires are CCI reclaiming zero from below, which is bullish and
was being scored as bearish.

## `FINAL_REGIME` — priority ladder

```
HARD_BEAR >= 30                              → BEARISH_PHASE
FBS < 20                                     → NEUTRAL_OR_LOW
FBS >= 140                                   → ELITE_CLEAN_BULL
FBS >= 120   clean>=15 ? A_PLUS_CLEAN_BULL : CONFIRMED_BULL
rocket AND FBS >= 60                         → ROCKET_WATCH
FBS >= 100   clean>=20 → A_PLUS · shakeout>=15 → SHAKEOUT_ABSORB · else CONFIRMED_BULL
FBS >=  80   clean>=15 → CLEAN_ENTRY · shakeout>=15 → SHAKEOUT_ABSORB · else CONFIRMED_BULL
FBS >=  60   clean>=10 → CLEAN_ENTRY · shakeout>=12 → SHAKEOUT_ABSORB · rebound>=8 → REBOUND_SQUEEZE · else ACTIONABLE_SETUP
FBS >=  40   rebound>=8 → RISK_REBOUND · else EARLY_WATCH
FBS >=  20                                   → EARLY_WATCH
```

`FINAL_SCORE_BUCKET`: `ELITE_140+ · STRONG_120+ · BULL_100+ · CONFIRMED_80+ · ACTIONABLE_60+ ·
EARLY_40+ · WEAK_20+ · NEUTRAL`.

## ☠️ Dead score columns (cols 42-43, 50, 54-86)

`SIGNAL_BUCKET` · `RESEARCH_SCORE` · `REGIME` · `VOLATILITY_RISK_SCORE` · all 20 `MDL_*` ·
`HAS_ELITE_MODEL` · `HAS_BEAR_MODEL` · `BEARISH_RISK_SCORE` · `GOG_BASE_SCORE` ·
`PREMIUM_CONTEXT_SCORE` · `LOAD_CONTEXT_SCORE` · `L_RECLAIM_SCORE` ·
`COMPRESSION_CONTEXT_SCORE` · `SQ_BCT_SCORE` · `BASE_SETUP_SCORE` · `RAW_SUPPORT_SCORE` ·
`RISK_PENALTY` · `RESEARCH_FORWARD_SCORE`.

The `MDL_*` names describe intended **pairwise co-occurrence models** — `MDL_F8_BCT` would be
"F8 together with BCT", `MDL_UM_GOG1` "UM together with GOG1" — and the header list exists in
three modules (`bulk_export`, `replay_engine`, `tpsl_engine`). None of them computes a value.
They have been zero since the columns were added.

---

# BLOCK 6 — RAW SIGNAL FLAGS (cols 115-151, 189-330)

The bulk of the file: one 0/1 column per signal, all **SAFE**. Grouped by producer.

### WLNBB / L-family — `wlnbb_engine.py`
`L34 L43 L64 L22` · `SIG_L555` (three consecutive L5) · `SIG_L2L4` **DEAD** ·
`SIG_FRI34/43/64` (BLUE + the composite) · `SIG_BLUE` · `SIG_CCI` `SIG_CCI0R` `SIG_CCIB` ·
`SIG_RL` `SIG_RH` (fuchsia reclaim low/high) · `SIG_PP` (pre-pump: ≥2 VSA hits in 20 bars, 6-bar
cooldown) · `BO_UP` `BE_UP` `BX_UP` `SIG_BO_DN` `SIG_BX_DN` `SIG_BE_DN` (leaving the level of the
most recent L34 / L43 bar) · `VBO_UP` `SIG_VBO_DN`.

Full definitions of `L1…L6`, `L34`, `BLUE`, `squat/nosupply/nod/climax` are in
SIGNAL_REFERENCE §3.

### TZ codes — `signal_engine.py`
`T10 T11 T12 Z4 Z6 Z9 Z10 Z11 Z12` (the individual codes exported) · `SIG_T` `SIG_Z` `SIG_TZ`
(any) · `SIG_TZ2` `SIG_TZ3` (cycle approach) · `SIG_TZ_FLIP` · `SIG_BIAS_UP/DN` ·
`SIG_WK_UP/DN` (weak bull/bear).

### F / FLY / B / G
`SIG_F1…F11` + `SIG_ANY_F` · `F3 F4 F6 F11` (exported separately) ·
`SIG_FLY_ABCD/CD/BD/AD` (role sequence A→B→C→D on the priority codes, EMA-cross context) ·
`SIG_B1…B11` + `SIG_ANY_B` · `SIG_G1 G2 G4 G6 G11` · `SIG_GOG_PLUS`.

### Volume / absorption
`SIG_VA` · `SIG_SVS` / `SVS_RAW` · `SIG_ABS` · `SIG_CLM` (climax) · `SIG_SC` `SIG_BC` ·
`SIG_NS_VABS` `SIG_ND_VABS` (no-supply / no-demand) · `SIG_VOL_5X/10X/20X` (volume ≥ N× average) ·
`SQ` (squat) · `LOAD` · `W` · `CONS` / `SIG_CONSO` · `NO_VOL_EVENT`.

### P / D EMA families
`SIG_P2 P3 P50 P55 P66 P89` — price crossing up through EMA *n*;
`SIG_D2 D3 D50 D55 D66 D89` — crossing down; `SIG_ANY_P` / `SIG_ANY_D`.
> **Research:** `project_p_signal_no_edge` — P50/P55/P66 **alone have no edge**, and VB volume on
> a P signal is a fade trap. Only `project_p55_setup_refined` (1D+1H P55 + absorption-T + non-VB +
> prelude) survived, 5 of 6 years.

### Divergence / delta — `d_*` family
`SIG_FLP_UP` `SIG_ORG_UP` `SIG_DD_UP_RED` `SIG_D_UP_RED` `SIG_D_DN_GREEN` `SIG_DD_DN_GREEN` ·
`SIG_NS_DELTA` `SIG_ND_DELTA` **both DEAD**.

### CISD — `cisd_engine.py`
`SIG_CISD_CPLUS` (`++--`) · `SIG_CISD_CPLUS_MINUS` (`++-`) · `SIG_CISD_CPLUS_MM` (`-+-`).

### PARA — `para_engine.py`
`SIG_PARA_PREP` → `SIG_PARA_START` → `SIG_PARA_PLUS` → `SIG_PARA_RETEST`, a stateful cycle with a
campaign lock / rearm / reset. SPARSE here, 1.1-3.4 % universe-wide.
> `project_p_parabola_ride`: the edge is in the **tail with a 25 % trailing stop**, not at the
> start signal. Do not read `PARA_START` as an entry.

### Price / RSI gates
`PRICE_GT_20/50/89/200` and `PRICE_LT_*` (close vs EMA *n*) · `RSI_LE_35` · `RSI_GE_70` ·
`SIG_NOT_EXT` (not already extended).

### Misc / dead
`SIG_X1 X2 X1G X3` **all DEAD** · `SIG_3G` · `SIG_CD` `SIG_CA` `SIG_CW` · `SIG_SEQ_BCONT` ·
`SIG_BEST` `SIG_STRONG` `SIG_BEST_UP` `SIG_3UP` `SIG_FBO_UP/DN` `SIG_EB_UP/DN` `SIG_4BF_DN` ·
`4BF` · `SIG_260308` · `L88` · `UM` · `BUY_HERE` · `HILO_BUY` · `RTV` · `THREE_G` · `ROCKET` ·
`BOLL_BREAKOUT` · `ATR_BREAKOUT` **DEAD** · `ALREADY_EXTENDED_FLAG` **DEAD** ·
`CROSS_2PLUS/3PLUS/4PLUS` and `EARLY_E` **DEAD in this export** · `YF_SOURCE` **DEAD**.

> `project_rtv_v1` measured `RTV` as a sealed family, k=4 → **4/4 NULL**; `RTV|RS` dies in 2026.
> There is still an open item: `turbo_engine._calc_turbo_score` adds +3 for `rtv` unconditionally.

---

# BLOCK 7 — DERIVED STATS (cols 152-161) — ALL DEAD

`PCT_CHANGE_3D` · `PCT_CHANGE_5D` · `PCT_CHANGE_10D` · `PCT_FROM_20D_HIGH` ·
`PCT_FROM_20D_LOW` · `DIST_20D_HIGH` · `VOL_RATIO_20D` · `DOLLAR_VOLUME` · `GAP_PCT`

Trivially computable, genuinely useful, and **never filled** by the export path. This is the
cheapest repair in the file.

---

# BLOCK 8 — BETA (cols 162-168)

`backend/beta_engine.py`, version `2026-05-11-v2.2`. *"NEVER reads `ret_*`, `mfe_*`, `mae_*`
— no lookahead."*

| column | logic |
|---|---|
| `BETA_SETUP` | 0-60, structural quality |
| `BETA_MOMENTUM` | −5…50, momentum / regime (v2: weight +30 %) |
| `BETA_EXCESS` | ≥0 extension **penalty** (v2: weight −35 %) — DEAD here |
| `BETA_RAW` | pre-transform total |
| `BETA_SCORE` | 0-100 after a non-linear transform |
| `BETA_ZONE` | `ELITE · OPTIMAL · BUY · WATCH · BUILDING · SHORT_WATCH · NEUTRAL` |
| `BETA_AUTO_BUY` | strict multi-condition gate — DEAD here |

Regime points are per exchange. Two gates modify it: an EMA89 cross-up aligned with
WATCH/BUY/OPTIMAL multiplies by 1.1; an active D89 downgrades BUILDING → NEUTRAL. **FITTED.**

---

# BLOCK 9 — FORWARD OUTCOME COLUMNS (cols 169-188) — ⚠️ LABELS

`FWD_1D` `FWD_3D` `FWD_5D` `FWD_10D` · `MAX_HIGH_5D` `MAX_HIGH_10D` ·
`HIT_5PCT_5D` `HIT_10PCT_5D` `HIT_5PCT_10D` `HIT_10PCT_10D` ·
`BARS_TO_VBO` `BARS_TO_GOG` · `VBO_W5` `VBO_W10` `GOG_W5` `GOG_W10` ·
`RET_TO_NEXT_VBO_CLOSE/HIGH` `RET_TO_NEXT_GOG_CLOSE/HIGH`

**Every one of these is computed from bars after *t*.** They exist so an export can be scored
offline. They are empty in this file, which is the correct state — but the columns are in the
header, so anyone building a model from this CSV must drop them by name. They are the answer,
not the question.

---

# BLOCK 10 — WYCKOFF, PREBREAK, SWING (cols 327-342)

| column | logic | status |
|---|---|---|
| `ad_fresh`, `ad_cluster` | accumulation/distribution freshness. Recomputed from **raw** T/Z patterns, not from `t_sig` — the stored codes are priority-resolved and a bar that is raw-Z1G *and* raw-Z4 stores only Z4 | SAFE |
| `wyc_phase` | `MARKUP · MKDN · ACC_TR · DIST_TR · SPRING · SOS · UTAD` | SAFE |
| `wyc_spring` `wyc_sos` `wyc_in_tr` | Wyckoff V2 state machine `0 idle → SC → AR → ST → SPRING → SOS/JAC → LPS`. Pivot stages fire on the **confirmation bar**, `pivotLen` bars after the pivot — which is what makes them screen-safe | SAFE (sparse) |
| `wyc_sow` | **DEAD** — 0 of 8.9M bars | DEAD |
| `prebreak_score` `prebreak_ready` `prebreak_watch` | pre-breakout calibration | FITTED |
| `prebreak_prime` | fires on 0.0087 % — 777 bars of 8.9M | effectively DEAD |
| `pb_lvbo` `pb_wvf_confirm` `pb_stop_cause` `pb_macro_penalty` | prebreak components | SAFE |
| **`swing_type`** | Williams pivot label — **a pivot at *t* is confirmed by bars after *t*** and the label is nonetheless stored on row *t*. `prebreak_v2.py:24` records that these fields "topped the raw-lift charts precisely because they are look-ahead" | 🚨 **LOOKAHEAD — never use** |

---

# BLOCK 11 — SCORES AND CONFIRMATION (cols 343-379)

| column | logic | status |
|---|---|---|
| `ultra_score`, `ultra_score_band` | ULTRA v1 | FITTED |
| `ultra_score_v3`, `_band` | ULTRA v3. `project_ultra_score_v3`: the old score does not rank (ρ −0.004); v3 gets ρ +0.078, Q5−Q1 +6.74 pp — and `project_score_audit_v1` found it is **the only ranker of twenty**, working through its RS and cluster axes while its core is ≈ 0 | FITTED |
| `prebreak_v2` | frozen logistic regression: bias −1.07019, 28 hard-coded weights, two standardised continuous inputs (RSI µ 48.53 σ 13.12, change% µ −0.036 σ 4.78). Target was `mfe_20d ≥ 20 % AND fwd_10d ≥ 0`. Bands `<15 WATCH · 15-27 BUY · >27 HOT` — and **HOT has the highest breakout rate with a NEGATIVE mean fwd_20d** | FITTED |
| `prebreak_v3` | v3 (RTB phase weight removed 2026-07-09 after path-sim) | FITTED |
| `rev_buy`, `brk_buy` | the 🟢REV / 🔵BRK buy flags. `project_score_zones`: **LOW momentum scores predict better** | SAFE |
| `mtf_echo`, `mtf_score_conf`, `turn_echo_n`, `h4_rev_today`, `h1_rev_today` | multi-timeframe confirmation. `project_mtf_confirmation`: a 1D signal with **zero** 4H/1H/15m echo is negative; ≥1 confirmation flips it | SAFE |
| `fly_fresh` | bars since the last FLY | SAFE |
| `EDGES` | which validated edge masks fired — see SIGNAL_REFERENCE §6.1 | SAFE |
| `SEQ34`, `SEQ_CTX*`, `SEQ_ENS*`, `CONF*` | the sequence conditioner — sequences **qualify** a BUY signal (⤴/⤵/🎲) rather than generating one | SAFE |
| **`SEQ34_WIN`** | the sequence's historical win rate | 🚨 outcome statistic — **DEAD here, exclude by name** |
| **`EDGE_GOLD`** | derived from edge outcome statistics | 🚨 exclude |
| `L_SIG`, `RSI`, `CCI` | raw values | SAFE |
| `BUY_SCORE` | `compute_buy_score(prebreak_v2, rsi_14, vol_bucket)` — two-sided veto, RSI ≥ 60 EXT / < 28 KNIFE. **Inverted-U**: peak 39-46, ≥66 is −0.21 | FITTED |
| `PROFILE_SCORE`, `PROFILE_CAT` | ticker temperament (🏦⚡🎲🎰). `project_temperament_segmentation`: the architecture is universal, the membership is local | FITTED |
| `ATR_PCT` | ATR14 / close | SAFE |
| `TT_UP10_DAYS` `TT_UP10_HIT` `TT_UP25_DAYS` `TT_DN10_DAYS` | ATR time-to-target **forecast** from `project_atr_time_forecast` — `days(+X%) ≈ (X/ATR%)^p`, calibrated TRAIN≈TEST. A forecast from *t*, not a future observation | SAFE, but calibrated |

---

# BLOCK 12 — DIVERGENCE (cols 386-395)

`project_oscillator_divergence_reclaim` — three builds out of two NULL Pine scripts.

| column | logic |
|---|---|
| `DIV_BUY`, `DIV_DEEP`, `DIV_TOP` | the divergence buy flags (DEAD for OKLO) |
| `DIV_R_BULL_STAGE`, `DIV_R_BEAR_STAGE` | RSI divergence stage |
| `DIV_C_BULL_STAGE`, `DIV_C_BEAR_STAGE` | CCI divergence stage |
| `DIV_PIVOT_RSI`, `DIV_PIVOT_CCI` | the oscillator value at the pivot being compared |
| `DIV_RS_INTACT` | 🏆 RS gate — close/benchmark above its own EMA200 |

**The finding that matters:** the *zone* carries the edge, not the divergence. And `_near` was made
causal — an earlier version confirmed the pivot with future bars. Pivot-based divergence is the
classic place this leaks; here it is handled.

---

# BLOCK 13 — PHYSICS (cols 396-415)

`backend/studio/bar_physics.py`, ported from Pine `260814_TZ_RC` + `260814_PHYS`. Each quantity is
stored as a **class** *and* a **raw** value, so thresholds can be re-cut without recomputing 132M
bars. All **SAFE** — the one pivot rule is shifted so no level is visible before Pine would see it.

| column | formula | classes |
|---|---|---|
| `PHYS_R` / `PHYS_R_RAW` | `(volume / SMA₂₀volume) ÷ max(\|Δclose\|/ATR, 0.05)` — **effort over result**, normalised on both sides so a $400 mega-cap and an $8 name compare | `RA` > 2× its 30-bar median (absorbing) · `RF` < 0.6× (frictionless) · `RN` |
| `PHYS_REGIME` | close vs EMA20 | `U` / `D` |
| `PHYS_C` / `_RAW` | Coulomb — charge at confirmed pivot levels (pivot 3-3, ≤12 levels, carrying the pivot bar's volume) | `C2` > 2× median · `C0` < 0.5× · `C1` |
| `PHYS_H` / `_RAW` | binary Shannon entropy of "which side of EMA20", 20 bars | `H0` < 0.60 · `H2` > 0.95 · `H1` |
| `PHYS_M` / `_RAW` | `\|EMA20 − EMA20[10]\| / (ATR×10)` × (20-bar ÷ 100-bar dollar volume) | `M2` > 1.8× median · `M0` < 0.4× · `M1` |
| `PHYS_E` / `_RAW` / `_RELEASE` | mean ATR compression over 15 bars | `E2` loaded · `E0` slack · `E1`; **★** appended on release (loaded yesterday AND today's range > 1.6 × base ATR) |
| `PHYS_K` / `_K_X` | `(close − EMA20) / ATR` | `K0` < 1 · `K1U/K1D` ≥ 1 · `K2U/K2D` ≥ 3 |
| `PHYS_S` / `_S_NET` | how many of EMA9>20, 20>50, 50>200 agree | `S0…S3U` up-aligned · `S…D` down |
| `PHYS_GAP_TRUE` | **TRUE gap** — measured from the previous **high or low**, not the previous close. Measuring from the close reports overnight displacement and inflates the class ~2× and never under | `G1` < 0.2 ATR · `G2` < 0.5 · `G3` above |
| `PHYS_AD` | accumulation/distribution freshness from raw patterns | |
| `PHYS_WYC` | the Wyckoff phase as physics sees it | |
| `PHYS_LINE6` | `R·regime·C·H` concatenated | |

> **Research: CONTEXT, not ranker.** `project_score_audit_v1`, k=20 → 0 RANKER / 1 ANTI /
> 19 CONTEXT. These letters describe a bar well and do not order forward returns.

---

# BLOCK 14 — THE DISPLAY LAYERS (cols 416-476)

Nightly parquets built by `update_all.sh`. **All DESCRIPTIVE, none is a ranking input.**

## L-BAL / UDN★ (416-422) — `lbal_build.py`
15-minute L-label balance per session. `LBAL_UDN` / `LBAL_UDN_C` = the two lines' verdict
(`U`/`D`); `LBAL_COLOUR` = the daily candle (`close > open → GREEN`); `LBAL_NPOS` / `NNEG` = label
counts. The **★** mark = the lines disagree with the candle: `U`+RED or `D`+GREEN.
A ★ on a doji is refused by the builder's own gate.

## L-VX (423-428) — `lvx_signals.parquet`
Daily L34/L46 graded by how much intraday support it has: `LVX_TIER` 0-4 → `V · VL · VH · VX`,
with `LVX_LV15` / `LVX_LV1H` the 15m and 1H levels.
> **VETO:** the weakest grade `V` = **−14.88 / −11.25** day-clustered (`project_ladder_v1`).

## OVD (429-433) — `ovdmap_signals.parquet`
Opening-vs-close volume displacement. `OVD_TOKENS` ∈ `OB · RC · CD · HO · NM?`;
`OVD_RV_O60` / `RV_C60` = opening / closing relative volume over 60 days;
`OVD_RECLAIM60`; `OVD_HANDOFF60`.

## VOL7 (434-440) — `vol7_signals.parquet`
Seven-level volume regime. `VOL7_RATIO` = volume / its median; `VOL7_MR` = the M-level 0-6;
`VOL7_SG` = the σ-level; `VOL7_CONS` = `Σ+` consolidation; `VOL7_TRANS` = level jump;
`VOL7_MARKS` = `Σ+ ▲+2 ▼−2` …
> **VETO:** `M6` (≥5× median) = **−11.97 / −12.11** day-clustered over 585/322 days.
> The middle levels are ≈ 0 — this is *avoid the extreme*, not *buy the middle*.

## RANK (441-444)
`RANK_PCT` · `RANK_EDGE` · `RANK_FAM` · `RANK_N` — 🏅 rank over edge fires.
> `project_rank_v1`, sealed k=3: the plain **state table wins**; adding display layers or scores
> **lowers** the top-10 head despite a higher IC. ⚠️ Derived from edge outcome statistics —
> exclude from modelling.

## SHAPE × CONTEXT (445-476) — `shape_ctx_build.py`
Port of Pine `260910_SHAPE_CTX`. Seven body-nest shapes under a fixed **display priority**
`MTH > CL4 > MID > EXP > CON > LST > WRP`; `SHAPE_MID/EXP/CON/LAST/WRAP/COIL/MOTH` are the raw
membership flags, `SHAPE_CODE` / `SHAPE_LABEL` the resolved one.

| column | meaning |
|---|---|
| `SHAPE_ARROW` | ↑/↓ direction on the swallow shapes |
| `SHAPE_GRADE` | effort grade 0-2 |
| `SHAPE_VR` | volume ratio |
| `SHAPE_ABSORB` `SHAPE_DRY` `SHAPE_SWEET` | absorption / dryness / sweet-spot flags |
| `SHAPE_POS20` | position in the 20-bar range |
| `SHAPE_FLOOR` `SHAPE_TOUCHES` `SHAPE_KEY` | 📍 at the floor · how many times the level was tested · 🧱 it is a real key level |
| `SHAPE_RS` | 🏆 RS intact |
| `SHAPE_RSI` `SHAPE_BAND` | RSI and its band (`<35 · 35-50 · 50-60 · ≥60`) |
| `SHAPE_KNIFE_VETO` | ⛔ falling-knife veto (DEAD here) |
| `SHAPE_LSTUP_VETO` | ⛔ the one validated veto — `LST↑` = **−0.63 / −0.47** in both windows, DSR_neg 0.998 |
| `SHAPE_DIR_UP` `SHAPE_DIR_DN` | direction |
| `SHAPE_CL_FAM` `SHAPE_CL_BARS` `SHAPE_BY_FAM` `SHAPE_BY_DEN` | the two cluster axes: family diversity 🎯 and bar density 🔁 |
| `SHAPE_MARK` `SHAPE_LEGS` | the printed marks 🎯🔁 and legs 📍🧱🏆 |

> **Research — read this before using any of it.** `project_shape_cluster_v1`: **NO_CLUSTER is
> BEST and 🎯🔁 is WORST, monotonically.** The marks are labelled on the chart precisely so 🎯🔁
> cannot be misread as strength. `SHAPE_TOUCHES` ranked highly in one early model and has **not**
> been validated with train-only thresholds — treat it as a hypothesis.

---

# APPENDIX — WHAT TO DROP BEFORE MODELLING

```
LOOKAHEAD   swing_type
            FWD_* MAX_HIGH_* HIT_* BARS_TO_* VBO_W* GOG_W* RET_TO_NEXT_*
            SEQ34_WIN  EDGE_GOLD  RANK_*
FITTED      turbo_score FINAL_BULL_SCORE and all sub-scores · ultra_score(_v3) · buy_score
            prebreak_v2/v3 · beta_* · GOG_SCORE · PROFILE_SCORE · rtb_total
DEAD        the 114 listed at the top — 20 MDL_*, the GOG sub-score block,
            the derived-stat block, SIG_X1/X2/X1G/X3, SIG_L2L4, SIG_NS_DELTA,
            SIG_ND_DELTA, wyc_sow, ALREADY_EXTENDED_FLAG, ATR_BREAKOUT
```

That leaves roughly **300 genuinely live, genuinely varying columns** — which is the real size of
the signal universe, not 476.
