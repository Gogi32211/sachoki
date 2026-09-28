# V4_REWEIGHT — Phase 1: history + census (no outcomes looked at)

**Date** 2026-09-27 · the user asked for a fresh, data-only re-weighting of V4 ("ki daiwye").
Phase 1 builds the per-bar history of every V4 catalog key and counts it. **No forward return was computed.**

## History
- **Universe.** S&P 500 (the `sp500` rows of the daily store): 792,983 ticker-days, 620 tickers, 2021-05-26 → 2026-09-25.
- **How the keys are evaluated.** Every key is evaluated by the app's own catalog code (V4_ALL_GROUPS + v4Fired, bundled from the current frontend source), not by a re-typed copy.
- **Rows.** They are built from the daily store plus the app's own `_to_ui()` of every display store: L-BAL, L-VX, OVD, PV, SHAPE, VOL7, VOL_ECHO. On top of that come anatomy, the 4H/1H REV store and the EDGE frame.

## Parity with the live app (2026-09-25, 500 tickers)
The app's own Ultra rows for the last session were run through the same catalog and compared key by key.

**Two pipeline defects were found and fixed:**
1. **Dependency probing missed fields behind early returns** (e.g. `l_sig !== 'L34'` hid `close`/`open`). The red-L34, TRIPLE and SHAPE keys never fired. Fields are now also read from each function's source.
2. **EDGE semantics.** Ultra lists fires from the last 5 calendar days (`CODE·Nd`) and includes MINED_DISPLAY. History had used same-day only.
   - After the fix, all 22 EDGE keys match the app exactly (0 mismatches).
   - Separately noted: the suffix field the app uses is `composite_full_suffix`, now used here as well.

**Remaining differences are expected:**
- **Fields the Ultra frontend merges client-side** (anatomy, body-wick, FLY-fresh, ctx) are present in history but absent from the backend payload.
- **4H/1H REV-today:** the store ends 2026-09-18. This affects the right edge only; training rows end ≈ 2026-08-27.
- **be/bo/bx_dn** are recomputed live on the last bar.

## Census (627 catalog keys)

| status | keys |
|---|---|
| **usable** (≥ 300 rows, ≤ 95 % of rows) | **472** |
| no history — live-only engines (SMX, MX, RGTI, turbo, seq/MTF live) | 66 |
| rare (< 300 rows) | 35 |
| never fires in history (incl. logically impossible VOL7 M·σ pairs, `atr_brk`/`LATE` never populated in the DB) | 31 |
| PT family — frozen research preview, by design not a ranking input | 17 |
| near-constant (> 95 %) | 3 |
| score overlays (RANK / CONF / ENS) — circular as V4 inputs | 3 |

- **Near-duplicates** (Jaccard ≥ 0.8): 30 groups covering 69 keys → **433 independent features**.
  - These are real properties, not bugs. L-VX grade chips are cumulative "at least this tier". The four FLY types co-fire 22-24 % of rows in the DB itself.
- **Frequency of usable keys:**

| share of rows | keys |
|---|---|
| < 0.1 % | 21 |
| 0.1-1 % | 92 |
| 1-5 % | 169 |
| 5-20 % | 106 |
| 20-50 % | 72 |
| 50-95 % | 12 |

- **Coverage by year:** 450 of 472 are present (≥ 30 rows) in all 6 years.

Artifacts (session scratchpad `v4hist/`): `build_rows2.py`, `runner2.mjs`, `deps_all.json`, `rows2.ndjson`, `fired.ndjson`, `X.npz` + `X_index.parquet` + `X_keys.json` (the key matrix), `census.csv`, `dup_groups.json`, `app_fired.ndjson`.
