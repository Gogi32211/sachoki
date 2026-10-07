// V4 SCORE — user-directed, per-signal WEIGHTS (2026-09-23, revised from the flat first pass).
// Every signal in the catalog starts at 0 points (src/lib/v4Weights.js); the user is filling them
// in incrementally, key by key. This module only sums whatever weights it is handed — it carries
// no opinion of its own about which signal matters, and no signal is a claimed ranker until the
// user has assigned it a weight through the book's own analysis standard (never assumed here).
//
// THE CATALOG. `SIG_GROUPS` (exported from UltraScanPanel.jsx) is the Ultra screener's OWN filter
// list — every chip a user can currently filter Ultra by, across every signal family in the app
// (T/Z, L-code, WLNBB, physics, CISD, Wyckoff, combo, F/G, wick, VABS, macro/sector, seq-edges,
// MTF-EMA, SHAPE × CONTEXT, PRICE × VOLUME, L-BAL, L-VX, OVD, VOL7, prebreak, edge fires…). Reusing
// it — rather than hand-picking a second list — means V4 answers exactly "of the filters I could
// already select, which fire on this ticker, and what has the user said each one is worth," with
// one definition, one place.
//
// THE EVALUATION mirrors the filter code's own semantics (UltraScanPanel.jsx's `selSigs` check):
// a `custom` predicate wins if present; otherwise an age-tracked key needs age < lookbackN;
// otherwise a bare truthy field read. lookbackN defaults to 1 — "fired today" — which is also the
// only sensible reading on Superchart, where each row already IS one specific day.
//
// ⚠️ KNOWN PARITY GAP, not yet measured. SIG_GROUPS was written against the ULTRA SCAN row shape.
// Superchart's per-bar object (built from /api/bar_signals + the merged display maps) overlaps
// heavily but is not field-identical: a chunk of "base" families (VABS, F/G, wick, combo/2809,
// delta/CISD/PARA, ULTRA v2 — everything the backend's own `_UI_KEY_TO_DB_COL` alias table exists
// to bridge, see studio/ultra_db_scan.py) reference UI-aliased keys like `bias_up` / `vol_spike_5x`
// that the raw bar_signals payload only carries under their `sig_*` names. Those predicates will
// under-fire on Superchart until that alias table is ported or applied server-side to bar_signals
// too — a real follow-up, not done here. The newer display layers (SHAPE × CONTEXT, PRICE × VOLUME,
// L-BAL, L-VX, OVD, VOL7) share ONE backend source for both surfaces and are NOT affected by this.

/**
 * The SIG_GROUPS entries that fire on `row`, evaluated with the same rule the Ultra filter UI
 * uses. Never throws — a predicate that errors on an unexpected row shape (e.g. a Superchart bar
 * missing a field a `custom` closure assumes) is treated as "did not fire," not as a crash.
 */
export function v4Fired(SIG_GROUPS, row, lookbackN = 1) {
  let ages = row._ages
  if (!ages && row.sig_ages) {
    try { ages = JSON.parse(row.sig_ages) } catch { ages = {} }
  }
  ages = ages || {}
  const fired = []
  for (const sig of SIG_GROUPS) {
    if (sig.divider) continue
    let on = false
    try {
      if (sig.custom) on = !!sig.custom(row, lookbackN)
      else if (sig.key in ages) on = ages[sig.key] < lookbackN
      else on = !!row[sig.key]
    } catch { on = false }
    if (on) fired.push(sig)
  }
  return fired
}

/** Sum of `weights[key] ?? 0` over every fired signal. Pure arithmetic on top of v4Fired — kept
 * separate so callers that also want the fired list (for a tooltip) don't run the catalog twice.
 * A signal missing from `weights` entirely (not yet reviewed) contributes 0, same as one the user
 * has explicitly set to 0 — the map in v4Weights.js lists all 402 so that distinction never
 * matters in practice, but the function does not assume the map is complete. */
export function v4Score(SIG_GROUPS, weights, row, lookbackN = 1) {
  const fired = v4Fired(SIG_GROUPS, row, lookbackN)
  let score = 0
  for (const sig of fired) score += weights[sig.key] ?? 0
  return score
}

/** fired signals as "label(weight)" strings, for a tooltip — lets the user see what fired on a
 * ticker even while most weights are still 0, without a second pass over the catalog. */
export function v4FiredLabels(SIG_GROUPS, weights, row, lookbackN = 1) {
  return v4Fired(SIG_GROUPS, row, lookbackN)
    .filter(sig => sig.label)
    .map(sig => `${sig.label}(${weights[sig.key] ?? 0})`)
}

/** V4 from STORED fired keys (data/v4_signals.parquet via /api/studio/v4-marks or /v4-latest) — the single
 * source the Superchart row, the CSV and Ultra all read for a bar the nightly store already holds, so the three
 * show the same number by construction (2026-10-07, user: "ertnairad iyos"). Labels follow catalog order and
 * the same "label(weight)" format as v4FiredLabels. */
export function v4FromKeys(SIG_GROUPS, weights, keys) {
  const set = new Set(keys || [])
  let score = 0
  const labels = []
  for (const sig of SIG_GROUPS) {
    if (sig.divider || !set.has(sig.key)) continue
    score += weights[sig.key] ?? 0
    if (sig.label) labels.push(`${sig.label}(${weights[sig.key] ?? 0})`)
  }
  return { score, labels, n: set.size }
}

