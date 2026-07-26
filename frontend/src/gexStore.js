// gexStore.js — shared 💠 GEX (options gamma) lazy store (2026-07-22).
// One module-level cache both the Ultra grid cell AND the sort comparator read, so
// GEX becomes sortable even though it's fetched async per ticker. Debounced batch,
// server-cached 10min. UNVALIDATED context (path-sim pending) — a lean, not a signal.
const cache = {}            // ticker → {available, regime, lean, lean_score, atm_iv, ...} | null (in-flight)
const pending = new Set()
const subs = new Set()
let timer = null

function flush() {
  const batch = [...pending].slice(0, 40)
  pending.clear()
  if (!batch.length) return
  batch.forEach(t => { if (cache[t] === undefined) cache[t] = null })
  fetch(`/api/gex-batch?tickers=${batch.join(',')}&cap=40`).then(r => r.json())
    .then(d => { Object.assign(cache, d.gex || {}); subs.forEach(fn => fn()) })
    .catch(() => {})
  // keep draining if the queue is still long (sort-all triggers many)
  if (pending.size) { clearTimeout(timer); timer = setTimeout(flush, 400) }
}

export function requestGex(ticker) {
  if (!ticker || cache[ticker] !== undefined) return
  pending.add(ticker)
  clearTimeout(timer)
  timer = setTimeout(flush, 350)
}

export function requestGexBulk(tickers) {
  let added = false
  for (const t of tickers) {
    if (t && cache[t] === undefined) { pending.add(t); added = true }
  }
  if (added) { clearTimeout(timer); timer = setTimeout(flush, 200) }
}

export function getGex(ticker) {
  return cache[ticker]
}

// Sort value: optionable names cluster ABOVE non-optionable regardless of lean data
// (base 100), then ordered by directional lean_score within that band. Non-optionable
// / in-flight sink to −Infinity. Available-first makes the column group the tradeable
// names even when after-hours greeks are thin (lean_score null → treated as 0).
export function gexSortVal(ticker) {
  const g = cache[ticker]
  if (!g || !g.available) return -Infinity
  return 100 + (typeof g.lean_score === 'number' ? g.lean_score : 0)
}

// ⚖️ VRP sort (2026-07-26): sort by IV/realized-vol ratio. Desc = EVENT-PRICED first,
// asc = COMPLACENT first. Missing/non-optionable sink to −Infinity.
export function vrpSortVal(ticker) {
  const g = cache[ticker]
  if (!g || !g.available || g.vrp == null) return -Infinity
  return g.vrp
}

export function subscribeGex(fn) {
  subs.add(fn)
  return () => subs.delete(fn)
}
