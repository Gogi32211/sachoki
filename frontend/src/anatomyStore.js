// anatomyStore.js — shared Bottom-Anatomy (▽△) latest-bar verdict map (2026-07-23).
// ONE /api/anatomy-latest call fills the whole-universe map (server-cached 1h); both the
// Ultra grid cell AND the sort comparator read it. 🔻 structural bottom / 🔻💪 durable
// (RS-intact) / 🔺 continues. A DETECTOR (1.37× lift / 76% recall), not a trade signal.
let map = null
let ts = 0
let pending = null
const subs = new Set()

export function requestAnatomy() {
  if (map && Date.now() - ts < 300000) return Promise.resolve(map)
  if (pending) return pending
  pending = fetch('/api/anatomy-latest')
    .then(r => r.json())
    .then(m => { map = m || {}; ts = Date.now(); pending = null; subs.forEach(fn => fn()); return map })
    .catch(() => { pending = null; return {} })
  return pending
}

export function getAnatomy(ticker) { return map ? (map[ticker] || null) : null }

export function subscribeAnatomy(fn) { subs.add(fn); return () => subs.delete(fn) }

// sort key: 🔻💪 (durable) → 🔻 (structural) → 🌀 (shakeout/spring) → 🔺 (markup) → none.
export function anatSortVal(ticker) {
  const a = map && map[ticker]
  if (!a) return -1
  if (a.v === 'rev') return (a.rs ? 300 : 200) + (a.s || 0)
  if (a.v === 'shake') return 100 + (a.s || 0)
  if (a.v === 'cont') return (a.s || 0)
  return 0
}
