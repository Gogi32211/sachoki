// setupsJournal — a lightweight, date-stamped log of setups the user tracks
// (separate from the AI paper-trading journal). localStorage-backed.
const KEY = 'sachoki_setups_journal'

export function sjLoad() {
  try { return JSON.parse(localStorage.getItem(KEY)) || [] } catch { return [] }
}
export function sjSave(items) {
  try { localStorage.setItem(KEY, JSON.stringify(items)) } catch { /* quota */ }
}
const today = () => new Date().toISOString().slice(0, 10)

/** add an entry; deduped per (ticker, day). returns true if newly added. */
export function sjAdd(entry, source = 'manual') {
  const items = sjLoad()
  const date = today()
  if (items.find(x => x.ticker === entry.ticker && x.date === date)) return false
  items.unshift({
    ticker: entry.ticker, sequence: entry.sequence || '', prob_up: entry.prob_up ?? null,
    score: entry.score ?? null, last_price: entry.last_price ?? null,
    date, addedAt: new Date().toISOString(), source,
  })
  sjSave(items.slice(0, 800))
  return true
}
export function sjRemove(ticker, date) {
  sjSave(sjLoad().filter(x => !(x.ticker === ticker && x.date === date)))
}
export function sjClear() { sjSave([]) }

/** grouped by date, newest day first. */
export function sjByDate() {
  const g = {}
  for (const it of sjLoad()) { (g[it.date] = g[it.date] || []).push(it) }
  return Object.entries(g).sort((a, b) => b[0].localeCompare(a[0]))
}
export function sjCount() { return sjLoad().length }

/** Download a TradingView-importable watchlist .txt.
 *  `sections` = array of {name, tickers:[]} → '###Name,\nA,B,C' blocks,
 *  or a flat array of tickers → comma-separated. */
export function downloadTV(filename, sections) {
  const uniq = (arr) => [...new Set(arr.filter(Boolean))]
  let text
  if (Array.isArray(sections) && (sections.length === 0 || typeof sections[0] === 'string')) {
    text = uniq(sections).join(',')
  } else {
    text = (sections || [])
      .filter(s => (s.tickers || []).length)
      .map(s => `###${(s.name || 'Setups').replace(/[\n,]/g, ' ')},\n${uniq(s.tickers).join(',')}`)
      .join('\n')
  }
  const blob = new Blob([text], { type: 'text/plain;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url; a.download = filename
  document.body.appendChild(a); a.click(); a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 1000)
}
