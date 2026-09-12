// ★ L-BAL counting mode — ONE persisted preference shared by the chart line, the Superchart
// UDN★ row and the Ultra chips / Signals badges (user, 2026-09-06: "V as the default, the
// every-bar count as an additional switch").
//
//   'V'   — only 15m bars with volume > SMA20 are counted (the TradingView "Count only
//           volume-confirmed lower-TF bars" setting; labels there read L34V / L46V). Default.
//   'ALL' — every labelled 15m bar (the Pine default).
//
// The backend serves both sets on every row: lbal_* (V) and lbal_all_* (ALL). `lbalPick`
// reads the field for the current mode, so no surface ever re-fetches on a switch.
import { useEffect, useState } from 'react'

const KEY = 'sachoki_lbal_mode_v1'
let current = (() => { try { return localStorage.getItem(KEY) === 'ALL' ? 'ALL' : 'V' } catch { return 'V' } })()
const listeners = new Set()

export function getLbalMode() { return current }

export function setLbalMode(m) {
  current = m === 'ALL' ? 'ALL' : 'V'
  try { localStorage.setItem(KEY, current) } catch { /* private mode */ }
  for (const fn of listeners) fn(current)
}

export function toggleLbalMode() { setLbalMode(current === 'V' ? 'ALL' : 'V') }

// field of a row / bar / mark for the current mode: lbalPick(r, 'marks') → r.lbal_marks | r.lbal_all_marks
export function lbalPick(r, base, mode = current) {
  if (!r) return undefined
  return mode === 'ALL' ? r['lbal_all_' + base] : r['lbal_' + base]
}

// subscribe a component to the shared mode
export function useLbalMode() {
  const [mode, setMode] = useState(current)
  useEffect(() => { listeners.add(setMode); return () => listeners.delete(setMode) }, [])
  return [mode, setLbalMode]
}

export const LBAL_MODE_TITLE = {
  V:   'V mode — only 15m bars with volume > SMA20 are counted, as on the TradingView chart (L34V / L46V). Click for all bars.',
  ALL: 'ALL mode — every labelled 15m bar is counted (the Pine default). Click for volume-confirmed bars only (V).',
}

// tiny toggle chip, usable inside any surface (chart toolbar, Superchart row label, Ultra divider)
export function LbalModeToggle({ className = '' }) {
  const [mode] = useLbalMode()
  return (
    <button type="button"
      onClick={(e) => { e.stopPropagation(); toggleLbalMode() }}
      title={LBAL_MODE_TITLE[mode]}
      className={`px-1 py-px rounded border font-mono text-[10px] leading-none select-none ${
        mode === 'V'
          ? 'bg-sky-900/60 text-sky-200 border-sky-500'
          : 'bg-md-surface-high text-md-on-surface-var border-white/10 hover:text-white'} ${className}`}>
      {mode === 'V' ? 'V' : 'all'}
    </button>
  )
}
