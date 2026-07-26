import { useEffect, useState } from 'react'
import { api } from '../api'
import CodeCandleChart from './CodeCandleChart'

const fmt = (v, d = 2) => (v == null || isNaN(v)) ? '—' : Number(v).toFixed(d)

/**
 * Lightweight hover chart popup — same look/behaviour as the Ultra screener mini-chart.
 * Generic: only needs a ticker + screen position; price/changePct/rsi/sub are optional.
 * Positioned next to the hovered ticker name, flips left/up near viewport edges.
 */
export default function MiniChartPopup({ ticker, tf = '1d', pos, price, changePct, rsi, sub }) {
  const [info, setInfo] = useState(null)
  useEffect(() => { let dead = false; api.tickerInfo(ticker).then(d => { if (!dead) setInfo(d) }).catch(() => {}); return () => { dead = true } }, [ticker])

  const CHART_W = 780, CHART_H = 380, POPUP_W = 820, POPUP_H = 520
  const vw = window.innerWidth, vh = window.innerHeight
  let left = pos.x + 16
  if (left + POPUP_W > vw - 8) left = pos.x - POPUP_W - 8
  if (left < 8) left = 8
  let top = pos.y + 20
  if (top + POPUP_H > vh - 8) top = vh - POPUP_H - 8
  if (top < 8) top = 8
  const chg = changePct ?? 0

  return (
    <div className="fixed z-50 bg-md-surface-con border border-md-outline-var rounded-lg shadow-2xl text-xs text-md-on-surface pointer-events-none"
      style={{ left, top, width: POPUP_W }}>
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-white/[0.07]">
        <div className="flex items-center gap-3 min-w-0">
          <span className="font-mono font-bold text-blue-300 text-base shrink-0">{ticker}</span>
          {sub && <span className="font-mono text-sm text-violet-300 shrink-0">{sub}</span>}
          {info?.name && info.name !== ticker && <span className="text-md-on-surface text-sm truncate">{info.name}</span>}
          {info?.sector && <span className="text-md-on-surface-var text-xs shrink-0 bg-md-surface-high px-1.5 py-0.5 rounded">{info.sector}</span>}
        </div>
        <div className="text-right shrink-0 ml-3">
          {price != null && <span className="font-mono text-md-on-surface text-base">${fmt(price)}</span>}
          {changePct != null && <span className={`ml-2 font-mono text-sm ${chg >= 0 ? 'text-lime-400' : 'text-red-400'}`}>{chg >= 0 ? '+' : ''}{fmt(chg)}%</span>}
        </div>
      </div>
      {/* Stats */}
      {rsi != null && (
        <div className="flex items-center gap-4 px-4 py-2 border-b border-white/[0.07] text-md-on-surface-var">
          <span>RSI <span className={rsi <= 35 ? 'text-lime-400' : rsi >= 70 ? 'text-red-400' : 'text-md-on-surface'}>{fmt(rsi, 0)}</span></span>
        </div>
      )}
      {/* Chart */}
      <div style={{ width: CHART_W }}>
        <CodeCandleChart bare codes={false} ticker={ticker} tf={tf} interactive={false} height={CHART_H} />
      </div>
    </div>
  )
}
