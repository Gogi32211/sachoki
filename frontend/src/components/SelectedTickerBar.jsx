import SignalChipList from './SignalChipList'
import { collectSignals } from './ScannerDataGrid'

// Compact info + signal strip shown ABOVE the global chart for the row the user
// clicked in a screener. Mirrors the chips/stats in the grid row so the signals
// and the chart are visible at the same time. Renders nothing without a row.
const fmt = (v, d = 2) => (v == null || isNaN(v)) ? '—' : Number(v).toFixed(d)

export default function SelectedTickerBar({ row }) {
  if (!row) return null
  const sigs  = collectSignals(row).map(s => s.label)
  const chg   = row.change_pct ?? 0
  const score = row.turbo_score ?? row.score ?? 0
  const rsi   = row.rsi
  const cci   = row.cci

  return (
    <div className="flex items-center gap-x-3 gap-y-1 flex-wrap bg-md-surface-con rounded-xl border border-md-outline-var px-4 py-2 text-xs mb-2">
      <span className="font-mono font-bold text-blue-300 text-sm shrink-0">{row.ticker}</span>
      {row.vol_bucket && <span className="text-md-on-surface-var shrink-0">{row.vol_bucket}</span>}
      {row.tz_sig && <span className="font-mono font-semibold text-md-on-surface shrink-0">{row.tz_sig}</span>}
      <span className="font-mono text-md-on-surface shrink-0">${fmt(row.last_price)}</span>
      <span className={`font-mono shrink-0 ${chg >= 0 ? 'text-lime-400' : 'text-red-400'}`}>
        {chg >= 0 ? '+' : ''}{fmt(chg)}%
      </span>
      <span className="text-md-on-surface-var shrink-0">
        RSI <span className={rsi <= 35 ? 'text-lime-400' : rsi >= 70 ? 'text-red-400' : 'text-md-on-surface'}>{fmt(rsi, 0)}</span>
      </span>
      <span className="text-md-on-surface-var shrink-0">
        CCI <span className={cci >= 100 ? 'text-lime-400' : cci <= -100 ? 'text-red-400' : 'text-md-on-surface'}>{fmt(cci, 0)}</span>
      </span>
      <span className="text-md-on-surface-var shrink-0">
        Score <span className="font-semibold text-md-on-surface">{fmt(score, 1)}</span>
      </span>
      {sigs.length > 0 && (
        <div className="min-w-0 flex-1">
          <SignalChipList signals={sigs} mode="table" />
        </div>
      )}
    </div>
  )
}
