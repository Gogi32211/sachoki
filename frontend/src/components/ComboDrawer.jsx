import { useState, useEffect } from 'react'
import { api } from '../api'
import TickerDrawer from './TickerDrawer'

const fmtPct = (v) => v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(2)}%`
const CAP_CLS = { mega:'text-amber-200', large:'text-emerald-300', mid:'text-sky-300',
                  small:'text-yellow-400', micro:'text-rose-400', unknown:'text-gray-500' }

export default function ComboDrawer({ combo, edgeInfo, onClose }) {
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)
  const [drawerTk, setDrawerTk] = useState(null)

  useEffect(() => {
    setData(null); setErr(null)
    if (!combo) return
    api.qlibComboActive(combo).then(setData).catch(e => setErr(String(e)))
  }, [combo])

  if (!combo) return null

  const rows = data?.rows || []
  const atoms = combo.split(',')

  return (
    <>
      <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />
      <div className="fixed inset-y-0 right-0 w-full max-w-[680px] z-50 bg-md-surface border-l border-white/10 shadow-2xl overflow-y-auto">
        <div className="sticky top-0 bg-md-surface/95 backdrop-blur border-b border-white/10 px-4 py-3 flex items-center gap-3">
          <div>
            <div className="text-xs text-md-on-surface-var">Combo · {atoms.length} predicates</div>
            <div className="text-base font-bold font-mono text-violet-300 break-all">{combo}</div>
            {edgeInfo && <div className="text-xs text-md-on-surface-var mt-0.5">
              OOS edge <span className={edgeInfo.edge>=0?'text-emerald-400':'text-rose-400'}>{edgeInfo.edge>=0?'+':''}{edgeInfo.edge}%</span>
              {' · '}n_oos {edgeInfo.n_oos}{' · H='}{edgeInfo.horizon}d{' · '}bonf-p {edgeInfo.bonf?.toExponential?.(1)}</div>}
          </div>
          <button onClick={onClose} className="ml-auto px-2 py-1 rounded hover:bg-white/10 text-md-on-surface-var text-lg">✕</button>
        </div>

        <div className="p-4 space-y-3">
          <div className="text-xs text-md-on-surface-var">
            Тикеры, удовлетворяющие <b>всем</b> предикатам комбо <b>прямо сейчас</b> (последний дневной бар {data?.as_of || '…'}).
          </div>

          {err && <div className="p-2 rounded bg-rose-900/40 text-rose-200 text-xs font-mono">{err}</div>}
          {!data ? <div className="text-sm text-md-on-surface-var">Загрузка…</div> :
           !rows.length ? <div className="text-sm text-md-on-surface-var italic p-3 rounded bg-md-surface-high border border-white/10">Сегодня (бар {data.as_of}) ни один тикер не удовлетворяет полному набору предикатов. Combo в каталоге — но активного триггера нет.</div> :
          <div>
            <div className="text-xs text-md-on-surface-var mb-1">{rows.length} тикеров активны</div>
            <table className="w-full text-xs">
              <thead><tr className="border-b border-white/10 text-md-on-surface-var">
                <th className="text-left px-2 py-1">Ticker</th>
                <th className="text-left px-2 py-1">Cap</th>
                <th className="text-left px-2 py-1">Sector</th>
                <th className="text-right px-2 py-1">Price</th>
                <th className="text-right px-2 py-1">Day Δ</th>
                <th className="text-right px-2 py-1">V3</th>
                <th className="text-right px-2 py-1">RSI</th>
                <th className="text-left px-2 py-1">tz/phase</th>
              </tr></thead>
              <tbody>{rows.map(r => (
                <tr key={r.ticker} className="border-b border-white/5 hover:bg-white/5 cursor-pointer"
                    onClick={() => setDrawerTk(r.ticker)}>
                  <td className="px-2 py-1 font-bold text-emerald-300">{r.ticker}</td>
                  <td className={`px-2 py-1 font-mono ${CAP_CLS[r.mcap_bucket]||'text-gray-400'}`}>{r.mcap_bucket}</td>
                  <td className="px-2 py-1 text-md-on-surface-var">{r.sector || '—'}</td>
                  <td className="px-2 py-1 text-right font-mono">${r.close?.toFixed(2)}</td>
                  <td className={`px-2 py-1 text-right font-mono ${(r.change_pct||0)>=0?'text-emerald-400':'text-rose-400'}`}>{fmtPct(r.change_pct)}</td>
                  <td className="px-2 py-1 text-right font-mono">{r.v3}</td>
                  <td className="px-2 py-1 text-right font-mono">{r.rsi ?? '—'}</td>
                  <td className="px-2 py-1 font-mono text-md-on-surface-var">{r.tz}{r.phase?`/${r.phase}`:''}</td>
                </tr>))}
              </tbody>
            </table>
            <div className="mt-2 text-[11px] text-md-on-surface-var italic">Клик по тикеру → drawer с графиком и сигналами.</div>
          </div>}
        </div>
      </div>
      {/* nested TickerDrawer for picked ticker */}
      <TickerDrawer ticker={drawerTk} onClose={() => setDrawerTk(null)} />
    </>
  )
}
