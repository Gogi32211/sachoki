import { useState, useEffect, useRef } from 'react'

const fmtPct = (v) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${Number(v).toFixed(2)}%`)
const fmtUsd = (v) => (v == null ? '—' : (Math.abs(v) >= 1e9 ? `$${(v/1e9).toFixed(1)}B` : Math.abs(v) >= 1e6 ? `$${(v/1e6).toFixed(0)}M` : `$${Number(v).toLocaleString()}`))

let _tvPromise = null
function loadTV() {
  if (window.TradingView) return Promise.resolve()
  if (_tvPromise) return _tvPromise
  _tvPromise = new Promise((res) => {
    const s = document.createElement('script')
    s.src = 'https://s3.tradingview.com/tv.js'; s.async = true; s.onload = () => res()
    document.body.appendChild(s)
  })
  return _tvPromise
}

function TVChart({ ticker }) {
  const cid = `tv_${ticker}`
  const ref = useRef(null)
  useEffect(() => {
    let dead = false
    loadTV().then(() => {
      if (dead || !ref.current || !window.TradingView) return
      ref.current.innerHTML = ''
      new window.TradingView.widget({
        symbol: ticker, container_id: cid, autosize: true, theme: 'dark',
        style: '1', interval: 'D', timezone: 'America/New_York', locale: 'en',
        hide_side_toolbar: true, allow_symbol_change: false, withdateranges: true,
      })
    })
    return () => { dead = true }
  }, [ticker, cid])
  return <div id={cid} ref={ref} style={{ height: 340 }} className="w-full rounded overflow-hidden bg-black" />
}

export default function TickerDrawer({ ticker, onClose }) {
  const [d, setD] = useState(null)
  useEffect(() => {
    setD(null)
    if (!ticker) return
    fetch(`/api/journal/ticker/${ticker}`).then(r => r.json()).then(setD).catch(() => {})
  }, [ticker])
  if (!ticker) return null

  const meta = d?.meta || {}, lat = d?.latest || {}
  const tvUrl = `https://www.tradingview.com/chart/?symbol=${encodeURIComponent(ticker)}`
  return (
    <>
      <div className="fixed inset-0 bg-black/50 z-40" onClick={onClose} />
      <div className="fixed inset-y-0 right-0 w-full max-w-[560px] z-50 bg-md-surface border-l border-white/10 shadow-2xl overflow-y-auto">
        <div className="sticky top-0 bg-md-surface/95 backdrop-blur border-b border-white/10 px-4 py-3 flex items-center gap-3">
          <div>
            <div className="text-xl font-bold">{ticker}</div>
            <div className="text-xs text-md-on-surface-var">{meta.name || ''}</div>
          </div>
          <div className="ml-auto text-right">
            <div className="font-mono text-lg">{lat.close != null ? `$${Number(lat.close).toFixed(2)}` : '—'}</div>
            <div className={`text-xs font-mono ${(lat.change_pct||0)>=0?'text-emerald-400':'text-rose-400'}`}>{fmtPct(lat.change_pct)}</div>
          </div>
          <a href={tvUrl} target="_blank" rel="noreferrer"
             className="px-3 py-1.5 rounded bg-blue-600 hover:bg-blue-500 text-white text-sm font-semibold whitespace-nowrap">TradingView ↗</a>
          <button onClick={onClose} className="px-2 py-1 rounded hover:bg-white/10 text-md-on-surface-var text-lg">✕</button>
        </div>

        <div className="p-4 space-y-4">
          <TVChart ticker={ticker} />

          {!d ? <div className="text-md-on-surface-var text-sm">Загрузка…</div> : <>
            {/* Signals */}
            <Section title="Signals (latest bar)">
              <div className="grid grid-cols-2 gap-2 text-sm">
                <KV k="V3" v={lat.prebreak_v3} />
                <KV k="T/Z" v={lat.t_sig || lat.z_sig || '—'} />
                <KV k="RTB phase" v={lat.rtb_phase || '—'} />
                <KV k="RSI" v={lat.rsi_14 != null ? Number(lat.rsi_14).toFixed(0) : '—'} />
                <KV k="CCI" v={lat.cci_20 != null ? Number(lat.cci_20).toFixed(0) : '—'} />
                <KV k="Vol" v={lat.vol_bucket || '—'} />
                <KV k="ULTRA" v={lat.ultra_score} />
                <KV k="Turbo" v={lat.turbo_score} />
              </div>
              {lat.prebreak_v3_reasons && <div className="mt-2 text-xs font-mono text-violet-300">{lat.prebreak_v3_reasons}</div>}
            </Section>

            {/* Meta */}
            <Section title="Company">
              <div className="grid grid-cols-2 gap-2 text-sm">
                <KV k="Sector" v={meta.sector || '—'} />
                <KV k="Industry" v={meta.industry || '—'} />
                <KV k="Market cap" v={fmtUsd(meta.market_cap)} />
                <KV k="Cap bucket" v={meta.mcap_bucket || '—'} />
                <KV k="Employees" v={meta.employees != null ? Number(meta.employees).toLocaleString() : '—'} />
              </div>
            </Section>

            {/* Journal positions */}
            {d.positions?.length > 0 && <Section title={`Journal positions (${d.positions.length})`}>
              {d.positions.map(p => (
                <div key={p.id} className="text-xs border-b border-white/5 py-1.5">
                  <div className="flex gap-2">
                    <span className={`font-bold ${p.status==='OPEN'?'text-emerald-300':p.status==='PENDING_OPEN'?'text-amber-300':'text-md-on-surface-var'}`}>{p.status}</span>
                    <span>conv {p.conviction}</span>
                    {p.entry_px && <span>entry ${p.entry_px.toFixed(2)}</span>}
                    {p.verdict && p.verdict!=='PENDING' && <span className={p.pnl_pct>=0?'text-emerald-400':'text-rose-400'}>{p.verdict} {fmtPct(p.pnl_pct)}</span>}
                    <span className="ml-auto text-md-on-surface-var">{p.decision_date}</span>
                  </div>
                  <div className="text-md-on-surface-var mt-0.5">{p.thesis}</div>
                </div>
              ))}
            </Section>}

            {/* Insider */}
            {d.insider?.length > 0 && <Section title={`Insider (Form 4) — ${d.insider.length}`}>
              <table className="w-full text-xs"><tbody>{d.insider.map((x,i)=>(
                <tr key={i} className="border-b border-white/5">
                  <td className="py-0.5 pr-2"><span className={x.code==='P'?'text-emerald-400 font-bold':'text-rose-300'}>{x.code==='P'?'BUY':x.code}</span></td>
                  <td className="py-0.5 pr-2 text-md-on-surface-var truncate max-w-[150px]" title={x.insider}>{x.insider}</td>
                  <td className="py-0.5 pr-2 text-right font-mono">{fmtUsd(x.value)}</td>
                  <td className="py-0.5 text-right font-mono text-md-on-surface-var">{x.tx_date}</td>
                </tr>))}</tbody></table>
            </Section>}
          </>}
        </div>
      </div>
    </>
  )
}

function Section({ title, children }) {
  return <div className="rounded-lg bg-md-surface-high border border-white/10 p-3">
    <div className="text-xs font-semibold text-md-on-surface-var mb-2 uppercase tracking-wide">{title}</div>{children}</div>
}
function KV({ k, v }) {
  return <div className="flex justify-between"><span className="text-md-on-surface-var">{k}</span><span className="font-mono">{v ?? '—'}</span></div>
}
