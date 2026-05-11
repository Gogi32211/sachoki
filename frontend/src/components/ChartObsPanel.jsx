import { useState } from 'react'

const BASE = import.meta.env.VITE_API_URL || ''

const K_OPTIONS = [
  'K1','K1G','K2','K2G','K3','K4','K5','K6','K9','K10','K11','NONE',
]
const QUALITY_OPTIONS = ['PERFECT','GOOD','OK','BAD']

export default function ChartObsPanel({ onSelectTicker }) {
  const [ticker, setTicker]   = useState('')
  const [obsDate, setObsDate] = useState(() => new Date().toISOString().slice(0, 10))
  const [prefilled, setPrefilled] = useState(null)
  const [status, setStatus]   = useState('')
  const [loading, setLoading] = useState(false)

  const [kSig, setKSig]         = useState('')
  const [quality, setQuality]   = useState('')
  const [lvbo, setLvbo]         = useState(false)
  const [eb, setEb]             = useState(false)
  const [kFired, setKFired]     = useState(false)
  const [notes, setNotes]       = useState('')

  const flash = (msg, ms = 2500) => {
    setStatus(msg)
    setTimeout(() => setStatus(''), ms)
  }

  const loadSignals = async () => {
    const t = ticker.trim().toUpperCase()
    if (!t || !obsDate) return
    onSelectTicker?.(t)
    setLoading(true)
    setStatus('⏳ loading...')
    try {
      const r = await fetch(`${BASE}/obs/prefill?ticker=${t}&obs_date=${obsDate}`)
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`)
      const data = await r.json()
      setPrefilled(data)
      flash('✓ loaded')
    } catch (e) {
      setPrefilled(null)
      flash('❌ ' + e.message, 4000)
    } finally {
      setLoading(false)
    }
  }

  const saveObs = async () => {
    if (!prefilled) return
    const payload = {
      ...prefilled,
      k_signal_match: kSig || null,
      entry_quality:  quality || null,
      notes:          notes || null,
      lvbo_present:   lvbo,
      eb_reversal:    eb,
      k_fired:        kFired,
    }
    try {
      const r = await fetch(`${BASE}/obs/save`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      if (!r.ok) throw new Error(`${r.status} ${await r.text()}`)
      const j = await r.json()
      flash('✅ saved ' + j.ticker, 3000)
    } catch (e) {
      flash('❌ ' + e.message, 4000)
    }
  }

  const resetForm = () => {
    setTicker('')
    setPrefilled(null)
    setKSig('')
    setQuality('')
    setLvbo(false)
    setEb(false)
    setKFired(false)
    setNotes('')
    setStatus('')
  }

  const onKey = (e) => { if (e.key === 'Enter') loadSignals() }

  const delta = prefilled
    ? (Number(prefilled.score_at || 0) - Number(prefilled.score_before || 0))
    : 0

  return (
    <div className="bg-gray-900 rounded-lg p-4 font-mono text-xs text-gray-100 max-w-md">
      <div className="text-sm font-bold text-green-400 mb-3">
        📊 Chart Observation
      </div>

      {/* STEP 1: ticker + date */}
      <div className="grid grid-cols-[1fr_1fr_auto] gap-2 mb-3">
        <input
          value={ticker}
          onChange={e => setTicker(e.target.value.toUpperCase())}
          onKeyDown={onKey}
          placeholder="TICKER"
          className="bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-sm text-gray-100 focus:border-blue-500 outline-none"
        />
        <input
          type="date"
          value={obsDate}
          onChange={e => setObsDate(e.target.value)}
          onKeyDown={onKey}
          className="bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-sm text-gray-100 focus:border-blue-500 outline-none"
        />
        <button
          onClick={loadSignals}
          disabled={loading}
          className="bg-blue-700 hover:bg-blue-600 disabled:opacity-50 text-white font-bold px-3 py-1.5 rounded text-sm"
        >
          LOAD →
        </button>
      </div>

      {/* STEP 2: auto-filled display */}
      {prefilled && (
        <>
          <div className="bg-gray-950 rounded p-2.5 mb-2.5 text-[11px] leading-relaxed">
            <div className="grid grid-cols-3 gap-1">
              <span>T: <b className="text-green-400">{prefilled.t_signal || '—'}</b></span>
              <span>Z-1: <b className="text-red-400">{prefilled.z_prev_1 || '—'}</b></span>
              <span>Z-2: <b className="text-red-400">{prefilled.z_prev_2 || '—'}</b></span>
              <span>L: <b className="text-yellow-300">{prefilled.l_signal || '—'}</b></span>
              <span>F: <b className="text-sky-400">{prefilled.f_signal || '—'}</b></span>
              <span>GOG: <b className="text-purple-300">{prefilled.gog_signal || '—'}</b></span>
              <span>Score: <b className="text-white">{prefilled.score_before ?? '?'}→{prefilled.score_at ?? '?'}</b></span>
              <span>Δ: <b className="text-green-300">{delta > 0 ? '+' : ''}{delta}</b></span>
              <span>RTB: <b className="text-white">{(prefilled.rtb_total ?? '')}/{(prefilled.rtb_phase ?? '')}</b></span>
              <span>Beta: <b className="text-white">{prefilled.beta_score ?? '—'}</b></span>
              <span>Zone: <b className="text-sky-400">{prefilled.beta_zone || '—'}</b></span>
              <span>Sweet: <b className="text-yellow-300">{prefilled.sweet_spot ? '★' : '—'}</b></span>
            </div>
            <div className="mt-1">Seq: <b className="text-gray-200">{prefilled.sequence_label || '—'}</b></div>
            {prefilled.signal_reasons && (
              <div className="mt-0.5 text-[10px] text-gray-500">{prefilled.signal_reasons}</div>
            )}
          </div>

          {/* STEP 3: user inputs */}
          <div className="grid grid-cols-3 gap-2 mb-2">
            <select
              value={kSig}
              onChange={e => setKSig(e.target.value)}
              className="bg-gray-800 border border-green-700 rounded px-1.5 py-1.5 text-xs text-gray-100"
            >
              <option value="">K match?</option>
              {K_OPTIONS.map(k => <option key={k} value={k}>{k}</option>)}
            </select>
            <select
              value={quality}
              onChange={e => setQuality(e.target.value)}
              className="bg-gray-800 border border-green-700 rounded px-1.5 py-1.5 text-xs text-gray-100"
            >
              <option value="">Quality?</option>
              {QUALITY_OPTIONS.map(q => <option key={q} value={q}>{q}</option>)}
            </select>
            <div className="flex flex-wrap gap-1.5 items-center text-[11px]">
              <label className="flex items-center gap-1 cursor-pointer">
                <input type="checkbox" checked={lvbo} onChange={e => setLvbo(e.target.checked)} />
                LVBO
              </label>
              <label className="flex items-center gap-1 cursor-pointer">
                <input type="checkbox" checked={eb} onChange={e => setEb(e.target.checked)} />
                EB↓↑
              </label>
              <label className="flex items-center gap-1 cursor-pointer">
                <input type="checkbox" checked={kFired} onChange={e => setKFired(e.target.checked)} />
                K✓
              </label>
            </div>
          </div>

          <input
            value={notes}
            onChange={e => setNotes(e.target.value)}
            placeholder="notes (optional)..."
            className="w-full bg-gray-800 border border-gray-700 rounded px-2 py-1.5 text-xs text-gray-100 mb-2 focus:border-blue-500 outline-none"
          />

          <div className="flex gap-2 items-center">
            <button
              onClick={saveObs}
              className="bg-green-500 hover:bg-green-400 text-black font-bold px-5 py-1.5 rounded text-sm"
            >
              SAVE
            </button>
            <button
              onClick={resetForm}
              className="bg-gray-700 hover:bg-gray-600 text-gray-100 px-3 py-1.5 rounded text-xs"
            >
              Reset
            </button>
            <span className="text-green-400 text-xs">{status}</span>
          </div>
        </>
      )}

      {!prefilled && status && (
        <div className="text-xs text-gray-400 mt-1">{status}</div>
      )}
    </div>
  )
}
