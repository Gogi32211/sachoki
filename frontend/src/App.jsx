import { useState, useEffect } from 'react'
import TickerInput from './components/TickerInput'
import WatchlistPanel from './components/WatchlistPanel'
import CandleChart from './components/CandleChart'
import PredictorPanel from './components/PredictorPanel'
import ScannerPanel from './components/ScannerPanel'
import CombinedScanPanel from './components/CombinedScanPanel'
import PumpComboPanel from './components/PumpComboPanel'
import TZLStatsPanel from './components/TZLStatsPanel'
import HowItWorksPanel from './components/HowItWorksPanel'
import ComboScanPanel from './components/ComboScanPanel'
import PowerScanPanel from './components/PowerScanPanel'
import JournalPanel from './components/JournalPanel'

// ── localStorage helpers ──────────────────────────────────────────────────────
const LS = {
  get: (key, fallback) => {
    try { const v = localStorage.getItem(key); return v !== null ? JSON.parse(v) : fallback }
    catch { return fallback }
  },
  set: (key, val) => {
    try { localStorage.setItem(key, JSON.stringify(val)) } catch {}
  },
}

const TABS = [
  { id: 'combined',  label: 'Combined Scan' },
  { id: 'combo260',  label: '260323 Combo' },
  { id: 'predictor', label: 'Predictor' },
  { id: 'scanner',   label: 'T/Z Scanner' },
  { id: 'tzlstats',  label: 'T/Z × L Stats' },
  { id: 'power',     label: 'Power Scan' },
  { id: 'pumps',     label: 'Pump Combos' },
  { id: 'journal',   label: 'Journal' },
  { id: 'howitworks', label: 'How It Works' },
]

const TF_OPTIONS = ['1d', '4h', '1h', '30m', '15m']

export default function App() {
  const [watchlist, setWatchlist] = useState(
    () => LS.get('watchlist', ['AAPL', 'TSLA', 'NVDA'])
  )
  const [selected, setSelected]   = useState(
    () => LS.get('selected_ticker', 'AAPL')
  )
  const [tf, setTf]               = useState(
    () => LS.get('tf', '1d')
  )
  const [activeTab, setActiveTab] = useState(
    () => LS.get('active_tab', 'combined')
  )
  const [journal, setJournal] = useState(
    () => LS.get('journal', [])
  )

  // Persist on change
  useEffect(() => { LS.set('watchlist', watchlist) }, [watchlist])
  useEffect(() => { LS.set('selected_ticker', selected) }, [selected])
  useEffect(() => { LS.set('tf', tf) }, [tf])
  useEffect(() => { LS.set('active_tab', activeTab) }, [activeTab])
  useEffect(() => { LS.set('journal', journal) }, [journal])

  const handleAddToJournal = ({ ticker, source, signals, score, price }) => {
    setJournal(prev => {
      // avoid duplicates from same source in same day
      const today = new Date().toDateString()
      const exists = prev.some(
        e => e.ticker === ticker && e.source === source &&
             new Date(e.addedAt).toDateString() === today
      )
      if (exists) return prev
      return [...prev, {
        id:       Date.now() + Math.random(),
        ticker,
        source,
        signals:  signals ?? '',
        score:    score ?? '',
        price:    price ?? null,
        addedAt:  new Date().toISOString(),
        note:     '',
      }]
    })
  }

  const handleJournalRemove = (id) =>
    setJournal(prev => prev.filter(e => e.id !== id))

  const handleJournalNote = (id, note) =>
    setJournal(prev => prev.map(e => e.id === id ? { ...e, note } : e))

  const handleSelect = (ticker) => setSelected(ticker)

  const handleAddTicker = (t) =>
    setWatchlist(prev => [...new Set([...prev, t.toUpperCase()])])

  const handleRemoveTicker = (t) =>
    setWatchlist(prev => prev.filter(x => x !== t))

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 p-3 flex flex-col gap-3">
      {/* ── Header ─────────────────────────────────────────────────────── */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold tracking-wide text-white">
          TZ Signal Dashboard{' '}
          <span className="text-xs font-normal text-gray-500">v2.2</span>
        </h1>
        <div className="flex items-center gap-3">
          {/* Timeframe selector */}
          <div className="flex gap-1">
            {TF_OPTIONS.map(t => (
              <button
                key={t}
                onClick={() => setTf(t)}
                className={`text-xs px-2 py-1 rounded transition-colors
                  ${tf === t
                    ? 'bg-blue-600 text-white font-semibold'
                    : 'bg-gray-800 text-gray-400 hover:text-white'}`}
              >
                {t}
              </button>
            ))}
          </div>
          <TickerInput
            watchlist={watchlist}
            onAdd={handleAddTicker}
            onRemove={handleRemoveTicker}
            tf={tf}
            onTfChange={setTf}
          />
        </div>
      </div>

      {/* ── Top row: Watchlist + Chart ──────────────────────────────────── */}
      <div className="grid grid-cols-12 gap-3" style={{ minHeight: '420px' }}>
        <div className="col-span-12 md:col-span-3">
          <WatchlistPanel
            tickers={watchlist}
            tf={tf}
            selected={selected}
            onSelect={handleSelect}
          />
        </div>
        <div className="col-span-12 md:col-span-9">
          <CandleChart ticker={selected} tf={tf} />
        </div>
      </div>

      {/* ── Bottom: Tab bar + panels ────────────────────────────────────── */}
      <div className="flex flex-col gap-0 flex-1">
        {/* Tab buttons */}
        <div className="flex gap-1 border-b border-gray-800">
          {TABS.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`text-xs px-4 py-2 rounded-t transition-colors border-b-2
                ${activeTab === tab.id
                  ? 'border-blue-500 text-blue-400 bg-gray-900'
                  : 'border-transparent text-gray-500 hover:text-gray-300 bg-transparent'}`}
            >
              {tab.label}
              {tab.id === 'journal' && journal.length > 0 && (
                <span className="ml-1 text-[10px] bg-blue-600 text-white rounded-full px-1.5 py-0.5">
                  {journal.length}
                </span>
              )}
            </button>
          ))}
        </div>

        {/* Tab content */}
        <div className="min-h-[340px]">
          {activeTab === 'combined' && (
            <CombinedScanPanel tf={tf} onSelectTicker={handleSelect} onAddToJournal={handleAddToJournal} />
          )}

          {activeTab === 'combo260' && (
            <ComboScanPanel tf={tf} onSelectTicker={handleSelect} onAddToJournal={handleAddToJournal} />
          )}

          {activeTab === 'predictor' && (
            <PredictorPanel ticker={selected} tf={tf} />
          )}

          {activeTab === 'scanner' && (
            <ScannerPanel tf={tf} onSelectTicker={handleSelect} onAddToJournal={handleAddToJournal} />
          )}

          {activeTab === 'tzlstats' && (
            <TZLStatsPanel ticker={selected} tf={tf} />
          )}

          {activeTab === 'power' && (
            <PowerScanPanel tf={tf} onSelectTicker={handleSelect} onAddToJournal={handleAddToJournal} />
          )}

          {activeTab === 'pumps' && (
            <PumpComboPanel />
          )}

          {activeTab === 'journal' && (
            <JournalPanel
              journal={journal}
              onRemove={handleJournalRemove}
              onUpdateNote={handleJournalNote}
              onSelectTicker={t => { handleSelect(t); setActiveTab('combined') }}
            />
          )}

          {activeTab === 'howitworks' && (
            <HowItWorksPanel />
          )}
        </div>
      </div>
    </div>
  )
}
