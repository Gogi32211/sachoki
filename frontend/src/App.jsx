import { useState, useEffect, useRef } from 'react'
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
import BRScanPanel from './components/BRScanPanel'
import TurboScanPanel from './components/TurboScanPanel'
import AdminPanel from './components/AdminPanel'
import SignalCorrelPanel from './components/SignalCorrelPanel'
import TickerAnalysisPanel from './components/TickerAnalysisPanel'
import PersonalWatchlistPanel from './components/PersonalWatchlistPanel'

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

// ── Primary tabs (visible) + overflow tabs (hidden in "More" menu) ────────────
const PRIMARY_TABS = [
  { id: 'turbo',      label: '⚡ TURBO' },
  { id: 'watchlist',  label: '⭐ Saved' },
  { id: 'combined',   label: 'Combined' },
  { id: 'predictor',  label: 'Predictor' },
  { id: 'analyze',    label: '🔍 Analyze' },
]

const MORE_TABS = [
  { id: 'combo260',   label: '260323 Combo' },
  { id: 'scanner',    label: 'T/Z Scanner' },
  { id: 'tzlstats',   label: 'T/Z × L Stats' },
  { id: 'power',      label: 'Power Scan' },
  { id: 'brscan',     label: 'BR Scan' },
  { id: 'pumps',      label: 'Pump Combos' },
  { id: 'corr',       label: '📊 Corr' },
  { id: 'howitworks', label: 'How It Works' },
  { id: 'admin',      label: '⚙ Admin' },
]

const ALL_TABS = [...PRIMARY_TABS, ...MORE_TABS]
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
    () => LS.get('active_tab', 'turbo')
  )
  const [moreOpen, setMoreOpen]   = useState(false)
  const moreRef                   = useRef(null)

  const [analyzeChart, setAnalyzeChart] = useState({ ticker: null, tf: '1d' })

  useEffect(() => { LS.set('watchlist', watchlist) }, [watchlist])
  useEffect(() => { LS.set('selected_ticker', selected) }, [selected])
  useEffect(() => { LS.set('tf', tf) }, [tf])
  useEffect(() => { LS.set('active_tab', activeTab) }, [activeTab])

  // Close "More" dropdown on outside click
  useEffect(() => {
    const handler = (e) => {
      if (moreRef.current && !moreRef.current.contains(e.target)) setMoreOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [])

  const handleSelect = (ticker) => setSelected(ticker)
  const handleAddTicker = (t) =>
    setWatchlist(prev => [...new Set([...prev, t.toUpperCase()])])
  const handleRemoveTicker = (t) =>
    setWatchlist(prev => prev.filter(x => x !== t))

  const isMoreActive = MORE_TABS.some(t => t.id === activeTab)

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 p-3 flex flex-col gap-3">

      {/* ── Header ─────────────────────────────────────────────────────── */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold tracking-wide text-white">
          Sachoki Screener{' '}
          <span className="text-xs font-normal text-gray-600">v4.0.104</span>
        </h1>
        <div className="flex items-center gap-3">
          {/* TF selector */}
          <div className="flex gap-1">
            {TF_OPTIONS.map(t => (
              <button
                key={t}
                onClick={() => setTf(t)}
                className={`text-xs px-2 py-1 rounded transition-colors
                  ${tf === t
                    ? 'bg-blue-600 text-white font-semibold'
                    : 'bg-gray-800 text-gray-500 hover:text-gray-200'}`}
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

      {/* ── Chart ──────────────────────────────────────────────────────── */}
      <div style={{ minHeight: '340px' }}>
        <CandleChart
          ticker={activeTab === 'analyze' && analyzeChart.ticker ? analyzeChart.ticker : selected}
          tf={activeTab === 'analyze' && analyzeChart.ticker ? analyzeChart.tf : tf}
        />
      </div>

      {/* ── Tab bar + panels ───────────────────────────────────────────── */}
      <div className="flex flex-col gap-0 flex-1">

        {/* Tab bar — 5 primary + "More ▾" */}
        <div className="flex items-stretch gap-0.5 border-b border-gray-800 bg-gray-900 px-1">
          {PRIMARY_TABS.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`text-xs px-4 py-2.5 rounded-t transition-colors border-b-2 whitespace-nowrap
                ${activeTab === tab.id
                  ? 'border-blue-400 text-blue-400 bg-gray-800 font-semibold'
                  : 'border-transparent text-gray-500 hover:text-gray-300 bg-transparent'}`}
            >
              {tab.label}
            </button>
          ))}

          {/* More dropdown */}
          <div className="relative ml-1" ref={moreRef}>
            <button
              onClick={() => setMoreOpen(o => !o)}
              className={`text-xs px-4 py-2.5 rounded-t transition-colors border-b-2 whitespace-nowrap
                ${isMoreActive
                  ? 'border-blue-400 text-blue-400 bg-gray-800 font-semibold'
                  : 'border-transparent text-gray-500 hover:text-gray-300'}`}
            >
              {isMoreActive
                ? (ALL_TABS.find(t => t.id === activeTab)?.label + ' ▾')
                : '··· More ▾'}
            </button>
            {moreOpen && (
              <div className="absolute top-full left-0 mt-0.5 z-50 bg-gray-900 border border-gray-700 rounded-lg shadow-xl py-1 min-w-[160px]">
                {MORE_TABS.map(tab => (
                  <button
                    key={tab.id}
                    onClick={() => { setActiveTab(tab.id); setMoreOpen(false) }}
                    className={`w-full text-left text-xs px-4 py-2 hover:bg-gray-800 transition-colors
                      ${activeTab === tab.id ? 'text-blue-400 font-semibold bg-gray-800' : 'text-gray-400'}`}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Tab content */}
        <div className="min-h-[400px]">
          <div style={{ display: activeTab === 'turbo' ? 'block' : 'none' }}>
            <TurboScanPanel onSelectTicker={handleSelect} />
          </div>

          {activeTab === 'watchlist' && (
            <PersonalWatchlistPanel
              watchlistTickers={watchlist}
              onSelectTicker={handleSelect}
              onAddTicker={handleAddTicker}
              onRemoveTicker={handleRemoveTicker}
            />
          )}
          {activeTab === 'combined' && (
            <CombinedScanPanel tf={tf} onSelectTicker={handleSelect} />
          )}
          {activeTab === 'combo260' && (
            <ComboScanPanel tf={tf} onSelectTicker={handleSelect} />
          )}
          {activeTab === 'predictor' && (
            <PredictorPanel ticker={selected} tf={tf} />
          )}
          {activeTab === 'scanner' && (
            <ScannerPanel tf={tf} onSelectTicker={handleSelect} />
          )}
          {activeTab === 'tzlstats' && (
            <TZLStatsPanel ticker={selected} tf={tf} />
          )}
          {activeTab === 'power' && (
            <PowerScanPanel tf={tf} onSelectTicker={handleSelect} />
          )}
          {activeTab === 'brscan' && (
            <BRScanPanel tf={tf} onSelectTicker={handleSelect} />
          )}
          {activeTab === 'pumps' && (
            <PumpComboPanel />
          )}
          {activeTab === 'corr' && (
            <SignalCorrelPanel />
          )}
          {activeTab === 'analyze' && (
            <TickerAnalysisPanel
              onAddToWatchlist={handleAddTicker}
              onChartChange={setAnalyzeChart}
            />
          )}
          {activeTab === 'howitworks' && (
            <HowItWorksPanel />
          )}
          {activeTab === 'admin' && (
            <AdminPanel />
          )}
        </div>
      </div>
    </div>
  )
}
