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
import BRScanPanel from './components/BRScanPanel'
import TurboScanPanel from './components/TurboScanPanel'
import AdminPanel from './components/AdminPanel'
import SignalCorrelPanel from './components/SignalCorrelPanel'
import TickerAnalysisPanel from './components/TickerAnalysisPanel'

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
  { id: 'turbo',      label: '⚡ TURBO' },
  { id: 'combined',   label: 'Combined Scan' },
  { id: 'combo260',   label: '260323 Combo' },
  { id: 'predictor',  label: 'Predictor' },
  { id: 'scanner',    label: 'T/Z Scanner' },
  { id: 'tzlstats',   label: 'T/Z × L Stats' },
  { id: 'power',      label: 'Power Scan' },
  { id: 'brscan',     label: 'BR Scan' },
  { id: 'pumps',      label: 'Pump Combos' },
  { id: 'corr',       label: '📊 Corr' },
  { id: 'analyze',    label: '🔍 Analyze' },
  { id: 'howitworks', label: 'How It Works' },
  { id: 'admin',      label: '⚙ Admin' },
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

  const [analyzeChart, setAnalyzeChart] = useState({ ticker: null, tf: '1d' })

  // Persist on change
  useEffect(() => { LS.set('watchlist', watchlist) }, [watchlist])
  useEffect(() => { LS.set('selected_ticker', selected) }, [selected])
  useEffect(() => { LS.set('tf', tf) }, [tf])
  useEffect(() => { LS.set('active_tab', activeTab) }, [activeTab])

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
          Sachoki Screener{' '}
          <span className="text-xs font-normal text-gray-500">v3.9.346</span>
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
            onRemove={handleRemoveTicker}
          />
        </div>
        <div className="col-span-12 md:col-span-9">
          <CandleChart
            ticker={activeTab === 'analyze' && analyzeChart.ticker ? analyzeChart.ticker : selected}
            tf={activeTab === 'analyze' && analyzeChart.ticker ? analyzeChart.tf : tf}
          />
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
            </button>
          ))}
        </div>

        {/* Tab content */}
        <div className="min-h-[340px]">
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

          {/* TURBO: always mounted so scan results survive tab switches */}
          <div style={{ display: activeTab === 'turbo' ? 'block' : 'none' }}>
            <TurboScanPanel onSelectTicker={(t) => { handleSelect(t); setActiveTab('predictor') }} />
          </div>

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
