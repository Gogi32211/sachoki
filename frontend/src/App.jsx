import { useState, useEffect, useRef, useCallback } from 'react'
import TickerInput from './components/TickerInput'
import WatchlistPanel from './components/WatchlistPanel'
import CandleChart from './components/CandleChart'
import PredictorPanel from './components/PredictorPanel'
import ScannerPanel from './components/ScannerPanel'
import CombinedScanPanel from './components/CombinedScanPanel'
import TZLStatsPanel from './components/TZLStatsPanel'
import HowItWorksPanel from './components/HowItWorksPanel'
import TurboScanPanel from './components/TurboScanPanel'
import UltraScanPanel from './components/UltraScanPanel'
import AdminPanel from './components/AdminPanel'
import SignalCorrelPanel from './components/SignalCorrelPanel'
import TickerAnalysisPanel from './components/TickerAnalysisPanel'
import PersonalWatchlistPanel from './components/PersonalWatchlistPanel'
import SuperchartPanel from './components/SuperchartPanel'
import SectorAnalysisPanel from './components/SectorAnalysisPanel'
import ReplayPanel from './components/ReplayPanel'
import TZWLNBBPanel from './components/TZWLNBBPanel'
import TZIntelligencePanel from './components/TZIntelligencePanel'
import RareReversalPanel from './components/RareReversalPanel'
import SequenceScanPanel from './components/SequenceScanPanel'

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
  { id: 'ultra',      label: '🧬 ULTRA' },
  { id: 'watchlist',  label: '⭐ Watchlist' },
  { id: 'combined',   label: 'Combined Scan' },
  { id: 'predictor',  label: 'Predictor' },
  { id: 'scanner',    label: 'T/Z Scanner' },
  { id: 'tzlstats',   label: 'T/Z × L Stats' },
  { id: 'corr',       label: '📊 Corr' },
  { id: 'superchart', label: '📋 Superchart' },
  { id: 'sectors',    label: '🌐 Sectors' },
  { id: 'analyze',    label: '🔍 Analyze' },
  { id: 'replay',     label: '🔬 Replay' },
  { id: 'tzwlnbb',       label: '📡 TZ/WLNBB' },
  { id: 'tzintelligence', label: '🧠 TZ Intel' },
  { id: 'rarereversal',   label: '🔄 Rare Reversal' },
  { id: 'sequences',      label: '🔢 Sequences' },
  { id: 'howitworks',    label: 'How It Works' },
  { id: 'admin',      label: '⚙ Admin' },
]

const TF_OPTIONS = ['1d', '4h', '1h', '30m', '15m']

const VALID_TAB_IDS = new Set(TABS.map(t => t.id))

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
  const [activeTab, setActiveTab] = useState(() => {
    const saved = LS.get('active_tab', 'combined')
    return VALID_TAB_IDS.has(saved) ? saved : 'combined'
  })
  const [analyzeChart, setAnalyzeChart] = useState({ ticker: null, tf: '1d' })

  // Superchart owns its own ticker/tf so the global chart follows it
  const [scTicker, setScTicker]   = useState(null)
  const [scTf, setScTf]           = useState('1d')
  const chartInstanceRef          = useRef(null)
  const [chartReady, setChartReady] = useState(false)

  const handleChartReady = useCallback((chart) => {
    chartInstanceRef.current = chart
    setChartReady(!!chart)
  }, [])

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

  // Which ticker/tf the global chart shows
  const chartTicker =
    activeTab === 'superchart' && scTicker ? scTicker :
    activeTab === 'analyze'    && analyzeChart.ticker ? analyzeChart.ticker :
    selected
  const chartTf =
    activeTab === 'superchart' && scTicker ? scTf :
    activeTab === 'analyze'    && analyzeChart.ticker ? analyzeChart.tf :
    tf

  return (
    <div className="min-h-screen bg-gray-950 text-gray-100 p-3 flex flex-col gap-3">
      {/* ── Header ─────────────────────────────────────────────────────── */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold tracking-wide text-white">
          Sachoki Screener{' '}
          <span className="text-xs font-normal text-gray-500">v4.4.552</span>
        </h1>
        <div className="flex items-center gap-3">
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

      {/* ── Tab bar — moved above chart ─────────────────────────────────── */}
      <div className="flex flex-wrap gap-1 border-b border-gray-800">
        {TABS.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`text-xs px-3 py-2 rounded-t transition-colors border-b-2
              ${activeTab === tab.id
                ? 'border-blue-500 text-blue-400 bg-gray-900'
                : 'border-transparent text-gray-500 hover:text-gray-300 bg-transparent'}`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* ── Chart — hidden on turbo tab (uses its own popup) ──────────────── */}
      {activeTab !== 'turbo' && (
      <div style={{ minHeight: '340px' }}>
        <CandleChart
          ticker={chartTicker}
          tf={chartTf}
          onChartReady={handleChartReady}
        />
      </div>
      )}

      {/* ── Tab content ─────────────────────────────────────────────────── */}
      <div className="min-h-[400px]">
        {/* TURBO: always mounted so scan results survive tab switches */}
        <div style={{ display: activeTab === 'turbo' ? 'block' : 'none' }}>
          <TurboScanPanel onSelectTicker={handleSelect} />
        </div>

        {/* ULTRA: always mounted so scan results survive tab switches */}
        <div style={{ display: activeTab === 'ultra' ? 'block' : 'none' }}>
          <UltraScanPanel onSelectTicker={handleSelect} />
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

        {activeTab === 'predictor' && (
          <PredictorPanel ticker={selected} tf={tf} />
        )}

        {activeTab === 'scanner' && (
          <ScannerPanel tf={tf} onSelectTicker={handleSelect} />
        )}

        {activeTab === 'tzlstats' && (
          <TZLStatsPanel ticker={selected} tf={tf} />
        )}

        {activeTab === 'corr' && (
          <SignalCorrelPanel />
        )}

        {activeTab === 'superchart' && (
          <SuperchartPanel
            initialTicker={selected}
            initialTf={tf}
            onTickerChange={(t, f) => { setScTicker(t); setScTf(f) }}
          />
        )}

        {activeTab === 'sectors' && (
          <SectorAnalysisPanel onSelectTicker={handleSelect} />
        )}

        {activeTab === 'analyze' && (
          <TickerAnalysisPanel
            onAddToWatchlist={handleAddTicker}
            onChartChange={setAnalyzeChart}
          />
        )}

        {activeTab === 'replay' && (
          <ReplayPanel />
        )}

        {activeTab === 'tzwlnbb' && (
          <TZWLNBBPanel />
        )}

        {activeTab === 'tzintelligence' && (
          <TZIntelligencePanel onSelectTicker={handleSelect} />
        )}

        {activeTab === 'rarereversal' && (
          <RareReversalPanel />
        )}

        {activeTab === 'sequences' && (
          <SequenceScanPanel />
        )}

        {activeTab === 'howitworks' && (
          <HowItWorksPanel />
        )}
        {activeTab === 'admin' && (
          <AdminPanel />
        )}
      </div>
    </div>
  )
}
