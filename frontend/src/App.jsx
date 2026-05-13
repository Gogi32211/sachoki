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
import PortfolioPanel from './components/PortfolioPanel'
import ChartObsPanel from './components/ChartObsPanel'
import SignalReplayPanel from './components/SignalReplayPanel'

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

// ── Navigation structure (grouped) ───────────────────────────────────────────
const TAB_GROUPS = [
  {
    label: 'Scan',
    tabs: [
      { id: 'turbo',    label: '⚡ TURBO' },
      { id: 'ultra',    label: '🧬 ULTRA' },
      { id: 'combined', label: 'Combined' },
      { id: 'scanner',  label: 'T/Z Scanner' },
      { id: 'sequences',label: '🔢 Sequences' },
    ],
  },
  {
    label: 'Research',
    tabs: [
      { id: 'analyze',        label: '🔍 Analyze' },
      { id: 'sigreplay',      label: '🧪 Sig Replay' },
      { id: 'replay',         label: '🔬 Replay' },
      { id: 'tzlstats',       label: 'T/Z × L Stats' },
      { id: 'corr',           label: '📊 Corr' },
      { id: 'tzwlnbb',        label: '📡 TZ/WLNBB' },
      { id: 'tzintelligence', label: '🧠 TZ Intel' },
      { id: 'rarereversal',   label: '🔄 Rare Rev.' },
    ],
  },
  {
    label: 'Market',
    tabs: [
      { id: 'sectors',    label: '🌐 Sectors' },
      { id: 'superchart', label: '📋 Superchart' },
      { id: 'chartobs',   label: '📊 Obs' },
      { id: 'predictor',  label: 'Predictor' },
    ],
  },
  {
    label: 'Portfolio',
    tabs: [
      { id: 'watchlist', label: '⭐ Watchlist' },
      { id: 'portfolio', label: '📋 Portfolio' },
    ],
  },
  {
    label: 'More',
    tabs: [
      { id: 'howitworks', label: 'How It Works' },
      { id: 'admin',      label: '⚙ Admin' },
    ],
  },
]

const TABS = TAB_GROUPS.flatMap(g => g.tabs)
const VALID_TAB_IDS = new Set(TABS.map(t => t.id))
const TF_OPTIONS = ['1d', '4h', '1h', '30m', '15m']

export default function App() {
  const [watchlist, setWatchlist] = useState(
    () => LS.get('watchlist', ['AAPL', 'TSLA', 'NVDA'])
  )
  const [selected, setSelected] = useState(
    () => LS.get('selected_ticker', 'AAPL')
  )
  const [tf, setTf] = useState(
    () => LS.get('tf', '1d')
  )
  const [activeTab, setActiveTab] = useState(() => {
    const saved = LS.get('active_tab', 'combined')
    return VALID_TAB_IDS.has(saved) ? saved : 'combined'
  })
  const [analyzeChart, setAnalyzeChart] = useState({ ticker: null, tf: '1d' })
  const [scTicker, setScTicker] = useState(null)
  const [scTf, setScTf]         = useState('1d')
  const chartInstanceRef        = useRef(null)
  const [chartReady, setChartReady] = useState(false)

  const handleChartReady = useCallback((chart) => {
    chartInstanceRef.current = chart
    setChartReady(!!chart)
  }, [])

  useEffect(() => { LS.set('watchlist', watchlist) }, [watchlist])
  useEffect(() => { LS.set('selected_ticker', selected) }, [selected])
  useEffect(() => { LS.set('tf', tf) }, [tf])
  useEffect(() => { LS.set('active_tab', activeTab) }, [activeTab])

  const handleSelect      = (ticker) => setSelected(ticker)
  const handleAddTicker   = (t) => setWatchlist(prev => [...new Set([...prev, t.toUpperCase()])])
  const handleRemoveTicker= (t) => setWatchlist(prev => prev.filter(x => x !== t))

  const chartTicker =
    activeTab === 'superchart' && scTicker          ? scTicker :
    activeTab === 'analyze'    && analyzeChart.ticker ? analyzeChart.ticker :
    selected
  const chartTf =
    activeTab === 'superchart' && scTicker          ? scTf :
    activeTab === 'analyze'    && analyzeChart.ticker ? analyzeChart.tf :
    tf

  const activeGroupLabel = TAB_GROUPS.find(g => g.tabs.some(t => t.id === activeTab))?.label

  return (
    <div className="min-h-screen flex flex-col bg-md-surface text-md-on-surface">
      {/* ── MD3 Top App Bar ──────────────────────────────────────────────── */}
      <header className="sticky top-0 z-40 flex items-center justify-between gap-3 px-4 h-14 shadow-md-2 bg-md-surface-high shrink-0">
        {/* Brand */}
        <div className="flex items-center gap-2.5 shrink-0">
          <span className="text-xl font-semibold tracking-tight text-md-primary">Sachoki</span>
          <span className="text-xs text-md-on-surface-var">v4.5.23</span>
        </div>

        {/* Timeframe segmented control */}
        <div className="flex rounded-md-sm overflow-hidden border border-md-outline-var shrink-0">
          {TF_OPTIONS.map((t, i) => (
            <button
              key={t}
              onClick={() => setTf(t)}
              className={[
                'px-3 py-1.5 text-xs font-medium transition-colors duration-100',
                i > 0 ? 'border-l border-md-outline-var' : '',
                tf === t
                  ? 'bg-md-primary-container text-md-on-primary-container'
                  : 'text-md-on-surface-var hover:bg-white/5',
              ].join(' ')}
            >
              {t}
            </button>
          ))}
        </div>

        {/* Ticker input */}
        <div className="flex-1 max-w-sm">
          <TickerInput
            watchlist={watchlist}
            onAdd={handleAddTicker}
            onRemove={handleRemoveTicker}
            tf={tf}
            onTfChange={setTf}
          />
        </div>
      </header>

      {/* ── MD3 Navigation Rail / Tab bar ────────────────────────────────── */}
      <nav className="sticky top-14 z-30 bg-md-surface-con border-b border-md-outline-var shrink-0">
        <div className="flex flex-wrap items-end px-2">
          {TAB_GROUPS.map((group, gi) => (
            <div key={group.label} className="flex items-end">
              {/* Group divider (not before first group) */}
              {gi > 0 && (
                <div className="self-stretch w-px my-2 mx-1 bg-md-outline-var" />
              )}
              {/* Group label on wide screens */}
              <span className="hidden xl:flex items-center px-2 text-xs text-md-on-surface-var font-medium self-center opacity-60 select-none">
                {group.label}
              </span>
              {/* Tabs */}
              {group.tabs.map(tab => {
                const active = activeTab === tab.id
                return (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    className={[
                      'relative px-3 py-2.5 text-xs font-medium transition-colors duration-100',
                      'whitespace-nowrap select-none',
                      active
                        ? 'text-md-primary'
                        : 'text-md-on-surface-var hover:text-md-on-surface',
                    ].join(' ')}
                  >
                    {tab.label}
                    {/* MD3 active indicator line */}
                    <span
                      className="absolute bottom-0 left-2 right-2 h-[3px] rounded-t-md-sm bg-md-primary transition-all duration-150"
                      style={{ opacity: active ? 1 : 0 }}
                    />
                  </button>
                )
              })}
            </div>
          ))}
        </div>
      </nav>

      {/* ── Content area ─────────────────────────────────────────────────── */}
      <main className="flex flex-col gap-3 p-3 flex-1">
        {/* Chart — hidden on turbo tab (uses its own popup) */}
        {activeTab !== 'turbo' && (
          <div style={{ minHeight: '340px' }}>
            <CandleChart
              ticker={chartTicker}
              tf={chartTf}
              onChartReady={handleChartReady}
            />
          </div>
        )}

        {/* Tab content */}
        <div className="min-h-[400px]">
          {/* Always-mounted: preserve scan results on tab switch */}
          <div style={{ display: activeTab === 'turbo' ? 'block' : 'none' }}>
            <TurboScanPanel onSelectTicker={handleSelect} />
          </div>
          <div style={{ display: activeTab === 'ultra' ? 'block' : 'none' }}>
            <UltraScanPanel onSelectTicker={handleSelect} />
          </div>

          {activeTab === 'watchlist'      && <PersonalWatchlistPanel watchlistTickers={watchlist} onSelectTicker={handleSelect} onAddTicker={handleAddTicker} onRemoveTicker={handleRemoveTicker} />}
          {activeTab === 'combined'       && <CombinedScanPanel tf={tf} onSelectTicker={handleSelect} />}
          {activeTab === 'predictor'      && <PredictorPanel ticker={selected} tf={tf} />}
          {activeTab === 'scanner'        && <ScannerPanel tf={tf} onSelectTicker={handleSelect} />}
          {activeTab === 'tzlstats'       && <TZLStatsPanel ticker={selected} tf={tf} />}
          {activeTab === 'corr'           && <SignalCorrelPanel />}
          {activeTab === 'superchart'     && <SuperchartPanel initialTicker={selected} initialTf={tf} onTickerChange={(t, f) => { setScTicker(t); setScTf(f) }} />}
          {activeTab === 'sectors'        && <SectorAnalysisPanel onSelectTicker={handleSelect} />}
          {activeTab === 'analyze'        && <TickerAnalysisPanel onAddToWatchlist={handleAddTicker} onChartChange={setAnalyzeChart} />}
          {activeTab === 'replay'         && <ReplayPanel />}
          {activeTab === 'tzwlnbb'        && <TZWLNBBPanel />}
          {activeTab === 'tzintelligence' && <TZIntelligencePanel onSelectTicker={handleSelect} />}
          {activeTab === 'rarereversal'   && <RareReversalPanel />}
          {activeTab === 'sequences'      && <SequenceScanPanel />}
          {activeTab === 'howitworks'     && <HowItWorksPanel />}
          {activeTab === 'portfolio'      && <PortfolioPanel />}
          {activeTab === 'chartobs'       && <ChartObsPanel onSelectTicker={handleSelect} />}
          {activeTab === 'sigreplay'      && <SignalReplayPanel />}
          {activeTab === 'admin'          && <AdminPanel />}
        </div>
      </main>
    </div>
  )
}
