import { useState } from 'react'
import TickerInput from './components/TickerInput'
import WatchlistPanel from './components/WatchlistPanel'
import CandleChart from './components/CandleChart'
import PredictorPanel from './components/PredictorPanel'
import ScannerPanel from './components/ScannerPanel'

export default function App() {
  const [watchlist, setWatchlist] = useState(['AAPL', 'TSLA', 'NVDA'])
  const [selected, setSelected] = useState('AAPL')
  const [tf, setTf] = useState('1d')

  return (
    <div className="min-h-screen bg-gray-950 p-4 space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-bold tracking-wide text-white">
          TZ Signal Dashboard
        </h1>
        <TickerInput
          watchlist={watchlist}
          onAdd={(t) => setWatchlist((p) => [...new Set([...p, t])])}
          onRemove={(t) => setWatchlist((p) => p.filter((x) => x !== t))}
          tf={tf}
          onTfChange={setTf}
        />
      </div>

      {/* Top row: Watchlist + Chart */}
      <div className="grid grid-cols-12 gap-4">
        <div className="col-span-3">
          <WatchlistPanel
            tickers={watchlist}
            tf={tf}
            selected={selected}
            onSelect={setSelected}
          />
        </div>
        <div className="col-span-9">
          <CandleChart ticker={selected} tf={tf} />
        </div>
      </div>

      {/* Bottom row: Predictor + Scanner */}
      <div className="grid grid-cols-12 gap-4">
        <div className="col-span-6">
          <PredictorPanel ticker={selected} tf={tf} />
        </div>
        <div className="col-span-6">
          <ScannerPanel tf={tf} />
        </div>
      </div>
    </div>
  )
}
