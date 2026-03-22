export default function HowItWorksPanel() {
  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 p-6 overflow-auto max-h-[700px]">
      <h2 className="text-lg font-bold text-white mb-6">How It Works</h2>

      <div className="space-y-8 text-sm text-gray-300">

        {/* Overview */}
        <section>
          <h3 className="text-blue-400 font-semibold text-base mb-2">Overview</h3>
          <p>
            TZ Signal Dashboard is a technical analysis tool that scans stocks across multiple
            timeframes, identifies bull/bear signal patterns, predicts next-bar outcomes based on
            historical matches, and mines multi-bar candlestick sequences that precede large price pumps.
          </p>
        </section>

        {/* Signals */}
        <section>
          <h3 className="text-blue-400 font-semibold text-base mb-2">Signals (1–25)</h3>
          <p className="mb-2">
            Every bar on a chart is evaluated against 25 named signal conditions derived from
            technical indicators (CCI, RSI, moving averages, price structure, etc.).
          </p>
          <ul className="list-disc list-inside space-y-1">
            <li><span className="text-green-400 font-mono">Signals 1–11</span> — Bullish signals (e.g. L34, L43, FRI34, CCI_READY). The more that fire simultaneously, the higher the Bull Score.</li>
            <li><span className="text-red-400 font-mono">Signals 12–25</span> — Bearish signals. Same logic, contributing to the Bear Score.</li>
            <li>Scores are on a <span className="text-white font-mono">0–10</span> scale. A score of 8+ means multiple strong signals aligned on that bar.</li>
          </ul>
        </section>

        {/* Watchlist */}
        <section>
          <h3 className="text-blue-400 font-semibold text-base mb-2">Watchlist Panel</h3>
          <p className="mb-2">
            The left panel shows all tickers you are tracking. Every 60 seconds it fetches the
            latest bar for each ticker on the selected timeframe and displays:
          </p>
          <ul className="list-disc list-inside space-y-1">
            <li><span className="text-white">Price</span> — current close price</li>
            <li><span className="text-green-400">Bull Score</span> / <span className="text-red-400">Bear Score</span> — how many bullish or bearish signals fired on the latest bar</li>
            <li><span className="text-white">Active signals</span> — the specific signal names that fired</li>
          </ul>
          <p className="mt-2">
            Click any ticker to load its chart. Use the input box in the header to add or remove tickers.
          </p>
        </section>

        {/* Chart */}
        <section>
          <h3 className="text-blue-400 font-semibold text-base mb-2">Candlestick Chart</h3>
          <p className="mb-2">
            The chart shows OHLCV candlestick data for the selected ticker and timeframe, rendered
            with the <span className="text-white font-mono">lightweight-charts</span> library.
            Signal markers are overlaid on the relevant bars:
          </p>
          <ul className="list-disc list-inside space-y-1">
            <li><span className="text-green-400">Green markers below bars</span> — bullish signal fired on that bar</li>
            <li><span className="text-red-400">Red markers above bars</span> — bearish signal fired on that bar</li>
          </ul>
          <p className="mt-2">
            Switch timeframes (1d / 4h / 1h / 30m / 15m) in the header — the chart and all panels
            update to the new timeframe automatically.
          </p>
        </section>

        {/* Combined Scan */}
        <section>
          <h3 className="text-blue-400 font-semibold text-base mb-2">Combined Scan</h3>
          <p className="mb-2">
            Scans every ticker in your watchlist (plus the broader universe) and ranks them by signal
            strength. Results are split into sub-tabs:
          </p>
          <ul className="list-disc list-inside space-y-1">
            <li><span className="text-green-400">Bull</span> — tickers with the highest bull scores</li>
            <li><span className="text-yellow-400">Strong</span> — tickers where both bull and bear scores are elevated (mixed / reversal setup)</li>
            <li><span className="text-orange-400">Fire</span> — extreme bull momentum (score near max)</li>
            <li><span className="text-red-400">Bear</span> — tickers with the highest bear scores</li>
            <li><span className="text-gray-400">All</span> — full list sorted by total score</li>
          </ul>
          <p className="mt-2">
            Use the <span className="text-white font-mono">Min Score</span> slider to filter out weak
            signals. Click <span className="text-white font-mono">Scan Now</span> to force a fresh scan
            rather than waiting for the auto-refresh.
          </p>
        </section>

        {/* Predictor */}
        <section>
          <h3 className="text-blue-400 font-semibold text-base mb-2">Predictor</h3>
          <p className="mb-2">
            For the currently selected ticker, the Predictor looks at the last 2–3 bars (the
            "current pattern") and searches the entire price history for every previous time that
            exact sequence of bars occurred. It then shows what happened on the <em>next</em> bar
            after each historical match:
          </p>
          <ul className="list-disc list-inside space-y-1">
            <li><span className="text-white">Match count</span> — how many times this pattern appeared historically</li>
            <li><span className="text-green-400">Bull %</span> — percentage of matches where the next bar was bullish</li>
            <li><span className="text-red-400">Bear %</span> — percentage of matches where the next bar was bearish</li>
            <li><span className="text-white">Avg gain / Avg loss</span> — average next-bar move in each direction</li>
          </ul>
          <p className="mt-2">
            A high bull % with a large match count gives statistical confidence that the next bar
            is more likely to move up. This is pattern-based probabilistic forecasting — not a guarantee.
          </p>
        </section>

        {/* T/Z Scanner */}
        <section>
          <h3 className="text-blue-400 font-semibold text-base mb-2">T/Z Scanner</h3>
          <p className="mb-2">
            The T/Z Scanner focuses on specific named signal pairs — T-signals and Z-signals — that
            historically mark high-probability turning points. It scans the full ticker universe and
            returns only those where a T or Z signal fired on the last completed bar.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <li>Results show the ticker, signal name, timeframe, and the bar timestamp when it fired</li>
            <li>Click a result row to jump to that ticker's chart</li>
          </ul>
        </section>

        {/* Pump Combos */}
        <section>
          <h3 className="text-blue-400 font-semibold text-base mb-2">Pump Combo Miner</h3>
          <p className="mb-2">
            This tool answers the question: <em>"What 2-bar or 3-bar candlestick sequences most
            reliably precede a large price pump?"</em>
          </p>
          <p className="mb-2">How it works step by step:</p>
          <ol className="list-decimal list-inside space-y-1">
            <li>You set a <span className="text-white">pump threshold</span> (2x = 100% gain, 3x = 200%, 5x = 400%) and a <span className="text-white">look-forward window</span> (10 / 20 / 30 days).</li>
            <li>Click <span className="text-purple-400 font-mono">Mine Pumps</span>. The backend fetches historical daily data for ~500 tickers from Yahoo Finance.</li>
            <li>For each ticker it labels every bar where the price reached the pump threshold within the window as a <em>pump event</em>.</li>
            <li>It then extracts the 2-bar or 3-bar candlestick pattern immediately before each pump event.</li>
            <li>All patterns are aggregated across tickers. Combos that appear most frequently before pumps, with the highest average gain, float to the top.</li>
            <li>Mining runs in the background (~15 minutes). The UI polls every 30 seconds and displays results as soon as they are ready.</li>
          </ol>
          <p className="mt-2">
            The result table shows: <span className="text-white">Combo</span> (the bar sequence),
            <span className="text-white"> Count</span> (how many times it preceded a pump),
            <span className="text-green-400"> Avg Gain</span>, and
            <span className="text-emerald-300"> Max Gain</span>.
          </p>
        </section>

        {/* Timeframes */}
        <section>
          <h3 className="text-blue-400 font-semibold text-base mb-2">Timeframes</h3>
          <p>
            All signal panels (Watchlist, Combined Scan, Predictor, T/Z Scanner) operate on the
            timeframe selected in the header. Switching timeframe re-fetches all data on that
            resolution. The Pump Combo Miner always uses daily bars regardless of the selected
            timeframe, since it is designed for multi-day pump detection.
          </p>
        </section>

        {/* Data flow */}
        <section>
          <h3 className="text-blue-400 font-semibold text-base mb-2">Data Flow</h3>
          <ul className="list-disc list-inside space-y-1">
            <li>Price data is fetched from <span className="text-white font-mono">Yahoo Finance</span> via the <span className="text-white font-mono">yfinance</span> Python library.</li>
            <li>Signals are computed in Python on the backend on every request.</li>
            <li>Pump combo results are stored in a local <span className="text-white font-mono">SQLite</span> database so you do not have to re-mine every session.</li>
            <li>The frontend communicates with the backend over a REST API served by <span className="text-white font-mono">FastAPI</span> on port 8080.</li>
            <li>All user preferences (watchlist, selected ticker, timeframe, active tab) are persisted in <span className="text-white font-mono">localStorage</span> so your state survives a page refresh.</li>
          </ul>
        </section>

      </div>
    </div>
  )
}
