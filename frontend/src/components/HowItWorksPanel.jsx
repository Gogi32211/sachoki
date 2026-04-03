// ── Helpers ───────────────────────────────────────────────────────────────────
function Section({ title, children }) {
  return (
    <section>
      <h3 className="text-blue-400 font-semibold text-base mb-2">{title}</h3>
      {children}
    </section>
  )
}

function Tag({ children, cls = 'text-white' }) {
  return <span className={`font-mono ${cls}`}>{children}</span>
}

function Row({ label, cls = 'text-gray-200', children }) {
  return (
    <li>
      <Tag cls={cls}>{label}</Tag>
      {children && <span className="text-gray-400"> — {children}</span>}
    </li>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
export default function HowItWorksPanel() {
  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 p-6 overflow-auto max-h-[700px]">

      {/* Title */}
      <div className="flex items-baseline gap-3 mb-6">
        <h2 className="text-lg font-bold text-white">How It Works</h2>
        <span className="text-xs text-gray-500">Sachoki Screener — all signal engines explained</span>
      </div>

      <div className="space-y-8 text-sm text-gray-300">

        {/* ── Overview ── */}
        <Section title="Overview">
          <p>
            Sachoki Screener fetches OHLCV data from Yahoo Finance and runs multiple independent
            signal engines on every ticker. Results are stored in SQLite and surfaced through a
            unified scoring system. All engines operate on the same pipeline: fetch → compute →
            score → rank.
          </p>
          <ul className="list-disc list-inside mt-2 space-y-1">
            <li>Backend: <Tag>Python / FastAPI</Tag> — signal computation, SQLite persistence</li>
            <li>Frontend: <Tag>React / Tailwind</Tag> — real-time tables, chart, watchlist</li>
            <li>Data source: <Tag>yfinance</Tag> — Yahoo Finance OHLCV (1W / 1D / 4H / 1H)</li>
            <li>State: watchlist, ticker, TF, active tab persisted in <Tag>localStorage</Tag></li>
          </ul>
        </Section>

        {/* ── TURBO Scanner ── */}
        <Section title="⚡ TURBO Scanner">
          <p className="mb-2">
            The flagship tab. Scans an entire universe (up to ~700 tickers) through every engine
            simultaneously and assigns each ticker a composite <Tag cls="text-lime-300">turbo_score 0–100</Tag>.
          </p>

          {/* Universes */}
          <p className="text-gray-400 mt-3 mb-1 font-medium">Universes</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="S&P 500" cls="text-blue-300">~500 large-caps from Wikipedia + fallback list</Row>
            <Row label="NASDAQ $3–20" cls="text-cyan-300">NASDAQ-100 + ~300 mid-cap NASDAQ stocks, price filtered $3–$20</Row>
            <Row label="NASDAQ $21–50" cls="text-teal-300">Same NASDAQ list, price filtered $21–$50</Row>
            <Row label="Russell 2000" cls="text-orange-300">Small-caps via iShares IWM CSV, ~500 fallback tickers</Row>
          </ul>

          {/* Score tiers */}
          <p className="text-gray-400 mt-3 mb-1 font-medium">Score Tiers</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="Fire ≥ 65" cls="text-lime-300">multiple strong signals aligning across engines</Row>
            <Row label="Strong ≥ 50" cls="text-yellow-300">solid multi-engine confirmation</Row>
            <Row label="Bull ≥ 35" cls="text-blue-300">base bullish setup</Row>
            <Row label="Base ≥ 20" cls="text-gray-300">sparse signals, worth watching</Row>
          </ul>

          {/* Score components */}
          <p className="text-gray-400 mt-3 mb-1 font-medium">Score Components (max ~100)</p>
          <div className="overflow-x-auto">
            <table className="text-xs border-collapse w-full mt-1">
              <thead>
                <tr className="text-gray-500 border-b border-gray-700">
                  <th className="text-left py-1 pr-4">Engine</th>
                  <th className="text-left py-1 pr-4">Signal</th>
                  <th className="text-left py-1">Points</th>
                </tr>
              </thead>
              <tbody className="space-y-0.5">
                {[
                  ['VABS',    'BEST★',               '+15'],
                  ['VABS',    'STRONG',               '+9'],
                  ['VABS',    'VBO↑',                 '+6'],
                  ['ULTRA v2','BEST↑ (FBO+4BF)',      '+8'],
                  ['ULTRA v2','FBO↑ or EB↑',          '+4 each'],
                  ['ULTRA v2','4BF buy',               '+3'],
                  ['ULTRA v2','3UP (VSA effort)',      '+4'],
                  ['260308+L88','L88 signal',          '+5'],
                  ['260308+L88','260308 only',         '+3'],
                  ['Combo',   '🚀 ROCKET',             '+12'],
                  ['Combo',   'BUY 2809',              '+8'],
                  ['Combo',   '3G',                    '+4'],
                  ['T/Z',     'T4 / T6',              '+7'],
                  ['T/Z',     'T1G / T2G',            '+5'],
                  ['T/Z',     'T1 / T2',              '+4'],
                  ['Wyckoff', 'NS / SQ',              '+4 each'],
                  ['WLNBB',   'FRI34',                 '+6'],
                  ['WLNBB',   'FRI43',                 '+4'],
                  ['WLNBB',   'BO↑ / BX↑',            '+3'],
                  ['Wick',    'WICK BULL confirm',     '+3'],
                  ['CISD',    'PPM pattern',           '+2'],
                  ['BR%',     'readiness bonus',       '+0.1 × score, max 8'],
                ].map(([eng, sig, pts], i) => (
                  <tr key={i} className="border-b border-gray-800/50">
                    <td className="py-0.5 pr-4 text-gray-500">{eng}</td>
                    <td className="py-0.5 pr-4 text-gray-300">{sig}</td>
                    <td className="py-0.5 text-lime-400">{pts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Columns */}
          <p className="text-gray-400 mt-3 mb-1 font-medium">Result Columns</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="Score">turbo_score 0–100</Row>
            <Row label="T/Z">active T/Z signal name (T4, T1G, …)</Row>
            <Row label="VABS">volume-action badges (BEST★, STR, VBO↑, ABS, CLB, LD)</Row>
            <Row label="Wyck">Wyckoff badges (NS, SQ, SC, BC, ND)</Row>
            <Row label="Combo">combo badges (🚀, BUY, 3G, RTV, HILO↑, ATR↑, ↑BIAS)</Row>
            <Row label="L-Sig / Ultra">L-signals + ultra signals (FRI34, L34, BEST↑, FBO↑, EB↑, 4BF, L88, 260308…)</Row>
            <Row label="RSI"><Tag cls="text-lime-400">≤30 oversold</Tag> / <Tag cls="text-red-400">≥70 overbought</Tag></Row>
            <Row label="CCI"><Tag cls="text-lime-400">≥+100 bullish momentum</Tag> / <Tag cls="text-red-400">≤-100 bearish</Tag></Row>
            <Row label="BR%">breakout-readiness score 0–100</Row>
          </ul>

          <p className="mt-2 text-gray-500 text-xs">
            ⬇ Export button copies all visible tickers (comma-separated) to clipboard for TradingView watchlist import.
          </p>
        </Section>

        {/* ── T/Z Signals ── */}
        <Section title="T/Z Signals (signal_engine)">
          <p className="mb-2">
            The core engine. Every bar is evaluated against 25 named conditions based on CCI,
            RSI, moving averages, and price structure. Bullish signals are named T1–T11 (and T1G,
            T2G for "golden" variants), bearish are Z1–Z12.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="T4 / T6 / T1G / T2G" cls="text-lime-300">highest-weight signals — strong reversal or trend-start setups</Row>
            <Row label="T1 / T2">classic bullish confirmation signals</Row>
            <Row label="T9 / T10">momentum continuation</Row>
            <Row label="T3 / T11 / T5">moderate or early-stage bullish reads</Row>
          </ul>
        </Section>

        {/* ── VABS ── */}
        <Section title="VABS Engine (vabs_engine) — Volume Action & Wyckoff">
          <p className="mb-2">
            Detects high-conviction volume events. Combines raw volume analysis with Wyckoff
            price–volume theory to identify professional accumulation and markup phases.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="BEST★" cls="text-lime-300">highest-confidence long setup — multiple volume + structure conditions met</Row>
            <Row label="STRONG">strong accumulation signal, 1 condition below BEST</Row>
            <Row label="VBO↑">Volume Breakout — price breaks above range on high volume</Row>
            <Row label="ABS">Absorption — selling absorbed by strong buying</Row>
            <Row label="CLIMB">steady high-volume uptrend</Row>
            <Row label="LOAD">volume loading / accumulation in progress</Row>
            <Row label="NS">No Supply — down bar on very low volume (supply dried up)</Row>
            <Row label="SQ">Stopping Quantity — high volume spike reversal</Row>
            <Row label="SC">Selling Climax — exhaustion of sellers</Row>
            <Row label="ND">No Demand — weak up bar on low volume</Row>
          </ul>
        </Section>

        {/* ── 260308 + L88 ── */}
        <Section title="260308 + L88 (ultra_engine)">
          <p className="mb-2">
            Detects impulsive volume-driven breakout bars and their combination with L-signal context.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="260308" cls="text-purple-300">
              Volume jump signal — current bar volume ≥ 2× previous bar AND bullish candle AND
              current delta ≥ 1.5× previous delta
            </Row>
            <Row label="L88" cls="text-violet-300">
              260308 signal AND (L34 or L43 is active on current OR previous bar) — the volume
              impulse happens at a key WLNBB L-signal level
            </Row>
          </ul>
          <p className="mt-2 text-gray-400 text-xs">
            L88 is a higher-conviction filter: the impulse volume bar occurs right at a support
            level defined by the L-signal system, suggesting institutional activity at a known level.
          </p>
        </Section>

        {/* ── ULTRA v2 ── */}
        <Section title="260315 ULTRA v2 (ultra_engine)">
          <p className="mb-2">
            Extended bar analysis based on the 260315 Pine Script. Detects five distinct setups,
            each targeting a different market micro-structure event.
          </p>
          <ul className="list-disc list-inside space-y-1 mb-3">
            <Row label="EB↑ Extended Bar" cls="text-amber-300">
              Big-body candle (body {'>'} 60% of range) with tiny wicks (each {'<'} 15% of range).
              Signals a strong directional move with no indecision.
            </Row>
            <Row label="FBO↑ Failed Breakout" cls="text-sky-300">
              Price broke below the recent N-bar low but closed back above it — sellers failed.
              Classic bear trap / spring reversal.
            </Row>
            <Row label="4BF Four-Bar Fractal" cls="text-pink-300">
              David Paul fractal: current bar closes above both the prior bar high AND the 3-bars-ago
              high, confirming a short-term trend break.
            </Row>
            <Row label="BEST↑" cls="text-yellow-300">
              FBO↑ + 4BF together on the same bar — the strongest signal in ULTRA v2.
              Failed breakout confirmed by fractal breakout = high-probability long entry.
            </Row>
          </ul>
          <p className="text-gray-400 font-medium mb-1">VSA (Volume Spread Analysis) signals:</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="SQ Stopping Quantity">high-volume reversal spike</Row>
            <Row label="NS No Supply">low-volume down bar with high close — supply exhausted</Row>
            <Row label="ND No Demand">low-volume up bar — bulls lack commitment</Row>
            <Row label="3UP Three Drives Up">effort bar → test bar → confirmation up — professional accumulation sequence</Row>
            <Row label="3DN Three Drives Down">bearish equivalent of 3UP</Row>
          </ul>
        </Section>

        {/* ── WLNBB / L-signals ── */}
        <Section title="WLNBB / L-Signals (wlnbb_engine)">
          <p className="mb-2">
            WLNBB stands for Weighted Level / Narrow Body / Breakout. It identifies specific
            price-level interactions and momentum confirmations.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="L34 / L43" cls="text-blue-300">key support levels based on EMA crossover proximity</Row>
            <Row label="L64 / L22">extended support/resistance levels</Row>
            <Row label="FRI34 / FRI43" cls="text-cyan-300">Friday close at L34/L43 — strongest weekly level confirmation</Row>
            <Row label="BLUE">trend quality signal — price action in bullish structure</Row>
            <Row label="CCI_READY">CCI about to cross signal line from below — early entry</Row>
            <Row label="BO↑ / BX↑">breakout / breakout extension above level</Row>
            <Row label="FUCHSIA RL">price retesting a rejected level from above</Row>
          </ul>
        </Section>

        {/* ── Combo (260323) ── */}
        <Section title="260323 Combo Engine (combo_engine)">
          <p className="mb-2">
            Multi-condition combo signals from the 260323 Pine Script. Stacks several technical
            conditions and fires only when multiple align simultaneously.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="🚀 ROCKET" cls="text-red-300">highest combo signal — 3+ conditions aligned including trend, volume, and structure</Row>
            <Row label="BUY 2809" cls="text-lime-300">260309 buy signal — price/volume/momentum setup</Row>
            <Row label="3G">three-green setup — three consecutive bullish confirmation signals</Row>
            <Row label="RTV">Relative Trading Volume spike aligned with trend</Row>
            <Row label="HILO↑">close in upper half of recent high-low range</Row>
            <Row label="ATR↑ / BB↑">ATR breakout / Bollinger Band squeeze breakout</Row>
            <Row label="↑BIAS">price above all major MAs — bullish bias confirmed</Row>
          </ul>
        </Section>

        {/* ── BR Scan ── */}
        <Section title="BR Scan — Breakout Readiness (br_engine)">
          <p className="mb-2">
            Scores each ticker on how close it is to a potential breakout using a composite of
            structure, volume, and momentum sub-scores.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="BR Score 0–100">weighted sum of: consolidation tightness, volume build-up, trend alignment, proximity to resistance</Row>
            <Row label="≥ 71" cls="text-lime-400">hot — breakout imminent</Row>
            <Row label="≥ 50" cls="text-yellow-300">warm — setup forming</Row>
            <Row label="< 30" cls="text-gray-500">cold — not ready</Row>
          </ul>
          <p className="mt-2 text-gray-400 text-xs">
            BR Scan supports multiple timeframes (1W / 1D / 4H / 1H) selectable in the BR Scan tab.
          </p>
        </Section>

        {/* ── Wick / CISD ── */}
        <Section title="Wick Engine + CISD Engine">
          <p className="mb-2">Two supplemental confirmation engines:</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="WICK BULL" cls="text-emerald-300">
              Long lower wick (≥ 2× body) on above-average volume — rejection of lower prices,
              buyers stepped in strongly
            </Row>
            <Row label="CISD PPM">
              Change In State of Delivery — price momentum pattern that signals a shift from
              distribution to accumulation
            </Row>
            <Row label="CISD SEQ">sequential CISD pattern — weaker version, trend is turning</Row>
          </ul>
        </Section>

        {/* ── RSI / CCI ── */}
        <Section title="RSI & CCI (computed per scan)">
          <p className="mb-2">
            Both indicators are computed fresh on each scan from raw OHLCV data.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="RSI(14)">
              Relative Strength Index — <Tag cls="text-lime-400">≤ 30 oversold</Tag> (potential bounce),{' '}
              <Tag cls="text-red-400">≥ 70 overbought</Tag> (extended)
            </Row>
            <Row label="CCI(20)">
              Commodity Channel Index — <Tag cls="text-lime-400">≥ +100 bullish momentum</Tag>,{' '}
              <Tag cls="text-red-400">≤ -100 bearish momentum</Tag>, ±100 crossings signal entries
            </Row>
          </ul>
        </Section>

        {/* ── Power Scan ── */}
        <Section title="Power Scan">
          <p>
            Focused scan that runs a subset of high-conviction signals and surfaces the
            strongest setups quickly. Uses the same engines as TURBO but with a tighter signal
            threshold — only tickers where multiple power conditions align are shown.
          </p>
        </Section>

        {/* ── Combined Scan ── */}
        <Section title="Combined Scan">
          <p className="mb-2">
            Scans the watchlist plus the broader S&P 500 universe and ranks by a bull/bear score
            derived from T/Z signals and WLNBB L-signals. Results are split into sub-tabs:
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="Bull" cls="text-green-400">highest bull scores</Row>
            <Row label="Fire" cls="text-orange-400">extreme bull momentum</Row>
            <Row label="Bear" cls="text-red-400">highest bear scores</Row>
            <Row label="All" cls="text-gray-400">full list by total score</Row>
          </ul>
        </Section>

        {/* ── 260323 Combo tab ── */}
        <Section title="260323 Combo Tab">
          <p>
            Dedicated scan for the 260323 combo engine signals. Shows the last N bars of active
            signals per ticker. Useful for quickly finding tickers where ROCKET, 3G, or BUY fired
            recently — not just on the last bar.
          </p>
        </Section>

        {/* ── Predictor ── */}
        <Section title="Predictor">
          <p className="mb-2">
            For the selected ticker, looks at the last 2–3 bars (the "pattern") and searches price
            history for every previous time the same bar sequence occurred. Then shows what happened
            on the <em>next</em> bar after each match.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="Match count">how many times this pattern appeared historically</Row>
            <Row label="Bull %" cls="text-green-400">% of matches where next bar was bullish</Row>
            <Row label="Bear %" cls="text-red-400">% of matches where next bar was bearish</Row>
            <Row label="Avg gain / loss">average next-bar move in each direction</Row>
          </ul>
        </Section>

        {/* ── T/Z × L Stats ── */}
        <Section title="T/Z × L Stats">
          <p>
            Statistical matrix showing, for each combination of T/Z signal and L-signal, how
            often they co-occur and what the average next-bar move is. Helps identify which
            signal combinations have the strongest historical follow-through.
          </p>
        </Section>

        {/* ── Pump Combos ── */}
        <Section title="Pump Combo Miner">
          <p className="mb-2">
            Answers: <em>"What candlestick sequences most reliably precede large price pumps?"</em>
          </p>
          <ol className="list-decimal list-inside space-y-1">
            <li>Set a pump threshold (2×, 3×, 5×) and look-forward window (10 / 20 / 30 days)</li>
            <li>Backend fetches ~500 tickers from Yahoo Finance</li>
            <li>Every bar where the price reached threshold within the window is labelled a pump event</li>
            <li>2-bar and 3-bar patterns before each pump are extracted and aggregated</li>
            <li>Patterns with highest frequency + average gain float to the top</li>
          </ol>
        </Section>

        {/* ── Data Flow ── */}
        <Section title="Data Flow">
          <ul className="list-disc list-inside space-y-1">
            <li>Price data: <Tag>Yahoo Finance</Tag> via <Tag>yfinance</Tag> Python library</li>
            <li>Scan results persisted in <Tag>SQLite</Tag> — last 3 runs per universe kept</li>
            <li>Backend REST API: <Tag>FastAPI</Tag> on port 8080</li>
            <li>TURBO scan: <Tag>ThreadPoolExecutor</Tag> (8 workers), 2–4 minutes per universe</li>
            <li>All user state: <Tag>localStorage</Tag> (watchlist, ticker, TF, active tab)</li>
          </ul>
        </Section>

      </div>
    </div>
  )
}
