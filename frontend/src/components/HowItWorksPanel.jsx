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

function ScoreRow({ engine, signal, pts }) {
  return (
    <tr className="border-b border-gray-800/50">
      <td className="py-0.5 pr-4 text-gray-500">{engine}</td>
      <td className="py-0.5 pr-4 text-gray-300">{signal}</td>
      <td className="py-0.5 text-lime-400">{pts}</td>
    </tr>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
export default function HowItWorksPanel() {
  return (
    <div className="bg-gray-900 rounded-xl border border-gray-800 p-6 overflow-auto max-h-[700px]">

      <div className="flex items-baseline gap-3 mb-6">
        <h2 className="text-lg font-bold text-white">How It Works</h2>
        <span className="text-xs text-gray-500">Sachoki Screener — all signal engines explained</span>
      </div>

      <div className="space-y-8 text-sm text-gray-300">

        {/* ── Overview ── */}
        <Section title="Overview">
          <p>
            Sachoki Screener fetches OHLCV data and runs multiple independent signal engines on
            every ticker. Results are stored in SQLite and ranked through a unified scoring system.
            All engines share the same pipeline: fetch → compute → score → rank.
          </p>
          <ul className="list-disc list-inside mt-2 space-y-1">
            <li>Backend: <Tag>Python / FastAPI</Tag> — signal computation, SQLite persistence</li>
            <li>Frontend: <Tag>React / Tailwind</Tag> — real-time tables, chart, watchlist</li>
            <li>Primary data: <Tag>Massive API</Tag> (Polygon-compatible) — fast, no rate limits</li>
            <li>Fallback data: <Tag>yfinance</Tag> — Yahoo Finance OHLCV when Massive unavailable</li>
            <li>Timeframes: <Tag>1W / 1D / 4H / 1H</Tag></li>
            <li>State: watchlist, ticker, TF, active tab persisted in <Tag>localStorage</Tag></li>
          </ul>
        </Section>

        {/* ── TURBO Scanner ── */}
        <Section title="⚡ TURBO Scanner">
          <p className="mb-2">
            The flagship tab. Scans an entire universe (up to ~2200 tickers) through every engine
            simultaneously and assigns each ticker a composite <Tag cls="text-lime-300">turbo_score 0–100</Tag>.
          </p>

          <p className="text-gray-400 mt-3 mb-1 font-medium">Universes</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="S&P 500" cls="text-blue-300">~500 large-caps</Row>
            <Row label="NASDAQ" cls="text-cyan-300">~1100 NASDAQ stocks across all price ranges</Row>
            <Row label="Russell 2K" cls="text-orange-300">~2000 small-caps via iShares IWM CSV</Row>
            <Row label="All US" cls="text-gray-300">combined universe, capped at 2000 random sample</Row>
          </ul>

          <p className="text-gray-400 mt-3 mb-1 font-medium">Score Tiers</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="Fire ≥ 65" cls="text-lime-300">multiple strong signals aligning across engines</Row>
            <Row label="Strong ≥ 50" cls="text-yellow-300">solid multi-engine confirmation</Row>
            <Row label="Bull ≥ 35" cls="text-blue-300">base bullish setup</Row>
            <Row label="Base ≥ 20" cls="text-gray-300">sparse signals, worth watching</Row>
          </ul>

          <p className="text-gray-400 mt-3 mb-1 font-medium">Score Families (max ~100)</p>
          <div className="overflow-x-auto">
            <table className="text-xs border-collapse w-full mt-1">
              <thead>
                <tr className="text-gray-500 border-b border-gray-700">
                  <th className="text-left py-1 pr-4">Family (cap)</th>
                  <th className="text-left py-1 pr-4">Signal</th>
                  <th className="text-left py-1">Points</th>
                </tr>
              </thead>
              <tbody>
                <ScoreRow engine="VABS (22)" signal="BEST★" pts="+15" />
                <ScoreRow engine="VABS (22)" signal="STRONG" pts="+9" />
                <ScoreRow engine="VABS (22)" signal="VBO↑" pts="+6" />
                <ScoreRow engine="Breakout (15)" signal="BEST↑ (FBO+4BF)" pts="+8" />
                <ScoreRow engine="Breakout (15)" signal="FBO↑ or EB↑" pts="+4 each" />
                <ScoreRow engine="Breakout (15)" signal="RS+ (vs SPY+IWM)" pts="+5" />
                <ScoreRow engine="Breakout (15)" signal="RS (relative strength)" pts="+3" />
                <ScoreRow engine="Breakout (15)" signal="BO↑ / BX↑" pts="+3" />
                <ScoreRow engine="Breakout (15)" signal="L88 signal" pts="+5" />
                <ScoreRow engine="Combo (14)" signal="🚀 ROCKET" pts="+12" />
                <ScoreRow engine="Combo (14)" signal="BUY 2809" pts="+8" />
                <ScoreRow engine="Combo (14)" signal="3G gap" pts="+4" />
                <ScoreRow engine="Combo (14)" signal="CD (Bull Dom + B)" pts="+5" />
                <ScoreRow engine="Combo (14)" signal="CA (Bull Att + B)" pts="+3" />
                <ScoreRow engine="Combo (14)" signal="CW (Bear Weak + B)" pts="+2" />
                <ScoreRow engine="L-struct (13)" signal="T4 / T6" pts="+7" />
                <ScoreRow engine="L-struct (13)" signal="T1G / T2G" pts="+5" />
                <ScoreRow engine="L-struct (13)" signal="FRI34" pts="+6" />
                <ScoreRow engine="Delta (12)" signal="Spring (Wyckoff)" pts="+6" />
                <ScoreRow engine="Delta (12)" signal="Blast↑ (delta surge)" pts="+6" />
                <ScoreRow engine="EMA-cross (8)" signal="P66 (cross EMA200+)" pts="+8" />
                <ScoreRow engine="EMA-cross (8)" signal="P55 (cross EMA89+)" pts="+6" />
                <ScoreRow engine="EMA-cross (8)" signal="P89 (cross EMA89)" pts="+4" />
                <ScoreRow engine="Context" signal="Wick Bull confirm" pts="+3" />
                <ScoreRow engine="Context" signal="BR% readiness" pts="+0.1×score, max 8" />
              </tbody>
            </table>
          </div>

          <p className="mt-2 text-gray-500 text-xs">
            N= selector (1/3/5/10) controls how many recent bars are checked for each signal —
            wider window catches setups that fired a few bars ago.
          </p>
        </Section>

        {/* ── T/Z Signals ── */}
        <Section title="T/Z Signals (signal_engine)">
          <p className="mb-2">
            Core candlestick pattern engine. Every bar gets exactly ONE signal (or NONE) based on
            open/close/high/low relationships vs the previous bar. A priority system prevents
            double-counting.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="T4 / T6" cls="text-lime-300">bullish engulfing patterns — strongest reversal signals</Row>
            <Row label="T1G / T2G" cls="text-lime-300">golden variants — gap-up reversals and continuation</Row>
            <Row label="T1 / T2">classic bullish setups</Row>
            <Row label="T9 / T10">inside-bar bullish setups</Row>
            <Row label="T3 / T11 / T5">moderate or early-stage bullish reads</Row>
            <Row label="Z4 / Z6" cls="text-red-400">bearish engulfing — strongest reversal</Row>
            <Row label="Z1G / Z2G" cls="text-red-400">bearish golden variants</Row>
            <Row label="Z7" cls="text-gray-400">doji — indecision, only when no T or Z fires</Row>
          </ul>
        </Section>

        {/* ── B Signals ── */}
        <Section title="B1–B11 Signals (260321 / 260410)">
          <p className="mb-2">
            Multi-bar pattern sequences built on top of T/Z codes. Each B signal describes a
            specific 2–3 bar sequence (e.g. "Z10 two bars ago, Z2G one bar ago, T1 now") that
            historically precedes reversals.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="B1" cls="text-orange-400">T3→T4 sequences and Z2G/Z6 absorption into T4</Row>
            <Row label="B2" cls="text-gray-300">Z2-led pullback into T2/T2G continuation</Row>
            <Row label="B3" cls="text-sky-300">T6/T3 momentum with Z3/Z4 absorption sequences</Row>
            <Row label="B4" cls="text-gray-300">Z4 trap into T1G / T2G recovery</Row>
            <Row label="B5" cls="text-cyan-400">T5 pivot into T2G/T6 — classic spring</Row>
            <Row label="B6" cls="text-gray-300">Z1G/Z3 multi-bar base into T1G/T9</Row>
            <Row label="B7" cls="text-green-400">T9/T2 momentum stacking into T2G/T4</Row>
            <Row label="B8" cls="text-blue-400">repeated T1G or Z2G→T1G recovery</Row>
            <Row label="B9" cls="text-gray-300">Z9 trap / Z10 sequence into T4</Row>
            <Row label="B10" cls="text-lime-400">Z10 multi-bar base with Z2G→T1/T6 breakout</Row>
            <Row label="B11" cls="text-fuchsia-400">Z11/Z9/Z6 into Z10 base then reversal</Row>
          </ul>

          <p className="text-gray-400 mt-3 mb-1 font-medium">TZ State Machine — regime filter</p>
          <p className="mb-2 text-gray-400 text-xs">
            Before surfacing B signals, the engine computes the current market regime using rolling
            dominance scores (T/Z pattern weights over 3 and 7 bars).
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="State 3: Bull Dom" cls="text-lime-300">bull patterns dominate both fast+slow windows, price above EMA20+50 + swing high</Row>
            <Row label="State 2: Bull Att" cls="text-cyan-300">bull patterns dominate fast window, price above EMA20</Row>
            <Row label="State 1: Bear Weak" cls="text-yellow-300">bears dominated slow window but bulls picking up in fast window</Row>
            <Row label="State 4: Bull Weak" cls="text-yellow-400">bulls dominated slow but bears picking up</Row>
            <Row label="State 5: Bear Att" cls="text-orange-400">bear patterns dominate fast window</Row>
            <Row label="State 0: Bear Dom" cls="text-red-400">bear patterns dominate both windows, price below EMA20+50</Row>
          </ul>

          <p className="text-gray-400 mt-3 mb-1 font-medium">Confluence signals (B × State)</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="CD" cls="text-lime-300">any B signal in Bull Dominance state — highest conviction</Row>
            <Row label="CA" cls="text-cyan-300">any B signal in Bull Attempt state</Row>
            <Row label="CW" cls="text-yellow-300">any B signal in Bear Weakening state — early reversal</Row>
          </ul>
        </Section>

        {/* ── Delta Engine ── */}
        <Section title="Delta V2 Engine (260403 — delta_engine)">
          <p className="mb-2">
            Order-flow proxy using Open-Adjusted CLV (Close Location Value). Separates each bar
            into wick and body components relative to the open price to estimate buy vs sell volume.
          </p>
          <div className="bg-gray-800/50 rounded p-2 text-xs font-mono mb-3 text-gray-300">
            <div>body_top  = max(open, close)</div>
            <div>body_bot  = min(open, close)</div>
            <div>bull_body = body_size  if close ≥ open  else 0</div>
            <div>buy_vol   = volume × (lower_wick + bull_body) / range</div>
            <div>delta     = buy_vol − sell_vol</div>
          </div>
          <ul className="list-disc list-inside space-y-1">
            <Row label="Strong Bull/Bear" cls="text-lime-300">delta strongly positive/negative relative to volume</Row>
            <Row label="Absorb Bull/Bear">buy/sell volume absorption — large opposing volume absorbed</Row>
            <Row label="Div Bull/Bear">delta divergence — price makes new extreme but delta doesn't</Row>
            <Row label="CD Bull/Bear">cumulative delta trend</Row>
            <Row label="Surge/Blast Bull">escalating delta intensity</Row>
            <Row label="vd_div (NS)" cls="text-teal-300">volume↓ + delta↑ on same bar — No Supply signal</Row>
            <Row label="Spring" cls="text-lime-300">Wyckoff Spring — divergence + absorption together (bear trap)</Row>
            <Row label="Upthrust" cls="text-red-400">Wyckoff Upthrust — bull trap version of spring</Row>
          </ul>
          <p className="mt-2 text-gray-500 text-xs">
            Note: delta is computed from OHLCV only (no bid/ask). Accuracy ±15-25% vs real order flow.
            Wick-heavy bars benefit most from V2 vs V1.
          </p>
        </Section>

        {/* ── PREUP / PREDN ── */}
        <Section title="PREUP / PREDN — EMA Cross Signals (260331_TZ_OSC)">
          <p className="mb-2">
            Fires on the bar where price crosses an EMA: bar opens on one side and closes on the
            other. Priority chain ensures only the strongest EMA crossed is labelled.
          </p>
          <p className="text-gray-400 mb-1 font-medium">PREUP (EMA cross ↑)</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="P66" cls="text-lime-300">crossed EMA200 + at least one smaller EMA — very strong</Row>
            <Row label="P55" cls="text-emerald-300">crossed EMA89 + another EMA</Row>
            <Row label="P89" cls="text-teal-300">crossed EMA89 alone</Row>
            <Row label="P3 / P2 / P50">crossed EMA9+20+50 / EMA9+20 / EMA50</Row>
          </ul>
          <p className="text-gray-400 mt-2 mb-1 font-medium">PREDN (EMA drop ↓)</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="D66" cls="text-red-300">dropped below EMA200 + another EMA</Row>
            <Row label="D55" cls="text-red-400">dropped below EMA89 + another</Row>
            <Row label="D89 / D3 / D2 / D50" cls="text-orange-400">progressively weaker EMA breaks</Row>
          </ul>
        </Section>

        {/* ── 2809 Phase Labels ── */}
        <Section title="2809 Phase Labels (260402_COMBO_OSC)">
          <p className="mb-2">
            Three phase markers from the 2809 setup cycle:
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="UM" cls="text-teal-300">Upmove — first bar where ROC(40) ≥ 8% with EMA trend and volume spike</Row>
            <Row label="SVS" cls="text-orange-300">Strong Volume Spike — volume ratio crosses 1.4× avg on a green bar</Row>
            <Row label="CON" cls="text-yellow-300">Consolidation — first bar where ATR/range/EMA-gap tightens enough</Row>
            <Row label="BUY" cls="text-lime-300">Buy Here — upmove + consolidation + breakout + cooldown all met simultaneously</Row>
          </ul>
        </Section>

        {/* ── RS / RS+ ── */}
        <Section title="RS / RS+ — Relative Strength">
          <p className="mb-2">
            Identifies tickers that are outperforming the broad market on a given bar.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="RS" cls="text-green-400">ticker up ≥ 0.5% while SPY AND IWM are both down ≥ 0.3% on the same bar</Row>
            <Row label="RS+" cls="text-lime-300">RS condition AND ticker is in the high-volume bucket (B or VB)</Row>
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
            <Row label="CLB">Climb — steady high-volume uptrend</Row>
            <Row label="LD">Load — volume loading / accumulation in progress</Row>
            <Row label="NS">No Supply — down bar on very low volume</Row>
            <Row label="SQ">Stopping Quantity — high volume spike reversal</Row>
            <Row label="SC">Selling Climax — exhaustion of sellers</Row>
            <Row label="ND">No Demand — weak up bar on low volume</Row>
          </ul>
        </Section>

        {/* ── ULTRA v2 ── */}
        <Section title="260315 ULTRA v2 (ultra_engine)">
          <ul className="list-disc list-inside space-y-1 mb-2">
            <Row label="EB↑" cls="text-amber-300">Extended Bar — big body, tiny wicks. Strong directional move.</Row>
            <Row label="FBO↑" cls="text-sky-300">Failed Breakout — broke below N-bar low then closed back above. Bear trap.</Row>
            <Row label="4BF" cls="text-pink-300">Four-Bar Fractal — close above both prior-bar high and 3-bars-ago high.</Row>
            <Row label="BEST↑" cls="text-yellow-300">FBO↑ + 4BF together — highest ULTRA signal.</Row>
            <Row label="3↑">Three Drives Up — effort → test → confirmation accumulation sequence.</Row>
          </ul>
        </Section>

        {/* ── 260308 + L88 ── */}
        <Section title="260308 + L88 (ultra_engine)">
          <ul className="list-disc list-inside space-y-1">
            <Row label="260308" cls="text-purple-300">
              Volume jump — bar volume ≥ 2× previous AND bullish candle AND delta ≥ 1.5× previous delta
            </Row>
            <Row label="L88" cls="text-violet-300">
              260308 signal occurring at an active L34/L43 level — institutional activity at a known support
            </Row>
          </ul>
        </Section>

        {/* ── WLNBB ── */}
        <Section title="WLNBB / L-Signals (wlnbb_engine)">
          <ul className="list-disc list-inside space-y-1">
            <Row label="L34 / L43" cls="text-blue-300">key support levels based on EMA crossover proximity</Row>
            <Row label="FRI34 / FRI43" cls="text-cyan-300">Friday close at L34/L43 — strongest weekly confirmation</Row>
            <Row label="L555">stateful multi-bar escalating L-signal (7-bar window)</Row>
            <Row label="L2L4">ONLY_L2 or ONLY_L4 — clean signal without higher-L interference</Row>
            <Row label="BO↑ / BX↑">breakout / breakout extension above L-level body high</Row>
            <Row label="BE↑ / BE↓">bar engulfs prior L-level body (candle opens below body, closes above)</Row>
            <Row label="BLUE">trend quality — price action in bullish structure</Row>
            <Row label="CCI">CCI about to cross signal line from below — early entry signal</Row>
          </ul>
        </Section>

        {/* ── BR / Wick / CISD ── */}
        <Section title="BR Score + Wick + CISD">
          <ul className="list-disc list-inside space-y-1">
            <Row label="BR 0–100">Breakout Readiness — consolidation tightness + volume build-up + trend + proximity to resistance</Row>
            <Row label="WK↑" cls="text-emerald-300">Long lower wick (≥ 2× body) on above-average volume — strong buyer rejection</Row>
            <Row label="C+- / C+--">Change In State of Delivery — shift from distribution to accumulation</Row>
          </ul>
        </Section>

        {/* ── Predictor ── */}
        <Section title="Predictor">
          <p className="mb-2">
            Pattern-match prediction engine. Looks at the last 2–3 signal bars (the "pattern")
            and finds every previous occurrence of the same sequence, then shows next-bar statistics.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="This Ticker">per-ticker history (~5–20 matches for common patterns)</Row>
            <Row label="SP500 Pooled" cls="text-blue-300">cross-universe statistics built from ~500 tickers — thousands of pattern instances for high-confidence stats</Row>
            <Row label="Bull %" cls="text-green-400">% of matches where next bar was bullish</Row>
            <Row label="Avg gain">average next-bar move after the pattern</Row>
          </ul>
        </Section>

        {/* ── Data Flow ── */}
        <Section title="Data Flow">
          <ul className="list-disc list-inside space-y-1">
            <li>Primary: <Tag>Massive API</Tag> (Polygon-compatible) — set <Tag>MASSIVE_API_KEY</Tag> env var</li>
            <li>Fallback: <Tag>yfinance</Tag> — used automatically when Massive unavailable</li>
            <li>Scan results: <Tag>SQLite</Tag> at <Tag>/tmp/scanner.db</Tag> — last 3 runs per universe kept</li>
            <li>TURBO scan: <Tag>ThreadPoolExecutor</Tag> (8 workers), ~3–6 min per 2000-ticker universe</li>
            <li>Market-open check: uses <Tag>America/New_York</Tag> timezone — bars kept after 16:15 ET</li>
            <li>All user state: <Tag>localStorage</Tag> (watchlist, ticker, TF, active tab)</li>
          </ul>
        </Section>

      </div>
    </div>
  )
}
