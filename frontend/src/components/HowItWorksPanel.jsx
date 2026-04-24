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

          <p className="text-gray-400 mt-3 mb-1 font-medium">Score Families (max ~100) — current weights</p>
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
                {/* Backbone */}
                <ScoreRow engine="Backbone (18)" signal="conso_2809 (CON)" pts="+4" />
                <ScoreRow engine="Backbone (18)" signal="tz_bull (T/Z bull state)" pts="+6" />
                <ScoreRow engine="Backbone (18)" signal="conso + tz_bull + bf_buy chain" pts="+8 bonus" />
                <ScoreRow engine="Backbone (18)" signal="conso + tz_bull (partial chain)" pts="+3 bonus" />
                {/* Vol/Accum */}
                <ScoreRow engine="Vol/Accum (22)" signal="VBO↑" pts="+6" />
                <ScoreRow engine="Vol/Accum (22)" signal="NS (no supply)" pts="+5" />
                <ScoreRow engine="Vol/Accum (22)" signal="ABS (absorption)" pts="+5" />
                <ScoreRow engine="Vol/Accum (22)" signal="L88 signal" pts="+5" />
                <ScoreRow engine="Vol/Accum (22)" signal="SQ (stopping quantity)" pts="+4" />
                <ScoreRow engine="Vol/Accum (22)" signal="CLB (climb)" pts="+4" />
                <ScoreRow engine="Vol/Accum (22)" signal="LD (load)" pts="+4" />
                <ScoreRow engine="Vol/Accum (22)" signal="SVS 2809 (vol spike in conso)" pts="+3" />
                <ScoreRow engine="Vol/Accum (22)" signal="UM 2809 (upmove label)" pts="+3" />
                <ScoreRow engine="Vol/Accum (22)" signal="VA (vol/avg >2× crossover)" pts="+3" />
                <ScoreRow engine="Vol/Accum (22)" signal="260308" pts="+3" />
                <ScoreRow engine="Vol/Accum (22)" signal="SC (selling climax)" pts="+2" />
                {/* Breakout */}
                <ScoreRow engine="Breakout (18)" signal="4BF (bf_buy)" pts="+6" />
                <ScoreRow engine="Breakout (18)" signal="FBO↑ (bear trap)" pts="+5" />
                <ScoreRow engine="Breakout (18)" signal="RS+ (vs SPY+IWM)" pts="+5" />
                <ScoreRow engine="Breakout (18)" signal="BO↑ / BX↑" pts="+5" />
                <ScoreRow engine="Breakout (18)" signal="EB↑ / 3↑" pts="+4 each" />
                <ScoreRow engine="Breakout (18)" signal="RS (relative strength)" pts="+3" />
                {/* Combo */}
                <ScoreRow engine="Combo (14)" signal="🚀 ROCKET" pts="+12" />
                <ScoreRow engine="Combo (14)" signal="BUY 2809" pts="+8" />
                <ScoreRow engine="Combo (14)" signal="CD (Bull Dom + B)" pts="+5" />
                <ScoreRow engine="Combo (14)" signal="HILO↑" pts="+4" />
                <ScoreRow engine="Combo (14)" signal="3G gap" pts="+4" />
                <ScoreRow engine="Combo (14)" signal="CA (Bull Att + B)" pts="+3" />
                <ScoreRow engine="Combo (14)" signal="SBC (seqBContLite)" pts="+3" />
                <ScoreRow engine="Combo (14)" signal="RTV" pts="+3" />
                <ScoreRow engine="Combo (14)" signal="CW (Bear Weak + B)" pts="+2" />
                <ScoreRow engine="Combo (14)" signal="ATR↑ / BB↑" pts="+2" />
                {/* L-structure */}
                <ScoreRow engine="L-struct (13)" signal="T4 / T6" pts="+7" />
                <ScoreRow engine="L-struct (13)" signal="T1G / T2G" pts="+5" />
                <ScoreRow engine="L-struct (13)" signal="T1 / T2" pts="+4" />
                <ScoreRow engine="L-struct (13)" signal="T9 / T10" pts="+3" />
                <ScoreRow engine="L-struct (13)" signal="T3 / T11" pts="+2" />
                <ScoreRow engine="L-struct (13)" signal="T5" pts="+1" />
                <ScoreRow engine="L-struct (13)" signal="FRI34" pts="+6" />
                <ScoreRow engine="L-struct (13)" signal="FRI43" pts="+4" />
                <ScoreRow engine="L-struct (13)" signal="L34 (without FRI34)" pts="+3" />
                <ScoreRow engine="L-struct (13)" signal="TZ→3 (Bull Dom flip)" pts="+3 or +4" />
                <ScoreRow engine="L-struct (13)" signal="TZ→2 (Rev Attempt)" pts="+2" />
                <ScoreRow engine="L-struct (13)" signal="BL (blue trend)" pts="+2" />
                <ScoreRow engine="L-struct (13)" signal="CCI ready" pts="+2" />
                {/* Delta */}
                <ScoreRow engine="Delta (12)" signal="d_blast_bull" pts="+6" />
                <ScoreRow engine="Delta (12)" signal="d_spring (Wyckoff)" pts="+6" />
                <ScoreRow engine="Delta (12)" signal="d_strong_bull" pts="+5" />
                <ScoreRow engine="Delta (12)" signal="d_absorb_bull" pts="+4" />
                <ScoreRow engine="Delta (12)" signal="d_surge_bull" pts="+4" />
                <ScoreRow engine="Delta (12)" signal="d_div_bull / vd_div" pts="+3" />
                <ScoreRow engine="Delta (12)" signal="d_cd_bull" pts="+2" />
                {/* EMA cross */}
                <ScoreRow engine="EMA-cross (8)" signal="P66 (cross EMA200+)" pts="+8" />
                <ScoreRow engine="EMA-cross (8)" signal="P55 (cross EMA89+)" pts="+6" />
                <ScoreRow engine="EMA-cross (8)" signal="P89 (cross EMA89)" pts="+4" />
                {/* Context */}
                <ScoreRow engine="Context (uncapped)" signal="WK↑ legacy confirm" pts="+5" />
                <ScoreRow engine="Context (uncapped)" signal="X2G (gap continuation)" pts="+5" />
                <ScoreRow engine="Context (uncapped)" signal="X2 / X1G (wick reversal)" pts="+4 each" />
                <ScoreRow engine="Context (uncapped)" signal="X1 (inside reversal)" pts="+3" />
                <ScoreRow engine="Context (uncapped)" signal="X3 (wick align)" pts="+2" />
              </tbody>
            </table>
          </div>

          <p className="text-gray-400 mt-4 mb-1 font-medium">New signal families — scores TBD (discuss)</p>
          <p className="text-gray-500 text-xs mb-2">
            These signals are computed and shown as badges but not yet added to the turbo_score formula.
            Suggested starting ranges shown — adjust based on hit-rate testing.
          </p>
          <div className="overflow-x-auto">
            <table className="text-xs border-collapse w-full mt-1">
              <thead>
                <tr className="text-gray-500 border-b border-gray-700">
                  <th className="text-left py-1 pr-4">Family</th>
                  <th className="text-left py-1 pr-4">Signal</th>
                  <th className="text-left py-1 pr-4 text-yellow-500">Current pts</th>
                  <th className="text-left py-1 text-sky-400">Suggested range</th>
                </tr>
              </thead>
              <tbody>
                {/* RGTI */}
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">RGTI 260404</td>
                  <td className="py-0.5 pr-4 text-gray-300">LL (Lower-Low setup)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+3 to +5 — entry within pullback</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">RGTI 260404</td>
                  <td className="py-0.5 pr-4 text-gray-300">UP (uptrend align)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+3 to +5</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">RGTI 260404</td>
                  <td className="py-0.5 pr-4 text-gray-300">↑↑ (UPUP multi-TF)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+4 to +6 — confirmed bull alignment</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">RGTI 260404</td>
                  <td className="py-0.5 pr-4 text-gray-300">↑↑↑ (UPUPUP strongest)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+5 to +7</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">RGTI 260404</td>
                  <td className="py-0.5 pr-4 text-gray-300">ORG (orange — bear dominates but reversal)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+2 to +4 — contrarian</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">RGTI 260404</td>
                  <td className="py-0.5 pr-4 text-gray-300">GRN (green — bull stack aligned)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+4 to +6 — strong trend</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">RGTI 260404</td>
                  <td className="py-0.5 pr-4 text-gray-300">GC (green circle — near EMA in bull)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+3 to +5 — pullback entry</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">SMX 260402</td>
                  <td className="py-0.5 pr-4 text-gray-300">SMX (near recent low in uptrend)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+4 to +6 — rare, high-conviction entry</td>
                </tr>
                {/* PARA */}
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">PARA 260420</td>
                  <td className="py-0.5 pr-4 text-gray-300">PREP (pre-parabola compression zone)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+2 to +3 — early context</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">PARA 260420</td>
                  <td className="py-0.5 pr-4 text-gray-300">PARA (parabola start)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+5 to +7</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">PARA 260420</td>
                  <td className="py-0.5 pr-4 text-gray-300">PARA+ (with dry vol or V+E)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+6 to +9 — stronger confirmation</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">PARA 260420</td>
                  <td className="py-0.5 pr-4 text-gray-300">RETEST (pullback re-entry)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+4 to +6 — lower risk entry</td>
                </tr>
                {/* FLY */}
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">FLY 260424</td>
                  <td className="py-0.5 pr-4 text-gray-300">ABCD (full A→B→C→D sequence)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+6 to +8 — full pattern confirmation</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">FLY 260424</td>
                  <td className="py-0.5 pr-4 text-gray-300">CD (C→D with EMA seq)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+3 to +5</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">FLY 260424</td>
                  <td className="py-0.5 pr-4 text-gray-300">BD (B→D with EMA seq)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+3 to +4</td>
                </tr>
                <tr className="border-b border-gray-800/50">
                  <td className="py-0.5 pr-4 text-gray-500">FLY 260424</td>
                  <td className="py-0.5 pr-4 text-gray-300">AD (A→D with EMA seq)</td>
                  <td className="py-0.5 pr-4 text-yellow-500">0</td>
                  <td className="py-0.5 text-sky-400">+4 to +5 — strong base then breakout</td>
                </tr>
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
        <Section title="B1–B11 Signals (260321_B_BUILDER)">
          <p className="mb-2">
            Multi-bar pattern sequences built on top of T/Z codes. Each B signal describes a
            specific 2–3 bar sequence (e.g. "Z10 two bars ago, Z2G one bar ago, T1 now") that
            historically precedes reversals. <span className="text-yellow-300">No RSI filter applied</span> — raw pattern logic only.
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

        {/* ── G Signals ── */}
        <Section title="G1 / G2 / G4 / G6 / G11 Signals (260410_G_BUILDER)">
          <p className="mb-2">
            Stateful setup signals that require a bearish trigger bar first, then fire on the
            first matching bullish confirmation. <span className="text-yellow-300">No RSI filter.</span>
          </p>
          <p className="text-gray-400 mb-1 font-medium">G1 / G2 / G4 / G6 — armed by Z10, Z11 or Z12</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="G1" cls="text-lime-300">first T1 after Z10 / Z11 / Z12 — inside-bar base then reversal</Row>
            <Row label="G2" cls="text-cyan-300">first T1G after Z10 / Z11 / Z12 — gap-up reversal after bearish inside bar</Row>
            <Row label="G4" cls="text-fuchsia-300">first T4 after Z10 / Z11 / Z12 — engulfing reversal after base</Row>
            <Row label="G6" cls="text-orange-300">first T6 after Z10 / Z11 / Z12 — bull-on-bull engulfing after base</Row>
          </ul>
          <p className="text-gray-400 mt-3 mb-1 font-medium">G11 — armed by T10 or T11</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="G11" cls="text-yellow-300">first T1 after T10 or T11 — reversal after bullish inside bars</Row>
          </ul>
          <p className="mt-2 text-gray-500 text-xs">
            State resets after the G signal fires — each new Z10/Z11/Z12 (or T10/T11 for G11) starts a fresh setup.
          </p>
        </Section>

        {/* ── VA + SBC + TZ Transitions ── */}
        <Section title="VA / SBC / TZ Transitions (260402_COMBO_OSC + 260412_TZ_SHIFT)">
          <p className="mb-2">
            Two additional engines contribute new signals that are not part of the core T/Z or B/G families.
          </p>

          <p className="text-gray-400 mb-1 font-medium">VA — ATR Volume Confirm (260402_COMBO_OSC)</p>
          <p className="mb-2 text-gray-400 text-xs">
            Fires when the current bar's volume ratio (volume ÷ 20-bar avg) crosses above 2.0 — i.e.,
            the bar's volume is the first to exceed twice the recent average. Uses a Pine-style
            <span className="font-mono"> ta.crossover</span> condition.
          </p>
          <ul className="list-disc list-inside space-y-1 mb-3">
            <Row label="VA" cls="text-lime-300">vol/avg_vol just crossed above 2× — sudden volume surge</Row>
          </ul>

          <p className="text-gray-400 mb-1 font-medium">SBC — seqBContLite (260412_TZ_SHIFT)</p>
          <p className="mb-2 text-gray-400 text-xs">
            Continuation-lite sequences using T/Z priority codes. Three pattern families:
          </p>
          <ul className="list-disc list-inside space-y-1 mb-3">
            <Row label="SBC" cls="text-violet-300">T1/T1G/T2/T2G/T9 two bars ago → T4 now; or T3/T11/T5 one bar ago → T4/T6; or T4/T2G/T3 one bar ago → T6</Row>
          </ul>

          <p className="text-gray-400 mb-1 font-medium">TZ Transition Signals (260412_TZ_SHIFT)</p>
          <p className="mb-2 text-gray-400 text-xs">
            Detects the bar where the TZ state machine transitions into a new regime. Fires only on
            the first bar of that state — unlike tz_state which persists.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="TZ→3" cls="text-lime-300">TZ state just flipped to Bull Dominance (state 3 ≠ prev state)</Row>
            <Row label="TZ→2" cls="text-cyan-300">TZ state just entered Bull Attempt / Reversal (state 2 ≠ prev state)</Row>
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

        {/* ── Wyckoff Accumulation / Distribution ── */}
        <Section title="Wyckoff Accumulation & Distribution (260225 — wyckoff_engine)">
          <p className="mb-2">
            Two independent state machines that track where a ticker sits in the classic Wyckoff cycle.
            Each carries forward bar-by-bar; cycles reset after completion or after{' '}
            <Tag>cycleMaxBars=160</Tag> bars without completing.
          </p>

          <p className="text-gray-400 mt-2 mb-1 font-medium">Accumulation state machine (bullish)</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="wSC" cls="text-orange-300">
              Selling Climax — wide-spread down bar, high volume ({'>'}1.8× avg), close near low,
              EMA14 below EMA50. Start of base.
            </Row>
            <Row label="AR" cls="text-yellow-300">
              Automatic Rally — first up bar closing above SC high after the climax.
            </Row>
            <Row label="ST" cls="text-amber-300">
              Secondary Test — pullback into SC zone on lower volume; holds above SC low.
            </Row>
            <Row label="SPR" cls="text-lime-300">
              Spring — brief dip below SC low that recovers immediately. Classic bear trap.
              Scored <Tag cls="text-lime-300">+7</Tag> (highest atomic signal in Vol/Accum family).
            </Row>
            <Row label="SOS" cls="text-lime-400">
              Sign of Strength / JAC — strong close above AR high on rising volume. Breakout.
              Scored <Tag cls="text-lime-300">+5</Tag>.
            </Row>
            <Row label="LPS" cls="text-green-300">
              Last Point of Support — minor dip after SOS on low volume; holds support.
              Buy-the-dip confirmation. Scored <Tag cls="text-lime-300">+5</Tag>.
            </Row>
            <Row label="ACC" cls="text-cyan-300">Accumulation active (states 1–4, pre-breakout). Context +2.</Row>
            <Row label="MKP" cls="text-lime-300">Markup phase (states 5–6, post-SOS). Context +3.</Row>
          </ul>

          <p className="text-gray-400 mt-3 mb-1 font-medium">Distribution state machine (bearish)</p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="wBC" cls="text-red-300">
              Buying Climax — wide-spread up bar, high volume, close near high, EMA14 above EMA50. Potential top.
            </Row>
            <Row label="ARD" cls="text-pink-300">Automatic Reaction — sharp decline below BC low.</Row>
            <Row label="STD" cls="text-rose-300">Secondary Test (dist) — rally back toward BC zone, lower volume, stalls.</Row>
            <Row label="UTAD" cls="text-orange-400">
              Upthrust After Distribution — brief push above BC high that closes back below. Bull trap.
            </Row>
            <Row label="SOW" cls="text-red-400">
              Sign of Weakness — strong close below ARD low. Breakdown.
              Reduces bull score by <Tag cls="text-red-400">−4</Tag>.
            </Row>
            <Row label="LPSY" cls="text-rose-400">Last Point of Supply — minor rally on low volume that fails.</Row>
            <Row label="DST" cls="text-red-400">Distribution active (states 1–4). Context −1.</Row>
            <Row label="MKD" cls="text-red-500">Markdown phase (states 5–6, post-SOW). Context −3.</Row>
          </ul>

          <p className="mt-2 text-gray-500 text-xs">
            Parameters: cycleMaxBars=160, emaFast=14, emaSlow=50, hiVolMult=1.8, wideSpreadATR=1.2.
          </p>
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
            <Row label="WK↑" cls="text-emerald-300">Long lower wick (≥ 2× body) on above-average volume — strong buyer rejection (legacy)</Row>
            <Row label="C+- / C+--">Change In State of Delivery — shift from distribution to accumulation</Row>
          </ul>
        </Section>

        {/* ── Wick X signals ── */}
        <Section title="260402_WICK — X2G / X2 / X1G / X1 / X3 (wick_engine)">
          <p className="mb-2">
            Wick-filtered reversal signals. Each checks the current bar's lower-wick dominance
            (lower wick ≥ 2× upper wick) combined with the wick shape of the prior bar(s).
            Four distinct patterns, ranked by strength:
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="X2G" cls="text-cyan-300">
              T2G continuation: prev bullish, current <em>gap-opens</em> above prev close
              and closes above prev close. Both bars have aligned wick shape.
              Scored <Tag cls="text-lime-300">+5</Tag>.
            </Row>
            <Row label="X2" cls="text-sky-300">
              T2 continuation: prev bullish, current opens <em>inside</em> prev body
              (at/below prev close) and closes above prev close. Wicks aligned.
              Scored <Tag cls="text-lime-300">+4</Tag>.
            </Row>
            <Row label="X1G" cls="text-lime-300">
              T1G reversal: prev bearish, current <em>gap-opens</em> above prev close and
              closes above prev open. Prev bar had dominant upper wick (exhaustion).
              Scored <Tag cls="text-lime-300">+4</Tag>.
            </Row>
            <Row label="X1" cls="text-green-300">
              T1 reversal: prev bearish, current opens at/above prev close (prev open ≥ curr open)
              and closes above prev open. Prev bar had dominant upper wick.
              Scored <Tag cls="text-lime-300">+3</Tag>.
            </Row>
            <Row label="X3" cls="text-yellow-300">
              Generic bullish wick alignment across 2–3 bars. Fires only when X2G/X2/X1G/X1 absent.
              Scored <Tag cls="text-lime-300">+2</Tag>.
            </Row>
          </ul>
          <p className="mt-2 text-gray-500 text-xs">
            wickMult=2.0: dominant wick must be ≥ 2× the opposing wick to qualify.
          </p>
        </Section>

        {/* ── Signal Tier Guide ── */}
        <Section title="Signal Tier Guide — How to Read Setups">
          <p className="mb-3 text-gray-400">
            Not all signals are equal. The key principle: <span className="text-white font-semibold">cross-engine confluence &gt; same-engine cluster</span>.
            Multiple independent engines agreeing on direction is stronger than many signals from the same family.
            The TURBO score reason line shows <Tag cls="text-lime-300">⚡×N</Tag> where N = number of distinct engine families active.
          </p>

          {/* A-tier */}
          <div className="mb-4">
            <p className="text-white font-semibold mb-1">Tier A — Strong surfaced signals</p>
            <p className="text-gray-500 text-xs mb-2">
              Already high-conviction on their own. Note: BEST★ and BUY/🚀 are themselves composite — they already passed multiple sub-conditions internally.
            </p>
            <div className="flex flex-wrap gap-2 text-xs">
              {[
                ['BEST★', 'text-lime-300',   'VABS composite — highest vol+structure conditions met'],
                ['wSPR',  'text-lime-300',   'Wyckoff Spring — Phase C reversal below support'],
                ['T4/T6', 'text-violet-300', 'Candlestick T4/T6 state — strongest T/Z pattern'],
                ['VBO↑',  'text-green-300',  'Volume Breakout Up — breakout confirmed by volume'],
                ['P66',   'text-lime-300',   'EMA66 cross ↑ — trend regime confirmed'],
                ['🚀/BUY','text-red-300',    'Rocket / Buy 2809 — multi-condition combo trigger'],
              ].map(([lbl, cls, desc]) => (
                <div key={lbl} className="bg-gray-800 rounded px-2 py-1">
                  <span className={`font-mono font-semibold ${cls}`}>{lbl}</span>
                  <span className="text-gray-500 ml-1">{desc}</span>
                </div>
              ))}
            </div>
          </div>

          {/* B-tier */}
          <div className="mb-4">
            <p className="text-white font-semibold mb-1">Tier B — Context amplifiers</p>
            <p className="text-gray-500 text-xs mb-2">
              Rarely sufficient alone, but significantly strengthen a Tier A signal.
              These represent regime confirmation, structural re-test, or readiness context.
            </p>
            <div className="flex flex-wrap gap-2 text-xs">
              {[
                ['TZ→3', 'text-lime-300',   'T/Z just flipped bullish — fresh regime change'],
                ['LPS',  'text-green-300',  'Wyckoff Last Point of Support — structure re-test before markup'],
                ['FRI34','text-cyan-400',   'WLNBB weekly L-structure confirmation'],
                ['RS+',  'text-lime-300',   'Relative strength vs SPY+IWM — sector leadership'],
                ['BR%',  'text-lime-400',   'Break-readiness context score ≥ 71'],
                ['CA/CD','text-lime-300',   'T/Z confluence context signals'],
              ].map(([lbl, cls, desc]) => (
                <div key={lbl} className="bg-gray-800 rounded px-2 py-1">
                  <span className={`font-mono font-semibold ${cls}`}>{lbl}</span>
                  <span className="text-gray-500 ml-1">{desc}</span>
                </div>
              ))}
            </div>
          </div>

          {/* C-tier combos */}
          <div className="mb-4">
            <p className="text-white font-semibold mb-1">Tier C — Best cross-engine combos (with timing)</p>
            <p className="text-gray-500 text-xs mb-2">
              These pairs/triples draw from genuinely different observation models — structure, state, flow, breakout.
              <Tag cls="text-sky-300"> [E]</Tag> = early phase (Phase C / fresh flip, best R/R, less confirmation).
              <Tag cls="text-orange-300"> [L]</Tag> = late/confirmed (strong conviction, possibly extended entry).
            </p>
            <table className="text-xs w-full max-w-2xl">
              <thead>
                <tr className="text-gray-500 text-left border-b border-gray-800">
                  <th className="pb-1 pr-4">Combo</th>
                  <th className="pb-1 pr-4">Engines</th>
                  <th className="pb-1 pr-4">Phase</th>
                  <th className="pb-1">Why it works</th>
                </tr>
              </thead>
              <tbody className="space-y-1">
                {[
                  ['wSPR + VBO↑',         'Wyk + VABS',         'Early',   'Structure spring confirmed by volume breakout'],
                  ['T4/T6 + TZ→3',        'T/Z + T/Z regime',   'Early→Mid','Strongest T/Z pattern into fresh bullish flip'],
                  ['FRI34 + VBO↑ + RS+',  'L + VABS + Brk',     'Mid',     'Weekly structure + volume + sector leadership'],
                  ['wLPS + T4 + BR%',     'Wyk + T/Z + Context','Mid',     'Re-test confirmed by state + readiness context'],
                  ['wSPR + dSPR + Ab↑',  'Wyk + Δ + Δ',        'Early',   'Same theme cluster — high conviction, less diversity'],
                  ['B1/B10 + CD + VBO↑',  'B/G + Cmb + VABS',   'Late [L]','Confirmed breakout — conviction high, entry may be extended'],
                ].map(([combo, engines, phase, why]) => (
                  <tr key={combo} className="border-b border-gray-800/40">
                    <td className="py-1 pr-4 font-mono text-white">{combo}</td>
                    <td className="py-1 pr-4 text-gray-500">{engines}</td>
                    <td className={`py-1 pr-4 ${phase.includes('[L]') ? 'text-orange-400' : phase === 'Early' ? 'text-sky-300' : 'text-gray-400'}`}>{phase}</td>
                    <td className="py-1 text-gray-500">{why}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Caution box */}
          <div className="bg-gray-800/50 border border-gray-700 rounded p-3 text-xs text-gray-400 mt-2">
            <span className="text-yellow-300 font-semibold">Caution:</span> The Corr panel shows co-occurrence frequency, not forward expectancy.
            High co-occurrence ≠ high win rate. Use Corr to <span className="text-white">discover</span> candidate combos,
            then validate with Predictor bull% and actual risk/reward.
            Missing dimension: drawdown and timing in the price cycle.
          </div>
        </Section>

        {/* ── FLY ABCD ── */}
        <Section title="FLY ABCD EMA DP (260424 — fly_engine)">
          <p className="mb-2">
            Sequence detector using T/Z role codes to find ABCD patterns, confirmed by an EMA
            drop-then-recovery context at the anchor bar. D must fire on the current bar.
          </p>
          <p className="text-gray-400 mb-1 font-medium">Role assignments (by T/Z code)</p>
          <ul className="list-disc list-inside space-y-1 mb-3">
            <Row label="A (anchor)" cls="text-red-400">ZC {'{3,4}'} — Z1G, Z2G: strong bearish reversal bar</Row>
            <Row label="B (base)"   cls="text-orange-300">ZC {'{9,1,2,5,10,8,12,7}'} — various bearish codes: continuation or trap bar</Row>
            <Row label="C (coil)"   cls="text-cyan-300">BC {'{9,10,12,7,5}'} — T3,T11,T12,T9,T1: moderate bullish recovery bar</Row>
            <Row label="D (drive)"  cls="text-lime-300">BC {'{1,2,4,6}'} — T4,T6,T2G,T2: strong bullish breakout bar (fires on current bar)</Row>
          </ul>
          <p className="text-gray-400 mb-1 font-medium">EMA context filter (checked at each anchor bar)</p>
          <p className="text-gray-400 text-xs mb-2">
            Requires: an EMA drop (E1) followed by an EMA cross-up (E2), both within 30 bars of the
            anchor. E1 must be older than E2. This ensures the setup had a dip-and-recovery structure.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="ABCD" cls="text-lime-300">Full sequence A→B→C on current D, EMA seq at A, all within 30 bars</Row>
            <Row label="CD"   cls="text-cyan-300">C within 20 bars before D, EMA seq at C</Row>
            <Row label="BD"   cls="text-blue-300">B within 20 bars before D, EMA seq at B</Row>
            <Row label="AD"   cls="text-violet-300">A within 20 bars before D, EMA seq at A</Row>
          </ul>
        </Section>

        {/* ── RGTI + SMX ── */}
        <Section title="RGTI 260404 + SMX 260402 — Multi-TF EMA Alignment (rgti_engine)">
          <p className="mb-2">
            Multi-timeframe EMA alignment patterns. Requires 4H, 1H, and 15m data simultaneously.
            For daily/weekly scans, 1H is fetched and resampled to 4H count-based.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <Row label="LL"  cls="text-purple-300">Lower-Low: price above EMA50/20 on 4H but EMAs showing deceleration; 1H+15m aligned</Row>
            <Row label="UP"  cls="text-blue-300">Uptrend: 4H EMA stack bullish + EMA9 above EMA200; 1H/15m price/EMA confirmed</Row>
            <Row label="↑↑"  cls="text-fuchsia-300">UPUP: 4H+1H+15m all show aligned bull stack; 1H bullish candle</Row>
            <Row label="↑↑↑" cls="text-sky-300">UPUPUP: EMA200 driving above EMA50 across timeframes</Row>
            <Row label="ORG" cls="text-orange-300">Orange: bear EMA stack on 4H/1H but 15m crossing up — early reversal</Row>
            <Row label="GRN" cls="text-green-300">Green: 4H+1H EMA deeply stacked bull; 15m breakout candle</Row>
            <Row label="GC"  cls="text-emerald-300">Green Circle: 4H bull stack but price dipping toward EMA — pullback entry</Row>
            <Row label="SMX" cls="text-lime-300">SMX: 4H bear stack + 1H recovery + 15m reversal + near 20-bar low (high R/R entry)</Row>
          </ul>
        </Section>

        {/* ── PARA ── */}
        <Section title="PARA 260420 — Parabola Start Detector v3.6 (para_engine)">
          <p className="mb-2">
            Stateful campaign system that detects the start of parabolic moves following base compression.
            Four sequential signal types from setup → entry → retest.
          </p>
          <ul className="list-disc list-inside space-y-1 mb-2">
            <Row label="PREP"  cls="text-green-300">Pre-parabola zone: base compressed + EMA aligned + price below breakout level. First bar entering zone.</Row>
            <Row label="PARA"  cls="text-lime-300">Parabola Start: base compressed → seed bar (breakout + volume ignition + candle quality) → follow-through. Campaign opens.</Row>
            <Row label="PARA+" cls="text-cyan-300">PARA with dry volume or V+E (volume+EMA) coincidence — highest conviction breakout.</Row>
            <Row label="RETEST" cls="text-emerald-300">Pullback re-entry within open campaign: price dips near EMA, holds, bullish candle with volume. Up to 2 retests per campaign.</Row>
          </ul>
          <p className="text-gray-500 text-xs">
            Campaign resets on: close below EMA50, consecutive closes below EMA20, or 18%+ drawdown from campaign high below EMA9.
            Daily chart uses relaxed parameters (wider base tolerance, more memory).
          </p>
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
