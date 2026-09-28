import { useState, useRef, useEffect, useCallback, useMemo, useReducer, Fragment } from 'react'
import { api } from '../api'
import CodeCandleChart from './CodeCandleChart'
import { lbalPick } from '../lbalMode'
import { atrForecast, computeAtr14, forecastCsvCells, FORECAST_CSV_HEADERS, fmtForecast } from '../atrForecast'
import { requestGex, getGex, subscribeGex } from '../gexStore'
import { V4_ALL_GROUPS } from './UltraScanPanel'
import { v4Score, v4FiredLabels, v4Fired } from '../lib/v4Score'
import { withTurnCount, TURN_LIFT } from '../lib/turnCount'
import { withRowSeq, ROW_ORDER, ROW_SHORT, ROW_LIFT, PAIR_LIFT } from '../lib/rowSeq'
import { withTopPairs, TOP_SINGLES, TOP_PAIRS } from '../lib/topPairs'
import { V4_WEIGHTS } from '../lib/v4Weights'

const TF_OPTIONS = ['1w', '1d', '4h', '1h', '30m', '15m']
const CELL_W  = 64   // px per bar column
const HDR_W   = 46   // px for the sticky label column
const MINI_H  = 24   // px height of mini-candle row

const BUCKET_HEX = { W: '#c3c0d3', L: '#0099ff', N: '#ffd000', B: '#e48100', VB: '#b02020' }
const PREUP_SET  = new Set(['P2', 'P3', 'P50', 'P89'])

// WLNBB 1H volume-class palette for the optional 1H-decomposition row (with1H)
const H_VOL_BG  = ['#9E9E9E', '#1E88E5', '#F9A825', '#EF6C00', '#C62828']
const H_VOL_TXT = ['#111', '#fff', '#111', '#fff', '#fff']
const H_VOL_LBL = ['W', 'L', 'N', 'B', 'VB']

// Bottom-Anatomy verdict row (with1H): 🔻 REVERSAL = accumulation-bottom structure
// (held/tested floor + multi-TF absorption + intraday reversal) · 🔺 CONTINUES = markup
// (upper-range, momentum, higher-low). A DEFINITION/detector, not a trade signal.
function AnatRow({ bars, hoursMap }) {
  return (
    <tr className="border-t border-white/[0.06] hover:bg-md-surface-high/20">
      <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1 text-right
                     border-r border-white/[0.08] font-mono whitespace-nowrap"
          style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13, lineHeight: 1 }}>▽△</td>
      {bars.map((b, i) => {
        const a = hoursMap[String(b.date).slice(0, 10)]?.anat
        let el = null
        if (a?.v === 'rev')
          el = a.rs
            ? <span className="rounded px-1 font-bold" style={{ fontSize: 11, background: '#b45309', color: '#fff7ed', boxShadow: '0 0 0 1px #fbbf24' }}
                title={`🔻💪 PRECISE bottom — anatomy REVERSAL + RS-intact (close/SPY > EMA200). score ${a.s}/8 (loc ${a.loc}, abs ${a.abs}, rev ${a.rev}). RS is the discriminator that separates a durable bottom from a mid-range absorption pause (🧊 coil-floor logic).`}>🔻💪{a.s}</span>
            : <span className="rounded px-1 font-bold" style={{ fontSize: 11, background: '#7c2d12', color: '#fdba74' }}
                title={`🔻 STRUCTURAL bottom-anatomy · score ${a.s}/8 (loc ${a.loc}, abs ${a.abs}, rev ${a.rev}). Structure only (~1.37× enriched for real lows) — NO RS gate, so lower precision. 🔻💪 (with RS) = the precise version.`}>🔻{a.s}</span>
        else if (a?.v === 'shake')
          el = <span className="rounded px-1 font-bold" style={{ fontSize: 11, background: '#4c1d95', color: '#ddd6fe' }}
            title={`🌀 SHAKEOUT / spring — bearish-engulf at a held floor, weak close, but a LATE hi-vol T-reversal at the close (the tell the daily bar hides). The 🔻 detector misses this (low-late/weak-close). Intraday-only signal (base −1.57 → +1H-tell −0.48 vs random −2.52). score ${a.s}`}>🌀{a.s}</span>
        else if (a?.v === 'cont')
          el = <span className="rounded px-1 font-bold" style={{ fontSize: 11, background: '#14532d', color: '#86efac' }}
            title={`🔺 CONTINUATION (markup) · score ${a.s}`}>🔺</span>
        return (
          <td key={i} className="px-0 py-px text-center border-r border-white/[0.05]"
              style={{ width: CELL_W, minWidth: CELL_W }}>{el}</td>
        )
      })}
    </tr>
  )
}

// ⛔ NO-VOLUME-EVENT row (2026-07-26): the session's biggest 15m bar never reached 2.5× that
// session's own average volume. Validated across ALL 29 TZ/L signal codes — on such a day every
// signal's median falls 4-8 points, and it holds inside every price band (not a mega-cap artifact).
// Rare per name (~0.5-3% for most, more on smooth-volume mega-caps), so the row is mostly empty.
function VolRow({ bars }) {
  return (
    <tr className="border-t border-white/[0.06] hover:bg-md-surface-high/20">
      <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1 text-right
                     border-r border-white/[0.08] font-mono whitespace-nowrap"
          style={{ width: HDR_W, minWidth: HDR_W, fontSize: 12, lineHeight: 1 }}
          title="⛔ NO intraday volume EVENT — the biggest 15m bar of that session never reached 2.5× the session's own average volume. Validated across ALL 29 TZ/L signal codes ($21-377): on such a day EVERY signal's median drops 4-8 points (Z9 −8.0, T1 −7.1, L5 −6.7; only Z11 resists at −0.8) and PF falls below 1.0 — and it holds separately inside $21-89, $89-377 and $377+, so it is not a mega-cap artifact. Treat any signal printed on a flagged bar as untrustworthy.">⛔</td>
      {bars.map((b, i) => (
        <td key={i} className="px-0 py-px text-center border-r border-white/[0.05]"
            style={{ width: CELL_W, minWidth: CELL_W }}>
          {b.no_vol_event ? (
            <span className="rounded px-1 font-bold" style={{ fontSize: 10, background: '#7f1d1d', color: '#fecaca' }}
              title="⛔ no intraday volume event on this bar — the day's biggest 15m bar stayed under 2.5× the session average. Every TZ/L signal's median falls 4-8 points on such days.">⛔</span>
          ) : null}
        </td>
      ))}
    </tr>
  )
}

// 📐 divergence funnel rows (2026-07-28, user: "the RSI/CCI interaction should be visible on
// EVERY bar — it often reaches the zone but no signal fires because of the restrictions").
// The fired edge is rare by construction (~0.16 buy per ticker per YEAR), so a row that only
// marks completions hides ~12 of every 13 occurrences. Stages: 1 raw divergence · 2 IN the
// oversold/overbought zone but BLOCKED by the RS gate (the near-miss worth seeing) · 3 signal ·
// 4 deep tier. One row per oscillator so agreement/disagreement between them is visible — CCI
// validated independently (+1.76/5-5yr with RS), and requiring BOTH is redundant (+1.77, 4/5).
// Glyphs must be READABLE on the dark matrix. The first cut used '·' at 35% opacity for
// stage 1 and it was effectively invisible — the row looked empty even where it wasn't
// (user: "only the balls show up, the other fields are empty"). Stage 2 is the whole point
// of this row (in the zone, blocked by RS), so it gets the loudest non-emoji treatment.
const DIV_STAGE = {
  1: { txt: '◦', cls: 'text-sky-300/70',                    sz: 13 },
  2: { txt: '◉', cls: 'text-amber-300 font-bold',           sz: 14 },
  3: { txt: '🟢', cls: '',                                   sz: 10 },
  4: { txt: '🟢⁺', cls: '',                                  sz: 10 },
}
const DIV_STAGE_T = {
  1: { txt: '◦', cls: 'text-rose-300/70',                   sz: 13 },
  2: { txt: '◉', cls: 'text-amber-300 font-bold',           sz: 14 },
  3: { txt: '🔻', cls: '',                                   sz: 10 },
}

function DivFunnelRow({ bars }) {
  // ONE row, merged (2026-07-28). It shipped as two rows (📐R / 📐C) on the theory that
  // disagreement between the oscillators was informative. Measured: on KO 5 of 8 marked bars
  // disagree — but NO validated rule uses a single oscillator. The divergence edge is RSI-only
  // (requiring both was redundant: +1.77 vs +1.76) and the 🔄 reclaim edge already REQUIRES
  // both inside the mask. So "CCI is in the zone, RSI is not" is not actionable, and the second
  // row was visual noise. The cell now shows the STRONGER of the two stages; the tooltip names
  // which oscillator produced it, its pivot value, and the RS state.
  const NAME = { r: 'RSI', c: 'CCI' }
  return (
    <tr className="border-t border-white/[0.06] hover:bg-md-surface-high/20">
      <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1 text-right
                     border-r border-white/[0.08] font-mono whitespace-nowrap"
          style={{ width: HDR_W, minWidth: HDR_W, fontSize: 12, lineHeight: 1 }}
          title={`📐 DIVERGENCE FUNNEL (RSI + CCI merged) vs 🏆RS. Every stage is shown, not just the fires — a completed signal happens ~0.16 times per ticker per YEAR, so a completions-only row would look broken.
◦  raw divergence: price made a LOWER low while the oscillator made a HIGHER low (bull), or a HIGHER high with a LOWER oscillator high (bear). Alone this is WORTHLESS (−0.64, worse than its own opposite cell) and deeper oversold makes it WORSE — naked divergence catches falling knives.
◉  reached the oversold/overbought zone but the 🏆RS gate BLOCKED it — the near-miss. Bull needs RS INTACT (leadership held = quality dip); bear needs RS BROKEN (leadership failing = distribution).
🟢 / 🟢⁺  full long signal / deep tier → +2.58 and +3.34 median, win 56-58%, PF 1.65-1.75, 5/5 positive years, worst +1.4/+1.5. Monotone across five oversold cuts, TRAIN ≈ TEST, DSR 1.000, replicates on CCI.
🔻  the SUPPRESSOR (RSI>65 + RS broken): holding a long through it returns −2.94%/win43/PF0.85, positive in only 2 of 6 years. Never a short — our short side is closed 0/29.
Hover a cell to see which oscillator fired it.`}>📐</td>
      {bars.map((b, i) => {
        // strongest stage across the two oscillators; remember which produced it
        let bs = 0, ts = 0, bw = [], tw = []
        for (const k of ['r', 'c']) {
          const vb = b[`dv${k}_b`] || 0, vt = b[`dv${k}_t`] || 0
          if (vb) { if (vb > bs) bs = vb; bw.push(NAME[k]) }
          if (vt) { if (vt > ts) ts = vt; tw.push(NAME[k]) }
        }
        const st = bs ? DIV_STAGE[bs] : ts ? DIV_STAGE_T[ts] : null
        if (!st) return <td key={i} className="px-0 py-px border-r border-white/[0.05]"
                            style={{ width: CELL_W, minWidth: CELL_W }} />
        const who = (bs ? bw : tw).join(' + ')
        const val = bs ? (b.dv_rlo ?? b.dv_clo) : (b.dv_rhi ?? b.dv_chi)
        const stage = bs || ts
        const why = stage === 1 ? 'raw divergence — not yet far enough into the zone'
                  : stage === 2 ? (bs ? 'in the oversold zone but 🏆RS is BROKEN — signal blocked'
                                      : 'overbought divergence but 🏆RS still INTACT — suppressor not armed')
                  : bs ? 'full signal — higher oscillator low + RS intact'
                       : 'suppressor — lower oscillator high + RS broken'
        return (
          <td key={i} className="px-0 py-px text-center border-r border-white/[0.05]"
              style={{ width: CELL_W, minWidth: CELL_W }}>
            <span className={`font-bold ${st.cls}`} style={{ fontSize: st.sz }}
              title={`📐 ${who}: ${why}${val != null ? ` · pivot ${val}` : ''}${b.dv_rs != null ? ` · RS ${b.dv_rs ? 'intact' : 'broken'}` : ''}`}>
              {st.txt}</span>
          </td>
        )
      })}
    </tr>
  )
}

// ⚖️ VRP header chip (2026-07-26): IV vs ATR-realized vol dissonance — current snapshot only
// (no IV history exists; accumulating in gex_edge_log). Descriptive, not a signal.
function VrpLine({ ticker }) {
  const [, force] = useReducer(x => x + 1, 0)
  useEffect(() => { requestGex(ticker); return subscribeGex(force) }, [ticker])
  const g = getGex(ticker)
  if (!g || !g.available || g.vrp == null) return null
  const cls = g.vrp_state === 'EVENT-PRICED' ? 'text-amber-300'
            : g.vrp_state === 'COMPLACENT' ? 'text-violet-300' : 'text-md-on-surface-var/60'
  return (
    <span className={`text-xs ml-2 ${cls}`}
      title={`⚖️ VRP = ATM IV ÷ ATR-realized vol. Calibrated on 40 liquid names (p25 0.77 · MEDIAN 0.81 · p75 1.31) — ATR is range-based, so 'fair' sits near 0.8, not 1.0. EVENT-PRICED ≥1.35: options expensive, a move is already priced in (earnings/news); IV deflates if nothing happens. COMPLACENT ≤0.65: the stock moves far more than options price in — often precedes a volatility expansion. 0.65-1.35 = the normal state, no signal. Snapshot only (no IV history — accumulating forward in gex_edge_log). Descriptive gauge, NOT a validated signal.`}>
      ⚖️ VRP {g.vrp.toFixed(2)} · IV {g.atm_iv}% vs real {g.rv_atr}%{g.vrp_state !== 'BALANCED' ? ` · ${g.vrp_state}` : ''}
    </span>
  )
}

// ⚖️ VRP matrix row — IV↔ATR-realized dissonance, now across history.
//
// It showed one cell for three weeks and the comment here explained why: the options
// provider sells a SNAPSHOT, there is no chain history to buy, so nothing could be
// backfilled. True when written, and it stopped being true without anyone noticing —
// gex_edge_logger has been writing a row per edge-fire since 2026-07-22 and the history
// it was waiting for now exists. A "not yet" left in a comment does not expire on its own.
//
// SPARSE, AND FOR A REASON WORTH SHOWING
// A day is captured when the ticker fired an edge that day. AAPL: 8 days of 18. So a gap
// means NOTHING FIRED — not a holiday, not a failed fetch — and the row says so on hover
// rather than leaving the reader to assume breakage.
function VrpRow({ bars, ticker }) {
  const [, force] = useReducer(x => x + 1, 0)
  const [hist, setHist] = useState(null)
  useEffect(() => { requestGex(ticker); return subscribeGex(force) }, [ticker])
  useEffect(() => {
    let live = true
    setHist(null)
    api.gexHistory(ticker).then(h => { if (live) setHist(h) }).catch(() => {})
    return () => { live = false }
  }, [ticker])

  const g = getGex(ticker)
  const byDate = {}
  for (const r of (hist?.rows || [])) byDate[r.date] = r

  const state = (vrp) => vrp == null ? null
    : vrp >= 1.35 ? 'EVENT-PRICED' : vrp <= 0.65 ? 'COMPLACENT' : 'BALANCED'

  const chip = (vrp, st, tip) => (
    <span className="rounded px-1 font-bold" style={{ fontSize: 10,
        background: st === 'EVENT-PRICED' ? '#78350f' : st === 'COMPLACENT' ? '#4c1d95' : '#1f2937',
        color: st === 'EVENT-PRICED' ? '#fcd34d' : st === 'COMPLACENT' ? '#ddd6fe' : '#9ca3af' }}
      title={tip}>⚖️{vrp.toFixed(2)}</span>
  )

  const cell = (b, isLast) => {
    const key = String(b.date).slice(0, 10)
    const h = byDate[key]
    if (h && h.vrp != null) {
      const st = state(h.vrp)
      return chip(h.vrp, st, `⚖️ VRP ${h.vrp.toFixed(2)} on ${key} — ATM IV ${h.atm_iv}% vs ` +
        `ATR-realized ${h.rv_atr}%. ${st}. GEX regime ${h.regime}` +
        (h.edges?.length ? ` · captured because ${h.edges.join(', ')} fired that day` : '') +
        `. Captured live at the time; options history cannot be backfilled.`)
    }
    // today's live snapshot, for the bar the log has not been written for yet
    if (isLast && g?.available && g.vrp != null) {
      return chip(g.vrp, g.vrp_state,
        `⚖️ VRP ${g.vrp} — ATM IV ${g.atm_iv}% vs ATR-realized ${g.rv_atr}%. ${g.vrp_state}. ` +
        `LIVE snapshot, not from the log (today has not been written yet).`)
    }
    return null
  }

  const captured = hist?.captured_days ?? 0
  const label = hist
    ? `⚖️ VRP — ATM IV ÷ ATR-realized vol. ${captured} day${captured === 1 ? '' : 's'} captured ` +
      `for ${ticker}, out of ${hist.log_days ?? 0} the log has been running (${hist.log_from} → ${hist.log_to}). ` +
      `A day is recorded only when this ticker fired an edge, so an empty cell means NOTHING FIRED — ` +
      `not missing data. Options history cannot be bought, only accumulated forward. ` +
      `Amber = EVENT-PRICED (≥1.35) · violet = COMPLACENT (≤0.65). Descriptive, not a signal.`
    : '⚖️ VRP — loading captured history…'

  return (
    <tr className="border-t border-white/[0.06] hover:bg-md-surface-high/20">
      <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1 text-right
                     border-r border-white/[0.08] font-mono whitespace-nowrap"
          style={{ width: HDR_W, minWidth: HDR_W, fontSize: 12, lineHeight: 1 }}
          title={label}>⚖️<span className="text-[9px] opacity-60">{captured || ''}</span></td>
      {bars.map((b, i) => (
        <td key={i} className="px-0 py-px text-center border-r border-white/[0.05]"
            style={{ width: CELL_W, minWidth: CELL_W }}>
          {cell(b, i === bars.length - 1)}
        </td>
      ))}
    </tr>
  )
}

// ⏱ ATR time-to-target row (2026-07-26): per-bar historical forecast — typical days for the
// stock to move +10% given THAT bar's ATR% (Wilder ATR14 from the loaded OHLC). OOS-calibrated
// volatility law days ≈ (10/ATR%)^0.67. TIMING/expectation context, NOT a buy signal.
function TtRow({ bars }) {
  const atrArr = computeAtr14(bars)
  return (
    <tr className="border-t border-white/[0.06] hover:bg-md-surface-high/20">
      <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1 text-right
                     border-r border-white/[0.08] font-mono whitespace-nowrap"
          style={{ width: HDR_W, minWidth: HDR_W, fontSize: 12, lineHeight: 1 }}
          title="⏱d — ATR time-to-target: typical (median) DAYS for this stock to move +10%, from each bar's ATR% (volatility). Small number = fast mover (targets/stops arrive sooner); big = slow. ≤7d lights up blue. OOS-calibrated (TRAIN 2021-23 ≈ TEST 2024-26). Hover a cell for hit-rate, +25% and −10% stop-timing. A volatility/timing gauge, NOT a buy signal.">⏱d</td>
      {bars.map((b, i) => {
        const ap = b.close > 0 && atrArr[i] > 0 ? atrArr[i] / b.close : 0
        const f = ap > 0 ? atrForecast(ap) : null
        const fast = f && f.up10.days <= 7
        return (
          <td key={i} className="px-0 py-px text-center border-r border-white/[0.05]"
              style={{ width: CELL_W, minWidth: CELL_W }}>
            {f ? (
              <span className={`font-mono ${fast ? 'text-sky-300' : 'text-sky-500/70'}`} style={{ fontSize: 11 }}
                title={`⏱ ATR ${f.atrPct}% → +10% ~${f.up10.days} დღეში (hit ${f.up10.hit}%) · +25% ~${f.up25.days}d · −10% ~${f.dn10.days}d (stop-timing). პატარა = სწრაფი აქცია`}>
                {f.up10.days}d
              </span>
            ) : null}
          </td>
        )
      })}
    </tr>
  )
}

// One matrix row that decomposes each day into its 1H bars (TZ token + L + vol class),
// stacked in the direction price moved — inserted between L and SCORE when with1H is on.
// Same cell width / fonts as every other ChipRow so it matches the Superchart exactly.
function Row1H({ bars, hoursMap }) {
  return (
    <tr className="border-t border-white/[0.06] hover:bg-md-surface-high/20">
      <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1 text-right
                     border-r border-white/[0.08] font-mono whitespace-nowrap"
          style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13, lineHeight: 1 }}>1H</td>
      {bars.map((b, i) => {
        const day = hoursMap[String(b.date).slice(0, 10)]
        const hrs = day ? (day.up ? day.hours.slice().reverse() : day.hours) : []
        return (
          <td key={i} className="px-0 py-px text-center border-r border-white/[0.05] align-top"
              style={{ width: CELL_W, minWidth: CELL_W }}>
            <div className="flex flex-col gap-px items-stretch px-px">
              {hrs.map((h, j) => (
                <span key={j}
                  title={`${h.t} · ${h.tok}${h.l ? ' ' + h.l : ''} · vol ${H_VOL_LBL[h.v]}${h.up ? ' ▲' : ' ▼'}`}
                  className="rounded border border-white/10 px-1 py-px font-mono leading-none text-center truncate"
                  style={{ fontSize: 12, background: H_VOL_BG[h.v], color: H_VOL_TXT[h.v] }}>
                  {h.tok !== '-' ? h.tok : '·'}{h.l || ''}
                </span>
              ))}
            </div>
          </td>
        )
      })}
    </tr>
  )
}

// ── EMA-cross chip capping (2026-07-29) ──────────────────────────────────────────────
// A single bar can raise up to six D codes (and four P codes), which stacked the T/D and Z
// cells five deep and blew up the matrix height. Cap each of those rows at two chips.
//
// The order below is NOT a strength ranking. Path-sim over six years, $21-377, found all
// twelve D/P codes indistinguishable from baseline (−0.47…−0.83 around a −0.69 baseline,
// every one 4/6 years, pf 1.07-1.14) — the doc's old "P2 +5.5%" is an artifact of its
// vol≥10× filter, where the VOLUME is the edge, not the cross. So this simply mirrors the
// hierarchy the row's own colours already encode: the slow/structural EMAs first
// (D66=EMA200, D55=EMA89-reclaim, D89=EMA89, D50, D3=9&20&50, D2=9&20).
// WLNBB event chips hidden from the L row (2026-07-29, user request — display only; the
// backend still computes them and the CSV export still carries SIG_CCI / SIG_BX_DN etc.).
// Both families were measured and came back empty:
//   CCI0R / CCI — the CCI-zero-reclaim pair. Measured 2026-07-28: no edge, and the score
//     weights riding on them pointed the WRONG way, so they were stripped from
//     replay_engine / canonical_scoring_engine / prebreak_v3 in a89ef7d.
//   BX↑ / BX↓ / BO↑ / BO↓ — the confluence kill-list ("confirmation costs"): the
//     breakout family is noise once the state layers are already on the bar.
//   L64 / L43 / L22 — the WLNBB *event* chips, which are NOT the bar's own l_sig (that
//     one renders with a `·` prefix and is untouched). l_sig never carries L43/L64/L22,
//     so nothing here overlaps with the L34/L46 absorption work.
// NOT hidden: L34 (the flagship absorption line), BE↑/↓, CCIB.
const HIDDEN_L_SIGS = new Set(['CCI', 'CCI0R', 'BX↑', 'BX↓', 'BO↑', 'BO↓',
                               'L64', 'L43', 'L22'])

// I-row chips hidden 2026-07-29 after the first measurement of the four (path-sim, 6yr,
// $21-377, baseline −0.63). Display only — both are still computed and exported.
//   UM  (ema9>20>50 & ROC40>=8% & max-vol40>1.4x)  −1.40 / win 46.5 / 3-6yr, 0.77pp WORSE
//       than baseline, and NOT-UM is −0.29. It also POISONS validated setups as a gate:
//       D+L1 −2.66, QZ-Capit −2.51. The worst of the four.
//   BB↑ (close>BB_upper(20,2) & vol crosses 1.5x & RSI>55)  −1.55 / win 46.1, 0.92pp worse.
//       Its +15 in ultra_score — the largest single bonus there — was removed the same day.
// Both are pure strength-chase, which is exactly what "fade strength, buy absorbed
// weakness" predicts should lose. SVS (−0.69 ≈ baseline) is left visible: it is merely
// empty, not harmful. CONSO stays too — its absence is a real suppressor (NOT-CONSO −3.67).
//   CONSO — hidden for a different reason: it is not harmful, it is REDUNDANT per bar. It
//       fires on 69% of the universe (299 of 300 bars on AAPL), so as a chip it marks almost
//       every bar and carries no information at that level. Its real content is the OTHER
//       side — NOT-CONSO is −3.67/win 43.6, a genuine suppressor — and that is now expressed
//       where it belongs, as the ❄️ gate behind Washout🧊CONSO and RTB-Base🧊CONSO on the
//       Replay board. SVS stays visible: empty, but rare (12 bars) and therefore harmless.
const HIDDEN_I_SIGS = new Set(['UM', 'BB↑', 'CONSO'])

const MAX_ROW_SIGS = 2
const DP_RANK = ['D66', 'D55', 'D89', 'D50', 'D3', 'D2',
                 'P66', 'P55', 'P89', 'P50', 'P3', 'P2']
const dpOrder = (a, b) => {
  const ia = DP_RANK.indexOf(a), ib = DP_RANK.indexOf(b)
  return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib)
}
// tz (at most one, from b.tz) always survives; the rest of the budget goes to the
// highest-ranked EMA-cross codes. Returns [shown, hiddenCount] so the tooltip can own up.
const capRow = (tz, dp) => {
  const ranked = dp.slice().sort(dpOrder)
  const keep = ranked.slice(0, Math.max(0, MAX_ROW_SIGS - tz.length))
  return [[...tz, ...keep], ranked.slice(keep.length)]
}
const zAll = (b) => [
  b.tz?.startsWith('Z') ? [b.tz] : [],
  (b.combo ?? []).filter(s => PREUP_SET.has(s)),
]
const tdAll = (b) => [
  b.tz?.startsWith('T') ? [b.tz] : [],
  [b.sig_d66 && 'D66', b.sig_d55 && 'D55', b.sig_d89 && 'D89',
   b.sig_d50 && 'D50', b.sig_d3 && 'D3', b.sig_d2 && 'D2'].filter(Boolean),
]
const cappedTitle = (all) => (s, b) => {
  const hidden = capRow(...all(b))[1]
  return hidden.length ? `${s}\nასევე: ${hidden.join(' · ')}` : undefined
}

// Row definitions — getSigs(bar) returns array of signal labels
const ROWS = [
  {
    key: 'z',
    label: 'Z',
    getSigs: (b) => capRow(...zAll(b))[0],
    sigTitle: cappedTitle(zAll),
    chipCls: (s) => PREUP_SET.has(s)
      ? 'bg-gray-700 text-white'
      : 'bg-red-900 text-red-300',
  },
  {
    // T (bullish) and D (PREDN bearish) merged onto one row — they almost never
    // co-occur on the same bar, so sharing a line keeps the matrix compact.
    key: 'td',
    label: 'T/D',
    getSigs: (b) => capRow(...tdAll(b))[0],
    sigTitle: cappedTitle(tdAll),
    chipCls: (s) => {
      if (s === 'D66' || s === 'D55') return 'bg-rose-900 text-rose-300 font-bold'
      if (s === 'D89')                return 'bg-red-900 text-red-300 font-semibold'
      if (s.startsWith('D'))          return 'bg-orange-900 text-orange-300'
      return 'bg-green-900 text-green-300'   // T-signals
    },
  },
  {
    key: 'l',
    label: 'L',
    // every bar's own VSA L-line (l_sig) first, then the WLNBB event chips (2026-07-19 —
    // the plain L was missing and it is load-bearing for the user's pattern work)
    getSigs: (b) => {
      const ev = (b.l ?? []).filter(s => !HIDDEN_L_SIGS.has(s))
      const sup = ['', '¹', '²', '³']
      const tag = b.l_sig === 'L34' && b.l34_grade > 0 ? `·L34${sup[b.l34_grade] ?? '³'}` : (b.l_sig ? `·${b.l_sig}` : null)
      const base = tag && !ev.includes(b.l_sig) ? [tag] : []
      return [...base, ...ev]
    },
    sigTitle: (s, b) => {
      if (!s.startsWith('·L34') || !b) return undefined
      const red = Number(b.close) < Number(b.open)
      if (red && b.rev_buy && b.h4_rev_today)
        return '💠 TRIPLE — red-L34 + 🟢REV + ▲4H on ONE bar: +2.05%/win47/PF1.29, TRAIN +1.98 ≈ TEST +1.80 (the most era-balanced enhancement). If a same-level red-L34 visited within the prior 20 bars this is also the L34camp→REV shape (+3.26%/med+2.27/PF1.62, both bear years positive).'
      if (red && (b.l34_grade ?? 0) > 0) return `red L34, grade +${b.l34_grade}/3 (axes: near-25bar-low / RSI<40 / same-level campaign — validated ladder: grade5 ps +3.46% vs grade0 −0.48%). Institutional absorption line.`
      if (red) return 'red L34 — institutional absorption line (heavy volume, pressed down intraday, held above prior close). The type that carries the edge; green L34 on reversal bars is the trap.'
      return undefined
    },
    chipCls: (s, b) => {
      // heavy institutional L (triple-confluence leg: 🟢REV + red-L34 + ▲4H, era-balanced).
      // Only the RED type (close<open, absorbed weakness) lights up — green L34 on a
      // reversal bar is the trap type (−1.01%, PF 0.87), so it stays muted.
      // 💠 (2026-07-21): when the SAME bar also has 🟢REV + ▲4H, the chip goes full-bright.
      if (s.startsWith('·L34') && b && Number(b.close) < Number(b.open) && b.rev_buy && b.h4_rev_today)
        return 'bg-amber-400 text-amber-950 ring-2 ring-amber-100 font-bold'
      if (s.startsWith('·L34') && b && Number(b.close) < Number(b.open))
        return (b.l34_grade ?? 0) >= 2
          ? 'bg-amber-600 text-amber-50 ring-1 ring-amber-300 font-bold'
          : 'bg-amber-900 text-amber-200 ring-1 ring-amber-400/70 font-semibold'
      // ·L46 in blue (2026-07-29, user request) — it is the other absorption line and was
      // lost in the muted `·` styling. Placed AFTER the red-L34 amber rules so the validated
      // grade colouring keeps priority, and before the generic muted rule.
      if (s.startsWith('·L46')) return 'bg-sky-900 text-sky-300 font-semibold'
      if (s.startsWith('·')) return 'bg-slate-800/80 text-slate-300'   // the bar's own l_sig (muted)
      // L34 event chip in green (2026-07-29, user request). Only this one — the bar's own
      // ·L34 keeps its validated colouring: amber/graded when RED (absorbed weakness, the
      // type that carries the edge) and muted when green, because a green L34 on a reversal
      // bar is the trap type (−1.01%, PF 0.87) and must not look inviting.
      if (s === 'L34')                           return 'bg-green-900 text-green-300 font-semibold'
      if (s.startsWith('FRI'))                   return 'bg-cyan-900 text-cyan-300'
      if (s === 'BL')                            return 'bg-sky-900 text-sky-300'
      if (s === 'CCI' || s === 'CCI0R' || s === 'CCIB') return 'bg-violet-900 text-violet-300'
      if (s === 'RL')                            return 'bg-fuchsia-900 text-fuchsia-300'
      if (s === 'RH')                            return 'bg-fuchsia-900 text-fuchsia-400'
      if (s === 'PP')                            return 'bg-yellow-900 text-yellow-300'
      if (s === 'L555' || s === 'L22')           return 'bg-rose-900 text-rose-300'
      if (s === 'L2L4')                          return 'bg-sky-900 text-sky-400'
      // BE↓ red, BE↑ green (2026-07-29). This rule sits ahead of the generic ↑/↓ rules
      // below, so both directions used to come out emerald.
      if (s.includes('BE'))                      return s.includes('↓')
        ? 'bg-red-900 text-red-400'
        : 'bg-emerald-900 text-emerald-300'
      if (s.includes('↑'))                       return 'bg-lime-900 text-lime-300'
      if (s.includes('↓'))                       return 'bg-red-900 text-red-400'
      return 'bg-blue-900 text-blue-300'
    },
  },
  {
    // ── ▭ body + wick shape, straight from bar_body_wick ──────────────────────
    // The TZ script's third label line, which the table has never shown. Already
    // served by /api/bar_signals — no engine, no column, no backfill: the data was
    // here and only the display was missing, the same gap the ⚛ row had.
    //
    // Two letters doing two jobs, and the second is the load-bearing one:
    //   body   X expanded (≥1.5× the previous body) · S normal · M minimal (≤0.5×)
    //   wick   TB upper wick ≥50% of range · BB lower ≥50% · J doji (body ≤20%)
    //          F both wicks under 30% — a flat bar that went nowhere at either end
    //
    // BB is the "hammer" this session measured: on the reserved 2024-26 window it
    // returned +2.09% under path simulation against +1.08% for random entries at the
    // same rate — and it BEAT the two-bar Wick-Flip pair (+1.47%) that contains it.
    // The pair discards 97% of BB's trades for a worse median, so the single letter
    // is the one worth watching.
    key: 'bodywick',
    label: '▭',
    getSigs: (b) => (b.bar_body_wick ? [b.bar_body_wick] : []),
    sigTitle: (s) => {
      const body = s.startsWith('X') ? 'X — body expanded, at least 1.5× the previous bar\'s'
                 : s.startsWith('M') ? 'M — body minimal, at most half the previous bar\'s'
                 : s.startsWith('S') ? 'S — body normal, between those'
                 : ''
      const w = s.replace(/^[XSM]/, '')
      const wick = w === 'TB' ? 'TB — upper wick is at least half the bar\'s range: highs rejected'
                 : w === 'BB' ? 'BB — lower wick is at least half the range: lows rejected. This is the hammer; measured on 2024-26 it beat the two-bar Wick-Flip pair that contains it.'
                 : w === 'J'  ? 'J — doji: body at most 20% of range, so both ends are wick by construction'
                 : w === 'F'  ? 'F — flat: both wicks under 30% of range, the bar went nowhere at either end'
                 : w ? w : 'no dominant wick shape'
      return `${body}${body && wick ? ' · ' : ''}${wick}`
    },
    chipCls: (s) => {
      const w = s.replace(/^[XSM]/, '')
      if (w === 'BB') return 'bg-teal-900 text-teal-300 font-semibold'
      if (w === 'TB') return 'bg-orange-900 text-orange-300 font-semibold'
      if (w === 'J')  return 'bg-slate-800 text-slate-300'
      if (w === 'F')  return 'bg-slate-900 text-slate-500'
      return 'bg-slate-900 text-slate-400'
    },
  },
  {
    // ── ⟂ CISD — change in state of delivery ──────────────────────────────────
    // Split by ORIGIN, because the merged column hid the whole effect. A +CISD born
    // from a structure break and one born from a level completion are different
    // events; sig_cisd_cplus sums them and dilutes the first with the second.
    //
    //   +S   structure break        7,040 fires · median fwd5 +0.650% vs +0.202%
    //   −S   its mirror            12,334 fires · +0.200% — flat, and that matters:
    //                              an artefact would move both ends, not one
    //   seq  ++--                     258 fires · +0.012% · 2/6 years — nothing
    //   mpm  -+-                    5,850 fires · +0.239% · 5/6 — near baseline
    //
    // Every one of these was zero or wrong until 260815: the engine seeded its market
    // structure from 0.0, so `low < bottomPrice` meant `low < 0` and the entire +CISD
    // structural path was dead code. On the reserved 2024-26 window the recovered half
    // scores +0.80% against a +0.25% baseline, 3/3 years. WATCH, not BUILD — the bar
    // is +1pp and this is +0.55.
    key: 'cisd',
    label: '⟂',
    getSigs: (b) => {
      const p = []
      if (b.sig_cisd_plus_struct)  p.push('+S')
      if (b.sig_cisd_minus_struct) p.push('−S')
      if (b.sig_cisd_seq)          p.push('seq')
      if (b.sig_cisd_mpm)          p.push('mpm')
      return p
    },
    sigTitle: (s) =>
      s === '+S'  ? '+CISD from a STRUCTURE BREAK — the half that carries the effect. 7,040 fires on sp500 1D, median 5-bar forward +0.650% against a +0.202% baseline, 5/6 years. Replicated on a reserved 2024-26 window at +0.80% vs +0.25%. WATCH, not a build: +0.55pp against a +1pp bar.'
    : s === '−S'  ? '−CISD from a structure break — the mirror, and it is FLAT (+0.200% vs a +0.202% baseline). That is the useful part: an artefact of the measurement would have moved both ends.'
    : s === 'seq' ? 'CISD_SEQ (++--) — unreachable before the 260815 engine fix, and now measurable at nothing: 258 fires, +0.012%, 2/6 years.'
    : s === 'mpm' ? 'CISD_MPM (-+-) — the engine has computed this since day one and the schema never kept it. 5,850 fires, +0.239%, 5/6 years — near baseline.'
    : undefined,
    chipCls: (s) =>
      s === '+S'  ? 'bg-emerald-900 text-emerald-300 font-bold'
    : s === '−S'  ? 'bg-slate-800 text-slate-400'
    : 'bg-slate-900 text-slate-500',
  },
  {
    // ⚛ PHYSICS (2026-08-14) — the 260814 Pine fields, one row directly under L.
    //
    // The chart strip and this row show the SAME set, because two surfaces with two ideas of
    // "what counts as a physics signal" would disagree the first time a threshold moved. What
    // is in it was measured over 8.7M sp500 bars rather than chosen by eye:
    //
    //   S3 resonance 57.1% · K1 stretch 45.6% · E2/★ 32.5% · RA 24.9% · M2 19.6%
    //   AD 6.8% · gap G3 3.7% · K2 3.6% · E★ 2.5% · Wyckoff events 1.9%
    //
    // R, E and M are three-way splits around a rolling median, so "not the middle" is two
    // thirds of every chart by construction — they were never extremes. S3 and K1 are regimes
    // that hold for weeks. Only the rare set plus RA is shown, because a row that marks every
    // bar marks nothing.
    key: 'phys',
    label: '⚛',
    // `mode` is injected at render from the ⚛ selector, so this row and the chart strip
    // stay one rule. 'all' prints the FULL per-bar state — what the Pine pane shows — and
    // is the honest answer to "why is my bar empty": nothing was missing, the row was
    // filtered. The DB carries every field on every bar.
    getSigs: (b, prev, mode = 'rare') => {
      const all = mode === 'all'
      const p = []
      if ((b.phys_e || '').includes('★')) p.push(b.phys_e)
      if ((b.phys_k || '').startsWith('K2')) p.push(b.phys_k)
      if (b.phys_ad) p.push(b.phys_ad)
      if (b.phys_gap_true === 'G3') p.push('gG3')
      if (['SPRING', 'SPRING★', 'UTAD', 'SOS★'].includes(b.phys_wyc)) p.push(b.phys_wyc)
      if (b.phys_r === 'RA' && (mode === 'ra' || all)) {
        p.push('RA' + (b.phys_regime ? '·' + b.phys_regime : ''))
      }
      if (all) {
        // S is a regime that holds for weeks — printed on TRANSITION, not on every bar,
        // or it becomes a solid wall of identical chips that hides the events beside it
        if (b.phys_r && b.phys_r !== 'RA') p.push(b.phys_r + (b.phys_regime ? '·' + b.phys_regime : ''))
        if (b.phys_m) p.push(b.phys_m)
        if (b.phys_e && !b.phys_e.includes('★')) p.push(b.phys_e)
        if (b.phys_k && !b.phys_k.startsWith('K2')) p.push(b.phys_k)
        if (b.phys_c) p.push(b.phys_c)
        if (b.phys_h) p.push(b.phys_h)
        if (b.phys_s && (!prev || b.phys_s !== prev.phys_s)) p.push(b.phys_s)
        if (b.phys_gap_true && b.phys_gap_true !== 'G3') p.push('g' + b.phys_gap_true)
      }
      return p
    },
    sigTitle: (s, b) => {
      if (s.startsWith('RA')) return 'RA — absorbed effort: heavy volume, little displacement. The book\'s first confluence law. ·U/·D = close above/below EMA20.'
      if (s.startsWith('K2')) return 'K2 — past the elastic limit (|close−EMA20| ≥ 3 ATR). The plastic zone: the spring no longer pulls back. 3.6% of bars.'
      if (s.includes('★') && s.startsWith('E')) return 'E★ — a loaded compression released: range expanded beyond the ATR baseline after the spring was charged. 2.5% of bars.'
      if (s === 'gG3') return 'gG3 — a LARGE gap, measured from the empty-space edge. The stored bar_gap_range measures from the previous close and overstates on 49.5% of gaps (median 1.80×, never under), so this is the corrected class.'
      if (s.startsWith('★')) return 'AD-FRESH — Z1G/Z2G exhaustion followed by a T-flip low in the range. ★A = the exhaustion bar was absorbed (RA). ★★ = clustered.'
      if (s === 'SPRING' || s === 'SPRING★') return 'Wyckoff spring — undercut of 20-bar support reclaimed on rising volume in a down macro. ★ = the bar was absorbed.'
      if (s === 'UTAD') return 'Wyckoff UTAD — upthrust after distribution: 20-bar resistance exceeded then lost, in an up macro.'
      if (s === 'SOS★') return 'Wyckoff SOS — AD-fresh flip during a VIX spike in a down macro.'
      if (/^R[123AB]/.test(s)) return 'R — resistance to motion: where this bar\'s range sits against its own recent baseline. ·U/·D = above/below EMA20.'
      if (/^M[0-9]/.test(s)) return 'M — momentum class: displacement carried per unit of effort. M2 = heavy and fast (19.6% of bars).'
      if (/^E[0-9]/.test(s)) return 'E — stored energy: how far the range is compressed below its own baseline. E2 = loaded spring (32.5%).'
      if (/^K[013]/.test(s)) return 'K — extension from EMA20 in ATR. K1 = inside the elastic zone (45.6% of bars), K2 = past it.'
      if (/^C/.test(s)) return 'C — coulomb: distance to the nearest confirmed pivot, which is only known three bars after the pivot printed.'
      if (/^H/.test(s)) return 'H — entropy of the last 20 bars: how disordered the price path is.'
      if (/^S[0-9]/.test(s)) return 'S — resonance regime, printed only when it CHANGES. S3 holds for weeks (57.1% of bars), so marking every bar would say nothing.'
      if (/^g[A-Z0-9]/.test(s)) return 'Gap class measured from the empty-space edge, not the previous close.'
      return undefined
    },
    chipCls: (s) => {
      if (s.includes('★') && s.startsWith('E')) return 'bg-violet-900 text-violet-300 font-semibold'
      if (s.startsWith('K2'))                   return 'bg-red-900 text-red-300 font-bold'
      if (s === 'SPRING★' || s === 'SOS★')      return 'bg-lime-900 text-lime-300 font-bold'
      if (s === 'SPRING')                       return 'bg-lime-900 text-lime-400'
      if (s === 'UTAD')                         return 'bg-rose-900 text-rose-300 font-semibold'
      if (s === 'gG3')                          return 'bg-amber-900 text-amber-300'
      if (s.startsWith('★'))                    return 'bg-fuchsia-900 text-fuchsia-300 font-semibold'
      if (s.startsWith('RA'))                   return 'bg-sky-900 text-sky-300'
      if (s === 'M2')                           return 'bg-blue-950 text-blue-300'
      if (s === 'E2')                           return 'bg-amber-950 text-amber-400'
      if (s === 'S3U')                          return 'bg-emerald-950 text-emerald-400'
      if (s === 'S3D')                          return 'bg-rose-950 text-rose-400'
      // the full-state fields are context, not events: dimmer than everything above so a
      // real signal still stands out when every bar is filled
      if (/^[RMEKCHS]/.test(s))                 return 'bg-slate-900 text-slate-400'
      return 'bg-slate-800 text-slate-300'
    },
  },
  // F-row (F1–F11) retired from display — matches ULTRA (still computed backend-side).
  // FLY row folded into EDGE 2026-07-29 (user) — one row instead of two. The FLY chips keep
  // their purple palette so they stay instantly separable from the emerald edge codes.
  //   ✦-prefix = FLY-fresh: first FLY after a >=15-bar absence (validated 2026-07-19:
  //   +0.84%/PF1.13/+4.3σ/5-6yr, TRAIN & TEST both positive). Waiting for the SECOND
  //   appearance was tested and REJECTED (+0.10% — the move runs away between them).
  {
    // ✅ EDGE row (2026-07-20): validated Edge-board setup fires per bar, computed by the
    // SAME edge_replay masks the backtest uses (backtest == display). Codes:
    // CAP=T1-CapBounce QZC=QZ-Capit D+L1 G3 ⚡G3A=G3-Abs ATM/ATMR=Atomic(-R) SPR=Spring
    // Z11=Z11-T11 L43=L43-TRIPLE WSH=Washout H1B=1H-bottom ENG/EL46=Engulf ZRT=Zone-Retest
    // HB15=HighBase-15m RTB=RTB-Base P55 PAR=Parabola 🎯3/🎯4=Cluster
    key: 'edges',
    label: 'EDGE',
    getSigs: (b) => {
      const f = b.fly ?? []
      const fly = b.fly_fresh && f.length ? [`✦${f[0]}`, ...f.slice(1)] : f
      return [...fly, ...(b.edges ?? [])]   // FLY leads whenever the bar has one
    },
    sigTitle: (s, b) => {
      if (s.startsWith('✦'))
        return '✦ FLY-fresh — first FLY after ≥15 bars of silence: +0.84%/PF1.13/+4.3σ, 5-6yr, TRAIN & TEST positive. (2nd-appearance waiting tested: edge gone — act on the first.)'
      if (s.startsWith('FLY')) return `FLY signal: ${s}`
      if (s.endsWith('🔇'))
        return ('🔇 L43-TRIPLE on a QUIET 1H tape — max(1h volume)/avg over the trailing 10 '
              + 'sessions under 4×. The validated tier: +2.72 → +3.43, win 57.0 → 59.5, '
              + 'pf 1.88 → 2.08, 6/6 years, worst year +0.9 → +0.1, DSR 1.000 against 20 '
              + 'trials. Found by inverting a spike hunt: a LOUD tape lifts the odds of a '
              + '+40% day 9.4× and still loses money (−1.65 median at ≥10×), because a '
              + 'volume event predicts volatility, not direction. Quiet helped 10 of 10 '
              + 'setups; only this one also survived deflation.')
      return (b?.rev_buy && b?.mtf_echo !== false && ['QZC', 'D+L1', 'RTB', 'P55'].includes(s))
        ? `EDGE🟢 premium combo: ${s} + same-bar 🟢REV (validated 2026-07-20 — QZC +2.69% med+ · D+L1 +3.46% TRAIN+ · RTB +2.04% 6/6yr · P55 +1.89%)`
        : `Edge-board setup fired on this bar: ${s} (edge_replay mask — identical to the backtest)`
    },
    chipCls: (s, b) => s.startsWith('✦')
      ? 'bg-purple-600 text-purple-50 font-bold ring-1 ring-purple-300'
      : s.startsWith('FLY')
      ? 'bg-purple-900 text-purple-200'
      : (b?.rev_buy && b?.mtf_echo !== false && ['QZC', 'D+L1', 'RTB', 'P55'].includes(s))
      ? 'bg-amber-600 text-amber-50 font-bold ring-1 ring-amber-300'
      : s.startsWith('🎯')
      ? 'bg-emerald-600 text-emerald-50 font-bold ring-1 ring-emerald-300'
      // 🔇 quiet-tape qualifier (2026-07-30) — the SAME chip at a brighter tier, not a second
      // chip: L43🔇 is a strict subset of L43, so a separate entry would only repeat it. The
      // quiet variant is the validated one (+2.72→+3.43, pf 1.88→2.08, 6/6yr, worst +0.9→+0.1,
      // DSR 1.000 vs 20 trials), so it earns the brighter treatment.
      : s.endsWith('🔇')
      ? 'bg-teal-500 text-teal-950 font-bold ring-2 ring-teal-200'
      : 'bg-emerald-900 text-emerald-200 ring-1 ring-emerald-500/50 font-semibold',
  },
  {
    // 🧬 SEQ row (2026-07-20): frozen-OOS 2-4-bar robust-sequence completed on this bar
    // (same gate as the Ultra 🧬SEQ chip: OOS✓ tier · depth≥3 · OOS win≥55% · ps_med>0 (bright ≥60)).
    // 🏆 = DSR≥0.6 selection-proof (the only fully-trustable tier).
    key: 'seq',
    label: 'SEQ',
    getSigs: (b) => {
      const out = []
      if (b.seq34) out.push(`🧬${b.seq34.coarse ? '°' : ''}${(b.seq34.dsr ?? 0) >= 0.6 ? '🏆' : ''}${Math.round(b.seq34.win)}`)
      if (b.seq_ctx) out.push(`${b.seq_ctx.kind === 'tail' ? '🎲' : b.seq_ctx.dir === 'up' ? '⤴' : '⤵'}${Math.round(b.seq_ctx.up)}`)
      return out
    },
    sigTitle: (s, b) => s.startsWith('⤴') || s.startsWith('⤵') || s.startsWith('🎲')
      ? (b.seq_ctx ? `${b.seq_ctx.kind === 'tail' ? '🎲 TAIL-context — rarely continues but FAT when it does (parabola fuel): mean ' + (b.seq_ctx.mean > 0 ? '+' : '') + b.seq_ctx.mean + '% despite low up-rate.' : b.seq_ctx.dir === 'up' ? '⤴ BOOSTER' : '⤵ SUPPRESSOR'} context for ${b.seq_ctx.sig.toUpperCase()} [layer ${b.seq_ctx.layer ?? 'TZ'}]: sequence (ends on this bar) ${b.seq_ctx.seq} → historical fwd-20 up ${b.seq_ctx.up}% (signal baseline ${b.seq_ctx.base_up}%, lift ${b.seq_ctx.lift > 0 ? '+' : ''}${b.seq_ctx.lift}pp, n=${b.seq_ctx.n}, era-consistent)` : undefined)
      : b.seq34
      ? `🧬 ${b.seq34.depth}-bar ${b.seq34.coarse ? 'COARSE (no-L token) ' : 'exact '}frozen-OOS sequence: ${b.seq34.seq} — OOS win ${b.seq34.win}% · ps_med +${b.seq34.ps_med}%${(b.seq34.dsr ?? 0) >= 0.6 ? ' · 🏆 DSR≥0.6 selection-proof' : ''}`
      : undefined,
    chipCls: (s, b) => s.startsWith('🎲')
      ? 'bg-amber-700 text-amber-50 font-bold ring-1 ring-amber-300'
      : s.startsWith('⤴')
      ? ((b?.seq_ctx?.up ?? 0) >= 60
          ? 'bg-teal-400 text-teal-950 font-bold ring-2 ring-teal-100'
          : 'bg-teal-700 text-teal-50 font-bold ring-1 ring-teal-300')
      : s.startsWith('⤵')
      ? 'bg-rose-800 text-rose-100 font-bold ring-1 ring-rose-400'
      : (b?.seq34?.dsr ?? 0) >= 0.6
      ? 'bg-violet-600 text-violet-50 font-bold ring-1 ring-violet-300'
      : (b?.seq34?.win ?? 0) >= 60
        ? 'bg-violet-900 text-violet-200 ring-1 ring-violet-500/50 font-semibold'
        : 'bg-violet-950 text-violet-300/80',
  },
  {
    // ⇶ ENS row (2026-07-20c): ENSEMBLE consensus across ALL descriptor layers —
    // one verdict per layer (TZ/TZ+L/suffix/body/gap/line5/volume), majority + 
    // |lift|-weighted avg up%. Separate row so the SEQ winner chip stays untouched.
    key: 'ens',
    label: 'ENS',
    getSigs: (b) => b.seq_ens
      ? [`${b.seq_ens.dir === 'up' ? '⤴' : '⤵'}${b.seq_ens.n_up}:${b.seq_ens.n_dn}·${Math.round(b.seq_ens.up_avg)}`]
      : [],
    sigTitle: (s, b) => b.seq_ens
      ? `ENSEMBLE ${b.seq_ens.n_up}⤴ / ${b.seq_ens.n_dn}⤵${b.seq_ens.n_tail ? ` (${b.seq_ens.n_tail}🎲)` : ''} · weighted up ${b.seq_ens.up_avg}% — per-layer: ${b.seq_ens.detail.map(v => `${v.layer} ${v.kind === 'tail' ? '🎲' : v.dir === 'up' ? '⤴' : '⤵'}${Math.round(v.up)} (${v.seq})`).join(' · ')}`
      : undefined,
    chipCls: (s, b) => {
      const e = b?.seq_ens
      const unanim = e && (e.n_up === 0 || e.n_dn === 0) && (e.n_up + e.n_dn) >= 3
      if (e?.dir === 'up') return unanim
        ? 'bg-teal-500 text-teal-950 font-bold ring-2 ring-teal-200'
        : 'bg-teal-900 text-teal-200 ring-1 ring-teal-500/50'
      return unanim
        ? 'bg-rose-600 text-rose-50 font-bold ring-2 ring-rose-200'
        : 'bg-rose-950 text-rose-300 ring-1 ring-rose-500/50'
    },
  },
  {
    // CONF row (2026-07-21): the all-vs-all confluence score — 812 dual-gate-qualified
    // signal pairs per bar (validated monotone: D0 −3.07% → D9 +3.63%, tails 6/6yr).
    // Bands from the decile ladder: ≥12 = D9 (bright), ≥8 D8, ≤−16 = D0 (bright red).
    key: 'conf',
    label: 'CONF',
    // gray ext tier (2026-07-21): sub-threshold cells, info-only — chip prefixed '·'
    getSigs: (b) => b.conf != null ? [String(b.conf)]
      : b.conf_ext != null ? ['·' + b.conf_ext] : [],
    sigTitle: (s, b) => b.conf != null
      ? `CONF ${b.conf} — all-vs-all confluence, 718 deduped cells (D9≥10 → ps +4.01%/med+1.51/6-6yr · D0≤−15 → −3.04%/0-6yr; both 5% tails era-robust). Top cells: ${b.conf_top ?? '—'}`
      : b.conf_ext != null
        ? `CONF~ ${b.conf_ext} (ნაცრისფერი info-ტიერი — sub-threshold უჯრები, არავალიდირებული; მხოლოდ საორიენტაციო). Top cells: ${b.conf_ext_top ?? '—'}`
        : undefined,
    chipCls: (s) => {
      if (s.startsWith('·')) return 'bg-md-surface-high text-zinc-500'
      const v = Number(s)
      if (v >= 10) return 'bg-emerald-500 text-emerald-950 font-bold ring-2 ring-emerald-200'
      if (v >= 7)  return 'bg-emerald-800 text-emerald-100 font-semibold'
      if (v >= 3)  return 'bg-teal-900 text-teal-300'
      if (v <= -15) return 'bg-red-600 text-red-50 font-bold ring-2 ring-red-200'
      if (v <= -7)  return 'bg-rose-900 text-rose-200 font-semibold'
      return 'bg-md-surface-high text-md-on-surface-var'
    },
  },
  {
    key: 'g',
    label: 'G',
    getSigs: (b) => b.g ?? [],
    chipCls: () => 'bg-violet-900 text-violet-200',
  },
  // B-row (B1–B11) retired from display — still computed backend-side.
  {
    key: 'combo',
    label: 'I',
    getSigs: (b) => (b.combo ?? []).filter(s => !PREUP_SET.has(s) && !HIDDEN_I_SIGS.has(s)),
    chipCls: (s) => {
      if (s === 'ROCKET' || s === 'BUY') return 'bg-green-900 text-green-200 font-bold'
      if (s.includes('↑') || s === '3G') return 'bg-lime-900 text-lime-300'
      if (s.includes('↓') || s === 'CONS' || s === '↓BIAS') return 'bg-red-900 text-red-300'
      return 'bg-teal-900 text-teal-300'
    },
  },
  {
    key: 'ultra',
    label: 'ULT',
    getSigs: (b) => b.ultra ?? [],
    chipCls: (s) => {
      if (s === 'BEST↑' || s === '4BF')   return 'bg-yellow-800 text-yellow-200 font-bold'
      if (s === 'FBO↑' || s === 'EB↑' || s === '3↑') return 'bg-lime-900 text-lime-300'
      if (s === 'FBO↓' || s === 'EB↓' || s === '4BF↓') return 'bg-red-900 text-red-300'
      if (s === 'L88')   return 'bg-violet-900 text-violet-200 font-bold'
      if (s === '260308') return 'bg-purple-900 text-purple-300'
      return 'bg-sky-900 text-sky-300'
    },
  },
  {
    key: 'vol',
    label: 'VOL',
    getSigs: (b) => b.vol ?? [],
    chipCls: () => 'bg-pink-900 text-pink-300 font-bold',
  },
  {
    key: 'vabs',
    label: 'VABS',
    getSigs: (b) => b.vabs ?? [],
    chipCls: (s) => {
      if (s === 'BEST★') return 'bg-lime-800 text-lime-200 font-bold'
      if (s === 'STRONG') return 'bg-emerald-900 text-emerald-200'
      if (s.includes('↑') || ['NS', 'ABS', 'CLM', 'LOAD'].includes(s))
        return 'bg-lime-900 text-lime-300'
      return 'bg-red-900/70 text-red-300'
    },
  },
  {
    key: 'wick',
    label: 'WICK',
    getSigs: (b) => b.wick ?? [],
    chipCls: (s) => s.includes('↑') ? 'bg-sky-900 text-sky-300' : 'bg-red-900/50 text-red-300',
  },
  // SETUP row (A / SM / N / MX from gog_engine) REMOVED 2026-07-30 — all four measured and
  // all four empty. Path-sim, 6yr, $21-377, baseline −0.63/win 48.4:
  //   SM −0.44 · MX −0.46 · A −0.48 · N −0.48   — a 0.04pp spread across four "different"
  //   signals, and each one's own complement lands in the same place: NOT-A −0.54,
  //   NOT-SM −0.55, NOT-N −0.54, NOT-MX −0.55. Presence vs absence is worth 0.08pp.
  // Stacking adds nothing (all four together −0.39 vs any single ≈ −0.46) and +🏆RS lifts
  // all four to the SAME number (+0.29/+0.33/+0.31/+0.31) — which is what you see when it
  // is one mask wearing four labels: they share preTurnStructure and the same
  // fullSequence|supportAbsSequence|preFinalSequence|resetSequence gates, differing only in
  // trigger flavour. As gates they hurt (D+L1+SM −2.30, Washout+N −0.99).
  // My prior that SM would be best (it is the only absorption-flavoured one) was WRONG — the
  // absorption logic is diluted to nothing by the shared structure gates.
  // No score anywhere reads these, so removal changes only the display. Still computed and
  // still exported (setup_tokens in the DB, SETUP in the CSV).
  {
    key: 'gog',
    label: 'GOG',
    getSigs: (b) => b.gog_tier ? [b.gog_tier] : [],
    chipCls: (s) => {
      if (s.startsWith('G1P') || s.startsWith('G2P') || s.startsWith('G3P'))
        return 'bg-green-800 text-green-100 ring-1 ring-green-400 font-bold'
      if (s.startsWith('G1L') || s.startsWith('G2L') || s.startsWith('G3L'))
        return 'bg-emerald-800 text-emerald-100 ring-1 ring-emerald-400 font-bold'
      if (s.startsWith('G1C') || s.startsWith('G2C') || s.startsWith('G3C'))
        return 'bg-teal-800 text-teal-100 ring-1 ring-teal-400 font-bold'
      return 'bg-fuchsia-800 text-fuchsia-100 ring-1 ring-fuchsia-400 font-bold'
    },
  },
  {
    key: 'context',
    label: 'CTX',
    getSigs: (b) => b.context ?? [],
    chipCls: (s) => {
      if (s === 'LDP' || s === 'LRP') return 'bg-green-900 text-green-200 font-semibold'
      if (s === 'LDC' || s === 'LRC') return 'bg-teal-900 text-teal-200'
      if (s === 'LDS' || s === 'LD')  return 'bg-cyan-900 text-cyan-300'
      if (s === 'SQB' || s === 'BCT') return 'bg-blue-900 text-blue-200'
      if (s === 'WRC' || s === 'F8C') return 'bg-slate-700 text-slate-200'
      return 'bg-md-surface-high text-md-on-surface'
    },
  },
  {
    // 260523 family: AD-FRESH / AD-CLUSTER / WYC / PREBREAK / Pullback / Swing.
    // Matches the chips emitted by collectSignals() in ScannerDataGrid so the
    // ULTRA tab's Signals column and Superchart show the same event surface.
    key: '523',
    label: '523',
    getSigs: (b) => b.wy523 ?? [],
    chipCls: (s) => {
      // AD / WYC bullish
      if (s === 'AD-CLU')   return 'bg-orange-800/80 text-orange-100 ring-1 ring-orange-400 font-bold'
      if (s === 'AD-FR')    return 'bg-orange-900 text-orange-300 font-semibold'
      if (s === 'SPRING')   return 'bg-emerald-800/80 text-emerald-100 ring-1 ring-emerald-400 font-bold'
      if (s === 'SOS')      return 'bg-emerald-900 text-emerald-300 font-semibold'
      // WYC bearish
      if (s === 'UTAD')     return 'bg-red-800/80 text-red-100 ring-1 ring-red-400 font-bold'
      if (s === 'SOW')      return 'bg-red-900 text-red-300 font-semibold'
      if (s === 'MKDN')     return 'bg-red-900/70 text-red-300'
      // WYC phase context
      if (s === 'MARKUP')   return 'bg-lime-900 text-lime-300 font-semibold'
      if (s === 'ACC_TR' || s === 'DIST_TR' || s === 'InTR')
                            return 'bg-slate-700 text-slate-200'
      // PREBREAK tiers
      if (s === 'PRIME★')   return 'bg-yellow-700 text-yellow-100 font-bold ring-1 ring-yellow-400'
      if (s === 'READY')    return 'bg-lime-800 text-lime-100 font-semibold ring-1 ring-lime-400'
      if (s === 'WATCH')    return 'bg-cyan-900 text-cyan-300'
      // Pullback / pivots
      if (s === 'LVBO')     return 'bg-sky-800 text-sky-100 font-semibold ring-1 ring-sky-400'
      if (s === 'WVF')      return 'bg-violet-900 text-violet-300 font-semibold'
      // measured league-worst (−7.90/pf 0.84) — red, not teal, since 2026-08-01
      if (s === 'W-PH')     return 'bg-red-900 text-red-300'
      if (s === 'PEN')      return 'bg-rose-900/50 text-rose-300'
      // PREBREAK extra sub-signals (synced with ULTRA)
      if (s === 'PP+RTV')   return 'bg-yellow-900/60 text-yellow-200 font-semibold'
      if (s === 'FLY-C')    return 'bg-lime-900/60 text-lime-200 font-semibold'
      if (s === 'FOLLOW')   return 'bg-green-900/60 text-green-200 font-semibold'
      // Swing classification
      if (s === 'HL')       return 'bg-emerald-900/60 text-emerald-300 font-semibold'
      if (s === 'LL')       return 'bg-sky-900/60 text-sky-300'
      if (s === 'HH')       return 'bg-lime-900/40 text-lime-200'
      if (s === 'LH')       return 'bg-red-900/60 text-red-300 font-semibold'
      return 'bg-md-surface-high text-md-on-surface'
    },
  },
  {
    // 260529 Wyckoff V2 (accumulation cycle) + structure triggers — mirrors
    // ULTRA's "Wyckoff cycle (260529)" chips. Stages: SC→AR→ST→SPR→SOS/JAC→LPS,
    // EVR absorption, plus valid-TR triggers (tSPR/tSOS/tLPS/tEVR).
    key: 'wyck',
    label: 'WYCK',
    getSigs: (b) => b.wyck ?? [],
    chipCls: (s) => {
      if (s === 'SPR' || s === 'tSPR')   return 'bg-teal-800/80 text-teal-100 ring-1 ring-teal-400 font-bold'
      if (s === 'SOS' || s === 'tSOS')   return 'bg-green-800/80 text-green-100 ring-1 ring-green-400 font-bold'
      if (s === 'JAC')                   return 'bg-lime-800 text-lime-100 font-bold'
      if (s === 'LPS' || s === 'tLPS')   return 'bg-blue-900 text-blue-200 font-semibold'
      if (s === 'EVR' || s === 'tEVR')   return 'bg-fuchsia-900 text-fuchsia-300'
      if (s === 'SC')                    return 'bg-red-900 text-red-300'
      if (s === 'AR')                    return 'bg-orange-900 text-orange-300'
      if (s === 'ST')                    return 'bg-purple-900 text-purple-300'
      return 'bg-md-surface-high text-md-on-surface'
    },
  },
  {
    // PRICE × VOLUME — the TradingView "260921_PV_MULTI" script, ported. Nine ordinal shapes read
    // off the last 3-5 daily bars: DIV UPP UPR REV RUP VUP TURN UP4 RE2. Values from
    // /api/studio/pv-multi-marks merged by date (barsPv), 1d only.
    //
    // TWO ROWS, NOT A TOGGLE, for the same reason UDN★ has two: the script offers `close` and
    // `ohlc4` as the price source and they DISAGREE — measured over 2.15M firing sessions, the two
    // agree on only 52.7% of them. A toggle would hide exactly the half that is interesting.
    //
    // DESCRIPTIVE ONLY, and the evidence is specific: PV_MULTI_V1, sealed 2026-09-21, k = 9 →
    // 0 BUILD / 4 VETO_CANDIDATE / 5 NULL, with EVERY cell negative in MINE:
    //   REV −0.909/−0.686 (negative in all four MINE years, dsr_neg 1.000) · RE2 −0.689/−0.320 ·
    //   UPP −0.618/−0.573 (4/4 negative years) · RUP −0.350/−0.316 · the rest NULL.
    // The four VETOes are RECORDED, NOT APPLIED — they cover ~13.6% of bars and applying them is
    // an interaction question with its own k. Never a score input. The chip colour says so: the
    // four VETO codes are rose, the five NULL codes are neutral grey, and nothing here is green.
    key: 'pv',
    label: 'PV',
    getSigs: (b, _prev, src = 'o') => {
      const code = src === 'c' ? b.pv_c : b.pv_o
      return code ? [code] : []
    },
    sigTitle: (sig, b, src = 'o') => {
      const MEAN = {
        DIV:  'price fell 3 bars while volume rose 3 bars',
        UPP:  'price rose 3 bars while volume rose 3 bars',
        UPR:  'volume rose 3 bars and price is above its level 2 bars ago (clean UPP excluded)',
        REV:  'price and volume both fell 3 bars, then both turned up',
        RUP:  'volume contracted 3 bars with day-3 price above day-1, then price and volume turned up',
        VUP:  'volume fell 3 bars while price stayed above its level 2 bars ago',
        TURN: 'price declined then recovered 2 bars, with the script\'s volume turn structure',
        UP4:  'price rose 4 bars while volume expanded, rested, then re-expanded above the prior peak',
        RE2:  'price down-down-up while volume went up-down-up',
      }
      const VERDICT = {
        REV: 'VETO_CANDIDATE — −0.909 MINE / −0.686 VERIFY, negative in all 4 MINE years, dsr_neg 1.000',
        RE2: 'VETO_CANDIDATE — −0.689 MINE / −0.320 VERIFY, dsr_neg 1.000',
        UPP: 'VETO_CANDIDATE — −0.618 MINE / −0.573 VERIFY, negative in all 4 MINE years, dsr_neg 1.000',
        RUP: 'VETO_CANDIDATE — −0.350 MINE / −0.316 VERIFY, negative in all 4 MINE years, dsr_neg 0.996',
      }
      const other = src === 'c' ? b.pv_o : b.pv_c
      const otherName = src === 'c' ? 'ohlc4' : 'close'
      return `${sig} (price = ${src === 'c' ? 'close' : 'ohlc4'}) — ${MEAN[sig] || ''}\n`
           + `${otherName}: ${other || 'no shape'}${other && other !== sig ? '  ← the two sources disagree on this bar' : ''}\n\n`
           + `sealed verdict: ${VERDICT[sig] || 'NULL — no effect that replicated'}\n`
           + 'PV_MULTI_V1, k = 9: 0 BUILD / 4 VETO_CANDIDATE / 5 NULL, every cell negative in MINE.\n'
           + 'Descriptive only — recorded, not applied. Never a ranking input.'
    },
    // No green anywhere: the best of these nine was NULL and four are VETO candidates, so a colour
    // that reads as strength would contradict the evidence printed in the tooltip.
    chipCls: (s) => (['REV', 'RE2', 'UPP', 'RUP'].includes(s)
      ? 'bg-rose-900/60 text-rose-200 font-bold'
      : 'bg-md-surface-high text-md-on-surface-var'),
  },
  {
    // VOL ECHO — the TradingView "260925_VOL_ECHO" script ported at its defaults (vol_echo_build.py
    // → data/vol_echo_signals.parquet, /api/studio/vol-echo-marks merged by date as barsVe), 1d only.
    // VE echo · Q quiet in the zone · R low volume after a breakdown · ▲/▼ release (1-3) ·
    // BO▲/BOV▲ BD▼/BDV▼ breakout · ● spike · SPK an origin that was echoed LATER (hindsight: it is
    // drawn back onto the spike bar, exactly as the Pine draws it) · ⛔QR▲ the QR_REL_V1 veto shape.
    // DESCRIPTIVE ONLY: every long study NULL (LONG_V1 0/274, LONG_2326 0/286, TRADE_V1 0/9);
    // QR_REL_V1 veto CONFIRMED (−1.40 pp VERIFY vs a random buy, 6/6 years) — recorded, not applied.
    key: 've',
    label: 'ECHO',
    chipCols: 2,
    getSigs: (b) => {
      const out = []
      if (b.ve_echo) out.push('VE')
      if (b.ve_q) out.push('Q')
      if (b.ve_r) out.push('R')
      if (b.ve_rel_up) out.push(`▲${b.ve_rel_n ?? ''}`)
      if (b.ve_rel_dn) out.push(`▼${b.ve_rel_n ?? ''}`)
      if (b.ve_bov) out.push('BOV▲'); else if (b.ve_bo) out.push('BO▲')
      if (b.ve_bdv) out.push('BDV▼'); else if (b.ve_bd) out.push('BD▼')
      if (b.ve_qr_rel_veto) out.push('⛔QR▲')
      if (b.ve_spk) out.push('SPK')
      else if (b.ve_spike && !b.ve_echo) out.push('●')
      return out
    },
    sigTitle: (sig, b) => {
      const base = b.ve_text ? `${b.ve_text}\n\n` : ''
      const T = {
        VE: `ECHO — volume within ±40% of an earlier spike, in the same price zone, ${b.ve_echo_gap ?? '?'} bars later (${b.ve_echo_col || ''}, ×${b.ve_echo_vr ?? '?'} of the spike's volume).`,
        Q: 'QUIET — closes inside the armed echo zone on volume ≤ 0.8 × the 20-bar median.',
        R: 'R — volume ≤ 0.8 × median within 20 bars after BD▼ / BDV▼.',
        SPK: 'SPK — this spike was echoed LATER. Hindsight: the mark is drawn back onto the spike bar, so it was not knowable on this day.',
        '●': 'Spike — volume ≥ 1.5 × the 20-bar median (the origin the script remembers).',
        '⛔QR▲': 'QR_REL_V1 VETO — yesterday Q∧R, today the first ▲ release. −1.82 pp MINE / −1.40 pp VERIFY vs a random buy, negative in all 6 years. Recorded, NOT applied.',
      }
      const t = T[sig]
        || (sig.startsWith('▲') ? `RELEASE ▲${b.ve_rel_n} — green bar with volume > SMA20 after ${b.ve_rel_q} quiet bar(s).`
        : sig.startsWith('▼') ? `RELEASE ▼${b.ve_rel_n} — red bar with volume > SMA20 after ${b.ve_rel_q} quiet bar(s).`
        : sig.startsWith('BO') ? `${sig} — first green candle that opens AND closes above the zone, ${b.ve_bo_age} bars after the echo${sig === 'BOV▲' ? `, volume ×${b.ve_bov_x ?? '?'} of SMA20` : ''}.`
        : sig.startsWith('BD') ? `${sig} — first red candle that opens AND closes below the zone, ${b.ve_bo_age} bars after the echo${sig === 'BDV▼' ? `, volume ×${b.ve_bov_x ?? '?'} of SMA20` : ''}.`
        : '')
      return `${base}${t}\n\nDescriptive only — VOL_ECHO long studies NULL; the one confirmed result is the ⛔QR▲ veto. Never a ranking input.`
    },
    chipCls: (s) => (
      s === 'VE' ? 'bg-cyan-900/60 text-cyan-200'
      : s === 'Q' ? 'bg-purple-900/60 text-purple-200'
      : s === 'R' ? 'bg-orange-900/60 text-orange-200'
      : s.startsWith('▲') ? 'bg-green-900/60 text-green-200'
      : s.startsWith('▼') ? 'bg-red-900/60 text-red-200'
      : s === 'BOV▲' ? 'bg-blue-950/80 text-blue-200 font-bold'
      : s === 'BO▲' ? 'bg-blue-900/60 text-blue-200'
      : s === 'BDV▼' ? 'bg-purple-950/80 text-fuchsia-200 font-bold'
      : s === 'BD▼' ? 'bg-fuchsia-900/60 text-fuchsia-200'
      : s === '⛔QR▲' ? 'bg-rose-900/70 text-rose-200 font-bold'
      : s === 'SPK' ? 'bg-amber-900/60 text-amber-200'
      : 'bg-md-surface-high text-md-on-surface-var'),
  },
  {
    // L-BAL — the TradingView "260906_LTF_L_COUNT" label, ported. Per 1D session, from its 26
    // 15m bars: UDN = effort line (L34+L3 vs L46), UDN+ = labelled 15m bars by their own candle;
    // then the agreement marks ★ divergence · ★★ conflict · ★★★ half-up · ○○○ half-down · XXX
    // double-neutral. First chip = the two letters ("U·D+"), then one chip per mark. Values come
    // from /api/studio/lbal-marks merged by date (barsLbal), 1d only. DESCRIPTIVE ONLY —
    // INTRADAY_EFFORT_BALANCE_V1 closed 16/16 NULL; never a score input.
    key: 'lbal',
    label: 'UDN★',
    // TWO ROWS, NOT A TOGGLE (user, 2026-09-10). The V and ALL counting modes are not two views of
    // the same number — they DISAGREE on individual bars, and the disagreement is the reading:
    // AMD 2026-03-30 is N·D+ ○○○ counting only volume-confirmed 15m bars, and D·D+ with no mark
    // counting all of them. A toggle shows one and hides the other, so the Superchart renders both
    // as independent rows (UDN★V / UDN★all) and the shared switch is left to the chart line and
    // the Ultra chips, which have their own copies of it.
    // The two letters print on EVERY session that has a state ("U·U+", "D·D+" included) — the
    // TradingView label prints both lines on every bar (user, 2026-09-06).
    getSigs: (b, _prev, mode = 'V') => {
      const udn = lbalPick(b, 'udn', mode), udnC = lbalPick(b, 'udn_c', mode)
      if (!udn) return []
      const out = [`${udn}·${udnC}+`]
      for (const m of String(lbalPick(b, 'marks', mode) || '').split(' ')) if (m) out.push(m)
      return out
    },
    sigTitle: (s, b, mode = 'V') => {
      const other = mode === 'V' ? 'ALL' : 'V'
      const P = (k) => lbalPick(b, k, mode), O = (k) => lbalPick(b, k, other)
      const head = (mode === 'V'
                      ? `V mode — only 15m bars with volume > SMA20 (${b.lbal_nv_bars ?? '?'} of ${b.lbal_n_bars ?? '?'})\n`
                      : `ALL mode — every labelled 15m bar (${b.lbal_n_bars ?? '?'} bars)\n`)
                 + `UDN (effort · L34+L3 vs L46) ${P('udn')} ${P('n_pos')}:${P('n_neg')}\n`
                 + `UDN+ (candle · labelled 15m bars) ${P('udn_c')} ${P('n_pos_c')}:${P('n_neg_c')}\n`
                 + `daily candle ${b.lbal_colour}\n`
                 + (O('text') ? `${other === 'V' ? 'V bars only' : 'all bars'}: ${O('text')}${O('marks') ? ' ' + O('marks') : ''}\n` : '')
      const meaning = {
        '★':   '★ divergence — UDN opposes the daily candle',
        '★★':  '★★ conflict — D & U+ / U & D+',
        '★★★': '★★★ half-up — U & N+ / N & U+',
        '○○○': '○○○ half-down — D & N+ / N & D+',
        'XXX': 'XXX double-neutral — N & N+',
      }
      const marks = P('marks')
      return head + (meaning[s] || (marks ? `marks: ${marks}` : 'lines agree with the candle — no mark'))
           + '\n\nDescriptive only (research family closed NULL).'
    },
    chipCls: (s) => {
      if (s === '★')   return 'bg-yellow-900/60 text-yellow-200 font-bold'
      if (s === '★★')  return 'bg-orange-900/60 text-orange-200 font-bold'
      if (s === '★★★' || s === '○○○') return 'bg-sky-900/60 text-sky-200 font-bold'
      if (s === 'XXX') return 'bg-md-surface-high text-gray-300'
      return 'bg-md-surface-high text-md-on-surface-var'
    },
  },
  {
    // SHAPE × CONTEXT — the TradingView "260910_SHAPE_CTX" script, ported. One label per bar under
    // the Pine display priority (MTH > CL4 > MID > EXP > CON > LST > WRP) with the ↑↓ arrow on the
    // swallow shapes, then the context legs. Values from /api/studio/shapectx-marks merged by date
    // (barsShape), 1d only.
    //
    // DESCRIPTIVE ONLY, and the evidence is unusually specific here — four sealed families,
    // k = 21, 0 BUILD:
    //   · MOTHER_V1        4 NULL + 1 VETO; RSI<35 was the WORST cell, not the best → ⛔KNF
    //   · SHAPE_CLUSTER_V1 NO_CLUSTER was the BEST cell and 🎯🔁 the WORST, MONOTONE decay
    //   · SHAPE_GATE_V1    a cluster before a book edge does NOT damage it (NOT_A_SUPPRESSOR)
    //   · SWALLOW_DIR_V1   LST↑ = −0.63 MINE / −0.47 VERIFY, 0/4 yrs, DSR_neg 0.998 → a VETO
    // So 🎯🔁 is NOT a strength mark: more clustering measured worse. The only chip with
    // two-window evidence is ⛔LST↑, and it says do not buy. Never a score input.
    key: 'shape',
    label: 'SHAPE',
    chipCols: 2,          // a label + up to four legs; one per line made this the tallest row
    // The Pine prints a label ONLY on a bar where a shape fired (`show = anyShape and ...`), and the
    // legs 📍🧱🏆🎯🔁 are part of THAT label, not standalone marks. Emitting them on every bar put
    // 🧱🏆 on ~70% of columns (214 of 300) when any_shape is only ~16% of bars — visual noise, and a
    // silent divergence from the chart the row is supposed to mirror. Shape bars only.
    getSigs: (b) => {
      if (!b.shape || !b.shape_label) return []
      const out = [b.shape_label]
      if (b.shape_lstup_veto) out.push('⛔LST↑')
      if (b.shape_veto) out.push('⛔KNF')
      if (b.shape_grade === 2) out.push('💨')
      else if (b.shape_grade === 0) out.push('⛔dry')
      if (b.shape_sweet) out.push('✅swt')
      if (b.shape_floor) out.push('📍')
      if (b.shape_key) out.push('🧱')
      if (b.shape_rs) out.push('🏆')
      if (b.shape_by_fam) out.push('🎯')
      if (b.shape_by_den) out.push('🔁')
      return out
    },
    sigTitle: (s, b) => {
      const name = { MTH: 'MOTHER  b1 ⊂ q1 · b2 ⊂ q1 · close > q1 top',
                     CL4: 'COIL4  b1 ⊂ b2 · b2 touches q1 · close > q1 top',
                     MID: 'MIDNEST  b1 ⊂ b2 ⊃ b3', EXP: 'EXPAND  b1 ⊂ b2 ⊂ b3',
                     CON: 'CONTRACT  b2 ⊂ b1 · b3 ⊂ b2', LST: 'LASTNEST  b1 ⊂ b3 · b2 ⊂ b3',
                     WRP: 'WRAP  b1 ⊂ b3 (minus LASTNEST / EXPAND)' }[b.shape_code] || ''
      const head = (b.shape_label ? `${b.shape_label} — ${name}\n` : '')
        + (b.shape_can_dir
             ? `${b.shape_dir_up ? '↑ closed ABOVE' : b.shape_dir_dn ? '↓ closed BELOW' : 'flat'} the engulfed body\n`
             : '')
        + `effort ${b.shape_grade ?? '?'}/2${b.shape_vr != null ? ` · vol ${b.shape_vr.toFixed(2)}×avg20` : ''}\n`
        + `rsi ${b.shape_rsi != null ? Math.round(b.shape_rsi) : '?'} (${b.shape_band || '?'}) · pos ${b.shape_pos20 != null ? Math.round(b.shape_pos20 * 100) : '?'}% · touches ${b.shape_touches ?? 0}\n`
        + `🎯 families ${b.shape_cl_fam ?? 0}/3 · 🔁 bars ${b.shape_cl_bars ?? 0}/4  (10-bar window ending here)\n\n`
      const meaning = {
        '⛔LST↑': '⛔ LST↑ — a GREEN bar swallowing both prior bodies. The one shape cell with\n'
                + 'two-window evidence: −0.63 MINE / −0.47 VERIFY, 0 of 4 positive years,\n'
                + 'worst year −3.41, DSR_neg 0.998 over 29,911 trades. This is a VETO — do not buy it.',
        '⛔KNF': '⛔ KNIFE — MOTHER / COIL4 fired while rsi < 35. MOTHER_V1 measured this cell at\n'
              + '−3.54 / −2.04, 0 of 4 positive years: a breakout close while still deeply\n'
              + 'oversold is a dead cat, the opposite of the original hypothesis.',
        '🎯': '🎯 DIVERSITY — ≥3 of the 4 shape families in the last 10 bars.\n'
            + '⚠ NOT a strength mark here. SHAPE_CLUSTER_V1 measured the ladder MONOTONE and\n'
            + 'DOWNWARD: NO_CLUSTER −0.13 was the BEST cell, 🎯🔁 −0.68 the WORST (0/4 yrs).',
        '🔁': '🔁 DENSITY — ≥4 of the last 10 bars carried a shape.\n'
            + 'Measured no different from 🎯 (−0.27 vs −0.19): the two marks read differently on a\n'
            + 'chart but did not behave as different predictive axes.',
        '💨': '💨 absorbed — volume ≥1.5×avg20 on a range ≤1 ATR. The only SCORED axis in the script.',
        '⛔dry': '⛔ dry — volume <0.7×avg20.',
        '✅swt': '✅ the 2-3× inverted-U volume band (project_volume_magnitude).',
        '📍': '📍 FLOOR — close in the bottom 35% of the 20-bar range. CONTEXT ONLY:\n'
            + 'corr(pos20, rsi) = +0.908, so scoring it would score rsi twice.',
        '🧱': '🧱 KEY — ≥2 bars in the last 40 whose low sits within 0.5 ATR of the 20-bar low.',
        '🏆': '🏆 RS intact — close/SPY above its EMA200. Cross-sectional: a badge, not a per-chart score.',
      }
      return head + (meaning[s] || name || 'shape reading')
           + '\n\nDescriptive only — 4 sealed families, k = 21, 0 BUILD.'
    },
    chipCls: (s) => {
      if (s === '⛔LST↑' || s === '⛔KNF') return 'bg-red-900/70 text-red-100 font-bold'
      if (s === '🎯' || s === '🔁') return 'bg-md-surface-high text-gray-400'
      if (s === '💨') return 'bg-emerald-900/60 text-emerald-200'
      if (s === '⛔dry') return 'bg-md-surface-high text-red-300'
      if (s === '✅swt') return 'bg-emerald-900/40 text-emerald-300'
      if (/^(MTH|CL4|MID|EXP|CON|LST|WRP)/.test(s)) return 'bg-indigo-900/60 text-indigo-100 font-bold'
      return 'bg-md-surface-high text-md-on-surface-var'
    },
  },
  {
    // L-VX — the TradingView "260906_WLNBB_L34_L46_VX_CHART" script, ported. The daily L34 / L46
    // graded V (volume > SMA20) · VL / VH (a lower TF echoes the plain / V label) · VX (15m AND
    // 60m both echo). Chip from tier V upward; the plain daily label is in the L row already.
    // Values from /api/studio/lvx-marks merged by date (barsLvx), 1d only. Descriptive only.
    key: 'lvx',
    label: 'L-VX',
    getSigs: (b) => (b.lvx_label && b.lvx_tier >= 1) ? [b.lvx_label] : [],
    sigTitle: (s, b) => {
      const lv = (n) => n === 2 ? 'H (an L·V bar inside)' : n === 1 ? 'L (a plain bar inside)' : '- (none)'
      return `${b.lvx_label} — daily ${b.lvx_fam}, volume > SMA20: ${b.lvx_v ? 'yes' : 'no'}\n`
           + `15m: ${lv(b.lvx_lv15)} · ${b.lvx_n15} plain / ${b.lvx_nv15} V bars of ${b.lvx_bars15}\n`
           + `60m: ${lv(b.lvx_lv1h)} · ${b.lvx_n1h} plain / ${b.lvx_nv1h} V bars of ${b.lvx_bars1h}\n`
           + `daily candle ${b.lvx_colour}\n\n`
           + 'V = daily volume > SMA20 · VL = one lower TF has a plain bar · VH = one lower TF has a V bar · '
           + 'VX = both 15m and 60m echo.\nDescriptive only (same standing as L-BAL).'
    },
    chipCls: (s) => {
      if (s === 'L34VX') return 'bg-lime-800/80 text-lime-200 font-bold ring-1 ring-lime-400'
      if (s === 'L34VH') return 'bg-green-800/80 text-green-200 font-bold'
      if (s.startsWith('L34')) return 'bg-green-900/70 text-green-300 font-semibold'
      if (s === 'L46VX') return 'bg-orange-800/80 text-orange-100 font-bold ring-1 ring-orange-400'
      if (s === 'L46VH') return 'bg-orange-900/80 text-orange-200 font-bold'
      return 'bg-red-900/70 text-red-300 font-semibold'
    },
  },
  {
    // OVD daily map — the TradingView "260904_OVD_4_VOLUME_LOGICS_DAILY_MAP" script ported for
    // display: OB opening build · RC prior-HV reclaim · CD close dominance · HO handoff (·30/·60)
    // · NM? near-miss proxy. From /api/studio/ovdmap-marks merged by date (barsOvd), 1d only.
    // Descriptive only — the sealed OVD research family closed 0 BUILD / 0 VETO.
    key: 'ovd',
    label: 'OVD',
    getSigs: (b) => String(b.ovdmap_tokens || '').split(' ').filter(Boolean),
    sigTitle: (s, b) => {
      const meaning = {
        'OB·30': 'Logic 1 · opening build — both opening 30m windows\' same-slot RVOL rose 3 sessions in a row',
        'OB·60': 'Logic 1 · opening build — opening-60m same-slot RVOL rose 3 sessions in a row',
        'RC·30': 'Logic 2 · prior-HV reclaim — both opening 30m windows ≥ the nearest HV-decline event (6..30 sessions back)',
        'RC·60': 'Logic 2 · prior-HV reclaim — opening 60m ≥ the nearest HV-decline event\'s opening 60m',
        'CD·30': 'Logic 3 · close dominance — 5-day decline, last-30m RVOL > 1, last 30m ≥ first 30m',
        'CD·60': 'Logic 3 · close dominance — 5-day decline, last-60m RVOL > 1, last 60m ≥ first 60m',
        'HO·30': 'Logic 4 · handoff — yesterday\'s heavy last 30m (in a decline) carried into today\'s first 30m (0.80..1.25×)',
        'HO·60': 'Logic 4 · handoff — yesterday\'s heavy last 60m (in a decline) carried into today\'s first 60m (0.80..1.25×)',
        'NM?':   'near-miss proxy — reclaim60 in [0.50, 0.75) and all four opening 15m slots RVOL > 1',
      }
      const f = (x) => (x == null ? '—' : Number(x).toFixed(2))
      return `${meaning[s] || s}\n\nrvO60 ${f(b.ovdmap_rv_o60)} · rvC60 ${f(b.ovdmap_rv_c60)} · `
           + (b.ovdmap_event_k ? `reclaim60 ${f(b.ovdmap_reclaim60)} (event ${b.ovdmap_event_k}d back)` : 'no HV event 6..30d back')
           + ` · handoff60 ${f(b.ovdmap_handoff60)}\nsame-slot RVOL = value / median of the last 20 full sessions\n\n`
           + 'Descriptive only — the sealed OVD research family closed 0 BUILD / 0 VETO.'
    },
    chipCls: (s) => {
      if (s.startsWith('OB')) return s.endsWith('60') ? 'bg-blue-900/70 text-blue-200 font-bold' : 'bg-cyan-900/70 text-cyan-200 font-semibold'
      if (s.startsWith('RC')) return s.endsWith('60') ? 'bg-green-900/70 text-green-200 font-bold' : 'bg-lime-900/70 text-lime-200 font-semibold'
      if (s.startsWith('CD')) return s.endsWith('60') ? 'bg-red-900/70 text-red-200 font-bold' : 'bg-orange-900/70 text-orange-200 font-semibold'
      if (s.startsWith('HO')) return s.endsWith('60') ? 'bg-violet-900/70 text-violet-200 font-bold' : 'bg-fuchsia-900/70 text-fuchsia-200 font-semibold'
      return 'bg-yellow-900/70 text-yellow-200 font-semibold'
    },
  },
  {
    // VOL7 — the TradingView "260829 • 7-Level Volume MR + Sigma" script ported for display. Chip 1
    // = "M4·σ5" (median-ratio level · sigma level) on EVERY bar, coloured by the MR level; then the
    // marks: Σ+/MR+ divergence, ▲+2 ▼−2 skips, ◆+3 ◆−3 big jumps, VB2, SHIFT↑/↓. M5/M6 are the
    // script's B/VB, not the WLNBB bucket. From /api/studio/vol7-marks (barsVol7), 1d only. Descriptive.
    key: 'vol7',
    label: 'VOL7',
    getSigs: (b) => {
      if (!b.vol7_label) return []
      const out = [b.vol7_label]
      for (const m of String(b.vol7_marks || '').split(' ')) if (m) out.push(m)
      return out
    },
    sigTitle: (s, b) => {
      const meaning = {
        'M0': 'extreme low volume — < 0.40× the 20-day median',
        'MR+': 'median-ratio level ≥ 2 above the sigma level (spiky window inflates σ)',
        'Σ+': 'sigma level ≥ 2 above the median-ratio level (quiet, uniform window makes σ tiny — σ-based labels over-rate this bar)',
        '▲+2': 'volume regime skipped UP two levels vs yesterday', '▼−2': 'volume regime skipped DOWN two levels vs yesterday',
        '◆+3': 'BIG volume jump UP (≥ 3 levels vs yesterday)', '◆−3': 'BIG volume jump DOWN (≥ 3 levels vs yesterday)',
        'VB2': 'new extreme-volume (M6) event ≥ 6 bars after the previous VB (VBs within 2 bars = one event)',
        'SHIFT↑': 'bullish regime shift — VB2 after a ≥ 3% 10-day decline, then a green close above the VB range within 5 bars',
        'SHIFT↓': 'bearish regime shift — VB2 after a ≥ 3% 10-day rise, then a red close below the VB range within 5 bars',
      }
      const f = (x, nd = 2) => (x == null ? '—' : Number(x).toFixed(nd))
      const head = `M${b.vol7_mr} = ${f(b.vol7_ratio)}× the 20-day median (M0 <0.4 · M1 <0.7 · M2 <1 · M3 <1.5 · M4 <2.5 · M5 <5 · M6 ≥5)\n`
                 + `σ${b.vol7_sg} = BB(volume,20,1) level (σ5 = +1σ..+2σ, σ6 ≥ +2σ) · consensus ${b.vol7_cons || '±1'}\n`
                 + `level vs yesterday ${b.vol7_trans > 0 ? '+' : ''}${b.vol7_trans ?? 0}` + (b.vol7_prior_move != null ? ` · prior 10d move ${f(b.vol7_prior_move, 1)}%` : '') + '\n'
      return head + (meaning[s] ? `\n${s}: ${meaning[s]}` : (b.vol7_mr >= 5 ? `\nM${b.vol7_mr} is the script's ${b.vol7_mr === 6 ? 'VB' : 'B'} — NOT the app's WLNBB bucket` : ''))
           + '\n\nDescriptive only; SHIFT is an unstudied setup marker.'
    },
    chipCls: (s, b) => {
      if (s === b.vol7_label) {
        const m = b.vol7_mr
        return m === 6 ? 'bg-red-900/70 text-red-200 font-bold' : m === 5 ? 'bg-orange-900/70 text-orange-200 font-bold'
             : m === 4 ? 'bg-yellow-900/50 text-yellow-200' : m === 0 ? 'bg-gray-800 text-gray-400' : 'bg-md-surface-high text-md-on-surface-var'
      }
      if (s === 'SHIFT↑') return 'bg-lime-800/80 text-lime-100 font-bold ring-1 ring-lime-400'
      if (s === 'SHIFT↓') return 'bg-red-800/80 text-red-100 font-bold ring-1 ring-red-400'
      if (s === 'VB2') return 'bg-purple-900/70 text-purple-200 font-bold'
      if (s === '◆+3') return 'bg-lime-800/80 text-lime-200 font-bold'
      if (s === '◆−3') return 'bg-red-800/80 text-red-200 font-bold'
      if (s === '▲+2') return 'bg-lime-900/70 text-lime-300'
      if (s === '▼−2') return 'bg-red-900/70 text-red-300'
      if (s === 'Σ+') return 'bg-orange-900/70 text-orange-200 font-semibold'
      if (s === 'MR+') return 'bg-cyan-900/70 text-cyan-200 font-semibold'
      return 'bg-gray-800 text-gray-400'
    },
  },
  {
    // V4 — flat baseline (user-directed FIRST PASS, 2026-09-23): 5 points per SIG_GROUPS entry
    // that fires on this bar, no weighting, no correlation control, UNMEASURED. Same catalog and
    // evaluator as the Ultra screener's V4 column (src/lib/v4Score.js) — not a second definition.
    // Placed directly after VOL7 (user's own placement).
    //
    // ⚠️ Not yet verified numerically identical to the Ultra screener for the same ticker/day —
    // see the caveat on barsV4 above. >45 = green is the user's OWN threshold for their OWN
    // all-5 weight map (restored 2026-09-24 after a one-day book-verdict trial —
    // research_out/V4_BOOK_WEIGHTS_V1.md). V4_HISTORY_V1 measured this map as a signal counter
    // with no forward-return ordering, so the colour is a preference, not an empirical zone.
    // Same cut as the Ultra screener's V4 column (ScannerDataGrid.jsx).
    key: 'v4',
    label: 'V4',
    // v4_score is often exactly 0 right now (every weight starts at 0) — a plain truthy
    // check would hide the chip on every such bar. Show it whenever a signal fired at all,
    // so the tooltip's fired-list stays visible while the user is still assigning weights.
    getSigs: (b) => (b.v4_fired_labels?.length ? [String(b.v4_score)] : []),
    sigTitle: (sig, b) => {
      const labels = (b.v4_fired_labels || []).join(' \u00b7 ')
      return `V4 = ${sig} (sum of per-signal weights, ${b.v4_fired_labels?.length ?? 0} signals fired today)\n`
           + (labels ? `fired: ${labels}\n\n` : '\n')
           + 'Every signal starts at 0 (src/lib/v4Weights.js); the user is assigning weights one '
           + 'at a time. Each fired signal above shows its label and current weight, e.g. "T2G(0)".'
    },
    chipCls: (sig, b) => (b?.v4_score > 45
      ? 'bg-green-900 text-green-300 font-mono font-bold ring-1 ring-green-500/60'
      : 'bg-md-surface-high text-md-on-surface-var font-mono'),
  },
  {
    // TURN·58 — descriptive turn-likelihood gauge (research_out/TURN_SET_V1.md, user "gaakete"
    // 2026-09-27). Only on a 10-bar low. Shown as "n/58": how many of the 58 keys that were
    // enriched at turns in MINE 2021-23 fired in the last 3 bars. Turn likelihood rises with n and
    // replicated in 2024-26, but as a TRADE it did not beat other 10-bar lows — so the colour
    // grades likelihood in neutral tones, never green, and it is never a score input.
    key: 'turn',
    label: 'TURN·58',
    getSigs: (b) => (b.turn_cand ? [`${b.turn_n}/58`] : []),
    sigTitle: (sig, b) => {
      const band = TURN_LIFT.find(([m]) => (b.turn_n ?? 0) >= m)
      return `TURN·58 = ${b.turn_n} of 58 turn-enriched signals in the last 3 bars (this bar is a 10-bar low)\n`
           + (band ? `2024-26: lows with ≥${band[0]} turned ${band[1]}× as often as an average 10-bar low\n`
                   : 'below 20: turn likelihood close to an average 10-bar low\n')
           + `\nmatched: ${(b.turn_keys || []).join(' · ') || '—'}\n\n`
           + 'DESCRIPTIVE: turn likelihood replicated out of sample, but as a trade (ATR×12 trail) these '
           + 'lows did not beat other 10-bar lows. Not a buy signal, never a score input.'
    },
    chipCls: (sig, b) => {
      const n = b?.turn_n ?? 0
      return n >= 28 ? 'bg-amber-900/70 text-amber-100 font-mono font-bold'
           : n >= 20 ? 'bg-amber-950/60 text-amber-200 font-mono'
           : n >= 10 ? 'bg-md-surface-high text-md-on-surface font-mono'
           : 'bg-md-surface-high text-md-on-surface-var font-mono opacity-70'
    },
  },
  {
    // ⟲ROW-TURN — per-row turn-zone tiers (research_out/ROWSEQ_V1.md, user "ki gaakete" 2026-09-28).
    // Each shown row's OWN 5-bar sequence scored with its MINE-selected features; ● = CONFIRMED
    // (MINE q95), ○ = EARLY (q80). ◆V∧M = VOL7 and MTF both confirmed. Turn-zone IDENTIFICATION
    // only: big-move prediction was NULL and same-day return ≈ 0 — neutral colours, never green,
    // never a score input. Frozen spec in lib/rowSeqSpec.json (JS↔research tier parity 100 %).
    key: 'rowseq',
    label: '⟲ROW',
    getSigs: (b) => {
      const t = b.rs_tiers; if (!t) return []
      const out = t.pair ? ['◆V∧M'] : []
      for (const r of ROW_ORDER) if (t[r] === 2) out.push(`${ROW_SHORT[r]}●`)
      for (const r of ROW_ORDER) if (t[r] === 1) out.push(`${ROW_SHORT[r]}○`)
      return out
    },
    sigTitle: (sig, b) => {
      const head = sig === '◆V∧M'
        ? `◆ VOL7 ∧ MTF both CONFIRMED — the one row pair that added beyond the best single row: turn-zone lift ${PAIR_LIFT} (after removing price-location × ATR% effects, 2024-26), ~0.4 % of bars.`
        : (() => {
            const r = ROW_ORDER.find(x => sig.startsWith(ROW_SHORT[x]))
            return `${r} row turn sequence — ${sig.endsWith('●') ? 'CONFIRMED (top 5 % of MINE scores)' : 'EARLY (top 20 %)'}; confirmed-tier turn-zone lift ${ROW_LIFT[r]} after removing price-location × ATR% effects (2024-26).`
          })()
      return head + '\n\nDESCRIPTIVE: identifies turn zones (21-bar pivot within ±3 bars, then +3 ATR). Many rows together add nothing over the best one; '
           + 'predicting ≥ +30 % moves was NULL (volatility only) and the same-day trade return ≈ 0. Not a buy signal, never a score input.'
    },
    chipCls: (sig) => (sig === '◆V∧M' ? 'bg-amber-800/80 text-amber-50 font-mono font-bold ring-1 ring-amber-400/60'
      : sig.endsWith('●') ? 'bg-amber-950/60 text-amber-200 font-mono font-semibold'
      : 'bg-md-surface-high text-md-on-surface-var font-mono opacity-80'),
  },
  {
    // TOP·58 — the best turn identifiers among the 58 TURN keys (research_out/TURN58_TOP_V1.md, user
    // "damimate eseni oriveshi" 2026-09-28). Pairs first (both keys on in t-2..t), then singles. All 20
    // passed on VERIFY as turn-zone IDENTIFIERS; only the 🕐DR pairs also showed a positive same-day
    // return (descriptive, not path-sim-tested) — they get the brighter chip. Never a score input.
    key: 'top58',
    label: 'TOP·58',
    getSigs: (b) => {
      const t = b.top58; if (!t) return []
      return [...TOP_PAIRS.filter(p => t.pairs.includes(p.code)).map(p => p.label),
              ...TOP_SINGLES.filter(x => t.singles.includes(x.code)).map(x => x.label)]
    },
    sigTitle: (sig) => {
      const p = TOP_PAIRS.find(x => x.label === sig); const x = TOP_SINGLES.find(y => y.label === sig)
      const it = p || x; if (!it) return undefined
      return `${p ? 'TOP pair' : 'TOP single'} ${sig} — on in the last 3 bars. Turn-zone lift ${it.lift} vs a bar with the same price location and ATR% (VERIFY 2024-26, all US tickers in the DB). `
           + `Same-day return vs the day's other bars: ${it.sd >= 0 ? '+' : ''}${it.sd.toFixed(2)} pp (descriptive).\n\n`
           + 'DESCRIPTIVE turn-zone identification (TURN58_TOP_V1). Not path-sim-tested as a setup; not a buy signal, never a score input.'
    },
    chipCls: (sig) => {
      const p = TOP_PAIRS.find(x => x.label === sig)
      if (p && p.sd > 1) return 'bg-amber-700/80 text-amber-50 font-mono font-bold ring-1 ring-amber-300/60'
      if (p) return 'bg-amber-950/60 text-amber-200 font-mono font-semibold'
      return 'bg-md-surface-high text-md-on-surface-var font-mono opacity-80'
    },
  },
  {
    // 🏅 RANK_V1 (2026-09-07, sealed family, user OK): per bar, the fire's expected edge from the A state table
    // as a percentile of THAT day's fires across the universe (last 270 sessions of the warm frame). Bars with
    // no edge fire carry nothing. Values arrive on the bar itself (api_bar_signals), no separate fetch.
    key: 'rank',
    label: '🏅',
    getSigs: (b) => (b.rank_pct != null ? [String(b.rank_pct)] : []),
    sigTitle: (s, b) => `🏅 RANK ${b.rank_pct} — expected edge ${b.rank_edge >= 0 ? '+' : ''}${b.rank_edge} pp vs that day's median · family ${b.rank_fam} · ${b.rank_n} fires that day\n`
                      + 'Sealed RANK_V1: state table (family × RSI band × conso × RS × price) — walk-forward top-10/day +4.57 vs pool +2.04 (2025-26, 5/5 yrs); '
                      + 'adding display layers or scores LOWERED the head, so this is the plain table on purpose.',
    chipCls: (s) => {
      const n = Number(s)
      if (n >= 90) return 'bg-yellow-700 text-yellow-100 font-bold ring-1 ring-yellow-400'
      if (n >= 75) return 'bg-green-900 text-green-200 font-semibold'
      if (n >= 50) return 'bg-md-surface-high text-md-on-surface-var'
      return 'bg-md-surface-high text-md-on-surface-var/50'
    },
  },
  {
    key: 'score',
    label: 'SCORE',
    getSigs: (b) => {
      const fbs = b.final_bull_score ?? 0
      const ss  = b.signal_score ?? 0
      const score = fbs > 0 ? fbs : ss
      // no display threshold (2026-07-18): SCORE = turbo_score (canonical alias), and the
      // separate turbo row was removed as a duplicate — so SCORE must show EVERY value.
      return score > 0 ? [score] : []
    },
    chipCls: (s) => {
      const n = Number(s)
      if (n >= 140) return 'bg-yellow-700 text-yellow-100 font-bold ring-1 ring-yellow-400'
      if (n >= 115) return 'bg-lime-800 text-lime-100 font-bold ring-1 ring-lime-400'
      if (n >= 90)  return 'bg-green-900 text-green-200 font-semibold'
      if (n >= 65)  return 'bg-teal-900 text-teal-300'
      return 'bg-md-surface-high text-md-on-surface-var'
    },
  },
  {
    key: 'prebreak_v3',
    label: 'V3',
    getSigs: (b) => {
      const v = b.prebreak_v3 ?? 0
      return v >= 8 ? [String(v)] : []
    },
    sigTitle: (s, b) => b?.prebreak_v3_reasons || `PreBreakout v3 = ${s}`,
    chipCls: (s) => {
      const n = Number(s)
      if (n >= 35) return 'bg-fuchsia-700 text-fuchsia-100 font-bold ring-1 ring-fuchsia-400'
      if (n >= 25) return 'bg-purple-800 text-purple-100 font-semibold'
      if (n >= 15) return 'bg-indigo-900 text-indigo-200'
      return 'bg-md-surface-high text-md-on-surface-var'
    },
  },
]


// Chip-style signal row (Z/T/L/FLY/... + SCORE/V3) — extracted so the score block can be
// rendered between the L and FLY rows in the screener's column order (2026-07-18).
function ChipRow({ row, bars }) {
  return (
    <tr className="border-t border-white/[0.06] hover:bg-md-surface-high/20">
      <td
        className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                   text-right border-r border-white/[0.08] font-mono whitespace-nowrap"
        style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13, lineHeight: 1 }}>
        {row.label}
      </td>
      {bars.map((b, i) => {
        const sigs = row.getSigs(b, bars[i - 1])
        return (
          <td key={i}
            className="px-0 py-px text-center border-r border-white/[0.05] align-top"
            style={{ width: CELL_W, minWidth: CELL_W }}>
            {/* `chipCols: 2` packs the chips two per line instead of one (user, 2026-09-10, for the
                SHAPE row: a label plus four legs was six lines tall and stretched the whole table,
                while one horizontal line would have been unreadable at 64 px). Rows that do not
                set it keep the original single column. */}
            <div className={row.chipCols === 2
                              ? 'grid grid-cols-2 gap-px justify-items-center'
                              : 'flex flex-col gap-px items-center'}>
              {sigs.map((s, si) => (
                <span key={s}
                  title={row.sigTitle ? row.sigTitle(s, b) : undefined}
                  // A 64 px cell splits into two ~32 px columns: that fits one glyph (📍🧱🏆🎯🔁) or a
                  // 3-character code, but not '⛔dry' / '⛔LST↑', which were being clipped. Two cases
                  // take the full width instead: the FIRST chip, which is the identity and should sit
                  // on its own line whether or not it carries an arrow (otherwise 'MID' and 'WRP↑'
                  // lay out differently), and anything wider than 3 glyphs.
                  // [...s].length, not s.length — an emoji is two UTF-16 units.
                  className={`px-1 py-px rounded border border-white/10 font-mono leading-none
                              ${row.chipCols === 2 && (si === 0 || [...s].length > 3) ? 'col-span-2' : ''} ${row.chipCls(s, b)}`}
                  style={{ fontSize: row.chipCols === 2 ? 11 : 12 }}>
                  {s}
                </span>
              ))}
            </div>
          </td>
        )
      })}
    </tr>
  )
}

const BETA_ZONE_CLS = {
  ELITE:       'text-amber-200 font-bold',
  OPTIMAL:     'text-emerald-300 font-bold',
  BUY:         'text-blue-300 font-bold',
  WATCH:       'text-violet-300',
  BUILDING:    'text-yellow-400',
  EXTENDED:    'text-amber-400',
  SHORT_WATCH: 'text-red-400',
  NEUTRAL:     'text-md-on-surface-var/70',
}
const BETA_ZONE_SHORT = {
  ELITE: 'ELT', OPTIMAL: 'OPT', BUY: 'BUY', WATCH: 'WCH',
  BUILDING: 'BLD', EXTENDED: 'EXT', SHORT_WATCH: 'SHT', NEUTRAL: '',
}

function barsForTf(tf) {
  // Per-bar signal-matrix history depth. Bumped so the matrix shows ~300 bars of
  // history instead of ~150 (it used to stop ~7 months back on the daily view).
  //
  // 1d: 750 (2026-09-22) reverted back to 300 (2026-09-23, user: page loads too slowly). The
  // isolated /api/bar_signals timing was flat either way (300 -> 3.69s, 750 -> 3.58s — it always
  // recomputes over the full series), but barsForTf(f) also sizes the LIMIT on every one of the
  // parallel display-layer fetches this panel makes on load (LBAL/LVX/OVD/VOL7/SHAPE/PV/V4), so
  // 750 meant seven separate calls each asking for a wider slice, not one. That compounded cost
  // is what the user felt. 300 is back to being the trailing default rather than its own branch.
  return tf === '15m' ? 500 : ['30m', '1h'].includes(tf) ? 400 : tf === '4h' ? 300
       : tf === '1w' ? 260 : 300   // 1d/1w-other -> 300; 1w → 260 (~5 years)
}

function fmtDate(d, isIntraday) {
  if (typeof d === 'number') {
    const dt = new Date(d * 1000)
    if (isIntraday)
      return `${dt.getMonth() + 1}/${dt.getDate()} ${String(dt.getHours()).padStart(2, '0')}:${String(dt.getMinutes()).padStart(2, '0')}`
    return `${dt.getMonth() + 1}/${dt.getDate()}`
  }
  return String(d).slice(5)
}

function MiniCandle({ b, globalMin, globalRange, h = MINI_H }) {
  const cx  = CELL_W / 2
  const bw  = 10
  const toY = (p) => h - ((p - globalMin) / globalRange) * (h - 2) - 1
  const isUp = b.close >= b.open
  const color = isUp ? '#22c55e' : '#ef4444'
  const bodyTop = Math.min(toY(b.open), toY(b.close))
  const bodyH   = Math.max(1, Math.abs(toY(b.open) - toY(b.close)))
  return (
    <svg width={CELL_W} height={h} style={{ display: 'block' }}>
      <line x1={cx} y1={toY(b.high)} x2={cx} y2={toY(b.low)}
            stroke={color} strokeWidth={0.8} />
      <rect x={cx - bw / 2} y={bodyTop} width={bw} height={bodyH} fill={color} />
    </svg>
  )
}

export default function SuperchartPanel({
  initialTicker = 'AAPL', initialTf = '1d', initialTrade = null,
  onTickerChange, with1H = false,
}) {
  const [ticker, setTicker]       = useState(initialTicker)
  const [inputVal, setInputVal]   = useState(initialTicker)
  const [tf, setTf]               = useState(initialTf)
  const [bars, setBars]           = useState([])
  const [v2Map, setV2Map]         = useState({})   // date(YYYY-MM-DD) → {v2, band} from DB (daily only)
  // date → the bar's physics fields. Merged from the studio DB rather than recomputed in
  // /api/bar-signals: that endpoint derives its signals from OHLCV in-process, and a second
  // implementation of the physics there would drift from the stored columns the moment a
  // threshold moved — the same reason the chart strip and this row share one rule set.
  const [physMap, setPhysMap]     = useState({})
  // rare | ra | all — same three settings as the chart strip's ⚛ selector, because the
  // two surfaces share one rule and must share its control too.
  //
  // Defaults to 'all' — the full per-bar state, matching the Pine pane.
  //
  // I defaulted this to the sparse set twice and defended it twice: R/E/M are three-way
  // splits so "not the middle" is two thirds of every chart, S3 and K1 hold for weeks, and
  // a row that marks every bar marks nothing. All measured, all true, and all beside the
  // point — the row was asked for as a port of the Pine script, and the Pine script prints
  // every bar. Density is a reading preference and it is the reader's to set, not a
  // conclusion I get to bake in from a percentage table.
  //
  // PERSISTED, because it is a preference and not a per-visit decision. Held only in
  // component state it reset on every reload and every ticker change, so re-selecting it
  // each time reasonably read as the setting not working.
  // The key is versioned, and that is not decoration. Persistence was added while the
  // default was still the sparse set, so anyone using the row at that moment had '+RA'
  // written to storage — and a stored value beats a default. Changing the default then did
  // nothing at all for exactly the person who had asked for the change, which is the worst
  // possible audience for a no-op. Bumping the key retires those saved values once; every
  // choice made from here is kept.
  const [physRowMode, setPhysRowMode] = useState(() => {
    try { return localStorage.getItem('sc_phys_mode_v2') || 'all' } catch { return 'all' }
  })
  useEffect(() => {
    try {
      localStorage.setItem('sc_phys_mode_v2', physRowMode)
      localStorage.removeItem('sc_phys_mode')
    } catch { /* private mode */ }
  }, [physRowMode])
  const [day1hMap, setDay1hMap]   = useState({})   // date(YYYY-MM-DD) → {up, hours[]} (with1H only)
  // date → lbal_* fields (UDN / UDN+ / marks) from /api/studio/lbal-marks (1d only). Kept as its
  // own map and merged like physMap: the fetch resolves independently of /api/bar_signals.
  const [lbalMap, setLbalMap]     = useState({})
  // No `useLbalMode()` here any more: the two UDN★ rows are FIXED to V and ALL, so this
  // panel no longer follows the shared switch. The switch still lives on the chart line
  // (CodeCandleChart) and on the Ultra chips, which do show one mode at a time.
  const [lvxMap, setLvxMap]       = useState({})   // date → lvx_* (daily L34/L46 V·VL·VH·VX), 1d only
  const [ovdMap, setOvdMap]       = useState({})   // date → ovdmap_* (OB/RC/CD/HO ·30/·60 · NM?), 1d only
  const [vol7Map, setVol7Map]     = useState({})   // date → vol7_* (M·σ levels, jumps, VB2, SHIFT), 1d only
  const [shapeMap, setShapeMap]   = useState({})   // date → shape_* (body-nest shape + context + cluster), 1d only
  const [pvMap, setPvMap]         = useState({})   // date → pv_* (nine price×volume shapes, both price sources), 1d only
  const [veMap, setVeMap]         = useState({})   // date → ve_* (VOL ECHO: VE · Q · R · ▲▼ · BO/BD · spike), 1d only
  const [loading, setLoading]     = useState(false)
  const [error, setError]         = useState(null)
  const [showStats, setShowStats] = useState(false)
  const [statsData, setStatsData] = useState(null)
  const [statsLoading, setStatsLoading] = useState(false)
  const [statsSort, setStatsSort] = useState('avg_5bar')
  const matrixRef  = useRef(null)
  const isIntraday = ['4h', '1h', '30m', '15m'].includes(tf)

  // Stats rows sorted by selected column
  const sortedStats = useMemo(() => {
    if (!statsData?.results) return []
    return Object.entries(statsData.results)
      .filter(([, v]) => (v.n ?? 0) >= 3 && !v.warning)
      .sort(([, a], [, b]) => (b[statsSort] ?? -999) - (a[statsSort] ?? -999))
  }, [statsData, statsSort])

  // Mini-candle global price range
  const { globalMin, globalRange } = useMemo(() => {
    if (!bars.length) return { globalMin: 0, globalRange: 1 }
    const lo = Math.min(...bars.map(b => b.low))
    const hi = Math.max(...bars.map(b => b.high))
    return { globalMin: lo, globalRange: (hi - lo) || 1 }
  }, [bars])

  // Notify parent so global chart follows Superchart ticker/tf
  useEffect(() => { onTickerChange?.(ticker, tf) }, [ticker, tf])

  // bars carrying their physics. Kept as a derived value rather than merged into `bars`
  // state: the two fetches resolve independently, and mutating the signal rows in place
  // would make the row that renders first show whichever arrived first.
  const barsPhys = useMemo(
    () => bars.map(b => ({ ...b, ...(physMap[String(b.date).slice(0, 10)] || {}) })),
    [bars, physMap])
  const barsLbal = useMemo(
    () => bars.map(b => ({ ...b, ...(lbalMap[String(b.date).slice(0, 10)] || {}) })),
    [bars, lbalMap])
  const barsLvx = useMemo(
    () => bars.map(b => ({ ...b, ...(lvxMap[String(b.date).slice(0, 10)] || {}) })),
    [bars, lvxMap])
  const barsOvd = useMemo(
    () => bars.map(b => ({ ...b, ...(ovdMap[String(b.date).slice(0, 10)] || {}) })),
    [bars, ovdMap])
  const barsVol7 = useMemo(
    () => bars.map(b => ({ ...b, ...(vol7Map[String(b.date).slice(0, 10)] || {}) })),
    [bars, vol7Map])
  const barsShape = useMemo(
    () => bars.map(b => ({ ...b, ...(shapeMap[String(b.date).slice(0, 10)] || {}) })),
    [bars, shapeMap])
  const barsPv = useMemo(
    () => bars.map(b => ({ ...b, ...(pvMap[String(b.date).slice(0, 10)] || {}) })),
    [bars, pvMap])
  const barsVe = useMemo(
    () => bars.map(b => ({ ...b, ...(veMap[String(b.date).slice(0, 10)] || {}) })),
    [bars, veMap])

  // V4 — per-signal WEIGHTS, user-directed (2026-09-23; revised from the flat first pass once
  // the user saw how many SIG_GROUPS entries repeat the same information). Every signal starts
  // at 0 (src/lib/v4Weights.js); reuses the Ultra screener's OWN filter catalog and the SAME
  // weight map (imported, not re-typed) so the two surfaces read one definition of "what a
  // signal is worth," not two that can drift apart.
  //
  // ⚠️ NOT YET VERIFIED IDENTICAL TO ULTRA. SIG_GROUPS was written against Ultra's scan row; this
  // merged bar shares the display-layer fields (SHAPE/PV/LBAL/LVX/OVD/VOL7 — same backend store,
  // guaranteed identical) but the "base" families read straight from /api/bar_signals, whose raw
  // sig_* naming differs from some UI-aliased keys (bias_up, vol_spike_5x, …) Ultra's scan row
  // carries after a backend rename table. Those specific predicates can under-fire here. See
  // src/lib/v4Score.js for the full caveat; a fix is a named follow-up, not done in this pass.
  // ▽△ anatomy verdict (AnatRow above, day1hMap) is Superchart-ONLY — it comes from
  // /api/day1h, a per-ticker 1H-decomposition fetch that Ultra's scan never makes (300
  // tickers × 1H history would be far too expensive for a screener pass). So x_anat_* can
  // score here but will simply never fire on the Ultra Screener's V4 column — a real,
  // permanent asymmetry, not a bug (2026-09-23, user confirmed wanting this added: "ki minda").
  const barsV4 = useMemo(
    () => bars.map(b => {
      const d = String(b.date).slice(0, 10)
      const anat = day1hMap[d]?.anat
      // physMap (2026-09-24): the ⚛ family (_ph_* — RA/E2/E★/K2/S3U/AD/SPRING⚛ …, ~35 pts of
      // the ICE 160-vs-100 gap) was never in this merge, so no physics signal could fire in V4
      // here while it fired on Ultra. physMap is the SAME studio-DB source Ultra reads.
      const merged = { ...b, ...(physMap[d] || {}), ...(shapeMap[d] || {}), ...(pvMap[d] || {}),
                       ...(lbalMap[d] || {}), ...(lvxMap[d] || {}), ...(ovdMap[d] || {}),
                       ...(vol7Map[d] || {}), ...(veMap[d] || {}),
                       anat_v: anat?.v, anat_s: anat?.s, anat_rs: anat?.rs }
      return { ...merged, v4_score: v4Score(V4_ALL_GROUPS, V4_WEIGHTS, merged, 1),
               v4_fired_labels: v4FiredLabels(V4_ALL_GROUPS, V4_WEIGHTS, merged, 1),
               _v4keys: v4Fired(V4_ALL_GROUPS, merged, 1).map(s => s.key) }
    }),
    [bars, physMap, shapeMap, pvMap, lbalMap, lvxMap, ovdMap, vol7Map, veMap, day1hMap])
  // TURN·58 (2026-09-27, research_out/TURN_SET_V1.md): on a 10-bar low, how many of the 58 keys
  // enriched at turns fired in t-2..t. Rides on barsV4 because it needs the same fired-key set.
  const barsTurn = useMemo(() => withTurnCount(barsV4), [barsV4])
  // ⟲ROW-TURN (2026-09-28, research_out/ROWSEQ_V1.md) — per-row turn-zone tiers; rides on barsV4 for _v4keys + phys_*.
  const barsRowSeq = useMemo(() => withRowSeq(barsV4), [barsV4])
  // TOP·58 (2026-09-28, research_out/TURN58_TOP_V1.md) — best singles/pairs of the 58 turn keys, t-2..t.
  const barsTop58 = useMemo(() => withTopPairs(barsV4), [barsV4])

  const load = useCallback((t, f) => {
    setLoading(true)
    setError(null)
    api.barSignals(t, f, barsForTf(f))
      .then(data => {
        setBars(data)
        setTimeout(() => {
          if (matrixRef.current)
            matrixRef.current.scrollLeft = matrixRef.current.scrollWidth
        }, 120)
      })
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
    // PreBreakout v2 is a DB enrichment (daily only) — fetch + merge by date.
    if (f === '1d') {
      api.studioBars(t, 400)
        .then(rows => {
          const m = {}
          const ph = {}
          for (const r of (rows || [])) {
            if (r?.date == null) continue
            const k = String(r.date).slice(0, 10)
            if (r.prebreak_v2 != null) m[k] = { v2: r.prebreak_v2, band: r.prebreak_v2_band }
            // EVERY phys_* column, copied by prefix rather than by a hand-written list.
            //
            // The list version carried seven fields — the ones the rare set happened to
            // need when it was written. phys_m, phys_c, phys_h and phys_s were never
            // fetched, so the 'all' mode read them, found undefined, and printed nothing:
            // it claimed to show the full per-bar state while silently missing four of its
            // nine lines, and the CSV export inherited the same hole. A hand-maintained
            // copy list goes stale the moment a column is added and fails by omission,
            // which is the failure mode nobody sees.
            const ps = {}
            for (const key in r) if (key.startsWith('phys_')) ps[key] = r[key]
            ph[k] = ps
          }
          setV2Map(m); setPhysMap(ph)
        })
        .catch(() => { setV2Map({}); setPhysMap({}) })
    } else {
      setV2Map({}); setPhysMap({})
    }
    // L-BAL (UDN / UDN+ / ★ marks) + L-VX (graded daily L34/L46) — 1d only, merged by date.
    if (f === '1d') {
      api.lbalMarks(t, barsForTf(f) + 20)
        .then(d => {
          const m = {}
          for (const r of (d?.marks ?? [])) if (r?.date) m[String(r.date).slice(0, 10)] = r
          setLbalMap(m)
        })
        .catch(() => setLbalMap({}))
      api.lvxMarks(t, barsForTf(f) + 20)
        .then(d => {
          const m = {}
          for (const r of (d?.marks ?? [])) if (r?.date) m[String(r.date).slice(0, 10)] = r
          setLvxMap(m)
        })
        .catch(() => setLvxMap({}))
      api.ovdmapMarks(t, barsForTf(f) + 20)
        .then(d => {
          const m = {}
          for (const r of (d?.marks ?? [])) if (r?.date) m[String(r.date).slice(0, 10)] = r
          setOvdMap(m)
        })
        .catch(() => setOvdMap({}))
      api.vol7Marks(t, barsForTf(f) + 20)
        .then(d => {
          const m = {}
          for (const r of (d?.marks ?? [])) if (r?.date) m[String(r.date).slice(0, 10)] = r
          setVol7Map(m)
        })
        .catch(() => setVol7Map({}))
      api.shapectxMarks(t, barsForTf(f) + 20)
        .then(d => {
          const m = {}
          for (const r of (d?.marks ?? [])) if (r?.date) m[String(r.date).slice(0, 10)] = r
          setShapeMap(m)
        })
        .catch(() => setShapeMap({}))
      api.pvMultiMarks(t, barsForTf(f) + 20)
        .then(d => {
          const m = {}
          for (const r of (d?.marks ?? [])) if (r?.date) m[String(r.date).slice(0, 10)] = r
          setPvMap(m)
        })
        .catch(() => setPvMap({}))
      api.volEchoMarks(t, barsForTf(f) + 20)
        .then(d => {
          const m = {}
          for (const r of (d?.marks ?? [])) if (r?.date) m[String(r.date).slice(0, 10)] = r
          setVeMap(m)
        })
        .catch(() => setVeMap({}))
    } else {
      setLbalMap({}); setLvxMap({}); setOvdMap({}); setVol7Map({}); setShapeMap({}); setPvMap({}); setVeMap({})
    }
    // Bottom-Anatomy + (with1H) 1H-decomposition — each day → its 1H bars + anatomy
    // verdict. Fetched on EVERY daily load so the ▽△ anatomy row shows on the main
    // Superchart too; the 1H-decomposition row itself renders only when with1H.
    if (f === '1d') {
      api.day1h(t, 300)
        .then(d => {
          const m = {}
          for (const day of (d?.days ?? [])) m[day.date] = day
          setDay1hMap(m)
        })
        .catch(() => setDay1hMap({}))
    } else {
      setDay1hMap({})
    }
  }, [with1H])

  const loadStats = useCallback((t, f) => {
    setStatsLoading(true)
    api.signalStats(t, f, [], false, 3)
      .then(d => setStatsData(d))
      .catch(() => setStatsData(null))
      .finally(() => setStatsLoading(false))
  }, [])

  // The matrix renders 300 bars because 300 columns is already more than anyone scrolls.
  // The CSV is not a screen, so it fetches the FULL history at click time instead of
  // inheriting that number. AAPL: 300 → 1255 bars, ~5s, and the chart's own 2000-bar
  // setting never had anything to do with either.
  const EXPORT_LIMIT = 5000        // bar-signals: no server cap, returns whatever exists
  const STUDIO_LIMIT = 2500        // /api/studio/bars rejects anything above this with 422
  const [csvBusy, setCsvBusy] = useState(false)

  const exportCsv = useCallback(async () => {
    if (!bars.length || csvBusy) return
    setCsvBusy(true)
    // Fall back to what is on screen if the wider fetch fails — a 300-bar export beats no
    // export, as long as it is the fallback and not the design.
    let src = bars, srcPhys = barsPhys
    // allSettled, NOT all. The first version awaited both together, so when studio/bars
    // rejected — it caps limit at 2500 and 422s above it — the whole thing fell into the
    // catch and the export silently shrank back to the 300 bars on screen. It looked like
    // the wider fetch had simply not been wired up. One optional enrichment failing must
    // not discard a request that succeeded.
    // 2026-09-10: the five display layers shipped since 2026-09-06 (L-BAL, L-VX, OVD, VOL7,
    // SHAPE) had Superchart ROWS but no CSV columns, so a year of them was invisible to any
    // analysis done outside the app. They live in per-date maps sized to the screen (~400 bars),
    // which is why they cannot simply be read off `bars` here — the export fetches its own full
    // history for each, exactly as it already does for physics. RANK is per-bar on the signal
    // rows themselves and needs no extra fetch. Each is independent: allSettled, and a failure
    // blanks its own columns instead of shrinking the export.
    const MARK_LIMIT = 5000
    const marksFor = (fn) => (tf === '1d' ? fn(ticker, MARK_LIMIT) : Promise.resolve({ marks: [] }))
    const [sigRes, physRes, lbalRes, lvxRes, ovdRes, vol7Res, shapeRes, pvRes, veRes] = await Promise.allSettled([
      api.barSignals(ticker, tf, EXPORT_LIMIT),
      tf === '1d' ? api.studioBars(ticker, STUDIO_LIMIT) : Promise.resolve([]),
      marksFor(api.lbalMarks), marksFor(api.lvxMarks), marksFor(api.ovdmapMarks),
      marksFor(api.vol7Marks), marksFor(api.shapectxMarks), marksFor(api.pvMultiMarks),
      marksFor(api.volEchoMarks),
    ])
    const markMap = (res) => {
      const m = {}
      if (res.status !== 'fulfilled') return m
      for (const r of (res.value?.marks ?? [])) if (r?.date) m[String(r.date).slice(0, 10)] = r
      return m
    }
    const mLbal = markMap(lbalRes), mLvx = markMap(lvxRes), mOvd = markMap(ovdRes)
    const mVol7 = markMap(vol7Res), mShape = markMap(shapeRes), mPv = markMap(pvRes), mVe = markMap(veRes)
    const missing = [['L-BAL', lbalRes], ['L-VX', lvxRes], ['OVD', ovdRes], ['VOL7', vol7Res],
                     ['SHAPE', shapeRes], ['PV', pvRes], ['VOL_ECHO', veRes]].filter(([, r]) => r.status === 'rejected').map(([n]) => n)
    if (missing.length) setError(`CSV: ${missing.join(', ')} could not be fetched; those columns will be blank`)
    const full = sigRes.status === 'fulfilled' ? sigRes.value : null
    if (full?.length) {
      const ph = {}
      if (physRes.status === 'fulfilled') {
        for (const r of (physRes.value || [])) {
          if (r?.date == null) continue
          const ps = {}
          for (const key in r) if (key.startsWith('phys_')) ps[key] = r[key]
          ph[String(r.date).slice(0, 10)] = ps
        }
      } else {
        // said out loud rather than exported as empty columns, which would read as
        // "this was never measured"
        setError(`CSV: physics could not be fetched (${physRes.reason}); PHYS_ columns will be blank`)
      }
      src = full
      srcPhys = full.map(b => ({ ...b, ...(ph[String(b.date).slice(0, 10)] || {}) }))
    } else if (sigRes.status === 'rejected') {
      setError(`CSV: full history fetch failed (${sigRes.reason}) — exporting the ${bars.length} bars on screen`)
    }
    try {
    const join = (arr) => (arr ?? []).join(' ')
    const headers = [
      'date','open','high','low','close','vol_bucket','turbo_score',
      'rtb_phase','rtb_total','rtb_transition',
      'rtb_build','rtb_turn','rtb_ready','rtb_late','rtb_bonus3',
      'dbg_context_ready','dbg_t4_ctx','dbg_t6_ctx','dbg_t4t6_activation_plus',
      'dbg_launch_cluster_count','dbg_pending_phase','dbg_pending_phase_count',
      'Z','T','L','F','FLY','G','B','Combo','ULT','VOL','VABS','WICK',
      // ── Text Summary
      'SETUP','CONTEXT','GOG_TIER','ALL_SIGNALS',
      // ── Primary Scores
      'GOG_SCORE','SIGNAL_SCORE','SIGNAL_BUCKET','RESEARCH_SCORE','REGIME',
      // ── New score system
      'CLEAN_ENTRY_SCORE','SHAKEOUT_ABSORB_SCORE','ROCKET_SCORE',
      'EXTRA_BULL_SCORE','EXPERIMENTAL_SCORE',
      'HARD_BEAR_SCORE','VOLATILITY_RISK_SCORE',
      'FINAL_BULL_SCORE','FINAL_REGIME','FINAL_SCORE_BUCKET',
      // ── Model booleans
      'MDL_UM_GOG1','MDL_BH_GOG1','MDL_F8_GOG1','MDL_F8_BCT','MDL_F8_LRP',
      'MDL_L22_BCT','MDL_L22_LRP','MDL_BE_GOG1','MDL_BO_GOG1','MDL_Z10_GOG1',
      'MDL_LOAD_GOG1','MDL_260_GOG1','MDL_RKT_GOG1','MDL_F8_SVS','MDL_F8_CONS',
      'MDL_L22_SQB','MDL_3UP_GOG1','MDL_BLUE_GOG1','MDL_BX_GOG1','MDL_UM_LRP',
      'HAS_ELITE_MODEL','HAS_BEAR_MODEL',
      // ── Backward compat
      'BEARISH_RISK_SCORE',
      // ── Score Sub-Components
      'GOG_BASE_SCORE','PREMIUM_CONTEXT_SCORE','LOAD_CONTEXT_SCORE','L_RECLAIM_SCORE',
      'COMPRESSION_CONTEXT_SCORE','SQ_BCT_SCORE','BASE_SETUP_SCORE','RAW_SUPPORT_SCORE',
      'RISK_PENALTY','RESEARCH_FORWARD_SCORE',
      // ── Setup / GOG booleans
      'A','SM','N','MX',
      'GOG1','GOG2','GOG3','G1P','G2P','G3P','G1L','G2L','G3L','G1C','G2C','G3C',
      // ── Context signals
      'LD','LDS','LDC','LDP','LRC','LRP','WRC','F8C','SQB','BCT','SVS',
      // ── Raw signals
      'LOAD','SQ','W','F8',
      'L34','L43','L64','L22',
      'VBO_UP','BO_UP','BE_UP','BX_UP',
      'T10','T11','T12','Z10','Z11','Z12','Z4','Z6','Z9',
      'F3','F4','F6','F11','4BF','SIG_260308','L88','UM','SVS_RAW','CONS',
      'BUY_HERE','ATR_BREAKOUT','BOLL_BREAKOUT','HILO_BUY','RTV','THREE_G','ROCKET',
      // ── Diagnostics
      'ALREADY_EXTENDED_FLAG',
      'PCT_CHANGE_3D','PCT_CHANGE_5D','PCT_CHANGE_10D',
      'PCT_FROM_20D_HIGH','PCT_FROM_20D_LOW','DIST_20D_HIGH','VOL_RATIO_20D',
      'DOLLAR_VOLUME','GAP_PCT',
      // ── BETA Score
      'BETA_SCORE','BETA_RAW','BETA_SETUP','BETA_MOMENTUM','BETA_EXCESS','BETA_ZONE','BETA_AUTO_BUY',
      // ── Forward returns
      'FWD_1D','FWD_3D','FWD_5D','FWD_10D','MAX_HIGH_5D','MAX_HIGH_10D',
      'HIT_5PCT_5D','HIT_10PCT_5D','HIT_5PCT_10D','HIT_10PCT_10D',
      // ── Next event
      'BARS_TO_VBO','BARS_TO_GOG',
      'VBO_W5','VBO_W10','GOG_W5','GOG_W10',
      'RET_TO_NEXT_VBO_CLOSE','RET_TO_NEXT_VBO_HIGH',
      'RET_TO_NEXT_GOG_CLOSE','RET_TO_NEXT_GOG_HIGH',
      // ── All TurboScan signal booleans
      // VABS
      'SIG_BEST','SIG_STRONG','SIG_VBO_DN',
      'SIG_NS_VABS','SIG_ND_VABS','SIG_SC','SIG_BC','SIG_ABS','SIG_CLM',
      // UltraV2
      'SIG_BEST_UP','SIG_FBO_UP','SIG_EB_UP','SIG_3UP',
      'SIG_FBO_DN','SIG_EB_DN','SIG_4BF_DN',
      // L sub
      'SIG_FRI34','SIG_FRI43','SIG_FRI64',
      'SIG_L555','SIG_L2L4','SIG_BLUE',
      'SIG_CCI','SIG_CCI0R','SIG_CCIB',
      'SIG_BO_DN','SIG_BX_DN','SIG_BE_DN',
      'SIG_RL','SIG_RH','SIG_PP',
      // G individual
      'SIG_G1','SIG_G2','SIG_G4','SIG_G6','SIG_G11',
      // B individual
      'SIG_B1','SIG_B2','SIG_B3','SIG_B4','SIG_B5','SIG_B6',
      'SIG_B7','SIG_B8','SIG_B9','SIG_B10','SIG_B11',
      // F individual
      'SIG_F1','SIG_F2','SIG_F3','SIG_F4','SIG_F5','SIG_F6',
      'SIG_F7','SIG_F8','SIG_F9','SIG_F10','SIG_F11',
      // FLY sub
      'SIG_FLY_ABCD','SIG_FLY_CD','SIG_FLY_BD','SIG_FLY_AD',
      // Wick sub
      'SIG_WK_UP','SIG_WK_DN','SIG_X1','SIG_X2','SIG_X1G','SIG_X3',
      // Combo sub
      'SIG_BIAS_UP','SIG_BIAS_DN','SIG_SVS','SIG_CONSO',
      'SIG_P2','SIG_P3','SIG_P50','SIG_P89','SIG_BUY','SIG_3G',
      // VA + vol
      'SIG_VA','SIG_VOL_5X','SIG_VOL_10X','SIG_VOL_20X',
      // TZ / state
      'SIG_TZ','SIG_T','SIG_Z',
      'SIG_TZ3','SIG_TZ2','SIG_TZ_FLIP',
      'SIG_CD','SIG_CA','SIG_CW','SIG_SEQ_BCONT',
      // ── NS/ND Delta (disambiguated from VABS)
      'SIG_NS_DELTA','SIG_ND_DELTA',
      // ── Meta family any-flags
      'SIG_ANY_F','SIG_ANY_B','SIG_ANY_P','SIG_ANY_D',
      'SIG_L_ANY','SIG_BE_ANY','SIG_GOG_PLUS','SIG_NOT_EXT',
      // ── Price vs EMA
      'PRICE_GT_20','PRICE_GT_50','PRICE_GT_89','PRICE_GT_200',
      'PRICE_LT_20','PRICE_LT_50','PRICE_LT_89','PRICE_LT_200',
      // ── RSI filters
      'RSI_LE_35','RSI_GE_70',
      // ── Source / cross-engine
      'YF_SOURCE','CROSS_2PLUS','CROSS_3PLUS','CROSS_4PLUS','EARLY_E',
      // ── P66/P55
      'SIG_P66','SIG_P55',
      // ── D-family PREDN
      'SIG_D66','SIG_D55','SIG_D89','SIG_D50','SIG_D3','SIG_D2',
      // ── Delta extras
      'SIG_FLP_UP','SIG_ORG_UP','SIG_DD_UP_RED','SIG_D_UP_RED',
      'SIG_D_DN_GREEN','SIG_DD_DN_GREEN',
      // ── CISD
      'SIG_CISD_CPLUS','SIG_CISD_CPLUS_MINUS','SIG_CISD_CPLUS_MM',
      // ── PARA context
      'SIG_PARA_PREP','SIG_PARA_START','SIG_PARA_PLUS','SIG_PARA_RETEST',
      // ── 260523 / ULTRA signals (sync with ULTRA screener)
      'ad_fresh','ad_cluster',
      'wyc_phase','wyc_spring','wyc_sos','wyc_in_tr','wyc_sow',
      'prebreak_score','prebreak_prime','prebreak_ready','prebreak_watch',
      'pb_lvbo','pb_wvf_confirm','pb_stop_cause','pb_macro_penalty',
      'swing_type',
      // ── All scores (2026-07-18 — every score visible historically in the export)
      'ultra_score','ultra_score_band','ultra_score_v3','ultra_score_v3_band',
      'prebreak_v2','prebreak_v3','rev_buy','brk_buy','mtf_echo','mtf_score_conf','turn_echo_n','h4_rev_today','h1_rev_today','fly_fresh','EDGES', 'SEQ34', 'SEQ34_WIN', 'SEQ34_DSR', 'SEQ34_COARSE', 'SEQ34_PS_MED', 'SEQ_CTX', 'SEQ_CTX_UP', 'SEQ_ENS', 'SEQ_ENS_UP', 'CONF', 'CONF_TOP', 'CONF_EXT', 'CONF_EXT_TOP',
      // ── chart↔CSV parity (2026-07-21): everything the Superchart renders
      'L_SIG','L34_GRADE','BAR_BODY_WICK','BUY_SCORE','RSI','CCI','PROFILE_SCORE','PROFILE_CAT','EDGE_GOLD',
      'SEQ_CTX_LAYER','SEQ_CTX_KIND','SEQ_CTX_MEAN','SEQ_CTX_SIG','SEQ_ENS_DETAIL',
      // ── ATR time-to-target forecast (2026-07-26): per-bar historical forecast for review
      ...FORECAST_CSV_HEADERS,
      'NO_VOL_EVENT',
      // ── 📐 oscillator divergence × 🏆RS (2026-07-28) — stored per bar so the history
      // accumulates in these exports. DIV_BUY/DIV_DEEP = the validated long tiers
      // (+2.58 / +3.34, 5/5yr); DIV_TOP = the suppressor (−2.94/pf0.85, "do not open a
      // long / consider exiting"). Rare by construction: ~0.16 buy and ~0.32 top fires
      // per ticker per YEAR, so most rows are legitimately 0.
      'DIV_BUY','DIV_DEEP','DIV_TOP',
      // the graduated funnel: 1 raw · 2 in-zone-but-RS-blocked · 3 signal · 4 deep. Stage 2 is
      // the near-miss — far more common than a completion, and the reason the row exists.
      'DIV_R_BULL_STAGE','DIV_R_BEAR_STAGE','DIV_C_BULL_STAGE','DIV_C_BEAR_STAGE',
      'DIV_PIVOT_RSI','DIV_PIVOT_CCI','DIV_RS_INTACT',
      // ── ⚛ physics, straight from the DB columns.
      // Exported RAW — every field on every bar, never the ⚛ row's display filter. The
      // filter is a reading aid for a screen; a CSV is for work done elsewhere, and a
      // export that silently dropped three quarters of the rows to match what was on
      // screen would be discovered as missing data long after the analysis was written.
      'PHYS_R','PHYS_REGIME','PHYS_M','PHYS_E','PHYS_K','PHYS_C','PHYS_H','PHYS_S',
      'PHYS_AD','PHYS_GAP_TRUE','PHYS_WYC','PHYS_LINE6',
      'PHYS_R_RAW','PHYS_M_RAW','PHYS_E_RAW','PHYS_K_X','PHYS_C_RAW','PHYS_H_RAW',
      'PHYS_E_RELEASE','PHYS_S_NET',
      // ── The five display layers shipped 2026-09-06..10. Every one of them is DESCRIPTIVE:
      // its research family closed NULL / 0 BUILD, and none feeds a score. They are exported so
      // the co-occurrence work can be done on them OUTSIDE the app — which is the whole reason
      // the gap was noticed.
      // L-BAL — 15m L-label balance of the session (INTRADAY_EFFORT_BALANCE_V1: 16/16 NULL)
      'LBAL_UDN','LBAL_UDN_C','LBAL_MARKS','LBAL_TEXT','LBAL_COLOUR','LBAL_NPOS','LBAL_NNEG',
      // L-VX — daily L34/L46 graded V · VL · VH · VX
      'LVX_LABEL','LVX_FAM','LVX_TIER','LVX_V','LVX_LV15','LVX_LV1H',
      // OVD daily map (sealed OVD family: 0 BUILD / 0 VETO)
      'OVD_TOKENS','OVD_RV_O60','OVD_RV_C60','OVD_RECLAIM60','OVD_HANDOFF60',
      // VOL7 — 7-level volume regime. M5/M6 are NOT the app's WLNBB B/VB bucket.
      'VOL7_LABEL','VOL7_MR','VOL7_SG','VOL7_RATIO','VOL7_CONS','VOL7_TRANS','VOL7_MARKS',
      // 🏅 RANK — per-bar on the signal rows already, no extra fetch
      'RANK_PCT','RANK_EDGE','RANK_FAM','RANK_N',
      // SHAPE × CONTEXT — 4 sealed families, k = 21, 0 BUILD. SHAPE_LSTUP_VETO is the ONE cell
      // with two-window evidence (LST↑ −0.63 MINE / −0.47 VERIFY, DSR_neg 0.998) and it is a
      // VETO. SHAPE_BY_FAM / SHAPE_BY_DEN are the 🎯 / 🔁 marks — measured MONOTONICALLY WORSE
      // with more clustering, so they are context, not strength.
      'SHAPE_CODE','SHAPE_ARROW','SHAPE_LABEL','SHAPE_GRADE','SHAPE_VR','SHAPE_ABSORB','SHAPE_DRY',
      'SHAPE_SWEET','SHAPE_POS20','SHAPE_FLOOR','SHAPE_TOUCHES','SHAPE_KEY','SHAPE_RS','SHAPE_RSI',
      'SHAPE_BAND','SHAPE_KNIFE_VETO','SHAPE_LSTUP_VETO','SHAPE_DIR_UP','SHAPE_DIR_DN',
      'SHAPE_CL_FAM','SHAPE_CL_BARS','SHAPE_BY_FAM','SHAPE_BY_DEN','SHAPE_MARK','SHAPE_LEGS',
      'SHAPE_MID','SHAPE_EXP','SHAPE_CON','SHAPE_LAST','SHAPE_WRAP','SHAPE_COIL','SHAPE_MOTH',
      // PRICE × VOLUME — the nine ordinal shapes, on BOTH price sources, because they agree on only
      // 52.7% of firing sessions. PV_MULTI_V1 sealed k = 9 → 0 BUILD / 4 VETO_CANDIDATE / 5 NULL,
      // every cell negative in MINE. PV_VETO_* marks the four (REV RE2 UPP RUP): recorded, NOT
      // applied. Never a ranking input.
      'PV_CLOSE','PV_OHLC4','PV_AGREE','PV_VETO_CLOSE','PV_VETO_OHLC4',
      // VOL ECHO — "260925_VOL_ECHO" at its defaults. Descriptive: every long study NULL. VE_SPK is
      // HINDSIGHT (an origin is only 'echoed' once the echo arrives). VE_QR_REL_VETO = the QR_REL_V1
      // veto shape (yesterday Q∧R, today the first ▲): confirmed, recorded, NOT applied.
      'VE_SPIKE','VE_SPK','VE_ECHO','VE_ECHO_GAP','VE_ECHO_NMATCH','VE_ECHO_COL','VE_ECHO_VR','VE_ECHO_HITS',
      'VE_Q','VE_QN','VE_R','VE_RN','VE_REL_UP','VE_REL_DN','VE_REL_N','VE_REL_Q',
      'VE_BO','VE_BD','VE_BOV','VE_BDV','VE_BO_AGE','VE_BOV_X',
      'VE_ZONE_ARMED','VE_ZONE_TOP','VE_ZONE_BOT','VE_ZONE_POS','VE_QR_REL_VETO',
    ]
    const ctx = (b, tok) => (b.context ?? []).includes(tok) ? 1 : 0
    const s = (b, k) => b[k] ?? 0
    // display-layer cells: a miss must export as empty / 0, never as a stale neighbour's value
    const B01 = (v) => (v ? 1 : 0)
    const NUM = (v, d = 2) => (v == null || Number.isNaN(Number(v)) ? '' : Number(v).toFixed(d))
    const _atrArr = computeAtr14(src)
    // physics lives in a separate fetch keyed by date; srcPhys is src already merged
    const _phys = Object.fromEntries(srcPhys.map(b => [b.date, b]))
    const rows = src.map((b, _bi) => [
      b.date,
      b.open?.toFixed(2), b.high?.toFixed(2), b.low?.toFixed(2), b.close?.toFixed(2),
      b.vol_bucket ?? '',
      b.turbo_score ?? 0,
      b.rtb_phase ?? '',
      b.rtb_total ?? 0,
      b.rtb_transition ?? '',
      b.rtb_build ?? 0,
      b.rtb_turn ?? 0,
      b.rtb_ready ?? 0,
      b.rtb_late ?? 0,
      b.rtb_bonus3 ?? 0,
      b.dbg_context_ready ? 1 : 0,
      b.dbg_t4_ctx ? 1 : 0,
      b.dbg_t6_ctx ? 1 : 0,
      b.dbg_t4t6_activation_plus ? 1 : 0,
      b.dbg_launch_cluster_count ?? 0,
      b.dbg_pending_phase ?? '',
      b.dbg_pending_phase_count ?? 0,
      b.tz?.startsWith('Z') ? b.tz : '',
      b.tz?.startsWith('T') ? b.tz : '',
      join(b.l),
      join(b.f),
      join(b.fly),
      join(b.g),
      join(b.b),
      join((b.combo ?? []).filter(s => !PREUP_SET.has(s))),
      join(b.ultra),
      join(b.vol),
      join(b.vabs),
      join(b.wick),
      // ── Text Summary
      join(b.setup), join(b.context), b.gog_tier ?? '', b.all_signals ?? '',
      // ── Primary Scores
      b.gog_score ?? 0,
      b.signal_score ?? 0, b.signal_bucket ?? '', b.research_score ?? 0, b.regime ?? '',
      // ── New score system
      b.clean_entry_score ?? 0, b.shakeout_absorb_score ?? 0, b.rocket_score ?? 0,
      b.extra_bull_score ?? 0, b.experimental_score ?? 0,
      b.hard_bear_score ?? 0, b.volatility_risk_score ?? 0,
      b.final_bull_score ?? 0, b.final_regime ?? '', b.final_score_bucket ?? '',
      // ── Model booleans
      b.mdl_um_gog1 ?? 0, b.mdl_bh_gog1 ?? 0, b.mdl_f8_gog1 ?? 0,
      b.mdl_f8_bct ?? 0,  b.mdl_f8_lrp ?? 0,
      b.mdl_l22_bct ?? 0, b.mdl_l22_lrp ?? 0, b.mdl_be_gog1 ?? 0,
      b.mdl_bo_gog1 ?? 0, b.mdl_z10_gog1 ?? 0,
      b.mdl_load_gog1 ?? 0, b.mdl_260_gog1 ?? 0, b.mdl_rkt_gog1 ?? 0,
      b.mdl_f8_svs ?? 0, b.mdl_f8_cons ?? 0,
      b.mdl_l22_sqb ?? 0, b.mdl_3up_gog1 ?? 0, b.mdl_blue_gog1 ?? 0,
      b.mdl_bx_gog1 ?? 0, b.mdl_um_lrp ?? 0,
      b.has_elite_model ?? 0, b.has_bear_model ?? 0,
      // ── Backward compat
      b.bearish_risk_score ?? 0,
      // ── Score Sub-Components
      b.gog_base_score ?? 0, b.premium_context_score ?? 0, b.load_context_score ?? 0,
      b.l_reclaim_score ?? 0, b.compression_context_score ?? 0, b.sq_bct_score ?? 0,
      b.base_setup_score ?? 0, b.raw_support_score ?? 0,
      b.risk_penalty ?? 0, b.research_forward_score ?? 0,
      // ── Setup / GOG booleans
      (b.setup ?? []).includes('A')  ? 1 : 0,
      (b.setup ?? []).includes('SM') ? 1 : 0,
      (b.setup ?? []).includes('N')  ? 1 : 0,
      (b.setup ?? []).includes('MX') ? 1 : 0,
      b.gog1 ?? 0, b.gog2 ?? 0, b.gog3 ?? 0,
      b.g1p ?? 0, b.g2p ?? 0, b.g3p ?? 0,
      b.g1l ?? 0, b.g2l ?? 0, b.g3l ?? 0,
      b.g1c ?? 0, b.g2c ?? 0, b.g3c ?? 0,
      // ── Context signals
      ctx(b,'LD'), ctx(b,'LDS'), ctx(b,'LDC'), ctx(b,'LDP'),
      ctx(b,'LRC'), ctx(b,'LRP'), ctx(b,'WRC'), ctx(b,'F8C'),
      ctx(b,'SQB'), ctx(b,'BCT'), ctx(b,'SVS'),
      // ── Raw signals
      b.raw_load ?? 0, b.raw_sq ?? 0, b.raw_w ?? 0, b.raw_f8 ?? 0,
      b.raw_l34 ?? 0, b.raw_l43 ?? 0, b.raw_l64 ?? 0, b.raw_l22 ?? 0,
      b.raw_vbo_up ?? 0, b.raw_bo_up ?? 0, b.raw_be_up ?? 0, b.raw_bx_up ?? 0,
      b.raw_t10 ?? 0, b.raw_t11 ?? 0, b.raw_t12 ?? 0,
      b.raw_z10 ?? 0, b.raw_z11 ?? 0, b.raw_z12 ?? 0,
      b.raw_z4 ?? 0, b.raw_z6 ?? 0, b.raw_z9 ?? 0,
      b.raw_f3 ?? 0, b.raw_f4 ?? 0, b.raw_f6 ?? 0, b.raw_f11 ?? 0,
      b.raw_bf4 ?? 0, b.raw_sig260308 ?? 0, b.raw_l88 ?? 0, b.raw_um ?? 0,
      b.raw_svs_raw ?? 0, b.raw_cons ?? 0,
      b.raw_buy_here ?? 0, b.raw_atr_brk ?? 0, b.raw_bb_brk ?? 0,
      b.raw_hilo_buy ?? 0, b.raw_rtv ?? 0, b.raw_three_g ?? 0, b.raw_rocket ?? 0,
      // ── Diagnostics
      b.already_extended ?? 0,
      b.pct_change_3d ?? '', b.pct_change_5d ?? '', b.pct_change_10d ?? '',
      b.pct_from_20d_high ?? '', b.pct_from_20d_low ?? '',
      b.distance_to_20d_high_pct ?? '', b.volume_ratio_20d ?? '',
      b.dollar_volume ?? '', b.gap_pct ?? '',
      // ── BETA Score
      b.beta_score ?? '', b.beta_raw ?? '', b.beta_setup ?? '', b.beta_momentum ?? '',
      b.beta_excess ?? '', b.beta_zone ?? '', b.beta_auto_buy ? 1 : 0,
      // ── Forward returns
      b.fwd_close_1d ?? '', b.fwd_close_3d ?? '', b.fwd_close_5d ?? '', b.fwd_close_10d ?? '',
      b.max_high_5d_pct ?? '', b.max_high_10d_pct ?? '',
      b.hit_5pct_5d ?? 0, b.hit_10pct_5d ?? 0, b.hit_5pct_10d ?? 0, b.hit_10pct_10d ?? 0,
      // ── Next event
      b.bars_to_next_vbo ?? '', b.bars_to_next_gog ?? '',
      b.vbo_within_5 ?? 0, b.vbo_within_10 ?? 0, b.gog_within_5 ?? 0, b.gog_within_10 ?? 0,
      b.ret_to_next_vbo_close ?? '', b.ret_to_next_vbo_high ?? '',
      b.ret_to_next_gog_close ?? '', b.ret_to_next_gog_high ?? '',
      // ── All TurboScan signal booleans
      s(b,'sig_best'), s(b,'sig_strong'), s(b,'sig_vbo_dn'),
      s(b,'sig_ns_vabs'), s(b,'sig_nd_vabs'), s(b,'sig_sc'), s(b,'sig_bc'), s(b,'sig_abs'), s(b,'sig_clm'),
      s(b,'sig_best_up'), s(b,'sig_fbo_up'), s(b,'sig_eb_up'), s(b,'sig_3up'),
      s(b,'sig_fbo_dn'), s(b,'sig_eb_dn'), s(b,'sig_4bf_dn'),
      s(b,'sig_fri34'), s(b,'sig_fri43'), s(b,'sig_fri64'),
      s(b,'sig_l555'), s(b,'sig_l2l4'), s(b,'sig_blue'),
      s(b,'sig_cci'), s(b,'sig_cci0r'), s(b,'sig_ccib'),
      s(b,'sig_bo_dn'), s(b,'sig_bx_dn'), s(b,'sig_be_dn'),
      s(b,'sig_rl'), s(b,'sig_rh'), s(b,'sig_pp'),
      s(b,'sig_g1'), s(b,'sig_g2'), s(b,'sig_g4'), s(b,'sig_g6'), s(b,'sig_g11'),
      s(b,'sig_b1'), s(b,'sig_b2'), s(b,'sig_b3'), s(b,'sig_b4'), s(b,'sig_b5'), s(b,'sig_b6'),
      s(b,'sig_b7'), s(b,'sig_b8'), s(b,'sig_b9'), s(b,'sig_b10'), s(b,'sig_b11'),
      s(b,'sig_f1'), s(b,'sig_f2'), s(b,'sig_f3'), s(b,'sig_f4'), s(b,'sig_f5'), s(b,'sig_f6'),
      s(b,'sig_f7'), s(b,'sig_f8'), s(b,'sig_f9'), s(b,'sig_f10'), s(b,'sig_f11'),
      s(b,'sig_fly_abcd'), s(b,'sig_fly_cd'), s(b,'sig_fly_bd'), s(b,'sig_fly_ad'),
      s(b,'sig_wk_up'), s(b,'sig_wk_dn'), s(b,'sig_x1'), s(b,'sig_x2'), s(b,'sig_x1g'), s(b,'sig_x3'),
      s(b,'sig_bias_up'), s(b,'sig_bias_dn'), s(b,'sig_svs'), s(b,'sig_conso'),
      s(b,'sig_p2'), s(b,'sig_p3'), s(b,'sig_p50'), s(b,'sig_p89'), s(b,'sig_buy'), s(b,'sig_3g'),
      s(b,'sig_va'), s(b,'sig_vol_5x'), s(b,'sig_vol_10x'), s(b,'sig_vol_20x'),
      s(b,'sig_tz'), s(b,'sig_t'), s(b,'sig_z'),
      s(b,'sig_tz3'), s(b,'sig_tz2'), s(b,'sig_tz_flip'),
      s(b,'sig_cd'), s(b,'sig_ca'), s(b,'sig_cw'), s(b,'sig_seq_bcont'),
      // ── NS/ND Delta
      s(b,'sig_ns_delta'), s(b,'sig_nd_delta'),
      // ── Meta flags
      s(b,'sig_any_f'), s(b,'sig_any_b'), s(b,'sig_any_p'), s(b,'sig_any_d'),
      s(b,'sig_l_any'), s(b,'sig_be_any'), s(b,'sig_gog_plus'), s(b,'sig_not_ext'),
      // ── Price vs EMA
      s(b,'sig_price_gt_20'), s(b,'sig_price_gt_50'),
      s(b,'sig_price_gt_89'), s(b,'sig_price_gt_200'),
      s(b,'sig_price_lt_20'), s(b,'sig_price_lt_50'),
      s(b,'sig_price_lt_89'), s(b,'sig_price_lt_200'),
      // ── RSI
      s(b,'sig_rsi_le_35'), s(b,'sig_rsi_ge_70'),
      // ── Source / cross
      s(b,'sig_yf_source'),
      s(b,'sig_cross_2plus'), s(b,'sig_cross_3plus'),
      s(b,'sig_cross_4plus'), s(b,'sig_early_e'),
      // ── P66/P55
      s(b,'sig_p66'), s(b,'sig_p55'),
      // ── D-family
      s(b,'sig_d66'), s(b,'sig_d55'), s(b,'sig_d89'),
      s(b,'sig_d50'), s(b,'sig_d3'),  s(b,'sig_d2'),
      // ── Delta extras
      s(b,'sig_flp_up'),    s(b,'sig_org_up'),
      s(b,'sig_dd_up_red'), s(b,'sig_d_up_red'),
      s(b,'sig_d_dn_green'), s(b,'sig_dd_dn_green'),
      // ── CISD
      s(b,'sig_cisd_cplus'), s(b,'sig_cisd_cplus_minus'), s(b,'sig_cisd_cplus_mm'),
      // ── PARA context
      s(b,'sig_para_prep'), s(b,'sig_para_start'),
      s(b,'sig_para_plus'), s(b,'sig_para_retest'),
      // ── 260523 / ULTRA signals
      b.ad_fresh ? 1 : 0,
      b.ad_cluster ? 1 : 0,
      b.wyc_phase ?? '',
      b.wyc_spring ? 1 : 0,
      b.wyc_sos ? 1 : 0,
      b.wyc_in_tr ? 1 : 0,
      b.wyc_sow ? 1 : 0,
      b.prebreak_score ?? 0,
      b.prebreak_prime ? 1 : 0,
      b.prebreak_ready ? 1 : 0,
      b.prebreak_watch ? 1 : 0,
      b.pb_lvbo ? 1 : 0,
      b.pb_wvf_confirm ? 1 : 0,
      b.pb_stop_cause ? 1 : 0,
      b.pb_macro_penalty ? 1 : 0,
      b.swing_type ?? '',
      // ── All scores (must stay in sync with the score headers above)
      b.ultra_score ?? '',
      b.ultra_score_band ?? '',
      b.ultra_score_v3 ?? '',
      b.ultra_score_v3_band ?? '',
      v2Map[String(b.date).slice(0, 10)]?.v2 ?? '',
      b.prebreak_v3 ?? '',
      b.rev_buy ? 1 : 0,
      b.brk_buy ? 1 : 0,
      b.mtf_echo === true ? 1 : (b.mtf_echo === false ? 0 : ''),
      b.mtf_score_conf ?? '',
      b.turn_echo_n ?? '',
      b.h4_rev_today ? 1 : 0,
      b.h1_rev_today ? 1 : 0,
      b.fly_fresh ? 1 : 0,
      join(b.edges),
      b.seq34 ? b.seq34.seq : '',
      b.seq34?.win ?? '',
      b.seq34?.dsr ?? '',
      b.seq34?.coarse ? 1 : 0,
      b.seq34?.ps_med ?? '',
      b.seq_ctx ? `${b.seq_ctx.dir}:${b.seq_ctx.seq}` : '',
      b.seq_ctx?.up ?? '',
      b.seq_ens ? `${b.seq_ens.n_up}up/${b.seq_ens.n_dn}dn` : '',
      b.seq_ens?.up_avg ?? '',
      b.conf ?? '',
      b.conf_top ?? '',
      b.conf_ext ?? '',
      b.conf_ext_top ?? '',
      b.l_sig ?? '',
      b.l34_grade ?? '',
      b.bar_body_wick ?? '',
      b.buy_score ?? '',
      b.rsi ?? b.RSI ?? '',
      b.cci ?? b.CCI ?? '',
      b.profile_score ?? '',
      b.profile_category ?? '',
      (b.rev_buy && b.mtf_echo !== false && (b.edges ?? []).some(e => ['QZC', 'D+L1', 'RTB', 'P55'].includes(e))) ? 1 : 0,
      b.seq_ctx?.layer ?? '',
      b.seq_ctx?.kind ?? '',
      b.seq_ctx?.mean ?? '',
      b.seq_ctx?.sig ?? '',
      b.seq_ens ? b.seq_ens.detail.map(v => `${v.layer}:${v.kind === 'tail' ? '~' : v.dir === 'up' ? '+' : '-'}${Math.round(v.up)}`).join('|') : '',
      ...forecastCsvCells(b.close > 0 ? _atrArr[_bi] / b.close : 0),
      b.no_vol_event ? 1 : 0,
      b.div_buy ? 1 : 0,
      b.div_deep ? 1 : 0,
      b.div_top ? 1 : 0,
      b.dvr_b ?? 0, b.dvr_t ?? 0, b.dvc_b ?? 0, b.dvc_t ?? 0,
      b.dv_rlo ?? b.dv_rhi ?? '', b.dv_clo ?? b.dv_chi ?? '',
      b.dv_rs == null ? '' : (b.dv_rs ? 1 : 0),
      ...(() => {
        const p = _phys[b.date] || {}
        return ['phys_r', 'phys_regime', 'phys_m', 'phys_e', 'phys_k', 'phys_c', 'phys_h',
                'phys_s', 'phys_ad', 'phys_gap_true', 'phys_wyc', 'phys_line6',
                'phys_r_raw', 'phys_m_raw', 'phys_e_raw', 'phys_k_x', 'phys_c_raw',
                'phys_h_raw', 'phys_e_release', 'phys_s_net'].map(k => p[k] ?? '')
      })(),
      // ── the five display layers, keyed by date from their own full-history fetches ──
      ...(() => {
        const d = String(b.date).slice(0, 10)
        const L = mLbal[d] || {}, X = mLvx[d] || {}, O = mOvd[d] || {}
        const V = mVol7[d] || {}, S = mShape[d] || {}
        return [
          L.lbal_udn ?? '', L.lbal_udn_c ?? '', L.lbal_marks ?? '', L.lbal_text ?? '',
          L.lbal_colour ?? '', L.lbal_n_pos ?? '', L.lbal_n_neg ?? '',
          X.lvx_label ?? '', X.lvx_fam ?? '', X.lvx_tier ?? '', B01(X.lvx_v),
          X.lvx_lv15 ?? '', X.lvx_lv1h ?? '',
          O.ovdmap_tokens ?? '', NUM(O.ovdmap_rv_o60), NUM(O.ovdmap_rv_c60),
          NUM(O.ovdmap_reclaim60), NUM(O.ovdmap_handoff60),
          V.vol7_label ?? '', V.vol7_mr ?? '', V.vol7_sg ?? '', NUM(V.vol7_ratio),
          V.vol7_cons ?? '', V.vol7_trans ?? '', V.vol7_marks ?? '',
          // 🏅 RANK rides on the signal row itself
          b.rank_pct ?? '', NUM(b.rank_edge), b.rank_fam ?? '', b.rank_n ?? '',
          S.shape_code ?? '', S.shape_arrow ?? '', S.shape_label ?? '',
          S.shape_grade ?? '', NUM(S.shape_vr), B01(S.shape_absorb), B01(S.shape_dry),
          B01(S.shape_sweet), NUM(S.shape_pos20, 3), B01(S.shape_floor),
          S.shape_touches ?? '', B01(S.shape_key), B01(S.shape_rs), NUM(S.shape_rsi, 1),
          S.shape_band ?? '', B01(S.shape_veto), B01(S.shape_lstup_veto),
          B01(S.shape_dir_up), B01(S.shape_dir_dn),
          S.shape_cl_fam ?? '', S.shape_cl_bars ?? '', B01(S.shape_by_fam), B01(S.shape_by_den),
          S.shape_mark ?? '', S.shape_legs ?? '',
          B01(S.shape_s_mid), B01(S.shape_s_exp), B01(S.shape_s_con), B01(S.shape_s_last),
          B01(S.shape_s_wrap), B01(S.shape_s_coil), B01(S.shape_s_moth),
          ...(() => { const P = mPv[d] || {}
            return [P.pv_c ?? '', P.pv_o ?? '', B01(P.pv_agree), B01(P.pv_veto_c), B01(P.pv_veto_o)] })(),
          ...(() => { const E = mVe[d] || {}
            return [B01(E.ve_spike), B01(E.ve_spk), B01(E.ve_echo), E.ve_echo_gap ?? '', E.ve_echo_nmatch ?? '',
                    E.ve_echo_col ?? '', NUM(E.ve_echo_vr), E.ve_echo_hits ?? '',
                    B01(E.ve_q), E.ve_qn ?? '', B01(E.ve_r), E.ve_rn ?? '',
                    B01(E.ve_rel_up), B01(E.ve_rel_dn), E.ve_rel_n ?? '', E.ve_rel_q ?? '',
                    B01(E.ve_bo), B01(E.ve_bd), B01(E.ve_bov), B01(E.ve_bdv), E.ve_bo_age ?? '', NUM(E.ve_bov_x),
                    B01(E.ve_zone_armed), NUM(E.ve_zone_top, 4), NUM(E.ve_zone_bot, 4), E.ve_zone_pos ?? '',
                    B01(E.ve_qr_rel_veto)] })(),
        ]
      })(),
    ])
    // A header/cell count drift silently shifts every column after the break, and the CSV still
    // opens fine — it is discovered weeks later as "the data is wrong". Fail loudly instead.
    if (rows.length && rows[0].length !== headers.length) {
      const msg = `CSV column mismatch: ${headers.length} headers vs ${rows[0].length} cells — export aborted`
      console.error(msg, { headers: headers.length, cells: rows[0].length })
      setError(msg)
      setCsvBusy(false)
      return
    }
    const csv = [headers, ...rows]
      .map(r => r.map(v => `"${String(v ?? '').replace(/"/g, '""')}"`).join(','))
      .join('\n')
    const blob = new Blob([csv], { type: 'text/csv' })
    const a = document.createElement('a')
    a.href = URL.createObjectURL(blob)
    a.download = `${ticker}_${tf}_signals.csv`
    a.click()
    URL.revokeObjectURL(a.href)
    } finally { setCsvBusy(false) }
  }, [bars, barsPhys, ticker, tf, v2Map, csvBusy])

  useEffect(() => { load(ticker, tf) }, [ticker, tf, load])

  useEffect(() => {
    if (showStats) { setStatsData(null); loadStats(ticker, tf) }
  }, [ticker, tf])

  const handleSubmit = (e) => {
    e.preventDefault()
    const t = inputVal.trim().toUpperCase()
    if (t && t !== ticker) setTicker(t)
  }

  return (
    <div className="p-2 flex flex-col gap-2">
      {/* Controls */}
      <div className="flex items-center gap-2 flex-wrap">
        <form onSubmit={handleSubmit} className="flex gap-1">
          <input
            className="bg-md-surface-high text-md-on-surface text-sm px-2 py-1 rounded border border-white/[0.12] w-24 uppercase placeholder:text-md-on-surface-var/40 focus:outline-none focus:border-white/30 transition-colors"
            value={inputVal}
            onChange={e => setInputVal(e.target.value.toUpperCase())}
            placeholder="TICKER"
          />
          <button type="submit" className="text-xs px-3 py-1 bg-md-primary text-md-on-primary rounded font-medium hover:opacity-90 transition-opacity">
            Go
          </button>
        </form>
        <div className="flex gap-0.5 border border-white/[0.10] rounded p-0.5 bg-md-surface-con">
          {TF_OPTIONS.map(t => (
            <button key={t} onClick={() => setTf(t)}
              className={`text-xs px-2 py-1 rounded transition-colors
                ${tf === t ? 'bg-md-surface-high text-md-on-surface font-semibold' : 'text-md-on-surface-var hover:text-md-on-surface'}`}>
              {t}
            </button>
          ))}
        </div>
        <button
          onClick={() => {
            const next = !showStats
            setShowStats(next)
            if (next && !statsData) loadStats(ticker, tf)
          }}
          className={`text-xs px-2 py-1 rounded transition-colors border
            ${showStats
              ? 'bg-violet-900/60 border-violet-600/50 text-violet-200'
              : 'bg-md-surface-high border-white/[0.10] text-md-on-surface-var hover:text-md-on-surface'}`}>
          📊 Stats
        </button>
        {tf === '1d' && (
          <label className="flex items-center gap-1 text-xs text-md-on-surface-var"
                 title="How much of the ⚛ physics row to print. The database holds every physics field on EVERY bar — this only decides how much of it is shown. 'rare' marks the 6 uncommon events; 'all' prints the full per-bar state, the same thing the Pine pane draws.">
            <span>⚛</span>
            <select value={physRowMode} onChange={e => setPhysRowMode(e.target.value)}
              className="bg-md-surface-high border border-white/[0.10] rounded px-1 py-0.5
                         text-xs text-md-on-surface-var">
              <option value="rare">rare · 17%</option>
              <option value="ra">+RA · 38%</option>
              <option value="all">all bars</option>
            </select>
          </label>
        )}
        {bars.length > 0 && (
          <button
            onClick={exportCsv}
            disabled={csvBusy}
            title={`Download the FULL ${ticker} ${tf.toUpperCase()} history as CSV — not the ${bars.length} bars shown in the matrix, which is a display limit`}
            className="text-xs px-2 py-1 rounded border border-white/[0.10] bg-md-surface-high text-md-on-surface-var hover:text-md-on-surface transition-colors">
            {csvBusy ? '⬇ preparing…' : '⬇ CSV'}
          </button>
        )}
        {loading && <span className="text-xs text-md-on-surface-var/60 animate-pulse">loading…</span>}
        {error   && <span className="text-xs text-red-400">{error}</span>}
        {/* ⏱ ATR time-to-target forecast (2026-07-26): volatility-driven timing, OOS-calibrated.
            NOT a buy signal — expectation/stop-timing context. */}
        {bars.length > 14 && (() => {
          const _a = computeAtr14(bars); const _last = bars[bars.length - 1]
          const _ap = _last?.close > 0 ? _a[bars.length - 1] / _last.close : 0
          const _lo20 = Math.min(...bars.slice(-20).map(b => +b.low || Infinity))
          const _off = _ap > 0 && isFinite(_lo20) ? (_last.close - _lo20) / (_ap * _last.close) : null
          const _txt = fmtForecast(_ap, _off)
          return _txt ? (
            <span className="text-xs text-sky-300/80 ml-2"
              title="⏱ ATR time-to-target forecast — OOS-calibrated (TRAIN 2021-23 ≈ TEST 2024-26): typical days & hit-rate to reach ±X% given this stock's current volatility (ATR%). days ≈ (X/ATR)^0.67. σ off low = distance above the 20d low in ATR units. TIMING/expectation & stop-calibration context — NOT a directional buy signal (targets hit at ~base-rate; the edge lives in downside/path).">
              ⏱ {_txt}
            </span>
          ) : null
        })()}
        <VrpLine ticker={ticker} />
      </div>

      {/* Candlestick chart — DB codes on 1d, live feed on intraday.
          Journal trade overlay (signal/buy/sell) shows only while the chart is on the
          trade's own ticker — changing ticker clears it. */}
      {/* 2000 bars by default (2026-07-29). Scoped to the Superchart on purpose — raising
          CodeCandleChart's own default would also hit every scanner thumbnail and the
          MiniChartPopup, which only need a few hundred bars. */}
      <CodeCandleChart ticker={ticker} tf={tf} height={420} showSector initialLimit={2000}
        tradeMarkers={initialTrade && initialTrade.ticker === ticker ? initialTrade : null} />

      {/* Matrix */}
      {bars.length > 0 && (
        <div className="bg-md-surface-con rounded-md-md border border-white/[0.08] overflow-hidden">
          <div
            ref={matrixRef}
            className="overflow-x-auto overflow-y-hidden"
          >
            <table className="text-xs border-collapse" style={{ tableLayout: 'fixed' }}>
              <thead>
                {/* Mini-candle row removed 2026-07-29 (user). It scaled every bar to the
                    price range of the WHOLE visible window, so at the new 2000-bar default
                    the range is so wide that every candle collapses to a 1px dash — the
                    real chart above already shows this, properly zoomed. The MiniCandle
                    component and its globalMin/globalRange memo are kept: one line restores
                    it if the window ever goes back to a few hundred bars. */}
                {/* Date + vol bucket row */}
                <tr className="bg-md-surface">
                  <th style={{ width: HDR_W, minWidth: HDR_W }}
                      className="sticky left-0 z-10 bg-md-surface border-r border-white/[0.08]" />
                  {bars.map((b, i) => (
                    <th key={i} style={{ width: CELL_W, minWidth: CELL_W }}
                        className="font-normal px-0 py-0 text-center border-r border-white/[0.05]">
                      <div className="flex flex-col items-center gap-px pb-0.5">
                        <span className="text-md-on-surface-var/70 font-mono" style={{ fontSize: 13 }}>
                          {/* ⛔ no intraday volume EVENT that day (biggest 15m bar < 4× the session
                              average). Validated across all 29 TZ/L codes: median drops 4-8pts. */}
                          {b.no_vol_event ? (
                            <span className="text-red-400/90" style={{ marginRight: 2 }}
                              title="⛔ NO intraday volume event — the biggest 15m bar never reached 2.5× that session's own average. Validated across ALL 29 TZ/L signal codes ($21-377): on such a day EVERY signal's median falls 4-8 points (Z9 −8.0, T1 −7.1, L5 −6.7; only Z11 resists at −0.8). Rare (~3% of days) but severe → treat any signal on this bar as untrustworthy.">⛔</span>
                          ) : null}
                          {fmtDate(b.date, isIntraday)}
                        </span>
                        <div className="rounded-sm"
                          style={{ width: 28, height: 2, backgroundColor: BUCKET_HEX[b.vol_bucket] ?? '#374151' }} />
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>

              <tbody>
                {/* z / T-D / L — then ALL score rows (screener column order), then the rest */}
                {/* ⚛ density. The default marks only rare events, which is why a bar can look
                    empty: the DB holds every physics field on EVERY bar, and "all" prints the
                    full per-bar state the Pine pane shows. Nothing is missing — it is filtered. */}
                {ROWS.filter(r => ['z', 'td', 'l'].includes(r.key)).map(row => <ChipRow key={row.key} row={row} bars={bars} />)}
                {/* ▭ sits directly under L, where it was asked for. Reads bars, not
                    barsPhys — bar_body_wick comes from /api/bar_signals already. */}
                {ROWS.filter(r => r.key === 'bodywick').map(row => <ChipRow key={row.key} row={row} bars={bars} />)}
                {ROWS.filter(r => r.key === 'cisd').map(row => <ChipRow key={row.key} row={row} bars={bars} />)}
                {tf === '1d' && ROWS.filter(r => r.key === 'phys').map(row => (
                  <ChipRow key={row.key} bars={barsPhys}
                           row={{ ...row,
                                  // The control sits ON the row it governs. It was only in the
                                  // toolbar at the top of the page, while this row is below the
                                  // chart — so at the moment you are looking at an empty row and
                                  // wondering why, the thing that explains it is off screen. An
                                  // invisible filter reads as missing data, and did.
                                  label: (
                                    <span onClick={() => setPhysRowMode(
                                            m => m === 'rare' ? 'ra' : m === 'ra' ? 'all' : 'rare')}
                                          className="cursor-pointer select-none hover:text-md-on-surface"
                                          title={`⚛ physics — showing ${
                                            physRowMode === 'rare' ? 'rare events only (17% of bars)'
                                            : physRowMode === 'ra' ? 'rare events + absorbed effort (38%)'
                                            : 'the FULL per-bar state, as the Pine pane draws it'
                                          }. Click to cycle: rare → +RA → all. The database holds every physics field on EVERY bar; this only decides how much is printed.`}>
                                      {/* The mode is spelled out, not encoded. '· : ⋯' told
                                          you a setting existed and not which one you were in,
                                          so there was no way to tell "already on all" from
                                          "the click did nothing" — and the click target was
                                          one character wide in a narrow sticky column. */}
                                      ⚛<span className="text-[9px] ml-0.5 px-1 py-px rounded
                                                        bg-white/10 text-md-on-surface-var">
                                        {physRowMode === 'rare' ? 'rare' : physRowMode === 'ra' ? '+RA' : 'all'}
                                      </span>
                                    </span>
                                  ),
                                  getSigs: (b, prev) => row.getSigs(b, prev, physRowMode) }} />
                ))}
                {/* ⏱ TtRow removed 2026-07-26 (user: adds nothing) — within ONE ticker the ATR%
                    bucket is stable, so the per-bar row is ~constant (AMD +108% breakout sat at
                    "13d" throughout). The forecast's value is CROSS-SECTIONAL (Ultra ⏱ column,
                    fast vs slow names) + the current-state header line — those stay. */}
                {/* 📐 DivFunnelRow and ⛔ VolRow removed from the matrix 2026-07-29 (user).
                    Both components are kept intact — restoring either is one line. Nothing
                    is lost analytically: the divergence masks still drive the Ultra 📐
                    column, the Superchart CSV (DIV_BUY / DIV_R_BULL_STAGE / …) and the
                    Replay board, and the no-volume-event flag still renders as the ⛔ badge
                    on the candle itself plus the ⛔ vol-adjacency veto in the edge gates. */}
                <VrpRow bars={bars} ticker={ticker} />
                <AnatRow bars={bars} hoursMap={day1hMap} />
                {with1H && <Row1H bars={bars} hoursMap={day1hMap} />}
                {/* PV — the nine ordinal price×volume shapes, directly ABOVE UDN★ (user,
                    2026-09-22). 1d only. Two rows for the two price sources, which agree on only
                    52.7% of firing sessions. Descriptive: PV_MULTI_V1 k = 9 → 0 BUILD / 4 VETO /
                    5 NULL, every cell negative in MINE; the four VETOes are recorded, not applied. */}
                {tf === '1d' && ROWS.filter(r => r.key === 'pv').map(row => (
                  ['c', 'o'].map(src => (
                    <ChipRow key={`${row.key}-${src}`} bars={barsPv}
                             row={{ ...row,
                                    label: (
                                      <span title={src === 'c'
                                        ? 'PRICE × VOLUME, price = CLOSE. Nine ordinal shapes from the last 3-5 daily bars. Shown beside the ohlc4 row because the two sources disagree on 47% of hits.'
                                        : 'PRICE × VOLUME, price = OHLC4 — the Pine default. Nine ordinal shapes from the last 3-5 daily bars.'}>
                                        PV·{src === 'c' ? 'C' : 'O'}
                                      </span>
                                    ),
                                    getSigs: (b, prev) => row.getSigs(b, prev, src),
                                    sigTitle: (sig, b) => row.sigTitle(sig, b, src) }} />
                  ))
                ))}
                {/* ECHO — VOL_ECHO marks, directly below the PV rows. 1d only. Descriptive; the
                    ⛔QR▲ chip is the one confirmed VETO (QR_REL_V1), recorded, not applied. */}
                {tf === '1d' && ROWS.filter(r => r.key === 've').map(row => (
                  <ChipRow key={row.key} row={row} bars={barsVe} />
                ))}
                {/* UDN★ — L-BAL agreement marks, directly above SCORE (user, 2026-09-06). 1d only:
                    the row is a per-SESSION decomposition of the daily bar into its 15m bars. */}
                {tf === '1d' && ROWS.filter(r => r.key === 'lbal').map(row => (
                  // Both counting modes side by side rather than one behind a switch: they
                  // disagree bar by bar, and seeing the disagreement is the point (user,
                  // 2026-09-10). The shared V/all toggle still lives on the chart line and the
                  // Ultra chips; it no longer governs these two rows, which are fixed.
                  ['V', 'ALL'].map(m => (
                    <ChipRow key={`${row.key}-${m}`} bars={barsLbal}
                             row={{ ...row,
                                    label: (
                                      <span title={m === 'V'
                                        ? '★ L-BAL, V count — only 15m bars whose volume > SMA20 (the TradingView setting). UDN = effort (L34+L3 vs L46), UDN+ = the labelled bars by their own candle.'
                                        : '★ L-BAL, ALL count — every labelled 15m bar. Shown next to the V row because the two disagree on individual bars, and the disagreement is the reading.'}>
                                        UDN★{m === 'V' ? 'V' : 'all'}
                                      </span>
                                    ),
                                    getSigs: (b, prev) => row.getSigs(b, prev, m),
                                    sigTitle: (sig, b) => row.sigTitle(sig, b, m) }} />
                  ))
                ))}
                {/* SHAPE × CONTEXT — the body-nest shape + its context legs + the two cluster
                    axes, directly under UDN★ (user, 2026-09-10). 1d only. Descriptive: 4 sealed
                    families, k = 21, 0 BUILD; ⛔LST↑ is the one chip with two-window evidence. */}
                {tf === '1d' && ROWS.filter(r => r.key === 'shape').map(row => <ChipRow key={row.key} row={row} bars={barsShape} />)}
                {/* L-VX — graded daily L34/L46, between UDN★ and SCORE (user, 2026-09-06). 1d only. */}
                {tf === '1d' && ROWS.filter(r => r.key === 'lvx').map(row => <ChipRow key={row.key} row={row} bars={barsLvx} />)}
                {/* OVD daily map — opening / closing 15m volume logics, above SCORE (user, 2026-09-06). 1d only. */}
                {tf === '1d' && ROWS.filter(r => r.key === 'ovd').map(row => <ChipRow key={row.key} row={row} bars={barsOvd} />)}
                {/* VOL7 — 7-level volume regime (M·σ, jumps, VB2, SHIFT), above SCORE (user, 2026-09-07). 1d only. */}
                {tf === '1d' && ROWS.filter(r => r.key === 'vol7').map(row => <ChipRow key={row.key} row={row} bars={barsVol7} />)}
                {/* V4 — flat baseline score, directly after VOL7 (user, 2026-09-23). 1d only. FIRST
                    PASS / UNMEASURED — see the caveat on barsV4 above and in v4Score.js. */}
                {tf === '1d' && ROWS.filter(r => r.key === 'v4').map(row => <ChipRow key={row.key} row={row} bars={barsV4} />)}
                {tf === '1d' && ROWS.filter(r => r.key === 'turn').map(row => <ChipRow key={row.key} row={row} bars={barsTurn} />)}
                {tf === '1d' && ROWS.filter(r => r.key === 'rowseq').map(row => <ChipRow key={row.key} row={row} bars={barsRowSeq} />)}
                {tf === '1d' && ROWS.filter(r => r.key === 'top58').map(row => <ChipRow key={row.key} row={row} bars={barsTop58} />)}
                {/* 🏅 RANK_V1 — percentile of that day's edge fires (sealed A table), above SCORE (user, 2026-09-07). 1d only. */}
                {tf === '1d' && ROWS.filter(r => r.key === 'rank').map(row => <ChipRow key={row.key} row={row} bars={bars} />)}
                {ROWS.filter(r => r.key === 'score').map(row => <ChipRow key={row.key} row={row} bars={bars} />)}

                                <tr className="border-t border-white/[0.06]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                                 text-right border-r border-white/[0.08] font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}>
                    ULTRA
                  </td>
                  {bars.map((b, i) => {
                    const s = Math.round(b.ultra_score ?? 0)
                    const cls = s >= 80 ? 'text-lime-300 font-bold'
                              : s >= 65 ? 'text-green-400 font-bold'
                              : s >= 50 ? 'text-yellow-400'
                              : s >= 30 ? 'text-md-on-surface'
                              : s > 0   ? 'text-md-on-surface-var'
                              : 'text-gray-700'
                    return (
                      <td key={i} title={b.ultra_score_reasons || (s ? `ULTRA ${s}` : '')}
                        className={`px-0 py-0.5 text-center border-r border-white/[0.05] font-mono ${cls}`}
                        style={{ fontSize: 13, width: CELL_W, minWidth: CELL_W }}>
                        {s > 0 ? s : ''}
                      </td>
                    )
                  })}
                </tr>

                                <tr className="border-t border-white/[0.06]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-cyan-300/90 px-1
                                 text-right border-r border-white/[0.08] font-mono font-semibold"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}
                      title="ULTRA Score v3 — reweighted ranker (per-bar CORE: oversold RSI + price-zone + earners; the live 🏆RS/🎯cluster/🎋TLS bonuses are omitted historically). Peaks in the oversold-reversal zone, fades into a breakout.">
                    UV3
                  </td>
                  {bars.map((b, i) => {
                    const s = Math.round(b.ultra_score_v3 ?? 0)
                    const band = b.ultra_score_v3_band
                    const cls = band === 'A' ? 'text-green-300 font-bold'
                              : band === 'B' ? 'text-lime-300 font-semibold'
                              : band === 'C' ? 'text-amber-300'
                              : s > 0        ? 'text-md-on-surface-var'
                              : 'text-gray-700'
                    return (
                      <td key={i}
                        title={Array.isArray(b.ultra_score_v3_reasons) ? b.ultra_score_v3_reasons.join(' · ') : (s ? `UV3 ${s}` : '')}
                        className={`px-0 py-0.5 text-center border-r border-white/[0.05] font-mono ${cls}`}
                        style={{ fontSize: 13, width: CELL_W, minWidth: CELL_W }}>
                        {s > 0 ? s : ''}
                      </td>
                    )
                  })}
                </tr>

                                <tr className="border-t border-white/[0.12]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                                 text-right border-r border-white/[0.08] font-mono font-semibold"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}
                      title="Validated BUY flags (6yr path-sim). 🟢 REV = bounce off oversold (min-5 RSI<38, RSI 30-55, up bar) + LOW beta ≤13 → +1.05%/win~50%/5-6yr — shown GREEN only when the 4H/1H also printed the turn (⏱️MTF echo, +1.09…+1.17%/6-6yr). ⚠️ = same REV but NO intraday echo — validated VETO (−1.07%/win39, 0/6yr): skip. 🔵 BRK = RSI crosses 50 up + LOW turbo ≤28 → +0.58%/5-6yr. DIGITS 0-3 = on days where the 1D buy_score≥60, how many intraday TFs (4H/1H/15M) also print buy_score≥60 (D or D-1): 0=red HARD SKIP (−2.10%/med−10.2/win36, 0/6yr) · 1-3 tradeable (+1.3…+1.6%, 5-6/6yr) — the whole edge is 0↔1. CIRCLED ①②③ = turn-echo: a loose daily up-turn (no depth needed, the AMD-type case) where N/3 intraday TFs printed the strict REV-turn (echo≥1 +0.7…+1.05%/5-6yr; zero-echo turns −1.40%/win39/0-6yr are simply not marked). High momentum scores are the trap — these buy the beaten-down turn, not the run.">
                    BUY
                  </td>
                  {bars.map((b, i) => (
                    <td key={i}
                      title={b.rev_buy
                        ? (b.mtf_echo === false
                          ? '⚠️ REV-buy WITHOUT 4H/1H echo — validated VETO (no-echo group: −1.07%/win39, 0/6yr). The intraday never printed the oversold-turn → the daily turn is likely shallow/fake. Skip.'
                          : b.mtf_echo === true
                            ? '🟢 REV-buy + ⏱️MTF echo — 4H/1H also printed the oversold-turn (echo group +1.09…+1.17%/win46, 6/6yr)'
                            : '🟢 REV-buy — oversold bounce + low beta (intraday echo data unavailable for this bar)')
                        : b.brk_buy ? '🔵 BRK-buy — RSI>50 cross + low turbo'
                        : b.mtf_score_conf != null
                          ? (b.mtf_score_conf === 0
                            ? '0/3 — 1D buy_score≥60 but NO intraday TF (4H/1H/15M) confirms ≥60 — validated HARD SKIP (−2.10%/med−10.2/win36, 0/6yr)'
                            : `${b.mtf_score_conf}/3 intraday TFs confirm the good 1D buy_score (≥1 confirmed → +1.3…+1.6%, 5-6/6yr; the big edge is 0↔1)`)
                          : b.turn_echo_n
                            ? `①②③ turn-echo: daily up-turn (no depth needed) where ${b.turn_echo_n}/3 intraday TFs printed the strict REV-turn on D or D-1 (echo≥1 → +0.7…+1.05%, 5/6yr; a turn with ZERO echo = −1.40%/win39, 0/6yr). The AMD-type shallow-dip turn marker.`
                            : ''}
                      className="px-0 py-0.5 text-center border-r border-white/[0.05]"
                      style={{ fontSize: 13, width: CELL_W, minWidth: CELL_W }}>
                      {b.rev_buy ? (b.mtf_echo === false ? '⚠️' : '🟢')
                        : b.brk_buy ? '🔵'
                        : b.mtf_score_conf != null ? (
                          <span className={`font-mono font-bold ${
                            b.mtf_score_conf === 0 ? 'text-red-400'
                            : b.mtf_score_conf === 1 ? 'text-amber-300'
                            : b.mtf_score_conf === 2 ? 'text-lime-300'
                            : 'text-green-300'}`}>{b.mtf_score_conf}</span>
                        ) : b.turn_echo_n ? (
                          <span className="text-cyan-300 font-bold" style={{ fontSize: 22, lineHeight: 1 }}>
                            {b.turn_echo_n === 1 ? '①' : b.turn_echo_n === 2 ? '②' : '③'}
                          </span>
                        ) : ''}
                    </td>
                  ))}
                </tr>

                {/* 4H intraday-leads row — the early-entry trigger existed inside this daily bar */}
                <tr className="border-t border-white/[0.06]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-cyan-400/90 px-1
                                 text-right border-r border-white/[0.08] font-mono font-semibold"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}
                      title="⏱ intraday-leads (validated 2026-07-19, lead4h.py, n=244k matched pairs): ▲ = a 4H REV-trigger fired DURING this daily bar — the early intraday entry existed. Entering on the 4H trigger instead of waiting for the daily close: +0.84pp mean, 6/6yr incl 2022 (fill advantage +0.70%). △ = only the 1H fired. On breakout mornings this is the validated early entry — open the 4H view and take the trigger.">
                    4H⏱
                  </td>
                  {bars.map((b, i) => (
                    <td key={i}
                      title={b.h4_rev_today ? '▲ 4H REV-trigger fired during this daily bar — early entry beat the daily close by +0.84pp mean (6/6yr). Switch to the 4H view for the exact bar.'
                        : b.h1_rev_today ? '△ 1H REV-trigger fired during this daily bar (4H stayed silent)' : ''}
                      className="px-0 py-0.5 text-center border-r border-white/[0.05]"
                      style={{ fontSize: 14, width: CELL_W, minWidth: CELL_W }}>
                      {b.h4_rev_today ? <span className="text-cyan-300 font-bold">▲</span>
                        : b.h1_rev_today ? <span className="text-cyan-500/70">△</span> : ''}
                    </td>
                  ))}
                </tr>

                                <tr className="border-t border-white/[0.06]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                                 text-right border-r border-white/[0.08] font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}>
                    β
                  </td>
                  {bars.map((b, i) => {
                    const sc   = b.beta_score
                    const zone = b.beta_zone ?? 'NEUTRAL'
                    const auto = b.beta_auto_buy
                    if (!sc) return <td key={i} style={{ width: CELL_W, minWidth: CELL_W }} className="border-r border-white/[0.05]" />
                    const cls = BETA_ZONE_CLS[zone] ?? 'text-md-on-surface-var'
                    return (
                      <td key={i}
                        className={`px-0 py-px text-center border-r border-white/[0.05] font-mono ${cls}`}
                        style={{ fontSize: 13, width: CELL_W, minWidth: CELL_W }}
                        title={`BETA ${sc} · ${zone}${auto ? ' · AUTO-BUY ★' : ''}`}>
                        <div className="flex flex-col items-center leading-none gap-px">
                          <span>{sc}{auto ? '★' : ''}</span>
                          <span style={{ fontSize: 11 }} className="text-md-on-surface-var font-mono">
                            {BETA_ZONE_SHORT[zone] ?? ''}
                          </span>
                        </div>
                      </td>
                    )
                  })}
                </tr>

                                <tr className="border-t border-white/[0.06]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                                 text-right border-r border-white/[0.08] font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}>
                    v2
                  </td>
                  {bars.map((b, i) => {
                    const e = v2Map[String(b.date).slice(0, 10)]
                    if (!e || e.v2 == null) return (
                      <td key={i} style={{ width: CELL_W, minWidth: CELL_W }}
                          className="border-r border-white/[0.05]" />
                    )
                    const cls = e.band === 'BUY' ? 'text-green-300 font-bold'
                              : e.band === 'HOT' ? 'text-amber-300 font-bold'
                              : 'text-md-on-surface-var'
                    return (
                      <td key={i}
                        className={`px-0 py-px text-center border-r border-white/[0.05] font-mono ${cls}`}
                        style={{ fontSize: 13, width: CELL_W, minWidth: CELL_W }}
                        title={`PreBreakout v2 = ${e.v2} · ${e.band}`}>
                        <div className="flex flex-col items-center leading-none gap-px">
                          <span>{e.v2}</span>
                          {e.band !== 'WATCH' && (
                            <span className="leading-none" style={{ fontSize: 10 }}>
                              {e.band === 'BUY' ? 'B' : 'H'}
                            </span>
                          )}
                        </div>
                      </td>
                    )
                  })}
                </tr>

                {ROWS.filter(r => r.key === 'prebreak_v3').map(row => <ChipRow key={row.key} row={row} bars={bars} />)}

                                <tr className="border-t border-white/[0.06]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                                 text-right border-r border-white/[0.08] font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}>
                    rtb
                  </td>
                  {bars.map((b, i) => {
                    const ph = b.rtb_phase
                    if (!ph || ph === '0') return (
                      <td key={i} style={{ width: CELL_W, minWidth: CELL_W }}
                          className="border-r border-white/[0.05]" />
                    )
                    const bgCls =
                      ph === 'C' ? 'bg-lime-700/80 text-lime-100 ring-1 ring-lime-500' :
                      ph === 'B' ? 'bg-sky-800/80  text-sky-200  ring-1 ring-sky-600' :
                      ph === 'A' ? 'bg-gray-700    text-md-on-surface' :
                      /* D */      'bg-orange-800/70 text-orange-200'
                    const isTransition = b.rtb_transition && b.rtb_transition.includes('TO')
                    return (
                      <td key={i}
                        className="px-0 py-px text-center border-r border-white/[0.05]"
                        style={{ width: CELL_W, minWidth: CELL_W }}
                        title={b.rtb_transition ? `${ph} — ${b.rtb_transition} (${b.rtb_total})` : `Phase ${ph} (${b.rtb_total})`}>
                        <div className="flex flex-col items-center gap-px">
                          <span className={`inline-block font-bold px-0.5 rounded font-mono leading-none ${bgCls} ${isTransition ? 'ring-2' : ''}`}
                            style={{ fontSize: 13 }}>
                            {ph}
                          </span>
                          <span className="font-mono text-md-on-surface-var leading-none" style={{ fontSize: 11 }}>
                            {b.rtb_total > 0 ? b.rtb_total.toFixed(0) : ''}
                          </span>
                        </div>
                      </td>
                    )
                  })}
                </tr>

                                <tr className="border-t border-white/[0.08]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                                 text-right border-r border-white/[0.08] font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}>
                    close
                  </td>
                  {bars.map((b, i) => {
                    const prev = i > 0 ? bars[i - 1].close : b.close
                    const up   = b.close >= prev
                    return (
                      <td key={i}
                        className={`px-0 py-0.5 text-center border-r border-white/[0.05] font-mono
                                    ${up ? 'text-green-400' : 'text-red-400'}`}
                        style={{ fontSize: 13, width: CELL_W, minWidth: CELL_W }}>
                        {b.close >= 1000 ? b.close.toFixed(0)
                          : b.close >= 100 ? b.close.toFixed(1)
                          : b.close.toFixed(2)}
                      </td>
                    )
                  })}
                </tr>

                                <tr className="border-t border-white/[0.06]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                                 text-right border-r border-white/[0.08] font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}>
                    RSI
                  </td>
                  {bars.map((b, i) => {
                    const v = b.rsi ?? b.RSI
                    if (v == null || v === 0) return <td key={i} style={{ width: CELL_W, minWidth: CELL_W }} className="border-r border-white/[0.05]" />
                    const cls = v <= 35 ? 'text-lime-300 font-bold' : v >= 70 ? 'text-red-400 font-bold' : 'text-md-on-surface-var'
                    return (
                      <td key={i}
                        className={`px-0 py-0.5 text-center border-r border-white/[0.05] font-mono ${cls}`}
                        style={{ fontSize: 13, width: CELL_W, minWidth: CELL_W }}>
                        {Math.round(v)}
                      </td>
                    )
                  })}
                </tr>

                                <tr className="border-t border-white/[0.06]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                                 text-right border-r border-white/[0.08] font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}>
                    CCI
                  </td>
                  {bars.map((b, i) => {
                    const v = b.cci ?? b.CCI
                    if (v == null) return <td key={i} style={{ width: CELL_W, minWidth: CELL_W }} className="border-r border-white/[0.05]" />
                    const cls = v >= 100 ? 'text-lime-300 font-bold' : v <= -100 ? 'text-red-400 font-bold' : 'text-md-on-surface-var'
                    return (
                      <td key={i}
                        className={`px-0 py-0.5 text-center border-r border-white/[0.05] font-mono ${cls}`}
                        style={{ fontSize: 13, width: CELL_W, minWidth: CELL_W }}>
                        {Math.round(v)}
                      </td>
                    )
                  })}
                </tr>

                                <tr className="border-t border-white/[0.06]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                                 text-right border-r border-white/[0.08] font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}>
                    Pf
                  </td>
                  {bars.map((b, i) => {
                    const v = b.profile_score
                    if (!v) return <td key={i} style={{ width: CELL_W, minWidth: CELL_W }} className="border-r border-white/[0.05]" />
                    const cls = v >= 20 ? 'text-lime-300 font-bold' : v >= 12 ? 'text-yellow-400' : 'text-md-on-surface-var'
                    return (
                      <td key={i}
                        className={`px-0 py-0.5 text-center border-r border-white/[0.05] font-mono ${cls}`}
                        style={{ fontSize: 13, width: CELL_W, minWidth: CELL_W }}>
                        {v}
                      </td>
                    )
                  })}
                </tr>

                                <tr className="border-t border-white/[0.08]">
                  <td className="sticky left-0 z-10 bg-md-surface-con text-md-on-surface-var px-1
                                 text-right border-r border-white/[0.08] font-mono"
                      style={{ width: HDR_W, minWidth: HDR_W, fontSize: 13 }}>
                    Cat
                  </td>
                  {bars.map((b, i) => {
                    const cat = b.profile_category
                    // 💠 TRIPLE (2026-07-21, user request — surfaced in the Cat row): red-L34 +
                    // 🟢REV + ▲4H on ONE bar (+2.05%/PF1.29, TRAIN≈TEST). Takes the cell over.
                    const triple = b.l_sig === 'L34' && Number(b.close) < Number(b.open) && b.rev_buy && b.h4_rev_today
                    if (triple) return (
                      <td key={i} style={{ width: CELL_W, minWidth: CELL_W }}
                        className="border-r border-white/[0.05] text-center"
                        title="💠 TRIPLE — red-L34 + 🟢REV + ▲4H same bar: +2.05%/win47/PF1.29, TRAIN +1.98 ≈ TEST +1.80 (the era-balanced triple confluence)">
                        <span style={{ fontSize: 14 }}>💠</span>
                      </td>
                    )
                    // ● whisper (2026-07-21, RGTI 09-08 case): strong bullish seq-context
                    // (⤴ up>=60, era-consistent cell) + an intraday echo on the SAME bar.
                    const whisper = b.seq_ctx?.dir === 'up' && (b.seq_ctx?.up ?? 0) >= 60
                      && ((b.turn_echo_n ?? 0) > 0 || (b.mtf_score_conf ?? 0) > 0 || b.h4_rev_today || b.h1_rev_today)
                    if (whisper) return (
                      <td key={i} style={{ width: CELL_W, minWidth: CELL_W }}
                        className="border-r border-white/[0.05] text-center"
                        title={`● whisper — strong seq-context ⤴${Math.round(b.seq_ctx.up)} (${b.seq_ctx.seq}) + intraday echo on the same bar (the RGTI-0908 shape: coil flagged before ignition)`}>
                        <span className="text-teal-300" style={{ fontSize: 13 }}>●</span>
                      </td>
                    )
                    if (!cat || cat === 'WATCH') return <td key={i} style={{ width: CELL_W, minWidth: CELL_W }} className="border-r border-white/[0.05]" />
                    const cls =
                      cat === 'SWEET_SPOT' ? 'text-green-300 font-bold' :
                      cat === 'BUILDING'   ? 'text-yellow-400' :
                      cat === 'LATE'       ? 'text-amber-500' : 'text-md-on-surface-var/70'
                    const label =
                      cat === 'SWEET_SPOT' ? '⭐' :
                      cat === 'BUILDING'   ? '↑' :
                      cat === 'LATE'       ? '⚠' : ''
                    return (
                      <td key={i}
                        className={`px-0 py-0.5 text-center border-r border-white/[0.05] ${cls}`}
                        title={cat}
                        style={{ fontSize: 13, width: CELL_W, minWidth: CELL_W }}>
                        {label}
                      </td>
                    )
                  })}
                </tr>

                {/* the remaining signal families */}
                {/* Every row rendered explicitly ABOVE this line must also be excluded HERE, or
                    this catch-all silently draws it a second time at the bottom — it already did
                    that once for 'phys'. Caught again 2026-09-23: 'pv' (2026-09-22) and 'v4'
                    (2026-09-23) were both added with their own dedicated render line and BOTH
                    were missing from this list, so PV·C/PV·O and V4 were each rendering twice. */}
                {ROWS.filter(r => !['z', 'td', 'l', 'score', 'prebreak_v3', 'phys', 'bodywick', 'cisd', 'lbal', 'lvx', 'ovd', 'vol7', 'rank', 'shape', 'pv', 've', 'v4', 'turn', 'rowseq', 'top58'].includes(r.key)).map(row => <ChipRow key={row.key} row={row} bars={bars} />)}

                {/* ULTRA row — computed per-bar (independent confluence ranking) */}

                {/* UV3 row — ULTRA Score v3, reweighted ranker (oversold + price + earners) */}



                {/* RTB v4 phase row */}

                {/* BETA Score row */}

                {/* Close price row */}

                {/* RSI row */}

                {/* CCI row */}

                {/* Pf Score row */}



              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Signal Statistics Panel ── */}
      {showStats && (
        <div className="bg-md-surface-con rounded-md-md border border-white/[0.08] overflow-hidden">
          <div className="flex items-center gap-3 px-3 py-2 border-b border-white/[0.07] bg-md-surface">
            <span className="text-xs font-semibold text-violet-300">Signal Performance — {ticker} {tf.toUpperCase()}</span>
            <span className="text-xs text-md-on-surface-var">avg max-high over next N bars · sorted by</span>
            {statsLoading && <span className="text-xs text-md-on-surface-var animate-pulse ml-auto">loading…</span>}
          </div>

          {statsLoading ? (
            <div className="p-6 text-xs text-md-on-surface-var/70 text-center animate-pulse">Computing stats for all signals…</div>
          ) : !statsData || statsData.error ? (
            <div className="p-4 text-xs text-red-400">Could not load stats — {statsData?.error ?? 'unknown error'}</div>
          ) : sortedStats.length === 0 ? (
            <div className="p-4 text-xs text-md-on-surface-var">Not enough data (need ≥3 occurrences per signal)</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-xs border-collapse">
                <thead>
                  <tr className="border-b border-white/[0.07] bg-md-surface text-md-on-surface-var select-none">
                    <th className="text-left px-3 py-1.5 sticky left-0 bg-md-surface font-normal">Signal</th>
                    {[
                      ['n',         'N',     'occurrences'],
                      ['bull_rate', 'Bull%', 'next bar closed higher'],
                      ['avg_1bar',  '+1bar', 'avg % close next bar'],
                      ['avg_3bar',  'max3',  'avg max-high over 3 bars'],
                      ['avg_5bar',  'max5',  'avg max-high over 5 bars ★'],
                      ['mae_3',     'DD3',   'avg max drawdown over 3 bars'],
                      ['false_rate','False%','% fires with no gain over 3 bars'],
                    ].map(([col, label, title]) => (
                      <th key={col}
                        title={title}
                        onClick={() => setStatsSort(col)}
                        className={`text-right px-2 py-1.5 cursor-pointer whitespace-nowrap font-normal hover:text-white transition-colors
                          ${statsSort === col ? 'text-violet-300 bg-violet-950/40' : ''}`}>
                        {label}{statsSort === col ? ' ▼' : ''}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {sortedStats.map(([key, st], idx) => {
                    const label = statsData.labels?.[key] ?? key
                    return (
                      <tr key={key}
                        className={`border-b border-white/[0.06] hover:bg-md-surface-high/30 ${idx === 0 && statsSort === 'avg_5bar' ? 'bg-violet-950/20' : ''}`}>
                        <td className="px-3 py-1 sticky left-0 bg-md-surface-con text-md-on-surface whitespace-nowrap font-mono" style={{ fontSize: 13 }}>
                          {label}
                        </td>
                        <td className="px-2 py-1 text-right font-mono text-md-on-surface-var">{st.n}</td>
                        <td className={`px-2 py-1 text-right font-mono
                          ${st.bull_rate >= 0.65 ? 'text-lime-300' : st.bull_rate >= 0.55 ? 'text-green-400' : st.bull_rate >= 0.45 ? 'text-yellow-400' : 'text-red-400'}`}>
                          {Math.round(st.bull_rate * 100)}%
                        </td>
                        <td className={`px-2 py-1 text-right font-mono ${st.avg_1bar > 0 ? 'text-green-400' : 'text-red-400'}`}>
                          {st.avg_1bar > 0 ? '+' : ''}{st.avg_1bar?.toFixed(1)}%
                        </td>
                        <td className={`px-2 py-1 text-right font-mono ${st.avg_3bar > 1.5 ? 'text-green-400' : st.avg_3bar > 0 ? 'text-md-on-surface' : 'text-md-on-surface-var/70'}`}>
                          {st.avg_3bar > 0 ? '+' : ''}{st.avg_3bar?.toFixed(1)}%
                        </td>
                        <td className={`px-2 py-1 text-right font-mono font-semibold
                          ${statsSort === 'avg_5bar' ? 'bg-violet-950/20' : ''}
                          ${st.avg_5bar > 4 ? 'text-lime-300' : st.avg_5bar > 2 ? 'text-green-400' : st.avg_5bar > 0 ? 'text-md-on-surface' : 'text-md-on-surface-var/70'}`}>
                          {st.avg_5bar > 0 ? '+' : ''}{st.avg_5bar?.toFixed(1)}%
                        </td>
                        <td className="px-2 py-1 text-right font-mono text-red-400/80">
                          {st.mae_3?.toFixed(1)}%
                        </td>
                        <td className={`px-2 py-1 text-right font-mono
                          ${st.false_rate < 0.25 ? 'text-green-400' : st.false_rate < 0.4 ? 'text-yellow-400' : 'text-red-400'}`}>
                          {Math.round(st.false_rate * 100)}%
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
              <div className="px-3 py-2 text-xs text-md-on-surface-var/70">
                {statsData.bars} bars analysed · signals with &lt;3 occurrences hidden · click column header to re-sort
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
