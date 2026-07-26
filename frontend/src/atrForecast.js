// ATR-based time-to-target forecast (port of backend/atr_forecast.py, 2026-07-26).
// The real price↔time law is volatility-driven: days-to-move-X% ≈ (X/ATR%)^0.67, OOS-calibrated
// (TRAIN 2021-23 ≈ TEST 2024-26). Forecasts TIMING/probability — NOT a directional buy signal
// (targets hit at ~base-rate; edge lives in downside/path). Use for stop/time-stop/expectation.

// ATR% bucket → {target%: [hit_rate%, median_days(, P(<=20d)%)]}
const BUCKETS = [[0, 0.02], [0.02, 0.03], [0.03, 0.04], [0.04, 0.06], [0.06, 0.10], [0.10, 9]]
const UP = {   // +X%
  0.00: { 10: [46, 38, 10], 25: [8, 66] },
  0.02: { 10: [57, 27, 22], 25: [18, 53] },
  0.03: { 10: [67, 19, 35], 25: [30, 44] },
  0.04: { 10: [73, 13, 49], 25: [43, 33] },
  0.06: { 10: [79, 7, 62],  25: [54, 21] },
  0.10: { 10: [82, 3, 72],  25: [62, 9] },
}
const DOWN = { // -X%
  0.00: { 10: [39, 34], 20: [10, 55] },
  0.02: { 10: [53, 26], 20: [22, 48] },
  0.03: { 10: [62, 19], 20: [33, 40] },
  0.04: { 10: [71, 13], 20: [46, 30] },
  0.06: { 10: [81, 8],  20: [61, 20] },
  0.10: { 10: [88, 4],  20: [75, 9] },
}

function bucketLo(atrPct) {
  for (const [lo, hi] of BUCKETS) if (atrPct >= lo && atrPct < hi) return lo
  return 0.10
}

// atrPct as a FRACTION (0.03 = 3%). Returns null if invalid.
export function atrForecast(atrPct) {
  if (!(atrPct > 0)) return null
  const lo = bucketLo(atrPct)
  const u = UP[lo], d = DOWN[lo]
  const label = lo >= 0.10 ? '10%+' : `${Math.round(lo * 100)}-${Math.round((BUCKETS.find(b => b[0] === lo)[1]) * 100)}%`
  return {
    atrPct: +(atrPct * 100).toFixed(2), bucket: label,
    up10: { hit: u[10][0], days: u[10][1], p20: u[10][2] },
    up25: { hit: u[25][0], days: u[25][1] },
    dn10: { hit: d[10][0], days: d[10][1] },
    dn20: { hit: d[20][0], days: d[20][1] },
  }
}

// Wilder ATR(14) per bar from OHLC (chronological). Returns array aligned to bars (NaN warmup).
export function computeAtr14(bars, P = 14) {
  const n = bars.length, tr = new Array(n).fill(NaN), atr = new Array(n).fill(NaN)
  for (let i = 0; i < n; i++) {
    const h = +bars[i].high, l = +bars[i].low
    if (i === 0) { tr[i] = h - l; continue }
    const pc = +bars[i - 1].close
    tr[i] = Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc))
  }
  let sum = 0
  for (let i = 0; i < n; i++) {
    if (i < P) { sum += tr[i]; if (i === P - 1) atr[i] = sum / P }
    else atr[i] = (atr[i - 1] * (P - 1) + tr[i]) / P
  }
  return atr
}

// Per-bar CSV cells for a given atrPct (fraction). Returns [] if invalid.
export function forecastCsvCells(atrPct) {
  const f = atrForecast(atrPct)
  if (!f) return ['', '', '', '', '']
  return [f.atrPct, f.up10.days, f.up10.hit, f.up25.days, f.dn10.days]
}
export const FORECAST_CSV_HEADERS = ['ATR_PCT', 'TT_UP10_DAYS', 'TT_UP10_HIT', 'TT_UP25_DAYS', 'TT_DN10_DAYS']

// Compact one-liner for the Superchart row / header.
export function fmtForecast(atrPct, offLow) {
  const f = atrForecast(atrPct)
  if (!f) return ''
  const off = (offLow != null && isFinite(offLow)) ? ` · ${offLow.toFixed(1)}σ off low` : ''
  return `ATR ${f.atrPct}% · +10% ~${f.up10.days}d (${f.up10.hit}%) · +25% ~${f.up25.days}d · −10% ~${f.dn10.days}d${off}`
}
