const BASE = import.meta.env.VITE_API_URL || ''

async function get(path) {
  const res = await fetch(BASE + path)
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`)
  return res.json()
}

async function post(path, body) {
  const res = await fetch(BASE + path, {
    method: 'POST',
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  })
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`)
  return res.json()
}

export const api = {
  // Signals + chart (now includes WLNBB columns + per-bar scores)
  signals: (ticker, tf = '1d', bars = 150) =>
    get(`/api/signals/${ticker}?tf=${tf}&bars=${bars}`),

  wlnbb: (ticker, tf = '1d', bars = 150) =>
    get(`/api/wlnbb/${ticker}?tf=${tf}&bars=${bars}`),

  // Watchlist
  watchlist: (tickers, tf = '1d') =>
    get(`/api/watchlist?tickers=${tickers.join(',')}&tf=${tf}`),
  watchlistSaved: () =>
    get('/api/watchlist/saved'),
  watchlistSave: (tickers) =>
    post('/api/watchlist/save', { tickers }),

  // Predictor (T/Z + L-combo, 4 tables)
  predict: (ticker, tf = '1d') =>
    get(`/api/predict/${ticker}?tf=${tf}`),

  // L-combo predictor (dedicated)
  lPredict: (ticker, tf = '1d') =>
    get(`/api/l-predict/${ticker}?tf=${tf}`),

  // T/Z × L stats matrix
  tzLStats: (ticker, tf = '1d') =>
    get(`/api/tz-l-stats/${ticker}?tf=${tf}`),

  // Scanner
  scanResults: (tf = '1d', limit = 100, tab = 'all', min_score = 0) =>
    get(`/api/scan/results?tf=${tf}&limit=${limit}&tab=${tab}&min_score=${min_score}`),
  scanTrigger: (tf = '1d') =>
    post(`/api/scan/trigger?tf=${tf}`),
  scanStatus: () =>
    get('/api/scan/status'),

  // Combined
  combinedScan: (tf = '1d', min_score = 4, tab = 'bull', limit = 100) =>
    get(`/api/combined-scan?tf=${tf}&min_score=${min_score}&tab=${tab}&limit=${limit}`),

  // Pump combos
  pumpCombos: (threshold = 2.0, window = 20, combo_len = 3, limit = 50) =>
    get(`/api/pump-combos?threshold=${threshold}&window=${window}&combo_len=${combo_len}&limit=${limit}`),
  pumpTrigger: (threshold = 2.0, window = 20, combo_len = 3) =>
    post(`/api/pump-combos/trigger?threshold=${threshold}&window=${window}&combo_len=${combo_len}`),

  // 260323 Combo scan
  comboScan: (signal = 'all', limit = 200) =>
    get(`/api/combo-scan?signal=${signal}&limit=${limit}`),
  comboScanTrigger: (tf = '1d', n_bars = 3) =>
    post(`/api/combo-scan/trigger?tf=${tf}&n_bars=${n_bars}`),
  comboScanStatus: () =>
    get('/api/combo-scan/status'),
  comboScanDebug: (ticker, tf = '1d', rows = 7, n_bars = 3) =>
    get(`/api/combo-scan/debug/${ticker}?tf=${tf}&rows=${rows}&n_bars=${n_bars}`),

  // Power Scan (260323 + T/Z + WLNBB confluence)
  powerScan: (limit = 200) =>
    get(`/api/power-scan?limit=${limit}`),
  powerScanTrigger: (tf = '1d', n_bars = 3) =>
    post(`/api/power-scan/trigger?tf=${tf}&n_bars=${n_bars}`),
  powerScanStatus: () =>
    get('/api/power-scan/status'),

  // BR Scan (260328 Break Readiness)
  brScan: (limit = 300, min_br = 0, entry = 'all') =>
    get(`/api/br-scan?limit=${limit}&min_br=${min_br}&entry=${entry}`),
  brScanTrigger: (tf = '1d') =>
    post(`/api/br-scan/trigger?tf=${tf}`),
  brScanStatus: () =>
    get('/api/br-scan/status'),

  // Settings
  getSettings: () => get('/api/settings'),
  saveSettings: (s) => post('/api/settings', s),
}
