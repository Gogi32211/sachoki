const BASE = import.meta.env.VITE_API_URL || ''

async function get(path) {
  const res = await fetch(BASE + path)
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`)
  return res.json()
}

async function post(path) {
  const res = await fetch(BASE + path, { method: 'POST' })
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`)
  return res.json()
}

export const api = {
  signals: (ticker, tf = '1d', bars = 150) =>
    get(`/api/signals/${ticker}?tf=${tf}&bars=${bars}`),

  predict: (ticker, tf = '1d') =>
    get(`/api/predict/${ticker}?tf=${tf}`),

  scanResults: (tf = '1d', limit = 50) =>
    get(`/api/scan/results?tf=${tf}&limit=${limit}`),

  scanTrigger: (tf = '1d') =>
    post(`/api/scan/trigger?tf=${tf}`),

  watchlist: (tickers, tf = '1d') =>
    get(`/api/watchlist?tickers=${tickers.join(',')}&tf=${tf}`),
}
