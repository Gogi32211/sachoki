import { useState } from 'react'

const TIMEFRAMES = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1wk']

export default function TickerInput({ watchlist, onAdd, onRemove, tf, onTfChange }) {
  const [input, setInput] = useState('')

  function handleAdd(e) {
    e.preventDefault()
    const t = input.trim().toUpperCase()
    if (t) { onAdd(t); setInput('') }
  }

  return (
    <div className="flex items-center gap-3">
      {/* Timeframe */}
      <select
        value={tf}
        onChange={(e) => onTfChange(e.target.value)}
        className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-sm"
      >
        {TIMEFRAMES.map((t) => (
          <option key={t} value={t}>{t}</option>
        ))}
      </select>

      {/* Add ticker */}
      <form onSubmit={handleAdd} className="flex gap-2">
        <input
          value={input}
          onChange={(e) => setInput(e.target.value.toUpperCase())}
          placeholder="Add ticker…"
          className="bg-gray-800 border border-gray-700 rounded px-2 py-1 text-sm w-28 uppercase"
        />
        <button
          type="submit"
          className="bg-blue-600 hover:bg-blue-500 rounded px-3 py-1 text-sm font-semibold"
        >
          +
        </button>
      </form>
    </div>
  )
}
