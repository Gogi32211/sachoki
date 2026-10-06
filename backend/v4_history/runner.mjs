// Node side of v4_history_build.py.
//   node runner.mjs <bundle> catalog              → JSON {keys:[…catalog keys in order…], weights:{key:w}}
//   node runner.mjs <bundle> fire < rows.ndjson   → one line per row: {t, d, k:"key1 key2 …", s:<score>}
// Same evaluation as the Superchart V4 row: v4Fired(V4_ALL_GROUPS, bar, 1).
globalThis.window = globalThis.window || globalThis
globalThis.localStorage = globalThis.localStorage || { getItem: () => null, setItem() {}, removeItem() {} }
globalThis.sessionStorage = globalThis.sessionStorage || { getItem: () => null, setItem() {}, removeItem() {} }
globalThis.document = globalThis.document || { addEventListener() {}, createElement: () => ({ style: {} }) }
globalThis.navigator = globalThis.navigator || { userAgent: 'node' }
const { pathToFileURL } = await import('node:url')
const [bundle, mode] = process.argv.slice(2)
const { V4_ALL_GROUPS, v4Fired, V4_WEIGHTS } = await import(pathToFileURL(bundle).href)
if (mode === 'catalog') {
  const keys = V4_ALL_GROUPS.filter(s => !s.divider && s.key).map(s => s.key)
  process.stdout.write(JSON.stringify({ keys, weights: V4_WEIGHTS }))
} else if (mode === 'fire') {
  const rl = (await import('node:readline')).createInterface({ input: process.stdin, crlfDelay: Infinity })
  const out = []
  for await (const line of rl) {
    if (!line) continue
    const b = JSON.parse(line)
    const fired = v4Fired(V4_ALL_GROUPS, b, 1)
    let s = 0
    for (const sig of fired) s += V4_WEIGHTS[sig.key] ?? 0
    out.push(JSON.stringify({ t: b.ticker, d: b.date, k: fired.map(f => f.key).join(' '), s }))
    if (out.length >= 5000) { process.stdout.write(out.join('\n') + '\n'); out.length = 0 }
  }
  if (out.length) process.stdout.write(out.join('\n') + '\n')
} else {
  console.error('mode must be catalog | fire'); process.exit(2)
}
