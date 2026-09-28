// Node side of turn_rowseq_build.py.
//   node runner.mjs <bundle> deps                         → {key: [fields…]} for every catalog key
//   node runner.mjs <bundle> compute <keep> < rows.ndjson → per ticker, the last <keep> sessions:
//        {t, d, turn_cand, turn_n, rs: {FLY:0|1|2, …, pair}}
// rows.ndjson must be sorted by ticker, date (oldest→newest) — the same bar order the Superchart feeds.
globalThis.window = globalThis.window || globalThis
globalThis.localStorage = globalThis.localStorage || { getItem: () => null, setItem() {}, removeItem() {} }
globalThis.sessionStorage = globalThis.sessionStorage || { getItem: () => null, setItem() {}, removeItem() {} }
globalThis.document = globalThis.document || { addEventListener() {}, createElement: () => ({ style: {} }) }
globalThis.navigator = globalThis.navigator || { userAgent: 'node' }
const { pathToFileURL } = await import('node:url')
const [bundle, mode, keepArg] = process.argv.slice(2)
const { V4_ALL_GROUPS, v4Fired, withTurnCount, withRowSeq, ROW_ORDER } = await import(pathToFileURL(bundle).href)
const catalog = V4_ALL_GROUPS.filter(s => !s.divider && s.key)
if (mode === 'deps') {
  const out = {}
  for (const sig of catalog) {
    const fields = new Set()
    const probe = (truthy) => new Proxy({}, {
      get: (_, p) => { if (typeof p !== 'string') return undefined; fields.add(p)
        if (truthy) return p === 'edges' ? [] : (p === 'seq_ctx' || p === 'seq34' ? {} : 1); return undefined },
      has: (_, p) => { if (typeof p === 'string') fields.add(p); return false },
    })
    try { if (sig.custom) { sig.custom(probe(true), 1); sig.custom(probe(false), 1) } else fields.add(sig.key) } catch {}
    // early-return branches can hide a read from the probe → also take every field named in the source
    if (sig.custom) {
      const src = sig.custom.toString()
      const params = (src.match(/^\s*(?:function\s*\w*)?\s*\(?\s*([A-Za-z_$][\w$]*)/) || [])[1]
      const names = new Set([params, 'b', 'r', 'row'].filter(Boolean))
      for (const m of src.matchAll(/\b([A-Za-z_$][\w$]*)\??\.([A-Za-z_][\w]*)/g)) if (names.has(m[1])) fields.add(m[2])
      for (const m of src.matchAll(/\b([A-Za-z_$][\w$]*)\[['"]([A-Za-z_][\w]*)['"]\]/g)) if (names.has(m[1])) fields.add(m[2])
    }
    fields.delete('_ages'); fields.delete('sig_ages')
    out[sig.key] = [...fields]
  }
  process.stdout.write(JSON.stringify(out))
} else if (mode === 'compute') {
  const keep = Number(keepArg || 15)
  const rl = (await import('node:readline')).createInterface({ input: process.stdin, crlfDelay: Infinity })
  let cur = null; let bars = []
  const flush = () => {
    if (!bars.length) return
    const withKeys = bars.map(b => ({ ...b, _v4keys: v4Fired(V4_ALL_GROUPS, b, 1).map(s => s.key) }))
    const t = withRowSeq(withTurnCount(withKeys))
    const out = []
    for (const b of t.slice(-keep)) {
      const rs = {}; for (const r of ROW_ORDER) rs[r] = b.rs_tiers[r]
      rs.pair = b.rs_tiers.pair
      out.push(JSON.stringify({ t: b.ticker, d: b.date, turn_cand: !!b.turn_cand, turn_n: b.turn_n, rs }))
    }
    process.stdout.write(out.join('\n') + '\n')
  }
  for await (const line of rl) {
    if (!line) continue
    const r = JSON.parse(line)
    if (r.ticker !== cur) { flush(); cur = r.ticker; bars = [] }
    bars.push(r)
  }
  flush()
} else {
  console.error('mode must be deps | compute'); process.exit(2)
}
