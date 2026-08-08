"""earnings_feed.py — SEC EDGAR report-date feed for the brain (built 2026-08-03).

refresh(): per-ticker report-event dates (10-Q/10-K filings + 8-K item 2.02), 2020+ →
data/earnings_dates.json {TICKER: [YYYY-MM-DD, ...]}. Resumable (skips tickers already
present), incremental save every 200, ~8 req/s with a proper User-Agent per SEC rules.
Runs weekly from update_db.sh (Sundays) — the nightly deletes the file first for a clean
re-pull, and a failure only leaves week-old dates (non-fatal by design).

load(): read-only accessor for brain/live.py's `days_since_report` state — a filing is
public the moment it exists, so this input is causal at decision time.

IMPORT-SAFE: network happens only inside refresh()/__main__ — live.py imports load() on
every decision run and must never trigger a 10-minute EDGAR pull.

Study that built this (2026-08-03, /tmp/earnings_prox.py): post-report ≤5d edge fires
underperform their complement by −1.17pp, sign 6/6 years, n=5,731 → gate_earnings
(report-only). The PRE side was NOT built: cadence prediction error median 33d = noise.
"""
import json
import time

OUT = '/Users/sachoki/Desktop/sachoki-desktop/data/earnings_dates.json'
UA = {'User-Agent': 'sachoki-desktop research demetrashviligoga@gmail.com'}


def load() -> dict:
    """{TICKER: [iso report dates]} — empty dict if the feed has never run."""
    try:
        return json.load(open(OUT))
    except Exception:
        return {}


def refresh() -> dict:
    """Full (re)pull for the active universe (price>=5, traded in the last 60 days)."""
    import requests
    import duckdb
    s = requests.Session(); s.headers.update(UA)

    con = duckdb.connect('/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb',
                         read_only=True)
    tks = [r[0] for r in con.execute("""
        SELECT DISTINCT ticker FROM bars WHERE universe<>'index'
        AND date >= current_date - INTERVAL 60 DAY AND close >= 5""").fetchall()]
    con.close()
    print(f"tickers: {len(tks)}", flush=True)

    r = s.get('https://www.sec.gov/files/company_tickers.json', timeout=30)
    r.raise_for_status()
    cmap = {str(v['ticker']).upper(): int(v['cik_str']) for v in r.json().values()}

    done = load()
    if done:
        print(f"resume: {len(done)} already fetched", flush=True)
    todo = [t for t in tks if t not in done]
    nof = 0
    for i, t in enumerate(todo):
        cik = cmap.get(t)
        if cik is None:
            done[t] = []; nof += 1; continue
        try:
            r = s.get(f'https://data.sec.gov/submissions/CIK{cik:010d}.json', timeout=30)
            if r.status_code != 200:
                done[t] = []; continue
            rec = r.json().get('filings', {}).get('recent', {})
            forms = rec.get('form', []); dates = rec.get('filingDate', [])
            items = rec.get('items', []) or [''] * len(forms)
            ds = set()
            for f, d, it in zip(forms, dates, items):
                if d < '2020-01-01':
                    continue
                if f in ('10-Q', '10-K') or (f == '8-K' and '2.02' in (it or '')):
                    ds.add(d)
            done[t] = sorted(ds)
        except Exception:
            done[t] = []
        time.sleep(0.12)
        if (i + 1) % 200 == 0:
            json.dump(done, open(OUT, 'w'))
            print(f"{i + 1}/{len(todo)} saved", flush=True)
    json.dump(done, open(OUT, 'w'))
    have = sum(1 for v in done.values() if v)
    print(f"DONE: {len(done)} tickers, {have} with report dates, {nof} no CIK", flush=True)
    return {"tickers": len(done), "with_dates": have, "no_cik": nof}


if __name__ == "__main__":
    refresh()
