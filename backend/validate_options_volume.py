"""
validate_options_volume.py — SURROGATE test: does options VOLUME alone (no OI/IV/greeks,
which Massive gates at 403) carry any forward edge on the underlying?

Massive gives us: option-contract reference (incl. expired) + per-contract daily aggregates
(OHLCV). NOT: snapshot greeks/IV/open-interest. So we can build daily CALL vs PUT volume
per underlying, but NOT Max Pain / Gamma Flip / EM (those need OI + IV).

Signals tested (volume-only):
    OPT_RVOL_HI   total option volume > 2× its own 20d baseline  (unusual activity, any dir)
    PC_HI         put/call volume ratio > 1.5                    (put-heavy)
    PC_LO         put/call volume ratio < 0.60                   (call-heavy)
    CALL_SURGE    call volume > 2× its 20d base                  (bullish-flow proxy)
Forward: underlying close→close +5d / +10d (from our Studio DB). Compared vs each ticker's
own all-day baseline. POC on liquid optionable names — small sample, HONEST caveats.
READ-ONLY. Massive only.
"""
import os, sys, time, glob, plistlib
import numpy as np
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, os.path.dirname(__file__))

BASE = "https://api.massive.com"
START, END = "2026-03-15", "2026-07-06"     # signal window; stock fwd needs data past signal
EXP_LO, EXP_HI = "2026-03-20", "2026-08-15"  # contracts expiring in/after the window
TICKERS = ["AMD", "MU", "LYFT", "RKLB", "SOFI", "PLTR", "INTC", "F"]
WORKERS = 4          # gentle — the API connect-times-out under heavy fan-out
_PRANGE = {}         # ticker → (lo, hi) close over the window; strike-band filter


def _key():
    k = os.environ.get("MASSIVE_API_KEY", "")
    if k:
        return k
    for p in glob.glob(os.path.expanduser("~/Library/LaunchAgents/com.sachoki.backend*.plist")):
        k = plistlib.load(open(p, "rb")).get("EnvironmentVariables", {}).get("MASSIVE_API_KEY", "") or k
    if k:
        return k
    for envp in (".env", "../.env"):
        if os.path.exists(envp):
            for line in open(envp):
                if line.startswith("MASSIVE_API_KEY"):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


KEY = _key()
S = requests.Session()


def _get(url, params, timeout=15, tries=4):
    """Robust GET: retry with backoff on timeout/5xx/429; NEVER let the URL (with the
    apiKey) surface in a traceback — swallow the exception and return None."""
    for i in range(tries):
        try:
            r = S.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(0.6 * (i + 1))
                continue
            return None                          # 4xx (not-authorized/not-found) → give up
        except requests.RequestException:
            time.sleep(0.6 * (i + 1))
    return None


def _contracts(tk):
    lo, hi = _PRANGE.get(tk, (0, 1e9))
    slo, shi = lo * 0.55, hi * 1.8              # strike band: drop far-OTM (≈0 volume)
    out = []
    for expired in ("true", "false"):
        url = f"{BASE}/v3/reference/options/contracts"
        params = {"underlying_ticker": tk, "expired": expired, "limit": 1000, "apiKey": KEY,
                  "expiration_date.gte": EXP_LO, "expiration_date.lte": EXP_HI}
        for _ in range(8):                       # paginate
            j = _get(url, params, timeout=20)
            if not j:
                break
            for c in j.get("results", []):
                if slo <= c.get("strike_price", 0) <= shi:
                    out.append((c["ticker"], c["contract_type"]))
            nxt = j.get("next_url")
            if not nxt:
                break
            url, params = nxt, {"apiKey": KEY}
    return out


def _contract_daily(args):
    tk, ctype = args
    j = _get(f"{BASE}/v2/aggs/ticker/{tk}/range/1/day/{START}/{END}",
             {"apiKey": KEY, "limit": 200, "adjusted": "true"})
    if not j:
        return ctype, []
    rows = [(pd.Timestamp(b["t"], unit="ms").strftime("%Y-%m-%d"), b.get("v", 0))
            for b in j.get("results", [])]
    return ctype, rows


def _stock_daily(tickers):
    import duckdb
    from studio.paths import ANALYTICS_DB
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    q = ",".join("'" + t + "'" for t in tickers)
    df = a.execute(f"""
        WITH r AS (SELECT ticker, date, close,
                          row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                   FROM bars WHERE ticker IN ({q}))
        SELECT ticker, CAST(date AS VARCHAR)[:10] dstr, close FROM r WHERE rn=1 ORDER BY ticker, date
    """).fetchdf()
    a.close()
    return df


def build_ticker(tk):
    cons = _contracts(tk)
    if not cons:
        return None
    call = {}; put = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for ctype, rows in ex.map(_contract_daily, cons):
            d = call if ctype == "call" else put
            for day, v in rows:
                d[day] = d.get(day, 0) + v
    days = sorted(set(call) | set(put))
    return pd.DataFrame({"ticker": tk, "dstr": days,
                         "call_vol": [call.get(d, 0) for d in days],
                         "put_vol": [put.get(d, 0) for d in days]})


def main():
    t0 = time.time()
    print(f"key={bool(KEY)} · tickers={len(TICKERS)} · window {START}..{END}", flush=True)
    stock = _stock_daily(TICKERS)
    for tk, g in stock.groupby("ticker"):
        _PRANGE[tk] = (float(g.close.min()), float(g.close.max()))
    parts = []
    for tk in TICKERS:
        p = build_ticker(tk)
        n = 0 if p is None else len(p)
        print(f"  {tk:6s} days={n} ({time.time()-t0:.0f}s)", flush=True)
        if p is not None:
            parts.append(p)
    opt = pd.concat(parts, ignore_index=True)
    m = stock.merge(opt, on=["ticker", "dstr"], how="inner").sort_values(["ticker", "dstr"])

    g = m.groupby("ticker", sort=False)
    m["tot"] = m.call_vol + m.put_vol
    m["pc"] = m.put_vol / m.call_vol.replace(0, np.nan)
    m["tot_base"] = g["tot"].transform(lambda s: s.rolling(20, min_periods=10).mean().shift(1))
    m["call_base"] = g["call_vol"].transform(lambda s: s.rolling(20, min_periods=10).mean().shift(1))
    m["rvol"] = m.tot / m.tot_base
    m["call_rvol"] = m.call_vol / m.call_base
    m["fwd5"] = g["close"].transform(lambda s: s.shift(-5) / s - 1) * 100
    m["fwd10"] = g["close"].transform(lambda s: s.shift(-10) / s - 1) * 100
    m = m[m.fwd5.notna() & m.rvol.notna()]

    def slice_(name, mask):
        sub = m[mask]
        if len(sub) < 15:
            print(f"  {name:12s} n={len(sub):>4} (too few)"); return
        print(f"  {name:12s} n={len(sub):>4}  fwd5 mean{sub.fwd5.mean():+5.2f} med{sub.fwd5.median():+5.2f} "
              f"win{(sub.fwd5>0).mean()*100:4.1f}  |  fwd10 mean{sub.fwd10.mean():+5.2f} med{sub.fwd10.median():+5.2f} win{(sub.fwd10>0).mean()*100:4.1f}")

    print(f"\nmerged rows {len(m):,} · {m.ticker.nunique()} tickers · {time.time()-t0:.0f}s\n")
    print("forward underlying return (close→close), by signal vs baseline:\n")
    slice_("BASELINE(all)", m.index >= 0)
    slice_("OPT_RVOL_HI", m.rvol > 2)
    slice_("OPT_RVOL_LO", m.rvol < 0.6)
    slice_("PC_HI(put>1.5)", m.pc > 1.5)
    slice_("PC_LO(call<.6)", m.pc < 0.6)
    slice_("CALL_SURGE", m.call_rvol > 2)
    print(f"\ndone {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
