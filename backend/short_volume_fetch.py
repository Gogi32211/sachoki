"""Download FINRA DAILY short volume (via Massive) for the board's tickers → parquet.

Phase 2. The bi-monthly short-interest layer was VETOed (crowded shorts are a suppressor,
project_short_interest_veto). The daily file is a different animal: 1-day lag instead of 12,
and it measures FLOW (are shorts selling or covering today?) rather than the standing book.

On CAR the flow told a story the standing book could not: short_volume_ratio sat at 75-87%
through the whole February-March base, then COLLAPSED to 57.0% / 54.0% on the two blow-off
days — that drop is shorts buying. Whether that is tradeable is what phase 2 tests.

POINT-IN-TIME: FINRA posts the daily file after the close. A bar may therefore only use the
PREVIOUS session's ratio; the study shifts by one bar. Same-day use would be lookahead.

One request per trading day returns the whole tape (~15k tickers, limit=50000), so this is
fetched by date and filtered to the frame's tickers. Writes data/short_volume.parquet.
"""
import os, sys, time
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pandas as pd
import requests
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
load_dotenv(os.path.join(HERE, ".env"))
KEY = os.environ.get("MASSIVE_API_KEY", "")
BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
OUT = os.path.join(ROOT, "data", "short_volume.parquet")
START, WORKERS = "2020-11-01", 6          # 2 months of warmup before the 2021 test window
KEEP = ["ticker", "date", "short_volume_ratio", "total_volume", "short_volume"]

if not KEY:
    sys.exit("MASSIVE_API_KEY missing")

# ── which tickers and which sessions ───────────────────────────────────────────
con = duckdb.connect(os.path.join(ROOT, "data", "studio_analytics.duckdb"), read_only=True)
tk = con.execute(
    "SELECT DISTINCT ticker FROM bars WHERE date >= '2020-11-01' "
    "AND close*volume > 1000000").fetchdf()["ticker"].tolist()
days = con.execute(
    f"SELECT DISTINCT CAST(date AS DATE) d FROM bars WHERE date >= '{START}' ORDER BY d"
).fetchdf()["d"].astype(str).tolist()
con.close()
TSET = set(tk)
print(f"{len(TSET):,} tickers · {len(days):,} sessions ({days[0]} → {days[-1]})", flush=True)

sess = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=WORKERS * 2,
                                        pool_maxsize=WORKERS * 2)
sess.mount("https://", adapter)


def one_day(d):
    url = f"{BASE}/stocks/v1/short-volume"
    params = {"date": d, "limit": 50000, "apiKey": KEY}
    out = []
    for attempt in range(5):
        try:
            r = sess.get(url, params=params, timeout=90)
            if r.status_code == 429:
                time.sleep(3 + attempt * 4); continue
            r.raise_for_status()
            j = r.json()
            out = [x for x in (j.get("results") or []) if x.get("ticker") in TSET]
            # a full session fits in one page at limit=50000, but never assume it
            nxt = j.get("next_url")
            while nxt:
                r2 = sess.get(nxt, params={"apiKey": KEY}, timeout=90)
                r2.raise_for_status()
                j2 = r2.json()
                out += [x for x in (j2.get("results") or []) if x.get("ticker") in TSET]
                nxt = j2.get("next_url")
            return d, out, None
        except Exception as e:
            if attempt == 4:
                return d, [], repr(e)
            time.sleep(3 + attempt * 4)
    return d, [], "exhausted"


rows, fails, t0, done = [], [], time.time(), 0
with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    for d, res, err in ex.map(one_day, days):
        done += 1
        if err:
            fails.append((d, err))
        rows.extend({k: x.get(k) for k in KEEP} for x in res)
        if done % 100 == 0:
            print(f"  {done:>5d}/{len(days)}  rows {len(rows):>10,}  "
                  f"fails {len(fails)}  {time.time()-t0:.0f}s", flush=True)

print(f"\nfetched {len(rows):,} rows · {len(fails)} failed sessions · "
      f"{time.time()-t0:.0f}s", flush=True)
if fails:
    # never a silent gap: a missing session shifts every rolling window built on top of it
    print("  FAILED SESSIONS (rolling stats near these dates are unreliable):", flush=True)
    for d, e in fails[:20]:
        print(f"    {d}  {e}", flush=True)

df = pd.DataFrame(rows)
if df.empty:
    sys.exit("no rows")
df["date"] = pd.to_datetime(df["date"]).astype("datetime64[ns]")
df["short_volume_ratio"] = pd.to_numeric(df["short_volume_ratio"], errors="coerce")
df = df.dropna(subset=["short_volume_ratio"]).sort_values(["ticker", "date"])
df = df.drop_duplicates(subset=["ticker", "date"])

g = df.groupby("ticker", sort=False)["short_volume_ratio"]
df["svr_5"] = g.transform(lambda s: s.rolling(5, min_periods=3).mean())
df["svr_60"] = g.transform(lambda s: s.rolling(60, min_periods=30).mean())
df["svr_60sd"] = g.transform(lambda s: s.rolling(60, min_periods=30).std())
df["svr_z"] = (df["short_volume_ratio"] - df["svr_60"]) / df["svr_60sd"].replace(0, pd.NA)
# the CAR signature: the 5-day flow drops hard below its own 60-day norm = covering
df["svr_drop"] = df["svr_5"] - df["svr_60"]

df.to_parquet(OUT, index=False)
print(f"\nwrote {OUT}  ({len(df):,} rows · {df.ticker.nunique():,} tickers · "
      f"{df.date.min().date()} → {df.date.max().date()})", flush=True)
print("\nshort_volume_ratio distribution:", flush=True)
print(df["short_volume_ratio"].describe(percentiles=[.05, .25, .5, .75, .95]).round(2).to_string(),
      flush=True)
print("\nDONE", flush=True)
