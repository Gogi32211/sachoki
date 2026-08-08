"""Download FINRA short interest (via Massive) for the whole universe → parquet.

Phase 1 of the squeeze-fuel study. Pulls short_interest / avg_daily_volume / days_to_cover
for every ticker, bi-monthly, 2020-06 → today.

POINT-IN-TIME: FINRA publishes ~8 BUSINESS days after the settlement date. The
`settlement_date` in the payload is NOT when a trader could have known it. We therefore stamp
every row with `known_from = settlement_date + 12 calendar days` (conservative) and the study
must join on known_from, never on settlement_date. Joining on settlement_date would be a
lookahead bug of exactly the kind that killed the swing_type feature.

Writes data/short_interest.parquet. Does NOT touch any DuckDB — no writer conflict.
"""
import os, sys, time
import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(HERE, ".env"))
KEY = os.environ.get("MASSIVE_API_KEY", "")
BASE = os.environ.get("MASSIVE_BASE", "https://api.massive.com")
OUT = os.path.join(os.path.dirname(HERE), "data", "short_interest.parquet")
START = "2020-06-01"

if not KEY:
    sys.exit("MASSIVE_API_KEY missing")

sess = requests.Session()
ENDPOINT = f"{BASE}/stocks/v1/short-interest"


def get(url, params):
    """one request with retry; returns the parsed body or {}"""
    for attempt in range(5):
        try:
            r = sess.get(url, params=params, timeout=40)
            if r.status_code == 429:
                time.sleep(2 + attempt * 3); continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 4:
                print(f"    giving up after 5 tries: {e}", flush=True)
                return {}
            time.sleep(2 + attempt * 3)
    return {}


# ── step 1: discover the settlement dates (one cheap sweep) ────────────────────
# NOTE: paginating a date-RANGE query silently advances the cursor to the next date
# after 1000 rows, so a range sweep returns only the alphabetically-first 1000 tickers
# per date (that bug cost the first run: CAR itself was missing). Dates first, then
# paginate WITHIN each date.
dates, url, params = [], ENDPOINT, {"settlement_date.gte": START,
                                    "sort": "settlement_date.asc", "limit": 1000,
                                    "apiKey": KEY}
while True:
    j = get(url, params)
    res = j.get("results") or []
    dates.extend(r["settlement_date"] for r in res)
    nxt = j.get("next_url")
    if not nxt or not res:
        break
    url, params = nxt, {"apiKey": KEY}
dates = sorted(set(dates))
print(f"settlement dates found: {len(dates)}  ({dates[0]} → {dates[-1]})", flush=True)

# ── step 2: full ticker list per date ──────────────────────────────────────────
rows, t0 = [], time.time()
for i, d in enumerate(dates, 1):
    url, params, n0 = ENDPOINT, {"settlement_date": d, "limit": 1000, "apiKey": KEY}, len(rows)
    while True:
        j = get(url, params)
        res = j.get("results") or []
        rows.extend(res)
        nxt = j.get("next_url")
        if not nxt or not res:
            break
        url, params = nxt, {"apiKey": KEY}
    if i % 20 == 0 or i == len(dates):
        print(f"  {i:>4d}/{len(dates)}  {d}  +{len(rows)-n0:>6,}  total {len(rows):>9,}  "
              f"{time.time()-t0:.0f}s", flush=True)
pages = len(dates)

print(f"\nfetched {len(rows):,} rows in {pages} pages, {time.time()-t0:.0f}s", flush=True)
df = pd.DataFrame(rows)
if df.empty:
    sys.exit("no rows returned")

df = df.drop_duplicates(subset=["ticker", "settlement_date"])
# days_to_cover is capped at 999.99 whenever avg_daily_volume rounds to ~0 — a sentinel,
# not a number. 15.5% of raw rows carry it. Left in, every "DTC>=10" cell would be mostly
# untradeable illiquid names rather than crowded shorts.
df["days_to_cover"] = pd.to_numeric(df["days_to_cover"], errors="coerce")
n_cap = int((df["days_to_cover"] >= 999).sum())
df.loc[df["days_to_cover"] >= 999, "days_to_cover"] = np.nan
df.loc[df["avg_daily_volume"].fillna(0) <= 0, "days_to_cover"] = np.nan
print(f"masked {n_cap:,} sentinel days_to_cover values (>=999.99)", flush=True)

df["settlement_date"] = pd.to_datetime(df["settlement_date"])
# the only date a backtest is allowed to use
df["known_from"] = (df["settlement_date"] + pd.Timedelta(days=12)).dt.strftime("%Y-%m-%d")
df["settlement_date"] = df["settlement_date"].dt.strftime("%Y-%m-%d")
df = df.sort_values(["ticker", "settlement_date"]).reset_index(drop=True)

# momentum of the short book: is it building or covering?
g = df.groupby("ticker")["short_interest"]
df["si_prev"] = g.shift(1)
df["si_chg_pct"] = (df["short_interest"] / df["si_prev"] - 1) * 100
df["si_up_2"] = (df["si_chg_pct"] > 0) & (g.shift(1) > g.shift(2))

df.to_parquet(OUT, index=False)
print(f"wrote {OUT}  ({len(df):,} rows · {df.ticker.nunique():,} tickers · "
      f"{df.settlement_date.min()} → {df.settlement_date.max()})", flush=True)
print("\ndays_to_cover distribution:", flush=True)
print(df["days_to_cover"].describe(percentiles=[.5, .75, .9, .95, .99]).round(2).to_string(),
      flush=True)
print("\nDONE", flush=True)
