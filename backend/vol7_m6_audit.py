"""VOL7 M6 — DATA-QUALITY AUDIT ONLY. No setup, no veto, no build, no outcome yet.

LADDER_V1 measured M6 (volume >= 5x the 20-day median) at -11.97 MINE / -12.11 VERIFY day-clustered.
A number that large has to be shown to be market behaviour and not a split / corporate-action /
bad-volume artifact before anything is built on it. This pass only characterises the population.
"""
from __future__ import annotations
import duckdb

DB = "/Users/sachoki/Desktop/sachoki-desktop/data/studio_analytics.duckdb"
con = duckdb.connect(DB, read_only=True)
con.execute("pragma threads=8")

# the exact frame vol7_build was fed: d1.bars, deduped by universe, no index
con.execute("""CREATE TEMP TABLE d1 AS
  SELECT ticker, CAST(date AS DATE) AS date, open, high, low, close, volume
  FROM bars WHERE universe <> 'index'
  QUALIFY row_number() OVER (PARTITION BY ticker, date ORDER BY universe) = 1""")
con.execute("""CREATE TEMP TABLE w AS
  SELECT *,
    lag(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close,
    lag(volume) OVER (PARTITION BY ticker ORDER BY date) AS prev_vol,
    -- the 20-bar window vol7 uses (pandas rolling includes the current bar)
    sum(CASE WHEN volume IS NULL OR volume = 0 THEN 1 ELSE 0 END)
        OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS zero_in_20,
    count(*) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS n_in_20
  FROM d1""")
con.execute("""CREATE TEMP TABLE m6 AS
  SELECT v.ticker, v.date, v.ratio, w.volume, w.close, w.prev_close, w.prev_vol,
         w.zero_in_20, w.n_in_20,
         w.volume / NULLIF(v.ratio, 0)                       AS implied_med,
         w.close / NULLIF(w.prev_close, 0)                   AS px_rel
  FROM read_parquet('data/vol7_signals.parquet') v
  JOIN w ON w.ticker = v.ticker AND w.date = CAST(v.date AS DATE)
  WHERE v.mr = 6""")
n = con.execute("SELECT COUNT(*) FROM m6").fetchone()[0]
L = lambda c="─": print(c * 84)

L("═"); print(f"VOL7 M6 population: {n:,} rows joined to their own bars"); L("═")

print("\n1 · RATIO — is this even a plausible volume spike?")
print(con.execute("""SELECT CASE WHEN ratio < 10 THEN 'a   5-10x' WHEN ratio < 20 THEN 'b  10-20x'
       WHEN ratio < 50 THEN 'c  20-50x' WHEN ratio < 100 THEN 'd 50-100x'
       WHEN ratio < 1000 THEN 'e 100-1000x' ELSE 'f  >1000x' END bucket,
       COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) pct,
       ROUND(MEDIAN(implied_med),0) med_vol_20d, ROUND(MEDIAN(volume),0) vol_on_day
       FROM m6 GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))

print("\n2 · THE DENOMINATOR — a tiny 20-day median manufactures the ratio")
print(con.execute("""SELECT CASE WHEN implied_med < 10 THEN 'a  < 10 shares' WHEN implied_med < 100 THEN 'b  < 100'
       WHEN implied_med < 1000 THEN 'c  < 1k' WHEN implied_med < 10000 THEN 'd  < 10k'
       WHEN implied_med < 100000 THEN 'e  < 100k' ELSE 'f  >= 100k' END med_bucket,
       COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) pct, ROUND(MEDIAN(ratio),1) med_ratio
       FROM m6 GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))

print("\n3 · ZERO-VOLUME BARS inside the 20-day window (nan_to_num turns missing into 0)")
print(con.execute("""SELECT CASE WHEN zero_in_20 = 0 THEN 'a none' WHEN zero_in_20 <= 2 THEN 'b 1-2'
       WHEN zero_in_20 <= 5 THEN 'c 3-5' WHEN zero_in_20 <= 10 THEN 'd 6-10' ELSE 'e 11+' END z,
       COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) pct, ROUND(MEDIAN(ratio),1) med_ratio
       FROM m6 GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))

print("\n4 · PRICE DISCONTINUITY on the same bar (split / corporate action proxy)")
print(con.execute("""SELECT CASE WHEN px_rel IS NULL THEN 'z no prior bar'
       WHEN px_rel BETWEEN 0.67 AND 1.5 THEN 'a normal 0.67-1.5x'
       WHEN px_rel BETWEEN 0.45 AND 0.67 THEN 'b  ~2:1 split-like'
       WHEN px_rel BETWEEN 0.28 AND 0.45 THEN 'c  ~3:1 split-like'
       WHEN px_rel < 0.28 THEN 'd  >3.5:1 drop'
       ELSE 'e  jump > 1.5x' END px, COUNT(*) n,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) pct, ROUND(MEDIAN(ratio),1) med_ratio
       FROM m6 GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))

print("\n5 · WARM-UP — is the 20-bar window even full?")
print(con.execute("""SELECT CASE WHEN n_in_20 < 20 THEN 'a window NOT full' ELSE 'b full 20' END wu,
       COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) pct, ROUND(MEDIAN(ratio),1) med_ratio
       FROM m6 GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))

print("\n6 · CONCENTRATION")
print(con.execute("""SELECT COUNT(DISTINCT ticker) AS tickers, COUNT(DISTINCT date) AS sessions FROM m6""").fetchdf().to_string(index=False))
print(con.execute("""SELECT ROUND(100.0*MAX(c)/SUM(c),2) AS top_ticker_pct,
       ROUND(100.0*SUM(c) FILTER (WHERE rn <= 10)/SUM(c),1) AS top10_pct
       FROM (SELECT ticker, COUNT(*) c, row_number() OVER (ORDER BY COUNT(*) DESC) rn FROM m6 GROUP BY 1)""").fetchdf().to_string(index=False))
print("  worst offenders:")
print(con.execute("""SELECT ticker, COUNT(*) n, ROUND(MEDIAN(ratio),0) med_ratio, ROUND(MAX(ratio),0) max_ratio,
       ROUND(MEDIAN(implied_med),0) med_vol FROM m6 GROUP BY 1 ORDER BY n DESC LIMIT 8""").fetchdf().to_string(index=False))

print("\n7 · the 10 most extreme ratios — eyeball them")
print(con.execute("""SELECT ticker, date, ROUND(ratio,0) AS ratio, volume, ROUND(implied_med,1) AS med20,
       prev_vol, ROUND(close,2) AS px, ROUND(prev_close,2) AS px_prev, ROUND(px_rel,3) AS px_rel, zero_in_20
       FROM m6 ORDER BY ratio DESC LIMIT 10""").fetchdf().to_string(index=False))

print("\n8 · PRICE BAND — where does M6 actually live? (the book's own segmentation)")
print(con.execute("""SELECT CASE WHEN close < 2 THEN 'a  < $2' WHEN close < 8 THEN 'b  $2-8'
       WHEN close < 21 THEN 'c  $8-21' WHEN close < 89 THEN 'd  $21-89'
       WHEN close < 377 THEN 'e  $89-377' ELSE 'f  >= $377' END AS px_band,
       COUNT(*) AS n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) AS pct,
       ROUND(MEDIAN(ratio),1) AS med_ratio, ROUND(MEDIAN(close*volume),0) AS med_dollar_vol
       FROM m6 GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))

print("\n9 · DOLLAR VOLUME on the M6 bar itself — is it tradeable at all?")
print(con.execute("""SELECT CASE WHEN close*volume < 1e5 THEN 'a  < $100k' WHEN close*volume < 1e6 THEN 'b  < $1M'
       WHEN close*volume < 3e6 THEN 'c  < $3M' WHEN close*volume < 1e7 THEN 'd  < $10M' ELSE 'e  >= $10M' END AS dv,
       COUNT(*) AS n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) AS pct, ROUND(MEDIAN(ratio),1) AS med_ratio
       FROM m6 GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))

print("\n10 · CONTAMINATION SUMMARY — rows a careful person would not call a volume spike")
print(con.execute("""SELECT
  COUNT(*) AS m6_total,
  SUM(CASE WHEN px_rel IS NOT NULL AND (px_rel < 0.67 OR px_rel > 1.5) THEN 1 ELSE 0 END) AS px_discontinuity,
  SUM(CASE WHEN implied_med < 1000 THEN 1 ELSE 0 END) AS median_under_1k_shares,
  SUM(CASE WHEN zero_in_20 > 0 THEN 1 ELSE 0 END) AS zero_vol_in_window,
  SUM(CASE WHEN ratio > 100 THEN 1 ELSE 0 END) AS ratio_over_100x,
  SUM(CASE WHEN (px_rel IS NOT NULL AND (px_rel < 0.67 OR px_rel > 1.5))
             OR implied_med < 1000 OR zero_in_20 > 0 OR ratio > 100 THEN 1 ELSE 0 END) AS any_flag
  FROM m6""").fetchdf().to_string(index=False))
