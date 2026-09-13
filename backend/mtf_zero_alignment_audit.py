"""MTF_ZERO — CAUSAL-ALIGNMENT AUDIT. Read-only, NO outcome access, no evaluation.

The only question: can `mtf_echo` be reconstructed honestly on the 1D fire bar from the 4H/1H
stores, without intraday leakage? Production definition (studio/ultra_db_scan.py:807-860):

    intraday REV on (ticker, calendar day) :=  EXISTS a bar that day with
        close >= 5, m5 = MIN(rsi_14) over the 5 strictly-preceding bars < 38,
        rsi_14 BETWEEN 30 AND 55, close > LAG(close), rsi_14 > LAG(rsi_14)
    mtf_echo := rev_buy AND (REV_4h OR REV_1h) on d0 (the 1D bar's own date) OR d1 (the prior one)

The live query is capped at `max(date) - INTERVAL 14 DAY`, so NO history exists: any study has to
recompute the condition over the whole span. That is why MTF_ZERO is NOT EVALUATED, not null.
"""
from __future__ import annotations
import duckdb

L = lambda c="─": print(c * 82)
d1 = duckdb.connect("data/studio_analytics.duckdb", read_only=True)

for tf in ("4h", "1h"):
    con = duckdb.connect(f"data/studio_{tf}.duckdb", read_only=True)
    L("═"); print(f"  {tf.upper()}"); L("═")

    # NOTE: do NOT bucket DST as "months 4-10 vs 11-3" — March and November are transitional and
    # that bucketing makes EST look like it starts at 13:30 UTC. Read it month by month instead.
    print("\nA · SESSION ALIGNMENT — the session's most common first on-grid bar, by month (UTC)")
    print(con.execute("""
        SELECT month(CAST(date AS DATE)) AS mon,
               mode(strftime(CAST(date AS TIMESTAMP),'%H:%M')) AS modal_bar_utc
        FROM bars WHERE strftime(CAST(date AS TIMESTAMP),'%M') = '30'
        GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))

    print("\nA2 · time-of-day range — does the US session ever cross midnight UTC?")
    print(con.execute("""
        SELECT MIN(strftime(CAST(date AS TIMESTAMP),'%H:%M')) AS earliest_tod,
               MAX(strftime(CAST(date AS TIMESTAMP),'%H:%M')) AS latest_tod
        FROM bars""").fetchdf().to_string(index=False))

    print("\nB · bars per ticker-session (is the grid what we think?)")
    print(con.execute("""
        SELECT bars_in_day, COUNT(*) AS ticker_sessions,
               ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) AS pct
        FROM (SELECT ticker, CAST(date AS DATE) d, COUNT(*) bars_in_day
              FROM bars GROUP BY 1,2)
        GROUP BY 1 ORDER BY ticker_sessions DESC LIMIT 8""").fetchdf().to_string(index=False))

    print("\nC · OFF-GRID bars (not on the regular hh:30 boundary)")
    print(con.execute("""
        SELECT CASE WHEN strftime(CAST(date AS TIMESTAMP),'%M') = '30' THEN 'on-grid :30'
                    ELSE 'OFF-GRID' END AS g, COUNT(*) AS n,
               ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) AS pct
        FROM bars GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))

    print("\nD · rsi_14 WARM-UP — how many bars into a ticker's series before rsi_14 exists?")
    print(con.execute("""
        SELECT CASE WHEN pos < 14 THEN 'a  bar 1-13' WHEN pos < 84 THEN 'b  bar 14-83 (Wilder unsettled)'
                    ELSE 'c  bar 84+' END AS phase,
               COUNT(*) AS n, SUM(CASE WHEN rsi_14 IS NULL THEN 1 ELSE 0 END) AS rsi_null,
               ROUND(100.0*SUM(CASE WHEN rsi_14 IS NULL THEN 1 ELSE 0 END)/COUNT(*),2) AS pct_null
        FROM (SELECT rsi_14, row_number() OVER (PARTITION BY ticker ORDER BY date) - 1 AS pos FROM bars)
        GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))
    con.close()

L("═"); print("  COVERAGE against the 1D universe"); L("═")
for tf in ("4h", "1h"):
    d1.execute(f"ATTACH 'data/studio_{tf}.duckdb' AS s{tf} (READ_ONLY)")
print("\nE · ticker-day coverage: 1D bars that have ANY intraday bar the same calendar day")
print(d1.execute("""
    WITH d AS (SELECT DISTINCT ticker, CAST(date AS DATE) d FROM bars WHERE universe <> 'index'),
         h4 AS (SELECT DISTINCT ticker, CAST(date AS DATE) d FROM s4h.bars),
         h1 AS (SELECT DISTINCT ticker, CAST(date AS DATE) d FROM s1h.bars)
    SELECT COUNT(*) AS d1_ticker_days,
           SUM(CASE WHEN h4.ticker IS NOT NULL THEN 1 ELSE 0 END) AS have_4h,
           SUM(CASE WHEN h1.ticker IS NOT NULL THEN 1 ELSE 0 END) AS have_1h,
           ROUND(100.0*SUM(CASE WHEN h4.ticker IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),2) AS pct_4h,
           ROUND(100.0*SUM(CASE WHEN h1.ticker IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),2) AS pct_1h
    FROM d LEFT JOIN h4 USING (ticker, d) LEFT JOIN h1 USING (ticker, d)""").fetchdf().to_string(index=False))

print("\nF · LIQUIDITY BIAS of the missing (median dollar volume, 1D)")
print(d1.execute("""
    WITH d AS (SELECT ticker, CAST(date AS DATE) d, close*volume AS dv FROM bars WHERE universe <> 'index'
               QUALIFY row_number() OVER (PARTITION BY ticker, CAST(date AS DATE) ORDER BY universe)=1),
         h4 AS (SELECT DISTINCT ticker, CAST(date AS DATE) d FROM s4h.bars)
    SELECT CASE WHEN h4.ticker IS NOT NULL THEN 'covered by 4h' ELSE 'MISSING' END AS cov,
           COUNT(*) AS ticker_days, COUNT(DISTINCT d.ticker) AS tickers,
           ROUND(MEDIAN(dv),0) AS med_dollar_vol
    FROM d LEFT JOIN h4 USING (ticker, d) GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))

print("\nG · the price floor — close >= 5 removes how much of the 1D universe?")
print(d1.execute("""
    SELECT CASE WHEN close >= 5 THEN 'close >= $5' ELSE 'below $5 — REV can never fire' END AS f,
           COUNT(*) AS n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) AS pct
    FROM bars WHERE universe <> 'index' GROUP BY 1 ORDER BY 1""").fetchdf().to_string(index=False))
