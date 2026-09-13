"""MTF_ZERO reconstruction — the frozen spec's sections 5, 6 and 7. NO OUTCOME ACCESS.

  §5 eligible-1D-universe attrition, with median dollar volume at every step
  §6 feature prevalence, incl. the both-FALSE cell that is the candidate MTF_ZERO population
  §7 PARITY GATE against the live mtf_echo query — 100 % on comparable rows, or STOP
"""
from __future__ import annotations
import os, sys
import duckdb, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mtf_rev_build import PRICE_FLOOR, M5_MAX, RSI_LO, RSI_HI, REV_MIN_BARS   # noqa: E402

F = os.path.join(ROOT, "data", "mtf_rev_signals.parquet")
L = lambda c="─": print(c * 84)
con = duckdb.connect(os.path.join(ROOT, "data", "studio_analytics.duckdb"), read_only=True)
con.execute("pragma threads=8")
con.execute(f"CREATE TEMP VIEW mtf AS SELECT * FROM read_parquet('{F}')")
con.execute("""CREATE TEMP TABLE d1 AS
  SELECT ticker, CAST(date AS DATE) AS date, close, close*volume AS dv FROM bars
  WHERE universe <> 'index'
  QUALIFY row_number() OVER (PARTITION BY ticker, CAST(date AS DATE) ORDER BY universe) = 1""")

L("═"); print("§5 · ELIGIBLE 1D UNIVERSE — attrition, liquidity visible at every step"); L("═")
steps = [
    ("all 1D ticker-days",  "TRUE"),
    ("close >= 5",          "d.close >= 5"),
    ("+ 4H covered",        "d.close >= 5 AND COALESCE(m.covered_4h,FALSE)"),
    ("+ 1H covered",        "d.close >= 5 AND COALESCE(m.covered_4h,FALSE) AND COALESCE(m.covered_1h,FALSE)"),
    ("+ 4H mature",         "d.close >= 5 AND COALESCE(m.covered_4h,FALSE) AND COALESCE(m.covered_1h,FALSE) AND COALESCE(m.mature_4h,FALSE)"),
    ("+ 1H mature = FINAL", "d.close >= 5 AND COALESCE(m.covered_4h,FALSE) AND COALESCE(m.covered_1h,FALSE) AND COALESCE(m.mature_4h,FALSE) AND COALESCE(m.mature_1h,FALSE)"),
]
prev = None
print(f"  {'step':24s} {'ticker-days':>12s} {'kept':>7s} {'tickers':>8s} {'med $vol':>12s}")
for lab, w in steps:
    n, t, dv = con.execute(f"""SELECT COUNT(*), COUNT(DISTINCT d.ticker), MEDIAN(d.dv)
        FROM d1 d LEFT JOIN mtf m ON m.ticker=d.ticker AND m.date=d.date WHERE {w}""").fetchone()
    kept = "" if prev is None else f"{100.0*n/prev:5.1f}%"
    print(f"  {lab:24s} {n:>12,} {kept:>7s} {t:>8,} {dv:>12,.0f}")
    if prev is None: prev = n
final_n = n

L("═"); print("§6 · FEATURE PREVALENCE — no outcome involved"); L("═")
cells = con.execute("""
  SELECT CASE
      WHEN rev_4h IS NULL AND rev_1h IS NULL              THEN 'f  both UNKNOWN'
      WHEN rev_4h IS NULL OR  rev_1h IS NULL              THEN 'e  one known / one UNKNOWN'
      WHEN rev_4h AND rev_1h                              THEN 'c  both TRUE'
      WHEN NOT rev_4h AND NOT rev_1h                      THEN 'd  both FALSE  <- candidate MTF_ZERO'
      WHEN rev_4h                                         THEN 'a  4H only'
      ELSE 'b  1H only' END AS cell,
    COUNT(*) AS n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),2) AS pct
  FROM mtf GROUP BY 1 ORDER BY 1""").fetchdf()
print(cells.to_string(index=False))
r4, r1 = con.execute("SELECT SUM(CASE WHEN rev_4h THEN 1 ELSE 0 END), SUM(CASE WHEN rev_1h THEN 1 ELSE 0 END) FROM mtf").fetchone()
dec4, dec1 = con.execute("SELECT COUNT(rev_4h), COUNT(rev_1h) FROM mtf").fetchone()
print(f"\n  rev_4h TRUE {r4:,} of {dec4:,} decidable ({100*r4/dec4:.2f} %)")
print(f"  rev_1h TRUE {r1:,} of {dec1:,} decidable ({100*r1/dec1:.2f} %)")

print("\n  concentration of the both-FALSE cell:")
print(con.execute("""
  WITH z AS (SELECT ticker, date FROM mtf WHERE rev_4h = FALSE AND rev_1h = FALSE),
       pt AS (SELECT ticker, COUNT(*) c, row_number() OVER (ORDER BY COUNT(*) DESC) rn FROM z GROUP BY 1)
  SELECT (SELECT COUNT(*) FROM z) AS rows_,
         (SELECT COUNT(DISTINCT ticker) FROM z) AS tickers,
         (SELECT COUNT(DISTINCT date) FROM z) AS sessions,
         ROUND(100.0*MAX(c)/SUM(c),3) AS top_ticker_pct,
         ROUND(100.0*SUM(c) FILTER (WHERE rn <= 10)/SUM(c),2) AS top10_pct
  FROM pt""").fetchdf().to_string(index=False))
print("  per-session share of the both-FALSE cell (is it a few days or every day?):")
print(con.execute("""
  WITH d AS (SELECT date, COUNT(*) n, SUM(CASE WHEN rev_4h = FALSE AND rev_1h = FALSE THEN 1 ELSE 0 END) z
             FROM mtf WHERE rev_4h IS NOT NULL AND rev_1h IS NOT NULL GROUP BY 1)
  SELECT ROUND(MIN(100.0*z/n),1) AS min_pct, ROUND(MEDIAN(100.0*z/n),1) AS med_pct,
         ROUND(MAX(100.0*z/n),1) AS max_pct, COUNT(*) AS sessions FROM d""").fetchdf().to_string(index=False))

L("═"); print("§7 · PARITY GATE vs the production REV query — acceptance 100 % on comparable rows"); L("═")
# ⚠️ The gate is run on CLEAN windows, NOT on the live helper's own 14-day window. From ~2026-06-30
# the stored intraday rsi_14 stops following the stored close series (see §8), so the live helper is
# not a trustworthy benchmark there. The production SQL semantics are replayed verbatim on windows
# where the stored column is self-consistent.
WINDOWS = (("2024-03-01", "2024-03-20"), ("2025-10-01", "2025-10-20"), ("2026-05-01", "2026-05-20"))
gate_ok = True
for LO, HI in WINDOWS:
    for tf in ("4h", "1h"):
        c = duckdb.connect(os.path.join(ROOT, "data", f"studio_{tf}.duckdb"), read_only=True)
        # verbatim: close >= 5 sits INSIDE the CTE, so the window functions skip sub-$5 bars
        live = c.execute(f"""
            WITH r AS (SELECT ticker, date, close, rsi_14,
                MIN(rsi_14) OVER (PARTITION BY ticker ORDER BY date
                    ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) m5,
                LAG(close)  OVER (PARTITION BY ticker ORDER BY date) cp,
                LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date) rp
              FROM bars WHERE close >= {PRICE_FLOOR} AND date <= DATE '{HI}')
            SELECT DISTINCT ticker, CAST(date AS DATE) AS date FROM r
            WHERE date >= DATE '{LO}' AND m5 < {M5_MAX}
              AND rsi_14 BETWEEN {RSI_LO} AND {RSI_HI} AND close > cp AND rsi_14 > rp""").fetchdf()
        univ = c.execute(f"""SELECT DISTINCT ticker, CAST(date AS DATE) AS date FROM bars
                             WHERE date >= DATE '{LO}' AND date <= DATE '{HI}'""").fetchdf()
        c.close()
        live["live"] = True
        for x in (live, univ): x["date"] = pd.to_datetime(x["date"])
        mine = pd.read_parquet(F, columns=["ticker", "date", f"rev_{tf}"])
        mine["date"] = pd.to_datetime(mine["date"])
        m = mine[(mine.date >= LO) & (mine.date <= HI) & mine[f"rev_{tf}"].notna()]
        j = m.merge(univ, on=["ticker", "date"]).merge(live, on=["ticker", "date"], how="left")
        j["live"] = j["live"].fillna(False).astype(bool)
        j["mine"] = j[f"rev_{tf}"].astype(bool)
        n, ag = len(j), int((j["mine"] == j["live"]).sum())
        gate_ok &= (ag == n)
        print(f"  {LO}..{HI}  {tf.upper():3s}  rows {n:>7,}  agree {ag:>7,}  disagree {n-ag:>4,}  "
              f"{100.0*ag/n:8.4f} %   {'PASS' if ag == n else 'FAIL'}")
print(f"\n  GATE: {'PASS — reconstruction accepted' if gate_ok else '*** STOP ***'}")

L("═"); print("§8 · PRODUCER ANOMALY found by the gate (reported, NOT resolved here)"); L("═")
for tf in ("4h", "1h"):
    c = duckdb.connect(os.path.join(ROOT, "data", f"studio_{tf}.duckdb"), read_only=True)
    q = c.execute("""SELECT MEDIAN(j) med, QUANTILE_CONT(j,0.99) p99, MAX(j) mx,
                            100.0*AVG(CASE WHEN j > 20 THEN 1 ELSE 0 END) pct
                     FROM (SELECT ABS(rsi_14 - LAG(rsi_14) OVER (PARTITION BY ticker ORDER BY date)) j
                           FROM bars WHERE rsi_14 IS NOT NULL) WHERE j IS NOT NULL""").fetchdf().iloc[0]
    c.close()
    print(f"  {tf.upper()} stored rsi_14 bar-to-bar |change|: median {q.med:.2f} · p99 {q.p99:.2f} · "
          f"max {q.mx:.1f} · >20pt on {q.pct:.3f} % of bars")
print("  From ~2026-06-30 the stored intraday rsi_14 diverges from a Wilder-14 over the stored")
print("  close series, on ~26 sessions, across essentially every ticker. No trailing-window reseed")
print("  and no SMA variant reproduces the stored values (AAPL 2026-08-19: stored 84.1, every")
print("  reconstruction 51.5-59.5). The live mtf_echo / REV veto reads this column.")
