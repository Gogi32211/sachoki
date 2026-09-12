"""OPENING_VOLUME_DYNAMICS_V1 — canonical 1D qualification audit (option A: A4 / A5 / A6).

Reads canonical_1d/CURRENT.json (the newest canonical fetch) and:
  A4  cross-timeframe continuity at split events from the PERSISTED split reference:
      canonical 1D close ratio across the execution date vs the stored 1H / 15m 09:30 bar
      ratio (the lower-TF stores are a DIAGNOSTIC here, never a producer of daily prices)
  A5  the exact seam detector that found 398 / 47 / 40 / 124 on the Studio store, re-run
      on the canonical store — required: true unadjusted seams = 0; residual candidates
      are cross-checked against the split reference (detector may FLAG, never adjust)
  A6  duplicate (ticker, session) audit on canonical — must be 0 by construction
  +   divergence census: Studio (deduped by UNIVERSE_PRIORITY_SQL) vs canonical close,
      to quantify how much of the old store the seams actually touched
Writes canonical_1d/AUDIT_<run>.json. No outcome access.
"""
from __future__ import annotations
import os, sys, json, time                                              # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
DATA = os.path.join(os.path.dirname(HERE), "data")
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"
CANON_DIR = os.path.join(FAMILY_DIR, "canonical_1d")
AUDIT_SET = ["TSLA", "AMZN", "GOOGL", "NVDA", "AVGO", "WMT", "CMG", "SMCI", "LRCX", "APH", "CRWD", "MNST", "SFBS", "WLFC", "DD"]
NY = "timezone('America/New_York', timezone('UTC', date))"

SEAM_SQL = """
 WITH d AS (SELECT ticker, session_date AS date, close, volume FROM {src}),
 s AS (SELECT ticker, date, close, lag(close) OVER w AS pc, close/lag(close) OVER w AS pr,
              volume::DOUBLE/nullif(lag(volume) OVER w,0) AS vr FROM d WINDOW w AS (PARTITION BY ticker ORDER BY date)),
 f AS (SELECT *, CASE WHEN pr BETWEEN 1.94 AND 2.06 THEN 2 WHEN pr BETWEEN 2.9 AND 3.1 THEN 3 WHEN pr BETWEEN 3.88 AND 4.12 THEN 4
                      WHEN pr BETWEEN 4.85 AND 5.15 THEN 5 WHEN pr BETWEEN 9.7 AND 10.3 THEN 10 WHEN pr BETWEEN 19.4 AND 20.6 THEN 20
                      WHEN pr BETWEEN 0.485 AND 0.515 THEN 2 WHEN pr BETWEEN 0.323 AND 0.343 THEN 3 WHEN pr BETWEEN 0.243 AND 0.257 THEN 4
                      WHEN pr BETWEEN 0.194 AND 0.206 THEN 5 WHEN pr BETWEEN 0.097 AND 0.103 THEN 10 WHEN pr BETWEEN 0.0485 AND 0.0515 THEN 20 END AS S FROM s)
 SELECT ticker, date, pc, close, pr, vr, S FROM f
 WHERE S IS NOT NULL AND ((pr>1 AND vr BETWEEN 1.0/(2*S) AND 2.0/S) OR (pr<1 AND vr BETWEEN S/2.0 AND 2.0*S))"""


def main():
    import duckdb
    cur = json.load(open(os.path.join(CANON_DIR, "CURRENT.json")))
    canon, splits = cur["canonical_parquet"], cur["splits_parquet"]
    c = duckdb.connect(); c.execute("pragma threads=8")
    c.execute(f"CREATE VIEW canon AS SELECT * FROM read_parquet('{canon}')")
    c.execute(f"CREATE VIEW splits AS SELECT * FROM read_parquet('{splits}')")
    c.execute(f"ATTACH '{os.path.join(DATA, 'studio_analytics.duckdb')}' AS d (READ_ONLY)")
    c.execute(f"ATTACH '{os.path.join(DATA, 'studio_1h.duckdb')}' AS h1 (READ_ONLY)")
    c.execute(f"ATTACH '{os.path.join(DATA, 'studio_15m.duckdb')}' AS m15 (READ_ONLY)")
    out = dict(run_id=cur["run_id"], canonical_sha256_16=cur["canonical_sha256_16"], splits_sha256_16=cur["splits_sha256_16"])

    # ── A6 duplicates ────────────────────────────────────────────────────────
    out["A6_duplicates"] = c.execute("SELECT count(*) - count(DISTINCT (ticker, session_date)) FROM canon").fetchone()[0]

    # ── A5 seam rescan on canonical (+ cross-check against the split reference) ──
    c.execute("CREATE TEMP TABLE seam_c AS " + SEAM_SQL.format(src="canon"))
    n_seam = c.execute("SELECT count(*) FROM seam_c").fetchone()[0]
    xref = c.execute("""SELECT count(*) FROM seam_c s JOIN splits p ON p.ticker=s.ticker
                        AND abs(date_diff('day', CAST(p.execution_date AS DATE), s.date)) <= 1""").fetchone()[0]
    out["A5_seam_rescan_canonical"] = dict(candidates=n_seam, coinciding_with_a_reference_split=xref,
                                           candidates_after_2026_05_22=c.execute("SELECT count(*) FROM seam_c WHERE date > DATE '2026-05-22'").fetchone()[0],
                                           examples=c.execute("SELECT ticker, date, round(pc,2), round(close,2), round(pr,3), round(vr,2), S FROM seam_c ORDER BY date DESC LIMIT 8").fetchall())
    # the same detector on the Studio store (deduped by the documented precedence) for the record
    c.execute("""CREATE TEMP TABLE studio AS
        SELECT ticker, date AS session_date, close, volume FROM (
          SELECT ticker, date, close, volume, row_number() OVER (PARTITION BY ticker, date
                 ORDER BY CASE universe WHEN 'sp500' THEN 1 WHEN 'nasdaq' THEN 2 WHEN 'russell2k' THEN 3 ELSE 9 END) rn
          FROM d.bars WHERE universe<>'index') WHERE rn=1""")
    c.execute("CREATE TEMP TABLE seam_s AS " + SEAM_SQL.format(src="studio"))
    out["A5_seam_scan_studio_for_record"] = dict(candidates=c.execute("SELECT count(*) FROM seam_s").fetchone()[0],
                                                 after_2026_05_22=c.execute("SELECT count(*) FROM seam_s WHERE date > DATE '2026-05-22'").fetchone()[0])

    # ── A4 cross-TF continuity at reference split events (audit set + all events in window) ──
    rows = []
    for tk, ex, f, t in c.execute("SELECT ticker, execution_date, split_from, split_to FROM splits WHERE ticker IN (" +
                                  ",".join(f"'{x}'" for x in AUDIT_SET) + ") AND CAST(execution_date AS DATE) >= DATE '2021-09-07' ORDER BY ticker, execution_date").fetchall():
        r1d = c.execute(f"""SELECT close FROM canon WHERE ticker='{tk}' AND session_date < DATE '{ex}' ORDER BY session_date DESC LIMIT 1""").fetchone()
        r1d2 = c.execute(f"""SELECT close FROM canon WHERE ticker='{tk}' AND session_date >= DATE '{ex}' ORDER BY session_date LIMIT 1""").fetchone()
        rat = round(r1d2[0] / r1d[0], 3) if r1d and r1d2 else None
        def ltf(store):
            a = c.execute(f"""SELECT close FROM {store}.bars WHERE ticker='{tk}' AND strftime({NY},'%H:%M')='09:30'
                              AND CAST({NY} AS DATE) < DATE '{ex}' ORDER BY date DESC LIMIT 1""").fetchone()
            b = c.execute(f"""SELECT close FROM {store}.bars WHERE ticker='{tk}' AND strftime({NY},'%H:%M')='09:30'
                              AND CAST({NY} AS DATE) >= DATE '{ex}' ORDER BY date LIMIT 1""").fetchone()
            return round(b[0] / a[0], 3) if a and b else None
        rows.append(dict(ticker=tk, execution_date=str(ex), split=f"{f}:{t}", canonical_1d_ratio=rat, h1_ratio=ltf("h1"), m15_ratio=ltf("m15"),
                         continuity="OK" if rat is not None and 0.85 <= rat <= 1.18 else ("NO_DATA" if rat is None else "BREAK")))
    out["A4_cross_tf_continuity"] = rows
    out["A4_summary"] = dict(events=len(rows), ok=sum(r["continuity"] == "OK" for r in rows), breaks=sum(r["continuity"] == "BREAK" for r in rows),
                             no_data=sum(r["continuity"] == "NO_DATA" for r in rows))

    # ── divergence census: Studio vs canonical (how much did the seams touch?) ──
    out["divergence_studio_vs_canonical"] = dict(zip(
        ["pairs_compared", "close_within_0_1pct", "close_diff_gt_1pct", "close_diff_gt_25pct", "tickers_with_any_gt_1pct"],
        c.execute("""SELECT count(*), count(*) FILTER (WHERE abs(s.close/c.close-1) <= 0.001), count(*) FILTER (WHERE abs(s.close/c.close-1) > 0.01),
                            count(*) FILTER (WHERE abs(s.close/c.close-1) > 0.25), count(DISTINCT ticker) FILTER (WHERE abs(s.close/c.close-1) > 0.01)
                     FROM studio s JOIN canon c USING (ticker, session_date)""").fetchone()))
    out["coverage"] = dict(zip(["canonical_rows", "canonical_tickers", "canonical_first", "canonical_last", "studio_rows_in_canonical_window"],
                               c.execute("""SELECT (SELECT count(*) FROM canon), (SELECT count(DISTINCT ticker) FROM canon), (SELECT min(session_date) FROM canon),
                                                   (SELECT max(session_date) FROM canon), (SELECT count(*) FROM studio WHERE session_date >= (SELECT min(session_date) FROM canon))""").fetchone()))
    p = os.path.join(CANON_DIR, f"AUDIT_{cur['run_id']}.json")
    json.dump(out, open(p, "w"), indent=1, default=str)
    print(json.dumps({k: out[k] for k in ("A6_duplicates", "A5_seam_rescan_canonical", "A5_seam_scan_studio_for_record", "A4_summary", "divergence_studio_vs_canonical", "coverage")}, indent=1, default=str))
    print("A4 detail:"); [print("  ", r) for r in rows]
    print("written ->", p)


if __name__ == "__main__":
    main()
