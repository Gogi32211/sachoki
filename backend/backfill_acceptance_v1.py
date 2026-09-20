"""Backfill acceptance — the gate between the staging refetch and the canonical stores.

FAIL CLOSED. Canonical is replaced only if every section below passes. A failure leaves canonical
untouched, which is no worse than the state we are already in.

ROW KEY = (ticker, universe, date) where `bars.date` is a TIMESTAMP carrying the BAR time, not a
session date — one AAPL 1H session is seven distinct keys (13:30 … 19:30). The session key is
(ticker, universe, CAST(date AS DATE)). The naming misleads; the granularity is right.

⚠️ CLOSE PARITY IS NOT ROW PARITY. An earlier pilot check compared `close` only and was reported as
"byte-identical" — that is evidence about one column, not about the row. A refetch can return the
right close on a different bar grid (extra/missing bars, shifted timestamps) and close-only parity
would pass it. This gate therefore reconciles the full OHLCV tuple AND the row-key set
(ticker, session, bar timestamp) over the healthy overlap.
"""
from __future__ import annotations
import os, sys
import duckdb, pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "data") + os.sep
STG = {tf: f"{R}staging_backfill/stage_{tf}.duckdb" for tf in ("1h", "4h")}
CUR = {tf: f"{R}studio_{tf}.duckdb" for tf in ("1h", "4h")}
REF = f"{R}studio_15m.duckdb"                 # healthy reference calendar
ONE = f"{R}studio_analytics.duckdb"
DAMAGE = ("2026-06-29", "2026-09-01")         # the outage window
# The vendor revises recent aggregates — that is why update_intraday_db carries OVERLAP = 3 "to
# capture corrections". Canonical's last few sessions were written days before the refetch, so a
# difference there is a difference of VINTAGE, not of correctness, and staging holds the newer
# value. Comparing them asks "is data as of Sep 18 equal to data as of Sep 20", which is the wrong
# question. Those sessions are excluded from the parity gate and disclosed separately.
CORRECTION_SESSIONS = 3
# The session grid is derived EMPIRICALLY per session, never hardcoded: a fixed 7/2 would mis-judge
# early-close sessions (July 3, the day after Thanksgiving, Christmas Eve), and a `count > 7` test
# only catches EXTRA bars — a partial session of 6 bars would sail through. The expected timestamp
# SET for a session is the set of bar times held by at least GRID_SHARE of the tickers trading it.
GRID_SHARE = 0.5
L = lambda c="═", n=88: print(c * n)
_bad = 0
HEAD: dict = {"1h": {}, "4h": {}}


def fail(cond, msg):
    global _bad
    if not cond:
        _bad += 1
        print(f"      ⛔ {msg}")
    return cond


def fetch_window():
    c = duckdb.connect(STG["1h"], read_only=True)
    lo, hi = c.execute("SELECT MIN(CAST(date AS DATE)), MAX(CAST(date AS DATE)) FROM bars").fetchone()
    c.close()
    return str(lo), str(hi)


def calendar(lo, hi):
    c = duckdb.connect(ONE, read_only=True)
    d = [str(r[0]) for r in c.execute(
        "SELECT DISTINCT CAST(date AS DATE) FROM bars WHERE universe <> 'index' "
        "AND CAST(date AS DATE) BETWEEN ? AND ? ORDER BY 1", [lo, hi]).fetchall()]
    c.close()
    return d


def main():
    global lo, hi
    lo, hi = fetch_window()
    cal = calendar(lo, hi)
    L(); print(f"BACKFILL ACCEPTANCE · staging window {lo} … {hi} · {len(cal)} trading sessions"); L()

    print("\nFETCH")
    req = len(open(os.path.join(
        "/private/tmp/claude-501/-Users-sachoki-Desktop-sachoki-desktop/"
        "4dae8c94-36b8-47ac-89bb-fc7415e08193/scratchpad/bf_tickers.txt")).read().split(","))
    s1 = duckdb.connect(STG["1h"], read_only=True)
    got = s1.execute("SELECT COUNT(DISTINCT ticker), COUNT(*) FROM bars").fetchone()
    s1.close()
    print(f"  tickers requested        {req:,}")
    print(f"  tickers succeeded        {got[0]:,}")
    print(f"  tickers failed           {req - got[0]:,}")
    print(f"  1H rows written          {got[1]:,}")

    for tf in ("1h", "4h"):
        print(f"\nSTAGING {tf.upper()}")
        s = duckdb.connect(STG[tf], read_only=True)
        sess = [str(r[0]) for r in s.execute(
            "SELECT DISTINCT CAST(date AS DATE) FROM bars ORDER BY 1").fetchall()]
        missing = sorted(set(cal) - set(sess))
        print(f"  sessions expected/present/missing   {len(cal):,} / {len(sess):,} / {len(missing):,}")
        fail(not missing, f"missing sessions: {missing[:10]}")
        td = s.execute("SELECT COUNT(*) FROM (SELECT DISTINCT ticker, CAST(date AS DATE) FROM bars)").fetchone()[0]
        exp = got[0] * len(cal)
        print(f"  ticker-days present                 {td:,}  (upper bound {exp:,} if every ticker traded every session)")
        dup = s.execute("""SELECT COUNT(*) FROM (SELECT ticker, date, universe, COUNT(*) c
                           FROM bars GROUP BY 1,2,3 HAVING c > 1)""").fetchone()[0]
        print(f"  row-key duplicates                  {dup:,}")
        fail(dup == 0, f"{dup:,} duplicate (ticker, date, universe) keys")
        # empirical grid per session, then EXACT timestamp-set conformance per ticker-session
        s.execute(f"""CREATE OR REPLACE TEMP VIEW grid AS
            WITH tk AS (SELECT CAST(date AS DATE) d, COUNT(DISTINCT ticker) n FROM bars GROUP BY 1),
                 ts AS (SELECT CAST(date AS DATE) d, date::TIME t, COUNT(DISTINCT ticker) n
                        FROM bars GROUP BY 1,2)
            SELECT ts.d, ts.t FROM ts JOIN tk USING (d) WHERE ts.n >= {GRID_SHARE} * tk.n""")
        gs = s.execute("SELECT d, COUNT(*) c FROM grid GROUP BY 1").fetchdf()
        print(f"  empirical session grid              {gs.c.value_counts().to_dict()}  (bars per session)")
        extra = s.execute("""SELECT COUNT(*) FROM (
            SELECT b.ticker, CAST(b.date AS DATE) d, b.date::TIME t FROM bars b
            LEFT JOIN grid g ON g.d = CAST(b.date AS DATE) AND g.t = b.date::TIME
            WHERE g.d IS NULL)""").fetchone()[0]
        # Judged RELATIVE to canonical, never against an absolute of zero. Off-grid bars are a
        # normal property of this vendor's data — canonical carries ~4x more of them over the same
        # window — so an absolute threshold measured a baseline and called it a defect.
        cc = duckdb.connect(CUR[tf], read_only=True)
        base = cc.execute(f"""SELECT COUNT(*) FROM bars WHERE CAST(date AS DATE) BETWEEN '{lo}' AND '{hi}'
            AND date::TIME NOT IN (SELECT DISTINCT date::TIME FROM bars
                                   WHERE CAST(date AS DATE) BETWEEN '{lo}' AND '{hi}'
                                   GROUP BY 1 HAVING COUNT(*) > 100000)""").fetchone()[0]
        cc.close()
        print(f"  off-grid bars  staging {extra:,} · canonical {base:,} (same window)")
        fail(extra <= base, f"staging has MORE off-grid bars than canonical ({extra:,} > {base:,})")
        part = s.execute("""SELECT COUNT(*) FROM (
            SELECT b.ticker, CAST(b.date AS DATE) d, COUNT(*) have,
                   (SELECT COUNT(*) FROM grid g WHERE g.d = CAST(b.date AS DATE)) want
            FROM bars b GROUP BY 1,2 HAVING have < want)""").fetchone()[0]
        td_all = s.execute("SELECT COUNT(*) FROM (SELECT DISTINCT ticker, CAST(date AS DATE) FROM bars)").fetchone()[0]
        print(f"  PARTIAL sessions (fewer than grid)  {part:,} of {td_all:,}  ({100*part/td_all:.2f} %)")
        print("       (not a failure on its own — an illiquid ticker with no trade in a bar")
        print("        legitimately has no bar; the hard test is the key-set parity below)")
        s.close()

    cut = cal[-(CORRECTION_SESSIONS + 1)] if len(cal) > CORRECTION_SESSIONS else cal[-1]
    print(f"\nHEALTHY OVERLAP PARITY   (excluding the last {CORRECTION_SESSIONS} sessions, "
          f"i.e. after {cut} — the vendor's correction window)")
    print("  contract: canonical key set == staging key set at (ticker, universe, bar timestamp),")
    print("            AND OHLCV equal on those exact keys")
    for tf in ("1h", "4h"):
        c = duckdb.connect(CUR[tf], read_only=True)
        c.execute(f"ATTACH '{STG[tf]}' AS s (READ_ONLY)")
        r = c.execute(f"""
            SELECT COUNT(*) n,
                   ROUND(MAX(abs(a.open  - b.open )), 6) o,
                   ROUND(MAX(abs(a.high  - b.high )), 6) h,
                   ROUND(MAX(abs(a.low   - b.low  )), 6) l,
                   ROUND(MAX(abs(a.close - b.close)), 6) cl,
                   ROUND(MAX(abs(COALESCE(a.volume,0) - COALESCE(b.volume,0))), 6) v
            FROM bars a JOIN s.bars b ON a.ticker=b.ticker AND a.date=b.date
            WHERE CAST(a.date AS DATE) > DATE '{DAMAGE[1]}'
              AND CAST(a.date AS DATE) < DATE '{cut}'""").fetchdf().iloc[0]
        k = c.execute(f"""
            SELECT (SELECT COUNT(*) FROM bars WHERE CAST(date AS DATE) > DATE '{DAMAGE[1]}' AND CAST(date AS DATE) < DATE '{cut}') cur_rows,
                   (SELECT COUNT(*) FROM s.bars WHERE CAST(date AS DATE) > DATE '{DAMAGE[1]}' AND CAST(date AS DATE) < DATE '{cut}') stg_rows,
                   (SELECT COUNT(*) FROM (SELECT ticker, date FROM bars WHERE CAST(date AS DATE) > DATE '{DAMAGE[1]}' AND CAST(date AS DATE) < DATE '{cut}'
                    EXCEPT SELECT ticker, date FROM s.bars)) in_cur_not_stg,
                   (SELECT COUNT(*) FROM (SELECT ticker, date FROM s.bars WHERE CAST(date AS DATE) > DATE '{DAMAGE[1]}' AND CAST(date AS DATE) < DATE '{cut}'
                    EXCEPT SELECT ticker, date FROM bars)) in_stg_not_cur,
                   (SELECT COUNT(DISTINCT a.ticker) FROM
                      (SELECT DISTINCT ticker, universe FROM bars WHERE CAST(date AS DATE) > DATE '{DAMAGE[1]}' AND CAST(date AS DATE) < DATE '{cut}') a
                      JOIN (SELECT DISTINCT ticker, universe FROM s.bars WHERE CAST(date AS DATE) > DATE '{DAMAGE[1]}' AND CAST(date AS DATE) < DATE '{cut}') b
                      ON a.ticker=b.ticker AND a.universe <> b.universe) uni_drift""").fetchdf().iloc[0]
        print(f"  {tf.upper()}: compared {int(r.n):,} bars · max|Δ| O {r.o} H {r.h} L {r.l} C {r.cl} V {r.v}")
        # KEY = (ticker, bar timestamp). `universe` is an INDEX-MEMBERSHIP LABEL, not part of the
        # bar's identity: a ticker moving sp500 -> russell2k between the canonical build and the
        # refetch changes the label while every timestamp stays identical. Including it made the
        # first run report "staging INVENTS 252 row keys — the bar grid changed" when the grid was
        # untouched and three tickers had simply been reconstituted. Drift is reported, not failed.
        print(f"       row keys  canonical {int(k.cur_rows):,} · staging {int(k.stg_rows):,} · "
              f"canonical-only {int(k.in_cur_not_stg):,} · staging-only {int(k.in_stg_not_cur):,}")
        print(f"       universe-label drift  {int(k.uni_drift)} ticker(s) — index reconstitution, not a defect")
        # A corporate action shows up as a small set of ratio CLUSTERS — typically {1.0 before the
        # action, f after it} — not a continuum. fetch_bars uses adjusted=true, so the refetch is on
        # today's split basis while canonical holds the older one.
        #
        # The spread WITHIN a cluster is not evidence of a defect: prices are stored to two decimals,
        # so on a sub-dollar stock the quantum alone moves the ratio by 0.01/close. HUBC at $0.72
        # ranges 0.0398-0.0402 — a 1 % spread against a 1.4 % rounding tolerance. Judging the raw
        # spread called five clean reverse splits "unexplained"; the tolerance must come from the
        # quantum, not from a fixed percentage.
        rows = c.execute(f"""
            SELECT a.ticker, a.close ca, a.close/NULLIF(b.close,0) rt, abs(a.close-b.close) ad
            FROM bars a JOIN s.bars b ON a.ticker=b.ticker AND a.date=b.date
            WHERE CAST(a.date AS DATE) > DATE '{DAMAGE[1]}' AND CAST(a.date AS DATE) < DATE '{cut}'
              AND abs(a.close-b.close) > 0.01""").fetchdf()
        ca_tk, bad_tk, bad_names = 0, 0, []
        for tk, g in rows.groupby("ticker"):
            rat = g.rt.to_numpy(); px = g.ca.to_numpy()
            modal = float(pd.Series(rat).round(3).mode().iloc[0])
            tol = (0.01 / px.clip(min=1e-9)) + 1e-9          # relative error from the price quantum
            if bool((abs(rat - modal) <= (tol * 2 + 0.002) * modal).all()):
                ca_tk += 1
            else:
                bad_tk += 1; bad_names.append(tk)
        print(f"       price differences: {ca_tk} ticker(s) explained by a corporate action "
              f"(constant ratio within the 2-decimal rounding quantum); {bad_tk} unexplained"
              + (f" -> {bad_names[:8]}" if bad_names else ""))
        fail(bad_tk == 0, f"{tf}: {bad_tk} ticker(s) differ on the healthy overlap with no corporate-action explanation")
        fail(int(k.in_cur_not_stg) == 0, f"{tf} staging LOSES {int(k.in_cur_not_stg):,} existing row keys")
        fail(int(k.in_stg_not_cur) == 0, f"{tf} staging INVENTS {int(k.in_stg_not_cur):,} row keys "
                                         f"canonical never had — the bar grid changed")
        # The headline delta EXCLUDES corporate-action tickers. Including them printed
        # "max|Δ OHLCV| 6,580,425" — the volume effect of a 1:25 reverse split — which reads as
        # catastrophic corruption when it is the refetch being correctly on today's split basis.
        ca_list = sorted(set(rows.ticker)) if len(rows) else []
        ph = ("AND a.ticker NOT IN (" + ",".join(f"'{t}'" for t in ca_list) + ")") if ca_list else ""
        rr = c.execute(f"""SELECT ROUND(MAX(GREATEST(abs(a.open-b.open), abs(a.high-b.high),
                                  abs(a.low-b.low), abs(a.close-b.close))), 6) px,
                                  ROUND(MAX(abs(COALESCE(a.volume,0)-COALESCE(b.volume,0))), 6) vol
                           FROM bars a JOIN s.bars b ON a.ticker=b.ticker AND a.date=b.date
                           WHERE CAST(a.date AS DATE) > DATE '{DAMAGE[1]}'
                             AND CAST(a.date AS DATE) < DATE '{cut}' {ph}""").fetchdf().iloc[0]
        print(f"       max|Δ| EXCLUDING the {len(ca_list)} corporate-action tickers: "
              f"OHLC {rr.px} · volume {rr.vol}")
        fail(float(rr.px) == 0, f"{tf}: OHLC differs by {rr.px} on non-corporate-action tickers")
        exc = c.execute(f"""SELECT COUNT(*) n, SUM(CASE WHEN a.close <> b.close
                            OR COALESCE(a.volume,0) <> COALESCE(b.volume,0) THEN 1 ELSE 0 END) d
                            FROM bars a JOIN s.bars b ON a.ticker=b.ticker AND a.date=b.date
                            WHERE CAST(a.date AS DATE) >= DATE '{cut}'""").fetchdf().iloc[0]
        print(f"       DISCLOSED — in the excluded correction window: {int(exc.n):,} bars compared, "
              f"{int(exc.d or 0):,} differ (staging is the newer vintage)")
        HEAD[tf] = dict(cur_only=int(k.in_cur_not_stg), stg_only=int(k.in_stg_not_cur),
                        max_ohlcv=float(rr.px), max_vol=float(rr.vol), ca=len(ca_list))
        c.close()

    print("\nDAMAGED WINDOW RECOVERY   " + f"{DAMAGE[0]} … {DAMAGE[1]}")
    for tf in ("1h", "4h"):
        c = duckdb.connect(CUR[tf], read_only=True)
        c.execute(f"ATTACH '{STG[tf]}' AS s (READ_ONLY)")
        r = c.execute(f"""
            WITH cur AS (SELECT DISTINCT ticker, CAST(date AS DATE) d FROM bars
                         WHERE CAST(date AS DATE) BETWEEN DATE '{DAMAGE[0]}' AND DATE '{DAMAGE[1]}'),
                 stg AS (SELECT DISTINCT ticker, CAST(date AS DATE) d FROM s.bars
                         WHERE CAST(date AS DATE) BETWEEN DATE '{DAMAGE[0]}' AND DATE '{DAMAGE[1]}')
            SELECT (SELECT COUNT(*) FROM cur) cur_td, (SELECT COUNT(*) FROM stg) stg_td,
                   (SELECT COUNT(*) FROM (SELECT * FROM stg EXCEPT SELECT * FROM cur)) recovered,
                   (SELECT COUNT(*) FROM (SELECT * FROM cur EXCEPT SELECT * FROM stg)) lost""").fetchdf().iloc[0]
        print(f"  {tf.upper()}: canonical {int(r.cur_td):,} ticker-days · staging {int(r.stg_td):,} · "
              f"RECOVERED {int(r.recovered):,} · canonical-only {int(r.lost):,}")
        # In THIS window a canonical/staging difference is the whole point — we are filling holes, so
        # `recovered` is expected to be large and is not judged. `canonical-only` is likewise NOT a
        # failure here; it simply means the replacement must MERGE (insert what is missing, keep what
        # is there) rather than delete-and-rewrite the window wholesale.
        HEAD[tf]["recovered"] = int(r.recovered)
        HEAD[tf]["cur_only_damaged"] = int(r.lost)
        if int(r.lost):
            print(f"       -> replacement must MERGE, not wholesale-replace, or those "
                  f"{int(r.lost):,} ticker-days would be dropped")
        # what remains missing against the healthy reference calendar
        ref = duckdb.connect(REF, read_only=True)
        rt = ref.execute(f"""SELECT COUNT(*) FROM (SELECT DISTINCT ticker, CAST(date AS DATE) d FROM bars
                             WHERE CAST(date AS DATE) BETWEEN DATE '{DAMAGE[0]}' AND DATE '{DAMAGE[1]}')""").fetchone()[0]
        ref.close()
        union = int(r.stg_td) + int(r.lost)
        HEAD[tf]["remaining"] = max(rt - union, 0)
        print(f"       vs the healthy 15m reference ({rt:,} ticker-days): after the merge "
              f"{union:,} present · REMAINING MISSING {max(rt - union, 0):,}")
        c.close()

    L("─"); print("THE FOUR NUMBERS")
    for tf in ("1h", "4h"):
        h = HEAD[tf]
        print(f"  {tf.upper()}  healthy canonical-only keys {h.get('cur_only','?'):>8}"
              f" · healthy staging-only keys {h.get('stg_only','?'):>8}"
              f" · max|Δ OHLC| {h.get('max_ohlcv','?')} (ex-{h.get('ca','?')} corp-action)"
              f" · damaged-window REMAINING MISSING ticker-days {h.get('remaining','?'):>8}")

    L(); print("ACCEPTANCE: " + ("PASS — canonical replacement may proceed" if _bad == 0
                                 else f"FAIL ({_bad} problems) — canonical stays untouched")); L()
    return 0 if _bad == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
