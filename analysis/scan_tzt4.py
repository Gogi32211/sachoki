"""
scan_tzt4.py — Daily T-Z-T4 scanner (1D)

Finds tickers where TODAY:
  sig_t4 > 0  (T4 fires at bar[0])
  sig_z  > 0  yesterday (bar[-1])
  T-variant   > 0  2 days ago (bar[-2])
  RSI ≥ 60   at T4 bar
  suffix      EBA or EUR  (primary) / other suffixes shown separately

Usage:
    python scan_tzt4.py
    python scan_tzt4.py --date 2026-06-18      # specific date
    python scan_tzt4.py --rsi 70               # raise threshold
    python scan_tzt4.py --all-suffix           # include all suffixes
    python scan_tzt4.py --db ~/Downloads/studio_analytics.duckdb
"""
from __future__ import annotations
import argparse, os, sys
import duckdb, pandas as pd

# ── Tier definitions ─────────────────────────────────────────────────────────
TIER1 = {"t4"}
TIER2 = {"t3", "t10", "t9"}
TIER3 = {"t5", "t2", "t2g"}
TIER4 = {"t1", "t1g", "t11", "t12"}   # marginal — shown but flagged

ALL_VARIANTS = TIER1 | TIER2 | TIER3 | TIER4

TIER_LABEL = {
    **{v: "T1" for v in TIER1},
    **{v: "T2" for v in TIER2},
    **{v: "T3" for v in TIER3},
    **{v: "T4" for v in TIER4},
}

EXP_TABLE = {
    # (tier, suffix, rsi_band)  →  expected exp label
    ("T1", "EBA", "70+"): "+4.0%",  ("T1", "EUR", "70+"): "+4.0%",
    ("T1", "EBA", "60-70"): "+3.6%",("T1", "EUR", "60-70"): "+3.6%",
    ("T2", "EBA", "70+"): "+5.8%",  ("T2", "EUR", "70+"): "+5.8%",
    ("T2", "EBA", "60-70"): "+3.6%",("T2", "EUR", "60-70"): "+3.6%",
    ("T3", "EBA", "70+"): "+4.1%",  ("T3", "EUR", "70+"): "+4.1%",
    ("T3", "EBA", "60-70"): "+3.2%",("T3", "EUR", "60-70"): "+3.2%",
}

PRIME_SUFFIX = {"EBA", "EUR"}

DEFAULT_DB = os.path.expanduser("~/Downloads/studio_analytics.duckdb")


def rsi_band(rsi: float) -> str:
    if rsi >= 70: return "70+"
    if rsi >= 60: return "60-70"
    if rsi >= 50: return "50-60"
    return "<50"


def tier_sort_key(tier: str) -> int:
    return {"T1": 0, "T2": 1, "T3": 2, "T4": 3}.get(tier, 9)


def scan(db_path: str, scan_date: str | None, min_rsi: float, all_suffix: bool):
    con = duckdb.connect(db_path, read_only=True)

    # ── find scan date ────────────────────────────────────────────────────────
    if scan_date:
        date_str = scan_date
    else:
        date_str = con.execute("SELECT max(date)::VARCHAR FROM bars").fetchone()[0][:10]

    print(f"\n  DB  : {db_path}")
    print(f"  Date: {date_str}  |  RSI ≥ {min_rsi}  |  suffix: {'ALL' if all_suffix else 'EBA/EUR'}\n")

    # ── build lag columns ─────────────────────────────────────────────────────
    lag_cols = ",\n        ".join([
        f"CAST(LAG(sig_{v},2) OVER (PARTITION BY ticker ORDER BY date) AS DOUBLE) AS s_{v}"
        for v in sorted(ALL_VARIANTS)
    ])
    any_variant = " OR ".join([f"s_{v}>0" for v in sorted(ALL_VARIANTS)])

    suffix_filter = "" if all_suffix else "AND composite_full_suffix IN ('EBA','EUR')"

    df = con.execute(f"""
        WITH deduped AS (
            SELECT * FROM bars
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY universe) = 1
        ),
        lagged AS (
            SELECT
                ticker, date, close, high, low,
                (high+low)/2.0                                AS midpoint,
                universe, rsi_14, vol_bucket,
                composite_full_suffix,
                CAST(sig_t4 AS DOUBLE)                        AS t4,
                CAST(LAG(sig_z,1)  OVER (PARTITION BY ticker ORDER BY date) AS DOUBLE) AS z_lag1,
                {lag_cols}
            FROM deduped
            WHERE close > 0
        )
        SELECT *
        FROM lagged
        WHERE date::VARCHAR LIKE '{date_str}%'
          AND t4 > 0
          AND z_lag1 > 0
          AND rsi_14 >= {min_rsi}
          {suffix_filter}
          AND ({any_variant})
        ORDER BY ticker
    """).df()
    con.close()

    if df.empty:
        print("  No signals found for this date/filter combo.")
        return

    # ── assign tier and which variant fired ──────────────────────────────────
    rows = []
    for _, r in df.iterrows():
        # find which variant(s) fired at [-2]
        fired = [v for v in sorted(ALL_VARIANTS) if r.get(f"s_{v}", 0) > 0]
        if not fired:
            continue
        # pick highest-priority tier variant
        best = min(fired, key=lambda v: tier_sort_key(TIER_LABEL[v]))
        tier = TIER_LABEL[best]

        sfx   = r["composite_full_suffix"] or ""
        rsi   = r["rsi_14"]
        band  = rsi_band(rsi)
        exp   = EXP_TABLE.get((tier, sfx, band), "~+2%")
        prime = sfx in PRIME_SUFFIX

        entry = r["midpoint"]
        stop  = r["low"]
        risk  = entry - stop
        tgt   = entry + 3.0 * risk

        rows.append({
            "ticker":  r["ticker"],
            "tier":    tier,
            "variant": best.upper(),
            "RSI":     round(rsi, 1),
            "band":    band,
            "suffix":  sfx,
            "entry":   round(entry, 2),
            "stop":    round(stop, 2),
            "target":  round(tgt, 2),
            "risk%":   round(risk / entry * 100, 1),
            "exp":     exp,
            "prime":   prime,
            "uni":     r["universe"],
        })

    if not rows:
        print("  No valid signals after variant assignment.")
        return

    out = pd.DataFrame(rows).sort_values(
        ["prime", "tier", "RSI"],
        ascending=[False, True, False]
    ).reset_index(drop=True)

    # ── print ─────────────────────────────────────────────────────────────────
    prime_rows = out[out["prime"]]
    other_rows = out[~out["prime"]]

    def _print_block(title: str, block: pd.DataFrame):
        if block.empty:
            return
        print(f"{'─'*74}")
        print(f"  {title}  ({len(block)} signals)")
        print(f"{'─'*74}")
        hdr = f"  {'TICKER':<8} {'TIER':>4} {'VAR':<6} {'RSI':>5} {'SFX':>5}  {'ENTRY':>8} {'STOP':>8} {'TGT':>8} {'RISK%':>5}  EXP"
        print(hdr)
        print("  " + "-"*70)
        prev_tier = None
        for _, r in block.iterrows():
            if r["tier"] != prev_tier and prev_tier is not None:
                print()
            prev_tier = r["tier"]
            flag = "★" if r["tier"] in ("T1","T2") and r["RSI"] >= 70 else " "
            print(f"  {r['ticker']:<8} {r['tier']:>4} {r['variant']:<6} {r['RSI']:>5.1f} {r['suffix']:>5}  "
                  f"{r['entry']:>8.2f} {r['stop']:>8.2f} {r['target']:>8.2f}  "
                  f"{r['risk%']:>4.1f}%  {r['exp']}{flag}")
        print()

    _print_block("PRIMARY — EBA / EUR", prime_rows)
    if not other_rows.empty:
        _print_block("SECONDARY — other suffixes", other_rows)

    # ── summary ───────────────────────────────────────────────────────────────
    print(f"{'═'*74}")
    by_tier = out.groupby("tier").size()
    print("  Summary: " + "  |  ".join(f"{t}: {n}" for t, n in by_tier.items()))
    stars = out[(out["tier"].isin(["T1","T2"])) & (out["RSI"] >= 70)]
    if not stars.empty:
        print(f"  ★ Premium (Tier1/2, RSI≥70): {', '.join(stars['ticker'].tolist())}")
    print(f"{'═'*74}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Daily T-Z-T4 scanner")
    ap.add_argument("--db",         default=DEFAULT_DB, help="DuckDB path")
    ap.add_argument("--date",       default=None,       help="Scan date YYYY-MM-DD (default: max in DB)")
    ap.add_argument("--rsi",        type=float, default=60.0, help="Min RSI threshold (default 60)")
    ap.add_argument("--all-suffix", action="store_true",      help="Include all suffixes, not just EBA/EUR")
    a = ap.parse_args()

    # fix column name bug: 'tgt' not 'target' in _print_block — already correct above
    scan(a.db, a.date, a.rsi, a.all_suffix)
