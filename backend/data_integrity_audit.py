"""A1 — is the bar database clean enough to retro-deflate the book on? (2026-08-09)

This is a GATE, not a study. Everything downstream — the research ledger, the deflation of
all 119 setups, the ranker — reads from these tables. If the universe is survivorship-biased
or carries spliced ticker histories, that work produces precise numbers that are wrong, and
precise wrong numbers are more dangerous than the imprecise ones we have now.

Three questions, each with a decisive test:

  Q1 SURVIVORSHIP. Does the DB contain companies that DIED? If every ticker's history runs
     to the last trading day, the database holds only survivors and every backtest in this
     repo is optimistic by an unknown amount.

  Q2 POINT-IN-TIME MEMBERSHIP. Is `universe` (sp500/nasdaq/russell2k) the membership AT THE
     TIME, or today's membership stamped onto all history? If the latter, "the S&P 500 in
     2021" in our data is really "companies that are in the S&P 500 in 2026, back-projected"
     — a textbook look-ahead.

  Q3 TICKER REUSE. META was Metamaterial ($11-17) before 2022-06-09 and Meta Platforms after.
     We found and deleted that one. 336 further candidates sit untouched in
     data_quality_splices.csv. How many are real?

Read-only. Touches no database.
"""
import os
import sys

import duckdb
import numpy as np
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "studio_analytics.duckdb")
SPLICES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_quality_splices.csv")
pd.set_option("display.width", 200)

con = duckdb.connect(DB, read_only=True)
gmax = con.execute("SELECT max(date) FROM bars").fetchone()[0]
gmin = con.execute("SELECT min(date) FROM bars").fetchone()[0]
ntk = con.execute("SELECT count(DISTINCT ticker) FROM bars").fetchone()[0]
print(f"{'='*88}\nDATA INTEGRITY AUDIT · {DB}\n"
      f"{ntk:,} tickers · {gmin} → {gmax}\n{'='*88}", flush=True)

# ── Q1 · SURVIVORSHIP ─────────────────────────────────────────────────────────
print("\n### Q1 — does the database contain companies that DIED?\n", flush=True)
life = con.execute("""
    SELECT ticker, min(date) first_bar, max(date) last_bar, count(*) n
    FROM bars GROUP BY ticker
""").fetchdf()
life["days_stale"] = (pd.Timestamp(gmax) - pd.to_datetime(life.last_bar)).dt.days
for thr in (7, 30, 90, 365):
    k = int((life.days_stale > thr).sum())
    print(f"  tickers whose data stops >{thr:>3d} days before {gmax}: "
          f"{k:>5,}  ({k/len(life)*100:5.1f}%)", flush=True)
dead = life[life.days_stale > 90]
print(f"\n  → interpretation:", flush=True)
if len(dead) < len(life) * 0.02:
    print(f"     ⛔ only {len(dead):,} of {len(life):,} tickers ({len(dead)/len(life)*100:.1f}%) "
          f"stop early.\n"
          f"     Over 5 years a real US equity universe loses roughly 5-10% of names to\n"
          f"     delisting, merger and bankruptcy. A number this small means the database\n"
          f"     holds SURVIVORS. Every backtest here is optimistic by an unknown amount,\n"
          f"     and the bias is largest exactly where our edges live — beaten-down names.",
          flush=True)
else:
    print(f"     ✅ {len(dead):,} tickers ({len(dead)/len(life)*100:.1f}%) end early — "
          f"consistent with real delisting. Survivorship looks addressed.", flush=True)
    print(life[life.days_stale > 90].sort_values("last_bar").head(8).to_string(index=False),
          flush=True)

# ── Q2 · POINT-IN-TIME MEMBERSHIP ─────────────────────────────────────────────
print("\n### Q2 — is `universe` membership point-in-time, or today's stamped on history?\n",
      flush=True)
mem = con.execute("""
    SELECT ticker, universe, min(date) u_first, max(date) u_last, count(*) n
    FROM bars GROUP BY ticker, universe
""").fetchdf()
span = life.set_index("ticker")[["first_bar", "last_bar"]]
mem = mem.join(span, on="ticker")
mem["starts_late"] = (pd.to_datetime(mem.u_first) - pd.to_datetime(mem.first_bar)).dt.days
mem["ends_early"] = (pd.to_datetime(mem.last_bar) - pd.to_datetime(mem.u_last)).dt.days
changed = mem[(mem.starts_late > 30) | (mem.ends_early > 30)]
print(f"  ticker×universe rows: {len(mem):,}", flush=True)
print(f"  rows where membership starts >30d after the ticker's first bar, "
      f"or ends >30d before its last: {len(changed):,} ({len(changed)/len(mem)*100:.1f}%)",
      flush=True)
multi = mem.groupby("ticker").size()
print(f"  tickers in >1 universe simultaneously: {int((multi > 1).sum()):,} "
      f"(e.g. CAR is nasdaq AND russell2k — expected, not a fault)", flush=True)
print(f"\n  → interpretation:", flush=True)
if len(changed) < len(mem) * 0.02:
    print("     ⛔ membership spans each ticker's WHOLE history. That is today's index\n"
          "     membership back-projected, NOT point-in-time. Any study filtered by\n"
          "     `universe` silently selects companies known to be in the index in 2026.",
          flush=True)
else:
    print("     ✅ membership starts/ends inside ticker histories — looks point-in-time.",
          flush=True)

# ── Q3 · TICKER REUSE / SPLICES ───────────────────────────────────────────────
print("\n### Q3 — how many of the flagged splice candidates are real?\n", flush=True)
if not os.path.exists(SPLICES):
    print(f"  {SPLICES} not found", flush=True)
else:
    sp = pd.read_csv(SPLICES)
    print(f"  candidates on file: {len(sp):,}   columns: {list(sp.columns)[:8]}", flush=True)
    print(sp.head(5).to_string(index=False), flush=True)
    # a splice is credible when the price level jumps by an order of magnitude in one day
    # AND the level never returns — a reused symbol, not a crash.
    col = next((c for c in sp.columns if "ticker" in c.lower()), sp.columns[0])
    cands = sp[col].astype(str).unique()[:400]
    hits = []
    for t in cands:
        try:
            d = con.execute(
                "SELECT date, close FROM bars WHERE ticker=? ORDER BY date", [t]).fetchdf()
        except Exception:
            continue
        if len(d) < 100:
            continue
        c = d.close.to_numpy(float)
        with np.errstate(divide="ignore", invalid="ignore"):
            r = c[1:] / c[:-1]
        i = int(np.nanargmax(np.abs(np.log(np.where(r > 0, r, np.nan)))))
        jump = r[i]
        if not np.isfinite(jump):
            continue
        pre, post = np.nanmedian(c[max(0, i - 60):i + 1]), np.nanmedian(c[i + 1:i + 61])
        if pre > 0 and post > 0 and (post / pre > 4 or pre / post > 4):
            hits.append((t, str(d.date.iloc[i])[:10], round(float(pre), 2),
                         round(float(post), 2), round(float(post / pre), 2)))
    H = pd.DataFrame(hits, columns=["ticker", "break_date", "median_before",
                                    "median_after", "ratio"])
    print(f"\n  of {len(cands)} candidates checked, {len(H)} show a >4x level shift that "
          f"does NOT revert (the META signature):", flush=True)
    if len(H):
        print(H.sort_values("ratio", ascending=False).head(20).to_string(index=False),
              flush=True)

con.close()
print(f"\n{'='*88}\nGATE VERDICT", flush=True)
print("  Q1 survivorship · Q2 point-in-time · Q3 splices — read the three interpretations.")
print("  Retro-deflation of the 119 setups is only meaningful once Q1 and Q2 are green,")
print("  because both bias the SAME direction: they make the past look kinder than it was.")
print("=" * 88, flush=True)
print("\nDONE", flush=True)
