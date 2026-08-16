"""M2.6 — label maturity and terminal events. What "a realized trade return" actually means.

THE DEFECT THIS EXISTS TO NAME

`_pathsim` and `true_path` both end with

    end = min(j0 + 1 + MAXH, len(d))
    ...
    if ret is None:
        ret = cl[end - 1] / entry - 1 - SLIP

so a trade whose trail never fired is closed at the LAST AVAILABLE BAR and recorded in the
same column, with the same meaning, as one that ran its full course. Measured on the frozen
base population at as_of 2026-08-07: 13,666 of 289,467 rows (4.72%) are right-censored paths
written as realized returns, median 31 observed bars out of 60. The engine is shared, so this
is a property of the label family — `ret` and `ret_true` alike — not of either column.

WHY THE OBVIOUS MATURITY RULE IS WRONG, AND IT WAS MY FIRST PROPOSAL

    mature = (exit already happened) OR (60 bars observable)          ← WRONG

For a cohort near the right edge, "the exit already happened" is a fact about the future
price path. Including those trades and excluding their still-open peers selects on the
outcome: P(included | Y) ≠ P(included), and specifically it keeps the names that moved far
enough to trigger a trailing stop. That is the exact failure Gate A exists to prevent,
reintroduced by the fix for a different one. The 77 rows it would have rescued cannot be
rescued.

    mature = 60 complete MARKET bars after entry fall inside the frozen snapshot   ← RIGHT

Calendar only. Independent of whether the trail fired on bar 3, on bar 47, or never.

AND THE MARKET CALENDAR IS NOT THE TICKER'S OWN BAR COUNT

The first measurement used `min(j0 + 1 + MAXH, len(d))` — the length of THAT TICKER's
history — which silently merges two unrelated things. A name delisted in 2024 has 2023
entries whose sixty market bars matured long ago; calling them "not yet mature" is simply
false. Maturity is a property of the CALENDAR; the ticker running out is a property of the
COMPANY. So they are decided in that order, and never by the same comparison.

TERMINAL EVENTS ARE NOT CENSORING

A company that was acquired did not fail to finish — it finished differently. Dropping those
rows conditions the study on future survival, and acquisitions settle at a premium, so the
rows removed are systematically the good ones. Closing them at the last regular close invents
an economic exit that nobody received: a cash merger at $95 recorded as a $82 close is not a
conservative approximation, it is a wrong number with a plausible face.

    THE PAYOFF DATA DOES NOT EXIST IN THIS REPO. `corporate_actions.csv` is a splice
    detector (med_before / med_after / level_shift), and Massive's /v3/reference/tickers
    returns a ticker reference, not the consideration of a merger. So this module marks
    the event and declares the payoff UNRESOLVED rather than modelling one. An honest
    UNRESOLVED with a published coverage count is a smaller lie than an imputed price.

NO OUTCOME COLUMN IS READ. Prices, dates and the ATR-derived trail width are inputs to the
label, not the label. `ret` and `ret_true` are never loaded.
"""
from __future__ import annotations

import json
import os
import sys
import time

import duckdb
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OPPO = os.path.join(ROOT, "data", "opportunities.parquet")
DB = os.path.join(ROOT, "data", "studio_analytics.duckdb")
OUT = os.path.join(HERE, "COMBO_LABEL_CONTRACT.json")
OUT_ROWS = os.path.join(ROOT, "data", "combo_label_status.parquet")

MAXH = 60
LABEL_AS_OF = "2026-08-07"     # the snapshot true_path was computed against — measured,
                               # not assumed: it reproduces the NaN set with 0 disagreements
DEFAULT_TRAIL = 0.25

STATUS = ("ORDINARY_TRAIL_EXIT", "ORDINARY_MAXH_TIMER", "CORPORATE_ACTION_TERMINAL",
          "DATA_GAP", "NOT_YET_MATURE", "INVALID_PATH")


def market_calendar(conn, as_of: str) -> np.ndarray:
    """Every trading date in the snapshot. The union across names, not one ticker's history.

    A single proxy ticker would inherit its own halts and listing date. The union is what
    "a market bar" means for a population that spans thousands of names.
    """
    df = conn.execute(f"""
        SELECT DISTINCT CAST(date AS VARCHAR) d FROM bars
        WHERE universe <> 'index' AND close > 0 AND CAST(date AS VARCHAR) <= '{as_of}'
        ORDER BY d
    """).fetchdf()
    return df["d"].to_numpy()


def classify(verbose: bool = True) -> dict:
    t0 = time.time()
    O = pd.read_parquet(OPPO, columns=["ticker", "sig_date", "date_in", "sig_close",
                                       "dup_group", "risk"])
    B = O[O["sig_close"].notna()].drop_duplicates("dup_group")
    B = B[(B["sig_close"] >= 21) & (B["sig_close"] <= 89)].reset_index(drop=True)
    B["date_in"] = B["date_in"].astype(str).str[:10]
    B["sig_date"] = B["sig_date"].astype(str).str[:10]

    conn = duckdb.connect(DB, read_only=True)
    try:
        cal = market_calendar(conn, LABEL_AS_OF)
        raw = conn.execute(f"""
            SELECT DISTINCT ticker, CAST(date AS VARCHAR) date, open, high, low
            FROM bars WHERE universe <> 'index' AND close > 0
              AND CAST(date AS VARCHAR) <= '{LABEL_AS_OF}'
            ORDER BY ticker, date
        """).fetchdf()
    finally:
        conn.close()

    # entry at calendar position k is mature iff k + MAXH <= last index
    cutoff = cal[len(cal) - 1 - MAXH]
    if verbose:
        print(f"\n  M2.6 · snapshot {LABEL_AS_OF} · {len(cal):,} market bars", flush=True)
        print(f"  maturity cutoff (computed, not hardcoded): entries after {cutoff} "
              f"cannot have {MAXH} complete market bars", flush=True)

    P = {tk: (g["date"].to_numpy(), g["open"].to_numpy(float),
              g["high"].to_numpy(float), g["low"].to_numpy(float))
         for tk, g in raw.groupby("ticker", sort=False)}
    last_bar = {tk: d[-1] for tk, (d, *_rest) in P.items()}
    del raw

    tick = B["ticker"].to_numpy()
    din = B["date_in"].to_numpy()
    rsk = B["risk"].to_numpy(float)
    n = len(B)
    status = np.empty(n, dtype=object)
    bars_seen = np.full(n, -1, dtype=np.int32)

    for i in range(n):
        # (1) CALENDAR. Nothing about this ticker enters here.
        if din[i] > cutoff:
            status[i] = "NOT_YET_MATURE"
            continue
        p = P.get(tick[i])
        if p is None:
            status[i] = "INVALID_PATH"
            continue
        d, o, hi, lo = p
        j0 = int(np.searchsorted(d, din[i]))
        if j0 >= len(d) or not np.isfinite(o[j0]) or o[j0] <= 0:
            status[i] = "INVALID_PATH"
            continue
        have = len(d) - 1 - j0                       # bars available after entry
        bars_seen[i] = have
        # (2) THE COMPANY. Calendar-mature but the name's own history stops short.
        if have < MAXH:
            # a name whose last bar is the snapshot edge still exists — the hole is data
            status[i] = ("DATA_GAP" if last_bar[tick[i]] == cal[-1]
                         else "CORPORATE_ACTION_TERMINAL")
            continue
        # (3) ORDINARY. Both arms of the exit policy were fully observable.
        trail = rsk[i] if np.isfinite(rsk[i]) and rsk[i] > 0 else DEFAULT_TRAIL
        entry, pk, exited = o[j0], o[j0], False
        for j in range(j0 + 1, j0 + 1 + MAXH):
            if o[j] <= pk * (1 - trail):
                exited = True
                break
            pk = max(pk, hi[j])
            if lo[j] <= pk * (1 - trail):
                exited = True
                break
        status[i] = "ORDINARY_TRAIL_EXIT" if exited else "ORDINARY_MAXH_TIMER"
        if verbose and (i + 1) % 50000 == 0:
            print(f"    {i+1:,}/{n:,} · {time.time()-t0:.0f}s", flush=True)

    B["label_status"] = status
    B["bars_after_entry"] = bars_seen
    counts = B["label_status"].value_counts().to_dict()
    for s in STATUS:
        counts.setdefault(s, 0)

    # ── the identity. No losses, no double counting. ─────────────────────────
    total = len(B)
    parts = sum(counts[s] for s in STATUS)
    if parts != total:
        raise RuntimeError(f"reconciliation failed: {parts:,} classified vs {total:,} rows")

    ordinary = counts["ORDINARY_TRAIL_EXIT"] + counts["ORDINARY_MAXH_TIMER"]
    ca = B[B["label_status"] == "CORPORATE_ACTION_TERMINAL"]

    rep = dict(
        label_as_of=LABEL_AS_OF, maxh=MAXH,
        market_bars_in_snapshot=int(len(cal)),
        maturity_cutoff=str(cutoff),
        maturity_rule="60 complete MARKET bars after entry inside the snapshot; "
                      "calendar only, independent of whether the trail fired",
        base_population=int(total),
        counts={s: int(counts[s]) for s in STATUS},
        reconciliation_ok=True,
        research_population=int(ordinary),
        terminal=dict(n=int(counts["CORPORATE_ACTION_TERMINAL"]),
                      tickers=int(ca["ticker"].nunique()) if len(ca) else 0,
                      payoff_resolved=0,
                      payoff_unresolved=int(counts["CORPORATE_ACTION_TERMINAL"]),
                      payoff_source="NONE AVAILABLE — corporate_actions.csv is a splice "
                                    "detector; Massive /v3/reference/tickers returns a "
                                    "ticker reference, not merger consideration"),
        outcome_read="NONE — prices, dates and the ATR trail width only",
        seconds=round(time.time() - t0, 1))

    B[["ticker", "sig_date", "date_in", "label_status", "bars_after_entry"]] \
        .to_parquet(OUT_ROWS, index=False, compression="zstd")
    with open(OUT, "w") as f:
        json.dump(rep, f, indent=2, default=str)

    if verbose:
        print(f"\n  {'status':<28} {'n':>9}   share", flush=True)
        for s in STATUS:
            print(f"  {s:<28} {counts[s]:>9,}  {counts[s]/total:>6.2%}", flush=True)
        print(f"  {'─'*48}", flush=True)
        print(f"  {'TOTAL':<28} {parts:>9,}  {parts/total:>6.2%}   identity holds",
              flush=True)
        print(f"\n  research population (ordinary, both arms observable): "
              f"{ordinary:,}  ({ordinary/total:.2%})", flush=True)
        if len(ca):
            print(f"  terminal events: {len(ca):,} rows · {ca['ticker'].nunique()} tickers "
                  f"· payoff UNRESOLVED for all of them", flush=True)
            print(f"      {' '.join(sorted(ca['ticker'].unique())[:18])}", flush=True)
        print(f"\n  WROTE {OUT}\n  WROTE {OUT_ROWS}\n  {rep['seconds']}s", flush=True)
    return rep


if __name__ == "__main__":
    classify()
