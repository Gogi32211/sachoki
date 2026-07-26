"""journal_bench.py — the benchmark that makes a paper-journal win% mean something.

WHY THIS EXISTS (2026-07-17). The Atomic/Capit journals exit with a 20-bar hold and a
-15% stop; the +100% target hits ~0.2% of the time, so mechanically they are "hold 20
bars". In a bull window that wins ~55-59% on ANY stock. So a journal reporting "win 73%"
is reporting the market, not the signal — the Atomic journal's 52 closed trades (all from
one week, 36 of them from a single day) beat a random basket by +15.4pp on win% but only
+0.59σ on MEAN, and the 🔥Capit→Atom replay's 627 trades over 12 months beat random by
+0.32σ on mean. Both numbers looked excellent and said nothing. A win% without a
same-window baseline is a market thermometer.

WHAT IT COMPUTES. For every (ticker, date) it precomputes the outcome of the journals'
exact rule — enter at the NEXT bar's open, -15% stop / +100% target, 20-bar horizon,
stop-first, gap-aware — vectorised. A journal's baseline is then the mean/win% over all
liquid tickers on the SAME signal dates, weighted by that date's trade count. That is the
EXACT expectation of a random same-size basket (not a Monte-Carlo estimate of it), so it
is deterministic and cheap to serve.

Cache: data/journal_bench.parquet, rebuilt nightly next to ob_days (the DB is static
intraday). Falls back to computing on demand.
"""
from __future__ import annotations
import os, time, logging
import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

log = logging.getLogger("journal_bench")

HOR = 20            # every journal's horizon (bars held)
DV_FLOOR = 3_000_000

# The journals do NOT share an exit, so neither can their baselines — comparing the Capit
# journal (no tight stop) against a -15%-stopped basket would be apples to oranges.
#   stop15 → Atomic + 🔥Capit→Atom: -15% stop / +100% target (target hits ~0.2%)
#   cat35  → Capit: NO tight stop, only a -35% catastrophe floor (the validated hold; a
#            tight stop cut the bounce: hold-20 +4.6% vs +1.5% stopped, 5/6 yrs)
RULES = {
    "stop15": {"stop": 0.15, "target": 1.00},
    "cat35":  {"stop": 0.35, "target": None},
}
STOP_PCT = RULES["stop15"]["stop"]      # back-compat for callers/readers
TARGET_PCT = RULES["stop15"]["target"]

_DIR = os.path.join(os.path.dirname(__file__), "data")
_PATH = os.path.join(_DIR, "journal_bench.parquet")
_MEM: dict = {}


def _outcomes(g: pd.DataFrame, stop_pct: float, target_pct: float | None) -> np.ndarray:
    """Per-bar forward outcome (%) of a journal's rule, signal on bar i.
    Entry = open[i+1]; the walk is bars i+2 .. i+1+HOR (the entry bar itself is not
    checked — it is the fill); time-exit at close[i+1+HOR]. NaN where the window is
    incomplete, which is exactly when the journal would still show the trade as OPEN."""
    o = g["open"].to_numpy(float); h = g["high"].to_numpy(float)
    lo = g["low"].to_numpy(float); c = g["close"].to_numpy(float)
    n = len(g)
    out = np.full(n, np.nan)
    if n < HOR + 3:
        return out
    # windows starting at each bar; window w[k] covers bars k .. k+HOR-1
    wl = sliding_window_view(lo, HOR)
    wh = sliding_window_view(h, HOR)
    wo = sliding_window_view(o, HOR)
    # signal bar i is valid while its walk (start i+2, length HOR) fits: i+2 <= n-HOR
    last_i = n - HOR - 2
    if last_i < 0:
        return out
    i = np.arange(0, last_i + 1)
    entry = o[i + 1]
    ok = entry > 0
    safe = np.where(ok, entry, np.nan)          # never divide by a zero fill
    stop = entry * (1 - stop_pct)
    W = i + 2                                    # walk window index
    rows = np.arange(len(i))
    hit_s = wl[W] <= stop[:, None]
    any_s = hit_s.any(1)
    j_s = np.where(any_s, hit_s.argmax(1), HOR + 1)   # first stop bar (HOR+1 = never)
    if target_pct is None:
        any_t = np.zeros(len(i), bool); j_t = np.full(len(i), HOR + 1); tgt = None
    else:
        tgt = entry * (1 + target_pct)
        hit_t = wh[W] >= tgt[:, None]
        any_t = hit_t.any(1)
        j_t = np.where(any_t, hit_t.argmax(1), HOR + 1)
    # stop-first when tied (conservative, matches the journals' grade())
    s_first = any_s & (j_s <= j_t)
    t_first = any_t & (j_t < j_s)
    ret = c[i + 1 + HOR] / safe - 1.0                                   # time exit
    if s_first.any():
        fill = np.minimum(stop[s_first], wo[W][rows[s_first], j_s[s_first]])   # gap → open
        ret[s_first] = fill / safe[s_first] - 1.0
    if t_first.any():
        fill = np.maximum(tgt[t_first], wo[W][rows[t_first], j_t[t_first]])
        ret[t_first] = fill / safe[t_first] - 1.0
    ret[~ok] = np.nan
    out[i] = ret * 100.0
    return out


def build(months: int = 78) -> pd.DataFrame:
    """Precompute the per-(ticker,date) outcome table over the whole liquid universe."""
    from ai_journal.db import get_analytics_conn
    a = get_analytics_conn()
    try:
        as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
        df = a.execute(f"""
            WITH r AS (
              SELECT ticker, date, open, high, low, close, close*volume AS dv,
                     row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
              FROM bars
              WHERE close > 0 AND date >= DATE '{as_of}' - INTERVAL {int(months)*31} DAY
            )
            SELECT * EXCLUDE rn FROM r WHERE rn = 1 ORDER BY ticker, date
        """).fetchdf()
    finally:
        a.close()
    df["date"] = df["date"].astype(str).str[:10]
    parts = []
    for tk, g in df.groupby("ticker", sort=False):
        g = g.reset_index(drop=True)
        row = {"ticker": tk, "date": g["date"], "dv": g["dv"]}
        for rule, p in RULES.items():
            row[rule] = _outcomes(g, p["stop"], p["target"])
        parts.append(pd.DataFrame(row))
    out = pd.concat(parts, ignore_index=True)
    keep = out["dv"] >= DV_FLOOR
    for rule in RULES:
        keep &= out[rule].notna()
    out = out.loc[keep, ["date"] + list(RULES)]
    os.makedirs(_DIR, exist_ok=True)
    out.to_parquet(_PATH, index=False)
    log.info("journal_bench built: %s liquid (ticker,date) outcomes × %s rules, as_of %s",
             len(out), len(RULES), as_of)
    return out


def _table() -> pd.DataFrame | None:
    """The cached outcome table, keyed by date. Rebuilt only by the nightly job."""
    st = _MEM.get("t")
    try:
        mt = os.path.getmtime(_PATH)
    except OSError:
        return None
    if st is None or st[0] != mt:
        _MEM["t"] = (mt, pd.read_parquet(_PATH))
    return _MEM["t"][1]


def baseline(signal_dates: list[str], rule: str = "stop15") -> dict | None:
    """The random-basket expectation for a journal's OWN signal dates, under ITS OWN exit.

    `signal_dates` is one entry PER TRADE (repeats intended) — each date is weighted by how
    many trades the journal opened that day, so a journal that put 36 of 52 trades on one
    day is compared against that day, not against a flat average of the window.
    Returns None when the cache is missing/stale rather than blocking on a rebuild."""
    t = _table()
    if t is None or not len(signal_dates) or rule not in RULES or rule not in t.columns:
        return None
    per = t.groupby("date")[rule].agg(["mean", "count", lambda s: (s > 0).mean() * 100])
    per.columns = ["mean", "n_universe", "win"]
    w = pd.Series(signal_dates).value_counts()
    w = w[w.index.isin(per.index)]
    if not len(w):
        return None
    sub = per.loc[w.index]
    tot = w.sum()
    return {
        "mean": round(float((sub["mean"] * w).sum() / tot), 2),
        "win": round(float((sub["win"] * w).sum() / tot), 1),
        "dates": int(len(w)),
        "trades_matched": int(tot),
        "universe_per_date": int(sub["n_universe"].median()),
        "rule": rule,
    }


def annotate(stats: dict, signal_dates: list[str], rule: str = "stop15",
             win_key: str = "win_rate", mean_key: str = "avg_pnl") -> dict:
    """Attach the same-window baseline + the lift to a journal's stats block, so the raw
    win% can never be read alone. Silent no-op if the cache isn't there."""
    b = baseline(signal_dates, rule)
    if not b:
        return stats
    stats["bench"] = b
    jw, jm = stats.get(win_key), stats.get(mean_key)
    if jw is not None:
        stats["win_vs_bench"] = round(float(jw) - b["win"], 1)
    if jm is not None:
        stats["mean_vs_bench"] = round(float(jm) - b["mean"], 2)
    p = RULES[rule]
    exit_desc = (f"-{int(p['stop']*100)}% stop"
                 + (f" / +{int(p['target']*100)}% target" if p["target"] else " floor, no tight stop")
                 + f" / {HOR}-bar")
    stats["bench_note"] = (
        f"Same {b['dates']} signal dates, same exit rule ({exit_desc}), applied to the whole "
        f"liquid universe (~{b['universe_per_date']} tickers/day) and weighted by this journal's "
        f"own trades per day = what a RANDOM basket would have returned. Read the LIFT, not the "
        f"raw win%: a 20-bar hold wins ~55-59% on any stock in a bull window."
    )
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    t0 = time.time()
    df = build()
    print(f"built {len(df)} rows in {time.time()-t0:.0f}s → {_PATH}")
    print(df.groupby("date")[list(RULES)].mean().tail())
