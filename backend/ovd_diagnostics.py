"""OPENING_VOLUME_DYNAMICS_V1 — REPORT-ONLY post-outcome diagnostics (semantics frozen pre-seal, spec A10).

POST_ENTRY_DISTRIBUTION_EXPOSURE_GE5
  TRUE  iff an authoritative cash-distribution ex-date whose cash / canonical close on the last
        session BEFORE the ex-date >= 0.05 occurs strictly AFTER the _pathsim entry session
        (entry = D+1 open) AND on or before the ACTUAL simulated exit session.
  FALSE otherwise — a distribution after the actual exit is FALSE even if it lies inside maxh
        (entry D+1 open, exit after 8 bars, distribution at bar 20 -> FALSE).
  NULL  when the outcome row has no exit session (outcome UNAVAILABLE) — never False.

It is NOT X, NOT eligibility, NOT a registered claim, NOT in k; it cannot promote / veto / rescue
a finding and cannot remove rows from the primary result. It is computed only AFTER the outcome
phase has produced entry/exit sessions. Reporting: PRIMARY = all registered observations;
DESCRIPTIVE SENSITIVITY = rows with / without the flag. Ex-distribution stratification is
post-entry descriptive diagnostics, not causal predictor evidence.

Coverage: authoritative CASH distributions only (/v3/reference/dividends, frozen parquet bound in
SEAL.json). It is NOT a complete non-split corporate-action flag — spin-offs, reorganisations and
other events may affect price paths without being captured.

This module never touches _pathsim and is never imported by the X builder or the canonical
producers (ovd_seal refuses otherwise; see assert_not_used_by_x_builder).
"""
from __future__ import annotations
import os, re

HERE = os.path.dirname(os.path.abspath(__file__))
NAME = "POST_ENTRY_DISTRIBUTION_EXPOSURE_GE5"
THRESHOLD = 0.05
X_PRODUCERS = ("ovd_build.py", "ovd_canonical_1d.py", "ovd_canonical_derive.py")


def post_entry_distribution_exposure_ge5(entry_session, exit_session, ex_dates) -> bool | None:
    """Pure predicate on sessions (dates). `ex_dates` = this ticker's ex-dates whose yield >= THRESHOLD.

    entry_session  the _pathsim entry session (D+1); required
    exit_session   the ACTUAL simulated exit session; None -> NULL (never False)
    """
    if entry_session is None:
        raise ValueError("entry session is required — no outcome row exists without an entry")
    if exit_session is None:
        return None
    if exit_session < entry_session:
        raise ValueError(f"exit {exit_session} before entry {entry_session}")
    return any(entry_session < x <= exit_session for x in ex_dates)


# Materiality table (frozen definition):
#   1. exact duplicate reference rows are removed (the vendor table holds a handful);
#   2. cash is SUMMED per (ticker, ex_date) — the ex-open gap reflects everything going ex that day
#      (a regular + a special dividend on one ex-date is ONE distribution event);
#   3. reference price = the canonical close on the last session strictly BEFORE the ex-date (ASOF);
#   4. yield = cash / reference close; the event is in the set iff yield >= THRESHOLD.
# The earlier per-EVENT census (one row per vendor dividend row, no summing) is a coarser count and
# is kept only as a reconciliation figure — see per_event_ge5_count.
GE5_SQL = """
WITH raw AS (
  SELECT DISTINCT ticker, CAST(ex_date AS DATE) AS ex_date, cash, dtype, pay_date, freq
  FROM read_parquet('{dividends}')
  WHERE cash IS NOT NULL AND cash > 0 AND ex_date IS NOT NULL),
d AS (
  SELECT ticker, ex_date, sum(cash) AS cash, max(cash) AS max_single_cash, count(*) AS n_distributions,
         string_agg(dtype, ',' ORDER BY dtype) AS dtypes
  FROM raw GROUP BY 1, 2),
p AS (SELECT ticker, session_date, close FROM read_parquet('{canonical}') WHERE close > 0)
SELECT d.ticker, d.ex_date, d.cash, d.n_distributions, d.dtypes, p.session_date AS ref_session, p.close AS ref_close,
       d.cash / p.close AS yld, d.max_single_cash / p.close AS max_single_yld
FROM d ASOF JOIN p ON d.ticker = p.ticker AND d.ex_date > p.session_date
WHERE d.cash / p.close >= {threshold}
"""
PER_EVENT_SQL = """
WITH raw AS (
  SELECT ticker, CAST(ex_date AS DATE) AS ex_date, cash FROM read_parquet('{dividends}')
  WHERE cash IS NOT NULL AND ex_date IS NOT NULL),
p AS (SELECT ticker, session_date, close FROM read_parquet('{canonical}') WHERE close > 0)
SELECT raw.ticker, raw.ex_date FROM raw ASOF JOIN p ON raw.ticker = p.ticker AND raw.ex_date > p.session_date
WHERE raw.cash / p.close >= {threshold}
"""


def _window(q: str, first, last) -> str:
    if first is not None and last is not None:
        return f"SELECT * FROM ({q}) WHERE ex_date BETWEEN DATE '{first}' AND DATE '{last}'"
    return q


def ge5_ex_date_table(dividends_parquet: str, canonical_parquet: str, first=None, last=None,
                      threshold: float = THRESHOLD):
    """The (ticker, ex_date) set that defines the flag. Reads ONLY the frozen dividend reference and
    canonical closes — no outcome data. `first`/`last` restrict ex-dates to the observation window."""
    import duckdb
    q = _window(GE5_SQL.format(dividends=dividends_parquet, canonical=canonical_parquet, threshold=float(threshold)), first, last)
    c = duckdb.connect()
    try:
        return c.execute(q + " ORDER BY ticker, ex_date").df()
    finally:
        c.close()


def per_event_ge5_count(dividends_parquet: str, canonical_parquet: str, first=None, last=None,
                        threshold: float = THRESHOLD) -> dict:
    """Reconciliation only: the per-vendor-row count (no summing, duplicates kept) and whether every
    such pair is contained in the summed table (it must be — summing can only add pairs)."""
    import duckdb
    q = _window(PER_EVENT_SQL.format(dividends=dividends_parquet, canonical=canonical_parquet, threshold=float(threshold)), first, last)
    g = _window(GE5_SQL.format(dividends=dividends_parquet, canonical=canonical_parquet, threshold=float(threshold)), first, last)
    c = duckdb.connect()
    try:
        n, nt = c.execute(f"SELECT count(*), count(DISTINCT ticker) FROM ({q})").fetchone()
        missing = c.execute(f"SELECT count(*) FROM (SELECT DISTINCT ticker, ex_date FROM ({q})) e "
                            f"WHERE NOT EXISTS (SELECT 1 FROM ({g}) s WHERE s.ticker = e.ticker AND s.ex_date = e.ex_date)").fetchone()[0]
        dup = c.execute(f"SELECT count(*) - count(DISTINCT (ticker, ex_date, cash, dtype, pay_date, freq)) FROM read_parquet('{dividends_parquet}')").fetchone()[0]
    finally:
        c.close()
    return dict(per_event_events_tickers=[int(n), int(nt)], per_event_pairs_missing_from_summed_table=int(missing),
                exact_duplicate_rows_in_reference=int(dup))


def materialize(out_dir: str, dividends_parquet: str, dividends_sha256_16: str, canonical_parquet: str,
                canonical_run: str, first, last, threshold: float = THRESHOLD) -> dict:
    """Freeze the exposure table BEFORE any outcome exists: parquet + MATERIALITY_CENSUS_A10.json.
    Reads X-side references only."""
    import hashlib, json, time
    df = ge5_ex_date_table(dividends_parquet, canonical_parquet, first, last, threshold)
    p = os.path.join(out_dir, f"GE5_EXPOSURE_TABLE_A10_{canonical_run}.parquet")
    df.to_parquet(p, index=False)
    rec = per_event_ge5_count(dividends_parquet, canonical_parquet, first, last, threshold)
    by_year = {int(y): int(n) for y, n in df.groupby(df.ex_date.map(lambda x: x.year)).size().items()}
    out = dict(name=NAME, status="REPORT-ONLY POST-OUTCOME DIAGNOSTIC — exposure set frozen pre-seal",
               definition="exact-duplicate reference rows removed; cash SUMMED per (ticker, ex_date); reference price = canonical close "
                          "on the last session strictly before the ex-date; in the set iff cash / reference close >= threshold",
               threshold=float(threshold), window=[str(first), str(last)], canonical_run=canonical_run,
               dividends_parquet=dividends_parquet, dividends_sha256_16=dividends_sha256_16,
               table=p, table_sha256_16=hashlib.sha256(open(p, "rb").read()).hexdigest()[:16],
               events_tickers=[int(len(df)), int(df.ticker.nunique())], by_year=by_year,
               multi_distribution_ex_dates=int((df.n_distributions > 1).sum()),
               only_by_summing=int((df.max_single_yld < threshold).sum()),
               reconciliation_to_per_event_census=rec,
               written_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               statement="Ex-distribution stratification is post-entry descriptive diagnostics, not causal predictor evidence.")
    json.dump(out, open(os.path.join(out_dir, "MATERIALITY_CENSUS_A10.json"), "w"), indent=1, default=str)
    return out


def assert_not_used_by_x_builder() -> None:
    """The diagnostic must never enter X / eligibility: no X producer may import this module."""
    pat = re.compile(r"^\s*(import|from)\s+ovd_diagnostics\b", re.M)
    for f in X_PRODUCERS:
        p = os.path.join(HERE, f)
        if os.path.exists(p) and pat.search(open(p).read()):
            raise RuntimeError(f"{f} imports ovd_diagnostics — {NAME} would leak into X / eligibility")
