"""M0 + M1 — the coverage audit, and the token sidecar it is allowed to write.

M0 IS THE GATE, AND IT RUNS FIRST FOR A REASON

    cols  = [c for c in SIG_COLS   if c in g]        build_opportunities.py:70
    flags = [c for c in _SEQ3_FLAGS if c in _avail]  studio/ultra_db_scan.py:681

Both lines silently shrink a declared vocabulary to whatever happened to be present. For a
screener that costs a chip. For a search manifest it costs the correspondence between k and
the space actually searched — and nothing downstream looks wrong, because a smaller search
produces a smaller, tidier, entirely believable table.

The measured cost of that pattern, today, in this repo: `build_opportunities.SIG_COLS`
declares 26 state columns and `opportunities.parquet` carries 17. The nine that vanished are
the entire VSA alphabet — l, fsfx, vb, gap, csfx, l5 — plus spring, anyd and l1. So the
population the incremental estimator runs on cannot see most of the vocabulary the screener
shows. This module refuses to be the third instance: a declared token with no materialized
column raises SilentMissingTokenError, and the manifest fails rather than narrows.

WHY A SIDECAR AND NOT A COLUMN IN `bars`

The `bars` writers are the nightly job and the manual incremental update. A third writer
would break a single-writer rule that exists for good reasons, and none of this is worth
that. `market_physics_token.py` already established the pattern — materialize to a parquet
keyed by (ticker, date) and let consumers join — so this follows it rather than inventing a
second convention.

THE GRAIN IS THE SIGNAL BAR

Key = (ticker, sig_date). Entry is the bar AFTER, exactly as `build_opportunities` defines
it, so every token here is known at the close of the bar it describes and nothing in the
sidecar can see the bar it would be traded on.

DEDUPLICATION IS NOT OPTIONAL

`bars` holds the same (ticker, date) once per universe — 39.6% of rows are duplicates of
another row's day. Joining tokens without collapsing that would multiply a ticker's rows by
its universe count and silently reweight every prevalence in the dependency map. The
universe priority here is the same one `_enrich_seq3` uses, so a bar resolves to the same
universe in the miner as it does on screen.

NO OUTCOME IS READ. `ret`, `ret_true`, `mae`, `mfe`, `hold` and the `mtm_*` grid are not
loaded, not joined and not present in the output. That is checked, not just intended:
`assert_no_outcome()` runs over the written frame's columns.
"""
from __future__ import annotations

import json
import os
import sys
import time

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_tokens_spec as TS                                       # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DB = os.path.join(ROOT, "data", "studio_analytics.duckdb")
OPPO = os.path.join(ROOT, "data", "opportunities.parquet")
OUT = os.path.join(ROOT, "data", "combo_tokens.parquet")
AUDIT = os.path.join(HERE, "COMBO_TOKENS_COVERAGE.json")

# any column whose presence would mean an outcome leaked into an X-only artifact
OUTCOME_MARKERS = ("ret", "ret_true", "ret_gap", "mae", "mfe", "hold", "risk",
                   "date_out", "mtm_")


def assert_no_outcome(cols) -> None:
    bad = [c for c in cols
           if c in OUTCOME_MARKERS or any(c.startswith(m) for m in ("mtm_",))]
    if bad:
        raise RuntimeError(f"outcome columns in an X-only artifact: {bad}. M0–M2 are "
                           f"frozen before Y is opened; a table that carries Y makes the "
                           f"freeze unverifiable after the fact.")


# ── M0 · coverage audit ──────────────────────────────────────────────────────
def coverage_audit(*, strict: bool = True, verbose: bool = True) -> dict:
    """Every declared token must have a materialized column. No silent shrinking."""
    conn = duckdb.connect(DB, read_only=True)
    try:
        bar_cols = {r[0] for r in conn.execute("DESCRIBE bars").fetchall()}
    finally:
        conn.close()
    oppo_cols = set(pd.read_parquet(OPPO, columns=None).columns) if os.path.exists(OPPO) \
        else set()
    have = {"bars": bar_cols, "opportunities": oppo_cols}

    rows, missing = [], []
    for t in TS.REGISTRY:
        ok = t.column in have[t.source]
        rows.append(dict(token_id=t.token_id, family=t.family, source=t.source,
                         column=t.column, kind=t.kind, materialized=ok,
                         null_requirement=t.null_requirement))
        if not ok:
            missing.append(t)

    fam = {}
    for r in rows:
        f = fam.setdefault(r["family"], dict(expected=0, materialized=0))
        f["expected"] += 1
        f["materialized"] += int(r["materialized"])
    for f in fam.values():
        f["coverage"] = round(f["materialized"] / f["expected"], 4)

    if verbose:
        print(f"\n  M0 · TOKEN COVERAGE AUDIT   registry {TS.digest()}", flush=True)
        print(f"  {'family':<16} {'expected':>9} {'materialized':>13} {'coverage':>9}",
              flush=True)
        for name in TS.FAMILIES:
            if name in fam:
                f = fam[name]
                mark = "" if f["coverage"] == 1.0 else "   ✗"
                print(f"  {name:<16} {f['expected']:>9} {f['materialized']:>13} "
                      f"{f['coverage']:>8.0%}{mark}", flush=True)
        tot_e = sum(f["expected"] for f in fam.values())
        tot_m = sum(f["materialized"] for f in fam.values())
        print(f"  {'─'*52}", flush=True)
        print(f"  {'TOTAL':<16} {tot_e:>9} {tot_m:>13} {tot_m/tot_e:>8.0%}", flush=True)

    if missing and strict:
        by_src = {}
        for t in missing:
            by_src.setdefault(t.source, []).append(f"{t.token_id}({t.column})")
        raise TS.SilentMissingTokenError(
            f"{len(missing)} declared token(s) have no materialized column: "
            + " · ".join(f"{s}: {', '.join(v)}" for s, v in sorted(by_src.items()))
            + ". The manifest declares a search space; a space that quietly becomes "
              "smaller than its declaration makes k a fiction. Either materialize the "
              "column or remove the token from the registry — both are visible, and "
              "narrowing in silence is not.")

    return dict(registry_digest=TS.digest(), spec_version=TS.SPEC_VERSION,
                n_declared=len(rows), n_materialized=sum(r["materialized"] for r in rows),
                by_family=fam, tokens=rows,
                missing=[t.token_id for t in missing])


# ── predicate evaluation ─────────────────────────────────────────────────────
def _evaluate(df: pd.DataFrame, t: TS.Token) -> np.ndarray:
    """One token → one boolean vector. Total function of one column, by construction."""
    col = df[t.column]
    if t.kind == "flag":
        v = pd.to_numeric(col, errors="coerce").fillna(0).to_numpy()
        return v != 0
    if t.kind == "band":
        lo, hi = t.value
        v = pd.to_numeric(col, errors="coerce").to_numpy(dtype=float)
        return (v >= lo) & (v < hi)          # NaN compares False in both, as intended
    s = col.astype("string").fillna("")
    if t.kind == "eq":
        return (s == t.value).to_numpy()
    if t.kind == "sw":
        return s.str.startswith(t.value, na=False).to_numpy()
    if t.kind == "ct":
        return s.str.contains(t.value, regex=False, na=False).to_numpy()
    if t.kind == "ne":
        return (s.str.len() > 0).to_numpy()
    if t.kind == "ldig":
        # single digits are PRESENCE inside a multi-digit code: L4 is true inside L46.
        valid = s.str.fullmatch(r"L[1-6]+", na=False).to_numpy()
        if t.value == "":
            return valid
        return valid & s.str.contains(t.value, regex=False, na=False).to_numpy()
    raise ValueError(t.kind)


# ── M1 · build the sidecar ───────────────────────────────────────────────────
def source_frame(*, verbose: bool = True) -> tuple[pd.DataFrame, float]:
    """The population's signal bars with every RAW source column a token reads.

    One definition of this query, used by both the token build and the dependency map.
    Two copies would drift, and the symptom would be a Cramér's V computed on a
    differently-deduplicated frame from the Jaccard beside it — two numbers about
    different populations printed in one table.
    """
    oppo_needed = sorted({t.column for t in TS.REGISTRY if t.source == "opportunities"})
    O = pd.read_parquet(OPPO, columns=["ticker", "sig_date", *oppo_needed])
    O["sig_date"] = O["sig_date"].astype(str).str[:10]
    keys = O.drop_duplicates(["ticker", "sig_date"]).reset_index(drop=True)
    if verbose:
        print(f"\n  population {len(O):,} opportunity rows → "
              f"{len(keys):,} distinct (ticker, sig_date)", flush=True)

    bar_cols = sorted({t.column for t in TS.REGISTRY if t.source == "bars"})
    conn = duckdb.connect(DB, read_only=True)
    try:
        conn.register("keys", keys[["ticker", "sig_date"]])
        sel = ", ".join(f"b.{c}" for c in bar_cols)
        # ONE row per (ticker, date). The universe priority is the same one the screener's
        # sequence enrichment uses, so a bar resolves identically in both places.
        df = conn.execute(f"""
            WITH d AS (
              SELECT b.ticker, CAST(b.date AS VARCHAR) AS sig_date, {sel},
                     row_number() OVER (PARTITION BY b.ticker, b.date ORDER BY
                       CASE b.universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1
                                       ELSE 2 END) rn
              FROM bars b
              JOIN keys k ON k.ticker = b.ticker
                         AND CAST(b.date AS VARCHAR) = k.sig_date
            )
            SELECT * EXCLUDE rn FROM d WHERE rn = 1
        """).fetchdf()
    finally:
        conn.close()
    df = df.merge(keys[["ticker", "sig_date", *oppo_needed]],
                  on=["ticker", "sig_date"], how="left", validate="1:1")
    cover = len(df) / len(keys)
    if cover < 0.98:
        raise RuntimeError(f"only {cover:.1%} of signal bars found in `bars` — the join "
                           f"key is wrong, and a partial token store would look like a "
                           f"rare token rather than a missing one")
    if verbose:
        print(f"  bars join: {len(df):,} rows · {len(bar_cols)} source columns "
              f"· coverage {cover:.2%}", flush=True)
    return df, cover


def build(*, verbose: bool = True) -> dict:
    t0 = time.time()
    audit = coverage_audit(verbose=verbose)
    df, cover = source_frame(verbose=verbose)

    # built column-at-a-time into a dict and concatenated once: 168 inserts into a live
    # frame is 168 reallocations, and pandas says so
    cols = {"ticker": df["ticker"].to_numpy(), "sig_date": df["sig_date"].to_numpy()}
    prevalence = {}
    for t in TS.REGISTRY:
        m = _evaluate(df, t)
        cols[t.token_id] = m.astype(np.uint8)
        prevalence[t.token_id] = int(m.sum())
    out = pd.DataFrame(cols)

    # ── the empirical check on a DECLARED property ───────────────────────────
    # `null_requirement` is a prior in the registry. A token that is constant within a
    # trading date is DAY_LEVEL whatever the registry says, and assuming otherwise is
    # exactly how N0's FWER 0.685 happened. So it is measured here rather than trusted.
    day_const = _date_constancy(out, verbose=verbose)

    assert_no_outcome(out.columns)
    out.to_parquet(OUT, index=False, compression="zstd")

    rep = dict(
        spec_version=TS.SPEC_VERSION, registry_digest=TS.digest(),
        rows=len(out), n_tokens=len(TS.REGISTRY),
        grain="(ticker, sig_date)", availability="signal_bar_close",
        dedup="one row per (ticker, date); universe priority sp500 < nasdaq < other",
        signal_bar_coverage=round(cover, 6),
        prevalence=prevalence,
        empty_tokens=sorted(k for k, v in prevalence.items() if v == 0),
        # A token true on almost every row is not a filter — conjoining it changes nothing
        # and it would occupy a candidate slot describing the population back to itself.
        # Reported rather than dropped: "L5_ANY is true on 99.9% of bars" is a fact about
        # line 5 worth knowing, and a silent removal would hide it.
        near_universal=sorted(k for k, v in prevalence.items() if v > 0.99 * len(out)),
        date_constancy=day_const,
        parquet_mb=round(os.path.getsize(OUT) / 1e6, 1),
        seconds=round(time.time() - t0, 1),
        coverage=audit,
    )
    with open(AUDIT, "w") as f:
        json.dump(rep, f, indent=2, default=str)

    if verbose:
        print(f"\n  WROTE {OUT}", flush=True)
        print(f"  {len(out):,} rows × {len(TS.REGISTRY)} tokens · "
              f"{rep['parquet_mb']} MB · {rep['seconds']}s", flush=True)
        if rep["empty_tokens"]:
            print(f"  ⚠ {len(rep['empty_tokens'])} token(s) never true on this "
                  f"population: {' '.join(rep['empty_tokens'])}", flush=True)
        if rep["near_universal"]:
            print(f"  ⚠ {len(rep['near_universal'])} token(s) true on >99% of rows "
                  f"(not filters): "
                  + " ".join(f"{k} {prevalence[k]/len(out):.1%}"
                             for k in rep["near_universal"]), flush=True)
    return rep


def _date_constancy(out: pd.DataFrame, *, verbose: bool = True) -> dict:
    """Share of (date) blocks in which a token is all-0 or all-1 across tickers.

    A predictor constant within a trading date cannot be nulled by permuting outcomes
    inside that date — the whole block sits in one arm. Near-1 here means DAY_LEVEL
    regardless of what the registry declared.
    """
    g = out.groupby("sig_date", sort=False)
    n = g.size().to_numpy(dtype=float)
    res, flagged = {}, []
    for t in TS.REGISTRY:
        s = g[t.token_id].sum().to_numpy(dtype=float)
        const = float(np.mean((s == 0) | (s == n)))
        res[t.token_id] = round(const, 4)
        declared_day = t.null_requirement == TS.DAY_LEVEL
        # 0.98 is a reporting threshold, not a decision rule: a rare token is all-zero on
        # most days by rarity alone, so this flags for reading, never reclassifies.
        if (const >= 0.98) != declared_day:
            flagged.append((t.token_id, t.null_requirement, const))
    if verbose and flagged:
        print(f"\n  null-requirement check · {len(flagged)} token(s) whose measured "
              f"within-date constancy disagrees with the registry:", flush=True)
        for tid, dec, c in sorted(flagged, key=lambda x: -x[2])[:15]:
            print(f"      {tid:<16} declared {dec:<18} constant on {c:>6.1%} of dates",
                  flush=True)
        print(f"      (rare tokens are trivially constant — read with prevalence, and "
              f"NOT a reclassification)", flush=True)
    return dict(by_token=res,
                disagreements=[dict(token=t, declared=d, constancy=c)
                               for t, d, c in flagged])


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "audit":
        coverage_audit()
    else:
        build()
