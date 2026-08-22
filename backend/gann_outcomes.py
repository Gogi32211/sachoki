"""GANN direction-neutral outcome pair — materialised from GANN_OUTCOME_SPEC_V1.

This is the FIRST module in the GANN chain that reads a real future price. It reads it for
one purpose only: so the registered block-nullization has something to permute.

WHAT IT MUST NOT DO, and does not:
    no treated/control comparison, no phi = 0 Z, no raw outcome difference, no ranking,
    no per-claim summary of any kind. It writes the pair and prints coverage counts.

ENTRY SEMANTICS are not re-invented here. They are imported from t5_outcomes, which is the
lab's single definition and is already frozen: NEXT_SESSION_OPEN_V1 — k numbers the sessions
STRICTLY AFTER D, entry is the open at k = 1, and the path is k = 1..10 with the entry
session's own high and low included. A second implementation of entry/maturity semantics is
exactly how two families drift apart, so there is not a second one.

    MFE_LONG_10D   max_{k=1..10} High_k / EntryOpen - 1
    MFE_SHORT_10D  1 - min_{k=1..10} Low_k / EntryOpen

Both are persisted for every row, independent of any phase. The statistic selects one at
evaluation time; this module does not know which.

NORMALISER DEVIATION, recorded rather than hidden: the spec names ATR20 at D-1. The 1D store
carries atr_14 and no ATR20 column. The ATR-normalised components are therefore built from
atr_14 and NAMED for it, so nothing can later read an ATR14 number as an ATR20 number. The
capability statistic does not read them at all — it reads the return-scale pair above,
because the registered injection is additive in percentage points.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                    # noqa: E402
import duckdb, numpy as np, pandas as pd                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402
import gann_grid as G                                                  # noqa: E402
import t5_outcomes as O5                                               # noqa: E402

STATE = os.path.join(os.path.dirname(HERE), "data", "gann_estimand_state.parquet")
OUT = os.path.join(os.path.dirname(HERE), "data", "gann_outcomes.parquet")
SPEC = "GANN_OUTCOME_VALUES_V1.json"
HORIZON = 10


def build():
    t0 = time.time()
    spec = json.load(open("GANN_OUTCOME_SPEC_V1.json"))
    assert spec["horizon_sessions"] == HORIZON
    assert O5.ENTRY_SEMANTICS == "NEXT_SESSION_OPEN_V1", O5.ENTRY_SEMANTICS

    S = pd.read_parquet(STATE, columns=["ticker", "date"])
    E = S.drop_duplicates().reset_index(drop=True)
    print(f"  estimand rows {len(S):,} · distinct (ticker,date) {len(E):,}", flush=True)

    conn = duckdb.connect(G.DB1D, read_only=True)
    conn.register("ev", E)
    q = f"""
    WITH b AS (
      SELECT * FROM (
        SELECT ticker, date, open, high, low, close, atr_14,
               row_number() OVER (PARTITION BY ticker,date ORDER BY
                 CASE universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1 ELSE 2 END) rn
        FROM bars WHERE universe IN ('sp500','nasdaq','russell2k')) WHERE rn=1
    ), f AS (
      SELECT e.ticker, e.date d, b.high, b.low, b.open,
             row_number() OVER (PARTITION BY e.ticker, e.date ORDER BY b.date) k
      FROM ev e JOIN b ON b.ticker = e.ticker AND b.date > CAST(e.date AS DATE)
      QUALIFY k <= {HORIZON}
    ), p AS (
      SELECT ticker, d,
             max(CASE WHEN k=1 THEN open END) entry_open,
             max(high) hi, min(low) lo, count(*) n_path
      FROM f GROUP BY 1,2
    ), a AS (
      SELECT e.ticker, e.date d,
             max(CASE WHEN b.date < CAST(e.date AS DATE) THEN b.atr_14 END) atr14_prev
      FROM ev e JOIN b ON b.ticker = e.ticker
       AND b.date BETWEEN CAST(e.date AS DATE) - INTERVAL 5 DAY AND CAST(e.date AS DATE)
      GROUP BY 1,2)
    SELECT p.ticker, CAST(p.d AS VARCHAR) date, p.entry_open, p.hi, p.lo, p.n_path,
           a.atr14_prev
    FROM p LEFT JOIN a ON a.ticker = p.ticker AND a.d = p.d"""
    F = conn.execute(q).fetchdf()
    conn.close()

    O = E.assign(date=E.date.astype(str)).merge(F, on=["ticker", "date"], how="left")
    ok = (O.n_path == HORIZON) & O.entry_open.notna() & (O.entry_open > 0)
    O["MFE_LONG_10D"] = np.where(ok, O.hi / O.entry_open - 1.0, np.nan)
    O["MFE_SHORT_10D"] = np.where(ok, 1.0 - O.lo / O.entry_open, np.nan)
    O["MFE_ATR14_LONG_10D"] = np.where(ok & O.atr14_prev.gt(0),
                                       (O.hi - O.entry_open) / O.atr14_prev, np.nan)
    O["MFE_ATR14_SHORT_10D"] = np.where(ok & O.atr14_prev.gt(0),
                                        (O.entry_open - O.lo) / O.atr14_prev, np.nan)
    O["path_complete"] = ok
    O = O[["ticker", "date", "entry_open", "n_path", "path_complete", "MFE_LONG_10D",
           "MFE_SHORT_10D", "MFE_ATR14_LONG_10D", "MFE_ATR14_SHORT_10D"]]
    O.to_parquet(OUT, index=False)

    # coverage ONLY. No value, no moment, no comparison.
    n_ok = int(ok.sum())
    print(f"  path complete {n_ok:,} of {len(O):,} ({n_ok/len(O):.2%}) · "
          f"missing {len(O)-n_ok:,}", flush=True)
    d = ART.seal(dict(
        spec_id="GANN_OUTCOME_VALUES_V1", status="MATERIALISED",
        outcome_spec=ART.file_digest("GANN_OUTCOME_SPEC_V1.json"),
        estimand_state=ART.file_digest("GANN_ESTIMAND_STATE_V1.json"),
        entry_semantics=dict(
            id=O5.ENTRY_SEMANTICS,
            source="t5_outcomes.ENTRY_SEMANTICS — the lab's single frozen definition, "
                   "imported rather than re-implemented",
            k="sessions STRICTLY after D; entry is the open at k = 1",
            path=f"k = 1..{HORIZON}, the entry session's own high and low INCLUDED"),
        availability_note=dict(
            sealed_state_rule="the next session open and 10 further sessions must EXIST "
                              "(n_fwd >= 11)",
            outcome_consumes="sessions k = 1..10",
            relation="the sealed availability is STRICTLY STRONGER than the outcome needs; "
                     "it is not relaxed here"),
        normaliser_deviation=dict(
            spec_says="ATR20 at D-1",
            store_has="atr_14 — the 1D bars table carries no ATR20 column",
            resolution="the ATR-normalised components are built from atr_14 and NAMED "
                       "MFE_ATR14_* so an ATR14 number can never be read as an ATR20 one",
            impact="NONE on the capability — the registered statistic reads the return-scale "
                   "pair, because the registered injection is additive in percentage points"),
        rows=int(len(O)), path_complete=n_ok, path_incomplete=int(len(O) - n_ok),
        columns=["MFE_LONG_10D", "MFE_SHORT_10D", "MFE_ATR14_LONG_10D",
                 "MFE_ATR14_SHORT_10D"],
        reported="COVERAGE COUNTS ONLY — no outcome value, moment, quantile or "
                 "treated/control comparison is computed or printed anywhere in this module",
        table=dict(path=os.path.basename(OUT), digest=ART.file_digest(OUT)),
        runtime_min=round((time.time() - t0) / 60, 1)),
        SPEC, required=("spec_id", "entry_semantics", "rows"),
        supersede=os.path.exists(SPEC))
    print(f"GANN_OUTCOME_VALUES_V1 · {d} · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    build()
