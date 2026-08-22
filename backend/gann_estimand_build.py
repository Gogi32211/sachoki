"""GANN estimand state — X only. Treated, control, direction and blocks per claim.

Built once from the sealed X freeze so the capability engine and, later, gate A read the
SAME rows. Direction for treated rows is the frozen touched-line direction; for controls it
is the nearest phase-specific line at D-1, per GANN_ESTIMAND_V1. Neither uses D's own bars.

Pre-treatment liquidity and volatility are taken at D-1, and the LOW/HIGH split is made
within each decision date, so a block never compares a 2021 microcap with a 2026 megacap.

Availability is a STATUS: an episode is outcome-eligible when the next session open and ten
further sessions EXIST. Counting sessions is not reading an outcome.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                  # noqa: E402
import duckdb, numpy as np, pandas as pd                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import gann_grid as G, gann_phase as P                               # noqa: E402

OUT = os.path.join(G.ROOT, "data", "gann_estimand_state.parquet")
SPEC = "GANN_ESTIMAND_STATE_V1.json"
HORIZON = 10
CLAIMS = dict(ASC="GANN_ASC_TOUCH_V1", DESC="GANN_DESC_TOUCH_V1",
              CONF="GANN_CONFLUENCE_TOUCH_V1")


def pre_treatment(rows: pd.DataFrame) -> pd.DataFrame:
    """dv20 and ATR%/close at D-1, plus the forward-session count, from the 1D store."""
    conn = duckdb.connect(G.DB1D, read_only=True)
    conn.register("ev", rows[["ticker", "date"]].drop_duplicates())
    q = """
    WITH b AS (
      SELECT * FROM (
        SELECT ticker, date, close, volume, atr_14,
               row_number() OVER (PARTITION BY ticker,date ORDER BY
                 CASE universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1 ELSE 2 END) rn
        FROM bars WHERE universe IN ('sp500','nasdaq','russell2k')) WHERE rn=1
    ), r AS (
      SELECT ticker, date, close, atr_14,
             median(close*volume) OVER (PARTITION BY ticker ORDER BY date
                 ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) dv20,
             count(*) OVER (PARTITION BY ticker ORDER BY date
                 ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) n20,
             count(*) OVER (PARTITION BY ticker ORDER BY date
                 ROWS BETWEEN 1 FOLLOWING AND 11 FOLLOWING) n_fwd
      FROM b
    ), a AS (
      SELECT e.ticker, CAST(e.date AS VARCHAR) date,
             max(CASE WHEN r.date < CAST(e.date AS DATE) THEN r.dv20 END) dv20_prev,
             max(CASE WHEN r.date < CAST(e.date AS DATE)
                      THEN r.atr_14/nullif(r.close,0) END) atrp_prev,
             max(CASE WHEN r.date < CAST(e.date AS DATE) THEN r.n20 END) n20_prev,
             max(CASE WHEN r.date = CAST(e.date AS DATE) THEN r.n_fwd END) n_fwd
      FROM ev e JOIN r ON r.ticker = e.ticker
       AND r.date BETWEEN CAST(e.date AS DATE) - INTERVAL 5 DAY AND CAST(e.date AS DATE)
      GROUP BY e.ticker, e.date)
    SELECT * FROM a"""
    A = conn.execute(q).fetchdf()
    conn.close()
    return A


def main():
    t0 = time.time()
    Gr = pd.read_parquet(G.OUT_GRID)
    st = P.prepare(Gr)
    ws = P.world_state(st, np.zeros(len(st["tickers"])))          # phi = 0, the frozen X

    base = pd.DataFrame(dict(
        ticker=np.asarray(st["tickers"])[st["tick_code"]],
        date=pd.Series(st["date"]).astype(str).str[:10].to_numpy(),
        di=st["di"]))
    for k, d in ws.items():
        base[f"{k}_treated"] = d["onset"]
        base[f"{k}_dir"] = d["direction"]

    print(f"  rows {len(base):,} · fetching pre-treatment state at D-1 …", flush=True)
    A = pre_treatment(base)
    B = base.merge(A, on=["ticker", "date"], how="left")
    B["available"] = (B.n_fwd >= HORIZON + 1) & B.dv20_prev.notna() \
        & B.atrp_prev.notna() & (B.n20_prev >= 20)
    print(f"  outcome-eligible rows {int(B.available.sum()):,} "
          f"({B.available.mean():.1%})", flush=True)

    # within-date LOW/HIGH halves, so a block never mixes eras
    E = B[B.available].copy()
    for col, out in (("dv20_prev", "liq_half"), ("atrp_prev", "vol_half")):
        g = E.groupby("date")[col]
        E[out] = np.where(g.rank(method="first") <= g.transform("size") / 2, "LOW", "HIGH")
    for k in CLAIMS:
        E[f"{k}_block"] = (E.date + "|" + E[f"{k}_dir"].astype(str) + "|"
                           + E.liq_half + "|" + E.vol_half)
    E.to_parquet(OUT, index=False)

    counts = {}
    for k, cid in CLAIMS.items():
        d = E[f"{k}_dir"]
        valid = d.isin([1, -1])
        tr = E[f"{k}_treated"] & valid
        counts[cid] = dict(
            rows_valid_direction=int(valid.sum()),
            treated=int(tr.sum()),
            control=int((valid & ~E[f"{k}_treated"]).sum()),
            long=int((valid & (d == 1)).sum()), short=int((valid & (d == -1)).sum()),
            blocks=int(E.loc[valid, f"{k}_block"].nunique()),
            invalid_equality=int((d == 0).sum()),
            invalid_conflict=int((d == 9).sum()))
        print(f"  {cid:<26} treated {counts[cid]['treated']:>7,} · control "
              f"{counts[cid]['control']:>8,} · blocks {counts[cid]['blocks']:>6,}")

    d = ART.seal(dict(
        spec_id="GANN_ESTIMAND_STATE_V1", status="X_ONLY",
        x_freeze=ART.file_digest("GANN_X_FREEZE_V1.json"),
        estimand_spec=ART.file_digest("GANN_ESTIMAND_V1.json"),
        phase_code=ART.file_digest("gann_phase.py"),
        rows_total=int(len(B)), rows_outcome_eligible=int(len(E)),
        availability=dict(rule=f"the next session open and {HORIZON} further sessions must "
                               "EXIST, and the D-1 anchors must be complete",
                          note="counting sessions is a STATUS, not an outcome value"),
        blocks=dict(dimensions=["decision_date", "direction", "pre_liquidity LOW/HIGH",
                                "pre_volatility LOW/HIGH"],
                    anchors="dv20 and ATR%/close at D-1",
                    split="LOW/HIGH within each decision date"),
        claims=counts,
        table=dict(path=os.path.basename(OUT), digest=ART.file_digest(OUT)),
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        runtime_min=round((time.time() - t0) / 60, 1)),
        SPEC, required=("spec_id", "claims", "blocks"),
        supersede=os.path.exists(SPEC))
    print(f"\nGANN_ESTIMAND_STATE_V1 · {d} · {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
