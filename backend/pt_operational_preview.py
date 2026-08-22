"""PT_OPERATIONAL_PREVIEW_V1 — the current-bar layer, built WITHOUT touching sealed research.

THE RULE THIS EXISTS TO OBEY: the sealed historical episode tables and the pt{N}_signals
parquets are never recomputed from the mutable store. Recomputing them would silently
re-date the provenance of research that is already frozen. So the chart reaches the last bar
through a SECOND, clearly-labelled layer instead.

    <= the family's sealed boundary        HISTORICAL_SEALED   (served from pt{N}_signals)
    >  boundary, qualified source          prospective states  (none exist yet)
    >  boundary, current unqualified 1H    FORWARD_SOURCE_HOLD (this file)

WHAT IS AND IS NOT EVALUATED HERE

    base_match   evaluated. The canonical 1D signal comes from each family's OWN
                 build_episodes — the same priority-resolved SQL the sealed research used,
                 imported rather than reimplemented, just run against the current store.

    h1_match     NOT evaluated, and reported as UNEVALUATED rather than false. The frozen
                 1H membership caches are built only over episodes whose 10-day path is
                 AVAILABLE, so a post-boundary episode has no 1H evaluation at all. Writing
                 h1_match = False there would state "we looked and found nothing" when the
                 truth is "nothing has been looked at" — the same three-valued distinction
                 the PT phase availability already enforces. Materialising the 1H
                 microstructure for these episodes from the CURRENT 1H store is a separate
                 step, and that store is under a qualification HOLD.

A mark in this file is NOT forward evidence. If the 1H source later qualifies, marks
produced during the HOLD do not backfill into evidence — that is the second no-backfill
invariant, restated here so the file cannot be read as an accrual ledger.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, importlib, json, os, sys, time                         # noqa: E402
import duckdb, numpy as np, pandas as pd                               # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                              # noqa: E402
import pt_family as PF                                                 # noqa: E402

OUT = os.path.join(os.path.dirname(HERE), "data", "pt_operational_preview.parquet")
SPEC = "PT_OPERATIONAL_PREVIEW_V1.json"
SIG = os.path.join(os.path.dirname(HERE), "data", "pt{}_signals.parquet")


def store_version(conn) -> dict:
    """A vintage fingerprint of the mutable 1D store — what this preview was read from."""
    r = conn.execute("""SELECT max(date), count(*), count(DISTINCT ticker)
                        FROM bars WHERE universe IN ('sp500','nasdaq','russell2k')""" ).fetchone()
    v = dict(store="studio_analytics.duckdb::bars (1D)", max_date=str(r[0]),
             rows=int(r[1]), tickers=int(r[2]), mutability="MUTABLE — rewritten nightly")
    v["digest"] = hashlib.sha256(
        f"{v['max_date']}|{v['rows']}|{v['tickers']}".encode()).hexdigest()[:16]
    return v


def source_status(F: str) -> dict:
    """The 1H forward source gate, read from the sealed artifact — never inferred."""
    p = f"{F}_FORWARD_1H_V1.json"
    if not os.path.exists(p):
        return dict(h1_source="UNQUALIFIED", verdict="NO_FORWARD_ARTIFACT", seal_date=None)
    d = json.load(open(p))
    v = d.get("source_gate_state", {}).get("verdict", "UNKNOWN")
    return dict(h1_source="QUALIFIED" if v == "QUALIFIED" else "UNQUALIFIED", verdict=v,
                seal_date=d.get("no_backfill", {}).get("seal_date"))


def build():
    t0 = time.time()
    conn = duckdb.connect(PF.dna("T5").DB1D, read_only=True)
    ver = store_version(conn)
    print(f"  1D store vintage {ver['digest']} · max {ver['max_date']} · "
          f"{ver['rows']:,} rows", flush=True)

    rows, per_family = [], {}
    for F in PF.FAMILIES:
        D = PF.dna(F)
        dcol = PF.date_col(F)
        sealed = pd.read_parquet(SIG.format(F[1:]), columns=["date"])
        boundary = str(sealed.date.max())
        src = source_status(F)
        # the family's OWN priority-resolved episode SQL, against the CURRENT store
        E = D.build_episodes(conn, verbose=False)
        # every family's build_episodes speaks t5_date internally — the rename to
        # {fam}_date happens only when a table is WRITTEN, and nothing is written here
        E["d"] = E[dcol if dcol in E.columns else "t5_date"].astype(str)
        new = E[E.d > boundary]
        per_family[F] = dict(
            sealed_boundary=boundary, sealed_rows=int(len(sealed)),
            current_episodes=int(len(E)), new_after_boundary=int(len(new)),
            new_max_date=str(new.d.max()) if len(new) else None,
            source_gate=src)
        print(f"  {F}: sealed to {boundary} · current store yields {len(new):,} episodes "
              f"after it (to {new.d.max() if len(new) else '—'}) · 1H source "
              f"{src['h1_source']}", flush=True)
        for r in new.itertuples():
            rows.append(dict(
                family=F, ticker=r.ticker, decision_date=r.d,
                base_match=True,
                h1_match="UNEVALUATED",
                source_version=ver["digest"],
                source_status=src["h1_source"],
                evidence_status=("FORWARD_SOURCE_HOLD" if src["h1_source"] != "QUALIFIED"
                                 else "PROSPECTIVE_UNSEALED")))
    conn.close()

    P = pd.DataFrame(rows, columns=["family", "ticker", "decision_date", "base_match",
                                    "h1_match", "source_version", "source_status",
                                    "evidence_status"])
    P.to_parquet(OUT, index=False)

    d = ART.seal(dict(
        spec_id="PT_OPERATIONAL_PREVIEW_V1", status="OPERATIONAL_NOT_EVIDENCE",
        purpose="let the chart reach the current bar WITHOUT recomputing any sealed "
                "historical artifact",
        sealed_artifacts_touched="NONE — the episode tables and pt{N}_signals parquets are "
                                 "read for their boundary only, never rewritten",
        layers={
            "<= sealed boundary": "HISTORICAL_SEALED — served from pt{N}_signals, unchanged",
            "> boundary, qualified source": "prospective states — none exist yet",
            "> boundary, current unqualified 1H": "FORWARD_SOURCE_HOLD — this file"},
        base_match=dict(
            evaluated=True,
            method="each family's own build_episodes (the priority-resolved t_sig SQL), "
                   "imported rather than reimplemented, run against the current 1D store"),
        h1_match=dict(
            evaluated=False, value="UNEVALUATED",
            why="the frozen 1H membership caches cover only episodes whose 10-day path is "
                "AVAILABLE, so a post-boundary episode has no 1H evaluation. Reporting "
                "False would claim 'looked and found nothing' when nothing has been looked "
                "at.",
            to_enable="materialise the 1H microstructure for these episodes from the "
                      "current 1H store — which is itself under a qualification HOLD"),
        no_backfill="a mark produced during FORWARD_SOURCE_HOLD does NOT become evidence if "
                    "the source later qualifies; this file is a view, not an accrual ledger",
        source_version=ver,
        families=per_family,
        rows=int(len(P)),
        table=dict(path=os.path.basename(OUT), digest=ART.file_digest(OUT)),
        outcome_exposure="NOT_EXPOSED — no outcome value is read",
        runtime_min=round((time.time() - t0) / 60, 1)),
        SPEC, required=("spec_id", "layers", "base_match", "h1_match", "families"),
        supersede=os.path.exists(SPEC))
    print(f"\nPT_OPERATIONAL_PREVIEW_V1 · {d} · {len(P):,} rows · "
          f"{(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    build()
