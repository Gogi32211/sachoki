"""ONE_HOUR_DATA_VERSION_DIVERGENCE_V1 — a source-data incident, kept in its own branch.

The session-validity remediation surfaced this; it is NOT part of it. The finding is that
the CURRENT mutable 1H store no longer contains rows that the frozen family microstructures
were demonstrably built from. That makes the current store unusable as an oracle for
reconstructing historical family X — a data-contract fact, independent of any grammar or
adjacency question.

NO CAUSE IS ASSIGNED HERE. Pruning, ingestion, a failed refresh, a retention window, a
provider revision — all are consistent with what is measured, and none is claimed until
the pipeline's own provenance proves it.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import importlib, json, os, sys, time                                 # noqa: E402
import duckdb, pandas as pd                                           # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D5                               # noqa: E402

OUT = "ONE_HOUR_DATA_VERSION_DIVERGENCE_V1.json"
FAMS = ("t5", "t9", "t3", "t1")


def main():
    t0 = time.time()
    conn = duckdb.connect(D5.DB1H, read_only=True)
    now = conn.execute("""
        SELECT CAST(date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS DATE) sess,
               count(DISTINCT ticker) tk, count(*) bars
        FROM bars GROUP BY sess""").fetchdf()
    total = conn.execute("SELECT count(*) n, count(DISTINCT ticker) tk FROM bars").fetchone()
    conn.close()
    now["sess"] = now.sess.astype(str)
    cur = dict(zip(now.sess, now.tk))

    per_family, evidence = {}, {}
    for fam in FAMS:
        D = importlib.import_module(f"{fam}_dna")
        M = pd.read_parquet(D.OUT_MS, columns=["ticker", "session_date", "episode_id"])
        g = M.groupby("session_date").agg(tickers_at_build=("ticker", "nunique"),
                                          episodes=("episode_id", "nunique"))
        g["tickers_now"] = g.index.map(lambda d: int(cur.get(d, 0)))
        lost = g[g.tickers_now * 5 < g.tickers_at_build]
        per_family[fam.upper()] = dict(
            microstructure=os.path.basename(D.OUT_MS),
            microstructure_digest=ART.file_digest(D.OUT_MS),
            dates_covered=int(len(g)),
            dates_with_row_loss=int(len(lost)),
            episodes_on_lost_dates=int(lost.episodes.sum()),
            detail=[dict(session_date=d, tickers_at_build=int(r.tickers_at_build),
                         tickers_now=int(r.tickers_now), episodes=int(r.episodes))
                    for d, r in lost.iterrows()])
        for d, r in lost.iterrows():
            evidence.setdefault(d, dict(tickers_now=int(r.tickers_now), families={}))
            evidence[d]["families"][fam.upper()] = int(r.tickers_at_build)

    bd = set(pd.bdate_range(now.sess.min(), now.sess.max()).strftime("%Y-%m-%d"))
    absent = sorted(bd - set(now.sess))
    body = dict(
        spec_id="ONE_HOUR_DATA_VERSION_DIVERGENCE_V1", status="INCIDENT",
        classification="SOURCE-DATA VERSION DIVERGENCE / ROW LOSS",
        statement="the current mutable 1H store cannot reproduce the data version the "
                  "frozen family microstructures were built from, row for row",
        cause="NOT DETERMINED — deliberately unassigned until the ingestion pipeline's own "
              "provenance proves it; pruning, a failed refresh, a retention window and a "
              "provider revision are all consistent with the measurements below",
        evidence_by_date=evidence,
        per_family=per_family,
        current_store=dict(path=os.path.basename(D5.DB1H),
                           rows=int(total[0]), tickers=int(total[1]),
                           dates=int(len(now)),
                           file_mtime=time.strftime(
                               "%Y-%m-%d %H:%M", time.localtime(os.path.getmtime(D5.DB1H))),
                           note="a DuckDB file has no content digest that survives "
                                "compaction, so the row/ticker/date counts above are the "
                                "snapshot identity recorded here"),
        trading_dates_absent_entirely=dict(
            n=len(absent), last=absent[-12:],
            note="mixes genuine market holidays with ingestion gaps; not classified here"),
        consequences=[
            "the current 1H store is NOT an oracle for historical family X — any audit of "
            "the form 'rebuild family X from the current store and compare to the frozen "
            "artifact' is apples-to-oranges and must not be run as a semantic check",
            "frozen family X artifacts remain authoritative for their own vintage; this "
            "incident does not invalidate them",
            "the remediation comparisons stay valid because they read the SEALED "
            "microstructure and take only expected_bars(date) from the current store — and "
            "every collapsed date still resolves to expected_bars = 7",
            "forward evidence is different in kind: there the mutable store IS the supply "
            "chain, so a data-version admissibility gate is needed before any forward "
            "outcome is finalized or promoted"],
        proposed_admissibility_axes=dict(
            note="stated as a consequence of this incident, not enacted here",
            axes=["CODE SEMANTICS", "ENVIRONMENT", "SOURCE DATA VERSION"],
            reading="a source-code semantic gate cannot see row loss; admissibility needs "
                    "the third axis"),
        related=dict(
            completeness_audit=ART.file_digest("SESSION_REFERENCE_COMPLETENESS_V1.json"),
            reference_universe=ART.file_digest("SESSION_REFERENCE_UNIVERSE_V1.json")),
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        runtime_s=round(time.time() - t0, 1))
    d = ART.seal(body, OUT, required=("spec_id", "classification", "evidence_by_date",
                                      "cause"), supersede=os.path.exists(OUT))
    for date, e in sorted(evidence.items()):
        print(f"  {date} · now {e['tickers_now']} tickers · at build {e['families']}")
    print(f"\nONE_HOUR_DATA_VERSION_DIVERGENCE_V1 · {d}")


if __name__ == "__main__":
    main()
