"""FORWARD_1H_SOURCE_V1 — is the mutable 1H store fit to carry PROSPECTIVE evidence?

A frozen research artifact can outlive a store that no longer reproduces it; that is what
ONE_HOUR_DATA_VERSION_DIVERGENCE_V1 recorded. Forward is different in kind: there the
store IS the evidence supply chain, so it needs its own qualification, and a sealed signal
spec is NOT enough to start counting evidence.

    version identity        what the store is right now, in reconstructable terms
    session mapping         the expected length per date, from the sealed calendar
    completeness            how many tickers/sessions actually arrive per date
    grain                   one bar per (ticker, timestamp) — duplicates are fatal
    revision policy         what happens when a past bar changes after it was used
    availability timestamp  when the data for a session became readable
    provenance              which writer produced it

VERDICT SEMANTICS
    QUALIFIED      prospective evidence may accrue
    HOLD           the spec may be sealed and the UI may show an operational preview, but
                   the prospective evidence counter does not move

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                  # noqa: E402
import duckdb, numpy as np, pandas as pd                             # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D5                              # noqa: E402
import session_calendar as SC                                        # noqa: E402

OUT = "FORWARD_1H_SOURCE_V1.json"
RECENT_DAYS = 60


def main():
    t0 = time.time()
    conn = duckdb.connect(D5.DB1H, read_only=True)
    tot = conn.execute("SELECT count(*), count(DISTINCT ticker) FROM bars").fetchone()
    per = conn.execute("""
        SELECT CAST(date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS DATE) sess,
               count(DISTINCT ticker) tk, count(*) bars
        FROM bars GROUP BY sess ORDER BY sess""").fetchdf()
    dup = conn.execute("""
        SELECT count(*) FROM (SELECT ticker, date, universe, count(*) n FROM bars
                              GROUP BY ticker, date, universe HAVING n > 1)""").fetchone()[0]
    conn.close()
    per["sess"] = per.sess.astype(str)
    exp = SC.expected_bars_by_date()
    per["expected"] = per.sess.map(exp)
    med_tk = float(per.tk.median())
    recent = per.tail(RECENT_DAYS)
    collapsed = recent[recent.tk < 0.5 * med_tk]

    checks = {
        "grain: one row per (ticker, timestamp, universe)": dup == 0,
        "every recent session has an expected length": bool(recent.expected.notna().all()),
        "no recent session collapsed below half the median ticker count":
            len(collapsed) == 0,
        "the store reproduces its own frozen research vintage":
            False,   # ONE_HOUR_DATA_VERSION_DIVERGENCE_V1 proved it does not
    }
    qualified = all(checks.values())
    body = dict(
        spec_id="FORWARD_1H_SOURCE_V1",
        verdict="QUALIFIED" if qualified else "HOLD",
        store=dict(path=os.path.basename(D5.DB1H), rows=int(tot[0]), tickers=int(tot[1]),
                   sessions=int(len(per)),
                   date_range=[per.sess.min(), per.sess.max()],
                   file_mtime=time.strftime("%Y-%m-%d %H:%M",
                                            time.localtime(os.path.getmtime(D5.DB1H))),
                   identity_note="a DuckDB file has no content digest that survives "
                                 "compaction, so the row / ticker / session counts above "
                                 "ARE the recorded snapshot identity"),
        session_mapping=dict(source=ART.file_digest("SESSION_REFERENCE_UNIVERSE_V1.json"),
                             mapping_digest=SC.mapping_digest(),
                             tie_policy=ART.file_digest("SESSION_MODE_TIE_POLICY_V1.json")),
        completeness=dict(
            tickers_per_session=dict(median=int(med_tk), min=int(per.tk.min()),
                                     q05=int(per.tk.quantile(.05))),
            recent_window_days=RECENT_DAYS,
            recent_collapsed_sessions=[dict(session_date=r.sess, tickers=int(r.tk))
                                       for r in collapsed.itertuples()]),
        grain=dict(duplicate_rows=int(dup), rule="duplicates are fatal, never deduplicated "
                                                 "silently"),
        revision_policy=dict(
            status="NOT ESTABLISHED",
            required="a signal generated under source version X must stay reconstructable "
                     "from X; today the store is rewritten in place by the nightly delta, "
                     "so a past bar can change after a signal used it and nothing records "
                     "that it did",
            consequence="this alone blocks QUALIFIED"),
        availability_timestamps=dict(status="NOT ESTABLISHED",
                                     required="when each session became readable, so a "
                                              "forward decision can be shown to have used "
                                              "only what existed at decision time"),
        build_provenance=dict(writer="launchd com.sachoki.dbupdate nightly 03:00 Tbilisi "
                                     "plus a manual admin endpoint",
                              known_incident=ART.file_digest(
                                  "ONE_HOUR_DATA_VERSION_DIVERGENCE_V1.json")),
        checks={k: bool(v) for k, v in checks.items()},
        consequence_of_hold=[
            "forward specifications MAY be sealed and the clock MAY start",
            "the UI may show an explicitly labelled operational preview",
            "the prospective evidence counter does NOT move",
            "no forward episode may be promoted to evidence",
            "historical frozen X is never repaired or reinterpreted from this store"],
        outcome_exposure="NOT_EXPOSED",
        runtime_s=round(time.time() - t0, 1),
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(body, OUT, required=("spec_id", "verdict", "checks"),
                 supersede=os.path.exists(OUT))
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")
    print(f"\n  store: {tot[0]:,} rows · {tot[1]:,} tickers · {len(per):,} sessions · "
          f"median {int(med_tk):,} tickers/session")
    print(f"  recent collapsed sessions: {len(collapsed)}")
    print(f"\nFORWARD_1H_SOURCE_V1 · {d} · {body['verdict']}")


if __name__ == "__main__":
    main()
