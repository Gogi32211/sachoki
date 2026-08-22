"""SESSION_REFERENCE_UNIVERSE_V1 — the population the registered session-validity rule
refers to, and the deterministic resolution of its one unstated detail: mode ties.

The registered rule (frozen grammar contract):

    expected_bars(date) = the modal bar count ACROSS ALL TICKERS THAT TRADED THAT DATE
    a ticker-session is complete iff observed_bars(ticker,date) == expected_bars(date)

The rule does not say what to do when two bar counts tie for the mode. The implementation
being remediated inherited pandas' behaviour — mode() returns the sorted values and the
first was taken, i.e. the SMALLEST — which is what turned a thin family-local sample into
expected_bars = 2 on 2022-06-10. A tie rule must therefore be fixed explicitly, and its
census reported, so that the choice can be seen to matter or not.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                   # noqa: E402
import duckdb, numpy as np, pandas as pd                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D5                               # noqa: E402

OUT = "SESSION_REFERENCE_UNIVERSE_V1.json"
CAL_PARQ = "/Users/sachoki/Desktop/sachoki-desktop/data/session_calendar_1h.parquet"
TIE_RULE = "on a tie for the modal bar count, take the LARGEST tied value"


def main():
    t0 = time.time()
    conn = duckdb.connect(D5.DB1H, read_only=True)
    # one row per (ticker, ET session date): how many 1H bars that ticker traded
    T = conn.execute("""
        SELECT CAST(date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS DATE) sess,
               ticker, count(*) n, count(DISTINCT date) n_distinct_ts
        FROM bars GROUP BY sess, ticker""").fetchdf()
    conn.close()
    T["sess"] = T.sess.astype(str)
    dup_ok = bool((T.n == T.n_distinct_ts).all())      # no duplicated bar timestamps

    rows, ties = [], []
    for d, g in T.groupby("sess", sort=True):
        vc = g.n.value_counts()
        top = vc.max()
        cands = sorted(int(x) for x in vc[vc == top].index)
        expected = max(cands)                          # TIE_RULE
        if len(cands) > 1:
            ties.append(dict(session_date=d, tied_values=cands, tied_at_count=int(top),
                             n_tickers=int(len(g)), chosen=expected))
        rows.append(dict(session_date=d, n_tickers=int(len(g)),
                         calendar_len=expected, mn=int(g.n.min()), mx=int(g.n.max()),
                         share_at_expected=round(float((g.n == expected).mean()), 4)))
    CAL = pd.DataFrame(rows)
    prev = pd.read_parquet(CAL_PARQ) if os.path.exists(CAL_PARQ) else None
    CAL.to_parquet(CAL_PARQ, index=False)
    changed = None
    if prev is not None:
        m = prev.merge(CAL, on="session_date", suffixes=("_prev", "_now"))
        changed = int((m.calendar_len_prev != m.calendar_len_now).sum())

    body = dict(
        spec_id="SESSION_REFERENCE_UNIVERSE_V1", status="FROZEN",
        registered_rule="expected_bars(date) = modal bar count across ALL TICKERS that "
                        "traded that date; a ticker-session is complete iff its observed "
                        "bar count equals that",
        source=dict(store=os.path.basename(D5.DB1H), table="bars",
                    timezone="bar timestamps converted UTC -> America/New_York before the "
                             "session date is taken",
                    inclusion_predicate="every ticker with at least one 1H bar on that ET "
                                        "session date — no universe, liquidity or family "
                                        "filter, because the rule says ALL TICKERS",
                    date_range=[CAL.session_date.min(), CAL.session_date.max()],
                    n_dates=int(len(CAL))),
        grain_assertion=dict(
            statement="one bar per (ticker, timestamp); the bar count equals the distinct "
                      "timestamp count for every ticker-session",
            holds=dup_ok),
        tickers_per_date=dict(
            min=int(CAL.n_tickers.min()), q10=int(CAL.n_tickers.quantile(.1)),
            median=int(CAL.n_tickers.median()), max=int(CAL.n_tickers.max()),
            dates_under_100_tickers=int((CAL.n_tickers < 100).sum())),
        expected_bars_distribution={str(int(k)): int(v) for k, v in
                                    CAL.calendar_len.value_counts().sort_index().items()},
        share_of_tickers_at_expected=dict(
            min=round(float(CAL.share_at_expected.min()), 4),
            median=round(float(CAL.share_at_expected.median()), 4)),
        tie_rule=dict(rule=TIE_RULE,
                      why="a tie must resolve deterministically and must never resolve "
                          "DOWNWARD — resolving to the smallest tied value is exactly the "
                          "failure mode that admitted a 2-bar gapped session on 2022-06-10",
                      n_dates_with_a_tie=len(ties), ties=ties[:20],
                      effect="none observed" if not ties else "see the list"),
        short_sessions=dict(
            note="dates whose expected length is below the regular 7 are early closes and "
                 "stay VALID; the remediation does not reclassify them",
            dates=CAL.loc[CAL.calendar_len < 7,
                          ["session_date", "calendar_len", "n_tickers"]].to_dict("records")),
        not_repaired="anything that is not part of this defect is left exactly as the "
                     "registered rule computes it — including any date whose market-wide "
                     "modal is unusual; changing that would be a design review, not a "
                     "remediation",
        artifact=dict(path=os.path.basename(CAL_PARQ),
                      digest=ART.file_digest(CAL_PARQ),
                      rows=int(len(CAL)),
                      changed_vs_previous_build=changed),
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        runtime_s=round(time.time() - t0, 1))
    d = ART.seal(body, OUT, required=("spec_id", "registered_rule", "source", "tie_rule"),
                 supersede=os.path.exists(OUT))
    print(f"dates {len(CAL):,} · expected-bars distribution "
          f"{body['expected_bars_distribution']} · ties {len(ties)} · grain {dup_ok}")
    print(f"tickers/date median {body['tickers_per_date']['median']:,} · "
          f"min {body['tickers_per_date']['min']:,}")
    print(f"short-session dates: {[r['session_date'] for r in body['short_sessions']['dates']]}")
    print(f"\nSESSION_REFERENCE_UNIVERSE_V1 · {d}")


if __name__ == "__main__":
    main()
