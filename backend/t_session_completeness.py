"""SESSION_MODE_TIE_POLICY_V1 + SESSION_REFERENCE_COMPLETENESS_V1.

Two governance closures the review asked for before any corrected T1 freeze:

    1  the mode-tie policy gets its own identity and an honest label — the registered rule
       is UNDEFINED when several bar counts tie for the mode, so choosing the largest is a
       DETERMINISTIC COMPLETION of an underspecified rule, not a restoration of it
    2  the reference universe is audited for COMPLETENESS: a date where the store holds
       two tickers is not the same object as a date where it holds 2,780, and the question
       is whether such dates are legitimately sparse or an incomplete data version

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import importlib, json, os, sys, time                                 # noqa: E402
import duckdb, numpy as np, pandas as pd                              # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART, t5_dna as D5                               # noqa: E402

TIE_OUT = "SESSION_MODE_TIE_POLICY_V1.json"
COMP_OUT = "SESSION_REFERENCE_COMPLETENESS_V1.json"
CAL_PARQ = "/Users/sachoki/Desktop/sachoki-desktop/data/session_calendar_1h.parquet"
COLLAPSE_FRACTION = 0.5          # a date holding under half the median ticker count


def tie_policy():
    body = dict(
        spec_id="SESSION_MODE_TIE_POLICY_V1", status="FROZEN",
        trigger="the registered market-wide modal rule does not say what to do when "
                "several bar counts attain the same maximum frequency; the function is "
                "therefore not total, and an implementation must complete it",
        policy="select the LARGEST modal bar count",
        classification="IMPLEMENTATION-COMPLETION AMENDMENT — a deterministic completion "
                       "of an underspecified registered rule. It is NOT a restoration of "
                       "the registered rule, and it is NOT an outcome-informed research "
                       "choice.",
        honest_provenance_note="the rationale (never resolve downward) is informed by the "
                               "OBSERVED defect on 2022-06-10, where downward resolution "
                               "admitted a two-bar gapped session. No outcome, Z, theta or "
                               "survivor was visible when this policy was fixed, and no "
                               "corrected claim-level impact had been opened — but the "
                               "policy is still an amendment, and is labelled as one "
                               "rather than presented as 'nothing outside the defect "
                               "changed'.",
        timing=dict(frozen_before=["corrected claim-level impact inspection",
                                   "any corrected Y or statistic computation"],
                    sealed_at=time.strftime("%Y-%m-%d %H:%M %Z")),
        scope="the session-validity reference builder only",
        forbidden=["family-specific tie rules",
                   "outcome-dependent tie resolution",
                   "post-impact modification of this policy"],
        binding="every corrected run must reference THIS policy version by digest",
        outcome_exposure="NOT_EXPOSED")
    d = ART.seal(body, TIE_OUT, required=("spec_id", "policy", "classification"),
                 supersede=os.path.exists(TIE_OUT))
    print(f"SESSION_MODE_TIE_POLICY_V1 · {d}")
    return d


def completeness():
    t0 = time.time()
    CAL = pd.read_parquet(CAL_PARQ)
    conn = duckdb.connect(D5.DB1H, read_only=True)
    T = conn.execute("""
        SELECT CAST(date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS DATE) sess,
               ticker, count(*) n FROM bars GROUP BY sess, ticker""").fetchdf()
    conn.close()
    T["sess"] = T.sess.astype(str)
    freq = []
    for d, g in T.groupby("sess"):
        vc = g.n.value_counts().sort_values(ascending=False)
        freq.append(dict(session_date=d, n_tickers=int(len(g)),
                         mode_freq=int(vc.iloc[0]),
                         second_mode_freq=int(vc.iloc[1]) if len(vc) > 1 else 0,
                         tie=bool(len(vc) > 1 and vc.iloc[0] == vc.iloc[1])))
    F = pd.DataFrame(freq).merge(CAL[["session_date", "calendar_len"]], on="session_date")
    med = float(F.n_tickers.median())
    F["collapsed"] = F.n_tickers < COLLAPSE_FRACTION * med

    # what each family SAW at build time on those dates
    fam_seen = {}
    for fam in ("t5", "t9", "t3", "t1"):
        D = importlib.import_module(f"{fam}_dna")
        M = pd.read_parquet(D.OUT_MS, columns=["ticker", "session_date", "episode_id"])
        g = M.groupby("session_date").agg(tickers_at_build=("ticker", "nunique"),
                                          episodes=("episode_id", "nunique"))
        fam_seen[fam.upper()] = g
    rows = []
    for _, r in F[F.collapsed].iterrows():
        rec = dict(session_date=r.session_date, tickers_now=int(r.n_tickers),
                   expected_bars=int(r.calendar_len),
                   mode_freq=int(r.mode_freq), second_mode_freq=int(r.second_mode_freq),
                   tie=bool(r.tie), families={})
        for fam, g in fam_seen.items():
            if r.session_date in g.index:
                rec["families"][fam] = dict(
                    tickers_at_build=int(g.loc[r.session_date, "tickers_at_build"]),
                    episodes=int(g.loc[r.session_date, "episodes"]))
        lost = any(v["tickers_at_build"] > r.n_tickers * 5 for v in rec["families"].values())
        rec["classification"] = ("DATA_VERSION_INCOMPLETE — the current store holds far "
                                 "fewer tickers than the vintage the frozen microstructure "
                                 "was built from" if lost else
                                 "SPARSE_IN_SOURCE — thin in the store at build time too; "
                                 "no family evidence of loss")
        rows.append(rec)

    # business days with no rows at all
    bd = set(pd.bdate_range(F.session_date.min(), F.session_date.max())
             .strftime("%Y-%m-%d"))
    absent = sorted(bd - set(F.session_date))

    expected_unaffected = all(r["expected_bars"] == 7 for r in rows)
    body = dict(
        spec_id="SESSION_REFERENCE_COMPLETENESS_V1", status="AUDIT",
        purpose="classify the reference universe's thin dates before any corrected freeze; "
                "this is an audit, NOT a new eligibility rule",
        tickers_per_date=dict(
            min=int(F.n_tickers.min()),
            q01=int(F.n_tickers.quantile(.01)), q05=int(F.n_tickers.quantile(.05)),
            median=int(med), q95=int(F.n_tickers.quantile(.95)),
            max=int(F.n_tickers.max())),
        collapse_threshold=f"< {COLLAPSE_FRACTION:.0%} of the median ({int(COLLAPSE_FRACTION*med)})",
        n_collapsed_dates=int(F.collapsed.sum()),
        collapsed_dates=rows,
        ties=[dict(session_date=r.session_date, n_tickers=int(r.n_tickers))
              for _, r in F[F.tie].iterrows()],
        business_days_absent_entirely=dict(
            n=len(absent), last=absent[-12:],
            note="this list mixes genuine market holidays (Presidents Day, Good Friday, "
                 "Memorial Day, Juneteenth, the observed July 4) with recent ingestion "
                 "gaps (2026-07-28, 07-31, 08-07, 08-10, 08-11) — the audit reports them "
                 "together and classifies nothing automatically"),
        effect_on_expected_bars=dict(
            all_collapsed_dates_still_expect_7=bool(expected_unaffected),
            reading="the thin dates do not change any expected_bars value — every one of "
                    "them still resolves to the regular 7-slot session, so no family's "
                    "session-validity classification depends on the collapse"),
        consequence="the completeness question sits ABOVE session-length inference, in the "
                    "data contract: whether a date whose store coverage collapsed after "
                    "the frozen microstructures were built should be admissible in that "
                    "snapshot at all. This audit does not decide it.",
        not_decided=["excluding any date", "rebuilding the 1H store",
                     "pinning a store vintage", "any change to family eligibility"],
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        runtime_s=round(time.time() - t0, 1))
    d = ART.seal(body, COMP_OUT, required=("spec_id", "collapsed_dates",
                                           "effect_on_expected_bars"),
                 supersede=os.path.exists(COMP_OUT))
    print(f"\ntickers/date: min {body['tickers_per_date']['min']} · q01 "
          f"{body['tickers_per_date']['q01']:,} · q05 {body['tickers_per_date']['q05']:,} · "
          f"median {body['tickers_per_date']['median']:,}")
    print(f"collapsed dates: {body['n_collapsed_dates']}")
    for r in rows:
        print(f"  {r['session_date']} · now {r['tickers_now']} · expected {r['expected_bars']}"
              f" · families {r['families'] or '—'}\n      {r['classification'][:100]}")
    print(f"expected_bars unaffected by the collapse: {expected_unaffected}")
    print(f"\nSESSION_REFERENCE_COMPLETENESS_V1 · {d}")


if __name__ == "__main__":
    tie_policy()
    completeness()
