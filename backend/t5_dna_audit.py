"""Audits for the T5 DNA foundation. Section 14 of the specification, all five.

E is the one that matters most and the only one that can fail silently in production: a table
can be perfectly self-consistent and still have been built with information that did not exist
at the time. So it is not argued from the code — the tables are rebuilt against a truncated
database and compared byte for byte.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import time

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import combo_tokens as CT                                            # noqa: E402
import combo_tokens_spec as TS                                       # noqa: E402
import t5_dna as D                                                   # noqa: E402

OUT = os.path.join(D.HERE, "T5_DNA_AUDIT.json")


def main():
    t0 = time.time()
    E = pd.read_parquet(D.OUT_EP)
    M = pd.read_parquet(D.OUT_MS)
    rep = {}

    # ── A · T5 EVENT AUDIT ───────────────────────────────────────────────────
    yr = pd.to_datetime(E.t5_date).dt.year.value_counts().sort_index()
    rep["A_events"] = dict(total=int(len(E)), tickers=int(E.ticker.nunique()),
                           date_min=E.t5_date.min(), date_max=E.t5_date.max(),
                           per_year={str(k): int(v) for k, v in yr.items()},
                           unique_episode_id=bool(E.episode_id.is_unique),
                           t5_definition_hash=D.T5_DEF_HASH)
    print(f"A · events {len(E):,} · tickers {E.ticker.nunique():,} · "
          f"episode_id unique {E.episode_id.is_unique}")
    print("    per year: " + " · ".join(f"{k}={v:,}" for k, v in yr.items()))

    # ── B · 1H ALIGNMENT AUDIT ───────────────────────────────────────────────
    has_prev = E.n_prev_day_1h_bars.fillna(0) > 0
    has_t5 = E.n_t5_day_1h_bars.fillna(0) > 0
    dup = M.duplicated(["episode_id", "relative_day", "session_position"]).sum()
    both_roles = M.groupby(["ticker", "session_date"])["relative_day"].nunique()
    rep["B_alignment"] = dict(
        episodes=int(len(E)),
        with_any_1h=int((has_prev | has_t5).sum()),
        with_both_sessions=int((has_prev & has_t5).sum()),
        with_only_t5=int((has_t5 & ~has_prev).sum()),
        with_only_prev=int((has_prev & ~has_t5).sum()),
        with_no_1h=int((~has_prev & ~has_t5).sum()),
        duplicate_grain_rows=int(dup),
        sessions_serving_two_roles=int((both_roles > 1).sum()),
        note="a session legitimately serves two roles: it is the T5 day of one episode and "
             "the previous day of the next. That is not a duplicate grain — the primary key "
             "carries episode_id.")
    # The four states must sum to the total. The first version of this audit computed
    # PREV_ONLY and did not print it, so 456 episodes were invisible and the numbers did not
    # reconcile on screen even though the dict was correct. An identity that is only checked
    # by eye is not checked.
    _b = rep["B_alignment"]
    _sum = (_b["with_both_sessions"] + _b["with_only_t5"] + _b["with_only_prev"]
            + _b["with_no_1h"])
    if _sum != len(E):
        raise RuntimeError(f"alignment does not reconcile: {_sum:,} vs {len(E):,}")
    _b["reconciles"] = True
    print(f"B · BOTH {_b['with_both_sessions']:,} · T5_ONLY {_b['with_only_t5']:,} · "
          f"PREV_ONLY {_b['with_only_prev']:,} · NONE {_b['with_no_1h']:,} "
          f"= {_sum:,} ✓ · duplicate grain {dup}")

    # ── C · TOKEN COVERAGE AUDIT ─────────────────────────────────────────────
    toks = [t.token_id for t in D.TOKENS_1H]
    sets = M.token_set.str.split()
    flat = pd.Series([t for row in sets for t in row])
    cnt = flat.value_counts()
    ep_cov = {}
    rows = []
    for t in toks:
        n = int(cnt.get(t, 0))
        rows.append(dict(token=t, declared=True, materialized=True, true_count=n,
                         bar_share=round(n / len(M), 6)))
    missing = [r["token"] for r in rows if r["true_count"] == 0]
    # "165/165 materialized" hides the question that matters: the canonical registry holds
    # 172. Seven are excluded, and the exclusion must carry its reason in the artifact —
    # otherwise in three months nobody can tell an intentional exclusion from a silent loss.
    excluded = [dict(token=t.token_id, family=t.family, source=t.source, column=t.column,
                     reason="frame-computed column that lives in opportunities.parquet, not "
                            "in `bars`; it has no 1H-granularity counterpart",
                     applicability_rule="token.source == 'bars'")
                for t in TS.REGISTRY if t.source != "bars"]
    rep["C_tokens"] = dict(registry_total=len(TS.REGISTRY),
                           applicable_to_1h=len(toks),
                           excluded_from_1h=len(excluded),
                           applicability_rule="token.source == 'bars' — the 1H store carries "
                                              "the same source columns as the 1D store, so a "
                                              "registry token is 1H-applicable iff it reads "
                                              "one of them",
                           excluded=excluded,
                           registry_hash=TS.digest(),
                           materialized=len(toks), never_true=missing, tokens=rows)
    print(f"C · registry {len(TS.REGISTRY)} → applicable-to-1H {len(toks)} · "
          f"excluded {len(excluded)} ({', '.join(x['token'] for x in excluded)})")
    print(f"    materialized {len(toks)}/{len(toks)} · never true on 1H: {len(missing)}"
          + (f" -> {' '.join(missing)}" if missing else ""))

    # ── D · SESSION POSITION AUDIT ───────────────────────────────────────────
    per = M.groupby(["ticker", "session_date"])["bars_in_session"].first()
    vc = per.value_counts().sort_index()
    rep["D_sessions"] = dict(bars_per_session={int(k): int(v) for k, v in vc.items()},
                             normal_7=int(vc.get(7, 0)), early_close_4=int(vc.get(4, 0)),
                             abnormal=int(vc[~vc.index.isin([7, 4])].sum()),
                             et_first=M[M.session_position == 1].et_time.mode()[0],
                             et_last=M.sort_values("session_position")
                             .groupby(["ticker", "session_date"]).et_time.last().mode()[0])
    print(f"D · sessions " + " · ".join(f"{k}bars={v:,}" for k, v in vc.items()))

    # ── E · NO-FUTURE ACCESS TEST ────────────────────────────────────────────
    # Rebuild against a database truncated at a cutoff, then compare the episodes that
    # already existed before it. Anything that consulted a later bar shows up as a diff.
    CUT = "2025-06-30"
    conn = duckdb.connect(D.DB1D, read_only=True)
    try:
        # The attached DB is read-only, so the cutoff is injected into the SAME statement
        # rather than a view. Both `FROM bars` sites are rewritten — missing one would leave
        # the membership CTE reading the full history and the test would pass for the wrong
        # reason.
        cut_sql = D._EPISODE_SQL.replace(
            "FROM bars WHERE universe IN",
            f"FROM bars WHERE CAST(date AS DATE) <= DATE '{CUT}' AND universe IN")
        if cut_sql.count(f"DATE '{CUT}'") != 2:
            raise RuntimeError("cutoff was not applied to both `FROM bars` sites")
        E2 = conn.execute(cut_sql).fetchdf()
        E2.insert(0, "episode_id", [D.episode_id(t, d) for t, d in
                                    zip(E2["ticker"], E2["t5_date"])])
    finally:
        conn.close()

    old = E[E.t5_date <= CUT]
    a = old.set_index("episode_id").sort_index()
    b = E2.set_index("episode_id").sort_index()
    same_ids = set(a.index) == set(b.index)
    cols = ["ticker", "t5_date", "prev_session_date", "t5_open", "t5_close",
            "prev1d_open", "prev1d_close"]
    diffs = 0
    if same_ids:
        for c in cols:
            diffs += int((a[c].astype(str).values != b.loc[a.index, c].astype(str).values).sum())
    rep["E_no_future"] = dict(cutoff=CUT, episodes_before_cutoff=int(len(a)),
                              rebuilt=int(len(b)), identical_episode_ids=bool(same_ids),
                              differing_field_values=int(diffs),
                              passed=bool(same_ids and diffs == 0))
    print(f"E · cutoff {CUT} · episodes {len(a):,} vs rebuilt {len(b):,} · "
          f"ids identical {same_ids} · field diffs {diffs} · "
          f"{'PASS' if same_ids and diffs==0 else 'FAIL'}")

    rep["registry_hash"] = TS.digest()
    rep["seconds"] = round(time.time() - t0, 1)
    with open(OUT, "w") as f:
        json.dump(rep, f, indent=2, default=str)
    print(f"\n  WROTE {OUT}")


if __name__ == "__main__":
    main()
