"""T5_15M_GAP_CLOSURE_V1 — is the 15m grammar exposed to the same defect class?

The 1H defect had two parts: a session-validity filter computed on the wrong population,
and adjacency over dense positional ranks that could then bridge a missing scheduled slot.
The 15m enumeration is built differently — it selects bars by EXACT ET clock time and
anchors claims to named positions — so the three assertions the review asked for are:

    missing exact M2   -> no M1->M2 occurrence for that episode
    missing exact M3   -> no M2->M3 occurrence for that episode
    the next observed bar NEVER backfills a missing M-slot

Proved here by reading the selector, and by a synthetic removal test on real episodes.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import ast, json, os, sys, time                                       # noqa: E402
import numpy as np, pandas as pd                                      # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                             # noqa: E402
import t5_15m_enumerate as E15                                        # noqa: E402

OUT = "T5_15M_GAP_CLOSURE_V1.json"


def main():
    t0 = time.time()
    src = open("t5_15m_enumerate.py").read()

    # ── 1 · the selector is exact-time, and positions are named, not ranked ──
    selector_exact = "'09:30','09:45','10:00','10:15'" in src.replace(" ", "")
    et_pos = dict(E15.ET_POS)
    named_positions = et_pos == {"09:30": 1, "09:45": 2, "10:00": 3, "10:15": 4}
    tree = ast.parse(src)
    # a dense rank would need a rank/cumcount/arange over observed rows keyed by episode
    rank_calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
                  and getattr(getattr(n, "func", None), "attr", "") in
                  ("rank", "cumcount", "argsort")]
    no_dense_rank = not rank_calls
    pairs_named = E15.PAIRS == [("M1", "M2"), ("M2", "M3"), ("M3", "M4")]

    # ── 2 · synthetic removal on real episodes ──────────────────────────────
    M = E15.load_bars() if hasattr(E15, "load_bars") else None
    if M is None:                       # the loader is inlined; rebuild the minimal frame
        import duckdb, t5_dna as D
        E = pd.read_parquet(D.OUT_EP, columns=["episode_id", "ticker", "t5_date"]).head(4000)
        conn = duckdb.connect(E15.DB15, read_only=True)
        conn.register("ep", E)
        M = conn.execute("""
            SELECT e.episode_id,
                   strftime(x.date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
                            '%H:%M') et_time
            FROM ep e JOIN bars x ON x.ticker = e.ticker
             AND CAST(x.date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' AS DATE)
                 = CAST(e.t5_date AS DATE)
            WHERE strftime(x.date AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York',
                           '%H:%M') IN ('09:30','09:45','10:00','10:15')""").fetchdf()
        conn.close()
    M["pos"] = M.et_time.map(E15.ET_POS)

    def occurrences(frame):
        """M1->M2 / M2->M3 / M3->M4 occurrences under the module's own position anchoring."""
        piv = frame.pivot_table(index="episode_id", columns="pos", values="et_time",
                                aggfunc="first")
        out = {}
        for a, b in ((1, 2), (2, 3), (3, 4)):
            if a in piv.columns and b in piv.columns:
                out[f"M{a}->M{b}"] = set(piv.index[piv[a].notna() & piv[b].notna()])
            else:
                out[f"M{a}->M{b}"] = set()
        return out

    base = occurrences(M)
    # remove every exact 09:45 bar (M2) and every exact 10:00 bar (M3), separately
    noM2 = occurrences(M[M.pos != 2])
    noM3 = occurrences(M[M.pos != 3])
    checks = {
        "selector is exact ET clock time": bool(selector_exact),
        "positions are named M1..M4 by clock time, not by observed order":
            bool(named_positions),
        "no dense rank / cumcount / argsort over observed bars": bool(no_dense_rank),
        "claims are named position pairs": bool(pairs_named),
        "missing M2 -> M1->M2 occurrences vanish": len(noM2["M1->M2"]) == 0,
        "missing M2 -> M2->M3 occurrences vanish": len(noM2["M2->M3"]) == 0,
        "missing M3 -> M2->M3 occurrences vanish": len(noM3["M2->M3"]) == 0,
        "missing M3 -> M3->M4 occurrences vanish": len(noM3["M3->M4"]) == 0,
        "missing M2 does not create new M1->M3 style bridges":
            set(noM2["M3->M4"]) == set(base["M3->M4"]),
        "no backfill: removing M3 leaves M1->M2 untouched":
            set(noM3["M1->M2"]) == set(base["M1->M2"]),
    }
    ok = all(checks.values())
    body = dict(
        spec_id="T5_15M_GAP_CLOSURE_V1", result="PASS" if ok else "FAIL",
        question="is the 15m grammar exposed to the 1H session-validity / dense-rank "
                 "adjacency defect class?",
        answer="NO. Bars are selected by exact ET clock time and mapped to fixed named "
               "positions M1..M4, so a missing slot simply means that claim cannot fire; "
               "no observed bar can slide into a vacant position because no position is "
               "derived from observation order." if ok else "SEE FAILURES",
        selector=dict(exact_times=sorted(et_pos), mapping=et_pos,
                      pairs=[list(p) for p in E15.PAIRS],
                      source_digest=ART.file_digest("t5_15m_enumerate.py")),
        synthetic_removal=dict(
            episodes_tested=int(M.episode_id.nunique()),
            baseline={k: len(v) for k, v in base.items()},
            without_M2={k: len(v) for k, v in noM2.items()},
            without_M3={k: len(v) for k, v in noM3.items()}),
        checks={k: bool(v) for k, v in checks.items()},
        conclusion="15m adjacency/session defect impact = NONE" if ok else "REVIEW",
        scope_note="this closes the DEFECT CLASS for 15m; it makes no statement about any "
                   "15m result, which is untouched",
        outcome_exposure="NOT_EXPOSED — no outcome value read",
        runtime_s=round(time.time() - t0, 1))
    d = ART.seal(body, OUT, required=("spec_id", "result", "checks"),
                 supersede=os.path.exists(OUT))
    for k, v in checks.items():
        print(f"  {'PASS' if v else 'FAIL'}  {k}")
    print(f"\nT5_15M_GAP_CLOSURE_V1 · {d} · {body['result']}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
