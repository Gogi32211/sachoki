"""GANN_X_FREEZE_V1 — the X evidence state, bound to the POST-FIX build only.

Everything downstream reads this artifact, never the pre-fix numbers. The build that
produced 146,140 / 143,694 / 110,354 events is recorded here as SUPERSEDED_PRE_FIX_X_BUILD
so that no consumer can reach for it by accident: it predates the onset-observability rule
(missing is not FALSE), the confluence consensus direction, and the multiplicative level
price.

No Y value is read anywhere in this module.
"""
from __future__ import annotations
import hashlib, json, os, sys, time                                  # noqa: E402
import pandas as pd                                                  # noqa: E402
HERE = os.path.dirname(os.path.abspath(__file__)); os.chdir(HERE); sys.path.insert(0, HERE)
import t5_artifact as ART                                            # noqa: E402
import gann_grid as G                                                # noqa: E402

OUT = "GANN_X_FREEZE_V1.json"


def main():
    E = pd.read_parquet(G.OUT_EVENTS)
    Gr = pd.read_parquet(G.OUT_GRID)
    C = E[E.claim_id == "GANN_CONFLUENCE_TOUCH_V1"]
    per = {c: int((E.claim_id == c).sum()) for c in sorted(E.claim_id.unique())}
    body = dict(
        spec_id="GANN_X_FREEZE_V1", status="FROZEN", family_id="GANN_VIBRATION_GRID_V1",
        phase="X ONLY — no outcome, no Z, no ranking",
        construction=dict(
            hist_lookback=G.HIST_LOOKBACK, auto_levels=G.AUTO_LEVELS,
            min_bars_per_step=G.MIN_BARS_PER_STEP, onset_quiet_bars=G.ONSET_QUIET_BARS,
            anchors="L = min Low, H = max High in the 504-session window ending D-1, ties "
                    "to the MOST RECENT occurrence",
            price_vibration="M = (H/L)^(1/6)",
            time_vibration="B = max(20, floor(|tH-tL|/6))",
            families="BOTH anchored at the LOW: asc L*M^(i+dt/B), desc L*M^(i-dt/B); H and "
                     "tH calibrate M and B only",
            levels="integer lattice i in Z, solved analytically; half steps are not V1 "
                   "evidence",
            level_price_form="L * M**e — never exp(logL + e*logM), which is off by an ulp "
                             "even at e = 0",
            pit="the grid on D is frozen on data through D-1; D's own H/L/C never enter "
                "the choice of L, H, tL, tH, M or B",
            onset="a family touch opens an opportunity only when the claim state is "
                  "OBSERVABLE on D and on each of D-1..D-5 and FALSE on all five; missing "
                  "is not FALSE",
            direction="Close[D-1] vs the same line at D-1; exact equality is INVALID",
            confluence_direction="CONSENSUS — LONG only if both families say LONG, SHORT "
                                 "only if both say SHORT, INVALID_CONFLICT on "
                                 "disagreement, INVALID_EQUALITY if either line sits on "
                                 "Close[D-1]"),
        bound_artifacts=dict(
            b_rule=ART.file_digest("GANN_B_RULE_V1.json"),
            x_integrity=ART.file_digest("GANN_X_INTEGRITY_V1.json"),
            x_audit=ART.file_digest("GANN_GRID_X_AUDIT_V1.json"),
            design_exposure=ART.file_digest("GANN_DESIGN_EXPOSURE_LEDGER_V1.json"),
            builder_code=ART.file_digest("gann_grid.py"),
            integrity_code=ART.file_digest("gann_x_integrity.py")),
        tables=dict(
            events=os.path.basename(G.OUT_EVENTS),
            events_digest=ART.file_digest(G.OUT_EVENTS), events_rows=int(len(E)),
            grid=os.path.basename(G.OUT_GRID),
            grid_digest=ART.file_digest(G.OUT_GRID), grid_rows=int(len(Gr))),
        claims=dict(
            k=3, ids=sorted(E.claim_id.unique()), events=per,
            grain="(ticker, claim_id, decision_date) — duplicate grain hard-fails",
            multiplicity="k = 3 is FIXED; no claim may be dropped later on support, and "
                         "support is a validity diagnostic, never a selection gate"),
        confluence_census={k: int(v) for k, v in C.direction.value_counts().items()},
        confluence_directional_valid=int(C.direction.isin(["LONG", "SHORT"]).sum()),
        universe=dict(tickers_with_grid=int(Gr.ticker.nunique()),
                      decision_window=[str(Gr.date.min())[:10], str(Gr.date.max())[:10]],
                      scope="evidence applies to the 504-session-observable subpopulation "
                            "over the resulting eligible historical decision window"),
        superseded_pre_fix_x_build=dict(
            events=dict(GANN_ASC_TOUCH_V1=146140, GANN_DESC_TOUCH_V1=143694,
                        GANN_CONFLUENCE_TOUCH_V1=110354),
            status="SUPERSEDED_PRE_FIX_X_BUILD",
            why=["onset treated an unobservable prior day as a quiet day",
                 "confluence direction used the nearest line instead of consensus",
                 "level prices used exp(logL + e*logM), an ulp off at e = 0"],
            usable_downstream=False),
        y_status="NO OUTCOME COLUMN EXISTS IN THESE TABLES",
        outcome_exposure="NOT_EXPOSED",
        sealed_at=time.strftime("%Y-%m-%d %H:%M %Z"))
    d = ART.seal(body, OUT, required=("spec_id", "construction", "tables", "claims"),
                 supersede=os.path.exists(OUT))
    print(f"GANN_X_FREEZE_V1 · {d}")
    print(f"  events {len(E):,} · {per}")
    print(f"  confluence valid {body['confluence_directional_valid']:,} of "
          f"{sum(body['confluence_census'].values()):,}")


if __name__ == "__main__":
    main()
