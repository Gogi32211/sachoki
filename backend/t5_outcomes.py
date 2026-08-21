"""Step 4 + 5 — freeze the X layer, then build the outcome sidecar beside it.

THE ORDER IS THE POINT

    X freeze -> OutcomeSpec freeze -> outcome compute -> grammar freeze -> Y expose

Outcomes are computed here and NOT shown. Printing a single MFE median before the sequence
grammar exists would let the outcome decide which sequence definition looks interesting, which
is the same adaptation the whole project spent the week removing — only earlier in the
pipeline, where it is harder to see.

ENTRY, AND THE EDGE CASE THAT MUST NOT BE A DROPPED ROW

    signal_time     T5 regular-session close
    decision_time   after that close
    entry_time      the ticker's next regular session open
    entry_semantics NEXT_SESSION_OPEN_V1

The whole 1H sequence being studied completes before the T5 close, so the earliest honest
entry is the following open. Using the T5 close would price a decision at the moment the last
input to it was still forming.

When there is no next open, the row stays with a reason rather than vanishing:

    TERMINAL_BEFORE_ENTRY       the ticker's history ends at or before the T5 date
    RIGHT_EDGE_NOT_YET_OBSERVED the next session has not happened yet in this snapshot
    DATA_UNAVAILABLE            a gap that is neither of those

Silent `dropna()` here would delete exactly the delisted and the newest episodes, and the
coverage audit already showed this population is selected on liquidity — losing more of it
quietly is how a survivorship bias becomes invisible.

HORIZON SEMANTICS, WRITTEN OUT BECAUSE OFF-BY-ONE IS THE DEFAULT

    horizon_1d   the entry session itself
    horizon_3d   the entry session plus the next two market sessions
    horizon_h    h sessions IN TOTAL, counting the entry session as the first

MATURITY IS THE MARKET CALENDAR, TERMINATION IS THE COMPANY

They are different objects and merging them is the mistake this project already made once:

    NOT_YET_MATURE          h sessions have not elapsed in the snapshot
    TERMINAL_DURING_HORIZON they elapsed, but the ticker's own history stopped inside them
    DATA_GAP                they elapsed, the ticker still trades, and bars are missing

An early peak never earns an immature episode a place in the table.

PRIMARY AND SECONDARY ARE DECLARED NOW, NOT AFTER ONE OF THEM RANKS BETTER

    PRIMARY    MFE in percent
    SECONDARY  MFE normalised by the entry-session ATR

Both are kept because they answer different questions — how large the move was, and how large
it was relative to what that name normally does — and the covered population is measurably
half as volatile as the uncovered one, so the two will not agree.
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
import combo_tokens_spec as TS                                       # noqa: E402
import t5_dna as D                                                   # noqa: E402

OUT_OC = os.path.join(D.ROOT, "data", "t5_episode_outcomes.parquet")
FREEZE = os.path.join(D.HERE, "T5_DNA_X_V1.json")
SPEC = os.path.join(D.HERE, "T5_OUTCOME_SPEC_V1.json")

HORIZONS = (1, 3, 5, 10, 20)
ENTRY_SEMANTICS = "NEXT_SESSION_OPEN_V1"
SNAPSHOT = "2026-08-17"

COVERAGE_ROLE = (
    "1D T5 episodes with observable 1H microstructure — a coverage-selected subset that is "
    "substantially more liquid, higher-priced and lower-volatility than uncovered T5 episodes "
    "(median dollar volume 165x, price 4.0x, ATR%% ratio 0.51)."
)


def _sha(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


# ── 4 · freeze the X layer ───────────────────────────────────────────────────
def freeze_x(E: pd.DataFrame) -> dict:
    has_t5 = E.n_t5_day_1h_bars.fillna(0) > 0
    has_prev = E.n_prev_day_1h_bars.fillna(0) > 0
    fz = dict(
        spec_id="T5_DNA_X_V1",
        setup="FINAL_PRIORITY_RESOLVED_T5",
        t5_definition_hash=D.T5_DEF_HASH,
        t5_definition=D.T5_DEFINITION,
        token_registry_hash=TS.digest(),
        registry_total=len(TS.REGISTRY),
        bars_applicable_tokens=len(D.TOKENS_1H),
        frame_computed_excluded=len(TS.REGISTRY) - len(D.TOKENS_1H),
        full_T5_population=int(len(E)),
        T5_day_1H_observable=int(has_t5.sum()),
        cross_day_observable=int((has_t5 & has_prev).sum()),
        coverage_role="OBSERVED_1H_SUBPOPULATION",
        coverage_statement=COVERAGE_ROLE,
        future_outcomes="FORBIDDEN",
        artifacts={os.path.basename(p): _sha(p) for p in
                   (D.OUT_EP, D.OUT_MS,
                    os.path.join(D.HERE, "T5_DNA_AUDIT.json"),
                    os.path.join(D.HERE, "T5_COVERAGE_BIAS.json"))},
        feature_data_as_of=SNAPSHOT,
    )
    fz["freeze_digest"] = hashlib.sha256(
        json.dumps(fz, sort_keys=True, default=str).encode()).hexdigest()[:16]
    if os.path.exists(FREEZE):
        old = json.load(open(FREEZE))
        if old.get("freeze_digest") != fz["freeze_digest"]:
            raise RuntimeError("T5_DNA_X_V1 exists and differs — the X layer is frozen; a "
                               "changed table is a new version, not a rebuild")
    else:
        with open(FREEZE, "w") as f:
            json.dump(fz, f, indent=2, default=str)
    return fz


# ── 5 · outcome sidecar ──────────────────────────────────────────────────────
def build_outcomes(E: pd.DataFrame, verbose=True) -> pd.DataFrame:
    conn = duckdb.connect(D.DB1D, read_only=True)
    try:
        cal = [r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM bars WHERE universe IN ('sp500','nasdaq','russell2k') "
            "ORDER BY date").fetchall()]
        conn.register("ep", E[["episode_id", "ticker", "t5_date"]])
        mx = ", ".join(
            f"max(CASE WHEN k<={h} THEN high END) hi_{h}, "
            f"min(CASE WHEN k<={h} THEN low END) lo_{h}, "
            f"max(CASE WHEN k={h} THEN close END) cl_{h}, "
            f"max(CASE WHEN k={h} THEN date END) dt_{h}" for h in HORIZONS)
        q = f"""
        WITH b AS (
          SELECT * FROM (
            SELECT ticker, date, open, high, low, close, atr_14,
                   row_number() OVER (PARTITION BY ticker,date ORDER BY
                     CASE universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1 ELSE 2 END) rn
            FROM bars WHERE universe IN ('sp500','nasdaq','russell2k')
          ) WHERE rn=1
        ), f AS (
          SELECT e.episode_id, b.date, b.open, b.high, b.low, b.close,
                 row_number() OVER (PARTITION BY e.episode_id ORDER BY b.date) k
          FROM ep e JOIN b ON b.ticker=e.ticker AND b.date > CAST(e.t5_date AS DATE)
          QUALIFY k <= {max(HORIZONS)}
        )
        SELECT episode_id,
               max(CASE WHEN k=1 THEN open END) entry_price,
               max(CASE WHEN k=1 THEN CAST(date AS VARCHAR) END) entry_session_date,
               count(*) n_future_bars, {mx}
        FROM f GROUP BY 1"""
        F = conn.execute(q).fetchdf()
        # last observed bar per ticker — separates termination from an unfinished horizon
        last = conn.execute(
            "SELECT ticker, max(date) last_bar FROM bars "
            "WHERE universe IN ('sp500','nasdaq','russell2k') GROUP BY 1").fetchdf()
        atr = conn.execute("""
          SELECT ticker, CAST(date AS VARCHAR) t5_date, atr_14 entry_atr FROM (
            SELECT ticker, date, atr_14, row_number() OVER (PARTITION BY ticker,date ORDER BY
              CASE universe WHEN 'sp500' THEN 0 WHEN 'nasdaq' THEN 1 ELSE 2 END) rn
            FROM bars WHERE universe IN ('sp500','nasdaq','russell2k')) WHERE rn=1""").fetchdf()
    finally:
        conn.close()

    O = E[["episode_id", "ticker", "t5_date", "t5_close"]].merge(F, on="episode_id", how="left")
    O = O.merge(last, on="ticker", how="left").merge(atr, on=["ticker", "t5_date"], how="left")
    O["signal_time"] = O["t5_date"] + " regular-session close"
    O["entry_semantics"] = ENTRY_SEMANTICS
    O["entry_atr_pct"] = O["entry_atr"] / O["t5_close"] * 100

    # entry status — every episode keeps a row and a reason
    cal_s = pd.Series(pd.to_datetime(cal))
    last_cal = cal_s.max()
    t5d = pd.to_datetime(O["t5_date"])
    lastbar = pd.to_datetime(O["last_bar"])
    O["entry_status"] = np.where(
        O["entry_price"].notna(), "OK",
        np.where(lastbar <= t5d, "TERMINAL_BEFORE_ENTRY",
                 np.where(t5d >= last_cal, "RIGHT_EDGE_NOT_YET_OBSERVED",
                          "DATA_UNAVAILABLE")))

    # maturity from the GLOBAL calendar, counting the entry session as the first
    pos = pd.Series(np.arange(len(cal_s)), index=cal_s.values)
    ent = pd.to_datetime(O["entry_session_date"])
    ent_pos = ent.map(pos)
    n_cal = len(cal_s)
    for h in HORIZONS:
        mature = (ent_pos + (h - 1)) < n_cal
        O[f"mature_{h}d"] = mature.fillna(False)
        e = O["entry_price"]
        hi, lo, cl = O[f"hi_{h}"], O[f"lo_{h}"], O[f"cl_{h}"]
        O[f"mfe_{h}d"] = hi / e - 1
        O[f"mae_{h}d"] = lo / e - 1
        O[f"ret_{h}d"] = cl / e - 1
        O[f"mfe_atr_{h}d"] = (hi - e) / O["entry_atr"].replace(0, np.nan)
        O[f"mae_atr_{h}d"] = (lo - e) / O["entry_atr"].replace(0, np.nan)
        have = O["n_future_bars"].fillna(0) >= h
        O[f"path_status_{h}d"] = np.select(
            [O["entry_status"] != "OK",
             ~O[f"mature_{h}d"],
             have,
             lastbar < cal_s.iloc[-1]],
            ["NO_NEXT_SESSION_OPEN", "NOT_YET_MATURE", "AVAILABLE",
             "TERMINAL_DURING_HORIZON"],
            default="DATA_GAP")
        # an unavailable horizon carries no number, and its status says why
        bad = O[f"path_status_{h}d"] != "AVAILABLE"
        for c in (f"mfe_{h}d", f"mae_{h}d", f"ret_{h}d", f"mfe_atr_{h}d", f"mae_atr_{h}d"):
            O.loc[bad, c] = np.nan

    O["label_source_snapshot_as_of"] = SNAPSHOT
    O["market_calendar_hash"] = hashlib.sha256(
        "|".join(str(d) for d in cal).encode()).hexdigest()[:16]
    O["price_source_hash"] = D.T5_DEF_HASH
    O = O.drop(columns=[c for c in O.columns
                        if c.startswith(("hi_", "lo_", "cl_", "dt_"))] + ["last_bar"])
    O.to_parquet(OUT_OC, index=False, compression="zstd")
    return O


def main():
    t0 = time.time()
    E = pd.read_parquet(D.OUT_EP)
    fz = freeze_x(E)
    print(f"4 · X LAYER FROZEN  {fz['freeze_digest']}")
    print(f"    T5 def {fz['t5_definition_hash']} · registry {fz['token_registry_hash']} "
          f"({fz['registry_total']} -> {fz['bars_applicable_tokens']} +"
          f"{fz['frame_computed_excluded']} excluded)")
    print(f"    population {fz['full_T5_population']:,} · 1H-observable "
          f"{fz['T5_day_1H_observable']:,} · cross-day {fz['cross_day_observable']:,}")
    for k, v in fz["artifacts"].items():
        print(f"      {k:<34} {v}")

    O = build_outcomes(E)
    spec = dict(spec_id="T5_OUTCOME_SPEC_V1", entry_semantics=ENTRY_SEMANTICS,
                signal_time="T5 regular-session close",
                decision_time="after T5 regular-session close",
                entry_time="ticker's next regular session open",
                horizons=list(HORIZONS),
                horizon_semantics="horizon_h spans h sessions IN TOTAL, the entry session "
                                  "counted as the first",
                primary_outcome="mfe_{h}d — MFE in percent",
                secondary_outcome="mfe_atr_{h}d — MFE normalised by entry-session ATR",
                mae="mae_{h}d and mae_atr_{h}d",
                forward_close="ret_{h}d = close_h / entry_open - 1",
                exit_policy="NONE — pure price potential. A trailing policy answers a "
                            "different question and is a separate outcome family.",
                maturity="global market calendar; termination is a separate status",
                path_status_enum=["AVAILABLE", "NOT_YET_MATURE", "NO_NEXT_SESSION_OPEN",
                                  "TERMINAL_DURING_HORIZON", "DATA_GAP", "INVALID_ENTRY"],
                snapshot_as_of=SNAPSHOT, x_freeze_digest=fz["freeze_digest"],
                exposure="NOT_EXPOSED — no value, median, quantile or ranking may be read "
                         "until SEQUENCE_GRAMMAR_V1 is frozen")
    spec["spec_digest"] = hashlib.sha256(
        json.dumps(spec, sort_keys=True, default=str).encode()).hexdigest()[:16]
    with open(SPEC, "w") as f:
        json.dump(spec, f, indent=2, default=str)

    print(f"\n5 · OUTCOME SIDECAR  {spec['spec_digest']}  ({ENTRY_SEMANTICS})")
    print(f"    rows {len(O):,} · {os.path.getsize(OUT_OC)/1e6:.1f} MB")
    print(f"    entry_status: " + " · ".join(
        f"{k}={v:,}" for k, v in O.entry_status.value_counts().items()))
    for h in HORIZONS:
        vc = O[f"path_status_{h}d"].value_counts()
        print(f"    h={h:>2}d  " + " · ".join(f"{k}={v:,}" for k, v in vc.items()))
    print(f"\n    NO OUTCOME VALUE PRINTED — exposure gated on SEQUENCE_GRAMMAR_V1")
    print(f"    {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
