"""OPENING_VOLUME_DYNAMICS — PRE-OUTCOME DESIGN AMENDMENT V2 · X-only builder (30m/60m layer, Logic 3/4).

Provenance: POST_V1_SEAL · PRE_OUTCOME · PRE_PATHSIM · USER_DIRECTED_HYPOTHESIS_EXPANSION.
V1 is immutable: nothing here writes into V1 files or edits ovd_build.py — V1 guards are imported and
reused. No `_pathsim`, no forward return, no outcome column, no local intraday exit engine.

Sources
  • the V1-sealed X.parquet (digest-bound to SEAL.json) — every V1 column is carried byte-identical
  • the canonical 1D derived parquet (digest-bound) — session calendar + closes for DECLINE_CONTEXT
  • the 15m store — the ONLY source of every 30m / 60m window. The 1H store is NOT attached: the stored
    15:30 1H bar is a 30-minute bar and can never stand in for the last hour.

Windows (America/New_York start slots, each constituent 15m bar REQUIRED, missing -> UNAVAILABLE):
  OPEN30_A = 09:30+09:45   OPEN30_B = 10:00+10:15   OPEN60 = OPEN30_A+OPEN30_B  (== V1 M1..M4 sum)
  CLOSE30_A = 15:00+15:15  CLOSE30_B = 15:30+15:45  CLOSE60 = CLOSE30_A+CLOSE30_B
  early-close session -> CLOSE30_A / CLOSE30_B / CLOSE60 = UNAVAILABLE (no shorter-session equivalent)
RVOL: same-slot, median of the PRIOR 20 regular-session observations, current session excluded.
"""
from __future__ import annotations
import os, sys, json, time, hashlib, datetime as dt                     # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import ovd_build as B                                                   # noqa: E402  (V1 guards, reused not edited)
from ovd_build import HardStop, RunSpace, LOOKBACK                      # noqa: E402
import ovd_registry_v2 as R2                                            # noqa: E402

FAMILY_DIR = B.FAMILY_DIR
DATA = B.DATA
V1_SEAL_SHA256 = R2.V1_SEAL_SHA256
SLOTS_V2 = ["09:30", "09:45", "10:00", "10:15", "15:00", "15:15", "15:30", "15:45"]
WINDOWS = {"open30_a": ("09:30", "09:45"), "open30_b": ("10:00", "10:15"),
           "close30_a": ("15:00", "15:15"), "close30_b": ("15:30", "15:45")}
RVOL_WINDOWS = ["open30_a", "open30_b", "close30_a", "close30_b", "open60", "close60"]
HANDOFF_BAND = (0.80, 1.25)        # NEW_RESEARCH_SPEC — frozen; never optimised after outcome access
DECLINE_LAG = 5                    # DECLINE_CONTEXT(D) = close(D) < close(D-5)
EVENT_RVOL, EVENT_MAX_LOOKBACK, EVENT_MIN_SEP = 2.0, 30, 6   # V1 event authority, unchanged (NEW_RESEARCH_SPEC)
EARLY_CLOSE_SHARE = 0.05           # session is early-close when <5% of its 09:30 tickers print a 15:45 bar
ENTRY_OFFSET = R2.ENTRY_OFFSET


def _dig(p: str) -> str:
    return hashlib.sha256(open(p, "rb").read()).hexdigest()[:16]


# ── V2 guards (each a function so fixtures hit it directly) ────────────────────────────────
def assert_slot_source(bar: dict) -> None:
    """SOURCE IDENTITY: a 30m/60m window is built ONLY from 15-minute bars of the 15m store."""
    if bar.get("store") != "15m" or bar.get("tf") != "15m" or int(bar.get("minutes", 0)) != 15:
        raise HardStop(f"SOURCE_IDENTITY: {bar} is not a 15m-store 15-minute bar — the stored 15:30 1H bar "
                       f"(30 minutes) is never a closing window or a CLOSE30_B source")


def window_from_bars(bars: list[dict], required: tuple[str, str]):
    """Exact sum of the two required NY-start slots. A missing / None constituent -> None (UNAVAILABLE),
    never 0; a duplicated slot -> HARD STOP (a bar can never be doubled)."""
    for b in bars:
        assert_slot_source(b)
    starts = [b["start_ny"] for b in bars]
    if len(set(starts)) != len(starts):
        raise HardStop(f"duplicate 15m slot in window input: {starts}")
    by = {b["start_ny"]: b.get("volume") for b in bars}
    vals = [by.get(s) for s in required]
    if any(v is None for v in vals):
        return None
    return vals[0] + vals[1]


def hour_from_halves(a, b):
    """OPEN60 / CLOSE60 = exact sum of the two half-hours; either UNAVAILABLE -> UNAVAILABLE."""
    return None if a is None or b is None else a + b


def assert_hour_conservation(hour, a, b, what: str) -> None:
    """HARD FAIL when an hour value disagrees with its two halves (fixtures 8 / 9)."""
    expect = hour_from_halves(a, b)
    if hour != expect:
        raise HardStop(f"{what}: {hour} != {a} + {b} (= {expect})")


def close_windows(early_close: bool, a, b):
    """Early-close session -> CLOSE30_A / CLOSE30_B / CLOSE60 = UNAVAILABLE (fixture 10)."""
    if early_close:
        return None, None, None
    return a, b, hour_from_halves(a, b)


def assert_denominator_slot(numerator_slot: str, denominator_slot: str) -> None:
    """Same-slot identity (fixture 12): OPEN30 history never normalises CLOSE30, etc."""
    if numerator_slot != denominator_slot:
        raise HardStop(f"same-slot identity violated: {numerator_slot} normalised by {denominator_slot} history")


def rvol_sql(col: str) -> str:
    """median over the PRIOR 20 available regular-session observations of the SAME window; current
    session excluded (V1 guard assert_no_self_in_denominator re-used, fixture 11)."""
    w = f"OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN {LOOKBACK} PRECEDING AND 1 PRECEDING)"
    B.assert_no_self_in_denominator(f"rvol_{col}", w)
    assert_denominator_slot(col, col)
    return f"CASE WHEN count({col}) {w} >= {LOOKBACK} THEN {col} / median({col}) {w} END"


# ── the six state predicates as pure functions (None = UNAVAILABLE, never False) ──────────
def _missing(*xs) -> bool:
    return any(x is None for x in xs)


def open30_sustained_build_3d(a2, a1, a0, b2, b1, b0):
    if _missing(a2, a1, a0, b2, b1, b0):
        return None
    return bool(a2 < a1 < a0 and b2 < b1 < b0)


def full_opening_reclaim_30(reclaim_a, reclaim_b):
    if _missing(reclaim_a, reclaim_b):
        return None
    return bool(reclaim_a >= 1.0 and reclaim_b >= 1.0)


def close60_dominance(decline, rvol_close60, close60, open60):
    if _missing(decline, rvol_close60, close60, open60) or open60 <= 0:
        return None
    return bool(decline and rvol_close60 > 1.0 and close60 / open60 >= 1.0)


def close30_dominance(decline, rvol_close30_b, close30_b, open30_a):
    if _missing(decline, rvol_close30_b, close30_b, open30_a) or open30_a <= 0:
        return None
    return bool(decline and rvol_close30_b > 1.0 and close30_b / open30_a >= 1.0)


def handoff(decline_D, rvol_close_D, rvol_open_D1, raw_ratio):
    """Logic 4 (60m and 30m share the predicate shape). Any missing input -> None (a missing D+1
    opening window is UNAVAILABLE, not FALSE); FALSE only when every input is valid."""
    if _missing(decline_D, rvol_close_D, rvol_open_D1, raw_ratio):
        return None
    lo, hi = HANDOFF_BAND
    return bool(decline_D and rvol_close_D > 1.0 and rvol_open_D1 > 1.0 and lo <= raw_ratio <= hi)


def claim_status(in_sample: bool, flag) -> str:
    """V2.1 per-cell status: a row outside the claim's bound sample is NOT_ELIGIBLE for that claim
    (never a FALSE of another family); inside the sample the state is TRUE / FALSE / UNAVAILABLE."""
    if not in_sample:
        return "NOT_ELIGIBLE"
    if flag is None:
        return "UNAVAILABLE"
    return "TRUE" if flag else "FALSE"


def assert_entry_offset(claim: str, entry_offset_from_D: int) -> None:
    """CAUSAL TIME (fixtures 21/22): Logic-4 states are known at 10:30 of D+1, so the sacred DAILY
    _pathsim entry is D+2 open — never D+1."""
    need = ENTRY_OFFSET[claim]
    if entry_offset_from_D < need:
        raise HardStop(f"CAUSAL_TIME: {claim} is known at 10:30 of D+{need - 1}; entry must be D+{need} open, not D+{entry_offset_from_D}")


# ── inputs bound to V1 ─────────────────────────────────────────────────────────────────────
def v1_seal() -> dict:
    p = os.path.join(FAMILY_DIR, "SEAL.json")
    if not os.path.exists(p):
        raise HardStop("V1 SEAL.json missing")
    if hashlib.sha256(open(p, "rb").read()).hexdigest() != V1_SEAL_SHA256:
        raise HardStop("V1 SEAL.json bytes changed — V1 must remain immutable provenance")
    return json.load(open(p))


# ── build ──────────────────────────────────────────────────────────────────────────────────
def build(run: RunSpace | None = None) -> dict:
    import duckdb
    B.assert_no_pathsim_copy(__file__)
    seal = v1_seal()
    x1 = os.path.join(FAMILY_DIR, "runs", seal["x_run"], "X.parquet")
    if _dig(x1) != seal["x_parquet_sha256_16"]:
        raise HardStop("V1 X.parquet digest != SEAL.json")
    cur = B.canonical_current()
    if cur["run_id"] != seal["canonical_1d"]["run_id"] or cur["canonical_sha256_16"] != seal["canonical_1d"]["canonical_sha256_16"] \
            or cur["derived_sha256_16"] != seal["canonical_1d"]["derived_sha256_16"]:
        raise HardStop("canonical 1D identity changed since the V1 seal")
    _, reg1 = R2.v1_sealed()
    enum = R2.enumerate_v2(reg1)
    cells_new = [c for c in enum["cells"] if c.get("kind") == "state"]
    R2.assert_registry_covers(list(R2.CLAIM_COLUMNS), cells_new)
    run = run or RunSpace(run_id=time.strftime("OVDV2_%Y%m%dT%H%M%SZ", time.gmtime()))
    t0 = time.time(); rep = dict(run_id=run.run_id, parent_v1_seal_sha256=V1_SEAL_SHA256, x1_run=seal["x_run"],
                                 x1_sha256_16=seal["x_parquet_sha256_16"], canonical_1d=seal["canonical_1d"], joins={},
                                 registry_version="V2_1", bindings_sha256_16=enum["bindings_sha256_16"], cells_sha256_16=enum["cells_sha256_16"],
                                 k_enumerated=enum["k"], hard_stop_for_review=enum["hard_stop_for_review"])
    con = duckdb.connect(); con.execute("pragma threads=8")
    m15 = os.path.join(DATA, "studio_15m.duckdb")
    con.execute(f"ATTACH '{m15}' AS m15 (READ_ONLY)")          # the 1H store is deliberately NOT attached
    ny = B.ny_local("b.date"); clock = f"strftime({ny}, '%H:%M')"
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE slots AS
      SELECT b.ticker, CAST({ny} AS DATE) AS session, {B.slot_identity(clock, SLOTS_V2)} AS slot, b.volume
      FROM m15.bars b
      WHERE b.volume > 0 AND {clock} IN ({','.join(repr(s) for s in SLOTS_V2)})""")
    B.assert_unique(con, "SELECT * FROM slots", "ticker, session, slot")
    slots_p = os.path.join(run.dir, "slots_v2.parquet")
    con.execute(f"COPY (SELECT * FROM slots ORDER BY ticker, session, slot) TO '{slots_p}' (FORMAT PARQUET)")
    n_slots, n_tk, s_min, s_max = con.execute("SELECT count(*), count(DISTINCT ticker), min(session), max(session) FROM slots").fetchone()
    rep["source_15m"] = dict(physical_path=os.path.realpath(m15), bytes=os.path.getsize(m15),
                             mtime_utc=dt.datetime.utcfromtimestamp(os.path.getmtime(m15)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                             slots=SLOTS_V2, slot_rows=n_slots, tickers=n_tk, session_range=[str(s_min), str(s_max)],
                             slots_extract_parquet=os.path.basename(slots_p), slots_extract_sha256_16=_dig(slots_p),
                             h1_store_attached=False)
    # early-close sessions from the cross-section (NYSE half days: <5% of the day's 09:30 tickers print 15:45)
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE early_close AS
      SELECT session FROM (SELECT session, count(DISTINCT ticker) FILTER (WHERE slot='09:30') AS n_open,
                                   count(DISTINCT ticker) FILTER (WHERE slot='15:45') AS n_last FROM slots GROUP BY 1)
      WHERE n_open >= 100 AND n_last < {EARLY_CLOSE_SHARE} * n_open""")
    rep["early_close_sessions"] = [str(r[0]) for r in con.execute("SELECT session FROM early_close ORDER BY 1").fetchall()]
    # per-ticker calendar = canonical sessions, so D-1 / D-2 / D+1 mean TRADING SESSIONS, never 15m rows
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE cal AS
      SELECT ticker, session_date AS session, close FROM read_parquet('{cur["derived_parquet"]}')
      WHERE ticker IN (SELECT DISTINCT ticker FROM slots)""")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE piv AS
      SELECT ticker, session,
             max(CASE WHEN slot='09:30' THEN volume END) v0930, max(CASE WHEN slot='09:45' THEN volume END) v0945,
             max(CASE WHEN slot='10:00' THEN volume END) v1000, max(CASE WHEN slot='10:15' THEN volume END) v1015,
             max(CASE WHEN slot='15:00' THEN volume END) v1500, max(CASE WHEN slot='15:15' THEN volume END) v1515,
             max(CASE WHEN slot='15:30' THEN volume END) v1530, max(CASE WHEN slot='15:45' THEN volume END) v1545
      FROM slots GROUP BY 1, 2""")
    rep["joins"]["calendar_x_15m"] = B.typed_join_report(con, "SELECT ticker, session FROM cal", "SELECT ticker, session FROM piv", "session")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE f AS
      SELECT c.ticker, c.session, c.close, (c.session IN (SELECT session FROM early_close)) AS early_close,
             p.v0930, p.v0945, p.v1000, p.v1015, p.v1500, p.v1515, p.v1530, p.v1545
      FROM cal c LEFT JOIN piv p USING (ticker, session)""")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE f AS
      SELECT *,
        CASE WHEN v0930 IS NOT NULL AND v0945 IS NOT NULL THEN v0930 + v0945 END AS open30_a,
        CASE WHEN v1000 IS NOT NULL AND v1015 IS NOT NULL THEN v1000 + v1015 END AS open30_b,
        CASE WHEN NOT early_close AND v1500 IS NOT NULL AND v1515 IS NOT NULL THEN v1500 + v1515 END AS close30_a,
        CASE WHEN NOT early_close AND v1530 IS NOT NULL AND v1545 IS NOT NULL THEN v1530 + v1545 END AS close30_b
      FROM f""")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE f AS
      SELECT *,
        CASE WHEN open30_a IS NOT NULL AND open30_b IS NOT NULL THEN open30_a + open30_b END AS open60,
        CASE WHEN close30_a IS NOT NULL AND close30_b IS NOT NULL THEN close30_a + close30_b END AS close60
      FROM f""")
    bad = con.execute("""SELECT count(*) FROM f WHERE (open60 IS NOT NULL AND open60 <> open30_a + open30_b)
                         OR (close60 IS NOT NULL AND close60 <> close30_a + close30_b)
                         OR (early_close AND (close30_a IS NOT NULL OR close30_b IS NOT NULL OR close60 IS NOT NULL))""").fetchone()[0]
    if bad:
        raise HardStop(f"window conservation violated on {bad} rows")
    # same-slot RVOL: prior 20 REGULAR sessions with the window available; early-close sessions contribute
    # neither numerator nor denominator; current session excluded
    for w in RVOL_WINDOWS:
        con.execute(f"""
          CREATE OR REPLACE TEMP TABLE rv_{w} AS
          SELECT ticker, session, {rvol_sql(w)} AS rvol_{w}
          FROM (SELECT ticker, session, {w} FROM f WHERE {w} IS NOT NULL AND NOT early_close)""")
    con.execute("CREATE OR REPLACE TEMP TABLE f AS SELECT f.*, " + ", ".join(f"rv_{w}.rvol_{w}" for w in RVOL_WINDOWS) +
                " FROM f " + " ".join(f"LEFT JOIN rv_{w} USING (ticker, session)" for w in RVOL_WINDOWS))
    B.assert_unique(con, "SELECT * FROM f", "ticker, session")
    # descriptive 30m structure + DECLINE_CONTEXT on the canonical calendar
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE f AS
      SELECT *,
        CASE WHEN open30_a > 0 THEN open30_b / open30_a END AS open30_internal_ratio,
        CASE WHEN rvol_open30_a > 0 THEN rvol_open30_b / rvol_open30_a END AS open30_rvol_ratio,
        CASE WHEN close30_a > 0 THEN close30_b / close30_a END AS close30_internal_ratio,
        CASE WHEN rvol_close30_a > 0 THEN rvol_close30_b / rvol_close30_a END AS close30_rvol_ratio,
        CASE WHEN rvol_open30_a IS NOT NULL AND rvol_open30_b IS NOT NULL THEN (rvol_open30_a > 1)::INT + (rvol_open30_b > 1)::INT END AS open30_breadth,
        CASE WHEN rvol_close30_a IS NOT NULL AND rvol_close30_b IS NOT NULL THEN (rvol_close30_a > 1)::INT + (rvol_close30_b > 1)::INT END AS close30_breadth,
        CASE WHEN rvol_close30_a IS NULL OR rvol_close30_b IS NULL THEN NULL
             WHEN rvol_close30_a >= 1.5 AND rvol_close30_b < 0.6 * rvol_close30_a THEN 'EARLY_CLOSE_SPIKE'
             WHEN rvol_close30_b >= 1.2 AND rvol_close30_b > 1.25 * rvol_close30_a THEN 'LATE_ACCELERATION'
             WHEN rvol_close30_a >= 1.0 AND rvol_close30_b >= 1.0 THEN 'SUSTAINED_CLOSE'
             WHEN rvol_close30_a < 1.0 AND rvol_close30_b < 1.0 THEN 'QUIET_CLOSE' ELSE 'OTHER' END AS close30_shape,
        CASE WHEN open60 > 0 THEN close60 / open60 END AS close60_over_open60,
        CASE WHEN open30_a > 0 THEN close30_b / open30_a END AS close30b_over_open30a,
        CASE WHEN lag(close, {DECLINE_LAG}) OVER w IS NULL THEN NULL ELSE close < lag(close, {DECLINE_LAG}) OVER w END AS decline_context,
        lag(rvol_open30_a, 1) OVER w AS rvol_open30_a_1, lag(rvol_open30_a, 2) OVER w AS rvol_open30_a_2,
        lag(rvol_open30_b, 1) OVER w AS rvol_open30_b_1, lag(rvol_open30_b, 2) OVER w AS rvol_open30_b_2,
        (rvol_open60 >= {EVENT_RVOL} AND lag(close, 1) OVER w < lag(close, 6) OVER w) AS is_hv_event
      FROM f WINDOW w AS (PARTITION BY ticker ORDER BY session)""")
    # Logic 1 · Logic 2 (event authority = V1 predicate on the V2 regular-session RVOL_OPEN60)
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE f AS
      SELECT *,
        CASE WHEN rvol_open30_a_2 IS NULL OR rvol_open30_a_1 IS NULL OR rvol_open30_a IS NULL
               OR rvol_open30_b_2 IS NULL OR rvol_open30_b_1 IS NULL OR rvol_open30_b IS NULL THEN NULL
             ELSE (rvol_open30_a_2 < rvol_open30_a_1 AND rvol_open30_a_1 < rvol_open30_a
                   AND rvol_open30_b_2 < rvol_open30_b_1 AND rvol_open30_b_1 < rvol_open30_b) END AS open30_sustained_build_3d,
        CASE WHEN rvol_open30_a_2 IS NULL OR rvol_open30_a_1 IS NULL OR rvol_open30_a IS NULL
               OR rvol_open30_b_2 IS NULL OR rvol_open30_b_1 IS NULL OR rvol_open30_b IS NULL THEN NULL
             WHEN (rvol_open30_a_2 < rvol_open30_a_1 AND rvol_open30_a_1 < rvol_open30_a)
                  AND (rvol_open30_b_2 < rvol_open30_b_1 AND rvol_open30_b_1 < rvol_open30_b) THEN 'BOTH_BUILD'
             WHEN (rvol_open30_a_2 < rvol_open30_a_1 AND rvol_open30_a_1 < rvol_open30_a) THEN 'ONLY_FIRST_HALF_BUILDS'
             WHEN (rvol_open30_b_2 < rvol_open30_b_1 AND rvol_open30_b_1 < rvol_open30_b) THEN 'ONLY_SECOND_HALF_BUILDS'
             ELSE 'NEITHER' END AS open30_build_decomposition,
        last_value(CASE WHEN is_hv_event THEN session END IGNORE NULLS)
          OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN {EVENT_MAX_LOOKBACK} PRECEDING AND {EVENT_MIN_SEP} PRECEDING) AS hv_event_session,
        last_value(CASE WHEN is_hv_event THEN open30_a END IGNORE NULLS)
          OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN {EVENT_MAX_LOOKBACK} PRECEDING AND {EVENT_MIN_SEP} PRECEDING) AS hv_event_open30_a,
        last_value(CASE WHEN is_hv_event THEN open30_b END IGNORE NULLS)
          OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN {EVENT_MAX_LOOKBACK} PRECEDING AND {EVENT_MIN_SEP} PRECEDING) AS hv_event_open30_b,
        last_value(CASE WHEN is_hv_event THEN open60 END IGNORE NULLS)
          OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN {EVENT_MAX_LOOKBACK} PRECEDING AND {EVENT_MIN_SEP} PRECEDING) AS hv_event_open60
      FROM f""")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE f AS
      SELECT *,
        CASE WHEN hv_event_open30_a > 0 THEN open30_a / hv_event_open30_a END AS reclaim_open30_a,
        CASE WHEN hv_event_open30_b > 0 THEN open30_b / hv_event_open30_b END AS reclaim_open30_b
      FROM f""")
    # Logic 3 (row = D) · Logic 4 (row = D+1; D = the previous canonical session of the ticker)
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE f AS
      SELECT *,
        CASE WHEN reclaim_open30_a IS NULL OR reclaim_open30_b IS NULL THEN NULL
             ELSE (reclaim_open30_a >= 1.0 AND reclaim_open30_b >= 1.0) END AS full_opening_reclaim_30,
        CASE WHEN decline_context IS NULL OR rvol_close60 IS NULL OR close60 IS NULL OR open60 IS NULL OR open60 <= 0 THEN NULL
             ELSE (decline_context AND rvol_close60 > 1.0 AND close60 / open60 >= 1.0) END AS close60_dominance,
        CASE WHEN decline_context IS NULL OR rvol_close30_b IS NULL OR close30_b IS NULL OR open30_a IS NULL OR open30_a <= 0 THEN NULL
             ELSE (decline_context AND rvol_close30_b > 1.0 AND close30_b / open30_a >= 1.0) END AS close30_dominance,
        lag(session, 1) OVER w AS h_D_session,
        lag(decline_context, 1) OVER w AS decline_context_D,
        lag(rvol_close60, 1) OVER w AS rvol_close60_D, lag(close60, 1) OVER w AS close60_D,
        lag(rvol_close30_b, 1) OVER w AS rvol_close30_b_D, lag(close30_b, 1) OVER w AS close30_b_D
      FROM f WINDOW w AS (PARTITION BY ticker ORDER BY session)""")
    lo, hi = HANDOFF_BAND
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE f AS
      SELECT *,
        CASE WHEN close60_D > 0 THEN open60 / close60_D END AS raw_handoff60,
        CASE WHEN rvol_close60_D > 0 THEN rvol_open60 / rvol_close60_D END AS norm_handoff60,
        CASE WHEN close30_b_D > 0 THEN open30_a / close30_b_D END AS raw_handoff30,
        CASE WHEN rvol_close30_b_D > 0 THEN rvol_open30_a / rvol_close30_b_D END AS norm_handoff30,
        CASE WHEN rvol_open30_a IS NULL OR rvol_open30_b IS NULL THEN NULL ELSE (rvol_open30_a > 1.0 AND rvol_open30_b > 1.0) END AS next_open30_persistence,
        CASE WHEN open30_a > 0 THEN open30_b / open30_a END AS next_open30_internal_ratio
      FROM f""")
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE f AS
      SELECT *,
        CASE WHEN decline_context_D IS NULL OR rvol_close60_D IS NULL OR rvol_open60 IS NULL OR raw_handoff60 IS NULL THEN NULL
             ELSE (decline_context_D AND rvol_close60_D > 1.0 AND rvol_open60 > 1.0 AND raw_handoff60 BETWEEN {lo} AND {hi}) END AS close60_to_next_open60_handoff,
        CASE WHEN decline_context_D IS NULL OR rvol_close30_b_D IS NULL OR rvol_open30_a IS NULL OR raw_handoff30 IS NULL THEN NULL
             ELSE (decline_context_D AND rvol_close30_b_D > 1.0 AND rvol_open30_a > 1.0 AND raw_handoff30 BETWEEN {lo} AND {hi}) END AS close30_to_next_open30_handoff
      FROM f""")
    # ── join onto the V1-sealed X (eligible rows); V1 columns must remain byte-identical ──
    con.execute(f"CREATE OR REPLACE TEMP TABLE x1 AS SELECT * FROM read_parquet('{x1}')")
    rep["joins"]["x1_x_v2"] = B.typed_join_report(con, "SELECT ticker, session FROM x1", "SELECT ticker, session FROM f", "session")
    v1_cols = [r[0] for r in con.execute("DESCRIBE x1").fetchall()]
    new_cols = ["early_close", "open30_a", "open30_b", "close30_a", "close30_b", "open60", "close60",
                "rvol_open30_a", "rvol_open30_b", "rvol_close30_a", "rvol_close30_b", "rvol_open60", "rvol_close60",
                "open30_internal_ratio", "open30_rvol_ratio", "close30_internal_ratio", "close30_rvol_ratio",
                "open30_breadth", "close30_breadth", "close30_shape", "close60_over_open60", "close30b_over_open30a",
                "decline_context", "open30_sustained_build_3d", "open30_build_decomposition",
                "hv_event_session", "hv_event_open60", "reclaim_open30_a", "reclaim_open30_b", "full_opening_reclaim_30",
                "close60_dominance", "close30_dominance",
                "h_D_session", "decline_context_D", "rvol_close60_D", "close60_D", "rvol_close30_b_D", "close30_b_D",
                "raw_handoff60", "norm_handoff60", "raw_handoff30", "norm_handoff30",
                "close60_to_next_open60_handoff", "close30_to_next_open30_handoff",
                "next_open30_persistence", "next_open30_internal_ratio"]
    con.execute("CREATE OR REPLACE TEMP TABLE X2 AS SELECT x.*, " + ", ".join(f"f.{c}" for c in new_cols) +
                ", CASE WHEN x.breakout THEN 'BREAKOUT' WHEN x.base THEN 'BASE' ELSE 'OTHER' END AS base_state"
                " FROM x1 x LEFT JOIN f USING (ticker, session)")
    # V2.1 per-cell claim status: NOT_ELIGIBLE outside the cell's bound sample (never a FALSE), else TRUE/FALSE/UNAVAILABLE
    status_cols = []
    for c in cells_new:
        col, sc, mask = c["column"], c["status_column"], c["sample_sql"]
        status_cols.append(f"CASE WHEN NOT coalesce(({mask}), FALSE) THEN 'NOT_ELIGIBLE' WHEN {col} IS NULL THEN 'UNAVAILABLE' "
                           f"WHEN {col} THEN 'TRUE' ELSE 'FALSE' END AS {sc}")
    con.execute("CREATE OR REPLACE TEMP TABLE X2 AS SELECT *, " + ", ".join(status_cols) + " FROM X2")
    n1 = con.execute("SELECT count(*) FROM x1").fetchone()[0]; n2 = con.execute("SELECT count(*) FROM X2").fetchone()[0]
    if n1 != n2:
        raise HardStop(f"V2 row count {n2} != V1 {n1}")
    diff = con.execute("SELECT count(*) FROM x1 a JOIN X2 b USING (ticker, session) WHERE " +
                       " OR ".join(f"a.{c} IS DISTINCT FROM b.{c}" for c in v1_cols if c not in ("ticker", "session"))).fetchone()[0]
    if diff:
        raise HardStop(f"{diff} rows where a V1 column changed — V1 columns must be byte-identical")
    bad = con.execute("SELECT count(*) FROM X2 WHERE open60 IS NOT NULL AND open1h_vol IS NOT NULL AND open60 <> open1h_vol").fetchone()[0]
    if bad:
        raise HardStop(f"OPEN60 != V1 sum of M1..M4 on {bad} rows")
    rep["conservation"] = dict(open60_vs_v1_open1h_vol_mismatch=0, hour_vs_halves_mismatch=0, early_close_close_windows_leaked=0,
                               v1_columns_changed=0, rows=n2)
    # ── X-only census ──
    avail = {c: con.execute(f"SELECT count({c}) FROM X2").fetchone()[0] for c in RVOL_WINDOWS + [f"rvol_{w}" for w in RVOL_WINDOWS]}
    claims = {}
    for name, col in R2.CLAIM_COLUMNS.items():
        t, fl, nu = con.execute(f"SELECT count(*) FILTER (WHERE {col}), count(*) FILTER (WHERE NOT {col}), count(*) FILTER (WHERE {col} IS NULL) FROM X2").fetchone()
        by_year = dict(con.execute(f"SELECT year(session), count(*) FILTER (WHERE {col}) FROM X2 GROUP BY 1 ORDER BY 1").fetchall())
        by_state = dict(con.execute(f"SELECT base_state, count(*) FILTER (WHERE {col}) FROM X2 GROUP BY 1 ORDER BY 1").fetchall())
        claims[name] = dict(all_rows_descriptive=dict(TRUE=t, FALSE=fl, UNAVAILABLE=nu), true_by_year_all_rows={int(k): v for k, v in by_year.items()},
                            true_by_base_state=by_state, cells={})
        if ENTRY_OFFSET[name] == 2:
            claims[name]["true_by_d1_gap_bucket"] = dict(con.execute(f"SELECT gap_bucket, count(*) FILTER (WHERE {col}) FROM X2 GROUP BY 1 ORDER BY 1").fetchall())
    # per-cell (sample-bound) census — the claim-level numbers
    for c in cells_new:
        sc = c["status_column"]
        d = dict(con.execute(f"SELECT {sc}, count(*) FROM X2 GROUP BY 1").fetchall())
        n_sample = d.get("TRUE", 0) + d.get("FALSE", 0) + d.get("UNAVAILABLE", 0)
        by_year = dict(con.execute(f"SELECT year(session), count(*) FILTER (WHERE {sc} = 'TRUE') FROM X2 GROUP BY 1 ORDER BY 1").fetchall())
        claims[c["feature"]]["cells"][c["family"]] = dict(sample_authority=c["sample_authority"], sample_sql=c["sample_sql"], status_column=sc,
                                                          n_sample=n_sample, TRUE=d.get("TRUE", 0), FALSE=d.get("FALSE", 0),
                                                          UNAVAILABLE=d.get("UNAVAILABLE", 0), NOT_ELIGIBLE=d.get("NOT_ELIGIBLE", 0),
                                                          true_by_year={int(k): v for k, v in by_year.items()})
    q = lambda col, where="TRUE": con.execute(f"SELECT quantile_cont({col}, [0.05,0.25,0.5,0.75,0.95]) FROM X2 WHERE {col} IS NOT NULL AND ({where})").fetchone()[0]
    desc = dict(
        raw_handoff60=q("raw_handoff60"), norm_handoff60=q("norm_handoff60"), raw_handoff30=q("raw_handoff30"), norm_handoff30=q("norm_handoff30"),
        raw_handoff60_given_decline_and_rvol_close_gt1=q("raw_handoff60", "decline_context_D AND rvol_close60_D > 1.0"),
        close60_over_open60=q("close60_over_open60"), close30b_over_open30a=q("close30b_over_open30a"),
        open30_internal_ratio=q("open30_internal_ratio"), close30_internal_ratio=q("close30_internal_ratio"),
        open30_breadth=dict(con.execute("SELECT open30_breadth, count(*) FROM X2 WHERE open30_breadth IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall()),
        close30_breadth=dict(con.execute("SELECT close30_breadth, count(*) FROM X2 WHERE close30_breadth IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall()),
        close30_shape=dict(con.execute("SELECT close30_shape, count(*) FROM X2 WHERE close30_shape IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall()),
        open30_build_decomposition=dict(con.execute("SELECT open30_build_decomposition, count(*) FROM X2 WHERE open30_build_decomposition IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall()),
        next_open30_persistence=dict(con.execute("SELECT next_open30_persistence, count(*) FROM X2 WHERE next_open30_persistence IS NOT NULL GROUP BY 1 ORDER BY 1").fetchall()),
        handoff60_persistence_split=dict(con.execute("SELECT next_open30_persistence, count(*) FROM X2 WHERE close60_to_next_open60_handoff GROUP BY 1 ORDER BY 1").fetchall()),
        decline_context=dict(con.execute("SELECT decline_context, count(*) FROM X2 GROUP BY 1 ORDER BY 1").fetchall()))
    # reconciliations against V1 (same rows, V1 columns untouched)
    rv = con.execute("""SELECT count(*), count(*) FILTER (WHERE abs(rvol_open60 - open1h_rvol) < 1e-9), count(*) FILTER (WHERE abs(rvol_open60/open1h_rvol - 1) < 0.01)
                        FROM X2 WHERE rvol_open60 IS NOT NULL AND open1h_rvol IS NOT NULL""").fetchone()
    ev = con.execute("""SELECT count(*) FILTER (WHERE hv_event_open60 IS NOT NULL), count(*) FILTER (WHERE prior_hv_vol IS NOT NULL),
                               count(*) FILTER (WHERE hv_event_open60 IS NOT NULL AND prior_hv_vol IS NOT NULL),
                               count(*) FILTER (WHERE hv_event_open60 IS NOT NULL AND prior_hv_vol IS NOT NULL AND hv_event_open60 = prior_hv_vol)
                        FROM X2""").fetchone()
    rep["reconciliation_vs_v1"] = dict(
        rvol_open60_vs_open1h_rvol=dict(both=rv[0], identical=rv[1], within_1pct=rv[2],
                                        note="V2 denominators use PRIOR 20 REGULAR sessions (early-close sessions excluded); V1 used the prior 20 four-slot sessions"),
        prior_hv_event=dict(v2_rows_with_event=ev[0], v1_rows_with_event=ev[1], both=ev[2], same_event_volume=ev[3],
                            note="V2 event authority = V1 predicate on the V2 RVOL_OPEN60 over the canonical calendar; V1 claims keep V1's own columns"))
    rep.update(rows=n2, availability=avail, claims=claims, descriptive=desc, tickers=con.execute("SELECT count(DISTINCT ticker) FROM X2").fetchone()[0],
               sessions=con.execute("SELECT count(DISTINCT session) FROM X2").fetchone()[0], elapsed_s=round(time.time() - t0),
               new_columns=new_cols + ["base_state"] + [c["status_column"] for c in cells_new], claim_columns=R2.CLAIM_COLUMNS,
               entry_offset_from_D=ENTRY_OFFSET, sample_bindings=enum["bindings"])
    out = os.path.join(run.dir, "X_v2.parquet")
    con.execute(f"COPY (SELECT * FROM X2 ORDER BY ticker, session) TO '{out}' (FORMAT PARQUET)")
    run.write_atomic("build_report.json", rep)
    run.complete("X_v2.parquet")
    run.write_atomic("STATUS.json", dict(run_id=run.run_id, status="CANONICAL_1D_BASIS_V2", parent_v1_seal_sha256=V1_SEAL_SHA256,
                                         x1_run=seal["x_run"], x1_sha256_16=seal["x_parquet_sha256_16"], canonical_1d=seal["canonical_1d"],
                                         registry_version="V2_1", bindings_sha256_16=enum["bindings_sha256_16"], cells_sha256_16=enum["cells_sha256_16"],
                                         classified_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
    con.close()
    return rep


def main():
    rep = build()
    print(json.dumps({k: v for k, v in rep.items() if k not in ("descriptive",)}, indent=1, default=str))


if __name__ == "__main__":
    main()
