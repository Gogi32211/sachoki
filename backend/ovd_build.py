"""OPENING_VOLUME_DYNAMICS_V1 — X-ONLY feature-table builder (phase 1/2, no outcomes).

Spec:  /Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1/FEATURE_SPEC_V1.draft.md
Prov:  /Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1/PROVENANCE.md

This module builds the research table and NOTHING else: no `_pathsim`, no forward return,
no breakout-success column. It is import-side-effect free; run via `main()` only.

Guards (each is a function so fixtures can hit it directly):
  ny_local()              naive-UTC store timestamp -> America/New_York wall time
  typed_join_report()     DuckDB DATE join with left/right/matched/only counts; zero = HARD STOP
  assert_unique()         duplicate (ticker, session, slot) = HARD STOP
  slot_identity()         a bar belongs to a slot only by NY start minute; off-grid = no slot
  rvol_same_slot()        20 PRIOR eligible sessions, same slot; current session excluded
  assert_no_self_in_denominator()
  assert_causal_entry()   entry timestamp must be > feature_available_at
  assert_conservation()   categorical decomposition sums to eligible n
  assert_no_pathsim_copy() this module must not define anything resembling the exit engine
  RunSpace                run_id namespace, atomic writes, COMPLETED marker; a readable file
                          without the marker is NOT current
"""
from __future__ import annotations
import os, sys, json, time, hashlib, tempfile, re                       # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(os.path.dirname(HERE), "data")
FAMILY_DIR = "/Users/sachoki/MASSIVE_DATA/OPENING_VOLUME_DYNAMICS_V1"
LOOKBACK = 20                      # frozen; not swept
NY = "timezone('America/New_York', timezone('UTC', {col}))"   # naive stamp holds UTC wall time
SLOTS_15 = ["09:30", "09:45", "10:00", "10:15"]               # M1..M4 by NY start
SLOTS_1H = ["09:30", "10:30", "11:30", "12:30", "13:30", "14:30", "15:30"]


class HardStop(RuntimeError):
    pass


# ── run namespace ─────────────────────────────────────────────────────────────
class RunSpace:
    """Every execution writes under runs/<run_id>/. CURRENT means: COMPLETED marker present
    AND the marker's digest matches the result file. Anything else is STALE_RUN."""

    def __init__(self, family_dir: str = FAMILY_DIR, run_id: str | None = None):
        self.run_id = run_id or time.strftime("OVD_%Y%m%dT%H%M%SZ", time.gmtime())
        self.dir = os.path.join(family_dir, "runs", self.run_id)
        os.makedirs(self.dir, exist_ok=True)

    def write_atomic(self, name: str, payload) -> str:
        p = os.path.join(self.dir, name)
        fd, tmp = tempfile.mkstemp(dir=self.dir, prefix=".tmp_")
        with os.fdopen(fd, "w") as f:
            if isinstance(payload, (dict, list)):
                json.dump(payload, f, indent=1, default=str)
            else:
                f.write(str(payload))
        os.replace(tmp, p)
        return p

    def complete(self, result_name: str) -> str:
        p = os.path.join(self.dir, result_name)
        dig = hashlib.sha256(open(p, "rb").read()).hexdigest()[:16]
        return self.write_atomic("COMPLETED", {"run_id": self.run_id, "result": result_name,
                                               "result_sha256_16": dig,
                                               "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})

    @staticmethod
    def is_current(run_dir: str, result_name: str) -> bool:
        m = os.path.join(run_dir, "COMPLETED"); r = os.path.join(run_dir, result_name)
        if not (os.path.exists(m) and os.path.exists(r)):
            return False
        try:
            mk = json.load(open(m))
        except Exception:
            return False
        return mk.get("result") == result_name and \
            mk.get("result_sha256_16") == hashlib.sha256(open(r, "rb").read()).hexdigest()[:16]


# ── guards ────────────────────────────────────────────────────────────────────
def ny_local(col: str) -> str:
    return NY.format(col=col)


def typed_join_report(con, left_sql: str, right_sql: str, on: str) -> dict:
    """Both sides must expose `on` as DATE. Returns the five counts; zero matched = HARD STOP."""
    lt = con.execute(f"SELECT typeof({on}) FROM ({left_sql}) LIMIT 1").fetchone()
    rt = con.execute(f"SELECT typeof({on}) FROM ({right_sql}) LIMIT 1").fetchone()
    if not lt or not rt or lt[0] != "DATE" or rt[0] != "DATE":
        raise HardStop(f"join key {on} is not DATE on both sides: left={lt}, right={rt}")
    rep = con.execute(f"""
        WITH L AS (SELECT DISTINCT ticker, {on} AS k FROM ({left_sql})),
             R AS (SELECT DISTINCT ticker, {on} AS k FROM ({right_sql}))
        SELECT (SELECT count(*) FROM L), (SELECT count(*) FROM R),
               (SELECT count(*) FROM L JOIN R USING (ticker, k)),
               (SELECT count(*) FROM L ANTI JOIN R USING (ticker, k)),
               (SELECT count(*) FROM R ANTI JOIN L USING (ticker, k))""").fetchone()
    out = dict(left=rep[0], right=rep[1], matched=rep[2], left_only=rep[3], right_only=rep[4])
    if out["matched"] == 0:
        raise HardStop(f"typed join produced ZERO matches: {out}")
    return out


def assert_unique(con, sql: str, keys: str) -> None:
    n = con.execute(f"SELECT count(*) - count(DISTINCT ({keys})) FROM ({sql})").fetchone()[0]
    if n:
        raise HardStop(f"{n} duplicate rows on ({keys})")


def slot_identity(nyclock: str, grid: list[str]) -> str:
    """SQL CASE mapping an NY 'HH:MM' start to its slot label or NULL (off-grid = no slot)."""
    return "CASE " + " ".join(f"WHEN {nyclock} = '{s}' THEN '{s}'" for s in grid) + " ELSE NULL END"


def assert_no_self_in_denominator(frame_desc: str, window_sql: str) -> None:
    """The rolling frame must end at 1 PRECEDING (current row excluded)."""
    if "1 PRECEDING" not in window_sql or "CURRENT ROW" in window_sql.split("BETWEEN")[-1].split("AND")[-1]:
        raise HardStop(f"{frame_desc}: denominator window includes the current session: {window_sql}")


def assert_causal_entry(feature_available_at_ny: str, entry_at_ny: str) -> None:
    if not entry_at_ny > feature_available_at_ny:
        raise HardStop(f"entry {entry_at_ny} is not after feature availability {feature_available_at_ny}")


def assert_conservation(parts: dict, total: int, what: str) -> None:
    s = sum(parts.values())
    if s != total:
        raise HardStop(f"{what}: parts sum {s} != eligible {total}: {parts}")


def assert_no_pathsim_copy(path: str = __file__) -> None:
    src = open(path).read()
    bad = [k for k in ("mae", "mfe", "date_out", "trail", "atr_k", "target -") if re.search(rf"\b{re.escape(k)}\b", src.replace('"', ' '))]
    # the words above appear only in this guard's own list; any use in code = reimplementation
    hits = [k for k in bad if src.count(k) > 2]
    if hits:
        raise HardStop(f"exit-engine vocabulary found in the X-only builder: {hits}")


# ── SQL builders (X-only) ─────────────────────────────────────────────────────
def rvol_same_slot(vol: str = "volume", part: str = "ticker, slot", order: str = "session") -> str:
    """median over the 20 PRIOR eligible sessions of the SAME slot; current row excluded."""
    w = f"OVER (PARTITION BY {part} ORDER BY {order} ROWS BETWEEN {LOOKBACK} PRECEDING AND 1 PRECEDING)"
    assert_no_self_in_denominator("rvol_same_slot", w)
    return (f"CASE WHEN count({vol}) {w} >= {LOOKBACK} THEN {vol} / median({vol}) {w} ELSE NULL END")


def sql_15m_slots(store: str) -> str:
    ny = ny_local("date")
    clock = f"strftime({ny}, '%H:%M')"
    return f"""
    SELECT ticker, CAST({ny} AS DATE) AS session,
           {slot_identity(clock, SLOTS_15)} AS slot,
           open, high, low, close, volume
    FROM read_duckdb('{store}', 'bars')   -- placeholder; replaced by ATTACH in build()
    """


def build_15m(con, months: int) -> str:
    """Creates temp table m15_feat(ticker, session, M1..M4 raw vol, RVOL15 per slot, share,
    breadth, concentration, persistence, shape). Only on-grid slots; duplicates HARD STOP."""
    ny = ny_local("b.date"); clock = f"strftime({ny}, '%H:%M')"
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE m15_slots AS
      SELECT b.ticker, CAST({ny} AS DATE) AS session,
             {slot_identity(clock, SLOTS_15)} AS slot, b.volume, b.open, b.high, b.low, b.close
      FROM m15.bars b
      WHERE b.volume > 0 AND {clock} IN ('09:30','09:45','10:00','10:15')
        AND CAST({ny} AS DATE) >= (SELECT max(CAST({ny.replace('b.date','date')} AS DATE)) FROM m15.bars)
                                   - INTERVAL {months * 31 + 60} DAY""")
    assert_unique(con, "SELECT * FROM m15_slots", "ticker, session, slot")
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE m15_rv AS
      SELECT ticker, session, slot, volume, open, high, low, close,
             {rvol_same_slot()} AS rvol
      FROM m15_slots""")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE m15_feat AS
      SELECT ticker, session,
             max(CASE WHEN slot='09:30' THEN volume END) v1, max(CASE WHEN slot='09:45' THEN volume END) v2,
             max(CASE WHEN slot='10:00' THEN volume END) v3, max(CASE WHEN slot='10:15' THEN volume END) v4,
             max(CASE WHEN slot='09:30' THEN rvol END) r1,   max(CASE WHEN slot='09:45' THEN rvol END) r2,
             max(CASE WHEN slot='10:00' THEN rvol END) r3,   max(CASE WHEN slot='10:15' THEN rvol END) r4,
             -- first-hour OHLC from the four slots (pre-seal issue 4: the 15m slots DEFINE the
             -- opening hour; the stored 1H 09:30 bar is reconciliation-only)
             max(CASE WHEN slot='09:30' THEN open END)  AS fh_open,
             max(CASE WHEN slot='10:15' THEN close END) AS fh_close,
             max(high) AS fh_high, min(low) AS fh_low,
             count(*) AS n_slots
      FROM m15_rv GROUP BY 1,2""")
    # OPEN1H_VOLUME := exact sum of the four valid 15m slots (UNAVAILABLE unless all four are
    # present). Its RVOL uses the same 20-prior-session median rule, partitioned by ticker
    # (the "slot" here is the whole opening hour). Sessions with <4 slots contribute NEITHER a
    # numerator NOR a denominator observation.
    con.execute("""
      CREATE OR REPLACE TEMP TABLE m15_feat AS
      SELECT *,
        CASE WHEN n_slots = 4 THEN v1 + v2 + v3 + v4 END AS open1h_vol15
      FROM m15_feat""")
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE m15_feat AS
      SELECT *,
        CASE WHEN open1h_vol15 IS NOT NULL
             AND count(open1h_vol15) OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN {LOOKBACK} PRECEDING AND 1 PRECEDING) >= {LOOKBACK}
             THEN open1h_vol15 / median(open1h_vol15) OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN {LOOKBACK} PRECEDING AND 1 PRECEDING) END AS open1h_rvol15
      FROM m15_feat""")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE m15_feat AS
      SELECT *,
        CASE WHEN n_slots=4 AND r1 IS NOT NULL AND r2 IS NOT NULL AND r3 IS NOT NULL AND r4 IS NOT NULL THEN
             (r1>1.0)::INT+(r2>1.0)::INT+(r3>1.0)::INT+(r4>1.0)::INT END AS open15_breadth,
        CASE WHEN n_slots=4 THEN v1::DOUBLE/(v1+v2+v3+v4) END AS open15_concentration,
        CASE WHEN n_slots=4 AND r1 IS NOT NULL AND r1>0 AND r2 IS NOT NULL AND r3 IS NOT NULL AND r4 IS NOT NULL
             THEN (r2+r3+r4)/3.0/r1 END AS open15_persistence,
        CASE WHEN n_slots<>4 OR r1 IS NULL OR r2 IS NULL OR r3 IS NULL OR r4 IS NULL THEN NULL
             WHEN r1>=1.5 AND r2<0.6*r1 AND r3<0.6*r1 AND r4<0.6*r1 AND r2>=r3 AND r3>=r4 THEN 'FRONT_LOADED'
             WHEN r1>=1.0 AND r2>=1.0 AND r3>=1.0 AND r4>=1.0 THEN 'SUSTAINED_HIGH'
             WHEN r1<r2 AND r2<r3 AND r3<r4 THEN 'BUILDING'
             WHEN r1>=1.2 AND r4>=1.2 AND least(r2,r3) < 0.8*least(r1,r4) THEN 'OPEN_AND_LATE'
             ELSE 'OTHER' END AS open15_shape
      FROM m15_feat""")
    # ── registry feature 15: VOLUME_TRANSFER = mean concentration over D-4..D-2 minus over
    #    D-1..D (participation moving from the M1 spike toward the rest of the hour). Prior
    #    sessions only via window frames; availability = 10:30 of session D.
    con.execute("""
      CREATE OR REPLACE TEMP TABLE m15_feat AS
      SELECT *,
        avg(open15_concentration) OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 4 PRECEDING AND 2 PRECEDING)
        - avg(open15_concentration) OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
          AS volume_transfer
      FROM m15_feat""")
    return "m15_feat"


def build_1h(con, months: int) -> str:
    ny = ny_local("b.date"); clock = f"strftime({ny}, '%H:%M')"
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE h1_slots AS
      SELECT b.ticker, CAST({ny} AS DATE) AS session,
             {slot_identity(clock, SLOTS_1H)} AS slot, b.volume, b.open, b.high, b.low, b.close, b.atr_14
      FROM h1.bars b
      WHERE b.volume > 0 AND {clock} IN ('09:30','10:30','11:30','12:30','13:30','14:30','15:30')
        AND CAST({ny} AS DATE) >= (SELECT max(CAST({ny.replace('b.date','date')} AS DATE)) FROM h1.bars)
                                   - INTERVAL {months * 31 + 60} DAY""")
    assert_unique(con, "SELECT * FROM h1_slots", "ticker, session, slot")
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE h1_rv AS
      SELECT ticker, session, slot, volume, open, high, low, close, atr_14,
             {rvol_same_slot()} AS rvol
      FROM h1_slots""")
    # PRE-SEAL ISSUE 4: the opening hour is DEFINED by the four 15m slots (reconciled against
    # the stored 1H 09:30 bar: 3,341,572 compared, 99.84% within 0.1%, 0.05% >1% apart). So
    # every OPEN1H feature is sourced from m15_feat (built BEFORE this stage); the 1H store
    # supplies only the all-day breadth count and a reconciliation column.
    con.execute("""
      CREATE OR REPLACE TEMP TABLE h1_feat AS
      SELECT h.ticker, h.session,
             m.open1h_vol15  AS open1h_vol,
             m.open1h_rvol15 AS open1h_rvol,
             CASE WHEN m.fh_open > 0 THEN m.fh_close / m.fh_open - 1 END AS open1h_ret,
             (m.fh_high - m.fh_low) / nullif(h.atr_open, 0) AS open1h_range_atr,
             (m.fh_close - m.fh_low) / nullif(m.fh_high - m.fh_low, 0) AS open1h_close_loc,
             h.open1h_vol_1hstore, h.h1_active_slots, h.n_slots
      FROM (SELECT ticker, session,
                   max(CASE WHEN slot='09:30' THEN volume END) AS open1h_vol_1hstore,
                   max(CASE WHEN slot='09:30' THEN atr_14 END) AS atr_open,
                   sum(CASE WHEN rvol > 1.0 THEN 1 ELSE 0 END) AS h1_active_slots,
                   count(*) AS n_slots
            FROM h1_rv GROUP BY 1,2) h
      LEFT JOIN m15_feat m USING (ticker, session)""")
    # multi-session features: prior sessions only (LAG), so availability = 10:30 of session
    con.execute("""
      CREATE OR REPLACE TEMP TABLE h1_feat AS
      SELECT *,
        lag(open1h_rvol,1) OVER w AS open1h_rvol_1, lag(open1h_rvol,2) OVER w AS open1h_rvol_2,
        lag(open1h_rvol,3) OVER w AS open1h_rvol_3, lag(open1h_rvol,4) OVER w AS open1h_rvol_4,
        lag(open1h_ret,1) OVER w AS open1h_ret_1,  lag(open1h_ret,2) OVER w AS open1h_ret_2,
        lag(h1_active_slots,1) OVER w AS h1_active_slots_1,
        median(open1h_rvol) OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS base_dryup
      FROM h1_feat WINDOW w AS (PARTITION BY ticker ORDER BY session)""")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE h1_feat AS
      SELECT *,
        CASE WHEN open1h_rvol_2 IS NULL OR open1h_rvol_1 IS NULL OR open1h_rvol IS NULL THEN NULL
             WHEN open1h_rvol_2 < open1h_rvol_1 AND open1h_rvol_1 < open1h_rvol THEN 'STRICT_RISING'
             WHEN open1h_rvol_2 > open1h_rvol_1 AND open1h_rvol_1 > open1h_rvol THEN 'STRICT_FALLING'
             ELSE 'MIXED' END AS ramp_3d,
        CASE WHEN open1h_rvol_2 > 0 THEN open1h_rvol / open1h_rvol_2 END AS ramp_mag,
        CASE WHEN base_dryup > 0 THEN open1h_rvol / base_dryup END AS dryup_to_expansion,
        CASE WHEN open1h_rvol_1 IS NULL OR open1h_ret IS NULL OR open1h_ret_1 IS NULL OR open1h_ret_2 IS NULL THEN NULL
             WHEN open1h_rvol > open1h_rvol_1 AND open1h_ret < 0 AND open1h_ret_1 < 0
                  AND abs(open1h_ret) < abs(open1h_ret_1) AND abs(open1h_ret_1) < abs(open1h_ret_2) THEN 'ABSORB_LIKE'
             ELSE 'NOT' END AS effort_result,
        CASE WHEN h1_active_slots_1 IS NULL THEN NULL
             WHEN h1_active_slots_1 <= 1 AND h1_active_slots >= 4 THEN 'QUIET_TO_BROAD'
             WHEN h1_active_slots_1 >= 4 AND h1_active_slots >= 4 THEN 'BROAD_TO_BROAD'
             WHEN h1_active_slots_1 >= 4 AND h1_active_slots <= 1 THEN 'BROAD_TO_COLLAPSE'
             ELSE 'OTHER' END AS breadth_shift
      FROM h1_feat""")
    # ── registry features 4, 5, 13: 5-session Theil–Sen ramp slope; stopping-day reclaim;
    #    stopping-day response. STOP_DAY = most recent session in D-30..D-6 with
    #    RVOL_OPEN1H >= 2.0 AND a down close (NEW_RESEARCH_SPEC, frozen pre-outcome).
    con.execute("""
      CREATE OR REPLACE TEMP TABLE h1_feat AS
      SELECT h.*,
        lag(close_1d,1) OVER w AS close_1d_1,
        CASE WHEN open1h_rvol_4 IS NULL OR open1h_rvol_3 IS NULL OR open1h_rvol_2 IS NULL OR open1h_rvol_1 IS NULL OR open1h_rvol IS NULL THEN NULL
             ELSE list_aggregate([
               (open1h_rvol_3-open1h_rvol_4)/1.0, (open1h_rvol_2-open1h_rvol_4)/2.0, (open1h_rvol_1-open1h_rvol_4)/3.0, (open1h_rvol-open1h_rvol_4)/4.0,
               (open1h_rvol_2-open1h_rvol_3)/1.0, (open1h_rvol_1-open1h_rvol_3)/2.0, (open1h_rvol-open1h_rvol_3)/3.0,
               (open1h_rvol_1-open1h_rvol_2)/1.0, (open1h_rvol-open1h_rvol_2)/2.0,
               (open1h_rvol-open1h_rvol_1)/1.0], 'median') END AS ramp_5d_slope
      FROM (SELECT f.*, d.close AS close_1d FROM h1_feat f LEFT JOIN d1 d USING (ticker, session)) h
      WINDOW w AS (PARTITION BY ticker ORDER BY session)""")
    # PRE-SEAL ISSUE 6 (user): the event must be NEUTRAL — a prior high-participation session
    # in a declining price context — and must NOT condition on that session's own response,
    # because the response is itself a registered feature (PRIOR_HV_DAY_RESPONSE).
    #   PRIOR_HV_DECLINE_EVENT(s) := RVOL_OPEN1H(s) >= 2.0 AND close(s-1) < close(s-6)
    # Window D-30..D-6: max lookback 30 sessions, min separation 5 full sessions. Both
    # constants and the 2.0 threshold are NEW_RESEARCH_SPEC (no historical authority found).
    # OHLCV cannot establish that selling "stopped"; the name says only what is measured.
    con.execute("""
      CREATE OR REPLACE TEMP TABLE h1_feat AS
      SELECT *,
        lag(close_1d,6) OVER w AS close_1d_6,
        (open1h_rvol >= 2.0 AND lag(close_1d,1) OVER w < lag(close_1d,6) OVER w) AS is_prior_hv_decline_event,
        CASE WHEN open1h_range_atr >= 1.5 AND open1h_close_loc < 0.3 THEN 'HV_WIDE_DOWN'
             WHEN open1h_range_atr < 1.0 THEN 'HV_NARROW_RESULT'
             WHEN open1h_close_loc >= 0.6 THEN 'HV_STRONG_RECOVERY'
             ELSE 'HV_OTHER' END AS own_day_response
      FROM h1_feat WINDOW w AS (PARTITION BY ticker ORDER BY session)""")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE h1_feat AS
      SELECT *,
        last_value(CASE WHEN is_prior_hv_decline_event THEN open1h_vol END IGNORE NULLS)
          OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 30 PRECEDING AND 6 PRECEDING) AS prior_hv_vol,
        last_value(CASE WHEN is_prior_hv_decline_event THEN own_day_response END IGNORE NULLS)
          OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 30 PRECEDING AND 6 PRECEDING) AS prior_hv_response
      FROM h1_feat""")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE h1_feat AS
      SELECT *, CASE WHEN prior_hv_vol > 0 THEN open1h_vol / prior_hv_vol END AS prior_hv_reclaim FROM h1_feat""")
    return "h1_feat"


def canonical_current() -> dict:
    """The newest canonical 1D authority (option A). HARD STOP if absent or under-derived."""
    p = os.path.join(FAMILY_DIR, "canonical_1d", "CURRENT.json")
    if not os.path.exists(p):
        raise HardStop("no canonical 1D authority (canonical_1d/CURRENT.json missing) — the Studio store is "
                       "NOT a price authority for this family (mixed corporate-action basis)")
    cur = json.load(open(p))
    if "derived_parquet" not in cur or not os.path.exists(cur["derived_parquet"]):
        raise HardStop("canonical 1D exists but price-derived fields (ATR/avg_vol/wt_*) are not derived yet")
    dig = hashlib.sha256(open(cur["derived_parquet"], "rb").read()).hexdigest()[:16]
    if dig != cur.get("derived_sha256_16"):
        raise HardStop(f"canonical derived digest mismatch: file {dig} != manifest {cur.get('derived_sha256_16')}")
    return cur


def build_1d(con, months: int) -> str:
    """Eligibility, context buckets, gap, and the REUSED base/breakout authority — ALL from the
    CANONICAL 1D store (option A). One row per (ticker, session) by construction; every
    price-derived field (atr_14, avg_vol_20d, wt_*) was recomputed on canonical prices by
    ovd_canonical_derive.py. The Studio store is never read here. Ceiling frozen at D-1."""
    cur = canonical_current()
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE d1 AS
      SELECT ticker, session_date AS session, open, high, low, close, volume, atr_14, avg_vol_20d,
             wt_resistance, wt_valid_tr::BOOLEAN AS wt_valid_tr, NULL::VARCHAR AS bar_gap_class
      FROM read_parquet('{cur["derived_parquet"]}')
      WHERE session_date >= (SELECT max(session_date) FROM read_parquet('{cur["derived_parquet"]}')) - INTERVAL {months * 31 + 60} DAY""")
    assert_unique(con, "SELECT * FROM d1", "ticker, session")
    con.execute("CREATE OR REPLACE TEMP TABLE canon_binding AS SELECT ? AS run_id, ? AS derived_sha256_16, ? AS canonical_sha256_16",
                [cur["run_id"], cur["derived_sha256_16"], cur["canonical_sha256_16"]])
    con.execute("""
      CREATE OR REPLACE TEMP TABLE d1 AS
      SELECT *,
        lag(close,1) OVER w AS close_1,
        lag(wt_resistance,1) OVER w AS ceiling,          -- frozen at D-1
        lag(wt_valid_tr,1) OVER w AS in_tr_1,
        max(high) OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 25 PRECEDING AND 1 PRECEDING) AS hi25,
        min(low)  OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 25 PRECEDING AND 1 PRECEDING) AS lo25,
        median(close*volume) OVER (PARTITION BY ticker ORDER BY session ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS dv20
      FROM d1 WINDOW w AS (PARTITION BY ticker ORDER BY session)""")
    con.execute("""
      CREATE OR REPLACE TEMP TABLE d1 AS
      SELECT *,
        (close >= 5 AND avg_vol_20d > 0 AND close*volume >= 3000000) AS eligible,
        open/nullif(close_1,0) - 1 AS gap,
        CASE WHEN close_1 IS NULL THEN NULL
             WHEN open/close_1-1 < -0.01 THEN 'DOWN' WHEN open/close_1-1 <= 0.01 THEN 'FLAT'
             WHEN open/close_1-1 <= 0.04 THEN 'UP_MOD' ELSE 'UP_LARGE' END AS gap_bucket,
        CASE WHEN close < 8 THEN '$5-8' WHEN close < 21 THEN '$8-21' WHEN close < 89 THEN '$21-89'
             WHEN close < 200 THEN '$89-200' ELSE '$200+' END AS price_bucket,
        CASE WHEN dv20 < 10e6 THEN '3-10M' WHEN dv20 < 50e6 THEN '10-50M' WHEN dv20 < 250e6 THEN '50-250M' ELSE '250M+' END AS dv_bucket,
        CASE WHEN atr_14/nullif(close,0) < 0.02 THEN '<2%' WHEN atr_14/close < 0.04 THEN '2-4%'
             WHEN atr_14/close < 0.07 THEN '4-7%' ELSE '7%+' END AS atr_bucket,
        (hi25 - lo25)/nullif(lo25,0) AS base_rng,
        (in_tr_1 AND close_1 <= ceiling) AS in_range,
        (in_tr_1 AND close_1 <= ceiling AND (hi25 - lo25)/nullif(lo25,0) <= 0.35) AS base,
        (in_tr_1 AND close_1 <= ceiling AND (hi25 - lo25)/nullif(lo25,0) <= 0.35 AND close > ceiling) AS breakout,
        (in_tr_1 AND close_1 <= ceiling AND (hi25 - lo25)/nullif(lo25,0) <= 0.35 AND close > ceiling AND close <= ceiling*1.05) AS breakout_q
      FROM d1""")
    return "d1"


def build(months: int = 72, run: RunSpace | None = None, with_15m: bool = True) -> dict:
    import duckdb
    assert_no_pathsim_copy()
    run = run or RunSpace()
    con = duckdb.connect(); con.execute("pragma threads=8")
    # Option A: the Studio 1D store is NOT attached — daily prices come only from the canonical
    # authority (build_1d -> canonical_current()). 1H / 15m stores are attached for volume slots.
    con.execute(f"ATTACH '{os.path.join(DATA, 'studio_1h.duckdb')}' AS h1 (READ_ONLY)")
    if with_15m:
        con.execute(f"ATTACH '{os.path.join(DATA, 'studio_15m.duckdb')}' AS m15 (READ_ONLY)")
    if not with_15m:
        raise HardStop("OPEN1H is defined by the four 15m slots (pre-seal issue 4); a build without the "
                       "15m store cannot produce any opening-hour feature")
    t0 = time.time(); rep = {"run_id": run.run_id, "months": months, "joins": {}}
    build_1d(con, months); rep["t_1d"] = round(time.time() - t0)
    build_15m(con, months); rep["t_15m"] = round(time.time() - t0)     # BEFORE build_1h: h1_feat joins m15_feat
    rep["joins"]["d1_x_m15"] = typed_join_report(con, "SELECT ticker, session FROM d1 WHERE eligible",
                                                "SELECT ticker, session FROM m15_feat", "session")
    build_1h(con, months); rep["t_1h"] = round(time.time() - t0)
    rep["joins"]["d1_x_h1"] = typed_join_report(con, "SELECT ticker, session FROM d1 WHERE eligible",
                                               "SELECT ticker, session FROM h1_feat", "session")
    rep["open1h_source"] = "sum of the four 15m slots (UNAVAILABLE unless all four present); 1H 09:30 bar kept as open1h_vol_1hstore for reconciliation only"
    # market-wide control: cross-sectional median per session over eligible tickers (<= D only)
    con.execute("""
      CREATE OR REPLACE TEMP TABLE mkt AS
      SELECT h.session, median(h.open1h_rvol) AS mkt_open1h, count(*) AS mkt_n
      FROM h1_feat h JOIN d1 USING (ticker, session) WHERE d1.eligible AND h.open1h_rvol IS NOT NULL
      GROUP BY 1""")
    m15_join = "LEFT JOIN m15_feat m USING (ticker, session)" if with_15m else ""
    m15_cols = ("m.v1, m.v2, m.v3, m.v4, m.r1, m.r2, m.r3, m.r4, m.n_slots AS m15_n_slots, m.open15_breadth, "
                "m.open15_concentration, m.open15_persistence, m.open15_shape, m.volume_transfer,") if with_15m else ""
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE X AS
      SELECT d.ticker, d.session, d.close, d.volume, d.atr_14, d.price_bucket, d.dv_bucket, d.atr_bucket,
             d.gap, d.gap_bucket, d.bar_gap_class, d.ceiling, d.in_range, d.base, d.breakout, d.breakout_q, d.base_rng,
             h.open1h_vol, h.open1h_rvol, h.open1h_ret, h.open1h_range_atr, h.open1h_close_loc, h.open1h_vol_1hstore,
             h.open1h_rvol_1, h.open1h_rvol_2, h.ramp_3d, h.ramp_mag, h.base_dryup, h.dryup_to_expansion,
             h.effort_result, h.h1_active_slots, h.h1_active_slots_1, h.breadth_shift,
             h.ramp_5d_slope, h.prior_hv_vol, h.prior_hv_reclaim, h.prior_hv_response,
             {m15_cols}
             k.mkt_open1h, k.mkt_n,
             CASE WHEN k.mkt_open1h > 0 THEN h.open1h_rvol / k.mkt_open1h END AS idio_open1h,
             '10:30' AS feature_available_at_ny, 'D+1 open' AS entry_at
      FROM d1 d LEFT JOIN h1_feat h USING (ticker, session) {m15_join}
      LEFT JOIN mkt k USING (session)
      WHERE d.eligible""")
    n = con.execute("SELECT count(*) FROM X").fetchone()[0]
    avail = {c: con.execute(f"SELECT count({c}) FROM X").fetchone()[0] for c in
             ["open1h_rvol", "ramp_3d", "ramp_5d_slope", "base_dryup", "effort_result", "breadth_shift",
              "prior_hv_reclaim", "prior_hv_response", "base", "breakout", "idio_open1h"]
             + (["open15_breadth", "open15_shape", "open15_persistence", "volume_transfer"] if with_15m else [])}
    if with_15m:
        parts = dict(con.execute("SELECT open15_shape, count(*) FROM X WHERE open15_shape IS NOT NULL GROUP BY 1").fetchall())
        assert_conservation(parts, avail["open15_shape"], "OPEN15_SHAPE")
    rep.update(rows=n, availability=avail, elapsed_s=round(time.time() - t0),
               tickers=con.execute("SELECT count(DISTINCT ticker) FROM X").fetchone()[0],
               sessions=con.execute("SELECT count(DISTINCT session) FROM X").fetchone()[0],
               base_days=con.execute("SELECT count(*) FROM X WHERE base").fetchone()[0],
               breakout_days=con.execute("SELECT count(*) FROM X WHERE breakout").fetchone()[0])
    out_parq = os.path.join(run.dir, "X.parquet")
    con.execute(f"COPY X TO '{out_parq}' (FORMAT PARQUET)")
    cb = con.execute("SELECT run_id, derived_sha256_16, canonical_sha256_16 FROM canon_binding").fetchone()
    rep["canonical_1d"] = dict(run_id=cb[0], derived_sha256_16=cb[1], canonical_sha256_16=cb[2])
    run.write_atomic("build_report.json", rep)
    run.complete("X.parquet")
    # Authority classification of THIS run (read by ovd_seal.latest_completed_run): only a
    # CANONICAL_1D_BASIS run may ever become input to the outcome phase.
    run.write_atomic("STATUS.json", dict(run_id=run.run_id, status="CANONICAL_1D_BASIS",
                                         canonical_1d=rep["canonical_1d"],
                                         basis="Massive daily aggregates adjusted=true as of the canonical fetch; dividends not adjusted; "
                                               "ATR/avg_vol/wt_* recomputed on canonical prices (ovd_canonical_derive)",
                                         classified_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
    con.close()
    return rep


def main():
    months = int(sys.argv[1]) if len(sys.argv) > 1 else 72
    with_15m = "--no-15m" not in sys.argv
    rep = build(months=months, with_15m=with_15m)
    print(json.dumps(rep, indent=1, default=str))


if __name__ == "__main__":
    main()
