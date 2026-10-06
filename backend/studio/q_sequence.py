"""Q Sequence — the Exact Sequence matcher, rebuilt on the Pine "260924_Q_WLNBB_CMB_MR_CONSENSUS" alphabet.

Same machinery as signal_stats.query_exact_sequence (LAG per bar, '*' wildcard, space = OR,
'!' = NOT, Williams-pivot HL/HH outcomes, live LEAD forward returns, MFE/MAE), but the lines are
the ones that script prints on a bar:

    q        Q0-Q8 + G/R  colour-blind body arrangement vs the previous bar (bodies, tol 0),
                          colour tag "All" mode: G close>open · R close<open        (computed here)
    l        L (WLNBB)                        l_sig
    vol      V0-V6 + ●▲▼ + ↑V/↓V + ↑v/↓v     median-ratio colour level, σ-agreement, Q volume
                                              support, same-colour volume move     (computed here)
    suffix   composite_full_suffix
    body_wick bar_body_wick                    (Pine line3)
    gap_range bar_gap_range + TRUE-gap "→"     (Pine line4: "G2→G1-N" when the true gap class differs)
    line5    bar_line5                         VIX / PSAR / RSI2
    line6    phys_line6                        R-OHM · regime · C · H
    ema      PREUP / PREDN                     P66 > P55 > P89 > P3 > P2 > P50 (Pine priority)
    wyc      phys_wyc                          Wyckoff lane 2
    rsi      rsi_14 numeric range

Q and the volume token are not stored in the DB; they are pure functions of the bar and its
predecessors, so they are computed in SQL on the deduped (one row per ticker-date) series — the
same series the LAG windows run on, so "previous bar" means the same thing in both places.

Additive. query_exact_sequence and its tab are untouched.
"""
from __future__ import annotations

import logging

from studio.db import get_conn, UNIVERSE_PRIORITY_SQL
from studio.signal_stats import _safe_universe, _multi_cond

log = logging.getLogger(__name__)

# line key → (bar-dict field, LAG alias prefix, label for the sequence string)
Q_LINES = [
    ("line1",  "q",         "q",   "Q"),
    ("line2",  "l",         "ls",  "L"),
    ("line3",  "vol",       "vt",  "VOL"),
    ("line4",  "suffix",    "sx",  "suffix"),
    ("line5",  "body_wick", "bw",  "body/wick"),
    ("line6",  "gap_range", "gr",  "gap/range"),
    ("line7",  "line5",     "l5",  "VIX/PSAR/RSI2"),
    ("line8",  "line6",     "l6",  "R·regime·C·H"),
    ("line9",  "ema",       "em",  "P/D"),
    ("line10", "wyc",       "wy",  "WYC"),
]
RSI_LINE = "line11"
DEFAULT_STRICT = {"line1": True, "line2": True}

# Pine colour level from volume / 20-bar median (inclusive of the current bar)
_VLEV = ("CASE WHEN vmed IS NULL OR vn < 20 THEN NULL "
         "WHEN vmed <= 0 THEN 3 "
         "WHEN volume / vmed < 0.40 THEN 0 WHEN volume / vmed < 0.70 THEN 1 "
         "WHEN volume / vmed < 1.00 THEN 2 WHEN volume / vmed < 1.50 THEN 3 "
         "WHEN volume / vmed < 2.50 THEN 4 WHEN volume / vmed < 5.00 THEN 5 ELSE 6 END")
# Pine "old" σ level from ta.bb(volume, 20, 1) — population stdev, like ta.stdev's default
_SLEV = ("CASE WHEN vn < 20 THEN NULL "
         "WHEN volume < vma - 1.5 * vsd THEN 0 WHEN volume < vma - vsd THEN 1 "
         "WHEN volume < vma - 0.5 * vsd THEN 2 WHEN volume < vma + 0.5 * vsd THEN 3 "
         "WHEN volume < vma + vsd THEN 4 WHEN volume < vma + 2.0 * vsd THEN 5 ELSE 6 END")
# Q code on body boxes, Pine evaluation order (equal > above > below > engulf > inside > overlap)
_QCODE = """CASE
  WHEN pqt IS NULL THEN NULL
  WHEN qt = pqt AND qb = pqb THEN 0
  WHEN qb > pqt THEN 1
  WHEN qt < pqb THEN 8
  WHEN qt >= pqt AND qb <= pqb THEN CASE WHEN (qt + qb) >= (pqt + pqb) THEN 3 ELSE 6 END
  WHEN qt <= pqt AND qb >= pqb THEN CASE WHEN (qt + qb) >= (pqt + pqb) THEN 4 ELSE 5 END
  WHEN qt > pqt THEN 2
  ELSE 7 END"""
_EMA = ("CASE WHEN sig_p66=1 THEN 'P66' WHEN sig_p55=1 THEN 'P55' WHEN sig_p89=1 THEN 'P89' "
        "WHEN sig_p3=1 THEN 'P3' WHEN sig_p2=1 THEN 'P2' WHEN sig_p50=1 THEN 'P50' "
        "WHEN sig_d66=1 THEN 'D66' WHEN sig_d55=1 THEN 'D55' WHEN sig_d89=1 THEN 'D89' "
        "WHEN sig_d3=1 THEN 'D3' WHEN sig_d2=1 THEN 'D2' WHEN sig_d50=1 THEN 'D50' ELSE '' END")
# Pine line4: gap class from bar_gap_range's "G*" head; append "→G*" when the TRUE gap differs
_GAP = ("CASE WHEN bar_gap_range LIKE 'G%' AND COALESCE(phys_gap_true, '') <> '' "
        "AND phys_gap_true <> split_part(bar_gap_range, '-', 1) "
        "THEN split_part(bar_gap_range, '-', 1) || '→' || phys_gap_true || "
        "CASE WHEN strpos(bar_gap_range, '-') > 0 THEN '-' || split_part(bar_gap_range, '-', 2) ELSE '' END "
        "ELSE COALESCE(bar_gap_range, '') END")


def _base_sql(universe: str | None, ticker: str | None = None) -> str:
    """One row per (ticker, date) with the Pine-only tokens (q_tok, v_tok) computed."""
    _uni = _safe_universe(universe)
    _w = ([f"universe = '{_uni}'"] if _uni else []) + \
         ([f"ticker = '{ticker}'"] if ticker else [])
    where = ("WHERE " + " AND ".join(_w)) if _w else ""
    return f"""
    d0 AS (
      SELECT * FROM bars {where}
      QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, date ORDER BY {UNIVERSE_PRIORITY_SQL}) = 1
    ),
    d1 AS (
      SELECT *,
        greatest(open, close) AS qt, least(open, close) AS qb,
        LAG(greatest(open, close)) OVER w AS pqt, LAG(least(open, close)) OVER w AS pqb,
        LAG(volume) OVER w AS pvol,
        median(volume)     OVER w20 AS vmed,
        avg(volume)        OVER w20 AS vma,
        stddev_pop(volume) OVER w20 AS vsd,
        count(volume)      OVER w20 AS vn
      FROM d0
      WINDOW w AS (PARTITION BY ticker ORDER BY date),
             w20 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
    ),
    d2 AS (
      SELECT *, {_QCODE} AS qcode, {_VLEV} AS vlev, {_SLEV} AS slev FROM d1
    ),
    d3 AS (
      SELECT *, LAG(vlev) OVER (PARTITION BY ticker ORDER BY date) AS pvlev FROM d2
    ),
    base AS (
      SELECT *,
        CASE WHEN qcode IS NULL THEN '' ELSE 'Q' || qcode ||
             CASE WHEN close > open THEN 'G' WHEN close < open THEN 'R' ELSE '' END END AS q_tok,
        CASE WHEN vlev IS NULL THEN '' ELSE 'V' || vlev ||
             CASE WHEN slev IS NULL THEN ''
                  WHEN vlev - slev = 0 THEN '●' WHEN vlev - slev >= 2 THEN '▲'
                  WHEN vlev - slev <= -2 THEN '▼' ELSE '' END ||
             CASE WHEN vlev >= 4 AND qcode BETWEEN 1 AND 4 THEN '↑V'
                  WHEN vlev >= 4 AND qcode BETWEEN 5 AND 8 THEN '↓V' ELSE '' END ||
             CASE WHEN qcode IS NOT NULL AND pvlev = vlev AND volume > pvol THEN '↑v'
                  WHEN qcode IS NOT NULL AND pvlev = vlev AND volume < pvol THEN '↓v' ELSE '' END
        END AS v_tok,
        {_EMA} AS ema_code,
        {_GAP} AS gap_tok
      FROM d3
    )"""


# DB column / expression feeding each LAG alias
_COL = {"q": "q_tok", "ls": "l_sig", "vt": "v_tok", "sx": "composite_full_suffix",
        "bw": "bar_body_wick", "gr": "gap_tok", "l5": "bar_line5", "l6": "phys_line6",
        "em": "ema_code", "wy": "phys_wyc", "rsi": "rsi_14"}


def _parse_range(v: str):
    v = (v or "").strip()
    if not v:
        return (None, None)
    parts = v.split('-')
    try:
        lo = float(parts[0]) if parts[0].strip() else None
        hi = float(parts[1]) if len(parts) > 1 and parts[1].strip() else None
        return (lo, hi)
    except Exception:
        return (None, None)


def _norm(v: str) -> str:
    """Upper-case + '*'→'%'. The ↑v/↓v lower-case marks survive: they are matched case-sensitively,
    so upper-casing would turn '↑v' into '↑V' — the volume token keeps its case (see _norm_vol)."""
    return (v or "").strip().upper().replace('*', '%')


def _norm_vol(v: str) -> str:
    s = (v or "").strip().replace('*', '%')
    # 'v4' → 'V4' for the level letter only; marks keep their case
    return " ".join(("!" if t.startswith("!") else "") + _fix_v(t.lstrip("!")) for t in s.split())


def _fix_v(t: str) -> str:
    return ("V" + t[1:]) if t[:1] == "v" and t[1:2].isdigit() else t


def query_q_sequence(
    bars: list[dict],
    universe: str | None = None,
    strictness: dict | None = None,
    pivot_lr: int = 3,
    conn=None,
    match_rows: bool = False,
    min_price: float | None = None,
    max_price: float | None = None,
    years: list | None = None,
    months: list | None = None,
) -> dict:
    n = len(bars)
    if n == 0:
        return {"matches": 0, "sequence_label": "", "outcomes": {}}
    strict = {**{k: False for k, *_ in Q_LINES}, RSI_LINE: False, **DEFAULT_STRICT,
              **(strictness or {})}
    pivot_lr = 3 if pivot_lr not in (3, 5) else pivot_lr
    P = str(pivot_lr)

    parsed = []
    for b in bars:
        b = b or {}
        p = {}
        for _line, field, _alias, _lab in Q_LINES:
            raw = b.get(field) or ""
            p[field] = _norm_vol(raw) if field == "vol" else _norm(raw)
            if field == "line6":            # plain '.' typed for the '·' separator (RA.U* = RA·U*)
                p[field] = p[field].replace(".", "·")
        p["rsi_rng"] = _parse_range(b.get("rsi") or "")
        parsed.append(p)

    _own = conn is None
    if _own:
        conn = get_conn(read_only=True)
    try:
        available = set(conn.execute("DESCRIBE bars").fetchdf()["column_name"].tolist())
        needed = {"l_sig", "composite_full_suffix", "bar_body_wick", "bar_gap_range", "bar_line5",
                  "phys_line6", "phys_gap_true", "phys_wyc", "rsi_14", "volume",
                  f"next_pivot_is_hl_{P}", f"next_pivot_is_hh_{P}"}
        missing = needed - available
        if missing:
            return {"matches": 0, "sequence_label": "", "outcomes": {},
                    "error": f"columns missing in this DB: {sorted(missing)}"}

        select_cols = ["ticker", "date", "universe"]
        for lag in range(n):
            for alias, col in _COL.items():
                select_cols.append(f"{col} AS {alias}_{lag}" if lag == 0
                                   else f"LAG({col}, {lag}) OVER w AS {alias}_{lag}")
        for col in (f"next_pivot_is_hl_{P}", f"next_pivot_is_hh_{P}",
                    f"pct_to_next_hl_{P}", f"pct_to_next_hh_{P}",
                    f"bars_to_next_hl_{P}", f"bars_to_next_hh_{P}"):
            select_cols.append(col)
        has_mfe = {"mfe_20d", "mae_20d"} <= available
        has_mfe_s = {"mfe_5d", "mae_5d", "mfe_10d", "mae_10d"} <= available
        if has_mfe:
            select_cols += ["mfe_20d", "mae_20d"]
        if has_mfe_s:
            select_cols += ["mfe_5d", "mae_5d", "mfe_10d", "mae_10d"]
        select_cols += ["LEAD(q_tok, 1) OVER w AS nxt_q", "close AS c_0",
                        "(LEAD(close, 5)  OVER w / close - 1) * 100 AS lf_5d",
                        "(LEAD(close, 10) OVER w / close - 1) * 100 AS lf_10d",
                        "(LEAD(close, 20) OVER w / close - 1) * 100 AS lf_20d"]

        conds = []
        for i, p in enumerate(parsed):
            lag = n - 1 - i
            for line, field, alias, _lab in Q_LINES:
                if strict.get(line) and p[field]:
                    c = _multi_cond(f"{alias}_{lag}", p[field])
                    if c:
                        conds.append(c)
            if strict.get(RSI_LINE):
                lo, hi = p["rsi_rng"]
                if lo is not None:
                    conds.append(f"rsi_{lag} >= {lo}")
                if hi is not None:
                    conds.append(f"rsi_{lag} <= {hi}")
        try:
            if min_price not in (None, ""):
                conds.append(f"c_0 >= {float(min_price)}")
            if max_price not in (None, ""):
                conds.append(f"c_0 <= {float(max_price)}")
        except (TypeError, ValueError):
            pass
        try:
            yrs = sorted({int(y) for y in (years or []) if str(y).strip()})
            if yrs:
                conds.append(f"EXTRACT(YEAR FROM date) IN ({','.join(map(str, yrs))})")
        except (TypeError, ValueError):
            pass
        try:
            mos = sorted({int(m) for m in (months or []) if str(m).strip() and 1 <= int(m) <= 12})
            if mos:
                conds.append(f"EXTRACT(MONTH FROM date) IN ({','.join(map(str, mos))})")
        except (TypeError, ValueError):
            pass
        where = ("WHERE " + " AND ".join(conds)) if conds else ""

        mfe_sql = """
          ,AVG(mfe_20d), AVG(mae_20d)
          ,COUNT(CASE WHEN mfe_20d >=  5 THEN 1 END)*100.0/NULLIF(COUNT(mfe_20d),0)
          ,COUNT(CASE WHEN mfe_20d >= 10 THEN 1 END)*100.0/NULLIF(COUNT(mfe_20d),0)
          ,COUNT(CASE WHEN mfe_20d >= 20 THEN 1 END)*100.0/NULLIF(COUNT(mfe_20d),0)
          ,COUNT(CASE WHEN mae_20d <= -4 THEN 1 END)*100.0/NULLIF(COUNT(mae_20d),0)
          ,COUNT(CASE WHEN mae_20d <= -8 THEN 1 END)*100.0/NULLIF(COUNT(mae_20d),0)
          ,COUNT(CASE WHEN mae_20d <=-15 THEN 1 END)*100.0/NULLIF(COUNT(mae_20d),0)""" if has_mfe else ""
        mfe_s_sql = ",AVG(mfe_5d), AVG(mae_5d), AVG(mfe_10d), AVG(mae_10d)" if has_mfe_s else ""

        # one materialised pass: the windowed frame is built once, every read below reuses it
        conn.execute(f"""CREATE OR REPLACE TEMP TABLE _qseq AS
            WITH {_base_sql(universe).strip()},
            lagged AS (SELECT {", ".join(select_cols)} FROM base
                       WINDOW w AS (PARTITION BY ticker ORDER BY date))
            SELECT * FROM lagged {where}""")
        baseline = conn.execute(
            f"SELECT COUNT(*) FROM (SELECT 1 FROM bars "
            f"{'WHERE universe = ' + repr(_safe_universe(universe)) if _safe_universe(universe) else ''} "
            f"GROUP BY ticker, date)").fetchone()[0]

        row = conn.execute(f"""
            SELECT COUNT(*),
              SUM(CASE WHEN next_pivot_is_hl_{P} = 1 THEN 1 ELSE 0 END),
              SUM(CASE WHEN next_pivot_is_hh_{P} = 1 THEN 1 ELSE 0 END),
              AVG(pct_to_next_hl_{P}), AVG(pct_to_next_hh_{P}),
              AVG(bars_to_next_hl_{P}), AVG(bars_to_next_hh_{P}),
              AVG(lf_5d), AVG(lf_10d), AVG(lf_20d),
              SUM(CASE WHEN lf_5d > 0 THEN 1 ELSE 0 END),
              SUM(CASE WHEN lf_10d > 0 THEN 1 ELSE 0 END),
              SUM(CASE WHEN lf_20d > 0 THEN 1 ELSE 0 END),
              COUNT(lf_5d), COUNT(lf_10d), COUNT(lf_20d)
              {mfe_sql} {mfe_s_sql}
            FROM _qseq""").fetchone()
        matches = int(row[0] or 0)
        hl_count, hh_count = int(row[1] or 0), int(row[2] or 0)
        known = hl_count + hh_count

        def _r(v, nd=2):
            return None if v is None else round(float(v), nd)

        def _pct(a, b):
            return round(a / b * 100, 1) if b > 0 else None

        o = {"hl_count": hl_count, "hh_count": hh_count, "next_pivot_known": known,
             "hl_pct": _pct(hl_count, known), "hh_pct": _pct(hh_count, known),
             "avg_pct_to_hl": _r(row[3]), "avg_pct_to_hh": _r(row[4]),
             "avg_bars_to_hl": _r(row[5], 1), "avg_bars_to_hh": _r(row[6], 1),
             "avg_fwd_5d": _r(row[7]), "avg_fwd_10d": _r(row[8]), "avg_fwd_20d": _r(row[9]),
             "win_5d_pct": _pct(int(row[10] or 0), int(row[13] or 0)),
             "win_10d_pct": _pct(int(row[11] or 0), int(row[14] or 0)),
             "win_20d_pct": _pct(int(row[12] or 0), int(row[15] or 0)),
             "fwd_5d_n": int(row[13] or 0), "fwd_10d_n": int(row[14] or 0),
             "fwd_20d_n": int(row[15] or 0)}
        k = 16
        if has_mfe:
            o.update({"avg_mfe_20d": _r(row[16]), "avg_mae_20d": _r(row[17]),
                      "spike_5pct": _r(row[18], 1), "spike_10pct": _r(row[19], 1),
                      "spike_20pct": _r(row[20], 1), "drop_4pct": _r(row[21], 1),
                      "drop_8pct": _r(row[22], 1), "drop_15pct": _r(row[23], 1)})
            k = 24
        if has_mfe_s:
            o.update({"avg_mfe_5d": _r(row[k]), "avg_mae_5d": _r(row[k + 1]),
                      "avg_mfe_10d": _r(row[k + 2]), "avg_mae_10d": _r(row[k + 3])})

        next_bar, next_total = [], 0
        if matches:
            nb = conn.execute("""SELECT nxt_q, COUNT(*) c FROM _qseq
                                 WHERE COALESCE(nxt_q, '') <> '' GROUP BY 1 ORDER BY 2 DESC""").fetchall()
            next_total = sum(int(c) for _, c in nb)
            for s, c in nb[:12]:
                code = int(s[1]) if len(s) > 1 and s[1].isdigit() else -1
                next_bar.append({"sig": s, "count": int(c),
                                 "pct": round(int(c) / next_total * 100, 1) if next_total else 0,
                                 "is_bull": 1 <= code <= 4, "is_bear": 5 <= code <= 8})

        rows = None
        if match_rows and matches:
            rows = [{"ticker": r[0], "date": str(r[1]), "universe": r[2], "hh": r[3], "hl": r[4],
                     "fwd_5d": r[5], "fwd_10d": r[6], "fwd_20d": r[7], "q": r[8], "vol": r[9]}
                    for r in conn.execute(f"""SELECT ticker, CAST(date AS DATE), universe,
                            next_pivot_is_hh_{P}, next_pivot_is_hl_{P}, lf_5d, lf_10d, lf_20d, q_0, vt_0
                            FROM _qseq ORDER BY date DESC""").fetchall()]
        conn.execute("DROP TABLE IF EXISTS _qseq")

        def _lab(p):
            parts = [p[f] or "—" for _l, f, _a, _x in Q_LINES[:3]]
            parts += [p[f] for _l, f, _a, _x in Q_LINES[3:] if p[f]]
            lo, hi = p["rsi_rng"]
            if lo is not None or hi is not None:
                parts.append(f"RSI{'' if lo is None else lo}-{'' if hi is None else hi}")
            return " / ".join(parts).replace("%", "*")

        return {"matches": matches, "baseline": int(baseline),
                "sequence_label": "  ➜  ".join(_lab(p) for p in parsed),
                "outcomes": o, "next_bar": next_bar, "next_bar_total": next_total,
                "pivot_lr": pivot_lr, "n_bars": n, "strictness": strict,
                **({"rows": rows} if rows is not None else {})}
    except Exception as exc:
        log.exception("query_q_sequence failed")
        return {"matches": 0, "sequence_label": "", "outcomes": {}, "error": str(exc)}
    finally:
        if _own:
            conn.close()


def describe_bars(ticker: str, n: int = 6, conn=None, universe: str | None = None) -> list[dict]:
    """The last n bars of one ticker in this tab's alphabet — fills the builder from a real chart."""
    _own = conn is None
    if _own:
        conn = get_conn(read_only=True)
    try:
        t = str(ticker or "").strip().upper().replace("'", "")
        sql = f"""WITH {_base_sql(universe, ticker=t).strip()}
            SELECT CAST(date AS DATE), q_tok, l_sig, v_tok, composite_full_suffix, bar_body_wick,
                   gap_tok, bar_line5, phys_line6, ema_code, phys_wyc, rsi_14
            FROM base ORDER BY date DESC LIMIT {int(n)}"""
        out = []
        for r in reversed(conn.execute(sql).fetchall()):
            out.append({"date": str(r[0]), "q": r[1] or "", "l": r[2] or "", "vol": r[3] or "",
                        "suffix": r[4] or "", "body_wick": r[5] or "", "gap_range": r[6] or "",
                        "line5": r[7] or "", "line6": r[8] or "", "ema": r[9] or "",
                        "wyc": r[10] or "", "rsi_val": None if r[11] is None else round(float(r[11]), 1)})
        return out
    finally:
        if _own:
            conn.close()
