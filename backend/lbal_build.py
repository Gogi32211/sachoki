"""L-BAL — 15m WLNBB L-label balance of each 1D session + the UDN / UDN+ agreement states.

WHAT IT IS
  A DESCRIPTIVE per-session decomposition of the daily bar into its 15-minute bars, exactly as the
  TradingView script "260906_LTF_L_COUNT + OVD 4 VOLUME LOGICS" draws it: the same WLNBB arithmetic,
  the same exact-label counting, the same two lines (UDN / UDN+) and the same five marks
  (★ · ★★ · ★★★ · ○○○ · XXX). Nothing here is an edge, a score input or a filter with claimed lift.

STATUS OF EVIDENCE — read this before trusting a mark
  The DIRECTION of intraday effort was tested as the research family INTRADAY_EFFORT_BALANCE_V1
  (k = 16, MINE/VERIFY OOS reserved before any outcome, direct sacred _pathsim): 16/16 NULL on
  2026-09-06 — every cell within ±0.26 pp of its same-day control. UDN carries the daily candle
  (Spearman 0.48 with the same-day return) and no tested increment beyond it. The marks exist so a
  bar can be READ the way the TradingView label reads it, not so it can be ranked. Never injected into
  EDGE / BUY / ULTRA scoring.

DEFINITIONS (frozen here; identical to the Pine script defaults)
  15m L-code = exact Pine WLNBB (ieb_family.lcode_sql, the fixture-tested port): ta.bb(volume, 20, 1)
    buckets W/L/N2/B/VB → bucketUp/Down → volUp/DownAdapted;
    L1 vol↓ & close>c1 · L2 vol↓ & close>=l1 · L3 vol↑ & close>c1 · L4 vol↑ & close<=h1 ·
    L5 vol↓ & close<c1 · L6 vol↑ & close<c1
    exact labels: L12=3 · L25=18 · L34=12 · L46=40 · L2=2 · L3=4 · L4=8 · L5=16
    (L1-alone and L6-alone cannot occur — L1 ⊂ L12, L6 ⊂ L46).
  MODE = "V" (user, 2026-09-06): only 15m bars with volume > SMA20 × 1.0 are counted, in BOTH lines — the Pine
    "Count only volume-confirmed lower-TF bars" switch that the user's TradingView chart has ON (labels L34V…).
    The every-bar variant (Pine default) is kept in the *_all columns.
  UDN  (line 1, "effort"):  n_pos = #{L34, L3 bars} · n_neg = #{L46 bars}
                             → U if n_pos > n_neg · D if n_neg > n_pos · else N
  UDN+ (line 2, "candle"):  n_pos_c = #{labelled 15m bars with close > open} · n_neg_c = #{… close < open}
                             → U / D / N likewise
  ★    divergence      the UDN (effort) line against the DAILY candle: U ∧ close<open, or D ∧ close>open
  ★★   conflict        (D ∧ U+) or (U ∧ D+)
  ★★★  half-up         (U ∧ N+) or (N ∧ U+)
  ○○○  half-down       (D ∧ N+) or (N ∧ D+)
  XXX  double-neutral  N ∧ N+
  Session = the America/New_York calendar day of the 15m stamp (store stamps are naive UTC). The 15m
  store holds regular-session bars only (26 per full session), so no RTH filter is applied. The daily
  colour comes from the 1D analytics store (the bar the app draws); a session missing there falls back
  to the 15m first-open / last-close (colour_src = '15m'). A session with ZERO labelled bars gets no
  state (udn NULL, marks '') — "no data" is not "double neutral".

OUTPUT
  data/lbal_signals.parquet   one row per (ticker, session): counts, udn, udn_c, colour, five flags,
                              marks (space-joined symbols) and text ("U 5:2 · D+ 8:11").
                              Sorted by (date, ticker): the Ultra enrichment filters by date, the
                              chart / Superchart route filters by ticker. Rebuilt from scratch every run.
  data/LBAL_SIGNALS_V1.json   spec + census (as_of, rows, state census, source store mtime).
RUN
  backend/.venv/bin/python backend/lbal_build.py      # after the nightly 15m derive (~06:20 Tbilisi)
"""
from __future__ import annotations
import os
import sys
import json
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from studio.paths import DATA_DIR, db_path, ANALYTICS_DB          # noqa: E402

OUT = os.path.join(DATA_DIR, "lbal_signals.parquet")
SPEC = os.path.join(DATA_DIR, "LBAL_SIGNALS_V1.json")
M15_DB = db_path("15m")
SPEC_ID = "LBAL_SIGNALS_V1"

# the five marks, in the order they are printed
MARKS = (("star", "★"), ("conflict", "★★"), ("half_up", "★★★"), ("half_dn", "○○○"), ("nn", "XXX"))
MARK_MEANING = {
    "★":   "divergence — UDN (effort: L34+L3 vs L46) opposes the daily candle",
    "★★":  "conflict — effort line and candle line oppose each other (D & U+ / U & D+)",
    "★★★": "half-up — one line U, the other neutral (U & N+ / N & U+)",
    "○○○": "half-down — one line D, the other neutral (D & N+ / N & D+)",
    "XXX": "double-neutral — both lines N",
}
# An outcome column reaching this table would let the chart show a result no study claimed.
FORBIDDEN = {"ret", "mfe", "mae", "ret_10d", "mfe_10d", "mae_10d", "median", "win", "pf", "edge"}
EFFORT_POS = (12, 4)      # L34, L3
EFFORT_NEG = (40,)        # L46
# Counting mode of the PRIMARY state. "V" = only 15m bars with volume > SMA20 × 1.0 are counted (the
# Pine "Count only volume-confirmed lower-TF bars" switch, which the user's TradingView chart has ON —
# its labels read L34V / L46V). "ALL" = every labelled bar (Pine default). Chosen by the user 2026-09-06.
MODE = "V"


class LbalGateFailure(RuntimeError):
    pass


# ── L-VX: the "260906_WLNBB_L34_L46_VX_CHART" Pine script, ported (same run, same stores) ──────
# Daily L34 / L46 (exact labels) graded by volume and by lower-TF echo:
#   V   daily volume > SMA20(daily volume) × 1.0
#   per lower TF (15m, 60m): level 2 (H) if ≥ LVX_MIN_NH bars inside the day are L34V/L46V,
#                            level 1 (L) if ≥ LVX_MIN_NL bars are plain L34/L46, else 0
#   VX  V ∧ both TFs ≥ LVX_X_NEED ("Any confirmation on both TFs", the script default)
#   VH  V ∧ max(level) == 2 · VL  V ∧ max(level) ≥ 1 · V  V alone · plain  daily label only
#   tier 4 VX > 3 VH > 2 VL > 1 V > 0 plain.  Descriptive, exactly like L-BAL.
LVX_OUT = os.path.join(DATA_DIR, "lvx_signals.parquet")
LVX_MIN_NL, LVX_MIN_NH, LVX_X_NEED = 1, 1, 1
LVX_TIER = {0: "", 1: "V", 2: "VL", 3: "VH", 4: "VX"}
LVX_FAM = {12: "L34", 40: "L46"}


def level_of(n_plain: int, n_v: int, has_tf: bool = True) -> int:
    if not has_tf:
        return 0
    return 2 if n_v >= LVX_MIN_NH else (1 if n_plain >= LVX_MIN_NL else 0)


def tier_of(v: bool, lv15: int, lv1h: int) -> int:
    if not v:
        return 0
    if lv15 >= LVX_X_NEED and lv1h >= LVX_X_NEED:
        return 4
    mx = max(lv15, lv1h)
    return 3 if mx == 2 else (2 if mx >= 1 else 1)


def lvx_counts_sql(codes_tbl: str) -> str:
    return f"""
    SELECT ticker, session, count(*) AS n_bars,
           count(*) FILTER (WHERE code = 12) AS n34, count(*) FILTER (WHERE code = 12 AND v_above) AS n34v,
           count(*) FILTER (WHERE code = 40) AS n46, count(*) FILTER (WHERE code = 40 AND v_above) AS n46v
    FROM {codes_tbl} GROUP BY 1, 2"""


def lvx_frame(daily, c15, c1h):
    """daily: rows with the exact daily label (code 12 / 40) + v_above + colour; c15 / c1h: per-session
    family counts. One row per (ticker, date) — a day is exactly one label, never both."""
    import numpy as np
    import pandas as pd
    df = daily.merge(c15, on=["ticker", "session"], how="left", suffixes=("", "_15")) \
              .merge(c1h.rename(columns={c: c + "_1h" for c in c1h.columns if c not in ("ticker", "session")}),
                     on=["ticker", "session"], how="left")
    for c in ("n_bars", "n34", "n34v", "n46", "n46v", "n_bars_1h", "n34_1h", "n34v_1h", "n46_1h", "n46v_1h"):
        df[c] = df[c].fillna(0).astype("int32")
    is34 = df["code"].to_numpy() == 12
    df["fam"] = np.where(is34, "L34", "L46")
    df["n_15"] = np.where(is34, df["n34"], df["n46"]); df["nv_15"] = np.where(is34, df["n34v"], df["n46v"])
    df["n_1h"] = np.where(is34, df["n34_1h"], df["n46_1h"]); df["nv_1h"] = np.where(is34, df["n34v_1h"], df["n46v_1h"])
    df["lv_15"] = [level_of(int(a), int(b), c > 0) for a, b, c in zip(df["n_15"], df["nv_15"], df["n_bars"])]
    df["lv_1h"] = [level_of(int(a), int(b), c > 0) for a, b, c in zip(df["n_1h"], df["nv_1h"], df["n_bars_1h"])]
    df["v"] = df["v_above"].astype(bool)
    df["tier"] = [tier_of(bool(v), int(a), int(b)) for v, a, b in zip(df["v"], df["lv_15"], df["lv_1h"])]
    df["label"] = [f + LVX_TIER[t] for f, t in zip(df["fam"], df["tier"])]
    lvc = {0: "-", 1: "L", 2: "H"}
    df["text"] = [f"{lab} · 15m {lvc[a]} ({p15}/{v15}v) · 1h {lvc[b]} ({p1h}/{v1h}v)"
                  for lab, a, b, p15, v15, p1h, v1h in zip(df["label"], df["lv_15"], df["lv_1h"], df["n_15"], df["nv_15"], df["n_1h"], df["nv_1h"])]
    df = df.rename(columns={"session": "date", "n_bars": "bars_15", "n_bars_1h": "bars_1h"})
    return df[["ticker", "date", "fam", "colour", "v", "lv_15", "lv_1h", "n_15", "nv_15", "n_1h", "nv_1h",
               "bars_15", "bars_1h", "tier", "label", "text"]].sort_values(["date", "ticker"]).reset_index(drop=True)


def udn_of(pos: int, neg: int) -> str:
    return "U" if pos > neg else ("D" if neg > pos else "N")


def states(udn: str, udn_c: str, colour: str) -> dict:
    """The five agreement flags from the two lines and the daily colour (pure, fixture-tested)."""
    star = (udn == "U" and colour == "RED") or (udn == "D" and colour == "GREEN")
    conflict = (udn == "D" and udn_c == "U") or (udn == "U" and udn_c == "D")
    half_up = (udn == "U" and udn_c == "N") or (udn == "N" and udn_c == "U")
    half_dn = (udn == "D" and udn_c == "N") or (udn == "N" and udn_c == "D")
    nn = udn == "N" and udn_c == "N"
    f = dict(star=star, conflict=conflict, half_up=half_up, half_dn=half_dn, nn=nn)
    f["marks"] = " ".join(sym for key, sym in MARKS if f[key])
    return f


def counts_sql(codes_tbl: str, src: str) -> str:
    """Per-session counts in BOTH modes. The L-code CTE only carries the code and the v_above flag;
    the 15m candle colour needed for UDN+ is re-joined from the source bars on the (ticker, stamp)
    grain. `n_*` = every labelled 15m bar (Pine onlyV = false); `nv_*` = only bars with volume >
    SMA20 × 1.0 (Pine onlyV = true, the "L34V" labels) — the Pine gate sits above ALL counters,
    the candle line included, so both lines are filtered the same way."""
    pos = ", ".join(str(c) for c in EFFORT_POS)
    neg = ", ".join(str(c) for c in EFFORT_NEG)
    cnt = []
    for pre, gate in (("n", ""), ("nv", "c.v_above AND ")):
        cnt += [
            f"count(*) FILTER (WHERE {gate}c.code <> 0)                   AS {pre}_lab",
            f"count(*) FILTER (WHERE {gate}c.code IN ({pos}))             AS {pre}_pos",
            f"count(*) FILTER (WHERE {gate}c.code IN ({neg}))             AS {pre}_neg",
            f"count(*) FILTER (WHERE {gate}c.code <> 0 AND b.close > b.open) AS {pre}_pos_c",
            f"count(*) FILTER (WHERE {gate}c.code <> 0 AND b.close < b.open) AS {pre}_neg_c",
            f"count(*) FILTER (WHERE {gate}c.code = 12)                   AS {pre}_l34",
            f"count(*) FILTER (WHERE {gate}c.code = 4)                    AS {pre}_l3",
            f"count(*) FILTER (WHERE {gate}c.code = 40)                   AS {pre}_l46",
            f"count(*) FILTER (WHERE {gate}c.code = 3)                    AS {pre}_l12",
            f"count(*) FILTER (WHERE {gate}c.code = 18)                   AS {pre}_l25",
            f"count(*) FILTER (WHERE {gate}c.code = 2)                    AS {pre}_l2",
            f"count(*) FILTER (WHERE {gate}c.code = 8)                    AS {pre}_l4",
            f"count(*) FILTER (WHERE {gate}c.code = 16)                   AS {pre}_l5",
        ]
    return f"""
    SELECT c.ticker, c.session,
           count(*)                                              AS n_bars,
           count(*) FILTER (WHERE c.v_above)                     AS nv_bars,
           {", ".join(cnt)},
           arg_min(b.open, c.date)                               AS open_15,
           arg_max(b.close, c.date)                              AS close_15
    FROM {codes_tbl} c
    JOIN {src} b ON b.ticker = c.ticker AND b.date = c.date
    GROUP BY 1, 2"""


def mode_states(df, pre: str):
    """udn / udn_c / five flags / marks / text for one counting mode (`n` = all labelled bars, `nv` =
    volume-confirmed only). A session with zero labelled bars in that mode gets no state."""
    import numpy as np
    lab = df[f"{pre}_lab"].to_numpy() > 0
    P, N, PC, NC = (df[f"{pre}_{c}"].to_numpy() for c in ("pos", "neg", "pos_c", "neg_c"))
    udn = [udn_of(int(p), int(n)) if ok else None for p, n, ok in zip(P, N, lab)]
    udn_c = [udn_of(int(p), int(n)) if ok else None for p, n, ok in zip(PC, NC, lab)]
    empty = dict(star=False, conflict=False, half_up=False, half_dn=False, nn=False, marks="")
    flags = [states(u, uc, col) if u is not None else empty for u, uc, col in zip(udn, udn_c, df["colour"])]
    text = [f"{u} {p}:{n} · {uc}+ {pc}:{nc}" if u is not None else ""
            for u, p, n, uc, pc, nc in zip(udn, P, N, udn_c, PC, NC)]
    return udn, udn_c, flags, text


def build(log=print):
    import duckdb
    import numpy as np
    import pandas as pd
    from ieb_family import lcode_sql          # the fixture-tested exact-Pine port; never re-implemented here

    t0 = time.time()
    src_mtime = os.path.getmtime(M15_DB)
    con = duckdb.connect()
    con.execute("pragma threads=8")
    con.execute(f"ATTACH '{M15_DB}' AS m15 (READ_ONLY)")
    con.execute(f"CREATE TEMP TABLE codes AS {lcode_sql('m15.bars')}")
    n_codes = con.execute("SELECT count(*) FROM codes").fetchone()[0]
    log(f"  15m L-codes: {n_codes:,} bars ({time.time() - t0:.0f}s)")
    con.execute(f"CREATE TEMP TABLE sc AS {counts_sql('codes', 'm15.bars')}")
    sc = con.execute("SELECT * FROM sc").df()
    import ovd_map_build as OM                # the OVD daily-map port (display only) shares this run's stores
    con.execute(f"CREATE TEMP TABLE ovd_slots AS {OM.slots_sql('m15.bars')}")
    con.execute("DETACH m15")
    log(f"  sessions: {len(sc):,} ({time.time() - t0:.0f}s)")

    # ── L-VX inputs: 15m family counts (from the codes above), 60m family counts, daily exact labels ──
    c15 = con.execute(lvx_counts_sql("codes")).df()
    con.execute(f"ATTACH '{db_path('1h')}' AS h1 (READ_ONLY)")
    con.execute(f"CREATE TEMP TABLE codes_1h AS {lcode_sql('h1.bars')}")
    c1h = con.execute(lvx_counts_sql("codes_1h")).df()
    con.execute("DETACH h1")
    log(f"  1h L-codes: {len(c1h):,} ticker-sessions ({time.time() - t0:.0f}s)")
    # daily bars are stamped at midnight (naive); +15h keeps the NY session on the same calendar day
    con.execute(f"ATTACH '{ANALYTICS_DB}' AS d1 (READ_ONLY)")
    con.execute("""CREATE TEMP TABLE d1src AS
                   SELECT ticker, CAST(date AS TIMESTAMP) + INTERVAL 15 HOUR AS date, open, high, low, close, volume
                   FROM d1.bars WHERE universe <> 'index'
                   QUALIFY row_number() OVER (PARTITION BY ticker, date ORDER BY universe) = 1""")
    con.execute(f"CREATE TEMP TABLE codes_1d AS {lcode_sql('d1src')}")
    daily = con.execute("""SELECT c.ticker, c.session, c.code, c.v_above,
                                  CASE WHEN s.close > s.open THEN 'GREEN' WHEN s.close < s.open THEN 'RED' ELSE 'DOJI' END AS colour
                           FROM codes_1d c JOIN d1src s ON s.ticker = c.ticker AND s.date = c.date
                           WHERE c.code IN (12, 40)""").df()
    daily_close = con.execute("SELECT ticker, strftime(CAST(date AS DATE), '%Y-%m-%d') AS date, close FROM d1src ORDER BY ticker, date").df()
    daily_ohlcv = con.execute("SELECT ticker, strftime(CAST(date AS DATE), '%Y-%m-%d') AS date, open, high, low, close, volume FROM d1src ORDER BY ticker, date").df()
    con.execute("DETACH d1")
    for f in (c15, c1h, daily):
        f["session"] = pd.to_datetime(f["session"]).dt.strftime("%Y-%m-%d")
    lvx = lvx_frame(daily, c15, c1h)
    log(f"  L-VX: {len(lvx):,} daily L34/L46 sessions ({time.time() - t0:.0f}s)")
    ovd = OM.build(con.execute("SELECT * FROM ovd_slots").df(), daily_close, log)
    import vol7_build as V7                    # the 7-level volume regime port (display only), daily bars only
    vol7 = V7.build(daily_ohlcv, log)
    log(f"  VOL7 done ({time.time() - t0:.0f}s)")
    log(f"  OVD map: {len(ovd):,} token days ({time.time() - t0:.0f}s)")

    # daily candle from the 1D analytics store — the bar the app draws. Per-universe duplicate rows
    # are collapsed exactly as mt5_build does (row_number over universe); OHLC is identical across them.
    a = duckdb.connect(ANALYTICS_DB, read_only=True)
    a.execute("pragma threads=8")
    as_of = str(a.execute("SELECT max(date) FROM bars").fetchone()[0])[:10]
    d1 = a.execute("""
        WITH r AS (SELECT ticker, CAST(date AS DATE) AS session, open, close,
                          row_number() OVER (PARTITION BY ticker, CAST(date AS DATE) ORDER BY universe) rn
                   FROM bars WHERE universe <> 'index')
        SELECT ticker, session, open AS open_1d, close AS close_1d FROM r WHERE rn = 1""").df()
    a.close()

    sc["session"] = pd.to_datetime(sc["session"]).dt.strftime("%Y-%m-%d")
    d1["session"] = pd.to_datetime(d1["session"]).dt.strftime("%Y-%m-%d")
    df = sc.merge(d1, on=["ticker", "session"], how="left")
    has_1d = df["close_1d"].notna()
    o = df["open_1d"].where(has_1d, df["open_15"])
    c = df["close_1d"].where(has_1d, df["close_15"])
    df["colour"] = np.where(c > o, "GREEN", np.where(c < o, "RED", "DOJI"))
    df["colour_src"] = np.where(has_1d, "1d", "15m")

    CNT = ("lab", "pos", "neg", "pos_c", "neg_c", "l34", "l3", "l46", "l12", "l25", "l2", "l4", "l5")
    for col in ["n_bars", "nv_bars"] + [f"{p}_{c}" for p in ("n", "nv") for c in CNT]:
        df[col] = df[col].astype("int32")
    # PRIMARY state = MODE (V: volume-confirmed bars only, the user's TradingView setting); the other
    # mode is kept as *_all / *_v columns so a later toggle needs no rebuild.
    pri, alt = ("nv", "n") if MODE == "V" else ("n", "nv")
    for pre, suffix in ((pri, ""), (alt, "_all" if MODE == "V" else "_v")):
        udn, udn_c, flags, text = mode_states(df, pre)
        df["udn" + suffix] = udn
        df["udn_c" + suffix] = udn_c
        for key in ("star", "conflict", "half_up", "half_dn", "nn"):
            df[key + suffix] = [f[key] for f in flags]
        df["marks" + suffix] = [f["marks"] for f in flags]
        df["text" + suffix] = text
    labelled = df[f"{pri}_lab"] > 0
    alt_sfx = "_all" if MODE == "V" else "_v"
    df = df.rename(columns={"session": "date"})
    df = df[["ticker", "date", "n_bars", "nv_bars"] + [f"{p}_{c}" for p in ("n", "nv") for c in CNT]
            + ["colour", "colour_src", "udn", "udn_c", "star", "conflict", "half_up", "half_dn", "nn", "marks", "text"]
            + [k + alt_sfx for k in ("udn", "udn_c", "star", "conflict", "half_up", "half_dn", "nn", "marks", "text")]]
    df = df.sort_values(["date", "ticker"]).reset_index(drop=True)

    # gates — a defect here must stop the build, not ship a quiet zero
    bad = set(df.columns) & FORBIDDEN
    if bad:
        raise LbalGateFailure(f"outcome column reached L-BAL: {bad}")
    if df.duplicated(["ticker", "date"]).any():
        raise LbalGateFailure("duplicate (ticker, date) grain")
    if int(labelled.sum()) == 0:
        raise LbalGateFailure("zero labelled sessions — that is a join defect, not a finding")
    if int((df["colour_src"] == "1d").sum()) == 0:
        raise LbalGateFailure("no session matched the 1D analytics store — session/date join defect")
    chk = df[labelled]
    toks = chk["marks"].str.split(" ")
    for key, sym in MARKS:                      # token-wise: "★" is a substring of "★★", so no str.contains
        if (chk[key] != toks.apply(lambda t, s=sym: s in t)).any():
            raise LbalGateFailure(f"marks string disagrees with flag {key}")
    if ((chk["star"] & (chk["colour"] == "DOJI"))).any():
        raise LbalGateFailure("★ on a doji — divergence needs a coloured candle")

    census = dict(
        mode=MODE, rows=int(len(df)), tickers=int(df["ticker"].nunique()), labelled_rows=int(labelled.sum()),
        date_range=[str(df["date"].min()), str(df["date"].max())],
        colour_src=df["colour_src"].value_counts().to_dict(),
        bars_per_session_mode=int(df["n_bars"].mode().iloc[0]),
        v_bars_per_session_median=float(df["nv_bars"].median()),
        udn=chk["udn"].value_counts().to_dict(), udn_c=chk["udn_c"].value_counts().to_dict(),
        marks={sym: int(chk[key].sum()) for key, sym in MARKS},
        no_mark_rows=int((chk["marks"] == "").sum()),
        pair=chk.groupby(["udn", "udn_c"]).size().rename("n").reset_index().assign(k=lambda x: x.udn + "/" + x.udn_c + "+").set_index("k")["n"].to_dict(),
    )
    # L-VX gates + census
    if lvx.duplicated(["ticker", "date"]).any():
        raise LbalGateFailure("L-VX: duplicate (ticker, date) grain")
    if not set(lvx["fam"].unique()) <= {"L34", "L46"}:
        raise LbalGateFailure("L-VX: unknown family")
    if ((lvx["tier"] == 4) & ~lvx["v"]).any() or ((lvx["tier"] >= 1) & ~lvx["v"]).any():
        raise LbalGateFailure("L-VX: a graded tier without daily volume confirmation")
    if int((lvx["tier"] == 4).sum()) == 0:
        raise LbalGateFailure("L-VX: zero VX rows — that is a join defect, not a finding")
    census["lvx"] = dict(rows=int(len(lvx)), fam=lvx["fam"].value_counts().to_dict(),
                         tier={f"{k}:{LVX_TIER[k] or 'plain'}": int(v) for k, v in lvx["tier"].value_counts().sort_index().items()},
                         label=lvx["label"].value_counts().to_dict(),
                         sessions_without_1h=int((lvx["bars_1h"] == 0).sum()), sessions_without_15m=int((lvx["bars_15"] == 0).sum()))
    census["ovdmap"] = dict(rows=int(len(ovd)), tokens={sym: int(ovd[key].sum()) for key, sym in OM.TOKENS},
                            hv_event_days=int(ovd["hv_event"].sum()))
    census["vol7"] = dict(rows=int(len(vol7)), mr={f"M{k}": int(v) for k, v in vol7["mr"].value_counts().sort_index().items()},
                          cons=vol7["cons"].value_counts().to_dict(), jump=vol7["jump"].value_counts().to_dict(),
                          vb2=int(vol7["vb2"].sum()), shift_up=int(vol7["shift_up"].sum()), shift_dn=int(vol7["shift_dn"].sum()))
    return df, as_of, src_mtime, census, time.time() - t0, lvx, ovd, vol7


def main():
    # Build into temp files and swap them in with os.replace: the backend reads the parquet on
    # every request, so the nightly rebuild must never expose a missing or half-written file.
    # A failed build leaves the previous complete file in place (the exception propagates).
    import ovd_map_build as OM
    import vol7_build as V7
    df, as_of, src_mtime, census, secs, lvx, ovd, vol7 = build()
    tmp_out, tmp_spec, tmp_lvx, tmp_ovd, tmp_v7 = OUT + ".tmp", SPEC + ".tmp", LVX_OUT + ".tmp", OM.OUT + ".tmp", V7.OUT + ".tmp"
    df.to_parquet(tmp_out, index=False)
    lvx.to_parquet(tmp_lvx, index=False)
    ovd.to_parquet(tmp_ovd, index=False)
    vol7.to_parquet(tmp_v7, index=False)
    spec = dict(
        spec_id=SPEC_ID, status="BUILT", as_of=as_of, mode=MODE,
        mode_note=("V: only 15m bars with volume > SMA20 × 1.0 are counted (Pine 'Count only volume-confirmed lower-TF "
                   "bars' = ON, the user's TradingView setting); the every-bar variant is kept in the *_all columns"),
        built_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), build_seconds=round(secs, 1),
        source=dict(store_15m=os.path.realpath(M15_DB), store_15m_mtime=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(src_mtime)),
                    analytics_1d=os.path.realpath(ANALYTICS_DB), lcode="ieb_family.lcode_sql (exact Pine WLNBB, fixture-tested)"),
        status_of_evidence=dict(
            claim="DESCRIPTIVE ONLY", is_edge=False, is_ranking_input=False, is_forward_validated=False,
            research_family="INTRADAY_EFFORT_BALANCE_V1 — 16/16 NULL (2026-09-06); the effort direction adds nothing "
                            "measurable to the daily candle under the registered standard",
            read_as="the TradingView label ported: which side did the 15m L-labels take, and does it agree with the candle"),
        vol7=dict(
            parquet=os.path.basename(V7.OUT), source_script="260829 • 7-Level Volume MR + Sigma Consensus/Divergence + B/VB + Shift "
                                                              "(and 260828 sigma-only), Pine v6, ported for display; daily bars only",
            marks=V7.MARK_MEANING, primary="MR (median-ratio) levels M0..M6; σ0..σ6 kept as the comparison channel",
            naming="M5/M6 are the script's B/VB — deliberately NOT named B/VB, which in this app is the WLNBB bucket the book edges use",
            status="DESCRIPTIVE ONLY; SHIFT is a setup marker, unstudied"),
        ovdmap=dict(
            parquet=os.path.basename(OM.OUT), source_script="260904_OVD_4_VOLUME_LOGICS_DAILY_MAP (Pine v6), ported for display",
            tokens=OM.TOKEN_MEANING, rvol="same-slot value / median of the last 20 FULL regular sessions strictly before the day",
            status="DESCRIPTIVE ONLY — the sealed OPENING_VOLUME_DYNAMICS family closed 0 BUILD / 0 VETO (2026-09-04); "
                   "the script reproduces the four logics visually, the sealed A/B sample gating is not reproduced"),
        lvx=dict(
            parquet=os.path.basename(LVX_OUT), source_script="260906_WLNBB_L34_L46_VX_CHART (Pine v5), ported",
            v="daily volume > SMA20(daily volume) × 1.0 (the lcode v_above flag on the deduplicated 1D bars)",
            level="per lower TF (15m, 60m): 2 = H if ≥ 1 bar inside the day is L34V/L46V, 1 = L if ≥ 1 plain L34/L46 bar, "
                  "0 otherwise; a TF with no stored bars that day = 0",
            tiers="VX = V ∧ both TFs ≥ 1 (script default 'Any confirmation on both TFs') · VH = V ∧ max level 2 · "
                  "VL = V ∧ max level ≥ 1 · V · plain; tier 4..0",
            colour_filters="not applied — the daily candle colour is stored per row (colour) for the tooltip",
            status="DESCRIPTIVE ONLY, same as L-BAL; never a ranking input"),
        definitions=dict(
            mode="V (primary state) — n_* columns = every labelled bar, nv_* = volume-confirmed bars; udn/marks/text are "
                 "computed on the MODE counts, udn_all/marks_all/text_all on the other set",
            udn="effort line: n_pos = #{L34, L3} vs n_neg = #{L46}; U / D / N",
            udn_c="candle line: labelled 15m bars split by their own candle (close>open → +, close<open → −); U / D / N",
            marks=MARK_MEANING, mark_order=[sym for _, sym in MARKS],
            star_follows="UDN (effort line) — the Pine 'Dot & ★ follow' default",
            session="America/New_York calendar day of the 15m stamp; regular-session bars only (26 per full day)",
            colour="daily candle from studio_analytics (fallback: 15m first-open/last-close, colour_src='15m')",
            no_data="n_lab = 0 → udn NULL, marks '' (not XXX)"),
        census=census,
        columns=list(df.columns),
    )
    with open(tmp_spec, "w") as fh:
        json.dump(spec, fh, indent=2, ensure_ascii=False)
    os.replace(tmp_out, OUT)
    os.replace(tmp_lvx, LVX_OUT)
    os.replace(tmp_ovd, OM.OUT)
    os.replace(tmp_v7, V7.OUT)
    os.replace(tmp_spec, SPEC)
    print(f"written {OUT} rows={census['rows']:,} tickers={census['tickers']:,} as_of={as_of} ({secs:.0f}s)")
    print(f"written {LVX_OUT} rows={census['lvx']['rows']:,} tiers={census['lvx']['tier']}")
    print(f"written {OM.OUT} rows={census['ovdmap']['rows']:,} tokens={census['ovdmap']['tokens']}")
    print(f"written {V7.OUT} rows={census['vol7']['rows']:,} mr={census['vol7']['mr']} vb2={census['vol7']['vb2']} "
          f"shift={census['vol7']['shift_up']}/{census['vol7']['shift_dn']}")
    print("  marks:", census["marks"], "· no-mark:", census["no_mark_rows"], "· pairs:", census["pair"])


if __name__ == "__main__":
    main()
