"""SHAPE × CONTEXT — the user's TradingView script "260910_SHAPE_CTX" ported for DISPLAY.

WHAT IT IS
  A per-session reading of the seven body-nest SHAPES plus the four context layers the Pine draws:
  EFFORT (the only scored axis), LOCATION, QUALITY and the two CLUSTER axes. It is the label the
  chart prints, computed on the app's own 1D bars so the screener, the Superchart row and the CSV
  all read the same thing. Nothing here is an edge, a score input, or a filter with claimed lift.

STATUS OF EVIDENCE — read this before trusting a mark. Four sealed families, k = 21, 0 BUILD:
  MOTHER_V1            4 NULL + 1 VETO_CANDIDATE. RSI<35 was the WORST cell (-3.54 / -2.04), the
                       opposite of the hypothesis: a breakout close while still deeply oversold is
                       a dead cat. Hence the ⛔KNIFE mark on the two directional shapes.
  SHAPE_CLUSTER_V1     5 cells. NO_CLUSTER was the BEST cell (-0.13) and 🎯🔁 BOTH the WORST
                       (-0.68, 0/4 years) — MONOTONE decay with more clustering in MINE, and VERIFY
                       reversed. 🎯 diversity and 🔁 density did NOT behave as different axes
                       (-0.19 vs -0.27). The two marks are kept because they read differently on a
                       chart, NOT because either ranks.
  SHAPE_GATE_V1        5 cells, pre-registered decay ladder -> NOT_A_SUPPRESSOR. A shape cluster in
                       the 10 bars before a book edge fires does not damage it in a way that
                       replicates. GEM1 is unmeasurable here (256 fires, 3 with 🎯🔁).
  SWALLOW_DIR_V1       THE ONE KEEPER. Splitting the swallow shapes by direction found
                       LST|UP = -0.63 MINE / -0.47 VERIFY, 0/4 positive years, worst -3.41,
                       DSR_neg 0.998 over 29,911 trades — the only shape cell that does NOT flip
                       sign between windows. WRP is stable UP > DN, the opposite of the book's
                       red-beats-green law, and both halves are negative anyway.
  So: the ONLY cell with two-window evidence is LST↑, and it is a VETO (do not buy a green swallow).
  `shape_lstup_veto` carries it. Everything else is descriptive. Never injected into EDGE / BUY /
  ULTRA / RANK scoring.

DEFINITIONS (frozen; identical to the Pine defaults — inclusive containment, wrapExcl on)
  bar index map   3-bar family b1=[2] b2=[1] b3=[0]=current · 4-bar family q1=[3] q2=b1 q3=b2 q4=[0]
  bodies          T = max(open, close) · B = min(open, close); bar colour is not part of containment
  MID   b1 ⊂ b2 ⊃ b3        EXP   b1 ⊂ b2 ⊂ b3        CON   b2 ⊂ b1 · b3 ⊂ b2
  LAST  b1 ⊂ b3 · b2 ⊂ b3   WRAP  b1 ⊂ b3, minus LAST and EXP
  COIL4 b1 ⊂ b2 · (open or close of b2 inside q1's body) · close > q1_top
  MOTHER b1 ⊂ q1 · b2 ⊂ q1 · close > q1_top
  display chain   MOTHER > COIL4 > MID > EXP > CON > LAST > WRAP — one label per bar, and the ↑↓
                  arrow and the ⛔KNIFE veto attach to THAT shape, not to whatever else fired.
  direction       close > open, on EXP / LAST / WRAP only (Pine `canDir`). Containment forces the
                  meaning: green = closed AT/ABOVE the engulfed body, red = AT/BELOW it.
  EFFORT (grade)  0 = ⛔dry (vol < 0.7x avg20) · 2 = 💨absorbed (vol >= 1.5x avg20 AND range <= 1 ATR)
                  · 1 = normal. ✅swt = the 2-3x inverted-U band (project_volume_magnitude).
  LOCATION        pos20 = (close - lowest20) / (highest20 - lowest20); 📍FLOOR = pos20 <= 0.35.
                  🧱KEY = >= 2 bars in the last 40 whose low is within 0.5 ATR of the 20-bar low
                  (abs() — without it a bar far BELOW the floor counted as a touch).
                  CONTEXT ONLY: corr(pos20, rsi) = +0.908, so scoring it scores rsi twice.
  QUALITY         🏆RS = close/SPY > EMA200(close/SPY). Cross-sectional: a badge, not a per-chart
                  score. ⛔KNIFE = rsi_14 < 35 on MOTHER / COIL4.
  CLUSTER         over the 10 bars ENDING ON this bar: 🔁 density = #bars carrying any shape (>= 4);
                  🎯 diversity = #distinct families present (>= 3 of 4), families being
                  fMid · fCon · fSwal(EXP|LAST|WRAP) · fDir(COIL|MOTHER) — EXP ⊂ LAST ⊂ WRAP, so
                  those three are ONE family. Never merged into one symbol: they can disagree.

INPUTS come from the app's own analytics `bars` (not the research canonical), so the layer matches
what the Superchart draws: open/high/low/close/volume, rsi_14, avg_vol_20d, atr_14. SPY is read from
the same table for the RS ratio.

OUTPUT
  data/shapectx_signals.parquet   one row per (ticker, session) where a shape fired OR the 10-bar
                                  cluster window is non-empty. Sorted by (date, ticker).
  data/SHAPECTX_SIGNALS_V1.json   spec + census.
RUN
  backend/.venv/bin/python backend/shape_ctx_build.py     # nightly, after the 1D analytics refresh
"""
from __future__ import annotations
import os
import sys
import json
import time

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from studio.paths import DATA_DIR, ANALYTICS_DB                       # noqa: E402

OUT = os.path.join(DATA_DIR, "shapectx_signals.parquet")
SPEC = os.path.join(DATA_DIR, "SHAPECTX_SIGNALS_V1.json")

# ── Pine defaults, frozen. Changing one changes what the chart and the screener disagree about. ──
INCLUSIVE = True
WRAP_EXCL = True
LOC_LEN = 20            # range lookback for pos20
FLOOR_MAX = 0.35        # 📍 FLOOR
KEY_LOOK = 40           # 🧱 KEY lookback
KEY_TOL_ATR = 0.5       # 🧱 KEY touch tolerance
KEY_MIN = 2             # 🧱 KEY touches required
VOL_MIN = 1.5           # 💨 ABSORB volume
RANGE_MAX = 1.0         # 💨 ABSORB range x ATR
DRY_MAX = 0.7           # ⛔ DRY
SWEET = (2.0, 3.0)      # ✅ the inverted-U band
RS_LEN = 200            # 🏆 RS EMA of the ratio
BENCH = "SPY"
KNIFE_MAX = 35.0        # ⛔ KNIFE rsi
CL_LOOK = 10            # cluster window (ends on the bar)
CL_MIN_FAM = 3          # 🎯 diversity
CL_MIN_BARS = 4         # 🔁 density
SHAPES = ["mid", "exp", "con", "last", "wrap", "coil", "moth"]
CODE = {"moth": "MTH", "coil": "CL4", "mid": "MID", "exp": "EXP", "con": "CON", "last": "LST", "wrap": "WRP"}
PRIORITY = ["moth", "coil", "mid", "exp", "con", "last", "wrap"]      # Pine display chain
CAN_DIR = {"exp", "last", "wrap"}                                     # Pine canDir
IS_DIR = {"moth", "coil"}                                             # Pine isDir — the knife attaches here


def _inside(aT, aB, bT, bB):
    return (aT <= bT) & (aB >= bB) if INCLUSIVE else (aT < bT) & (aB > bB)


def _between(v, lo, hi):
    return ((v >= lo) & (v <= hi)) if INCLUSIVE else ((v > lo) & (v < hi))


def compute(df: pd.DataFrame, log=print) -> pd.DataFrame:
    """df: ticker, date, open, high, low, close, volume, rsi_14, avg_vol_20d, atr_14, bench_close.
    Must be sorted (ticker, date) with the FULL consecutive session sequence per ticker."""
    g = df.groupby("ticker", sort=False)
    T = np.maximum(df["open"], df["close"])
    B = np.minimum(df["open"], df["close"])
    df = df.assign(_T=T, _B=B)
    g = df.groupby("ticker", sort=False)
    sh = lambda c, n: g[c].shift(n)

    b1T, b1B = sh("_T", 2), sh("_B", 2)
    b2T, b2B = sh("_T", 1), sh("_B", 1)
    b3T, b3B = df["_T"], df["_B"]
    q1T, q1B = sh("_T", 3), sh("_B", 3)

    s = {}
    s["mid"] = _inside(b1T, b1B, b2T, b2B) & _inside(b3T, b3B, b2T, b2B)
    s["exp"] = _inside(b1T, b1B, b2T, b2B) & _inside(b2T, b2B, b3T, b3B)
    s["con"] = _inside(b2T, b2B, b1T, b1B) & _inside(b3T, b3B, b2T, b2B)
    s["last"] = _inside(b1T, b1B, b3T, b3B) & _inside(b2T, b2B, b3T, b3B)
    wrap_raw = _inside(b1T, b1B, b3T, b3B)
    s["wrap"] = (wrap_raw & ~s["last"] & ~s["exp"]) if WRAP_EXCL else wrap_raw
    o1, c1 = g["open"].shift(1), g["close"].shift(1)
    q3_in_q1 = _between(o1, q1B, q1T) | _between(c1, q1B, q1T)
    s["coil"] = _inside(b1T, b1B, b2T, b2B) & q3_in_q1 & (df["close"] > q1T)
    s["moth"] = _inside(b1T, b1B, q1T, q1B) & _inside(b2T, b2B, q1T, q1B) & (df["close"] > q1T)
    for k in SHAPES:
        df[f"s_{k}"] = s[k].fillna(False).to_numpy()
    enough = df.groupby("ticker", sort=False).cumcount().to_numpy() >= 3
    df["any_shape"] = df[[f"s_{k}" for k in SHAPES]].any(axis=1).to_numpy() & enough
    for k in SHAPES:                                   # a shape below the 4-bar warmup cannot fire
        df[f"s_{k}"] = df[f"s_{k}"].to_numpy() & enough

    # ── display priority chain: exactly one label per bar ──
    code = np.full(len(df), "", dtype=object)
    disp = np.full(len(df), "", dtype=object)
    taken = np.zeros(len(df), bool)
    for k in PRIORITY:
        hit = df[f"s_{k}"].to_numpy() & ~taken
        code[hit] = CODE[k]
        disp[hit] = k
        taken |= hit
    df["shape_code"] = code
    df["_disp"] = disp
    df["dir_up"] = (df["close"] > df["open"]).to_numpy()
    df["dir_dn"] = (df["close"] < df["open"]).to_numpy()
    can = np.isin(disp, list(CAN_DIR))
    isdir = np.isin(disp, list(IS_DIR))
    df["can_dir"] = can
    arrow = np.where(can & df["dir_up"].to_numpy(), "↑",
                     np.where(can & df["dir_dn"].to_numpy(), "↓", ""))
    df["shape_arrow"] = arrow
    df["shape_label"] = np.where(df["shape_code"].to_numpy() != "",
                                 df["shape_code"].to_numpy() + arrow, "")

    # ── EFFORT — the only scored axis ──
    vavg = df["avg_vol_20d"].to_numpy(float)
    vr = np.where(vavg > 0, df["volume"].to_numpy(float) / np.where(vavg > 0, vavg, 1), np.nan)
    atr = df["atr_14"].to_numpy(float)
    rng = (df["high"] - df["low"]).to_numpy(float)
    absorb = (~np.isnan(vr)) & (vr >= VOL_MIN) & (atr > 0) & (rng <= RANGE_MAX * atr)
    dry = (~np.isnan(vr)) & (vr < DRY_MAX)
    df["vr"] = vr
    df["absorb"] = absorb
    df["dry"] = dry
    df["sweet"] = (~np.isnan(vr)) & (vr >= SWEET[0]) & (vr <= SWEET[1])
    df["grade"] = np.where(dry, 0, np.where(absorb, 2, 1)).astype(np.int8)

    # ── LOCATION — context only ──
    g2 = df.groupby("ticker", sort=False)
    roll = lambda c, n, f: getattr(g2[c].rolling(n, min_periods=n), f)().reset_index(level=0, drop=True)
    hh = roll("high", LOC_LEN, "max").to_numpy()
    ll = roll("low", LOC_LEN, "min").to_numpy()
    span = hh - ll
    df["pos20"] = np.where(span > 0, (df["close"].to_numpy() - ll) / np.where(span > 0, span, 1), 0.5)
    df["floor"] = df["pos20"].to_numpy() <= FLOOR_MAX
    # 🧱 KEY — count bars in the last KEY_LOOK whose LOW is within tol of THIS bar's 20-bar low.
    # abs() is the Pine FIX B: without it a bar far BELOW the floor scored as a touch.
    tol = KEY_TOL_ATR * atr
    lows = df["low"].to_numpy(float)
    tk = df["ticker"].to_numpy()
    touches = np.zeros(len(df), np.int16)
    start = 0
    for i in range(1, len(df) + 1):                   # per-ticker contiguous blocks
        if i == len(df) or tk[i] != tk[start]:
            lo_b, ll_b, tol_b = lows[start:i], ll[start:i], tol[start:i]
            n = len(lo_b)
            cnt = np.zeros(n, np.int16)
            for back in range(KEY_LOOK):              # 40 vectorised passes, not a per-bar loop
                if back >= n:
                    break
                shifted = np.empty(n); shifted[:] = np.nan
                shifted[back:] = lo_b[:n - back]
                ok = np.abs(shifted - ll_b) <= tol_b
                cnt += np.nan_to_num(ok, nan=False).astype(np.int16)
            touches[start:i] = cnt
            start = i
    df["touches"] = touches
    df["key"] = touches >= KEY_MIN

    # ── QUALITY ──
    ratio = np.where(df["bench_close"].to_numpy(float) > 0,
                     df["close"].to_numpy(float) / np.where(df["bench_close"].to_numpy(float) > 0,
                                                            df["bench_close"].to_numpy(float), 1), np.nan)
    df["_ratio"] = ratio
    ema = (df.groupby("ticker", sort=False)["_ratio"]
             .transform(lambda x: x.ewm(span=RS_LEN, adjust=False, min_periods=RS_LEN).mean()))
    df["rs"] = (df["_ratio"] > ema).fillna(False).to_numpy()
    rsi = pd.to_numeric(df["rsi_14"], errors="coerce").to_numpy(float)
    df["rsi"] = rsi
    df["knife"] = rsi < KNIFE_MAX
    band = np.full(len(df), "", dtype=object)
    band[rsi < 35] = "<35"
    band[(rsi >= 35) & (rsi < 50)] = "35-50"
    band[(rsi >= 50) & (rsi < 60)] = "50-60"
    band[rsi >= 60] = ">=60"
    df["band"] = band
    df["veto"] = isdir & df["knife"].to_numpy()          # Pine ⛔KNF, on MOTHER / COIL4 only

    # ── CLUSTER — two axes over the CL_LOOK window ENDING on this bar ──
    df["_f_mid"] = df["s_mid"]
    df["_f_con"] = df["s_con"]
    df["_f_swal"] = df[["s_exp", "s_last", "s_wrap"]].any(axis=1)
    df["_f_dir"] = df[["s_coil", "s_moth"]].any(axis=1)
    g3 = df.groupby("ticker", sort=False)
    rsum = lambda c: g3[c].rolling(CL_LOOK, min_periods=1).sum().reset_index(level=0, drop=True)
    df["cl_bars"] = rsum("any_shape").fillna(0).astype(np.int16)
    fam = None
    for f in ("_f_mid", "_f_con", "_f_swal", "_f_dir"):
        p = (rsum(f) > 0).astype(np.int16)
        fam = p if fam is None else fam + p
    df["cl_fam"] = fam.fillna(0).astype(np.int16)
    df["by_fam"] = df["cl_fam"].to_numpy() >= CL_MIN_FAM
    df["by_den"] = df["cl_bars"].to_numpy() >= CL_MIN_BARS
    # numpy fixed-width string arrays cannot be concatenated with `+` — build these as object
    # arrays, which is also what the parquet column ends up as.
    def _pick(cond, yes, no=""):
        return np.where(np.asarray(cond, bool), yes, no).astype(object)

    df["mark"] = _pick(df["by_fam"], "\U0001F3AF") + _pick(df["by_den"], "\U0001F501")

    # ── the one cell with two-window evidence: LST|UP is a VETO (SWALLOW_DIR_V1) ──
    df["lstup_veto"] = (disp == "last") & df["dir_up"].to_numpy()

    df["legs"] = (_pick(df["floor"], "\U0001F4CD")
                  + _pick(df["key"], "\U0001F9F1")
                  + _pick(df["absorb"], "\U0001F4A8", "").astype(object)
                  + _pick(df["dry"] & ~df["absorb"], "⛔")
                  + _pick(df["rs"], "\U0001F3C6")
                  + df["mark"].to_numpy().astype(object))
    return df


def build(log=print) -> dict:
    import duckdb
    t0 = time.time()
    con = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        bench = con.execute(
            "SELECT CAST(date AS VARCHAR) AS date, close AS bench_close FROM bars "
            "WHERE ticker = ? ORDER BY date", [BENCH]).fetchdf()
        df = con.execute("""
            WITH r AS (SELECT ticker, CAST(date AS VARCHAR) AS date, open, high, low, close, volume,
                              rsi_14, avg_vol_20d, atr_14,
                              row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
                       FROM bars WHERE universe <> 'index')
            SELECT * EXCLUDE (rn) FROM r WHERE rn = 1 ORDER BY ticker, date""").fetchdf()
    finally:
        con.close()
    df["date"] = df["date"].str[:10]
    bench["date"] = bench["date"].str[:10]
    log(f"  bars {len(df):,} · {df['ticker'].nunique():,} tickers · {BENCH} rows {len(bench):,} ({time.time()-t0:.0f}s)")
    df = df.merge(bench, on="date", how="left")
    df = df.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    df = compute(df, log)
    log(f"  computed ({time.time()-t0:.0f}s) · any_shape {int(df['any_shape'].sum()):,}")

    keep = df[df["any_shape"].to_numpy() | (df["cl_bars"].to_numpy() > 0)].copy()
    out = pd.DataFrame({
        "ticker": keep["ticker"], "date": keep["date"],
        "shape": keep["any_shape"].astype(bool),
        "code": keep["shape_code"].astype(str), "arrow": keep["shape_arrow"].astype(str),
        "label": keep["shape_label"].astype(str),
        "grade": keep["grade"].astype("int8"),
        "vr": keep["vr"].astype("float32"), "absorb": keep["absorb"].astype(bool),
        "dry": keep["dry"].astype(bool), "sweet": keep["sweet"].astype(bool),
        "pos20": keep["pos20"].astype("float32"), "floor": keep["floor"].astype(bool),
        "touches": keep["touches"].astype("int16"), "key": keep["key"].astype(bool),
        "rs": keep["rs"].astype(bool), "rsi": keep["rsi"].astype("float32"),
        "band": keep["band"].astype(str), "knife": keep["knife"].astype(bool),
        "veto": keep["veto"].astype(bool), "lstup_veto": keep["lstup_veto"].astype(bool),
        "dir_up": keep["dir_up"].astype(bool), "dir_dn": keep["dir_dn"].astype(bool),
        "can_dir": keep["can_dir"].astype(bool),
        "cl_fam": keep["cl_fam"].astype("int16"), "cl_bars": keep["cl_bars"].astype("int16"),
        "by_fam": keep["by_fam"].astype(bool), "by_den": keep["by_den"].astype(bool),
        "mark": keep["mark"].astype(str), "legs": keep["legs"].astype(str),
    })
    for k in SHAPES:
        out[f"s_{k}"] = keep[f"s_{k}"].astype(bool).to_numpy()
    out = out.sort_values(["date", "ticker"], kind="mergesort").reset_index(drop=True)

    census = dict(
        rows=int(len(out)), tickers=int(out["ticker"].nunique()), dates=int(out["date"].nunique()),
        first=str(out["date"].min()), last=str(out["date"].max()),
        any_shape=int(out["shape"].sum()),
        code_census={k: int(v) for k, v in out.loc[out["shape"], "code"].value_counts().items()},
        shape_raw={k: int(out[f"s_{k}"].sum()) for k in SHAPES},
        grade_census={int(k): int(v) for k, v in out.loc[out["shape"], "grade"].value_counts().items()},
        by_fam=int(out["by_fam"].sum()), by_den=int(out["by_den"].sum()),
        both=int((out["by_fam"] & out["by_den"]).sum()),
        lstup_veto=int(out["lstup_veto"].sum()), knife_veto=int(out["veto"].sum()),
        rs_intact=int(out["rs"].sum()))
    spec = dict(spec_id="SHAPECTX_SIGNALS_V1", built_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                source="TradingView 260910_SHAPE_CTX, ported for display",
                status="DESCRIPTIVE ONLY — 4 sealed families (MOTHER_V1, SHAPE_CLUSTER_V1, "
                       "SHAPE_GATE_V1, SWALLOW_DIR_V1), k = 21, 0 BUILD. The single cell with "
                       "two-window evidence is LST|UP = a VETO (-0.63 MINE / -0.47 VERIFY, 0/4 yrs, "
                       "DSR_neg 0.998) -> `lstup_veto`. Never a ranking or score input.",
                params=dict(inclusive=INCLUSIVE, wrap_excl=WRAP_EXCL, loc_len=LOC_LEN,
                            floor_max=FLOOR_MAX, key_look=KEY_LOOK, key_tol_atr=KEY_TOL_ATR,
                            key_min=KEY_MIN, vol_min=VOL_MIN, range_max=RANGE_MAX, dry_max=DRY_MAX,
                            sweet=list(SWEET), rs_len=RS_LEN, bench=BENCH, knife_max=KNIFE_MAX,
                            cl_look=CL_LOOK, cl_min_fam=CL_MIN_FAM, cl_min_bars=CL_MIN_BARS),
                census=census, parquet=OUT)
    tmp = OUT + ".tmp"
    out.to_parquet(tmp, index=False)
    os.replace(tmp, OUT)
    tmp_s = SPEC + ".tmp"
    json.dump(spec, open(tmp_s, "w"), indent=1, default=str)
    os.replace(tmp_s, SPEC)
    log(f"  wrote {OUT} · {len(out):,} rows ({time.time()-t0:.0f}s)")
    log(f"  codes {census['code_census']}")
    log(f"  🎯 {census['by_fam']:,} · 🔁 {census['by_den']:,} · 🎯🔁 {census['both']:,} · "
        f"LST↑ veto {census['lstup_veto']:,} · ⛔knife {census['knife_veto']:,}")
    return spec


if __name__ == "__main__":
    build()
