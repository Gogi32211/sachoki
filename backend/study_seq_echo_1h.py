"""1H ECHO test for the 7 new signals (user-approved plan, 2026-08-05).

Fractal criterion (edge_echo.py, 2026-07-05): a REAL edge is approximately fractal —
the same STATE should pay at least in SIGN on a neighboring timeframe. A signal that
only works on 1d is not wrong (GEM1 and Z-Absorb are proven 1d-native), but a 1H echo
is strong evidence the state is structural rather than a daily-bar coincidence.

Masks are rebuilt 1H-NATIVELY (same logic, hourly bars). RS gate joins the DAILY
rs_intact by ticker+calendar-date — RS is a daily-scale state; there is no SPY in the
1H DB and remapping it hourly would invent a new definition.

Horizon: maxh = 420 hourly bars ≈ 60 trading days (edge_echo BPD convention, 1h=7).
Memory-light: one process, one TF, column-pruned pull, floor $400k/bar (~$3M/day).
"""
import gc
import numpy as np
import pandas as pd
import duckdb
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
from studio.paths import db_path

MAXH = 420

# ── 1. daily rs_intact map from the canonical frame (already warm in nobody's cache
#      here — but the daily pull is cheap relative to the 1H one) ────────────────
grp_d, as_of = er._frame(60, 3_000_000)
rs_rows = []
for tk, g in grp_d.items():
    rs_rows.append(pd.DataFrame({
        "ticker": tk,
        "day": pd.to_datetime(g["date"]).astype(str).str[:10],
        "rs_intact": g["rs_intact"].to_numpy(dtype=bool),
    }))
RS = pd.concat(rs_rows, ignore_index=True)
del grp_d, rs_rows
gc.collect()
print(f"daily rs map: {len(RS):,} rows, as_of {as_of}", flush=True)

# ── 2. 1H pull (pruned columns) ──────────────────────────────────────────────────
c = duckdb.connect(db_path("studio_1h.duckdb"), read_only=True)
df = c.execute("""
    SELECT ticker, CAST(date AS VARCHAR) dt, open, high, low, close, rsi_14,
           coalesce(t_sig,'') t, coalesce(z_sig,'') z, coalesce(l_sig,'') l,
           coalesce(full_suffix,'') fsfx
    FROM bars
    WHERE close >= 5 AND close * volume >= 400000
    ORDER BY ticker, date
""").fetchdf()
c.close()
print(f"1H frame: {len(df):,} rows, {df['ticker'].nunique()} tickers", flush=True)

df["day"] = df["dt"].str[:10]
df["date"] = df["dt"]                      # _pathsim uses g["date"] for date_in/out
df = df.merge(RS, on=["ticker", "day"], how="left")
df["rs_intact"] = df["rs_intact"].fillna(False)
del RS
gc.collect()

# ── 3. build the 7 masks 1H-natively ─────────────────────────────────────────────
_CROWN = {("Z1G","Z2G","T5","Z3"),("Z1G","Z2G","T3","Z3"),("Z1G","Z2G","T5","Z4"),
          ("T5","Z3","T4","Z9"),("T12","Z1G","T5","Z3"),("Z1G","T5","T11","Z3"),
          ("T12","Z1G","T3","Z3"),("Z1G","T5","T12","Z4"),("T3","Z3","T4","Z9")}
_SEQ20 = {("Z1","Z2G","T1","T6"),("T4","Z3","T1G","T6"),("T1G","T6","Z3","T3"),
          ("Z6","T3","Z1G","T3"),("Z1G","T5","Z3","T3"),("Z9","T3","Z5","T9"),
          ("T5","Z3","Z6","T9"),("T6","Z1","T5","Z5"),("Z1G","T5","T11","Z5"),
          ("T4","Z4","T5","Z5"),("T12","T2G","Z4","T1"),("T3","T6","Z9","T1"),
          ("Z6","T3","Z1","T1G"),("T5","T2","Z1","T1G"),("T11","Z5","T1G","T2"),
          ("T1","T2G","T12","T2"),("T12","Z3","T1G","T2G"),("Z3","T9","T11","T2G"),
          ("T6","T11","T2G","T2G"),("T10","Z3","T1G","T2G")}
_Z9HL  = {("Z3","T4","Z9","T3"),("T4","Z9","T3","Z5")}
_Z1GT36= {("T6","Z1G","Z2G","T3"),("T6","Z1G","T5","T6"),("Z1G","T1","T2G","T6")}
_Z1GT4 = {("T6","Z1G","Z2G"),("Z1G","T1G","Z5"),("Z1G","Z6","Z2G")}

g = df.groupby("ticker", sort=False)
code = np.where(df["t"] != "", df["t"], df["z"])
codeS = pd.Series(code, index=df.index)
b1 = g["t"].shift(1).fillna(""); z1s = g["z"].shift(1).fillna("")
b2 = g["t"].shift(2).fillna(""); z2s = g["z"].shift(2).fillna("")
b3 = g["t"].shift(3).fillna(""); z3s = g["z"].shift(3).fillna("")
_b1 = np.where(b1 != "", b1, z1s); _b2 = np.where(b2 != "", b2, z2s)
_b3 = np.where(b3 != "", b3, z3s); _b0 = code
# guard: shifts cross ticker boundaries only within groupby -> b1..b3 already per-ticker
_quad = pd.Series(list(zip(_b3, _b2, _b1, _b0)), index=df.index)
_tri  = pd.Series(list(zip(_b3, _b2, _b1)), index=df.index)

_rng = (df["high"] - df["low"])
_cpos = ((df["close"] - df["low"]) / _rng.where(_rng > 0)).fillna(1.0)
_t_p1 = g["t"].shift(1).fillna(""); _t_p2 = g["t"].shift(2).fillna("")
_l = df["l"].fillna("")
_pre34 = (g["l"].shift(1).eq("L34") | g["l"].shift(2).eq("L34") | g["l"].shift(3).eq("L34"))

df["E_z1gcrown"] = (df["close"].between(8, 377) & _quad.isin(_CROWN).to_numpy() & (_cpos < 0.5))
df["E_seq20"]    = df["close"].between(21, 377) & _quad.isin(_SEQ20).to_numpy()
df["E_z9hl"]     = df["close"].between(21, 377) & _quad.isin(_Z9HL).to_numpy()
df["E_z1gt36"]   = df["close"].between(21, 377) & _quad.isin(_Z1GT36).to_numpy()
df["E_z1gt4"]    = ((df["t"] == "T4") & df["close"].between(21, 377) & _tri.isin(_Z1GT4).to_numpy())
df["E_t2gsand_rs"] = ((df["t"] == "T2G") & (_t_p1 == "T10") & (_t_p2 == "T2G")
                      & (df["rsi_14"] >= 70) & df["rs_intact"] & df["close"].between(21, 89))
df["E_t1gnb_rs"] = ((df["t"] == "T1G") & (df["fsfx"] == "NB") & df["rs_intact"]
                    & df["close"].between(21, 89))
df["E_t1gnb_l34pre"] = df["E_t1gnb_rs"] & _pre34.to_numpy()
df["E_base10"] = (np.arange(len(df)) % 10 == 0)

COLS = ["E_z1gcrown","E_seq20","E_z9hl","E_z1gt4","E_z1gt36",
        "E_t2gsand_rs","E_t1gnb_rs","E_t1gnb_l34pre","E_base10"]
print("fires:", {c: int(df[c].sum()) for c in COLS}, flush=True)

grp = {tk: gg.reset_index(drop=True) for tk, gg in df.groupby("ticker", sort=False)}
del df, g, code, codeS, _quad, _tri
gc.collect()

# ── 4. path-sim + report ─────────────────────────────────────────────────────────
# 1d reference signs/medians (from the validation records) for the ECHO verdict
REF_1D = {"E_z1gcrown": 14.53, "E_seq20": 2.92, "E_z9hl": 12.89, "E_z1gt4": 7.74,
          "E_z1gt36": 15.36, "E_t2gsand_rs": 1.70, "E_t1gnb_rs": 2.42,
          "E_t1gnb_l34pre": 6.40}
NAMES = {"E_z1gcrown": "👑 CROWN", "E_seq20": "🧺 SEQ-20", "E_z9hl": "🧲 Z9-HL",
         "E_z1gt4": "🌉 Z1G→T4", "E_z1gt36": "🌉v2", "E_t2gsand_rs": "🥪 SAND",
         "E_t1gnb_rs": "🪨 T1G-NB", "E_t1gnb_l34pre": "🪨+ L34pre",
         "E_base10": "BASELINE (10th bar)"}

HDR = (f"{'signal':22s} {'n':>6s} {'med':>7s} {'win':>5s} {'pf':>5s} "
       f"{'2021':>6s}{'2022':>6s}{'2023':>6s}{'2024':>6s}{'2025':>6s}{'2026':>6s}"
       f"  {'pos':>4s} {'worst':>6s}  {'1d med':>7s}  echo")
print("\n===== 1H ECHO — path-sim trail 10/25/25, maxh 420 h-bars =====\n" + HDR, flush=True)

base_med = None
for col in ["E_base10"] + [c for c in COLS if c != "E_base10"]:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, MAXH)
    if len(tr) == 0:
        print(f"{NAMES[col]:22s}    n=0", flush=True); continue
    tr["yr"] = pd.to_datetime(tr["date_in"]).dt.year
    yr = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    med = tr["ret"].median() * 100
    if col == "E_base10":
        base_med = med
    ys = "".join(f"{yr.get(y, float('nan')):>6.1f}" for y in range(2021, 2027))
    ref = REF_1D.get(col)
    echo = ""
    if ref is not None and base_med is not None:
        echo = "✅ ECHO" if (med > base_med and med > 0) else ("〰 sign-only" if med > base_med else "❌ none")
    print(f"{NAMES[col]:22s} {len(tr):>6d} {med:>+7.2f} {w.mean()*100:>5.1f} {pf:>5.2f}"
          f" {ys}  {int((yr>0).sum())}/{len(yr)} {yr.min():>+6.1f}  "
          f"{'' if ref is None else f'{ref:>+7.2f}'}  {echo}", flush=True)
    del tr
    gc.collect()

print("\nDONE", flush=True)
