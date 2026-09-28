"""VOL ECHO — the user's TradingView script "260925_VOL_ECHO" ported for DISPLAY.

WHAT IT IS
  A volume spike (origin) is remembered; a later bar with similar volume in the same price zone is an
  ECHO. After an echo the shared zone is armed and the script marks what happens next:
      VE     echo                          Q      low-volume bar that closes inside the zone
      ▲/▼    release: the first 3 above-average-volume bars after Q (green ▲ / red ▼)
      BO▲ BD▼   first candle that opens AND closes above / below the zone (green / red)
      BOV▲ BDV▼ the same with volume > SMA20
      R      low-volume bar within 20 bars after BD▼ / BDV▼
      SPK    an origin spike that was later echoed (drawn retroactively, exactly as the Pine does —
             it is HINDSIGHT on its own bar and must never be read as a same-day signal)
  Computed on the app's own 1D bars so the Ultra screener, the Superchart row and the CSV read the
  same thing. Parameters are the Pine defaults and are frozen here (spike 1.5 · echo 1.5 · ±40 %
  volume · 50 % range overlap · quiet ≤ 0.8 · zone 60 bars · breakout 120 bars · release 3 bars
  within 10 · R ≤ 0.8 within 20).

STATUS OF EVIDENCE — read before trusting a mark (research_out/, all 2026-09-25..27):
  VOL_ECHO_TZL_MAP_V1   descriptive: VE fires on 13.5 % of bars, same TZ/L profile as any spike.
  VOL_ECHO_LONG_V1      NULL — 0/274 cells (signal × TZ·L), 20d excess, MINE 2021-23.
  VOL_ECHO_LONG_2326    NULL — 0/286 cells, MINE 2023-24.
  BDZ5L12_V1            NULL — the one recurring cell (BD▼ × Z5·L12) did not replicate 2025-26.
  ABSP_BD_GATE_V1       NULL — a BD▼ anchor does not improve Absorption→P (path-sim worse).
  VOL_ECHO_TRADE_V1     NULL — ▲ / BO▲ / BOV▲ with SL5/TP15, raw / EMA200 / RS, k = 9.
  QR_REL_V1             ⛔ VETO CONFIRMED — Q∧R bar then the first ▲ Release: −1.82 pp MINE and
                        −1.40 pp VERIFY vs a random buy, negative in all 6 years (book trail exit).
  QR_RSI_V1             NULL — Q∧R + RSI > 50 −0.88 pp (below the veto bar).
  DESCRIPTIVE ONLY. Never injected into EDGE / BUY / ULTRA / RANK scoring. The one confirmed veto is
  carried as its own flag (`ve_qr_rel_veto`) and is RECORDED, NOT APPLIED.

OUTPUT  data/vol_echo_signals.parquet — one row per (ticker, date) on which any mark fired.
        Built nightly by update_all.sh. 1D only; does not read the intraday stores.
PARITY  backend/tests/test_vol_echo_port.py pins the port on synthetic bars and on the
        TradingView-checked RGTI Oct-2024 sequence.
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
from studio.paths import DATA_DIR, ANALYTICS_DB                          # noqa: E402

OUT = os.path.join(DATA_DIR, "vol_echo_signals.parquet")
SPEC = os.path.join(DATA_DIR, "VOL_ECHO_SIGNALS_V1.json")
SPEC_ID = "VOL_ECHO_V1_DISPLAY"

P = dict(vol_len=20, spike=1.5, conf=1.5, tol_pct=40.0, min_overlap=50.0, min_gap=5, max_gap=250,
         quiet=0.8, quiet_bars=60, bo_bars=120, bov_mult=1.0, rel_len=20, rel_mult=1.0,
         rel_count=3, min_q=1, rel_wait=10, r_mult=0.8, r_bars=20, max_store=300)

BOOL = ("spike", "spk", "echo", "q", "r", "rel_up", "rel_dn", "bo", "bd", "bov", "bdv",
        "zone_armed", "qr_rel_veto")
INT = ("echo_gap", "echo_nmatch", "echo_hits", "qn", "rn", "rel_n", "rel_q", "bo_age", "zone_pos")
FLOAT = ("echo_vr", "bov_x", "zone_top", "zone_bot")
STR = ("echo_col",)
EVENTS = ("spike", "spk", "echo", "q", "r", "rel_up", "rel_dn", "bo", "bd", "bov", "bdv")


def baselines(vol: pd.Series, ticker: pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """base  = ta.percentile_nearest_rank(vol, 20, 50)[1]  — the 10th smallest of the previous 20
               (nearest-rank ceil(0.5·20) = 10 ≡ pandas 'lower' interpolation), current bar excluded
       sma   = ta.sma(vol, 20)                              — includes the current bar (TV's volume MA)"""
    g = vol.groupby(ticker, sort=False)
    base = g.transform(lambda s: s.rolling(P["vol_len"], min_periods=P["vol_len"])
                       .quantile(0.5, interpolation="lower").shift(1))
    sma = g.transform(lambda s: s.rolling(P["rel_len"], min_periods=P["rel_len"]).mean())
    return base.to_numpy(float), sma.to_numpy(float)


def compute(o, h, l, c, v, base, sma) -> dict:
    """One ticker, bars oldest→newest. A line-for-line port of the Pine state machine at its
    defaults; the order of the blocks (echo → quiet → breakout → R → release → spike push) matters
    and is the Pine's order."""
    n = len(c)
    out = {k: np.zeros(n, bool) for k in BOOL}
    out.update({k: np.zeros(n, np.int32) for k in INT})
    out.update({k: np.full(n, np.nan) for k in FLOAT})
    out["echo_col"] = np.full(n, "", dtype=object)

    sp = []                                   # [bar, hi, lo, vol, colour, hits]
    wTop = wBot = None; wStart = None; wCount = 0; wActive = False
    bo = bd = bov = bdv = False
    relArmed = False; lastSig = None; relQ = 0; relN = 0
    rArmed = False; rStart = None; rCount = 0
    tolLo, tolHi = 1.0 / (1.0 + P["tol_pct"] / 100.0), 1.0 + P["tol_pct"] / 100.0
    prev_q = prev_r = False
    for j in range(n):
        oj, hj, lj, cj, vj = o[j], h[j], l[j], c[j], v[j]
        bj, sj = base[j], sma[j]
        vr = vj / bj if (bj == bj and bj > 0) else np.nan
        isSpike = vr == vr and vr >= P["spike"]
        cand = vr == vr and vr >= P["conf"]
        col = "G" if cj > oj else "R" if cj < oj else "D"
        tick = 0.01 if cj >= 1 else 0.0001

        while sp and j - sp[0][0] > P["max_gap"]:
            sp.pop(0)
        m = -1; nm = 0
        if cand and sp:
            for i in range(len(sp) - 1, -1, -1):
                ob, oh, ol, ov_, _, _ = sp[i]
                if j - ob < P["min_gap"]:
                    continue
                r_ = vj / ov_ if ov_ > 0 else 0.0
                if not (tolLo <= r_ <= tolHi):
                    continue
                ovl = min(hj, oh) - max(lj, ol)
                sm = max(min(hj - lj, oh - ol), tick)
                if ovl > 0 and ovl / sm * 100.0 >= P["min_overlap"]:
                    nm += 1
                    if m == -1:
                        m = i
        echo = m >= 0
        if echo:
            ob, oh, ol, ov_, ocol, hits = sp[m]
            hits += 1; sp[m][5] = hits
            out["echo"][j] = True
            out["echo_gap"][j] = j - ob
            out["echo_nmatch"][j] = nm
            out["echo_hits"][j] = hits
            out["echo_vr"][j] = vj / ov_ if ov_ > 0 else np.nan
            out["echo_col"][j] = f"{ocol}→{col}"
            if hits == 1:
                out["spk"][ob] = True
            zt, zb = min(hj, oh), max(lj, ol)
            wTop, wBot = (zt, zb) if zt > zb else (hj, lj)
            wStart = j; wCount = 0; wActive = True
            bo = bd = bov = bdv = True

        quiet = False
        if wActive and not echo:
            if j - wStart > P["quiet_bars"]:
                wActive = False
            elif wBot <= cj <= wTop and vr == vr and vr <= P["quiet"]:
                quiet = True; wCount += 1
                out["q"][j] = True; out["qn"][j] = wCount
                relArmed = wCount >= P["min_q"]; lastSig = j; relQ = wCount; relN = 0

        bdNow = False
        if not echo and (bo or bd or bov or bdv):
            if j - wStart > P["bo_bars"]:
                bo = bd = bov = bdv = False
            else:
                up = oj > wTop and cj > wTop and cj > oj
                dn = oj < wBot and cj < wBot and cj < oj
                vok = sj == sj and sj > 0 and vj > P["bov_mult"] * sj
                if bo and up:
                    out["bo"][j] = True; bo = False
                if bd and dn:
                    out["bd"][j] = True; bd = False; bdNow = True
                if bov and up and vok:
                    out["bov"][j] = True; bov = False
                if bdv and dn and vok:
                    out["bdv"][j] = True; bdv = False; bdNow = True
                if out["bo"][j] or out["bd"][j] or out["bov"][j] or out["bdv"][j]:
                    out["bo_age"][j] = j - wStart
                    out["bov_x"][j] = vj / sj if (sj == sj and sj > 0) else np.nan

        if rArmed and not bdNow:
            if j - rStart > P["r_bars"]:
                rArmed = False
            elif vr == vr and vr <= P["r_mult"]:
                rCount += 1
                out["r"][j] = True; out["rn"][j] = rCount
        if bdNow:
            rArmed = True; rStart = j; rCount = 0

        if relArmed and not quiet:
            if j - lastSig > P["rel_wait"]:
                relArmed = False
            elif sj == sj and sj > 0 and vj > P["rel_mult"] * sj and cj != oj:
                relN += 1; lastSig = j
                out["rel_up" if cj > oj else "rel_dn"][j] = True
                out["rel_n"][j] = relN; out["rel_q"][j] = relQ
                if relN >= P["rel_count"]:
                    relArmed = False

        # QR_REL_V1 veto shape: yesterday Q∧R, today the first ▲ Release of the series
        if out["rel_up"][j] and prev_q and prev_r:
            out["qr_rel_veto"][j] = True
        prev_q, prev_r = bool(out["q"][j]), bool(out["r"][j])

        out["zone_armed"][j] = wActive
        if wTop is not None and j - wStart <= P["bo_bars"]:
            out["zone_top"][j] = wTop; out["zone_bot"][j] = wBot
            out["zone_pos"][j] = 1 if cj > wTop else -1 if cj < wBot else 0

        if isSpike:
            out["spike"][j] = True
            sp.append([j, hj, lj, vj, col, 0])
            if len(sp) > P["max_store"]:
                sp.pop(0)
    return out


def text_of(row) -> str:
    parts = []
    if row["echo"]:
        parts.append(f"VE·{row['echo_gap']}b{' +' + str(row['echo_nmatch'] - 1) if row['echo_nmatch'] > 1 else ''} "
                     f"{row['echo_col']} ×{row['echo_vr']:.2f}{' #' + str(row['echo_hits']) if row['echo_hits'] > 1 else ''}")
    if row["q"]:
        parts.append(f"Q{row['qn']}")
    if row["r"]:
        parts.append(f"R{row['rn']}")
    if row["rel_up"]:
        parts.append(f"▲{row['rel_n']}·Q{row['rel_q']}")
    if row["rel_dn"]:
        parts.append(f"▼{row['rel_n']}·Q{row['rel_q']}")
    if row["bov"]:
        parts.append(f"BOV▲ {row['bo_age']}b ×{row['bov_x']:.1f}")
    elif row["bo"]:
        parts.append(f"BO▲ {row['bo_age']}b")
    if row["bdv"]:
        parts.append(f"BDV▼ {row['bo_age']}b ×{row['bov_x']:.1f}")
    elif row["bd"]:
        parts.append(f"BD▼ {row['bo_age']}b")
    if row["spk"]:
        parts.append("SPK (echoed later — hindsight)")
    if row["qr_rel_veto"]:
        parts.append("⛔ Q∧R→▲1 (QR_REL_V1 veto)")
    return " · ".join(parts)


def build(log=print) -> dict:
    import duckdb
    t0 = time.time()
    con = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        df = con.execute("""
            WITH r AS (SELECT ticker, CAST(date AS VARCHAR) AS date, open, high, low, close, volume,
                              row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
                       FROM bars WHERE universe <> 'index' AND close IS NOT NULL)
            SELECT * EXCLUDE (rn) FROM r WHERE rn = 1 ORDER BY ticker, date""").fetchdf()
    finally:
        con.close()
    df["date"] = df["date"].str[:10]
    df = df.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    dup = len(df) - len(df[["ticker", "date"]].drop_duplicates())
    if dup:
        raise AssertionError(f"{dup:,} duplicate (ticker,date) rows — dedupe before any shift")
    df["volume"] = df["volume"].fillna(0).astype(float)
    log(f"  bars {len(df):,} · {df['ticker'].nunique():,} tickers ({time.time()-t0:.0f}s)")

    base, sma = baselines(df["volume"], df["ticker"])
    cols = {k: [] for k in BOOL + INT + FLOAT + STR}
    O, H, L, C, V = (df[x].to_numpy(float) for x in ("open", "high", "low", "close", "volume"))
    for _, idx in df.groupby("ticker", sort=False).indices.items():
        r = compute(O[idx], H[idx], L[idx], C[idx], V[idx], base[idx], sma[idx])
        for k in cols:
            cols[k].append(r[k])
    for k in cols:
        df[k] = np.concatenate(cols[k]) if cols[k] else []
    census = {k: int(df[k].sum()) for k in EVENTS + ("qr_rel_veto",)}
    log("  " + " · ".join(f"{k} {v:,}" for k, v in census.items()))

    hit = np.zeros(len(df), bool)
    for k in EVENTS:
        hit |= df[k].to_numpy()
    keep = df.loc[hit, ["ticker", "date", *BOOL, *INT, *FLOAT, *STR]].copy()
    keep["ve_text"] = keep.apply(text_of, axis=1)
    keep = keep.rename(columns={k: f"ve_{k}" for k in BOOL + INT + FLOAT + STR})

    tmp = OUT + ".tmp"
    keep.to_parquet(tmp, index=False)
    os.replace(tmp, OUT)
    spec = dict(spec_id=SPEC_ID, built_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                source="TradingView 260925_VOL_ECHO, ported for display at its default parameters",
                status="DESCRIPTIVE ONLY — VOL_ECHO long studies NULL (V1 0/274, 2326 0/286, TRADE_V1 0/9); "
                       "QR_REL_V1 veto CONFIRMED (Q∧R then first ▲: −1.40 pp VERIFY, 6/6 years) and "
                       "RECORDED, NOT APPLIED. SPK is hindsight. Never a ranking or score input.",
                params=P, rows=int(len(keep)), tickers=int(keep["ticker"].nunique()), census=census)
    tmp_s = SPEC + ".tmp"
    json.dump(spec, open(tmp_s, "w"), indent=1)
    os.replace(tmp_s, SPEC)
    log(f"written {OUT} rows={len(keep):,} ({time.time()-t0:.0f}s)")
    return spec


if __name__ == "__main__":
    build()
