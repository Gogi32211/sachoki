"""PRICE × VOLUME MULTI — the user's TradingView script "260921_PV_MULTI" ported for DISPLAY.

WHAT IT IS
  Nine ordinal price×volume shapes read off the last 3-5 daily bars, computed on the app's own 1D
  bars so the Ultra screener, the Superchart row and the CSV all read the same thing. Computed
  TWICE, once per price source, because the script itself offers the choice and the two disagree:
      PV·C   price = close     PV·O   price = ohlc4   (the Pine default)
  They are two rows, like UDN★V / UDN★all, not one row with a toggle — the whole point is to see
  where the two definitions differ on the same bar.

STATUS OF EVIDENCE — read before trusting a mark. PV_MULTI_V1, sealed 2026-09-21, k = 9:
  0 BUILD · 4 VETO_CANDIDATE · 5 NULL, and EVERY ONE of the nine was negative in MINE.
      REV  -0.909 / -0.686   negative in all 4 MINE years, best year -0.57, dsr_neg 1.000
      RE2  -0.689 / -0.320                                             dsr_neg 1.000
      UPP  -0.618 / -0.573   negative in all 4 MINE years              dsr_neg 1.000
      RUP  -0.350 / -0.316   negative in all 4 MINE years              dsr_neg 0.996
      TURN VUP UPR DIV UP4   NULL
  Every cell cleared its size-matched placebo in both windows. An ordinary eligible bar scored
  +0.009 in MINE against the same control, so MINE is interpretable as written; VERIFY carries a
  -0.179 offset of its own and is that much too generous to the negative side.
  ⭐ The four VETO candidates are RECORDED, NOT APPLIED. They cover ~13.6% of bars between them and
  turning them into suppressors is an interaction question needing its own family and its own k.
  DESCRIPTIVE ONLY. Never injected into EDGE / BUY / ULTRA / RANK scoring.

WHAT THE CENSUS SAID, and why the marks are labelled the way they are
  The nine are MUTUALLY EXCLUSIVE by construction — every pair contradicts on the price or the
  volume leg — so a bar carries at most one per price source and the builder hard-stops otherwise.
  Base rates on liquid bars: VUP 10.55% · UPP 5.97% · DIV 5.95% · RUP 3.83% · UPR 3.58% · RE2 2.19%
  · REV 1.64% · UP4 1.42% · TURN 0.74%. A mark this common is CONTEXT, not a signal.
  And "volume rising three days" is NOT "high volume": median RVOL on a firing bar is 1.33-1.43 and
  only 12-15% reach the app's B/VB buckets, while VUP sits at 0.81 with 76% BELOW the 20-day median.

DEFINITIONS (verbatim from the Pine; "3 observations" = 3 bars = 2 steps, the script's own
convention, so each shape carries its own depth and DIV/UPP/UPR/VUP are legal on a ticker's 3rd bar)
  DIV   price down 3 bars          AND volume up 3 bars
  UPP   price up 3 bars            AND volume up 3 bars
  UPR   price > price[2]           AND volume up 3 bars, minus a clean UPP
  REV   price[3]>price[2]>price[1] AND volume[3]>volume[2]>volume[1], then both turn up
  RUP   volume[3]>volume[2]>volume[1] AND price[1]>price[3], then price and volume both turn up
  VUP   volume down 3 bars         AND price > price[2]
  TURN  price[4]>price[3]>price[2], price[1]>price[2], price>price[1]; volume[4]>volume[3],
        volume[2]>volume[3], volume[1]<volume[2], volume>volume[1]
  UP4   price up 4 bars; volume[3]<volume[2], volume[1]<volume[2], volume>volume[1], volume>volume[2]
  RE2   price[3]>price[2]>price[1], price>price[1]; volume[2]>volume[3], volume[1]<volume[2],
        volume>volume[1]
  Every comparison is strict, so ties fire nothing, and a NaN from the per-ticker shift is already
  False — no global warm-up mask is applied or wanted.

OUTPUT  data/pv_multi_signals.parquet — one row per (ticker, date) on which EITHER source fired:
        pv_c / pv_o (the code, '' when that source is silent), pv_text (tooltip).
        Built nightly by update_all.sh. 1D only; does not read the intraday stores.

PARITY  backend/tests/test_pv_multi_display_parity.py asserts this file's OHLC4 output is IDENTICAL
        to pv_multi_family.signal_masks — the sealed research's own implementation — on real bars.
        The research module is NOT imported here and NOT modified: its SEAL records a digest of it.
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

OUT = os.path.join(DATA_DIR, "pv_multi_signals.parquet")
SPEC = os.path.join(DATA_DIR, "PV_MULTI_SIGNALS_V1.json")
SPEC_ID = "PV_MULTI_V1_DISPLAY"

CODES = ("DIV", "UPP", "UPR", "REV", "RUP", "VUP", "TURN", "UP4", "RE2")
MEANING = {
    "DIV":  "price fell 3 bars while volume rose 3 bars",
    "UPP":  "price rose 3 bars while volume rose 3 bars",
    "UPR":  "volume rose 3 bars and price is above its level 2 bars ago (clean UPP excluded)",
    "REV":  "price and volume both fell 3 bars, then both turned up",
    "RUP":  "volume contracted 3 bars with day-3 price above day-1, then price and volume turned up",
    "VUP":  "volume fell 3 bars while price stayed above its level 2 bars ago",
    "TURN": "price declined then recovered 2 bars, with the script's volume turn structure",
    "UP4":  "price rose 4 bars while volume expanded, rested, then re-expanded above the prior peak",
    "RE2":  "price down-down-up while volume went up-down-up",
}
# The sealed family's verdict per code, carried into the tooltip so a chart never shows a mark
# without the evidence that it did not work.
VERDICT = {"REV": "VETO_CANDIDATE −0.909/−0.686", "RE2": "VETO_CANDIDATE −0.689/−0.320",
           "UPP": "VETO_CANDIDATE −0.618/−0.573", "RUP": "VETO_CANDIDATE −0.350/−0.316",
           "TURN": "NULL", "VUP": "NULL", "UPR": "NULL", "DIV": "NULL", "UP4": "NULL"}


def masks(price: pd.Series, volume: pd.Series, ticker: pd.Series) -> dict:
    """The nine, on an arbitrary price series. Shifts are per ticker, every comparison strict."""
    sh = lambda s, k: s.groupby(ticker, sort=False).shift(k)
    p, p1, p2, p3, p4 = price, sh(price, 1), sh(price, 2), sh(price, 3), sh(price, 4)
    v, v1, v2, v3, v4 = volume, sh(volume, 1), sh(volume, 2), sh(volume, 3), sh(volume, 4)
    priceDown3 = (p2 > p1) & (p1 > p)
    priceUp3 = (p2 < p1) & (p1 < p)
    volUp3 = (v2 < v1) & (v1 < v)
    S = {}
    S["DIV"] = priceDown3 & volUp3
    S["UPP"] = priceUp3 & volUp3
    S["UPR"] = (p > p2) & volUp3 & ~S["UPP"]
    S["REV"] = (p3 > p2) & (p2 > p1) & (v3 > v2) & (v2 > v1) & (p > p1) & (v > v1)
    S["RUP"] = (v3 > v2) & (v2 > v1) & (p1 > p3) & (p > p1) & (v > v1)
    S["VUP"] = (v2 > v1) & (v1 > v) & (p > p2)
    S["TURN"] = ((p4 > p3) & (p3 > p2) & (p1 > p2) & (p > p1)
                 & (v4 > v3) & (v2 > v3) & (v1 < v2) & (v > v1))
    S["UP4"] = ((p3 < p2) & (p2 < p1) & (p1 < p)
                & (v3 < v2) & (v1 < v2) & (v > v1) & (v > v2))
    S["RE2"] = (p3 > p2) & (p2 > p1) & (p > p1) & (v2 > v3) & (v1 < v2) & (v > v1)
    return {k: m.fillna(False).to_numpy() for k, m in S.items()}


def codes_of(M: dict, n: int, label: str) -> np.ndarray:
    """One code per bar. The nine are mutually exclusive by construction; prove it rather than
    assume it, because a silent overlap would make the row lie about which shape fired."""
    stack = np.vstack([M[c] for c in CODES])
    multi = int((stack.sum(axis=0) > 1).sum())
    if multi:
        raise AssertionError(f"{label}: {multi:,} bars carry more than one of the nine — "
                             f"they are not mutually exclusive, the display cannot pick one")
    out = np.full(n, "", dtype=object)
    for c in CODES:
        out[M[c]] = c
    return out


def build(log=print) -> dict:
    import duckdb
    t0 = time.time()
    con = duckdb.connect(ANALYTICS_DB, read_only=True)
    try:
        df = con.execute("""
            WITH r AS (SELECT ticker, CAST(date AS VARCHAR) AS date, open, high, low, close, volume,
                              row_number() OVER (PARTITION BY ticker, date ORDER BY universe) rn
                       FROM bars WHERE universe <> 'index' AND close IS NOT NULL AND volume IS NOT NULL)
            SELECT * EXCLUDE (rn) FROM r WHERE rn = 1 ORDER BY ticker, date""").fetchdf()
    finally:
        con.close()
    df["date"] = df["date"].str[:10]
    df = df.sort_values(["ticker", "date"], kind="mergesort").reset_index(drop=True)
    dup = len(df) - len(df[["ticker", "date"]].drop_duplicates())
    if dup:
        raise AssertionError(f"{dup:,} duplicate (ticker,date) rows — the bars table holds one row "
                             f"per index membership and must be deduped before any shift")
    log(f"  bars {len(df):,} · {df['ticker'].nunique():,} tickers ({time.time()-t0:.0f}s)")

    vol = df["volume"].astype(float)
    src = {"c": df["close"].astype(float),
           "o": (df["open"] + df["high"] + df["low"] + df["close"]) / 4.0}
    codes, census = {}, {}
    for k, price in src.items():
        M = masks(price, vol, df["ticker"])
        codes[k] = codes_of(M, len(df), k)
        census[k] = {c: int(M[c].sum()) for c in CODES}
        log(f"  {'close' if k == 'c' else 'ohlc4':>5}: " +
            " · ".join(f"{c} {census[k][c]:,}" for c in CODES))

    hit = (codes["c"] != "") | (codes["o"] != "")
    keep = df.loc[hit, ["ticker", "date"]].copy()
    keep["pv_c"] = codes["c"][hit]
    keep["pv_o"] = codes["o"][hit]

    def text(row):
        parts = []
        for key, src_name in (("pv_c", "close"), ("pv_o", "ohlc4")):
            c = row[key]
            if c:
                parts.append(f"{c} ({src_name}) — {MEANING[c]} · sealed verdict {VERDICT[c]}")
        return " | ".join(parts)

    keep["pv_text"] = keep.apply(text, axis=1)
    agree = int(((keep["pv_c"] != "") & (keep["pv_c"] == keep["pv_o"])).sum())
    log(f"  rows kept {len(keep):,} · both sources agree on {agree:,} "
        f"({agree/max(len(keep),1)*100:.1f}%) — the disagreement is the reason there are two rows")

    tmp = OUT + ".tmp"
    keep.to_parquet(tmp, index=False)
    os.replace(tmp, OUT)
    spec = dict(spec_id=SPEC_ID, built_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                source="TradingView 260921_PV_MULTI, ported for display",
                status="DESCRIPTIVE ONLY — PV_MULTI_V1 sealed k=9: 0 BUILD / 4 VETO_CANDIDATE / "
                       "5 NULL, every cell negative in MINE. The four VETOes are recorded, NOT "
                       "applied. Never a ranking or score input.",
                price_sources={"pv_c": "close", "pv_o": "ohlc4 (the Pine default)"},
                codes={c: dict(meaning=MEANING[c], sealed_verdict=VERDICT[c]) for c in CODES},
                mutually_exclusive=True, rows=int(len(keep)),
                tickers=int(keep["ticker"].nunique()), census=census,
                both_sources_agree=agree)
    tmp_s = SPEC + ".tmp"
    json.dump(spec, open(tmp_s, "w"), indent=1)
    os.replace(tmp_s, SPEC)
    log(f"written {OUT} rows={len(keep):,} ({time.time()-t0:.0f}s)")
    return spec


if __name__ == "__main__":
    build()
