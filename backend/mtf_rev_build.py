"""MTF_ZERO feature reconstruction — builds data/mtf_rev_signals.parquet.

FEATURE BUILD, NOT A STUDY. No outcome is opened. MTF_ZERO stays NOT EVALUATED.
Frozen spec: research_out/MTF_ZERO_RECONSTRUCTION_SPEC.md (c88f4e4). Audit: a2faf01.

WHAT THIS PORTS, AND WHAT IT DELIBERATELY DOES NOT.
Semantics come verbatim from studio/ultra_db_scan.py:807-819 — the intraday REV condition. The live
query's `date >= max(date) - INTERVAL 14 DAY` and its `ticker IN (...)` are OPERATIONAL scope, not
feature definition, and are NOT carried over (spec §4).

THE RSI. The stored intraday rsi_14 is never used as a feature value: the alignment audit found it
populated from each ticker's SECOND bar, i.e. built on fewer than 14 observations, while the whole
REV condition is RSI-based. This recomputes it from raw close with the producer's own formula
(studio/enricher.py:328-334 — Wilder ewm(alpha=1/14, adjust=False), rs_dn zero-guard, round(1); the
rounding matters because REV compares against 30 / 38 / 55 and against its own lag) and then gates
it on maturity imported from the warm-up contract rather than a literal.

TRI-STATE. rev_* is True/False only when the source is covered AND mature. Otherwise NULL — never
False. Coding UNKNOWN as False is the one failure mode that turns MTF_ZERO into a liquidity proxy
(coverage 64.3 %, the missing population 82x less liquid).
"""
from __future__ import annotations

import os
import sys
import time

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from v3a_warmup_contract import RSI14                                      # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "data", "mtf_rev_signals.parquet")
TFS = ("4h", "1h")
BATCH = 400                      # tickers per batch — keeps peak memory modest on the 25M-row 1H store

# rsi must be mature at bar i, at its LAG, and across the 5 strictly-preceding m5 window.
REV_MIN_BARS = RSI14 + 5 + 1     # 81 + 6 = 87 bars; ~13 sessions on 1H (7/day), ~44 on 4H (2/day)

PRICE_FLOOR = 5.0                # `close >= 5` in the production condition
M5_MAX, RSI_LO, RSI_HI = 38.0, 30.0, 55.0


def wilder_rsi14(close: pd.Series) -> pd.Series:
    """studio/enricher.py:328-334 exactly, including the zero-guard and round(1)."""
    d = close.diff()
    up = d.clip(lower=0)
    dn = (-d).clip(lower=0)
    ru = up.ewm(alpha=1.0 / 14, adjust=False, min_periods=1).mean()
    rd = dn.ewm(alpha=1.0 / 14, adjust=False, min_periods=1).mean()
    return (100.0 - 100.0 / (1.0 + ru / rd.replace(0, 1e-10))).round(1)


def _one_ticker(g: pd.DataFrame) -> pd.DataFrame:
    """g: one ticker's bars on one timeframe, sorted by timestamp.

    ⚠️ THE ONE SUBTLETY, and the reason the parity gate exists. In the production query
    `close >= 5` sits INSIDE the CTE's WHERE, so it filters bars BEFORE the window functions run:
    m5 / LAG(close) / LAG(rsi_14) reference the previous bar ABOVE $5, not the previous bar. The
    RSI itself is a stored column computed by the enricher over the FULL series. Porting the floor
    as a per-bar predicate instead (as the first build did) fires where production does not — BBBY
    on 2026-05-01, after a three-day dip below $5, is the worked example. Semantics, verbatim.
    """
    c_all = g["close"].astype(float).reset_index(drop=True)
    rsi_all = wilder_rsi14(c_all)                       # full series, as the enricher computes it
    pos_all = np.arange(len(g))
    keep = (c_all >= PRICE_FLOOR).to_numpy()

    rev_bar = np.zeros(len(g), bool)
    idx = pos_all[keep]
    if len(idx) >= 6:
        c = c_all.to_numpy()[keep]
        r = rsi_all.to_numpy()[keep]
        rs = pd.Series(r)
        m5 = rs.shift(1).rolling(5, min_periods=5).min().to_numpy()
        cp = np.r_[np.nan, c[:-1]]
        rp = np.r_[np.nan, r[:-1]]
        # every bar the m5 window touches must itself have a mature RSI: the 5th-preceding KEPT
        # bar's position in the FULL series is the binding one.
        pos5 = np.r_[np.full(5, -1), idx[:-5]]
        ok = (m5 < M5_MAX) & (r >= RSI_LO) & (r <= RSI_HI) & (c > cp) & (r > rp) & (pos5 >= RSI14)
        rev_bar[idx[np.nan_to_num(ok, nan=False).astype(bool)]] = True

    mature_bar = pos_all >= (REV_MIN_BARS - 1)
    day = g["day"].to_numpy()
    out = pd.DataFrame(dict(day=day, rev_bar=rev_bar, mature_bar=mature_bar))
    # A day is mature only if ALL its bars are (maturity is monotone in bar index, so this is just
    # "the day's first bar is mature"). That removes the one straddling day per ticker rather than
    # letting a half-immature day answer an EXISTS query.
    agg = out.groupby("day", sort=True).agg(rev=("rev_bar", "any"), mature=("mature_bar", "all"))
    return agg.reset_index()


def build_tf(tf: str, log=print) -> pd.DataFrame:
    con = duckdb.connect(os.path.join(ROOT, "data", f"studio_{tf}.duckdb"), read_only=True)
    tickers = [r[0] for r in con.execute("SELECT DISTINCT ticker FROM bars ORDER BY 1").fetchall()]
    log(f"  {tf}: {len(tickers):,} tickers")
    parts, t0 = [], time.time()
    for i in range(0, len(tickers), BATCH):
        chunk = tickers[i:i + BATCH]
        ph = ",".join("?" * len(chunk))
        df = con.execute(f"""SELECT ticker, CAST(date AS DATE) AS day, close
                             FROM bars WHERE ticker IN ({ph})
                             ORDER BY ticker, date""", chunk).fetchdf()
        for tk, g in df.groupby("ticker", sort=False):
            a = _one_ticker(g)
            a.insert(0, "ticker", tk)
            parts.append(a)
        log(f"    {min(i + BATCH, len(tickers)):>5,}/{len(tickers):,} tickers "
            f"({time.time() - t0:.0f}s)", flush=True)
    con.close()
    out = pd.concat(parts, ignore_index=True)
    out = out.rename(columns={"rev": f"rev_{tf}", "mature": f"mature_{tf}"})
    out[f"covered_{tf}"] = True                      # a row exists here iff a bar existed
    log(f"  {tf}: {len(out):,} ticker-days")
    return out


def main(log=print):
    frames = {tf: build_tf(tf, log) for tf in TFS}
    m = frames["4h"].merge(frames["1h"], on=["ticker", "day"], how="outer")
    for tf in TFS:
        m[f"covered_{tf}"] = m[f"covered_{tf}"].fillna(False).astype(bool)
        m[f"mature_{tf}"] = m[f"mature_{tf}"].fillna(False).astype(bool)
        # TRI-STATE: decidable only when covered AND mature; otherwise NULL, never False.
        ok = m[f"covered_{tf}"] & m[f"mature_{tf}"]
        m[f"rev_{tf}"] = m[f"rev_{tf}"].where(ok, other=pd.NA).astype("boolean")
    m = m.rename(columns={"day": "date"}).sort_values(["ticker", "date"]).reset_index(drop=True)
    m = m[["ticker", "date", "rev_4h", "rev_1h", "covered_4h", "covered_1h", "mature_4h", "mature_1h"]]
    m.to_parquet(OUT, index=False)
    log(f"\nwrote {OUT}  {len(m):,} rows")
    for tf in TFS:
        log(f"  {tf}: covered {int(m[f'covered_{tf}'].sum()):,} · mature {int(m[f'mature_{tf}'].sum()):,} · "
            f"decidable {int(m[f'rev_{tf}'].notna().sum()):,} · REV {int((m[f'rev_{tf}'] == True).sum()):,}")
    return m


if __name__ == "__main__":
    main()
