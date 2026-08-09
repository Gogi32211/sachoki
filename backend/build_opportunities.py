"""B2 — the opportunity table: every (ticker × setup × signal date) with its full label vector.

WHY THIS IS THE HIGHEST-LEVERAGE ENGINEERING STEP IN THE PLAN

Every study in this repo re-runs _pathsim. In one session that meant rebuilding the frame six
times and re-simulating 119 setups over and over — ten to fifteen minutes before the first
number appears, every time. The simulation is deterministic, so almost all of it is repeated
work.

Compute it once. After this table exists, ranking research is a pandas query.

WHAT EACH ROW IS

The unit is the OPPORTUNITY, not the ticker: one stock can fire several setups on the same
bar (CAR fired four QZ-Capit variants on 2026-02-23). Ranking has to see them separately —
but ALLOCATION must not fund the same trade four times, so every row carries `dup_group`
= (ticker, date_in), and the allocator collapses on it.

LABELS, NOT A LABEL

_pathsim already computes ret, mae, mfe, hold and risk on every trade and this repo has been
using `ret` and discarding the rest. A ranker wants the vector: how far it ran (mfe), how much
pain it took first (mae), how long it tied up a slot (hold). Those are free — they are already
in the return value.

BOTH RETURNS ARE STORED

`ret`      what the book was built on: the path as the filtered frame sees it.
`ret_true` the same exit rule re-run on UNFILTERED bars, so a name that fell through the
           $5 / $3M screen mid-trade does not silently exit at the top of a spike (A5c:
           medians move −0.06pp, but individual trades were misrecorded by 150-190pp and
           the extreme tail collapses, 1528% → 599%).

Storing both means downstream work chooses honestly instead of re-deriving the correction.
"""
from __future__ import annotations

import os
import sys
import time

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er            # noqa: E402
from ledger import family_of        # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "studio_analytics.duckdb")
OUT = os.path.join(ROOT, "data", "opportunities.parquet")
MAXH, ATR_K, SLIP = 60, 12.0, er.SLIP

# ⚠ these are the FRAME's column names, not the database's. _pull renames on the way in
# (vol_bucket→vb, l_sig→l, full_suffix→fsfx, bar_gap_class→gap); asking for the DB names
# silently drops the column and the loss only surfaces downstream.
SIG_COLS = ("t", "z", "l", "fsfx", "vb", "gap", "csfx", "l5", "close", "rsi_14",
            "atr_14", "conso", "rs_intact", "lead_in_lag", "adx_regime", "adx",
            "volume", "hurst", "macro_vix_up", "m15_zdom", "h1_quiet", "h1_dr",
            "iv_vspike", "spring", "anyd", "l1")

t0 = time.time()
grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp):,} tickers · {time.time()-t0:.0f}s", flush=True)

# ── signal-bar lookup, built once (entry is the bar AFTER the signal) ─────────
print("building signal-bar lookup...", flush=True)
frames = []
for tk, g in grp.items():
    cols = [c for c in SIG_COLS if c in g]
    d = g[cols].copy()
    d.columns = [f"sig_{c}" for c in cols]
    d["ticker"] = tk
    d["date_in"] = g["date"].astype(str).shift(-1).to_numpy()
    d["sig_date"] = g["date"].astype(str).to_numpy()
    frames.append(d.dropna(subset=["date_in"]))
LOOK = pd.concat(frames, ignore_index=True)
del frames
print(f"  {len(LOOK):,} signal-bar rows · {time.time()-t0:.0f}s", flush=True)

# ── unfiltered paths for the honest label ────────────────────────────────────
print("loading UNFILTERED paths for ret_true...", flush=True)
con = duckdb.connect(DB, read_only=True)
raw = con.execute(f"""
    SELECT DISTINCT ticker, date, open, high, low, close FROM bars
    WHERE date >= DATE '{as_of}' - INTERVAL {60*31 + 40} DAY
      AND universe <> 'index' AND close > 0
    ORDER BY ticker, date
""").fetchdf()
con.close()
PATH = {}
for tk, g in raw.groupby("ticker", sort=False):
    PATH[tk] = (g["date"].astype(str).to_numpy(), g["open"].to_numpy(float),
                g["high"].to_numpy(float), g["low"].to_numpy(float),
                g["close"].to_numpy(float))
del raw
print(f"  {len(PATH):,} tickers indexed · {time.time()-t0:.0f}s", flush=True)


def true_path(tr: pd.DataFrame) -> np.ndarray:
    """Same exit rule, same per-trade trail, UNFILTERED bars."""
    out = np.full(len(tr), np.nan)
    for i, (tk, din, risk) in enumerate(zip(tr["ticker"].to_numpy(),
                                            tr["date_in"].astype(str).to_numpy(),
                                            tr["risk"].to_numpy(float))):
        p = PATH.get(tk)
        if p is None:
            continue
        d, o, hi, lo, cl = p
        j0 = int(np.searchsorted(d, din[:10]))
        if j0 >= len(d) - 2:
            continue
        entry = o[j0] * (1 + SLIP)
        if not np.isfinite(entry) or entry <= 0:
            continue
        trail = float(risk) if np.isfinite(risk) and risk > 0 else 0.25
        end = min(j0 + 1 + MAXH, len(d))
        pk, ret = entry, None
        for j in range(j0 + 1, end):
            if o[j] <= pk * (1 - trail):
                ret = o[j] / entry - 1 - SLIP; break
            pk = max(pk, hi[j])
            if lo[j] <= pk * (1 - trail):
                ret = pk * (1 - trail) / entry - 1 - SLIP; break
        if ret is None:
            ret = cl[end - 1] / entry - 1 - SLIP
        out[i] = ret
    return out


# ── the table ────────────────────────────────────────────────────────────────
print(f"\nsimulating {len(er.SETUPS)} setups...", flush=True)
parts = []
for k, (name, col) in enumerate(er.SETUPS, 1):
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, MAXH, atr_k=ATR_K)
    if len(tr) == 0:
        continue
    tr["setup"] = name
    tr["family"] = family_of(name)
    tr["ret_true"] = true_path(tr)
    parts.append(tr)
    if k % 20 == 0:
        print(f"  {k}/{len(er.SETUPS)} · rows so far {sum(len(p) for p in parts):,} · "
              f"{time.time()-t0:.0f}s", flush=True)

O = pd.concat(parts, ignore_index=True)
del parts
print(f"\nraw opportunity rows: {len(O):,} · {time.time()-t0:.0f}s", flush=True)

O = O.merge(LOOK, on=["ticker", "date_in"], how="left", validate="m:1")
miss = O["sig_close"].isna().mean()
if miss > 0.02:
    raise RuntimeError(f"signal-bar join failed on {miss*100:.1f}% — alignment is wrong")
print(f"  signal-bar join: {(1-miss)*100:.2f}% matched", flush=True)

# the allocator must not fund one trade several times because several variants named it
O["dup_group"] = O["ticker"] + "|" + O["date_in"].astype(str).str[:10]
O["yr"] = pd.to_datetime(O["date_in"]).dt.year
O["ret_gap"] = O["ret_true"] - O["ret"]

for c in O.select_dtypes(include=["float64"]).columns:
    O[c] = O[c].astype("float32")

O.to_parquet(OUT, index=False, compression="zstd")
sz = os.path.getsize(OUT) / 1e6
print(f"\n{'='*88}", flush=True)
print(f"WROTE {OUT}", flush=True)
print(f"  {len(O):,} opportunities · {O.setup.nunique()} setups · "
      f"{O.family.nunique()} families · {O.ticker.nunique():,} tickers", flush=True)
print(f"  {O.dup_group.nunique():,} distinct (ticker, entry-date) — "
      f"{len(O)/max(O.dup_group.nunique(),1):.2f} setups fire per opportunity on average",
      flush=True)
print(f"  {sz:,.0f} MB · built in {time.time()-t0:.0f}s", flush=True)
print(f"\n  labels: ret · ret_true · mae · mfe · hold · risk   "
      f"(ret_gap median {O.ret_gap.median()*100:+.3f}pp)", flush=True)
print(f"  state:  {len([c for c in O.columns if c.startswith('sig_')])} sig_* columns",
      flush=True)
print(f"\n  fires per trading day: {len(O)/max(O.date_in.nunique(),1):.0f} "
      f"(the number the allocator has to choose from)", flush=True)
print("=" * 88, flush=True)
print("\nDONE", flush=True)
