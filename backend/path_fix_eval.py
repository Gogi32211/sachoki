"""A5c — the fix, measured before it is proposed. (2026-08-09)

THE PRINCIPLE THIS CORRECTS

edge_replay._pull applies `close >= 5 AND close*volume >= dv_floor` PER BAR. That filter has
two jobs mixed into one:

    1. decide which bars may generate a SIGNAL   ← legitimate, this is universe selection
    2. decide which bars the simulator may SEE   ← wrong, and it is where the damage is

Once a position is open you hold it through whatever happens. Whether the name still clears a
$3M screening threshold on day 34 has nothing to do with your P&L. But because the filter
removes those bars, _pathsim runs out of data and books the trade at the last bar that
survived the screen.

A5b measured what that costs: dropout trades recorded −5.15% median against a true −16.68%,
and individual trades recorded as +216%, +152%, +126% whose real outcomes were +28%, −13%,
−18%. The mechanism is a spike (which passes the filter) followed by a collapse (which does
not), so the simulator exits at the top of the spike and never sees the fall.

THE FIX: signals from the filtered frame, PATH from unfiltered bars.

Implemented here as a standalone re-simulation, NOT a change to _pathsim. Nothing in the
engine is touched until the size of the correction is known for every setup — the same way
the ⚡ATR×12 exit was introduced.
"""
import os
import sys

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er            # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "studio_analytics.duckdb")
MAXH, ATR_K, SLIP = 60, 12.0, er.SLIP
pd.set_option("display.width", 210)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp):,} tickers", flush=True)

# ── unfiltered price paths: the bars the screen removed still exist here ─────
print("loading UNFILTERED paths (no $5 / $3M floor)...", flush=True)
con = duckdb.connect(DB, read_only=True)
raw = con.execute(f"""
    SELECT DISTINCT ticker, date, open, high, low, close
    FROM bars
    WHERE date >= DATE '{as_of}' - INTERVAL {60*31 + 40} DAY
      AND universe <> 'index' AND close > 0
    ORDER BY ticker, date
""").fetchdf()
con.close()
print(f"  {len(raw):,} unfiltered bars on {raw.ticker.nunique():,} tickers", flush=True)
PATH = {}
for tk, g in raw.groupby("ticker", sort=False):
    PATH[tk] = (g["date"].astype(str).to_numpy(),
                g["open"].to_numpy(float), g["high"].to_numpy(float),
                g["low"].to_numpy(float), g["close"].to_numpy(float))
del raw
print(f"  indexed {len(PATH):,} tickers\n", flush=True)


def resim(tr: pd.DataFrame) -> np.ndarray:
    """Re-run each trade's exit rule on the UNFILTERED path, entry unchanged."""
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
            ts_prev = pk * (1 - trail)
            if o[j] <= ts_prev:
                ret = o[j] / entry - 1 - SLIP; break
            pk = max(pk, hi[j])
            if lo[j] <= pk * (1 - trail):
                ret = pk * (1 - trail) / entry - 1 - SLIP; break
        if ret is None:
            ret = cl[end - 1] / entry - 1 - SLIP
        out[i] = ret
    return out


print("### every setup, filtered path vs true path\n", flush=True)
print(f"  {'setup':28s} {'n':>7s} {'med_now':>8s} {'med_true':>9s} {'Δmed':>7s} "
      f"{'mean_now':>9s} {'mean_true':>10s} {'Δmean':>8s} {'maxret→':>16s}", flush=True)
rows = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, MAXH, atr_k=ATR_K)
    if len(tr) < 300:
        continue
    tru = resim(tr)
    ok = np.isfinite(tru)
    if ok.sum() < 200:
        continue
    a, b = tr["ret"].to_numpy(float)[ok] * 100, tru[ok] * 100
    rows.append(dict(setup=name, n=int(ok.sum()),
                     med=float(np.median(a)), med_t=float(np.median(b)),
                     mean=float(a.mean()), mean_t=float(b.mean()),
                     mx=float(a.max()), mx_t=float(b.max()),
                     worse=float((b < a - 0.5).mean() * 100)))
    r = rows[-1]
    print(f"  {name:28s} {r['n']:>7,} {r['med']:>+8.2f} {r['med_t']:>+9.2f} "
          f"{r['med_t']-r['med']:>+7.2f} {r['mean']:>+9.2f} {r['mean_t']:>+10.2f} "
          f"{r['mean_t']-r['mean']:>+8.2f} {r['mx']:>7.0f}→{r['mx_t']:>7.0f}", flush=True)

R = pd.DataFrame(rows)
R["d_med"] = R.med_t - R.med
R["d_mean"] = R.mean_t - R["mean"]
print(f"\n{'='*100}\nSUMMARY — {len(R)} setups\n{'='*100}", flush=True)
print(f"  median Δmed  {R.d_med.median():+.3f}pp   ·  setups worsened: "
      f"{int((R.d_med < 0).sum())} of {len(R)}", flush=True)
print(f"  median Δmean {R.d_mean.median():+.3f}pp   ·  setups worsened: "
      f"{int((R.d_mean < 0).sum())} of {len(R)}", flush=True)
print(f"  worst Δmed:  {R.d_med.min():+.2f}pp ({R.loc[R.d_med.idxmin(), 'setup']})",
      flush=True)
print(f"  worst Δmean: {R.d_mean.min():+.2f}pp ({R.loc[R.d_mean.idxmin(), 'setup']})",
      flush=True)
print(f"  max recorded return across the board: {R.mx.max():,.0f}% → "
      f"true {R.loc[R.mx.idxmax(), 'mx_t']:,.0f}%", flush=True)
print("\n  10 setups most damaged on the MEAN (the statistic DSR is built from):", flush=True)
print(R.nsmallest(10, "d_mean")[["setup", "n", "mean", "mean_t", "d_mean", "med", "med_t"]]
      .to_string(index=False, float_format=lambda x: f"{x:.2f}"), flush=True)

# does the ORDER change? that is what decides whether the book needs re-ranking
from scipy import stats as sps       # noqa: E402
for key in ("med", "mean"):
    rho = sps.spearmanr(R[key].rank(ascending=False),
                        R[f"{key}_t"].rank(ascending=False)).statistic
    moved = int((R[key].rank(ascending=False) - R[f"{key}_t"].rank(ascending=False))
                .abs().ge(5).sum())
    print(f"\n  ranking by {key:5s}: spearman {rho:.5f} · {moved} setups move ≥5 places",
          flush=True)

R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "path_fix_eval.csv"),
         index=False)
print("\n  → path_fix_eval.csv\nDONE", flush=True)
