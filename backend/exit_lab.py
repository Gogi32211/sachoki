"""C0 — the exit, reopened. Because 90% of our trades are closed by a timer.

WHAT B2 EXPOSED

    hold == 60 bars (the max-hold cap):  90.4%
    the trail actually fires on:          9.6%
    median MFE +12.39%  ·  median realised +1.89%   → 15% of the move captured

So the "⚡ATR×12 trailing exit" is, on nine trades out of ten, not an exit rule at all — it is
a 60-bar timer. The median per-trade trail is 37% and 17% of trades sit at the 60% cap; a
stop that wide almost never gets touched. ATR×12 improved 49/49 setups by getting OUT OF THE
WAY, not by managing the position.

That reframes the earlier maxh sweep too: extending 60 → 90 helped because the cap IS the
exit.

THE OBJECTIVE IS NOT RETURN PER TRADE

The portfolio layer established that slots bind: the account holds 8-11 positions against
~400 distinct opportunities per day. A trade that makes +2% in 60 bars and one that makes
+2% in 20 bars are not equivalent — the second frees the slot three times sooner. So every
variant here is judged on RETURN PER SLOT-DAY as well as return per trade, and an exit that
earns slightly less per trade while turning over faster can be strictly better.

METHOD

Signal detection is already done — it lives in the opportunity table. Only the exit is
re-simulated, on unfiltered price paths, vectorised across a stratified sample. Every variant
sees the SAME trades, so differences are the rule and nothing else.
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

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.path.join(ROOT, "data", "studio_analytics.duckdb")
OPP = os.path.join(ROOT, "data", "opportunities.parquet")
H, SLIP = 120, er.SLIP              # simulate 120 bars so 90/120-bar caps are testable
SAMPLE = 60_000
pd.set_option("display.width", 220)

t0 = time.time()
O = pd.read_parquet(OPP, columns=["ticker", "date_in", "dup_group", "yr", "setup",
                                  "family", "risk", "sig_atr_14", "sig_close"])
print(f"opportunities: {len(O):,}", flush=True)
U = O.drop_duplicates(subset="dup_group").reset_index(drop=True)   # one trade, once
print(f"unique (ticker, entry): {len(U):,}", flush=True)
if len(U) > SAMPLE:
    U = (U.groupby("yr", group_keys=False)
           .apply(lambda d: d.sample(min(len(d), SAMPLE // U.yr.nunique()), random_state=0))
           .reset_index(drop=True))
    print(f"⚠ stratified sample by year: {len(U):,} (reported, not silent)", flush=True)

# ── price matrices: one pass, then every exit rule is pure numpy ─────────────
print("loading unfiltered paths...", flush=True)
con = duckdb.connect(DB, read_only=True)
raw = con.execute(f"""SELECT DISTINCT ticker, date, open, high, low, close FROM bars
                      WHERE universe <> 'index' AND close > 0 ORDER BY ticker, date""").fetchdf()
con.close()
PATH = {tk: (g["date"].astype(str).to_numpy(), g["open"].to_numpy(float),
             g["high"].to_numpy(float), g["low"].to_numpy(float), g["close"].to_numpy(float))
        for tk, g in raw.groupby("ticker", sort=False)}
del raw
print(f"  {len(PATH):,} tickers · {time.time()-t0:.0f}s", flush=True)

n = len(U)
OP = np.full((n, H + 1), np.nan); HI = np.full((n, H + 1), np.nan)
LO = np.full((n, H + 1), np.nan); CL = np.full((n, H + 1), np.nan)
for i, (tk, din) in enumerate(zip(U.ticker.to_numpy(), U.date_in.astype(str).to_numpy())):
    p = PATH.get(tk)
    if p is None:
        continue
    d, o, hi, lo, cl = p
    j = int(np.searchsorted(d, din[:10]))
    if j >= len(d) - 2:
        continue
    k = min(H + 1, len(d) - j)
    OP[i, :k] = o[j:j + k]; HI[i, :k] = hi[j:j + k]
    LO[i, :k] = lo[j:j + k]; CL[i, :k] = cl[j:j + k]
del PATH
ok = np.isfinite(OP[:, 0]) & (OP[:, 0] > 0) & np.isfinite(CL[:, 20])
OP, HI, LO, CL, U = OP[ok], HI[ok], LO[ok], CL[ok], U[ok].reset_index(drop=True)
n = len(U)
ENTRY = OP[:, 0] * (1 + SLIP)
ATRP = np.clip(U.sig_atr_14.to_numpy(float) / np.maximum(U.sig_close.to_numpy(float), 1e-9),
               0.005, 0.5)
print(f"  usable trades: {n:,} · matrices built · {time.time()-t0:.0f}s\n", flush=True)


def simulate(trail=None, target=None, stop=None, maxh=60) -> tuple[np.ndarray, np.ndarray]:
    """Vectorised bar-by-bar walk. Stop/trail is checked BEFORE target on the same bar —
    the pessimistic order, matching _pathsim."""
    ret = np.full(n, np.nan); held = np.full(n, maxh, dtype=float)
    peak = ENTRY.copy(); live = np.ones(n, bool)
    tgt = ENTRY * (1 + target) if target is not None else None
    for j in range(1, min(maxh, H) + 1):
        o, hi, lo = OP[:, j], HI[:, j], LO[:, j]
        val = live & np.isfinite(o)
        if not val.any():
            break
        lvl = peak * (1 - trail) if trail is not None else (
            ENTRY * (1 - stop) if stop is not None else None)
        if lvl is not None:
            gap = val & (o <= lvl)                       # gapped through overnight
            ret[gap] = o[gap] / ENTRY[gap] - 1 - SLIP; held[gap] = j; live[gap] = False
            val = live & np.isfinite(o)
            peak[val] = np.maximum(peak[val], hi[val])
            lvl = peak * (1 - trail) if trail is not None else ENTRY * (1 - stop)
            touch = val & (lo <= lvl)
            ret[touch] = lvl[touch] / ENTRY[touch] - 1 - SLIP
            held[touch] = j; live[touch] = False
            val = live & np.isfinite(o)
        else:
            peak[val] = np.maximum(peak[val], hi[val])
        if tgt is not None:
            hit = val & (hi >= tgt)
            ret[hit] = target - SLIP; held[hit] = j; live[hit] = False
    last = np.clip(np.full(n, min(maxh, H)), 0, H)
    fin = live & np.isfinite(CL[np.arange(n), last])
    ret[fin] = CL[np.arange(n), last][fin] / ENTRY[fin] - 1 - SLIP
    return ret, held


def report(label, ret, held):
    m = np.isfinite(ret)
    r, h = ret[m] * 100, held[m]
    w = r > 0
    den = -r[~w].sum()
    pf = r[w].sum() / den if den > 0 else np.inf
    per_day = np.median(r) / max(np.median(h), 1)
    yr = pd.DataFrame({"r": r, "y": U.yr.to_numpy()[m]}).groupby("y")["r"].median()
    print(f"  {label:34s} med{np.median(r):>+7.2f} mean{r.mean():>+7.2f} win{w.mean()*100:>5.1f} "
          f"pf{pf:>5.2f} hold{np.median(h):>5.0f} /day{per_day:>+6.3f} "
          f"{int((yr>0).sum())}/{len(yr)}yr worst{yr.min():>+6.2f}", flush=True)
    return dict(label=label, med=np.median(r), mean=r.mean(), hold=np.median(h),
                per_day=per_day, pf=pf, yrs=int((yr > 0).sum()), worst=yr.min())


HDR = (f"  {'exit rule':34s} {'med':>10s} {'mean':>11s} {'win':>8s} {'pf':>7s} "
       f"{'hold':>8s} {'ret/day':>10s} {'yrs':>7s} {'worst':>10s}")
rows = []
print("═══ A. TIME ONLY — no stop at all (what we effectively have today) ═══", flush=True)
print(HDR, flush=True)
for mh in (20, 40, 60, 90, 120):
    rows.append(report(f"hold {mh} bars, no stop", *simulate(maxh=mh)))

print("\n═══ B. TRAIL ONLY ═══", flush=True)
print(HDR, flush=True)
for tr in (0.15, 0.25, 0.35, 0.50):
    rows.append(report(f"trail {int(tr*100)}% · cap 120", *simulate(trail=tr, maxh=120)))

print("\n═══ C. TARGET + STOP — the untested family ═══", flush=True)
print(HDR, flush=True)
for tg in (0.08, 0.12, 0.16, 0.20, 0.30):
    for st in (0.10, 0.15):
        rows.append(report(f"target +{int(tg*100)}% · stop {int(st*100)}% · cap 60",
                           *simulate(target=tg, stop=st, maxh=60)))

print("\n═══ D. ATR-SCALED TARGET (parallel to the ⚡ATR trail law) ═══", flush=True)
print(HDR, flush=True)
for k in (3, 5, 8):
    tgt = np.clip(k * ATRP, 0.04, 0.60)
    ret = np.full(n, np.nan); held = np.full(n, 60.0)
    live = np.ones(n, bool); T = ENTRY * (1 + tgt); S = ENTRY * (1 - 0.15)
    for j in range(1, 61):
        o, hi, lo = OP[:, j], HI[:, j], LO[:, j]
        val = live & np.isfinite(o)
        g = val & (o <= S); ret[g] = o[g] / ENTRY[g] - 1 - SLIP; held[g] = j; live[g] = False
        val = live & np.isfinite(o)
        s = val & (lo <= S); ret[s] = -0.15 - SLIP; held[s] = j; live[s] = False
        val = live & np.isfinite(o)
        t = val & (hi >= T); ret[t] = tgt[t] - SLIP; held[t] = j; live[t] = False
    fin = live & np.isfinite(CL[:, 60])
    ret[fin] = CL[fin, 60] / ENTRY[fin] - 1 - SLIP
    rows.append(report(f"target {k}×ATR% · stop 15% · cap 60", ret, held))

R = pd.DataFrame(rows)
print(f"\n{'='*112}", flush=True)
base = R[R.label == "hold 60 bars, no stop"].iloc[0]
print(f"BASELINE (today's effective rule) : med {base.med:+.2f}% · hold {base.hold:.0f} · "
      f"ret/day {base.per_day:+.3f}", flush=True)
print("\nBEST BY RETURN PER SLOT-DAY — the metric that matters when slots bind:", flush=True)
print(R.nlargest(6, "per_day")[["label", "med", "hold", "per_day", "pf", "yrs", "worst"]]
      .to_string(index=False, float_format=lambda x: f"{x:.3f}"), flush=True)
print("\nBEST BY MEDIAN RETURN PER TRADE:", flush=True)
print(R.nlargest(6, "med")[["label", "med", "hold", "per_day", "pf", "yrs", "worst"]]
      .to_string(index=False, float_format=lambda x: f"{x:.3f}"), flush=True)
R.to_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "exit_lab.csv"),
         index=False)
print(f"\n  → exit_lab.csv · total {time.time()-t0:.0f}s\nDONE", flush=True)
