"""
validate_engine_fix.py — backtest-expert audit fixes, verified end-to-end:
  A) FILL A/B: old fill model (gap-through-stop fills AT stop = optimistic) vs the new
     gap-realistic engine (gap fills at OPEN) — every setup, board window (36mo).
  B) SLIP STRESS: 1× (15bps) vs 2× (30bps each way) on the new engine.
  C) PORTFOLIO SIM: GEM1 62mo — 5 concurrent slots, 20%/slot, real equity curve → max DD.
  D) GEM1 PLATEAU + ROLLING: body-ratio × RSI grid (curve-fit check) + rolling 24mo windows.
READ-ONLY.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
import edge_replay as ER
from edge_replay import _pathsim, _stats, _frame, SETUPS, SLIP

KW = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)


def _pathsim_old(grp, col, mode, stop, target, trail, maxh):
    """PRE-FIX fill model (verbatim old logic) — for the A/B comparison only."""
    trades = []
    for tk, gdf in grp.items():
        if col not in gdf:
            continue
        o = gdf["open"].to_numpy(float); hi = gdf["high"].to_numpy(float)
        lo = gdf["low"].to_numpy(float); cl = gdf["close"].to_numpy(float)
        ent = gdf[col].to_numpy(bool); n = len(gdf); last = -99
        dts = gdf["date"].astype(str).str[:4].to_numpy()
        for i in range(n - 1):
            if not ent[i] or i + 1 >= n or i - last < 5:
                continue
            ep = o[i + 1]
            if ep <= 0:
                continue
            last = i
            entry = ep * (1 + SLIP); ret = None; end = min(i + 1 + maxh, n); pk = entry
            for j in range(i + 1, end):
                if mode == "trail":
                    pk = max(pk, hi[j]); ts = pk * (1 - trail)
                    if lo[j] <= ts:
                        ret = ts / entry - 1 - SLIP; break
                else:
                    if lo[j] <= entry * (1 - stop):
                        ret = -stop - SLIP; break
                    if hi[j] >= entry * (1 + target):
                        ret = target - SLIP; break
            if ret is None:
                ret = cl[end - 1] / entry - 1 - SLIP
            trades.append({"ticker": tk, "ret": ret, "yr": dts[i]})
    return pd.DataFrame(trades)


def _fmt(s):
    if not s or s.get("n", 0) == 0:
        return "n=0"
    return f"n={s['n']:>4} m{s['mean']:+6.2f} md{s['median']:+6.2f} pf{str(s['pf']):>5} y{s['pos_years']}/{s['total_years']}"


def section_ab_and_stress():
    grp, as_of = _frame(36, 3_000_000)
    print(f"═══ A) FILL A/B + B) SLIP STRESS · 36mo · as_of {as_of} ═══")
    print(f"{'setup':14s} | {'OLD fill (optimistic)':>44s} | {'NEW gap-realistic':>44s} | {'NEW @2× slip':>44s}")
    for name, col in SETUPS:
        so = _stats(name, _pathsim_old(grp, col, **KW))
        sn = _stats(name, _pathsim(grp, col, **KW))
        s2 = _stats(name, _pathsim(grp, col, **KW, slip=2 * SLIP))
        print(f"{name:14s} | {_fmt(so):>44s} | {_fmt(sn):>44s} | {_fmt(s2):>44s}")
    print()


def section_portfolio():
    grp, as_of = _frame(62, 3_000_000)
    tr = _pathsim(grp, "E_t1capbounce", **KW).sort_values("date_in").reset_index(drop=True)
    print(f"═══ C) PORTFOLIO SIM · GEM1 62mo · {len(tr)} signals · 5 slots × 20% ═══")
    SLOTS, W = 5, 0.20
    eq, peak, maxdd = 1.0, 1.0, 0.0
    open_pos, taken, skipped = [], 0, 0     # open_pos = list of (date_out, ret)
    curve = []
    for _, t in tr.iterrows():
        din, dout = str(t["date_in"]), str(t["date_out"])
        # close positions whose exit is before this entry
        for (do, r) in sorted([p for p in open_pos if p[0] <= din]):
            eq *= (1 + W * r)
            peak = max(peak, eq); maxdd = max(maxdd, 1 - eq / peak)
            curve.append((do, eq))
        open_pos = [p for p in open_pos if p[0] > din]
        if len(open_pos) < SLOTS:
            open_pos.append((dout, float(t["ret"]))); taken += 1
        else:
            skipped += 1
    for (do, r) in sorted(open_pos):
        eq *= (1 + W * r)
        peak = max(peak, eq); maxdd = max(maxdd, 1 - eq / peak)
        curve.append((do, eq))
    yrs = 62 / 12
    cagr = eq ** (1 / yrs) - 1
    print(f"  taken {taken} / skipped {skipped} (capacity) · final equity ×{eq:.2f} "
          f"· CAGR {cagr*100:+.1f}% · MAX PORTFOLIO DD {maxdd*100:.1f}%")
    print(f"  as_of {as_of}\n")


def section_plateau():
    grp, as_of = _frame(62, 3_000_000)
    # rebuild GEM1 variants from raw cols on each ticker frame
    print("═══ D) GEM1 PLATEAU · body-ratio × RSI grid (fixed: T1·prevZ·volB·clean) ═══")
    frames = []
    for tk, g in grp.items():
        d = g[["ticker", "date", "open", "high", "low", "close", "rsi_14", "t", "z", "vb"]].copy()
        d["clean"] = g["clean"] if "clean" in g else True
        body = (d["close"] - d["open"]).abs()
        d["ratio"] = body / body.shift(1).replace(0, np.nan)
        d["prevZ"] = d["z"].shift(1).fillna("") != ""
        frames.append(d)
    allf = frames  # keep per-ticker
    def run(mask_fn, label):
        g2 = {}
        for d in allf:
            m = mask_fn(d)
            dd = d.copy(); dd["_m"] = m.fillna(False).values
            g2[dd["ticker"].iloc[0]] = dd.reset_index(drop=True)
        s = _stats(label, _pathsim(g2, "_m", **KW))
        print(f"  {label:24s} {_fmt(s)}")
        return s
    base = lambda d: (d["t"] == "T1") & d["clean"] & d["prevZ"] & (d["vb"] == "B")
    for br in (0.35, 0.5, 0.65, 0.8):
        run(lambda d, br=br: base(d) & (d["ratio"] < br) & d["rsi_14"].between(30, 50), f"body<{br} · RSI30-50")
    for lo_, hi_ in ((25, 45), (30, 50), (35, 55)):
        run(lambda d, lo_=lo_, hi_=hi_: base(d) & (d["ratio"] < 0.5) & d["rsi_14"].between(lo_, hi_), f"body<0.5 · RSI{lo_}-{hi_}")
    # rolling 24mo windows (step ~12mo) on canonical GEM1
    print("\n  · rolling 24mo windows (canonical GEM1):")
    g3 = {}
    for d in allf:
        dd = d.copy()
        dd["_m"] = (base(d) & (d["ratio"] < 0.5) & d["rsi_14"].between(30, 50)).fillna(False).values
        g3[dd["ticker"].iloc[0]] = dd.reset_index(drop=True)
    tr = _pathsim(g3, "_m", **KW)
    tr["ym"] = tr["date_in"].astype(str).str[:7]
    months = sorted(tr["ym"].unique())
    for start in range(0, max(1, len(months) - 23), 12):
        win = months[start:start + 24]
        sub = tr[tr["ym"].isin(win)]
        if len(sub) < 10:
            continue
        print(f"    {win[0]}→{win[-1]}: n={len(sub):>3} mean{sub['ret'].mean()*100:+6.2f} med{sub['ret'].median()*100:+6.2f}")
    print(f"  as_of {as_of}")


if __name__ == "__main__":
    section_ab_and_stress()
    section_portfolio()
    section_plateau()
