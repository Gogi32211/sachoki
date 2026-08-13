"""FREE FLIGHT v1 · stage 4 — the first study that reads a return.

Everything before this was descriptive. This file reads outcomes, so it charges k, and its
verdict is exploratory forever: the data is already exposed and no machinery changes that.

THE HYPOTHESIS IS SMALLER THAN THE ONE FIRST DESCRIBED, and the shrinkage is the finding that
earned it. The original composite wanted a liquidity vacuum AND strong momentum AND high
multiscale coherence. Stage 2.5 found momentum re-encoded by the nine lines at OOS R² 0.857;
stage 2 found coherence carrying 0.01 distinct values per thousand rows with no persistence at
any horizon. Two of the three ingredients were the incumbent or empty. Only the vacuum survived.

So the question is not "does free flight work" but the sharper one:

    does a liquidity vacuum ABOVE price add anything to the SAME position without it?

THE CONTRAST IS THE WHOLE TEST. Comparing vacuum bars to all bars would credit the vacuum for
where it lives — those bars sit above the volume mode, which is a trending state with its own
base rate. The contrast is the same positions (F, A) with structure above instead of thin air,
so only the vacuum is left standing between the two arms.

WINDOWS RESERVED BEFORE ANY NUMBER EXISTED. FREE_FLIGHT_V1_SPEC.json, hash 77ade3d1930e4731,
mining 2021-05-27 → 2025-08-12, validation 2025-08-13 → 2026-08-06, twelve cells declared. The
reserved window is not read until the mining window has already produced its verdict.
"""
from __future__ import annotations

import json
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = os.path.join(HERE, "FREE_FLIGHT_V1_SPEC.json")
REPORT = os.path.join(HERE, "FREE_FLIGHT_V1_RESULT.json")

HORIZONS = (5, 10, 20)
PRICE_BUCKETS = [(3, 8), (8, 21), (21, 89), (89, 10_000)]
MIN_CELL = 100


def _spec() -> dict:
    with open(SPEC) as f:
        return json.load(f)


def _forward(df: pd.DataFrame) -> pd.DataFrame:
    """Outcomes recomputed from OHLC. Entry at the NEXT bar's open, no exit rule.

    Our stored fwd_* columns are not used: they bake in a horizon and a convention this study
    should re-make itself. Windows never cross a ticker boundary.
    """
    tk = df["ticker"].to_numpy()
    o, c = df["open"].to_numpy(float), df["close"].to_numpy(float)
    ent = np.r_[o[1:], np.nan]
    ent = np.where(np.r_[tk[1:] == tk[:-1], False], ent, np.nan)
    for N in HORIZONS:
        cc = np.r_[c[N:], np.full(N, np.nan)]
        same = np.r_[tk[N:] == tk[:-N], np.zeros(N, bool)]
        df[f"r{N}"] = np.where(same, cc / ent - 1, np.nan) * 100
    df["ent"] = ent
    return df


def _boot_ci(x: pd.Series, dates: pd.Series, n_boot: int = 600, seed: int = 0) -> tuple:
    """Median CI, resampling TRADING DATES rather than rows.

    Rows on the same day are not independent draws — one market-wide move produces hundreds of
    them — so a row bootstrap would report an interval several times too narrow.
    """
    rng = np.random.default_rng(seed)
    d = pd.DataFrame({"x": x.to_numpy(), "d": dates.to_numpy()}).dropna()
    if len(d) < 30:
        return (np.nan, np.nan)
    groups = [g["x"].to_numpy() for _, g in d.groupby("d")]
    n = len(groups)
    meds = np.empty(n_boot)
    for i in range(n_boot):
        pick = rng.integers(0, n, n)
        meds[i] = np.median(np.concatenate([groups[j] for j in pick]))
    return tuple(np.percentile(meds, [2.5, 97.5]))


def _stats(d: pd.DataFrame, label: str) -> dict:
    out = {"label": label, "n": int(len(d)), "dates": int(d["dstr"].nunique())}
    for N in HORIZONS:
        r = d[f"r{N}"].dropna()
        if len(r) < MIN_CELL:
            out[f"r{N}"] = None
            continue
        lo, hi = _boot_ci(d[f"r{N}"], d["dstr"])
        yr = d.groupby("yr")[f"r{N}"].median()
        out[f"r{N}"] = {"n": int(len(r)), "median": round(float(r.median()), 3),
                        "win_pct": round(float((r > 0).mean() * 100), 2),
                        "ci": [round(float(lo), 3), round(float(hi), 3)],
                        "per_year": {str(int(y)): round(float(v), 2) for y, v in yr.items()},
                        "years_positive": int((yr > 0).sum()), "years": int(yr.size),
                        "worst_year": round(float(yr.min()), 2)}
    return out


def load(universe: str = "sp500") -> pd.DataFrame:
    import sources as srcs                                            # noqa: PLC0415
    df = srcs.bars("1d", universe=universe,
                   columns=("land_token", "t_sig", "z_sig", "vol_sig"), verbose=False)
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    df = _forward(df)
    df["dstr"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["yr"] = pd.to_datetime(df["date"]).dt.year
    return df


def run(universe: str = "sp500", window: str = "mining") -> dict:
    t0 = time.time()
    spec = _spec()
    w = spec["mining_window"] if window == "mining" else spec["validation_window"]
    df = load(universe)
    df = df[(df["dstr"] >= w["start"]) & (df["dstr"] <= w["end"])]
    tok = df["land_token"]

    vac = spec["cell_family"]           # position above the mode, easier escape UP
    con = spec["contrast_family"]       # same positions, structure above instead
    bull = df["t_sig"].astype(str).str.startswith("T")

    arms = {
        "vacuum_above": tok.isin(vac),
        "same_position_no_vacuum": tok.isin(con),
        "vacuum_above + bullish T": tok.isin(vac) & bull,
        "same_position_no_vacuum + bullish T": tok.isin(con) & bull,
        "ALL BARS (base rate)": tok.notna(),
    }

    out = {"study": "FREE_FLIGHT_v1", "spec_hash": spec["spec_hash"], "universe": universe,
           "window": window, "window_dates": [w["start"], w["end"]],
           "rows": int(len(df)), "arms": {}, "by_price_bucket": {}}

    for name, mask in arms.items():
        d = df[mask.fillna(False)]
        out["arms"][name] = _stats(d, name)

    # price buckets, mandatory: pooling inflates through the sub-$8 lottery
    for lo, hi in PRICE_BUCKETS:
        band = (df["close"] >= lo) & (df["close"] < hi)
        key = f"${lo}-{hi if hi < 10_000 else '+'}"
        out["by_price_bucket"][key] = {
            n: _stats(df[m.fillna(False) & band], f"{n} {key}")
            for n, m in list(arms.items())[:2]}

    out["seconds"] = round(time.time() - t0, 1)
    return out


def verdict(res: dict) -> dict:
    """The declared gates, applied in order, naming the one that decided."""
    spec = _spec()
    v = res["arms"]["vacuum_above"]["r10"]
    c = res["arms"]["same_position_no_vacuum"]["r10"]
    if not v or not c:
        return {"verdict": "UNCOMPUTABLE", "deciding_gate": "cell size"}
    delta = round(v["median"] - c["median"], 3)
    l1 = v["years_positive"] >= max(4, v["years"] - 2) and v["worst_year"] >= -2
    l2 = delta >= 1.0
    return {"delta_vs_contrast_r10": delta,
            "L1_years": f"{v['years_positive']}/{v['years']} positive, worst {v['worst_year']}",
            "L1_pass": bool(l1), "L2_pass": bool(l2),
            "verdict": ("PASSES L1+L2 — proceed to L3" if (l1 and l2)
                        else "DEAD" if not l2 else "DEAD on year stability"),
            "deciding_gate": ("L2 — the vacuum did not beat the same position without it by 1pp"
                              if not l2 else
                              "L1 — year stability" if not l1 else "none; L3 next"),
            "kill_criterion_declared": spec["kill_criterion"]}


if __name__ == "__main__":
    uni = sys.argv[1] if len(sys.argv) > 1 else "sp500"
    win = sys.argv[2] if len(sys.argv) > 2 else "mining"
    r = run(uni, win)
    r["verdict"] = verdict(r)
    path = REPORT if win == "mining" else REPORT.replace(".json", "_VALIDATION.json")
    with open(path, "w") as f:
        json.dump(r, f, indent=1, sort_keys=True)

    print("=" * 104, flush=True)
    print(f"  FREE FLIGHT v1 · {win.upper()} window {r['window_dates'][0]} → "
          f"{r['window_dates'][1]} · spec {r['spec_hash']}", flush=True)
    print("=" * 104, flush=True)
    print(f"  {r['rows']:,} bars · {r['seconds']}s\n", flush=True)
    print(f"  {'arm':<38}{'n':>9}{'dates':>7}{'med5':>8}{'med10':>8}"
          f"{'CI10':>18}{'win10':>8}{'yrs+':>7}{'worst':>8}", flush=True)
    for name, a in r["arms"].items():
        h = a.get("r10")
        if not h:
            print(f"  {name:<38}{a['n']:>9,}   too small", flush=True)
            continue
        f5 = a["r5"]["median"] if a.get("r5") else float("nan")
        print(f"  {name:<38}{a['n']:>9,}{a['dates']:>7,}{f5:>+8.2f}{h['median']:>+8.2f}"
              f"{f'[{h[chr(99)+chr(105)][0]:+.2f},{h[chr(99)+chr(105)][1]:+.2f}]':>18}"
              f"{h['win_pct']:>8.1f}{h['years_positive']}/{h['years']:<5}"
              f"{h['worst_year']:>+8.2f}", flush=True)

    print(f"\n  BY PRICE BUCKET (median r10, vacuum vs same position without it)", flush=True)
    for band, arms in r["by_price_bucket"].items():
        a = arms["vacuum_above"].get("r10")
        b = arms["same_position_no_vacuum"].get("r10")
        if a and b:
            print(f"    {band:<12}vacuum {a['median']:>+7.2f} (n={a['n']:>6,})   "
                  f"contrast {b['median']:>+7.2f} (n={b['n']:>6,})   "
                  f"Δ {a['median'] - b['median']:>+6.2f}", flush=True)
        else:
            print(f"    {band:<12}too small", flush=True)

    vd = r["verdict"]
    print(f"\n  Δ vs contrast (r10): {vd.get('delta_vs_contrast_r10')}", flush=True)
    print(f"  L1 {vd.get('L1_years')} → {'pass' if vd.get('L1_pass') else 'FAIL'}", flush=True)
    print(f"  L2 Δ ≥ +1pp → {'pass' if vd.get('L2_pass') else 'FAIL'}", flush=True)
    print(f"\n  VERDICT: {vd['verdict']}", flush=True)
    print(f"  deciding gate: {vd['deciding_gate']}", flush=True)
    print(f"\n  written to {os.path.basename(path)}", flush=True)
