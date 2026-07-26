"""
validate_edge_seq.py — does a Robust-Sequence match ADD edge to an EDGE setup?

For every EDGE setup, split its historical entries into:
  +SEQ  = entry bar ALSO completes a robust 2/3/4-bar sequence (seq_rules.json)
  -SEQ  = entry bar does NOT
and path-sim both with the SAME engine (edge_replay), comparing mean/win/PF/per-year
+ 2022 survival. Confirmation is REAL only if +SEQ genuinely beats -SEQ, time-robustly.
Also reports the overlap base-rate (what % of each setup's entries carry a seq).

READ-ONLY on the 1d DB. Does not touch the intraday crawl.
"""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
import edge_replay as ER
import seq_analytics as SA

DVF = 3_000_000
MONTHS = 60          # ~5.2yr → includes 2022 for survival check


def _seqmap(dv_floor=DVF):
    """{(ticker, 'YYYY-MM-DD'): True} for bars completing any robust 2/3/4-bar seq."""
    df = SA._pull(tf="1d", dv_floor=dv_floor).sort_values(["ticker", "date"]).reset_index(drop=True)
    g = df.groupby("ticker", sort=False)
    df["k1"] = g["tok"].shift(1); df["k2"] = g["tok"].shift(2); df["k3"] = g["tok"].shift(3)
    rules = set(json.load(open(os.path.join(os.path.dirname(__file__), "seq_rules.json"))).keys())
    tok = df["tok"].astype(str)
    d2 = df["k1"].astype(str) + " " + tok
    d3 = df["k2"].astype(str) + " " + d2
    d4 = df["k3"].astype(str) + " " + d3
    has = (df["k1"].notna() & d2.isin(rules)) | (df["k2"].notna() & d3.isin(rules)) | (df["k3"].notna() & d4.isin(rules))
    df["has_seq"] = has
    ds = df["date"].astype(str).str[:10]
    return {(t, d): bool(h) for t, d, h in zip(df["ticker"], ds, df["has_seq"])}, float(has.mean())


def _fmt(s):
    if not s or s.get("n", 0) == 0:
        return f"{'n=0':>28}"
    return (f"n={s['n']:<4d} mean{s['mean']:+5.2f} med{s['median']:+5.2f} "
            f"win{s['win']:4.1f} pf{str(s['pf']):>4} yr{s['pos_years']}/{s['total_years']} "
            f"'22={s['per_year'].get('2022', float('nan')):+5.2f}")


def run(months=MONTHS, dv_floor=DVF):
    print(f"pulling seq tokens (all history, dv≥${dv_floor/1e6:.0f}M)…", flush=True)
    seqmap, base_rate = _seqmap(dv_floor)
    print(f"  global bar seq-match rate: {base_rate*100:.1f}%\n")

    print(f"building edge frame ({months}mo)…", flush=True)
    grp, as_of = ER._frame(months, dv_floor)
    for tk, gdf in grp.items():
        ds = gdf["date"].astype(str).str[:10]
        gdf["has_seq"] = np.array([seqmap.get((tk, d), False) for d in ds])
        for _, col in ER.SETUPS:
            if col in gdf:
                gdf[col + "_ws"] = gdf[col] & gdf["has_seq"]
                gdf[col + "_ns"] = gdf[col] & (~gdf["has_seq"])

    kw = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
    print(f"\nas_of {as_of} · path-sim trail25 · +SEQ = entry also completes a robust seq\n")
    print(f"{'setup':<12}{'overlap':>8}   ALL / +SEQ / -SEQ  (Δ = +SEQ mean − -SEQ mean)")
    print("-" * 118)
    rows = []
    for name, col in ER.SETUPS:
        allt = ER._stats(name, ER._pathsim(grp, col, **kw))
        ws = ER._stats(name, ER._pathsim(grp, col + "_ws", **kw))
        ns = ER._stats(name, ER._pathsim(grp, col + "_ns", **kw))
        n_all = allt.get("n", 0); n_ws = ws.get("n", 0)
        ov = f"{(n_ws/n_all*100):.0f}%" if n_all else "-"
        dmean = (ws.get("mean", 0) - ns.get("mean", 0)) if (n_ws and ns.get("n", 0)) else None
        rows.append((name, n_all, ov, allt, ws, ns, dmean))
        print(f"{name:<12}{ov:>8}")
        print(f"    ALL   {_fmt(allt)}")
        print(f"    +SEQ  {_fmt(ws)}")
        print(f"    -SEQ  {_fmt(ns)}")
        if dmean is not None:
            verdict = "✅ seq HELPS" if dmean > 0.3 else ("⚪ neutral" if abs(dmean) <= 0.3 else "❌ seq HURTS")
            print(f"    Δmean(+SEQ − -SEQ) = {dmean:+.2f}pp   {verdict}")
        print()
    return rows, as_of


if __name__ == "__main__":
    run()
