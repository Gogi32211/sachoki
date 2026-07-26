"""
validate_edge_seq_oos.py — WALK-FORWARD (out-of-sample) version of validate_edge_seq.

Kills the circularity: robust seq-rules are rebuilt on TRAIN only (2021-2023), then a
+SEQ/-SEQ split is applied to EDGE entries in the OOS window (2024-2026) and path-sim'd.
If +SEQ still beats -SEQ on trades entered AFTER the rules were fixed, the confirmation
is genuine — not the seq rules memorising the same years we test on.

READ-ONLY on the 1d DB.
"""
import os, sys, json
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(__file__))
import edge_replay as ER
import seq_analytics as SA
from validate_edge_seq import _fmt

DVF = 3_000_000
TRAIN_YRS = {"2021", "2022", "2023"}
OOS_YRS = {"2024", "2025", "2026"}
TRAIN_MIN_N = 80          # lower than prod (less data in a 3-yr train slice)


def _train_rules(sdf):
    tr = sdf[sdf["yr"].isin(TRAIN_YRS)].copy()
    rules = set()
    per_depth = {}
    for depth in (2, 3, 4):
        r = SA.build_sequences(tr, depth=depth, min_n=TRAIN_MIN_N)
        robust = set(r[r["robust"]]["seq"])
        per_depth[depth] = len(robust)
        rules |= robust
    return rules, per_depth


def _has_seq_map(sdf, rules):
    df = sdf.sort_values(["ticker", "date"]).reset_index(drop=True)
    g = df.groupby("ticker", sort=False)
    df["k1"] = g["tok"].shift(1); df["k2"] = g["tok"].shift(2); df["k3"] = g["tok"].shift(3)
    tok = df["tok"].astype(str)
    d2 = df["k1"].astype(str) + " " + tok
    d3 = df["k2"].astype(str) + " " + d2
    d4 = df["k3"].astype(str) + " " + d3
    has = (df["k1"].notna() & d2.isin(rules)) | (df["k2"].notna() & d3.isin(rules)) | (df["k3"].notna() & d4.isin(rules))
    ds = df["date"].astype(str).str[:10]
    return {(t, d): bool(h) for t, d, h in zip(df["ticker"], ds, has)}


def run(dv_floor=DVF):
    print(f"pulling seq tokens (all history, dv≥${dv_floor/1e6:.0f}M)…", flush=True)
    sdf = SA._pull(tf="1d", dv_floor=dv_floor)
    print("building TRAIN-only robust rules (2021-2023)…", flush=True)
    rules, per_depth = _train_rules(sdf)
    print(f"  train robust rules: {len(rules)}  (by depth: {per_depth})")
    seqmap = _has_seq_map(sdf, rules)
    oos_rate = np.mean([v for (t, d), v in seqmap.items() if d[:4] in OOS_YRS])
    print(f"  OOS bar train-seq-match rate: {oos_rate*100:.1f}%\n")

    grp, as_of = ER._frame(60, dv_floor)
    for tk, gdf in grp.items():
        ds = gdf["date"].astype(str).str[:10]
        yr = ds.str[:4]
        oos = yr.isin(OOS_YRS).to_numpy()
        hs = np.array([seqmap.get((tk, d), False) for d in ds])
        for _, col in ER.SETUPS:
            if col in gdf:
                m = gdf[col].to_numpy(bool)
                gdf[col + "_o"] = m & oos
                gdf[col + "_ows"] = m & oos & hs
                gdf[col + "_ons"] = m & oos & (~hs)

    kw = dict(mode="trail", stop=0.10, target=0.25, trail=0.25, maxh=60)
    print(f"as_of {as_of} · OOS = entries 2024-2026 · rules frozen on 2021-2023 · trail25\n")
    print(f"{'setup':<12}{'OOS n':>7}{'ovlp':>6}   +SEQ vs -SEQ  (OOS only)")
    print("-" * 100)
    summary = []
    for name, col in ER.SETUPS:
        o = ER._stats(name, ER._pathsim(grp, col + "_o", **kw))
        ws = ER._stats(name, ER._pathsim(grp, col + "_ows", **kw))
        ns = ER._stats(name, ER._pathsim(grp, col + "_ons", **kw))
        n_o = o.get("n", 0); n_ws = ws.get("n", 0)
        ov = f"{(n_ws/n_o*100):.0f}%" if n_o else "-"
        dmean = (ws.get("mean", 0) - ns.get("mean", 0)) if (n_ws and ns.get("n", 0)) else None
        dwin = (ws.get("win", 0) - ns.get("win", 0)) if (n_ws and ns.get("n", 0)) else None
        summary.append((name, n_o, ov, dmean, dwin, ws, ns))
        print(f"{name:<12}{n_o:>7}{ov:>6}")
        print(f"    +SEQ  {_fmt(ws)}")
        print(f"    -SEQ  {_fmt(ns)}")
        if dmean is not None:
            v = "✅ HELPS" if (dmean > 0.3 and dwin >= 0) else ("❌ HURTS" if dmean < -0.3 else "⚪ neutral")
            print(f"    Δmean {dmean:+.2f}pp · Δwin {dwin:+.1f}pp   {v}")
        print()
    print("=" * 100)
    print("OOS VERDICT (rules never saw 2024-2026):")
    helps = [s[0] for s in summary if s[3] is not None and s[3] > 0.3 and s[4] >= 0]
    hurts = [s[0] for s in summary if s[3] is not None and s[3] < -0.3]
    print(f"  ✅ seq confirmation holds OOS: {helps}")
    print(f"  ❌ seq hurts OOS:             {hurts}")
    return summary, as_of


if __name__ == "__main__":
    run()
