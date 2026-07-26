"""Does adding a P or D signal anywhere in a robust sequence's window STRENGTHEN it?
Tested the robust way: for each robust base sequence, split its occurrences by whether
sig_any_p (P) / sig_any_d (D) fired on ANY bar of the window, and compare med20/win +
TIME-ROBUSTNESS (a booster only counts if it lifts AND the variant stays 5-6/6yr, not
a 2025-artifact). Prior: D+L1 is a real reversal edge (D may help); P alone is no-edge.
Run AFTER the DB update + seq_analytics regen. READ-ONLY on bars."""
from __future__ import annotations
import json, os
import numpy as np, pandas as pd
from seq_analytics import build_token, _ULTRA_TOK


def _load_rules():
    p = os.path.join(os.path.dirname(__file__), "seq_rules.json")
    with open(p) as f:
        return json.load(f)


def run(min_n=120, top=300):
    from ai_journal.db import get_analytics_conn
    rules = _load_rules()
    a = get_analytics_conn()
    sigsel = ", ".join(f"coalesce(sig_{s},0) {s}" for s in _ULTRA_TOK)
    df = a.execute(f"""
        WITH r AS (SELECT *, row_number() OVER (PARTITION BY ticker,date ORDER BY universe) rn
                   FROM bars WHERE close>=5 AND avg_vol_20d>0 AND close*volume>=3000000)
        SELECT ticker, date, close, coalesce(z_sig,'') z_sig, coalesce(t_sig,'') t_sig,
               coalesce(l_sig,'') l_sig, coalesce(sig_any_p,0) P, coalesce(sig_any_d,0) D,
               coalesce(sig_p55,0) P55, {sigsel},
               lead(close,20) OVER (PARTITION BY ticker ORDER BY date) f20
        FROM r WHERE rn=1 ORDER BY ticker, date
    """).fetchdf()
    a.close()
    build_token(df)
    df["yr"] = df["date"].astype(str).str[:4]
    df["ret"] = df["f20"]/df["close"] - 1
    g = df.groupby("ticker")
    # window P/D presence: P or D on the current bar OR the prior (depth-1) bars
    for k in (1, 2, 3):
        df[f"k{k}"] = g["tok"].shift(k)
        df[f"P{k}"] = g["P"].shift(k); df[f"D{k}"] = g["D"].shift(k); df[f"P55_{k}"] = g["P55"].shift(k)

    def regime(s):
        ym = s.groupby("yr")["ret"].median(); pos = (ym > 0).sum(); n = len(ym)
        return pos, n, (pos/n if n else 0) >= 0.6 and s["yr"].value_counts(normalize=True).max() <= 0.45

    # take the strongest robust rules, by depth
    robust = sorted(rules.items(), key=lambda kv: -kv[1]["med20"])[:top]
    lift_P = []; lift_D = []; lift_P55 = []
    for seq, meta in robust:
        d = meta["depth"]
        toks = seq.split(" ")
        # match all bars where this sequence ends
        cols = [f"k{d-1-i}" for i in range(d-1)] + ["tok"]
        m = pd.Series(True, index=df.index)
        for c, tv in zip(cols, toks):
            m &= (df[c] == tv) if c != "tok" else (df["tok"] == tv)
        sub = df[m & df["ret"].notna()]
        if len(sub) < min_n:
            continue
        base_med = sub["ret"].median()*100
        # window flags over the depth bars (current + d-1 priors)
        def win_any(prefix):
            cur = sub[prefix] if prefix in sub else sub.get(prefix, 0)
            acc = (sub[prefix] == 1)
            for k in range(1, d):
                acc = acc | (sub[f"{prefix}{k}"] == 1)
            return acc
        for flagbase, store in [("P", lift_P), ("D", lift_D), ("P55", lift_P55)]:
            acc = (sub[flagbase] == 1)
            for k in range(1, d):
                col = f"{flagbase}{k}" if flagbase != "P55" else f"P55_{k}"
                acc = acc | (sub[col] == 1)
            withf = sub[acc]
            if len(withf) < 40:
                continue
            pos, ny, rob = regime(withf)
            store.append({"seq": seq, "n": len(withf), "base_med": round(base_med, 2),
                          "med": round(withf["ret"].median()*100, 2),
                          "lift": round(withf["ret"].median()*100 - base_med, 2),
                          "win": round((withf["ret"] > 0).mean()*100, 1),
                          "yrs": f"{pos}/{ny}", "robust": rob})

    def summ(name, lst):
        if not lst:
            print(f"\n{name}: no qualifying variants"); return
        L = pd.DataFrame(lst)
        rob = L[L.robust]
        print(f"\n===== +{name} booster (n>=40 in-window, {len(L)} seqs tested, {len(rob)} stay robust) =====")
        print(f"  median LIFT across all: {L.lift.median():+.2f}pp  ·  robust-only: {rob.lift.median() if len(rob) else 0:+.2f}pp")
        print(f"  % where +{name} LIFTS (robust): {(rob.lift>0).mean()*100 if len(rob) else 0:.0f}%")
        for _, r in rob.sort_values("lift", ascending=False).head(8).iterrows():
            print(f"    {r.seq:>30}  base{r.base_med:+.1f} → +{name}{r.med:+.1f} (lift {r.lift:+.2f}) win{r.win:.0f} {r.yrs}")
    summ("D", lift_D); summ("P", lift_P); summ("P55", lift_P55)


if __name__ == "__main__":
    run()
