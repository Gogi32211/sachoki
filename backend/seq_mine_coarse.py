"""
seq_mine_coarse.py — COARSE-token Robust-Seq mining (2026-07-20, user request).

Identical frozen-OOS pipeline to seq_revalidate_v2.py (mine TRAIN 2021-23 only,
verify TEST 2024-26 with med20 + trail25 stop-first path-sim, DSR deflated vs all
mined candidates) — but the token drops the L-suffix: `Z2G  Z11  -T3` instead of
`Z2GL46 Z11L12 -T3L12`. Each rule fires ~6× more often → bigger n, denser 🧬 layer
for the confluence read. Output: seq_rules_v2_coarse.json (same schema).

READ-ONLY on bars.
"""
from __future__ import annotations
import json, os, sys, time
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
import seq_analytics as SA
from overfit_stats import sharpe, dsr
from seq_revalidate_v2 import pathsim_all, TRAIN_YRS, TEST_YRS

OUT = os.path.join(os.path.dirname(__file__), "seq_rules_v2_coarse.json")


def main():
    t0 = time.time()
    print("pulling frame (tokens + fwd)…", flush=True)
    df = SA._pull(tf="1d")
    # COARSE token: the prim part only (Z* > -T* > *ULT > '-'), NO L-suffix
    z = df["z_sig"].fillna(""); t = df["t_sig"].fillna("")
    ult = pd.Series("", index=df.index)
    for u in reversed(SA._ULTRA_TOK):
        if u in df:
            ult = ult.mask(df[u] == 1, u.upper().replace("_", ""))
    df["tok"] = z.where(z != "", ("-" + t).where(t != "", ("*" + ult).where(ult != "", "-")))
    df["key"] = df.ticker + "|" + df.date.astype(str).str[:10]
    print(f"frame {len(df):,} ({time.time()-t0:.0f}s) — path-sim all bars…", flush=True)
    psmap = pathsim_all()
    df["ps"] = df["key"].map(psmap)
    print(f"path-sim mapped ({time.time()-t0:.0f}s)", flush=True)

    d = df.sort_values(["ticker", "date"]).copy()
    g = d.groupby("ticker")["tok"]
    laggs = {k: g.shift(k).astype(str) for k in (1, 2, 3)}
    seqs = {2: laggs[1] + " " + d.tok,
            3: laggs[2] + " " + laggs[1] + " " + d.tok,
            4: laggs[3] + " " + laggs[2] + " " + laggs[1] + " " + d.tok}

    rules = {}
    N_mined = 0
    for depth, s in seqs.items():
        d["seq"] = s
        sub = d[d.r20.notna() & ~d.seq.str.contains("nan", na=False)]
        tr = sub[sub.yr.isin(TRAIN_YRS)]
        te = sub[sub.yr.isin(TEST_YRS)]
        grp = tr.groupby("seq")
        cand = []
        for sq, sg in grp:
            if len(sg) < 50:
                continue
            ym = sg.groupby("yr")["r20"].median()
            if sg.r20.median() <= 0 or (ym > 0).sum() < 2:
                continue
            cand.append(sq)
        N_mined += len(cand)
        print(f"depth {depth}: mined {len(cand)} candidates ({time.time()-t0:.0f}s)", flush=True)
        te_g = {k: v for k, v in te.groupby("seq") if k in set(cand)}
        tr_g = {k: v for k, v in grp if k in set(cand)}
        for sq in cand:
            sg_tr = tr_g[sq]; sg_te = te_g.get(sq)
            rec = {"depth": depth,
                   "n_train": int(len(sg_tr)),
                   "med20_train": round(sg_tr.r20.median() * 100, 2),
                   "train_yrs": {y: round(v * 100, 2) for y, v in sg_tr.groupby("yr")["r20"].median().items()}}
            if sg_te is None or len(sg_te) < 20:
                rec.update({"tier": "no-oos-data", "n_test": int(0 if sg_te is None else len(sg_te))})
            else:
                ps = sg_te.ps.dropna()
                ym_te = sg_te.groupby("yr")["r20"].median()
                rec.update({
                    "n_test": int(len(sg_te)),
                    "med20_test": round(sg_te.r20.median() * 100, 2),
                    "ps_med_test": round(ps.median() * 100, 2),
                    "ps_mean_test": round(ps.mean() * 100, 2),
                    "ps_win_test": round((ps > 0).mean() * 100, 1),
                    "test_yrs": {y: round(v * 100, 2) for y, v in ym_te.items()},
                    "sr_test": round(sharpe(ps.to_numpy()), 4),
                })
                ok = (ps.median() > 0) and int((ym_te > 0).sum()) >= 2
                rec["tier"] = "OOS_VERIFIED" if ok else "in-sample-only"
            rules[sq] = rec

    fam = [r["sr_test"] for r in rules.values() if "sr_test" in r]
    ver = [sq for sq, r in rules.items() if r.get("tier") == "OOS_VERIFIED"]
    print(f"\nmined {N_mined} → OOS-verified {len(ver)} ({time.time()-t0:.0f}s) — DSR pass…", flush=True)
    for depth, s in seqs.items():
        d["seq"] = s
        te = d[d.yr.isin(TEST_YRS) & d.seq.isin([v for v in ver if rules[v]["depth"] == depth])]
        for sq, sg in te.groupby("seq"):
            ps = sg.ps.dropna().to_numpy()
            if len(ps) >= 20:
                rules[sq]["dsr"] = dsr(ps, fam, n_trials=max(N_mined, len(fam)))["dsr"]

    with open(OUT, "w") as f:
        json.dump({"as_of": time.strftime("%Y-%m-%d"), "mined": N_mined,
                   "train": "2021-2023", "test": "2024-2026", "min_n_train": 50,
                   "token": "coarse (no L-suffix)", "rules": rules}, f)

    R = pd.DataFrame([{**{"seq": k}, **v} for k, v in rules.items()])
    ver_df = R[R.tier == "OOS_VERIFIED"].sort_values("ps_med_test", ascending=False)
    print(f"\n===== SUMMARY (COARSE) =====")
    print(f"mined: {N_mined} · OOS_VERIFIED: {len(ver_df)} ({len(ver_df)/max(N_mined,1)*100:.1f}%)")
    tv = ver_df[ver_df.seq.str.split().str[-1].str.startswith("-T")]
    print(f"…of which T-ending (served): {len(tv)}")
    if len(ver_df):
        print(f"\nTOP-25 OOS-verified by TEST path-sim median:")
        print(f"{'seq':22s} {'d':>2s} {'nTr':>6s} {'nTe':>6s} {'medTe':>6s} {'psMed':>6s} {'psWin':>5s} {'DSR':>5s}")
        for _, r in ver_df.head(25).iterrows():
            print(f"{r.seq:22s} {r.depth:>2d} {r.n_train:>6d} {r.n_test:>6d} "
                  f"{r.med20_test:>6.2f} {r.ps_med_test:>6.2f} {r.ps_win_test:>5.1f} {r.get('dsr', float('nan')):>5.2f}")
        print(f"\nDSR>=0.6 among verified: {(ver_df.get('dsr', pd.Series(dtype=float)) >= 0.6).sum()}")
    print(f"\ndone {time.time()-t0:.0f}s → {OUT}")


if __name__ == "__main__":
    main()
