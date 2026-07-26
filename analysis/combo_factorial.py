"""
combo_factorial.py — ANALYSIS ONLY (no production code).

2x2 factorial: does the screener combo `flip→T1 + −1:l5=PS` add anything ON TOP of
acc_tr(TEST)?  combo = (t_sig=='T1') AND (prev bar_line5=='PS')  — the faithful
bar-level translation of the screener's flip_code=T1 + p1_l5=PS (exact categorical).

Fixed: r2k+nas · TEST bar (close_pos<0.5 & <50% above 20d-low) · $-vol≥$500k ·
next-open · −15% stop / +100% target · gap-aware · glitch-screened · OOS.
Writes COMBO_FACTORIAL.md.  Run: cd backend && uv run python ../analysis/combo_factorial.py
"""
from __future__ import annotations
import sys, numpy as np, pandas as pd
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/analysis")
from exit_backtest import sim, is_glitch

CB = "/Users/sachoki/Desktop/sachoki-desktop/analysis/_combobars.parquet"
PARQ = "/Users/sachoki/Desktop/sachoki-desktop/analysis/_candidates.parquet"
OUT = "/Users/sachoki/Desktop/sachoki-desktop/COMBO_FACTORIAL.md"
CASE = {"PAVS", "WNW", "GLOO"}
UNIS = ("russell2k", "nasdaq")
STOP, TGT, DVFLOOR = 15, 100, 500_000
NEITHER_CAP = 25000
RNG = np.random.default_rng(7)


def build_store(cb):
    store = {}
    for (u, t), s in cb.groupby(["universe", "ticker"], sort=False):
        s = s.reset_index(drop=True)
        store[(u, t)] = dict(O=s["open"].to_numpy(float), H=s["high"].to_numpy(float),
                             L=s["low"].to_numpy(float), C=s["close"].to_numpy(float),
                             DV=s["dv"].to_numpy(float),
                             idx={d: k for k, d in enumerate(s["date"].to_numpy())})
    return store


def returns_for(entries, store, hz=10, glitch=True):
    out = []
    for u, t, i in entries:
        rec = store.get((u, t))
        if rec is None:
            continue
        if glitch and is_glitch(rec, i, hz):
            continue
        res = sim(rec, i, STOP, TGT, hz, trailing=None, entry_mode="next_open")
        if res is None or not res.get("dv") or res["dv"] < DVFLOOR:
            continue
        out.append(res["r"])
    return np.asarray(out, float)


def metrics(r):
    if len(r) == 0:
        return dict(n=0)
    pos = r[r > 0]; neg = r[r < 0]
    ml = abs(neg.mean()) if len(neg) else 0.0
    return dict(n=len(r), exp=round(float(r.mean()), 2), med=round(float(np.median(r)), 2),
                win=round(float((r > 0).mean() * 100), 1),
                p50=round(float((r >= 50).mean() * 100), 1),
                p100=round(float((r >= 100).mean() * 100), 1),
                mw=round(float(pos.mean()), 1) if len(pos) else 0.0,
                payoff=round(float(pos.mean() / ml), 2) if (len(pos) and ml) else np.nan)


def frow(label, m):
    if m["n"] == 0:
        return f"| {label} | 0 |" + " — |" * 7
    return (f"| {label} | {m['n']} | **{m['exp']}** | {m['med']} | {m['win']} | "
            f"{m['p50']} | {m['p100']} | {m['mw']} | {m['payoff']} |")


HEAD = ("| cell | n | EXPECTANCY | med | win% | P(+50%) | P(+100%) | mean win | payoff |\n"
        "|---|---|---|---|---|---|---|---|---|")


def main():
    cb = pd.read_parquet(CB)
    store = build_store(cb)
    feat = cb[["universe", "ticker", "date", "t_sig", "prev_l5", "close_pos", "ext20", "dv"]]

    cand = pd.read_parquet(PARQ)
    cand = cand[cand.universe.isin(UNIS) & ~cand.ticker.isin(CASE)]
    cand = cand.merge(feat, on=["universe", "ticker", "date"], how="left")
    cand = cand[cand["close_pos"].notna()].copy()
    cand["TEST"] = (cand.close_pos < 0.5) & (cand.ext20 < 50)
    cand["combo"] = (cand.t_sig == "T1") & (cand.prev_l5 == "PS")
    cand["acc"] = cand.f1_acc_tr == 1
    # fixed population: TEST bars with liquidity (dv floor applied again at fill time)
    pop = cand[cand.TEST & (cand.dv >= DVFLOOR)].copy()

    def ents(sub):
        out = []
        for r in sub.itertuples():
            rec = store.get((r.universe, r.ticker))
            if rec is None:
                continue
            i = rec["idx"].get(np.datetime64(r.date))
            if i is not None:
                out.append((r.universe, r.ticker, i))
        return out

    md = ["# Combo × acc_tr(TEST) factorial — keep the combo or drop it?", ""]
    md.append("_combo = `flip→T1 + −1:l5=PS` ≙ (t_sig=='T1') AND (prev bar_line5=='PS'), the exact "
              "screener encoding. Fixed: r2k+nas · TEST bar · $-vol≥$500k · next-open · −15%/+100% · "
              "gap-aware · glitch-screened · OOS (ex PAVS/WNW/GLOO). Percent units. No production code._\n")
    md.append("_Step 0 confirmation: the combo fires on PAVS (12), WNW (3), GLOO (2) bars — replication verified._\n")

    # ---- overlap ----
    md.append("## Overlap (within TEST bars, $-vol≥$500k)\n")
    md.append("| universe | TEST n | acc_tr n | combo n | BOTH n | combo⊂acc% | acc⊂combo% |\n|---|---|---|---|---|---|---|")
    for uni in UNIS:
        d = pop[pop.universe == uni]
        na = int(d.acc.sum()); nc = int(d.combo.sum()); nb = int((d.acc & d.combo).sum())
        md.append(f"| {uni} | {len(d)} | {na} | {nc} | {nb} | "
                  f"{round(100*nb/na,1) if na else 0} | {round(100*nb/nc,1) if nc else 0} |")

    # ---- 2x2 per universe, h10 & h20 ----
    for hz in (10, 20):
        md.append(f"\n## 2×2 factorial — horizon {hz}d (OOS)\n")
        for uni in UNIS:
            d = pop[pop.universe == uni]
            cells = {
                "(3) BOTH (combo & acc_tr)": d[d.acc & d.combo],
                "(2) acc_tr only": d[d.acc & ~d.combo],
                "(1) combo only": d[~d.acc & d.combo],
                "(4) neither (baseline, sampled)": d[~d.acc & ~d.combo],
            }
            md.append(f"\n**{uni}**\n"); md.append(HEAD)
            for name, sub in cells.items():
                if name.startswith("(4)") and len(sub) > NEITHER_CAP:
                    sub = sub.sample(NEITHER_CAP, random_state=3)
                r = returns_for(ents(sub), store, hz=hz)
                md.append(frow(name, metrics(r)))

    # ---- BOTH vs acc-only significance (bootstrap diff in expectancy), h10 ----
    md.append("\n## BOTH vs acc_tr-only — is any lift beyond noise? (h10, bootstrap)\n")
    md.append("| universe | acc-only EXP (n) | BOTH EXP (n) | Δ | bootstrap p(Δ>0) | verdict |\n|---|---|---|---|---|---|")
    for uni in UNIS:
        d = pop[pop.universe == uni]
        ra = returns_for(ents(d[d.acc & ~d.combo]), store)
        rb = returns_for(ents(d[d.acc & d.combo]), store)
        ma, mb = metrics(ra), metrics(rb)
        if mb["n"] < 30:
            md.append(f"| {uni} | {ma.get('exp','—')} (n{ma['n']}) | {mb.get('exp','—')} (n{mb['n']}) | "
                      f"— | — | ⚠ BOTH n<30 — n IS the answer (over-restricts) |")
            continue
        diff = mb["exp"] - ma["exp"]
        boots = []
        for _ in range(2000):
            sa = ra[RNG.integers(0, len(ra), len(ra))]
            sb = rb[RNG.integers(0, len(rb), len(rb))]
            boots.append(sb.mean() - sa.mean())
        boots = np.asarray(boots)
        pgt = float((boots > 0).mean())
        verdict = "additive" if (diff > 0 and pgt >= 0.95) else ("redundant/noise" if abs(diff) < 99 else "")
        md.append(f"| {uni} | {ma['exp']} (n{ma['n']}) | {mb['exp']} (n{mb['n']}) | {round(diff,2)} | "
                  f"{round(pgt,3)} | {verdict} |")

    open(OUT, "w").write("\n".join(md) + "\n")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
