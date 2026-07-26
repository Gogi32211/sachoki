"""
exit_backtest.py — ANALYSIS ONLY (no production code touched).

Path-dependent exit backtest on the TEST/absorption subset of the discriminator
gates (acc_tr, range_exp, acc_tr x range_exp), russell2k+nasdaq, OOS (ex case-study).
Entry = NEXT bar open. Gap-aware fills. Stop-first on same-bar conflict. Liquidity
floor + glitch screen. Per-year stability. Writes EXIT_BACKTEST.md.

Run: cd backend && uv run python ../analysis/exit_backtest.py
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
PARQ = "/Users/sachoki/Desktop/sachoki-desktop/analysis/_candidates.parquet"
OUT = "/Users/sachoki/Desktop/sachoki-desktop/EXIT_BACKTEST.md"
CASE = {"PAVS", "WNW", "GLOO"}
UNIS = ("russell2k", "nasdaq")
STOPS = [8, 12, 15, 20]
TARGETS = [25, 50, 100]
TRAIL = 20
HORIZONS = [10, 20]


# ---------- bar store ----------
def build_store(tickers):
    con = duckdb.connect(DB, read_only=True)
    ph = ",".join("?" * len(tickers))
    bars = con.execute(f"""
        SELECT universe,ticker,date,open,high,low,close,volume
        FROM bars WHERE ticker IN ({ph}) AND universe IN ('russell2k','nasdaq')
        ORDER BY universe,ticker,date
    """, tickers).fetchdf()
    con.close()
    bars = bars.drop_duplicates(["universe", "ticker", "date"]).reset_index(drop=True)
    g = bars.groupby(["universe", "ticker"], sort=False)
    rng = (bars["high"] - bars["low"]).replace(0, np.nan)
    bars["close_pos"] = ((bars["close"] - bars["low"]) / rng).clip(0, 1)
    bars["low20"] = g["low"].transform(lambda s: s.rolling(20, min_periods=5).min())
    bars["ext20"] = (bars["close"] / bars["low20"] - 1) * 100
    bars["dv"] = bars["close"] * bars["volume"]
    store = {}
    for (u, t), sub in g:
        s = sub.reset_index(drop=True)
        store[(u, t)] = dict(
            O=s["open"].to_numpy(float), H=s["high"].to_numpy(float),
            L=s["low"].to_numpy(float), C=s["close"].to_numpy(float),
            DV=s["dv"].to_numpy(float),
            cp=s["close_pos"].to_numpy(float), ext=s["ext20"].to_numpy(float),
            idx={d: k for k, d in enumerate(s["date"].to_numpy())},
            dates=s["date"].to_numpy(),
        )
    return store


# ---------- single-trade path sim ----------
def sim(rec, i, stop_pct, target_pct, horizon, trailing=None, entry_mode="next_open"):
    O, H, L, C = rec["O"], rec["H"], rec["L"], rec["C"]
    n = len(C)
    if entry_mode == "next_open":
        if i + 1 >= n:
            return None
        entry = O[i + 1]; start = i + 1
    else:  # close entry on signal bar
        entry = C[i]; start = i + 1
        if start >= n:
            return None
    if not (entry > 0):
        return None
    end = min(i + horizon, n - 1) if entry_mode == "next_open" else min(i + horizon, n - 1)
    stop = entry * (1 - stop_pct / 100) if stop_pct else None
    target = entry * (1 + target_pct / 100) if target_pct else None
    run_high = max(entry, H[start])
    gap_stop = gap_tgt = False
    for k in range(start, end + 1):
        o, h, l = O[k], H[k], L[k]
        if trailing:
            run_high = max(run_high, h)
            tstop = run_high * (1 - trailing / 100)
            if l <= tstop:
                if o < tstop:
                    gap_stop = True
                return dict(r=(min(tstop, o) / entry - 1) * 100, how="trail",
                            bars=k - start + 1, gs=gap_stop, gt=gap_tgt, dv=rec["DV"][start])
            continue
        stop_hit = l <= stop
        tgt_hit = h >= target
        if stop_hit and tgt_hit:          # conservative: stop first
            if o < stop:
                gap_stop = True
            return dict(r=(min(stop, o) / entry - 1) * 100, how="stop_both",
                        bars=k - start + 1, gs=gap_stop, gt=gap_tgt, dv=rec["DV"][start])
        if stop_hit:
            if o < stop:
                gap_stop = True
            return dict(r=(min(stop, o) / entry - 1) * 100, how="stop",
                        bars=k - start + 1, gs=gap_stop, gt=gap_tgt, dv=rec["DV"][start])
        if tgt_hit:
            if o > target:
                gap_tgt = True
            return dict(r=(max(target, o) / entry - 1) * 100, how="target",
                        bars=k - start + 1, gs=gap_stop, gt=gap_tgt, dv=rec["DV"][start])
    return dict(r=(C[end] / entry - 1) * 100, how="horizon", bars=end - start + 1,
                gs=gap_stop, gt=gap_tgt, dv=rec["DV"][start])


# ---------- metrics ----------
def metrics(trades):
    if len(trades) < 1:
        return dict(n=0)
    r = np.array([t["r"] for t in trades], float)
    dates = [t["date"] for t in trades]
    order = np.argsort(dates)
    eq = np.cumprod(1 + r[order] / 100.0)
    peak = np.maximum.accumulate(eq)
    dd = float(((eq - peak) / peak).min() * 100)
    return dict(n=len(r), exp=round(float(r.mean()), 2), med=round(float(np.median(r)), 2),
                win=round(float((r > 0).mean() * 100), 1),
                p50=round(float((r >= 50).mean() * 100), 1),
                maxloss=round(float(r.min()), 1), std=round(float(r.std()), 1),
                ra=round(float(r.mean() / r.std()), 3) if r.std() else np.nan,
                maxdd=round(dd, 1))


def run_config(eps, store, stop, target, horizon, trailing=None, entry_mode="next_open",
               dv_floor=0.0, drop_glitch=False):
    trades = []
    for u, t, i, edate in eps:
        rec = store.get((u, t))
        if rec is None:
            continue
        if drop_glitch and is_glitch(rec, i, horizon):
            continue
        res = sim(rec, i, stop, target, horizon, trailing, entry_mode)
        if res is None:
            continue
        if dv_floor and (not res["dv"] or res["dv"] < dv_floor):
            continue
        res["date"] = edate
        trades.append(res)
    return trades


def is_glitch(rec, i, horizon):
    """Bad-print/reverse-split heuristic in the forward window: a single bar whose
    high >= 2.5x prior close that fully reverses next bar, or a >4x close jump that
    reverts — price discontinuity inconsistent with a tradeable move."""
    O, H, L, C = rec["O"], rec["H"], rec["L"], rec["C"]
    n = len(C)
    for k in range(i + 1, min(i + horizon, n - 1)):
        if C[k - 1] <= 0:
            continue
        if H[k] >= 2.5 * C[k - 1] and C[k + 1] <= 1.2 * C[k - 1]:   # spike fully reversed
            return True
        if C[k] >= 4 * C[k - 1] and C[k + 1] <= 1.5 * C[k - 1]:     # split-like jump+revert
            return True
    return False


def main():
    df = pd.read_parquet(PARQ)
    df = df[df["universe"].isin(UNIS) & ~df["ticker"].isin(CASE)]
    df = df[(df["f1_acc_tr"] == 1) | (df["f4_range_exp"] == 1)].copy()
    tickers = sorted(df["ticker"].unique())
    print(f"building store for {len(tickers)} tickers ...", flush=True)
    store = build_store(tickers)

    # attach close_pos / ext20 at the signal bar and the integer index i
    def lookup(row):
        rec = store.get((row["universe"], row["ticker"]))
        if rec is None:
            return pd.Series([np.nan, np.nan, -1])
        i = rec["idx"].get(np.datetime64(row["date"]))
        if i is None:
            return pd.Series([np.nan, np.nan, -1])
        return pd.Series([rec["cp"][i], rec["ext"][i], i])
    df[["cp", "ext", "i"]] = df.apply(lookup, axis=1)
    df = df[df["i"] >= 0].copy()
    df["i"] = df["i"].astype(int)
    df["TEST"] = (df["cp"] < 0.5) & (df["ext"] < 50)
    df["year"] = pd.to_datetime(df["date"]).dt.year

    GATES = {
        "acc_tr(TEST)": lambda d: d[(d.f1_acc_tr == 1) & d.TEST],
        "range_exp(TEST)": lambda d: d[(d.f4_range_exp == 1) & d.TEST],
        "acc_tr x range_exp(TEST)": lambda d: d[(d.f1_acc_tr == 1) & (d.f4_range_exp == 1) & d.TEST],
    }

    def eps_of(sub):
        return [(r.universe, r.ticker, int(r.i), r.date) for r in sub.itertuples()]

    md = ["# Path-dependent exit backtest — discriminator promotion gate", ""]
    md.append("_TEST/absorption subset (close_pos<0.5 & <50% above 20d-low). Entry = NEXT bar open "
              "(close-entry shown for the best config). Gap-aware fills; same-bar stop+target = STOP first. "
              "russell2k+nasdaq, OOS (ex PAVS/WNW/GLOO). Percent units. No production code touched._\n")

    # gate sizes
    md.append("### TEST-subset episode counts (OOS)\n")
    md.append("| gate | russell2k | nasdaq |\n|---|---|---|")
    for g, fn in GATES.items():
        sub = fn(df)
        md.append(f"| {g} | {int((sub.universe=='russell2k').sum())} | {int((sub.universe=='nasdaq').sum())} |")

    # ===== PART A grid (horizon 10 headline) =====
    md.append("\n## PART A — stop/target/trailing expectancy grid (entry=next open, horizon=10d, OOS)\n")
    H = ("| config | n | EXPECTANCY | med | win% | P(+50%) | max loss | std | exp/std | maxDD |\n"
         "|---|---|---|---|---|---|---|---|---|---|")
    configs = [("trail20", None, None, TRAIL)] + [(f"s{s}/t{tg}", s, tg, None) for s in STOPS for tg in TARGETS]
    best = {}   # gate,uni -> (exp, config, n)
    for g, fn in GATES.items():
        for uni in UNIS:
            sub = fn(df); sub = sub[sub.universe == uni]
            eps = eps_of(sub)
            md.append(f"\n**{g} · {uni}**  (TEST n={len(eps)})\n")
            md.append(H)
            for name, s, tg, tr in configs:
                tr_trades = run_config(eps, store, s, tg, 10, trailing=tr)
                m = metrics(tr_trades)
                if m["n"] == 0:
                    md.append(f"| {name} | 0 |" + " — |" * 9); continue
                md.append(f"| {name} | {m['n']} | **{m['exp']}** | {m['med']} | {m['win']} | "
                          f"{m['p50']} | {m['maxloss']} | {m['std']} | {m['ra']} | {m['maxdd']} |")
                if m["n"] >= 30 and (g, uni) not in best or (m["n"] >= 30 and m["exp"] > best.get((g, uni), (-1e9,))[0]):
                    best[(g, uni)] = (m["exp"], name, s, tg, tr, m["n"])

    # ===== best config deep-dive =====
    md.append("\n## PART A — best config per gate×universe (n≥30, by OOS expectancy)\n")
    md.append("| gate · universe | best config | n | EXPECTANCY | positive? |\n|---|---|---|---|---|")
    for (g, uni), v in sorted(best.items()):
        md.append(f"| {g} · {uni} | {v[1]} | {v[5]} | **{v[0]}** | {'✅ YES' if v[0] > 0 else '❌ no'} |")

    # pick a single overall candidate: highest-n gate (acc_tr) config that is positive in BOTH unis, else report none
    # evaluate acc_tr(TEST) configs pooled-by-universe-reported (not pooled stats)
    md.append("\n### Does any config give POSITIVE OOS expectancy on a high-n gate?\n")
    accfn = GATES["acc_tr(TEST)"]
    pos_cfgs = []
    for name, s, tg, tr in configs:
        ok = True; rowtxt = [name]
        for uni in UNIS:
            sub = accfn(df); sub = sub[sub.universe == uni]
            m = metrics(run_config(eps_of(sub), store, s, tg, 10, trailing=tr))
            rowtxt.append(f"{uni}:{m.get('exp','—')}(n{m.get('n',0)})")
            if not (m.get("n", 0) >= 30 and m.get("exp", -1) > 0):
                ok = False
        if ok:
            pos_cfgs.append(name)
        md.append("- " + " · ".join(rowtxt) + (" ✅BOTH+" if ok else ""))
    md.append(f"\n**Configs positive in BOTH universes (acc_tr TEST, n≥30):** {pos_cfgs or 'NONE'}\n")

    # ===== PART B: liquidity / glitch / gap on the most-traded best config =====
    # choose reference config = best acc_tr russell2k config (highest n gate)
    ref = best.get(("acc_tr(TEST)", "russell2k")) or best.get(("acc_tr(TEST)", "nasdaq"))
    md.append("\n## PART B — tail capturability / fill realism\n")
    if ref:
        _, cname, cs, ctg, ctr, _ = ref
        md.append(f"Reference config = **{cname}** (best acc_tr(TEST) config).\n")
        md.append("### Liquidity floor — entry-day $-volume\n")
        md.append("| gate · uni | floor | n kept | n dropped | EXPECTANCY kept |\n|---|---|---|---|---|")
        for g in ("acc_tr(TEST)", "range_exp(TEST)"):
            for uni in UNIS:
                sub = GATES[g](df); sub = sub[sub.universe == uni]; eps = eps_of(sub)
                base = run_config(eps, store, cs, ctg, 10, trailing=ctr)
                for floor in (100_000, 500_000, 1_000_000):
                    kept = [t for t in base if t["dv"] and t["dv"] >= floor]
                    m = metrics(kept)
                    md.append(f"| {g} · {uni} | ${floor//1000}k | {m.get('n',0)} | {len(base)-len(kept)} | "
                              f"{m.get('exp','—') if m.get('n') else '—'} |")
        # tail winners below floor
        md.append("\n### Tail winners (realized ≥+100%) vs liquidity floor\n")
        md.append("| gate · uni | tail winners | <$100k | <$500k | <$1M |\n|---|---|---|---|---|")
        for g in ("acc_tr(TEST)", "range_exp(TEST)"):
            for uni in UNIS:
                sub = GATES[g](df); sub = sub[sub.universe == uni]
                base = run_config(eps_of(sub), store, cs, ctg, 10, trailing=ctr)
                tails = [t for t in base if t["r"] >= 100]
                b1 = sum(1 for t in tails if not t["dv"] or t["dv"] < 100_000)
                b5 = sum(1 for t in tails if not t["dv"] or t["dv"] < 500_000)
                b10 = sum(1 for t in tails if not t["dv"] or t["dv"] < 1_000_000)
                md.append(f"| {g} · {uni} | {len(tails)} | {b1} | {b5} | {b10} |")
        # gap-through tax + glitch on acc_tr both unis pooled-report
        md.append("\n### Gap-through tax + glitch screen (acc_tr(TEST), per uni)\n")
        md.append("| uni | n | stop exits | gapped-through-stop% | target exits | gapped-thru-target% | glitch flagged |\n|---|---|---|---|---|---|---|")
        for uni in UNIS:
            sub = accfn(df); sub = sub[sub.universe == uni]; eps = eps_of(sub)
            base = run_config(eps, store, cs, ctg, 10, trailing=ctr)
            stops = [t for t in base if "stop" in t["how"]]
            tgts = [t for t in base if t["how"] == "target"]
            gstop = sum(1 for t in stops if t["gs"]); gtgt = sum(1 for t in tgts if t["gt"])
            ng = sum(1 for (u, tk, i, ed) in eps if is_glitch(store[(u, tk)], i, 10))
            md.append(f"| {uni} | {len(base)} | {len(stops)} | "
                      f"{round(100*gstop/len(stops),1) if stops else 0} | {len(tgts)} | "
                      f"{round(100*gtgt/len(tgts),1) if tgts else 0} | {ng} |")
        # de-rated re-run: liquidity $500k + glitch drop
        md.append("\n### De-rated best config (≥$500k entry $-vol + glitch-dropped)\n")
        md.append("| gate · uni | raw EXP (n) | de-rated EXP (n) |\n|---|---|---|")
        for g in ("acc_tr(TEST)", "range_exp(TEST)", "acc_tr x range_exp(TEST)"):
            for uni in UNIS:
                sub = GATES[g](df); sub = sub[sub.universe == uni]; eps = eps_of(sub)
                raw = metrics(run_config(eps, store, cs, ctg, 10, trailing=ctr))
                der = metrics(run_config(eps, store, cs, ctg, 10, trailing=ctr,
                                         dv_floor=500_000, drop_glitch=True))
                md.append(f"| {g} · {uni} | {raw.get('exp','—')} (n{raw.get('n',0)}) | "
                          f"{der.get('exp','—')} (n{der.get('n',0)}) |")

        # ===== per-year stability (acc_tr TEST, ref config, de-rated) =====
        md.append("\n## Per-year stability — acc_tr(TEST), ref config, ≥$500k + glitch-dropped\n")
        md.append("| year | russell2k EXP (n) | nasdaq EXP (n) |\n|---|---|---|")
        for yr in range(2021, 2027):
            cells = []
            for uni in UNIS:
                sub = accfn(df); sub = sub[(sub.universe == uni) & (sub.year == yr)]
                m = metrics(run_config(eps_of(sub), store, cs, ctg, 10, trailing=ctr,
                                       dv_floor=500_000, drop_glitch=True))
                cells.append(f"{m.get('exp','—') if m.get('n') else '—'} (n{m.get('n',0)})")
            md.append(f"| {yr} | {cells[0]} | {cells[1]} |")

        # horizon 20 + close-entry comparison for ref config
        md.append("\n## Horizon & entry sensitivity — acc_tr(TEST), ref config (≥$500k+glitch)\n")
        md.append("| variant | russell2k EXP (n) | nasdaq EXP (n) |\n|---|---|---|")
        for label, hz, em in [("h10 next-open", 10, "next_open"), ("h20 next-open", 20, "next_open"),
                              ("h10 close-entry", 10, "close")]:
            cells = []
            for uni in UNIS:
                sub = accfn(df); sub = sub[sub.universe == uni]
                m = metrics(run_config(eps_of(sub), store, cs, ctg, hz, trailing=ctr,
                                       entry_mode=em, dv_floor=500_000, drop_glitch=True))
                cells.append(f"{m.get('exp','—') if m.get('n') else '—'} (n{m.get('n',0)})")
            md.append(f"| {label} | {cells[0]} | {cells[1]} |")

    open(OUT, "w").write("\n".join(md) + "\n")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
