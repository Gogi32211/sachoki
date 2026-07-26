"""
alpha_regime.py — ANALYSIS ONLY (no production code touched).

TEST 1: is acc_tr(TEST) ALPHA (selection) or BETA (regime switch)? Bootstrap
        week-matched control baskets (CONTROL-A random microcap, CONTROL-B
        candidate non-acc_tr) under the SAME exit rule + liquidity floor.
TEST 2: a CAUSAL universe-internal regime gate (breadth + pumpiness, data<=i),
        per-year ON/OFF, gated expectancy, 2022 stand-down + leakage check.

Fixed strategy: russell2k+nasdaq · acc_tr(TEST) (close_pos<0.5 & <50% above 20d-low)
· entry-day $-vol>=$500k · entry NEXT-bar open · -15% stop / +100% target · gap-aware,
stop-first. Writes ALPHA_AND_REGIME.md.  Run: cd backend && uv run python ../analysis/alpha_regime.py
"""
from __future__ import annotations
import sys, numpy as np, pandas as pd, duckdb
sys.path.insert(0, "/Users/sachoki/Desktop/sachoki-desktop/analysis")
from exit_backtest import sim   # reuse the exact path-aware fill logic

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
PARQ = "/Users/sachoki/Desktop/sachoki-desktop/analysis/_candidates.parquet"
OUT = "/Users/sachoki/Desktop/sachoki-desktop/ALPHA_AND_REGIME.md"
CASE = {"PAVS", "WNW", "GLOO"}
UNIS = ("russell2k", "nasdaq")
STOP, TGT = 15, 100
DVFLOOR = 500_000
NBOOT = 600
KWEEK = 80          # control bars sampled per (universe, week)
RNG = np.random.default_rng(20260609)


def load_full():
    con = duckdb.connect(DB, read_only=True)
    df = con.execute("""
        SELECT universe,ticker,date,open,high,low,close,volume
        FROM bars WHERE universe IN ('russell2k','nasdaq')
        ORDER BY universe,ticker,date
    """).fetchdf()
    con.close()
    df = df.drop_duplicates(["universe", "ticker", "date"]).reset_index(drop=True)
    g = df.groupby(["universe", "ticker"], sort=False)
    rng = (df["high"] - df["low"]).replace(0, np.nan)
    df["close_pos"] = ((df["close"] - df["low"]) / rng).clip(0, 1)
    df["low20"] = g["low"].transform(lambda s: s.rolling(20, min_periods=5).min())
    df["ext20"] = (df["close"] / df["low20"] - 1) * 100
    df["dv"] = df["close"] * df["volume"]
    df["ma50"] = g["close"].transform(lambda s: s.rolling(50, min_periods=30).mean())
    df["ma200"] = g["close"].transform(lambda s: s.rolling(200, min_periods=120).mean())
    df["ret10"] = g["close"].transform(lambda s: s / s.shift(10) - 1)
    df["week"] = pd.to_datetime(df["date"]).dt.strftime("%G-%V")
    df["year"] = pd.to_datetime(df["date"]).dt.year
    return df


def build_store(df):
    store = {}
    for (u, t), s in df.groupby(["universe", "ticker"], sort=False):
        s = s.reset_index(drop=True)
        store[(u, t)] = dict(
            O=s["open"].to_numpy(float), H=s["high"].to_numpy(float),
            L=s["low"].to_numpy(float), C=s["close"].to_numpy(float),
            DV=s["dv"].to_numpy(float),
            idx={d: k for k, d in enumerate(s["date"].to_numpy())})
    return store


def trade_returns(entries, store):
    """entries: list of (u,t,i,week,year,date). Returns list of dicts incl r."""
    out = []
    for u, t, i, wk, yr, dt in entries:
        rec = store.get((u, t))
        if rec is None:
            continue
        res = sim(rec, i, STOP, TGT, 10, trailing=None, entry_mode="next_open")
        if res is None or not res.get("dv") or res["dv"] < DVFLOOR:
            continue
        out.append(dict(u=u, week=wk, year=yr, r=res["r"]))
    return out


def basket_stats(rs):
    r = np.asarray(rs, float)
    pos = r[r > 0]
    return dict(exp=r.mean(), p50=(r >= 50).mean() * 100, p100=(r >= 100).mean() * 100,
                win=(r > 0).mean() * 100, mw=pos.mean() if len(pos) else 0.0)


def bootstrap_pct(treat_rs, pool_by_week, weeks, nboot=NBOOT):
    """Draw, for each treatment entry, a control return from the SAME week pool;
    build nboot basket distributions; return treatment percentile per metric."""
    treat = basket_stats(treat_rs)
    keys = ["exp", "p50", "p100", "win", "mw"]
    dist = {k: [] for k in keys}
    usable_weeks = [w for w in weeks if w in pool_by_week and len(pool_by_week[w])]
    if not usable_weeks:
        return None
    for _ in range(nboot):
        draws = []
        for w in weeks:
            pool = pool_by_week.get(w)
            if pool:
                draws.append(pool[RNG.integers(len(pool))])
            elif usable_weeks:
                w2 = usable_weeks[RNG.integers(len(usable_weeks))]
                draws.append(pool_by_week[w2][RNG.integers(len(pool_by_week[w2]))])
        if len(draws) < 10:
            continue
        bs = basket_stats(draws)
        for k in keys:
            dist[k].append(bs[k])
    res = {}
    for k in keys:
        arr = np.asarray(dist[k], float)
        if len(arr) == 0:
            res[k] = None; continue
        res[k] = dict(treat=round(float(treat[k]), 2),
                      cmean=round(float(arr.mean()), 2),
                      c5=round(float(np.percentile(arr, 5)), 2),
                      c95=round(float(np.percentile(arr, 95)), 2),
                      pct=round(float((arr < treat[k]).mean() * 100), 1))
    return res


def pool_by_week(rs):
    d = {}
    for x in rs:
        d.setdefault(x["week"], []).append(x["r"])
    return d


def fmt_pct_block(title, res):
    if res is None:
        return [f"**{title}** — _no week-matched control pool_\n"]
    lines = [f"**{title}**\n",
             "| metric | acc_tr(TEST) | control mean | control 5–95% | **percentile** |",
             "|---|---|---|---|---|"]
    nm = {"exp": "EXPECTANCY", "p50": "P(+50%)", "p100": "P(+100%)", "win": "win%", "mw": "mean winner"}
    for k in ["exp", "p50", "p100", "win", "mw"]:
        r = res[k]
        flag = " ✅>90th" if r["pct"] >= 90 else (" ⚠ in-band" if r["pct"] >= 10 else " ❌<10th")
        lines.append(f"| {nm[k]} | {r['treat']} | {r['cmean']} | {r['c5']} … {r['c95']} | **{r['pct']}th**{flag} |")
    return lines


def main():
    print("loading full r2k+nas ...", flush=True)
    df = load_full()
    print(f"  {len(df):,} rows; building store", flush=True)
    store = build_store(df)

    # ---- breadth / pumpiness time series (causal) ----
    print("building causal regime series ...", flush=True)
    df["above50"] = (df["close"] > df["ma50"]).astype(float)
    df["above200"] = (df["close"] > df["ma200"]).astype(float)
    df["pumpf"] = (df["ret10"] >= 0.5).astype(float)
    breadth = (df.groupby(["universe", "date"])
                 .agg(b50=("above50", "mean"), b200=("above200", "mean"),
                      pump=("pumpf", "mean"), nact=("ticker", "count")).reset_index())
    breadth = breadth[breadth["nact"] >= 50].copy()
    gate = {}
    for uni in UNIS:
        b = breadth[breadth["universe"] == uni].sort_values("date").reset_index(drop=True)
        # expanding-median thresholds, strictly past (shift 1) -> causal, self-calibrating
        b["thr_b"] = b["b50"].expanding(min_periods=60).median().shift(1)
        b["thr_p"] = b["pump"].expanding(min_periods=60).median().shift(1)
        b["ON"] = ((b["b50"] > b["thr_b"]) & (b["pump"] > b["thr_p"])).astype(float)
        # leakage-robust variant: lag the gate by 5 trading days
        b["ON_lag5"] = b["ON"].shift(5)
        gate[uni] = b.set_index("date")
    df.drop(columns=["above50", "above200", "pumpf"], inplace=True)

    # ---- candidate episodes (parquet) ----
    cand = pd.read_parquet(PARQ)
    cand = cand[cand.universe.isin(UNIS) & ~cand.ticker.isin(CASE)].copy()

    def attach_i(c):
        rows = []
        for r in c.itertuples():
            rec = store.get((r.universe, r.ticker))
            if rec is None:
                continue
            i = rec["idx"].get(np.datetime64(r.date))
            if i is None:
                continue
            rows.append((r.universe, r.ticker, i, r.date))
        return rows

    # treatment: acc_tr(TEST) + dv floor  (TEST recomputed from store close_pos/ext)
    # need close_pos/ext at signal bar -> pull from df via merge
    feat = df[["universe", "ticker", "date", "close_pos", "ext20", "week", "year"]]
    cand = cand.merge(feat, on=["universe", "ticker", "date"], how="left")
    cand["TEST"] = (cand.close_pos < 0.5) & (cand.ext20 < 50)
    treat_ep = cand[(cand.f1_acc_tr == 1) & cand.TEST]
    ctrlB_ep = cand[~((cand.f1_acc_tr == 1) & cand.TEST)]   # candidate, NOT acc_tr(TEST)

    def to_entries(sub):
        out = []
        for r in sub.itertuples():
            rec = store.get((r.universe, r.ticker))
            if rec is None:
                continue
            i = rec["idx"].get(np.datetime64(r.date))
            if i is None:
                continue
            out.append((r.universe, r.ticker, i, r.week, r.year, r.date))
        return out

    treat_entries = to_entries(treat_ep)
    treat_rs = trade_returns(treat_entries, store)

    # CONTROL-A pool: random microcap bars (any), dv>=floor, sampled per (uni,week)
    slim = df[df["dv"] >= DVFLOOR][["universe", "ticker", "date", "week", "year"]].copy()
    # sample up to KWEEK per (universe, week) to bound sim count
    sampA = (slim.groupby(["universe", "week"], group_keys=False)
                 .apply(lambda g: g.sample(min(len(g), KWEEK), random_state=7)))
    ctrlA_entries = to_entries(sampA.assign(f=0))  # week/year already columns
    # to_entries needs itertuples fields; sampA has them
    ctrlA_entries = []
    for r in sampA.itertuples():
        rec = store.get((r.universe, r.ticker))
        if rec is None:
            continue
        i = rec["idx"].get(np.datetime64(r.date))
        if i is None:
            continue
        ctrlA_entries.append((r.universe, r.ticker, i, r.week, r.year, r.date))
    ctrlA_rs = trade_returns(ctrlA_entries, store)

    # CONTROL-B pool: candidate non-acc_tr(TEST), sampled per (uni,week)
    sampB = (ctrlB_ep.groupby(["universe", "week"], group_keys=False)
                     .apply(lambda g: g.sample(min(len(g), KWEEK), random_state=11)))
    ctrlB_rs = trade_returns(to_entries(sampB), store)

    md = ["# Alpha-vs-beta + causal regime gate — acc_tr(TEST)", ""]
    md.append("_Fixed strategy: russell2k+nasdaq · acc_tr(TEST) · entry-day $-vol≥$500k · next-open · "
              "−15% stop / +100% target · gap-aware. OOS (ex PAVS/WNW/GLOO). Percent units. No production code._\n")

    # ===== TEST 1 =====
    md.append("## TEST 1 — ALPHA vs BETA (week-matched bootstrap controls)\n")
    md.append(f"CONTROL-A = random microcap bars (no signal), matched (universe, ISO-week, $-vol≥$500k). "
              f"CONTROL-B = candidate T-signal bars that are NOT acc_tr(TEST), same matching. "
              f"{NBOOT} bootstrap baskets; treatment percentile within each control distribution.\n")
    for uni in UNIS:
        for span_lbl, yrs in [("all years", None), ("risk-on 2025–26", {2025, 2026})]:
            t = [x for x in treat_rs if x["u"] == uni and (yrs is None or x["year"] in yrs)]
            if len(t) < 30:
                md.append(f"\n### {uni} · {span_lbl} — n={len(t)} (<30, skipped)\n"); continue
            wks = [x["week"] for x in t]
            poolA = pool_by_week([x for x in ctrlA_rs if x["u"] == uni and (yrs is None or x["year"] in yrs)])
            poolB = pool_by_week([x for x in ctrlB_rs if x["u"] == uni and (yrs is None or x["year"] in yrs)])
            md.append(f"\n### {uni} · {span_lbl}  (acc_tr(TEST) n={len(t)})\n")
            md += fmt_pct_block("vs CONTROL-A (random microcap, same week)", bootstrap_pct([x["r"] for x in t], poolA, wks))
            md.append("")
            md += fmt_pct_block("vs CONTROL-B (candidate non-acc_tr, same week)", bootstrap_pct([x["r"] for x in t], poolB, wks))

    # ===== TEST 2 =====
    md.append("\n\n## TEST 2 — CAUSAL regime gate (universe-internal, data ≤ i)\n")
    md.append("Gate inputs at date i (all backward-looking): **breadth50** = % of active universe with "
              "close>MA50; **pump** = % of active universe with trailing-10d return ≥+50%. "
              "Gate **ON** if breadth50 AND pump each exceed their own **expanding-median** (computed only "
              "from dates < i → self-calibrating, no future data). A 5-day-lagged variant tests leakage.\n")

    def gate_on_for(u, dt, col="ON"):
        gb = gate[u]
        try:
            v = gb.at[dt, col]
            return (not pd.isna(v)) and v >= 1.0
        except KeyError:
            return False

    # per-year ON-fraction (of trading days) + gated vs ungated strategy
    md.append("### Per-year: gate ON-fraction (days) + strategy expectancy\n")
    md.append("| year | uni | gate ON% days | ungated EXP (n) | **gated EXP (n)** | gated-lag5 EXP (n) |")
    md.append("|---|---|---|---|---|---|")
    treat_by = {}
    for x, ent in zip(treat_rs, [e for e in treat_entries if store.get((e[0], e[1])) is not None]):
        pass  # (entries already aligned via trade_returns filtering; recompute mapping below)
    # rebuild treatment with date + gate flags directly
    trows = []
    for u, t, i, wk, yr, dt in treat_entries:
        rec = store.get((u, t))
        if rec is None:
            continue
        res = sim(rec, i, STOP, TGT, 10, trailing=None, entry_mode="next_open")
        if res is None or not res.get("dv") or res["dv"] < DVFLOOR:
            continue
        trows.append(dict(u=u, year=yr, date=dt, r=res["r"],
                          on=gate_on_for(u, dt, "ON"), on5=gate_on_for(u, dt, "ON_lag5")))
    tdf = pd.DataFrame(trows)
    for yr in range(2021, 2027):
        for uni in UNIS:
            gb = gate[uni]
            onfrac = gb[pd.to_datetime(gb.index).year == yr]["ON"].mean() if len(gb) else np.nan
            sub = tdf[(tdf.year == yr) & (tdf.u == uni)]
            ung = sub["r"]
            g_on = sub[sub.on]["r"]; g5 = sub[sub.on5]["r"]
            def cell(s):
                return f"{round(s.mean(),2)} (n{len(s)})" if len(s) else "— (n0)"
            md.append(f"| {yr} | {uni} | {round(float(onfrac)*100,0) if pd.notna(onfrac) else '—'}% | "
                      f"{cell(ung)} | **{cell(g_on)}** | {cell(g5)} |")

    # 2022 stand-down + risk-on capture summary
    md.append("\n### Stand-down / capture summary\n")
    for uni in UNIS:
        s = tdf[tdf.u == uni]
        bear = s[s.year == 2022]; ro = s[s.year.isin([2025, 2026])]
        bear_on = bear[bear.on]; ro_on = ro[ro.on]
        md.append(f"- **{uni}**: 2022 ungated {round(bear['r'].mean(),2) if len(bear) else '—'} "
                  f"(n{len(bear)}) → gated {round(bear_on['r'].mean(),2) if len(bear_on) else '—'} "
                  f"(n{len(bear_on)}, {round(100*len(bear_on)/max(len(bear),1),0)}% of bear entries taken). "
                  f"2025–26 ungated {round(ro['r'].mean(),2) if len(ro) else '—'} (n{len(ro)}) → "
                  f"gated {round(ro_on['r'].mean(),2) if len(ro_on) else '—'} (n{len(ro_on)}, "
                  f"{round(100*len(ro_on)/max(len(ro),1),0)}% taken).")

    # leakage check: correlation of gate with PAST vs construction
    md.append("\n### Leakage check\n")
    md.append("- Gate uses only MA/trailing-return/expanding-median of dates **< i** (shift(1) on the "
              "threshold). The **gated-lag5** column delays the gate a further 5 trading days; if results "
              "survive that lag, the edge is not riding an i-coincident peek.\n")

    open(OUT, "w").write("\n".join(md) + "\n")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
