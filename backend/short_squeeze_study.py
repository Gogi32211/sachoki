"""Is short-squeeze fuel (days-to-cover / short interest) a real gate for our reversal edges?

Born from CAR: float 35M, short interest 25.6% of shares out and days-to-cover ~14 through
the whole February-March base, then 9.9x in 40 bars. The obvious hypothesis is that a crowded
short book amplifies the reversal edges we already own (QZ-Capit, Washout, 🧊Coil-Floor) —
the same shape as 🥇lead-in-lag, which is the only new-data idea that has worked lately.

POINT-IN-TIME is the whole ballgame. FINRA publishes ~8 business days after settlement, so
every row is joined on `known_from` (= settlement + 12 calendar days), never on
settlement_date. merge_asof with direction="backward" means a bar can only ever see a reading
that was already public. Getting this wrong would manufacture a beautiful lookahead edge.

Pre-specified order, and the first test can close the whole thing:
  1. DECIDING — is DTC new information, or a restatement of liquidity/beta/price we own?
  2. does it amplify the three reversal edges (bands, so the plateau is visible)?
  3. CONTROL — high DTC with NO edge. If that alone pays, it is a trigger, not a gate,
     and everything in (2) is contaminated by it.
  4. the covering side: is a FALLING short book better than a rising one?
  5. price buckets on whatever survived.
"""
import os, sys
import numpy as np
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import edge_replay as er
import overfit_stats as ofs

N_TRIALS = 18
SI_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "data", "short_interest.parquet")
print(f"PRE-SPECIFIED TRIAL COUNT: {N_TRIALS}\n", flush=True)

si = pd.read_parquet(SI_PATH)
print(f"short interest: {len(si):,} rows · {si.ticker.nunique():,} tickers · "
      f"{si.settlement_date.min()} → {si.settlement_date.max()}", flush=True)

grp, as_of = er._frame(60, 3_000_000)
print(f"frame as_of {as_of} · {len(grp)} tickers", flush=True)

# ── point-in-time join ─────────────────────────────────────────────────────────
si["known_from"] = pd.to_datetime(si["known_from"]).astype("datetime64[ns]")
si = si.sort_values("known_from")
# pre-group: a boolean scan of 2.9M rows per ticker x 5k tickers would take hours
SI_BY_TK = {t: d for t, d in si.groupby("ticker", sort=False)}
matched = 0
for tk, g in grp.items():
    s = SI_BY_TK.get(tk, si.iloc[:0])
    g["_dt"] = pd.to_datetime(g["date"]).astype("datetime64[ns]")
    if s.empty:
        g["dtc"] = np.nan; g["si"] = np.nan; g["si_chg"] = np.nan
        continue
    m = pd.merge_asof(g[["_dt"]].sort_values("_dt"),
                      s[["known_from", "days_to_cover", "short_interest", "si_chg_pct"]],
                      left_on="_dt", right_on="known_from", direction="backward",
                      tolerance=pd.Timedelta(days=45))
    g["dtc"] = m["days_to_cover"].to_numpy()
    g["si"] = m["short_interest"].to_numpy()
    g["si_chg"] = m["si_chg_pct"].to_numpy()
    matched += 1
print(f"joined on known_from (settlement + 12d) for {matched:,} tickers", flush=True)

cov = np.mean([g["dtc"].notna().mean() for g in grp.values()])
print(f"bar coverage: {cov*100:.1f}% of bars carry a published DTC reading", flush=True)

# ── 1. DECIDING TEST ───────────────────────────────────────────────────────────
print("\n===== 1. is days-to-cover NEW information? =====", flush=True)
rows = []
for tk, g in grp.items():
    m = g["dtc"].notna()
    if not m.any():
        continue
    rows.append(pd.DataFrame({
        "dtc": g["dtc"][m], "si_chg": g["si_chg"][m], "close": g["close"][m],
        "rsi": g["rsi_14"][m],
        "beta": g["beta_score"][m] if "beta_score" in g else np.nan,
        "rs": g["rs_intact"][m].astype(float) if "rs_intact" in g else np.nan,
        "atrp": (g["atr_14"][m] / g["close"][m]) if "atr_14" in g else np.nan,
        "dv": (g["close"][m] * g["volume"][m]) if "volume" in g else np.nan,
    }))
X = pd.concat(rows, ignore_index=True)
print(f"  bars with DTC: {len(X):,}", flush=True)
print("  DTC percentiles: " + " · ".join(
    f"p{int(p*100)} {X.dtc.quantile(p):.1f}" for p in [.25, .5, .75, .9, .95, .99]), flush=True)
for c, lab in [("dv", "dollar volume"), ("atrp", "ATR%"), ("beta", "beta_score"),
               ("close", "price"), ("rs", "rs_intact"), ("rsi", "RSI")]:
    if X[c].notna().any():
        print(f"    corr(DTC, {lab:14s}) = {X['dtc'].corr(X[c], method='spearman'):+.3f}",
              flush=True)
hi = X.dtc >= 10
print(f"  DTC>=10 fires on {hi.mean()*100:.1f}% of bars", flush=True)

# ── family for DSR ─────────────────────────────────────────────────────────────
fam = []
for name, col in er.SETUPS:
    tr = er._pathsim(grp, col, "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) >= 30:
        fam.append(ofs.sharpe(tr["ret"].to_numpy()))
print(f"\nboard family: {len(fam)} setups", flush=True)


def run(label, build):
    for tk, g in grp.items():
        g["_B"] = build(g).fillna(False).astype(bool)
    tr = er._pathsim(grp, "_B", "trail", 0.10, 0.25, 0.25, 60, atr_k=12.0)
    if len(tr) < 80:
        print(f"  {label:38s} n={len(tr)} thin", flush=True); return None
    ym = tr.groupby("yr")["ret"].median() * 100
    w = tr["ret"] > 0
    den = -tr.loc[~w, "ret"].sum()
    pf = (tr.loc[w, "ret"].sum() / den) if den > 0 else float("inf")
    d = ofs.dsr(tr["ret"].to_numpy(), fam, n_trials=N_TRIALS)
    med = tr["ret"].median() * 100
    ys = "".join(f"{ym.get(str(y), float('nan')):>7.2f}" for y in range(2021, 2027))
    print(f"  {label:38s} n={len(tr):>6d} med{med:>+7.2f} win{w.mean()*100:>5.1f} "
          f"pf{pf:>5.2f} |{ys} | {int((ym>0).sum())}/{len(ym)} worst{ym.min():>+6.2f} "
          f"DSR{d['dsr']:>6.3f}", flush=True)
    return dict(med=med, n=len(tr), worst=ym.min(), yrs=int((ym > 0).sum()), dsr=d["dsr"])


# ── 2. does it amplify the reversal edges? ─────────────────────────────────────
EDGES = [("QZ-Capit", "E_qzcapit"), ("Washout", "E_washout"), ("🧊Coil-Floor", "E_coilfloor")]
g0 = next(iter(grp.values()))
EDGES = [(n, c) for n, c in EDGES if c in g0]
BANDS = [("no data", lambda g: g["dtc"].isna()),
         ("DTC < 3", lambda g: g["dtc"] < 3),
         ("DTC 3-6", lambda g: g["dtc"].between(3, 6)),
         ("DTC 6-10", lambda g: g["dtc"].between(6, 10)),
         ("DTC >= 10", lambda g: g["dtc"] >= 10)]

print("\n===== 2. squeeze fuel as a gate on the reversal edges =====", flush=True)
for nm, col in EDGES:
    print(f"\n  -- {nm} --", flush=True)
    run("base (no gate)", lambda g, c=col: g[c])
    for lab, f in BANDS:
        run(f"  {lab}", lambda g, c=col, f=f: g[c] & f(g))

# ── 3. CONTROL — is high DTC a trigger on its own? ─────────────────────────────
print("\n===== 3. CONTROL — high DTC with NO edge =====", flush=True)
run("BASELINE (10th bar)", lambda g: pd.Series(np.arange(len(g)) % 10 == 0, index=g.index))
run("DTC>=10 alone (10th bar)",
    lambda g: pd.Series(np.arange(len(g)) % 10 == 0, index=g.index) & (g["dtc"] >= 10))
run("DTC>=10 + RSI<40 (no edge)", lambda g: (g["dtc"] >= 10) & (g["rsi_14"] < 40)
    & pd.Series(np.arange(len(g)) % 5 == 0, index=g.index))

# ── 4. building vs covering ────────────────────────────────────────────────────
print("\n===== 4. is a BUILDING short book better than a covering one? =====", flush=True)
for nm, col in EDGES:
    print(f"  -- {nm} --", flush=True)
    run("  DTC>=6 & short book RISING", lambda g, c=col: g[c] & (g["dtc"] >= 6) & (g["si_chg"] > 0))
    run("  DTC>=6 & short book FALLING", lambda g, c=col: g[c] & (g["dtc"] >= 6) & (g["si_chg"] < 0))

# ── 5. price buckets on the best cell ──────────────────────────────────────────
print("\n===== 5. price buckets · Washout + DTC>=10 =====", flush=True)
for lo, hi_ in [(5, 21), (21, 89), (89, 377)]:
    run(f"Washout+DTC>=10 ${lo}-{hi_}",
        lambda g, a=lo, b=hi_: g["E_washout"] & (g["dtc"] >= 10) & g["close"].between(a, b))

print("\nDONE", flush=True)
