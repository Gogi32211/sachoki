"""
range_timing.py — ANALYSIS ONLY (no production code touched).

PART A: is range_exp PREDICTIVE (fires on a TEST/absorption bar, before the move)
        or LAGGING/coincident (fires on the LAUNCH bar, move already underway)?
        Recomputes bar-level timing fields ONLY for the ~1164 range_exp episodes.
PART B: full payoff distribution + EXPECTANCY (mean) per score bucket / HIGH-LOW /
        gates [acc_tr, range_exp, acc_tr x range_exp]. Uses cached parquet.

Writes RANGE_TIMING_AND_EXPECTANCY.md.  Run: cd backend && uv run python ../analysis/range_timing.py
"""
from __future__ import annotations
import numpy as np, pandas as pd, duckdb

DB = "/Users/sachoki/Downloads/studio_analytics.duckdb"
PARQ = "/Users/sachoki/Desktop/sachoki-desktop/analysis/_candidates.parquet"
OUT = "/Users/sachoki/Desktop/sachoki-desktop/RANGE_TIMING_AND_EXPECTANCY.md"
CASE = {"PAVS", "WNW", "GLOO"}
FWD, MFE, MAE = "fwd_10d", "mfe_10d", "mae_10d"


def clip(s):
    return pd.to_numeric(s, errors="coerce").clip(-100, 500)


# ---------------- PART A ----------------
def part_a(allc):
    re = allc[allc["f4_range_exp"] == 1].copy()
    tickers = sorted(re["ticker"].unique())
    con = duckdb.connect(DB, read_only=True)
    ph = ",".join("?" * len(tickers))
    bars = con.execute(f"""
        SELECT universe,ticker,date,open,high,low,close
        FROM bars WHERE ticker IN ({ph}) ORDER BY universe,ticker,date
    """, tickers).fetchdf()
    con.close()
    bars = bars.drop_duplicates(["universe", "ticker", "date"]).reset_index(drop=True)
    g = bars.groupby(["universe", "ticker"], sort=False)
    bars["prev_close"] = g["close"].shift(1)
    bars["same_bar_ret"] = (bars["close"] / bars["prev_close"] - 1) * 100
    rng = (bars["high"] - bars["low"]).replace(0, np.nan)
    bars["close_pos"] = ((bars["close"] - bars["low"]) / rng).clip(0, 1)
    bars["low20"] = g["low"].transform(lambda s: s.rolling(20, min_periods=5).min())
    bars["pct_from_20d_low"] = (bars["close"] / bars["low20"] - 1) * 100
    bars["ret_5d"] = (bars["close"] / g["close"].shift(5) - 1) * 100

    m = re.merge(bars[["universe", "ticker", "date", "close_pos", "same_bar_ret",
                       "pct_from_20d_low", "ret_5d"]],
                 on=["universe", "ticker", "date"], how="left")
    m = m[np.isfinite(m["close_pos"]) & np.isfinite(m["same_bar_ret"])].copy()

    # classify
    def cls(r):
        if r["close_pos"] >= 0.5 and r["same_bar_ret"] >= 20:
            return "LAUNCH"
        if r["close_pos"] < 0.5 and r["same_bar_ret"] < 20:
            return "TEST"
        return "MIXED"
    m["bucket"] = m.apply(cls, axis=1)
    return m


def a_metrics(d):
    if len(d) == 0:
        return dict(n=0)
    fwd = clip(d[FWD]); mfe = d[MFE]
    return dict(n=len(d),
                med_fwd=round(float(np.median(fwd)), 2),
                mean_fwd=round(float(np.mean(fwd)), 2),
                p50=round(float((mfe >= 50).mean() * 100), 1),
                p100=round(float((mfe >= 100).mean() * 100), 1),
                win=round(float((d[FWD] > 0).mean() * 100), 1),
                cp=round(float(d["close_pos"].mean()), 2),
                sbr=round(float(d["same_bar_ret"].median()), 1),
                ext=round(float(d["pct_from_20d_low"].median()), 1))


def fmt_a(label, m):
    if m.get("n", 0) == 0:
        return f"| {label} | 0 | — | — | — | — | — | — | — | — |"
    return (f"| {label} | {m['n']} | {m['cp']} | {m['sbr']} | {m['ext']} | "
            f"{m['mean_fwd']} | {m['med_fwd']} | {m['p50']} | {m['p100']} | {m['win']} |")


# ---------------- PART B ----------------
def dist(d):
    if len(d) == 0:
        return dict(n=0)
    f = pd.to_numeric(d[FWD], errors="coerce").dropna()
    fc = f.clip(-100, 500)
    mfe = d[MFE]; mae = d[MAE]
    pos = f[f > 0]; neg = f[f < 0]
    mean_win = float(pos.mean()) if len(pos) else 0.0
    mean_loss = float(neg.mean()) if len(neg) else 0.0
    exp = float(fc.mean()); std = float(fc.std())
    q = f.quantile([.05, .25, .5, .75, .90, .95, .99]).to_dict()
    return dict(
        n=len(d), exp=round(exp, 2), std=round(std, 1),
        risk_adj=round(exp / std, 3) if std else np.nan,
        p5=round(q[.05], 1), p25=round(q[.25], 1), p50=round(q[.5], 1),
        p75=round(q[.75], 1), p90=round(q[.90], 1), p95=round(q[.95], 1), p99=round(q[.99], 1),
        ploss=round(float((f < 0).mean() * 100), 1), mean_loss=round(mean_loss, 1),
        mean_mae=round(float(mae.mean()), 1),
        pwin=round(float((f > 0).mean() * 100), 1), mean_win=round(mean_win, 1),
        p25g=round(float((mfe >= 25).mean() * 100), 1), p50g=round(float((mfe >= 50).mean() * 100), 1),
        p100g=round(float((mfe >= 100).mean() * 100), 1), p200g=round(float((mfe >= 200).mean() * 100), 1),
        payoff=round(mean_win / abs(mean_loss), 2) if mean_loss else np.nan,
        cap_gap=round(float(np.median(mfe) - np.median(f)), 1),
    )


def fmt_dist(label, m):
    if m.get("n", 0) == 0:
        return f"| {label} | 0 |" + " — |" * 13
    return (f"| {label} | {m['n']} | **{m['exp']}** | {m['p5']} | {m['p25']} | {m['p50']} | "
            f"{m['p75']} | {m['p90']} | {m['p95']} | {m['p99']} | {m['std']} | {m['risk_adj']} | {m['cap_gap']} |")


def fmt_pay(label, m):
    if m.get("n", 0) == 0:
        return f"| {label} | 0 |" + " — |" * 10
    return (f"| {label} | {m['n']} | **{m['exp']}** | {m['ploss']} | {m['mean_loss']} | {m['mean_mae']} | "
            f"{m['pwin']} | {m['mean_win']} | {m['payoff']} | {m['p50g']} | {m['p100g']} | {m['p200g']} |")


DIST_H = ("| segment | n | EXPECTANCY | p5 | p25 | p50 | p75 | p90 | p95 | p99 | std | exp/std | cap-gap |\n"
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|")
PAY_H = ("| segment | n | EXPECTANCY | P(loss)% | mean loss | mean MAE | P(win)% | mean win | payoff | P(+50%) | P(+100%) | P(+200%) |\n"
         "|---|---|---|---|---|---|---|---|---|---|---|---|")


def segments(d):
    segs = []
    for s in range(5):
        segs.append((f"score {s}", d[d["score"] == s]))
    segs.append(("LOW (<2)", d[d["score"] < 2]))
    segs.append(("HIGH (>=2)", d[d["score"] >= 2]))
    segs.append(("gate: acc_tr", d[d["f1_acc_tr"] == 1]))
    segs.append(("gate: range_exp", d[d["f4_range_exp"] == 1]))
    segs.append(("gate: acc_tr x range_exp", d[(d["f1_acc_tr"] == 1) & (d["f4_range_exp"] == 1)]))
    return segs


def main():
    allc = pd.read_parquet(PARQ)
    oos = allc[~allc["ticker"].isin(CASE)]
    md = ["# range_exp fire-timing + expectancy", ""]
    md.append("_Follow-up to DISCRIMINATOR_VALIDATION.md. PART A recomputes bar-level "
              "timing for the range_exp subset; PART B uses the cached 1.1M-episode parquet. "
              "Units = percent. Headline numbers exclude PAVS/WNW/GLOO (OOS). No production code touched._")

    # ===== PART A =====
    md.append("\n## PART A — is range_exp PREDICTIVE or LAGGING?\n")
    m = part_a(oos)            # OOS for headline
    md.append(f"range_exp episodes with timing fields (OOS, ex-PAVS/WNW/GLOO): **{len(m)}** "
              f"(nasdaq {int((m.universe=='nasdaq').sum())}, russell2k {int((m.universe=='russell2k').sum())}, "
              f"sp500 {int((m.universe=='sp500').sum())}).\n")
    # extension stat
    for thr in (50, 100, 200):
        pct = float((m["pct_from_20d_low"] > thr).mean() * 100)
        md.append(f"- {pct:.0f}% of range_exp bars are already **>{thr}% above their 20-day low** (extended).")
    md.append(f"- median close_pos = {m['close_pos'].median():.2f}, median same-bar return = "
              f"{m['same_bar_ret'].median():.1f}%, median %-from-20d-low = {m['pct_from_20d_low'].median():.0f}%.\n")
    md.append("**TEST/absorption** = close_pos<0.5 & same-bar<20% (spike rejected). "
              "**LAUNCH** = close_pos≥0.5 & same-bar≥20% (move underway). MIXED = the rest.\n")
    md.append("| universe · bucket | n | close_pos | same-bar% | ext%(20dlow) | mean fwd% | med fwd% | P(+50%) | P(+100%) | win% |")
    md.append("|---|---|---|---|---|---|---|---|---|---|")
    for uni in ("russell2k", "nasdaq"):
        du = m[m["universe"] == uni]
        for b in ("TEST", "LAUNCH", "MIXED"):
            md.append(fmt_a(f"{uni} · {b}", a_metrics(du[du["bucket"] == b])))
        md.append(fmt_a(f"{uni} · ALL range_exp", a_metrics(du)))
    # small-cap combined (clearly labelled supplementary, since per-uni buckets are thin)
    md.append("| | | | | | | | | | |")
    for b in ("TEST", "LAUNCH", "MIXED"):
        md.append(fmt_a(f"[supp] r2k+nasdaq · {b}", a_metrics(m[m["bucket"] == b])))

    # ===== PART B =====
    md.append("\n\n## PART B — expectancy + payoff distribution\n")
    md.append("EXPECTANCY = mean fwd_10d per trade (clipped [-100,500]). cap-gap = median(MFE)−median(fwd) "
              "= tail given back if you exit on close. payoff = mean_win/|mean_loss|.\n")
    for uni in ("russell2k", "nasdaq", "sp500"):
        d = oos[oos["universe"] == uni]
        md.append(f"\n### {uni}  (n={len(d):,}, OOS)\n")
        md.append("#### Distribution & risk-adjusted\n")
        md.append(DIST_H)
        for lbl, seg in segments(d):
            md.append(fmt_dist(lbl, dist(seg)))
        md.append("\n#### Payoff decomposition\n")
        md.append(PAY_H)
        for lbl, seg in segments(d):
            md.append(fmt_pay(lbl, dist(seg)))

    open(OUT, "w").write("\n".join(md) + "\n")
    print("wrote", OUT)
    # quick console signal for the verdict
    for uni in ("russell2k", "nasdaq"):
        du = m[m["universe"] == uni]
        for b in ("TEST", "LAUNCH"):
            mm = a_metrics(du[du["bucket"] == b])
            print(uni, b, mm)


if __name__ == "__main__":
    main()
